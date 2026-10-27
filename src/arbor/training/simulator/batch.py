"""Batch control adapter over the existing action-dependent Relay event engine."""
import heapq
import math
import time
import gymnasium as gym
import numpy as np
FIT_DAYS = tuple(range(13,21))
from .environment import PlacementEnv
from .features import FEATURES

N_FEATURES = 19
FLAG_COLUMNS = (5, 6, 15)


def candidates(env):
    a = min(range(env.shards), key=lambda s: (env.work[s], env.population[s], s))
    def key(s):
        row = env.signals[s]
        outstanding = row['Assigned'] - row['Confirmed']
        q = row.get('Queue')
        return (outstanding if q is None else max(outstanding, q), env.population[s], s)
    return int(a), int(min(range(env.shards), key=key))


class AreaCurve:
    """Integral of released, incompletely executed original transactions."""
    def __init__(self, release, completed):
        self.release = np.sort(np.asarray(release, np.float64))
        self.completed = np.sort(np.asarray(completed, np.float64))
        if len(self.release) != len(self.completed) or not np.isfinite(self.completed).all():
            raise ValueError('incomplete reference curve')
        self.prefix = [np.r_[0., np.cumsum(x)] for x in (self.release, self.completed)]
        self.end = float(self.completed[-1])

    def at(self, t):
        value = 0.
        for sign, array, sums in zip((1, -1), (self.release, self.completed), self.prefix):
            n = np.searchsorted(array, t, side='right')
            value += sign * (n*t - sums[n])
        return float(value)




class BatchEnv(PlacementEnv):
    def __init__(self, *args, reference=None, normal=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.reference, self.normal = reference, normal
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Dict({
            'shards': gym.spaces.Box(-np.inf, np.inf, (self.shards, 19), np.float32),
            'candidates': gym.spaces.MultiDiscrete([self.shards, self.shards]),
            'previous': gym.spaces.Box(0., 1., (2,), np.float32)})

    def reset(self, **kwargs):
        self.mode, self.mode_batch, self.previous = 0, -1, 0
        self.in_block = np.zeros(self.shards, np.int64)
        self.reserved = np.zeros(self.shards, np.int64)
        self.queue_trace, self.nn_trace, self.decision_trace = [], [], []
        self.roots = None
        self.divergent_roots = set()
        self.divergent_associated_stages = np.zeros(self.shards, np.int64)
        self.divergent_first_stages = np.zeros(self.shards, np.int64)
        self.reward_total = self.previous_comparison = 0.
        self.ledger_checks = 0
        obs, info = super().reset(**kwargs)
        if self.reference is not None and not np.array_equal(self.release, self.reference.release):
            raise AssertionError('reference and policy arrivals differ')
        return obs, info

    def _run_to_decision(self):
        u, v, _ = self.source
        if self.roots is None:
            self.roots = np.full(len(self.owners), -1, np.int64)
        while self.completed < len(u):
            while self.ready:
                i = self.ready.popleft()
                a, b = int(u[i]), int(v[i])
                ka, kb = self.owners[a] >= 0, self.owners[b] >= 0
                if not ka and not kb and i // self.batch != self.mode_batch:
                    self.pending_decision = i
                    return
                target = None
                if not ka and not kb:
                    target = candidates(self)[self.mode]
                elif not ka or not kb:
                    target = int(self.owners[a if ka else b])
                self._place(i, target)
            if not self.events:
                raise AssertionError('unfinished transactions without service')
            at, _, _, kind, payload = heapq.heappop(self.events)
            self._advance_clock(at)
            self._handle(kind, payload)
        self.pending_decision = None

    def _place(self, i, target=None):
        a, b = int(self.source[0][i]), int(self.source[1][i])
        ka, kb = self.owners[a] >= 0, self.owners[b] >= 0
        nn = not ka and not kb
        if nn:
            ca, cb = candidates(self)
            self.roots[a] = self.roots[b] = i
            if ca != cb:
                self.divergent_roots.add(i)
            self.nn_trace.append((i, i//self.batch, ca, cb, int(target), self.mode, self.now))
        elif not ka or not kb:
            self.roots[b if ka else a] = self.roots[a if ka else b]
        super()._place(i, target)
        sa, sb = map(int, self.route[i])
        if sa != sb:
            self.reserved[sb] += 1
        related = self.roots[a] in self.divergent_roots or self.roots[b] in self.divergent_roots
        if related:
            self.divergent_associated_stages[sa] += 1
            if sa != sb:
                self.divergent_associated_stages[sb] += 1
            if nn:
                self.divergent_first_stages[sa] += 1

    def check_ledger(self):
        pending = np.array([x['Assigned']-x['Confirmed'] for x in self.signals])
        expected = np.array([len(q) for q in self.queues]) + self.in_block + self.reserved
        np.testing.assert_array_equal(pending, expected)
        for s, row in enumerate(self.signals):
            if row['Queue'] is not None and row['Queue'] > pending[s]:
                raise AssertionError('proxy Q_last exceeds outstanding work')
        self.ledger_checks += 1

    def _handle(self, kind, payload):
        if kind == 'proposal':
            self.in_block[payload] = min(self.capacity, len(self.queues[payload]))
        elif kind == 'commit':
            s, body, _ = payload
            self.in_block[s] = 0
            for i, leg in body:
                sa, sb = self.route[i]
                if leg == 0 and sa != sb:
                    self.reserved[sb] -= 1
        super()._handle(kind, payload)
        if kind in ('proposal', 'commit'):
            self.check_ledger()
            s = payload if kind == 'proposal' else payload[0]
            self.queue_trace.append(dict(time=self.now, kind=kind, shard=int(s),
                queue=[len(q) for q in self.queues], in_block=self.in_block.tolist(),
                reserved=self.reserved.tolist(),
                pending=[r['Assigned']-r['Confirmed'] for r in self.signals],
                body_size=int(self.in_block[s]) if kind == 'proposal' else len(payload[1])))

    def raw_features(self):
        self.check_ledger()
        return super().raw_observation()[:, :19]

    def _observation(self):
        raw = self.raw_features()
        return dict(shards=(raw.astype(np.float32) if self.normal is None else self.normal.transform(raw)),
                    candidates=np.array(candidates(self), np.int64),
                    previous=np.eye(2, dtype=np.float32)[self.previous])

    def step(self, action):
        if self.pending_decision is None or not self.action_space.contains(action):
            raise ValueError('invalid batch action')
        i, start = self.pending_decision, self.now
        ca, cb = candidates(self)
        self.mode = self.previous = int(action)
        self.mode_batch = i // self.batch
        self.decision_trace.append(dict(sequence=i, batch=self.mode_batch, time=start,
            A=ca, B=cb, action=int(action), prior_cutoff=self.visible_prior['accepted_cutoff_exclusive'],
            prior_source_time=self.visible_prior['source_time'], prior_version=self.visible_prior['version']))
        self._place(i, (ca, cb)[action])
        self.decisions += 1
        self.pending_decision = None
        self._run_to_decision()
        terminal = self.pending_decision is None
        until = max(self.now, self.reference.end) if terminal and self.reference is not None else self.now
        reference_area = 0. if self.reference is None else self.reference.at(until)
        comparison = (reference_area-self.area)/(self.capacity*self.interval)
        reward = comparison-self.previous_comparison
        self.previous_comparison = comparison
        self.reward_total += reward
        info = dict(delta_sim_seconds=self.now-start, occurrences_routed=self.routed)
        if terminal:
            info['episode_report'] = self.report()
            obs = dict(shards=np.zeros((self.shards, 19), np.float32),
                       candidates=np.zeros(2, np.int64), previous=np.array([1., 0.], np.float32))
        else:
            obs = self._observation()
        return obs, reward, terminal, False, info

    def report(self):
        report = super().report()
        reference_area = 0. if self.reference is None else self.reference.at(self.reference.end)
        expected_reward = (reference_area-self.area)/(self.capacity*self.interval)
        if not math.isclose(self.reward_total, expected_reward, abs_tol=1e-6, rel_tol=1e-10):
            raise AssertionError('complete-drain comparison reward mismatch')
        curve = AreaCurve(self.release, self.done_at)
        last = float(self.release[-1])
        first_boundary = min(10., last)
        phases = [curve.at(first_boundary), curve.at(last)-curve.at(first_boundary),
                  curve.at(curve.end)-curve.at(last)]
        np.testing.assert_allclose(sum(phases), self.area, rtol=1e-10, atol=1e-5)
        nn = np.array(self.nn_trace, dtype=float)
        disagreement = nn[:, 2] != nn[:, 3]
        choices = np.array([r['action'] for r in self.decision_trace])
        report.update(reward_sum=self.reward_total, reference_area=reference_area,
            max_shard_work=int(self.work.max()), ledger_checks=self.ledger_checks,
            pressure_semantics='Q_last <= U; B equals outstanding-stage balancing in this proxy',
            newnew_occurrences=len(nn), divergent_newnew=int(disagreement.sum()),
            divergent_batches=int(len(np.unique(nn[disagreement, 1]))),
            first_newnew_disagreements=sum(x['A'] != x['B'] for x in self.decision_trace),
            B_fraction=float(choices.mean()),
            effective_B_choices=int(np.count_nonzero((nn[:, 5] == 1) & disagreement)),
            rule_switches=int(np.count_nonzero(choices[1:] != choices[:-1])),
            divergent_root_associated_stages=self.divergent_associated_stages.tolist(),
            divergent_first_stages=self.divergent_first_stages.tolist(),
            associated_work_is_not_eliminable_work=True, phases_wait_integral=phases,
            phase_boundaries=[0., first_boundary, last, curve.end],
            graph_forest_observe_wall_seconds=self.prior.observe_seconds,
            graph_forest_publish_wall_seconds=self.prior.publish_seconds,
            published_cutoff_at_transaction_end=self.visible_prior['accepted_cutoff_exclusive'],
            observation_unconsumed_occurrences=self.base+len(self.source[0])-self.prior.cutoff,
            observer_catchup_run=False, graph_forest_source_age_at_end=int(self.source[2][-1])-self.visible_prior['source_time'])
        return report
