"""Action-dependent FIFO Relay proxy. Wall and historical clocks stay separate.

No transaction is fabricated for a relay leg. Reward integrates the number of
released original occurrences that have not completed. This is NOT PBFT/EVM.
"""
from collections import deque
import heapq
import math
import time
import gymnasium as gym
from gymnasium import spaces
import numpy as np
SHARDS, CAPACITY, INTERVAL, TPS, BATCH = 16, 1000, 5., 2000, 1000
from .features import FEATURES, build_features


def pressure_choice(signals, prior, population):
    pending = np.array([x['Assigned']-x['Confirmed'] for x in signals], dtype=float)
    pressure = np.array([max(p, x['Queue']) if x.get('Queue') is not None else p
                         for p, x in zip(pending, signals)])
    channels = [np.asarray(prior[k], dtype=float) for k in
                ('relation_stage_rate_proxy', 'forest_incidence_rate_proxy')]
    risk = sum((x/x.sum() for x in channels if x.sum() > 0), start=np.zeros(len(signals)))
    return min(range(len(signals)), key=lambda s: (pressure[s], risk[s], population[s], s))


class PlacementEnv(gym.Env):
    metadata = {}

    def __init__(self, source, prior_factory, calibration,
                 normalizer=None, split='fit', shards=SHARDS, capacity=CAPACITY,
                 interval=INTERVAL, tps=TPS, batch=BATCH, record=False):
        super().__init__()
        self.source = source
        self.prior_factory = prior_factory
        self.calibration = calibration
        self.normalizer, self.record = normalizer, record
        self.shards, self.capacity, self.interval, self.tps, self.batch = shards, capacity, interval, tps, batch
        self.action_space = spaces.Discrete(shards)
        self.observation_space = spaces.Box(-np.inf, np.inf, (shards, len(FEATURES)), np.float32)
        self.resets = 0

    def _event(self, at, kind, payload=None):
        # Same-time contracts: released input -> commit -> observer -> proposal.
        priority = {'release':0, 'commit':1, 'observe':2, 'publish':2, 'proposal':3}[kind]
        self.serial += 1
        heapq.heappush(self.events, (float(at), priority, self.serial, kind, payload))

    def _advance_clock(self, at):
        if at < self.now:
            raise AssertionError('simulation clock decreased')
        self.area += (at-self.now)*self.unfinished
        self.now = at

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        self.resets += 1
        self.prior = self.prior_factory()
        self.base = self.prior.cutoff
        self.initial_n = self.prior.n
        u, v, t = self.source
        if not len(t) or t[0] < self.prior.as_of or np.any(t[1:] < t[:-1]):
            raise ValueError('input predates seed or is not chronological')
        self.prior.prepare_at(int(t[0]))
        maximum = max(int(np.max(u)), int(np.max(v)), self.initial_n-1)
        self.owners = np.full(maximum+1, -1, np.int32)
        self.owners[:self.initial_n] = self.prior.routes[:self.initial_n]
        self.population = np.bincount(self.owners[:self.initial_n], minlength=self.shards)
        self.work = np.zeros(self.shards, np.int64)
        self.signals = [dict(Assigned=0, Confirmed=0, Queue=None, BodyLength=0, Intervals=[])
                        for _ in range(self.shards)]
        self.queues = [deque() for _ in range(self.shards)]
        self.busy = [False]*self.shards
        self.events, self.ready, self.serial = [], deque(), 0
        self.now = self.area = self.reward_area = 0.
        self.unfinished = self.completed = self.routed = self.decisions = 0
        self.release = np.arange(len(t), dtype=float)//self.batch*self.batch/self.tps
        self.done_at = np.full(len(t), np.nan)
        self.route = np.full((len(t), 2), -1, np.int16)
        self.pending_decision = None
        self.observer_rows, self.observer_tasks = [], deque()
        self.observer_busy = False
        self.visible_prior = self.prior.candidate_demand()
        self.visible_profiles = self.prior.entry_profiles
        self.observer_versions = 0
        self.max_lag = 0
        self.feature_records, self.teacher_records = [], []
        self.cpu_start = time.process_time()
        self._event(0., 'release', 0)
        for s in range(self.shards):
            self._event(self.interval, 'proposal', s)
        self._run_to_decision()
        if self.pending_decision is None:
            raise ValueError('episode has no first-placement decisions')
        self.predecision_constant_area = self.area
        return self._observation(), {}

    def _complete(self, i):
        if np.isfinite(self.done_at[i]):
            raise AssertionError('duplicate original completion')
        self.done_at[i] = self.now
        self.unfinished -= 1
        self.completed += 1

    def _flush_observer(self):
        if self.observer_rows:
            self.observer_tasks.append(self.observer_rows)
            self.observer_rows = []
        self._start_observer()

    def _start_observer(self):
        if self.observer_busy or not self.observer_tasks:
            return
        self.observer_busy = True
        rows = self.observer_tasks.popleft()
        duration = (self.calibration['observer_poll_seconds'] +
                    len(rows)*self.calibration['observer_per_occurrence_seconds'])
        self._event(self.now+duration, 'observe', rows)

    def _place(self, i, target=None):
        u, v, t = self.source
        a, b = int(u[i]), int(v[i])
        sa, sb = int(self.owners[a]), int(self.owners[b])
        if sa < 0 or sb < 0:
            owner = target
            if owner is None or not 0 <= owner < self.shards:
                raise ValueError('first placement requires a valid action')
            for x in dict.fromkeys((a, b)):
                if self.owners[x] < 0:
                    self.owners[x] = owner
                    self.population[owner] += 1
            sa, sb = int(self.owners[a]), int(self.owners[b])
        self.route[i] = sa, sb
        self.queues[sa].append((i, 0))
        self.work[sa] += 1
        self.signals[sa]['Assigned'] += 1
        if sa != sb:
            self.work[sb] += 1
            self.signals[sb]['Assigned'] += 1
        self.routed += 1
        self.observer_rows.append(i)
        # Observe only routed occurrences; raw arrays never imply admission.
        first = self.observer_rows[0]
        if (len(self.observer_rows) >= self.batch or i+1 == len(t) or
                t[i]//3600 != t[first]//3600):
            self._flush_observer()

    def _handle(self, kind, payload):
        u, v, t = self.source
        if kind == 'release':
            lo = payload
            hi = min(lo+self.batch, len(t))
            self.ready.extend(range(lo, hi))
            self.unfinished += hi-lo
            if hi < len(t):
                self._event(hi/self.tps, 'release', hi)
        elif kind == 'proposal':
            s = payload
            if self.busy[s]:
                raise AssertionError('overlapping shard proposals')
            body = [self.queues[s].popleft() for _ in range(min(self.capacity, len(self.queues[s])))]
            self.busy[s] = True
            self._event(self.now+self.calibration['commit_seconds'], 'commit', (s, body, self.now))
        elif kind == 'commit':
            s, body, proposed = payload
            self.busy[s] = False
            row = self.signals[s]
            for i, leg in body:
                row['Confirmed'] += 1
                if row['Confirmed'] > row['Assigned']:
                    raise AssertionError('unassigned committed leg')
                sa, sb = self.route[i]
                if leg == 0 and sa != sb:
                    # No relay second leg is visible before source commit.
                    self.queues[sb].append((i, 1))
                else:
                    self._complete(i)
            if 'CommitAt' in row:
                row['Intervals'] = (row['Intervals']+[self.now-row['CommitAt']])[-5:]
            row.update(CommitAt=self.now, ReceivedAt=self.now, Queue=len(self.queues[s]), BodyLength=len(body))
            self._event(max(proposed+self.interval, self.now), 'proposal', s)
        elif kind == 'observe':
            rows = payload
            lo, hi = rows[0], rows[-1]+1
            if rows != list(range(lo, hi)) or self.prior.cutoff != self.base+lo:
                raise AssertionError('observer prefix hole')
            old = self.prior.snapshot['version']
            self.prior.observe(np.asarray(u[lo:hi]), np.asarray(v[lo:hi]), np.asarray(t[lo:hi]),
                               self.route[lo:hi].astype(np.int64), self.base+lo)
            snapshot = self.prior.candidate_demand()
            if snapshot['version'] != old:
                costs = self.calibration['observer_publication_seconds']
                cost = costs[self.observer_versions % len(costs)]
                self.observer_versions += 1
                self._event(self.now+cost, 'publish', (snapshot,self.prior.entry_profiles))
            else:
                self.observer_busy = False
                self._start_observer()
        elif kind == 'publish':
            snapshot, profiles = payload
            if snapshot['accepted_cutoff_exclusive'] > self.base+self.routed:
                raise AssertionError('future publication')
            self.visible_prior, self.visible_profiles = snapshot, profiles
            self.observer_busy = False
            self._start_observer()
        else:
            raise AssertionError(kind)

    def _run_to_decision(self):
        u, v, _ = self.source
        while self.completed < len(u):
            while self.ready:
                i = self.ready.popleft()
                if self.owners[u[i]] < 0 or self.owners[v[i]] < 0:
                    self.pending_decision = i
                    return
                self._place(i)
            if not self.events:
                raise AssertionError('unfinished transactions without service events')
            at, _, _, kind, payload = heapq.heappop(self.events)
            self._advance_clock(at)
            self._handle(kind, payload)
        self.pending_decision = None

    def raw_observation(self):
        if self.pending_decision is None:
            raise RuntimeError('no current decision')
        i = self.pending_decision
        self.max_lag = max(self.max_lag, self.base+i-self.visible_prior['accepted_cutoff_exclusive'])
        context,partner = self.entry_context()
        profile = self.visible_profiles[partner] if 0<=partner<len(self.visible_profiles) else None
        return build_features(self.signals, self.visible_prior, self.population,
            decision_sequence=self.base+i, source_time=int(self.source[2][i]), wall_time=self.now,
            block_capacity=self.capacity, block_interval=self.interval, batch_size=self.batch,
            cumulative_work=self.work,context=context,partner_profile=profile)

    def entry_context(self):
        i=self.pending_decision
        a,b=int(self.source[0][i]),int(self.source[1][i])
        ka,kb=self.owners[a]>=0,self.owners[b]>=0
        partner=a if ka else b if kb else -1
        return dict(known_sender=bool(ka),known_recipient=bool(kb),self_transfer=a==b,
                    novel_distinct_count=len({x for x in (a,b) if self.owners[x]<0}),
                    partner_shard=int(self.owners[partner]) if partner>=0 else -1),partner

    def _observation(self):
        raw = self.raw_observation()
        return np.asarray(raw if self.normalizer is None else self.normalizer.transform(raw), dtype=np.float32)

    def cumulative_action(self):
        context,_=self.entry_context()
        if context['partner_shard']>=0:return context['partner_shard']
        return min(range(self.shards), key=lambda s: (self.work[s], self.population[s], s))

    def pressure_action(self):
        return pressure_choice(self.signals, self.visible_prior, self.population)

    def step(self, action):
        if self.pending_decision is None or not self.action_space.contains(action):
            raise ValueError('no valid pending first-placement action')
        action_time=self.now
        if self.record:
            self.feature_records.append(self.raw_observation())
            self.teacher_records.append(self.cumulative_action())
        self._place(self.pending_decision, int(action))
        self.decisions += 1
        self.pending_decision = None
        self._run_to_decision()
        reward = -(self.area-self.reward_area)/(self.capacity*self.interval)
        self.reward_area = self.area
        done = self.pending_decision is None
        info = dict(occurrences_routed=self.routed,action_time_s=action_time,
                    next_decision_or_completion_time_s=self.now,delta_sim_seconds=self.now-action_time)
        if done:
            report = self.report()
            info['episode_report'] = report
            obs = np.zeros(self.observation_space.shape, np.float32)
        else:
            obs = self._observation()
        return obs, reward, done, False, info

    def report(self):
        if self.completed != len(self.source[0]):
            raise ValueError('report requested before drain')
        lat = self.done_at-self.release
        stages = len(lat)+int(np.count_nonzero(self.route[:, 0] != self.route[:, 1]))
        if stages != int(self.work.sum()) or stages != sum(x['Confirmed'] for x in self.signals):
            raise AssertionError('Relay stage conservation failed')
        if not math.isclose(float(lat.sum()), self.area, rel_tol=1e-10, abs_tol=1e-5):
            raise AssertionError('unfinished integral != sum original sojourns')
        return dict(completed=self.completed, unfinished=0, decisions=self.decisions,
            predecision_constant_area=self.predecision_constant_area,
            latency_mean=float(lat.mean()), latency_p95=float(np.quantile(lat,.95)),
            latency_p99=float(np.quantile(lat,.99)), completion_tps=len(lat)/self.done_at.max(),
            stages=stages, stages_by_shard=self.work.tolist(),
            unfinished_integral=self.area, sum_sojourns=float(lat.sum()),
            reward_sum=-self.area/(self.capacity*self.interval),
            drain_seconds=float(self.done_at.max()-self.release[-1]),
            max_prior_lag_occurrences=self.max_lag, observer_cutoff_at_transaction_end=self.prior.cutoff,
            cpu_seconds=time.process_time()-self.cpu_start,
            limitations=['FIFO timing proxy, no PBFT/balance/EVM emulation',
                        'terminal observer catch-up is separate from transaction drain',
                        'no virtual relay network delay; development calibration only'])


def rollout(env, policy):
    observation, _ = env.reset(seed=43)
    total = 0.
    while True:
        action = policy(env, observation)
        observation, reward, done, _, info = env.step(int(action))
        total += reward
        if done:
            report = info['episode_report']
            if not math.isclose(total, report['reward_sum'], rel_tol=1e-10, abs_tol=1e-5):
                raise AssertionError('terminal reward omitted')
            return report
