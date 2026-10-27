"""Per-newnew control over the existing Relay event engine.

Only queue/report timing, first-placement decisions and versioned observations
change. RobustEnv remains responsible for execution, Relay and completion.
"""

import bisect
from collections import deque
import heapq
import math
import time
import gymnasium as gym
import numpy as np
from .service import RobustEnv, AreaCurve
from .batch import candidates
from arbor.admission.pressure import (FEATURES, SCHEMA, Normalizer, proportions, random_uniform,
                      sample_target, PRESSURE_SCALE, RESIDUAL_BOUND)
from .fast_facts import FastFacts, mono_ns


def pressure(env):
    result = np.array([row['Assigned']-row['Confirmed'] for row in env.signals], np.float64)
    for shard, row in enumerate(env.signals):
        if row.get('Queue') is not None:
            result[shard] = max(result[shard], row['Queue'])
    return result


class LightEnv(RobustEnv):
    MODES = ('A', 'pressure', 'model')

    def __init__(self, source, prior_factory, calibration, *, policy='model',
                 placement_context='light-v2/development', queue_report_seconds=1., **kwargs):
        if policy not in self.MODES or queue_report_seconds != 1.:
            raise ValueError('frozen policy/one-second telemetry contract')
        random_uniform(placement_context, 0)
        self.policy, self.placement_context = policy, placement_context
        self.queue_report_seconds = float(queue_report_seconds)
        original_factory = prior_factory
        def prepared_factory():
            prior = original_factory()
            if hasattr(prior, 'prepare_at'):
                prior.prepare_at(int(source[2][0]))
            return prior
        super().__init__(source, prepared_factory, calibration, **kwargs)
        self.action_space = gym.spaces.Box(-np.inf, np.inf, (self.shards,), np.float64)
        self.observation_space = gym.spaces.Dict(dict(
            shards=gym.spaces.Box(-np.inf, np.inf, (self.shards, len(FEATURES)), np.float32)))

    def reset(self, **kwargs):
        self.proportions = np.full(self.shards, 1/self.shards)
        self.previous_share = self.proportions.copy()
        self.admission_trace = []
        self.fast_facts = None
        self.fast_fact_cpu = self.sampling_cpu = self.feature_cpu = 0.
        self.queue_capture_sequence = np.zeros(self.shards, np.int64)
        self.queue_received_sequence = np.full(self.shards, -1, np.int64)
        self.queue_received_at = np.full(self.shards, np.nan)
        self.queue_independent = np.zeros(self.shards, bool)
        self.queue_reports = []
        self.queue_stale_reports = 0
        self.actual_services = [deque(maxlen=5) for _ in range(self.shards)]
        self.actual_service_cache = [None]*self.shards
        self.visible_service = [None]*self.shards
        self.service_received_height = np.zeros(self.shards, np.int64)
        self.receipt_bodies = [deque() for _ in range(self.shards)]
        self.graph_received_at = 0.
        self.clock_id = 'simulation-light-v2'
        self.startup_queue_scheduled = False
        return super().reset(**kwargs)

    def _ensure_execution(self):
        super()._ensure_execution()
        if self.fast_facts is None:
            self.initial_routes = self.owners[:self.initial_n].copy()
            self.fast_facts = FastFacts(self.shards, self.initial_n, len(self.owners),
                                       self.base, self.clock_id)
        if not self.startup_queue_scheduled:
            self.startup_queue_scheduled = True
            for shard in range(self.shards):
                self._event(0., 'queue_tick', shard)

    def _event(self, at, kind, payload=None):
        if kind in ('queue_tick', 'queue_report'):
            # An equal-time startup report precedes the released batch. During
            # dispatch, ready occurrences retain their serial prefix order.
            priority = -2 if kind == 'queue_tick' else -1
            self.serial += 1
            heapq.heappush(self.events, (float(at), priority, self.serial, kind, payload))
        else:
            super()._event(at, kind, payload)

    def _receive_service(self, shard, report, received_at):
        if report is None:
            return
        height = int(report['height'])
        if height <= self.service_received_height[shard]:
            return
        if report['committed'] > received_at or report['body_size'] <= 0:
            raise AssertionError('invalid nonempty service evidence')
        self.service_received_height[shard] = height
        self.visible_service[shard] = dict(report, received_at=float(received_at))

    def _deliver_queue(self, at, shard, sequence, captured, clock_id=None):
        if clock_id != self.clock_id:
            raise ValueError('foreign/missing queue report instance')
        if sequence <= self.queue_received_sequence[shard]:
            self.queue_stale_reports += 1
            return False
        if captured['captured_at'] > at or captured['queue'] < 0:
            raise ValueError('queue report timestamp/count invalid')
        self.queue_received_sequence[shard] = sequence
        self.queue_received_at[shard] = at
        self.queue_independent[shard] = True
        self.signals[shard]['Queue'] = int(captured['queue'])
        self._receive_service(shard, captured['service'], at)
        self.queue_reports.append(dict(received_at=at, shard=shard, sequence=sequence,
                                       **captured, confirms_stages=False))
        return True

    def _deliver(self, at, shard, height, captured):
        independent = self.queue_independent[shard]
        previous_queue = self.signals[shard].get('Queue')
        before = int(self.report_height[shard])
        super()._deliver(at, shard, height, captured)
        if independent:
            self.signals[shard]['Queue'] = previous_queue
        elif height > before:
            self.queue_received_at[shard] = at
        if height <= before:
            return
        pending = self.receipt_bodies[shard]
        while pending and pending[0][0] <= height:
            _, body = pending.popleft()
            for sequence, leg in body:
                self.fast_facts.confirm(sequence, leg, shard, at)

    def _handle(self, kind, payload):
        self._ensure_execution()
        if kind == 'queue_tick':
            shard = int(payload)
            sequence = int(self.queue_capture_sequence[shard])
            self.queue_capture_sequence[shard] += 1
            captured = dict(captured_at=self.now, queue=len(self.queues[shard]),
                            service=self.actual_service_cache[shard])
            self._event(self.now+self.calendar.report_delay, 'queue_report',
                        (shard, sequence, captured, self.clock_id))
            self._event(self.now+self.queue_report_seconds, 'queue_tick', shard)
            return
        if kind == 'queue_report':
            self._deliver_queue(self.now, *payload)
            return
        if kind == 'commit':
            shard, body, proposed = payload
            height = int(self.block_height[shard])+1
            self.receipt_bodies[shard].append((height, body))
            if body:
                duration = self.now-proposed
                self.actual_services[shard].append(duration)
                service = dict(height=height, committed=self.now, body_size=len(body),
                               duration_mean=float(np.mean(self.actual_services[shard])),
                               fill=len(body)/self.capacity)
                self.actual_service_cache[shard] = service
                self.execution_signals[shard]['LightService'] = service
        super()._handle(kind, payload)
        if kind == 'publish':
            row = self.visible_prior
            if row['graph_cutoff'] != row['forest_cutoff'] or \
                    row['graph_source_time'] != row['forest_source_time'] or \
                    row['accepted_cutoff_exclusive'] > self.base+self.routed:
                raise AssertionError('mixed/noncausal graph-forest generation')
            self.graph_received_at = self.now

    def _run_to_decision(self):
        u, v, _ = self.source
        if self.roots is None:
            self.roots = np.full(len(self.owners), -1, np.int64)
        self._ensure_execution()
        while self.completed < len(u):
            while self.ready:
                sequence = self.ready.popleft()
                a, b = int(u[sequence]), int(v[sequence])
                known_a, known_b = self.owners[a] >= 0, self.owners[b] >= 0
                if not known_a and not known_b:
                    self.pending_decision = sequence
                    return
                target = int(self.owners[a if known_a else b]) if known_a != known_b else None
                self._place(sequence, target)
            if not self.events:
                raise AssertionError('unfinished occurrences without service')
            at, _, _, kind, payload = heapq.heappop(self.events)
            self._advance_clock(at)
            self._handle(kind, payload)
        self.pending_decision = None

    def _place(self, sequence, target=None):
        a, b = int(self.source[0][sequence]), int(self.source[1][sequence])
        known_a, known_b = self.owners[a] >= 0, self.owners[b] >= 0
        super()._place(sequence, target)
        start = time.process_time()
        source, destination = map(int, self.route[sequence])
        self.fast_facts.assign(sequence, a, b, bool(known_a), bool(known_b),
                               source, destination, self.now)
        self.fast_fact_cpu += time.process_time()-start

    def raw_features(self):
        if self.pending_decision is None:
            raise RuntimeError('no newnew decision')
        start = time.process_time()
        raw = np.zeros((self.shards, len(FEATURES)), np.float64)
        current = np.array([row['Assigned']-row['Confirmed'] for row in self.signals], np.int64)
        raw[:, 0] = current/1000.
        times = [item[0] for item in self.history]
        index = bisect.bisect_right(times, self.now-5.)-1
        if self.now >= 5. and index >= 0:
            raw[:, 4] = (current-self.history[index][1])/1000.
        else:
            raw[:, 5] = 1.
        for shard, row in enumerate(self.signals):
            queue = row.get('Queue')
            if queue is None or not np.isfinite(self.queue_received_at[shard]):
                raw[shard, 3] = 1.
            else:
                raw[shard, 1] = queue/1000.
                raw[shard, 2] = (self.now-self.queue_received_at[shard])/5.
            service = self.visible_service[shard]
            if service is None:
                raw[shard, 9] = 1.
            else:
                raw[shard, 6:9] = (service['fill'], service['duration_mean']/5.,
                                   (self.now-service['received_at'])/5.)
        prior = self.visible_prior
        if prior is None:
            raw[:, 13] = 1.
        else:
            cutoff = int(prior['accepted_cutoff_exclusive'])
            source_time = int(prior['source_time'])
            if cutoff > self.base+self.pending_decision or source_time > int(self.source[2][self.pending_decision]):
                raise AssertionError('future prior at decision')
            rates = np.asarray(prior['relation_stage_rate_by_scale'], np.float64)
            if rates.shape != (self.shards, 3) or not np.isfinite(rates).all() or np.any(rates < 0):
                raise ValueError('day graph feature shape/value mismatch')
            raw[:, 10] = np.log1p(rates[:, 1]*3600.)
            raw[:, 11] = (int(self.source[2][self.pending_decision])-source_time)/3600.
            raw[:, 12] = (self.now-self.graph_received_at)/5.
            self.max_lag = max(self.max_lag, self.base+self.pending_decision-cutoff)
        facts = self.fast_facts.snapshot(self.now)
        if facts['assigned_prefix'] != self.pending_decision:
            raise AssertionError('forest query is not strictly before current occurrence')
        raw[:, 14] = np.log1p(facts['new_origins_20s'])
        raw[:, 15] = np.log1p(facts['active_origins_20s'])
        raw[:, 16] = np.asarray(facts['assigned_nn_half_stages_5s'])/2000.
        raw[:, 17] = facts['concentration_20s']
        raw[:, 18] = facts['no_activity_20s']
        raw[:, 19] = self.previous_share
        if not np.isfinite(raw).all() or np.any(raw[:, [2, 8, 11, 12]] < 0):
            raise AssertionError('invalid causal feature clock/value')
        self.feature_cpu += time.process_time()-start
        return raw

    def _observation(self):
        raw = self.raw_features()
        return dict(shards=raw.astype(np.float32) if self.normal is None else self.normal.transform(raw))

    def step(self, action):
        if self.pending_decision is None:
            raise ValueError('no pending per-newnew decision')
        latent = np.asarray(action, np.float64)
        if latent.shape != (self.shards,) or not np.isfinite(latent).all():
            raise ValueError('invalid residual latent')
        sequence, started = self.pending_decision, self.now
        ordinal = len(self.admission_trace)
        ca, cb = candidates(self)
        current_pressure = pressure(self)
        effective = latent if self.policy == 'model' else np.zeros(self.shards)
        self.proportions, residual, logits = proportions(current_pressure, effective)
        uniform = random_uniform(self.placement_context, ordinal)
        target = ca if self.policy == 'A' else sample_target(self.proportions, uniform)
        self.mode = 0 if self.policy == 'A' else 1
        self.admission_trace.append(dict(sequence=sequence, batch=sequence//self.batch,
            ordinal=ordinal, uniform=uniform, target=target, A=ca, B=cb, time=started,
            runtime_mono_ns=mono_ns(started), clock_id=self.clock_id,
            random_seed=43, placement_context=self.placement_context))
        self.decision_trace.append(dict(sequence=sequence, batch=sequence//self.batch,
            ordinal=ordinal, time=started, action=self.mode, A=ca, B=cb,
            runtime_mono_ns=mono_ns(started), clock_id=self.clock_id,
            pressure=current_pressure.tolist(), latent=latent.tolist(), residual=residual.tolist(),
            logits=logits.tolist(), proportions=self.proportions.tolist(),
            prior_cutoff=self.visible_prior['accepted_cutoff_exclusive'],
            prior_source_time=self.visible_prior['source_time'], prior_version=self.visible_prior['version']))
        self.previous_share = self.proportions.copy()
        self._place(sequence, target)
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
        info = dict(delta_sim_seconds=self.now-started, occurrences_routed=self.routed)
        if terminal:
            info['episode_report'] = self.report()
            obs = dict(shards=np.zeros((self.shards, len(FEATURES)), np.float32))
        else:
            obs = self._observation()
        return obs, reward, terminal, False, info

    def audit_bindings(self):
        owners = np.full(len(self.owners), -1, np.int32)
        owners[:self.initial_n] = self.initial_routes
        population = np.bincount(self.initial_routes, minlength=self.shards)
        work = np.zeros(self.shards, np.int64)
        ordinal = 0
        for sequence, (a, b) in enumerate(zip(self.source[0], self.source[1])):
            known_a, known_b = owners[a] >= 0, owners[b] >= 0
            if not known_a and not known_b:
                row, decision = self.admission_trace[ordinal], self.decision_trace[ordinal]
                uniform = random_uniform(self.placement_context, ordinal)
                if row['sequence'] != sequence or decision['sequence'] != sequence or \
                        row['ordinal'] != ordinal or row['uniform'] != uniform:
                    raise AssertionError('per-entry random/decision evidence mismatch')
                target = min(range(self.shards), key=lambda s: (work[s], population[s], s)) \
                    if self.policy == 'A' else sample_target(decision['proportions'], uniform)
                if row['target'] != target:
                    raise AssertionError('first placement decision mismatch')
                owners[a] = owners[b] = target
                population[target] += 1 if a == b else 2
                ordinal += 1
            elif known_a != known_b:
                known, novel = (a, b) if known_a else (b, a)
                owners[novel] = owners[known]
                population[owners[known]] += 1
            if tuple(self.route[sequence]) != (owners[a], owners[b]):
                raise AssertionError('binding replay differs at '+str(sequence))
            work[owners[a]] += 1
            if owners[a] != owners[b]:
                work[owners[b]] += 1
        if ordinal != len(self.admission_trace) or ordinal != len(self.decision_trace):
            raise AssertionError('unused entry evidence')
        np.testing.assert_array_equal(owners, self.owners)
        np.testing.assert_array_equal(population, self.population)
        np.testing.assert_array_equal(work, self.work)
        return dict(status='PASS', occurrences=len(self.route), bindings=ordinal,
                    random_algorithm='SHA256 compact JSON [43,ASCII context,ordinal]; first53 bits')

    def report(self):
        report = super().report()
        for key in ('B_fraction', 'effective_B_choices', 'rule_switches', 'first_newnew_disagreements'):
            report.pop(key, None)
        p = np.asarray([row['proportions'] for row in self.decision_trace])
        expected = p.sum(0)
        actual = np.bincount([row['target'] for row in self.admission_trace], minlength=self.shards)
        report.update(schema=SCHEMA, policy=self.policy, placement_context=self.placement_context,
            feature_count=len(FEATURES), decision_frequency='each newnew first binding',
            binding_audit=self.audit_bindings(), forest_work_audit=self.fast_facts.audit(len(self.route)),
            share_entropy=float((-p*np.log(np.maximum(p, 1e-300))).sum(1).mean()),
            share_max_mean=float(p.max(1).mean()), newnew_by_shard=actual.tolist(),
            target_expected_newnew_by_shard=expected.tolist(), expected_shares_applicable=self.policy != 'A',
            same_batch_share_frozen=False, pressure_scale=PRESSURE_SCALE, residual_bound=RESIDUAL_BOUND,
            queue_report_seconds=self.queue_report_seconds, queue_reports=len(self.queue_reports),
            queue_stale_reports=self.queue_stale_reports, queue_reports_confirm_stages=False,
            graph_receive_age_at_transaction_end=self.now-self.graph_received_at,
            feature_cpu_seconds=self.feature_cpu, fast_forest_cpu_seconds=self.fast_fact_cpu)
        return report
