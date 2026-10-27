"""Candidate target adapter over the existing LightEnv execution engine."""

import time
import numpy as np
from .light import LightEnv, pressure
from .fast_facts import mono_ns
from arbor.admission.pressure import proportions
from arbor.admission.candidates import (SCHEMA, COST_FEATURES, MAX_CANDIDATES,
    pressure_candidates, sample_candidates, choose_candidate)

DEFAULT_SAMPLE_TIMES = (0., 5., 10., 20., 35., 50., 65., 80., 95., 110., 130., 150.)


class CostEnv(LightEnv):
    """Only target selection differs; placement, Relay, service and reports reuse LightEnv."""
    def __init__(self, *args, cost_policy='P', model=None, capture_callback=None,
                 sample_times=DEFAULT_SAMPLE_TIMES, **kwargs):
        if cost_policy not in ('P', 'R4', 'M4') or (cost_policy == 'M4') != (model is not None):
            raise ValueError('M4 requires a frozen model; other policies cannot load one')
        if capture_callback is not None and cost_policy != 'P':
            raise ValueError('state capture is restricted to fixed P backbone')
        times = tuple(float(value) for value in sample_times)
        if any(not np.isfinite(value) or value < 0 for value in times) or \
                any(a >= b for a, b in zip(times, times[1:])):
            raise ValueError('sample times must be finite, nonnegative and strictly increasing')
        self.cost_policy, self.cost_model = cost_policy, model
        self.capture_callback, self.sample_times = capture_callback, times
        self.capture_cursor, self.capture_trace = 0, []
        kwargs.pop('policy', None)
        super().__init__(*args, policy='pressure', **kwargs)

    @property
    def nn_ordinal(self):
        return len(self.admission_trace)

    def reset(self, **kwargs):
        self.previous_pressure_share = np.full(self.shards, 1 / self.shards)
        self.capture_cursor, self.capture_trace = 0, []
        self.ranker_cpu_seconds = self.candidate_cpu_seconds = 0.
        self.ranker_wall_times = []
        self.maintenance_wall = dict(observe=0.,publish=0.)
        self.maintenance_cpu = dict(observe=0.,publish=0.)
        return super().reset(**kwargs)

    def _handle(self, kind, payload):
        if kind not in ('observe','publish'):
            return super()._handle(kind,payload)
        wall,cpu=time.perf_counter(),time.process_time()
        result=super()._handle(kind,payload)
        self.maintenance_wall[kind]+=time.perf_counter()-wall
        self.maintenance_cpu[kind]+=time.process_time()-cpu
        return result

    def raw_features(self):
        raw = super().raw_features()
        raw[:, 19] = self.previous_pressure_share
        return raw

    def decision_context(self):
        if self.pending_decision is None:
            raise ValueError('no current first-binding decision')
        raw = self.raw_features()
        current_pressure = pressure(self)
        # Keep P's original integer-counter -> probability arithmetic exactly;
        # do not divide feature slots and multiply them back into stage counts.
        probability, _, _ = proportions(current_pressure)
        candidates, uniforms = sample_candidates(probability, self.placement_context, self.nn_ordinal)
        return dict(sequence=int(self.pending_decision), nn_ordinal=self.nn_ordinal,
                    now=float(self.now), raw_features=raw, pressure=current_pressure,
                    proportions=probability, candidates=candidates, uniforms=uniforms,
                    placement_context=self.placement_context,
                    source_time=int(self.source[2][self.pending_decision]))

    def _capture(self, context):
        if self.capture_callback is None:
            return
        due = []
        while self.capture_cursor < len(self.sample_times) and \
                self.sample_times[self.capture_cursor] <= self.now:
            due.append(self.sample_times[self.capture_cursor])
            self.capture_cursor += 1
        if due:
            # Multiple thresholds crossed without a newnew occurrence share
            # one real state. Never duplicate the same sequence into training.
            context = dict(context, trigger_times=tuple(due))
            sequence, routed = self.pending_decision, self.routed
            self.capture_callback(self, context)
            if self.pending_decision != sequence or self.routed != routed:
                raise AssertionError('capture changed the P backbone state')
            self.capture_trace.append(dict(sequence=int(sequence), now=float(self.now),
                                           trigger_times=tuple(due)))

    def step(self, action=None):
        if action is not None:
            raise ValueError('candidate policy is frozen; no Gaussian/external action')
        started_cpu = time.process_time()
        context = self.decision_context()
        self.candidate_cpu_seconds += time.process_time() - started_cpu
        self._capture(context)
        raw, candidates = context['raw_features'], context['candidates']
        costs = None
        if self.cost_policy == 'M4':
            from arbor.admission.model import scores
            began = time.process_time()
            began_wall = time.perf_counter()
            costs = np.asarray(scores(self.cost_model, raw, candidates), np.float64)
            self.ranker_wall_times.append(time.perf_counter()-began_wall)
            self.ranker_cpu_seconds += time.process_time() - began
        target = choose_candidate(self.cost_policy, candidates, context['pressure'], costs)
        r4 = choose_candidate('R4', candidates, context['pressure'])
        sequence, started, ordinal = self.pending_decision, self.now, self.nn_ordinal
        self.mode = 1
        self.proportions = context['proportions'].copy()
        common = dict(sequence=sequence, batch=sequence // self.batch, ordinal=ordinal,
            uniform=float(context['uniforms'][0]), target=target, A=int(candidates[0]), B=r4,
            time=started, runtime_mono_ns=mono_ns(started), clock_id=self.clock_id,
            random_seed=43, placement_context=self.placement_context,
            candidates=candidates.tolist(), candidate_uniforms=context['uniforms'].tolist(),
            pressure=context['pressure'].tolist(), proportions=self.proportions.tolist(),
            candidate_costs=None if costs is None else costs.tolist(),
            r4_target=r4, cost_policy=self.cost_policy)
        self.admission_trace.append(common)
        self.decision_trace.append(dict(common, action=1,
            raw_features=raw.tolist(), feature_order=list(COST_FEATURES),
            prior_cutoff=self.visible_prior['accepted_cutoff_exclusive'],
            prior_source_time=self.visible_prior['source_time'], prior_version=self.visible_prior['version']))
        self.previous_pressure_share = self.proportions.copy()
        self.previous_share = self.proportions.copy()
        # Reuse the common execution path and complete-drain accounting.
        self._place(sequence, target)
        self.decisions += 1
        self.pending_decision = None
        self._run_to_decision()
        terminal = self.pending_decision is None
        until = max(self.now, self.reference.end) if terminal and self.reference is not None else self.now
        reference_area = 0. if self.reference is None else self.reference.at(until)
        comparison = (reference_area - self.area) / (self.capacity * self.interval)
        reward = comparison - self.previous_comparison
        self.previous_comparison = comparison
        self.reward_total += reward
        info = dict(delta_sim_seconds=self.now-started, occurrences_routed=self.routed)
        if terminal:
            info['episode_report'] = self.report()
            observation = dict(shards=np.zeros((self.shards, 20), np.float32))
        else:
            observation = self._observation()
        return observation, reward, terminal, False, info

    def audit_bindings(self):
        owners = np.full(len(self.owners), -1, np.int32)
        owners[:self.initial_n] = self.initial_routes
        population = np.bincount(self.initial_routes, minlength=self.shards)
        work = np.zeros(self.shards, np.int64)
        ordinal = 0
        for sequence, (a, b) in enumerate(zip(self.source[0], self.source[1])):
            ka, kb = owners[a] >= 0, owners[b] >= 0
            if not ka and not kb:
                row = self.admission_trace[ordinal]
                candidates, uniforms = sample_candidates(row['proportions'], self.placement_context, ordinal)
                np.testing.assert_array_equal(candidates, row['candidates'])
                np.testing.assert_array_equal(uniforms, row['candidate_uniforms'])
                target = choose_candidate(self.cost_policy, candidates, row['pressure'], row['candidate_costs'])
                if row['sequence'] != sequence or row['ordinal'] != ordinal or row['target'] != target:
                    raise AssertionError('candidate binding replay mismatch')
                owners[a] = owners[b] = target
                population[target] += 1 if a == b else 2
                ordinal += 1
            elif ka != kb:
                known, novel = (a, b) if ka else (b, a)
                owners[novel] = owners[known]
                population[owners[known]] += 1
            if tuple(self.route[sequence]) != (owners[a], owners[b]):
                raise AssertionError('immutable binding replay mismatch')
            work[owners[a]] += 1
            if owners[a] != owners[b]:
                work[owners[b]] += 1
        if ordinal != len(self.admission_trace):
            raise AssertionError('unused candidate decisions')
        np.testing.assert_array_equal(owners, self.owners)
        np.testing.assert_array_equal(work, self.work)
        np.testing.assert_array_equal(population, self.population)
        return dict(status='PASS', occurrences=len(self.route), bindings=ordinal,
            candidate_zero_equals_pressure=True, without_replacement=True)

    def report(self):
        value = super().report()
        value.update(schema=SCHEMA, policy=self.cost_policy, feature_order=list(COST_FEATURES),
            maximum_candidates=MAX_CANDIDATES, gaussian_exploration=False,
            previous_share_semantics='previous base pressure distribution, regardless of selected target',
            expected_shares_applicable=self.cost_policy == 'P', captures=self.capture_trace,
            ranker_cpu_seconds=self.ranker_cpu_seconds,
            candidate_cpu_seconds=self.candidate_cpu_seconds,
            graph_forest_observe_wall_seconds=self.maintenance_wall['observe'],
            graph_forest_publish_wall_seconds=self.maintenance_wall['publish'],
            graph_forest_observe_cpu_seconds=self.maintenance_cpu['observe'],
            graph_forest_publish_cpu_seconds=self.maintenance_cpu['publish'])
        value.pop('residual_bound', None)
        return value
