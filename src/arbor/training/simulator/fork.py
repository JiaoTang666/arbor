"""Isolated, action-dependent P continuations of the repaired Relay engine.

Only the current newnew target is forced. Later first placements use exactly
the same pressure probabilities, counter-random context and admission ordinal.
Slow graph/source maintenance can be omitted because P never reads those facts.
"""

import copy
from dataclasses import dataclass
import heapq
import math
import time
import numpy as np
from .light import LightEnv, pressure
from arbor.admission.pressure import proportions, random_uniform, sample_target
from .fast_facts import mono_ns


class StageProjection:
    """Outstanding verified stage identities, without copied source-family maps."""
    def __init__(self, facts):
        self.cutoff, self.completed = int(facts.cutoff), int(facts.completed)
        self.assigned = np.array(facts.assigned, dtype=np.int64, copy=True)
        self.confirmed = np.array(facts.confirmed, dtype=np.int64, copy=True)
        self.pending = {
            int(sequence): dict(legs=tuple(row['legs']), done=set(row['done']), at=int(row['at']))
            for sequence, row in facts.pending.items()
        }

    def assign(self, sequence, a, b, known_a, known_b, source, target, at):
        if sequence != self.cutoff or sequence in self.pending or a == b and source != target:
            raise ValueError('fork stage assignment prefix/self mismatch')
        legs = (int(source),) if source == target else (int(source), int(target))
        self.pending[sequence] = dict(legs=legs, done=set(), at=mono_ns(at))
        for shard in legs:
            self.assigned[shard] += 1
        self.cutoff += 1

    def confirm(self, sequence, leg, shard, at):
        row = self.pending.get(sequence)
        if row is None or not 0 <= leg < len(row['legs']) or row['legs'][leg] != shard or mono_ns(at) < row['at']:
            raise ValueError('fork stage receipt identity/route/clock mismatch')
        if leg in row['done']:
            return False
        row['done'].add(leg)
        self.confirmed[shard] += 1
        if len(row['done']) == len(row['legs']):
            del self.pending[sequence]
            self.completed += 1
        return True

    def audit(self, expected=None):
        if np.any(self.confirmed > self.assigned):
            raise AssertionError('fork stage confirmations exceed assignments')
        if expected is not None and (self.cutoff != expected or self.completed != expected or self.pending):
            raise AssertionError('fork stage occurrence coverage incomplete')
        return dict(status='PASS', assigned_prefix=self.cutoff, completed=self.completed,
                    assigned_stages=self.assigned.tolist(), confirmed_stages=self.confirmed.tolist(),
                    outstanding_occurrences=len(self.pending),
                    attribution='execution-stage projection; source-family maps are intentionally omitted')


@dataclass(frozen=True)
class PressureSnapshot:
    sequence: int
    nn_ordinal: int
    start_time: float
    source: tuple
    calendar: object
    calibration: dict
    _state: dict
    capture_seconds: float

    @property
    def shards(self):
        return int(self._state['shards'])


_OMIT = {
    'source', 'calendar', 'calibration', 'prior', 'prior_factory', 'normal', 'normalizer', 'reference',
    'fast_facts', 'visible_prior', 'visible_profiles', 'action_space', 'observation_space', 'spec',
    '_np_random', '_np_random_seed', 'feature_records', 'teacher_records', 'decision_trace',
    'admission_trace', 'queue_trace', 'nn_trace', 'report_events', 'service_events', 'queue_reports',
    'observer_rows', 'observer_tasks', 'observer_busy', 'divergent_roots',
    'divergent_associated_stages', 'divergent_first_stages',
    'capture_callback', 'capture_trace', 'capture_cursor', 'cost_model',
    'ranker_wall_times',
}


def capture_pressure_state(environment):
    """Capture before the current NN binding; do not copy or mutate slow priors."""
    began = time.perf_counter()
    if not isinstance(environment, LightEnv) or environment.pending_decision is None:
        raise ValueError('capture requires a pending LightEnv newnew decision')
    sequence = int(environment.pending_decision)
    if sequence != environment.routed:
        raise ValueError('capture is not at the admitted continuous prefix')
    a, b = int(environment.source[0][sequence]), int(environment.source[1][sequence])
    if environment.owners[a] >= 0 or environment.owners[b] >= 0:
        raise ValueError('capture cannot override an already bound account')
    if environment.execution_signals is None or environment.fast_facts is None:
        raise ValueError('capture lacks execution/fact state')
    projection = StageProjection(environment.fast_facts)
    if not np.array_equal(projection.assigned, environment.work) or not np.array_equal(
            projection.confirmed, [row['Confirmed'] for row in environment.signals]):
        raise ValueError('capture work and valid receipt ledgers disagree')
    # Remove slow-observer payloads before deepcopy; those may contain full
    # graph/forest arrays. All execution/report/telemetry payloads remain.
    state = {key: value for key, value in environment.__dict__.items() if key not in _OMIT}
    state['events'] = [event for event in environment.events if event[3] not in ('observe', 'publish')]
    # Filtering an array-backed heap is NOT a heap-preserving operation: an
    # internal removed node can shift smaller children behind larger parents.
    # Restore the event ordering before any continuation processes the list.
    heapq.heapify(state['events'])
    state = copy.deepcopy(state)
    state['fast_facts'] = projection
    ordinal = int(getattr(environment, 'nn_ordinal', len(environment.admission_trace)))
    if ordinal != len(environment.admission_trace):
        raise ValueError('capture ordinal disagrees with first-binding evidence')
    shared_source = []
    for array in environment.source:
        view = np.asarray(array).view()
        view.flags.writeable = False
        shared_source.append(view)
    return PressureSnapshot(sequence=sequence, nn_ordinal=ordinal, start_time=float(environment.now),
        source=tuple(shared_source), calendar=environment.calendar,
        calibration=copy.deepcopy(environment.calibration), _state=state,
        capture_seconds=time.perf_counter()-began)


class _PressureContinuation(LightEnv):
    def _flush_observer(self):
        # This operation never affects the original event kernel's queues,
        # commit calendar or visible confirmations.
        self.observer_rows = []

    def _start_observer(self):
        return

    def _event(self, at, kind, payload=None):
        if kind in ('observe', 'publish'):
            raise AssertionError('slow observation leaked into P-only continuation')
        super()._event(at, kind, payload)

    def _handle(self, kind, payload):
        if kind in ('observe', 'publish'):
            raise AssertionError('slow observer event was not removed from snapshot')
        super()._handle(kind, payload)

    def _target(self, sequence, forced=None):
        # Share the original fixed-P arithmetic with CostEnv. The normalized
        # model features are never inverted to reconstruct integer work.
        probability, _, _ = proportions(pressure(self))
        uniform = random_uniform(self.placement_context, self._fork_nn_ordinal)
        expected = sample_target(probability, uniform)
        if forced is not None and (isinstance(forced, (bool, np.bool_)) or int(forced) != forced):
            raise ValueError('candidate target must be an integer shard')
        target = expected if forced is None else int(forced)
        if not 0 <= target < self.shards:
            raise ValueError('candidate target outside shard range')
        self.admission_trace.append(dict(sequence=int(sequence), ordinal=self._fork_nn_ordinal,
            uniform=uniform, target=target, pressure_target=expected, time=self.now))
        self.previous_share = probability.copy()
        self.proportions = probability
        self._fork_nn_ordinal += 1
        self.decisions += 1
        return target

    def _run_to_decision(self):
        # Control adapter only: proposals, commits, Relay legs and releases are
        # processed by the original LightEnv/RobustEnv/PlacementEnv handlers.
        u, v, _ = self.source
        while self.completed < len(u):
            while self.ready:
                sequence = self.ready.popleft()
                a, b = int(u[sequence]), int(v[sequence])
                known_a, known_b = self.owners[a] >= 0, self.owners[b] >= 0
                target = self._target(sequence) if not known_a and not known_b else \
                    int(self.owners[a if known_a else b]) if known_a != known_b else None
                self._place(sequence, target)
            if not self.events:
                raise AssertionError('fork unfinished transactions without execution events')
            at, _, _, kind, payload = heapq.heappop(self.events)
            self._advance_clock(at)
            self._handle(kind, payload)
        self.pending_decision = None


def restore_pressure_state(snapshot):
    """Each restore owns mutable execution state; input/calendar stay shared."""
    if not isinstance(snapshot, PressureSnapshot):
        raise TypeError('not a pressure snapshot')
    environment = object.__new__(_PressureContinuation)
    environment.__dict__.update(copy.deepcopy(snapshot._state))
    environment.source, environment.calendar = snapshot.source, snapshot.calendar
    environment.calibration = copy.deepcopy(snapshot.calibration)
    environment.prior = environment.prior_factory = None
    environment.normal = environment.normalizer = environment.reference = None
    environment.visible_prior = environment.visible_profiles = None
    environment.observer_rows, environment.observer_tasks, environment.observer_busy = [], [], False
    environment.feature_records, environment.teacher_records = [], []
    environment.decision_trace, environment.admission_trace = [], []
    environment.queue_trace, environment.nn_trace = [], []
    environment.report_events, environment.service_events, environment.queue_reports = [], [], []
    environment.divergent_roots = set()
    environment.divergent_associated_stages = np.zeros(environment.shards, np.int64)
    environment.divergent_first_stages = np.zeros(environment.shards, np.int64)
    environment.mode = 1
    environment._fork_nn_ordinal = snapshot.nn_ordinal
    return environment


def _finish(environment, snapshot, target, began):
    # Match RobustEnv.report's receipt-only closeout without invoking the old
    # reward/controller summaries or advancing transaction completion times.
    if not environment.reports_drained:
        for at, _, _, kind, payload in sorted(environment.events):
            if kind == 'report':
                environment._deliver(at, *payload)
        environment.reports_drained = True
    n = len(environment.source[0])
    if environment.completed != n or not np.isfinite(environment.done_at).all() or \
            not np.isfinite(environment.received_at).all():
        raise AssertionError('fork did not completely execute and receive all transactions')
    actual = np.asarray([row['Confirmed'] for row in environment.execution_signals], np.int64)
    visible = np.asarray([row['Confirmed'] for row in environment.signals], np.int64)
    if not np.array_equal(actual, environment.work) or not np.array_equal(visible, environment.work):
        raise AssertionError('fork executed/confirmed stage ledger does not drain')
    cross = environment.route[:, 0] != environment.route[:, 1]
    stages = n+int(cross.sum())
    projected = np.bincount(environment.route[:, 0], minlength=environment.shards)
    projected += np.bincount(environment.route[cross, 1], minlength=environment.shards)
    if stages != int(environment.work.sum()) or not np.array_equal(projected, environment.work):
        raise AssertionError('fork directed Relay stage conservation failed')
    latency = environment.done_at-environment.release
    if np.any(latency < 0) or not math.isclose(float(latency.sum()), environment.area, rel_tol=1e-10, abs_tol=1e-5):
        raise AssertionError('fork full waiting integral does not equal completed sojourns')
    audit = environment.fast_facts.audit(n)
    if not np.array_equal(environment.fast_facts.assigned, environment.work) or not np.array_equal(environment.fast_facts.confirmed, environment.work):
        raise AssertionError('fork independent stage projection disagrees with routing')
    first = environment.admission_trace[0]
    return dict(routes=environment.route.copy(), completed=environment.done_at.copy(),
        received=environment.received_at.copy(), work=environment.work.copy(), area=float(environment.area),
        stages=stages, completed_count=n, unfinished=0, completion_time=float(environment.done_at.max()),
        release=environment.release.copy(), latency_mean=float(latency.mean()),
        latency_p99=float(np.quantile(latency, .99)), sequence=snapshot.sequence,
        target=int(target), pressure_target=first['pressure_target'], nn_ordinal=snapshot.nn_ordinal,
        context=environment.placement_context, future_newnew_decisions=len(environment.admission_trace)-1,
        stage_audit=audit, continuation_seconds=time.perf_counter()-began,
        observer_removed=True, hidden_execution_used_as_actor_input=False,
        limitation='One forced binding followed by P; full hindsight continuation is a training label, not a deployed lookahead.')


def rollout_candidate(snapshot, target):
    began = time.perf_counter()
    environment = restore_pressure_state(snapshot)
    sequence = snapshot.sequence
    if environment.pending_decision != sequence or environment.routed != sequence:
        raise ValueError('restored snapshot does not identify pending current binding')
    chosen = environment._target(sequence, target)
    environment._place(sequence, chosen)
    environment.pending_decision = None
    environment._run_to_decision()
    return _finish(environment, snapshot, chosen, began)
