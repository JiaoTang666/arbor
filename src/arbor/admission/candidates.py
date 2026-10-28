"""Finite causal candidate sampling and cost-ranking contract."""
import hashlib
import json
import numpy as np
from .pressure import FEATURES, proportions, random_uniform, sample_target

SCHEMA = 'entry_cost_ranker_v1'
COST_FEATURES = tuple(FEATURES[:-1]) + ('previous_pressure_share',)
MAX_CANDIDATES = 4
TIE_TOLERANCE = 1e-6
SEED = 43


def contract():
    """Serialization contract shared by training exports and native inference."""
    return dict(feature_order=list(COST_FEATURES), candidates=MAX_CANDIDATES, seed=SEED,
                first="original P CDF using original pressure arithmetic",
                additional="counter-hash independent domain; remaining positive probability without replacement",
                tie_tolerance_cost_units=TIE_TOLERANCE, tie_order="candidate order",
                continuation="one current target forced, every subsequent NN fixed P; complete input and drain",
                pressure="max(queue,assigned-visible_confirmed), missing queue uses unconfirmed",
                previous_share="previous base pressure proportions, never a learned output")


def candidate_uniform(context, ordinal, k):
    """Candidate zero consumes exactly the existing P counter-random draw."""
    if isinstance(k, bool) or int(k) != k or k < 0:
        raise ValueError('invalid candidate draw index')
    baseline = random_uniform(context, ordinal, seed=SEED)
    if k == 0:
        return baseline
    encoded = json.dumps([SEED, context, int(ordinal), int(k)],
                         ensure_ascii=True, separators=(',', ':')).encode('utf-8')
    number = int.from_bytes(hashlib.sha256(encoded).digest()[:8], 'big') >> 11
    return number / float(1 << 53)


def pressure_from_features(raw):
    raw = np.asarray(raw, np.float64)
    if raw.ndim != 2 or raw.shape[1] != 20 or not np.isfinite(raw).all():
        raise ValueError('invalid causal cost feature matrix')
    if np.any(raw[:, :2] < 0) or np.any((raw[:, 3] != 0) & (raw[:, 3] != 1)):
        raise ValueError('invalid visible pressure or missing flag')
    return np.where(raw[:, 3] == 0, np.maximum(raw[:, 0], raw[:, 1]), raw[:, 0]) * 1000.


def sample_candidates(probabilities, context, ordinal):
    p = np.asarray(probabilities, np.float64)
    # sample_target checks the original distribution, including exact zero
    # mass and the same right-open CDF rule as the production pressure policy.
    first_u = candidate_uniform(context, ordinal, 0)
    first = sample_target(p, first_u)
    remaining = p.copy()
    remaining[first] = 0.
    candidates, uniforms = [first], [first_u]
    for k in range(1, min(MAX_CANDIDATES, int(np.count_nonzero(p > 0)))):
        available = np.flatnonzero(remaining > 0)
        weights = remaining[available]
        weights = weights / weights.sum()
        uniform = candidate_uniform(context, ordinal, k)
        selected = int(available[sample_target(weights, uniform)])
        candidates.append(selected)
        uniforms.append(uniform)
        remaining[selected] = 0.
    return np.asarray(candidates, np.int64), np.asarray(uniforms, np.float64)


def choose_candidate(policy, candidates, pressure, costs=None):
    candidates = np.asarray(candidates, np.int64)
    pressure = np.asarray(pressure, np.float64)
    if policy not in ('P', 'R4', 'M4') or candidates.ndim != 1 or not len(candidates) or \
            len(np.unique(candidates)) != len(candidates) or np.any(candidates < 0) or \
            np.any(candidates >= len(pressure)) or not np.isfinite(pressure).all():
        raise ValueError('invalid cost policy candidates')
    if policy == 'P':
        return int(candidates[0])
    if policy == 'R4':
        values = pressure[candidates]
        index = int(np.flatnonzero(values == values.min())[0])
    else:
        values = np.asarray(costs, np.float64)
        if values.shape != candidates.shape or not np.isfinite(values).all():
            raise ValueError('invalid candidate cost predictions')
        index = int(np.flatnonzero(values <= values.min() + TIE_TOLERANCE)[0])
    return int(candidates[index])


def pressure_candidates(raw, context, ordinal):
    pressure = pressure_from_features(raw)
    probability, _, _ = proportions(pressure)
    candidates, uniforms = sample_candidates(probability, context, ordinal)
    return pressure, probability, candidates, uniforms
