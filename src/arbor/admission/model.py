"""Weighted within-slate ridge ranking of measured complete-waiting costs.

The model predicts cost differences within the current candidate slate. It does
not identify a slate's common cost offset or predict an account's future heat.
"""
from collections import Counter, defaultdict
import hashlib
import json
import numpy as np
from .candidates import COST_FEATURES

SCHEMA = 'entry_cost_ranker_v1'
FEATURES = tuple(COST_FEATURES)
UNNORMALIZED = (3, 5, 9, 13, 18, 19)
RIDGE = .01
COST_TOLERANCE = 1e-6
COST_SECONDS = 5000.


def _plain(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _digest(value):
    text = json.dumps(_plain(value), sort_keys=True, ensure_ascii=True,
                      separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def _validated(records):
    rows = list(records)
    if not rows:
        raise ValueError('ranker requires nonempty cost records')
    result, seen = [], set()
    for row in rows:
        day, family, sequence = row['day'], row['family'], row['sequence']
        if (isinstance(day, bool) or not isinstance(day, (int, np.integer)) or
                not isinstance(family, str) or not family or isinstance(sequence, bool) or
                not isinstance(sequence, (int, np.integer)) or sequence < 0):
            raise ValueError('invalid day, scene or decision sequence')
        identity = (int(day), family, int(sequence))
        if identity in seen:
            raise ValueError('duplicate cost point within one day and scene')
        seen.add(identity)
        raw, cost, pressure = (np.asarray(row[key], np.float64)
                               for key in ('raw_features', 'costs', 'pressure'))
        original = list(row['candidates'])
        if not 1 <= len(original) <= 4 or any(isinstance(x, bool) or
                not isinstance(x, (int, np.integer)) or not 0 <= x < 16 for x in original):
            raise ValueError('candidate slate must contain one to four actual shard IDs')
        candidates = np.asarray(original, np.int64)
        if len(set(original)) != len(original):
            raise ValueError('candidates must already be de-duplicated in rule order')
        if raw.shape != (16, 20) or pressure.shape != (16,) or cost.shape != (len(candidates),):
            raise ValueError('cost point feature, pressure or label shape mismatch')
        if not all(np.isfinite(x).all() for x in (raw, cost, pressure)) or np.any(pressure < 0):
            raise ValueError('cost point contains nonfinite data or negative pressure')
        r4 = row['r4_target']
        if isinstance(r4, bool) or not isinstance(r4, (int, np.integer)) or int(r4) not in candidates:
            raise ValueError('R4 target missing from the unique candidate slate')
        expected_r4 = int(candidates[int(np.argmin(pressure[candidates]))])
        if int(r4) != expected_r4:
            raise ValueError('R4 target differs from minimum pressure in candidate order')
        result.append(dict(day=int(day), family=family, sequence=int(sequence),
                           raw_features=raw, costs=cost, candidates=candidates,
                           pressure=pressure, r4_target=int(r4)))
    return result


def point_weights(records):
    """Equal days, equal scenes within day, equal cost points within scene."""
    days = {row['day'] for row in records}
    scenes = defaultdict(set)
    counts = Counter((row['day'], row['family']) for row in records)
    for row in records:
        scenes[row['day']].add(row['family'])
    weights = np.array([1. / (len(days) * len(scenes[row['day']]) *
                             counts[row['day'], row['family']]) for row in records])
    if not np.isclose(weights.sum(), 1., atol=1e-12, rtol=0):
        raise AssertionError('hierarchical point weights do not sum to one')
    return weights


def fit_normalizer(records, weights):
    # Use all 16 visible shards, even when candidate de-duplication leaves K<4.
    mean, squares = np.zeros(20), np.zeros(20)
    for row, weight in zip(records, weights):
        x = row['raw_features']
        mean += weight * x.mean(0)
        squares += weight * np.square(x).mean(0)
    variance = np.maximum(squares - np.square(mean), 0.)
    scale = np.sqrt(variance)
    scale[scale < 1e-6] = 1.
    mean[list(UNNORMALIZED)], scale[list(UNNORMALIZED)] = 0., 1.
    return dict(features=list(FEATURES), mean=mean.tolist(), scale=scale.tolist(),
                unnormalized_columns=list(UNNORMALIZED),
                fitted_on='training points only; all 16 shards per point',
                weighting='equal day, scene within day, point within scene, shard within point')


def validate_bundle(bundle):
    if bundle.get('schema') != SCHEMA or bundle.get('feature_order') != list(FEATURES):
        raise ValueError('wrong cost ranker semantic version or feature order')
    normal = bundle['normalizer']
    mean, scale, coefficient = (np.asarray(x, np.float64) for x in
                                (normal['mean'], normal['scale'], bundle['coef']))
    if (normal.get('features') != list(FEATURES) or
            normal.get('unnormalized_columns') != list(UNNORMALIZED) or
            mean.shape != (20,) or scale.shape != (20,) or coefficient.shape != (20,) or
            not all(np.isfinite(x).all() for x in (mean, scale, coefficient)) or np.any(scale <= 0)):
        raise ValueError('invalid frozen ranker parameters or normalization')
    if np.any(mean[list(UNNORMALIZED)] != 0) or np.any(scale[list(UNNORMALIZED)] != 1):
        raise ValueError('missing flags and previous pressure proportions were normalized')
    target_scale = float(bundle['target_scale'])
    if not np.isfinite(target_scale) or target_scale <= 0 or bundle['ridge'] != RIDGE:
        raise ValueError('invalid target scale or ridge strength')
    if bundle.get('status') not in ('FIT', 'NO_SIGNAL'):
        raise ValueError('ranker has no valid fit status')
    if bundle.get('intercept') is not False or bundle.get('model_parameters') != 20 or bundle.get('tie_tolerance') != COST_TOLERANCE:
        raise ValueError('ranker architecture or actual-cost tie contract changed')
    if bundle['status'] == 'NO_SIGNAL' and np.any(coefficient != 0):
        raise ValueError('a no-signal result cannot carry a manufactured ranking')
    return mean, scale, coefficient, target_scale


def scores(bundle, raw_features, candidates=None):
    """Return q in physical d=(total original waiting seconds)/5000 units."""
    mean, scale, coefficient, target_scale = validate_bundle(bundle)
    raw = np.asarray(raw_features, np.float64)
    if raw.ndim != 2 or raw.shape[0] < 1 or raw.shape[1] != 20 or not np.isfinite(raw).all():
        raise ValueError('invalid ranker decision features')
    value = ((raw - mean) / scale) @ coefficient * target_scale
    if not np.isfinite(value).all():
        raise ValueError('nonfinite cost ranker score')
    if candidates is None:
        return value
    slate = np.asarray(candidates)
    if slate.ndim != 1 or not 1 <= len(slate) <= 4 or not np.issubdtype(slate.dtype, np.integer) or np.any((slate < 0) | (slate >= len(raw))) or len(np.unique(slate)) != len(slate):
        raise ValueError('invalid inference candidate slate')
    return value[slate]


def choose(bundle, raw_features, candidates):
    candidates = list(candidates)
    value = scores(bundle, raw_features, candidates)
    # Find the first candidate within tolerance of the true minimum; pairwise
    # chained tolerance comparisons would make order-dependent non-transitive ties.
    index = int(np.flatnonzero(value <= float(value.min()) + COST_TOLERANCE)[0])
    return int(candidates[index])


def fit(records, candidate_contract=None, source_hashes=None):
    rows = _validated(records)
    weights = point_weights(rows)
    normal = fit_normalizer(rows, weights)
    mean, scale = np.asarray(normal['mean']), np.asarray(normal['scale'])
    designs, targets, rms_squared = [], [], 0.
    for row, weight in zip(rows, weights):
        x = (row['raw_features'][row['candidates']] - mean) / scale
        x = x - x.mean(0)
        y = row['costs'] - row['costs'].mean()
        designs.append(x)
        targets.append(y)
        rms_squared += weight * float(np.square(y).mean())
    maximum_spread = max(float(np.ptp(row['costs'])) for row in rows)
    signal = maximum_spread > COST_TOLERANCE
    target_scale = float(np.sqrt(rms_squared)) if signal else 1.
    if signal and (not np.isfinite(target_scale) or target_scale <= 0):
        raise ValueError('usable cost signal has an invalid RMS target scale')
    lhs, rhs = RIDGE * np.eye(20), np.zeros(20)
    if signal:
        for row, weight, x, y in zip(rows, weights, designs, targets):
            mass = weight / len(row['candidates'])
            lhs += mass * x.T @ x
            rhs += mass * x.T @ (y / target_scale)
        coefficient = np.linalg.solve(lhs, rhs)
        if not np.isfinite(coefficient).all():
            raise ValueError('nonfinite closed-form ranker coefficients')
    else:
        coefficient = np.zeros(20)
    objective = sum(weight * float(np.square(x @ coefficient - y / target_scale).mean())
                    for weight, x, y in zip(weights, designs, targets))
    source_hashes = {} if source_hashes is None else _plain(source_hashes)
    candidate_contract = {} if candidate_contract is None else _plain(candidate_contract)
    counts = Counter((row['day'], row['family']) for row in rows)
    bundle = dict(schema=SCHEMA, status='FIT' if signal else 'NO_SIGNAL',
        feature_order=list(FEATURES), normalizer=normal, coef=coefficient.tolist(),
        target_scale=target_scale, ridge=RIDGE, intercept=False, model_parameters=20,
        score_units='d=total original complete-waiting seconds/5000',
        score_formula='target_scale * dot(coef, (raw-mean)/scale)',
        tie_tolerance=COST_TOLERANCE, tie_order='first unique candidate in rule order',
        target_centering='within each unique candidate slate',
        target_scaling='one hierarchical-weighted RMS across training centered labels',
        objective='weighted mean squared centered normalized cost error + 0.01 * squared coefficient norm',
        candidate_contract=candidate_contract, candidate_contract_sha256=_digest(candidate_contract),
        source_hashes=source_hashes, training_records_sha256=_digest(rows),
        training=dict(points=len(rows), days=sorted({row['day'] for row in rows}),
            slate_candidates=sum(len(row['candidates']) for row in rows),
            points_by_day_scene=[dict(day=day, family=family, points=count)
                                 for (day, family), count in sorted(counts.items())],
            informative_points=sum(float(np.ptp(row['costs'])) > COST_TOLERANCE for row in rows),
            maximum_label_spread_d=maximum_spread, raw_centered_target_rms_d=float(np.sqrt(rms_squared)),
            normalized_mse=objective, penalty=RIDGE * float(coefficient @ coefficient),
            regularized_objective=objective + RIDGE * float(coefficient @ coefficient),
            normal_equation_residual=float(np.abs(lhs @ coefficient - rhs).max())))
    validate_bundle(bundle)
    return bundle


def diagnostics(bundle, records):
    rows = _validated(records)
    weights = point_weights(rows)
    validate_bundle(bundle)
    totals = defaultdict(float)
    counts = Counter()
    for key in ('pair_points', 'candidate_pairs', 'actual_non_tie_pairs',
                'predicted_non_tie_on_actual_non_tie_pairs', 'correct_non_tie_pairs',
                'predicted_difference_on_actual_tie_pairs', 'selected_Pfirst_points',
                'selected_R4_points', 'oracle_within_tolerance_points'):
        counts[key] = 0
    point_rows = []
    for row, weight in zip(rows, weights):
        candidates, cost = row['candidates'], row['costs']
        prediction = scores(bundle, row['raw_features'], candidates)
        centered_prediction, centered_cost = prediction - prediction.mean(), cost - cost.mean()
        totals['centered_cost_mae_d'] += weight * float(np.abs(centered_prediction - centered_cost).mean())
        chosen = choose(bundle, row['raw_features'], candidates)
        index = int(np.flatnonzero(candidates == chosen)[0])
        r4_index = int(np.flatnonzero(candidates == row['r4_target'])[0])
        minimum = float(cost.min())
        regrets = dict(model=float(cost[index] - minimum), Pfirst=float(cost[0] - minimum),
                       R4=float(cost[r4_index] - minimum))
        for name, value in regrets.items():
            totals[name + '_mean_regret_d'] += weight * value
        for name, base in (('Pfirst', float(cost[0])), ('R4', float(cost[r4_index]))):
            difference = float(cost[index] - base)
            totals['model_minus_' + name + '_mean_cost_d'] += weight * difference
            outcome = 'better' if difference < -COST_TOLERANCE else 'worse' if difference > COST_TOLERANCE else 'tie'
            counts[name + '_' + outcome + '_points'] += 1
            totals[name + '_' + outcome + '_point_fraction'] += weight
        pairs = [(a, b) for a in range(len(cost)) for b in range(a + 1, len(cost))]
        if pairs:
            counts['pair_points'] += 1
            totals['pair_point_mass'] += weight
            pair_weight = weight / len(pairs)
            for a, b in pairs:
                true_delta, predicted_delta = float(cost[a] - cost[b]), float(prediction[a] - prediction[b])
                totals['pairwise_cost_mae_d_numerator'] += pair_weight * abs(predicted_delta - true_delta)
                counts['candidate_pairs'] += 1
                if abs(true_delta) > COST_TOLERANCE:
                    counts['actual_non_tie_pairs'] += 1
                    totals['actual_non_tie_pair_mass'] += pair_weight
                    if abs(predicted_delta) > COST_TOLERANCE:
                        totals['predicted_non_tie_on_actual_non_tie_mass'] += pair_weight
                        counts['predicted_non_tie_on_actual_non_tie_pairs'] += 1
                        if np.sign(predicted_delta) == np.sign(true_delta):
                            totals['correct_non_tie_pair_mass'] += pair_weight
                            counts['correct_non_tie_pairs'] += 1
                elif abs(predicted_delta) > COST_TOLERANCE:
                    counts['predicted_difference_on_actual_tie_pairs'] += 1
        counts['selected_Pfirst_points'] += int(index == 0)
        counts['selected_R4_points'] += int(index == r4_index)
        counts['oracle_within_tolerance_points'] += int(cost[index] <= minimum + COST_TOLERANCE)
        point_rows.append(dict(day=row['day'], family=row['family'], sequence=row['sequence'],
            candidates=candidates.tolist(), costs_d=cost.tolist(), predicted_scores_d=prediction.tolist(),
            selected=chosen, Pfirst=int(candidates[0]), R4=row['r4_target'], regrets_d=regrets,
            prediction_within_slate_only=True))
    mass = totals['actual_non_tie_pair_mass']
    predicted_mass = totals['predicted_non_tie_on_actual_non_tie_mass']
    result = dict(status='DIAGNOSTIC_NOT_POLICY_PERFORMANCE_PROOF', model_status=bundle['status'],
        points=len(rows), days=sorted({row['day'] for row in rows}),
        centered_cost_mae_d=totals['centered_cost_mae_d'],
        centered_cost_mae_total_waiting_seconds=totals['centered_cost_mae_d'] * COST_SECONDS,
        pairwise_cost_mae_d=None if totals['pair_point_mass'] == 0 else
            totals['pairwise_cost_mae_d_numerator'] / totals['pair_point_mass'],
        non_tie_sign_accuracy_including_prediction_ties=None if mass == 0 else totals['correct_non_tie_pair_mass'] / mass,
        non_tie_sign_coverage=None if mass == 0 else predicted_mass / mass,
        non_tie_sign_accuracy_when_predicting_non_tie=None if predicted_mass == 0 else totals['correct_non_tie_pair_mass'] / predicted_mass,
        counts=dict(counts), cost_tolerance=COST_TOLERANCE,
        model_mean_regret_d=totals['model_mean_regret_d'],
        Pfirst_mean_regret_d=totals['Pfirst_mean_regret_d'],
        R4_mean_regret_d=totals['R4_mean_regret_d'],
        model_minus_Pfirst_mean_cost_d=totals['model_minus_Pfirst_mean_cost_d'],
        model_minus_R4_mean_cost_d=totals['model_minus_R4_mean_cost_d'],
        comparisons={name: {outcome: totals[name + '_' + outcome + '_point_fraction']
                           for outcome in ('better', 'worse', 'tie')} for name in ('Pfirst', 'R4')},
        point_results=point_rows,
        limitations=['Ranks only the measured candidate slate, not all shard actions.',
                    'Cost labels are simulation counterfactuals from one point with a fixed continuation rule.',
                    'Point regret is not full-trajectory deployment regret; repeated decisions change later states.',
                    'Overlapping historical labels and reused scenes are not independent observations.'])
    for key in ('model_mean_regret_d', 'Pfirst_mean_regret_d', 'R4_mean_regret_d',
                'model_minus_Pfirst_mean_cost_d', 'model_minus_R4_mean_cost_d'):
        result[key.removesuffix('_d') + '_total_waiting_seconds'] = result[key] * COST_SECONDS
    result['pairwise_cost_mae_total_waiting_seconds'] = None if result['pairwise_cost_mae_d'] is None else result['pairwise_cost_mae_d'] * COST_SECONDS
    return result
