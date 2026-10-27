"""Finite numerical and causal-boundary checks for the linear cost ranker."""
import copy
import json
import unittest
import numpy as np
from arbor.admission.model import (COST_SECONDS, COST_TOLERANCE, UNNORMALIZED, choose, diagnostics,
                    fit, point_weights, scores, validate_bundle)


def row(day=13, family='N', sequence=0, candidates=(3, 1, 2, 0), shift=0.):
    raw = np.zeros((16, 20), np.float64)
    raw[:, 0] = np.arange(16.) + .1 * sequence
    raw[:, 19] = 1 / 16.
    pressure = np.arange(16.) * 100.
    return dict(day=day, family=family, sequence=sequence, raw_features=raw,
                pressure=pressure, candidates=list(candidates),
                costs=1.7 * raw[list(candidates), 0] + shift,
                r4_target=int(min(candidates, key=lambda shard: pressure[shard])))


class RankerTests(unittest.TestCase):
    def test_centered_linear_fit_and_physical_units(self):
        records = [row(day=13 + i, sequence=i, shift=20. * i) for i in range(4)]
        bundle = fit(records, candidate_contract={'count': 4}, source_hashes={'fixture': 'a' * 64})
        self.assertEqual(bundle['status'], 'FIT')
        self.assertEqual(len(bundle['coef']), 20)
        self.assertFalse(bundle['intercept'])
        self.assertEqual(bundle['ridge'], .01)
        self.assertLess(bundle['training']['normal_equation_residual'], 1e-12)
        chosen = choose(bundle, records[0]['raw_features'], records[0]['candidates'])
        self.assertEqual(chosen, 0)
        report = diagnostics(bundle, records)
        self.assertEqual(report['model_mean_regret_d'], 0.)
        self.assertEqual(report['non_tie_sign_accuracy_including_prediction_ties'], 1.)
        self.assertGreater(report['Pfirst_mean_regret_d'], 0.)
        self.assertAlmostEqual(report['centered_cost_mae_total_waiting_seconds'],
                               report['centered_cost_mae_d'] * COST_SECONDS)
        raw = records[0]['raw_features']
        mean, scale, coef, target_scale = validate_bundle(bundle)
        np.testing.assert_allclose(scores(bundle, raw), ((raw - mean) / scale) @ coef * target_scale)
        # Export remains ordinary JSON with no object arrays or hidden intercept.
        restored = json.loads(json.dumps(bundle, allow_nan=False))
        np.testing.assert_array_equal(scores(restored, raw), scores(bundle, raw))

    def test_normalization_uses_only_fit_points_and_all_16_shards(self):
        training = [row(candidates=(0, 1))]
        bundle = fit(training)
        normal = bundle['normalizer']
        self.assertEqual(normal['mean'][0], 7.5)
        for column in UNNORMALIZED:
            self.assertEqual(normal['mean'][column], 0.)
            self.assertEqual(normal['scale'][column], 1.)
        before = copy.deepcopy(bundle)
        future = row(day=20, sequence=1)
        future['raw_features'][:, 0] *= 100000.
        future['costs'] *= -10000.
        diagnostics(bundle, [future])
        self.assertEqual(bundle, before)

    def test_common_label_offset_cannot_change_fit(self):
        records = [row(day=13 + i // 2, sequence=i) for i in range(4)]
        changed = copy.deepcopy(records)
        for i, record in enumerate(changed):
            record['costs'] = record['costs'] + 1000. + 5. * i
        original, shifted = fit(records), fit(changed)
        np.testing.assert_allclose(original['coef'], shifted['coef'], atol=1e-12)
        self.assertAlmostEqual(original['target_scale'], shifted['target_scale'], places=12)
        for record in records:
            self.assertEqual(choose(original, record['raw_features'], record['candidates']),
                             choose(shifted, record['raw_features'], record['candidates']))

    def test_hierarchical_equal_day_scene_point_weights(self):
        records = [row(13, 'N', 0), row(13, 'N', 1), row(13, 'O', 0), row(14, 'N', 0)]
        np.testing.assert_array_equal(point_weights(records), np.array([.125, .125, .25, .5]))

    def test_no_signal_and_one_candidate_are_explicit(self):
        records = [row(), row(day=14, candidates=(4,))]
        records[0]['costs'] = np.array([10., 10. + COST_TOLERANCE / 2, 10., 10.])
        records[1]['costs'] = np.array([-50.])
        bundle = fit(records)
        self.assertEqual(bundle['status'], 'NO_SIGNAL')
        np.testing.assert_array_equal(bundle['coef'], np.zeros(20))
        self.assertEqual(choose(bundle, records[0]['raw_features'], records[0]['candidates']), 3)
        report = diagnostics(bundle, records)
        self.assertIsNone(report['non_tie_sign_accuracy_including_prediction_ties'])
        self.assertEqual(report['counts']['actual_non_tie_pairs'], 0)

    def test_tolerance_is_applied_after_inverse_target_scale(self):
        bundle = fit([row()])
        bundle['coef'] = [0.] * 20
        bundle['coef'][0] = 1.
        bundle['normalizer']['mean'] = [0.] * 20
        bundle['normalizer']['scale'] = [1.] * 20
        bundle['target_scale'] = 100.
        raw = np.zeros((16, 20))
        raw[3, 0] = COST_TOLERANCE * .5 / 100.
        self.assertEqual(choose(bundle, raw, [3, 0]), 3)
        raw[3, 0] = COST_TOLERANCE * 2. / 100.
        self.assertEqual(choose(bundle, raw, [3, 0]), 0)

    def test_deduplicated_candidate_order_controls_ties(self):
        bundle = fit([row()])
        raw = np.zeros((16, 20))
        self.assertEqual(choose(bundle, raw, [7, 2, 9]), 7)
        self.assertEqual(choose(bundle, raw, [2, 7, 9]), 2)
        with self.assertRaises(ValueError):
            choose(bundle, raw, [2, 2])

    def test_invalid_records_or_contract_fail_loudly(self):
        original = row()
        with self.assertRaises(ValueError):
            fit([original, original])
        for mutate in (lambda r: r.update(costs=[1., 2.]),
                       lambda r: r.update(candidates=[0, 0, 1, 2]),
                       lambda r: r.update(r4_target=3),
                       lambda r: r['raw_features'].__setitem__((0, 0), np.nan),
                       lambda r: r['pressure'].__setitem__(0, -1.)):
            invalid = copy.deepcopy(original)
            mutate(invalid)
            with self.assertRaises(ValueError):
                fit([invalid])
        bundle = fit([original])
        bundle['normalizer']['scale'][19] = .1
        with self.assertRaises(ValueError):
            scores(bundle, original['raw_features'])


if __name__ == '__main__':
    unittest.main()
