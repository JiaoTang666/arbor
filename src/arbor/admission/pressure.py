"""Versioned, causal 20-feature and counter-random placement contract."""
import hashlib
import json
import math
import numpy as np

SCHEMA = 'entry_share_residual_v2'
PRESSURE_SCALE = 1000.
RESIDUAL_BOUND = math.log(2.)
FEATURES = (
    'visible_pending_slots', 'queue_slots', 'queue_receive_age_intervals',
    'queue_missing', 'visible_pending_delta_5s', 'pending_history_missing',
    'last_nonempty_block_fill', 'nonempty_service_mean_intervals',
    'nonempty_commit_receive_age_intervals', 'service_missing',
    'graph_day_log_hour_stage_rate', 'graph_source_age_hours',
    'graph_receive_age_intervals', 'graph_missing',
    'new_origins_20s_log_count', 'active_origins_20s_log_count',
    'assigned_nn_half_stages_5s_slots', 'family_concentration_20s',
    'family_concentration_missing', 'previous_share',
)
UNNORMALIZED = (3, 5, 9, 13, 18, 19)


def random_uniform(context, ordinal, seed=43):
    if not isinstance(context, str) or not context.isascii() or not context:
        raise ValueError('placement context must be nonempty ASCII')
    if isinstance(ordinal, bool) or int(ordinal) != ordinal or ordinal < 0:
        raise ValueError('invalid newnew ordinal')
    encoded = json.dumps([int(seed), context, int(ordinal)], ensure_ascii=True,
                         separators=(',', ':')).encode('utf-8')
    number = int.from_bytes(hashlib.sha256(encoded).digest()[:8], 'big') >> 11
    return number / float(1 << 53)


def softmax(logits):
    x = np.asarray(logits, np.float64)
    if x.ndim != 1 or not len(x) or not np.isfinite(x).all():
        raise ValueError('invalid logit vector')
    weights = np.exp(x-x.max())
    return weights/weights.sum()


def proportions(pending_pressure, latent=None):
    pressure = np.asarray(pending_pressure, np.float64)
    if pressure.ndim != 1 or not np.isfinite(pressure).all() or np.any(pressure < 0):
        raise ValueError('invalid visible pressure')
    x = np.zeros_like(pressure) if latent is None else np.asarray(latent, np.float64)
    if x.shape != pressure.shape or not np.isfinite(x).all():
        raise ValueError('invalid residual latent')
    residual = RESIDUAL_BOUND*np.tanh(x)
    logits = -pressure/PRESSURE_SCALE+residual
    return softmax(logits), residual, logits


def sample_target(probability, uniform):
    p = np.asarray(probability, np.float64)
    if p.ndim != 1 or not len(p) or not np.isfinite(p).all() or np.any(p < 0) or \
            not np.isclose(p.sum(), 1., rtol=0, atol=1e-12) or not 0 <= uniform < 1:
        raise ValueError('invalid probability draw')
    return min(int(np.searchsorted(np.cumsum(p), uniform, side='right')), len(p)-1)


class Normalizer:
    def __init__(self, mean, scale):
        self.mean, self.scale = (np.asarray(x, np.float64) for x in (mean, scale))
        if self.mean.shape != (20,) or self.scale.shape != (20,) or \
                not np.isfinite(self.mean).all() or not np.isfinite(self.scale).all() or np.any(self.scale <= 0):
            raise ValueError('invalid 20-feature normalizer')
        if np.any(self.mean[list(UNNORMALIZED)] != 0) or np.any(self.scale[list(UNNORMALIZED)] != 1):
            raise ValueError('flags/previous share must remain unnormalized')

    @classmethod
    def fit(cls, blocks):
        count, total, squares = 0, np.zeros(20), np.zeros(20)
        for block in blocks:
            x = np.asarray(block, np.float64).reshape(-1, 20)
            if not np.isfinite(x).all():
                raise ValueError('nonfinite training features')
            count += len(x)
            total += x.sum(0)
            squares += (x*x).sum(0)
        if not count:
            raise ValueError('empty normalization source')
        mean = total/count
        scale = np.sqrt(np.maximum(squares/count-mean*mean, 0))
        scale[scale < 1e-6] = 1.
        mean[list(UNNORMALIZED)], scale[list(UNNORMALIZED)] = 0., 1.
        return cls(mean, scale)

    def transform(self, raw):
        x = np.asarray(raw, np.float64)
        if x.shape[-1] != 20 or not np.isfinite(x).all():
            raise ValueError('invalid 20-feature input')
        return ((x-self.mean)/self.scale).astype(np.float32)

    def to_dict(self):
        return dict(mean=self.mean.tolist(), scale=self.scale.tolist(), features=list(FEATURES),
                    unnormalized_columns=list(UNNORMALIZED), fit='training fixed-A traces only',
                    schema=SCHEMA, decisions='every newnew first binding')
