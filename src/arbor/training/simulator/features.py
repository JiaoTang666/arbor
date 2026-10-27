"""Causal, explicitly unit-labelled shard features; no placement side effects."""
from datetime import datetime
import numpy as np

SCHEMA = 'entry_placement_features_v1'
FEATURES = (
    'queue_slots', 'pending_slots', 'last_block_fill', 'interval_over_target',
    'queue_age_intervals', 'queue_missing', 'interval_missing', 'population_share',
    'graph_6h_log_hour_rate', 'graph_1d_log_hour_rate', 'graph_7d_log_hour_rate',
    'graph_day_weighted_persistence', 'forest_origin_log_hour_arrivals',
    'forest_origin_log_hour_followup_incidence', 'forest_log_mature_mass',
    'forest_no_mature_evidence', 'prior_source_age_hours', 'prior_lag_batches',
    'cumulative_work_slots', 'partner_here', 'known_sender', 'known_recipient',
    'self_transfer', 'novel_distinct_count', 'partner_log_hour_arrivals',
    'partner_log_mature_mass', 'partner_log_followup_incidence',
    'partner_profile_missing', 'partner_no_mature_evidence', 'first_extra_relay',
)
RESIDUAL_INDICES = tuple(range(8,18)) + tuple(range(19,len(FEATURES)))


def timestamp(value):
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()
    return float(value)


def vector(values, shards, name):
    out = np.asarray(values, dtype=float)
    if out.shape != (shards,) or not np.isfinite(out).all() or np.any(out < 0):
        raise ValueError('invalid nonnegative shard channel: ' + name)
    return out


def build_features(signals, prior, population, *, decision_sequence, source_time,
                   wall_time, block_capacity=1000, block_interval=5., batch_size=1000,
                   cumulative_work=None, context=None, partner_profile=None):
    """History timestamps and wall timestamps are checked in separate clocks.

    Queue and pending are overlapping views, not additive work. Graph and forest
    rates are descriptive source-time proxies, not queue or service predictions.
    Returned values describe evidence visible BEFORE the consuming occurrence.
    """
    shards = len(signals)
    if shards < 2 or block_capacity <= 0 or block_interval <= 0 or batch_size <= 0:
        raise ValueError('invalid topology/service configuration')
    cutoff, at = int(prior['accepted_cutoff_exclusive']), int(prior['source_time'])
    if cutoff > decision_sequence or at > source_time:
        raise ValueError('future graph/forest evidence')
    if prior['forest_cutoff'] != cutoff or prior['graph_cutoff'] != cutoff:
        raise ValueError('graph and forest generations differ')
    if prior['forest_source_time'] != at or prior['graph_source_time'] != at:
        raise ValueError('graph and forest source clocks differ')
    population = vector(population, shards, 'population')
    rates = np.asarray(prior['relation_stage_rate_by_scale'], dtype=float)
    if rates.shape != (shards, 3) or not np.isfinite(rates).all() or np.any(rates < 0):
        raise ValueError('invalid relation scale rates')
    persistence = vector(prior['relation_day_rate_weighted_persistence'], shards, 'persistence')
    if np.any(persistence > 1+1e-9):
        raise ValueError('persistence outside fraction range')
    arrivals = vector(prior['direct_arrival_rate'], shards, 'arrivals')
    followup = vector(prior['forest_followup_incidence_rate_proxy'], shards, 'followup')
    mass = vector(prior['forest_mature_child_mass_by_shard'], shards, 'mature mass')
    out = np.zeros((shards, len(FEATURES)), dtype=np.float64)
    for s, row in enumerate(signals):
        assigned, confirmed = row.get('Assigned', 0), row.get('Confirmed', 0)
        if assigned < confirmed or confirmed < 0:
            raise ValueError('invalid outstanding stage ledger')
        queue = row.get('Queue')
        intervals = row.get('Intervals') or []
        if intervals and (not np.isfinite(intervals).all() or min(intervals) <= 0):
            raise ValueError('invalid committed-block intervals')
        age = 0.
        received = row.get('ReceivedAt')
        if received is not None and timestamp(received) > wall_time:
            raise ValueError('block report was not yet received')
        if queue is not None:
            received = timestamp(row['ReceivedAt'])
            if queue < 0:
                raise ValueError('negative observed queue')
            # Supervisor receipt time shares the decision clock. The remote
            # capture timestamp is retained for audit, not assumed synchronized.
            age = (wall_time-received)/block_interval
        out[s, :8] = (0 if queue is None else queue/block_capacity,
            (assigned-confirmed)/block_capacity, row.get('BodyLength', 0)/block_capacity,
            float(np.median(intervals))/block_interval if intervals else 1., age,
            float(queue is None), float(not intervals),
            population[s]/population.sum() if population.sum() else 0.)
    out[:, 8:11] = np.log1p(rates*3600.)
    out[:, 11] = persistence
    out[:, 12] = np.log1p(arrivals*3600.)
    out[:, 13] = np.log1p(followup*3600.)
    out[:, 14] = np.log1p(mass)
    out[:, 15] = mass == 0
    out[:, 16] = (source_time-at)/3600.
    out[:, 17] = (decision_sequence-cutoff)/batch_size
    if context is None or cumulative_work is None:
        raise ValueError('entry context and cumulative work are required')
    out[:,18] = vector(cumulative_work,shards,'cumulative work')/block_capacity
    partner = context['partner_shard']
    out[:,19] = np.arange(shards)==partner
    out[:,20:24] = (context['known_sender'], context['known_recipient'],
                   context['self_transfer'], context['novel_distinct_count'])
    missing = partner_profile is None
    profile = (0.,0.,0.) if missing else partner_profile
    if len(profile)!=3 or not np.isfinite(profile).all() or min(profile)<0:
        raise ValueError('invalid partner profile')
    out[:,24:29] = (np.log1p(profile[0]*3600),np.log1p(profile[1]),
                    np.log1p(profile[2]),float(missing),float(missing or profile[1]==0))
    out[:,29] = (partner>=0)&(np.arange(shards)!=partner)
    if not np.isfinite(out).all():
        raise ValueError('nonfinite features')
    return out


class TrainingNormalizer:
    """Fitted only on policy-fitting episodes, frozen for selection and test."""
    def __init__(self, mean, scale):
        self.mean, self.scale = map(lambda a: np.asarray(a, dtype=float), (mean, scale))
        if self.mean.shape != (len(FEATURES),) or self.scale.shape != self.mean.shape or \
                not np.isfinite(self.mean).all() or not np.isfinite(self.scale).all() or np.any(self.scale <= 0):
            raise ValueError('invalid normalizer')

    @classmethod
    def fit(cls, episodes):
        total, count, squares = np.zeros(len(FEATURES)), 0, np.zeros(len(FEATURES))
        for block in episodes:
            a = np.asarray(block, dtype=float).reshape(-1, len(FEATURES))
            if not np.isfinite(a).all():
                raise ValueError('invalid fitting features')
            total += a.sum(axis=0); squares += np.square(a).sum(axis=0); count += len(a)
        if count == 0:
            raise ValueError('no fitting observations')
        mean = total/count
        std = np.sqrt(np.maximum(squares/count-mean**2, 0))
        scale = np.where(std < 1e-6, 1., std)
        # Keep missing-evidence indicators legible, including a category that
        # does not occur during fitting but appears later.
        for name in ('queue_missing','interval_missing','forest_no_mature_evidence',
                     'partner_here','known_sender','known_recipient','self_transfer',
                     'partner_profile_missing','partner_no_mature_evidence','first_extra_relay'):
            i=FEATURES.index(name);mean[i],scale[i]=0.,1.
        return cls(mean, scale)

    def transform(self, block):
        return (np.asarray(block, dtype=float)-self.mean)/self.scale

    def to_dict(self):
        return dict(schema=SCHEMA, features=list(FEATURES), mean=self.mean.tolist(),
                    scale=self.scale.tolist(), fitted_on='policy_fit_only')
