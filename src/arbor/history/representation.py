"""Temporal graph aggregation and finite-window activity profiles."""
import math
import numpy as np

def aggregate_graph(u, v, timestamps, start, as_of, bin_seconds, half_lives):
    """One ordered endpoint pair per directed edge, including self edges."""
    keys = (u.astype(np.uint64) << np.uint64(32)) | v.astype(np.uint64)
    unique, inv = np.unique(keys, return_inverse=True)
    E = len(unique)
    first = np.full(E, np.iinfo(np.int64).max, np.int64)
    last = np.zeros(E, np.int64)
    np.minimum.at(first, inv, timestamps); np.maximum.at(last, inv, timestamps)
    mass = np.empty((E, len(half_lives)), np.float64)
    age = as_of - timestamps
    for k, h in enumerate(half_lives):
        mass[:, k] = np.bincount(inv, weights=np.exp2(-age / h), minlength=E)
    bins = (timestamps - start) // bin_seconds
    B = int(bins.max()) + 1
    window_key, window_count = np.unique(inv.astype(np.int64) * B + bins, return_counts=True)
    graph = dict(sender=(unique >> np.uint64(32)).astype(np.int32),
                 recipient=(unique & np.uint64(0xffffffff)).astype(np.int32),
                 count=np.bincount(inv, minlength=E).astype(np.int64),
                 first_timestamp=first, last_timestamp=last,
                 decayed_mass=mass)
    windows = dict(edge=(window_key // B).astype(np.int32),
                   window=(window_key % B).astype(np.int32), count=window_count.astype(np.int64))
    return graph, windows

def temporal_profiles(cols, forest, n, start, as_of, bin_seconds, half_lives, horizons):
    u, v, ts = cols['sender'], cols['recipient'], cols['timestamp']
    # Account incidence counts each occurrence once for a self transfer.
    nonself = u != v
    ids = np.concatenate((u, v[nonself]))
    seq = np.concatenate((np.arange(len(u)), np.flatnonzero(nonself)))
    stamp = ts[seq]
    B = int((as_of - start) // bin_seconds) + 1
    keys, values = np.unique(ids.astype(np.int64) * B + (stamp - start) // bin_seconds,
                             return_counts=True)
    active = np.bincount(keys // B, minlength=n).astype(np.int64)
    count = np.bincount(ids, minlength=n).astype(np.int64)
    last = np.zeros(n, np.int64)
    np.maximum.at(last, ids, stamp)
    rates = np.empty((n, len(half_lives)), np.float64)
    birth_rates = np.zeros_like(rates)
    exposure = as_of - forest['first_timestamp']
    parents = forest['parent']; children = np.flatnonzero(parents >= 0)
    birth_count = np.bincount(parents[children], minlength=n).astype(np.int64)
    age = as_of - stamp
    for k, h in enumerate(half_lives):
        mass = np.bincount(ids, weights=np.exp2(-age / h), minlength=n)
        # Raw exponential-kernel rate; short trace exposure is explicitly recorded.
        rates[:, k] = mass * math.log(2) / h
        birth_rates[:, k] = np.bincount(parents[children],
            weights=np.exp2(-(as_of - forest['first_timestamp'][children]) / h), minlength=n) * math.log(2) / h
    eligible_windows = B - ((forest['first_timestamp'] - start) // bin_seconds)
    # Predictable maturity mask; unfinished cohorts are never labelled as zero opportunity.
    followup = np.zeros((n, len(horizons)), np.int64)
    is_later = seq > forest['first_sequence'][ids]
    age_at_event = stamp - forest['first_timestamp'][ids]
    for k, h in enumerate(horizons):
        eligible = is_later & (age_at_event <= h)
        followup[:, k] = np.bincount(ids[eligible], minlength=n)
    complete = exposure[:, None] >= np.asarray(horizons)[None, :]
    profiles = dict(occurrences=count, last_timestamp=last, age_since_last_seconds=as_of-last,
        observed_span_seconds=last-forest['first_timestamp'], exposure_seconds=exposure,
        active_windows=active, eligible_windows=eligible_windows,
        active_window_fraction=active / eligible_windows,
        kernel_rate_per_source_second=rates, direct_new_account_count=birth_count,
        direct_new_account_kernel_rate=birth_rates,
        followup_incidence_observed=followup, followup_window_complete=complete)
    windows = dict(account=(keys//B).astype(np.int32), window=(keys%B).astype(np.int32),
                   count=values.astype(np.int64))
    return profiles, windows
