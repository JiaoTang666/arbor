import numpy as np
from arbor.forest.recent import RecentActivity, credits

def mono_ns(seconds):
    if not np.isfinite(seconds) or seconds < 0:
        raise ValueError('invalid simulation runtime')
    return int(round(float(seconds)*1_000_000_000))

class FastFacts:
    def __init__(self, shards, initial_accounts, total_accounts, base, clock_id):
        self.shards, self.base, self.clock_id = int(shards), int(base), clock_id
        self.roots = np.full(total_accounts, -1, np.int64)
        self.roots[:initial_accounts] = np.arange(initial_accounts, dtype=np.int64)
        self.parents = {}
        self.recent = RecentActivity(shards, clock_id)
        self.assigned = np.zeros(shards, np.int64)
        self.confirmed = np.zeros(shards, np.int64)
        self.category_assigned = np.zeros((3, shards), np.int64)
        self.category_confirmed = np.zeros((3, shards), np.int64)
        self.family_assigned, self.family_confirmed = {}, {}
        self.pending = {}
        self.cutoff = self.completed = self.birth_count = 0

    def assign(self, sequence, a, b, known_a, known_b, source, target, at):
        if sequence != self.cutoff or known_a != (self.roots[a] != -1) or known_b != (self.roots[b] != -1):
            raise ValueError('source binding prefix/visibility mismatch')
        if a == b and (known_a != known_b or source != target):
            raise ValueError('self source binding mismatch')
        timestamp = mono_ns(at)
        self.recent.advance(timestamp, self.clock_id)
        if not known_a and not known_b:
            origin = -(self.base+sequence+2)
            self.roots[a] = self.roots[b] = origin
            self.recent.birth(timestamp, source, self.clock_id)
            if source != target:
                self.recent.birth(timestamp, target, self.clock_id)
            self.birth_count += 1
        elif not known_a:
            self.roots[a] = self.roots[b]
            self.parents[a] = b
        elif not known_b:
            self.roots[b] = self.roots[a]
            self.parents[b] = a
        split = credits(self.roots[a], self.roots[b])
        legs = (source,) if source == target else (source, target)
        self.pending[sequence] = dict(split=split, legs=legs, done=set(), at=timestamp)
        for shard in legs:
            self.assigned[shard] += 1
            for family, amount in split:
                category = 1 if family < -1 else 2 if family == -1 else 0
                self.category_assigned[category, shard] += amount
                key = (family, shard)
                self.family_assigned[key] = self.family_assigned.get(key, 0)+amount
            self.recent.stage(timestamp, shard, split, self.clock_id)
        self.cutoff += 1

    def confirm(self, sequence, leg, shard, at):
        timestamp = mono_ns(at)
        row = self.pending.get(sequence)
        if row is None or not 0 <= leg < len(row['legs']) or row['legs'][leg] != shard or timestamp < row['at']:
            raise ValueError('source receipt identity/leg/clock mismatch')
        if leg in row['done']:
            return False
        row['done'].add(leg)
        self.confirmed[shard] += 1
        for family, amount in row['split']:
            category = 1 if family < -1 else 2 if family == -1 else 0
            self.category_confirmed[category, shard] += amount
            key = (family, shard)
            self.family_confirmed[key] = self.family_confirmed.get(key, 0)+amount
        if len(row['done']) == len(row['legs']):
            del self.pending[sequence]
            self.completed += 1
        return True

    def snapshot(self, at):
        row = self.recent.snapshot(mono_ns(at), self.clock_id)
        row.update(assigned_prefix=self.cutoff, confirmed_stages=self.confirmed.tolist(),
                   assigned_stages=self.assigned.tolist(), completed_occurrences=self.completed)
        return row

    def audit(self, expected=None):
        for totals, categories, families in (
                (self.assigned, self.category_assigned, self.family_assigned),
                (self.confirmed, self.category_confirmed, self.family_confirmed)):
            if not np.array_equal(categories.sum(0), 2*totals):
                raise AssertionError('source category half-stage conservation')
            sums = np.zeros(self.shards, np.int64)
            for (_, shard), units in families.items():
                sums[shard] += units
            if not np.array_equal(sums, 2*totals):
                raise AssertionError('source family half-stage conservation')
        if np.any(self.confirmed > self.assigned):
            raise AssertionError('source confirmed work exceeds assigned')
        if expected is not None and (self.cutoff != expected or self.completed != expected or self.pending):
            raise AssertionError('source ledger incomplete occurrence coverage')
        return dict(status='PASS', assigned_prefix=self.cutoff, completed=self.completed,
                    outstanding_occurrences=len(self.pending), assigned=self.assigned.tolist(),
                    confirmed=self.confirmed.tolist(), category_assigned_half=self.category_assigned.tolist(),
                    category_confirmed_half=self.category_confirmed.tolist(),
                    newnew_origins=self.birth_count, planning_cutoff_entrances=True)
