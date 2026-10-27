from collections import deque
import numpy as np
SECOND = 1_000_000_000
UNKNOWN = -1
SCHEMA = 'family_work_facts_v2'

def credits(a, b):
    a, b = int(a), int(b)
    return ((a, 2),) if a == b else ((a, 1), (b, 1))

def stages(source, target):
    return (('inner',source),) if source == target else (('relay1',source),('relay2',target))

class RecentActivity:
    """Entries expire once; query is O(S) plus amortized expiration work."""
    def __init__(self, shards, clock_id):
        if not clock_id:
            raise ValueError('missing clock instance')
        self.shards, self.clock_id, self.now = shards, clock_id, -1
        self.births, self.short, self.long = deque(), deque(), deque()
        self.new = [0]*shards
        self.work5 = [0]*shards
        self.mass20, self.squares20 = [0]*shards, [0]*shards
        self.by_family = [dict() for _ in range(shards)]

    def advance(self, at, clock_id):
        if type(at) is not int or at < 0 or at < self.now or clock_id != self.clock_id:
            raise ValueError('missing/regressed/cross-instance runtime clock')
        self.now = at
        while self.births and self.births[0][0] <= at-20*SECOND:
            _, s = self.births.popleft(); self.new[s] -= 1
        while self.short and self.short[0][0] <= at-5*SECOND:
            _, s, amount = self.short.popleft(); self.work5[s] -= amount
        while self.long and self.long[0][0] <= at-20*SECOND:
            _, s, f, amount = self.long.popleft()
            self._change(s, f, -amount)

    def _change(self, s, f, delta):
        old = self.by_family[s].get(f, 0)
        new = old+delta
        if new < 0:
            raise AssertionError('negative window credit')
        self.mass20[s] += delta
        self.squares20[s] += new*new-old*old
        if new:
            self.by_family[s][f] = new
        else:
            self.by_family[s].pop(f, None)

    def birth(self, at, shard, clock_id):
        self.advance(at, clock_id)
        self.births.append((at, shard)); self.new[shard] += 1

    def stage(self, at, shard, split, clock_id):
        self.advance(at, clock_id)
        for family, amount in split:
            if family >= UNKNOWN:  # Historical entrance or unknown, not runtime NN.
                continue
            self.short.append((at, shard, amount)); self.work5[shard] += amount
            self.long.append((at, shard, family, amount)); self._change(shard,family,amount)

    def snapshot(self, at, clock_id):
        self.advance(at, clock_id)
        return dict(schema=SCHEMA, clock_id=clock_id, runtime_mono_ns=at,
                    new_origins_20s=list(self.new), active_origins_20s=[len(x) for x in self.by_family],
                    assigned_nn_half_stages_5s=list(self.work5),
                    concentration_20s=[a/(b*b) if b else 0. for a,b in zip(self.squares20,self.mass20)],
                    no_activity_20s=[b == 0 for b in self.mass20],
                    retained_events=len(self.births)+len(self.short)+len(self.long),
                    time_boundary='(now-window, now], only already consumed sequence events')
