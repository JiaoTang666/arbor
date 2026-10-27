"""Causal source facts and complete, finite-window activity labels. No predictor."""
from collections import deque
from pathlib import Path
import hashlib
import json
import math
import numpy as np

SCHEMA = 'entry_forest_facts_v2'
UNKNOWN = -1
PENDING, MATURE, CENSORED, ROOT = 0, 1, 2, 3
FIELDS = {'parent': ('int64', -1), 'root': ('int64', -1),
          'birth_seq': ('int64', -1), 'birth_time': ('int64', -1),
          'followup': ('int64', 0), 'state': ('uint8', ROOT),
          'arrival': ('float64', 0), 'mass': ('float64', 0),
          'active_mass': ('float64', 0), 'label_mass': ('float64', 0),
          'mature_count': ('int64', 0)}


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(8 << 20), b''):
            h.update(block)
    return h.hexdigest()


def write_json(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, allow_nan=False, indent=2)


class SourceForest:
    """Dense IDs are storage addresses only; birth_seq determines causal presence.

    Equal source timestamps are ordered by sequence. Fixed maturity horizons let
    us use a FIFO instead of an ancestor scan or a heap for every event.
    """
    def __init__(self, capacity=16, namespace='history', horizon=3600, half_life=86400):
        if capacity < 1 or horizon <= 0 or half_life <= 0 or not namespace:
            raise ValueError('invalid source contract')
        self.namespace, self.horizon, self.half_life = namespace, horizon, half_life
        self.n = self.cutoff = 0
        self.as_of = self.anchor = None
        self.pending, self.gaps = deque(), []
        for key, (dtype, fill) in FIELDS.items():
            setattr(self, key, np.full(capacity, fill, dtype=dtype))

    def _ensure(self, size):
        if size <= len(self.parent):
            return
        cap = max(size, len(self.parent)*2)
        for key, (_, fill) in FIELDS.items():
            setattr(self, key, np.pad(getattr(self, key), (0, cap-len(getattr(self, key))), constant_values=fill))

    def advance(self, at):
        at = int(at)
        if self.as_of is not None and at < self.as_of:
            raise ValueError('source clock regressed')
        if self.anchor is None:
            self.anchor = at
        if at-self.anchor > 32*self.half_life:
            decay = 2.0 ** (-(at-self.anchor)/self.half_life)
            for key in ('arrival', 'mass', 'active_mass', 'label_mass'):
                getattr(self, key)[:self.n] *= decay
            self.anchor = at
        self.as_of = at
        while self.pending and int(self.birth_time[self.pending[0]])+self.horizon < at:
            child = self.pending.popleft()
            if self.state[child] != PENDING:
                continue
            p, y = int(self.parent[child]), int(self.followup[child])
            w = 2.0 ** ((int(self.birth_time[child])-self.anchor)/self.half_life)
            self.mass[p] += w
            self.active_mass[p] += w*(y > 0)
            self.label_mass[p] += w*y
            self.mature_count[p] += 1
            self.state[child] = MATURE

    def observe(self, a, b, at, sequence):
        a, b, at, sequence = map(int, (a, b, at, sequence))
        if min(a, b) < 0 or sequence != self.cutoff:
            raise ValueError('non-contiguous source event')
        self._ensure(max(a, b)+1)
        self.advance(at)
        endpoints = (a,) if a == b else (a, b)
        known = [x for x in endpoints if self.birth_seq[x] >= 0]
        novel = [x for x in endpoints if self.birth_seq[x] < 0]
        # Per-account labels can overlap across accounts, never used as chain work.
        for child in known:
            if self.state[child] == PENDING:
                self.followup[child] += 1
        parent = known[0] if len(known) == len(novel) == 1 else UNKNOWN
        root = int(self.root[parent]) if parent >= 0 else -(sequence+2)
        for child in novel:
            self.parent[child], self.root[child] = parent, root
            self.birth_seq[child], self.birth_time[child] = sequence, at
            self.state[child] = PENDING if parent >= 0 else ROOT
            if parent >= 0:
                self.arrival[parent] += 2.0 ** ((at-self.anchor)/self.half_life)
                self.pending.append(child)
        self.n = max(self.n, max(a, b)+1)
        self.cutoff += 1
        return parent, root, tuple(novel)

    def mark_gap(self, until_source_time):
        """Unknown time is not a run of zero activity. No fake maturity labels."""
        if self.as_of is None or until_source_time < self.as_of:
            raise ValueError('invalid observation gap')
        start = self.as_of
        for child in self.pending:
            self.state[child] = CENSORED
        self.pending.clear()
        self.gaps.append([start, int(until_source_time)])
        self.advance(until_source_time)

    def profiles(self, at=None):
        at = self.as_of if at is None else int(at)
        if at < self.as_of:
            raise ValueError('profile before consumed evidence')
        decay = 2.0 ** (-(at-self.anchor)/self.half_life)
        mass, active, labels = (getattr(self, k)[:self.n] for k in ('mass','active_mass','label_mass'))
        divide = lambda a, b: np.divide(a, b, out=np.zeros_like(a), where=b > 0)
        return dict(arrival_rate=self.arrival[:self.n]*decay*math.log(2)/self.half_life,
                    mature_mass=mass*decay, active_mass=active*decay, label_mass=labels*decay,
                    active_fraction=divide(active, mass), active_child_mean=divide(labels, active),
                    no_mature=mass == 0, no_active=active == 0,
                    mature_count=self.mature_count[:self.n].copy())

    def cutoff_view(self, cutoff):
        if not 0 <= cutoff <= self.cutoff:
            raise ValueError('invalid planning cutoff')
        roots = np.full(self.n, UNKNOWN, np.int64)
        order = np.argsort(self.birth_seq[:self.n], kind='stable')
        for a in order:
            seq, p = int(self.birth_seq[a]), int(self.parent[a])
            if seq < 0:
                continue
            roots[a] = int(a) if seq < cutoff else roots[p] if p >= 0 else self.root[a]
        return roots

    def grouped_profiles(self, groups):
        groups = np.asarray(groups)
        if groups.shape != (self.n,) or groups.dtype.kind not in 'iu' or np.any(groups < 0):
            raise ValueError('fixed group membership mismatch')
        decay=2.0 ** (-(self.as_of-self.anchor)/self.half_life)
        result={name:np.bincount(groups,weights=getattr(self,key)[:self.n])
                for name,key in [('arrival_rate','arrival'),('mature_mass','mass'),
                                 ('active_mass','active_mass'),('label_mass','label_mass'),('mature_count','mature_count')]}
        for key in ('arrival_rate','mature_mass','active_mass','label_mass'):result[key]*=decay
        result['arrival_rate']*=math.log(2)/self.half_life
        for name, top, bottom in [('active_fraction','active_mass','mature_mass'),
                                  ('active_child_mean','label_mass','active_mass')]:
            result[name] = np.divide(result[top], result[bottom], out=np.zeros_like(result[top]),
                                     where=result[bottom] > 0)
        result['no_mature'] = result['mature_mass'] == 0
        result['no_active'] = result['active_mass'] == 0
        return result

    def summary(self):
        states = np.bincount(self.state[:self.n], minlength=4)
        return dict(schema=SCHEMA, namespace=self.namespace, accounts=self.n, cutoff=self.cutoff,
                    source_time=self.as_of, mature=int(states[MATURE]), pending=int(states[PENDING]),
                    censored=int(states[CENSORED]), root_accounts=int(states[ROOT]),
                    horizon_seconds=self.horizon, half_life_seconds=self.half_life,
                    gaps=self.gaps, prediction=False)

    def save(self, directory):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=False)
        files = {}
        for key in FIELDS:
            path = directory/(key+'.npy')
            np.save(path, getattr(self, key)[:self.n], allow_pickle=False)
            files[path.name] = sha(path)
        write_json(directory/'manifest.json', dict(self.summary(), anchor=self.anchor, files=files))

    @classmethod
    def load(cls, directory):
        directory = Path(directory)
        m = json.loads((directory/'manifest.json').read_text(encoding='utf-8'))
        if m['schema'] != SCHEMA:
            raise ValueError('incompatible forest schema')
        obj = cls(max(1,m['accounts']), m['namespace'], m['horizon_seconds'],m['half_life_seconds'])
        for key in FIELDS:
            path = directory/(key+'.npy')
            if sha(path) != m['files'][path.name]:
                raise ValueError('checkpoint hash mismatch')
            setattr(obj, key, np.load(path, allow_pickle=False))
        obj.n, obj.cutoff, obj.as_of, obj.anchor = m['accounts'],m['cutoff'],m['source_time'],m['anchor']
        obj.gaps = m['gaps']
        ids = np.flatnonzero(obj.state == PENDING)
        obj.pending = deque(map(int, ids[np.argsort(obj.birth_seq[ids], kind='stable')]))
        return obj
