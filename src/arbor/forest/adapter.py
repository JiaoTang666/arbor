"""Publish source and activity facts at the graph's accepted-prefix watermark.

Replay stops at the graph publication cutoff, including before a triggering
transaction when the graph is published first. Background publish() aggregates
accounts in O(accounts); runtime queries read the cached snapshot(). Historical
facts are loaded from the source-forest checkpoint. Source relationships and
route ownership are tracked separately, allowing a family to span shards.
"""
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile

import numpy as np


from . import source as _MODULE
from .source import SourceForest

SCHEMA = 'forest_slow_facts_v2'
SEMANTICS = 'source_activity_facts_v2.0'
SEED_SCHEMA = 'forest_native_seed_v2'


def _json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def _atomic_json(path, value):
    """Only publication cadence writes; no transaction-level durability barrier."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix='.' + path.name + '.', dir=str(path.parent))
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(_json_bytes(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class SlowForestAdapter:
    def __init__(self, forest, routes, shards=16):
        if not isinstance(forest, SourceForest) or shards < 1:
            raise ValueError('invalid source/shard contract')
        route_array = np.asarray(routes)
        if route_array.ndim != 1 or route_array.dtype.kind not in 'iu' or len(route_array) < forest.n:
            raise ValueError('seed route shape/type mismatch')
        known = forest.birth_seq[:forest.n] >= 0
        if np.any((route_array[:forest.n][known] < 0) | (route_array[:forest.n][known] >= shards)):
            raise ValueError('known historical account has no valid route')
        self.owners = np.full(forest.n, -1, dtype=np.int32)
        self.owners[known] = route_array[:forest.n][known]
        if np.any((self.owners[known] < 0) | (self.owners[known] >= shards)):
            raise ValueError('known historical account has no valid route')
        self.forest, self.shards = forest, int(shards)
        self.seed_cutoff = int(forest.cutoff)
        self.seed_source_time = forest.as_of
        self.seed_known = int(known.sum())
        self.seed_pending = int(np.count_nonzero(known & (forest.state[:forest.n] == _MODULE.PENDING)))
        self.runtime_bound = 0
        self._published = None
        self._publication_signature = None

    @classmethod
    def from_checkpoint(cls, checkpoint_dir, routes, shards=16):
        if isinstance(routes, (str, Path)):
            routes = np.load(routes, allow_pickle=False)
        return cls(SourceForest.load(checkpoint_dir), routes, shards)

    def observe(self, a, b, source_time, global_sequence, source_shard, target_shard):
        a, b, source_time, global_sequence, source_shard, target_shard = map(
            int, (a, b, source_time, global_sequence, source_shard, target_shard))
        if min(a, b) < 0 or global_sequence != self.forest.cutoff:
            raise ValueError('non-contiguous accepted source event')
        if not (0 <= source_shard < self.shards and 0 <= target_shard < self.shards):
            raise ValueError('invalid observed route')
        if a == b and source_shard != target_shard:
            raise ValueError('self-transfer cannot occupy two shards')
        if self.forest.as_of is not None and source_time < self.forest.as_of:
            raise ValueError('source clock regressed')
        endpoints = ((a, source_shard),) if a == b else ((a, source_shard), (b, target_shard))
        for account, shard in endpoints:
            if account < self.forest.n and self.forest.birth_seq[account] >= 0:
                if self.owners[account] != shard:
                    raise ValueError('bound account route changed')
        if max(a, b) >= len(self.owners):
            capacity = max(max(a, b) + 1, max(1, len(self.owners)) * 2)
            self.owners = np.pad(self.owners, (0, capacity - len(self.owners)), constant_values=-1)
        result = self.forest.observe(a, b, source_time, global_sequence)
        for account, shard in endpoints:
            if self.owners[account] < 0:
                self.owners[account] = shard
                self.runtime_bound += 1
        return result

    def advance_watermark(self, source_time, accepted_cutoff_exclusive):
        """Advance only from an externally established continuous source watermark.

        This does not accept any transaction or fill an observation gap with zeros.
        The caller must use mark_gap() instead for a genuinely missing interval.
        No wall clock or desired publication time is valid evidence for this call.
        """
        if int(accepted_cutoff_exclusive) != self.forest.cutoff:
            raise ValueError('watermark prefix mismatch')
        self.forest.advance(int(source_time))

    def mark_gap(self, until_source_time):
        self.forest.mark_gap(int(until_source_time))

    def _aggregate(self):
        f, size = self.forest, self.shards
        known = f.birth_seq[:f.n] >= 0
        owners = self.owners[:f.n]
        decay = 2.0 ** (-(f.as_of - f.anchor) / f.half_life)
        totals = {}
        for output, source in (('mature_weight', 'mass'), ('active_weight', 'active_mass'),
                               ('participation_weight', 'label_mass'), ('arrival_weight', 'arrival')):
            totals[output] = np.bincount(owners[known], weights=getattr(f, source)[:f.n][known], minlength=size) * decay
        totals['arrival_rate'] = totals.pop('arrival_weight') * math.log(2) / f.half_life
        # Counters are int64, not floating-point model demands.
        totals['mature_sample_count'] = np.zeros(size, np.int64)
        np.add.at(totals['mature_sample_count'], owners[known], f.mature_count[:f.n][known])
        totals['bound_accounts'] = np.bincount(owners[known], minlength=size)
        for name, state in (('pending_children', _MODULE.PENDING), ('censored_children', _MODULE.CENSORED)):
            children = np.flatnonzero(known & (f.state[:f.n] == state))
            parents = f.parent[children]
            if np.any(parents < 0) or np.any(owners[parents] < 0):
                raise ValueError('child activity has no known direct entrance')
            totals[name] = np.bincount(owners[parents], minlength=size)
        for name, numerator, denominator in (
                ('active_fraction', 'active_weight', 'mature_weight'),
                ('active_child_mean', 'participation_weight', 'active_weight')):
            totals[name] = np.divide(totals[numerator], totals[denominator],
                                    out=np.zeros(size), where=totals[denominator] > 0)
        totals['no_mature'] = totals['mature_weight'] == 0
        totals['no_active'] = totals['active_weight'] == 0
        return {name: value.tolist() for name, value in totals.items()}

    def publish(self, graph_version, graph_digest, accepted_cutoff_exclusive, source_time, path=None):
        """Cache one immutable graph/forest generation with identical watermarks."""
        version, cutoff, source_time = map(int, (graph_version, accepted_cutoff_exclusive, source_time))
        if version < 0 or cutoff != self.forest.cutoff or source_time != self.forest.as_of:
            raise ValueError('graph/forest publication watermarks differ')
        if not isinstance(graph_digest, str) or len(graph_digest) != 64 or any(c not in '0123456789abcdef' for c in graph_digest):
            raise ValueError('graph digest must be lowercase SHA-256')
        previous = self._published
        if previous is not None:
            if version < previous['graph_version'] or cutoff < previous['accepted_cutoff_exclusive'] or source_time < previous['source_time']:
                raise ValueError('publication regressed')
        value = dict(schema=SCHEMA, semantics=SEMANTICS, graph_version=version, graph_sha256=graph_digest,
                     accepted_cutoff_exclusive=cutoff, source_time=source_time,
                     namespace=self.forest.namespace, shards=self.shards,
                     horizon_seconds=self.forest.horizon, half_life_seconds=self.forest.half_life,
                     seed_cutoff=self.seed_cutoff, seed_known_accounts=self.seed_known,
                     seed_pending_carried=self.seed_pending, runtime_bound_accounts=self.runtime_bound,
                     submitted_creation_status='not_inferred_from_binding',
                     projection='direct_entrance_actual_bound_shard',
                     activity=self._aggregate(), observed_gaps=copy.deepcopy(self.forest.gaps), prediction=False)
        signature = hashlib.sha256(_json_bytes(value)).hexdigest()
        if previous is not None and version == previous['graph_version'] and signature != self._publication_signature:
            raise ValueError('same generation was mutated')
        value['facts_sha256'] = signature
        if path is not None:
            _atomic_json(path, value)
        self._published, self._publication_signature = value, signature
        return copy.deepcopy(value)

    def snapshot(self):
        if self._published is None:
            raise ValueError('no forest generation published')
        return copy.deepcopy(self._published)

    def export_native_seed(self, directory, addresses, chunk_size=65536):
        """Stream the immutable historical presence/root map, never future IDs.

        Each record: 40 lowercase ASCII hex address bytes + signed LE int64 root.
        No 0x prefix, binary-address conversion, numeric-ID pseudo-address, or
        address-based random assignment is performed. The catalog only identifies
        already-observed account IDs; birth_seq determines which rows are emitted.
        """
        if self.forest.cutoff != self.seed_cutoff or self.forest.as_of != self.seed_source_time or self.runtime_bound:
            raise ValueError('native seed must be exported before runtime observations')
        if chunk_size < 1:
            raise ValueError('invalid export chunk size')
        catalog = np.asarray(addresses)
        if catalog.ndim != 1 or len(catalog) < self.forest.n or catalog.dtype.kind not in 'SU':
            raise ValueError('real address catalog required')
        known = np.flatnonzero(self.forest.birth_seq[:self.forest.n] >= 0)
        address_view = catalog[known]
        # Validate BEFORE narrowing the dtype; otherwise long addresses truncate.
        encoded = []
        for start in range(0, len(known), chunk_size):
            raw = address_view[start:start + chunk_size]
            values = []
            for address in raw:
                item = bytes(address) if catalog.dtype.kind == 'S' else str(address).encode('ascii', errors='strict')
                if len(item) != 40 or any(c not in b'0123456789abcdef' for c in item):
                    raise ValueError('catalog address must be 40 lowercase ASCII hex bytes')
                values.append(item)
            encoded.append(np.asarray(values, dtype='S40'))
        account_addresses = np.concatenate(encoded) if encoded else np.empty(0, dtype='S40')
        ordered = np.sort(account_addresses)
        if len(ordered) > 1 and np.any(ordered[1:] == ordered[:-1]):
            raise ValueError('duplicate historical address identity')
        del ordered, encoded, address_view
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=False)
        binary_path = directory / 'sources.bin'
        digest = hashlib.sha256()
        dtype = np.dtype([('address', 'S40'), ('root', '<i8')], align=False)
        with binary_path.open('xb') as stream:
            for start in range(0, len(known), chunk_size):
                ids = known[start:start + chunk_size]
                records = np.empty(len(ids), dtype=dtype)
                records['address'] = account_addresses[start:start + chunk_size]
                records['root'] = self.forest.root[ids]
                if np.any(records['root'] >= -1):
                    raise ValueError('historical source root is not a v2 virtual root')
                block = records.tobytes(order='C')
                stream.write(block)
                digest.update(block)
        manifest = dict(schema=SEED_SCHEMA, count=int(len(known)), record_bytes=48,
                        address_format='40_lowercase_ascii_hex_no_prefix', root_format='signed_int64_little_endian',
                        cutoff=int(self.forest.cutoff), source_time=int(self.forest.as_of),
                        namespace=self.forest.namespace, sha256=digest.hexdigest(), file=binary_path.name,
                        pending_carried=self.seed_pending, input_presence='birth_seq>=0_not_catalog_membership')
        _atomic_json(directory / 'manifest.json', manifest)
        return manifest
