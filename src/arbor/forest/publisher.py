"""Publish graph and forest facts at one accepted source watermark."""
import hashlib
from collections import deque
from .adapter import _atomic_json, _json_bytes
from arbor.io import snapshot_digest as graph_digest

class JointPublisher:
    def __init__(self, adapter):
        self.adapter = adapter
        self.pending = deque()
        self.accepted = adapter.forest.cutoff
        self.source_seen = adapter.forest.as_of
        self.last = None

    def accept(self, a, b, at, sequence, sa, sb):
        if sequence != self.accepted or at < self.source_seen:
            raise ValueError('accepted prefix regressed or has a gap')
        self.pending.append((int(a), int(b), int(at), int(sequence), int(sa), int(sb)))
        self.accepted += 1
        self.source_seen = int(at)

    def publish(self, graph, path=None, next_accepted_source_time=None):
        if graph_digest(graph) != graph['snapshot_sha256']:
            raise ValueError('graph publication content hash mismatch')
        cutoff, at = int(graph['accepted_cutoff_exclusive']), int(graph['source_time'])
        if cutoff > self.accepted or cutoff < self.adapter.forest.cutoff:
            raise ValueError('graph prefix is not available from accepted records')
        while self.pending and self.pending[0][3] < cutoff:
            self.adapter.observe(*self.pending.popleft())
        # An observed next event can establish that no source-time gap occurred.
        # It is NOT consumed or included in any statistic at this publication.
        seen = self.source_seen
        if next_accepted_source_time is not None:
            seen = max(seen, int(next_accepted_source_time))
        if at > seen:
            raise ValueError('graph watermark has no continuous observation witness')
        if self.pending and at > self.pending[0][2]:
            raise ValueError('watermark skips an unconsumed earlier source event')
        self.adapter.advance_watermark(at, cutoff)
        facts = self.adapter.publish(graph['version'], graph['snapshot_sha256'], cutoff, at)
        payload = _json_bytes(dict(graph=graph, forest=facts))
        # Digest the exact UTF-8 payload, avoiding Python/Go float rendering differences.
        envelope = dict(schema='graph_forest_joint_v2', payload_json=payload.decode('utf-8'),
                        payload_sha256=hashlib.sha256(payload).hexdigest())
        if path is not None:
            _atomic_json(path, envelope)
        self.last = envelope
        return envelope
