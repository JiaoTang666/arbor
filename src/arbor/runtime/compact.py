"""Read native projected events without coupling to optional effect indexing."""
import json
import re


class CompactReader:
    def __init__(self, input_sha256, count, expected_routes=None):
        if not re.fullmatch('[0-9a-f]{64}', input_sha256) or count < 1:
            raise ValueError('invalid observer scope')
        self.input = input_sha256
        self.count, self.expected = count, expected_routes
        self.header = False
        self.hashes, self.completed = {}, set()

    def feed(self, event):
        if not self.header:
            if not isinstance(event, dict) or event.get('schema') != 'occurrence_observer_v1' or \
                    event.get('input_sha256') != self.input:
                raise ValueError('foreign observer stream')
            self.header = True
            return None
        if not isinstance(event, list) or len(event) not in (4, 8):
            raise ValueError('invalid compact event')
        kind, seq, tx_hash = event[:3]
        if type(seq) is not int or not 0 <= seq < self.count or \
                not isinstance(tx_hash, str) or not re.fullmatch('[0-9a-f]{64}', tx_hash):
            raise ValueError('invalid occurrence binding')
        if kind == 'r' and len(event) == 8:
            if seq in self.hashes:
                raise ValueError('duplicate route occurrence')
            if self.expected is not None and tuple(event[4:6]) != tuple(self.expected[seq]):
                raise ValueError('route differs from replay')
            self.hashes[seq] = tx_hash
            return event
        if kind == 'c' and len(event) == 4:
            if self.hashes.get(seq) != tx_hash or seq in self.completed:
                raise ValueError('completion without unique matching route')
            self.completed.add(seq)
            return None
        raise ValueError('unknown compact event')


def read_complete_line(stream):
    start = stream.tell()
    line = stream.readline()
    if line and not line.endswith('\n'):
        stream.seek(start)
        return None
    return json.loads(line) if line else None
