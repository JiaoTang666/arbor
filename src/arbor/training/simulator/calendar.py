import numpy as np

class Calendar:
    def __init__(self, row):
        self.row = row
        self.delays = np.asarray(row['commit_delays'], np.float64)
        self.phases = np.asarray(row['first_proposal'], np.float64)
        if self.delays.ndim != 2 or self.delays.shape[1] != len(self.phases) or \
                not np.isfinite(self.delays).all() or np.any(self.delays <= 0):
            raise ValueError('invalid service calendar')
        self.report_delay = float(row.get('report_delay', 0.))
        self.publish_delay = float(row.get('publish_delay', 0.))
        self.pauses = row.get('pauses', [])

    def available(self, shard, at):
        for pause in self.pauses:
            if shard in pause['shards'] and pause['start'] <= at < pause['end']:
                at = pause['end']
        return at

    def finish(self, shard, start):
        duration = self.delays[int(start // 5) % len(self.delays), shard]
        end = start + duration
        for pause in self.pauses:
            if shard in pause['shards'] and start < pause['end'] and end > pause['start']:
                end += pause['end'] - max(start, pause['start'])
        return float(end)

    @classmethod
    def nominal(cls, shards=16, commit=.354, interval=5.):
        return cls(dict(family='N', commit_delays=[[commit]*shards],
                        first_proposal=[interval]*shards, report_delay=0., publish_delay=0., pauses=[]))
