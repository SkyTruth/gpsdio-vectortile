import datetime
import math

class Cluster(object):
    def __init__(self):
        self.counts = {}
        self.sums = {}
        self.sqr_sums = {}

    @classmethod
    def from_cluster_row(cls, row):
        self = cls()
        for key, value in row.iteritems():
            if key.startswith("counts__"):
                key = key[len("counts__"):]
                self.counts[key] = value
            elif key.startswith("sums__"):
                key = key[len("sums__"):]
                self.sums[key] = value
            elif key.startswith("sqr_sums__"):
                key = key[len("sqr_sums__"):]
                self.sqr_sums[key] = value
        return self

    @classmethod
    def from_row(cls, row):
        self = cls()
        self += row
        return self

    def _add_col(self, name):
        if name not in self.counts:
            self.counts[name] = 0.0
            self.sums[name] = 0.0
            self.sqr_sums[name] = 0.0

    def __iadd__(self, other):
        if isinstance(other, Cluster):
            for key in other.counts.iterkeys():
                self._add_col(key)
                self.counts[key] += other.counts[key]
                self.sums[key] += other.sums[key]
                self.sqr_sums[key] += other.sqr_sums[key]

        else:
            for key, value in other.iteritems():
                if isinstance(value, datetime.datetime):
                    value = float(value.strftime("%s"))
                if not isinstance(value, (int, float, bool)): continue
                self._add_col(key)
                self.counts[key] += 1.0
                self.sums[key] += value
                self.sqr_sums[key] += value**2
        return self

    def get_cluster_row(self):
        res = {}
        for key in self.counts.iterkeys():
            res['counts__' + key] = self.counts[key]
            res['sums__' + key] = self.sums[key]
            res['sqr_sums__' + key] = self.sqr_sums[key]
        return res

    def get_row(self):
        res = {}
        for key in self.counts.iterkeys():            
            res[key] = self.sums[key] / self.counts[key]
            var = self.sqr_sums[key]/self.counts[key] - (self.sums[key]/self.counts[key])**2
            # Handle overflow and precission underflow
            if var < 0.0:
                if var < -1.0e-5:
                    continue
                var = 0
            res[key + "_stddev"] =  math.sqrt(var)
        return res
