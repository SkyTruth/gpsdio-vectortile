import gpsdio
import datetime
import quad_tree_node
import utils

class Quadtree(object):
    max_depth = None
    max_count = 16000
    remove = True

    clustering_levels = 6

    latitude_col = "lat"
    longitude_col = "lon"

    columnMap = {
        "datetime": "timestamp",
        "latitude": "lat",
        "longitude": "lon",
        "course": "course",
        "speed": "speed",
        "series": "track",
        "seriesgroup": "utils.bits2float(mmsi)"
        }

    def map_row(self, row):
        row['row'] = row
        out_row = {}
        for key, expr in self.columnMap.iteritems():
            try:
                value = eval(expr, globals(), row)
            except Exception, e:
                value = None
            out_row[key] = value
        del row['row']
        return out_row

    def __init__(self, filename, __bare__ = False, **kw):
        self.filename = filename
        for key, value in kw.iteritems():
            setattr(self, key, value)

        if __bare__: return

        self.root = quad_tree_node.QuadtreeNode(self)

        print "Loading data..."

        with utils.msgpack_open(self.root.source_filename, "w") as outf:
            with gpsdio.open(filename) as f:
                for row in f:
                    out_row = {}
                    for key, value in row.iteritems():
                        if isinstance(value, datetime.datetime):
                            value = float(value.strftime("%s")) * 1000.0
                        if not isinstance(value, (float, int, bool)):
                            continue
                        out_row[key] = value
                    outf.write(out_row)
                    self.root.count += 1


    @property
    def name(self):
        return self.filename.split(".")[0]

    def save(self):
        with utils.msgpack_open("tree.msg", "w") as f:
            f.write({"max_depth": self.max_depth,
                     "max_count": self.max_count,
                     "remove": self.remove,
                     "clustering_levels": self.clustering_levels,
                     "filename": self.filename,
                     })
        self.root.save()

    @classmethod
    def load(cls):
        with utils.msgpack_open("tree.msg") as f:
            spec = f.next()
        self = cls(spec.pop("filename"), __bare__ = True, **spec)
        self.root = quad_tree_node.QuadtreeNode(self)
        self.root.load()
        return self
