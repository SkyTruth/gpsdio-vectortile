import gpsdio
import datetime
import quad_tree_node
import utils
import json

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



    def generate_header(self):
        with open("header", "w") as f:
            f.write(json.dumps(
                    {"colsByName": self.root.colsByName,
                     "seriesTilesets": False,
                     "tilesetName": self.name,
                     "tilesetVersion": "0.0.1"
                     }))

    def generate_workspace(self):
        time = datetime.datetime.fromtimestamp((self.root.colsByName['datetime']['min'] + self.root.colsByName['datetime']['max']) / 2.0 / 1000.0).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        timeExtent = (self.root.colsByName['datetime']['max'] - self.root.colsByName['datetime']['min']) / 10

        with open("workspace", "w") as f:
            f.write(json.dumps(
                    {
                        "state": {
                            "title": self.name,
                            "offset": 20,
                            "maxoffset": 100,
                            "lat": 0.0,
                            "lon": 0.0,
                            "zoom":3,
                            "time":{"__jsonclass__":["Date",time]},
                            "timeExtent": timeExtent,
                            "paused":True
                            },
                        "map": {
                            "animations": [
                                {
                                    "args": {
                                        "title": self.name,
                                        "visible": True,
                                        "source": {
                                            "type": "TiledBinFormat",
                                            "args": {
                                                "url": "./"
                                                }
                                            },
                                        "columns": {
                                            "longitude":{"type":"Float32",
                                                         "hidden":True,
                                                         "source":{"longitude":1}},
                                            "latitude":{"type":"Float32",
                                                        "hidden":True,
                                                        "source":{"latitude":1}},
                                            "sigma":{"type":"Float32",
                                                     "source":{"sigma":1},
                                                     "min":0,
                                                     "max":1},
                                            "weight":{"type":"Float32",
                                                      "source":{"speed":1},
                                                      "min":0,
                                                      "max":1},
                                            "time":{"type":"Float32",
                                                    "hidden":True,
                                                    "source":{"datetime":1}},
                                            "filter":{"type":"Float32",
                                                      "source":{"_":None,
                                                                "timerange":-1,
                                                                "active_category":-1}},
                                            "selected":{"type":"Float32",
                                                        "hidden":True,
                                                        "source":{"selected":1}},
                                            "hover":{"type":"Float32",
                                                     "hidden":True,
                                                     "source":{"hover":1}}
                                            },
                                        "selections": {
                                            "selected": {
                                                "sortcols": ["seriesgroup"]
                                                },
                                            "hover": {
                                                "sortcols": ["seriesgroup"]
                                                }
                                            }
                                        },
                                    "type": "ClusterAnimation"
                                    }
                                ],
                            "options": {
                                "mapTypeId": "roadmap",
                                "styles": [
                                    {
                                        "featureType": "poi",
                                        "stylers": [
                                            {
                                                "visibility": "off"
                                                }
                                            ]
                                        },
                                    {
                                        "featureType": "administrative",
                                        "stylers": [{ "visibility": "simplified" }]
                                        },
                                    {
                                        "featureType": "administrative.country",
                                        "stylers": [
                                            { "visibility": "on" }
                                            ]
                                        },
                                    {
                                        "featureType": "road",
                                        "stylers": [
                                            { "visibility": "off" }
                                            ]
                                        },
                                    {
                                        "featureType": "landscape.natural",
                                        "stylers": [
                                            { "visibility": "off" }
                                            ]
                                        }
                                    ]
                                }
                            }
                        }        
                    ))
