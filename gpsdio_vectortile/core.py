#!/usr/bin/env python


"""
Core components for gpsdio_vectortile
"""


import click
import gpsdio
import gpsdio.schema
import vectortile
import datetime
import hashlib
import datetime
import json
import os.path
import math
import msgpack
import contextlib
import struct

class Writer(object):
    def __init__(self, file):
        self.file = file
        self.packer = msgpack.Packer()

    def write(self, obj):
        self.file.write(self.packer.pack(obj))

@contextlib.contextmanager
def msgpack_open(name, mode='r'):
    if mode == 'r':
        with open(name) as f:
            yield msgpack.Unpacker(f)
    else:
        with open(name, mode) as f:
            yield Writer(f)

def float2bits(f):
    return struct.unpack('>l', struct.pack('>f', f))[0]

def bits2float(b):
    return struct.unpack('>f', struct.pack('>l', b))[0]

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
        "seriesgroup": "bits2float(mmsi)"
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

        self.root = QuadtreeNode(self)

        print "Loading data..."

        with msgpack_open(self.root.source_filename, "w") as outf:
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
        with msgpack_open("tree.msg", "w") as f:
            f.write({"max_depth": self.max_depth,
                     "max_count": self.max_count,
                     "remove": self.remove,
                     "clustering_levels": self.clustering_levels,
                     "filename": self.filename,
                     })
        self.root.save()

    @classmethod
    def load(cls):
        with msgpack_open("tree.msg") as f:
            spec = f.next()
        self = cls(spec.pop("filename"), __bare__ = True, **spec)
        self.root = QuadtreeNode(self)
        self.root.load()
        return self


class QuadtreeNode(object):
    def __init__(self, tree, bounds = vectortile.TileBounds(), count = 0, hollow = False, colsByName = None):
        self.tree = tree
        self.bounds = bounds
        self.bbox = self.bounds.get_bbox()
        self.count = count
        self.hollow = hollow
        self.colsByName = colsByName or {}
        self.children = None


    @property
    def source_filename(self):
        return "%s-src.msg" % self.bounds

    @property
    def cluster_filename(self):
        return "%s-cluster.msg" % self.bounds

    @property
    def info_filename(self):
        return "%s-info.msg" % self.bounds

    @property
    def tile_filename(self):
        return "%s" % self.bbox

    def save(self):
        with msgpack_open(self.info_filename, "w") as f:
            f.write({"bounds": str(self.bounds),
                     "count": self.count,
                     "hollow": self.hollow,
                     "colsByName": self.colsByName
                     })
        if self.children is not None:
            for child in self.children:
                child.save()

    def load(self):
        with msgpack_open(self.info_filename) as f:
            info = f.next()
            self.count = info['count']
            self.hollow = info['hollow']
            self.colsByName = info['colsByName']
        self.children = []
        for child_bounds in self.bounds.get_children():
            child = QuadtreeNode(self.tree, child_bounds)
            if os.path.exists(child.info_filename):
                self.children.append(child)
                child.load()
        if not self.children:
            self.children = None

    def generate_children(self):
        """Generates the fours child files for this child if they don't already exist."""

        if self.children is not None:
            return

        print "Generating children for %s (%s rows)" % (self.bbox, self.count)

        self.children = [QuadtreeNode(self.tree, b)
                         for b in self.bounds.get_children()]

        with msgpack_open(self.source_filename) as f:
            with msgpack_open(self.children[0].source_filename, "w") as self.children[0].file:
                with msgpack_open(self.children[1].source_filename, "w") as self.children[1].file:
                    with msgpack_open(self.children[2].source_filename, "w") as self.children[2].file:
                        with msgpack_open(self.children[3].source_filename, "w") as self.children[3].file:
                            for row in f:
                                for child in self.children:
                                    if self.tree.latitude_col in row and self.tree.longitude_col in row and child.bbox.contains(row[self.tree.longitude_col], row[self.tree.latitude_col]):
                                        child.file.write(row)
                                        child.count += 1
                                        break
        for child in self.children:
            del child.file

        return self.children

    def generate_tree(self, max_depth = None):
        """Generates child files down to self.max_depth, or until each
        file is smaller than self.tree.max_count. Parent files are removed,
        unless self.tree.remove = False"""

        if max_depth is None:
            max_depth = self.tree.max_depth
        else:
            max_depth -= 1
            if max_depth == 0:
                return
        self.generate_children()
        if self.tree.remove:
            os.unlink(self.source_filename)
            self.hollow = True
        for child in self.children:
            if child.count > self.tree.max_count:
                child.generate_tree(max_depth)

    def generate_tiles(self):
        """Generate tiles for all levels, assuming the tree has been
        generated using generate_tree() first."""
        if self.children:
            for child in self.children:
                child.generate_tiles()
            print "Generating tile for %s using child tiles" % self.bbox
            self.generate_tile_from_child_tiles()
        else:
            print "Generating tile for %s using source data" % self.bbox
            self.generate_tile_from_source()

    def update_colsByName(self, row):
        for key, value in row.iteritems():
            if key == 'datetime' and value is None:
                import pdb
                pdb.set_trace()
            if key not in self.colsByName:
                self.colsByName[key] = {"min": value, "max": value}
            else:
                if value < self.colsByName[key]['min']:
                    self.colsByName[key]['min'] = value
                if value > self.colsByName[key]['max']:
                    self.colsByName[key]['max'] = value
        return row

    def write_tile(self, clusters):
        with msgpack_open(self.cluster_filename, "w") as f:
            for cluster in clusters:
                f.write(cluster.get_cluster_row())

        with open(self.tile_filename, "w") as f:
            f.write(str(vectortile.Tile.fromdata(
                        [self.update_colsByName(self.tree.map_row(cluster.get_row()))
                         for cluster in clusters], {})))

    def generate_tile_from_source(self):
        with msgpack_open(self.source_filename) as f:
            rows = list(f)

        self.write_tile([Cluster.from_row(row) for row in rows])

    def generate_tile_from_child_tiles(self):
        clusters = {}
        for child in self.children:
            with msgpack_open(child.cluster_filename) as f:
                for row in f:
                    cluster = Cluster.from_cluster_row(row)
                    row = cluster.get_row()
                    gridcode = str(vectortile.TileBounds.from_point(row[self.tree.longitude_col], row[self.tree.latitude_col], self.bounds.zoom_level + self.tree.clustering_levels))
                    if gridcode not in clusters:
                        clusters[gridcode] = cluster
                    else:
                        clusters[gridcode] += cluster

        # Merge clusters until we have few enough for a tile
        while len(clusters) > self.tree.max_count:
            newclusters = {}
            for gridcode, cluster in clusters.iteritems():
                gridcode = gridcode[:-1]
                if gridcode not in newclusters:
                    newclusters[gridcode] = cluster
                else:
                    newclusters[gridcode] += cluster
            clusters = newclusters

        self.write_tile(clusters.values())

    def generate_header(self):
        with open("header", "w") as f:
            f.write(json.dumps(
                    {"colsByName": self.colsByName,
                     "seriesTilesets": False,
                     "tilesetName": self.tree.name,
                     "tilesetVersion": "0.0.1"
                     }))

    def generate_workspace(self):
        time = datetime.datetime.fromtimestamp((self.colsByName['datetime']['min'] + self.colsByName['datetime']['max']) / 2.0 / 1000.0).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        timeExtent = (self.colsByName['datetime']['max'] - self.colsByName['datetime']['min']) / 10

        with open("workspace", "w") as f:
            f.write(json.dumps(
                    {
                        "state": {
                            "title": self.tree.name,
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
                                        "title": self.tree.name,
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

    # def __add__(self, other):
    #     res = type(self)()

    #     res.a = self
    #     res.b = other

    #     res.counts = dict(self.counts)
    #     res.sums = dict(self.sums)
    #     res.sqr_sums = dict(self.sqr_sums)

    #     if isinstance(other, Cluster):
    #         for key in other.counts.iterkeys():
    #             res._add_col(key)
    #             res.counts[key] += other.counts[key]
    #             res.sums[key] += other.sums[key]
    #             res.sqr_sums[key] += other.sqr_sums[key]

    #     else:
    #         for key, value in other.iteritems():
    #             if isinstance(value, datetime.datetime):
    #                 value = float(value.strftime("%s"))
    #             if not isinstance(value, (int, float, bool)): continue
    #             res._add_col(key)
    #             res.counts[key] += 1.0
    #             res.sums[key] += value
    #             res.sqr_sums[key] += value**2
    #     return res

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

    def format(self, key, indent = ''):
        res = "%scount=%s, sum=%s, sqr_sum=%s" % (indent, self.counts[key], self.sums[key], self.sqr_sums[key])
        if hasattr(self, "a"):
            res += "\n" + self.a.format(key, indent + '  ')
            res += "\n" + self.b.format(key, indent + '  ')
        return res

@click.command(name='vectortile-generate-tree')
@click.argument("infile", metavar="INFILENAME")
@click.pass_context
def gpsdio_vectortile_generate_tree(ctx, infile):
    tree = Quadtree(infile)
    tree.root.generate_tree()
    tree.save()


@click.command(name='vectortile-generate-tiles')
@click.pass_context
def gpsdio_vectortile_generate_tiles(ctx):
    tree = Quadtree.load()
    tree.root.generate_tiles()
    tree.save()

@click.command(name='vectortile-generate-headers')
@click.pass_context
def gpsdio_vectortile_generate_headers(ctx):
    tree = Quadtree.load()
    tree.root.generate_header()
    tree.root.generate_workspace()

if __name__ == '__main__':
    gpsdio_vectortile()
