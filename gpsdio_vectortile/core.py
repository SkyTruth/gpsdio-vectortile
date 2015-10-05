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


class Quadtree(object):
    max_depth = None
    max_count = 16000
    remove = True

    clustering_levels = 6

    columns = None

    def __init__(self, filename, __bare__ = False, **kw):
        self.filename = filename
        for key, value in kw.iteritems():
            setattr(self, key, value)

        if __bare__: return

        self.root = QuadtreeNode(self, vectortile.TileBounds(), filename, is_source=True)

        print "Calculating length..."
        with gpsdio.open(self.root.filename) as f:
            for row in f:
                for key, value in row.iteritems():
                    if self.columns is not None and key not in self.columns:
                        continue
                    if isinstance(value, datetime.datetime):
                        value = float(value.strftime("%s"))
                    if isinstance(value, (int, float, bool)):
                        if key not in self.root.colsByName:
                            self.root.colsByName[key] = {"min": value, "max": value}
                        else:
                            if value < self.root.colsByName[key]['min']:
                                self.root.colsByName[key]['min'] = value
                            if value > self.root.colsByName[key]['max']:
                                self.root.colsByName[key]['max'] = value
                self.root.count += 1


    def serialize(self):
        return {
            "max_depth": self.max_depth,
            "max_count": self.max_count,
            "remove": self.remove,
            "clustering_levels": self.clustering_levels,
            "columns": self.columns,
            "filename": self.filename,
            "root": self.root.serialize()
            }

    @classmethod
    def deserialize(cls, spec):
        root = spec.pop("root")
        self = cls(spec.pop("filename"), __bare__ = True, **spec)
        self.root = QuadtreeNode.deserialize(self, root)
        return self


class QuadtreeNode(object):
    def __init__(self, tree, bounds, filename = None, count = 0, is_source = False, hollow = False, colsByName = None):
        self.tree = tree
        self.bounds = bounds
        self.bbox = self.bounds.get_bbox()
        self.filename = filename
        self.tile_filename = str(self.bbox)
        self.cluster_filename = "%s.clusters.msg" % self.bbox
        self.count = count
        self.is_source = is_source
        self.hollow = hollow
        self.colsByName = colsByName or {}
        self.children = None

        if self.filename is None:
            self.filename = "%s.msg" % self.bounds.get_bbox()

    def serialize(self):
        return {
            "bounds": str(self.bounds),
            "filename": self.filename,
            "count": self.count,
            "is_source": self.is_source,
            "hollow": self.hollow,
            "colsByName": self.colsByName,
            "children": self.children and [child.serialize() for child in self.children]
            }

    @classmethod
    def deserialize(cls, tree, spec):
        children = spec.pop("children")
        spec['bounds'] = vectortile.TileBounds(spec['bounds'])
        self = cls(tree, **spec)
        if children:
            self.children = [cls.deserialize(tree, child) for child in children]
        return self

    def generate_children(self):
        """Generates the fours child files for this child if they don't already exist."""

        if self.children is not None:
            return

        print "Generating children for %s (%s rows)" % (self.bbox, self.count)

        self.children = [QuadtreeNode(self.tree, b)
                         for b in self.bounds.get_children()]

        with gpsdio.open(self.filename) as f:
            with gpsdio.open(self.children[0].filename, "w") as self.children[0].file:
                with gpsdio.open(self.children[1].filename, "w") as self.children[1].file:
                    with gpsdio.open(self.children[2].filename, "w") as self.children[2].file:
                        with gpsdio.open(self.children[3].filename, "w") as self.children[3].file:
                            for row in f:
                                for child in self.children:
                                    if 'lat' in row and 'lon' in row and child.bbox.contains(row['lon'], row['lat']):
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
        if self.tree.remove and not self.is_source:
            os.unlink(self.filename)
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

    def write_tile(self, clusters):
        with gpsdio.open(self.cluster_filename, "w") as f:
            for cluster in clusters:
                f.write(cluster.get_cluster_row())

        with open(self.tile_filename, "w") as f:
            def get_row(cluster):
                row = cluster.get_row()
                if self.tree.columns is not None:
                    row = {key: value
                           for key, value in row.iteritems()
                           if key in self.tree.columns}
                return row

            f.write(str(vectortile.Tile.fromdata(
                        [get_row(cluster)
                         for cluster in clusters], {})))

    def generate_tile_from_source(self):
        with gpsdio.open(self.filename) as f:
            rows = list(f)
        
        if self.tree.columns is not None:
            rows = [{key: value
                     for (key, value) in row.iteritems()
                     if key in self.tree.columns}
                    for row in rows]

        self.write_tile([Cluster.from_row(row) for row in rows])

    def generate_tile_from_child_tiles(self):
        clusters = {}
        for child in self.children:
            with open(child.tile_filename) as f:
                header, data = vectortile.Tile(f.read()).unpack()
                for row in data:
                    gridcode = str(vectortile.TileBounds.from_point(row['lon'], row['lat'], self.bounds.zoom_level + self.tree.clustering_levels))
                    if gridcode not in clusters: clusters[gridcode] = Cluster()
                    clusters[gridcode] += row

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

    @property
    def name(self):
        return self.filename.split(".")[0]
    
    def generate_header(self):
        with open("header", "w") as f:
            f.write(json.dumps(
                    {"colsByName": self.colsByName,
                     "seriesTilesets": False,
                     "tilesetName": self.name,
                     "tilesetVersion": "0.0.1"
                     }))

    def generate_workspace(self):
        time = datetime.datetime.fromtimestamp((self.colsByName['timestamp']['min'] + self.colsByName['timestamp']['max']) / 2.0).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        timeExtent = (self.colsByName['timestamp']['max'] - self.colsByName['timestamp']['min']) / 10

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
            self.counts[name] = 0
            self.sums[name] = 0
            self.sqr_sums[name] = 0

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
                self.counts[key] += 1
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
            res[key + "_stddev"] =  math.sqrt(self.sqr_sums[key]/self.counts[key] - (self.sums[key]/self.counts[key])**2)
        return res


@click.command(name='vectortile-generate-tree')
@click.argument("infile", metavar="INFILENAME")
@click.pass_context
def gpsdio_vectortile_generate_tree(ctx, infile):
    tree = Quadtree(infile, columns=["timestamp", "lat", "lon", "course", "speed"])
    tree.root.generate_tree()
    with open("tree.json", "w") as f:
        f.write(json.dumps(tree.serialize()))


@click.command(name='vectortile-generate-tiles')
@click.pass_context
def gpsdio_vectortile_generate_tiles(ctx):
    with open("tree.json") as f:
        tree = Quadtree.deserialize(json.loads(f.read()))
    tree.root.generate_tiles()

@click.command(name='vectortile-generate-headers')
@click.pass_context
def gpsdio_vectortile_generate_headers(ctx):
    with open("tree.json") as f:
        tree = Quadtree.deserialize(json.loads(f.read()))
    tree.root.generate_header()
    tree.root.generate_workspace()

if __name__ == '__main__':
    gpsdio_vectortile()
