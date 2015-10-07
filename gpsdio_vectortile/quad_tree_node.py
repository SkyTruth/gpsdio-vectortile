import vectortile
import datetime
import os.path
import cluster as cluster_mod
import utils

class QuadtreeNode(object):
    def __init__(self, tree, bounds = vectortile.TileBounds(), count = 0, colsByName = None):
        self.tree = tree
        self.bounds = bounds
        self.bbox = self.bounds.get_bbox()
        self.count = count
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
        with utils.msgpack_open(self.info_filename, "w") as f:
            f.write({"bounds": str(self.bounds),
                     "count": self.count,
                     "colsByName": self.colsByName
                     })
        if self.children is not None:
            for child in self.children:
                child.save()

    def load(self):
        with utils.msgpack_open(self.info_filename) as f:
            info = f.next()
            self.count = info['count']
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

        with utils.msgpack_open(self.source_filename) as f:
            with utils.msgpack_open(self.children[0].source_filename, "w") as self.children[0].file:
                with utils.msgpack_open(self.children[1].source_filename, "w") as self.children[1].file:
                    with utils.msgpack_open(self.children[2].source_filename, "w") as self.children[2].file:
                        with utils.msgpack_open(self.children[3].source_filename, "w") as self.children[3].file:
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
            if key not in self.colsByName:
                self.colsByName[key] = {"min": value, "max": value}
            else:
                if value < self.colsByName[key]['min']:
                    self.colsByName[key]['min'] = value
                if value > self.colsByName[key]['max']:
                    self.colsByName[key]['max'] = value
        return row

    def write_tile(self, clusters):
        with utils.msgpack_open(self.cluster_filename, "w") as f:
            for cluster in clusters:
                f.write(cluster.get_cluster_row())

        with open(self.tile_filename, "w") as f:
            data = [self.update_colsByName(self.tree.map_row(cluster.get_row()))
                    for cluster in clusters]
            f.write(str(
                    vectortile.Tile.fromdata(
                        data,
                        {"colsByName": self.colsByName})))

    def generate_tile_from_source(self):
        with utils.msgpack_open(self.source_filename) as f:
            rows = list(f)

        self.write_tile([cluster_mod.Cluster.from_row(row) for row in rows])

    def generate_tile_from_child_tiles(self):
        clusters = {}
        for child in self.children:
            with utils.msgpack_open(child.cluster_filename) as f:
                for row in f:
                    cluster = cluster_mod.Cluster.from_cluster_row(row)
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
