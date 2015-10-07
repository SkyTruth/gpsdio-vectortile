#!/usr/bin/env python


"""
Core components for gpsdio_vectortile
"""

import quad_tree
import click


@click.command(name='vectortile-generate-tree')
@click.argument("infile", metavar="INFILENAME")
@click.pass_context
def gpsdio_vectortile_generate_tree(ctx, infile):
    tree = quad_tree.Quadtree(infile)
    tree.root.generate_tree()
    tree.save()


@click.command(name='vectortile-generate-tiles')
@click.pass_context
def gpsdio_vectortile_generate_tiles(ctx):
    tree = quad_tree.Quadtree.load()
    tree.root.generate_tiles()
    tree.save()

@click.command(name='vectortile-generate-headers')
@click.pass_context
def gpsdio_vectortile_generate_headers(ctx):
    tree = quad_tree.Quadtree.load()
    tree.generate_header()
    tree.generate_workspace()

if __name__ == '__main__':
    gpsdio_vectortile()
