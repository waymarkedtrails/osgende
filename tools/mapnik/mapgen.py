# This file is part of Lonvia's Hiking Map
# Copyright (C) 2011 Sarah Hoffmann
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
"""
Map generation using Mapnik.
"""
# Code esentially borrowed from tile generation scrips of openstreetmap.org.
# See: http://trac.openstreetmap.org/browser/applications/rendering/mapnik/generate_tiles.py

from copy import copy
from optparse import OptionParser, Option, OptionValueError

import os
from math import pi,cos,sin,log,exp,atan
from datetime import datetime
from Queue import Queue
import threading

import psycopg2
import mapnik2 as mapnik

DEG_TO_RAD = pi/180
RAD_TO_DEG = 180/pi

class GoogleProjection:
    def __init__(self,levels=18):
        self.Bc = []
        self.Cc = []
        self.zc = []
        self.Ac = []
        c = 256
        for d in range(0,levels):
            e = c/2;
            self.Bc.append(c/360.0)
            self.Cc.append(c/(2 * pi))
            self.zc.append((e,e))
            self.Ac.append(c)
            c *= 2

    def minmax (self, a,b,c):
        a = max(a,b)
        a = min(a,c)
        return a

    def fromLLtoPixel(self,ll,zoom):
         d = self.zc[zoom]
         e = round(d[0] + ll[0] * self.Bc[zoom])
         f = self.minmax(sin(DEG_TO_RAD * ll[1]),-0.9999,0.9999)
         g = round(d[1] + 0.5*log((1+f)/(1-f))*-self.Cc[zoom])
         return (e,g)

    def fromPixelToLL(self,px,zoom):
         e = self.zc[zoom]
         f = (px[0] - e[0])/self.Bc[zoom]
         g = (px[1] - e[1])/-self.Cc[zoom]
         h = RAD_TO_DEG * ( 2 * atan(exp(g)) - 0.5 * pi)
         return (f,h)


class MapnikOverlayGenerator:
    """Generates tiles in Google format in a top-down way.

       It will start at the lowest zoomlevel, render a tile, then its
       subtiles and so on until the highest zoomlevel. Then it proceeds
       to the next tile. The rendering process can be influences with
       two query strings. 'changequery' should capture all data that has
       been changed, 'dataquery' should return all renderable data.

       'changequery' determines if a tile is rendered at all. If no data
       is returned by this query, the tile is skipped and so are all its
       subtiles.

       'dataquery' is only necessary in the update process in order to
       delete tiles that no longer contain any data. If 'changequery'
       determined that a tile has been changed, but 'dataquery' yields no
       data, then the tile is deleted.

       Both queries must contain a '%s' placeholder for the BBOX of the tile.
       The result of the query is not inspected. It is only checked, if any
       data is returned. Therefore it is advisable to add a 'LIMIT 1' to the
       query to speed up the process.

       'numprocesses' changes the number of parallel processes to use.

       If 'tilenumber_rewrite' is True, then for tiles of zoomlevel 10 and
       above an additional intermediate directory in order to avoid having
       too many files per directory. The first three digits make up the
       first level, the remaining digits the second level. For tile numbers
       below 1000, the second part is 'o'.

       With this schema tiles can still served statically with Apache if
       the following rewrite rules are used:

       ..

         RewriteRule ^(.*)/([0-9]{2})/([0-9]?[0-9]?[0-9]?)([0-9]*)/([0-9]?[0-9]?[0-9]?)([0-9]*).png$ /$1/$2/$3/$4/$5/$6.png
         RewriteRule (.*[0-9])//([0-9].*)     $1/o/$2
         RewriteRule (.*)/.png                $1/o.png

       Note: this tile numbering schema will work up to about level 20,
             afterwards another split should be done, which is not yet
             implemented.
   """

    def __init__(self, dba, minversion=501, dataquery=None, changequery=None,
                  numprocesses=1, tilenumber_rewrite=False):
        self.num_threads = numprocesses
        self.tilenumber_rewrite=tilenumber_rewrite
        self.conn = psycopg2.connect(dba)
        if dataquery is None:
            self.dataquery = None
        else:
            self.dataquery = dataquery % "SetSRID('BOX3D(%f %f, %f %f)'::box3d,900913)"
        if changequery is None:
            self.changequery = None
        else:
            self.changequery = changequery % "SetSRID('BOX3D(%f %f, %f %f)'::box3d,900913)"

        try:
          self.mapnik_version = mapnik.mapnik_version()
        except:
          # hrmpf, old version
          self.mapnik_version = 500
        print "Mapnik Version:",self.mapnik_version
        if self.mapnik_version < minversion:
          raise Exception("Mapnik is too old. Need version above %d." % minversion)


    def _get_tile_uri(self, zoom, x, y):
        """Compute the tile URI and create directories as required.
        """
        if self.tilenumber_rewrite and zoom >= 10:
            # split mode
            if x < 1000:
                tdir = os.path.join(self.tiledir, "%d" % zoom, "%d" % x, 'o')
            else:
                xdir = '%d' % x
                tdir = os.path.join(self.tiledir, "%d" % zoom,
                                    xdir[:3], xdir[3:])
            if y < 1000:
                tilefile = 'o.png'
                tdir = os.path.join(tdir, '%d' % y)
            else:
                ydir = '%d.png' % y
                tilefile = ydir[3:]
                tdir = os.path.join(tdir, ydir[:3])

        else:
            # write tile numbers as is
            tdir = os.path.join(self.tiledir, "%d" % zoom, "%d" % x)
            tilefile = '%d.png'% y

        if not os.path.isdir(tdir):
            os.makedirs(tdir)

        return os.path.join(tdir, tilefile)


    def _render_tile(self, x, y, zoom, maxzoom):
        if zoom < 7:
            print "Rendering Zoom",zoom,"tile",x,"/",y,"(",datetime.isoformat(datetime.now()),")"

        # reproject...
        p0 = self.gprojection.fromPixelToLL((x * 256.0, (y+1) * 256.0), zoom)
        p1 = self.gprojection.fromPixelToLL(((x+1) * 256.0, y * 256.0), zoom)

        c0 = self.projection.forward(mapnik.Coord(p0[0],p0[1]))
        c1 = self.projection.forward(mapnik.Coord(p1[0],p1[1]))

        params = (c0.x, c0.y, c1.x, c1.y)

        # is there an update pending?
        if self.changequery is not None:
            self.cursor.execute(self.changequery % params)
            if self.cursor.fetchone() is None:
                return

        # is there something on the tile?
        hasdata = True
        if self.dataquery is not None:
            self.cursor.execute(self.dataquery % params)
            if self.cursor.fetchone() is None:
                hasdata = False

        tile_url = self._get_tile_uri(zoom, x, y)
        if hasdata:
            try:
                self.queue.put((tile_url, c0, c1))
            except KeyboardInterrupt:
                raise SystemExit("Ctrl-c detected, exiting...")
        else:
            try:
                os.remove(tile_url)
            except:
                pass # don't care if that doesn't work


        if zoom < maxzoom:
            self._render_tile(2*x, 2*y, zoom+1, maxzoom)
            self._render_tile(2*x, 2*y+1, zoom+1, maxzoom)
            self._render_tile(2*x+1, 2*y, zoom+1, maxzoom)
            self._render_tile(2*x+1, 2*y+1, zoom+1, maxzoom)




    def render(self, stylefile, outdir, box):
        """
            Render all non-empty tiles in a certain range.
            'stylefile' is the Mapnik XML style file to use, in 'outdir'
            the rendered tiles are stored. 'box' must contain a triple
            of from/to tuples: zoomlevels, tiles in x range, tiles in y range.
            x and y are tile numbers for the highest zoomlevel to be rendered.
            All tuples are Python ranges, i.e. the to value is non-inclusive.
        """
        self.tiledir = outdir
        zrange, xrange, yrange = box

        self.gprojection = GoogleProjection(zrange[1])

        # set up the rendering threads
        print "Using", self.num_threads, "parallel threads."
        self.queue = Queue(4*self.num_threads)
        self.projection = None
        renderers = []
        for i in range(self.num_threads):
            renderer = RenderThread(stylefile, self.queue)
            if self.projection is None:
                self.projection = mapnik.Projection(renderer.map.srs)
            render_thread = threading.Thread(target=renderer.loop)
            render_thread.start()
            renderers.append(render_thread)

        try:
            self.cursor = self.conn.cursor()
            for x in range(xrange[0], xrange[1]):
                for y in range(yrange[0], yrange[1]):
                    self._render_tile(x,y, zrange[0], zrange[1]-1)
            self.cursor.close()
        finally:
            for i in range(self.num_threads):
                self.queue.put(None)
            for r in renderers:
                print "Waiting for thread (",datetime.isoformat(datetime.now()),")"
                r.join()

class RenderThread:

    def __init__(self, stylefile, queue):
        self.tile_queue = queue
        self.map = mapnik.Map(256, 256)
        mapnik.load_map(self.map, stylefile)

    def render_tile(self, request):
        bbox = mapnik.Box2d(request[1].x, request[1].y,
                            request[2].x, request[2].y)
        self.map.zoom_to_box(bbox)

        im = mapnik.Image(256, 256)
        mapnik.render(self.map, im)
        im.save(request[0], 'png256')


    def loop(self):
        while True:
            req = self.tile_queue.get()
            if req is None:
                self.tile_queue.task_done()
                break

            self.render_tile(req)


class MapGenOptions(Option):
    """ Adds two types to the action parser: intrange and inttuple.

        'intrange' expects a range in the form of <from>-<to> and
        returns a Python tuple range(from,to+1).

        'inttuple' epects two comma-separated integers.
    """


    def check_intrange(option, opt, value):
        try:
            pos = value.index('-')
            return (int(value[0:pos]), int(value[pos+1:])+1)
        except:
            raise optparse.OptionValueError(
            "option %s: expect a range, e.g 0-19, got %s" % (opt, value))

    def check_inttuple(option, opt, value):
        try:
            pos = value.index(',')
            return (int(value[0:pos]), int(value[pos+1:]))
        except:
            raise optparse.OptionValueError(
            "option %s: expect a tuple of numbers, e.g 10,19, got %s" % (opt, value))


    TYPES = Option.TYPES + ("intrange","inttuple")
    TYPE_CHECKER = copy(Option.TYPE_CHECKER)
    TYPE_CHECKER["intrange"] = check_intrange
    TYPE_CHECKER["inttuple"] = check_inttuple


def make_table_query(table):
    if table is None:
        return None
    else:
        return """SELECT 'a' FROM %s WHERE ST_Intersects(geom, %%s) LIMIT 1""" % table

if __name__ == '__main__':
    # if Python 2.6+, get us the number of CPUs, otherwise just invent a number
    try:
        import multiprocessing
        numproc = multiprocessing.cpu_count()
    except (ImportError,NotImplementedError):
        numproc = 4

    # fun with command line options
    parser = OptionParser(description=__doc__,
                          option_class=MapGenOptions,
                          usage='%prog [options] <stylefile> <tiledir>')
    parser.add_option('-d', action='store', dest='database', default='planet',
                       help='name of database')
    parser.add_option('-u', action='store', dest='username', default='osm',
                       help='database user')
    parser.add_option('-p', action='store', dest='password', default='',
                       help='password for database')
    parser.add_option('-z', action='store', type='intrange', dest='zoom', default='0-16',
                       help='zoom levels to create tiles for')
    parser.add_option('-t', action='store', type='inttuple', dest='tiles', default=None,
                       help='tile to render on lowest zoomlevel(x,y)')
    parser.add_option('-q', action='store', dest='datatable', default=None,
                       help='table to query for existing objects (column is always geom)')
    parser.add_option('-c', action='store', dest='changetable', default=None,
                       help='table to query for updated objects (column is always geom)')
    parser.add_option('-j', action='store', dest='numprocesses', default=numproc, type='int',
            help='number of parallel processes to use (default: %d)' % numproc)
    parser.add_option('-r', action='store_true', dest='rewrite_tileschema', default=False,
                       help='split tile numbers for high zoom levels')

    (options, args) = parser.parse_args()

    if len(args) != 2:
        parser.print_help()
        exit(-1)

    print options.zoom

    maxtilenr = 2**options.zoom[0]
    if options.tiles is not None:
        x,y = options.tiles
        if x >= maxtilenr:
            print "x position of tile exceeds limit (%d)." % maxtilenr
            exit(-1)
        if y >= maxtilenr:
            print "x position of tile exceeds limit (%d)." % maxtilenr
            exit(-1)

        box = (options.zoom, (x,x+1), (y,y+1))
    else:
        box = (options.zoom, (0,maxtilenr), (0,maxtilenr))

    print box

    dataquery = make_table_query(options.datatable)
    changequery = make_table_query(options.changetable)
    renderer = MapnikOverlayGenerator('user=%s dbname=%s' % (options.username, options.database),
                         minversion=701,
                         dataquery=dataquery,
                         changequery=changequery,
                         numprocesses=options.numprocesses,
                         tilenumber_rewrite=options.rewrite_tileschema)
    renderer.render(args[0], args[1], box)
