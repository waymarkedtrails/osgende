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

from optparse import OptionParser

import os
from math import pi,cos,sin,log,exp,atan
from datetime import datetime

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
    """Generates tiles on Google format in a top-down way.
     
       It will start at the lowest zoomlevel, render a tile, then its 
       subtiles and so on until the highest zoomlevel. Then it proceeds 
       to the next tile. The rendering process can be influences with 
       two query strings. 'changequery' should capture all data that has 
       been changed, 'dataquery' should return all renderable data.

       'changequery' determines if a tile is rendered at all. If no data 
       is returned by this query, the tile is skiped and so are all its 
       subtiles.

       'dataquery' is only necessary in the update process in order to 
       delete tiles that no longer contain any data. If 'changequery' 
       determined that a tile has been changed, but 'dataquery'yields no 
       data, then the tile is deleted.
       
       Both query must contain a '%s' placeholder for the BBOX of the tile. 
       The result of the query is not inspected, it is only checked, if any 
       data is returned. Therefore it is advisable to add a 'LIMIT 1' to the 
       query to spped up the process.
   """

    def __init__(self, dba, minversion=501, dataquery=None, changequery=None):
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

    def _create_zoom_dirs(self, zfrom, zto):
        for z in range(zfrom, zto):
            zoomdir = os.path.join(self.tiledir, "%d" % z)
            if not os.path.isdir(zoomdir):
                os.mkdir(zoomdir)

    def _render_tile(self, x, y, zoom, maxzoom):
        if zoom < 10:
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

        # is there somethign on the tile?
        hasdata = True
        if self.dataquery is not None:
            self.cursor.execute(self.dataquery % params)
            if self.cursor.fetchone() is None:
                hasdata = False
              
        tdir = os.path.join(self.tiledir, "%d" % zoom, "%d" % x)
        if not os.path.isdir(tdir):
            os.mkdir(tdir)
        tile_url = os.path.join(tdir, "%d.png" % y)
        if hasdata:
            bbox = mapnik.Box2d(c0.x, c0.y, c1.x, c1.y)
            self.map.zoom_to_box(bbox)

            im = mapnik.Image(256, 256)
            mapnik.render(self.map, im)
            im.save(tile_url, 'png256')
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

        self._create_zoom_dirs(zrange[0], zrange[1])

        self.map = mapnik.Map(256, 256)
        mapnik.load_map(self.map, stylefile)

        self.projection = mapnik.Projection(self.map.srs)

        self.cursor = self.conn.cursor()
        for x in range(xrange[0], xrange[1]):
            for y in range(yrange[0], yrange[1]):
                self._render_tile(x,y, zrange[0], zrange[1]-1)
        self.cursor.close()

def make_table_query(table):
    if table is None:
        return None
    else:
        return """SELECT 'a' FROM %s WHERE ST_Intersects(geom, %%s) LIMIT 1""" % table

if __name__ == '__main__':
    # fun with command line options
    parser = OptionParser(description=__doc__,
                          usage='%prog [options] <stylefile> <tiledir>')
    parser.add_option('-d', action='store', dest='database', default='planet',
                       help='name of database')
    parser.add_option('-u', action='store', dest='username', default='osm',
                       help='database user')
    parser.add_option('-p', action='store', dest='password', default='',
                       help='password for database')
    parser.add_option('-z', action='store', dest='zoom', default='0-16',
                       help='zoom levels to create tiles for')
    parser.add_option('-t', action='store', dest='tiles', default='',
                       help='tile to render on lowest zoomlevel(x,y)')
    parser.add_option('-q', action='store', dest='datatable', default=None,
                       help='table to query for existing objects (column is always geom)')
    parser.add_option('-c', action='store', dest='changetable', default=None,
                       help='table to query for updated objects (column is always geom)')

    (options, args) = parser.parse_args()

    if len(args) != 2:
        parser.print_help()
        exit(-1)

    zoom = [int(x) for x in options.zoom.split('-')]
    if len(zoom) != 2 or zoom[0] >= zoom[1]:
        print "Zoom paramter must be of format <minzoom>-<maxzoom>. (you gave: %s)" % zoom
        parser.print_help()
        exit(-1)
    zoom[1] += 1

    maxtilenr = 2**zoom[0]
    if options.tiles:
        tilepos = [int(x) for x in options.tiles.split(',')]
        if len(tilepos) != 2:
            print "Tile parameter must be of format x,y. You gave: %s" % tilepos
            exit(-1)
        x,y = tilepos
        if x >= maxtilenr:
            print "x position of tile exceeds limit (%d)." % maxtilenr
            exit(-1)
        if y >= maxtilenr:
            print "x position of tile exceeds limit (%d)." % maxtilenr
            exit(-1)
        
        box = (zoom, (x,x+1), (y,y+1))
    else:
        box = (zoom, (0,maxtilenr), (0,maxtilenr))
        
        
    dataquery = make_table_query(options.datatable)
    changequery = make_table_query(options.changetable)
    renderer = MapnikOverlayGenerator('user=osm dbname=planet',
                         minversion=701,
                         dataquery=dataquery,
                         changequery=changequery)
    #renderer.render('hiking/styles/default.xml', '/secondary/osm/tiles/nghiking', box)
    renderer.render(args[0], args[1], box)
