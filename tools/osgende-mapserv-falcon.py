# This file is part of Osgende
# Copyright (C) 2020 Sarah Hoffmann
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
Falcon-based tile server for tile databases generated with osgende-mapgen.
Use with uWSGI.
"""

import datetime
import os
import sys
import threading
import hashlib
from math import pi,exp,atan

import falcon
import mapnik

RAD_TO_DEG = 180/pi

class TileProjection:
    def __init__(self,levels=18):
        self.Bc = []
        self.Cc = []
        self.zc = []
        self.Ac = []
        c = 256
        for d in range(0,levels + 1):
            e = c/2;
            self.Bc.append(c/360.0)
            self.Cc.append(c/(2 * pi))
            self.zc.append((e,e))
            self.Ac.append(c)
            c *= 2

    def fromTileToLL(self, zoom, x, y):
         e = self.zc[zoom]
         f = (x*256.0 - e[0])/self.Bc[zoom]
         g = (y*256.0 - e[1])/-self.Cc[zoom]
         h = RAD_TO_DEG * ( 2 * atan(exp(g)) - 0.5 * pi)
         return (f,h)


def mk_tileid(zoom, x, y):
    """Create a unique 64 bit tile ID.
       Works up to zoom level 24."
    """
    return zoom + (x << 5) + (y << (5 + zoom))


class DummyCache(object):
    """ A tile cache that does not remember any tiles. 

        Useful when testing out a new style.
    """
    def __init__(self, config):
        pass

    def get(self, zoom, x, y, fmt):
        return None

    def set(self, zoom, x, y, fmt, image=None):
        pass


class PostgresCache(object):
    """ A cache that saves tiles in postgres.
    """

    def __init__(self, config):
        self.empty = dict()
        for fmt, fname in config['empty_tile'].items():
            with open(fname, 'rb') as myfile:
                self.empty[fmt] = myfile.read()

        self.max_zoom = config.get('max_zoom', 100)
        self.pg = __import__('psycopg2')
        self.dba = config['dba']

        self.cmd_get = "SELECT pixbuf FROM %s WHERE id=%%s" % config['table']
        self.cmd_check = "SELECT count(*) FROM %s WHERE id=%%s" % config['table']
        self.cmd_set = "UPDATE %s SET pixbuf=%%s WHERE id=%%s AND pixbuf is Null" % config['table']
        self.thread_data = threading.local()

    def get_db(self):
        if not hasattr(self.thread_data, 'cache_db'):
            self.thread_data.cache_db = self.pg.connect(self.dba)
            # set into autocommit mode so that tiles still can be
            # read while the db is updated
            self.thread_data.cache_db.autocommit = True
            self.thread_data.cache_db.cursor().execute("SET synchronous_commit TO OFF")

        return self.thread_data.cache_db

    def get(self, zoom, x, y, fmt):
        c = self.get_db().cursor()
        if zoom > self.max_zoom:
            shift = zoom - self.max_zoom
            c.execute(self.cmd_check,
                      (mk_tileid(self.max_zoom, x >> shift, y >> shift), ))
            if c.fetchone()[0]:
                return None
        else:
            c.execute(self.cmd_get, (mk_tileid(zoom, x, y), ))
            if c.rowcount > 0:
                return c.fetchone()[0]

        return self.empty[fmt]

    def set(self, zoom, x, y, fmt, image=None):
        if zoom <= self.max_zoom:
            c = self.get_db().cursor()
            c.execute(self.cmd_set, (image, mk_tileid(zoom, x, y)))

class MapnikRenderer(object):

    def __init__(self, name, config, styleconfig):
        self.name = name
        # defaults
        self.config = dict({ 'formats' : [ 'png' ],
                        'tile_size' : (256, 256),
                        'max_zoom' : 18
                      })
        self.stylecfg = dict()
        # local configuration
        if config is not None:
            self.config.update(config)
        if styleconfig is not None:
            self.stylecfg.update(styleconfig)

        if self.config['source_type'] == 'xml':
            self.create_map = self._create_map_xml
        if self.config['source_type'] == 'python':
            self.python_map =__import__(self.config['source'])
            self.create_map = self._create_map_python

        m = mapnik.Map(*self.config['tile_size'])
        self.create_map(m)

        self.mproj = mapnik.Projection(m.srs)
        self.gproj = TileProjection(self.config['max_zoom'])
        self.thread_data = threading.local()

    def get_map(self):
        self.thread_map()
        return self.thread_data.map

    def thread_map(self):
        if not hasattr(self.thread_data, 'map'):
            m = mapnik.Map(*self.config['tile_size'])
            self.create_map(m)
            self.thread_data.map = m

    def _create_map_xml(self, mapnik_map):
        src = os.path.join(self.config['source'])
        mapnik.load_map(mapnik_map, src)

    def _create_map_python(self, mapnik_map):
        self.python_map.construct_map(mapnik_map, self.stylecfg)

    def split_url(self, zoom, x, y):
        ypt = y.find('.')
        if ypt < 0:
            return None
        tiletype = y[ypt+1:]
        if tiletype not in self.config['formats']:
            return None
        try:
            zoom = int(zoom)
            x = int(x)
            y = int(y[:ypt])
        except ValueError:
            return None

        if zoom > self.config['max_zoom']:
            return None

        return (zoom, x, y, tiletype)

    def render(self, zoom, x, y, fmt):
        p0 = self.gproj.fromTileToLL(zoom, x, y+1)
        p1 = self.gproj.fromTileToLL(zoom, x+1, y)

        c0 = self.mproj.forward(mapnik.Coord(p0[0],p0[1]))
        c1 = self.mproj.forward(mapnik.Coord(p1[0],p1[1]))

        bbox = mapnik.Box2d(c0.x, c0.y, c1.x, c1.y)
        im = mapnik.Image(256, 256)

        m = self.get_map()
        m.zoom_to_box(bbox)
        mapnik.render(m, im)

        return im.tostring('png256')


class TestMap(object):

    DEFAULT_TESTMAP="""\
<!DOCTYPE html>
<html>
<head>
    <title>Testmap - %(style)s</title>
    <link rel="stylesheet" href="%(leaflet_path)s/leaflet.css" />
</head>
<body >
    <div id="map" style="position: absolute; width: 99%%; height: 97%%"></div>

    <script src="%(leaflet_path)s/leaflet.js"></script>
    <script src="%(leaflet_path)s/leaflet-hash.js"></script>
    <script>
        var map = L.map('map').setView([47.3317, 8.5017], 13);
        var hash = new L.Hash(map);

        L.tileLayer('http://a.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            maxZoom: 18,
        }).addTo(map);
        L.tileLayer('%(script_name)s/%(style)s/{z}/{x}/{y}.png', {
            maxZoom: 18,
        }).addTo(map);
    </script>
</body>
</html>
"""

    def __init__(self, style, script):
        self.map_config = {
            'style' : style,
            'script_name' : script,
            'leaflet_path' : os.environ.get('LEAFLET_PATH',
                                            'http://cdn.leafletjs.com/leaflet-0.7.5')
        }

    def on_get(self, req, resp):
        resp.content_type = falcon.MEDIA_HTML
        resp.body = self.DEFAULT_TESTMAP % self.map_config


class TileServer(object):

    def __init__(self, style, config):
        self.cachecfg = dict({ 'type' : 'DummyCache'})
        if 'TILE_CACHE' in config:
            self.cachecfg.update(config['TILE_CACHE'])
        cacheclass = globals()[self.cachecfg['type']]
        self.cache = cacheclass(self.cachecfg)
        self.renderer = MapnikRenderer(style,
                                       config.get('RENDERER'),
                                       config.get('TILE_STYLE'))

    def on_get(self, req, resp, zoom, x, y):
        tile_desc = self.renderer.split_url(zoom, x, y)
        if tile_desc is None:
            raise falcon.HTTPNotFound()

        tile = self.cache.get(*tile_desc)
        if tile is None:
            tile = self.renderer.render(*tile_desc)
            self.cache.set(*tile_desc, image=tile)

        # compute etag
        m = hashlib.md5()
        m.update(tile)
        content_etag = m.hexdigest()

        for etag in (req.if_none_match or []):
            if etag == '*' or etag == content_etag:
                resp.status = falcon.HTTP_304
                return

        resp.content_type = falcon.MEDIA_PNG
        resp.expires = datetime.datetime.utcnow() + datetime.timedelta(hours=3)
        resp.body = tile
        resp.etag = content_etag


def setup_site(app, site, script_name=''):
    try:
        __import__(site)
    except ImportError:
        print("Missing config for site '%s'. Skipping." % site)
        return

    site_cfg = dict()
    for var in dir(sys.modules[site]):
        site_cfg[var] = getattr(sys.modules[site], var)

    basename = site.split('.')[-1]

    print("Setting up site", basename)

    app.add_route('/' + basename + '/test-map', TestMap(basename, script_name))
    app.add_route('/' + basename + '/{zoom}/{x}/{y}', TileServer(basename, site_cfg))


application = falcon.API()

for site in os.environ['TILE_SITES'].split(','):
    setup_site(application, site)
