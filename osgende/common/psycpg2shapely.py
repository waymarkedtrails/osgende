# This file is part of Osgende
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
#
#
# Modified version of the hooking code of the GeoTypes library.
# Copyright (c) QinetiQ Plc 2003

"""
Initialisation support for hooking the Shapely types into the psycopg typecast machinery.
"""
import sys, traceback,re, struct

import shapely.wkb as wkblib
from shapely.geometry.base import BaseGeometry
import shapely.geometry as sgeom

class _GeometryFactory:
    """
    Private class used as a factory for OpenGID types.

    """
    def __call__(self,s=None,c=None):
        """
        A factory method for creating objects of the correct OpenGIS type.
        """
        if s is None:
            return None

        wkb = s.decode('hex')

        geom = wkblib.loads(wkb)

        # add SRID if available
        (endian,) = struct.unpack('=b', wkb[0])
        fmt = '>i' if endian == 0 else '<i'
        (geomtype,) = struct.unpack(fmt, wkb[1:5])
        if geomtype & 0x20000000:
            geom._crs = struct.unpack(fmt, wkb[5:9])[0]

        return geom


class _GeometryWriter:
    """
    Private class that writes the HEXWKB for Postgis.

    Unfortunately, Shapely does not implement proper SRIDs but Postgis cannot
    do without them. So, define _crs (as a number) before sending a Geometry type to
    Postgis.
    """
    def __init__(self, asis):
        self.asis = asis

    def __call__(self, geom):
        if geom._crs is None:
            raise(RuntimeError, "SRID required. Please define _crs field of geometry.")
        # Hack the SRID information extension of postgis in there
        wkb = geom.wkb
        (endian,) = struct.unpack('=b', wkb[0])
        prefix = '>' if endian == 0 else '<'
        (geomtype,) = struct.unpack(prefix + 'i', wkb[1:5])
        if geomtype & 0x20000000:
            return self.asis("'%s'" % wkb.encode('hex'))
        else:
            wkbhead = struct.pack(prefix + 'bii', endian, geomtype | 0x20000000, geom._crs)
            return self.asis("'%s%s'::geometry" % (wkbhead.encode('hex'), wkb[5:].encode('hex')))

def _getPostgisVersion(conn,curs):
    """returns the postgis version as (major,minor,patch)"""
    curs.execute("select postgis_full_version()")    
    m = re.compile('POSTGIS="([^"]*)"').match(curs.fetchall()[0][0])
    return m.group(1).split('.')

def _getTypeOid(conn,curs,typename):
    curs.execute("select oid from pg_type where typname='%s'" % (typename,))
    return curs.fetchall()[0][0]

def initialisePsycopgTypes(psycopg_module, connect_string, 
                           psycopg_extensions_module=None):
    """
    Inform psycopg about the Shapely types.
    """
    if int(psycopg_module.__version__[0]) > 1:
        if psycopg_extensions_module == None:
            raise(RuntimeError,
                """
                You are using Psycopg2 but you have not provided the psycopg_extensions_module
                to initialisePsycopgTypes. You need to pass the psycopg2.extensions module
                as the 'psycopg_extensions_module' parameter to initialisePsycopgTypes.
                """)
        connect=psycopg_module.connect
        register_type=psycopg_extensions_module.register_type
        register_adapter=psycopg_extensions_module.register_adapter
        new_type=psycopg_extensions_module.new_type
        asis=psycopg_extensions_module.AsIs
    else:
        connect=psycopg_module.connect
        register_type=psycopg_module.register_type
        register_adapter=psycopg_module.register_adapter
        new_type=psycopg_module.new_type
        asis=psycopg_module.AsIs

    if connect_string is None:
        raise(RuntimeError, "Valid database connection required.")

    conn = connect(connect_string)

    # Start by working out the oids for the standard Postgres geo types
    curs = conn.cursor()

    # check the postgis version number
    (major,minor,patch) = _getPostgisVersion(conn,curs)

    if int(major) < 1:
        raise(RuntimeError, "You will need a PostGIS version 1.x.")

    # sentinals
    geometry_type_oid = -1

    try:
        geometry_type_oid = _getTypeOid(conn,curs,'geometry')
    except:
        # We failed to find a working combination of oids.
        type, value, tb = sys.exc_info()[:3]
        error = ("%s , %s \n" % (type, value))
        for bits in traceback.format_exception(type,value,tb):
            error = error + bits + '\n'
        del tb

        raise (RuntimeError,
              "Failed to get the type oid for the 'geometry' type from the database:\n\n"\
              "                   connection_string = '%s' \n\n"\
              "This is probably because you have not initialised the OpenGIS types\n"\
              "for this database. Look at http://postgis.refractions.net/docs/x83.html\n"\
              "for instructions on how to do this.\n\n"\
              "The actual exception raised was:\n\n"\
              "%s" % (connect_string, error))


    # Register the type factory for the OpenGIS types.
    register_type(new_type((geometry_type_oid,), 'Geometry', _GeometryFactory()))
    register_adapter(sgeom.Point, _GeometryWriter(asis))
    register_adapter(sgeom.Polygon, _GeometryWriter(asis))
    register_adapter(sgeom.MultiPolygon, _GeometryWriter(asis))
    register_adapter(sgeom.LineString, _GeometryWriter(asis))
    register_adapter(sgeom.MultiLineString, _GeometryWriter(asis))


