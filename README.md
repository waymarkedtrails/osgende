About osgende
-------------

osgende is a Python-framework for creating customized Postgresql databases from
OSM data. It expects the OSM data to be already in a random-accesibly
storage and extracts and updates relevant data from there. Currently, the
only backing storage implemented is a Osmosis-like Postgresql database
with a file-based node-location storage provided by libosmium.

Requirements
------------

Osgende requires Python3. Python2 might work but is not officially supported.

- psycopg >= 2.5.0    http://initd.org/psycopg/

    Python bindings for PostgreSQL.

- Shapely             http://trac.gispython.org/lab/wiki/Shapely

    Python bindings for the geos library.
    (available as Debian package: python-shapely)

- pyosmium            https://github.com/osmcode/pyosmium

    Python bindings for libosmium, needed for the import tool.

- SQLAlchemy >= 1.0.8 http://www.sqlalchemy.org/

    SQL toolkit for python. osgende uses the Core package only.

- GeoAlchemy2         http://geoalchemy-2.readthedocs.org

    Postgis extendsion for SQLAlchemy.

Installation
------------

Osgende can be installed using Python's default setup tools:

    python3 setup.py install

Usage
-----

### Creating Backing Databases

Osgende always needs a backing database that contains a full copy of
the OSM data you like to process. You can use the osgende-import tool
to create such a database. To create a new database and import an
OSM file into the database run:

    osgende-import -d planet -c -i liechtenstein.osm.pbf

It is strongly recommended that you make use of an external node
location file to speed up processing using the `-n` option.

### Creating a custom Database

You need to create your own MapDB and instances of tables. For an
example see the
[waymarked-trails project](https://github.com/lonvia/waymarked-trails-site).
