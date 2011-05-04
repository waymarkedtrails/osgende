#!/bin/sh
#
# Create an empty Postgis/HStore Database, dropping any previously existing DB.

dbname=$1

dropdb -U osm $dbname
createdb -E UTF8 -O osm $dbname
createlang plpgsql $dbname
psql -U osm -d $dbname -f /usr/share/postgresql/8.4/contrib/_int.sql
psql -U osm -d $dbname -f /usr/share/postgresql/8.4/contrib/postgis-1.5/postgis.sql
psql -U osm -d $dbname -f /usr/share/postgresql/8.4/contrib/postgis-1.5/spatial_ref_sys.sql
psql -U osm -d $dbname -f /usr/share/postgresql/8.4/contrib/hstore.sql
psql -U osm -d $dbname -f /home/suzuki/osm/dev/osmosis/package/script/pgsnapshot_schema_0.6.sql
psql -U osm -d $dbname -f /home/suzuki/osm/dev/osmosis/package/script/pgsnapshot_schema_0.6_action.sql
