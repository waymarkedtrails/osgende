--- XXX This function does not necessarily return the points
---     in right order. Needs fixing.
CREATE OR REPLACE FUNCTION osgende_way_geom(bigint) 
    RETURNS geometry AS
      'SELECT ST_MakeLine(n.geom) 
        FROM (select unnest(nodes) from ways w where id = $1) as w, 
             nodes n 
        WHERE w.unnest = n.id;'
    LANGUAGE SQL
    STABLE;


CREATE OR REPLACE FUNCTION osgende_way_geom(ANYARRAY) 
    RETURNS geometry AS
      'SELECT ST_MakeLine(n.geom) 
        FROM (select unnest($1)) as w, 
             nodes n 
        WHERE w.unnest = n.id;'
    LANGUAGE SQL
    STABLE;
