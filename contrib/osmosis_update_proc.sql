DROP TABLE IF EXISTS node_changeset;
DROP TABLE IF EXISTS way_changeset;
DROP TABLE IF EXISTS relation_changeset;

CREATE TABLE node_changeset (id bigint, action char, tags hstore, geom geometry);
CREATE TABLE way_changeset (id bigint, action char);
CREATE TABLE relation_changeset (id bigint, action char);

CREATE OR REPLACE FUNCTION osmosisUpdate() RETURNS void AS $$
  INSERT INTO node_changeset SELECT a.id,a.action,n.tags,n.geom FROM actions a, nodes n 
               WHERE (data_type = 'N') and a.id = n.id;
  INSERT INTO way_changeset SELECT id,action FROM actions 
               WHERE (data_type = 'W');
  INSERT INTO relation_changeset SELECT id,action FROM actions 
               WHERE (data_type = 'R');
  
$$ LANGUAGE SQL;
