DROP TABLE IF EXISTS node_changeset;
DROP TABLE IF EXISTS way_changeset;
DROP TABLE IF EXISTS relation_changeset;

CREATE TABLE node_changeset (id bigint, action char);
CREATE TABLE way_changeset (id bigint, action char);
CREATE TABLE relation_changeset (id bigint, action char);

CREATE OR REPLACE FUNCTION osmosisUpdate() RETURNS void AS $$
  INSERT INTO node_changeset SELECT id,action FROM actions 
               WHERE (data_type = 'N');
  INSERT INTO way_changeset SELECT id,action FROM actions 
               WHERE (data_type = 'W');
  -- also mark ways whose nodes have been moved
  INSERT INTO way_changeset 
               SELECT DISTINCT way_id, 'M' FROM way_nodes 
               WHERE node_id IN (SELECT id FROM actions WHERE data_type = 'N')
                 AND way_id NOT IN (SELECT id FROM way_changeset);
  INSERT INTO relation_changeset SELECT id,action FROM actions 
               WHERE (data_type = 'R');
  -- also mark relations whose ways have been changed
  INSERT INTO relation_changeset 
               SELECT DISTINCT relation_id, 'M' FROM relation_members
               WHERE member_type = 'W'
                 AND member_id IN (SELECT id FROM actions WHERE data_type = 'W')
                 AND relation_id NOT IN (SELECT id FROM relation_changeset);
  
$$ LANGUAGE SQL;
