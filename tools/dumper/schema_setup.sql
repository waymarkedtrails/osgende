-- Database creation script for the simple PostgreSQL schema.
-- Borrowed from osmosis.

-- Drop all tables if they exist.
DROP TABLE IF EXISTS node_changeset;
DROP TABLE IF EXISTS way_changeset;
DROP TABLE IF EXISTS relation_changeset;
DROP TABLE IF EXISTS nodes;
DROP TABLE IF EXISTS ways;
DROP TABLE IF EXISTS relations;
DROP TABLE IF EXISTS relation_members;


-- Create a table for nodes.
CREATE TABLE nodes (
    id bigint NOT NULL,
    tags hstore
);
-- Add a postgis point column holding the location of the node.
SELECT AddGeometryColumn('nodes', 'geom', 4326, 'POINT', 2);


-- Create a table for ways.
CREATE TABLE ways (
    id bigint NOT NULL,
    tags hstore,
    nodes bigint[]
);


-- Create a table for relations.
CREATE TABLE relations (
    id bigint NOT NULL,
    tags hstore
);

-- Create a table for representing relation member relationships.
CREATE TABLE relation_members (
    relation_id bigint NOT NULL,
    member_id bigint NOT NULL,
    member_type character(1) NOT NULL,
    member_role text NOT NULL,
    sequence_id int NOT NULL
);

-- Create tables for changesets.
CREATE TABLE node_changeset (
    id bigint NOT NULL,
    action character(1) NOT NULL,
    tags hstore
);
SELECT AddGeometryColumn('node_changeset', 'geom', 4326, 'POINT', 2);
CREATE TABLE way_changeset (
    id bigint NOT NULL,
    action character(1) NOT NULL,
);
CREATE TABLE relation_changeset (
    id bigint NOT NULL,
    action character(1) NOT NULL,
);


-- Add primary keys to tables.
ALTER TABLE ONLY nodes ADD CONSTRAINT pk_nodes PRIMARY KEY (id);

ALTER TABLE ONLY ways ADD CONSTRAINT pk_ways PRIMARY KEY (id);

ALTER TABLE ONLY relations ADD CONSTRAINT pk_relations PRIMARY KEY (id);

ALTER TABLE ONLY relation_members ADD CONSTRAINT pk_relation_members PRIMARY KEY (relation_id, sequence_id);


-- Add indexes to tables.
--CREATE INDEX idx_nodes_geom ON nodes USING gist (geom);


-- Cluster tables by geographical location.
--CLUSTER nodes USING idx_nodes_geom;


