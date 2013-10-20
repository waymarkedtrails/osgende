
# Copyright (C) 2010-11 Sarah Hoffmann
#               2012-13 Michael Spreng
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

from osgende.common.postgisconn import PGTable

class JoinedWays(PGTable):
    """
    Table for ways that belong together:
     - they have the same atributes in mastertable
     - they share at least one node
    """

    def __init__(self, db, mastertable, name="joined_ways"):
        PGTable.__init__(self, db, name)

        self.master_table = mastertable

    def create(self):
        """(Re)create a new, empty joined ways table.
        """
        self.layout((
                    ('virtual_id', 'bigint'),
                    ('child',  'bigint')
                   ))
        self.db.query("""CREATE INDEX %s_child_idx ON %s (child)""" % (self._table.table, self.table))
        self.db.query("""DROP SEQUENCE IF EXISTS %s_vid""" % (self.table,))
        self.db.query("""CREATE SEQUENCE %s_vid""" % (self.table,))

    def construct(self):
        """Fill the table from the current master table.
        """
        self.truncate()

        # the worker threads
        workers = self.create_worker_queue(self._process_next)

        cur = self.db.select("""SELECT id FROM %s""" % (self.master_table.fullname,))

        for obj in cur:
            workers.add_task(obj)

        workers.finish()

    def _get_all_adjacent_way_ids(self, wid, properties):
        """
            finds all ways adjacent to the one given in wid iteratively
            (sharing at least one node and all properties)
            It checks for directly adjacent ways and repeats this procedure
            for all found ways until all indirectly adjacent ways are found
        """
        unchecked_ways = [wid]
        all_adjacent_ways = set(unchecked_ways)
        ways_not_adjacent = set()

        unchecked_ways += self._get_adjacent_way_ids(wid, properties, all_adjacent_ways, ways_not_adjacent)

        while unchecked_ways:
            #print unchecked_ways
            unchecked_ways += self._get_adjacent_way_ids(unchecked_ways.pop(), properties,
                    all_adjacent_ways, ways_not_adjacent)

        return all_adjacent_ways

    def _get_adjacent_way_ids(self, wid, properties, known_adjacent_ways, ways_not_adjacent):
        """
            finds all directly adjacent ways to wid
            (sharing at least one node and all properties)
            It fist requests all geometrically overlapping ways (assuming
            there is a geometry index which makes this operation fast) and
            then checks those candidates for common nodes and properties
        """
        intersecting_ways = self.db.select("""WITH refway AS (SELECT geom AS ref FROM %s WHERE id = %s)
                                SELECT * FROM refway, %s WHERE ST_Intersects(geom,ref)"""
                % (self.master_table.fullname, wid, self.master_table.fullname))

        new_ways = []

        for candidate in intersecting_ways:
            cid = candidate['id']
            if cid in known_adjacent_ways or cid in ways_not_adjacent:
                continue

            same_tags = True

            if 'name' not in properties:
                if candidate['name'][0] != '(' or candidate['name'][-1] != ')':
                    same_tags = False

            for k,v in properties.items():
                if candidate[k] != v:
                    same_tags = False

            if same_tags:
                node_in_common = self.db.select_one("""SELECT 1 AS res FROM ways WHERE id = %d AND
                                                       nodes && (SELECT nodes FROM ways WHERE id = %d)
                                                """ % (wid, cid), cur = self.thread.cursor)
                if node_in_common:
                    known_adjacent_ways.update([cid])
                    new_ways += [cid]
                else:
                    ways_not_adjacent.update([cid])
            else:
                ways_not_adjacent.update([cid])

        return new_ways

    def _process_next(self, obj):
        cur = self.thread.cursor
        wid=obj['id']

        # check if way already has a virtual_id
        vid = self.db.select_one("""SELECT virtual_id FROM %s WHERE child = %s"""
                % (self.table, wid), cur = cur)

        # if it has an id, it is already done. Otherwise do search
        if vid == None:

            row=self.db.select_row("""SELECT * FROM %s WHERE id = %s"""
                    % (self.master_table.fullname,wid), cur = cur)

            properties = dict()
            for col,val in zip(cur.description, row):
                properties[col[0]] = val

            del properties['id']
            del properties['geom']
            if properties['name'][0] == '(' and properties['name'][-1] == ')':
                del properties['name']

            merge_list = self._get_all_adjacent_way_ids(wid, properties)

            # only insert ways thet has neighbours
            if len(merge_list) > 1:
                vid = self.db.select_one("""SELECT nextval('%s_vid')"""
                        % (self.table,), cur = cur)

                print "wid=", wid, "vid=", vid
                print merge_list

                for i in merge_list:
                    line = {'virtual_id' : vid,
                            'child'      : i}
                    self.insert_values(line, cur)


