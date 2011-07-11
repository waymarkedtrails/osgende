# This file is part of Osgende
# Copyright (C) 2010-11 Sarah Hoffmann
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

from common.postgisconn import PGTable

class RelationHierarchy(PGTable):
    """Table describing the relation hierarchies of the OSM relations table.

       'subset' can be used to limit the relations taken into account.
       It must be a SELECT query that yields one row containing relation
       IDs that should be included. If no subset is given only relations
       that actually have sub- or super-relations are included. If subset
       is given all relations given in the subset will appear.

       This table does not support updating because recreation is simply
       faster.
    """

    def __init__(self, db, name="relations_hier", subset=None):
        PGTable.__init__(self, db, name)

        if subset is None:
            self._subset = """SELECT DISTINCT relation_id 
                              FROM relation_members
                              WHERE relation_type = 'R'"""
        else:
            self._subset = subset

    def create(self):
        """(Re)create a new, empty hierarchy table.
        """
        PGTable.create(self, """( parent bigint,
                                   child bigint,
                                   depth int
                         )""")

    def construct(self):
        """Fill the table from the current relation table.
        """
        self.truncate()
        # compute top-level relations
        self.query("""INSERT INTO %s
                          SELECT id as parent, id as child, 1 as depth
                            FROM (%s) as s""" % (self.table, self._subset))

        # recurse till there are no more children
        depth = 1
        todo = True
        while todo:
            # then go through the recursive parts
            print "Recursion",depth
            res = self.select_one("""INSERT INTO %s
                            SELECT h.parent, m.member_id, %s 
                            FROM relation_members m, %s h 
                            WHERE h.depth=%s 
                                  and h.child=m.relation_id 
                                  and m.member_type='R' 
                                  and h.parent <>m.member_id
                                  and m.member_id IN (%s)
                            RETURNING parent
                            """ % (self.table, depth+1, self.table, depth, self._subset))
            todo = (res is not None)
            depth += 1

