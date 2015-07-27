# This file is part of Osgende
# Copyright (C) 2015 Sarah Hoffmann
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
Various classes that provide connections between processed tables.
"""

from sqlalchemy import String, Table, Column

class TableSource:
    """ Describes a source for another table.
    """

    def __init__(self, data_table, change_table=None,
                 id_column=None):
        """ Create a new table source. `data_table` must be an SQLAlchemy table
            with the data, The optional `change_table` may either be an
            SQLAlchemy table with id and action column or it may be a string
            and a table with that name will be created using the same MetaObject
            as the data_table. If no `change table is given, then it is
            assumed that a complete wipe-out/recreation is expected on update.
        """
        self.data = data_table

        if id_column is None:
            self.id_column = self.data.c.id
        else:
            self.id_column = id_column

        if change_table is None:
            self.change = None
        elif isinstance(change_table, str):
            self.change = Table(change_table, data_table.metadata,
                                self.id_column.copy(),
                                Column('action', String(1))
                               )
        else:
            self.change = change_table
