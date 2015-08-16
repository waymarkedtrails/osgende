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

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement, Executable


class CreateTableAs(Executable, ClauseElement):
    """ Creates a table from a select query.

        Code courtesy of http://stackoverflow.com/questions/30575111/how-to-create-a-new-table-from-select-statement-in-sqlalchemy
    """
    def __init__(self, name, query, temporary=True):
        self.name = name
        self.query = query
        if temporary:
            self.prefix = 'TEMPORARY'
        else:
            self.prefix = ''


@compiles(CreateTableAs, "postgresql")
def _create_table_as(element, compiler, **kw):
    return "CREATE %s TABLE %s AS %s" % (
        self.prefix, element.name, compiler.process(element.query)
    )
