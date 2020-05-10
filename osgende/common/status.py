# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2020 Sarah Hoffmann

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql.functions import min as sql_min

class StatusManager(object):
    """A class that monitors the import status of the database. It exports
       functions to set and query the last import status of each table.

       The information is saved in a table itself. The name of the table
       may be set with the `name` parameter. The default is `status`.

       `meta` contains the information about the database.
    """

    def __init__(self, meta, name='status'):
        self.table = sa.Table('status', meta,
                              sa.Column('part', sa.String, primary_key=True),
                              sa.Column('date', sa.DateTime(timezone=True)),
                              sa.Column('sequence', sa.Integer)
                             )

    def create(self, engine):
        self.table.create(bind=engine, checkfirst=True)

    def get_sequence(self, conn, part='base'):
        """ Get sequence status of table `part`. Use connection object
            `conn` to query the database.
        """
        return conn.scalar(sa.select([self.table.c.sequence])
                             .where(self.table.c.part == part))

    def get_min_sequence(self, conn):
        """ Get the smallest sequence number of any table.
        """
        return conn.scalar(sa.select([sql_min(self.table.c.sequence)]))

    def set_status_from(self, conn, part, src):
        """ Set the new status of table `part` to the same as table `src`.
            Use the conneciton object `conn` to query the database.
        """
        data = conn.execute(self.table.select().where(self.table.c.part == src))
        data = data.fetchone()

        self.set_status(conn, part, data['date'], data['sequence'])

    def set_status(self, conn, part, date, sequence):
        """ Set a new status of table `part` to date `sate` and sequence id
            `sequence`.
        """
        upsert = insert(self.table).\
                   on_conflict_do_update(index_elements=[self.table.c.part],
                                         set_= { 'date' : sa.text('EXCLUDED.date'),
                                                 'sequence' : sa.text('EXCLUDED.sequence')})

        conn.execute(upsert.values([{'part' : part,
                                     'date' : date,
                                     'sequence' : sequence}]))

    def remove_status(self, conn, part):
        """ Completely remove the status for the given table.
        """
        conn.execute(self.table.delete().where(self.table.c.part==part))
