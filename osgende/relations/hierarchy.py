# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2022 Sarah Hoffmann

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.dialects.postgresql import JSONB

from osgende.common.sqlalchemy import Truncate

class RelationHierarchy:
    """Table describing the relation hierarchies of the OSM relations table.

       'subset' can be used to limit the relations taken into account.
       It must be a SELECT query that yields one row containing relation
       IDs that should be included. If no subset is given only relations
       that actually have sub- or super-relations are included. If subset
       is given all relations given in the subset will appear.

       If `self_ref` is True, then each relation is added as its own child
       with depth = 1.
    """

    def __init__(self, meta, name, source, self_ref=False):
        self.data = sa.Table(name, meta,
                             sa.Column('parent', sa.BigInteger, index=True),
                             sa.Column('child', sa.BigInteger, index=True),
                             sa.Column('depth', sa.Integer)
                            )

        self.src = source
        self.self_reference = self_ref

    @property
    def c(self):
        """ Return the columns of the data table.
        """
        return self.data.c


    def truncate(self, conn):
        conn.execute(Truncate(self.data))

    def create(self, engine):
        self.data.create(bind=engine, checkfirst=True)

    def construct(self, engine):
        """Fill the table from the current relation table.
        """
        with engine.begin() as conn:
            self.truncate(conn)
            # Insert all direct children
            rels = self.src.data.alias('r')
            members = sa.func.jsonb_array_elements(rels.c.members)\
                             .table_valued(sa.column('value', JSONB)).lateral()
            id_bigint = members.c.value['id'].astext.cast(sa.BigInteger)

            sql = sa.select(rels.c.id.label('parent'), id_bigint.label('child'), 2)\
                    .select_from(rels.join(members, onclause=sa.text("True")))\
                    .where(members.c.value['type'].astext == 'R')\
                    .where(id_bigint != rels.c.id.label('parent'))
            # XXX adding 'returning' here because otherwise SQLAlchemy/psycopg3
            # refuses to give us a rowcount,
            # see https://github.com/sqlalchemy/sqlalchemy/issues/10974
            res = conn.execute(self.data.insert().from_select(self.data.c, sql)
                                                 .returning(self.data.c.parent))

            level = 3
            while res.rowcount > 0 and level < 6:
                pd = self.data.alias()
                prev = sa.select(pd.c.parent, pd.c.child).where(pd.c.depth == (level - 1)).alias()
                nd = self.data.alias()
                newly = sa.select(nd.c.parent, nd.c.child).where(nd.c.depth == 2).alias()
                old = self.data.alias()
                subs = sa.select(prev.c.parent, newly.c.child, level)\
                        .where(prev.c.child == newly.c.parent)\
                        .where(prev.c.parent != newly.c.child)\
                        .except_(sa.select(old.c.parent, old.c.child, level))

                res = conn.execute(self.data.insert().from_select(self.data.c, subs)
                                                     .returning(self.data.c.parent))
                level = level + 1

            # Finally add all relations themselves.
            if self.self_reference:
                s = self.src.data
                conn.execute(self.data.insert().from_select(self.data.c,
                    sa.select(s.c.id.label('parent'), s.c.id.label('child'), 1)))

    def update(self, engine):
        """Update the table.

           The table is actually simply reconstructed because that is faster.
        """
        self.construct(engine)
