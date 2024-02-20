# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2017-2020 Sarah Hoffmann

"""
Tests for FilteredTable
"""

import pytest
import sqlalchemy as sa

from osgende.generic import FilteredTable

@pytest.fixture(params=[True, False])
def filter_table(request, db):
    table = db.add_table(FilteredTable(db.db.metadata, 'test',
                                       db.db.osmdata.relation,
                                       sa.literal_column("tags ? 'foo'"),
                                       view_only=request.param))

    db.import_data("""\
      r1 Tname=house,foo=bar Mn23@,w4@forward
      r2 Ttype=multipolygon,building=yes Mw2@,w3@,w5@
      """)

    return table

R1_EXPECT = dict(id=1, tags={'foo': 'bar', 'name': 'house'},
                 members= [dict(id=23, type='N', role=''),
                           dict(id=4, type='W', role='forward')])


def test_create(filter_table):
    filter_table.has_data(R1_EXPECT)


def test_update_add(db, filter_table):
    db.update_data("""\
      r10 Ttype=nothing Mw7@,w8@,w9@
      r11 Tfoo=foo,source=gogo Mr11@
      """)

    if filter_table.table.view_only:
        filter_table.has_changes('M11', 'D10')
    else:
        filter_table.has_changes('M11')

    filter_table.has_data(R1_EXPECT,
                          dict(id=11, tags={'foo': 'foo', 'source': 'gogo'},
                               members=[dict(id=11, type='R', role='')]))


def test_update_delete(db, filter_table):
    db.update_data("r1 dD")

    filter_table.has_changes('D1')
    filter_table.has_data()


def test_update_delete_unrelated(db, filter_table):
    db.update_data("r2 dD")

    if filter_table.table.view_only:
        filter_table.has_changes('D2')
    else:
        filter_table.has_changes()
    filter_table.has_data(R1_EXPECT)


def test_update_add_filter_tags(db, filter_table):
    db.update_data("""r2 Tfoo=x,building=yes Mw2@,w3@,w5@""")

    filter_table.has_changes('M2')
    filter_table.has_data(R1_EXPECT,
                          dict(id=2, tags={'foo': 'x', 'building': 'yes'},
                               members=[dict(id=2, type='W', role=''),
                                        dict(id=3, type='W', role=''),
                                        dict(id=5, type='W', role='')]))


def test_update_remove_filter_tags(db, filter_table):
    db.update_data("r1 Tfooo=bar,name=house Mn23@,w4@forward")

    filter_table.has_changes('D1')
    filter_table.has_data()
