# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende
# Copyright (C) 2024 Sarah Hoffmann
import os

import pytest

import sqlalchemy as sa
from datetime import datetime, timezone
from sqlalchemy.engine.url import URL

from osgende.common.status import StatusManager

@pytest.fixture
def test_conn():
    assert 0 == os.system('dropdb --if-exists osgende_test')
    assert 0 == os.system('createdb osgende_test')

    dba = URL.create('postgresql', database='osgende_test')
    engine = sa.create_engine(dba)
    with engine.begin() as conn:
        yield conn
    engine.dispose()

def test_set_base(test_conn):
    status = StatusManager(sa.MetaData())

    status.create(test_conn)

    for i in range(2):
        date = datetime.now(timezone.utc)
        status.set_status(test_conn, 'base', date, i)

        assert date == status.get_date(test_conn)
        assert i == status.get_sequence(test_conn)

    assert 1 == status.get_min_sequence(test_conn)

def test_set_subtables(test_conn):
    status = StatusManager(sa.MetaData())

    status.create(test_conn)
    date = datetime.now(timezone.utc)
    status.set_status(test_conn, 'base', date, 123)
    status.set_status_from(test_conn, 'next', 'base')

    assert date == status.get_date(test_conn, 'next')
    assert 123 == status.get_sequence(test_conn, 'next')

def test_remove(test_conn):
    status = StatusManager(sa.MetaData())

    status.create(test_conn)
    date = datetime.now(timezone.utc)
    status.set_status(test_conn, 'base', date, 123)

    assert 123 == status.get_sequence(test_conn)

    status.remove_status(test_conn, 'base')
    assert status.get_sequence(test_conn) is None
    assert status.get_date(test_conn) is None
