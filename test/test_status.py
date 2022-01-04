# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2020 Sarah Hoffmann

import unittest
import os
import sqlalchemy as sa
from datetime import datetime, timezone
from sqlalchemy.engine.url import URL

from osgende.common.status import StatusManager

class TestStatusManager(unittest.TestCase):

    def setUp(self):
        self.assertEqual(0, os.system('dropdb --if-exists osgende_test'))
        self.assertEqual(0, os.system('createdb osgende_test'))

        dba = URL.create('postgresql', database='osgende_test')
        self.engine = sa.create_engine(dba)

    def test_set_base(self):
        status = StatusManager(sa.MetaData())

        with self.engine.connect() as conn:
            status.create(conn)

            for i in range(2):
                date = datetime.now(timezone.utc)
                status.set_status(conn, 'base', date, i)

                self.assertEqual(date, status.get_date(conn))
                self.assertEqual(i, status.get_sequence(conn))

            self.assertEqual(1, status.get_min_sequence(conn))

    def test_set_subtables(self):
        status = StatusManager(sa.MetaData())

        with self.engine.connect() as conn:
            status.create(conn)
            date = datetime.now(timezone.utc)
            status.set_status(conn, 'base', date, 123)
            status.set_status_from(conn, 'next', 'base')

            self.assertEqual(date, status.get_date(conn, 'next'))
            self.assertEqual(123, status.get_sequence(conn, 'next'))

    def test_remove(self):
        status = StatusManager(sa.MetaData())

        with self.engine.connect() as conn:
            status.create(conn)
            date = datetime.now(timezone.utc)
            status.set_status(conn, 'base', date, 123)

            self.assertEqual(123, status.get_sequence(conn))

            status.remove_status(conn, 'base')
            self.assertIsNone(status.get_sequence(conn))
            self.assertIsNone(status.get_date(conn))

    def tearDown(self):
        self.engine.dispose()
