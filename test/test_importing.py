# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende
# Copyright (C) 2026 thaussma

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import URL

from osgende.tools.importing import BaseImportManager
from osgende.common.sqlalchemy.database import database_drop

DB_NAME = 'osgende_test'


def test_grant_read_access_without_replication():
    """Test that grant_read_access works even when no replication URL is set.
    
    This verifies that StatusManager is always initialized, ensuring the
    status table exists regardless of replication configuration.
    """
    database_drop(DB_NAME, True)
    
    with BaseImportManager(DB_NAME) as mgr:
        mgr.create_database()
        mgr.grant_read_access('postgres')
        
    engine = create_engine(URL.create('postgresql', database=DB_NAME))
    insp = inspect(engine)
    assert 'status' in insp.get_table_names()
    engine.dispose()
