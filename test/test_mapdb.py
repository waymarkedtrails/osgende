# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2020 Sarah Hoffmann

import pytest

from osgende.mapdb import _Tables

@pytest.fixture
def tables():
    tables = _Tables()
    tables.add('foo', 23)
    tables.add('bar', 45)
    return tables

def test_get_as_dict(tables):
    assert 23 == tables['foo']
    assert 45 == tables['bar']

def test_get_as_dict_fail(tables):
    with pytest.raises(KeyError):
        tables['baz']

def test_in(tables):
    assert 'foo' in tables
    assert 'nothing' not in tables

def test_get_as_attribute(tables):
    23 == tables.foo
    45 == tables.bar

def test_get_as_attribute_fail(tables):
    with pytest.raises(AttributeError):
        tables.baz

def test_has_attribute(tables):
    assert hasattr(tables, 'foo')
    assert not hasattr(tables, 'foot')

def test_len(tables):
    assert 2 == len(tables)
