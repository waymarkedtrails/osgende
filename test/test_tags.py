# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende.
# Copyright (C) 2024 Sarah Hoffmann
import pytest

from collections import OrderedDict

from osgende.common.tags import TagStore

@pytest.mark.parametrize('keys,result', [(('one', 'two'), 'foo'),
                                         (('two', 'one'), 'bar'),
                                         (('three', 'two'), 'bar')])
def test_firstof_simple(keys, result):
    t = TagStore({'one': 'foo', 'two': 'bar'})

    assert t.firstof(*keys) == result


def test_firstof_default():
    t = TagStore({'one': 'foo', 'two': 'bar'})

    assert t.firstof('three', 'siz', default='baz') == 'baz'


def test_firstof_default_none():
    t = TagStore({'one': 'foo', 'two': 'bar'})

    assert t.firstof('three', 'siz') is None


def test_get_booleans():
    t = TagStore(dict(a='true', b='True', c='no', d='noo', e='yes', f='false'))

    assert t.get_booleans() == dict(a=True, b=True, c=False, e=True, f=False)


def test_get_wikipedia_url_simple():
    t = TagStore(dict(wikipedia='Something'))

    assert t.get_wikipedia_url() == 'https://en.wikipedia.org/wiki/Something'


def test_get_wikipedia_url_lang_in_value():
    t = TagStore(dict(wikipedia='de:Etwas'))

    assert t.get_wikipedia_url() == 'https://de.wikipedia.org/wiki/Etwas'


def test_get_wikipedia_url_special():
    t = TagStore(dict(wikipedia='de:Etw as'))

    assert t.get_wikipedia_url() == 'https://de.wikipedia.org/wiki/Etw%20as'


def test_get_wikipedia_url_lang_in_key():
    t = TagStore({'wikipedia:est': 'Nada'})

    assert t.get_wikipedia_url() == 'https://est.wikipedia.org/wiki/Nada'


@pytest.mark.parametrize('tags', [dict(name='wikipedia'),
                                  dict(wikipedia='http://en.wikipedia.org/wiki/Something'),
                                  dict(wikipedia='xxxx:Entry'),
                                  {'wikipedia:x': 'Nada'},
                                  {'wikipedia:1234': 'Nada'}])
def test_get_wikipedia_invalid(tags):
    assert TagStore(tags).get_wikipedia_url() is None


def test_get_wikipedia_tags_simple():
    t = TagStore(dict(wikipedia='Something'))
    assert t.get_wikipedia_tags() == dict(en='Something')


def test_get_wikipedia_tags_lang_in_value():
    t = TagStore(dict(wikipedia='de:Etwas'))
    assert t.get_wikipedia_tags() == dict(de='Etwas')


def test_get_wikipedia_tags_with_space():
    t = TagStore(dict(wikipedia='de:Etw as'))
    assert t.get_wikipedia_tags() == dict(de='Etw as')


def test_get_wikipedia_tags_lang_in_key():
    t = TagStore({'wikipedia:hun': 'Nada', 'wikipedia:de': 'Nichts'})
    assert t.get_wikipedia_tags() == dict(hun='Nada', de='Nichts')


@pytest.mark.parametrize('tags', [dict(name='wikipedia'),
                                  dict(wikipedia='http://en.wikipedia.org/wiki/Something'),
                                  dict(wikipedia='xxxx:Entry'),
                                  {'wikipedia:x': 'Nada'},
                                  {'wikipedia:1234': 'Nada'}])
def test_get_wikipedia_tags_bad(tags):
    assert TagStore(tags).get_wikipedia_tags() == {}


def test_get_url_multiple():
    t = TagStore({'url' : 'http://foo.bar', 'website' : 'https://foo.bar'})
    assert t.get_url() == 'http://foo.bar'


@pytest.mark.parametrize('value', ('http://google.com', 'https://google.com'))
def test_get_url_allowed(value):
    t = TagStore({'website': value})
    assert t.get_url() == value


def test_get_url_custom_schemas():
    t = TagStore({'website' : 'ftp://google.com'})
    assert t.get_url(schemes=('ftp','http')) == 'ftp://google.com'


def test_get_url_special_chars():
    t = TagStore({'url' : 'http://myspace.de/me\'me"'})
    assert t.get_url() == 'http://myspace.de/me%27me%22'


@pytest.mark.parametrize('url', ('ftp://foo.bar', 'foo.bar'))
def test_get_url_illegal(url):
    t = TagStore({'url' : url})
    assert t.get_url() is None


def test_get_url_custom_key():
    t = TagStore({'image' : 'http://foo.bar', 'url': 'http://not.that'})
    assert t.get_url(keys=['image']) == 'http://foo.bar'


def test_get_url_custom_key_not_there():
    t = TagStore({'url': 'http://not.that'})
    assert t.get_url(keys=['image']) is None


def test_get_length_bad_unit():
    with pytest.raises(ValueError):
        TagStore({}).get_length(unit='xx')


def test_get_length_no_tag():
    t = TagStore({'name' : 'A'})
    assert t.get_length('ele') is None


def test_get_length_bad_value():
    t = TagStore({'dist' : '100xx'})
    assert t.get_length('dist') is None


@pytest.mark.parametrize('inp,outp', [('10', 10), ('10km', 10),
                                      ('10.3 km', 10.3), ('10,3', 10.3),
                                      ('100m', 0.1), ('100 m ', 0.1)])
def test_get_length_valid(inp, outp):
    t = TagStore({'dist': inp})
    assert t.get_length('dist') == outp


@pytest.mark.parametrize('inp,outp', [('10km', 10000), ('453', 453000),
                                      ("1000ft", 304.8), ("1000'", 304.8),
                                      ('889m', 889)])
def test_get_length_convert(inp, outp):
    t = TagStore({'ele': inp})
    assert t.get_length('ele', unit='m') == pytest.approx(outp)


def test_get_length_converted_with_default():
    t = TagStore({'ele': '10'})
    assert t.get_length('ele', unit='m', default='ft') == pytest.approx(3.048)


@pytest.mark.parametrize('tags,out', [(('name', 'B', 'name:en', 'A'), {'name': 'A'}),
                                      (('name:en', 'B', 'name', 'A'), {'name' : 'B'}),
                                      (('name:en', 'B', 'name:de', 'A'), {'name' : 'B'}),
                                      (('name:de', 'A', 'name:en', 'B'), {'name' : 'B'}),
                                      (('name', 'B', 'name:fr', 'A'), {'name' : 'B', 'name:fr' : 'A'})])
def test_make_localized(tags, out):
    transformed = OrderedDict(zip(tags[::2], tags[1::2]))
    assert TagStore.make_localized(transformed, ('en', 'de')) == out


def test_get_prefixed():
    t = TagStore({'name': 'A', 'name:fr': 'B', 'ref': 'C', 'name:XXX': 'D'})
    assert t.get_prefixed('name:') == dict(fr='B', XXX='D')


def test_get_prefixed_no_tags():
    t = TagStore({'name': 'A', 'name:fr': 'B', 'ref': 'C', 'name:XXX': 'D'})
    assert t.get_prefixed('addr:') == {}
