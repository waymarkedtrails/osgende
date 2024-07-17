# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Osgende.
# Copyright (C) 2024 Sarah Hoffmann
from itertools import count

import pytest

from shapely.geometry import LineString, MultiLineString

import osgende.common.build_geometry as builder

@pytest.fixture
def build(monkeypatch):

    def _build_func(*members):
        rel_members = []
        geoms = { 'W' : {}, 'R' : {} }
        ids = { 'W' : count(1), 'R' : count(1) }
        for member in members:
            typ = member[0]
            if member[1] == '[':
                eoid = member.find(']')
                oid = member[2:eoid]
                nodes = member[eoid + 1:]
            else:
                oid = next(ids[typ])
                nodes = member[1:]

            rel_members.append({'id' : oid, 'type' : typ, 'role' : ''})

            nodes = [[(0.0, float(x)*0.0001) for x in s.split(',')]
                     for s in nodes.split(' ')]

            if len(nodes) == 1:
                geoms[typ][oid] = builder._MultiLine(LineString(nodes[0]))
            else:
                geoms[typ][oid] = builder._MultiLine(MultiLineString(nodes))

        monkeypatch.setattr(builder, '_get_member_geometries',
                            lambda *a, **p: geoms)
        return builder.build_route_geometry(None, rel_members, None, None)

    return _build_func

def check_geom(expected, result):
    lines = expected.split()
    if len(lines) == 1:
        assert 'LineString' == result.geom_type
        coords = [(0.0, float(c)*0.0001) for c in lines[0].split(',')]
        assert coords == list(result.coords)
    else:
        assert 'MultiLineString' == result.geom_type
        assert len(lines) == len(result.geoms)
        for exp, res in zip(lines, result.geoms):
            coords = [(0.0, float(c)*0.0001) for c in exp.split(',')]
            assert coords == list(res.coords)

def test_one_way(build):
    check_geom('1,2,3', build('W1,2,3'))

def test_two_joint_ways(build):
    check_geom('1,2,3', build('W1,2', 'W2,3'))
    check_geom('1,2,3', build('W2,1', 'W2,3'))
    check_geom('1,2,3', build('W1,2', 'W3,2'))
    check_geom('1,2,3', build('W2,1', 'W3,2'))

def test_two_separate_ways(build):
    check_geom('1,2 3,4', build('W1,2', 'W3,4'))
    check_geom('1,2 3,4', build('W1,2', 'W4,3'))
    check_geom('1,2 3,4', build('W2,1', 'W3,4'))
    check_geom('1,2 3,4', build('W2,1', 'W4,3'))

def test_one_separate_two_joint_ways(build):
    check_geom('1,2 4,3,5', build('W1,2', 'W4,3', 'W3,5'))
    check_geom('1,2 4,3,5', build('W1,2', 'W3,4', 'W3,5'))
    check_geom('1,2 4,3,5', build('W1,2', 'W4,3', 'W5,3'))
    check_geom('1,2 4,3,5', build('W1,2', 'W3,4', 'W5,3'))

def test_three_joint_ways(build):
    check_geom('1,2,3,4', build('W2,1', 'W2,3', 'W3,4'))
    check_geom('1,2,3,4', build('W2,1', 'W3,2', 'W3,4'))

def test_simple_relation(build):
    check_geom('3,4 1,2', build('R3,4 1,2'))

def test_two_joint_relations(build):
    check_geom('1,2 3,4,5 7,8', build('R1,2 3,4', 'R4,5 7,8'))
    check_geom('1,2 3,4,5 7,8', build('R1,2 3,4', 'R8,7 5,4'))
    check_geom('1,2 3,4,5 7,8', build('R4,3 2,1', 'R4,5 7,8'))
    check_geom('1,2 3,4,5 7,8', build('R4,3 2,1', 'R8,7 5,4'))

def test_two_separate_relations(build):
    check_geom('1,2 3,4 5,6 7,8', build('R1,2 3,4', 'R5,6 7,8'))
    check_geom('1,2 3,4 5,6 7,8', build('R1,2 3,4', 'R8,7 6,5'))
    check_geom('1,2 3,4 5,6 7,8', build('R4,3 2,1', 'R5,6 7,8'))
    check_geom('1,2 3,4 5,6 7,8', build('R4,3 2,1', 'R8,7 6,5'))

def test_circular_with_end(build):
    check_geom('1,2,3,4,5,2,1', build('W[100]1,2', 'W2,3,4', 'W2,5,4', 'W[100]1,2'))
