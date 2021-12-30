# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende.
# Copyright (C) 2020 Sarah Hoffmann

import unittest
from unittest.mock import MagicMock
from itertools import count

from shapely.geometry import LineString, MultiLineString

import osgende.common.build_geometry as builder

class TestBuildGeometry(unittest.TestCase):

    def build(self, *members):
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

        builder._get_member_geometries = MagicMock(return_value=geoms)
        return builder.build_route_geometry(None, rel_members, None, None)

    def check_geom(self, expected, result):
        lines = expected.split()
        if len(lines) == 1:
            self.assertEqual('LineString', result.geom_type)
            coords = [(0.0, float(c)*0.0001) for c in lines[0].split(',')]
            self.assertListEqual(coords, list(result.coords))
        else:
            self.assertEqual('MultiLineString', result.geom_type)
            self.assertEqual(len(lines), len(result.geoms))
            for exp, res in zip(lines, result.geoms):
                coords = [(0.0, float(c)*0.0001) for c in exp.split(',')]
                self.assertListEqual(coords, list(res.coords))

    def test_one_way(self):
        self.check_geom('1,2,3', self.build('W1,2,3'))

        self.check_geom('1,2,3', self.build('W1,2', 'W2,3'))
        self.check_geom('1,2,3', self.build('W2,1', 'W2,3'))
        self.check_geom('1,2,3', self.build('W1,2', 'W3,2'))
        self.check_geom('1,2,3', self.build('W2,1', 'W3,2'))

        self.check_geom('1,2 3,4', self.build('W1,2', 'W3,4'))
        self.check_geom('1,2 3,4', self.build('W1,2', 'W4,3'))
        self.check_geom('1,2 3,4', self.build('W2,1', 'W3,4'))
        self.check_geom('1,2 3,4', self.build('W2,1', 'W4,3'))

        self.check_geom('1,2 4,3,5', self.build('W1,2', 'W4,3', 'W3,5'))
        self.check_geom('1,2 4,3,5', self.build('W1,2', 'W3,4', 'W3,5'))
        self.check_geom('1,2 4,3,5', self.build('W1,2', 'W4,3', 'W5,3'))
        self.check_geom('1,2 4,3,5', self.build('W1,2', 'W3,4', 'W5,3'))

        self.check_geom('1,2,3,4', self.build('W2,1', 'W2,3', 'W3,4'))
        self.check_geom('1,2,3,4', self.build('W2,1', 'W3,2', 'W3,4'))

        self.check_geom('3,4 1,2', self.build('R3,4 1,2'))

        self.check_geom('1,2 3,4,5 7,8', self.build('R1,2 3,4', 'R4,5 7,8'))
        self.check_geom('1,2 3,4,5 7,8', self.build('R1,2 3,4', 'R8,7 5,4'))
        self.check_geom('1,2 3,4,5 7,8', self.build('R4,3 2,1', 'R4,5 7,8'))
        self.check_geom('1,2 3,4,5 7,8', self.build('R4,3 2,1', 'R8,7 5,4'))

        self.check_geom('1,2 3,4 5,6 7,8', self.build('R1,2 3,4', 'R5,6 7,8'))
        self.check_geom('1,2 3,4 5,6 7,8', self.build('R1,2 3,4', 'R8,7 6,5'))
        self.check_geom('1,2 3,4 5,6 7,8', self.build('R4,3 2,1', 'R5,6 7,8'))
        self.check_geom('1,2 3,4 5,6 7,8', self.build('R4,3 2,1', 'R8,7 6,5'))

    def test_circular_with_end(self):
        self.check_geom('1,2,3,4,5,2,1',
                        self.build('W[100]1,2', 'W2,3,4', 'W2,5,4', 'W[100]1,2'))

