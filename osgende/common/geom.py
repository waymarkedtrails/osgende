# This file is part of Lonvia's Hiking Map
# Copyright (C) 2010 Sarah Hoffmann
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

""" Geometry objects.
"""

class Bbox:
    """A simple bounding box.

       It can be constructed from a single point, two points or
       a point and another bounding box.
    """

    def __init__(self, pt1, pt2 = None):
        if isinstance(pt1,Bbox):
            self.xmin = pt1.xmin
            self.xmax = pt1.xmax
            self.ymin = pt1.ymin
            self.ymax = pt1.ymax
        else:
            self.xmin = pt1[0]
            self.xmax = pt1[0]
            self.ymin = pt1[1]
            self.ymax = pt1[1]
        if pt2 is not None:
            self.xmin = min(self.xmin, pt2[0])
            self.xmax = max(self.xmax, pt2[0])
            self.ymin = min(self.ymin, pt2[1])
            self.ymax = max(self.ymax, pt2[1])

    def intersects(self, other):
        return not (self.xmax < other.xmin or other.xmax < self.xmin or
                     self.ymax < other.ymin or other.ymax < self.ymin)


    def __repr__(self):
        return "BBOX(%.5f,%.5f,%.5f,%.5f)" % (self.xmin,self.xmax,self.ymin,self.ymax)



class FusableWay:
    """A segment made up of one or multiple OSM ways. It remembers the ways
       and nodes involved.

       It can be fused with other fusable ways, thus creating lines
       that span multiple OSM ways.
    """
    def __init__(self,way, nodes):
        self.ways = [ way ]
        self.nodes = nodes

    def __repr__(self):
         return self.ways.__str__()+":"+self.nodes.__str__()

    def first(self):
        """Return ID of first node on way."""
        return self.nodes[0]

    def last(self):
        """Return ID of last node on way."""
        return self.nodes[-1]

    def is_closed(self):
        """A way is considered closed when first and last nodes are the same.
        """
        return self.nodes[0] == self.nodes[-1]

    def fuse(self, other, node):
        """Fuse this way with the way 'other'on the end of node 'node'.
           It returns the now open end of the 'other'way.
        
           Note that the direction of the fused way is arbitrary.
        """
        #print "Fusing",self,"and",other,"on",node
        assert(self != other)
        if other.nodes[0] == node:
            if self.nodes[0] == node:
                self.nodes.reverse()
                self.ways.reverse()
            if self.nodes == other.nodes:
                # the way is reversing back on itself
                # throw away the other part
                return
            self.nodes[-1:] = other.nodes
            if self.ways[-1] == other.ways[0]:
                self.ways[-1:] = other.ways
            else:
                self.ways.extend(other.ways)
            return other.nodes[-1]
        else:
            if self.nodes[-1] == node:
                self.nodes.reverse()
                self.ways.reverse()
            if self.nodes == other.nodes:
                # the way is reversing back on itself
                # throw away the other part
                return
            self.nodes[:1] = other.nodes
            if self.ways[0] == other.ways[-1]:
               self.ways[:1] = other.ways
            else:
               self.ways[:0] = other.ways
            return other.nodes[0]
            

    def append_at(self, node, other, othernode):
        """Append the way 'other' at the side given by node.
           self and other must not overlap at the ends.
        """
        if self.nodes[0] == node:
            self.nodes.reverse()
            self.ways.reverse()
        else:
            assert self.nodes[-1] == node
        if other.nodes[-1] == othernode:
            # XXX yuck in place
            other.nodes.reverse()
        self.nodes.extend(other.nodes)
        self.ways.extend(other.ways)
        return self.nodes[-1]

