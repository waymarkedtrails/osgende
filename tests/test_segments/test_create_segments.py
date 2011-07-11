# This file is part of Lonvia's Hiking Map
# Copyright (C) 2011 Sarah Hoffmann
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
""" Test for creating the segments table correctly.
"""

import unittest
import segment_tester as st

#
# TEST ON SIMPLE RELATIONS
#
#

class TestSimpleWays(st.CreateSegmentTableTestCase):
    ways = [
        { 'id' : 1, 'nodes' : [ 1, 2, 3 ] },
        { 'id' : 2, 'nodes' : [ 4, 5 ] },
        { 'id' : 3, 'nodes' : [ 5, 6 ] },
        { 'id' : 4, 'nodes' : [ 7, 8 ] },
        { 'id' : 5, 'nodes' : [ 9, 10, 11, 12] }
    ]
    rels = [ st.rhike(1, [1] ), 
             st.rhike(2, [2,3]), 
             st.rhike(3, [4,5])
    ]

    out = [
      { 'nodes' : [1, 2, 3],
        'ways'  : [1],   'rels'  : [1]
      },
      { 'nodes' : [4, 5, 6],
        'ways'  : [2,3], 'rels'  : [2]
      },
      { 'nodes' : [7,8],
        'ways'  : [4],   'rels'  : [3]
      },
      { 'nodes' : [9,10,11,12],
        'ways'  : [5],   'rels'  : [3]
      }
     ]

class TestCircularWay(st.CreateSegmentTableTestCase):

    ways = [
        { 'id' : 1, 'nodes' : [ 1, 2, 3 ] },
        { 'id' : 2, 'nodes' : [ 4, 5 ] },
        { 'id' : 3, 'nodes' : [ 3, 6, 4, 7, 3 ]},
    ]
    rels = [
       st.rhike(1, [1,2,3])
    ]

    out = [
      { 'nodes' : [1, 2, 3],
        'ways'  : [1],   'rels'  : [1]
      },
      { 'nodes' : [4, 5],
        'ways'  : [2],   'rels'  : [1]
      },
      { 'nodes' : [3, 6, 4],
        'ways'  : [3],   'rels'  : [1]
      },
      { 'nodes' : [4, 7, 3],
        'ways'  : [3],   'rels'  : [1]
      },
      ]

class TestCircularWayUnattached(st.CreateSegmentTableTestCase):

    ways = [
        { 'id' : 1, 'nodes' : [ 1, 2, 3 ] },
        { 'id' : 2, 'nodes' : [ 4, 5 ] },
        { 'id' : 3, 'nodes' : [ 6, 8, 7, 1, 6 ]},
    ]
    rels = [
       st.rhike(1, [1,2,3])
    ]

    out = [
      { 'nodes' : [1, 2, 3],
        'ways'  : [1],  'rels'  : [1]
      },
      { 'nodes' : [4, 5],
        'ways'  : [2],  'rels'  : [1]
      },
      { 'nodes' : [1, 6, 8, 7, 1],
        'ways'  : [3],  'rels'  : [1]
      },
      ]



class TestCircularWayUnattachedSplit(st.CreateSegmentTableTestCase):

    ways = [
        { 'id' : 1, 'nodes' : [ 1, 2, 3 ] },
        { 'id' : 2, 'nodes' : [ 4, 5 ] },
        { 'id' : 3, 'nodes' : [ 6, 5, 7, 1, 6 ]},
    ]
    rels = [
       st.rhike(1, [1,2,3])
    ]

    out = [
      { 'nodes' : [1, 2, 3],
        'ways'  : [1],  'rels'  : [1]
      },
      { 'nodes' : [4, 5],
        'ways'  : [2],  'rels'  : [1]
      },
      { 'nodes' : [1, 6, 5],
        'ways'  : [3],  'rels'  : [1]
      },
      { 'nodes' : [5, 7, 1],
        'ways'  : [3],  'rels'  : [1]
      },
      ]

class TestCircularRelation(st.CreateSegmentTableTestCase):

    ways = [
        { 'id' : 1, 'nodes' : [ 1, 2, 3 ] },
        { 'id' : 2, 'nodes' : [ 3, 4, 5 ] },
        { 'id' : 3, 'nodes' : [ 5, 6, 7, 1 ]},
    ]
    rels = [
        st.rhike(1, [1,2,3])
    ]

    out = [
        { 'nodes' : [ 5,6,7,1,2,3,4,5 ] ,
          'ways'  : [ 3,1,2 ],  'rels'  : [1]
        }
    ]
    

class TestYIntersection(st.CreateSegmentTableTestCase):

    ways = [
        { 'id' : 1, 'nodes' : [ 1, 2, 3 ] },
        { 'id' : 2, 'nodes' : [ 1, 4, 5 ] },
        { 'id' : 3, 'nodes' : [ 1, 6 ]},
    ]
    rels = [
      st.rhike(1, [1, 2, 3])
    ]

    out = [
      { 'nodes' : [1, 2, 3],
        'ways'  : [1],  'rels'  : [1]
      },
      { 'nodes' : [1, 4, 5],
        'ways'  : [2],  'rels'  : [1]
      },
      { 'nodes' : [1, 6],
        'ways'  : [3],  'rels'  : [1]
      },
    ]

class TestCrossingIntersection(st.CreateSegmentTableTestCase):

    ways = [
        { 'id' : 1, 'nodes' : [ 1, 2, 3 ] },
        { 'id' : 2, 'nodes' : [ 4, 2, 5 ] },
    ]
    rels = [
      st.rhike(1, [1, 2])
    ]

    out = [
      { 'nodes' : [1, 2],
        'ways'  : [1],  'rels'  : [1]
      },
      { 'nodes' : [2, 3],
        'ways'  : [1],  'rels'  : [1]
      },
      { 'nodes' : [4, 2],
        'ways'  : [2],  'rels'  : [1]
      },
      { 'nodes' : [2, 5],
        'ways'  : [2],  'rels'  : [1]
      },
    ]


#
# TESTS ON TAGGING
#

class TestRelationTypes(st.CreateSegmentTableTestCase):

    ways = [
        { 'id' : 1, 'nodes' : [ 1, 2, 3 ] },
    ]

    rels = [
        { 'id' : 1, 'members' : ['W1'],
          'tags' : { 'type' : 'route' }
        },
        { 'id' : 2, 'members' : ['W1'],
          'tags' : { 'type' : 'route', 'route' : 'hiking' }
        },
        { 'id' : 3, 'members' : ['W1'],
          'tags' : { 'type' : 'route', 'route' : 'walking' }
        },
        { 'id' : 4, 'members' : ['W1'],
          'tags' : { 'type' : 'route', 'route' : 'foot' }
        },
        { 'id' : 5, 'members' : ['W1'],
          'tags' : { 'route' : 'hiking' }
        }
    ]

    out = [
        { 'nodes' : [1,2,3],
          'ways' : [1], "rels" : [2,3,4]
        }
    ]

#
# TEST ON OVERLAPPING RELATIONS
#

class TestFullOverlap(st.CreateSegmentTableTestCase):

    ways = [
        { 'id' : 1, 'nodes' : [ 1, 2 ] },
        { 'id' : 2, 'nodes' : [ 2, 3 ] },
    ]
    rels = [
        st.rhike(1, [ 1, 2]),
        st.rhike(2, [ 2, 1])
    ]

    out = [
        { 'nodes' : [1,2,3],
          'ways' : [1,2], "rels" : [1,2]
        }
    ]


class TestForkingOverlap(st.CreateSegmentTableTestCase):

    ways = [
        { 'id' : 1, 'nodes' : [ 1, 2 ] },
        { 'id' : 2, 'nodes' : [ 2, 3 ] },
        { 'id' : 3, 'nodes' : [ 2, 4 ] },
    ]
    rels = [
        st.rhike(1, [1,2]),
        st.rhike(2, [3,1]),
    ]

    out = [
        { 'nodes' : [1,2],
          'ways' : [1], "rels" : [1,2]
        },
        { 'nodes' : [2,3],
          'ways' : [2], "rels" : [1]
        },
        { 'nodes' : [2,4],
          'ways' : [3], "rels" : [2]
        },
    ]

class TestPartialOverlap(st.CreateSegmentTableTestCase):

    ways = [
        { 'id' : 1, 'nodes' : [ 1, 2 ] },
        { 'id' : 2, 'nodes' : [ 2, 3 ] },
        { 'id' : 3, 'nodes' : [ 3, 4 ] },
    ]
    rels = [
        st.rhike(1, [1,2]),
        st.rhike(2, [2,3])
    ]

    out = [
        { 'nodes' : [1,2],
          'ways' : [1], "rels" : [1]
        },
        { 'nodes' : [2,3],
          'ways' : [2], "rels" : [1,2]
        },
        { 'nodes' : [3,4],
          'ways' : [3], "rels" : [2]
        },
    ]

class TestForkAway(st.CreateSegmentTableTestCase):

    ways = [
        { 'id' : 1, 'nodes' : [ 1, 2 ] },
        { 'id' : 2, 'nodes' : [ 2, 3 ] },
        { 'id' : 3, 'nodes' : [ 2, 4 ] },
    ]
    rels = [
        st.rhike(1, [1,2]),
        st.rhike(2, [3])
    ]

    out = [
        { 'nodes' : [1,2],
          'ways' : [1], "rels" : [1]
        },
        { 'nodes' : [2,3],
          'ways' : [2], "rels" : [1]
        },
        { 'nodes' : [2,4],
          'ways' : [3], "rels" : [2]
        },
    ]


class TestForkAwayInWay(st.CreateSegmentTableTestCase):

    ways = [
        { 'id' : 1, 'nodes' : [ 1, 2, 3 ] },
        { 'id' : 3, 'nodes' : [ 2, 4 ] },
    ]
    rels = [
        st.rhike(1, [1]),
        st.rhike(2, [3])
    ]

    out = [
        { 'nodes' : [1,2],
          'ways' : [1], "rels" : [1]
        },
        { 'nodes' : [2,3],
          'ways' : [1], "rels" : [1]
        },
        { 'nodes' : [2,4],
          'ways' : [3], "rels" : [2]
        },
    ]



if __name__ == '__main__':
    unittest.main()
