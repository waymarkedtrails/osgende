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
# SIMPLE UPDATES
#

class TestMoveNode(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1 ])
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]}
    ]

    upnodes = [ 1 ]

    out = [
      { 'nodes' : [-1, 2],
        'ways'  : [1],   'rels'  : [1]
      },
    ]


class TestChangeWayGeometry(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1 ])
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]}
    ]

    upways = [
        {'id' : 1, 'nodes' : [ 1, 3, 2 ]}
    ]

    out = [
      { 'nodes' : [1, 3, 2],
        'ways'  : [1],   'rels'  : [1]
      },
    ]


class TestReplaceWay(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1 ])
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]},
        { 'id' : 2, 'nodes' : [ 3, 4 ]}
    ]

    uprels = [ 
        st.rhike(1, [ 2 ])
    ]

    out = [
      { 'nodes' : [3, 4],
        'ways'  : [2],   'rels'  : [1]
      },
    ]

class TestAddUnrelatedWay(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1 ])
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]}
    ]

    uprels = [ 
      st.rhike(1, [ 1, 2 ]) 
    ]

    upways = [
      {'id' : 2, 'nodes' : [3, 4]}
    ]
    
    out = [
      { 'nodes' : [1, 2],
        'ways'  : [1],   'rels'  : [1]
      },
      { 'nodes' : [3, 4],
        'ways'  : [2],   'rels'  : [1]
      },
    ]

class TestAddAdjoiningWay(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1 ])
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]}
    ]

    uprels = [ 
      st.rhike(1, [ 1, 2 ]) 
    ]

    upways = [
      {'id' : 2, 'nodes' : [2, 3, 4]}
    ]

    out = [
      { 'nodes' : [1, 2, 3, 4],
        'ways'  : [1, 2],   'rels'  : [1]
      },
    ]


class TestAddJoiningWay(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1, 2 ])
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]},
        {'id' : 2, 'nodes' : [ 3, 4 ]}
    ]
    uprels = [ 
      st.rhike(1, [ 1, 2, 3 ]) 
    ]
    upways = [
      {'id' : 3, 'nodes' : [2, 3]}
    ]

    out = [
      { 'nodes' : [1, 2, 3, 4],
        'ways'  : [1, 3, 2],   'rels'  : [1]
      },
    ]


class TestRemoveUnrelatedWay(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1, 2 ])
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]},
        {'id' : 2, 'nodes' : [ 3, 4 ]}
    ]
    uprels = [ 
      st.rhike(1, [ 1 ]) 
    ]
    upways = [
      {'id' : -2}
    ]

    out = [
      { 'nodes' : [1, 2],
        'ways'  : [1],   'rels'  : [1]
      },
    ]


class TestRemoveAdjoiningWay(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1, 2 ])
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]},
        {'id' : 2, 'nodes' : [ 2, 4 ]}
    ]
    uprels = [ 
      st.rhike(1, [ 1 ]) 
    ]
    upways = [
      {'id' : -2}
    ]

    out = [
      { 'nodes' : [1, 2],
        'ways'  : [1],   'rels'  : [1]
      },
    ]


class TestRemoveJoiningWay(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1, 2, 3 ])
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]},
        {'id' : 2, 'nodes' : [ 2, 3 ]},
        {'id' : 3, 'nodes' : [ 3, 4 ]}
    ]
    uprels = [ 
      st.rhike(1, [ 1, 3 ]) 
    ]
    upways = [
      {'id' : -2}
    ]

    out = [
      { 'nodes' : [1, 2],
        'ways'  : [1],   'rels'  : [1]
      },
      { 'nodes' : [3, 4],
        'ways'  : [3],   'rels'  : [1]
      },
    ]

class ChangeSemiSegment(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1, 2 ]),
        st.rhike(2, [ 3, 4 ])
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2, 3 ]},
        {'id' : 2, 'nodes' : [ 3, 4, 5 ]},
        {'id' : 3, 'nodes' : [ 6, 2 ]},
        {'id' : 4, 'nodes' : [ 4, 7 ]},
    ]
    uprels = [ 
      st.rhike(2, [ 3, 4, 1 ]) 
    ]

    out = [
      { 'nodes' : [1, 2],
        'ways'  : [1],   'rels'  : [1,2]
      },
      { 'nodes' : [2, 3],
        'ways'  : [1],   'rels'  : [1,2]
      },
      { 'nodes' : [3, 4],
        'ways'  : [2],   'rels'  : [1]
      },
      { 'nodes' : [4, 5],
        'ways'  : [2],   'rels'  : [1]
      },
      { 'nodes' : [6, 2],
        'ways'  : [3],   'rels'  : [2]
      },
      { 'nodes' : [4, 7],
        'ways'  : [4],   'rels'  : [2]
      },
    ]



#
# TEST WITH MULTIPLE RELATIONS
#

class TestAddRelationFully(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1 ])
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]}
    ]
    uprels = [ 
      st.rhike(2, [ 1 ]) 
    ]
    upways = [
      {'id' : 2, 'nodes' : [3, 4]}
    ]

    out = [
      { 'nodes' : [1, 2],
        'ways'  : [1],   'rels'  : [1,2]
      },
    ]


class TestAddRelationPartial(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1, 2 ])
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]},
        {'id' : 2, 'nodes' : [3, 4]}
    ]
    uprels = [ 
      st.rhike(2, [ 1 ]) 
    ]

    out = [
      { 'nodes' : [1, 2],
        'ways'  : [1],   'rels'  : [1,2]
      },
      { 'nodes' : [3, 4],
        'ways'  : [2],   'rels'  : [1]
      },
    ]



class TestAddRelationPartialJoining(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1, 2 ])
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]},
        {'id' : 2, 'nodes' : [ 2, 3 ]}
    ]
    uprels = [ 
      st.rhike(2, [ 1 ]) 
    ]

    out = [
      { 'nodes' : [1, 2],
        'ways'  : [1],   'rels'  : [1,2]
      },
      { 'nodes' : [2, 3],
        'ways'  : [2],   'rels'  : [1]
      },
    ]
    

class TestAddRelationPartialCompleting(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1, 2 ]),
        st.rhike(2, [ 1 ]) 
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]},
        {'id' : 2, 'nodes' : [ 2, 3 ]}
    ]
    uprels = [ 
      st.rhike(2, [ 1, 2 ]) 
    ]

    out = [
      { 'nodes' : [1, 2, 3],
        'ways'  : [1, 2],   'rels'  : [1,2]
      },
      ]

class TestAddYIntersectionOther(st.UpdateSegmentTableTestCase):
    
    rels = [
       st.rhike(1, [ 1, 2 ]),
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]},
        {'id' : 2, 'nodes' : [ 2, 3 ]}
    ]
    uprels = [ 
      st.rhike(2, [ 3 ]) 
    ]
    upways = [
      { 'id' : 3, 'nodes' : [ 4, 2 ] }
    ]

    out = [
      { 'nodes' : [1, 2],
        'ways'  : [1],   'rels'  : [1]
      },
      { 'nodes' : [2, 3],
        'ways'  : [2],   'rels'  : [1]
      },
      { 'nodes' : [2, 4],
        'ways'  : [3],   'rels'  : [2]
      },
    ]

class TestAddCrossingOther(st.UpdateSegmentTableTestCase):
    
    rels = [
       st.rhike(1, [ 1 ]),
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2, 3 ]},
    ]
    uprels = [ 
      st.rhike(2, [ 2 ]) 
    ]
    upways = [
      { 'id' : 2, 'nodes' : [ 4, 2, 5 ] }
    ]

    out = [
      { 'nodes' : [1, 2],
        'ways'  : [1],   'rels'  : [1]
      },
      { 'nodes' : [2, 3],
        'ways'  : [1],   'rels'  : [1]
      },
      { 'nodes' : [2, 4],
        'ways'  : [2],   'rels'  : [2]
      },
      { 'nodes' : [2, 5],
        'ways'  : [2],   'rels'  : [2]
      },
    ]

class TestExtendCrossingOther(st.UpdateSegmentTableTestCase):
    
    rels = [
       st.rhike(1, [ 1 ]),
       st.rhike(2, [ 2 ]),
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2, 3 ]},
        {'id' : 2, 'nodes' : [ 4, 5, 6 ]},
    ]
    upways = [
      { 'id' : 2, 'nodes' : [ 4, 5, 6, 2, 7 ] }
    ]

    out = [
      { 'nodes' : [1, 2],
        'ways'  : [1],   'rels'  : [1]
      },
      { 'nodes' : [2, 3],
        'ways'  : [1],   'rels'  : [1]
      },
      { 'nodes' : [4, 5, 6, 2],
        'ways'  : [2],   'rels'  : [2]
      },
      { 'nodes' : [2, 7],
        'ways'  : [2],   'rels'  : [2]
      },
    ]


class TestRemovePartialAdjoining(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1, 2 ]),
        st.rhike(2, [ 2, 3 ]) 
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]},
        {'id' : 2, 'nodes' : [ 2, 3 ]},
        {'id' : 3, 'nodes' : [ 3, 4 ]}
    ]
    uprels = [ 
      st.rhike(2, [ 3 ]) 
    ]

    out = [
      { 'nodes' : [1, 2, 3],
        'ways'  : [1,2],   'rels'  : [1]
      },
      { 'nodes' : [3, 4],
        'ways'  : [3],   'rels'  : [2]
      },
    ]


class TestRemoveFully(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1, 2 ]),
        st.rhike(2, [ 1, 2 ]) 
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2 ]},
        {'id' : 2, 'nodes' : [ 2, 3 ]},
    ]
    uprels = [ 
      { 'id' : -2 } 
    ]

    out = [
      { 'nodes' : [1, 2, 3],
        'ways'  : [1,2],   'rels'  : [1]
      },
    ]

class TestRemoveCrossing(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1 ]),
        st.rhike(2, [ 2 ]) 
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2, 4 ]},
        {'id' : 2, 'nodes' : [ 5, 2, 3 ]},
    ]
    uprels = [ 
      { 'id' : -2 } 
    ]

    out = [
      { 'nodes' : [1, 2, 4],
        'ways'  : [1],   'rels'  : [1]
      },
    ]

class TestMoveTouchingInWay(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1 ]),
        st.rhike(2, [ 2 ]) 
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2, 3, 4 ]},
        {'id' : 2, 'nodes' : [ 5, 2 ]},
    ]
    upways = [ 
        {'id' : 2, 'nodes' : [ 5, 3 ]},
    ]

    out = [
      { 'nodes' : [1, 2, 3],
        'ways'  : [1],   'rels'  : [1]
      },
      { 'nodes' : [3, 4],
        'ways'  : [1],   'rels'  : [1]
      },
      { 'nodes' : [5, 3],
        'ways'  : [2],   'rels'  : [2]
      },
    ]

class TestMoveTouchingBetweenWays(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1, 2 ]),
        st.rhike(2, [ 3 ]) 
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2, 3 ]},
        {'id' : 2, 'nodes' : [ 3, 4, 5 ]},
        {'id' : 3, 'nodes' : [ 2, 6 ]},
    ]
    upways = [ 
        {'id' : 3, 'nodes' : [ 4, 6 ]},
    ]

    out = [
      { 'nodes' : [1, 2, 3, 4],
        'ways'  : [1,2],   'rels'  : [1]
      },
      { 'nodes' : [4, 5],
        'ways'  : [2],   'rels'  : [1]
      },
      { 'nodes' : [4, 6],
        'ways'  : [3],   'rels'  : [2]
      },
    ]


#
# TAGGING TESTS
#

class TestRemoveCorrectTagging(st.UpdateSegmentTableTestCase):

    rels = [
        st.rhike(1, [ 1, 2 ]),
        st.rhike(2, [ 1, 2 ]) 
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2, 3 ]},
        {'id' : 2, 'nodes' : [ 3, 4, 5 ]},
    ]
    uprels = [ 
        { 'id' : 1, 'members' : ['W1', 'W2'],
          'tags' : { 'type' : 'route' }
        },
    ]

    out = [
      { 'nodes' : [1, 2, 3, 4, 5],
        'ways'  : [1,2],   'rels'  : [2]
      },
    ]


class TestAddCorrectTagging(st.UpdateSegmentTableTestCase):

    rels = [
        { 'id' : 1, 'members' : ['W1', 'W2'],
          'tags' : { 'type' : 'route' }
        },
        st.rhike(2, [ 1, 2 ]) 
    ]
    ways = [ 
        {'id' : 1, 'nodes' : [ 1, 2, 3 ]},
        {'id' : 2, 'nodes' : [ 3, 4, 5 ]},
    ]
    uprels = [ 
        st.rhike(1, [ 1, 2 ]),
    ]

    out = [
      { 'nodes' : [1, 2, 3, 4, 5],
        'ways'  : [1,2],   'rels'  : [1,2]
      },
    ]





if __name__ == '__main__':
    unittest.main()
