Feature: RouteSegments

    Background:
      Given the following tag sets
       | name   | tags |
       | HIKING | 'type' : 'route', 'route' : 'hiking' |

    Scenario: Simple ways
      Given a 0.0001 node grid
        | 1 | 2 | 3 |   |   |
        | 4 | 5 | 6 | 7 | 8 |
        |   | 9 | 10| 11| 12|
      And the osm data
        | id | data       | tags |
        | W1 | 1,2,3      | |
        | W2 | 4,5        | |
        | W3 | 5,6        | |
        | W4 | 7,8        | |
        | W5 | 9,10,11,12 | |
        | R1 | W1     | HIKING |
        | R2 | W2,W3  | HIKING |
        | R3 | W4,W5  | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Then table Hiking consists of rows
        | nodes      | ways | rels | geom |
        | 1,2,3      | 1    | 1    | 0.0 0.0, 0.0001 0.0, 0.0002 0.0 |
        | 4,5,6      | 2,3  | 2    | 0.0 0.0001, 0.0001 0.0001, 0.0002 0.0001 |
        | 7,8        | 4    | 3    | 0.0003 0.0001, 0.0004 0.0001 |
        | 9,10,11,12 | 5    | 3    | 0.0001 0.0002, 0.0002 0.0002, 0.0003 0.0002, 0.0004 0.0002 |

    Scenario: Circular ways
      Given a 0.0001 node grid
        |   | 1 | 2 |   |
        |   |   | 3 |   |
        |   | 6 |   | 7 |
        |   |   | 4 |   |
        |   |   | 5 |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3     | |
        | W2  | 4,5       | |
        | W3  | 3,6,4,7,3 | |
        | R1  | W1,W2,W3  | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2,3      | 1    | 1    |
        | 4,5        | 2    | 1    |
        | 3,6,4      | 3    | 1    |
        | 4,7,3      | 3    | 1    |

    Scenario: Circlar way unattached
      Given a 0.0001 node grid
        |   |   | 6 |   | 4 |
        | 2 | 1 |   | 8 | 5 |
        | 3 |   | 7 |   |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3     | |
        | W2  | 4,5       | |
        | W3  | 6,8,7,1,6 | |
        | R1  | W1,W2,W3  | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2,3      | 1    | 1    |
        | 4,5        | 2    | 1    |
        | 1,6,8,7,1  | 3    | 1    |

    Scenario: Circular way unattached split
      Given a 0.0001 node grid
        |   | 3 | 2 |   |
        |   |   | 1 |   |
        |   | 6 |   | 7 |
        |   |   | 5 |   |
        |   |   | 4 |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3     | |
        | W2  | 4,5       | |
        | W3  | 6,5,7,1,6 | |
        | R1  | W1,W2,W3  | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2,3      | 1    | 1    |
        | 4,5        | 2    | 1    |
        | 1,6,5      | 3    | 1    |
        | 5,7,1      | 3    | 1    |

    Scenario: Circular relation
      Given a 0.0001 node grid
        | 1 | 2 | 3 |
        | 7 |   | 4 |
        |   | 6 | 5 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3     | |
        | W2  | 3,4,5     | |
        | W3  | 5,6,7,1   | |
        | R1  | W1,W2,W3  | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Then table Hiking consists of rows
        | nodes           | ways  | rels |
        | 5,6,7,1,2,3,4,5 | 3,1,2 | 1 |

    Scenario: Y intersection
      Given a 0.0001 node grid
        | 3 | 2 | 1 | 4 | 5 |
        |   |   | 6 |   |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3     | |
        | W2  | 1,4,5     | |
        | W3  | 1,6       | |
        | R1  | W1,W2,W3  | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2,3      | 1    | 1    |
        | 1,4,5      | 2    | 1    |
        | 1,6        | 3    | 1    |

    Scenario: Crossing intersection
      Given a 0.0001 node grid
       |   | 1 |   |
       | 4 | 2 | 5 |
       |   | 3 |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3     | |
        | W2  | 4,2,5     | |
        | R1  | W1,W2     | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2        | 1    | 1    |
        | 2,3        | 1    | 1    |
        | 4,2        | 2    | 1    |
        | 2,5        | 2    | 1    |

    Scenario: Relation types
      Given a 0.0001 node grid
        | 1 | 2 | 3 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3     | |
        | R1  | W1        | 'type' : 'route' |
        | R2  | W1        | 'type' : 'route', 'route' : 'hiking' |
        | R3  | W1        | 'route' : 'hiking' |
      When constructing a RouteSegments table 'Hiking'
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2,3      | 1    | 2 |

    Scenario: Full route overlap
      Given a 0.0001 node grid
       | 1 | 2 | 3 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 2,3       | |
        | R1  | W1,W2     | HIKING |
        | R2  | W2, W1    | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2,3      | 1,2  | 1,2  |

    Scenario: Forking route overlap
      Given a 0.0001 node grid
       | 1 | 2 | 3 |
       |   | 4 |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 2,3       | |
        | W3  | 2,4       | |
        | R1  | W1,W2     | HIKING |
        | R2  | W3,W1     | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2        | 1    | 1,2  |
        | 2,3        | 2    | 1    |
        | 2,4        | 3    | 2    |

    Scenario: Partial route overlap
      Given a 0.0001 node grid
       | 1 | 2 | 3 | 4 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 2,3       | |
        | W3  | 3,4       | |
        | R1  | W1,W2     | HIKING |
        | R2  | W2,W3     | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2        | 1    | 1    |
        | 2,3        | 2    | 1,2  |
        | 3,4        | 3    | 2    |

    Scenario: Fork route away
      Given a 0.0001 node grid
       | 1 | 2 | 3 |
       |   | 4 |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 2,3       | |
        | W3  | 2,4       | |
        | R1  | W1,W2     | HIKING |
        | R2  | W3        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2        | 1    | 1    |
        | 2,3        | 2    | 1    |
        | 2,4        | 3    | 2    |

    Scenario: Fork route away in way
      Given a 0.0001 node grid
       | 1 | 2 | 3 |
       |   | 4 |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3     | |
        | W3  | 2,4       | |
        | R1  | W1        | HIKING |
        | R2  | W3        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2        | 1    | 1    |
        | 2,3        | 1    | 1    |
        | 2,4        | 3    | 2    |

    Scenario: Move node
      Given a 0.0001 node grid
        | 1 | 2 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | R1  | W1        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | M      | N1 | 0.0 -0.001 | |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways | rels | geom |
        | 1,2        | 1    | 1    | 0.0 -0.001, 0.0001 0.0 |

    Scenario: Change way geometry
      Given a 0.0001 node grid
        | 1 | 3 | 2 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | R1  | W1        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | M      | W1 | 1,3,2     | |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,3,2      | 1    | 1    |

    Scenario: Move node in segmented way
      Given a 0.0001 node grid
        |   |   | 6 |   |   |
        | 1 | 3 | 2 | 4 | 5 |
        |   |   | 7 |   |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,3,2,4,5 |        |
        | W2  | 6,2,7     |        |
        | R1  | W1        | HIKING |
        | R2  | W2        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,3,2      | 1    | 1    |
        | 2,4,5      | 1    | 1    |
        | 6,2        | 2    | 2    |
        | 2,7        | 2    | 2    |
      Given an update of osm data
        | action | id | data      | tags |
        | M      | N3 | 0.0 -0.001 | |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,3,2      | 1    | 1    |
        | 2,4,5      | 1    | 1    |
        | 6,2        | 2    | 2    |
        | 2,7        | 2    | 2    |

    Scenario: Replace way
      Given a 0.0001 node grid
        | 1 | 2 |
        | 3 | 4 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 3,4       | |
        | R1  | W1        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | M      | R1 | W2        | HIKING |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 3,4        | 2    | 1    |

    Scenario: Add unrelated way
      Given a 0.0001 node grid
        | 1 | 2 |
        | 3 | 4 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | R1  | W1        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | A      | W2 | 3,4       | |
        | M      | R1 | W1,W2     | HIKING |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2        | 1    | 1    |
        | 3,4        | 2    | 1    |

    Scenario: Add adjoining way
      Given a 0.0001 node grid
       | 1 | 2 | 3 | 4 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | R1  | W1        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | A      | W2 | 2,3,4     | |
        | M      | R1 | W1,W2     | HIKING |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2,3,4    | 1,2  | 1    |

    Scenario: Add joining way
      Given a 0.0001 node grid
       | 1 | 2 | 3 | 4 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 3,4       | |
        | R1  | W1,W2     | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | A      | W3 | 2,3       | |
        | M      | R1 | W1,W2,W3  | HIKING |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways  | rels |
        | 1,2,3,4    | 1,3,2 | 1    |

    Scenario: Remove unrelated way
      Given a 0.0001 node grid
        | 1 | 2 |
        | 3 | 4 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 3,4       | |
        | R1  | W1,W2     | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | D      | W2 |           |      |
        | M      | R1 | W1        | HIKING |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2        | 1    | 1    |

    Scenario: Remove adjoining way
      Given a 0.0001 node grid
        | 1 | 2 |  | 4 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 2,4       | |
        | R1  | W1,W2     | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | M      | R1 | W1        | HIKING |
        | D      | W2 |           |  |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2        | 1    | 1    |

    Scenario: Remove joining way
      Given a 0.0001 node grid
        | 1 | 2 | 3 | 4 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 2,3       | |
        | W3  | 3,4       | |
        | R1  | W1,W2,W3  | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | M      | R1 | W1,W3     | HIKING |
        | D      | W2 |           | |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2        | 1    | 1    |
        | 3,4        | 3    | 1    |

    Scenario: Change semi-segment
      Given a 0.0001 node grid
        | 1 | 2 | 3 | 4 | 5 |
        |   | 6 |   | 7 |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3     | |
        | W2  | 3,4,5     | |
        | W3  | 6,2       | |
        | W4  | 4,7       | |
        | R1  | W1,W2     | HIKING |
        | R2  | W3,W4     | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | M      | R2 | W3,W4,W1  | HIKING |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes | ways | rels |
        | 1,2   | 1    | 1,2  |
        | 2,3   | 1    | 1,2  |
        | 3,4   | 2    | 1    |
        | 4,5   | 2    | 1    |
        | 6,2   | 3    | 2    |
        | 4,7   | 4    | 2    |

    Scenario: Add route fully
      Given a 0.0001 node grid
        | 1 | 2 |  | 3 | 4 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | R1  | W1        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | A      | W2 | 3,4       | |
        | A      | R2 | W1        | HIKING |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2        | 1    | 1,2  |

    Scenario: Add route partial
      Given a 0.0001 node grid
        | 1 | 2 |  | 3 | 4 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 3,4       | |
        | R1  | W1,W2     | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | A      | R2 | W1        | HIKING |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2        | 1    | 1,2  |
        | 3,4        | 2    | 1    |

    Scenario: Add route partial joining
      Given a 0.0001 node grid
        | 1 | 2 | 3 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 2,3       | |
        | R1  | W1,W2     | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | A      | R2 | W1        | HIKING |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2        | 1    | 1,2  |
        | 2,3        | 2    | 1    |

    Scenario: Add route partial completing
      Given a 0.0001 node grid
       | 1 | 2 | 3 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 2,3       | |
        | R1  | W1,W2     | HIKING |
        | R2  | W1        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | M      | R2 | W1,W2     | HIKING |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2,3      | 1,2  | 1,2  |

    Scenario: Add Y intersection other
      Given a 0.0001 node grid
        | 1 | 2 | 3 |
        |   | 4 |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 2,3       | |
        | R1  | W1,W2     | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | A      | W3 | 4,2       | |
        | A      | R2 | W3        | HIKING |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes      | ways | rels |
        | 1,2        | 1    | 1    |
        | 2,3        | 2    | 1    |
        | 4,2        | 3    | 2    |

    Scenario: Add route crossing other
      Given a 0.0001 node grid
        |   | 5 |   |
        | 1 | 2 | 3 |
        |   | 4 |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3     | |
        | R1  | W1        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | A      | W2 | 4,2,5     | |
        | A      | R2 | W2        | HIKING |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes  | ways | rels |
        | 1,2    | 1    | 1    |
        | 2,3    | 1    | 1    |
        | 4,2    | 2    | 2    |
        | 2,5    | 2    | 2    |

    Scenario: Extend route crossing other
      Given a 0.0001 node grid
        |   | 7 |   |
        | 1 | 2 | 3 |
        |   | 6 |   |
        |   | 5 | 4 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3     | |
        | W2  | 4,5,6     | |
        | R1  | W1        | HIKING |
        | R2  | W2        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | M      | W2 | 4,5,6,2,7 | |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes  | ways | rels |
        | 1,2    | 1    | 1    |
        | 2,3    | 1    | 1    |
        | 4,5,6,2| 2    | 2    |
        | 2,7    | 2    | 2    |

    Scenario: Remove route partial adjoining
      Given a 0.0001 node grid
        | 1 | 2 | 3 | 4 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 2,3       | |
        | W3  | 3,4       | |
        | R1  | W1,W2     | HIKING |
        | R2  | W2,W3     | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | M      | R2 | W3        | HIKING |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes  | ways | rels |
        | 1,2,3  | 1,2  | 1    |
        | 3,4    | 3    | 2    |

    Scenario: Remove route fully
      Given a 0.0001 node grid
        | 1 | 2 | 3 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2       | |
        | W2  | 2,3       | |
        | R1  | W1,W2     | HIKING |
        | R2  | W1,W2     | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | D      | R2 |           | |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes  | ways | rels |
        | 1,2,3  | 1,2  | 1    |

    Scenario: Remove crossing route
      Given a 0.0001 node grid
        |   | 5 |   |
        | 1 | 2 | 3 |
        |   | 4 |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,4     | |
        | W2  | 5,2,3     | |
        | R1  | W1        | HIKING |
        | R2  | W2        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | D      | R2 |           | |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes  | ways | rels |
        | 1,2,4  | 1    | 1    |

    Scenario: Move touching point in way
      Given a 0.0001 node grid
        | 1 | 2 |   | 3 | 4 |
        |   |   | 5 |   |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3,4   | |
        | W2  | 5,2       | |
        | R1  | W1        | HIKING |
        | R2  | W2        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | M      | W2 | 5,3       | |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes  | ways | rels |
        | 1,2,3  | 1    | 1    |
        | 3,4    | 1    | 1    |
        | 5,3    | 2    | 2    |

    Scenario: Move touching between ways
      Given a 0.0001 node grid
        | 1 | 2 | 3 | 4 | 5 |
        |   |   |   |   |   |
        |   |   | 6 |   |   |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3     | |
        | W2  | 3,4,5     | |
        | W3  | 2,6       | |
        | R1  | W1,W2     | HIKING |
        | R2  | W3        | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | M      | W3 | 4,6       | |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes  | ways | rels |
        | 1,2,3,4| 1,2  | 1    |
        | 4,5    | 2    | 1    |
        | 4,6    | 3    | 2    |

    Scenario: Remove route tagging from relation
      Given a 0.0001 node grid
        | 1 | 2 | 3 | 4 | 5 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3     | |
        | W2  | 3,4,5     | |
        | R1  | W1,W2     | HIKING |
        | R2  | W1,W2     | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | M      | R1 | W1, W2    | 'type' : 'route' |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes     | ways | rels |
        | 1,2,3,4,5 | 1,2  | 2    |

    Scenario: Add route tagging to relation
      Given a 0.0001 node grid
        | 1 | 2 | 3 | 4 | 5 |
      And the osm data
        | id  | data      | tags |
        | W1  | 1,2,3     | |
        | W2  | 3,4,5     | |
        | R1  | W1,W2     | 'type' : 'route' |
        | R2  | W1,W2     | HIKING |
      When constructing a RouteSegments table 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | M      | R1 | W1, W2    | HIKING |
      When updating table Hiking
      Then table Hiking consists of rows
        | nodes     | ways | rels |
        | 1,2,3,4,5 | 1,2  | 1,2  |

