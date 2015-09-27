Feature: Route table from RouteSegments

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
        | R1 | W1     | 'type' : 'route', 'route' : 'hiking', 'name' : 'foo' |
        | R2 | W2,W3  | 'type' : 'route', 'route' : 'hiking', 'name' : 'bar' |
        | R3 | W4,W5  | 'type' : 'route', 'route' : 'hiking', 'name' : 'bazz' |
      When constructing a RouteSegments table 'Hiking'
      And constructing a Routes table 'HikingRoutes' from 'Hiking'
      Then table HikingRoutes consists of
        | id | name |
        | 1  | foo  |
        | 2  | bar  |
        | 3  | bazz |

    Scenario: Remove relation
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
        | R1 | W1     | 'type' : 'route', 'route' : 'hiking', 'name' : 'foo' |
        | R2 | W2,W3  | 'type' : 'route', 'route' : 'hiking', 'name' : 'bar' |
        | R3 | W4,W5  | 'type' : 'route', 'route' : 'hiking', 'name' : 'bazz' |
      When constructing a RouteSegments table 'Hiking'
      And constructing a Routes table 'HikingRoutes' from 'Hiking'
      Given an update of osm data
        | action | id | data      | tags |
        | D      | R1 | | |
      When updating table Hiking
      And updating table HikingRoutes
      Then table HikingRoutes consists of
        | id | name |
        | 2  | bar  |
        | 3  | bazz |

    Scenario: Rename relation
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
        | R1 | W1     | 'type' : 'route', 'route' : 'hiking', 'name' : 'foo' |
        | R2 | W2,W3  | 'type' : 'route', 'route' : 'hiking', 'name' : 'bar' |
        | R3 | W4,W5  | 'type' : 'route', 'route' : 'hiking', 'name' : 'bazz' |
      When constructing a RouteSegments table 'Hiking'
      And constructing a Routes table 'HikingRoutes' from 'Hiking'
      Given an update of osm data
        | action | id | data   | tags |
        | M      | R1 | W1     | 'type' : 'route', 'route' : 'hiking', 'name' : 'FOO' |
      When updating table Hiking
      And updating table HikingRoutes
      Then table HikingRoutes consists of
        | id | name |
        | 1  | FOO  |
        | 2  | bar  |
        | 3  | bazz |

    Scenario: Add relation
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
        | R2 | W2,W3  | 'type' : 'route', 'route' : 'hiking', 'name' : 'bar' |
        | R3 | W4,W5  | 'type' : 'route', 'route' : 'hiking', 'name' : 'bazz' |
      When constructing a RouteSegments table 'Hiking'
      And constructing a Routes table 'HikingRoutes' from 'Hiking'
      Given an update of osm data
        | action | id | data   | tags |
        | A      | R1 | W1     | 'type' : 'route', 'route' : 'hiking', 'name' : 'foo' |
      When updating table Hiking
      And updating table HikingRoutes
      Then table HikingRoutes consists of
        | id | name |
        | 1  | foo  |
        | 2  | bar  |
        | 3  | bazz |

    @wip
    Scenario: Change a super relation
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
        | R2 | W2,W3  | 'type' : 'route', 'route' : 'hiking', 'name' : 'bar' |
        | R3 | W4,W5  | 'type' : 'route', 'route' : 'hiking', 'name' : 'bazz' |
        | R4 | R2,R3  | 'type' : 'route', 'route' : 'hiking', 'name' : 'sup' |
      When constructing a RouteSegments table 'Hiking'
      And constructing a RelationHierarchy 'routehier' with subset: tags->'type' = 'route' AND tags->'route' = 'hiking'
      And constructing a Routes table 'HikingRoutes' from 'Hiking' and 'routehier'
      Given an update of osm data
        | action | id | data     | tags |
        | M      | R2 | W1,W2,W3 | 'type' : 'route', 'route' : 'hiking', 'name' : 'foo' |
      When updating table Hiking
      And updating table routehier
      And updating table HikingRoutes
      Then table HikingRoutes consists of
        | id | name |
        | 2  | foo  |
        | 3  | bazz |
        | 4  | sup |

