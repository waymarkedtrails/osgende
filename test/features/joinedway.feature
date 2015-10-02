Feature: Joined way table

    Scenario: Simple way
        Given a 0.0001 node grid
         | 1 |   |   | 4 |
         |   | 2 |   | 5 |
         |   |   | 3 | 6 |
        And the osm data
          | id | tags                  | data |
          | W5 | "type" : "foo"        | 1,2,3 |
          | W6 | "type" : "foo"        | 4,5  |
          | W7 | "type" : "foo"        | 5,6  |
        When constructing a WaySubTable 'Slopes'
        And constructing a JoinedWay table 'joined' from 'Slopes' with rows 'type,name'
        Then table joined consists of
          | virtual_id | child |
          | 6      | 6     |
          | 6      | 7     |

    Scenario: Touching with different tags
        Given a 0.0001 node grid
         | 1 | 2 | 3 | 4 |
        And the osm data
          | id | tags                         | data |
          | W5 | "type" : "foo"               | 1,2,3 |
          | W6 | "type" : "foo", "name" : "x" | 3,4  |
        When constructing a WaySubTable 'Slopes'
        And constructing a JoinedWay table 'joined' from 'Slopes' with rows 'type,name'
        Then table joined consists of
          | virtual_id | child |

    Scenario: Crossing ways
        Given a 0.0001 node grid
         |   | 1 |   |
         | 2 | 3 | 4 |
         |   | 5 |   |
        And the osm data
          | id | data    | tags |
          | W1 | 2,3,4   | "type" : "red", "name" : "id" |
          | W2 | 1,3,5   | "type" : "red", "name" : "id" |
        When constructing a WaySubTable 'Slopes'
        And constructing a JoinedWay table 'joined' from 'Slopes' with rows 'type,name'
        Then table joined consists of
          | virtual_id | child |
          | 1          | 1     |
          | 1          | 2     |

   Scenario: Crossing ways with different tags
        Given a 0.0001 node grid
         |   | 1 |   |
         | 2 | 3 | 4 |
         |   | 5 |   |
        And the osm data
          | id | data    | tags |
          | W1 | 2,3,4   | "type" : "red", "name" : "id" |
          | W2 | 1,3,5   | "name" : "id" |
        When constructing a WaySubTable 'Slopes'
        And constructing a JoinedWay table 'joined' from 'Slopes' with rows 'type,name'
        Then table joined consists of
          | virtual_id | child |


    Scenario: Crossing ways without touching
        Given a 0.0001 node grid
         |   | 1 |   |
         | 2 | 3 | 4 |
         |   | 5 |   |
        And the osm data
          | id | data  | tags |
          | W1 | 2,3,4 | "type" : "red", "name" : "id" |
          | W2 | 1,5   | "type" : "red", "name" : "id" |
        When constructing a WaySubTable 'Slopes'
        And constructing a JoinedWay table 'joined' from 'Slopes' with rows 'type,name'
        Then table joined consists of
          | virtual_id | child |

    Scenario: Simple append
        Given a 0.0001 node grid
         | 1 | 2 | 3 |
         | 4 | 5 | 6 |
        And the osm data
          | id | data    | tags |
          | W1 | 1,2,3   | "type" : "red" |
        When constructing a WaySubTable 'Slopes'
        And constructing a JoinedWay table 'joined' from 'Slopes' with rows 'type,name'
        Given an update of osm data
          | action | id  | data    | tags |
          | A      | W2  | 3,6,5,4 | "type" : "red" |
        When updating table Slopes
        And updating table joined
        Then table joined consists of
          | virtual_id | child |
          | 2          | 1     |
          | 2          | 2     |

