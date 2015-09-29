Feature: Joined way table

    @wip
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

