Feature: Route graph
    Testing route graph creating

    Scenario: Simple route with one way
        When given the following route segments
          | id | first | last | geom
          | 1  | 1     | 2    | 1,1 1,2 1,3
        Then the main route is 1,1 1,2 1,3

    Scenario: Simple route with many ways
        When given the following route segments
          | id | first | last | geom
          | 1  | 1     | 2    | 1,1 1,2 1,3
          | 2  | 2     | 3    | 1,3 2,2 2,3
          | 3  | 3     | 4    | 2,3 3,2 3,3
        Then the main route is 1,1 1,2 1,3 2,2 2,3 3,2 3,3


