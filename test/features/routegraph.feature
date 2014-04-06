Feature: Route graph
    Testing route graph creating

    Scenario: Simple route with one way
        Given the following route segments
          | id | geom
          | 1  | 1,1 1,2 1,3
        Then the main route is 1,1 1,2 1,3

    Scenario: Simple route with two ways
        Given the following route segments
          | id | geom
          | 1  | 1,1 1,2 1,3
          | 2  | 1,3 2,2 2,3
          | 3  | 2,3 3,2 3,3
        Then the main route is 1,1 1,2 1,3 2,2 2,3 3,2 3,3

    Scenario: Simple route with many ways
        Given the following route segments
          | id | geom
          | 1  | 1,1 1,3
          | 2  | 1,3 2,3
          | 3  | 2,3 3,3
          | 4  | 3,3 4,3
          | 5  | 4,3 5,3
        Then the main route is 1,1 1,3 2,3 3,3 4,3 5,3

    Scenario: Simple route with roundabout
        Given the following route segments
          | id | geom
          | 1  | 1,1 1,2
          | 2  | 1,2 2,3 1,4
          | 3  | 1,2 -1,3 1,4
          | 4  | 1,4 1,5
        Then the main route is 1,1 1,2 2,3 1,4 1,5

    Scenario: Simple route with fork
        Given the following route segments
          | id | geom
          | 1  | 1,1 1,2 2,2 4,5
          | 2  | 4,5 5,5 6,4 
          | 3  | 4,5 5,4
        Then the main route is 1,1 1,2 2,2 4,5 5,5 6,4

    Scenario: Diamond Formation
        Given the following route segments
          | id | geom
          | 1  | 1,1 1,5
          | 2  | 1,5 3,6 
          | 3  | 3,6 4,6
          | 4  | 3,6 1,7
          | 5  | 1,5 0,6
          | 6  | 0,6 -1,6
          | 7  | 0,6 1,7
          | 8  | 1,7 1,10
        Then the main route is 1,1 1,5 3,6 1,7 1,10

    Scenario: Simple route with a hole
        Given the following route segments
          | id | geom
          | 1  | 1,1 1,5
          | 2  | 1,6 1,10 
        Then the main route is 1,1 1,5 1,6 1,10

    Scenario: Simple route with two holes
        Given the following route segments
          | id | geom
          | 1  | 1,1 1,5
          | 2  | 1,6 1,10
          | 3  | 1,11 1,20
        Then the main route is 1,1 1,5 1,6 1,10 1,11 1,20

    Scenario: Star route hole
        Given the following route segments
          | id | geom
          | 1  | 1,1 4,5
          | 2  | 10,10 5,4
          | 3  | 10,1 4,4
        Then the main route is 10,10 5,4 4,4 10,1


    Scenario: Test real routes
        Given the segments in scenario routes.sql
        Then all routes have a main route


