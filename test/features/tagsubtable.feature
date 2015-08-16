Feature: TagSubTable

    @wip
    Scenario: Simple
        Given the osm data
          | id  | tags                     | data |
          | N1  | "foo" : "1", "bar" : "2" | 1 1  |
          | W2  | "a" : "b"                | 1,2,1 |
          | R33 | "1" : "2"                | N1,W2/hell,R33 |
