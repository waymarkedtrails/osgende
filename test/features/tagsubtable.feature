Feature: TagSubTable

    @wip
    Scenario: Simple
        Given the osm data
          | id  | tags                      | data |
          | N1  | "foo" : "1", "bar" : "2"  | 1 1  |
          | N43 | "a" : "a", "b" : "b"      | 2 2.2 |
          | N8  | "x": "...", "bar" : "bar" | 3.33 -4.5 |
        When constructing a TagSubTable FooBar on node
        Then table FooBar consists of
          | id  | foo | bar |
          | 1   | 1   | 2   |
          | 8   | ~~~ | bar |
