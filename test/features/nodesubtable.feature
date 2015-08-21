Feature: NodeSubTable

    Scenario: Simple import
        Given the osm data
          | id  | tags                      | data |
          | N1  | "foo" : "1", "bar" : "2"  | 1 1  |
          | N43 | "a" : "a", "b" : "b"      | 2 2.2 |
          | N8  | "x": "...", "bar" : "bar" | 3.33 -4.5 |
        When constructing a NodeSubTable 'FooBar'
        Then table FooBar consists of
          | id  | foo | bar | geom |
          | 1   | 1   | 2   | 1.0 1.0 |
          | 8   | ~~~ | bar | 3.33 -4.5 |

    Scenario: Simple import with subset
        Given the osm data
          | id  | tags                      | data |
          | N1  | "foo" : "1", "bar" : "2"  | 1 1  |
          | N43 | "a" : "a", "b" : "b"      | 2 2.2 |
          | N8  | "x": "...", "bar" : "bar" | 3.33 -4.5 |
        When constructing a NodeSubTable 'FooBar' with subset: tags ? 'x'
        Then table FooBar consists of
          | id  | foo | bar | geom |
          | 8   | ~~~ | bar | 3.33 -4.5 |

    @wip
    Scenario: Simple import with transform
        Given the osm data
          | id  | tags                      | data |
          | N1  | "foo" : "1", "bar" : "2"  | 1 1  |
          | N43 | "a" : "a", "b" : "b"      | 2 2.2 |
          | N8  | "x": "...", "bar" : "bar" | 0 0 |
        When constructing a NodeSubTable 'FooBarTransform'
        Then table FooBarTransform consists of
          | id  | foo | bar | geom |
          | 1   | 1   | 2   | 111319.49079327231 111325.14286638486 |
          | 8   | ~~~ | bar | 0.0 -7.081154551613622e-10 |

    Scenario: Simple delete
        Given the osm data
          | id  | tags                      | data |
          | N1  | "foo" : "1", "bar" : "2"  | 1 1  |
          | N43 | "a" : "a", "b" : "b"      | 2 2.2 |
          | N8  | "x": "...", "bar" : "bar" | 3.33 -4.5 |
        When constructing a NodeSubTable 'FooBar'
        Given an update of osm data
          | action | id |
          | D      | N1 |
        When updating table FooBar
        Then table FooBar consists of
          | id  | foo | bar | geom |
          | 8   | ~~~ | bar | 3.33 -4.5 |

    Scenario: Simple modify
        Given the osm data
          | id  | tags                      | data |
          | N1  | "foo" : "1", "bar" : "2"  | 1 1  |
          | N43 | "a" : "a", "b" : "b"      | 2 2.2 |
          | N8  | "x": "...", "bar" : "bar" | 3.33 -4.5 |
        When constructing a NodeSubTable 'FooBar'
        Given an update of osm data
          | action | id  | tags                      | data |
          | M      | N1  | "foo" : "2", "bar" : "2"  | 1 1  |
        When updating table FooBar
        Then table FooBar consists of
          | id  | foo | bar | geom |
          | 1   | 2   | 2   | 1.0 1.0 |
          | 8   | ~~~ | bar | 3.33 -4.5 |


    Scenario: Simple add
        Given the osm data
          | id  | tags                      | data |
          | N1  | "foo" : "1", "bar" : "2"  | 1 1  |
          | N43 | "a" : "a", "b" : "b"      | 2 2.2 |
          | N8  | "x": "...", "bar" : "bar" | 3.33 -4.5 |
        When constructing a NodeSubTable 'FooBar'
        Given an update of osm data
          | action | id  | tags                      | data |
          | A      | N4  | "foo" : "x"  | 4 5  |
        When updating table FooBar
        Then table FooBar consists of
          | id  | foo | bar | geom |
          | 1   | 1   | 2   | 1.0 1.0 |
          | 8   | ~~~ | bar | 3.33 -4.5 |
          | 4   | x   | ~~~ | 4.0 5.0 |
