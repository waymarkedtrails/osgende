Feature: TagSubTable

    Scenario: Simple import
        Given the osm data
          | id  | tags                      | data |
          | N1  | "foo" : "1", "bar" : "2"  | 1 1  |
          | N43 | "a" : "a", "b" : "b"      | 2 2.2 |
          | N8  | "x": "...", "bar" : "bar" | 3.33 -4.5 |
        When constructing a TagSubTable FooBar on 'node'
        Then table FooBar consists of
          | id  | foo | bar |
          | 1   | 1   | 2   |
          | 8   | ~~~ | bar |

    Scenario: Simple import with subset
        Given the osm data
          | id  | tags                      | data |
          | N1  | "foo" : "1", "bar" : "2"  | 1 1  |
          | N43 | "a" : "a", "b" : "b"      | 2 2.2 |
          | N8  | "x": "...", "bar" : "bar" | 3.33 -4.5 |
        When constructing a TagSubTable FooBar on 'node' with subset: tags ? 'x'
        Then table FooBar consists of
          | id  | foo | bar |
          | 8   | ~~~ | bar |

    Scenario: Simple delete
        Given the osm data
          | id  | tags                      | data |
          | N1  | "foo" : "1", "bar" : "2"  | 1 1  |
          | N43 | "a" : "a", "b" : "b"      | 2 2.2 |
          | N8  | "x": "...", "bar" : "bar" | 3.33 -4.5 |
        When constructing a TagSubTable FooBar on 'node'
        Given an update of osm data
          | action | id |
          | D      | N1 |
        When updating table FooBar
        Then table FooBar consists of
          | id  | foo | bar |
          | 8   | ~~~ | bar |

    Scenario: Simple modify
        Given the osm data
          | id  | tags                      | data |
          | N1  | "foo" : "1", "bar" : "2"  | 1 1  |
          | N43 | "a" : "a", "b" : "b"      | 2 2.2 |
          | N8  | "x": "...", "bar" : "bar" | 3.33 -4.5 |
        When constructing a TagSubTable FooBar on 'node'
        Given an update of osm data
          | action | id  | tags                      | data |
          | M      | N1  | "foo" : "2", "bar" : "2"  | 1 1  |
        When updating table FooBar
        Then table FooBar consists of
          | id  | foo | bar |
          | 1   | 2   | 2   |
          | 8   | ~~~ | bar |


    Scenario: Simple add
        Given the osm data
          | id  | tags                      | data |
          | N1  | "foo" : "1", "bar" : "2"  | 1 1  |
          | N43 | "a" : "a", "b" : "b"      | 2 2.2 |
          | N8  | "x": "...", "bar" : "bar" | 3.33 -4.5 |
        When constructing a TagSubTable FooBar on 'node'
        Given an update of osm data
          | action | id  | tags                      | data |
          | A      | N4  | "foo" : "x"  | 4 5  |
        When updating table FooBar
        Then table FooBar consists of
          | id  | foo | bar |
          | 1   | 1   | 2   |
          | 8   | ~~~ | bar |
          | 4   | x   | ~~~ |

