Feature: WaySubTable

    Scenario: Simple import
        Given a 0.0001 node grid
         | 1 |   |   |
         |   | 2 |   |
         |   |   | 3 |
        And the osm data
          | id | tags                  | data |
          | W5 | "highway" : "service" | 1, 2, 3 |
          | W6 | "building" : "service" | 1, 2, 3 |
        When constructing a WaySubTable 'Highway'
        Then table Highway consists of
          | id | type    | geom |
          | 5  | service | 0.0 0.0, 0.0001 0.0001, 0.0002 0.0002 |

    Scenario: Import of broken geometries
        Given a 0.0001 node grid
         | 1 |   |   |
         |   | 2 |   |
         |   |   | 3 |
        And the osm data
          | id | tags            | data |
          | W1 | "highway" : "b" | 1, 2, 3 |
          | W5 | "highway" : "x" |  |
          | W10| "highway" : "x" | 2 |
          | W33| "highway" : "x" | 1, 2, 2, 3 |
        When constructing a WaySubTable 'Highway'
        Then table Highway consists of
          | id | type | geom |
          | 1  | b    | 0.0 0.0, 0.0001 0.0001, 0.0002 0.0002 |
          | 33 | x    | 0.0 0.0, 0.0001 0.0001, 0.00010001 0.0001, 0.0002 0.0002 |

    Scenario: Simple import with subset
        Given a 0.0001 node grid
         | 1 |   |   |
         |   | 2 |   |
         |   |   | 3 |
        And the osm data
          | id | tags                               | data |
          | W5 | "highway" : "service"              | 1, 2, 3 |
          | W8 | "highway" : "service", "ref" : "1" | 1, 2, 3 |
          | W6 | "building" : "service"             | 1, 2, 3 |
        When constructing a WaySubTable 'Highway' with subset: tags ? 'ref'
        Then table Highway consists of
          | id | type    | geom |
          | 8  | service | 0.0 0.0, 0.0001 0.0001, 0.0002 0.0002 |

    Scenario: Simple import with transform
        Given a 0.0001 node grid
         | 1 |   |   |
         |   | 2 |   |
         |   |   | 3 |
        And the osm data
          | id | tags                  | data |
          | W5 | "highway" : "service" | 1, 2 |
        When constructing a WaySubTable 'HighwayTransform'
        Then table HighwayTransform consists of
          | id | type    | geom |
          | 5  | service | 0.0 -7.081154551613622e-10, 11.131949078023732 11.131949078903947 |

    Scenario: Simple delete
        Given a 0.0001 node grid
         | 1 |   |   |
         |   | 2 |   |
         |   |   | 3 |
        And the osm data
          | id | tags                  | data |
          | W5 | "highway" : "service" | 1, 2, 3 |
          | W6 | "highway" : "house"   | 3, 2, 1 |
        And a geometry change table 'Change'
        When constructing a WaySubTable 'Highway' using geometry change 'Change'
        Given an update of osm data
          | action | id |
          | D      | W6 |
        When updating table Highway
        Then table Highway consists of
          | id | type    | geom |
          | 5  | service | 0.0 0.0, 0.0001 0.0001, 0.0002 0.0002 |
        And table Change consists of
          | action | geom |
          | D      | 0.0002 0.0002, 0.0001 0.0001, 0.0 0.0 |

    Scenario: Simple modify
        Given a 0.0001 node grid
         | 1 |   |   |
         |   | 2 |   |
         |   |   | 3 |
        And the osm data
          | id | tags                  | data |
          | W5 | "highway" : "service" | 1, 2, 3 |
          | W6 | "highway" : "house"   | 3, 2, 1 |
        And a geometry change table 'Change'
        When constructing a WaySubTable 'Highway' using geometry change 'Change'
        Given an update of osm data
          | action | id  | tags                 | data    |
          | M      | W6  | "highway" : "house"  | 2, 3, 1 |
        When updating table Highway
        Then table Highway consists of
          | id | type    | geom |
          | 5  | service | 0.0 0.0, 0.0001 0.0001, 0.0002 0.0002 |
          | 6  | house   | 0.0001 0.0001, 0.0002 0.0002, 0.0 0.0 |
        And table Change consists of
          | action | geom |
          | D      | 0.0002 0.0002, 0.0001 0.0001, 0.0 0.0 |
          | A      | 0.0001 0.0001, 0.0002 0.0002, 0.0 0.0 |

    Scenario: Simple add
        Given a 0.0001 node grid
         | 1 |   |   |
         |   | 2 |   |
         |   |   | 3 |
        And the osm data
          | id | tags                  | data |
          | W5 | "highway" : "service" | 1, 2, 3 |
        And a geometry change table 'Change'
        When constructing a WaySubTable 'Highway' using geometry change 'Change'
        Given an update of osm data
          | action | id  | tags                 | data    |
          | A      | W6  | "highway" : "house"  | 2, 3, 1 |
        When updating table Highway
        Then table Highway consists of
          | id | type    | geom |
          | 5  | service | 0.0 0.0, 0.0001 0.0001, 0.0002 0.0002 |
          | 6  | house   | 0.0001 0.0001, 0.0002 0.0002, 0.0 0.0 |
        And table Change consists of
          | action | geom |
          | A      | 0.0001 0.0001, 0.0002 0.0002, 0.0 0.0 |

