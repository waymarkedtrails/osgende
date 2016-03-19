Feature: Hierarchy table

    Scenario: Simple import
        Given the osm data
          | id | data |
          | R1 | N1   |
          | R2 | R1   |
          | R3 | W1,W2 |
        When constructing a RelationHierarchy 'hier'
        Then table hier consists of
          | parent | child | depth |
          |  1     |  1    | 1 |
          |  2     |  2    | 1 |
          |  2     |  1    | 2 |

    Scenario: Multi-depth relations
        Given the osm data
          | id | data    |
          | R1 | R2,R3   |
          | R2 | R4,R5   |
          | R3 | W1,W2   |
          | R4 | W4      |
          | R5 | R6      |
          | R6 | W6      |
        When constructing a RelationHierarchy 'hier'
        Then table hier consists of
          | parent | child | depth |
          |  1     |  1    | 1 |
          |  2     |  2    | 1 |
          |  3     |  3    | 1 |
          |  4     |  4    | 1 |
          |  5     |  5    | 1 |
          |  6     |  6    | 1 |
          |  1     |  2    | 2 |
          |  1     |  3    | 2 |
          |  2     |  4    | 2 |
          |  2     |  5    | 2 |
          |  5     |  6    | 2 |
          |  1     |  4    | 3 |
          |  1     |  5    | 3 |
          |  2     |  6    | 3 |
          |  1     |  6    | 4 |

    Scenario: Simple import with subset
        Given the osm data
          | id | tags |data |
          | R1 | "x" : "1" | N1   |
          | R2 | "x" : "a" | R1,R5 |
          | R3 | "x" : "x" | W1,W2 |
          | R4 | "y" : "x" | R3,R2,N4 |
          | R5 | "a" : "a" | N3,N5 |
        When constructing a RelationHierarchy 'hier' with subset: tags ? 'x'
        Then table hier consists of
          | parent | child | depth |
          |  1     |  1    | 1 |
          |  2     |  2    | 1 |
          |  3     |  3    | 1 |
          |  2     |  1    | 2 |

    Scenario: Recursive relations
        Given the osm data
          | id | data |
          | R1 | R2   |
          | R2 | R1   |
        When constructing a RelationHierarchy 'hier'
        Then table hier consists of
          | parent | child | depth |
          |  1     |  1    | 1 |
          |  2     |  2    | 1 |
          |  2     |  1    | 2 |
          |  1     |  2    | 2 |


