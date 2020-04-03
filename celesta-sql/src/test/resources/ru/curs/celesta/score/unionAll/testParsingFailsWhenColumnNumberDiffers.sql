CREATE GRAIN test VERSION '1.0';

CREATE table testTableA (
  idA INT NOT NULL PRIMARY KEY,
  f2A int NOT NULL,
  f3A VARCHAR (2)
);

CREATE table testTableB (
  idB INT NOT NULL PRIMARY KEY,
  f2B int NOT NULL,
  f3B VARCHAR (3)
);

-- invalid UNION ALL query: different number of fields
CREATE VIEW testUnionAll AS
  SELECT idA as A, f3A as B FROM testTableA WHERE f2A = 1
  UNION ALL
  SELECT f3B FROM testTableB WHERE f2B = 1;
