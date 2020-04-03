CREATE GRAIN test VERSION '1.0';

CREATE table testTableA (
  idA INT NOT NULL PRIMARY KEY,
  f2A int NOT NULL,
  f3A VARCHAR (2) NOT NULL
);

CREATE table testTableB (
  idB INT NOT NULL PRIMARY KEY,
  f2B int NOT NULL,
  f3B VARCHAR (3)
);

-- valid UNION ALL queries

CREATE VIEW testUnionAll AS
  SELECT idA as A, f3A as B FROM testTableA WHERE f2A = 1
  UNION ALL
  SELECT idB, f3B FROM testTableB WHERE f2B = 1;

CREATE FUNCTION testUnionAllFunc(id int) AS
  SELECT idA as A, f2A, f3A as B FROM testTableA WHERE f2A = 1
  UNION ALL
  SELECT idB, f2B, f3B FROM testTableB WHERE f2B = $id;

