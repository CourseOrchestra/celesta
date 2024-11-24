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

-- valid subquery
CREATE VIEW testSubquery AS
  SELECT idA as A, f3A as B FROM testTableA
  WHERE f2A IN (SELECT idB FROM testTableB where f2B = 10);
