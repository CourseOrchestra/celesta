CREATE GRAIN test VERSION '1.0';

CREATE table testTable (
  id INT NOT NULL IDENTITY PRIMARY KEY,
  f1 int NOT NULL,
  f2 VARCHAR (2) NOT NULL
);

CREATE VIEW testView AS
  SELECT sum(f2) as sumv, f1, id FROM testTable GROUP BY f1;