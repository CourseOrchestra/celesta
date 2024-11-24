CREATE GRAIN test VERSION '1.0';

CREATE SEQUENCE testTable_id;

CREATE table testTable (
  id INT NOT NULL DEFAULT NEXTVAL(testTable_id) PRIMARY KEY,
  f1 int NOT NULL,
  f2 VARCHAR (2) NOT NULL
);

CREATE VIEW testView AS
  SELECT sum(f2) as sumv, f1 FROM testTable GROUP BY f1;