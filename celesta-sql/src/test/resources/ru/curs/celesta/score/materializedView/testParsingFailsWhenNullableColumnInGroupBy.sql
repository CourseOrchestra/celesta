CREATE GRAIN test VERSION '1.0';

CREATE SEQUENCE testTable_id;

CREATE table testTable (
  id INT NOT NULL DEFAULT NEXTVAL(testTable_id) PRIMARY KEY,
  f1 int NOT NULL,
  f2 int NOT NULL,
  f3 VARCHAR (2)
);

CREATE MATERIALIZED VIEW testView1 AS
  select MAX(f1), f3 from testTable group by f3;