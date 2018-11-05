CREATE GRAIN test VERSION '1.0';

CREATE SEQUENCE testTable_id;

CREATE table testTable (
  id INT NOT NULL DEFAULT NEXTVAL(testTable_id) PRIMARY KEY,
  created datetime
);

CREATE MATERIALIZED VIEW testView1 AS
  select SUM(created) as s from testTable group by id;