CREATE GRAIN test VERSION '1.0';

CREATE table testTable (
  id INT NOT NULL IDENTITY PRIMARY KEY,
  created datetime
);

CREATE MATERIALIZED VIEW testView1 AS
  select SUM(created) as s from testTable group by id;