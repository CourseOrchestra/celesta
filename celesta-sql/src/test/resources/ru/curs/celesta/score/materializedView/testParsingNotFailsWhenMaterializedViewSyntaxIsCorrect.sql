CREATE GRAIN test VERSION '1.0';

CREATE table testTable (
  id INT NOT NULL IDENTITY PRIMARY KEY,
  f1 int NOT NULL,
  f2 int NOT NULL,
  f3 VARCHAR (2) NOT NULL
);

CREATE MATERIALIZED VIEW testView1 AS
  select sum (f1) as sumv, f3 from testTable group by f3;

CREATE MATERIALIZED VIEW testView2 AS
select count(*) as c from testTable;

CREATE MATERIALIZED VIEW testView3 AS
  select f1, f2, count(*) as id from testTable group by f1, f2;