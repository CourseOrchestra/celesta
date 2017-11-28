CREATE GRAIN test VERSION '1.0';

CREATE table testTable (
  id INT NOT NULL IDENTITY PRIMARY KEY,
  f1 int NOT NULL,
  f2 int NOT NULL,
  f3 VARCHAR (2) NOT NULL
);

CREATE VIEW testView1 AS
  select sum (f1 * 2 + f2) as sumv, f3 from testTable group by f3;

CREATE VIEW testView2 AS
select count(*) as countv, max(f2) as maxv, min (f1+f2) as minv from testTable;

CREATE VIEW testView3 AS
  select f1, f2 from testTable group by f1, f2;