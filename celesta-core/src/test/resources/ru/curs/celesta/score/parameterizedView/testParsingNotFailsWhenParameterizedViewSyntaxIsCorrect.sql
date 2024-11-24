CREATE GRAIN test VERSION '1.0';

CREATE SEQUENCE t1_id;

CREATE table t1 (
  id INT NOT NULL DEFAULT NEXTVAL(t1_id) PRIMARY KEY,
  f1 int,
  f2 int,
  f3 VARCHAR (2)
);

CREATE SEQUENCE t2_id;

CREATE table t2 (
  id INT NOT NULL DEFAULT NEXTVAL(t2_id) PRIMARY KEY,
  ff1 int,
  ff2 int,
  ff3 VARCHAR (2)
);

CREATE FUNCTION pView1(p int) AS
  select sum (f1) as sumv, f3 as f3
  from t1 as t1
  where f2 = $p
  group by f3;

CREATE FUNCTION pView2(p1 int,/**TEST*/ p2 varchar) AS
  select f1, f2, f3 from t1
  where f2 = $p1 AND f3 = $p2;

CREATE FUNCTION pView3(p int) AS
  select count(*) as c from t1
  LEFT JOIN t2 ON f2 = ff2
  WHERE f2 = $p;