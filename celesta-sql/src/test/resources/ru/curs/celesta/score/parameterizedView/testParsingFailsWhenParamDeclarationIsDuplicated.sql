CREATE GRAIN test VERSION '1.0';

CREATE table t1 (
  id INT NOT NULL IDENTITY PRIMARY KEY,
  f1 int,
  f2 int,
  f3 VARCHAR (2)
);

CREATE FUNCTION pView1(p int, p int) AS
  select sum (f1) as sumv, f3 from t1
  where f2 = $p
  group by f3;