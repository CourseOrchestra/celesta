CREATE GRAIN test VERSION '1.0';

CREATE SEQUENCE t1_id;

CREATE table t1 (
  id INT NOT NULL DEFAULT NEXTVAL(t1_id) PRIMARY KEY,
  f1 int,
  f2 int,
  f3 VARCHAR (2)
);

CREATE FUNCTION pView1() AS
  select sum (f1) as sumv, f3 from t1
  where f2 = f1;