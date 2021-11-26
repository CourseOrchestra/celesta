create grain pView version '1.0';

CREATE SEQUENCE t1_id;

CREATE table t1 (
  id INT NOT NULL DEFAULT NEXTVAL(t1_id) PRIMARY KEY,
  f1 int,
  f2 int,
  f3 VARCHAR (2),
  f4 REAL NOT NULL DEFAULT 0
);

CREATE SEQUENCE t2_id;

CREATE table t2 (
  id INT NOT NULL DEFAULT NEXTVAL(t2_id) PRIMARY KEY,
  ff1 int,
  ff2 int,
  ff3 VARCHAR (2),
  ff4 decimal(4, 2) not null default 24.01,
  ff5 decimal(5, 4) not null default 1.0001
);

CREATE FUNCTION pView1(p int) AS
  select sum (f1) as sumv, f3 as f3
  from t1 as t1
  where f2 = $p
  group by f3;

CREATE FUNCTION pView2(param int,/**TEST*/ param2 varchar) AS
  select f1, f2, f3 from t1
  where f2 = $param AND f3 = $param2 AND f3 = $param2;

CREATE FUNCTION pView3(p int) AS
  select count(*) as c from t1
  LEFT JOIN t2 ON f2 = ff2
  WHERE f2 = $p;

CREATE SEQUENCE t3Num;

create table t3 (
  id int default NEXTVAL(t3Num) not null,
  f1 decimal(4, 2) not null default 24.01,
  f2 decimal(5, 4) not null default 1.0001,
  CONSTRAINT Pk_pView_t3Num PRIMARY KEY (id)
);

CREATE FUNCTION pView4(p decimal) AS
  select sum(f1) as f1, sum(f2) as f2, sum(f1 + f2) as f12 from t3
  WHERE f2 > $p;

CREATE FUNCTION pView5(p int) AS
  select sum(f2 * f4) as c from t1
  where f1 = $p;
