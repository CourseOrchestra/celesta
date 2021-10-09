CREATE GRAIN test VERSION '1.0';

CREATE SEQUENCE testTable_id;

CREATE table testTable (
  id INT NOT NULL DEFAULT NEXTVAL(testTable_id) PRIMARY KEY,
  f1 int NOT NULL,
  f2 int NOT NULL
);

CREATE table testTable2 (
  id INT NOT NULL DEFAULT NEXTVAL(testTable_id) PRIMARY KEY,
  f3 int NOT NULL
);

CREATE MATERIALIZED VIEW testView1 AS
  select SUM(t1.f1) as s,
         t1.f2 from testTable as t1
            inner join testTable2 as t2 on t1.id = t2.id
  group by t1.f2;