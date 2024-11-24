CREATE GRAIN test2 VERSION '1.0';

CREATE MATERIALIZED VIEW testView1 AS
  select sum (f1) as sumv, f3 from test.testTable group by f3;