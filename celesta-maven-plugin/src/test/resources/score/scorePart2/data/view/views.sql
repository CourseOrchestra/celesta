set schema test;

/*
  {implements: [java.io.Serializable, java.lang.Cloneable]}
 */
create view testTableV AS
  SELECT id, toDelete from testTable;

/*
  {implements: [java.io.Serializable, java.lang.Cloneable]}
 */
create materialized view testTableMv AS
  SELECT count(*) as c, cost from testTable
  GROUP BY cost;