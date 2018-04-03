set schema test;

/*
  {implements: [java.io.Serializable, java.lang.Cloneable]}
 */
create view testTableV AS
  SELECT id from testTable;

/*
  {implements: [java.io.Serializable, java.lang.Cloneable]}
 */
create materialized view testTableMv AS
  SELECT count(*) as c from testTable;