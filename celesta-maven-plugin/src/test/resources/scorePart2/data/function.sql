set schema test;

/*
  {implements: [java.io.Serializable, java.lang.Cloneable]}
 */
CREATE FUNCTION testTablePv(p int) AS
  select sum(id) as s from testTable
  where $p > id;