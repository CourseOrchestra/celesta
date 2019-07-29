CREATE SCHEMA oraTablePK1 version '1.0';

/* The sequence names have to be kept globally unique otherwise they clashe in SQLServer
   TODO: reuse local naming after this issue is resolved.
 */

CREATE SEQUENCE oraTPK1_table1_id;

CREATE TABLE table1 (
  id INT NOT NULL DEFAULT NEXTVAL(oraTPK1_table1_id) PRIMARY KEY
);


CREATE SEQUENCE oraTPK1_table2_id;

CREATE TABLE table2 (
  id INT NOT NULL DEFAULT NEXTVAL(oraTPK1_table2_id),
  CONSTRAINT pk_table2_id PRIMARY KEY (id)
);
