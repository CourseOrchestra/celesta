CREATE SCHEMA oraTablePK1 version '1.0';

CREATE SEQUENCE table1_id;

CREATE TABLE table1 (
  id INT NOT NULL DEFAULT NEXTVAL(table1_id) PRIMARY KEY
);


CREATE SEQUENCE table2_id;

CREATE TABLE table2 (
  id INT NOT NULL DEFAULT NEXTVAL(table2_id),
  CONSTRAINT pk_table2_id PRIMARY KEY (id)
);
