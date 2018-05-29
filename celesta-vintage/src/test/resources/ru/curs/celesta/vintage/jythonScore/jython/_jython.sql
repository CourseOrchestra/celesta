create schema jython version '1.0';

CREATE SEQUENCE jythonSeq;

create table jythonTable (
  id int DEFAULT NEXTVAL(jythonSeq) not null,
  val INT,
  CONSTRAINT Pk_jython_jythonTable PRIMARY KEY (id)
);
