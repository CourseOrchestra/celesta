create schema java version '1.0';

CREATE SEQUENCE javaSeq;

create table javaTable (
  id int DEFAULT NEXTVAL(javaSeq) not null,
  val INT,
  CONSTRAINT Pk_java_javaTable PRIMARY KEY (id)
);