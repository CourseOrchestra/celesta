create schema manualIntPkToSeqBased version '1.1';

CREATE SEQUENCE idSeq;

create table t (
  id int DEFAULT NEXTVAL(idSeq) not null,
  CONSTRAINT Pk_manualIntPkToSeqBased_t PRIMARY KEY (id)
);
