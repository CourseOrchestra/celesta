create schema manualIntPkToSeqBased version '1.0';

create table t (
  id int not null,
  CONSTRAINT Pk_manualIntPkToSeqBased_t PRIMARY KEY (id)
);
