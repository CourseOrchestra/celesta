create schema a version '1.0';

CREATE SEQUENCE taNum;

create table ta (
  id int default NEXTVAL(taNum) not null,
  CONSTRAINT Pk_taNum PRIMARY KEY (id)
);

create view va as
  select id from b.tb;
