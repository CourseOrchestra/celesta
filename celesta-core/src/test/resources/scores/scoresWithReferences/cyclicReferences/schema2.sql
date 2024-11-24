create schema b version '1.0';

CREATE SEQUENCE tbNum;

create table tb (
  id int default NEXTVAL(tbNum) not null,
  CONSTRAINT Pk_tbNum PRIMARY KEY (id)
);

create view vb as
  select id from a.ta;
