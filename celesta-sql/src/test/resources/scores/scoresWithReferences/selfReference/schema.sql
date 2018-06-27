create schema a version '1.0';

create table t(
 id INT NOT NULL PRIMARY KEY
);

create view idFromBT as
  select id from a.t;