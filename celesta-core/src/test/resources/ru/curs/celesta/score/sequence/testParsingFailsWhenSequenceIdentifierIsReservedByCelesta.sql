CREATE GRAIN test VERSION '1.0';

CREATE SEQUENCE t_seq;

CREATE SEQUENCE t_id;

CREATE TABLE t(
  id int not null default nextval(t_id) primary key
);