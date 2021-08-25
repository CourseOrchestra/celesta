create schema s1 version '1.0';

create table header (
  id int not null primary key
);

create table line (
  id int not null,
  header_id int not null foreign key references header(id),
  CONSTRAINT pk_line PRIMARY KEY (id, header_id)
);

create sequence seq1;

create materialized view linecount as
    select header_id, sum(id) as line_count from line group by header_id;