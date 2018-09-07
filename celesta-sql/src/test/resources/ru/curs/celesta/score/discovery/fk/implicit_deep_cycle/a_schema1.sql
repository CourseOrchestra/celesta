CREATE SCHEMA schema1 version '1.0';

create table a(
  id int not null primary key,
  b_id int not null,
  constraint fk_a_b foreign key (b_id) references b(id)
);

create table b(
  id int not null primary key,
  c_id int not null,
  constraint fk_b_c foreign key (c_id) references schema2.c(id)
);