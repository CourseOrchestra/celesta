CREATE SCHEMA test version '1.0';

create table a(
  id int not null primary key,
  b_id int not null,
  constraint fk_a_b foreign key (b_id) references b(id)
);