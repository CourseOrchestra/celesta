CREATE SCHEMA schema2 version '1.0';

create table c(
  id int not null primary key,
  d_id int not null,
  constraint fk_c_d foreign key (d_id) references schema1.d(id)
);