SET SCHEMA schema1;

create table d(
  id int not null primary key,
  b_id int not null,
  constraint fk_d_b foreign key (b_id) references b(id)
);