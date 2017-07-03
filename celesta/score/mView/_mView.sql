create grain mView version '1.1';

create table table1 (
  id int identity not null primary key,
  numb int,
  date datetime,
  var varchar(2) not null
);

create materialized view mView1 as
   select id, var, max(numb) as s
   FROM table1
   group by id, var;