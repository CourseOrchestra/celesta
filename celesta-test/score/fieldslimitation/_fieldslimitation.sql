create grain fieldslimitation version '1.0';

create table a (
  id int identity not null,
  var varchar(2) not null,
  numb int not null,
  age int not null,
  CONSTRAINT Pk_fieldslimitation_a PRIMARY KEY (id)
);

create view av as
  select id as id, var as var, numb as numb, age as age
  from a;

create materialized view amv as
  select sum(id) as id, var as var, sum(numb) as numb, sum(age) as age
  from a
  group by var;