create grain fieldslimitation version '1.0';

create table a (
  id int identity not null primary key,
  var varchar(2) not null,
  numb int,
  age int
);

create view av as
  select id as i, var as v, numb as n, age as a
  from a;

create materialized view amv as
  select sum(numb) as s, var
  from a
  group by var;