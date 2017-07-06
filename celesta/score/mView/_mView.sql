create grain mView version '1.0';

create table table1 (
  id int identity not null primary key,
  numb int,
  date datetime,
  var varchar(2) not null
);

create materialized view mView1 as
   select var, sum(numb) as s
   FROM mView.table1
   group by var;