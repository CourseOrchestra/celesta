create grain mView version '1.0';

create table table1 (
  id int identity not null primary key,
  numb int,
  date datetime,
  var varchar(2) not null
);

create table table2 (
  id int identity not null primary key,
  numb int,
  date datetime,
  var varchar(2) not null
) with no version check;

create table table3 (
  id int identity not null primary key,
  numb int not null,
  date datetime not null
);

create table table4 (
  id int identity not null primary key,
  var1 VARCHAR (2) not null,
  var2 VARCHAR (2) not null,
  numb int
) with no version check;

create materialized view mView1 as
   select var, sum(numb) as s, count(*) as c
   FROM mView.table1
   group by var;

create materialized view mView2 AS
  select var as v, sum(numb) as s
  FROM mView.table1
  group by v;


create materialized view mView3 as
   select var, sum(numb) as s, count(*) as c
   FROM mView.table2
   group by var;

create materialized view mView4 as
   select date, sum(numb) as s
   FROM mView.table3
   group by date;

create materialized view mView5 AS
  select var1, var2 as vvv, sum(numb) as s
  from table4
  group by var1, vvv;
