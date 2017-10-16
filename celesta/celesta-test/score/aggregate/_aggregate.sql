create grain aggregate version '1.0';

create table countConditionLess (
  id int identity not null primary key,
  date datetime
);

create table countGetDateCond (
  id int identity not null primary key,
  date datetime
);

create view viewCountCondLess as
 select count(*) as c from countConditionLess;

create view viewCountGetDateCond as
 select count(*) as c from countGetDateCond where date > getdate();

create table tableSumOneField (
  id int identity not null primary key,
  f int
);

create view viewSumOneField as
 select sum(f) as s from tableSumOneField;

create view sumFieldAndNumber as
 select sum(f + 1) as s from tableSumOneField;

create view viewSumTwoNumbers as
 select sum(2 + 1) as s from tableSumOneField;

create table tableSumTwoFields (
  id int identity not null primary key,
  f1 int,
  f2 int
);

create view viewSumTwoFields as
  select sum(f1 + f2) as s from tableSumTwoFields;

create table tableMinMax (
  id int identity not null primary key,
  f1 int,
  f2 int
);

create view viewMinOneField as
  select min(f1) as m from tableMinMax;

create view viewMaxOneField as
  select max(f1) as m from tableMinMax;

create view viewMinTwoFields as
  select min(f1 + f2) as m from tableMinMax;

create view viewMaxTwoFields as
  select max(f1 + f2) as m from tableMinMax;

create view viewCountMinMax as
  select count(*) as countv, max(f1) as maxv, min(f2) as minv from tableMinMax;

create table tableGroupBy (
  id int identity not null primary key,
  name varchar(255),
  cost int
);

create view viewGroupByAggregate as
  select name, sum(cost) as s from tableGroupBy group by name;

create view viewGroupBy as
  select name, cost from tableGroupBy group by name, cost;