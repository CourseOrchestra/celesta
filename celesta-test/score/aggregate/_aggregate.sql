create grain aggregate version '1.1';

create sequence countConditionLess_id;

create table countConditionLess (
  id int not null default nextval(countConditionLess_id) primary key,
  date datetime
);

create sequence countGetDateCond_id;

create table countGetDateCond (
  id int not null default nextval(countGetDateCond_id) primary key,
  date datetime
);

create view viewCountCondLess as
 select count(*) as c from countConditionLess;

create view viewCountGetDateCond as
 select count(*) as c from countGetDateCond where date > getdate();

create sequence tableSumOneField_id;

create table tableSumOneField (
  id int not null default nextval(tableSumOneField_id) primary key,
  f int
);

create view viewSumOneField as
 select sum(f) as s from tableSumOneField;

create view sumFieldAndNumber as
 select sum(f + 1) as s from tableSumOneField;

create view viewSumTwoNumbers as
 select sum(2 + 1) as s from tableSumOneField;

create sequence tableSumTwoFields_id;

create table tableSumTwoFields (
  id int not null default nextval(tableSumTwoFields_id) primary key,
  f1 int,
  f2 int
);

create view viewSumTwoFields as
  select sum(f1 + f2) as s from tableSumTwoFields;

create sequence tableMinMax_id;

create table tableMinMax (
  id int not null default nextval(tableMinMax_id) primary key,
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

create sequence tableGroupBy_id;

create table tableGroupBy (
  id int not null default nextval(tableGroupBy_id) primary key,
  name varchar(255),
  cost int
);

create view viewGroupByAggregate as
  select name, sum(cost) as s from tableGroupBy group by name;

create view viewGroupBy as
  select name, cost from tableGroupBy group by name, cost;


CREATE SEQUENCE tWithDecimalNum;

create table tWithDecimal (
  id int default NEXTVAL(tWithDecimalNum) not null,
  f1 decimal(4, 2) not null default 24.01,
  f2 decimal(5, 4) not null default 1.0001,
  CONSTRAINT Pk_aggregate_tWithDecimal PRIMARY KEY (id)
);

create view viewWithDecimal as
  select sum(f1) as f1, sum(f2) as f2, sum(f1 + f2) as f12 from tWithDecimal;