create grain mView version '1.0';

create sequence table1_id;

create table table1 (
  id int not null default nextval(table1_id) primary key,
  numb int,
  date datetime,
  var varchar(2) not null
);

create sequence table2_id;

create table table2 (
  id int not null default nextval(table2_id) primary key,
  numb int,
  date datetime,
  var varchar(2) not null
) with no version check;

create sequence table3_id;

create table table3 (
  id int not null default nextval(table3_id) primary key,
  numb int not null,
  date datetime not null
);

create sequence table4_id;

create table table4 (
  id int not null default nextval(table4_id) primary key,
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

CREATE SEQUENCE table5Num;

create table table5 (
  id int default NEXTVAL(table5Num) not null,
  f1 decimal(4, 2) not null default 24.01,
  f2 decimal(5, 4) not null default 1.0001,
  CONSTRAINT Pk_mView_table5 PRIMARY KEY (id)
);

CREATE materialized view mView6 AS
  select f1, sum(f1) as s1, sum(f2) as s2
  from table5
  group by f1;