create grain gtest version '1.0';

create table test (
id int identity not null primary key,
attrVarchar varchar(2),
attrInt int default 3,
f1 bit not null,
f2 bit default true,
f4 real,
f5 real not null default 5.5,
f6 text not null default 'abc',
f7 varchar(8),
f8 datetime default '20130401',
f9 datetime not null default getdate(),
f10 blob default 0xFFAAFFAAFF,
f11 blob not null
);

create index idxTest on test (f1);

create index idxTest2 on test (f7, f1);

create table refTo (
  k1 varchar(2) not null,
  k2 int not null,
  descr varchar(10),
  primary key (k1, k2)
);

create table refTo2(
 f1 bit not null primary key
);

create table  aLongIdentityTableNaaame(
 f1 int identity not null primary key,
 field2 bit foreign key references refTo2(f1),
 field3 bit foreign key references refTo2(f1),
 aFieldwithAVeryVeryLongName bit
);

alter table test add constraint fk_testNameVeryVeryLongLonName
 foreign key (attrVarchar, attrInt) references refTo (k1, k2)
 on update cascade on delete set null;
 
create view testview as 
  select id, descr, descr || 'foo' as descr2, k2 from test inner join refTo on attrVarchar = k1 and attrInt = k2;
  
create view testview2 as 
  select id, descr from test t1 inner join refTo t2 on attrVarchar = k1 and not t2.descr is null and attrInt = k2;  

create view testGetDateView as
 select id from test where f8 > getdate();

create view testCountView AS
 select count(*) as c from test;

create view testSumView AS
 select sum(f5) as s from test;

create view testMaxView AS
 select max(f5) as s from test;

create view testMinView AS
 select min(f5) as s from test;

create view testGroupByView AS
 select max(f5) as m, f8 from test GROUP BY f8;

create view v3 as select 1 as a, /**test celestadoc*/1.4 as b, /**test celestadoc2*/1 as c, 1 as d, 1 as e, 1 as f, 1 as g, 1 as h, 1 as j, 1 as k
  from test;
  
create view v4 as select f1, f4, f5, f4 + f5 as s, f5 * f5 + 1 as s2 from test where f1 = true;

create view v5 as select attrVarchar as foo, f7 as bar, attrVarchar || f7 as baz from test;

create table tableForMatView (
  id int identity not null primary key,
  f1 varchar(2) not null,
  f2 int not null default 3,
  f3 bit not null,
  f4 real not null,
  f5 datetime not null
);

create materialized view mView1gTest as
  select sum(id) as idsum, f1, f2, f3, f4, f5
  from tableForMatView
  group by f1, f2, f3, f4, f5;

create table tableForInitMvData (
  id int identity not null primary key,
  var varchar(2) not null,
  numb int
);

create materialized view mViewForInit as
  select sum(numb) as s, var
  from tableForInitMvData
  group by var;