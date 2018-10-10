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
f11 blob not null,
f12 decimal(11, 7),
f13 decimal(5, 3) not null default 46.123,
f14 datetime with time zone
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

create table tableForMatView (
  id int identity not null primary key,
  f1 varchar(2) not null,
  f2 int not null default 3,
  f3 bit not null,
  f4 real not null,
  f5 datetime not null,
  f6 decimal(2, 1) not null
);

create materialized view mView1gTest as
  select sum(id) as idsum, f1, f2, f3, f4, f5, f6
  from tableForMatView
  group by f1, f2, f3, f4, f5, f6;

create table tableForInitMvData (
  id int identity not null primary key,
  var varchar(2) not null,
  numb int,
  d datetime not null
);

create materialized view mViewForInit as
  select sum(numb) as s, var, d
  from tableForInitMvData
  group by var, d;

create function pView(p int) as
  select id from test
  where id = $p;

create table testInFilterClause (
  id int identity not null primary key,
  atVarchar varchar(2),
  atInt int default 3
);

CREATE SEQUENCE testSequence START WITH 5;
CREATE SEQUENCE testSequence2 START WITH 5;

CREATE TABLE tableForTestSequence(
  id int not null DEFAULT NEXTVAL(testSequence),
  numb int,
  CONSTRAINT Pk_gtest_tableForTestSequence PRIMARY KEY (id)
);


CREATE SEQUENCE tForAddSecondMvNum;

CREATE TABLE tForAddSecondMv (
  id int not null DEFAULT NEXTVAL(tForAddSecondMvNum),
  num1 int not null,
  num2 int not null,
  CONSTRAINT Pk_gtest_tForAddSecondMv PRIMARY KEY (id)
);

CREATE MATERIALIZED VIEW tForAddSecondMv1 AS
  select sum(num1) as s, num2
  from tForAddSecondMv
  group by num2;

