create grain gtest version '1.0';

create table test (
id int identity not null primary key,
attrVarchar varchar(2),
attrInt int default 3,
f1 bit not null,
f2 bit default 'true',
f4 real,
f5 real not null default 5.5,
f6 text not null default 'abc',
f7 varchar(8),
f8 datetime default '20130401',
f9 datetime not null default getdate(),
f10 blob default 0xFFAAFFAAFF,
f11 blob not null
);

create index idxTest on test (attrInt);

create table refTo (
  k1 varchar(2) not null,
  k2 int not null,
  descr varchar(10),
  primary key (k1, k2)
);

create table refTo2(
 f1 bit not null primary key
);

create table test2(
 f1 int identity not null primary key,
 field2 bit foreign key references refTo2(f1),
 field3 bit foreign key references refTo2(f1)
);

alter table test add constraint fk_testName foreign key (attrVarchar, attrInt) references refTo (k1, k2)
 on update cascade on delete set null;
 
create view testview as 
  select id, descr, descr || 'foo' as descr2 from test inner join refTo on attrVarchar = k1 and attrInt = k2;