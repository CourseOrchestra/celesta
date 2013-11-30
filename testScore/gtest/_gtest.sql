create grain gtest version '1.0';

create table test (
id int identity not null primary key,
attrVarchar nvarchar(2),
attrInt int default 3,
f1 bit not null,
f2 bit default 'true',
f4 real,
f5 real not null default 5.5,
f6 nvarchar(max) not null default 'abc',
f7 nvarchar(8),
f8 datetime default '20130401',
f9 datetime not null default getdate(),
f10 image default 0xFFAAFFAAFF,
f11 image not null
);

create index idxTest on test (attrInt);

create table refTo (
  k1 nvarchar(2) not null,
  k2 int not null,
  descr nvarchar(10),
  primary key (k1, k2)
);

alter table test add constraint fk_testName foreign key (attrVarchar, attrInt) references refTo (k1, k2);