create grain testGrain version '1.0';

create table testTable (
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

create index idxTest on testTable (f1);

create index idxTest2 on testTable (f7, f1);

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

alter table testTable add constraint fk_testNameVeryVeryLongLonName
 foreign key (attrVarchar, attrInt) references refTo (k1, k2)
 on update cascade on delete set null;
 
create view testView as
  select id, descr, descr || 'foo' as descr2, k2 from testTable inner join refTo on attrVarchar = k1 and attrInt = k2;

create view testView2 as
  select id, descr from testTable t1 inner join refTo t2 on attrVarchar = k1 and not t2.descr is null and attrInt = k2;

create view testView3 as select 1 as a, /**test celestadoc*/1.4 as b,
  /**test celestadoc2*/1 as c, 1 as d, 1 as e, 1 as f, 1 as g, 1 as h, 1 as j, 1 as k
  from testTable;
  
create view testView4 as select f1, f4, f5, f4 + f5 as s, f5 * f5 + 1 as s2 from testTable where f1 = true;

create view testView5 as select attrVarchar as foo, f7 as bar, attrVarchar || f7 as baz from testTable;

create view testView6 as select -attrInt as foo from testTable
where attrInt in (2, 3);


