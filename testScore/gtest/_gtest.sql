create grain gtest version '1.0';

create table test (
id int identity not null primary key,
attrVarchar nvarchar(2),
attrInt int default 3
);

create index idxtest on test (attrInt);