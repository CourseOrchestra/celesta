create grain g3 version '1.0';

create table c (
idc int identity not null ,
descr nvarchar(2),
idb int foreign key references g2.b(idb),
aaa nvarchar(10),
bbb int default 3,
dat image,
test int not null default 0,
primary key (idc)
--, test2 int not null
);

/**описание индекса*/
create index idxc on c (descr, aaa);