create grain grain3 version '1.0';

create sequence c_idc;

create table c (
idc int not null default nextval(c_idc),
descr varchar(2),
idb int foreign key references grain2.b(idb),
aaa varchar(10),
bbb int default 3,
dat blob,
longtext text,
test int not null default 0,
primary key (idc),
doublefield real,
datefield datetime not null default getdate()
--, test2 int not null 
);

/**описание индекса*/
create index idxc on c (datefield, test);