create grain g3 version '1.0';

create table c (
idc int identity not null ,
descr varchar(2),
idb int foreign key references g2.b(idb),
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