create grain g3 version '1.0';

create table c (
idc int identity not null primary key,
descr nvarchar(2),
idb int foreign key references g2.b(idb)
);