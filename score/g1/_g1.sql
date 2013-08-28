create grain g1 version '1.0';

create table aa(
idaa int not null primary key,  
idc int, -- foreign key references g2.b(idb)
textvalue nvarchar(10)
);

create table a (
ida int identity not null primary key,
descr nvarchar(2)
);
