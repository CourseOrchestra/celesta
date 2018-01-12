create grain grain2 version '1.0';


create table b (
idb int identity not null primary key,
descr varchar(2),
ida int foreign key references grain1.a(ida)
);