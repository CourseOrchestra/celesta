create grain g2 version '1.0';


create table b (
idb int identity not null primary key,
descr nvarchar(2),
ida int foreign key references g1.a(ida)
);