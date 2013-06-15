create table c (
iii int not null identity primary key,
bbb nvarchar(2)
);

create table d (
e nvarchar(5) not null default '-',
primary key (e)
);

create table a (
 a int not null default 0,
 b nvarchar(5) not null default '',
 c datetime,
 d int,
 kk nvarchar(5)  foreign key references d(e) on update set null, 
 foreign key(d) references c(iii),
 primary key (a, b)
 );


create table b (
 a nvarchar(5) not null default '' primary key,
 b int,
 c int,
 foreign key (b, a) references a(a, b) on delete cascade on update cascade
 );


