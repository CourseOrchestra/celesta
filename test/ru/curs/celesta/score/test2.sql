create grain test2 version '2.5';

create table c (
iii int not null identity primary key,
bbb varchar(2),
sss int -- identity (no more than one identity field is allowed!)
);

create table d (
e varchar(5) not null default '-',
primary key (e)
);

create table a (
 a int not null default 0,
/**a celestadoc*/
 b varchar(5) not null default '',
 c datetime,
 d int,
 kk varchar(5)  foreign key references d(e) on update set null, 
 foreign key(d) references c(iii),
 primary key (a, b)
 );


create table b (
 a varchar(5) not null default '' primary key,
 b int,
 c int,
 foreign key (b, a) references a(a, b) on delete cascade on update cascade
 );


