create grain filters version '1.0';

create table aFilter (
  id int identity not null primary key,
  date datetime,
  number int,
  description varchar(10),
  noIndexA int,
  a1 int,
  a2 int
);

create index idxDate on aFilter (date);
create index idxDateNumb on aFilter (date, number);
create index idxDateNumbDesc on aFilter (date, number, description);
create index idxA on aFilter (a1, a2, number);

create table bFilter (
  id int identity not null primary key,
  created datetime default GETDATE(),
  numb int,
  title varchar(10),
  noIndexA int,
  b1 int,
  b2 int
);

create index idxCreated on bFilter (created);
create index idxCreatedNumb on bFilter (created, numb);
create index idxCreatedNumbTitle on bFilter (created, numb, title);
create index idxB on bFilter (b1, b2, numb);
