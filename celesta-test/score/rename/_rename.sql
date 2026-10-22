create grain rename version '1.0';

create table simple (
  id int not null,
  title varchar(20),
  CONSTRAINT Pk_rename_simple PRIMARY KEY (id)
);

create table parent (
  code varchar(10) not null,
  num int not null,
  title varchar(20),
  CONSTRAINT Pk_rename_parent PRIMARY KEY (code, num)
);

create table child (
  id int not null,
  parentCode varchar(10),
  parentNum int,
  CONSTRAINT Pk_rename_child PRIMARY KEY (id)
);

alter table child add constraint fk_rename_child_parent
  foreign key (parentCode, parentNum) references parent (code, num)
  on update cascade;

create table noVersion (
  id int not null,
  title varchar(20),
  CONSTRAINT Pk_rename_noVersion PRIMARY KEY (id)
) WITH NO VERSION CHECK;
