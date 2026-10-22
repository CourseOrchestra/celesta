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

create table childSetNull (
  id int not null,
  parentCode varchar(10),
  parentNum int,
  CONSTRAINT Pk_rename_childSetNull PRIMARY KEY (id)
);

alter table childSetNull add constraint fk_rename_childSetNull_parent
  foreign key (parentCode, parentNum) references parent (code, num)
  on update set null;

create table decimalKey (
  id decimal(10, 2) not null,
  title varchar(20),
  CONSTRAINT Pk_rename_decimalKey PRIMARY KEY (id)
);

create table logged (
  id int not null,
  title varchar(20),
  CONSTRAINT Pk_rename_logged PRIMARY KEY (id)
);

create table noVersion (
  id int not null,
  title varchar(20),
  CONSTRAINT Pk_rename_noVersion PRIMARY KEY (id)
) WITH NO VERSION CHECK;
