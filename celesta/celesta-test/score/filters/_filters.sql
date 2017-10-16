create grain filters version '1.0';

create table aFilter (
  id int identity not null primary key,
  date datetime,
  number1 int,
  number2 int,
  noIndexA int
);

create index idxDateNumb1Numb2 on aFilter (date, number1, number2);

create table bFilter (
  id int identity not null primary key,
  created datetime default GETDATE(),
  numb1 int,
  numb2 int,
  noIndexB int
);

create index idxCreatedNumb1Numb2 on bFilter (created, numb1, numb2);

create table cFilter (
  id int not null primary key
);

create table dFilter (
  id int not null primary key
);


create table eFilter (
  id int not null,
  number int not null,
  str varchar(2) not null,
  CONSTRAINT Pk_filters_e PRIMARY KEY (id, number, str)
);

create table fFilter (
  id int not null,
  numb int not null,
  CONSTRAINT Pk_filters_d PRIMARY KEY (id, numb)
);

create table gFilter (
  id int identity not null primary key,
  createDate datetime default GETDATE(),
  num1 int,
  num2 int,
  noIndexG int
);

create index idxCreateDateNum1 on gFilter (createDate, num1);

create table hFilter (
  id VARCHAR(36) NOT NULL,
  CONSTRAINT pk_hFilter PRIMARY KEY (id)
);

create table iFilter (
  id VARCHAR(36) NOT NULL,
  hFilterId VARCHAR(36) NOT NULL,
  CONSTRAINT pk_iFilter PRIMARY KEY (id, hFilterId)
);

CREATE INDEX idx_iFilter_hFilterId on iFilter(hFilterId);