create grain testTable version '1.0';

CREATE SEQUENCE tBlobNum;

create table tBlob (
  id int default NEXTVAL(tBlobNum) not null,
  dat blob,
  CONSTRAINT Pk_testTable_tBlob PRIMARY KEY (id)
);


CREATE SEQUENCE tXRecNum;

create table tXRec (
  id int default NEXTVAL(tXRecNum) not null,
  num int,
  cost real,
  title varchar(10),
  isActive bit,
  created datetime,
  CONSTRAINT Pk_testTable_tXRec PRIMARY KEY (id)
);


CREATE SEQUENCE tCsvLineNum;

create table tCsvLine (
  id int default NEXTVAL(tCsvLineNum) not null,
  title varchar(10),
  CONSTRAINT Pk_testTable_tCsvLine PRIMARY KEY (id)
);

CREATE SEQUENCE tIterateNum;

create table tIterate (
  id int default NEXTVAL(tIterateNum) not null,
  CONSTRAINT Pk_testTable_tIterate PRIMARY KEY (id)
);

CREATE SEQUENCE tCopyFieldsNum;

create table tCopyFields (
  id int default NEXTVAL(tCopyFieldsNum) not null,
  title varchar(10),
  CONSTRAINT Pk_testTable_tCopyFields PRIMARY KEY (id)
);

CREATE SEQUENCE tLimitNum;

create table tLimit (
  id int default NEXTVAL(tLimitNum) not null,
  CONSTRAINT Pk_testTable_tLimit PRIMARY KEY (id)
);