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