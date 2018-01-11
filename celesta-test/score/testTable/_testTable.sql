create grain testTable version '1.0';

CREATE SEQUENCE tBlobNum;

create table tBlob (
  id int default NEXTVAL(tBlobNum) not null,
  dat blob,
  CONSTRAINT Pk_testTable_tableWithBlob PRIMARY KEY (id)
);