create grain navigation version '1.0';

CREATE SEQUENCE navigationTable_id;

CREATE TABLE navigationTable(
  id INT NOT NULL DEFAULT NEXTVAL(navigationTable_id),
  numb INT NOT NULL,
  CONSTRAINT Pk_navigationTable PRIMARY KEY (id)
);