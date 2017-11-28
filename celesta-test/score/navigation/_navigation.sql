create grain navigation version '1.0';

CREATE TABLE navigationTable(
  id INT NOT NULL IDENTITY,
  numb INT NOT NULL,
  CONSTRAINT Pk_navigationTable PRIMARY KEY (id)
);