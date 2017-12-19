CREATE GRAIN test VERSION '1.0';

CREATE TABLE foo (
  CYCLE INT NOT NULL,
  MAXVALUE int,
  MINVALUE int,
  INCREMENT int,
  VERSION int,
  GRAIN int,
  AUTOUPDATE int,
  READ int,
  ONLY int,
  CONSTRAINT pk PRIMARY KEY (CYCLE)
);