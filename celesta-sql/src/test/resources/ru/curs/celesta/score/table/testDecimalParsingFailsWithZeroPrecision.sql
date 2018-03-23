CREATE GRAIN test VERSION '1.0';

CREATE table t (
  id INT NOT NULL,
  CONSTRAINT pk1 PRIMARY KEY (id),
  cost decimal(0, 0)
);