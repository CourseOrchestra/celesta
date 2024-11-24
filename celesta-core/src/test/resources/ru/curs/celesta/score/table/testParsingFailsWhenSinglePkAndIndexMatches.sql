CREATE GRAIN test VERSION '1.0';

CREATE SEQUENCE t_id;

CREATE table t (
  id INT NOT NULL DEFAULT NEXTVAL(t_id) PRIMARY KEY,
  f1 int,
  f2 int,
  f3 VARCHAR (2)
);

CREATE INDEX idx_t_id on t(id);