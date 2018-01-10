CREATE GRAIN test VERSION '1.0';

CREATE table t (
  id INT NOT NULL IDENTITY PRIMARY KEY,
  f1 int,
  f2 int,
  f3 VARCHAR (2)
);

CREATE INDEX idx_t_id on t(id);