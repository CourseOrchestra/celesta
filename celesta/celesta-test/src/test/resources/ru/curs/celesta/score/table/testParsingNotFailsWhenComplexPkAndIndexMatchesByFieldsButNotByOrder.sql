CREATE GRAIN test VERSION '1.0';

CREATE table t (
  id INT NOT NULL,
  f1 int NOT NULL,
  f2 int,
  f3 VARCHAR (2),
  CONSTRAINT pk_hFilter PRIMARY KEY (id, f1)
);

CREATE INDEX idx_t_f1_id on t(f1, id);