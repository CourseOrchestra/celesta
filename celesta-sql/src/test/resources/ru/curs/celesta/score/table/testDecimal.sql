CREATE GRAIN test VERSION '1.0';

CREATE table t1 (
  id INT NOT NULL,
  cost decimal(1, 0) default 4.0,
  CONSTRAINT pk1 PRIMARY KEY (id)
);

CREATE table t2 (
  id INT NOT NULL,
  cost decimal(38, 0),
  CONSTRAINT pk2 PRIMARY KEY (id)
);

CREATE table t3 (
  id INT NOT NULL,
  cost decimal(38, 37) DEFAULT 0.1234,
  CONSTRAINT pk3 PRIMARY KEY (id)
);

CREATE table t4 (
  id INT NOT NULL,
  cost decimal(5, 4) not null DEFAULT 1.134,
  CONSTRAINT pk PRIMARY KEY (id)
);
