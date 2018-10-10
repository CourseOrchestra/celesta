/**описание гранулы: * grain celestadoc*/
CREATE SCHEMA test1 VERSION '2.0';

CREATE SEQUENCE table1_column1;

-- *** TABLES ***
CREATE TABLE table1(
  column1 INT NOT NULL DEFAULT NEXTVAL(table1_column1),
  column2 REAL NOT NULL DEFAULT -12323.2,
  c3 BIT NOT NULL DEFAULT 'FALSE',
  aaa VARCHAR(23) NOT NULL DEFAULT 'testtes''ttest',
  bbb TEXT,
  ccc BLOB,
  e INT DEFAULT -112,
  f REAL,
  f1 INT DEFAULT 4,
  f2 REAL DEFAULT 5.5,
  CONSTRAINT pk_table1 PRIMARY KEY (column1, c3, column2)
);

CREATE SEQUENCE table2_column1;

/**table2 celestadoc*/
CREATE TABLE table2(
  /**описание первой колонки*/
  column1 INT NOT NULL DEFAULT NEXTVAL(table2_column1),
  /**описание второй колонки*/
  column2 DATETIME DEFAULT '20111231',
  column3 DATETIME NOT NULL DEFAULT GETDATE(),
  column4 BLOB DEFAULT 0x22AB15FF,
  column5 INT DEFAULT 11,
  CONSTRAINT pk_table2 PRIMARY KEY (column1)
);

CREATE TABLE employees(
  emp_id VARCHAR(11) NOT NULL DEFAULT 'aaa',
  emp_lname VARCHAR(40) NOT NULL DEFAULT 'bbb',
  emp_fname TEXT,
  emp_hire_date DATETIME DEFAULT GETDATE(),
  emp_mgr VARCHAR(30),
  CONSTRAINT pk_employees PRIMARY KEY (emp_id)
);

CREATE TABLE ttt1(
  /**{option: [open, closed]}*/
  id INT) WITH READ ONLY;

CREATE TABLE ttt2(
  id INT NOT NULL,
  /**{option: [one, two, three]}*/
  descr VARCHAR(10),
  CONSTRAINT pk_ttt2 PRIMARY KEY (id)
);

CREATE TABLE ttt3(
  id INT NOT NULL,
  CONSTRAINT pk_ttt3 PRIMARY KEY (id)
) WITH NO VERSION CHECK NO AUTOUPDATE;

-- *** FOREIGN KEYS ***
-- *** INDICES ***
/**описание индекса idx1*/
CREATE INDEX idx1 ON table1(aaa, column2);
CREATE INDEX table2_idx2 ON table2(column3, column1);
-- *** VIEWS ***
-- *** MATERIALIZED VIEWS ***
-- *** PARAMETERIZED VIEWS ***
