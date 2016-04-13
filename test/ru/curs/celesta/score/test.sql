/**описание гранулы: * grain celestadoc*/
CREATE GRAIN test1 VERSION '1.0';

CREATE TABLE table1 --single-line comment русские буквы
(
  column1  INT NOT NULL IDENTITY,
  column2  REAL NOT NULL DEFAULT -12323.2,
  c3       BIT NOT NULL DEFAULT 'FALSE',
  PRIMARY KEY (column1, c3, column2),
  aaa      VARCHAR(23) NOT NULL DEFAULT 'testtes''ttest',
  bbb      TEXT,
  ccc      BLOB NULL,
  e        INT DEFAULT -112,
  f        REAL,
  f1       INT DEFAULT 4,
  f2       REAL DEFAULT 5.5
) 
/*
multi-line comment
русские буквы
*/
;

/**table2 celestadoc*/
CREATE TABLE table2(
    /**описание первой колонки*/
	column1 INT NOT NULL IDENTITY PRIMARY KEY,
	/**описание второй колонки*/
	column2 DATETIME DEFAULT '20111231',
	column3 DATETIME NOT NULL DEFAULT GETDATE(),
	column4 BLOB DEFAULT 0x22AB15FF,
	column5 INT DEFAULT 11
);
 
 /**описание индекса idx1*/
CREATE INDEX idx1 ON  table1 (aaa, column2);

CREATE TABLE employees
(
 emp_id VARCHAR(11) NOT NULL DEFAULT 'aaa' PRIMARY KEY,
 emp_lname VARCHAR(40) NOT NULL DEFAULT 'bbb',
 emp_fname TEXT,
 emp_hire_date datetime DEFAULT GETDATE(),
 emp_mgr VARCHAR(30)
);

CREATE INDEX table2_idx2 ON table2 (column3, column1);

CREATE TABLE ttt1(
id int
) with read only;

CREATE TABLE ttt2(
id int not null primary key
) with version check;

CREATE TABLE ttt3(
id int not null primary key
) with no version check no autoupdate;