CREATE GRAIN test1 VERSION '1.0';

CREATE TABLE table1 --single-line comment русские буквы
(
  column1  INT NOT NULL IDENTITY,
  column2  REAL NOT NULL DEFAULT -12323.2,
  c3       BIT NOT NULL DEFAULT 'FALSE',
  PRIMARY KEY (column1, c3, column2),
  aaa      NVARCHAR(23) NOT NULL DEFAULT 'testtes''ttest',
  bbb      NVARCHAR(MAX),
  ccc      IMAGE NULL,
  e        INT DEFAULT -112,
  f        REAL
) 
/*
multi-line comment
русские буквы
*/
;

CREATE TABLE table2(
	column1 INT NOT NULL IDENTITY PRIMARY KEY,
	column2 DATETIME DEFAULT '20111231',
	column3 DATETIME NOT NULL DEFAULT GETDATE(),
	column4 IMAGE DEFAULT 0x22AB15FF,
	column5 INT DEFAULT 11
);
 
CREATE INDEX idx1 ON  table1 (f, e, c3);

CREATE TABLE employees
(
 emp_id nvarchar(11) NOT NULL DEFAULT 'aaa' PRIMARY KEY,
 emp_lname nvarchar(40) NOT NULL DEFAULT 'bbb',
 emp_fname nvarchar(MAX),
 emp_hire_date datetime DEFAULT GETDATE(),
 emp_mgr nvarchar(30)
);

CREATE INDEX table2_idx2 ON table2 (column3, column2);