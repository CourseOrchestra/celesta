CREATE TABLE table1 --single-line comment
(
  column1  INT NULL,
  column2  REAL NOT NULL DEFAULT -12323.2,
  c3       BIT NOT NULL DEFAULT 'FALSE',
  aaa      NVARCHAR(23) NOT NULL DEFAULT 'testtes''ttest',
  bbb      NVARCHAR(MAX),
  ccc      IMAGE,
  e        INT DEFAULT -112,
  f        REAL
) 
/*
multi-line comment
*/
;

CREATE TABLE table2(
	column1 INT NOT NULL IDENTITY,
	column2 DATETIME DEFAULT '20111231',
	column3 DATETIME NOT NULL DEFAULT GETDATE()
);
 
CREATE TABLE employees
(
 emp_id nvarchar(11) NOT NULL DEFAULT 'aaa',
 emp_lname nvarchar(40) NOT NULL DEFAULT 'bbb',
 emp_fname nvarchar(MAX),
 emp_hire_date datetime DEFAULT GETDATE(),
 emp_mgr nvarchar(30)
);