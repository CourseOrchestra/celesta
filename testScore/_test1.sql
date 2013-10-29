/**описание гранулы: * grain celestadoc*/
CREATE GRAIN test1 VERSION '2.0';

-- *** TABLES ***
/**описание гранулы: * grain celestadoc*/
CREATE TABLE table1(
  column1 INT NOT NULL IDENTITY,
  column2 REAL NOT NULL DEFAULT -12323.2,
  c3 BIT NOT NULL DEFAULT 'FALSE',
  aaa NVARCHAR(23) NOT NULL DEFAULT 'testtes''ttest',
  bbb NVARCHAR(MAX),
  ccc IMAGE,
  e INT DEFAULT -112,
  f REAL
  PRIMARY KEY (column1, c3, column2);
);

/**table2 celestadoc*/
CREATE TABLE table2(
  /**описание первой колонки*/
  column1 INT NOT NULL IDENTITY,
  /**описание второй колонки*/
  column2 DATETIME DEFAULT '20111231',
  column3 DATETIME NOT NULL DEFAULT GETDATE(),
  column4 IMAGE DEFAULT 0x22AB15FF,
  column5 INT DEFAULT 11
  PRIMARY KEY (column1);
);

CREATE TABLE employees(
  emp_id NVARCHAR(11) NOT NULL DEFAULT 'aaa',
  emp_lname NVARCHAR(40) NOT NULL DEFAULT 'bbb',
  emp_fname NVARCHAR(MAX),
  emp_hire_date DATETIME DEFAULT GETDATE(),
  emp_mgr NVARCHAR(30)
  PRIMARY KEY (emp_id);
);

-- *** FOREIGN KEYS ***
-- *** INDICES ***
/**описание индекса idx1*/
CREATE INDEX idx1 ON table1(f, e, c3);
CREATE INDEX table2_idx2 ON table2(column3, column2);
