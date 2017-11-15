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

CREATE TABLE table1(
  column1 INT NOT NULL IDENTITY,
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
