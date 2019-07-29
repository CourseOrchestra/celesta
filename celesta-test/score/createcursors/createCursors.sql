CREATE GRAIN createCursors VERSION '1.0';


CREATE SEQUENCE wtable_id;

CREATE TABLE wtable
(
  id   INT NOT NULL DEFAULT NEXTVAL(wtable_id) PRIMARY KEY,
  data VARCHAR(8)
);


CREATE SEQUENCE roTable_id;

CREATE TABLE roTable
(
  id   INT NOT NULL DEFAULT NEXTVAL(roTable_id),
  data VARCHAR(8)
)
WITH READ ONLY;


CREATE VIEW wtableDataView AS
  SELECT data FROM wtable;


CREATE SEQUENCE mvtable_id;

CREATE TABLE mvtable
(
  id   INT NOT NULL DEFAULT NEXTVAL(mvtable_id) PRIMARY KEY,
  data VARCHAR(8) NOT NULL
);

CREATE MATERIALIZED VIEW mvtableMView AS
   SELECT data, count(*) AS d
   FROM mvtable
   GROUP BY data;


CREATE SEQUENCE pvtable_id;

CREATE TABLE pvtable
(
  id   INT NOT NULL DEFAULT NEXTVAL(pvtable_id) PRIMARY KEY,
  data INT NOT NULL
);

CREATE FUNCTION pvtablePView(d INT) AS
  SELECT data FROM pvtable
  WHERE data < $d;


CREATE SEQUENCE crCurSeq START WITH 3;
