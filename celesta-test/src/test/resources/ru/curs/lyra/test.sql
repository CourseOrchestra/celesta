CREATE GRAIN testlyra VERSION '1.0';

CREATE SEQUENCE table1_column1;

CREATE TABLE table1 
(
  /** {"caption": "длинный кириллический текст",
       "visible": false}*/
  column1 INT NOT NULL DEFAULT NEXTVAL(table1_column1) PRIMARY KEY,
  /** {"caption": "текст с \"кавычками\"",
       "editable": false,
       "visible": true}*/
  column2  REAL NOT NULL DEFAULT -12323.2,
  column3 BIT NOT NULL DEFAULT 'FALSE'
 ) ;