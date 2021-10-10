set schema test;

/**
  {implements: [java.io.Serializable, java.lang.Cloneable]}
 */
create table testTable(
  id int  DEFAULT NEXTVAL(seq) NOT NULL,
  /**{option: [one, two, three]}*/
  str VARCHAR(4),
  deleted bit,
  /**{option: [1.5, 2.5, 3.6]}*/
  weight real,
  content text,
  created datetime,
  rawData blob,
  cost decimal(4,2) not null,
  toDelete datetime with time zone,
  CONSTRAINT Pk_test_testTable PRIMARY KEY (id)
);

/*
  {implements: [java.io.Serializable, java.lang.Cloneable]}
 */
create table testRoTable(
  /**{option: [open, closed]}*/
  id int  NOT NULL,
  CONSTRAINT Pk_test_testRoTable PRIMARY KEY (id)
) WITH READ ONLY;

create table test_snake_table (
  snake_field int not null  primary key,
  snake_blob blob,
  date_one datetime,
  date_two datetime with time zone,
  text_field varchar(10)
);