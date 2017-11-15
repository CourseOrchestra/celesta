create grain simpleCases version '1.0';

create table getDateForView (
  id int identity not null primary key,
  date datetime
);

create view viewWithGetDate as
 select id from getDateForView where date > getdate();

 create table zeroInsert (
  id int identity not null primary key,
  date datetime not null default getdate()
 ) with no version check;
 
CREATE TABLE simple_table(
  id INT NOT NULL IDENTITY ,
  name VARCHAR(255) NOT NULL,
  CONSTRAINT Pk_simple_table PRIMARY KEY (id)
);

create table duplicate(
 id INT NOT NULL PRIMARY KEY,
 val INT
);
