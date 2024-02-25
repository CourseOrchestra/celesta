create grain simpleCases version '1.0';

create sequence getDateForView_id;

create table getDateForView (
  id int not null default nextval(getDateForView_id) primary key,
  date datetime
);

create view viewWithGetDate as
 select id from getDateForView where date > getdate();

create sequence zeroInsert_id;
 
create table zeroInsert (
  id int not null default nextval(zeroInsert_id) primary key,
  date datetime not null default getdate()
) with no version check;

CREATE SEQUENCE simple_table_id;

CREATE TABLE simple_table(
  id INT NOT NULL DEFAULT NEXTVAL(simple_table_id),
  name VARCHAR(255) NOT NULL,
  text_field TEXT,
  CONSTRAINT Pk_simple_table PRIMARY KEY (id)
);

create view simple_view as
  select name || '!' as name from simple_table;

create table duplicate(
 id INT NOT NULL PRIMARY KEY,
 val INT
);


create sequence custom;

create table usesequence(
  id int not null primary key,
  val int not null default nextval(custom)
);

create table sequenceAndMView(
  id int not null primary key,
  val int default nextval(custom),
  category varchar(3) not null
);

create materialized view categoryCount as
       select category, count(*) as cnt from sequenceAndMView group by category;


CREATE TABLE forTriggers(
  id INT NOT NULL PRIMARY KEY,
  val INT
);

/**This is to test view creation from neighbouring grain*/
create view tCopyField AS
  select id from testTable.tCopyFields;