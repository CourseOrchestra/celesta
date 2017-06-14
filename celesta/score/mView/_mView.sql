create grain mView version '1.0';

create table table1 (
  id int identity not null primary key,
  numb int,
  date datetime
);

create materialized view mView1 as
   select id, id + numb as plus, id - numb as minus, max(numb) as s
   FROM table1
   group by id, plus, minus;