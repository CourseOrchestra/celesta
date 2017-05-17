create grain ztest version '1.0';

create table tableForGetDateInView (
  id int identity not null primary key,
  date datetime
);

create view viewWithGetDate as
 select id from tableForGetDateInView where date > getdate();