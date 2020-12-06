create grain upperlower version '1.0';

create table idWithValue(
   id int not null primary key ,
   value varchar (10)
);

create view upperLower as
  select id,
  value,
  upper(value) as v1,
  lower(value) as v2,
  upper(value) || lower(value) as v3,
  upper(lower(value||value)) as v4
  from idWithValue;
