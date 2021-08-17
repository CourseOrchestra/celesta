create schema substrng version '1.0';

create table idWithValueStartLen(
    id int not null primary key ,
    value varchar (10),
    start int,
    len int
);

create view substrngView as
select id,
       value,
       substring(value from start for len) as v1,
       substring(value from 2 for 3) as v2,
       substring(value from start + 1 for start + len) as v3,
       substring(value from start for len) || 'foo' as v4
from idWithValueStartLen;