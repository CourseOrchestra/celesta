create grain g1 version '1.0';

create table aa(
idaa int not null primary key,  
idc int ,
textvalue varchar(10)
);

create index aaidx on aa (idc, textvalue);

/*multiline 
 * 
 * comment
 */
 
/** описание таблицы */
create table a (
ida int identity not null primary key,
/** описание поля*/
descr varchar(2),
parent int foreign key references a(ida), --ссылка на саму себя
fff int foreign key references aa(idaa) --первая часть круговой ссылки
);

--alter table aa add constraint fk1 foreign key (idc) references a(ida); --вторая часть круговой ссылки

 create table adresses (
 postalcode varchar(10) not null,
 country varchar(30) not null,
 city varchar(30) not null,
 street varchar(50) not null,
 building varchar(5) not null,
 flat varchar(5) not null,

 primary key (postalcode, building, flat)
 );
 
 /**view description */
create view testview as 
 select distinct grainid as fieldAlias , ta.tablename, grains.checksum ,
   ta.tablename || grains.checksum as f1
   from celesta.tables ta inner join celesta.grains on ta.grainid = grains.id
   where tablename >= 'aa' and 5 between 0 and 6 or '55' > '1';