create grain g1 version '1.0';

create table aa(
idaa int not null primary key,  
idc int ,
textvalue nvarchar(10)
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
descr nvarchar(2),
parent int foreign key references a(ida), --ссылка на саму себя
fff int foreign key references aa(idaa) --первая часть круговой ссылки
);

--alter table aa add constraint fk1 foreign key (idc) references a(ida); --вторая часть круговой ссылки

 create table adresses (
 postalcode nvarchar(10) not null,
 country nvarchar(30) not null,
 city nvarchar(30) not null,
 street nvarchar(50) not null,
 building nvarchar(5) not null,
 flat nvarchar(5) not null,

 primary key (postalcode, building, flat)
 );