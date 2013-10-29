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
create table a (
ida int identity not null primary key,
descr nvarchar(2),
parent int foreign key references a(ida), --ссылка на саму себя
fff int foreign key references aa(idaa) --первая часть круговой ссылки
);

alter table aa add constraint fk1 foreign key (idc) references a(ida); --вторая часть круговой ссылки