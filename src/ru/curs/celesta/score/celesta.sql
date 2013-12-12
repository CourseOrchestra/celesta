create grain celesta version '1.02';

create table grains(
  id nvarchar(16) not null primary key, --префикс (код) гранулы
  version  nvarchar(max) not null, -- version tag гранулы
  length int not null, -- длина creation-скрипта гранулы  в байтах (составляющая часть контрольной суммы)
  checksum nvarchar(8) not null, --CRC32 creation-скрипта гранулы (составляющая часть контрольной суммы)
  state int not null default 3, -- статус гранулы — см. далее
  lastmodified datetime not null default getdate(), -- дата и время последнего обновления статуса гранулы
  message nvarchar(max) not null default '' -- комментарий (например, сообщение об ошибке при последнем неудавшемся автообновлении)
);

create table tables(
  grainid nvarchar(16) not null,
  tablename nvarchar(100) not null,
  orphaned bit not null default 0,
  constraint pk_tables primary key (grainid, tablename),
  constraint fk_tables_grains foreign key (grainid) references grains(id)
);

create table roles(
  id nvarchar(16) not null primary key,
  description nvarchar(20)
);

create table userroles(
  userid nvarchar(250) not null,
  roleid nvarchar(16) not null,
  constraint pk_userroles primary key (userid, roleid),
  constraint fk_userroles_roles foreign key (roleid) references roles(id) on update cascade
);

create table permissions(
  roleid nvarchar(16) not null,
  grainid nvarchar(16) not null,
  tablename nvarchar(100) not null,
  r bit not null default 'FALSE',
  i bit not null default 'FALSE',
  m bit not null default 'FALSE',
  d bit not null default 'FALSE',
  constraint pk_permissions primary key (roleid, grainid, tablename), 
  constraint fk_permissions_roles foreign key(roleid) references roles(id) on update cascade,
  constraint fk_permissions_tables foreign key(grainid, tablename) references tables(grainid, tablename)
);

create table logsetup(
  grainid nvarchar(16) not null,
  tablename nvarchar(100) not null,
  i bit,
  m bit,
  d bit,
  constraint pk_logsetup primary key (grainid, tablename),
  constraint fk_logsetup_tables foreign key (grainid, tablename) references tables(grainid, tablename)
);

create table log(
  entryno int identity not null primary key,
  entry_time datetime not null default getdate(),
  userid nvarchar(250) not null,
  grainid nvarchar(16) not null,
  tablename nvarchar(100) not null,
  action_type nvarchar(1) not null,
  pkvalue1 nvarchar(100),
  pkvalue2 nvarchar(100),
  pkvalue3 nvarchar(100),
  oldvalues nvarchar(3999), -- there is wisdom in this number (3999), do not modify.
  newvalues nvarchar(3999), -- we need definite max length and it must be different from varchar(max) in oracle
  constraint fk_log_tables foreign key(grainid, tablename) references tables(grainid, tablename)
);

create table sequences(
  grainid nvarchar(16) not null,
  tablename nvarchar(100) not null,
  seqvalue int not null default 0,
  constraint pk_sequences primary key (grainid, tablename),
  constraint fk_sequences_grains foreign key(grainid) references grains(id)
);
