CREATE GRAIN celesta VERSION '1.00';

CREATE TABLE grains(
  id nvarchar(16) NOT NULL PRIMARY KEY, --префикс (код) гранулы
  version  nvarchar(max) not null, -- version tag гранулы
  length int not null, -- длина creation-скрипта гранулы  в байтах (составляющая часть контрольной суммы)
  checksum nvarchar(8) not null, --CRC32 creation-скрипта гранулы (составляющая часть контрольной суммы)
  state int not null default 3, -- статус гранулы — см. далее
  lastmodified datetime not null default getdate(), -- дата и время последнего обновления статуса гранулы
  message nvarchar(max) not null default '' -- комментарий (например, сообщение об ошибке при последнем неудавшемся автообновлении)
);

CREATE TABLE tables(
  grainid nvarchar(16) NOT NULL FOREIGN KEY REFERENCES grains(id),
  tablename nvarchar(100) NOT NULL,
  orphaned bit not null default 0,
  CONSTRAINT pk_tables PRIMARY KEY (grainid, tablename)
);

CREATE TABLE roles(
  id int NOT NULL IDENTITY PRIMARY KEY,
  descritpion nvarchar(20)
);

CREATE TABLE userroles(
  userid nvarchar(250) NOT NULL,
  roleid int NOT NULL,
  CONSTRAINT pk_userroles PRIMARY KEY (userid, roleid),
  CONSTRAINT fk_userroles FOREIGN KEY (roleid) REFERENCES roles(id)
);

CREATE TABLE permissions(
  roleid int NOT NULL,
  grainid nvarchar(16) NOT NULL,
  tablename nvarchar(100) NOT NULL,
  r bit NOT NULL DEFAULT 'FALSE',
  i bit NOT NULL DEFAULT 'FALSE',
  m bit NOT NULL DEFAULT 'FALSE',
  d bit NOT NULL DEFAULT 'FALSE',
  CONSTRAINT pk_permissions PRIMARY KEY (roleid, grainid, tablename), 
  CONSTRAINT fk_permissions1 FOREIGN KEY(roleid) REFERENCES roles(id),
  CONSTRAINT fk_permissions2 FOREIGN KEY(grainid, tablename) REFERENCES tables(grainid, tablename)
);

CREATE TABLE logsetup(
  grainid nvarchar(16) NOT NULL,
  tablename nvarchar(100) NOT NULL,
  i bit,
  m bit,
  d bit,
  CONSTRAINT pk_logsetup PRIMARY KEY (grainid, tablename),
  CONSTRAINT fk_logsetup FOREIGN KEY (grainid, tablename) REFERENCES tables(grainid, tablename)
);

CREATE TABLE log(
  entryno int IDENTITY NOT NULL PRIMARY KEY,
  entry_time DATETIME NOT NULL DEFAULT GETDATE(),
  userid nvarchar(250) NOT NULL,
  grainid nvarchar(16) NOT NULL,
  tablename nvarchar(100) NOT NULL,
  action_type nvarchar(1) NOT NULL,
  pkvalue1 nvarchar(100),
  pkvalue2 nvarchar(100),
  pkvalue3 nvarchar(100),
  oldvalues nvarchar(4000),
  newvalues nvarchar(4000),
  CONSTRAINT fk_log FOREIGN KEY(grainid, tablename) REFERENCES tables(grainid, tablename)
);