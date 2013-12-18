/**Celesta system grain. Not for modification.*/
create grain celesta version '1.02';

/**Active grains list.*/
create table grains(
  /**grain prefix (id)*/
  id nvarchar(16) not null primary key, 
  /**grain version tag*/
  version  nvarchar(max) not null,
  /**grain creation script length in bytes*/
  length int not null,
  /**grain creation script CRC32 value*/
  checksum nvarchar(8) not null,
  /**grain status*/  
  state int not null default 3,
  /**date and time of last grain status update*/
  lastmodified datetime not null default getdate(), 
  /**comment (e. g. error message for the last failed auto-update)*/
  message nvarchar(max) not null default '' 
);

/**Tables list.*/
create table tables(
  grainid nvarchar(16) not null,
  tablename nvarchar(100) not null,
  orphaned bit not null default 0,
  constraint pk_tables primary key (grainid, tablename),
  constraint fk_tables_grains foreign key (grainid) references grains(id)
);

/**Roles list.*/
create table roles(
  id nvarchar(16) not null primary key,
  description nvarchar(20)
);

/**Links users to their roles.*/
create table userroles(
  userid nvarchar(250) not null,
  roleid nvarchar(16) not null,
  constraint pk_userroles primary key (userid, roleid),
  constraint fk_userroles_roles foreign key (roleid) references roles(id) on update cascade
);

/**Security permissions for the roles.*/
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

/**Change-logging system setup.*/
create table logsetup(
  grainid nvarchar(16) not null,
  tablename nvarchar(100) not null,
  i bit,
  m bit,
  d bit,
  constraint pk_logsetup primary key (grainid, tablename),
  constraint fk_logsetup_tables foreign key (grainid, tablename) references tables(grainid, tablename)
);

/**Changelog.*/
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

/**This table emulates sequences functionality for MS SQL Server and MySQL.*/
create table sequences(
  grainid nvarchar(16) not null,
  tablename nvarchar(100) not null,
  seqvalue int not null default 0,
  constraint pk_sequences primary key (grainid, tablename),
  constraint fk_sequences_grains foreign key(grainid) references grains(id)
);
