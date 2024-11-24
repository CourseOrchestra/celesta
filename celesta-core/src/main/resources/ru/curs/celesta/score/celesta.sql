/*
   Copyright 2013 COURSE-IT Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/**Celesta system grain. Not for modification.*/
create grain celesta version '1.15';

/**Active grains list.
   {implements: [ru.curs.celesta.syscursors.ISchemaCursor]}*/
create table grains(
  /**grain prefix (id)*/
  id varchar(30) not null primary key, 
  /**grain version tag*/
  version varchar(2000) not null,
  /**grain creation script length in bytes*/
  length int not null,
  /**grain creation script CRC32 value*/
  checksum varchar(8) not null,
  /**grain status
   {option: [ready, upgrading, error, recover, lock]}*/  
  state int not null default 3,
  /**date and time of last grain status update*/
  lastmodified datetime not null default getdate(), 
  /**comment (e. g. error message for the last failed auto-update)*/
  message text not null default '' 
) with no version check;

/**Tables and views list.*/
create table tables(
  /**grain id */
  grainid varchar(30) not null,
  /**table name*/
  tablename varchar(30) not null,
  /**table type: t for table, v for view*/
  tabletype varchar(2) not null default 'T',
  /**true if this table is no longer in Celesta metadata */
  orphaned bit not null default 0,
  constraint pk_tables primary key (grainid, tablename),
  constraint fk_tables_grains foreign key (grainid) references grains(id)
) with no version check;

/**Roles list.*/
create table roles(
  /**role id*/
  id varchar(16) not null primary key,
  /**role description*/
  description varchar(250)
);

/**Links users to their roles.*/
create table userroles(
  /**user id or sid*/
  userid varchar(250) not null,
  /**role id from roles table*/
  roleid varchar(16) not null,
  constraint pk_userroles primary key (userid, roleid),
  constraint fk_userroles_roles foreign key (roleid) references roles(id) on update cascade
);

/**Security permissions for the roles.*/
create table permissions(
  /**role id from roles table*/
  roleid varchar(16) not null,
  /**grain id */
  grainid varchar(30) not null,
  /**table name*/
  tablename varchar(30) not null,
  /**can read*/
  r bit not null default 'FALSE',
  /**can insert*/
  i bit not null default 'FALSE',
  /**can modify*/
  m bit not null default 'FALSE',
  /**can delete*/
  d bit not null default 'FALSE',
  constraint pk_permissions primary key (roleid, grainid, tablename), 
  constraint fk_permissions_roles foreign key(roleid) references roles(id) on update cascade,
  constraint fk_permissions_tables foreign key(grainid, tablename) references tables(grainid, tablename)
);

/**Change-logging system setup.*/
create table logsetup(
  /**grain id */
  grainid varchar(30) not null,
  /**table name*/
  tablename varchar(30) not null,
  /**log insertion*/
  i bit,
  /**log modification*/
  m bit,
  /**log deletion*/
  d bit,
  constraint pk_logsetup primary key (grainid, tablename),
  constraint fk_logsetup_tables foreign key (grainid, tablename) references tables(grainid, tablename)
);

create sequence log_entryno;

/**Changelog.*/
create table log(
  /**log entry number*/
  entryno int not null default nextval(log_entryno) primary key,
  /**log entry timestamp*/
  entry_time datetime not null default getdate(),
  /**user id*/
  userid varchar(250) not null,
  /**session id**/
  sessionid varchar(250), --no sessions anymore, can be null
  /**grain id*/
  grainid varchar(30) not null,
  /**table name*/
  tablename varchar(30) not null,
  /**logged action (i for insertion, m for modification, d for deletion)*/
  action_type varchar(1) not null,
  /**primary key field 1 value*/
  pkvalue1 varchar(100),
  /**primary key field 2 value*/
  pkvalue2 varchar(100),
  /**primary key field 3 value*/
  pkvalue3 varchar(100),
  /**old values in csv format*/
  oldvalues varchar(2000), 
  /**new values in csv format*/
  newvalues varchar(2000), 
  constraint fk_log_tables foreign key(grainid, tablename) references tables(grainid, tablename)
) with no version check;

/**This table emulates sequences functionality for MS SQL Server.*/
create table sequences(
  /**grain id*/
  grainid varchar(30) not null,
  /**table name*/
  tablename varchar(30) not null,
  /**current sequence value*/
  seqvalue int not null default 0,
  constraint pk_sequences primary key (grainid, tablename),
  constraint fk_sequences_grains foreign key(grainid) references grains(id)
) with no version check;

create sequence sessionlog_entryno;

/**deprecated table -- to be removed*/
create table sessionlog (
  entryno int not null default nextval(sessionlog_entryno) primary key,
  sessionid varchar(250) not null default 'n/a',
  userid varchar(250) not null,
  logintime datetime not null default getdate(),
  logoutime datetime,
  timeout bit not null default 0,
  failedlogin bit not null default 0
) with no version check;

create sequence calllog_entryno;

create table calllog (
  entryno int not null default nextval(calllog_entryno) primary key,
  sessionid varchar(250) not null default 'n/a',
  userid varchar(250) not null,
  procname varchar(250) not null,
  starttime datetime not null,
  duration int not null
) with no version check;

create index ixsessionlog on sessionlog (sessionid, userid, entryno);