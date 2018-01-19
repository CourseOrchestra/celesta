create SCHEMA celestaSql version '1.0';

/**Active grains list.*/
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