CREATE GRAIN BC VERSION '0.1';

CREATE TABLE element_type ( 
	id                   int NOT NULL IDENTITY,
	name                 nvarchar( 250 ) NOT NULL DEFAULT '',
	is_human             bit NOT NULL  DEFAULT 0,
	 PRIMARY KEY ( id )
 );

CREATE TABLE position ( 
	id                   int NOT NULL IDENTITY,
	name                 nvarchar( 250 ) NOT NULL DEFAULT '',
	 PRIMARY KEY ( id )
 );

CREATE TABLE structure_elements ( 
	id                   int NOT NULL IDENTITY,
	bkey                 nvarchar( 32 ) NOT NULL DEFAULT '',
	structure_type       int NOT NULL  DEFAULT 0,
	create_date          datetime NOT NULL DEFAULT GETDATE(),
    modify_date          datetime NOT NULL DEFAULT GETDATE(),
	deleted              bit NOT NULL  DEFAULT 0,
	old_key       		 int NULL,
	 PRIMARY KEY ( id )
 );

CREATE TABLE structure_hierarchy ( 
	structureid          int NOT NULL default 0,
	parentid             int NOT NULL default 0,
	from_date            datetime NOT NULL default getdate(),
	to_date              datetime,
	 PRIMARY KEY ( structureid, parentid, from_date )
 );

CREATE INDEX ix_structure_parent ON structure_hierarchy ( parentid );

CREATE TABLE structure_subordination ( 
	structureid          int NOT NULL default 0,
	managerid            int NOT NULL default 0,
	from_date            datetime NOT NULL default getdate(),
	to_date              datetime,
	 PRIMARY KEY ( structureid, managerid, from_date )
 );

CREATE INDEX ix_structure_manager ON structure_subordination ( managerid );

CREATE TABLE calendar ( 
	date                 datetime NOT NULL default getdate(),
	day                  int NOT NULL default 0,
	dayOfYear            int NOT NULL default 0,
	isWorkday            bit NOT NULL default 0,
	monthName            nvarchar( 255 ) NOT NULL default '',
	monthNumber          int NOT NULL default 0,
	quarter              int ,
	week                 int NOT NULL default 0,
	weekdayName          nvarchar( 255 ) NOT NULL default '',
	weekdayNumber        int NOT NULL default 0,
	year                 int NOT NULL default 0,
	 PRIMARY KEY ( date )
 );

CREATE TABLE groupmaillist ( 
	id                   int NOT NULL default 0,
	name                 nvarchar( 255 ),
	 PRIMARY KEY ( id )
 );

CREATE TABLE groupmaillist_employee ( 
	employeeid           int NOT NULL default 0,
	groupmaillistid      int NOT NULL default 0,
	 PRIMARY KEY ( employeeid, groupmaillistid )
 );

CREATE TABLE employee_positions ( 
	employeeid           int NOT NULL default 0,
	positionid           int NOT NULL default 0,
	from_date            datetime NOT NULL default getdate(),
	to_date              datetime,
	 PRIMARY KEY ( employeeid, positionid, from_date )
 );

CREATE TABLE sc_user ( 
	sid                  nvarchar( 255 ) NOT NULL default '',
	employeeid           int NOT NULL default 0,
	userlogin            nvarchar( 255 ),
	fullname             nvarchar( 250 ),
	email                nvarchar( 50 ),
	 PRIMARY KEY ( sid )
 );

CREATE INDEX ix_employee_login ON sc_user ( employeeid );

CREATE TABLE structure_characteristics ( 
	structureid          int NOT NULL default 0,
	from_date            datetime NOT NULL default getdate(),
	fullname             nvarchar( 250 ) NOT NULL default '',
	email                nvarchar( 50 ),
	note                 nvarchar( 50 ),
	info_binary          image,
	info_image           image,
	to_date              datetime,
	 PRIMARY KEY ( structureid, from_date )
 );

CREATE INDEX ix_structure_fullname ON structure_characteristics ( fullname );

CREATE TABLE structure_element_errors_log ( 
	element_id           int NOT NULL default 0,
	errormessage         nvarchar( 255 ),
	import_row_id        int NOT NULL default 0,
	primary key (import_row_id, element_id)
 );
 
 CREATE TABLE structure_import_errors_log ( 
	id                   int NOT NULL IDENTITY,
	errormessage         nvarchar( 255 ),
	file_name            nvarchar( 2000 ) NOT NULL default '',
	import_date          datetime,
	 PRIMARY KEY ( id )
 );

CREATE TABLE mailmessages(
	inboxid nvarchar(50) NOT NULL default '',
	msgid nvarchar(300) NOT NULL default '',
	sender nvarchar(200),
	subject nvarchar(max),
	body nvarchar(max),
     PRIMARY KEY (inboxid, msgid)
);

CREATE TABLE mailattachments(
   inboxid nvarchar(50) NOT NULL default '',
   msgid nvarchar(300) NOT NULL default '',
   attachno int NOT NULL default 0,
   attachname nvarchar(400),
   attachbody image,
    PRIMARY KEY (inboxid, msgid, attachno)
);

CREATE TABLE mail_spam(
	id int  NOT NULL IDENTITY,
	spamlist nvarchar(max) NULL,
  PRIMARY KEY 
(
	id 
));

ALTER TABLE employee_positions ADD  FOREIGN KEY ( positionid ) REFERENCES position( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE employee_positions ADD  FOREIGN KEY ( employeeid ) REFERENCES structure_elements( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE sc_user ADD  FOREIGN KEY ( employeeid ) REFERENCES structure_elements( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE structure_characteristics ADD  FOREIGN KEY ( structureid ) REFERENCES structure_elements( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE structure_elements ADD  FOREIGN KEY ( structure_type ) REFERENCES element_type( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE structure_hierarchy ADD  FOREIGN KEY ( parentid ) REFERENCES structure_elements( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE structure_hierarchy ADD  FOREIGN KEY ( structureid ) REFERENCES structure_elements( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE structure_subordination ADD  FOREIGN KEY ( structureid ) REFERENCES structure_elements( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE structure_subordination ADD  FOREIGN KEY ( managerid ) REFERENCES structure_elements( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE structure_element_errors_log ADD  FOREIGN KEY ( import_row_id ) REFERENCES structure_import_errors_log( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE groupmaillist_employee ADD  FOREIGN KEY ( groupmaillistid ) REFERENCES groupmaillist( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;

ALTER TABLE groupmaillist_employee ADD  FOREIGN KEY ( employeeid ) REFERENCES structure_elements( id ) ON DELETE NO ACTION ON UPDATE NO ACTION;
