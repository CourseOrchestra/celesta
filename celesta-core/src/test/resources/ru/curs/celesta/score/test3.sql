CREATE GRAIN BC VERSION '0.1';

CREATE SEQUENCE element_type_id;

CREATE TABLE element_type ( 
	id                   INT NOT NULL DEFAULT NEXTVAL(element_type_id),
	name                 varchar( 250 ) NOT NULL DEFAULT '',
	is_human             bit NOT NULL  DEFAULT 0,
	 PRIMARY KEY ( id )
 );

CREATE SEQUENCE position_id;

CREATE TABLE position ( 
	id                   INT NOT NULL DEFAULT NEXTVAL(position_id),
	name                 varchar( 250 ) NOT NULL DEFAULT '',
	 PRIMARY KEY ( id )
 );

CREATE SEQUENCE structure_elements_id;

CREATE TABLE structure_elements ( 
	id                   INT NOT NULL DEFAULT NEXTVAL(structure_elements_id),
	bkey                 varchar( 32 ) NOT NULL DEFAULT '',
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
	monthName            varchar( 255 ) NOT NULL default '',
	monthNumber          int NOT NULL default 0,
	quarter              int ,
	week                 int NOT NULL default 0,
	weekdayName          varchar( 255 ) NOT NULL default '',
	weekdayNumber        int NOT NULL default 0,
	year                 int NOT NULL default 0,
	 PRIMARY KEY ( date )
 );

CREATE TABLE groupmaillist ( 
	id                   int NOT NULL default 0,
	name                 varchar( 255 ),
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
	sid                  varchar( 255 ) NOT NULL default '',
	employeeid           int NOT NULL default 0,
	userlogin            varchar( 255 ),
	fullname             varchar( 250 ),
	email                varchar( 50 ),
	 PRIMARY KEY ( sid )
 );

CREATE INDEX ix_employee_login ON sc_user ( employeeid );

CREATE TABLE structure_characteristics ( 
	structureid          int NOT NULL default 0,
	from_date            datetime NOT NULL default getdate(),
	fullname             varchar( 250 ) NOT NULL default '',
	email                varchar( 50 ),
	note                 varchar( 50 ),
	info_binary          blob,
	info_image           blob,
	to_date              datetime,
	 PRIMARY KEY ( structureid, from_date )
 );

CREATE INDEX ix_structure_fullname ON structure_characteristics ( fullname );

CREATE TABLE structure_element_errors_log ( 
	element_id           int NOT NULL default 0,
	errormessage         varchar( 255 ),
	import_row_id        int NOT NULL default 0,
	primary key (import_row_id, element_id)
 );

CREATE SEQUENCE structure_import_errors_log_id;

CREATE TABLE structure_import_errors_log ( 
	id                   INT NOT NULL DEFAULT NEXTVAL(structure_import_errors_log_id),
	errormessage         varchar( 255 ),
	file_name            varchar( 2000 ) NOT NULL default '',
	import_date          datetime,
	 PRIMARY KEY ( id )
 );

CREATE TABLE mailmessages(
	inboxid varchar(50) NOT NULL default '',
	msgid varchar(300) NOT NULL default '',
	sender varchar(200),
	subject text,
	body text,
     PRIMARY KEY (inboxid, msgid)
);

CREATE TABLE mailattachments(
   inboxid varchar(50) NOT NULL default '',
   msgid varchar(300) NOT NULL default '',
   attachno int NOT NULL default 0,
   attachname varchar(400),
   attachbody blob,
    PRIMARY KEY (inboxid, msgid, attachno)
);

CREATE SEQUENCE mail_spam_id;

CREATE TABLE mail_spam(
    id INT NOT NULL DEFAULT NEXTVAL(mail_spam_id),
	spamlist text NULL,
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
