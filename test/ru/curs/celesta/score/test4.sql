CREATE GRAIN skk VERSION '1.0';

CREATE TABLE app_division(
	id int NOT NULL DEFAULT 0,
	name text NULL,
 CONSTRAINT pk_app_division PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE app_division_add_info_el(
	app_division_id int NOT NULL DEFAULT 0,
	add_info_element_id int NOT NULL DEFAULT 0,
 CONSTRAINT pk_app_division_add_info_el PRIMARY KEY  
(
	app_division_id,
	add_info_element_id
)
) ;

CREATE TABLE app_division_currentstatus(
	id int IDENTITY NOT NULL,
	app_id int NOT NULL DEFAULT 0,
	app_division_id int NOT NULL DEFAULT 0,
	status_id int NOT NULL DEFAULT 0,
 CONSTRAINT pk_app_div_crntsts_id PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE app_division_currentstatus_emp(
	app_division_currentstatus_id int NOT NULL DEFAULT 0,
	emp_id int NOT NULL DEFAULT 0,
	resp bit NOT NULL DEFAULT 0,
 CONSTRAINT pk_app_div_curst_id_emp_id PRIMARY KEY  
(
	app_division_currentstatus_id,
	emp_id
)
) ;

CREATE TABLE app_division_group(
	app_division int NOT NULL DEFAULT 0,
	group varchar(50) NOT NULL DEFAULT '',
 CONSTRAINT pk_app_division_group PRIMARY KEY  
(
	app_division,
	group
)
) ;

CREATE TABLE app_division_status(
	id int NOT NULL DEFAULT 0,
	name text NOT NULL DEFAULT '',
 CONSTRAINT pk_app_division_status PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE app_indicator_group(
	groupid varchar(30) NOT NULL DEFAULT '',
	application_id int NOT NULL DEFAULT 0,
	sort int IDENTITY NOT NULL,
	readableName text NULL,
 CONSTRAINT pk_app_indicator_group PRIMARY KEY  
(
	groupid
)
) ;

CREATE TABLE app_indicator_group_log(
	version_no int NOT NULL DEFAULT 0,
	groupid varchar(30) NOT NULL DEFAULT '',
	application_id int NOT NULL DEFAULT 0,
	inserted bit NULL,
	deleted bit NULL,
	textf text NULL,
 CONSTRAINT pk_app_indicator_group_log PRIMARY KEY  
(
	version_no,
	application_id,
	groupid
)
) ;

CREATE TABLE app_indicator_group_version(
	ver int NOT NULL DEFAULT 0,
	groupid varchar(30) NOT NULL DEFAULT '',
	application_id int NOT NULL DEFAULT 0,
	sort int NOT NULL DEFAULT 0,
	collectedId text NULL,
	readableName text NULL,
 CONSTRAINT pk_app_indicator_group_version PRIMARY KEY  
(
	ver,
	application_id,
	groupid
)
) ;

CREATE TABLE app_indicators(
	indicator_id int NOT NULL DEFAULT 0,
	groupid varchar(30) NOT NULL,
	number_proposed real NULL,
	date_proposed datetime NULL,
	option_proposed int NULL,
	text_proposed varchar(1024) NULL,
	number_approved real NULL,
	date_approved datetime NULL,
	option_approved int NULL,
	text_approved varchar(1024) NULL,
 CONSTRAINT pk_app_indicators PRIMARY KEY  
(
	indicator_id,
	groupid
)
) ;

CREATE TABLE app_indicators_log(
	version_no int NOT NULL DEFAULT 0,
	indicator_id int NOT NULL DEFAULT 0,
	groupid varchar(30) NOT NULL  DEFAULT '',
	number_new real NULL,
	text_new varchar(1024) NULL,
	date_new datetime NULL,
	option_new int NULL,
	number_old real NULL,
	text_old varchar(1024) NULL,
	date_old datetime NULL,
	option_old int NULL,
	textf text NULL,
 CONSTRAINT pk_app_indicators_log PRIMARY KEY  
(
	version_no,
	indicator_id,
	groupid
)
) ;

CREATE TABLE app_indicators_version(
	ver int NOT NULL DEFAULT 0,
	indicator_id int NOT NULL DEFAULT 0,
	groupid varchar(30) NOT NULL  DEFAULT '',
	number_proposed real NULL,
	date_proposed datetime NULL,
	option_proposed int NULL,
	text_proposed text NULL,
	number_approved real NULL,
	date_approved datetime NULL,
	option_approved int NULL,
	text_approved text NULL,
 CONSTRAINT pk_app_indicators_version PRIMARY KEY  
(
	ver,
	indicator_id,
	groupid
)
) ;

CREATE TABLE app_type(
	id int NOT NULL DEFAULT 0,
	name varchar(45) NOT NULL  DEFAULT '',
 CONSTRAINT pk_app_type PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE app_version(
	ver int IDENTITY NOT NULL,
	application_id int NOT NULL DEFAULT 0,
	app_division_id int NOT NULL DEFAULT 0,
	ver_date datetime NOT NULL,
	employee_id int NULL,
	changes text NULL,
	status text NOT NULL  DEFAULT '',
 CONSTRAINT pk_app_version_ver PRIMARY KEY  
(
	ver
)
) ;

CREATE TABLE appContractorType(
	id int NOT NULL DEFAULT 0,
	name varchar(255) NOT NULL  DEFAULT '',
 CONSTRAINT pk_appContractorType PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE application(
	id int NOT NULL DEFAULT 0,
	application_date datetime NOT NULL,
	voting_date datetime NULL,
	contractor_nat_per varchar(100) NULL,
	contractor real NULL,
	agreement text NULL,
	coll_authority_id int NULL,
	application_status_id int NOT NULL DEFAULT 0,
	question_type_id int NULL,
	application_type_id int NULL,
	businessline_id int NULL,
	product_id int NULL,
	sort int NOT NULL DEFAULT 0,
	is_group bit NULL,
	is_template bit NULL,
	parent_id int NULL,
	parent_app_id int NULL,
	is_deleted int NOT NULL DEFAULT 0,
	created_by int NOT NULL DEFAULT 0,
	app_type_id int NULL,
	appContractorTypeId int NULL,
	dateLastModified datetime NULL,
	verAppId int NULL,
	verDate datetime NULL,
	is_blocked int NULL,
	parentAppGroupId int NULL,
 CONSTRAINT pk_application PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE application_status(
	id int NOT NULL DEFAULT 0,
	name varchar(45) NOT NULL  DEFAULT '',
 CONSTRAINT pk_application_status PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE application_template(
	app_id int NOT NULL DEFAULT 0,
	emp_id int NULL,
	name varchar(255) NOT NULL  DEFAULT '',
 CONSTRAINT pk_application_template PRIMARY KEY  
(
	app_id
)
) ;

CREATE TABLE application_type(
	id int NOT NULL DEFAULT 0,
	name varchar(255) NOT NULL DEFAULT '',
 CONSTRAINT pk_application_type PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE application_views(
	id int IDENTITY NOT NULL,
	caption varchar(100) NOT NULL DEFAULT '',
	employeeid int NULL,
	fields varchar(1024) NULL,
	conditions varchar(1024) NULL,
	sorting varchar(1024) NULL,
 CONSTRAINT PK_application_views PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE authority(
	id int NOT NULL DEFAULT 0,
	name varchar(250) NULL,
 CONSTRAINT pk_authority PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE board(
	id int NOT NULL DEFAULT 0,
	name varchar(250) NOT NULL  DEFAULT '',
 CONSTRAINT pk_board PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE branch(
	id int NOT NULL DEFAULT 0,
	name varchar(250) NOT NULL  DEFAULT '',
 CONSTRAINT pk_branch PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE businessline(
	id int NOT NULL DEFAULT 0,
	name varchar(70) NOT NULL  DEFAULT '',
 CONSTRAINT pk_businessline PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE client_mappings(
	openru_person_id int NOT NULL DEFAULT 0,
	skk_contrag_id int NOT NULL DEFAULT 0,
 CONSTRAINT PK_client_mappings PRIMARY KEY  
(
	openru_person_id
)
) ;

CREATE TABLE coll_authority(
	id int NOT NULL DEFAULT 0,
	authority_id int NOT NULL DEFAULT 0,
	meeting_id int NOT NULL DEFAULT 0,
	name varchar(250) NULL,
 CONSTRAINT pk_coll_authority PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE coll_authority_businessline(
	coll_authority_id int NOT NULL DEFAULT 0,
	businessline_id int NOT NULL DEFAULT 0,
PRIMARY KEY  
(
	coll_authority_id,
	businessline_id
)
) ;

CREATE TABLE commissions(
	id int NOT NULL DEFAULT 0,
	name text NOT NULL  DEFAULT '',
	base text NULL,
 CONSTRAINT pk_commissions PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE company(
	id int NOT NULL DEFAULT 0,
	name varchar(250) NOT NULL  DEFAULT '',
 CONSTRAINT pk_company PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE createLogicScript(
	order int NOT NULL DEFAULT 0,
	delorder int NULL,
	schema varchar(100) NOT NULL  DEFAULT '',
	object varchar(100) NOT NULL  DEFAULT '',
	type varchar(4) NOT NULL  DEFAULT '',
	documentation text NULL,
 CONSTRAINT PK_createLogicScript_1 PRIMARY KEY  
(
	order
)
) ;

CREATE TABLE credRisk(
	id int NOT NULL DEFAULT 0,
	dewey varchar(20) NOT NULL  DEFAULT '',
	name varchar(250) NOT NULL  DEFAULT '', 
 CONSTRAINT pk_credRisk PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE crpos_sec(
	app_id int NOT NULL DEFAULT 0,
	group_id varchar(30) NOT NULL  DEFAULT '',
	type varchar(20) NOT NULL  DEFAULT '',
 CONSTRAINT pk_cross_sec PRIMARY KEY  
(
	app_id,
	group_id
)
) ;

CREATE TABLE department(
	id int NOT NULL DEFAULT 0,
	name varchar(250) NOT NULL DEFAULT '',
 CONSTRAINT pk_department PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE division(
	id int NOT NULL DEFAULT 0,
	name varchar(250) NOT NULL  DEFAULT '',
 CONSTRAINT pk_division PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE fields(
	id int NOT NULL DEFAULT 0 PRIMARY KEY,
	name varchar(100) NOT NULL
) ;

CREATE TABLE filesForApplication(
	uid varchar(30) NOT NULL  DEFAULT '',
	filename varchar(250) NULL,
	file blob NULL,
	type varchar(50) NULL,
	application int NULL,
 CONSTRAINT pk_FilesForApplication PRIMARY KEY  
(
	uid
)
)  ;

CREATE TABLE fines(
	id int NOT NULL DEFAULT 0,
	parent int NULL,
	name text NOT NULL  DEFAULT '',
	base text NULL,
 CONSTRAINT pk_fines PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE group_type_template(
	grouptype varchar(255) NOT NULL  DEFAULT '',
	template text NOT NULL,
 CONSTRAINT pk_group_type_template PRIMARY KEY  
(
	grouptype
)
) ;

CREATE TABLE indicator(
	id int NOT NULL DEFAULT 0,
	indtype int NOT NULL DEFAULT 0,
	round int NULL,
	name text NOT NULL,
	indgroup varchar(250) NOT NULL,
	is_required bit NULL,
 CONSTRAINT pk_indicator PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE indicator_group_type(
	groupid varchar(30) NOT NULL,
	grouptype varchar(255) NOT NULL,
 CONSTRAINT pk_indicator_group_type PRIMARY KEY  
(
	groupid
)
) ;

CREATE TABLE indicatoroption_orders(
	indicator_id int NOT NULL DEFAULT 0,
	order int NOT NULL DEFAULT 0,
PRIMARY KEY  
(
	indicator_id
)
) ;

CREATE TABLE indicatoroptions(
	indicator_id int NOT NULL DEFAULT 0,
	optionorder int NOT NULL DEFAULT 0,
	optionvalue varchar(1000) NOT NULL,
 CONSTRAINT pk_indicatoroptions PRIMARY KEY  
(
	indicator_id,
	optionorder
)
) ;

CREATE TABLE indicatortype(
	id int NOT NULL DEFAULT 0,
	name varchar(45) NOT NULL,
 CONSTRAINT pk_indicatortype PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE marketRisk(
	id int NOT NULL DEFAULT 0,
	dewey varchar(20) NOT NULL,
	name varchar(250) NOT NULL,
 CONSTRAINT pk_marketRisk PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE meeting_type(
	id int NOT NULL DEFAULT 0,
	name varchar(250) NULL,
 CONSTRAINT pk_meeting_type PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE oper_data(
	application_id int NOT NULL DEFAULT 0,
	oper_date datetime NOT NULL,
	oper_type int NOT NULL DEFAULT 0,
	amount real NOT NULL,
 CONSTRAINT pk_oper_data PRIMARY KEY  
(
	application_id,
	oper_date,
	oper_type
)
) ;

CREATE TABLE operation_type(
	id int NOT NULL DEFAULT 0,
	name varchar(100) NULL,
 CONSTRAINT pk_operation_type PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE perms_group(
	id int NOT NULL DEFAULT 0,
	perms_group_type int NOT NULL DEFAULT 0,
	name varchar(250) NOT NULL,
 CONSTRAINT pk_perms_group PRIMARY KEY  
(
	id,
	perms_group_type
)
) ;

CREATE TABLE perms_group_type(
	id int NOT NULL DEFAULT 0,
	name varchar(250) NOT NULL,
 CONSTRAINT pk_perms_group_type PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE position(
	id int NOT NULL DEFAULT 0,
	name varchar(250) NOT NULL,
 CONSTRAINT pk_position PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE product(
	id int NOT NULL DEFAULT 0,
	name varchar(230) NOT NULL,
	type int NULL,
	is_monetary int NULL,
 CONSTRAINT pk_product PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE product_application_type(
	product_id int NOT NULL DEFAULT 0,
	application_type_id int NOT NULL DEFAULT 0,
 CONSTRAINT pk_product_application_type PRIMARY KEY  
(
	product_id,
	application_type_id
)
) ;

CREATE TABLE question_type(
	id int NOT NULL DEFAULT 0,
	dewey_id varchar(70) NULL,
	name text NOT NULL,
	is_valid int NOT NULL DEFAULT 0,
	type int NULL,
	for_report text NOT NULL,
 CONSTRAINT pk_question_type PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE ReportData(
	GUID varchar(30) NOT NULL DEFAULT '',
	TemplateName varchar(255) NULL,
	Filterxml text NULL,
	link text NULL,
	REPORTBLOB blob NULL,
	EmployeeID int NULL,
	STATUS varchar(255) NULL,
	COMMENT varchar(1024) NULL,
	link_result text NULL,
 CONSTRAINT pk_ReportData PRIMARY KEY  
(
	GUID
)
) ;

CREATE TABLE subgroup(
	id int IDENTITY NOT NULL,
	name varchar(250) NOT NULL,
	groupid int NOT NULL DEFAULT 0,
 CONSTRAINT pk_subgroup PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE subgroup_org(
	subgroupid int NOT NULL DEFAULT 0,
	orgid int NOT NULL DEFAULT 0,
 CONSTRAINT pk_subgroup_org PRIMARY KEY  
(
	subgroupid,
	orgid
)
) ;

CREATE TABLE t_group_orgs(
	id int IDENTITY NOT NULL,
	gr_id real NOT NULL,
	gr_date datetime NOT NULL,
	bank_id real NOT NULL,
 CONSTRAINT pk_t_group_orgs PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE x_dates(
	curr_date datetime NOT NULL,
 CONSTRAINT pk_x_dates PRIMARY KEY  
(
	curr_date
)
) ;

CREATE TABLE x_list_object_type(
	id int NOT NULL DEFAULT 0,
	name varchar(250) NOT NULL,
 CONSTRAINT pk_x_list_object_type PRIMARY KEY  
(
	id
)
) ;

CREATE TABLE x_list_objects(
	id int NOT NULL DEFAULT 0,
	object_type_id int NOT NULL DEFAULT 0,
	perms_group_type int NOT NULL DEFAULT 0,
	name varchar(250) NOT NULL,
	description varchar(250) NULL,
 CONSTRAINT pk_x_list_objects PRIMARY KEY  
(
	id,
	object_type_id
)
) ;

CREATE TABLE x_role_employees(
	role_id int NOT NULL DEFAULT 0,
	role_type_id int NOT NULL DEFAULT 0,
	emp_id int NOT NULL DEFAULT 0,
	is_manager bit NULL,
 CONSTRAINT pk_x_role_employees PRIMARY KEY  
(
	role_id,
	role_type_id,
	emp_id
)
) ;

CREATE TABLE x_role_perms(
	role_id int NOT NULL DEFAULT 0,
	role_type_id int NOT NULL DEFAULT 0,
	object_id int NOT NULL DEFAULT 0,
	object_type int NOT NULL DEFAULT 0,
	perm_id int NOT NULL DEFAULT 0,
 CONSTRAINT pk_x_role_perms PRIMARY KEY  
(
	role_id,
	role_type_id,
	object_id,
	object_type
)
) ;

CREATE TABLE x_roles(
	id int NOT NULL DEFAULT 0,
	type int NOT NULL DEFAULT 0,
	name text NULL,
 CONSTRAINT pk_x_roles PRIMARY KEY  
(
	id,
	type
)
) ;

CREATE TABLE xlreports(
	id int NOT NULL DEFAULT 0,
	template varchar(250) NOT NULL,
	parameters text NULL,
	status int NOT NULL DEFAULT 0,
	result blob NULL,
	errortext varchar(1024) NULL,
	report_date datetime NULL,
	script varchar(250) NULL,
 CONSTRAINT pk_xlreports PRIMARY KEY  
(
	id
)
);


ALTER TABLE app_division_currentstatus  ADD CONSTRAINT fk_app_div_curst_app_div FOREIGN KEY(app_division_id)
REFERENCES app_division (id)
;

ALTER TABLE app_division_currentstatus  ADD  CONSTRAINT fk_app_div_curstappdivst FOREIGN KEY(status_id)
REFERENCES app_division_status (id)
;

ALTER TABLE app_division_currentstatus  ADD  CONSTRAINT fk_app_div_curst_app FOREIGN KEY(app_id)
REFERENCES application (id)
ON DELETE CASCADE
;

ALTER TABLE app_division_currentstatus_emp  ADD  CONSTRAINT fk_ap_divcrstempapdivcrst FOREIGN KEY(app_division_currentstatus_id)
REFERENCES app_division_currentstatus (id)
;

ALTER TABLE app_division_group  ADD  CONSTRAINT fk_app_div_grp_app_div FOREIGN KEY(app_division)
REFERENCES app_division (id)
;

ALTER TABLE app_indicator_group  ADD  CONSTRAINT fk_app_indic_grp_app FOREIGN KEY(application_id)
REFERENCES application (id)
ON DELETE CASCADE
;

ALTER TABLE app_indicator_group_log  ADD  CONSTRAINT fk_appindicgrplogappver FOREIGN KEY(version_no)
REFERENCES app_version (ver)
;


ALTER TABLE app_indicator_group_log  ADD  CONSTRAINT fk_app_indic_grplogapp FOREIGN KEY(application_id)
REFERENCES application (id)
ON DELETE CASCADE
;

ALTER TABLE app_indicator_group_version  ADD  CONSTRAINT fk_appindcgrpverappver FOREIGN KEY(ver)
REFERENCES app_version (ver)
;

ALTER TABLE app_indicator_group_version  ADD  CONSTRAINT fk_app_indicgrpverapp FOREIGN KEY(application_id)
REFERENCES application (id)
ON DELETE CASCADE
;

ALTER TABLE app_indicators  ADD  CONSTRAINT fk_group_indicators_group FOREIGN KEY(groupid)
REFERENCES app_indicator_group (groupid)
;

ALTER TABLE app_indicators  ADD  CONSTRAINT fk_group_indicatoicator FOREIGN KEY(indicator_id)
REFERENCES indicator (id)
;

ALTER TABLE app_indicators_log  ADD  CONSTRAINT fk_appindicatorslondicator FOREIGN KEY(indicator_id)
REFERENCES indicator (id)
;


ALTER TABLE app_indicators_log  ADD  CONSTRAINT fk_appinogappversion FOREIGN KEY(version_no)
REFERENCES app_version (ver)
;

ALTER TABLE app_indicators_version  ADD  CONSTRAINT fk_appincatsverpicar FOREIGN KEY(indicator_id)
REFERENCES indicator (id)
;

ALTER TABLE app_indicators_version  ADD  CONSTRAINT fk_appiicatoronappvsio FOREIGN KEY(ver)
REFERENCES app_version (ver)
;

ALTER TABLE app_version  ADD  CONSTRAINT fk_apversionappdivision FOREIGN KEY(app_division_id)
REFERENCES app_division (id)
;

ALTER TABLE app_version  ADD  CONSTRAINT fk_appversionaication FOREIGN KEY(application_id)
REFERENCES application (id)
ON DELETE CASCADE
;

ALTER TABLE application  ADD  CONSTRAINT fk_applicatippContractoype FOREIGN KEY(appContractorTypeId)
REFERENCES appContractorType (id)
;

ALTER TABLE application  ADD  CONSTRAINT fk_appicaiopplicationstat FOREIGN KEY(application_status_id)
REFERENCES application_status (id)
;

ALTER TABLE application  ADD  CONSTRAINT fk_applicationbusinessline FOREIGN KEY(businessline_id)
REFERENCES businessline (id)
;

ALTER TABLE application  ADD  CONSTRAINT fk_applicationcollauthorit FOREIGN KEY(coll_authority_id)
REFERENCES coll_authority (id)
;

ALTER TABLE application  ADD  CONSTRAINT fk_application_product FOREIGN KEY(product_id)
REFERENCES product (id)
;

ALTER TABLE application  ADD  CONSTRAINT fk_applitio_qetiontype FOREIGN KEY(question_type_id)
REFERENCES question_type (id)
;

ALTER TABLE application_template  ADD  CONSTRAINT fk_appiaionteplatapplcaion FOREIGN KEY(app_id)
REFERENCES application (id)
;


ALTER TABLE coll_authority  ADD  CONSTRAINT fk_coll_auhorityauthority1 FOREIGN KEY(authority_id)
REFERENCES authority (id)
;


ALTER TABLE coll_authority ADD  CONSTRAINT fk_colluthoitymeetingype1 FOREIGN KEY(meeting_id)
REFERENCES meeting_type (id)
;


ALTER TABLE coll_authority_businessline  ADD  CONSTRAINT fk_colaurbulineessline1 FOREIGN KEY(businessline_id)
REFERENCES businessline (id)
;

ALTER TABLE coll_authority_businessline  ADD  CONSTRAINT fk_clluthoy_nsnco_auhort1 FOREIGN KEY(coll_authority_id)
REFERENCES coll_authority (id)
;

ALTER TABLE crpos_sec  ADD  CONSTRAINT fk_cross_secpplication FOREIGN KEY(app_id)
REFERENCES application (id)
;

ALTER TABLE crpos_sec  ADD  CONSTRAINT fk_cross_sec_group FOREIGN KEY(group_id)
REFERENCES app_indicator_group (groupid)
;


ALTER TABLE filesForApplication  ADD  CONSTRAINT fk_fileForppiconplcation FOREIGN KEY(application)
REFERENCES application (id)
ON DELETE CASCADE
;

ALTER TABLE indicator  ADD  CONSTRAINT fk_indicator_indicatortype FOREIGN KEY(indtype)
REFERENCES indicatortype (id)
;

ALTER TABLE indicatoroption_orders  ADD  CONSTRAINT fk_orders_indicator1 FOREIGN KEY(indicator_id)
REFERENCES indicator (id)
ON DELETE CASCADE
;

ALTER TABLE indicatoroptions  ADD  CONSTRAINT fk_indicatorptionndictor FOREIGN KEY(indicator_id)
REFERENCES indicator (id)
ON DELETE CASCADE
;

ALTER TABLE oper_data  ADD  CONSTRAINT fk_oper_data FOREIGN KEY(application_id)
REFERENCES application (id)
ON DELETE CASCADE
;

ALTER TABLE oper_data  ADD  CONSTRAINT fk_oper_data_oper_type FOREIGN KEY(oper_type)
REFERENCES operation_type (id)
;

ALTER TABLE perms_group  ADD  CONSTRAINT fk_perms_perms_group_type FOREIGN KEY(perms_group_type)
REFERENCES perms_group_type (id)
;


ALTER TABLE product_application_type  ADD  CONSTRAINT fk_produppintyapicatontye1 FOREIGN KEY(application_type_id)
REFERENCES application_type (id)
;


ALTER TABLE product_application_type  ADD  CONSTRAINT fk_productappiatonyppruct1 FOREIGN KEY(product_id)
REFERENCES product (id)
;

ALTER TABLE subgroup_org  ADD  CONSTRAINT fk_subgroup_org_subgroup FOREIGN KEY(subgroupid)
REFERENCES subgroup (id)
;

ALTER TABLE x_list_objects  ADD  CONSTRAINT fk_x_st_bjpe_optype FOREIGN KEY(perms_group_type)
REFERENCES perms_group_type (id)
;

ALTER TABLE x_list_objects  ADD  CONSTRAINT fk_x_litobje_i_j_tpe FOREIGN KEY(object_type_id)
REFERENCES x_list_object_type (id)
;


ALTER TABLE x_role_employees  ADD  CONSTRAINT fk_x_rolempyees_xroles FOREIGN KEY(role_id, role_type_id)
REFERENCES x_roles (id, type)
;


ALTER TABLE x_role_perms  ADD  CONSTRAINT fk_x_rle_permsject_type FOREIGN KEY(object_type)
REFERENCES x_list_object_type (id)
;

