CREATE GRAIN test VERSION '1.0';

CREATE table testTableA (
  idA INT NOT NULL PRIMARY KEY,
  f2A int NOT NULL,
  f3A VARCHAR (2) NOT NULL
);

CREATE table testTableB (
  idB INT NOT NULL PRIMARY KEY,
  f2B int NOT NULL,
  f3B VARCHAR (3)
);

-- valid UNION ALL queries

CREATE VIEW testUnionAll AS
  SELECT idA as A, f3A as B FROM testTableA WHERE f2A = 1
  UNION ALL
  SELECT idB, f3B FROM testTableB WHERE f2B = 1;

CREATE FUNCTION testUnionAllFunc(id int) AS
  SELECT idA as A, f2A, f3A as B FROM testTableA WHERE f2A = 1
  UNION ALL
  SELECT idB, f2B, f3B FROM testTableB WHERE f2B = $id;

-- complex valid case with function

create table role (
  id int not null primary key,
  name varchar(50)
);

create table message (
   message_id int not null primary key,
   user_id int
);

create table user_role (
  user_id int not null,
  role_id int not null foreign key references role(id),
  primary key (user_id, role_id)
);

create table role_permissions (
 role_id int not null foreign key references role(id),
 description varchar(10) not null,
 primary key (role_id, description)
);

create function security(uid int) as
    select message_id, user_id as user
    from message
	  where user_id = $uid
  union all
    select message_id, message.user_id
    from message
      inner join user_role on
        user_role.user_id = 1;
