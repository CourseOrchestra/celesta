create grain unionAll version '1.0';

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

create function security1(uid int) as
    select message_id, user_id as user
    from message
	  where user_id = 100
  union all
    select message_id, message.user_id
    from message
      inner join user_role on
        user_role.user_id = 1;
