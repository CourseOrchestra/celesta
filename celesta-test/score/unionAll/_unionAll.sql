create grain unionAll version '1.0';

create table user (
  id int not null primary key,
  name varchar(50)
);

create table role (
  id int not null primary key,
  name varchar(50)
);

create table message (
   message_id int not null primary key,
   user_id int foreign key references user(id)
);

create table user_role (
  user_id int not null foreign key references user(id),
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
      inner join user_role as ur on 1 = 1
      inner join role_permissions as rp
        on ur.role_id = rp.role_id
        and rp.description = 'super'
      where ur.user_id = $uid;
