create grain selfRef version '1.0';

/**
 * A self-referencing table with a cascading foreign key. It lives in a score
 * of its own: such a foreign key cannot be declared in MS SQL Server
 * (error 1785, "may cause cycles or multiple cascade paths").
 */
create table node (
  id int not null,
  parentId int,
  title varchar(20),
  CONSTRAINT Pk_selfRef_node PRIMARY KEY (id)
);

alter table node add constraint fk_selfRef_node_parent
  foreign key (parentId) references node (id)
  on update cascade on delete cascade;

create table nodeSetNull (
  id int not null,
  parentId int,
  title varchar(20),
  CONSTRAINT Pk_selfRef_nodeSetNull PRIMARY KEY (id)
);

alter table nodeSetNull add constraint fk_selfRef_nodeSetNull_parent
  foreign key (parentId) references nodeSetNull (id)
  on update set null on delete cascade;
