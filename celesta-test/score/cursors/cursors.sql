CREATE SCHEMA cursors VERSION '1.0';

create table log_setup_test(
    grain_id varchar(30) not null,
    table_name varchar(30) not null,
    i bit,
    m bit,
    d bit,
    constraint pk_logsetup primary key (grain_id, table_name)
);