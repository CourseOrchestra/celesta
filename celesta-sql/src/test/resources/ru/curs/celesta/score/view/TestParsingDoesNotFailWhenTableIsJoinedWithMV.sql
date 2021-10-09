CREATE GRAIN test VERSION '1.0';

CREATE table master (
    id INT NOT NULL PRIMARY KEY,
    f1 int NOT NULL
);

CREATE table ledger (
    id int not null primary key,
    master_id int not null foreign key references master(id),
    value int not null
);

CREATE MATERIALIZED VIEW  balance as
    select master_id, sum (value) as balance
     from ledger group by master_id;

CREATE VIEW masterView as
    select m.id, m.f1, b.balance from
   master as m inner join balance b on m.id = b.master_id;

CREATE VIEW view2 as
    select distinct master_id from balance;
