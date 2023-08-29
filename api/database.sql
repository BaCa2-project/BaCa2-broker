drop table if exists baca_requests;

create table baca_requests(
    id text primary key,
    course varchar(2048) not null,
    submit_id integer not null,
    submit_path varchar(2048) not null,
    package_path varchar(2048) not null,
    mod_time datetime not null,
    state int not null
);
