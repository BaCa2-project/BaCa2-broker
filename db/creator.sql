drop table if exists set_submit_records;
drop table if exists submit_records;

create table submit_records(
    id text primary key,
    launch_datetime timestamp not null,
    submit_path varchar(2048) not null,
    package_path varchar(2048) not null,
    commit_id varchar(127) not null,
    result_path varchar(2048) not null,
    error_msg text null,
    state int not null
 );

create table set_submit_records(
    id integer primary key autoincrement,
    submit_id text not null references submit_records(id),
    set_name text not null,
    error_msg text null,
    state int not null
);