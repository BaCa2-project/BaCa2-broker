drop table if exists submit_records;

create table submit_records(
    id text primary key,
    launch_datetime datetime not null,
    submit_path varchar(2048) not null,
    package_path varchar(2048) not null,
    result_path varchar(2048) not null,
    error_msg text null,
    state int not null
 );
