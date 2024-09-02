create table event_store_details
(
    esid        text primary key,
    type        text not null,
    provider    text not null,
    data_format text not null,
    state       text not null
);

create table job_status
(
    job_id         text primary key,
    state          text   not null,
    description    text,
    progress       smallint,
    updated_at     bigint not null,
    created_at     bigint not null,
    problem_type   text,
    problem_title  text,
    problem_status smallint,
    problem_detail text
);

create table event_store_alias
(
    esid  text primary key,
    alias text not null
);

create table group_leader
(
    group_id text not null primary key,
    token    text not null
);

create table group_session
(
    token     text      not null primary key,
    group_id  text      not null,
    joined_at timestamp not null,
    last_seen timestamp not null
)