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
    updated_at     bigint not null,
    created_at     bigint not null,
    progress       smallint,
    problem_status smallint,
    state          text   not null,
    description    text,
    problem_type   text,
    problem_title  text,
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
    joined_at timestamp(6) not null,
    last_seen timestamp(6) not null,
    token     text         not null primary key,
    group_id  text         not null
)