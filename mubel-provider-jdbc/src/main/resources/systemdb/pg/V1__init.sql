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
    job_id      uuid primary key,
    state       text      not null,
    description text,
    message     text,
    progress    smallint,
    updated_at  timestamp not null default now(),
    created_at  timestamp not null default now()
);