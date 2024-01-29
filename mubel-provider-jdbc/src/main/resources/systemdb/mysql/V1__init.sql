CREATE TABLE event_store_details
(
    esid        VARCHAR(255) PRIMARY KEY,
    type        VARCHAR(255) NOT NULL,
    provider    VARCHAR(255) NOT NULL,
    data_format VARCHAR(255) NOT NULL,
    state       VARCHAR(255) NOT NULL
);

CREATE TABLE job_status
(
    job_id         VARCHAR(255) PRIMARY KEY,
    state          VARCHAR(255) NOT NULL,
    description    TEXT,
    progress       SMALLINT,
    updated_at     BIGINT       NOT NULL,
    created_at     BIGINT       NOT NULL,
    problem_type   VARCHAR(255),
    problem_title  VARCHAR(255),
    problem_status smallint,
    problem_detail TEXT
);
