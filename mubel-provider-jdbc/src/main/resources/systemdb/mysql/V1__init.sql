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

CREATE TABLE event_store_alias
(
    esid  VARCHAR(255) PRIMARY KEY,
    alias VARCHAR(255) NOT NULL
);

CREATE TABLE system_messages
(
    id         BIGINT PRIMARY KEY AUTO_INCREMENT,
    topic      VARCHAR(255) NOT NULL,
    message    TEXT,
    published_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE group_leader
(
    group_id VARCHAR(255) not null primary key,
    token    VARCHAR(255) not null
);

CREATE TABLE group_session
(
    token     VARCHAR(255)      NOT NULL PRIMARY KEY,
    group_id  VARCHAR(255)      NOT NULL,
    joined_at DATETIME(3) NOT NULL,
    last_seen DATETIME(3) NOT NULL
)