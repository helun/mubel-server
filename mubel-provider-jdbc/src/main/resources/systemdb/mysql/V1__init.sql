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
    updated_at     BIGINT       NOT NULL,
    created_at     BIGINT       NOT NULL,
    progress       SMALLINT,
    problem_status SMALLINT,
    state          VARCHAR(255) NOT NULL,
    problem_type   VARCHAR(255),
    problem_title  VARCHAR(255),
    description    TEXT,
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
    group_id VARCHAR(255) NOT NULL PRIMARY KEY,
    token    VARCHAR(255) NOT NULL
);

CREATE TABLE group_session
(
    joined_at DATETIME(6) NOT NULL,
    last_seen DATETIME(6) NOT NULL,
    token     VARCHAR(255)      NOT NULL PRIMARY KEY,
    group_id  VARCHAR(255)      NOT NULL
)
