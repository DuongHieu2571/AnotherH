-- CREATE table SI (
--     si_id VARCHAR(50) PRIMARY KEY,
--     si_name VARCHAR(50)
-- );

-- CREATE table groupp(
--     group_id VARCHAR(50) PRIMARY KEY,
--     si_id VARCHAR(50),
--     group_name VARCHAR(50),
--     FOREIGN KEY (si_id) REFERENCES SI (si_id)
-- );

-- CREATE table users(
--     user_id VARCHAR(50) PRIMARY KEY,
--     group_id VARCHAR(50),
--     user_name VARCHAR(50),
--     FOREIGN KEY (group_id) REFERENCES groupp (group_id)
-- );

-- CREATE table source(
--     source_id VARCHAR(50) PRIMARY KEY,
--     user_id VARCHAR(50),
--     source_info VARCHAR(100),
--     source_name VARCHAR(50),
--     FOREIGN KEY (user_id) REFERENCES users (user_id)
-- );

-- CREATE table person(
--     person_id VARCHAR(50) PRIMARY KEY,
--     person_name VARCHAR(50),
--     full_name VARCHAR(50),
--     source_id VARCHAR(50),
--     FOREIGN KEY (source_id) REFERENCES source (source_id)
-- );

CREATE table event(
    si_id VARCHAR(50),
    groupp VARCHAR(50),
    user_id VARCHAR(50),
    source_id VARCHAR(50),
    object_id VARCHAR(50),
    bbox VARCHAR(50),
    confidence VARCHAR(50),
    image_path VARCHAR(200),
    time_stamp BIGINT,
    fair_face VARCHAR(50),
    person_id VARCHAR(50),
    dict VARCHAR(200)
);