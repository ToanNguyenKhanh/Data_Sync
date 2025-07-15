CREATE TABLE IF NOT EXISTS Users(
    user_id BIGINT,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS Repositories(
    repo_id BIGINT,
    name VARCHAR(255),
    url VARCHAR(255)
);