CREATE TABLE IF NOT EXISTS user_log_before (
    user_id BIGINT,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255),
    state VArCHAR(255),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_log_after (
    user_id BIGINT,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255),
    state VArCHAR(255),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DELIMITER //

CREATE TRIGGER before_update_users
    BEFORE UPDATE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, 'UPDATE');
END //

-- BEFORE INSERT
CREATE TRIGGER before_insert_users
    BEFORE INSERT ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, 'INSERT');
END //

-- BEFORE DELETE
CREATE TRIGGER before_delete_users
    BEFORE DELETE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, 'DELETE');
END //

-- AFTER UPDATE
CREATE TRIGGER after_update_users
    AFTER UPDATE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, 'UPDATE');
END //

-- AFTER INSERT
CREATE TRIGGER after_insert_users
    AFTER INSERT ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, 'INSERT');
END //

-- AFTER DELETE
CREATE TRIGGER after_delete_users
    AFTER DELETE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, 'DELETE');
END //

DELIMITER ;


INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (1, 'alice', 'abc123', 'https://example.com/avatar/alice.png', 'https://example.com/users/alice');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (2, 'bob', 'def456', 'https://example.com/avatar/bob.png', 'https://example.com/users/bob');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (3, 'charlie', 'ghi789', 'https://example.com/avatar/charlie.png', 'https://example.com/users/charlie');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (4, 'david', 'jkl012', 'https://example.com/avatar/david.png', 'https://example.com/users/david');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (5, 'eva', 'mno345', 'https://example.com/avatar/eva.png', 'https://example.com/users/eva');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (6, 'frank', 'pqr678', 'https://example.com/avatar/frank.png', 'https://example.com/users/frank');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (7, 'grace', 'stu901', 'https://example.com/avatar/grace.png', 'https://example.com/users/grace');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (8, 'henry', 'vwx234', 'https://example.com/avatar/henry.png', 'https://example.com/users/henry');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (9, 'isabel', 'yz5678', 'https://example.com/avatar/isabel.png', 'https://example.com/users/isabel');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (10, 'jack', 'abc901', 'https://example.com/avatar/jack.png', 'https://example.com/users/jack');

-- update --
UPDATE Users SET login = 'alice_updated', url = 'https://example.com/users/alice_updated' WHERE user_id = 1;
UPDATE Users SET login = 'bob_updated', url = 'https://example.com/users/bob_updated' WHERE user_id = 2;
UPDATE Users SET login = 'charlie_updated', url = 'https://example.com/users/charlie_updated' WHERE user_id = 3;
UPDATE Users SET login = 'david_updated', url = 'https://example.com/users/david_updated' WHERE user_id = 4;
UPDATE Users SET login = 'eva_updated', url = 'https://example.com/users/eva_updated' WHERE user_id = 5;
UPDATE Users SET login = 'frank_updated', url = 'https://example.com/users/frank_updated' WHERE user_id = 6;
UPDATE Users SET login = 'grace_updated', url = 'https://example.com/users/grace_updated' WHERE user_id = 7;
UPDATE Users SET login = 'henry_updated', url = 'https://example.com/users/henry_updated' WHERE user_id = 8;
UPDATE Users SET login = 'isabel_updated', url = 'https://example.com/users/isabel_updated' WHERE user_id = 9;
UPDATE Users SET login = 'jack_updated', url = 'https://example.com/users/jack_updated' WHERE user_id = 10;


-- delete
DELETE FROM Users WHERE user_id = 1;
DELETE FROM Users WHERE user_id = 2;
DELETE FROM Users WHERE user_id = 3;




