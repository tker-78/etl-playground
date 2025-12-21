CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    password VARCHAR(255) NOT NULL,
    email VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS posts (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

INSERT INTO users (username, password, email) VALUES ('admin', 'password', 'admin@example.com');
INSERT INTO users (username, password, email) VALUES ('general', 'password', 'general@example.com');
INSERT INTO users (username, password, email) VALUES ('viewer', 'password', 'viewer@example.com');

INSERT INTO posts (title, content, user_id) VALUES ('First Post', 'This is the first post!', 1);
INSERT INTO posts (title, content, user_id) VALUES ('Second Post', 'This is the second post!', 2);
INSERT INTO posts (title, content, user_id) VALUES ('Third Post', 'This is the third post!', 3);



