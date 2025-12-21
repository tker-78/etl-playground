import pytest

from parse_sql import extract_columns


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            "SELECT posts.id as post_id,title, content, user_id, username, password, email "
            "FROM posts LEFT JOIN users ON users.id = posts.user_id;",
            ["post_id", "title", "content", "user_id", "username", "password", "email"],
        ),
        ("SELECT * FROM users;", ["*"]),
        ("select distinct id, count(*) as cnt from users u", ["id", "cnt"]),
        ("SELECT CASE WHEN x > 1 THEN y END AS result, z FROM t", ["result", "z"]),
    ],
)
def test_extract_columns(query, expected):
    assert extract_columns(query) == expected
