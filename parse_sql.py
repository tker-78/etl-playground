from __future__ import annotations

from typing import Iterable, List

import sqlparse
from sqlparse.sql import Function, Identifier, IdentifierList, Token
from sqlparse.tokens import DML, Keyword, Wildcard


def _identifier_name(identifier: Identifier | Function | Token) -> str:
    """Return a readable name for an identifier or fallback to its raw value."""
    alias = identifier.get_alias() if hasattr(identifier, "get_alias") else None
    if alias:
        return alias

    if isinstance(identifier, Identifier):
        real_name = identifier.get_real_name()
        if real_name:
            return real_name
        if identifier.get_name():
            return identifier.get_name()

    if isinstance(identifier, Function) and identifier.get_name():
        return identifier.get_name()  # count, sum, etc.

    return identifier.value


def _extract_identifier_tokens(token: Token) -> Iterable[str]:
    """Yield identifier names from a token or group of tokens."""
    if isinstance(token, IdentifierList):
        for identifier in token.get_identifiers():
            yield from _extract_identifier_tokens(identifier)
    elif isinstance(token, (Identifier, Function)):
        yield _identifier_name(token)
    elif token.ttype is Wildcard:
        yield token.value
    elif token.is_group:  # Dive into nested structures (subqueries, CASE, etc.)
        for sub in token.tokens:
            yield from _extract_identifier_tokens(sub)


def extract_columns(sql: str) -> List[str]:
    """
    Extract column-like names from the SELECT clause of a SQL statement.

    Notes:
    - Returns aliases when available, otherwise the real column name.
    - Supports nested expressions (functions, CASE, subqueries) by drilling into groups.
    - Stops parsing at the first FROM keyword.
    """
    parsed = sqlparse.parse(sql)
    if not parsed:
        return []

    statement = parsed[0]
    select_seen = False
    collected_tokens: List[Token] = []

    for token in statement.tokens:
        if token.is_whitespace:
            continue
        if token.ttype is DML and token.normalized == "SELECT":
            select_seen = True
            continue
        if not select_seen:
            continue
        if token.ttype is Keyword and token.normalized == "FROM":
            break
        collected_tokens.append(token)

    columns: List[str] = []
    for token in collected_tokens:
        columns.extend(_extract_identifier_tokens(token))

    return columns
