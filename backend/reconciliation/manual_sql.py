import re
from typing import Iterable


_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MODIFY|UPSERT|MERGE|DROP|ALTER|CREATE|TRUNCATE|"
    r"EXECUTE|CALL|DO|GRANT|REVOKE|COMMIT|ROLLBACK)\b",
    re.IGNORECASE,
)
_UNSAFE_READ_CLAUSE = re.compile(r"\bFOR\s+UPDATE\b|\bLOCK\s+TABLE\b", re.IGNORECASE)
_SQL_WORDS = {
    "ALL", "AND", "AS", "ASC", "BETWEEN", "BY", "CASE", "CAST", "CROSS",
    "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DESC", "DISTINCT",
    "ELSE", "END", "ESCAPE", "EXISTS", "FALSE", "FETCH", "FIRST", "FOLLOWING",
    "FROM", "FULL", "GROUP", "HAVING", "IN", "INNER", "INTERSECT", "IS", "JOIN",
    "LAST", "LEFT", "LIKE", "LIMIT", "NOT", "NULL", "NULLS", "OFFSET", "ON",
    "ONLY", "OR", "ORDER", "OUTER", "OVER", "PARTITION", "PRECEDING", "RANGE",
    "RIGHT", "ROWS", "SELECT", "THEN", "TIES", "TOP", "TRUE", "UNBOUNDED",
    "UNION", "WHEN", "WHERE", "WITH", "VAL", "OFF", "LEN", "FIELDS", "INTO",
    "TABLE", "ROWS", "OF", "CORRESPONDING", "DECIMAL", "INTEGER",
    "INT", "BIGINT", "SMALLINT", "REAL", "DOUBLE", "FLOAT", "CHAR", "VARCHAR",
    "NVARCHAR", "DATE", "TIME", "TIMESTAMP", "BOOLEAN", "STRING", "YEAR", "MONTH",
    "DAY", "HOUR", "MINUTE", "SECOND", "ASOF", "PRIOR",
}


def _mask_string_literals(sql: str) -> str:
    return re.sub(r"'(?:''|[^'])*'", lambda match: " " * len(match.group()), sql)


def validate_manual_select(
    sql: str,
    *,
    allowed_sources: Iterable[str],
    allowed_fields: Iterable[str],
    expected_schema: str | None = None,
    allow_source_aliases: bool = True,
) -> None:
    """Conservatively validate a single read-only SELECT against discovered metadata."""
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("SQL query cannot be empty.")
    if sql != sql.strip():
        # Preserve whitespace exactly in the approval binding; execution code may trim only outer whitespace.
        sql = sql.strip()
    if not re.match(r"^SELECT\b", sql, re.IGNORECASE):
        raise ValueError("Only a single SELECT query is permitted.")
    if ";" in sql or "--" in sql or "/*" in sql or "*/" in sql:
        raise ValueError("SQL comments and statement separators are not permitted.")
    if re.search(r'(?m)^\s*"(?:\s|$)|^\s*\*', sql):
        raise ValueError("SQL comments are not permitted.")

    clean = _mask_string_literals(sql)
    if "?" in clean or re.search(r"[@:][A-Za-z_]", clean):
        raise ValueError("Unbound SQL parameters are not supported in manual queries.")
    if _FORBIDDEN.search(clean):
        raise ValueError("Only read-only SELECT statements are permitted.")
    if _UNSAFE_READ_CLAUSE.search(clean) or re.search(r"\bINTO\b", clean, re.IGNORECASE):
        raise ValueError("Locking and INTO clauses are not supported for manual read-only queries.")
    if re.search(r"\bSELECT\s+\*", clean, re.IGNORECASE):
        raise ValueError("SELECT * is not permitted; select metadata-verified fields explicitly.")

    sources = {str(source).casefold() for source in allowed_sources if source}
    fields = {str(field).casefold() for field in allowed_fields if field}
    if not sources or not fields:
        raise ValueError("Verified source and field metadata are required before SQL can be approved.")

    source_pattern = re.compile(
        r"\b(?:FROM|JOIN)\s+(?:\"?([A-Za-z][A-Za-z0-9_]*)\"?\.)?"
        r"\"?([A-Za-z][A-Za-z0-9_]*)\"?(?:\s+(?:AS\s+)?([A-Za-z][A-Za-z0-9_]*))?",
        re.IGNORECASE,
    )
    refs = list(source_pattern.finditer(clean))
    if not refs:
        raise ValueError("The SELECT query must name a verified source entity.")
    aliases: set[str] = set()
    for match in refs:
        schema, source, possible_alias = match.groups()
        if source.casefold() not in sources:
            raise ValueError(f"SQL source {source!r} is not verified for the selected model.")
        if expected_schema and (
            not schema or schema.casefold() != expected_schema.casefold()
        ):
            raise ValueError(
                "Datasphere manual SQL must qualify the verified runtime object with its metadata schema."
            )
        if possible_alias and possible_alias.upper() not in _SQL_WORDS:
            if not allow_source_aliases:
                raise ValueError("Source aliases are not supported for this ABAP Open SQL query.")
            aliases.add(possible_alias.casefold())

    entity_names = sources | aliases
    for qualified in re.finditer(
        r"\b([A-Za-z][A-Za-z0-9_]*)[.~]([A-Za-z][A-Za-z0-9_]*)\b", clean
    ):
        if any(source.start() <= qualified.start() < source.end() for source in refs):
            continue
        qualifier, field = qualified.groups()
        if qualifier.casefold() not in entity_names:
            raise ValueError(f"SQL field qualifier {qualifier!r} is not a verified source.")
        if field.casefold() not in fields:
            raise ValueError(f"SQL field {field!r} is not present in verified source metadata.")

    output_aliases = {
        alias.casefold()
        for alias in re.findall(r"\bAS\s+([A-Za-z][A-Za-z0-9_]*)", clean, re.IGNORECASE)
    }
    source_words = {
        token.casefold()
        for match in refs
        for token in match.groups()
        if token
    }
    source_words |= entity_names
    identifiers = list(re.finditer(r"\b[A-Za-z_][A-Za-z0-9_]*\b", clean))
    for match in identifiers:
        name = match.group()
        folded = name.casefold()
        if folded in fields or name.upper() in _SQL_WORDS or folded in source_words or folded in output_aliases:
            continue
        following = clean[match.end():]
        if re.match(r"\s*\(", following):
            continue
        if re.match(r"\s*=", following) and folded in {"val", "off", "len"}:
            continue
        # A field immediately following a qualified source is checked above.
        if match.start() and clean[match.start() - 1] in ".~":
            continue
        raise ValueError(
            f"SQL identifier {name!r} is not present in verified source metadata or a supported SQL construct."
        )
