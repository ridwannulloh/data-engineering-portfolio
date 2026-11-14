from pathlib import Path

import pytest

from ch_etl.config import MIGRATIONS_DIR
from ch_etl.migrate import discover, split_statements


def test_split_ignores_semicolons_in_strings_and_comments():
    sql = """
    -- comment; with semicolon
    CREATE TABLE a (s String DEFAULT 'x;y');
    SELECT 'it''s; fine', 'esc\\'; still';  -- trailing; comment
    SELECT 1
    """
    assert split_statements(sql) == [
        "CREATE TABLE a (s String DEFAULT 'x;y')",
        "SELECT 'it''s; fine', 'esc\\'; still'",
        "SELECT 1",
    ]


def test_split_drops_empty_statements():
    assert split_statements(";;\n-- only a comment\n;") == []


def test_discover_orders_by_version(tmp_path: Path):
    (tmp_path / "010_b.sql").write_text("SELECT 2")
    (tmp_path / "002_a.sql").write_text("SELECT 1")
    assert [m.version for m in discover(tmp_path)] == [2, 10]


def test_discover_rejects_duplicates_and_bad_names(tmp_path: Path):
    (tmp_path / "001_a.sql").write_text("")
    (tmp_path / "01_b.sql").write_text("")
    with pytest.raises(ValueError, match="duplicate"):
        discover(tmp_path)
    (tmp_path / "01_b.sql").unlink()
    (tmp_path / "notes.sql").write_text("")
    with pytest.raises(ValueError, match="bad migration filename"):
        discover(tmp_path)


def test_project_migrations_parse():
    migrations = discover(MIGRATIONS_DIR)
    assert [m.version for m in migrations] == list(range(1, len(migrations) + 1))
    for m in migrations:
        assert m.statements, m.name
        assert all(s.upper().startswith("CREATE") for s in m.statements), m.name
