"""Read/write the `kg_meta` table that stamps a DB build with version info.

Used by `pipeline/scripts/stamp_db_meta.py` to mark a finished DB build, and
read by the API's `/version` endpoint and `scripts/sync_db.sh` to compare
local vs. S3 vs. deployed state.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

META_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS kg_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
"""


def _git_sha(cwd: Path) -> str | None:
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=cwd,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return sha or None
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def _content_hash(conn: sqlite3.Connection) -> str:
    """Hash of stable row counts across major tables.

    Cheap, deterministic, and changes whenever ingestion changes — gives us a
    sanity check that two DBs labeled with the same `version` actually carry
    the same content. Not a substitute for the file-level sha256 in
    `meta.json`; that one is for byte-exact verification of the artifact.
    """
    tables = [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' AND name != 'kg_meta' "
            "ORDER BY name"
        )
    ]
    h = hashlib.sha256()
    for t in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except sqlite3.DatabaseError:
            continue
        h.update(f"{t}={count}\n".encode())
    return h.hexdigest()


def read_meta(db_path: Path | str) -> dict[str, str]:
    """Return the kg_meta key/value pairs as a dict (empty if table is missing)."""
    db_path = Path(db_path)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        try:
            rows = conn.execute("SELECT key, value FROM kg_meta").fetchall()
        except sqlite3.OperationalError:
            return {}
        return dict(rows)
    finally:
        conn.close()


def stamp_db(
    db_path: Path | str,
    *,
    version: str | None = None,
    git_sha: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, str]:
    """Write the kg_meta table into the DB. Returns the meta dict written.

    - `version` defaults to the current UTC timestamp (lexically sortable, so
      `make deploy` can compare strings to decide if S3 is newer than what's
      live).
    - `git_sha` defaults to `git rev-parse HEAD` from `repo_root` (or the
      DB's grandparent dir if not given).
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(db_path)

    if version is None:
        version = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if git_sha is None:
        git_sha = _git_sha(repo_root or db_path.resolve().parent.parent)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(META_TABLE_DDL)
        content = _content_hash(conn)
        meta = {
            "version": version,
            "pipeline_git_sha": git_sha or "",
            "content_hash": content,
        }
        conn.execute("DELETE FROM kg_meta")
        conn.executemany(
            "INSERT INTO kg_meta(key, value) VALUES(?, ?)",
            list(meta.items()),
        )
        conn.commit()
    finally:
        conn.close()
    return meta


def file_sha256(path: Path | str, *, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            buf = f.read(chunk)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def write_meta_json(db_path: Path | str, out_path: Path | str) -> dict:
    """Compose the companion meta.json that ships next to the DB in S3."""
    db_path = Path(db_path)
    meta = read_meta(db_path)
    payload = {
        **meta,
        "sha256": file_sha256(db_path),
        "size_bytes": os.path.getsize(db_path),
    }
    Path(out_path).write_text(json.dumps(payload, indent=2) + "\n")
    return payload
