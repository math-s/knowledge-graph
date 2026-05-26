"""Stamp the SQLite DB with build metadata in the `kg_meta` table.

Run after a successful pipeline build (or to back-stamp an existing DB):

    python pipeline/scripts/stamp_db_meta.py
    python pipeline/scripts/stamp_db_meta.py --db data/knowledge-graph.db
    python pipeline/scripts/stamp_db_meta.py --version 2026-04-25T20:00:00Z

The companion meta.json (carrying the file sha256) is written by
`scripts/sync_db.sh push`, not here — we don't want to re-hash 8 GB on every
pipeline run.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.src.db_meta import stamp_db  # noqa: E402

DEFAULT_DB = PROJECT_ROOT / "data" / "knowledge-graph.db"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--version", default=None, help="Override the version string")
    parser.add_argument("--git-sha", default=None, help="Override the pipeline git sha")
    args = parser.parse_args()

    meta = stamp_db(
        args.db,
        version=args.version,
        git_sha=args.git_sha,
        repo_root=PROJECT_ROOT,
    )
    print(f"Stamped {args.db}:")
    for k, v in meta.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
