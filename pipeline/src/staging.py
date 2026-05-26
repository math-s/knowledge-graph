"""Write corpus JSONL alongside SQLite inserts.

Every inject/refresh script should use CorpusWriter so that data/corpus/
stays in sync with the DB.  The writer buffers records in memory and
flushes atomically on close — partial writes never reach disk.

Usage (document inject scripts):
    from pipeline.src.staging import CorpusWriter

    writer = CorpusWriter("documents")
    for row in section_rows:
        writer.write({
            "document_id": doc_id,
            "section_num": section_num,
            "text_la": text_la,      # None values are dropped
            "text_pt": text_pt,
            "meta_json": meta_dict,  # dict is embedded as object, not string
        })
    writer.flush()                   # or use as context manager

Usage (other corpora):
    with CorpusWriter("library") as w:
        for doc in docs:
            w.write({"id": doc.id, "title": doc.title, ...})

Routing by corpus kind:
    documents   → data/corpus/documents/<record["document_id"]>/sections.jsonl
    encyclopedia→ data/corpus/encyclopedia/<title[0]>/articles.jsonl
    bible       → data/corpus/bible/<record["book_id"]>/verses.jsonl
    patristic   → data/corpus/patristic/<author>/<work>/sections.jsonl
                  (author/work extracted from record["chapter_id"])
    summa       → data/corpus/summa/articles.jsonl
    library     → data/corpus/library/docs.jsonl
    ccc         → data/corpus/ccc/paragraphs.jsonl
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CORPUS_DIR = PROJECT_ROOT / "data" / "corpus"

_FILENAMES: dict[str, str] = {
    "documents":    "sections.jsonl",
    "patristic":    "sections.jsonl",
    "encyclopedia": "articles.jsonl",
    "bible":        "verses.jsonl",
    "summa":        "articles.jsonl",
    "library":      "docs.jsonl",
    "ccc":          "paragraphs.jsonl",
}


class CorpusWriter:
    """Buffer + route corpus records to JSONL files in data/corpus/."""

    def __init__(self, kind: str, dry_run: bool = False) -> None:
        if kind not in _FILENAMES:
            raise ValueError(f"unknown corpus kind {kind!r}; choices: {list(_FILENAMES)}")
        self._kind = kind
        self._dry_run = dry_run
        # path → list of records
        self._buffers: dict[Path, list[dict[str, Any]]] = {}

    # ── public API ────────────────────────────────────────────────────────────

    def write(self, record: dict[str, Any]) -> None:
        """Buffer one record.  None values are dropped; nested dicts/lists
        are kept as-is (written as embedded JSON objects, not strings)."""
        clean = {k: v for k, v in record.items() if v is not None}
        path = self._route(clean)
        self._buffers.setdefault(path, []).append(clean)

    def flush(self) -> None:
        """Write all buffered records to disk and update manifests."""
        if self._dry_run:
            total = sum(len(v) for v in self._buffers.values())
            print(f"  [staging dry-run] {self._kind}: would write {total} records "
                  f"to {len(self._buffers)} file(s)")
            return
        for path, records in sorted(self._buffers.items()):
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as f:
                for r in records:
                    f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
        self._write_manifests()

    # ── context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> "CorpusWriter":
        return self

    def __exit__(self, *_: Any) -> None:
        self.flush()

    # ── internals ─────────────────────────────────────────────────────────────

    def _route(self, record: dict[str, Any]) -> Path:
        kind = self._kind
        fname = _FILENAMES[kind]

        if kind == "documents":
            doc_id = record.get("document_id")
            if not doc_id:
                raise ValueError(f"documents record missing 'document_id': {record!r}")
            return CORPUS_DIR / "documents" / doc_id / fname

        if kind == "encyclopedia":
            title = record.get("title") or record.get("id") or "0"
            letter = title[0].lower() if title[0].isalpha() else "0"
            return CORPUS_DIR / "encyclopedia" / letter / fname

        if kind == "bible":
            book_id = record.get("book_id")
            if not book_id:
                raise ValueError(f"bible record missing 'book_id': {record!r}")
            return CORPUS_DIR / "bible" / book_id / fname

        if kind == "patristic":
            chapter_id = record.get("chapter_id", "")
            parts = chapter_id.split("/")
            if len(parts) >= 2:
                author, work = parts[0], parts[1]
            elif len(parts) == 1:
                author, work = parts[0], "_misc"
            else:
                author, work = "_unknown", "_misc"
            return CORPUS_DIR / "patristic" / author / work / fname

        # Single-file corpora: summa, library, ccc
        return CORPUS_DIR / kind / fname

    def _write_manifests(self) -> None:
        """Write one manifest.json per touched corpus root directory."""
        # Group paths by their corpus root (depth depends on kind)
        roots: dict[Path, dict[str, int]] = {}
        for path, records in self._buffers.items():
            root = self._corpus_root(path)
            key = "/".join(path.relative_to(root).parts)
            roots.setdefault(root, {})[key] = len(records)

        ts = datetime.now(timezone.utc).isoformat()
        for root, counts in roots.items():
            manifest = {"exported_at": ts, "counts": counts}
            manifest_path = root / "manifest.json"
            with manifest_path.open("w", encoding="utf-8") as f:
                json.dump(manifest, f, ensure_ascii=False, indent=2)
                f.write("\n")

    def _corpus_root(self, path: Path) -> Path:
        """Return the top-level corpus directory for a given file path."""
        # data/corpus/<kind>/...  → data/corpus/<kind>/
        rel = path.relative_to(CORPUS_DIR)
        return CORPUS_DIR / rel.parts[0]
