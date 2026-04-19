"""Score lemma-bakeoff outputs against the lemma_la / lemma_el dictionaries.

Metric: % of content lemmas (excluding PUNCT/NUM/PROPN/X) that exist in our
dictionary. A high rate means the tokenizer's lemmas are usable for joining
text → lemma nodes; a low rate means it's emitting non-headword forms.

Run: uv run --project pipeline --extra lemma python pipeline/scripts/lemma_bakeoff/score_bakeoff.py
"""

from __future__ import annotations

import glob
import json
import re
import sqlite3
import unicodedata
from collections import Counter
from pathlib import Path

# L&S adds numeric suffixes for homographs ("in1", "qui2", "sum3"). Strip them
# so the dict accepts the bare form Stanza emits.
HOMOGRAPH_SUFFIX = re.compile(r"\d+$")

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"

# Skip these UPOS tags from scoring — punctuation/numbers/proper-nouns/symbols
# are noise for our purpose (we want lookup rate on content words).
SKIP_POS = {"PUNCT", "NUM", "PROPN", "X", "SYM", None}


def fold_la(s: str) -> str:
    """Normalize Latin lemmas: lowercase, strip macrons/breves."""
    if not s:
        return ""
    nfd = unicodedata.normalize("NFD", s)
    stripped = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return stripped.lower().strip()


def fold_el(s: str) -> str:
    """Normalize Greek lemmas: NFC, lowercase. Keep diacritics — LSJ keeps them."""
    if not s:
        return ""
    return unicodedata.normalize("NFC", s).lower().strip()


def load_dict(conn: sqlite3.Connection, lang: str) -> set[str]:
    if lang == "la":
        out: set[str] = set()
        for (val,) in conn.execute("SELECT id FROM lemma_la"):
            v = val.lower()
            out.add(v)
            out.add(HOMOGRAPH_SUFFIX.sub("", v))  # accept bare form
        for (val,) in conn.execute("SELECT lemma FROM lemma_la"):
            v = fold_la(val)
            out.add(v)
            out.add(HOMOGRAPH_SUFFIX.sub("", v))
        out.discard("")
        return out
    else:
        out = set()
        for (val,) in conn.execute("SELECT lemma FROM lemma_el"):
            v = fold_el(val)
            out.add(v)
            out.add(HOMOGRAPH_SUFFIX.sub("", v))
        out.discard("")
        return out


def fold(lemma: str, lang: str) -> str:
    return fold_la(lemma) if lang == "la" else fold_el(lemma)


def score_file(path: Path, dict_set: set[str]) -> dict:
    with open(path) as f:
        data = json.load(f)
    lang = data["lang"]
    tool = data["tool"]
    treebank = data.get("treebank", "default")

    total = 0
    skipped = 0
    matched = 0
    misses: Counter[tuple[str, str]] = Counter()  # (form, lemma)
    by_verse_rate: list[float] = []

    for verse in data["results"]:
        v_total = 0
        v_match = 0
        for tok in verse["tokens"]:
            pos = tok.get("pos")
            if pos in SKIP_POS:
                skipped += 1
                continue
            lemma = tok.get("lemma") or ""
            folded = fold(lemma, lang)
            if not folded:
                skipped += 1
                continue
            total += 1
            v_total += 1
            if folded in dict_set:
                matched += 1
                v_match += 1
            else:
                misses[(tok.get("form", ""), lemma)] += 1
        if v_total > 0:
            by_verse_rate.append(v_match / v_total)

    rate = matched / total if total else 0.0
    return {
        "tool": tool,
        "lang": lang,
        "treebank": treebank,
        "tokens_total": total + skipped,
        "tokens_scored": total,
        "tokens_matched": matched,
        "lookup_rate": round(rate, 4),
        "median_verse_rate": round(sorted(by_verse_rate)[len(by_verse_rate)//2], 4) if by_verse_rate else 0.0,
        "load_seconds": data.get("load_seconds"),
        "parse_seconds_per_verse": data.get("parse_seconds_per_verse"),
        "top_misses": misses.most_common(10),
    }


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)

    print("Loading dictionaries...")
    dict_la = load_dict(conn, "la")
    dict_el = load_dict(conn, "el")
    print(f"  lemma_la entries (folded): {len(dict_la)}")
    print(f"  lemma_el entries (folded): {len(dict_el)}")
    conn.close()

    out_files = sorted(glob.glob(str(HERE / "out_*.json")))
    if not out_files:
        raise SystemExit("No bake-off output files found.")

    rows = []
    for path in out_files:
        path = Path(path)
        with open(path) as f:
            lang = json.load(f)["lang"]
        rows.append(score_file(path, dict_la if lang == "la" else dict_el))

    # Header
    print()
    print(f"{'lang':<4} {'tool':<8} {'treebank':<12} {'tokens':>7} {'rate':>8} {'med/v':>8} {'load':>6} {'s/verse':>8}")
    print("-" * 74)
    for r in sorted(rows, key=lambda x: (x["lang"], -x["lookup_rate"])):
        print(
            f"{r['lang']:<4} {r['tool']:<8} {r['treebank']:<12} "
            f"{r['tokens_scored']:>7} {r['lookup_rate']*100:>7.2f}% "
            f"{r['median_verse_rate']*100:>7.2f}% "
            f"{r['load_seconds']:>6}s {r['parse_seconds_per_verse']:>7}s"
        )
    print()
    print("--- Top misses per run (form → lemma → count) ---")
    for r in rows:
        print(f"\n  {r['lang']}/{r['tool']}/{r['treebank']}:")
        for (form, lemma), cnt in r["top_misses"]:
            print(f"    {cnt:>3}× {form!r:>20} → {lemma!r}")


if __name__ == "__main__":
    main()
