"""Demo: tracing λόγος / verbum across the corpus.

Showcases what the lemma layer makes queryable: cross-language alignment,
co-occurrence neighborhoods, density per book, scholastic re-use.

All queries hit the lemma_id index once and aggregate in Python — no LIKE
scans of text_lemma_forms.
"""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "knowledge-graph.db"


def banner(title: str) -> None:
    print()
    print("─" * 78)
    print(f"  {title}")
    print("─" * 78)


def split_bible_sid(sid: str) -> tuple[str, int, int] | None:
    try:
        left, v = sid.rsplit(":", 1)
        b, c = left.rsplit("-", 1)
        return b, int(c), int(v)
    except Exception:
        return None


def main() -> None:
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA temp_store = MEMORY")

    # One-shot pulls — every query below filters on lemma_id (indexed).
    logos_rows = conn.execute(
        "SELECT source_type, source_id FROM text_lemma_forms "
        "WHERE lang='el' AND lemma_id='lo/gos'"
    ).fetchall()
    verbum_rows = conn.execute(
        "SELECT source_type, source_id FROM text_lemma_forms "
        "WHERE lang='la' AND lemma_id='verbum'"
    ).fetchall()

    # ── 1. Profile of each lemma ────────────────────────────────────────────
    banner("1. The lemmas")
    rows = conn.execute(
        """
        SELECT 'el' AS lang, l.id, l.lemma, l.gender,
               substr(l.definition_en, 1, 90) AS def, f.tokens, f.sources
        FROM lemma_el l JOIN lemma_corpus_freq f ON f.lang='el' AND f.lemma_id=l.id
        WHERE l.id = 'lo/gos'
        UNION ALL
        SELECT 'la', l.id, l.lemma, NULL, substr(l.definition_en, 1, 90),
               f.tokens, f.sources
        FROM lemma_la l JOIN lemma_corpus_freq f ON f.lang='la' AND f.lemma_id=l.id
        WHERE l.id = 'verbum'
        """
    ).fetchall()
    for r in rows:
        gender = f" ({r['gender']})" if r['gender'] else ""
        print(f"  [{r['lang']}] {r['lemma']}{gender}  →  {r['tokens']:,} tokens, {r['sources']:,} sources")
        print(f"      L&S/LSJ: {r['def']}…")

    # ── 2. Verses where both Greek λόγος AND Latin verbum appear ────────────
    banner("2. Verses with λόγος in Greek AND verbum in Latin (parallel hits)")
    el_bible_sids = {r["source_id"] for r in logos_rows if r["source_type"] == "bible-verse"}
    la_bible_sids_count = Counter(
        r["source_id"] for r in verbum_rows if r["source_type"] == "bible-verse"
    )
    el_bible_sids_count = Counter(
        r["source_id"] for r in logos_rows if r["source_type"] == "bible-verse"
    )
    parallels = sorted(
        (sid for sid in el_bible_sids if sid in la_bible_sids_count),
        key=lambda s: -(el_bible_sids_count[s] + la_bible_sids_count[s]),
    )[:6]
    for sid in parallels:
        parts = split_bible_sid(sid)
        if not parts:
            continue
        b, c, v = parts
        row = conn.execute(
            "SELECT text_la, text_el FROM bible_verses WHERE book_id=? AND chapter=? AND verse=?",
            (b, c, v),
        ).fetchone()
        print(f"  • {sid}  (λόγος×{el_bible_sids_count[sid]}, verbum×{la_bible_sids_count[sid]})")
        if row:
            print(f"      LA: {row['text_la'][:120]}")
            print(f"      EL: {row['text_el'][:120]}")

    # ── 3. Books ranked by λόγος density (per 1000 verses) ──────────────────
    banner("3. Where Greek λόγος is most concentrated (per 1000 verses)")
    book_hits: Counter[str] = Counter()
    for sid in (r["source_id"] for r in logos_rows if r["source_type"] == "bible-verse"):
        parts = split_bible_sid(sid)
        if parts:
            book_hits[parts[0]] += 1
    book_verse_counts = dict(conn.execute(
        "SELECT book_id, COUNT(*) FROM bible_verses WHERE text_el IS NOT NULL GROUP BY book_id"
    ).fetchall())
    ranked = sorted(
        ((b, hits, book_verse_counts.get(b, 0)) for b, hits in book_hits.items()),
        key=lambda r: -(1000.0 * r[1] / max(1, r[2])),
    )[:8]
    print(f"  {'book':<22} {'hits':>5} {'verses':>7} {'per 1k':>7}")
    for book, hits, verses in ranked:
        density = 1000.0 * hits / max(1, verses)
        print(f"  {book:<22} {hits:>5} {verses:>7} {density:>7.1f}")

    # ── 4. Co-occurrence: lemmas traveling with λόγος ───────────────────────
    banner("4. Lemmas most often co-occurring with λόγος in the same verse")
    logos_verses = list(el_bible_sids)
    if logos_verses:
        # Single bulk fetch via temp table to avoid N×lookups
        conn.execute("CREATE TEMP TABLE _lv (sid TEXT PRIMARY KEY)")
        conn.executemany("INSERT INTO _lv VALUES (?)", [(s,) for s in logos_verses])
        cooc = conn.execute(
            """
            SELECT t.lemma_id, COUNT(DISTINCT t.source_id) AS co_verses
            FROM text_lemma_forms t JOIN _lv ON _lv.sid = t.source_id
            WHERE t.lang='el' AND t.lemma_id IS NOT NULL AND t.lemma_id != 'lo/gos'
              AND t.pos NOT IN ('DET','CCONJ','SCONJ','ADP','PRON','PART')
            GROUP BY t.lemma_id
            HAVING co_verses >= 20
            """
        ).fetchall()
        # Fold corpus freq + lemma name in Python (small set)
        ids = [r["lemma_id"] for r in cooc]
        ph = ",".join("?" for _ in ids)
        meta = {r["id"]: r for r in conn.execute(
            f"SELECT id, lemma FROM lemma_el WHERE id IN ({ph})", ids
        )}
        freq = {r["lemma_id"]: r["tokens"] for r in conn.execute(
            f"SELECT lemma_id, tokens FROM lemma_corpus_freq "
            f"WHERE lang='el' AND lemma_id IN ({ph})", ids
        )}
        scored = sorted(
            ((r["lemma_id"], r["co_verses"], freq.get(r["lemma_id"], 0)) for r in cooc),
            key=lambda x: -(x[1] * x[1] / max(1, x[2])),
        )[:10]
        print(f"  (affinity = co² / corpus_freq — boosts content words)")
        print(f"  {'lemma':<14} {'co-verses':>9} {'corpus':>8} {'affinity':>9}")
        for lid, co, total in scored:
            affinity = co * co / max(1, total)
            print(f"  {meta[lid]['lemma']:<14} {co:>9} {total:>8} {affinity:>9.2f}")

    # ── 5. Aquinas's hottest verbum chapters ────────────────────────────────
    banner("5. Where Aquinas pours over verbum (Summa LA, top 5 articles)")
    verbum_summa_count: Counter[str] = Counter()
    for r in verbum_rows:
        if r["source_type"] == "patristic-section" and r["source_id"].startswith("thomas-aquinas/"):
            verbum_summa_count[r["source_id"]] += 1
    top5 = verbum_summa_count.most_common(5)
    if top5:
        ids = [sid for sid, _ in top5]
        ph = ",".join("?" for _ in ids)
        # patristic_sections has no title; chapter has it via chapter_id
        titles = {r["id"]: r["title"] for r in conn.execute(
            f"""SELECT ps.id, pc.title
                FROM patristic_sections ps
                LEFT JOIN patristic_chapters pc ON pc.id = ps.chapter_id
                WHERE ps.id IN ({ph})""",
            ids,
        )}
        for sid, hits in top5:
            title = (titles.get(sid) or "(untitled)").strip().replace("\n", " ")
            print(f"  • {sid:<48} verbum×{hits:>4}")
            print(f"      \"{title[:90]}\"")

    # ── 6. Pauline vs Johannine λόγος usage (per 1000 verses) ───────────────
    banner("6. Who uses λόγος more — Paul or John? (per 1000 verses)")
    PAULINE = {"romans","1-corinthians","2-corinthians","galatians","ephesians","philippians",
               "colossians","1-thessalonians","2-thessalonians","1-timothy","2-timothy","titus","philemon"}
    JOHANNINE = {"john","1-john","2-john","3-john","revelation"}

    # `book_hits` from query #3 already in memory; verse counts come from the
    # tiny bible_verses GROUP BY (one row per book, indexed PK).
    for label, books in (("Pauline corpus", PAULINE), ("Johannine corpus", JOHANNINE)):
        hits = sum(book_hits.get(b, 0) for b in books)
        verses = sum(book_verse_counts.get(b, 0) for b in books)
        per_1k = 1000.0 * hits / max(1, verses)
        print(f"  {label:<18} {hits:>4} hits / {verses:>5} verses → {per_1k:>5.2f} per 1k")

    print()
    conn.close()


if __name__ == "__main__":
    main()
