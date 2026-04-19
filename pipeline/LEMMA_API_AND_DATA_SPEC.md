# Lemma layer — API & data spec for UX work

Handoff doc for a follow-up Claude session focused on UX. Everything below is
**already shipping** in the API and SQLite DB (`data/knowledge-graph.db`,
~7.2 GB). Read this before designing any lemma-aware UI.

Coverage today: ~94% Latin Bible, ~91% Greek Bible, ~95% Latin Summa, plus
~60 sections of other Latin patristic authors. ~1.16M `contains_lemma` graph
edges. Greek patristic: essentially empty (3 sections only).

---

## 1. Quick mental model

Three node types added by this layer:

```
lemma-la:<id>     — one node per Latin headword (~52k from L&S + ~500 supplement)
lemma-el:<id>     — one node per Greek headword (~117k from LSJ-Logeion)
                    Greek ids are Perseus beta-code (e.g. "lo/gos", "*xristo/s")
```

Connected via:

```
contains_lemma  — bible-verse:matt-5:3  →  lemma-la:beatus
contains_lemma  — patristic-section:thomas-aquinas/...  →  lemma-la:verbum
```

Plus token-level detail in `text_lemma_forms` (form, lemma, pos, feats per
position per source).

---

## 2. API endpoints

All under `api/routers/lexicon.py` and `api/routers/search.py`. All return
JSON. Latencies measured on the local DB after the indexing pass.

### `GET /lexicon/{lang}` — search the dictionary
- `lang` ∈ `la` | `el`
- `q` (required) — FTS query against `lemma + definition_en`
- `lemma_only` (bool, default `false`) — restrict match to the lemma column
- `limit` (default 20, max 100)
- Returns: `{query, lang, count, results: [{id, lemma, snippet, rank}]}`
- ~50–200ms; FTS5-backed.

### `GET /lexicon/{lang}/entry?id=…` or `?lemma=…`
- One of `id` (canonical L&S/LSJ key) or `lemma` (surface lemma) required
- Returns the entry plus an `occurrences` summary:
  ```json
  {
    "id": "gratia", "lemma": "grātĭa", "pos": "noun",
    "definition": "...",
    "occurrences": {
      "total_tokens": 7586, "total_sources": 2058,
      "by_source_type": {"bible-verse": {...}, "patristic-section": {...}},
      "top_forms": [{"form": "gratiam", "count": 130}, ...]
    }
  }
  ```
- ~150–300ms (does several aggregations against `text_lemma_forms`).
- Greek entry also has `gender` and `etymology`.

### `GET /lexicon/{lang}/occurrences?id=…&limit=&offset=&source_type=&snippet_chars=`
- Paginated list of texts containing the lemma, with text snippets.
- `source_type` optional filter: `bible-verse` | `patristic-section`.
- Each result: `{source_type, source_id, token_count, forms[], text}`.
- ~150–400ms; resolves verse/section text per page.

### `GET /lexicon/{lang}/hapax?corpus=&limit=&offset=`
- Lemmas appearing **exactly once** in a chosen corpus.
- `corpus` ∈ `nt` | `ot` | `all` | `gospels` | `pauline` | `catholic-epistles` | `<book_id>`
- Each result: `{id, lemma, pos, definition, form, source_id, verse_text}`.
- ~20–80ms (uses pre-aggregated `bible_lemma_per_book`).

### `GET /lexicon/{lang}/vocab?corpus=&pos=&min_count=&limit=&offset=`
- Top lemmas by frequency in a corpus slice. POS filter optional (Universal POS:
  `NOUN`, `VERB`, `ADJ`, `ADV`, `PROPN`, `ADP`, `DET`, ...).
- Each result: `{id, lemma, pos, count, verses, definition_preview}`.
- ~15–100ms.

### `GET /search/lemma?q=&lang=&source_type=&limit=&offset=`
- **Lemma-aware search** — query → matching lemmas via lexicon FTS → verses/
  sections containing any of those lemmas.
- `q="love"` (English) finds verses with any form of `amare`/`amo`/`diligo`/
  `dilectio` etc. (and `ἀγαπάω`/`φιλέω` if `lang=el|both`).
- Returns `{query, lemmas_matched, total, results}` where each result has
  matched_forms, matched_lemma_ids, distinct_lemmas, total_hits, text snippet.
- ~150–800ms depending on how many lemmas the query expands into.

### Other endpoints (not lemma-specific but useful for cross-linking)
- `GET /search/bible?q=…&lang=…` — literal-text FTS over `bible_verses`
- `GET /search/patristic?q=…&lang=…` — literal-text FTS over `patristic_sections`
- `GET /bible/books`, `/bible/books/{id}`, `/bible/books/{id}/chapters/{n}`
- `GET /paragraphs/{id}` — CCC paragraphs (already had `themes`, `entities`,
  `topics`, `bible_citations`, `document_citations`, `author_citations`)

---

## 3. Direct-DB capabilities (use when API doesn't expose what you need)

The DB ships with these tables/indexes specifically to make lemma queries fast.
Open it read-only with `mode=ro` + `temp_store = MEMORY` (FTS5 needs it).

### Core lemma tables
| Table | Rows | Purpose |
|---|---:|---|
| `lemma_la` | 52,091 | L&S Latin headwords + 500 biblical-PROPN supplement (`source_ref='supplement:bible-propn'`) |
| `lemma_el` | 117,080 | LSJ-Logeion Greek headwords (Unicode) |
| `lemma_la_fts`, `lemma_el_fts` | – | FTS5 over `lemma + definition_en` |
| `lemma_alias` | 6 | Hand-curated Stanza-bug overrides (extend freely) |
| `text_lemma_forms` | 11.2M | Token-level: `(source_type, source_id, lang, position, form, lemma, lemma_id, pos, feats)` |
| `lemma_parse_cache` | 72.5k | sha1(lang[+treebank]+text) → tokens_json. Lets re-resolution skip Stanza. |

### Pre-aggregations (the secret sauce — query these, not text_lemma_forms)
| Table | Rows | What it answers fast |
|---|---:|---|
| `lemma_corpus_freq (lang, lemma_id, tokens, sources)` | 26.5k | Whole-corpus frequency of a lemma; used by `/search/lemma` ranking |
| `bible_lemma_pos_freq (lang, lemma_id, pos, tokens, sources)` | 20.4k | Whole-Bible vocab/freq by POS; used by `vocab?corpus=all` |
| `bible_lemma_per_book (book_id, lang, lemma_id, pos, tokens, sources)` | 144k | Per-corpus hapax + vocab; sub-100ms for any corpus slice |

### Graph
- `graph_nodes` — lemma nodes coexist with verse, paragraph, summa-article,
  patristic-section, encyclopedia, theme, entity, author nodes
- `graph_edges` — `contains_lemma` (1.16M of them), plus the existing
  `cites`, `discussed_in`, `shared_topic`, `mentions`, `child_of`, etc.

### Indexes worth knowing
- `idx_tlf_lemma_id (lemma_id)` — fast "all rows for a lemma"
- `idx_tlf_lang_lemma_id (lang, lemma_id)` — fast lang-scoped lemma lookup
- `idx_tlf_source_id (source_id)` — fast source-id JOIN (cooc queries)
- `idx_tlf_source_lang_lemma (source_id, lang, lemma_id)` — covers per-book LIKE-prefix scans (use `INDEXED BY` if planner picks wrong)

---

## 4. UX feature ideas this data unlocks

### Low effort, high impact
- **Inline reader tooltips** — on the verse view, hovering a word shows its
  form, lemma, POS, and a snippet of the L&S/LSJ definition. All data is in
  `text_lemma_forms` (pos+lemma per position) joined to `lemma_{lang}`.
- **Lemma chip in search results** — when `/search/lemma` returns matched
  lemmas, render them as clickable chips that link to `/lexicon/{lang}/entry`.
- **Hapax browser** — `/lexicon/{lang}/hapax?corpus=…` returns 20-2000 entries
  per corpus; perfect for a "rare words in this book" sidebar or course tool.
- **Vocabulary builder** — `/lexicon/{lang}/vocab?corpus=romans&pos=VERB`
  returns top verbs in Romans. Build "learn Romans in 100 verbs" lists.
- **"More verses with this lemma"** — on a verse page, show 5 random other
  verses where the same content lemmas appear (use `/lexicon/.../occurrences`).

### Medium effort
- **Side-by-side reader (Vulgate ↔ LXX)** — for any verse with both LA and EL
  text, show them aligned. Use `text_lemma_forms` to highlight matching lemmas
  by color (same color = same concept). Requires verse-level pairing only;
  per-token alignment would need extra work.
- **Cross-corpus concept page** — given a lemma (e.g. `gratia`), one page that
  shows: its definition, top forms, distribution by book (use
  `bible_lemma_per_book`), Aquinas's most-loaded articles (sort
  `text_lemma_forms` patristic rows by count), "verses where it co-occurs with
  X". This is the killer feature for theology readers.
- **Co-occurrence neighborhood** — pick a lemma, surface the top 10 lemmas
  that travel with it in the same verse. Demo at `pipeline/scripts/demo_logos_verbum.py`
  shows the SQL pattern.

### Higher effort (data is there but UI needs design)
- **Concept genealogy** — "trace gratia from Vulgate → Augustine → Aquinas →
  CCC". Combine `contains_lemma` edges with the existing CCC citation graph.
- **Lemma → entity links** — if you wire the existing entity-extraction to
  parse `lemma_{lang}.definition_en`, you get auto-discovered links like
  `lemma:gratia → entity:Grace`. Listed as deferred follow-up #7 in
  `LEMMA_FOLLOWUPS.md`.

---

## 5. What's NOT supported (gotchas)

- **Greek patristic is essentially empty** (3 sections lemmatized of ~50
  candidates). Don't ship features that assume Greek patristic coverage.
- **No LA↔EL cognate edges yet.** The `verbum/logos` pair is implicit in the
  data (parallel verses both have hits); no `cognate_of` graph edge exists.
- **No per-token cross-language alignment.** We know verse N has `λόγος` and
  the same verse has `verbum`; we don't know `λόγος` at position 4 maps to
  `verbum` at position 3. (Doable by Stanford's awesome-align or similar, but
  not built.)
- **Greek lemma ids are Perseus beta-code** (`lo/gos`, `*xristo/s`, `xa/ris`),
  containing `/`, `*`, `(`, `)`. They're SQLite-safe but URL-unsafe — pass
  via query string, not path. The Unicode `lemma` column is the human-friendly
  display value.
- **L&S homograph numbering** (`in1`, `qui1`, `sum1`) leaks into ids. The
  resolver handles this internally; the API mostly hides it; UI lookups
  should accept both `in` and `in1` if possible.
- **Some Stanza misses are unfixable.** E.g. capitalized `Verbum` at start of
  John 1:1 still resolves to `verbum` thanks to a fallback, but biblical
  proper nouns L&S doesn't catalog (Iohannes vs Ioannes spellings, place
  names) drag OT genealogy book coverage to ~85%. Document these as
  "missing translations" in tooltips rather than hiding them.
- **Definition text is raw L&S/LSJ.** No HTML, no markup — full of inline
  citations and abbreviated references. Render in a `<pre>` or with a
  monospace font for now; sanitization is a follow-up.

---

## 6. Useful query recipes

```sql
-- Verses where two lemmas co-occur (gratia AND fides)
SELECT t1.source_id, t1.source_type
FROM text_lemma_forms t1
JOIN text_lemma_forms t2 USING (source_type, source_id)
WHERE t1.lang='la' AND t1.lemma_id='gratia'
  AND t2.lang='la' AND t2.lemma_id='fides1'
GROUP BY t1.source_type, t1.source_id;

-- Top 20 content lemmas in Genesis
SELECT lemma_id, SUM(tokens) AS n
FROM bible_lemma_per_book
WHERE book_id='genesis' AND lang='la'
  AND pos IN ('NOUN','VERB','ADJ','PROPN')
GROUP BY lemma_id ORDER BY n DESC LIMIT 20;

-- Co-occurrence neighbors of any lemma X (lang='el' here)
WITH x_verses AS (
  SELECT DISTINCT source_id FROM text_lemma_forms
  WHERE lang='el' AND lemma_id='lo/gos'
)
SELECT t.lemma_id, COUNT(DISTINCT t.source_id) AS co
FROM text_lemma_forms t JOIN x_verses USING (source_id)
WHERE t.lang='el' AND t.lemma_id != 'lo/gos' AND t.lemma_id IS NOT NULL
GROUP BY t.lemma_id HAVING co >= 20 ORDER BY co DESC LIMIT 10;

-- Verses where Greek λόγος AND Latin verbum both appear
SELECT t1.source_id
FROM text_lemma_forms t1
JOIN text_lemma_forms t2 USING (source_id)
WHERE t1.lang='el' AND t1.lemma_id='lo/gos'
  AND t2.lang='la' AND t2.lemma_id='verbum'
  AND t1.source_type='bible-verse'
GROUP BY t1.source_id;

-- Top Aquinas articles by usage of a lemma
SELECT source_id, COUNT(*) AS n
FROM text_lemma_forms
WHERE lang='la' AND lemma_id='verbum'
  AND source_id LIKE 'thomas-aquinas/%'
GROUP BY source_id ORDER BY n DESC LIMIT 10;
```

---

## 7. Where to look in the code

- API: `api/routers/lexicon.py`, `api/routers/search.py`, `api/db.py`
- Pipeline core: `pipeline/src/lemmatizer.py` (LemmaIndex, resolve_token, treebank-aware hash)
- Inject scripts (one-shot, idempotent):
  - `pipeline/scripts/inject_lexicon_la.py` / `inject_lexicon_el.py` — dictionary import
  - `pipeline/scripts/inject_lemma_edges.py` — main lemmatizer driver
  - `pipeline/scripts/relemmatize_summa_ittb.py` — Summa with Aquinas treebank
  - `pipeline/scripts/inject_lemma_la_supplement.py` — biblical proper noun supplement
  - `pipeline/scripts/inject_lemma_alias_overrides.py` — Stanza-bug fixups
  - `pipeline/scripts/refresh_lemma_resolution.py` — re-resolve from cache, rebuild aggregations
  - `pipeline/scripts/fix_greek_bible_html.py` — one-shot data cleanup
- Demo: `pipeline/scripts/demo_logos_verbum.py` (works as a worked-example template)
- Followups + known issues: `pipeline/LEMMA_FOLLOWUPS.md`, `pipeline/LEMMA_KNOWN_ISSUES.md`

---

## 8. Performance contract

After the indexing/aggregation work, these are the latency floors and ceilings
worth honoring in UI design:

| Operation | Latency | Notes |
|---|---|---|
| Single lemma entry + summary | 150–300 ms | OK for click-through |
| Per-corpus vocab/hapax (any slice) | 15–100 ms | Live filter is fine |
| Lemma-aware search | 150–800 ms | Show a spinner |
| FTS over lexicon | 50–200 ms | OK for typeahead |
| Verse text resolution (per ~20 verses) | 30–60 ms | Within page budget |
| Whole-corpus token aggregation (no index) | 1–10 s | Avoid; use pre-agg tables |

If you need sub-50ms across the board, build per-feature pre-aggregations the
same way `bible_lemma_per_book` was added — pattern documented in
`refresh_lemma_resolution.py`.
