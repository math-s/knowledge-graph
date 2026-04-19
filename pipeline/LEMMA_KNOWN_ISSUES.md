# Lemma data: known issues

Findings from the post-Step-3 sanity check. Updated as fixes land.

**Final coverage after fix pass:**
| | sources | tokens | matched |
|---|---:|---:|---:|
| Latin bible | 35,507 | 600,923 | **94.44%** (was 90.88%) |
| Greek bible | 35,084 | 678,570 | **91.23%** (was 89.0%) |
| Latin patristic (all 205 sections) | 205 | 9,943,461 | **94.75%** |
| └─ thomas-aquinas (ittb) | 84 | 6,845,686 | 96.38% |
| └─ tertullian | 30 | 1,579,420 | 92.89% |
| └─ ambrose | 4 | 342,453 | 90.99% |
| └─ augustine | 9 | 99,835 | 91.73% |
| └─ isidore-seville | 21 | 611,400 | 86.81% |
| └─ (others — bede, arnobius, cassiodorus, etc.) | 57 | rest | 86–92% |

**Total `contains_lemma` edges:** 1,162,465. **DB size:** 6.1 GB.

## ✅ Resolved

### 1. HTML pollution in Greek Bible verses — FIXED
Stripped residual markup + appended English from 15 NT verses; re-lemmatized.
Garbage-lemma count now 0.
*Script:* `pipeline/scripts/fix_greek_bible_html.py`

### 2. Stanza scholastic-Latin errors in Summa — FIXED
Re-lemmatized all 84 Summa sections with `ittb` treebank. The original
proiel-introduced cluster (`inquantum`, `compr(eh)endo`, `seipsus`,
`est,um`, `calendar`, `greek.expression`) is gone.
*Script:* `pipeline/scripts/relemmatize_summa_ittb.py`
*Treebank-aware cache key:* `pipeline/src/lemmatizer.py::hash_text`
*Treebank-aware refresh:* `pipeline/scripts/refresh_lemma_resolution.py::_treebank_for`

### 3. Biblical proper-noun gap — REDUCED
Added 500 supplementary entries to `lemma_la` (top unmatched PROPN lemmas).
1-Chronicles coverage jumped 78.57% → **87.14%** (+8.6 pts). Stored under
ids like `bib:Iacob` with `source_ref='supplement:bible-propn'` so they're
identifiable / removable as a set.
*Script:* `pipeline/scripts/inject_lemma_la_supplement.py`

### 4. Capitalization-dependent Stanza inconsistency — FIXED
`Lemmatizer.resolve_token` now falls back to the lowercased surface form
when the primary lemma resolution fails. John 1:1 `Verbum` (positions 3,12)
correctly resolves to `verbum` despite Stanza emitting fictitious `Verbus`.

### 5. Whole-corpus `vocab` query slowness — FIXED
Added `bible_lemma_pos_freq` pre-aggregation table; vocab endpoint routes
`corpus=all` queries through it.
- `corpus=all + pos=VERB`: 1300ms → **8ms** (162× speedup)
- `corpus=all` (no pos): 1300ms → 299ms (aggregates across pos rows; could
  be 50ms with a second pre-agg, but acceptable)

### 6. Stanza known-bug aliases (NEW; surfaced during ittb migration)
ittb (the Aquinas treebank) has its own basic-verb errors: `videre→vido`
(26,275 tokens), `venit→vigo` (1,923). Captured in a hand-curated
`lemma_alias` table that the `LemmaIndex.resolve` consults first.
*Schema:* `pipeline/scripts/inject_lemma_alias_overrides.py`
*Resolver hook:* `pipeline/src/lemmatizer.py::LemmaIndex.overrides`

### 7. Patristic backlog (un-lemmatized Latin) — DONE
121 of 124 sections processed (3 empty/skipped): Tertullian, Augustine,
Isidore, Ambrose, Cassiodorus, Bede, Arnobius, Hugo of St Victor, etc.
Used default `proiel` treebank (suits early-Christian Latin). 16 min run.
Coverage 86-93% per author. `isidore-seville` (86.81%) lowest — encyclopedic
content has lots of specialized vocabulary L&S doesn't fully cover.

## ⏳ Still open

(none — all known issues resolved.)
