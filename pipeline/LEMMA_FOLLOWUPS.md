# Lemma feature follow-ups

Deferred ideas after lemmatization landed (Step 3 of the lexicon plan). Pick
when there's time; each is independently useful and shippable.

Data state these depend on:
- `lemma_la` / `lemma_el` (~168k headword nodes)
- `text_lemma_forms` (token-level forms with morphology)
- `graph_edges` of type `contains_lemma` (deduped source → lemma node)

## Quick wins (hours each)

### More-like-this verses
Given a verse, find verses that share 3+ rare lemmas. TF-IDF-style weighting
(rare lemmas score higher than `et`/`καί`). Pure SQL aggregation; surface in
the verse-detail UI.

### Hapax legomena lists
Lemmas appearing exactly once in a corpus slice (whole NT, Pauline epistles,
LXX, etc.). One query per slice; classicists love these. Could ship as
`/lexicon/hapax?corpus=nt&lang=el`.

### Vocabulary lists per book
"Top 50 verbs in Romans by frequency" — three-line query, useful for language
learners. Could ship as `/bible/books/{id}/vocab?lang=la&pos=verb`.

### Inline reader tooltips
Web-side feature: in the verse view, hover a word → pop form, lemma,
definition. Data is already there via `text_lemma_forms` joined to
`lemma_{lang}`; needs frontend hookup only.

## Medium (a day or two each)

### Cross-language verse alignment
For verses present in both Vulgate and LXX/NT, align lemmas position-aware to
build an automatic Vulgate↔LXX gloss table. Rough alignment via shared verse
key; sharper alignment via word-position overlap. Prerequisite for cognate
auto-detection.

### Lemma → entity edges (Step 4 in original plan)
Scan `definition_en` for known entity names from `entity_definitions.py`
(Grace, Trinity, etc.). Creates `lemma:gratia → entity:Grace` graph edges.
Powers theme-aware navigation and cross-references.

### Lemma co-occurrence graph
Lemmas that appear in the same verse → weighted edge. Run Louvain on the
result; theological clusters likely surface automatically (e.g. λόγος-θεός,
gratia-fides-spes-caritas).

### CCC-to-source by shared lemma
Current CCC ↔ Bible/patristic edges are citation-based. Adding a "shares
vocabulary X, Y, Z" signal would surface conceptually-related-but-not-cited
connections. Cheap to compute as a post-hoc edge type.

## Ambitious

### LA ↔ EL cognate edges
Pair `verbum/logos`, `gratia/charis`, `spiritus/pneuma`, etc. Probably
hand-curate the first ~100 (highest-frequency theological terms), then
bootstrap from etymology fields + alignment data. Stored as
`graph_edges` of type `cognate_of`.

### Authorship / genre stylometry
POS + lemma-frequency profile per author. Less rigorous but interesting —
"Paul vs Luke vocabulary signature." Could surface via PCA/clustering on
per-source lemma-frequency vectors.

### Concept genealogy queries
Combine lemma edges with the existing citation graph for narrative threads:
"trace `gratia` from Vulgate → Augustine → Aquinas → CCC." API endpoint that
walks both citation and shared-lemma edges with a configurable hop limit.

---

If you only do one more, **lemma → entity edges** is highest-leverage —
small implementation, unlocks a lot of cross-references throughout the graph.
