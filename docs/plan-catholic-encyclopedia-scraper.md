# Plan: Scrape Catholic Encyclopedia from New Advent

## Goal
Scrape the full Catholic Encyclopedia (~11,500 articles) from newadvent.org/cathen/
and load into the knowledge-graph SQLite database.

The Catholic Encyclopedia (1907-1913) is public domain.

## Source Structure

### Index Pages (26 total)
- Full index: `https://www.newadvent.org/cathen/{letter}-ce.htm` (a-ce.htm through z-ce.htm)
- Abridged index: `https://www.newadvent.org/cathen/{letter}.htm` (subset, skip these)
- Each entry: `<a href="../cathen/XXXXX.htm">Title</a> - Short description<br>`

Article counts per letter:
```
a:1349  b:969   c:1287  d:494   e:411   f:396   g:540   h:539
i:218   j:332   k:165   l:574   m:993   n:287   o:277   p:884
q:37    r:452   s:981   t:528   u:91    v:294   w:303   x:6
y:20    z:78    TOTAL: ~11,500
```

### Article Pages
- URL pattern: `https://www.newadvent.org/cathen/{id}.htm` (e.g., `05649a.htm`)
- Content lives in `<div id="springfield2">`
- Title in `<h1>` tag (also in `<title>CATHOLIC ENCYCLOPEDIA: {title}</title>`)
- Strip boilerplate:
  - Donation prompt: `<p><em><a href="https://gumroad.com/l/na2">Please help support...</a></em></p>`
  - Footer: everything after `<h2>About this page</h2>`
  - Ad divs: `<div class='catholicadnet-*'>`

## Database Schema

```sql
CREATE TABLE encyclopedia (
    id TEXT PRIMARY KEY,          -- URL id, e.g. "05649a"
    title TEXT NOT NULL,          -- article title, e.g. "Evil"
    summary TEXT,                 -- short description from index page
    text_en TEXT,                 -- full article text (plain text, stripped HTML)
    url TEXT                      -- full source URL
);

CREATE VIRTUAL TABLE encyclopedia_fts USING fts5(
    id, title, summary, text_en
);
```

## Implementation Steps

### Step 1: Scrape index pages
```python
# For each letter a-z, fetch {letter}-ce.htm
# Parse all <a href="../cathen/XXXXX.htm">Title</a> - Description<br>
# Build list of (article_id, title, summary, url)
# ~11,500 entries expected
```

### Step 2: Download article HTML
```python
# For each article URL, download HTML to pipeline/data/raw/cathen/{id}.htm
# Skip if file already exists (cache)
# Rate limit: 0.3-0.5s between requests (~1-1.5 hours total)
# Log progress every 100 articles
```

### Step 3: Parse article content
```python
# For each cached HTML file:
# 1. Find <div id="springfield2">
# 2. Remove donation prompt <p><em>..support..</em></p>
# 3. Remove everything after <h2>About this page</h2>
# 4. Remove ad divs (class contains 'catholicadnet')
# 5. Extract text with get_text(separator=" ", strip=True)
```

### Step 4: Load into SQLite
```python
# CREATE TABLE encyclopedia ...
# INSERT all articles
# CREATE VIRTUAL TABLE encyclopedia_fts ...
# INSERT INTO encyclopedia_fts SELECT id, title, summary, text_en FROM encyclopedia
```

### Step 5: Add CLI command
```python
# In pipeline/src/chat/cli.py, add:
@cli.command("search-encyclopedia")
@click.argument("query")
@click.option("--limit", "-n", default=10)
# Queries encyclopedia_fts, returns matching articles

@cli.command("encyclopedia")
@click.argument("article_id")
# Gets a specific article by ID
```

### Step 6: Link to existing data (optional enrichment)
```python
# Cross-reference encyclopedia articles with:
# - lexicon terms (match by title)
# - CCC paragraphs (if articles reference CCC numbers)
# - entities (match encyclopedia titles to entity labels)
```

## Script Location
Create: `pipeline/src/scraper_cathen.py`

## CLI Flags
```
--download / --no-download    # control whether to fetch from web
--load / --no-load            # control whether to load into DB
--dry-run                     # just show what would be done
--delay FLOAT                 # seconds between HTTP requests (default 0.5)
--db PATH                     # path to SQLite DB
```

## Reference: Similar Scripts
- `pipeline/src/scraper_patristic.py` — same site, similar HTML structure, same caching pattern
- `pipeline/src/build_lexicon.py` — similar DB loading + FTS pattern

## Notes
- Use `get_text(separator=" ", strip=True)` to avoid missing-space bug (learned from patristic cleanup)
- The site uses iso-8859-1 or utf-8 encoding — check `<meta charset>` per page
- Some articles may span multiple pages (rare) — check for "continued" links
- WAL files (*.db-shm, *.db-wal) should be removed before any git operations on the DB
