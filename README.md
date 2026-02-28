# Catechism Knowledge Graph

Interactive visualization of the **Catechism of the Catholic Church** — 2,865 paragraphs connected by cross-references, rendered as a navigable knowledge graph.

## Architecture

- **Data pipeline** (`pipeline/`): Python + Pydantic + NetworkX. Downloads CCC data, extracts cross-references from shared footnote citations, computes ForceAtlas2 layout, exports to static JSON.
- **Web app** (`web/`): Next.js 15 (static export) + Sigma.js (WebGL graph rendering) + Tailwind CSS. Deploys to GitHub Pages.

## Development

### Prerequisites

- Node.js 22+
- Python 3.11+

### Run the data pipeline

```bash
cd pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cd ..
python3 pipeline/scripts/run_pipeline.py
```

This downloads the CCC data, builds the graph, computes layout, and outputs JSON files to `web/public/data/`.

### Run the web app

```bash
cd web
npm install
npm run dev
```

Open http://localhost:3000 to see the app. The graph explorer is at `/graph`.

### Build for production

```bash
cd web
npm run build
npx serve out
```

## Pages

- `/` — Landing page with stats
- `/graph` — Full graph explorer (Sigma.js WebGL)
- `/paragraph/[id]` — Paragraph detail (2,865 pages)
- `/structure` — Hierarchical CCC browser

## Data source

[nossbigg/catechism-ccc-json](https://github.com/nossbigg/catechism-ccc-json) v0.0.2 — scraped from Vatican.va.
