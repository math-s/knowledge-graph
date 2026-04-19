"""Run CLTK lemmatizer on the bake-off sample.

Run with: uv run --project pipeline --extra lemma python pipeline/scripts/lemma_bakeoff/bakeoff_cltk.py [la|el]
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent

LANG_TO_CLTK = {"la": "lat", "el": "grc"}


def run_for(lang: str) -> None:
    sample_path = HERE / f"sample_{lang}.json"
    out_path = HERE / f"out_cltk_{lang}.json"
    if not sample_path.exists():
        print(f"!! missing {sample_path}", file=sys.stderr)
        return
    with open(sample_path) as f:
        sample = json.load(f)

    print(f"[cltk/{lang}] importing CLTK + initializing pipeline (downloads on first use)...")
    t0 = time.time()
    from cltk import NLP

    nlp = NLP(language=LANG_TO_CLTK[lang], suppress_banner=True)
    load_s = time.time() - t0
    print(f"[cltk/{lang}] init took {load_s:.1f}s")

    text_field = f"text_{lang}"
    results = []
    parse_t = 0.0
    for v in sample:
        text = v[text_field]
        t0 = time.time()
        doc = nlp.analyze(text=text)
        parse_t += time.time() - t0
        tokens = []
        for w in doc.words:
            tokens.append({
                "form": getattr(w, "string", None),
                "lemma": getattr(w, "lemma", None),
                "pos": getattr(w, "upos", None) or getattr(w, "pos", None),
            })
        results.append({
            "book_id": v["book_id"],
            "chapter": v["chapter"],
            "verse": v["verse"],
            "text": text,
            "tokens": tokens,
        })

    summary = {
        "tool": "cltk",
        "lang": lang,
        "model": LANG_TO_CLTK[lang],
        "n_verses": len(results),
        "n_tokens": sum(len(r["tokens"]) for r in results),
        "load_seconds": round(load_s, 2),
        "parse_seconds_total": round(parse_t, 2),
        "parse_seconds_per_verse": round(parse_t / max(1, len(results)), 3),
        "results": results,
    }
    with open(out_path, "w") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(
        f"[cltk/{lang}] {len(results)} verses, {summary['n_tokens']} tokens "
        f"in {parse_t:.1f}s ({summary['parse_seconds_per_verse']}s/verse) → {out_path.name}"
    )


def main() -> None:
    langs = sys.argv[1:] or ["la", "el"]
    for lang in langs:
        run_for(lang)


if __name__ == "__main__":
    main()
