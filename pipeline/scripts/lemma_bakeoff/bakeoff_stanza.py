"""Run Stanza lemmatizer on the Vulgate/LXX bake-off sample.

Run with: uv run --project pipeline --extra lemma python pipeline/scripts/lemma_bakeoff/bakeoff_stanza.py [la|el] [treebank]
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import stanza

HERE = Path(__file__).resolve().parent

LANG_TO_STANZA = {"la": "la", "el": "grc"}  # Stanza calls Ancient Greek "grc"


def run_for(lang: str, treebank: str = "default") -> None:
    sample_path = HERE / f"sample_{lang}.json"
    tag = treebank.replace("-", "_")
    out_path = HERE / f"out_stanza_{lang}_{tag}.json"
    if not sample_path.exists():
        print(f"!! missing {sample_path}", file=sys.stderr)
        return
    with open(sample_path) as f:
        sample = json.load(f)

    print(f"[stanza/{lang}/{treebank}] downloading model...")
    t0 = time.time()
    stanza.download(
        LANG_TO_STANZA[lang],
        processors="tokenize,pos,lemma",
        package=treebank,
        verbose=False,
    )
    print(f"[stanza/{lang}/{treebank}] download took {time.time() - t0:.1f}s")

    print(f"[stanza/{lang}/{treebank}] loading pipeline...")
    t0 = time.time()
    nlp = stanza.Pipeline(
        LANG_TO_STANZA[lang],
        processors="tokenize,pos,lemma",
        package=treebank,
        use_gpu=False,
        verbose=False,
    )
    load_s = time.time() - t0
    print(f"[stanza/{lang}] load took {load_s:.1f}s")

    text_field = f"text_{lang}"
    results = []
    parse_t = 0.0
    for v in sample:
        text = v[text_field]
        t0 = time.time()
        doc = nlp(text)
        parse_t += time.time() - t0
        tokens = []
        for sent in doc.sentences:
            for w in sent.words:
                tokens.append({
                    "form": w.text,
                    "lemma": w.lemma,
                    "pos": w.upos,
                    "feats": w.feats,
                })
        results.append({
            "book_id": v["book_id"],
            "chapter": v["chapter"],
            "verse": v["verse"],
            "text": text,
            "tokens": tokens,
        })

    summary = {
        "tool": "stanza",
        "lang": lang,
        "model": LANG_TO_STANZA[lang],
        "treebank": treebank,
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
        f"[stanza/{lang}/{treebank}] {len(results)} verses, {summary['n_tokens']} tokens "
        f"in {parse_t:.1f}s ({summary['parse_seconds_per_verse']}s/verse) → {out_path.name}"
    )


# Treebanks worth comparing for biblical / scholastic use.
DEFAULT_MATRIX = {
    "la": ["proiel", "ittb", "perseus"],
    "el": ["proiel", "perseus"],
}


def main() -> None:
    args = sys.argv[1:]
    if not args:
        for lang, banks in DEFAULT_MATRIX.items():
            for tb in banks:
                run_for(lang, tb)
        return
    lang = args[0]
    treebanks = args[1:] or DEFAULT_MATRIX.get(lang, ["default"])
    for tb in treebanks:
        run_for(lang, tb)


if __name__ == "__main__":
    main()
