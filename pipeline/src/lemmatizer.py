"""Stanza-based lemmatizer for Vulgate Latin and biblical Greek.

Treebank choices (after Step 3a bake-off + Matthew validation):
  - Latin: stanza/la/proiel — biblical/early-Christian Latin, ~94% lookup rate.
    The bake-off had ittb edging proiel by 2pts on 55 random verses, but ittb
    has systematic errors on common Vulgate verbs (videre→"vido", venit→"vigo",
    vobis→"vobus") that proiel handles correctly. Aggregate score was misleading.
  - Greek: stanza/grc/proiel — ~94% lookup rate on LXX/NT.

If we ever lemmatize Summa Latin specifically, ittb is worth re-evaluating
(it's Aquinas-trained); for biblical text proiel is the correct choice.

Wraps Stanza with the normalizations needed to join against our lemma_la /
lemma_el dictionaries:
  - æ → ae, œ → oe (Vulgate orthography)
  - macron / breve folded for Latin lookup
  - homograph suffix stripped (L&S "in1" matches Stanza "in")
  - trailing punctuation stripped from token forms
"""

from __future__ import annotations

import re
import sqlite3
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

# Treebank choices — locked in by Step 3a bake-off.
TREEBANKS = {"la": "proiel", "el": "proiel"}
LANG_TO_STANZA = {"la": "la", "el": "grc"}

SKIP_POS = {"PUNCT", "NUM", "X", "SYM", None}
# Keep PROPN — proper nouns like "Christus", "Maria" are real lemmas in L&S/LSJ.

HOMOGRAPH_SUFFIX = re.compile(r"\d+$")
PUNCT_TRIM = re.compile(r"^[\W_]+|[\W_]+$", re.UNICODE)

# Vulgate-era ligatures and orthography quirks → restore classical spelling
# so the tokenizer (trained on classical text) lemmatizes correctly.
LIGATURE_MAP = str.maketrans({
    "æ": "ae", "Æ": "Ae",
    "œ": "oe", "Œ": "Oe",
    # j/v are sometimes used in Vulgate editions; Stanza ittb expects classical i/u
    "j": "i", "J": "I",
})


_LA_JV_FOLD = str.maketrans({"j": "i", "J": "i", "v": "u", "V": "u"})


def _fold_la(s: str) -> str:
    """Lowercase, strip macrons/breves, fold j→i and v→u (classical form)."""
    if not s:
        return ""
    nfd = unicodedata.normalize("NFD", s)
    stripped = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return stripped.lower().translate(_LA_JV_FOLD).strip()


def _fold_el(s: str) -> str:
    if not s:
        return ""
    return unicodedata.normalize("NFC", s).lower().strip()


def _fold(lemma: str, lang: str) -> str:
    return _fold_la(lemma) if lang == "la" else _fold_el(lemma)


def normalize_text(text: str, lang: str) -> str:
    """Pre-tokenization text normalization."""
    if lang == "la":
        return text.translate(LIGATURE_MAP)
    return text


def trim_punct(form: str) -> str:
    """Strip leading/trailing punctuation that Stanza occasionally bundles in."""
    if not form:
        return form
    return PUNCT_TRIM.sub("", form)


@dataclass
class Token:
    """Stanza's raw output for one content token. lemma_id is resolved at
    materialization time (cheap dict lookup), not stored in the parse cache —
    that way fixes to LemmaIndex don't require invalidating cached parses."""
    form: str
    lemma: str
    pos: str | None
    feats: str | None


class LemmaIndex:
    """Resolve a Stanza lemma string to a canonical lemma_la / lemma_el id.

    Lookup order:
      1. Exact match on `id` (after lowercasing).
      2. Match on folded `lemma` column.
      3. Bare-form match: dict entry's id with trailing digits stripped
         (handles L&S homograph numbering — "in" → first of in1..in9).

    Ambiguous bare-form matches deterministically resolve to the lowest-numbered
    homograph (in1 not in9). The full candidate list is preserved in
    `bare_to_ids` for callers who want to record alternatives.
    """

    # Sort key shared by all alias maps. Prefer lowercase over uppercase
    # ("rex1" over "Rex2") because Stanza lowercases output and proper-noun
    # homographs (Rex, Petrus, …) are almost never the right target for a
    # generic surface form. Then prefer shorter, then alphabetic.
    _PREFERENCE_KEY = staticmethod(lambda x: (not x[0].islower(), len(x), x))

    def __init__(self, conn: sqlite3.Connection, lang: str) -> None:
        self.lang = lang
        self.id_aliases: dict[str, list[str]] = {}     # folded id → [original ids]
        self.lemma_aliases: dict[str, list[str]] = {}  # folded lemma → [original ids]
        self.bare_to_ids: dict[str, list[str]] = {}    # "in" → ["in1","in2",...]
        self.overrides: dict[str, str] = {}            # folded bad lemma → canonical id

        if lang == "la":
            rows = conn.execute("SELECT id, lemma FROM lemma_la").fetchall()
        else:
            rows = conn.execute("SELECT id, lemma FROM lemma_el").fetchall()

        # Hand-curated mappings for known Stanza lemmatizer bugs (lemma_alias
        # table). Optional — script that creates it may not have run yet.
        try:
            for bad, correct in conn.execute(
                "SELECT bad_lemma, correct_id FROM lemma_alias WHERE lang = ?",
                (lang,),
            ):
                self.overrides[_fold(bad, lang)] = correct
        except sqlite3.OperationalError:
            pass  # table doesn't exist yet — fine

        for original_id, lemma_text in rows:
            id_folded = _fold(original_id, lang)
            lemma_folded = _fold(lemma_text, lang) if lemma_text else ""
            self.id_aliases.setdefault(id_folded, []).append(original_id)
            if lemma_folded:
                self.lemma_aliases.setdefault(lemma_folded, []).append(original_id)
            bare = HOMOGRAPH_SUFFIX.sub("", id_folded)
            if bare and bare != id_folded:
                self.bare_to_ids.setdefault(bare, []).append(original_id)

        for ids in self.id_aliases.values():
            ids.sort(key=LemmaIndex._PREFERENCE_KEY)
        for ids in self.lemma_aliases.values():
            ids.sort(key=LemmaIndex._PREFERENCE_KEY)
        for ids in self.bare_to_ids.values():
            ids.sort(key=LemmaIndex._PREFERENCE_KEY)

    def resolve(self, lemma: str) -> tuple[str | None, list[str]]:
        """Return (canonical_id, alternatives). alternatives empty unless ambiguous.

        Hand-curated overrides (lemma_alias table) win first, then we gather
        candidates from all three alias tables (id / lemma / bare) and re-sort
        by preference. The bare-form table with the right answer (`rex1`) wins
        over a lemma-form table that only has the proper-noun homograph
        (`Rex2`, because L&S stores its lemma as 'Rex').
        """
        folded = _fold(lemma, self.lang)
        if not folded:
            return None, []
        if folded in self.overrides:
            return self.overrides[folded], []
        candidates: list[str] = []
        for table in (self.id_aliases, self.lemma_aliases, self.bare_to_ids):
            candidates.extend(table.get(folded, []))
        if not candidates:
            return None, []
        seen: set[str] = set()
        unique = [c for c in candidates if not (c in seen or seen.add(c))]
        unique.sort(key=LemmaIndex._PREFERENCE_KEY)
        return unique[0], unique[1:]


class Lemmatizer:
    """Lazy Stanza pipeline + dictionary index for one language."""

    def __init__(self, lang: str, conn: sqlite3.Connection, treebank: str | None = None):
        if lang not in ("la", "el"):
            raise ValueError(f"unsupported lang: {lang}")
        self.lang = lang
        # Default treebank per language is set by Step 3a bake-off; callers can
        # override (e.g. ittb for Aquinas-era Latin) without affecting other runs.
        self.treebank = treebank or TREEBANKS[lang]
        self.index = LemmaIndex(conn, lang)
        self._nlp = None

    def _ensure_nlp(self) -> None:
        if self._nlp is not None:
            return
        import stanza
        self._nlp = stanza.Pipeline(
            LANG_TO_STANZA[self.lang],
            processors="tokenize,pos,lemma",
            package=self.treebank,
            use_gpu=False,
            verbose=False,
            download_method=None,  # assume already downloaded
        )

    def resolve_token(self, token: Token) -> tuple[str | None, list[str]]:
        """Resolve a Token's lemma_id with a surface-form fallback.

        Stanza occasionally emits non-existent lemmas for capitalized tokens
        (e.g. clause-initial 'Verbum' → fictitious 'Verbus'). When the primary
        resolution fails, retry against the lowercased surface form — this
        catches the common case where the form itself is the canonical lemma
        (mostly nominative singular).
        """
        lemma_id, alts = self.index.resolve(token.lemma)
        if lemma_id is None and token.form:
            lemma_id, alts = self.index.resolve(token.form)
        return lemma_id, alts

    def parse(self, text: str) -> list[Token]:
        if not text or not text.strip():
            return []
        self._ensure_nlp()
        normalized = normalize_text(text, self.lang)
        doc = self._nlp(normalized)
        tokens: list[Token] = []
        for sent in doc.sentences:
            for w in sent.words:
                pos = w.upos
                if pos in SKIP_POS:
                    continue
                form = trim_punct(w.text or "")
                lemma = trim_punct(w.lemma or "")
                if not form or not lemma:
                    continue
                tokens.append(Token(
                    form=form,
                    lemma=lemma,
                    pos=pos,
                    feats=w.feats,
                ))
        return tokens


def hash_text(lang: str, text: str, treebank: str | None = None) -> str:
    """Stable cache key for (lang, treebank, text).

    The default treebank for a language is *elided* from the key, so existing
    cache entries keyed under just (lang, text) remain valid. Callers using a
    non-default treebank (e.g. `ittb` for Aquinas) get a separate cache
    namespace automatically.
    """
    import hashlib
    h = hashlib.sha1()
    h.update(lang.encode("utf-8"))
    h.update(b"\0")
    if treebank and treebank != TREEBANKS.get(lang):
        h.update(treebank.encode("utf-8"))
        h.update(b"\0")
    h.update(text.encode("utf-8"))
    return h.hexdigest()


_SENTENCE_SPLIT = re.compile(r"(?<=[.;:!?])\s+")


def chunk_text(text: str, max_chars: int = 50000) -> list[str]:
    """Split long texts on sentence-ish boundaries so Stanza never sees a giant
    document at once. Bounds memory use and lets the cache hit per-chunk on
    re-runs. Short texts pass through unchanged."""
    if len(text) <= max_chars:
        return [text]
    parts = _SENTENCE_SPLIT.split(text)
    chunks: list[str] = []
    buf: list[str] = []
    buf_len = 0
    for part in parts:
        # +1 for the joining space
        addition = len(part) + (1 if buf else 0)
        if buf_len + addition > max_chars and buf:
            chunks.append(" ".join(buf))
            buf = [part]
            buf_len = len(part)
        else:
            buf.append(part)
            buf_len += addition
    if buf:
        chunks.append(" ".join(buf))
    return chunks
