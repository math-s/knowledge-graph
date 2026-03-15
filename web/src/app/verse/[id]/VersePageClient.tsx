"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useEffect, useState } from "react";
import { useLang } from "@/lib/LangContext";
import { apiFetch } from "@/lib/api";
import {
  type BibleBookMeta,
  type Lang,
  type MultiLangText,
  LANG_NAMES,
  LANG_SHORT,
  resolveLang,
} from "@/lib/types";
import { SOURCE_COLORS } from "@/lib/colors";

const ALL_LANGS: Lang[] = ["la", "en", "pt", "el"];

/** Parse a verse ID like "matthew-5:3" into { bookId, chapter, verse }. */
function parseVerseId(id: string): { bookId: string; chapter: number; verse: number } | null {
  const match = id.match(/^(.+)-(\d+):(\d+)$/);
  if (!match) return null;
  return {
    bookId: match[1],
    chapter: parseInt(match[2], 10),
    verse: parseInt(match[3], 10),
  };
}

export default function VersePageClient() {
  const params = useParams<{ id: string }>();
  const verseId = params.id;
  const parsed = parseVerseId(verseId);
  const { lang, sideBySide, compareLang, setCompareLang } = useLang();

  const [meta, setMeta] = useState<BibleBookMeta | null>(null);
  const [verseText, setVerseText] = useState<MultiLangText | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!parsed) {
      setLoading(false);
      return;
    }

    async function load() {
      setLoading(true);
      try {
        const [bookData, chapterData] = await Promise.all([
          apiFetch<BibleBookMeta>(`/bible/books/${encodeURIComponent(parsed!.bookId)}`),
          apiFetch<{ verses: { verse: number; text: MultiLangText }[] }>(
            `/bible/books/${encodeURIComponent(parsed!.bookId)}/chapters/${parsed!.chapter}`,
          ),
        ]);
        setMeta(bookData);
        const found = chapterData.verses.find((v) => v.verse === parsed!.verse);
        if (found) setVerseText(found.text);
      } catch {
        // API error — leave state as null
      }
      setLoading(false);
    }
    load();
  }, [verseId]);

  if (!parsed) {
    return (
      <div className="mx-auto max-w-3xl px-6 py-12">
        <p className="text-zinc-500">Invalid verse ID format. Expected: book-chapter:verse</p>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="mx-auto max-w-3xl px-6 py-12">
        <div className="animate-pulse text-zinc-500">Loading verse...</div>
      </div>
    );
  }

  const { bookId, chapter, verse } = parsed;
  const bookName = meta?.name || bookId.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  const abbreviation = meta?.abbreviation || bookId;

  // Determine available languages
  const availableLangs = verseText
    ? ALL_LANGS.filter((l) => verseText[l])
    : [];

  const effectiveCompareLang =
    availableLangs.includes(compareLang) && compareLang !== lang
      ? compareLang
      : availableLangs.find((l) => l !== lang) || lang;

  const showSideBySide = sideBySide && verseText && availableLangs.length > 1;

  return (
    <div className="mx-auto max-w-3xl px-6 py-12">
      {/* Navigation */}
      <div className="mb-6 flex items-center gap-2 text-sm">
        <Link href="/" className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300">Home</Link>
        <span className="text-zinc-400">/</span>
        <Link href={`/bible/${bookId}`} className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300">
          {bookName}
        </Link>
        <span className="text-zinc-400">/</span>
        <Link
          href={`/bible/${bookId}/${chapter}`}
          className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300"
        >
          Chapter {chapter}
        </Link>
        <span className="text-zinc-400">/</span>
        <span className="text-zinc-700 dark:text-zinc-300">Verse {verse}</span>
      </div>

      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center gap-3">
          <span
            className="inline-block h-3 w-3 rounded-full"
            style={{ backgroundColor: SOURCE_COLORS.bible }}
          />
          <h1 className="text-2xl font-bold">
            {abbreviation} {chapter}:{verse}
          </h1>
        </div>
        <p className="mt-1 text-sm text-zinc-500">
          {bookName}, Chapter {chapter}, Verse {verse}
        </p>
      </div>

      {/* Verse text */}
      {verseText ? (
        <>
          {showSideBySide ? (
            /* ── Side-by-side mode ── */
            <div className="mb-6 grid grid-cols-2 gap-4">
              <div
                className="rounded-lg border-l-4 bg-green-50/50 p-4 dark:bg-green-900/10"
                style={{ borderLeftColor: SOURCE_COLORS.bible }}
              >
                <div className="mb-2 text-xs font-semibold text-zinc-500">
                  {LANG_NAMES[lang]}
                </div>
                <p className="text-lg leading-relaxed text-zinc-800 dark:text-zinc-200">
                  {verseText[lang] || (
                    <span className="italic text-zinc-400">Not available</span>
                  )}
                </p>
              </div>
              <div className="rounded-lg border-l-4 border-zinc-300 bg-zinc-50/50 p-4 dark:border-zinc-600 dark:bg-zinc-900/30">
                <div className="mb-2 flex items-center gap-2">
                  <span className="text-xs font-semibold text-zinc-500">
                    {LANG_NAMES[effectiveCompareLang]}
                  </span>
                  {availableLangs.filter((l) => l !== lang).length > 1 && (
                    <select
                      value={effectiveCompareLang}
                      onChange={(e) => setCompareLang(e.target.value as Lang)}
                      className="rounded border border-zinc-200 bg-white px-1 py-0.5 text-[10px] dark:border-zinc-700 dark:bg-zinc-800"
                    >
                      {availableLangs
                        .filter((l) => l !== lang)
                        .map((l) => (
                          <option key={l} value={l}>
                            {LANG_SHORT[l]}
                          </option>
                        ))}
                    </select>
                  )}
                </div>
                <p className="text-lg leading-relaxed text-zinc-700 dark:text-zinc-300">
                  {verseText[effectiveCompareLang] || (
                    <span className="italic text-zinc-400">Not available</span>
                  )}
                </p>
              </div>
            </div>
          ) : (
            /* ── Single language mode ── */
            <div
              className="mb-6 rounded-lg border-l-4 bg-green-50/50 p-4 dark:bg-green-900/10"
              style={{ borderLeftColor: SOURCE_COLORS.bible }}
            >
              <p className="text-lg leading-relaxed text-zinc-800 dark:text-zinc-200">
                {resolveLang(verseText, lang)}
              </p>
            </div>
          )}

          {/* All available translations */}
          <div className="mb-8">
            <h2 className="mb-3 text-sm font-semibold uppercase text-zinc-500">
              Available Translations
            </h2>
            <div className="space-y-3">
              {(Object.entries(verseText) as [Lang, string][])
                .filter(([, text]) => text)
                .map(([langCode, text]) => (
                  <div
                    key={langCode}
                    className={`rounded border p-3 ${
                      langCode === lang
                        ? "border-green-300 bg-green-50 dark:border-green-700 dark:bg-green-900/20"
                        : "border-zinc-200 bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-900"
                    }`}
                  >
                    <div className="mb-1 flex items-center gap-2">
                      <span className="rounded bg-zinc-200 px-1.5 py-0.5 text-[10px] font-bold text-zinc-600 dark:bg-zinc-700 dark:text-zinc-400">
                        {LANG_SHORT[langCode]}
                      </span>
                      <span className="text-xs text-zinc-500">
                        {LANG_NAMES[langCode]}
                      </span>
                    </div>
                    <p className="text-sm text-zinc-700 dark:text-zinc-300">{text}</p>
                  </div>
                ))}
            </div>
          </div>
        </>
      ) : (
        <div className="mb-8 rounded border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900">
          Verse text not available. Run the pipeline with full Bible fetch to populate.
        </div>
      )}

      {/* Navigation */}
      <div className="flex items-center justify-between border-t pt-4 dark:border-zinc-800">
        {verse > 1 ? (
          <Link
            href={`/verse/${bookId}-${chapter}:${verse - 1}`}
            className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400"
          >
            &larr; Verse {verse - 1}
          </Link>
        ) : chapter > 1 ? (
          <Link
            href={`/bible/${bookId}/${chapter - 1}`}
            className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400"
          >
            &larr; Chapter {chapter - 1}
          </Link>
        ) : (
          <span />
        )}
        <Link
          href={`/bible/${bookId}/${chapter}`}
          className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400"
        >
          Full Chapter
        </Link>
        <Link
          href={`/verse/${bookId}-${chapter}:${verse + 1}`}
          className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400"
        >
          Verse {verse + 1} &rarr;
        </Link>
      </div>
    </div>
  );
}
