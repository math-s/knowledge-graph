"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useEffect, useState } from "react";
import { useLang } from "@/lib/LangContext";
import { fetchBibleMeta, fetchBibleBookVerses } from "@/lib/graph-data";
import {
  type BibleBookMeta,
  type BibleChapterData,
  type Lang,
  type MultiLangText,
  LANG_SHORT,
  LANG_NAMES,
  resolveLang,
  resolvedLangCode,
} from "@/lib/types";
import { SOURCE_COLORS } from "@/lib/colors";

const ALL_LANGS: Lang[] = ["la", "en", "pt", "el"];

export default function BibleChapterClient() {
  const params = useParams<{ id: string; chapter: string }>();
  const bookId = params.id;
  const chapterNum = parseInt(params.chapter, 10);
  const { lang, sideBySide, compareLang, setCompareLang } = useLang();

  const [meta, setMeta] = useState<BibleBookMeta | null>(null);
  const [chapter, setChapter] = useState<BibleChapterData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      setLoading(true);
      const [metaData, versesData] = await Promise.all([
        fetchBibleMeta(),
        fetchBibleBookVerses(bookId),
      ]);
      setMeta(metaData[bookId] || null);
      if (versesData) {
        const ch = versesData.find((c) => c.chapter === chapterNum);
        setChapter(ch || null);
      }
      setLoading(false);
    }
    load();
  }, [bookId, chapterNum]);

  if (loading) {
    return (
      <div className="mx-auto max-w-3xl px-6 py-12">
        <div className="animate-pulse text-zinc-500">Loading chapter...</div>
      </div>
    );
  }

  const bookName = meta?.name || bookId.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  const abbreviation = meta?.abbreviation || bookId;

  // Determine available languages from actual verse data
  const availableLangs = chapter
    ? ALL_LANGS.filter((l) =>
        Object.values(chapter.verses).some((v) => v[l])
      )
    : [];

  // Ensure compareLang is valid for this chapter's available languages
  const effectiveCompareLang =
    availableLangs.includes(compareLang) && compareLang !== lang
      ? compareLang
      : availableLangs.find((l) => l !== lang) || lang;

  const showSideBySide = sideBySide && availableLangs.length > 1;

  return (
    <div className={`mx-auto px-6 py-12 ${showSideBySide ? "max-w-5xl" : "max-w-3xl"}`}>
      {/* Navigation */}
      <div className="mb-6 flex items-center gap-3 text-sm">
        <Link href="/" className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300">Home</Link>
        <span className="text-zinc-400">/</span>
        <Link href={`/bible/${bookId}`} className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300">
          {bookName}
        </Link>
        <span className="text-zinc-400">/</span>
        <span className="text-zinc-700 dark:text-zinc-300">Chapter {chapterNum}</span>
      </div>

      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center gap-3">
          <span
            className="inline-block h-3 w-3 rounded-full"
            style={{ backgroundColor: SOURCE_COLORS.bible }}
          />
          <h1 className="text-2xl font-bold">
            {bookName} {chapterNum}
          </h1>
        </div>
        {meta && (
          <div className="mt-2 flex items-center gap-3 text-sm text-zinc-500">
            <span className="rounded bg-green-50 px-2 py-0.5 text-xs font-medium text-green-800 dark:bg-green-900/30 dark:text-green-300">
              {abbreviation}
            </span>
            <span className="capitalize">{meta.testament} Testament</span>
            {meta.total_chapters > 0 && (
              <span>
                Chapter {chapterNum} of {meta.total_chapters}
              </span>
            )}
          </div>
        )}
      </div>

      {/* Side-by-side comparison language selector */}
      {showSideBySide && (
        <div className="mb-4 flex items-center gap-2 text-xs text-zinc-500">
          <span>Comparing:</span>
          <span className="font-semibold">{LANG_NAMES[lang]}</span>
          <span>vs</span>
          <select
            value={effectiveCompareLang}
            onChange={(e) => setCompareLang(e.target.value as Lang)}
            className="rounded border border-zinc-300 bg-white px-1.5 py-0.5 text-xs dark:border-zinc-600 dark:bg-zinc-800"
          >
            {availableLangs
              .filter((l) => l !== lang)
              .map((l) => (
                <option key={l} value={l}>
                  {LANG_SHORT[l]} — {LANG_NAMES[l]}
                </option>
              ))}
          </select>
        </div>
      )}

      {/* Chapter navigation */}
      <div className="mb-6 flex items-center gap-2">
        {chapterNum > 1 && (
          <Link
            href={`/bible/${bookId}/${chapterNum - 1}`}
            className="rounded bg-zinc-100 px-3 py-1 text-sm hover:bg-zinc-200 dark:bg-zinc-800 dark:hover:bg-zinc-700"
          >
            &larr; Chapter {chapterNum - 1}
          </Link>
        )}
        {meta && chapterNum < meta.total_chapters && (
          <Link
            href={`/bible/${bookId}/${chapterNum + 1}`}
            className="rounded bg-zinc-100 px-3 py-1 text-sm hover:bg-zinc-200 dark:bg-zinc-800 dark:hover:bg-zinc-700"
          >
            Chapter {chapterNum + 1} &rarr;
          </Link>
        )}
      </div>

      {/* Verses */}
      {chapter ? (
        showSideBySide ? (
          /* ── Side-by-side mode ── */
          <div>
            {/* Column headers */}
            <div className="mb-2 grid grid-cols-[2.5rem_1fr_1fr] gap-x-4 text-xs font-semibold uppercase text-zinc-400">
              <span />
              <span>{LANG_NAMES[lang]}</span>
              <span>{LANG_NAMES[effectiveCompareLang]}</span>
            </div>
            <div className="space-y-1">
              {Object.entries(chapter.verses)
                .sort(([a], [b]) => parseInt(a) - parseInt(b))
                .map(([verseNum, verseText]) => {
                  const primaryText = verseText[lang] || "";
                  const compareText = verseText[effectiveCompareLang] || "";

                  return (
                    <div key={verseNum} className="grid grid-cols-[2.5rem_1fr_1fr] gap-x-4 py-1">
                      <Link
                        href={`/verse/${bookId}-${chapterNum}:${verseNum}`}
                        className="mt-0.5 text-right text-xs font-medium text-green-700 hover:text-green-900 dark:text-green-400"
                      >
                        {verseNum}
                      </Link>
                      <span className="text-sm text-zinc-700 dark:text-zinc-300">
                        {primaryText || (
                          <span className="italic text-zinc-400">--</span>
                        )}
                      </span>
                      <span className="text-sm text-zinc-600 dark:text-zinc-400">
                        {compareText || (
                          <span className="italic text-zinc-400">--</span>
                        )}
                      </span>
                    </div>
                  );
                })}
            </div>
          </div>
        ) : (
          /* ── Single language mode ── */
          <div className="space-y-1">
            {Object.entries(chapter.verses)
              .sort(([a], [b]) => parseInt(a) - parseInt(b))
              .map(([verseNum, verseText]) => {
                const text = resolveLang(verseText, lang);
                const actualLang = resolvedLangCode(verseText, lang);
                const isFallback = actualLang && actualLang !== lang;

                return (
                  <div key={verseNum} className="group flex gap-2 py-1">
                    <Link
                      href={`/verse/${bookId}-${chapterNum}:${verseNum}`}
                      className="mt-0.5 min-w-[2.5rem] text-right text-xs font-medium text-green-700 hover:text-green-900 dark:text-green-400"
                    >
                      {verseNum}
                    </Link>
                    <span className="text-sm text-zinc-700 dark:text-zinc-300">
                      {text}
                      {isFallback && (
                        <span className="ml-1 rounded bg-zinc-100 px-1 py-0.5 text-[10px] text-zinc-500 dark:bg-zinc-800">
                          {LANG_SHORT[actualLang]}
                        </span>
                      )}
                    </span>
                  </div>
                );
              })}
          </div>
        )
      ) : (
        <div className="rounded border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900">
          Chapter data not available. Run the pipeline with full Bible fetch to populate.
        </div>
      )}

      {/* Chapter navigation (bottom) */}
      <div className="mt-8 flex items-center justify-between border-t pt-4 dark:border-zinc-800">
        {chapterNum > 1 ? (
          <Link
            href={`/bible/${bookId}/${chapterNum - 1}`}
            className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400"
          >
            &larr; Chapter {chapterNum - 1}
          </Link>
        ) : (
          <span />
        )}
        <Link
          href={`/bible/${bookId}`}
          className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400"
        >
          All Chapters
        </Link>
        {meta && chapterNum < meta.total_chapters ? (
          <Link
            href={`/bible/${bookId}/${chapterNum + 1}`}
            className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400"
          >
            Chapter {chapterNum + 1} &rarr;
          </Link>
        ) : (
          <span />
        )}
      </div>
    </div>
  );
}
