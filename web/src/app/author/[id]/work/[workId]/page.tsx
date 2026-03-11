"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { fetchAuthorWorks, fetchAuthorMeta } from "@/lib/graph-data";
import type { AuthorMeta, PatristicWorkData, MultiLangText } from "@/lib/types";
import { resolveLang, LANG_SHORT } from "@/lib/types";
import { useLang } from "@/lib/LangContext";
import { SOURCE_COLORS } from "@/lib/colors";

export default function WorkPage() {
  const params = useParams<{ id: string; workId: string }>();
  const authorId = params.id;
  const workId = params.workId;
  const { lang } = useLang();

  const [author, setAuthor] = useState<AuthorMeta | null>(null);
  const [work, setWork] = useState<PatristicWorkData | null>(null);
  const [loading, setLoading] = useState(true);
  const [expandedChapters, setExpandedChapters] = useState<Set<number>>(new Set());

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const [metaData, worksData] = await Promise.all([
          fetchAuthorMeta(),
          fetchAuthorWorks(authorId),
        ]);

        if (metaData[authorId]) {
          setAuthor(metaData[authorId]);
        }

        if (worksData) {
          const found = worksData.find(
            (w) => w.id === `${authorId}/${workId}` || w.id.endsWith(`/${workId}`),
          );
          if (found) setWork(found);
        }
      } catch {
        // Ignore fetch errors
      }
      setLoading(false);
    }
    load();
  }, [authorId, workId]);

  const toggleChapter = (chNum: number) => {
    setExpandedChapters((prev) => {
      const next = new Set(prev);
      if (next.has(chNum)) next.delete(chNum);
      else next.add(chNum);
      return next;
    });
  };

  if (loading) {
    return (
      <div className="mx-auto max-w-3xl px-6 py-12">
        <div className="text-sm text-zinc-400">Loading...</div>
      </div>
    );
  }

  const authorLabel =
    author?.name ||
    authorId.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());

  return (
    <div className="mx-auto max-w-3xl px-6 py-12">
      {/* Navigation */}
      <div className="mb-6 flex items-center gap-3 text-sm">
        <Link
          href="/"
          className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300"
        >
          Home
        </Link>
        <span className="text-zinc-300">&rsaquo;</span>
        <Link
          href={`/author/${authorId}`}
          className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300"
        >
          {authorLabel}
        </Link>
        <span className="text-zinc-300">&rsaquo;</span>
        <span className="text-zinc-700 dark:text-zinc-300">
          {work?.title || workId}
        </span>
      </div>

      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center gap-3">
          <span
            className="inline-block h-3 w-3 rounded-full"
            style={{ backgroundColor: SOURCE_COLORS.author }}
          />
          <h1 className="text-2xl font-bold">{work?.title || workId}</h1>
        </div>
        <div className="mt-2 text-sm text-zinc-500">
          by {authorLabel}
          {author?.era && <span> ({author.era})</span>}
          {work?.chapter_count
            ? ` \u00b7 ${work.chapter_count} chapter${work.chapter_count !== 1 ? "s" : ""}`
            : ""}
        </div>
        {work?.source_url && (
          <a
            href={work.source_url}
            className="mt-1 inline-block text-xs text-blue-600 hover:text-blue-800 dark:text-blue-400"
            target="_blank"
            rel="noopener noreferrer"
          >
            Source &rarr;
          </a>
        )}
      </div>

      {/* Chapters */}
      {work && work.chapters.length > 0 ? (
        <div className="space-y-3">
          <h2 className="text-sm font-semibold uppercase text-zinc-500">
            Chapters ({work.chapters.length})
          </h2>
          {work.chapters.map((ch) => {
            const isExpanded = expandedChapters.has(ch.number);
            return (
              <div
                key={ch.number}
                className="rounded border border-zinc-200 dark:border-zinc-700"
              >
                <button
                  onClick={() => toggleChapter(ch.number)}
                  className="flex w-full items-center justify-between px-4 py-3 text-left text-sm font-medium hover:bg-zinc-50 dark:hover:bg-zinc-800/50"
                >
                  <span>
                    Chapter {ch.number}
                    {ch.title && (
                      <span className="ml-2 font-normal text-zinc-500">
                        {ch.title}
                      </span>
                    )}
                  </span>
                  <span className="text-zinc-400">
                    {isExpanded ? "\u25B2" : "\u25BC"}
                  </span>
                </button>
                {isExpanded && (
                  <div className="border-t px-4 py-3 dark:border-zinc-700">
                    {ch.sections.map((sec) => {
                      const text = resolveLang(
                        sec.text as MultiLangText,
                        lang,
                      );
                      const actualLang =
                        sec.text[lang]
                          ? lang
                          : Object.keys(sec.text)[0] || lang;
                      const showBadge = actualLang !== lang;
                      return (
                        <div key={sec.id} className="mb-3">
                          {showBadge && (
                            <span className="mr-1 rounded bg-zinc-200 px-1 py-0.5 text-[10px] font-medium uppercase text-zinc-500 dark:bg-zinc-700 dark:text-zinc-400">
                              {LANG_SHORT[actualLang as keyof typeof LANG_SHORT] || actualLang}
                            </span>
                          )}
                          <p className="text-sm leading-relaxed text-zinc-800 dark:text-zinc-200">
                            {text.length > 2000
                              ? text.slice(0, 2000) + "..."
                              : text}
                          </p>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      ) : work ? (
        <div className="rounded border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900">
          No chapter text available. This work is referenced as metadata only.
        </div>
      ) : (
        <div className="rounded border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900">
          Work not found. Run the pipeline to populate patristic work data.
        </div>
      )}

      {/* View in graph */}
      <div className="mt-8 border-t pt-4 dark:border-zinc-800">
        <Link
          href="/graph"
          className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400"
        >
          View in graph &rarr;
        </Link>
      </div>
    </div>
  );
}
