"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
import { hasApi, apiFetch, type ApiTheme } from "@/lib/api";
import { resolveLang, type Lang, type MultiLangText, LANG_NAMES } from "@/lib/types";

interface ParagraphSummary {
  id: number;
  text: Record<string, string>;
  part: string;
  section: string;
  chapter: string;
  article: string;
  themes: string[];
}

interface ParagraphsResponse {
  total: number;
  page: number;
  limit: number;
  paragraphs: ParagraphSummary[];
}

const PAGE_SIZE = 30;

export default function ParagraphsPage() {
  const [paragraphs, setParagraphs] = useState<ParagraphSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [theme, setTheme] = useState<string>("");
  const [themes, setThemes] = useState<ApiTheme[]>([]);
  const [lang, setLang] = useState<Lang>("en");

  // Fetch themes list
  useEffect(() => {
    if (!hasApi) return;
    apiFetch<ApiTheme[]>("/graph/themes").then(setThemes).catch(() => {});
  }, []);

  // Fetch paragraphs
  const fetchPage = useCallback(
    (p: number, t: string) => {
      if (!hasApi) return;
      setLoading(true);
      const themeParam = t ? `&theme=${encodeURIComponent(t)}` : "";
      apiFetch<ParagraphsResponse>(
        `/paragraphs?page=${p}&limit=${PAGE_SIZE}${themeParam}`,
      )
        .then((data) => {
          setParagraphs(data.paragraphs);
          setTotal(data.total);
        })
        .catch((err) => console.error("Failed to fetch paragraphs:", err))
        .finally(() => setLoading(false));
    },
    [],
  );

  useEffect(() => {
    fetchPage(page, theme);
  }, [page, theme, fetchPage]);

  const totalPages = Math.ceil(total / PAGE_SIZE);

  const handleThemeChange = (t: string) => {
    setTheme(t);
    setPage(1);
  };

  if (!hasApi) {
    return (
      <div className="mx-auto max-w-3xl px-6 py-12">
        <h1 className="mb-4 text-2xl font-bold">Browse Paragraphs</h1>
        <p className="text-zinc-500">
          Paginated browsing requires the API backend. Use the{" "}
          <Link href="/structure" className="text-blue-600 hover:underline">
            structure page
          </Link>{" "}
          to browse paragraphs.
        </p>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-3xl px-6 py-12">
      <div className="mb-6 flex items-center justify-between">
        <h1 className="text-2xl font-bold">Browse Paragraphs</h1>
        <Link
          href="/"
          className="text-sm text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300"
        >
          Home
        </Link>
      </div>

      {/* Controls */}
      <div className="mb-6 flex flex-wrap items-center gap-4">
        <div className="flex items-center gap-2">
          <label className="text-xs text-zinc-500">Theme:</label>
          <select
            value={theme}
            onChange={(e) => handleThemeChange(e.target.value)}
            className="rounded-lg border px-3 py-1.5 text-sm dark:border-zinc-700 dark:bg-zinc-900 dark:text-white"
          >
            <option value="">All ({total})</option>
            {themes.map((t) => (
              <option key={t.id} value={t.id}>
                {t.label} ({t.count})
              </option>
            ))}
          </select>
        </div>

        <div className="flex items-center gap-2">
          <label className="text-xs text-zinc-500">Language:</label>
          <select
            value={lang}
            onChange={(e) => setLang(e.target.value as Lang)}
            className="rounded-lg border px-3 py-1.5 text-sm dark:border-zinc-700 dark:bg-zinc-900 dark:text-white"
          >
            {(["en", "la", "pt"] as Lang[]).map((l) => (
              <option key={l} value={l}>
                {LANG_NAMES[l]}
              </option>
            ))}
          </select>
        </div>

        <span className="text-xs text-zinc-400">
          {total} paragraph{total !== 1 ? "s" : ""}
          {totalPages > 1 && ` · page ${page} of ${totalPages}`}
        </span>
      </div>

      {/* Paragraphs */}
      {loading ? (
        <p className="text-sm text-zinc-500">Loading...</p>
      ) : (
        <div className="space-y-4">
          {paragraphs.map((p) => (
            <Link
              key={p.id}
              href={`/paragraph/${p.id}`}
              className="block rounded-lg border p-4 transition-colors hover:border-blue-300 hover:bg-blue-50/50 dark:border-zinc-700 dark:hover:border-blue-800 dark:hover:bg-blue-950/20"
            >
              <div className="mb-1 flex items-center gap-2">
                <span className="text-sm font-semibold">CCC {p.id}</span>
                {p.themes.map((t) => (
                  <span
                    key={t}
                    className="rounded bg-zinc-100 px-1.5 py-0.5 text-[10px] text-zinc-500 dark:bg-zinc-800"
                  >
                    {t}
                  </span>
                ))}
              </div>
              <p className="line-clamp-3 text-sm text-zinc-600 dark:text-zinc-400">
                {resolveLang(p.text as MultiLangText, lang)}
              </p>
              <p className="mt-1 text-[10px] text-zinc-400">
                {p.part}
              </p>
            </Link>
          ))}
        </div>
      )}

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="mt-8 flex items-center justify-center gap-2">
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1}
            className="rounded border px-3 py-1.5 text-sm disabled:opacity-30 dark:border-zinc-700"
          >
            Previous
          </button>
          {Array.from({ length: Math.min(7, totalPages) }, (_, i) => {
            let pageNum: number;
            if (totalPages <= 7) {
              pageNum = i + 1;
            } else if (page <= 4) {
              pageNum = i + 1;
            } else if (page >= totalPages - 3) {
              pageNum = totalPages - 6 + i;
            } else {
              pageNum = page - 3 + i;
            }
            return (
              <button
                key={pageNum}
                onClick={() => setPage(pageNum)}
                className={`rounded border px-3 py-1.5 text-sm dark:border-zinc-700 ${
                  pageNum === page
                    ? "bg-blue-600 text-white"
                    : "hover:bg-zinc-100 dark:hover:bg-zinc-800"
                }`}
              >
                {pageNum}
              </button>
            );
          })}
          <button
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={page >= totalPages}
            className="rounded border px-3 py-1.5 text-sm disabled:opacity-30 dark:border-zinc-700"
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
}
