"use client";

import { useState, useCallback, useRef } from "react";
import Link from "next/link";
import { apiFetch, type ApiSearchResponse, type ApiSearchResult } from "@/lib/api";

type SearchScope = "all" | "bible" | "patristic";

const SCOPE_LABELS: Record<SearchScope, string> = {
  all: "Paragraphs & Sources",
  bible: "Bible Verses",
  patristic: "Patristic Texts",
};

const SCOPE_ENDPOINTS: Record<SearchScope, string> = {
  all: "/search",
  bible: "/search/bible",
  patristic: "/search/patristic",
};

const LANG_OPTIONS = [
  { value: "en", label: "English" },
  { value: "la", label: "Latin" },
  { value: "pt", label: "Portuguese" },
  { value: "el", label: "Greek" },
];

function resultLink(result: ApiSearchResult, scope: SearchScope): string {
  if (scope === "bible") {
    const r = result as unknown as Record<string, unknown>;
    return `/verse/${r.book_id}-${r.chapter}:${r.verse}`;
  }
  if (scope === "all") {
    if (result.type === "paragraph") return `/paragraph/${result.id}`;
    // Source nodes: "bible:john" → /bible/john, "author:augustine" → /author/augustine
    const parts = result.id.split(":");
    if (parts.length === 2) {
      const [kind, id] = parts;
      if (kind === "bible" || kind === "bible-book") return `/bible/${id}`;
      if (kind === "author") return `/author/${id}`;
      if (kind === "document") return `/document/${id}`;
    }
  }
  return "#";
}

function resultLabel(result: ApiSearchResult, scope: SearchScope): string {
  if (scope === "bible") {
    const r = result as unknown as Record<string, unknown>;
    const bookName = String(r.book_id || "").replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
    return `${bookName} ${r.chapter}:${r.verse}`;
  }
  if (scope === "patristic") {
    const r = result as unknown as Record<string, unknown>;
    const author = String(r.author_id || "").replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
    return `${author} — ${r.work_id || ""}`;
  }
  if (result.type === "paragraph") return `CCC ${result.id}`;
  const parts = result.id.split(":");
  if (parts.length === 2) {
    return parts[1].replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  }
  return result.id;
}

export default function SearchPage() {
  const [query, setQuery] = useState("");
  const [scope, setScope] = useState<SearchScope>("all");
  const [lang, setLang] = useState("en");
  const [bilingual, setBilingual] = useState(false);
  const [results, setResults] = useState<ApiSearchResult[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout>>(null);

  const doSearch = useCallback(
    (q: string, s: SearchScope, l: string, bi: boolean) => {
      if (!q.trim() || q.length < 2) {
        setResults([]);
        setTotalCount(0);
        return;
      }
      setLoading(true);
      setSearched(true);

      const endpoint = SCOPE_ENDPOINTS[s];
      const biParam = bi ? "&bilingual=true" : "";
      apiFetch<ApiSearchResponse>(
        `${endpoint}?q=${encodeURIComponent(q)}&lang=${l}&limit=50${biParam}`,
      )
        .then((data) => {
          setResults(data.results);
          setTotalCount(data.count);
        })
        .catch((err) => console.error("Search error:", err))
        .finally(() => setLoading(false));
    },
    [],
  );

  const handleInput = (value: string) => {
    setQuery(value);
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = setTimeout(() => doSearch(value, scope, lang, bilingual), 300);
  };

  const handleScopeChange = (s: SearchScope) => {
    setScope(s);
    if (query.length >= 2) doSearch(query, s, lang, bilingual);
  };

  const handleLangChange = (l: string) => {
    setLang(l);
    if (query.length >= 2) doSearch(query, scope, l, bilingual);
  };

  const handleBilingualToggle = (bi: boolean) => {
    setBilingual(bi);
    if (query.length >= 2) doSearch(query, scope, lang, bi);
  };

  return (
    <div className="mx-auto max-w-3xl px-6 py-12">
      <div className="mb-6 flex items-center justify-between">
        <h1 className="text-2xl font-bold">Search</h1>
        <Link
          href="/"
          className="text-sm text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300"
        >
          Home
        </Link>
      </div>

      {/* Search input */}
      <div className="mb-4">
        <input
          type="text"
          value={query}
          onChange={(e) => handleInput(e.target.value)}
          placeholder="Search paragraphs, Bible verses, patristic texts..."
          autoFocus
          className="w-full rounded-lg border px-4 py-3 text-sm shadow-sm focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-200 dark:border-zinc-700 dark:bg-zinc-900 dark:text-white"
        />
      </div>

      {/* Controls */}
      <div className="mb-6 flex flex-wrap gap-4">
        <div className="flex gap-1 rounded-lg border p-1 dark:border-zinc-700">
          {(Object.keys(SCOPE_LABELS) as SearchScope[]).map((s) => (
            <button
              key={s}
              onClick={() => handleScopeChange(s)}
              className={`rounded-md px-3 py-1 text-xs font-medium transition-colors ${
                scope === s
                  ? "bg-blue-600 text-white"
                  : "text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800"
              }`}
            >
              {SCOPE_LABELS[s]}
            </button>
          ))}
        </div>

        <select
          value={lang}
          onChange={(e) => handleLangChange(e.target.value)}
          className="rounded-lg border px-3 py-1 text-xs dark:border-zinc-700 dark:bg-zinc-900 dark:text-white"
        >
          {LANG_OPTIONS.map((l) => (
            <option key={l.value} value={l.value}>
              {l.label}
            </option>
          ))}
        </select>

        <label className="flex items-center gap-1.5 text-xs text-zinc-600 dark:text-zinc-400">
          <input
            type="checkbox"
            checked={bilingual}
            onChange={(e) => handleBilingualToggle(e.target.checked)}
            className="rounded"
          />
          Show all translations
        </label>
      </div>

      {/* Results */}
      {loading && <p className="text-sm text-zinc-500">Searching...</p>}

      {!loading && searched && results.length === 0 && (
        <p className="text-sm text-zinc-500">No results found.</p>
      )}

      {results.length > 0 && (
        <div>
          <p className="mb-4 text-xs text-zinc-400">
            {totalCount} result{totalCount !== 1 ? "s" : ""}
          </p>
          <div className="space-y-3">
            {results.map((r, i) => (
              <Link
                key={`${r.id}-${i}`}
                href={resultLink(r, scope)}
                className="block rounded-lg border p-4 transition-colors hover:border-blue-300 hover:bg-blue-50/50 dark:border-zinc-700 dark:hover:border-blue-800 dark:hover:bg-blue-950/20"
              >
                <div className="mb-1 flex items-center gap-2">
                  <span className="text-sm font-medium">
                    {resultLabel(r, scope)}
                  </span>
                  {r.type && (
                    <span className="rounded bg-zinc-100 px-1.5 py-0.5 text-[10px] text-zinc-500 dark:bg-zinc-800">
                      {r.type}
                    </span>
                  )}
                </div>
                <div
                  className="text-sm text-zinc-600 dark:text-zinc-400 [&>mark]:bg-yellow-200 [&>mark]:dark:bg-yellow-800"
                  dangerouslySetInnerHTML={{ __html: r.snippet }}
                />
                {r.translations && Object.keys(r.translations).length > 0 && (
                  <div className="mt-2 space-y-1 border-t pt-2 dark:border-zinc-700">
                    {Object.entries(r.translations)
                      .filter(([l]) => l !== lang)
                      .map(([l, text]) => (
                        <div key={l} className="flex gap-2 text-xs">
                          <span className="shrink-0 font-medium uppercase text-zinc-400">
                            {l}
                          </span>
                          <span
                            className="text-zinc-500 dark:text-zinc-500 [&>mark]:bg-yellow-200 [&>mark]:dark:bg-yellow-800"
                            dangerouslySetInnerHTML={{ __html: text }}
                          />
                        </div>
                      ))}
                  </div>
                )}
              </Link>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
