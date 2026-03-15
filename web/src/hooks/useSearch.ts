"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import Fuse from "fuse.js";
import type { Lang, SearchEntry } from "@/lib/types";
import { fetchSearchIndex } from "@/lib/graph-data";
import {
  hasApi,
  apiFetch,
  type ApiSearchResponse,
} from "@/lib/api";

/**
 * Search hook that uses the API when available, falling back to Fuse.js.
 *
 * When NEXT_PUBLIC_API_URL is set: calls /search?q=... (server-side FTS5).
 * When not set: downloads search-index.json and uses Fuse.js client-side.
 */
export function useSearch(lang: Lang = "en") {
  // Fuse.js fallback state
  const fuseRef = useRef<Fuse<SearchEntry> | null>(null);

  const [results, setResults] = useState<SearchEntry[]>([]);
  const [query, setQuery] = useState("");
  const [ready, setReady] = useState(false);

  // Abort controller for in-flight API requests
  const abortRef = useRef<AbortController | null>(null);

  // Initialize Fuse.js only when API is not available
  useEffect(() => {
    if (hasApi) {
      setReady(true);
      return;
    }
    fetchSearchIndex().then((data) => {
      fuseRef.current = new Fuse(data, {
        keys: [
          { name: "text", weight: 0.5 },
          { name: "themes", weight: 0.15 },
          { name: "part", weight: 0.1 },
          { name: "section", weight: 0.1 },
          { name: "chapter", weight: 0.1 },
          { name: "article", weight: 0.05 },
        ],
        threshold: 0.4,
        includeScore: true,
        minMatchCharLength: 3,
      });
      setReady(true);
    });
  }, []);

  const search = useCallback(
    (q: string) => {
      setQuery(q);

      if (!q.trim() || q.length < 2) {
        setResults([]);
        return;
      }

      if (hasApi) {
        // Cancel previous in-flight request
        abortRef.current?.abort();
        const controller = new AbortController();
        abortRef.current = controller;

        apiFetch<ApiSearchResponse>(
          `/search?q=${encodeURIComponent(q)}&limit=20&lang=${lang}&bilingual=true`,
        )
          .then((data) => {
            if (controller.signal.aborted) return;
            // Map API results to SearchEntry shape for SearchBar compatibility
            setResults(
              data.results.map((r) => ({
                id: r.type === "paragraph" ? Number(r.id) : r.id,
                text: r.snippet.replace(/<\/?mark>/g, ""),
                snippet_html: r.snippet,
                translations: r.translations,
                themes: "",
                part: "",
                section: "",
                chapter: "",
                article: "",
              })),
            );
          })
          .catch((err) => {
            if (err instanceof DOMException && err.name === "AbortError") return;
            console.error("Search API error:", err);
          });
      } else {
        // Fuse.js fallback
        if (!fuseRef.current || q.length < 3) {
          setResults([]);
          return;
        }
        const hits = fuseRef.current.search(q, { limit: 20 });
        setResults(hits.map((h) => h.item));
      }
    },
    [],
  );

  return { query, results, search, ready };
}
