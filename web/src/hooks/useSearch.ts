"use client";

import { useState, useCallback, useRef } from "react";
import type { Lang, SearchEntry } from "@/lib/types";
import {
  apiFetch,
  type ApiSearchResponse,
} from "@/lib/api";

/**
 * Search hook that uses the API for full-text search.
 */
export function useSearch(lang: Lang = "en") {
  const [results, setResults] = useState<SearchEntry[]>([]);
  const [query, setQuery] = useState("");
  const ready = true;

  // Abort controller for in-flight API requests
  const abortRef = useRef<AbortController | null>(null);

  const search = useCallback(
    (q: string) => {
      setQuery(q);

      if (!q.trim() || q.length < 2) {
        setResults([]);
        return;
      }

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
    },
    [],
  );

  return { query, results, search, ready };
}
