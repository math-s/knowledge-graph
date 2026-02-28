"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import Fuse from "fuse.js";
import type { SearchEntry } from "@/lib/types";
import { fetchSearchIndex } from "@/lib/graph-data";

export function useSearch() {
  const fuseRef = useRef<Fuse<SearchEntry> | null>(null);
  const [results, setResults] = useState<SearchEntry[]>([]);
  const [query, setQuery] = useState("");
  const [ready, setReady] = useState(false);

  useEffect(() => {
    fetchSearchIndex().then((data) => {
      fuseRef.current = new Fuse(data, {
        keys: [
          { name: "text", weight: 0.6 },
          { name: "part", weight: 0.1 },
          { name: "section", weight: 0.1 },
          { name: "chapter", weight: 0.1 },
          { name: "article", weight: 0.1 },
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
      if (!fuseRef.current || !q.trim() || q.length < 3) {
        setResults([]);
        return;
      }
      const hits = fuseRef.current.search(q, { limit: 20 });
      setResults(hits.map((h) => h.item));
    },
    [],
  );

  return { query, results, search, ready };
}
