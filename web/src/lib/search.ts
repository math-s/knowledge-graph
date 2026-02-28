import Fuse from "fuse.js";
import type { SearchEntry } from "./types";

let fuseInstance: Fuse<SearchEntry> | null = null;

export function initSearch(data: SearchEntry[]): Fuse<SearchEntry> {
  fuseInstance = new Fuse(data, {
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
  return fuseInstance;
}

export function search(query: string): SearchEntry[] {
  if (!fuseInstance || !query.trim()) return [];
  return fuseInstance.search(query, { limit: 20 }).map((r) => r.item);
}
