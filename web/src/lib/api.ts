/**
 * API client for the Knowledge Graph backend.
 *
 * When NEXT_PUBLIC_API_URL is set, hooks use the API for search and graph.
 * When not set, the app falls back to static JSON files (Fuse.js search, full graph).
 */

export const API_URL = process.env.NEXT_PUBLIC_API_URL || "";

/** True when the API backend is configured. */
export const hasApi = !!API_URL;

/** Fetch JSON from the API, throwing on non-OK responses. */
export async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${API_URL}${path}`);
  if (!res.ok) {
    throw new Error(`API ${res.status}: ${res.statusText}`);
  }
  return res.json();
}

// -- Search types --

export interface ApiSearchResult {
  id: string;
  type: string;
  snippet: string;
  rank: number;
  translations?: Record<string, string>;
}

export interface ApiSearchResponse {
  query: string;
  lang: string;
  count: number;
  results: ApiSearchResult[];
}

// -- Graph types --

export interface ApiGraphNode {
  id: string;
  label: string;
  node_type: string;
  x: number;
  y: number;
  size: number;
  color: string;
  part: string;
  degree: number;
  community: number;
  themes: string[];
  entities: string[];
  is_seed?: boolean;
}

export interface ApiGraphEdge {
  source: string;
  target: string;
  edge_type: string;
}

export interface ApiThemeGraph {
  theme: string;
  seed_count: number;
  node_count: number;
  edge_count: number;
  nodes: ApiGraphNode[];
  edges: ApiGraphEdge[];
}

export interface ApiSubgraph {
  seed_count: number;
  node_count: number;
  edge_count: number;
  nodes: ApiGraphNode[];
  edges: ApiGraphEdge[];
  [key: string]: unknown;
}

export interface ApiTheme {
  id: string;
  label: string;
  count: number;
}
