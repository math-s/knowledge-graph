/**
 * API client for the Knowledge Graph backend.
 *
 * All runtime data goes through the API at NEXT_PUBLIC_API_URL.
 */

export const API_URL = process.env.NEXT_PUBLIC_API_URL || "";

if (!API_URL && typeof window !== "undefined") {
  console.warn(
    "[knowledge-graph] NEXT_PUBLIC_API_URL is not set. API calls will fail.",
  );
}

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
  topics?: number[];
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

export interface ApiEntity {
  id: string;
  label: string;
  category: string;
  count: number;
}

export interface ApiTopic {
  id: number;
  terms: string[];
}
