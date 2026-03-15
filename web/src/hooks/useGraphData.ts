"use client";

import { useEffect, useState } from "react";
import Graph from "graphology";
import type { GraphData } from "@/lib/types";
import { fetchGraphData } from "@/lib/graph-data";
import {
  hasApi,
  apiFetch,
  type ApiSubgraph,
  type ApiTheme,
  type ApiEntity,
  type ApiTopic,
} from "@/lib/api";

const EDGE_STYLES: Record<string, { color: string; size: number }> = {
  cross_reference:       { color: "#cc666688", size: 0.5 },
  cites:                 { color: "#59A14F44", size: 0.3 },
  bible_cross_reference: { color: "#59A14F22", size: 0.15 },
  shared_theme:          { color: "#6A3D9A33", size: 0.25 },
  shared_entity:         { color: "#E6832A33", size: 0.25 },
  shared_topic:          { color: "#1B9E7733", size: 0.25 },
  shared_citation:       { color: "#D62F2F33", size: 0.25 },
};
const DEFAULT_EDGE_STYLE = { color: "#cccccc44", size: 0.2 };

function buildGraphology(
  nodes: { id: string; x: number; y: number; size: number; color: string; label: string; node_type: string; part: string; degree: number; community: number; themes: string[]; entities: string[]; topics?: number[]; is_seed?: boolean }[],
  edges: { source: string; target: string; edge_type: string }[],
): Graph {
  const g = new Graph();

  for (const node of nodes) {
    g.addNode(node.id, {
      x: node.x,
      y: node.y,
      size: node.size,
      color: node.color,
      label: node.label,
      node_type: node.node_type,
      part: node.part,
      degree: node.degree,
      community: node.community,
      themes: node.themes || [],
      entities: node.entities || [],
      topics: node.topics || [],
      is_seed: node.is_seed ?? false,
    });
  }

  for (const edge of edges) {
    if (g.hasNode(edge.source) && g.hasNode(edge.target)) {
      const key = `${edge.source}--${edge.target}`;
      if (!g.hasEdge(key)) {
        const style = EDGE_STYLES[edge.edge_type] || DEFAULT_EDGE_STYLE;
        g.addEdgeWithKey(key, edge.source, edge.target, {
          edge_type: edge.edge_type,
          color: style.color,
          size: style.size,
        });
      }
    }
  }

  return g;
}

/** Describes which subgraph to load from the API. */
export type GraphQuery =
  | { mode: "theme"; theme: string }
  | { mode: "entity"; entityId: string }
  | { mode: "topic"; topicId: number }
  | { mode: "filter"; themes?: string[]; entities?: string[]; topics?: number[] }
  | { mode: "paragraph"; paragraphId: number; depth?: number }
  | { mode: "node"; nodeId: string }
  | { mode: "connect"; nodeIds: string[] }
  | null;  // null = load full graph.json (static fallback)

function queryToApiPath(q: GraphQuery): string | null {
  if (!q) return null;
  switch (q.mode) {
    case "theme":
      return `/graph/theme/${encodeURIComponent(q.theme)}`;
    case "entity":
      return `/graph/entity/${encodeURIComponent(q.entityId)}`;
    case "topic":
      return `/graph/topic/${q.topicId}`;
    case "filter": {
      const params = new URLSearchParams();
      if (q.themes?.length) params.set("themes", q.themes.join(","));
      if (q.entities?.length) params.set("entities", q.entities.join(","));
      if (q.topics?.length) params.set("topics", q.topics.map(String).join(","));
      return `/graph/filter?${params.toString()}`;
    }
    case "paragraph":
      return `/graph/paragraph/${q.paragraphId}?depth=${q.depth ?? 1}`;
    case "node":
      return `/graph/node/${encodeURIComponent(q.nodeId)}`;
    case "connect":
      return `/graph/connect?sources=${q.nodeIds.map(encodeURIComponent).join(",")}`;
  }
}

/** Cache key for deduplication. */
function queryKey(q: GraphQuery): string {
  if (!q) return "__full__";
  switch (q.mode) {
    case "theme": return `theme:${q.theme}`;
    case "entity": return `entity:${q.entityId}`;
    case "topic": return `topic:${q.topicId}`;
    case "filter": return `filter:${[...(q.themes || []), ...(q.entities || []), ...(q.topics || []).map(String)].sort().join(",")}`;
    case "paragraph": return `para:${q.paragraphId}:${q.depth ?? 1}`;
    case "node": return `node:${q.nodeId}`;
    case "connect": return `connect:${q.nodeIds.sort().join(",")}`;
  }
}

/**
 * Load graph data from the API or fall back to static graph.json.
 *
 * Pass a GraphQuery to fetch a specific subgraph from the API.
 * Pass null (or omit) to load the full graph.json (static mode).
 */
export function useGraphData(query: GraphQuery = null) {
  const [graph, setGraph] = useState<Graph | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const key = queryKey(query);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    async function load() {
      try {
        let g: Graph;

        const apiPath = hasApi ? queryToApiPath(query) : null;
        if (apiPath) {
          const data = await apiFetch<ApiSubgraph>(apiPath);
          if (cancelled) return;
          g = buildGraphology(data.nodes, data.edges);
        } else {
          // Fallback: load full graph.json
          const data: GraphData = await fetchGraphData();
          if (cancelled) return;
          g = buildGraphology(
            data.nodes.map((n) => ({ ...n, is_seed: false })),
            data.edges,
          );
        }

        setGraph(g);
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load graph");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, [key]);

  return { graph, loading, error };
}

/**
 * Fetch the list of available themes from the API.
 * Returns empty array when API is not configured.
 */
export function useGraphThemes() {
  const [themes, setThemes] = useState<ApiTheme[]>([]);

  useEffect(() => {
    if (!hasApi) return;
    apiFetch<ApiTheme[]>("/graph/themes")
      .then(setThemes)
      .catch((err) => console.error("Failed to fetch themes:", err));
  }, []);

  return themes;
}

export function useGraphEntities() {
  const [entities, setEntities] = useState<ApiEntity[]>([]);

  useEffect(() => {
    if (!hasApi) return;
    apiFetch<ApiEntity[]>("/graph/entities")
      .then(setEntities)
      .catch((err) => console.error("Failed to fetch entities:", err));
  }, []);

  return entities;
}

export function useGraphTopics() {
  const [topics, setTopics] = useState<ApiTopic[]>([]);

  useEffect(() => {
    if (!hasApi) return;
    apiFetch<ApiTopic[]>("/graph/topics")
      .then(setTopics)
      .catch((err) => console.error("Failed to fetch topics:", err));
  }, []);

  return topics;
}
