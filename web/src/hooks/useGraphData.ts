"use client";

import { useEffect, useState } from "react";
import Graph from "graphology";
import type { GraphData } from "@/lib/types";
import { fetchGraphData } from "@/lib/graph-data";
import {
  hasApi,
  apiFetch,
  type ApiThemeGraph,
  type ApiTheme,
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

/**
 * Load graph data for a specific theme from the API, or fall back to
 * loading the full graph.json when no API is configured.
 */
export function useGraphData(theme?: string | null) {
  const [graph, setGraph] = useState<Graph | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    async function load() {
      try {
        let g: Graph;

        if (hasApi && theme) {
          // Fetch per-theme subgraph from API
          const data = await apiFetch<ApiThemeGraph>(
            `/graph/theme/${encodeURIComponent(theme)}`,
          );
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
  }, [theme]);

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
