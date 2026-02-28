"use client";

import { useEffect, useState } from "react";
import Graph from "graphology";
import type { GraphData } from "@/lib/types";
import { fetchGraphData } from "@/lib/graph-data";

export function useGraphData() {
  const [graph, setGraph] = useState<Graph | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const data: GraphData = await fetchGraphData();
        if (cancelled) return;

        const g = new Graph();

        for (const node of data.nodes) {
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
          });
        }

        for (const edge of data.edges) {
          if (g.hasNode(edge.source) && g.hasNode(edge.target)) {
            const key = `${edge.source}--${edge.target}`;
            if (!g.hasEdge(key)) {
              g.addEdgeWithKey(key, edge.source, edge.target, {
                edge_type: edge.edge_type,
                color: edge.edge_type === "cross_reference" ? "#cc666688" : "#cccccc44",
                size: edge.edge_type === "cross_reference" ? 0.5 : 0.2,
              });
            }
          }
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
  }, []);

  return { graph, loading, error };
}
