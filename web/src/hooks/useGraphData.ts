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
            themes: node.themes || [],
            entities: node.entities || [],
            topics: node.topics || [],
          });
        }

        for (const edge of data.edges) {
          if (g.hasNode(edge.source) && g.hasNode(edge.target)) {
            const key = `${edge.source}--${edge.target}`;
            if (!g.hasEdge(key)) {
              let color: string;
              let size: number;
              if (edge.edge_type === "cross_reference") {
                color = "#cc666688";
                size = 0.5;
              } else if (edge.edge_type === "cites") {
                color = "#59A14F44";
                size = 0.3;
              } else if (edge.edge_type === "bible_cross_reference") {
                color = "#59A14F22";
                size = 0.15;
              } else if (edge.edge_type === "shared_theme") {
                color = "#6A3D9A33";
                size = 0.25;
              } else if (edge.edge_type === "shared_entity") {
                color = "#E6832A33";
                size = 0.25;
              } else if (edge.edge_type === "shared_topic") {
                color = "#1B9E7733";
                size = 0.25;
              } else if (edge.edge_type === "shared_citation") {
                color = "#D62F2F33";
                size = 0.25;
              } else {
                color = "#cccccc44";
                size = 0.2;
              }
              g.addEdgeWithKey(key, edge.source, edge.target, {
                edge_type: edge.edge_type,
                color,
                size,
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
