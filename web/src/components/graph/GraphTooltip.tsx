"use client";

import { useEffect, useRef, useState } from "react";
import { useSigma } from "@react-sigma/core";
import Graph from "graphology";
import type { ParagraphData } from "@/lib/types";
import { resolveLang } from "@/lib/types";
import { fetchParagraphs } from "@/lib/graph-data";

interface GraphTooltipProps {
  hoveredNode: string | null;
  graph: Graph;
}

/** Singleton paragraph text cache — loaded once on first hover of a paragraph node. */
const paraCache: { map: Map<number, string> | null; loading: boolean } = {
  map: null,
  loading: false,
};

function loadParaCache(): Promise<Map<number, string>> {
  if (paraCache.map) return Promise.resolve(paraCache.map);
  if (paraCache.loading) {
    return new Promise((resolve) => {
      const interval = setInterval(() => {
        if (paraCache.map) {
          clearInterval(interval);
          resolve(paraCache.map);
        }
      }, 50);
    });
  }
  paraCache.loading = true;
  return fetchParagraphs().then((data: ParagraphData[]) => {
    const map = new Map<number, string>();
    for (const p of data) {
      const text = resolveLang(p.text, "en");
      map.set(p.id, text.slice(0, 200));
    }
    paraCache.map = map;
    paraCache.loading = false;
    return map;
  });
}

export default function GraphTooltip({ hoveredNode, graph }: GraphTooltipProps) {
  const sigma = useSigma();
  const [position, setPosition] = useState<{ x: number; y: number } | null>(
    null,
  );
  const [snippet, setSnippet] = useState<string | null>(null);
  const lastNodeRef = useRef<string | null>(null);

  useEffect(() => {
    if (!hoveredNode || !graph.hasNode(hoveredNode)) {
      setPosition(null);
      setSnippet(null);
      lastNodeRef.current = null;
      return;
    }

    const nodeDisplayData = sigma.getNodeDisplayData(hoveredNode);
    if (nodeDisplayData) {
      setPosition({ x: nodeDisplayData.x, y: nodeDisplayData.y });
    }

    // Load paragraph snippet
    const attrs = graph.getNodeAttributes(hoveredNode);
    if (attrs.node_type === "paragraph" && hoveredNode.startsWith("p:")) {
      const paraId = parseInt(hoveredNode.slice(2), 10);
      lastNodeRef.current = hoveredNode;
      loadParaCache().then((map) => {
        if (lastNodeRef.current !== hoveredNode) return; // stale
        setSnippet(map.get(paraId) || null);
      });
    } else {
      setSnippet(null);
    }
  }, [hoveredNode, sigma, graph]);

  if (!hoveredNode || !position || !graph.hasNode(hoveredNode)) return null;

  const attrs = graph.getNodeAttributes(hoveredNode);

  // Convert graph coordinates to viewport coordinates
  const viewportPos = sigma.graphToViewport({ x: position.x, y: position.y });

  return (
    <div
      className="pointer-events-none absolute z-30 max-w-xs rounded bg-zinc-900 px-3 py-2 text-sm text-white shadow-lg"
      style={{
        left: viewportPos.x + 15,
        top: viewportPos.y - 10,
      }}
    >
      <div className="font-semibold">{attrs.label}</div>
      {snippet && (
        <div className="mt-1 text-xs text-zinc-300 line-clamp-3">
          {snippet}{snippet.length >= 200 ? "..." : ""}
        </div>
      )}
    </div>
  );
}
