"use client";

import { useEffect, useRef, useState } from "react";
import { useSigma } from "@react-sigma/core";
import Graph from "graphology";
import { apiFetch } from "@/lib/api";
import { resolveLang } from "@/lib/types";
import type { MultiLangText } from "@/lib/types";

interface GraphTooltipProps {
  hoveredNode: string | null;
  graph: Graph;
}

/** Per-paragraph snippet cache — one ~1KB API call per cache miss. */
const snippetCache = new Map<number, string>();

async function loadSnippet(paraId: number): Promise<string | null> {
  if (snippetCache.has(paraId)) return snippetCache.get(paraId)!;
  try {
    const data = await apiFetch<{ text: MultiLangText }>(`/paragraphs/${paraId}`);
    const text = resolveLang(data.text, "en").slice(0, 200);
    snippetCache.set(paraId, text);
    return text;
  } catch {
    return null;
  }
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
      loadSnippet(paraId).then((text) => {
        if (lastNodeRef.current !== hoveredNode) return; // stale
        setSnippet(text);
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
