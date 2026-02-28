"use client";

import { useEffect, useState } from "react";
import { useSigma } from "@react-sigma/core";
import Graph from "graphology";

interface GraphTooltipProps {
  hoveredNode: string | null;
  graph: Graph;
}

export default function GraphTooltip({ hoveredNode, graph }: GraphTooltipProps) {
  const sigma = useSigma();
  const [position, setPosition] = useState<{ x: number; y: number } | null>(
    null,
  );

  useEffect(() => {
    if (!hoveredNode || !graph.hasNode(hoveredNode)) {
      setPosition(null);
      return;
    }

    const nodeDisplayData = sigma.getNodeDisplayData(hoveredNode);
    if (nodeDisplayData) {
      setPosition({ x: nodeDisplayData.x, y: nodeDisplayData.y });
    }
  }, [hoveredNode, sigma, graph]);

  if (!hoveredNode || !position || !graph.hasNode(hoveredNode)) return null;

  const attrs = graph.getNodeAttributes(hoveredNode);
  const textPreview = attrs.text_preview || attrs.label;

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
      {attrs.node_type === "paragraph" && textPreview && (
        <div className="mt-1 text-xs text-zinc-300 line-clamp-3">
          {textPreview}
        </div>
      )}
    </div>
  );
}
