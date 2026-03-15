"use client";

import { useEffect, useMemo } from "react";
import {
  SigmaContainer,
  useLoadGraph,
  useRegisterEvents,
  useSigma,
  useSetSettings,
} from "@react-sigma/core";
import "@react-sigma/core/lib/style.css";
import Graph from "graphology";
import GraphControls from "./GraphControls";
import GraphTooltip from "./GraphTooltip";
import type { GraphFilters } from "./FilterPanel";
import { matchesHighlightFilters, hasHighlightFilters } from "./FilterPanel";

interface GraphCanvasProps {
  graph: Graph;
  selectedNode: string | null;
  onSelectNode: (nodeId: string | null) => void;
  hoveredNode: string | null;
  onHoverNode: (nodeId: string | null) => void;
  filters: GraphFilters;
}

function GraphEvents({
  onSelectNode,
  onHoverNode,
}: {
  onSelectNode: (nodeId: string | null) => void;
  onHoverNode: (nodeId: string | null) => void;
}) {
  const registerEvents = useRegisterEvents();

  useEffect(() => {
    registerEvents({
      clickNode: (event) => onSelectNode(event.node),
      clickStage: () => onSelectNode(null),
      enterNode: (event) => onHoverNode(event.node),
      leaveNode: () => onHoverNode(null),
    });
  }, [registerEvents, onSelectNode, onHoverNode]);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") onSelectNode(null);
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onSelectNode]);

  return null;
}

function GraphLoader({ graph }: { graph: Graph }) {
  const loadGraph = useLoadGraph();

  useEffect(() => {
    loadGraph(graph);
  }, [loadGraph, graph]);

  return null;
}

function GraphReducers({
  selectedNode,
  hoveredNode,
  filters,
}: {
  selectedNode: string | null;
  hoveredNode: string | null;
  filters: GraphFilters;
}) {
  const sigma = useSigma();
  const setSettings = useSetSettings();

  useEffect(() => {
    const activeNode = hoveredNode || selectedNode;
    const graph = sigma.getGraph();

    let neighbors: Set<string> | null = null;
    if (activeNode && graph.hasNode(activeNode)) {
      neighbors = new Set(graph.neighbors(activeNode));
      neighbors.add(activeNode);
    }

    const hasHighlight = hasHighlightFilters(filters);

    setSettings({
      nodeReducer: (node, data) => {
        const attrs = graph.getNodeAttributes(node);
        const nodeType = attrs.node_type;
        const part = attrs.part || "";

        // Apply node size scale
        const scaled = filters.nodeSizeScale !== 1
          ? { ...data, size: data.size * filters.nodeSizeScale }
          : data;

        // Apply node type filters
        if (nodeType === "structure" && !filters.showStructural) {
          return { ...scaled, hidden: true };
        }
        if ((nodeType === "bible" || nodeType === "bible-testament" || nodeType === "bible-book") && !filters.showBibleNodes) {
          return { ...scaled, hidden: true };
        }
        if (nodeType === "bible-chapter" && !filters.showBibleChapters) {
          return { ...scaled, hidden: true };
        }
        if (nodeType === "bible-verse" && !filters.showBibleVerses) {
          return { ...scaled, hidden: true };
        }
        if (nodeType === "author" && !filters.showAuthorNodes) {
          return { ...scaled, hidden: true };
        }
        if (nodeType === "patristic-work" && !filters.showPatristicWorks) {
          return { ...scaled, hidden: true };
        }
        if (nodeType === "document" && !filters.showDocumentNodes) {
          return { ...scaled, hidden: true };
        }
        if (nodeType === "document-section" && !filters.showDocumentSections) {
          return { ...scaled, hidden: true };
        }
        if (nodeType === "paragraph" && part && !filters.visibleParts.has(part)) {
          return { ...scaled, hidden: true };
        }

        // Apply highlight filters (theme + entity + topic AND-logic)
        if (hasHighlight && nodeType === "paragraph") {
          if (!matchesHighlightFilters(filters, attrs)) {
            return {
              ...scaled,
              color: "#ddd",
              size: Math.max(scaled.size * 0.4, 1),
              zIndex: 0,
            };
          }
        }

        // Apply neighbor highlighting — dim non-neighbors but keep them visible
        if (neighbors) {
          if (neighbors.has(node)) {
            return { ...scaled, highlighted: true, zIndex: 1 };
          }
          return {
            ...scaled,
            color: "#ddd",
            size: Math.max(scaled.size * 0.5, 1),
            zIndex: 0,
          };
        }

        return scaled;
      },
      edgeReducer: (edge, data) => {
        const edgeAttrs = graph.getEdgeAttributes(edge);
        const edgeType = edgeAttrs.edge_type;

        // Apply edge type filters
        if (edgeType === "cross_reference" && !filters.showCrossRefs) {
          return { ...data, hidden: true };
        }
        if (edgeType === "cites" && !filters.showCites) {
          return { ...data, hidden: true };
        }
        if (edgeType === "belongs_to" && !filters.showBelongsTo) {
          return { ...data, hidden: true };
        }
        if (edgeType === "child_of" && !filters.showChildOf) {
          return { ...data, hidden: true };
        }
        if (edgeType === "shared_theme" && !filters.showSharedTheme) {
          return { ...data, hidden: true };
        }
        if (edgeType === "bible_cross_reference" && !filters.showBibleCrossRefs) {
          return { ...data, hidden: true };
        }
        if (edgeType === "shared_entity" && !filters.showSharedEntity) {
          return { ...data, hidden: true };
        }
        if (edgeType === "shared_topic" && !filters.showSharedTopic) {
          return { ...data, hidden: true };
        }
        if (edgeType === "shared_citation" && !filters.showSharedCitation) {
          return { ...data, hidden: true };
        }

        // Apply neighbor highlighting
        if (neighbors) {
          const source = graph.source(edge);
          const target = graph.target(edge);
          if (neighbors.has(source) && neighbors.has(target)) {
            return { ...data, hidden: false };
          }
          return { ...data, hidden: true };
        }

        return data;
      },
    });
  }, [selectedNode, hoveredNode, filters, sigma, setSettings]);

  return null;
}

function CameraAnimator({ targetNode }: { targetNode: string | null }) {
  const sigma = useSigma();

  useEffect(() => {
    if (!targetNode) return;
    const nodeData = sigma.getNodeDisplayData(targetNode);
    if (nodeData) {
      // Convert graph coordinates → viewport pixels → framed graph (camera) coordinates
      const viewportPos = sigma.graphToViewport({ x: nodeData.x, y: nodeData.y });
      const framedPos = sigma.viewportToFramedGraph(viewportPos);
      sigma.getCamera().animate(
        { x: framedPos.x, y: framedPos.y },
        { duration: 300 },
      );
    }
  }, [targetNode, sigma]);

  return null;
}

export default function GraphCanvas({
  graph,
  selectedNode,
  onSelectNode,
  hoveredNode,
  onHoverNode,
  filters,
}: GraphCanvasProps) {
  const sigmaSettings = useMemo(
    () => ({
      defaultNodeColor: "#999",
      defaultEdgeColor: "#ccc",
      defaultEdgeType: "line" as const,
      labelDensity: 0.15,
      labelGridCellSize: 100,
      labelRenderedSizeThreshold: 6,
      renderEdgeLabels: false,
      zIndex: true,
      minCameraRatio: 0.02,
      maxCameraRatio: 10,
    }),
    [],
  );

  return (
    <SigmaContainer className="h-full w-full" settings={sigmaSettings}>
      <GraphLoader graph={graph} />
      <GraphEvents onSelectNode={onSelectNode} onHoverNode={onHoverNode} />
      <GraphReducers
        selectedNode={selectedNode}
        hoveredNode={hoveredNode}
        filters={filters}
      />
      <CameraAnimator targetNode={selectedNode} />
      <GraphControls filters={filters} />
      <GraphTooltip hoveredNode={hoveredNode} graph={graph} />
    </SigmaContainer>
  );
}
