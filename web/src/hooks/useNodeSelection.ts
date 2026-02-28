"use client";

import { useState, useCallback, useRef } from "react";
import type { GraphFilters } from "@/components/graph/FilterPanel";

interface NavigationEntry {
  nodeId: string;
  filters: GraphFilters;
}

function cloneFilters(filters: GraphFilters): GraphFilters {
  return {
    ...filters,
    visibleParts: new Set(filters.visibleParts),
    selectedThemes: new Set(filters.selectedThemes),
  };
}

const MAX_HISTORY = 20;

export function useNodeSelection() {
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
  const historyRef = useRef<NavigationEntry[]>([]);

  const selectNode = useCallback((nodeId: string | null) => {
    setSelectedNode(nodeId);
  }, []);

  const pushState = useCallback((currentNodeId: string, currentFilters: GraphFilters) => {
    const entry: NavigationEntry = {
      nodeId: currentNodeId,
      filters: cloneFilters(currentFilters),
    };
    historyRef.current = [...historyRef.current.slice(-(MAX_HISTORY - 1)), entry];
  }, []);

  const goBack = useCallback((): NavigationEntry | null => {
    const stack = historyRef.current;
    if (stack.length === 0) return null;
    const entry = stack[stack.length - 1];
    historyRef.current = stack.slice(0, -1);
    setSelectedNode(entry.nodeId);
    return entry;
  }, []);

  // Derived from ref, not state — only re-evaluated when the component
  // re-renders via setSelectedNode (called by selectNode/goBack/clearSelection).
  const canGoBack = historyRef.current.length > 0;

  const clearSelection = useCallback(() => {
    historyRef.current = [];
    setSelectedNode(null);
  }, []);

  return { selectedNode, selectNode, pushState, goBack, canGoBack, clearSelection };
}
