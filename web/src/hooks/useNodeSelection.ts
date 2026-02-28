"use client";

import { useState, useCallback } from "react";

export function useNodeSelection() {
  const [selectedNode, setSelectedNode] = useState<string | null>(null);

  const selectNode = useCallback((nodeId: string | null) => {
    setSelectedNode(nodeId);
  }, []);

  const clearSelection = useCallback(() => {
    setSelectedNode(null);
  }, []);

  return { selectedNode, selectNode, clearSelection };
}
