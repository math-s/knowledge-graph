"use client";

import { useState, useCallback } from "react";
import { useGraphData } from "@/hooks/useGraphData";
import { useNodeSelection } from "@/hooks/useNodeSelection";
import { useSearch } from "@/hooks/useSearch";
import GraphCanvas from "./GraphCanvas";
import GraphDetailPanel from "./GraphDetailPanel";
import GraphLegend from "./GraphLegend";
import FilterPanel, { type GraphFilters, DEFAULT_FILTERS } from "./FilterPanel";
import SearchBar from "../search/SearchBar";

export default function GraphExplorer() {
  const { graph, loading, error } = useGraphData();
  const { selectedNode, selectNode, clearSelection } = useNodeSelection();
  const { query, results, search } = useSearch();
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);
  const [filters, setFilters] = useState<GraphFilters>(DEFAULT_FILTERS);
  const [filterOpen, setFilterOpen] = useState(false);

  const handleSelectNode = useCallback(
    (nodeId: string | null) => {
      selectNode(nodeId);
    },
    [selectNode],
  );

  const handleNavigate = useCallback(
    (nodeId: string) => {
      selectNode(nodeId);
    },
    [selectNode],
  );

  const handleSearchSelect = useCallback(
    (paragraphId: number) => {
      selectNode(`p:${paragraphId}`);
    },
    [selectNode],
  );

  if (loading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="text-center">
          <div className="mb-2 text-lg font-medium">Loading graph...</div>
          <div className="text-sm text-zinc-500">3,260 nodes, 3,928 edges</div>
        </div>
      </div>
    );
  }

  if (error || !graph) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="text-center text-red-600">
          Failed to load graph: {error}
        </div>
      </div>
    );
  }

  return (
    <div className="relative h-screen w-screen overflow-hidden">
      <GraphCanvas
        graph={graph}
        selectedNode={selectedNode}
        onSelectNode={handleSelectNode}
        hoveredNode={hoveredNode}
        onHoverNode={setHoveredNode}
        filters={filters}
      />

      <SearchBar
        query={query}
        results={results}
        onSearch={search}
        onSelect={handleSearchSelect}
      />

      <FilterPanel
        filters={filters}
        onFiltersChange={setFilters}
        isOpen={filterOpen}
        onToggle={() => setFilterOpen(!filterOpen)}
      />

      <GraphLegend />

      {selectedNode && (
        <GraphDetailPanel
          nodeId={selectedNode}
          graph={graph}
          onClose={clearSelection}
          onNavigate={handleNavigate}
        />
      )}
    </div>
  );
}
