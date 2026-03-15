"use client";

import { useState, useCallback, useMemo } from "react";
import { useSearchParams } from "next/navigation";
import { useGraphData, useGraphThemes, useGraphEntities, useGraphTopics, type GraphQuery } from "@/hooks/useGraphData";
import { useNodeSelection } from "@/hooks/useNodeSelection";
import { useSearch } from "@/hooks/useSearch";
import { hasApi } from "@/lib/api";
import type { SearchEntry } from "@/lib/types";
import GraphCanvas from "./GraphCanvas";
import GraphDetailPanel from "./GraphDetailPanel";
import GraphLegend from "./GraphLegend";
import FilterPanel, { type GraphFilters, DEFAULT_FILTERS, hasHighlightFilters, matchesHighlightFilters } from "./FilterPanel";
import SearchBar from "../search/SearchBar";

const DEFAULT_THEME = "church";

export default function GraphExplorer() {
  const searchParams = useSearchParams();
  const paramTheme = searchParams.get("theme");
  const paramEntity = searchParams.get("entity");
  const paramTopic = searchParams.get("topic");

  const [selectedTheme, setSelectedTheme] = useState<string>(paramTheme || DEFAULT_THEME);
  const [selectedEntity, setSelectedEntity] = useState<string | null>(paramEntity);
  const [selectedTopic, setSelectedTopic] = useState<number | null>(
    paramTopic !== null ? Number(paramTopic) : null,
  );
  const apiThemes = useGraphThemes();
  const apiEntities = useGraphEntities();
  const apiTopics = useGraphTopics();

  // Determine graph query priority: entity > topic > theme
  const graphQuery: GraphQuery = hasApi
    ? selectedEntity
      ? { mode: "entity", entityId: selectedEntity }
      : selectedTopic !== null
        ? { mode: "topic", topicId: selectedTopic }
        : selectedTheme
          ? { mode: "theme", theme: selectedTheme }
          : null
    : null;
  const { graph, loading, error } = useGraphData(graphQuery);
  const { selectedNode, selectNode, pushState, goBack, canGoBack, clearSelection } =
    useNodeSelection();
  const { query, results, search } = useSearch();
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);
  const [filters, setFilters] = useState<GraphFilters>(DEFAULT_FILTERS);
  const [filterOpen, setFilterOpen] = useState(false);

  const highlightedCount = useMemo(() => {
    if (!graph || !hasHighlightFilters(filters)) return undefined;
    let count = 0;
    graph.forEachNode((node, attrs) => {
      if (attrs.node_type === "paragraph" && matchesHighlightFilters(filters, attrs)) {
        count++;
      }
    });
    return count;
  }, [graph, filters]);

  const handleSelectNode = useCallback(
    (nodeId: string | null) => {
      selectNode(nodeId);
    },
    [selectNode],
  );

  const handleNavigate = useCallback(
    (nodeId: string) => {
      // Push current state so we can go back
      if (selectedNode) {
        pushState(selectedNode, filters);
      }

      // Auto-enable the relevant source filter based on node ID prefix
      const nextFilters = { ...filters, visibleParts: new Set(filters.visibleParts), selectedThemes: new Set(filters.selectedThemes) };
      if (nodeId.startsWith("bible:") || nodeId.startsWith("bible-book:") || nodeId.startsWith("bible-testament:")) {
        nextFilters.showBibleNodes = true;
      } else if (nodeId.startsWith("bible-chapter:")) {
        nextFilters.showBibleNodes = true;
        nextFilters.showBibleChapters = true;
      } else if (nodeId.startsWith("bible-verse:")) {
        nextFilters.showBibleNodes = true;
        nextFilters.showBibleChapters = true;
        nextFilters.showBibleVerses = true;
      } else if (nodeId.startsWith("author:")) {
        nextFilters.showAuthorNodes = true;
      } else if (nodeId.startsWith("patristic-work:")) {
        nextFilters.showAuthorNodes = true;
        nextFilters.showPatristicWorks = true;
      } else if (nodeId.startsWith("document-section:")) {
        nextFilters.showDocumentNodes = true;
        nextFilters.showDocumentSections = true;
      } else if (nodeId.startsWith("document:")) {
        nextFilters.showDocumentNodes = true;
      }
      setFilters(nextFilters);
      selectNode(nodeId);
    },
    [selectedNode, filters, pushState, selectNode],
  );

  const handleThemeFilter = useCallback(
    (themeId: string) => {
      if (hasApi) {
        // With API: switch the loaded theme entirely
        setSelectedTheme(themeId);
        clearSelection();
      } else {
        // Without API: client-side filter (original behavior)
        if (selectedNode) {
          pushState(selectedNode, filters);
        }
        setFilters({
          ...filters,
          visibleParts: new Set(filters.visibleParts),
          selectedThemes: new Set([themeId]),
        });
      }
    },
    [selectedNode, filters, pushState, clearSelection],
  );

  const handleGoBack = useCallback(() => {
    const entry = goBack();
    if (entry) {
      setFilters(entry.filters);
    }
  }, [goBack]);

  const handleResetFilters = useCallback(() => {
    // Push current state so reset is undoable
    if (selectedNode) {
      pushState(selectedNode, filters);
    }
    setFilters({
      ...DEFAULT_FILTERS,
      visibleParts: new Set(DEFAULT_FILTERS.visibleParts),
      selectedThemes: new Set(DEFAULT_FILTERS.selectedThemes),
    });
  }, [selectedNode, filters, pushState]);

  const handleSearchSelect = useCallback(
    (entry: SearchEntry) => {
      if (typeof entry.id === "string") {
        // Source node ID like "bible:john" or "author:augustine"
        selectNode(entry.id);
      } else {
        // Paragraph numeric ID
        selectNode(`p:${entry.id}`);
      }
    },
    [selectNode],
  );

  if (loading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="text-center">
          <div className="mb-2 text-lg font-medium">Loading graph...</div>
          <div className="text-sm text-zinc-500">
            {hasApi && selectedEntity
              ? `Entity: ${selectedEntity}`
              : hasApi && selectedTopic !== null
                ? `Topic: ${selectedTopic}`
                : hasApi && selectedTheme
                  ? `Theme: ${selectedTheme}`
                  : "Full graph"}
          </div>
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

      {/* Subgraph selectors — only shown when API is available */}
      {hasApi && (
        <div className="absolute right-4 top-4 z-20 flex flex-col gap-2">
          {apiThemes.length > 0 && (
            <select
              value={selectedEntity || selectedTopic !== null ? "" : selectedTheme}
              onChange={(e) => {
                setSelectedEntity(null);
                setSelectedTopic(null);
                setSelectedTheme(e.target.value);
                clearSelection();
              }}
              className="rounded-lg border bg-white px-3 py-2 text-sm shadow-md focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-200 dark:border-zinc-700 dark:bg-zinc-900 dark:text-white"
            >
              {apiThemes.map((t) => (
                <option key={t.id} value={t.id}>
                  Theme: {t.label} ({t.count})
                </option>
              ))}
            </select>
          )}
          {apiEntities.length > 0 && (
            <select
              value={selectedEntity || ""}
              onChange={(e) => {
                const val = e.target.value || null;
                setSelectedEntity(val);
                if (val) setSelectedTopic(null);
                clearSelection();
              }}
              className="rounded-lg border bg-white px-3 py-2 text-sm shadow-md focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-200 dark:border-zinc-700 dark:bg-zinc-900 dark:text-white"
            >
              <option value="">Entity: none</option>
              {apiEntities.map((e) => (
                <option key={e.id} value={e.id}>
                  {e.label} ({e.count})
                </option>
              ))}
            </select>
          )}
          {apiTopics.length > 0 && (
            <select
              value={selectedTopic !== null ? String(selectedTopic) : ""}
              onChange={(e) => {
                const val = e.target.value ? Number(e.target.value) : null;
                setSelectedTopic(val);
                if (val !== null) setSelectedEntity(null);
                clearSelection();
              }}
              className="rounded-lg border bg-white px-3 py-2 text-sm shadow-md focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-200 dark:border-zinc-700 dark:bg-zinc-900 dark:text-white"
            >
              <option value="">Topic: none</option>
              {apiTopics.map((t) => (
                <option key={t.id} value={String(t.id)}>
                  {t.terms.slice(0, 4).join(", ")}
                </option>
              ))}
            </select>
          )}
        </div>
      )}

      <FilterPanel
        filters={filters}
        onFiltersChange={setFilters}
        isOpen={filterOpen}
        onToggle={() => setFilterOpen(!filterOpen)}
        onReset={handleResetFilters}
        highlightedCount={highlightedCount}
      />

      <GraphLegend />

      {selectedNode && (
        <GraphDetailPanel
          nodeId={selectedNode}
          graph={graph}
          onClose={clearSelection}
          onNavigate={handleNavigate}
          onThemeFilter={handleThemeFilter}
          canGoBack={canGoBack}
          onGoBack={handleGoBack}
        />
      )}
    </div>
  );
}
