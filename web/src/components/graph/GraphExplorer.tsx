"use client";

import { useState, useCallback, useMemo, useRef } from "react";
import { useSearchParams } from "next/navigation";
import { useGraphData, useGraphThemes, useGraphEntities, useGraphTopics, type GraphQuery } from "@/hooks/useGraphData";
import { useNodeSelection } from "@/hooks/useNodeSelection";
import { useSearch } from "@/hooks/useSearch";
import { useLang } from "@/lib/LangContext";
import type { ApiEntity, ApiTopic } from "@/lib/api";
import type { SearchEntry } from "@/lib/types";
import GraphCanvas from "./GraphCanvas";
import GraphDetailPanel from "./GraphDetailPanel";
import GraphLegend from "./GraphLegend";
import FilterPanel, { type GraphFilters, DEFAULT_FILTERS, hasHighlightFilters, matchesHighlightFilters } from "./FilterPanel";
import SearchBar from "../search/SearchBar";

const DEFAULT_THEME = "church";

/** Parse a filter expression like "mary + salvation" into theme/entity/topic IDs. */
function parseFilterExpr(
  expr: string,
  themes: { id: string; label: string }[],
  entities: ApiEntity[],
  topics: ApiTopic[],
): { themes: string[]; entities: string[]; topics: number[] } | null {
  const terms = expr.split("+").map((t) => t.trim().toLowerCase()).filter(Boolean);
  if (terms.length === 0) return null;

  const matchedThemes: string[] = [];
  const matchedEntities: string[] = [];
  const matchedTopics: number[] = [];

  for (const term of terms) {
    // Try theme ID match first
    const theme = themes.find((t) => t.id.toLowerCase() === term || t.label.toLowerCase() === term);
    if (theme) { matchedThemes.push(theme.id); continue; }

    // Try entity ID or label
    const entity = entities.find(
      (e) => e.id.toLowerCase() === term || e.label.toLowerCase() === term,
    );
    if (entity) { matchedEntities.push(entity.id); continue; }

    // Try topic by term keyword
    const topic = topics.find(
      (t) => t.terms.some((kw) => kw.toLowerCase() === term),
    );
    if (topic !== undefined) { matchedTopics.push(topic.id); continue; }

    // Fuzzy: partial match on theme/entity labels
    const fuzzyTheme = themes.find((t) => t.label.toLowerCase().includes(term));
    if (fuzzyTheme) { matchedThemes.push(fuzzyTheme.id); continue; }
    const fuzzyEntity = entities.find((e) => e.label.toLowerCase().includes(term));
    if (fuzzyEntity) { matchedEntities.push(fuzzyEntity.id); continue; }

    // Unrecognized term — ignore
  }

  if (matchedThemes.length === 0 && matchedEntities.length === 0 && matchedTopics.length === 0) {
    return null;
  }
  return { themes: matchedThemes, entities: matchedEntities, topics: matchedTopics };
}

export default function GraphExplorer() {
  const searchParams = useSearchParams();
  const paramTheme = searchParams.get("theme");
  const paramEntity = searchParams.get("entity");
  const paramTopic = searchParams.get("topic");
  const paramFilter = searchParams.get("filter");

  const [selectedTheme, setSelectedTheme] = useState<string>(paramTheme || DEFAULT_THEME);
  const [selectedEntity, setSelectedEntity] = useState<string | null>(paramEntity);
  const [selectedTopic, setSelectedTopic] = useState<number | null>(
    paramTopic !== null ? Number(paramTopic) : null,
  );
  const [filterExpr, setFilterExpr] = useState<string>(paramFilter || "");
  const [activeFilter, setActiveFilter] = useState<{ themes: string[]; entities: string[]; topics: number[] } | null>(null);
  const filterInputRef = useRef<HTMLInputElement>(null);
  const apiThemes = useGraphThemes();
  const apiEntities = useGraphEntities();
  const apiTopics = useGraphTopics();

  // Parse filter param from URL once metadata is loaded
  const filterParamParsed = useRef(false);
  useMemo(() => {
    if (filterParamParsed.current || !paramFilter || apiThemes.length === 0) return;
    const parsed = parseFilterExpr(paramFilter, apiThemes, apiEntities, apiTopics);
    if (parsed) {
      setActiveFilter(parsed);
      filterParamParsed.current = true;
    }
  }, [paramFilter, apiThemes, apiEntities, apiTopics]);

  // Determine graph query priority: active filter > entity > topic > theme
  const graphQuery: GraphQuery = activeFilter
    ? { mode: "filter", ...activeFilter }
    : selectedEntity
      ? { mode: "entity", entityId: selectedEntity }
      : selectedTopic !== null
        ? { mode: "topic", topicId: selectedTopic }
        : { mode: "theme", theme: selectedTheme };
  const { graph, loading, error } = useGraphData(graphQuery);
  const { selectedNode, selectNode, pushState, goBack, canGoBack, clearSelection } =
    useNodeSelection();
  const { lang } = useLang();
  const { query, results, search } = useSearch(lang);
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
      setSelectedTheme(themeId);
      clearSelection();
    },
    [clearSelection],
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
            {activeFilter
              ? `Filter: ${[...activeFilter.themes, ...activeFilter.entities, ...activeFilter.topics.map(String)].join(" + ")}`
              : selectedEntity
                ? `Entity: ${selectedEntity}`
                : selectedTopic !== null
                  ? `Topic: ${selectedTopic}`
                  : `Theme: ${selectedTheme}`}
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

      {/* Subgraph selectors */}
      <div className="absolute right-4 top-4 z-20 flex flex-col gap-2 w-64">
        {/* Multi-filter input */}
        <form
          onSubmit={(e) => {
            e.preventDefault();
            if (!filterExpr.trim()) {
              setActiveFilter(null);
              return;
            }
            const parsed = parseFilterExpr(filterExpr, apiThemes, apiEntities, apiTopics);
            setActiveFilter(parsed);
            if (parsed) {
              setSelectedEntity(null);
              setSelectedTopic(null);
            }
            clearSelection();
            filterInputRef.current?.blur();
          }}
          className="relative"
        >
          <input
            ref={filterInputRef}
            type="text"
            value={filterExpr}
            onChange={(e) => setFilterExpr(e.target.value)}
            placeholder="Filter: mary + salvation"
            className="w-full rounded-lg border bg-white px-3 py-2 pr-8 text-sm shadow-md focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-200 dark:border-zinc-700 dark:bg-zinc-900 dark:text-white"
          />
          {activeFilter && (
            <button
              type="button"
              onClick={() => {
                setFilterExpr("");
                setActiveFilter(null);
                clearSelection();
              }}
              className="absolute right-2 top-2 text-zinc-400 hover:text-zinc-600 dark:hover:text-zinc-200"
              title="Clear filter"
            >
              <svg className="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
                <path d="M6.28 5.22a.75.75 0 00-1.06 1.06L8.94 10l-3.72 3.72a.75.75 0 101.06 1.06L10 11.06l3.72 3.72a.75.75 0 101.06-1.06L11.06 10l3.72-3.72a.75.75 0 00-1.06-1.06L10 8.94 6.28 5.22z" />
              </svg>
            </button>
          )}
        </form>
        {activeFilter && (
          <div className="rounded bg-blue-50 px-2 py-1 text-xs text-blue-700 dark:bg-blue-900/30 dark:text-blue-300">
            {activeFilter.themes.length > 0 && <span>Themes: {activeFilter.themes.join(", ")}</span>}
            {activeFilter.entities.length > 0 && <span>{activeFilter.themes.length > 0 ? " + " : ""}Entities: {activeFilter.entities.join(", ")}</span>}
            {activeFilter.topics.length > 0 && <span>{(activeFilter.themes.length > 0 || activeFilter.entities.length > 0) ? " + " : ""}Topics: {activeFilter.topics.join(", ")}</span>}
          </div>
        )}
        {!activeFilter && (
          <>
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
          </>
        )}
      </div>

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
