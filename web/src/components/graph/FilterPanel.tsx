"use client";

import { useEffect, useState } from "react";
import { PART_COLORS, PART_SHORT_NAMES, SOURCE_COLORS, THEME_COLORS } from "@/lib/colors";
import type { ThemeDefinition } from "@/lib/types";
import { fetchThemes } from "@/lib/graph-data";

export interface GraphFilters {
  visibleParts: Set<string>;
  showStructural: boolean;
  showBibleNodes: boolean;
  showAuthorNodes: boolean;
  showDocumentNodes: boolean;
  showCrossRefs: boolean;
  showCites: boolean;
  showBelongsTo: boolean;
  showChildOf: boolean;
  selectedThemes: Set<string>;
}

export const DEFAULT_FILTERS: GraphFilters = {
  visibleParts: new Set(Object.keys(PART_COLORS)),
  showStructural: false,
  showBibleNodes: false,
  showAuthorNodes: false,
  showDocumentNodes: false,
  showCrossRefs: true,
  showCites: true,
  showBelongsTo: true,
  showChildOf: true,
  selectedThemes: new Set(),
};

interface FilterPanelProps {
  filters: GraphFilters;
  onFiltersChange: (filters: GraphFilters) => void;
  isOpen: boolean;
  onToggle: () => void;
}

export default function FilterPanel({
  filters,
  onFiltersChange,
  isOpen,
  onToggle,
}: FilterPanelProps) {
  const [themes, setThemes] = useState<Record<string, ThemeDefinition>>({});

  useEffect(() => {
    fetchThemes().then(setThemes).catch(() => {});
  }, []);

  const togglePart = (part: string) => {
    const next = new Set(filters.visibleParts);
    if (next.has(part)) next.delete(part);
    else next.add(part);
    onFiltersChange({ ...filters, visibleParts: next });
  };

  const toggleTheme = (themeId: string) => {
    const next = new Set(filters.selectedThemes);
    if (next.has(themeId)) next.delete(themeId);
    else next.add(themeId);
    onFiltersChange({ ...filters, selectedThemes: next });
  };

  return (
    <>
      {/* Toggle button */}
      <button
        onClick={onToggle}
        className="absolute left-4 top-4 z-20 rounded bg-white px-3 py-1.5 text-sm shadow hover:bg-zinc-50 dark:bg-zinc-800 dark:hover:bg-zinc-700"
      >
        {isOpen ? "Hide Filters" : "Filters"}
      </button>

      {/* Panel */}
      {isOpen && (
        <div className="absolute left-4 top-14 z-20 w-56 max-h-[calc(100vh-5rem)] overflow-y-auto rounded-lg bg-white/95 p-4 shadow-lg backdrop-blur dark:bg-zinc-900/95">
          <h3 className="mb-3 text-xs font-semibold uppercase text-zinc-500">
            Parts
          </h3>
          <div className="space-y-1.5">
            {Object.entries(PART_COLORS).map(([part, color]) => (
              <label key={part} className="flex items-center gap-2 text-xs">
                <input
                  type="checkbox"
                  checked={filters.visibleParts.has(part)}
                  onChange={() => togglePart(part)}
                  className="rounded"
                />
                <span
                  className="inline-block h-2 w-2 rounded-full"
                  style={{ backgroundColor: color }}
                />
                <span className="text-zinc-700 dark:text-zinc-300">
                  {PART_SHORT_NAMES[part] || part}
                </span>
              </label>
            ))}
          </div>

          <h3 className="mb-2 mt-4 text-xs font-semibold uppercase text-zinc-500">
            Sources
          </h3>
          <div className="space-y-1.5">
            <label className="flex items-center gap-2 text-xs">
              <input
                type="checkbox"
                checked={filters.showBibleNodes}
                onChange={() =>
                  onFiltersChange({
                    ...filters,
                    showBibleNodes: !filters.showBibleNodes,
                  })
                }
                className="rounded"
              />
              <span
                className="inline-block h-2 w-2 rounded-full"
                style={{ backgroundColor: SOURCE_COLORS.bible }}
              />
              <span className="text-zinc-700 dark:text-zinc-300">
                Bible books
              </span>
            </label>
            <label className="flex items-center gap-2 text-xs">
              <input
                type="checkbox"
                checked={filters.showAuthorNodes}
                onChange={() =>
                  onFiltersChange({
                    ...filters,
                    showAuthorNodes: !filters.showAuthorNodes,
                  })
                }
                className="rounded"
              />
              <span
                className="inline-block h-2 w-2 rounded-full"
                style={{ backgroundColor: SOURCE_COLORS.author }}
              />
              <span className="text-zinc-700 dark:text-zinc-300">
                Church Fathers
              </span>
            </label>
            <label className="flex items-center gap-2 text-xs">
              <input
                type="checkbox"
                checked={filters.showDocumentNodes}
                onChange={() =>
                  onFiltersChange({
                    ...filters,
                    showDocumentNodes: !filters.showDocumentNodes,
                  })
                }
                className="rounded"
              />
              <span
                className="inline-block h-2 w-2 rounded-full"
                style={{ backgroundColor: SOURCE_COLORS.document }}
              />
              <span className="text-zinc-700 dark:text-zinc-300">
                Ecclesiastical Documents
              </span>
            </label>
          </div>

          <h3 className="mb-2 mt-4 text-xs font-semibold uppercase text-zinc-500">
            Nodes
          </h3>
          <label className="flex items-center gap-2 text-xs">
            <input
              type="checkbox"
              checked={filters.showStructural}
              onChange={() =>
                onFiltersChange({
                  ...filters,
                  showStructural: !filters.showStructural,
                })
              }
              className="rounded"
            />
            <span className="text-zinc-700 dark:text-zinc-300">
              Show structural nodes
            </span>
          </label>

          <h3 className="mb-2 mt-4 text-xs font-semibold uppercase text-zinc-500">
            Edges
          </h3>
          <div className="space-y-1.5">
            <label className="flex items-center gap-2 text-xs">
              <input
                type="checkbox"
                checked={filters.showCrossRefs}
                onChange={() =>
                  onFiltersChange({
                    ...filters,
                    showCrossRefs: !filters.showCrossRefs,
                  })
                }
                className="rounded"
              />
              <span className="text-zinc-700 dark:text-zinc-300">
                Cross-references
              </span>
            </label>
            <label className="flex items-center gap-2 text-xs">
              <input
                type="checkbox"
                checked={filters.showCites}
                onChange={() =>
                  onFiltersChange({
                    ...filters,
                    showCites: !filters.showCites,
                  })
                }
                className="rounded"
              />
              <span className="text-zinc-700 dark:text-zinc-300">
                Citations
              </span>
            </label>
            <label className="flex items-center gap-2 text-xs">
              <input
                type="checkbox"
                checked={filters.showBelongsTo}
                onChange={() =>
                  onFiltersChange({
                    ...filters,
                    showBelongsTo: !filters.showBelongsTo,
                  })
                }
                className="rounded"
              />
              <span className="text-zinc-700 dark:text-zinc-300">
                Belongs-to
              </span>
            </label>
            <label className="flex items-center gap-2 text-xs">
              <input
                type="checkbox"
                checked={filters.showChildOf}
                onChange={() =>
                  onFiltersChange({
                    ...filters,
                    showChildOf: !filters.showChildOf,
                  })
                }
                className="rounded"
              />
              <span className="text-zinc-700 dark:text-zinc-300">
                Child-of (hierarchy)
              </span>
            </label>
          </div>

          {Object.keys(themes).length > 0 && (
            <>
              <h3 className="mb-2 mt-4 text-xs font-semibold uppercase text-zinc-500">
                Themes
              </h3>
              <div className="space-y-1.5">
                {Object.entries(themes).map(([themeId, def]) => (
                  <label key={themeId} className="flex items-center gap-2 text-xs">
                    <input
                      type="checkbox"
                      checked={filters.selectedThemes.has(themeId)}
                      onChange={() => toggleTheme(themeId)}
                      className="rounded"
                    />
                    <span
                      className="inline-block h-2 w-2 rounded-full"
                      style={{ backgroundColor: THEME_COLORS[themeId] || "#999" }}
                    />
                    <span className="text-zinc-700 dark:text-zinc-300">
                      {def.label}
                    </span>
                    <span className="text-zinc-400">({def.count})</span>
                  </label>
                ))}
              </div>
            </>
          )}
        </div>
      )}
    </>
  );
}
