"use client";

import { useEffect, useState } from "react";
import { PART_COLORS, PART_SHORT_NAMES, SOURCE_COLORS, BIBLE_HIERARCHY_COLORS, PATRISTIC_HIERARCHY_COLORS, THEME_COLORS } from "@/lib/colors";
import type { ThemeDefinition } from "@/lib/types";
import { fetchThemes } from "@/lib/graph-data";

export interface GraphFilters {
  visibleParts: Set<string>;
  showStructural: boolean;
  showBibleNodes: boolean;
  showBibleChapters: boolean;
  showBibleVerses: boolean;
  showAuthorNodes: boolean;
  showPatristicWorks: boolean;
  showDocumentNodes: boolean;
  showCrossRefs: boolean;
  showCites: boolean;
  showBelongsTo: boolean;
  showChildOf: boolean;
  showSharedTheme: boolean;
  showBibleCrossRefs: boolean;
  selectedThemes: Set<string>;
}

export const DEFAULT_FILTERS: GraphFilters = {
  visibleParts: new Set(Object.keys(PART_COLORS)),
  showStructural: false,
  showBibleNodes: false,
  showBibleChapters: false,
  showBibleVerses: false,
  showAuthorNodes: false,
  showPatristicWorks: false,
  showDocumentNodes: false,
  showCrossRefs: true,
  showCites: true,
  showBelongsTo: true,
  showChildOf: true,
  showSharedTheme: false,
  showBibleCrossRefs: false,
  selectedThemes: new Set(),
};

function filtersMatchDefaults(filters: GraphFilters): boolean {
  if (filters.showStructural !== DEFAULT_FILTERS.showStructural) return false;
  if (filters.showBibleNodes !== DEFAULT_FILTERS.showBibleNodes) return false;
  if (filters.showBibleChapters !== DEFAULT_FILTERS.showBibleChapters) return false;
  if (filters.showBibleVerses !== DEFAULT_FILTERS.showBibleVerses) return false;
  if (filters.showAuthorNodes !== DEFAULT_FILTERS.showAuthorNodes) return false;
  if (filters.showPatristicWorks !== DEFAULT_FILTERS.showPatristicWorks) return false;
  if (filters.showDocumentNodes !== DEFAULT_FILTERS.showDocumentNodes) return false;
  if (filters.showCrossRefs !== DEFAULT_FILTERS.showCrossRefs) return false;
  if (filters.showCites !== DEFAULT_FILTERS.showCites) return false;
  if (filters.showBelongsTo !== DEFAULT_FILTERS.showBelongsTo) return false;
  if (filters.showChildOf !== DEFAULT_FILTERS.showChildOf) return false;
  if (filters.showSharedTheme !== DEFAULT_FILTERS.showSharedTheme) return false;
  if (filters.showBibleCrossRefs !== DEFAULT_FILTERS.showBibleCrossRefs) return false;
  if (filters.visibleParts.size !== DEFAULT_FILTERS.visibleParts.size) return false;
  for (const p of DEFAULT_FILTERS.visibleParts) {
    if (!filters.visibleParts.has(p)) return false;
  }
  if (filters.selectedThemes.size !== DEFAULT_FILTERS.selectedThemes.size) return false;
  return true;
}

interface FilterPanelProps {
  filters: GraphFilters;
  onFiltersChange: (filters: GraphFilters) => void;
  isOpen: boolean;
  onToggle: () => void;
  onReset: () => void;
}

export default function FilterPanel({
  filters,
  onFiltersChange,
  isOpen,
  onToggle,
  onReset,
}: FilterPanelProps) {
  const [themes, setThemes] = useState<Record<string, ThemeDefinition>>({});
  const isDefault = filtersMatchDefaults(filters);

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
        className="absolute left-4 top-4 z-20 flex items-center gap-1.5 rounded bg-white px-3 py-1.5 text-sm shadow hover:bg-zinc-50 dark:bg-zinc-800 dark:hover:bg-zinc-700"
      >
        {isOpen ? "Hide Filters" : "Filters"}
        {!isDefault && (
          <span className="inline-block h-2 w-2 rounded-full bg-blue-500" />
        )}
      </button>

      {/* Panel */}
      {isOpen && (
        <div className="absolute left-4 top-14 z-20 w-56 max-h-[calc(100vh-5rem)] overflow-y-auto rounded-lg bg-white/95 p-4 shadow-lg backdrop-blur dark:bg-zinc-900/95">
          {!isDefault && (
            <button
              onClick={onReset}
              className="mb-3 w-full rounded bg-zinc-100 px-2 py-1 text-xs text-zinc-600 hover:bg-zinc-200 dark:bg-zinc-800 dark:text-zinc-400 dark:hover:bg-zinc-700"
            >
              Reset to defaults
            </button>
          )}
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
            {filters.showBibleNodes && (
              <>
                <label className="ml-4 flex items-center gap-2 text-xs">
                  <input
                    type="checkbox"
                    checked={filters.showBibleChapters}
                    onChange={() =>
                      onFiltersChange({
                        ...filters,
                        showBibleChapters: !filters.showBibleChapters,
                      })
                    }
                    className="rounded"
                  />
                  <span
                    className="inline-block h-2 w-2 rounded-full"
                    style={{ backgroundColor: BIBLE_HIERARCHY_COLORS["bible-chapter"] }}
                  />
                  <span className="text-zinc-700 dark:text-zinc-300">
                    Chapters
                  </span>
                </label>
                <label className="ml-4 flex items-center gap-2 text-xs">
                  <input
                    type="checkbox"
                    checked={filters.showBibleVerses}
                    onChange={() =>
                      onFiltersChange({
                        ...filters,
                        showBibleVerses: !filters.showBibleVerses,
                      })
                    }
                    className="rounded"
                  />
                  <span
                    className="inline-block h-2 w-2 rounded-full"
                    style={{ backgroundColor: BIBLE_HIERARCHY_COLORS["bible-verse"] }}
                  />
                  <span className="text-zinc-700 dark:text-zinc-300">
                    Verses
                  </span>
                </label>
              </>
            )}
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
            {filters.showAuthorNodes && (
              <label className="ml-4 flex items-center gap-2 text-xs">
                <input
                  type="checkbox"
                  checked={filters.showPatristicWorks}
                  onChange={() =>
                    onFiltersChange({
                      ...filters,
                      showPatristicWorks: !filters.showPatristicWorks,
                    })
                  }
                  className="rounded"
                />
                <span
                  className="inline-block h-2 w-2 rounded-full"
                  style={{ backgroundColor: PATRISTIC_HIERARCHY_COLORS["patristic-work"] }}
                />
                <span className="text-zinc-700 dark:text-zinc-300">
                  Works
                </span>
              </label>
            )}
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
            <label className="flex items-center gap-2 text-xs">
              <input
                type="checkbox"
                checked={filters.showSharedTheme}
                onChange={() =>
                  onFiltersChange({
                    ...filters,
                    showSharedTheme: !filters.showSharedTheme,
                  })
                }
                className="rounded"
              />
              <span className="text-zinc-700 dark:text-zinc-300">
                Shared themes
              </span>
            </label>
            <label className="flex items-center gap-2 text-xs">
              <input
                type="checkbox"
                checked={filters.showBibleCrossRefs}
                onChange={() =>
                  onFiltersChange({
                    ...filters,
                    showBibleCrossRefs: !filters.showBibleCrossRefs,
                  })
                }
                className="rounded"
              />
              <span className="text-zinc-700 dark:text-zinc-300">
                Bible cross-refs
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
