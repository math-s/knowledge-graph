"use client";

import { PART_COLORS, PART_SHORT_NAMES } from "@/lib/colors";

export interface GraphFilters {
  visibleParts: Set<string>;
  showStructural: boolean;
  showCrossRefs: boolean;
  showBelongsTo: boolean;
  showChildOf: boolean;
}

export const DEFAULT_FILTERS: GraphFilters = {
  visibleParts: new Set(Object.keys(PART_COLORS)),
  showStructural: false,
  showCrossRefs: true,
  showBelongsTo: true,
  showChildOf: true,
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
  const togglePart = (part: string) => {
    const next = new Set(filters.visibleParts);
    if (next.has(part)) next.delete(part);
    else next.add(part);
    onFiltersChange({ ...filters, visibleParts: next });
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
        <div className="absolute left-4 top-14 z-20 w-56 rounded-lg bg-white/95 p-4 shadow-lg backdrop-blur dark:bg-zinc-900/95">
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
        </div>
      )}
    </>
  );
}
