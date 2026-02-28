"use client";

import { PART_COLORS, PART_SHORT_NAMES, STRUCTURE_COLOR, SOURCE_COLORS } from "@/lib/colors";

export default function GraphLegend() {
  return (
    <div className="absolute bottom-4 left-4 z-10 rounded bg-white/90 p-3 shadow backdrop-blur dark:bg-zinc-900/90">
      <h3 className="mb-2 text-xs font-semibold uppercase text-zinc-500">
        Legend
      </h3>
      <div className="space-y-1">
        {Object.entries(PART_COLORS).map(([part, color]) => (
          <div key={part} className="flex items-center gap-2">
            <span
              className="inline-block h-2.5 w-2.5 rounded-full"
              style={{ backgroundColor: color }}
            />
            <span className="text-xs text-zinc-700 dark:text-zinc-300">
              {PART_SHORT_NAMES[part] || part}
            </span>
          </div>
        ))}
        <div className="flex items-center gap-2">
          <span
            className="inline-block h-2.5 w-2.5 rounded-full"
            style={{ backgroundColor: STRUCTURE_COLOR }}
          />
          <span className="text-xs text-zinc-700 dark:text-zinc-300">
            Structural node
          </span>
        </div>
        <div className="flex items-center gap-2">
          <span
            className="inline-block h-2.5 w-2.5 rounded-full"
            style={{ backgroundColor: SOURCE_COLORS.bible }}
          />
          <span className="text-xs text-zinc-700 dark:text-zinc-300">
            Bible book
          </span>
        </div>
        <div className="flex items-center gap-2">
          <span
            className="inline-block h-2.5 w-2.5 rounded-full"
            style={{ backgroundColor: SOURCE_COLORS.author }}
          />
          <span className="text-xs text-zinc-700 dark:text-zinc-300">
            Church Father
          </span>
        </div>
        <div className="flex items-center gap-2">
          <span
            className="inline-block h-2.5 w-2.5 rounded-full"
            style={{ backgroundColor: SOURCE_COLORS.document }}
          />
          <span className="text-xs text-zinc-700 dark:text-zinc-300">
            Ecclesiastical Document
          </span>
        </div>
      </div>
      <div className="mt-2 border-t pt-2 dark:border-zinc-700">
        <div className="flex items-center gap-2 text-xs text-zinc-500">
          <span className="inline-block h-1.5 w-1.5 rounded-full bg-zinc-400" />
          <span>Small = few connections</span>
        </div>
        <div className="flex items-center gap-2 text-xs text-zinc-500">
          <span className="inline-block h-3 w-3 rounded-full bg-zinc-400" />
          <span>Large = many connections</span>
        </div>
      </div>
    </div>
  );
}
