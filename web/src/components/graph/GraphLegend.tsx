"use client";

import { useState } from "react";
import { PART_COLORS, PART_SHORT_NAMES, STRUCTURE_COLOR, SOURCE_COLORS } from "@/lib/colors";

const EDGE_TYPE_LEGEND: { type: string; label: string; color: string }[] = [
  { type: "cross_reference", label: "Cross-reference", color: "#cc6666" },
  { type: "cites", label: "Citation", color: "#59A14F" },
  { type: "shared_theme", label: "Shared theme", color: "#6A3D9A" },
  { type: "shared_entity", label: "Shared entity", color: "#E6832A" },
  { type: "shared_topic", label: "Shared topic", color: "#1B9E77" },
  { type: "shared_citation", label: "Shared citation", color: "#D62F2F" },
  { type: "bible_cross_reference", label: "Bible cross-ref", color: "#59A14F" },
  { type: "belongs_to", label: "Belongs-to", color: "#999999" },
  { type: "child_of", label: "Child-of", color: "#999999" },
];

export default function GraphLegend() {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="absolute bottom-4 left-4 z-10 rounded bg-white/90 p-3 shadow backdrop-blur dark:bg-zinc-900/90">
      <button
        onClick={() => setExpanded(!expanded)}
        className="mb-2 flex items-center gap-1 text-xs font-semibold uppercase text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300"
      >
        Legend
        <span className="text-[10px]">{expanded ? "▼" : "▶"}</span>
      </button>

      {expanded && (
        <>
          <h4 className="mb-1 text-[10px] font-semibold uppercase text-zinc-400">Nodes</h4>
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

          <h4 className="mb-1 mt-3 text-[10px] font-semibold uppercase text-zinc-400">Edges</h4>
          <div className="space-y-1">
            {EDGE_TYPE_LEGEND.map(({ type, label, color }) => (
              <div key={type} className="flex items-center gap-2">
                <span
                  className="inline-block h-0.5 w-4 rounded"
                  style={{ backgroundColor: color }}
                />
                <span className="text-xs text-zinc-700 dark:text-zinc-300">{label}</span>
              </div>
            ))}
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
        </>
      )}
    </div>
  );
}
