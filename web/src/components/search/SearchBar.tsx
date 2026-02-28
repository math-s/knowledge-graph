"use client";

import { useCallback, useRef, useState } from "react";
import type { SearchEntry } from "@/lib/types";

interface SearchBarProps {
  query: string;
  results: SearchEntry[];
  onSearch: (query: string) => void;
  onSelect: (paragraphId: number) => void;
}

export default function SearchBar({
  query,
  results,
  onSearch,
  onSelect,
}: SearchBarProps) {
  const [open, setOpen] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout>>(null);

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const value = e.target.value;
      if (timerRef.current) clearTimeout(timerRef.current);
      timerRef.current = setTimeout(() => {
        onSearch(value);
      }, 200);
      setOpen(true);
    },
    [onSearch],
  );

  const handleSelect = useCallback(
    (id: number) => {
      onSelect(id);
      setOpen(false);
      if (inputRef.current) inputRef.current.blur();
    },
    [onSelect],
  );

  return (
    <div className="absolute left-1/2 top-4 z-20 w-96 -translate-x-1/2">
      <div className="relative">
        <input
          ref={inputRef}
          type="text"
          placeholder="Search paragraphs..."
          defaultValue={query}
          onChange={handleChange}
          onFocus={() => setOpen(true)}
          onBlur={() => setTimeout(() => setOpen(false), 200)}
          className="w-full rounded-lg border bg-white px-4 py-2 pl-10 text-sm shadow-md focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-200 dark:border-zinc-700 dark:bg-zinc-900 dark:text-white"
        />
        <svg
          className="absolute left-3 top-2.5 h-4 w-4 text-zinc-400"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
          />
        </svg>
      </div>

      {open && results.length > 0 && (
        <div className="mt-1 max-h-80 overflow-y-auto rounded-lg border bg-white shadow-lg dark:border-zinc-700 dark:bg-zinc-900">
          {results.map((entry) => (
            <button
              key={entry.id}
              onMouseDown={() => handleSelect(entry.id)}
              className="block w-full px-4 py-2 text-left hover:bg-zinc-50 dark:hover:bg-zinc-800"
            >
              <div className="text-sm font-medium">CCC {entry.id}</div>
              <div className="line-clamp-2 text-xs text-zinc-500">
                {entry.text}
              </div>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
