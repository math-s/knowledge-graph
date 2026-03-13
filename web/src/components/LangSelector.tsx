"use client";

import { useLang } from "@/lib/LangContext";
import { type Lang, LANG_NAMES, LANG_SHORT } from "@/lib/types";

const LANGS: Lang[] = ["la", "en", "pt", "el"];

export default function LangSelector() {
  const { lang, setLang, sideBySide, setSideBySide, compareLang, setCompareLang } = useLang();

  return (
    <div className="flex items-center gap-2">
      <select
        value={lang}
        onChange={(e) => setLang(e.target.value as Lang)}
        className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs text-zinc-700 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300"
        title="Select language"
      >
        {LANGS.map((l) => (
          <option key={l} value={l}>
            {LANG_SHORT[l]} — {LANG_NAMES[l]}
          </option>
        ))}
      </select>
      <button
        onClick={() => setSideBySide(!sideBySide)}
        className={`rounded border px-2 py-1 text-xs ${
          sideBySide
            ? "border-blue-400 bg-blue-50 text-blue-700 dark:border-blue-600 dark:bg-blue-900/30 dark:text-blue-300"
            : "border-zinc-300 text-zinc-500 hover:bg-zinc-50 dark:border-zinc-600 dark:text-zinc-400 dark:hover:bg-zinc-800"
        }`}
        title="Toggle side-by-side language comparison"
      >
        {sideBySide ? "||" : "| |"}
      </button>
      {sideBySide && (
        <select
          value={compareLang}
          onChange={(e) => setCompareLang(e.target.value as Lang)}
          className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs text-zinc-700 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-300"
          title="Comparison language"
        >
          {LANGS.filter((l) => l !== lang).map((l) => (
            <option key={l} value={l}>
              {LANG_SHORT[l]} — {LANG_NAMES[l]}
            </option>
          ))}
        </select>
      )}
    </div>
  );
}
