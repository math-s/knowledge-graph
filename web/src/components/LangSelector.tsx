"use client";

import { useLang } from "@/lib/LangContext";
import { type Lang, LANG_NAMES, LANG_SHORT } from "@/lib/types";

const LANGS: Lang[] = ["la", "en", "pt", "el"];

export default function LangSelector() {
  const { lang, setLang } = useLang();

  return (
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
  );
}
