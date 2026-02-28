"use client";

import { useLang } from "@/lib/LangContext";
import type { BilingualStr, BilingualArr } from "@/lib/types";
import { t, tArr } from "@/lib/types";

interface ParagraphContentProps {
  text: string | BilingualStr;
  footnotes: string[] | BilingualArr;
}

export default function ParagraphContent({
  text,
  footnotes,
}: ParagraphContentProps) {
  const { lang, setLang } = useLang();

  const hasPt = typeof text === "object" && !!text.pt;
  const resolvedFootnotes = tArr(footnotes, lang);

  return (
    <>
      {/* Language toggle */}
      {hasPt && (
        <div className="mb-4 flex gap-1 text-sm">
          <button
            onClick={() => setLang("en")}
            className={`rounded px-3 py-1 ${lang === "en" ? "bg-zinc-200 font-semibold dark:bg-zinc-700" : "text-zinc-500 hover:bg-zinc-100 dark:hover:bg-zinc-800"}`}
          >
            English
          </button>
          <button
            onClick={() => setLang("pt")}
            className={`rounded px-3 py-1 ${lang === "pt" ? "bg-zinc-200 font-semibold dark:bg-zinc-700" : "text-zinc-500 hover:bg-zinc-100 dark:hover:bg-zinc-800"}`}
          >
            Português
          </button>
        </div>
      )}

      {/* Text */}
      <div className="mb-8 text-base leading-relaxed text-zinc-800 dark:text-zinc-200">
        {t(text, lang)}
      </div>

      {/* Footnotes */}
      {resolvedFootnotes.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-2 text-sm font-semibold uppercase text-zinc-500">
            {lang === "pt" ? "Notas" : "Footnotes"}
          </h2>
          <ul className="space-y-1 text-sm text-zinc-600 dark:text-zinc-400">
            {resolvedFootnotes.map((fn, i) => (
              <li
                key={i}
                className="border-l-2 border-zinc-200 pl-3 dark:border-zinc-700"
              >
                {fn}
              </li>
            ))}
          </ul>
        </div>
      )}
    </>
  );
}
