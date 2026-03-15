"use client";

import { useLang } from "@/lib/LangContext";
import type { MultiLangText } from "@/lib/types";
import { resolveLang } from "@/lib/types";
import LangSelector from "@/components/LangSelector";

interface ParagraphContentProps {
  text: string | MultiLangText;
  footnotes: string[];
}

export default function ParagraphContent({
  text,
  footnotes,
}: ParagraphContentProps) {
  const { lang } = useLang();

  // Check if multilingual text is available (more than just English)
  const hasMultipleLangs =
    typeof text === "object" &&
    Object.keys(text).filter((k) => text[k as keyof typeof text]).length > 1;

  const resolvedText =
    typeof text === "string" ? text : resolveLang(text, lang);

  return (
    <>
      {/* Language selector */}
      {hasMultipleLangs && (
        <div className="mb-4">
          <LangSelector />
        </div>
      )}

      {/* Text */}
      <div className="mb-8 text-base leading-relaxed text-zinc-800 dark:text-zinc-200">
        {resolvedText}
      </div>

      {/* Footnotes */}
      {footnotes.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-2 text-sm font-semibold uppercase text-zinc-500">
            Footnotes
          </h2>
          <ul className="space-y-1 text-sm text-zinc-600 dark:text-zinc-400">
            {footnotes.map((fn, i) => (
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
