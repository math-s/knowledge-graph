"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import type { DocumentMeta, DocumentSectionData, Lang, MultiLangText } from "@/lib/types";
import { resolveLang, resolvedLangCode, LANG_SHORT, LANG_NAMES } from "@/lib/types";
import { fetchDocumentMeta, fetchDocumentSections, fetchParagraphs } from "@/lib/graph-data";
import { useLang } from "@/lib/LangContext";
import { PART_COLORS, SOURCE_COLORS, DOCUMENT_HIERARCHY_COLORS } from "@/lib/colors";

const CATEGORY_LABELS: Record<string, string> = {
  "vatican-ii": "Vatican II",
  encyclical: "Papal Document",
  "canon-law": "Canon Law",
  reference: "Reference Collection",
};

const ALL_LANGS: Lang[] = ["la", "en", "pt", "el"];

export default function DocumentPageClient() {
  const params = useParams<{ id: string }>();
  const id = params.id;
  const { lang, setLang, sideBySide, compareLang, setCompareLang } = useLang();

  const [doc, setDoc] = useState<DocumentMeta | null>(null);
  const [sections, setSections] = useState<DocumentSectionData | null>(null);
  const [paragraphColors, setParagraphColors] = useState<Map<number, string>>(new Map());
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchDocumentMeta().then((meta) => {
      setDoc(meta[id] || null);
      setLoading(false);
    });
  }, [id]);

  useEffect(() => {
    if (doc && doc.section_count > 0) {
      fetchDocumentSections(id).then(setSections);
    }
  }, [id, doc]);

  useEffect(() => {
    if (doc && doc.citing_paragraphs.length > 0) {
      fetchParagraphs().then((paras) => {
        const colors = new Map<number, string>();
        for (const p of paras) {
          if (doc.citing_paragraphs.includes(p.id)) {
            const partStr = typeof p.part === "string" ? p.part : resolveLang(p.part, "en");
            colors.set(p.id, PART_COLORS[partStr] || "#999");
          }
        }
        setParagraphColors(colors);
      });
    }
  }, [doc]);

  if (loading) {
    return (
      <div className="mx-auto max-w-3xl px-6 py-12">
        <div className="text-sm text-zinc-400">Loading document...</div>
      </div>
    );
  }

  const label = doc?.name || id.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());

  const sectionEntries = sections
    ? Object.entries(sections).sort((a, b) => {
        const numA = parseInt(a[0]) || 0;
        const numB = parseInt(b[0]) || 0;
        return numA - numB;
      })
    : [];

  const availableLangs = doc?.available_langs || ["en"];
  const effectiveCompareLang =
    availableLangs.includes(compareLang) && compareLang !== lang
      ? compareLang
      : (availableLangs.find((l) => l !== lang) as Lang) || lang;

  const showSideBySide = sideBySide && availableLangs.length > 1 && sectionEntries.length > 0;

  return (
    <div className={`mx-auto px-6 py-12 ${showSideBySide ? "max-w-5xl" : "max-w-3xl"}`}>
      <div className="mb-6 flex items-center gap-3 text-sm">
        <Link href="/" className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300">Home</Link>
        <Link href="/structure" className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300">Structure</Link>
        <Link href="/graph" className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300">Graph</Link>
      </div>

      <div className="mb-6">
        <div className="flex items-center gap-3">
          <span
            className="inline-block h-3 w-3 rounded-full"
            style={{ backgroundColor: SOURCE_COLORS.document }}
          />
          <h1 className="text-2xl font-bold">{label}</h1>
        </div>
        {doc && (
          <div className="mt-2 flex flex-wrap items-center gap-3 text-sm text-zinc-500">
            <span className="rounded bg-amber-50 px-2 py-0.5 text-xs font-medium text-amber-800 dark:bg-amber-900/30 dark:text-amber-300">
              {doc.abbreviation}
            </span>
            <span>{CATEGORY_LABELS[doc.category] || doc.category}</span>
            <span>
              Cited by {doc.citing_paragraphs.length} paragraph
              {doc.citing_paragraphs.length !== 1 ? "s" : ""}
            </span>
            {doc.section_count > 0 && (
              <span>{doc.section_count} sections</span>
            )}
          </div>
        )}
      </div>

      {availableLangs.length > 1 && (
        <div className="mb-6 flex items-center gap-2">
          <span className="text-xs text-zinc-500">Language:</span>
          <div className="flex gap-1">
            {ALL_LANGS.filter((l) => availableLangs.includes(l)).map((l) => (
              <button
                key={l}
                onClick={() => setLang(l)}
                className={`rounded px-2 py-0.5 text-xs ${
                  lang === l
                    ? "bg-zinc-200 font-semibold dark:bg-zinc-700"
                    : "text-zinc-500 hover:bg-zinc-100 dark:hover:bg-zinc-800"
                }`}
              >
                {LANG_SHORT[l]}
              </button>
            ))}
          </div>
          {showSideBySide && (
            <>
              <span className="text-xs text-zinc-400">vs</span>
              <select
                value={effectiveCompareLang}
                onChange={(e) => setCompareLang(e.target.value as Lang)}
                className="rounded border border-zinc-300 bg-white px-1.5 py-0.5 text-xs dark:border-zinc-600 dark:bg-zinc-800"
              >
                {ALL_LANGS.filter((l) => availableLangs.includes(l) && l !== lang).map((l) => (
                  <option key={l} value={l}>
                    {LANG_SHORT[l]}
                  </option>
                ))}
              </select>
            </>
          )}
        </div>
      )}

      {doc?.source_url && (
        <div className="mb-6">
          <a
            href={doc.source_url}
            className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400"
            target="_blank"
            rel="noopener noreferrer"
          >
            Read full text on Vatican.va &rarr;
          </a>
        </div>
      )}

      {doc && !doc.fetchable && (
        <div className="mb-8 rounded border border-amber-200 bg-amber-50 p-4 text-sm text-amber-800 dark:border-amber-800 dark:bg-amber-900/20 dark:text-amber-300">
          This is a print reference collection. Section texts are not available online.
        </div>
      )}

      {sectionEntries.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-3 text-sm font-semibold uppercase text-zinc-500">
            Sections ({sectionEntries.length})
          </h2>
          <div className="space-y-2">
            {sectionEntries.map(([num, textData]) => {
              if (showSideBySide) {
                const primaryText = typeof textData === "string"
                  ? textData
                  : (textData as MultiLangText)[lang] || "";
                const compareText = typeof textData === "string"
                  ? ""
                  : (textData as MultiLangText)[effectiveCompareLang] || "";

                return (
                  <div
                    key={num}
                    className="rounded border-l-4 bg-amber-50/50 p-3 dark:bg-amber-900/10"
                    style={{ borderLeftColor: DOCUMENT_HIERARCHY_COLORS["document-section"] }}
                  >
                    <div className="mb-2 text-xs font-semibold text-amber-800 dark:text-amber-300">
                      {doc?.abbreviation || id} {num}
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <div className="mb-1 text-[10px] font-semibold uppercase text-zinc-400">
                          {LANG_NAMES[lang]}
                        </div>
                        <span className="text-sm text-zinc-700 dark:text-zinc-300">
                          {primaryText
                            ? primaryText.length > 500 ? primaryText.slice(0, 500) + "..." : primaryText
                            : <span className="italic text-zinc-400">Not available</span>}
                        </span>
                      </div>
                      <div>
                        <div className="mb-1 text-[10px] font-semibold uppercase text-zinc-400">
                          {LANG_NAMES[effectiveCompareLang]}
                        </div>
                        <span className="text-sm text-zinc-600 dark:text-zinc-400">
                          {compareText
                            ? compareText.length > 500 ? compareText.slice(0, 500) + "..." : compareText
                            : <span className="italic text-zinc-400">Not available</span>}
                        </span>
                      </div>
                    </div>
                  </div>
                );
              }

              const text = typeof textData === "string"
                ? textData
                : resolveLang(textData as MultiLangText, lang);
              const actualLang = typeof textData === "string"
                ? null
                : resolvedLangCode(textData as MultiLangText, lang);
              const isFallback = actualLang !== null && actualLang !== lang;

              return (
                <div
                  key={num}
                  className="rounded border-l-4 bg-amber-50/50 p-3 dark:bg-amber-900/10"
                  style={{ borderLeftColor: DOCUMENT_HIERARCHY_COLORS["document-section"] }}
                >
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-xs font-semibold text-amber-800 dark:text-amber-300">
                      {doc?.abbreviation || id} {num}
                    </span>
                    {isFallback && (
                      <span className="rounded bg-zinc-200 px-1 py-0.5 text-[10px] font-medium text-zinc-600 dark:bg-zinc-700 dark:text-zinc-400">
                        {LANG_SHORT[actualLang]}
                      </span>
                    )}
                  </div>
                  <span className="text-sm text-zinc-700 dark:text-zinc-300">
                    {text.length > 500 ? text.slice(0, 500) + "..." : text}
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {sections === null && doc && doc.fetchable && doc.section_count > 0 && (
        <div className="mb-8 text-sm text-zinc-400">Loading sections...</div>
      )}

      {doc && doc.fetchable && doc.section_count === 0 && (
        <div className="mb-8 rounded border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900">
          Section texts not yet fetched. Run the pipeline to populate.
        </div>
      )}

      {!doc && (
        <div className="mb-8 rounded border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900">
          Document not found.
        </div>
      )}

      {doc && doc.citing_paragraphs.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-2 text-sm font-semibold uppercase text-zinc-500">
            Citing Paragraphs ({doc.citing_paragraphs.length})
          </h2>
          <div className="flex flex-wrap gap-2">
            {doc.citing_paragraphs.map((paraId) => {
              const color = paragraphColors.get(paraId) || "#999";
              return (
                <Link
                  key={paraId}
                  href={`/paragraph/${paraId}`}
                  className="rounded bg-zinc-100 px-2 py-1 text-sm font-medium text-zinc-700 hover:bg-zinc-200 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                  style={{ borderLeft: `3px solid ${color}` }}
                >
                  CCC {paraId}
                </Link>
              );
            })}
          </div>
        </div>
      )}

      <div className="border-t pt-4 dark:border-zinc-800">
        <Link href="/graph" className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400">
          View in graph &rarr;
        </Link>
      </div>
    </div>
  );
}
