import Link from "next/link";
import { notFound } from "next/navigation";
import paragraphsData from "../../../../public/data/paragraphs.json";
import { PART_COLORS, PART_SHORT_NAMES, SOURCE_COLORS, THEME_COLORS } from "@/lib/colors";
import ParagraphContent from "./ParagraphContent";

type ParagraphEntry = (typeof paragraphsData)[number];

const paragraphMap = new Map<number, ParagraphEntry>(
  paragraphsData.map((p) => [p.id, p]),
);

export function generateStaticParams() {
  return paragraphsData.map((p) => ({ id: String(p.id) }));
}

export default async function ParagraphPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const paraId = parseInt(id, 10);
  const paragraph = paragraphMap.get(paraId);

  if (!paragraph) {
    notFound();
  }

  const breadcrumb = [
    paragraph.part,
    paragraph.section,
    paragraph.chapter,
    paragraph.article,
  ].filter(Boolean);

  const partColor = PART_COLORS[paragraph.part] || "#999";

  const paraAny = paragraph as Record<string, unknown>;
  const bibleCitations: string[] = paraAny.bible_citations as string[] || [];
  const authorCitations: string[] = paraAny.author_citations as string[] || [];
  const documentCitations: string[] = paraAny.document_citations as string[] || [];
  const bibleCitationDetails: { book: string; reference: string }[] =
    paraAny.bible_citation_details as { book: string; reference: string }[] || [];
  const documentCitationDetails: { document: string; section: string }[] =
    paraAny.document_citation_details as { document: string; section: string }[] || [];
  const themes: string[] = paraAny.themes as string[] || [];

  return (
    <div className="mx-auto max-w-3xl px-6 py-12">
      {/* Navigation */}
      <div className="mb-6 flex items-center justify-between text-sm">
        <div className="flex gap-3">
          <Link
            href="/"
            className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300"
          >
            Home
          </Link>
          <Link
            href="/structure"
            className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300"
          >
            Structure
          </Link>
          <Link
            href="/graph"
            className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300"
          >
            Graph
          </Link>
        </div>
        <div className="flex gap-2">
          {paraId > 1 && (
            <Link
              href={`/paragraph/${paraId - 1}`}
              className="text-zinc-500 hover:text-zinc-700"
            >
              &larr; {paraId - 1}
            </Link>
          )}
          {paragraphMap.has(paraId + 1) && (
            <Link
              href={`/paragraph/${paraId + 1}`}
              className="text-zinc-500 hover:text-zinc-700"
            >
              {paraId + 1} &rarr;
            </Link>
          )}
        </div>
      </div>

      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center gap-3">
          <span
            className="inline-block h-3 w-3 rounded-full"
            style={{ backgroundColor: partColor }}
          />
          <h1 className="text-2xl font-bold">CCC {paraId}</h1>
        </div>
        {breadcrumb.length > 0 && (
          <div className="mt-2 text-sm text-zinc-500">
            {breadcrumb.map((item, i) => (
              <span key={i}>
                {i > 0 && <span className="mx-1">&rsaquo;</span>}
                {PART_SHORT_NAMES[item] || item}
              </span>
            ))}
          </div>
        )}
      </div>

      {/* Theme badges */}
      {themes.length > 0 && (
        <div className="mb-4 flex flex-wrap gap-1.5">
          {themes.map((theme) => (
            <span
              key={theme}
              className="rounded-full px-2.5 py-0.5 text-xs font-medium text-white"
              style={{ backgroundColor: THEME_COLORS[theme] || "#999" }}
            >
              {theme}
            </span>
          ))}
        </div>
      )}

      {/* Text + Footnotes (client component for bilingual toggle) */}
      <ParagraphContent
        text={paragraph.text}
        footnotes={paragraph.footnotes}
      />

      {/* Bible citations */}
      {bibleCitations.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-2 text-sm font-semibold uppercase text-zinc-500">
            Bible Citations ({bibleCitations.length})
          </h2>
          <div className="flex flex-wrap gap-2">
            {bibleCitations.map((bookId) => {
              const label = bookId.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
              return (
                <Link
                  key={bookId}
                  href={`/bible/${bookId}`}
                  className="rounded bg-green-50 px-2 py-1 text-sm font-medium text-green-800 hover:bg-green-100 dark:bg-green-900/30 dark:text-green-300 dark:hover:bg-green-900/50"
                  style={{ borderLeft: "3px solid #59A14F" }}
                >
                  {label}
                </Link>
              );
            })}
          </div>
          {/* Specific references */}
          {bibleCitationDetails.length > 0 && (
            <div className="mt-2 text-xs text-zinc-500">
              {bibleCitationDetails.map((d, i) => {
                const bookLabel = d.book.replace(/-/g, " ").replace(/\b\w/g, (c: string) => c.toUpperCase());
                return (
                  <span key={i}>
                    {i > 0 && ", "}
                    <Link href={`/bible/${d.book}`} className="hover:text-green-700 dark:hover:text-green-400">
                      {bookLabel} {d.reference}
                    </Link>
                  </span>
                );
              })}
            </div>
          )}
        </div>
      )}

      {/* Author citations */}
      {authorCitations.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-2 text-sm font-semibold uppercase text-zinc-500">
            Church Fathers ({authorCitations.length})
          </h2>
          <div className="flex flex-wrap gap-2">
            {authorCitations.map((authorId) => {
              const label = authorId.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
              return (
                <Link
                  key={authorId}
                  href={`/author/${authorId}`}
                  className="rounded bg-purple-50 px-2 py-1 text-sm font-medium text-purple-800 hover:bg-purple-100 dark:bg-purple-900/30 dark:text-purple-300 dark:hover:bg-purple-900/50"
                  style={{ borderLeft: "3px solid #B07AA1" }}
                >
                  {label}
                </Link>
              );
            })}
          </div>
        </div>
      )}

      {/* Document citations */}
      {documentCitations.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-2 text-sm font-semibold uppercase text-zinc-500">
            Ecclesiastical Documents ({documentCitations.length})
          </h2>
          <div className="flex flex-wrap gap-2">
            {documentCitations.map((docId) => {
              const label = docId.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
              return (
                <Link
                  key={docId}
                  href={`/document/${docId}`}
                  className="rounded bg-amber-50 px-2 py-1 text-sm font-medium text-amber-800 hover:bg-amber-100 dark:bg-amber-900/30 dark:text-amber-300 dark:hover:bg-amber-900/50"
                  style={{ borderLeft: `3px solid ${SOURCE_COLORS.document}` }}
                >
                  {label}
                </Link>
              );
            })}
          </div>
          {/* Specific sections */}
          {documentCitationDetails.length > 0 && (
            <div className="mt-2 text-xs text-zinc-500">
              {documentCitationDetails.map((d, i) => {
                const docLabel = d.document.replace(/-/g, " ").replace(/\b\w/g, (c: string) => c.toUpperCase());
                return (
                  <span key={i}>
                    {i > 0 && ", "}
                    <Link href={`/document/${d.document}`} className="hover:text-amber-700 dark:hover:text-amber-400">
                      {docLabel} {d.section}
                    </Link>
                  </span>
                );
              })}
            </div>
          )}
        </div>
      )}

      {/* Cross-references */}
      {paragraph.cross_references.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-2 text-sm font-semibold uppercase text-zinc-500">
            Cross-References ({paragraph.cross_references.length})
          </h2>
          <div className="flex flex-wrap gap-2">
            {paragraph.cross_references.map((refId) => {
              const refPara = paragraphMap.get(refId);
              const refColor = refPara
                ? PART_COLORS[refPara.part] || "#999"
                : "#999";
              return (
                <Link
                  key={refId}
                  href={`/paragraph/${refId}`}
                  className="rounded bg-zinc-100 px-2 py-1 text-sm font-medium text-zinc-700 hover:bg-zinc-200 dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
                  style={{ borderLeft: `3px solid ${refColor}` }}
                >
                  CCC {refId}
                </Link>
              );
            })}
          </div>
        </div>
      )}

      {/* View in graph */}
      <div className="border-t pt-4 dark:border-zinc-800">
        <Link
          href={`/graph`}
          className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400"
        >
          View in graph &rarr;
        </Link>
      </div>
    </div>
  );
}
