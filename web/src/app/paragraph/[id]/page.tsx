import Link from "next/link";
import { notFound } from "next/navigation";
import paragraphsData from "../../../../public/data/paragraphs.json";
import { PART_COLORS, PART_SHORT_NAMES } from "@/lib/colors";

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

      {/* Text */}
      <div className="mb-8 text-base leading-relaxed text-zinc-800 dark:text-zinc-200">
        {paragraph.text}
      </div>

      {/* Footnotes */}
      {paragraph.footnotes.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-2 text-sm font-semibold uppercase text-zinc-500">
            Footnotes
          </h2>
          <ul className="space-y-1 text-sm text-zinc-600 dark:text-zinc-400">
            {paragraph.footnotes.map((fn, i) => (
              <li key={i} className="border-l-2 border-zinc-200 pl-3 dark:border-zinc-700">
                {fn}
              </li>
            ))}
          </ul>
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
