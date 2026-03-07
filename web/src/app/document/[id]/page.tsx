import Link from "next/link";
import { notFound } from "next/navigation";
import type { DocumentData } from "@/lib/types";
import graphData from "../../../../public/data/graph.json";
import documentSources from "../../../../public/data/sources-documents.json";
import paragraphsData from "../../../../public/data/paragraphs.json";
import { PART_COLORS, SOURCE_COLORS } from "@/lib/colors";

type ParagraphEntry = (typeof paragraphsData)[number];

const sourceMap = documentSources as unknown as Record<string, DocumentData>;
const paragraphMap = new Map<number, ParagraphEntry>(
  paragraphsData.map((p) => [p.id, p]),
);

const documentNodeIds = graphData.nodes
  .filter((n) => n.node_type === "document")
  .map((n) => n.id.replace("document:", ""));

export function generateStaticParams() {
  return documentNodeIds.map((id) => ({ id }));
}

const CATEGORY_LABELS: Record<string, string> = {
  "vatican-ii": "Vatican II",
  encyclical: "Papal Document",
  "canon-law": "Canon Law",
  reference: "Reference Collection",
};

export default async function DocumentPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;

  if (!documentNodeIds.includes(id)) {
    notFound();
  }

  const doc = sourceMap[id];
  const graphNode = graphData.nodes.find((n) => n.id === `document:${id}`);
  const label = doc?.name || graphNode?.label || id.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());

  const sectionEntries = Object.entries(doc?.sections || {}).sort((a, b) => {
    const numA = parseInt(a[0]) || 0;
    const numB = parseInt(b[0]) || 0;
    return numA - numB;
  });

  return (
    <div className="mx-auto max-w-3xl px-6 py-12">
      {/* Navigation */}
      <div className="mb-6 flex items-center gap-3 text-sm">
        <Link href="/" className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300">Home</Link>
        <Link href="/structure" className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300">Structure</Link>
        <Link href="/graph" className="text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300">Graph</Link>
      </div>

      {/* Header */}
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
          </div>
        )}
      </div>

      {/* External link */}
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

      {/* Not fetchable notice */}
      {doc && !doc.fetchable && (
        <div className="mb-8 rounded border border-amber-200 bg-amber-50 p-4 text-sm text-amber-800 dark:border-amber-800 dark:bg-amber-900/20 dark:text-amber-300">
          This is a print reference collection. Section texts are not available
          online.
        </div>
      )}

      {/* Sections */}
      {sectionEntries.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-3 text-sm font-semibold uppercase text-zinc-500">
            Cited Sections ({sectionEntries.length})
          </h2>
          <div className="space-y-2">
            {sectionEntries.map(([num, text]) => (
              <div
                key={num}
                className="rounded border-l-4 bg-amber-50/50 p-3 dark:bg-amber-900/10"
                style={{ borderLeftColor: SOURCE_COLORS.document }}
              >
                <span className="mr-2 text-xs font-semibold text-amber-800 dark:text-amber-300">
                  {doc?.abbreviation || id} {num}
                </span>
                <span className="text-sm text-zinc-700 dark:text-zinc-300">
                  {typeof text === "string" && text.length > 500
                    ? text.slice(0, 500) + "..."
                    : text}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {(!doc || (doc.fetchable && sectionEntries.length === 0)) && (
        <div className="mb-8 rounded border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900">
          Section texts not yet fetched. Run the pipeline to populate.
        </div>
      )}

      {/* Citing Paragraphs */}
      {doc && doc.citing_paragraphs.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-2 text-sm font-semibold uppercase text-zinc-500">
            Citing Paragraphs ({doc.citing_paragraphs.length})
          </h2>
          <div className="flex flex-wrap gap-2">
            {doc.citing_paragraphs.map((paraId) => {
              const para = paragraphMap.get(paraId);
              const color = para ? PART_COLORS[para.part] || "#999" : "#999";
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

      {/* View in graph */}
      <div className="border-t pt-4 dark:border-zinc-800">
        <Link href="/graph" className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400">
          View in graph &rarr;
        </Link>
      </div>
    </div>
  );
}
