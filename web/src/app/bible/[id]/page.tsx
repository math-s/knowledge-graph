import Link from "next/link";
import { notFound } from "next/navigation";
import type { BibleBookData } from "@/lib/types";
import graphData from "../../../../public/data/graph.json";
import bibleSources from "../../../../public/data/sources-bible.json";
import paragraphsData from "../../../../public/data/paragraphs.json";
import { PART_COLORS, SOURCE_COLORS } from "@/lib/colors";

type ParagraphEntry = (typeof paragraphsData)[number];

const sourceMap = bibleSources as unknown as Record<string, BibleBookData>;
const paragraphMap = new Map<number, ParagraphEntry>(
  paragraphsData.map((p) => [p.id, p]),
);

// Generate params from graph nodes (always populated) rather than source JSON
// (which may be empty before the pipeline fetch step runs).
const bibleNodeIds = graphData.nodes
  .filter((n) => n.node_type === "bible-book")
  .map((n) => n.id.replace("bible-book:", ""));

export function generateStaticParams() {
  return bibleNodeIds.map((id) => ({ id }));
}

export default async function BiblePage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;

  if (!bibleNodeIds.includes(id)) {
    notFound();
  }

  const book = sourceMap[id];
  const graphNode = graphData.nodes.find((n) => n.id === `bible-book:${id}`);
  const label = book?.name || graphNode?.label || id.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());

  const verseEntries = Object.entries(book?.verses || {}).sort((a, b) => {
    const [chA, vA] = a[0].split(":").map(Number);
    const [chB, vB] = b[0].split(":").map(Number);
    return chA - chB || vA - vB;
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
            style={{ backgroundColor: SOURCE_COLORS.bible }}
          />
          <h1 className="text-2xl font-bold">{label}</h1>
        </div>
        {book && (
          <div className="mt-2 flex items-center gap-3 text-sm text-zinc-500">
            <span className="rounded bg-green-50 px-2 py-0.5 text-xs font-medium text-green-800 dark:bg-green-900/30 dark:text-green-300">
              {book.abbreviation}
            </span>
            <span className="capitalize">{book.testament} Testament</span>
            <span>
              Cited by {book.citing_paragraphs.length} paragraph
              {book.citing_paragraphs.length !== 1 ? "s" : ""}
            </span>
          </div>
        )}
      </div>

      {/* Cited Verses */}
      {verseEntries.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-3 text-sm font-semibold uppercase text-zinc-500">
            Cited Verses ({verseEntries.length})
          </h2>
          <div className="space-y-2">
            {verseEntries.map(([ref, text]) => (
              <div
                key={ref}
                className="rounded border-l-4 bg-green-50/50 p-3 dark:bg-green-900/10"
                style={{ borderLeftColor: SOURCE_COLORS.bible }}
              >
                <span className="mr-2 text-xs font-semibold text-green-800 dark:text-green-300">
                  {book?.abbreviation || id} {ref}
                </span>
                <span className="text-sm text-zinc-700 dark:text-zinc-300">
                  {text}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {verseEntries.length === 0 && (
        <div className="mb-8 rounded border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900">
          Verse texts not yet fetched. Run the pipeline to populate.
        </div>
      )}

      {/* Citing Paragraphs */}
      {book && book.citing_paragraphs.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-2 text-sm font-semibold uppercase text-zinc-500">
            Citing Paragraphs ({book.citing_paragraphs.length})
          </h2>
          <div className="flex flex-wrap gap-2">
            {book.citing_paragraphs.map((paraId) => {
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
