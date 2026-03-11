import Link from "next/link";
import { notFound } from "next/navigation";
import type { AuthorData } from "@/lib/types";
import graphData from "../../../../public/data/graph.json";
import authorSources from "../../../../public/data/sources-authors.json";
import paragraphsData from "../../../../public/data/paragraphs.json";
import { PART_COLORS, SOURCE_COLORS, PATRISTIC_HIERARCHY_COLORS } from "@/lib/colors";

type ParagraphEntry = (typeof paragraphsData)[number];

const sourceMap = authorSources as unknown as Record<string, AuthorData>;
const paragraphMap = new Map<number, ParagraphEntry>(
  paragraphsData.map((p) => [p.id, p]),
);

const authorNodeIds = graphData.nodes
  .filter((n) => n.node_type === "author")
  .map((n) => n.id.replace("author:", ""));

// Collect patristic work nodes from graph keyed by author
const workNodesByAuthor = new Map<string, { workId: string; label: string }[]>();
for (const n of graphData.nodes) {
  if (n.node_type === "patristic-work") {
    // patristic-work:augustine/confessions -> authorId=augustine, workId=confessions
    const fullId = n.id.replace("patristic-work:", "");
    const slashIdx = fullId.indexOf("/");
    if (slashIdx > 0) {
      const authorId = fullId.slice(0, slashIdx);
      const workId = fullId.slice(slashIdx + 1);
      if (!workNodesByAuthor.has(authorId)) workNodesByAuthor.set(authorId, []);
      workNodesByAuthor.get(authorId)!.push({ workId, label: n.label });
    }
  }
}

export function generateStaticParams() {
  return authorNodeIds.map((id) => ({ id }));
}

export default async function AuthorPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;

  if (!authorNodeIds.includes(id)) {
    notFound();
  }

  const author = sourceMap[id];
  const graphNode = graphData.nodes.find((n) => n.id === `author:${id}`);
  const label = author?.name || graphNode?.label || id.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  const works = author?.works || [];

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
            style={{ backgroundColor: SOURCE_COLORS.author }}
          />
          <h1 className="text-2xl font-bold">{label}</h1>
        </div>
        {author && (
          <div className="mt-2 flex items-center gap-3 text-sm text-zinc-500">
            {author.era && <span>{author.era}</span>}
            <span>
              Cited by {author.citing_paragraphs.length} paragraph
              {author.citing_paragraphs.length !== 1 ? "s" : ""}
            </span>
          </div>
        )}
      </div>

      {/* Works from graph (with full text) */}
      {(() => {
        const graphWorks = workNodesByAuthor.get(id) || [];
        if (graphWorks.length > 0) {
          return (
            <div className="mb-8">
              <h2 className="mb-3 text-sm font-semibold uppercase text-zinc-500">
                Works ({graphWorks.length})
              </h2>
              <div className="space-y-2">
                {graphWorks.map((gw) => (
                  <Link
                    key={gw.workId}
                    href={`/author/${id}/work/${gw.workId}`}
                    className="flex items-center gap-2 rounded border border-zinc-200 px-3 py-2 text-sm hover:bg-zinc-50 dark:border-zinc-700 dark:hover:bg-zinc-800/50"
                    style={{ borderLeft: `3px solid ${PATRISTIC_HIERARCHY_COLORS["patristic-work"]}` }}
                  >
                    <span className="font-medium text-zinc-800 dark:text-zinc-200">
                      {gw.label}
                    </span>
                    <span className="text-xs text-blue-600 dark:text-blue-400">&rarr;</span>
                  </Link>
                ))}
              </div>
            </div>
          );
        }
        // Fallback to legacy works list (external URLs only)
        if (works.length > 0) {
          return (
            <div className="mb-8">
              <h2 className="mb-3 text-sm font-semibold uppercase text-zinc-500">
                Works ({works.length})
              </h2>
              <ul className="space-y-1.5">
                {works.map((work, i) => (
                  <li key={i}>
                    {work.url ? (
                      <a
                        href={work.url}
                        className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400"
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        {work.title}
                      </a>
                    ) : (
                      <span className="text-sm text-zinc-700 dark:text-zinc-300">
                        {work.title}
                      </span>
                    )}
                  </li>
                ))}
              </ul>
            </div>
          );
        }
        return (
          <div className="mb-8 rounded border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900">
            Works list not yet fetched. Run the pipeline to populate.
          </div>
        );
      })()}

      {/* Citing Paragraphs */}
      {author && author.citing_paragraphs.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-2 text-sm font-semibold uppercase text-zinc-500">
            Citing Paragraphs ({author.citing_paragraphs.length})
          </h2>
          <div className="flex flex-wrap gap-2">
            {author.citing_paragraphs.map((paraId) => {
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
