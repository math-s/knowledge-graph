import Link from "next/link";
import graphData from "../../public/data/graph.json";

export default function Home() {
  const nodeCount = graphData.nodes.length;
  const edgeCount = graphData.edges.length;
  const paragraphCount = graphData.nodes.filter(
    (n) => n.node_type === "paragraph",
  ).length;
  const crossRefCount = graphData.edges.filter(
    (e) => e.edge_type === "cross_reference",
  ).length;
  const bibleBookCount = graphData.nodes.filter(
    (n) => n.node_type === "bible",
  ).length;
  const documentCount = graphData.nodes.filter(
    (n) => n.node_type === "document",
  ).length;
  const authorCount = graphData.nodes.filter(
    (n) => n.node_type === "author",
  ).length;

  return (
    <div className="flex min-h-screen flex-col items-center justify-center bg-zinc-50 px-6 dark:bg-zinc-950">
      <main className="max-w-2xl text-center">
        <h1 className="mb-4 text-4xl font-bold tracking-tight text-zinc-900 dark:text-white">
          Catechism Knowledge Graph
        </h1>
        <p className="mb-8 text-lg leading-relaxed text-zinc-600 dark:text-zinc-400">
          An interactive visualization of the{" "}
          <strong>Catechism of the Catholic Church</strong> &mdash; {paragraphCount.toLocaleString()}{" "}
          paragraphs connected by {crossRefCount.toLocaleString()} cross-references, organized
          across 4 Parts of Catholic doctrine.
        </p>

        <div className="mb-10 grid grid-cols-2 gap-4 sm:grid-cols-4">
          {[
            { label: "Paragraphs", value: paragraphCount.toLocaleString() },
            { label: "Cross-refs", value: crossRefCount.toLocaleString() },
            { label: "Bible books", value: bibleBookCount.toLocaleString() },
            { label: "Documents", value: documentCount.toLocaleString() },
            { label: "Church Fathers", value: authorCount.toLocaleString() },
            { label: "Total nodes", value: nodeCount.toLocaleString() },
            { label: "Total edges", value: edgeCount.toLocaleString() },
          ].map((stat) => (
            <div
              key={stat.label}
              className="rounded-lg border bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900"
            >
              <div className="text-2xl font-bold text-zinc-900 dark:text-white">
                {stat.value}
              </div>
              <div className="text-xs text-zinc-500">{stat.label}</div>
            </div>
          ))}
        </div>

        <div className="flex flex-col items-center gap-3 sm:flex-row sm:justify-center">
          <Link
            href="/graph"
            className="rounded-lg bg-zinc-900 px-6 py-3 font-medium text-white transition hover:bg-zinc-800 dark:bg-white dark:text-zinc-900 dark:hover:bg-zinc-200"
          >
            Explore the Graph
          </Link>
          <Link
            href="/structure"
            className="rounded-lg border px-6 py-3 font-medium text-zinc-700 transition hover:bg-zinc-100 dark:border-zinc-700 dark:text-zinc-300 dark:hover:bg-zinc-900"
          >
            Browse by Structure
          </Link>
        </div>

        <p className="mt-12 text-xs text-zinc-400">
          Data from the{" "}
          <a
            href="https://www.vatican.va/archive/ENG0015/_INDEX.HTM"
            className="underline hover:text-zinc-600"
            target="_blank"
            rel="noopener noreferrer"
          >
            Vatican.va
          </a>{" "}
          edition. Cross-references extracted from shared footnote citations.
        </p>
      </main>
    </div>
  );
}
