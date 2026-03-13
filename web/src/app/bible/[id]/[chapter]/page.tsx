import graphData from "../../../../../public/data/graph.json";
import BibleChapterClient from "./BibleChapterClient";

// Extract chapter params from graph nodes: "bible-chapter:matthew-5" -> { id: "matthew", chapter: "5" }
const chapterParams = graphData.nodes
  .filter((n) => n.node_type === "bible-chapter")
  .map((n) => {
    const full = n.id.replace("bible-chapter:", "");
    const dashIdx = full.lastIndexOf("-");
    return {
      id: full.slice(0, dashIdx),
      chapter: full.slice(dashIdx + 1),
    };
  });

export function generateStaticParams() {
  return chapterParams;
}

export default function BibleChapterPage() {
  return <BibleChapterClient />;
}
