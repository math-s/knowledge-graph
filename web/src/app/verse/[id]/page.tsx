import graphData from "../../../../public/data/graph.json";
import VersePageClient from "./VersePageClient";

// Extract verse IDs from graph nodes: "bible-verse:matthew-5:3" -> { id: "matthew-5:3" }
const verseIds = graphData.nodes
  .filter((n) => n.node_type === "bible-verse")
  .map((n) => ({ id: n.id.replace("bible-verse:", "") }));

export function generateStaticParams() {
  return verseIds;
}

export default function VersePage() {
  return <VersePageClient />;
}
