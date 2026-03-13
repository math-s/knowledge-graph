import graphData from "../../../../../../public/data/graph.json";
import WorkPageClient from "./WorkPageClient";

// Extract work params from graph nodes: "patristic-work:augustine/confessions" -> { id: "augustine", workId: "confessions" }
const workParams = graphData.nodes
  .filter((n) => n.node_type === "patristic-work")
  .map((n) => {
    const full = n.id.replace("patristic-work:", "");
    const slashIdx = full.indexOf("/");
    return {
      id: full.slice(0, slashIdx),
      workId: full.slice(slashIdx + 1),
    };
  });

export function generateStaticParams() {
  return workParams;
}

export default function WorkPage() {
  return <WorkPageClient />;
}
