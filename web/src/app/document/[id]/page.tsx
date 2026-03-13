import graphData from "../../../../public/data/graph.json";
import DocumentPageClient from "./DocumentPageClient";

const documentNodeIds = graphData.nodes
  .filter((n) => n.node_type === "document")
  .map((n) => n.id.replace("document:", ""));

export function generateStaticParams() {
  return documentNodeIds.map((id) => ({ id }));
}

export default function DocumentPage() {
  return <DocumentPageClient />;
}
