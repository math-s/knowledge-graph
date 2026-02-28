export interface GraphNode {
  id: string;
  label: string;
  node_type: "paragraph" | "structure";
  x: number;
  y: number;
  size: number;
  color: string;
  part: string;
  degree: number;
  community: number;
}

export interface GraphEdge {
  source: string;
  target: string;
  edge_type: "cross_reference" | "belongs_to" | "child_of";
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface ParagraphData {
  id: number;
  text: string;
  footnotes: string[];
  cross_references: number[];
  part: string;
  section: string;
  chapter: string;
  article: string;
}

export interface SearchEntry {
  id: number;
  text: string;
  part: string;
  section: string;
  chapter: string;
  article: string;
}
