export interface GraphNode {
  id: string;
  label: string;
  node_type: "paragraph" | "structure" | "bible" | "author" | "document";
  x: number;
  y: number;
  size: number;
  color: string;
  part: string;
  degree: number;
  community: number;
  themes: string[];
}

export interface GraphEdge {
  source: string;
  target: string;
  edge_type: "cross_reference" | "belongs_to" | "child_of" | "cites";
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
  bible_citations: string[];
  author_citations: string[];
  document_citations: string[];
  themes: string[];
  part: string;
  section: string;
  chapter: string;
  article: string;
}

export interface SearchEntry {
  id: number | string;
  text: string;
  themes: string;
  part: string;
  section: string;
  chapter: string;
  article: string;
}

export interface ThemeDefinition {
  label: string;
  count: number;
}
