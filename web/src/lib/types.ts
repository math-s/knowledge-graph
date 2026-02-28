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
  edge_type: "cross_reference" | "belongs_to" | "child_of" | "cites" | "shared_theme";
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export type Lang = "en" | "pt";
export type BilingualStr = { en: string; pt: string };
export type BilingualArr = { en: string[]; pt: string[] };

/** Resolve a bilingual field to a single string for the given language. */
export function t(value: BilingualStr | string, lang: Lang = "en"): string {
  if (typeof value === "string") return value;
  return value[lang] || value.en || "";
}

/** Resolve a bilingual array field. */
export function tArr(value: BilingualArr | string[], lang: Lang = "en"): string[] {
  if (Array.isArray(value)) return value;
  return value[lang] || value.en || [];
}

export interface ParagraphData {
  id: number;
  text: string | BilingualStr;
  footnotes: string[] | BilingualArr;
  cross_references: number[];
  bible_citations: string[];
  author_citations: string[];
  document_citations: string[];
  themes: string[];
  part: string | BilingualStr;
  section: string | BilingualStr;
  chapter: string | BilingualStr;
  article: string | BilingualStr;
}

export interface SearchEntry {
  id: number | string;
  text: string;
  text_pt?: string;
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
