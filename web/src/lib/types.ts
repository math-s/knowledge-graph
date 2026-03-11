export interface GraphNode {
  id: string;
  label: string;
  node_type:
    | "paragraph"
    | "structure"
    | "bible"
    | "bible-testament"
    | "bible-book"
    | "bible-chapter"
    | "bible-verse"
    | "author"
    | "patristic-work"
    | "document";
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
  edge_type:
    | "cross_reference"
    | "belongs_to"
    | "child_of"
    | "cites"
    | "shared_theme"
    | "bible_cross_reference";
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

// ── Multi-language support ────────────────────────────────────────────────────

export type Lang = "la" | "en" | "pt" | "el";

/** Text stored as partial record of lang -> string. Not all languages available for all texts. */
export type MultiLangText = Partial<Record<Lang, string>>;

/** @deprecated Use MultiLangText instead */
export type BilingualStr = { en: string; pt: string };
/** @deprecated Use MultiLangText instead */
export type BilingualArr = { en: string[]; pt: string[] };

const FALLBACK_ORDER: Lang[] = ["la", "en", "pt", "el"];

/**
 * Resolve a MultiLangText to a single string for the given language.
 * Falls back: preferred -> la -> en -> pt -> el -> first available.
 */
export function resolveLang(
  text: MultiLangText | string | undefined,
  preferred: Lang = "en",
): string {
  if (!text) return "";
  if (typeof text === "string") return text;
  if (text[preferred]) return text[preferred]!;
  for (const lang of FALLBACK_ORDER) {
    if (text[lang]) return text[lang]!;
  }
  // Return first available
  for (const v of Object.values(text)) {
    if (v) return v;
  }
  return "";
}

/**
 * Get which language was actually resolved for a MultiLangText.
 * Returns the lang code that resolveLang would use, or null if empty.
 */
export function resolvedLangCode(
  text: MultiLangText | string | undefined,
  preferred: Lang = "en",
): Lang | null {
  if (!text || typeof text === "string") return null;
  if (text[preferred]) return preferred;
  for (const lang of FALLBACK_ORDER) {
    if (text[lang]) return lang;
  }
  return null;
}

/** Resolve a bilingual field to a single string for the given language.
 * @deprecated Use resolveLang instead */
export function t(
  value: BilingualStr | MultiLangText | string,
  lang: Lang = "en",
): string {
  if (typeof value === "string") return value;
  return resolveLang(value as MultiLangText, lang);
}

/** Resolve a bilingual array field.
 * @deprecated Use resolveLang with arrays instead */
export function tArr(
  value: BilingualArr | string[],
  lang: Lang = "en",
): string[] {
  if (Array.isArray(value)) return value;
  return value[lang as "en" | "pt"] || value.en || [];
}

// ── Data interfaces ───────────────────────────────────────────────────────────

export interface ParagraphData {
  id: number;
  text: string | MultiLangText;
  footnotes: string[] | BilingualArr;
  cross_references: number[];
  bible_citations: string[];
  author_citations: string[];
  document_citations: string[];
  bible_citation_details?: { book: string; reference: string }[];
  document_citation_details?: { document: string; section: string }[];
  themes: string[];
  part: string | MultiLangText;
  section: string | MultiLangText;
  chapter: string | MultiLangText;
  article: string | MultiLangText;
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

// ── Bible data ────────────────────────────────────────────────────────────────

export interface BibleBookData {
  id: string;
  name: string;
  abbreviation: string;
  testament: string;
  category?: string;
  citing_paragraphs: number[];
  verses: Record<string, string>;
  total_verses?: number;
}

export interface BibleBookMeta {
  id: string;
  name: string;
  abbreviation: string;
  testament: string;
  category: string;
  total_verses: number;
  total_chapters: number;
  citing_paragraphs: number[];
}

export interface BibleVerseData {
  text: MultiLangText;
}

export interface BibleChapterData {
  book_id: string;
  chapter: number;
  verses: Record<number, MultiLangText>;
}

// ── Document data ─────────────────────────────────────────────────────────────

export interface DocumentData {
  id: string;
  name: string;
  abbreviation: string;
  category: string;
  source_url: string;
  fetchable: boolean;
  citing_paragraphs: number[];
  sections: Record<string, string>;
}

// ── Author data ───────────────────────────────────────────────────────────────

export interface AuthorData {
  id: string;
  name: string;
  era: string;
  works: { title: string; url: string }[];
  citing_paragraphs: number[];
}

// ── Patristic work data ──────────────────────────────────────────────────────

export interface AuthorMeta {
  id: string;
  name: string;
  era: string;
  citing_paragraphs: number[];
  work_count: number;
  work_titles: string[];
}

export interface PatristicWorkData {
  id: string;
  title: string;
  source_url: string;
  chapter_count: number;
  chapters: PatristicChapterData[];
}

export interface PatristicChapterData {
  id: string;
  number: number;
  title: string;
  sections: PatristicSectionData[];
}

export interface PatristicSectionData {
  id: string;
  number: number;
  text: MultiLangText;
}

// ── Language display names ────────────────────────────────────────────────────

export const LANG_NAMES: Record<Lang, string> = {
  la: "Latina",
  en: "English",
  pt: "Português",
  el: "Ελληνικά",
};

export const LANG_SHORT: Record<Lang, string> = {
  la: "LA",
  en: "EN",
  pt: "PT",
  el: "EL",
};
