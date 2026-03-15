import type {
  AuthorData,
  AuthorMeta,
  BibleBookData,
  BibleBookMeta,
  BibleChapterData,
  DocumentData,
  DocumentMeta,
  DocumentSectionData,
  EntityDefinition,
  GraphData,
  ParagraphData,
  PatristicWorkData,
  SearchEntry,
  ThemeDefinition,
  TopicDefinition,
} from "./types";

const BASE_PATH = process.env.NEXT_PUBLIC_BASE_PATH || "";

export async function fetchGraphData(): Promise<GraphData> {
  const res = await fetch(`${BASE_PATH}/data/graph.json`);
  return res.json();
}

export async function fetchParagraphs(): Promise<ParagraphData[]> {
  const res = await fetch(`${BASE_PATH}/data/paragraphs.json`);
  return res.json();
}

export async function fetchSearchIndex(): Promise<SearchEntry[]> {
  const res = await fetch(`${BASE_PATH}/data/search-index.json`);
  return res.json();
}

export async function fetchThemes(): Promise<Record<string, ThemeDefinition>> {
  const res = await fetch(`${BASE_PATH}/data/themes.json`);
  return res.json();
}

export async function fetchEntities(): Promise<EntityDefinition[]> {
  const res = await fetch(`${BASE_PATH}/data/entities.json`);
  if (!res.ok) return [];
  return res.json();
}

export async function fetchTopics(): Promise<TopicDefinition[]> {
  const res = await fetch(`${BASE_PATH}/data/topics.json`);
  if (!res.ok) return [];
  return res.json();
}

export async function fetchBibleSources(): Promise<Record<string, BibleBookData>> {
  const res = await fetch(`${BASE_PATH}/data/sources-bible.json`);
  return res.json();
}

/** Fetch lightweight Bible book metadata (no verse text). */
export async function fetchBibleMeta(): Promise<Record<string, BibleBookMeta>> {
  const res = await fetch(`${BASE_PATH}/data/sources-bible-meta.json`);
  if (!res.ok) {
    // Fallback to legacy sources-bible.json
    return fetchBibleSources() as unknown as Promise<Record<string, BibleBookMeta>>;
  }
  return res.json();
}

/** Fetch verse data for a specific Bible book (lazy-loaded). */
export async function fetchBibleBookVerses(bookId: string): Promise<BibleChapterData[] | null> {
  const res = await fetch(`${BASE_PATH}/data/sources-bible-verses/${bookId}.json`);
  if (!res.ok) return null;
  return res.json();
}

export async function fetchDocumentSources(): Promise<Record<string, DocumentData>> {
  const res = await fetch(`${BASE_PATH}/data/sources-documents.json`);
  return res.json();
}

/** Fetch lightweight document metadata (no section text). */
export async function fetchDocumentMeta(): Promise<Record<string, DocumentMeta>> {
  const res = await fetch(`${BASE_PATH}/data/sources-documents-meta.json`);
  if (!res.ok) {
    // Fallback to legacy sources-documents.json
    return fetchDocumentSources() as unknown as Promise<Record<string, DocumentMeta>>;
  }
  return res.json();
}

/** Fetch section data for a specific document (lazy-loaded). */
export async function fetchDocumentSections(docId: string): Promise<DocumentSectionData | null> {
  const res = await fetch(`${BASE_PATH}/data/sources-documents-sections/${docId}.json`);
  if (!res.ok) return null;
  return res.json();
}

export async function fetchAuthorSources(): Promise<Record<string, AuthorData>> {
  const res = await fetch(`${BASE_PATH}/data/sources-authors.json`);
  return res.json();
}

/** Fetch lightweight author metadata (no work text). */
export async function fetchAuthorMeta(): Promise<Record<string, AuthorMeta>> {
  const res = await fetch(`${BASE_PATH}/data/sources-authors-meta.json`);
  if (!res.ok) {
    // Fallback to legacy sources-authors.json
    return fetchAuthorSources() as unknown as Promise<Record<string, AuthorMeta>>;
  }
  return res.json();
}

/** Fetch work data for a specific author (lazy-loaded). */
export async function fetchAuthorWorks(authorId: string): Promise<PatristicWorkData[] | null> {
  const res = await fetch(`${BASE_PATH}/data/sources-authors-works/${authorId}.json`);
  if (!res.ok) return null;
  return res.json();
}
