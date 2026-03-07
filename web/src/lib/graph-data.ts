import type {
  AuthorData,
  BibleBookData,
  DocumentData,
  GraphData,
  ParagraphData,
  SearchEntry,
  ThemeDefinition,
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

export async function fetchBibleSources(): Promise<Record<string, BibleBookData>> {
  const res = await fetch(`${BASE_PATH}/data/sources-bible.json`);
  return res.json();
}

export async function fetchDocumentSources(): Promise<Record<string, DocumentData>> {
  const res = await fetch(`${BASE_PATH}/data/sources-documents.json`);
  return res.json();
}

export async function fetchAuthorSources(): Promise<Record<string, AuthorData>> {
  const res = await fetch(`${BASE_PATH}/data/sources-authors.json`);
  return res.json();
}
