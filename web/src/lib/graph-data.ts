import type { GraphData, ParagraphData, SearchEntry } from "./types";

const BASE_PATH = process.env.NODE_ENV === "production" ? "/knowledge-graph" : "";

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
