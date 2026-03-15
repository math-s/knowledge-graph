import { apiFetch } from "./api";
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
  MultiLangText,
  PatristicWorkData,
  ThemeDefinition,
  TopicDefinition,
} from "./types";

// -- Graph metadata --

export async function fetchThemes(): Promise<Record<string, ThemeDefinition>> {
  const arr = await apiFetch<{ id: string; label: string; count: number }[]>("/graph/themes");
  const record: Record<string, ThemeDefinition> = {};
  for (const t of arr) {
    record[t.id] = { label: t.label, count: t.count };
  }
  return record;
}

export async function fetchEntities(): Promise<EntityDefinition[]> {
  return apiFetch<EntityDefinition[]>("/graph/entities");
}

export async function fetchTopics(): Promise<TopicDefinition[]> {
  return apiFetch<TopicDefinition[]>("/graph/topics");
}

// -- Bible --

export async function fetchBibleSources(): Promise<Record<string, BibleBookData>> {
  const arr = await apiFetch<BibleBookData[]>("/bible/books");
  const record: Record<string, BibleBookData> = {};
  for (const b of arr) {
    record[b.id] = b;
  }
  return record;
}

export async function fetchBibleMeta(): Promise<Record<string, BibleBookMeta>> {
  const arr = await apiFetch<BibleBookMeta[]>("/bible/books");
  const record: Record<string, BibleBookMeta> = {};
  for (const b of arr) {
    record[b.id] = b;
  }
  return record;
}

export async function fetchBibleBookVerses(bookId: string): Promise<BibleChapterData[] | null> {
  // Get book metadata to know how many chapters exist
  const book = await apiFetch<{ total_chapters: number }>(`/bible/books/${encodeURIComponent(bookId)}`);
  if (!book || !book.total_chapters) return null;

  // Fetch all chapters in parallel
  const chapterPromises = Array.from({ length: book.total_chapters }, (_, i) =>
    apiFetch<{ book_id: string; chapter: number; verses: { verse: number; text: MultiLangText }[] }>(
      `/bible/books/${encodeURIComponent(bookId)}/chapters/${i + 1}`,
    ).catch(() => null),
  );
  const results = await Promise.all(chapterPromises);

  const chapters: BibleChapterData[] = [];
  for (const r of results) {
    if (!r) continue;
    const versesRecord: Record<number, MultiLangText> = {};
    for (const v of r.verses) {
      versesRecord[v.verse] = v.text;
    }
    chapters.push({
      book_id: r.book_id,
      chapter: r.chapter,
      verses: versesRecord,
    });
  }
  return chapters.length > 0 ? chapters : null;
}

// -- Documents --

export async function fetchDocumentSources(): Promise<Record<string, DocumentData>> {
  const arr = await apiFetch<DocumentData[]>("/documents");
  const record: Record<string, DocumentData> = {};
  for (const d of arr) {
    record[d.id] = d;
  }
  return record;
}

export async function fetchDocumentMeta(): Promise<Record<string, DocumentMeta>> {
  const arr = await apiFetch<DocumentMeta[]>("/documents");
  const record: Record<string, DocumentMeta> = {};
  for (const d of arr) {
    record[d.id] = d;
  }
  return record;
}

export async function fetchDocumentSections(docId: string): Promise<DocumentSectionData | null> {
  const data = await apiFetch<{ sections: DocumentSectionData }>(`/documents/${encodeURIComponent(docId)}/sections`);
  return data.sections || null;
}

// -- Authors --

export async function fetchAuthorSources(): Promise<Record<string, AuthorData>> {
  const arr = await apiFetch<AuthorData[]>("/authors");
  const record: Record<string, AuthorData> = {};
  for (const a of arr) {
    record[a.id] = a;
  }
  return record;
}

export async function fetchAuthorMeta(): Promise<Record<string, AuthorMeta>> {
  const arr = await apiFetch<AuthorMeta[]>("/authors");
  const record: Record<string, AuthorMeta> = {};
  for (const a of arr) {
    record[a.id] = a;
  }
  return record;
}

export async function fetchAuthorWorks(authorId: string): Promise<PatristicWorkData[] | null> {
  const data = await apiFetch<{ works: PatristicWorkData[] }>(`/authors/${encodeURIComponent(authorId)}/works`);
  return data.works || null;
}

// -- Paragraphs (lightweight) --

export async function fetchParagraphParts(): Promise<{ id: number; part: string }[]> {
  return apiFetch<{ id: number; part: string }[]>("/paragraphs/parts");
}
