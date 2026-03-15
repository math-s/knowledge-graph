"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import Graph from "graphology";
import type { AuthorData, BibleBookData, DocumentData, EntityDefinition, ParagraphData, TopicDefinition } from "@/lib/types";
import { t, tArr, resolveLang } from "@/lib/types";
import { fetchAuthorSources, fetchBibleSources, fetchDocumentSources, fetchEntities, fetchParagraphs, fetchTopics } from "@/lib/graph-data";
import { useLang } from "@/lib/LangContext";
import { PART_SHORT_NAMES, SOURCE_COLORS, THEME_COLORS } from "@/lib/colors";
import LangSelector from "@/components/LangSelector";

interface GraphDetailPanelProps {
  nodeId: string;
  graph: Graph;
  onClose: () => void;
  onNavigate: (nodeId: string) => void;
  onThemeFilter: (themeId: string) => void;
  canGoBack: boolean;
  onGoBack: () => void;
}

function SourcePreview({
  nodeType,
  nodeId,
  label,
  bibleSources,
  documentSources,
  authorSources,
}: {
  nodeType: string;
  nodeId: string;
  label: string;
  bibleSources: Record<string, BibleBookData>;
  documentSources: Record<string, DocumentData>;
  authorSources: Record<string, AuthorData>;
}) {
  const sourceId = nodeId.split(":").slice(1).join(":");

  if (nodeType === "bible" || nodeType === "bible-book") {
    const book = bibleSources[sourceId];
    if (book) {
      const verseKeys = Object.keys(book.verses || {}).slice(0, 3);
      return (
        <div className="space-y-2 text-sm">
          <div className="text-zinc-500">
            <span className="capitalize">{book.testament} Testament</span>
            {" \u00b7 "}
            {book.citing_paragraphs.length} citing paragraphs
          </div>
          {verseKeys.length > 0 && (
            <div className="space-y-1">
              {verseKeys.map((ref) => (
                <div key={ref} className="rounded bg-green-50/50 p-2 text-xs dark:bg-green-900/10">
                  <span className="font-semibold text-green-800 dark:text-green-300">{book.abbreviation} {ref}</span>{" "}
                  <span className="text-zinc-600 dark:text-zinc-400">{book.verses[ref]?.slice(0, 120)}{(book.verses[ref]?.length || 0) > 120 ? "..." : ""}</span>
                </div>
              ))}
            </div>
          )}
          <Link href={`/bible/${sourceId}`} className="inline-block text-xs text-blue-600 hover:text-blue-800 dark:text-blue-400">
            View full details &rarr;
          </Link>
        </div>
      );
    }
    return <div className="text-sm text-zinc-500">Bible book: {label}</div>;
  }

  if (nodeType === "bible-testament") {
    return <div className="text-sm text-zinc-500">Bible: {label}</div>;
  }

  if (nodeType === "bible-chapter") {
    // Parse chapter node ID: "bible-chapter:matthew-5" -> bookId=matthew, ch=5
    const parts = sourceId.split("-");
    const chNum = parts.pop();
    const bookId = parts.join("-");
    return (
      <div className="space-y-2 text-sm">
        <div className="text-zinc-500">{label}</div>
        <Link href={`/bible/${bookId}/${chNum}`} className="inline-block text-xs text-blue-600 hover:text-blue-800 dark:text-blue-400">
          Read chapter &rarr;
        </Link>
      </div>
    );
  }

  if (nodeType === "bible-verse") {
    // Parse verse node ID: "bible-verse:matthew-5:3" -> bookId=matthew, ch=5, v=3
    return (
      <div className="space-y-2 text-sm">
        <div className="text-zinc-500">{label}</div>
        <Link href={`/verse/${sourceId}`} className="inline-block text-xs text-blue-600 hover:text-blue-800 dark:text-blue-400">
          View verse &rarr;
        </Link>
      </div>
    );
  }

  if (nodeType === "document") {
    const doc = documentSources[sourceId];
    if (doc) {
      const firstSection = Object.entries(doc.sections || {})[0];
      return (
        <div className="space-y-2 text-sm">
          <div className="text-zinc-500">
            <span className="rounded bg-amber-50 px-1.5 py-0.5 text-xs font-medium text-amber-800 dark:bg-amber-900/30 dark:text-amber-300">{doc.abbreviation}</span>
            {" \u00b7 "}
            {doc.citing_paragraphs.length} citing paragraphs
          </div>
          {firstSection && (() => {
            const sectionText = typeof firstSection[1] === "string" ? firstSection[1] : resolveLang(firstSection[1], "en");
            return (
              <div className="rounded bg-amber-50/50 p-2 text-xs dark:bg-amber-900/10">
                <span className="font-semibold text-amber-800 dark:text-amber-300">{doc.abbreviation} {firstSection[0]}</span>{" "}
                <span className="text-zinc-600 dark:text-zinc-400">{sectionText.slice(0, 150)}{sectionText.length > 150 ? "..." : ""}</span>
              </div>
            );
          })()}
          {!doc.fetchable && (
            <div className="text-xs text-amber-600 dark:text-amber-400">Print reference collection</div>
          )}
          <Link href={`/document/${sourceId}`} className="inline-block text-xs text-blue-600 hover:text-blue-800 dark:text-blue-400">
            View full details &rarr;
          </Link>
        </div>
      );
    }
    return <div className="text-sm text-zinc-500">Ecclesiastical document: {label}</div>;
  }

  if (nodeType === "author") {
    const author = authorSources[sourceId];
    if (author) {
      return (
        <div className="space-y-2 text-sm">
          <div className="text-zinc-500">
            {author.era && <span>{author.era}</span>}
            {author.era && " \u00b7 "}
            {author.citing_paragraphs.length} citing paragraphs
          </div>
          {author.works.length > 0 && (
            <div className="text-xs text-zinc-600 dark:text-zinc-400">
              Works: {author.works.slice(0, 3).map((w) => w.title).join(", ")}
              {author.works.length > 3 ? ` +${author.works.length - 3} more` : ""}
            </div>
          )}
          <Link href={`/author/${sourceId}`} className="inline-block text-xs text-blue-600 hover:text-blue-800 dark:text-blue-400">
            View full details &rarr;
          </Link>
        </div>
      );
    }
    return <div className="text-sm text-zinc-500">Patristic author: {label}</div>;
  }

  if (nodeType === "patristic-work") {
    // Parse work node ID: "patristic-work:augustine/confessions"
    const parts = sourceId.split("/");
    const authorId = parts[0];
    const workId = parts.slice(1).join("/");
    return (
      <div className="space-y-2 text-sm">
        <div className="text-zinc-500">
          Work by {authorId.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())}
        </div>
        <Link
          href={`/author/${authorId}/work/${workId}`}
          className="inline-block text-xs text-blue-600 hover:text-blue-800 dark:text-blue-400"
        >
          Read work &rarr;
        </Link>
      </div>
    );
  }

  if (nodeType === "document-section") {
    // Parse section node ID: "document-section:lumen-gentium/12"
    const slashIdx = sourceId.indexOf("/");
    const docId = slashIdx >= 0 ? sourceId.slice(0, slashIdx) : sourceId;
    const secNum = slashIdx >= 0 ? sourceId.slice(slashIdx + 1) : "";
    return (
      <div className="space-y-2 text-sm">
        <div className="text-zinc-500">{label}</div>
        <Link
          href={`/document/${docId}`}
          className="inline-block text-xs text-blue-600 hover:text-blue-800 dark:text-blue-400"
        >
          View document &rarr;
        </Link>
      </div>
    );
  }

  return <div className="text-sm text-zinc-500">{label}</div>;
}

export default function GraphDetailPanel({
  nodeId,
  graph,
  onClose,
  onNavigate,
  onThemeFilter,
  canGoBack,
  onGoBack,
}: GraphDetailPanelProps) {
  const { lang, setLang } = useLang();
  const [paragraphs, setParagraphs] = useState<Map<number, ParagraphData>>(
    new Map(),
  );
  const [loaded, setLoaded] = useState(false);
  const [bibleSources, setBibleSources] = useState<Record<string, BibleBookData>>({});
  const [documentSources, setDocumentSources] = useState<Record<string, DocumentData>>({});
  const [authorSources, setAuthorSources] = useState<Record<string, AuthorData>>({});
  const [entityDefs, setEntityDefs] = useState<Map<string, EntityDefinition>>(new Map());
  const [topicDefs, setTopicDefs] = useState<Map<number, TopicDefinition>>(new Map());

  useEffect(() => {
    fetchParagraphs().then((data) => {
      const map = new Map<number, ParagraphData>();
      for (const p of data) map.set(p.id, p);
      setParagraphs(map);
      setLoaded(true);
    });
    fetchEntities().then((data) => {
      setEntityDefs(new Map(data.map((e) => [e.id, e])));
    }).catch(() => {});
    fetchTopics().then((data) => {
      setTopicDefs(new Map(data.map((t) => [t.id, t])));
    }).catch(() => {});
  }, []);

  // Lazy-load source data when a source node is selected
  useEffect(() => {
    if (!graph.hasNode(nodeId)) return;
    const nType = graph.getNodeAttributes(nodeId).node_type;
    if (nType === "bible" && Object.keys(bibleSources).length === 0) {
      fetchBibleSources().then(setBibleSources);
    } else if (nType === "document" && Object.keys(documentSources).length === 0) {
      fetchDocumentSources().then(setDocumentSources);
    } else if ((nType === "author" || nType === "patristic-work") && Object.keys(authorSources).length === 0) {
      fetchAuthorSources().then(setAuthorSources);
    }
  }, [nodeId, graph, bibleSources, documentSources, authorSources]);

  if (!graph.hasNode(nodeId)) return null;

  const attrs = graph.getNodeAttributes(nodeId);
  const nodeType = attrs.node_type;
  const isParagraph = nodeType === "paragraph";
  const isSource = nodeType === "bible" || nodeType === "author" || nodeType === "patristic-work" || nodeType === "document" || nodeType === "document-section";
  const paraId = isParagraph ? parseInt(nodeId.replace("p:", "")) : null;
  const paraData = paraId ? paragraphs.get(paraId) : null;

  // All connections grouped by edge type
  const connectionsByType: Record<string, string[]> = {};

  graph.forEachEdge(nodeId, (_edge, edgeAttrs, source, target) => {
    const neighbor = source === nodeId ? target : source;
    const type: string = edgeAttrs.edge_type;
    if (!connectionsByType[type]) connectionsByType[type] = [];
    connectionsByType[type].push(neighbor);
  });

  // Sort each connection group
  for (const type of Object.keys(connectionsByType)) {
    connectionsByType[type].sort((a, b) => {
      // Paragraphs sort numerically
      if (a.startsWith("p:") && b.startsWith("p:")) {
        return parseInt(a.replace("p:", "")) - parseInt(b.replace("p:", ""));
      }
      const labelA = graph.hasNode(a) ? graph.getNodeAttributes(a).label : a;
      const labelB = graph.hasNode(b) ? graph.getNodeAttributes(b).label : b;
      return labelA.localeCompare(labelB);
    });
  }

  const EDGE_TYPE_LABELS: Record<string, string> = {
    cross_reference: "Cross-References",
    cites: "Citations",
    belongs_to: "Structure",
    child_of: "Children",
    shared_theme: "Shared Theme",
    bible_cross_reference: "Bible Cross-References",
    shared_entity: "Shared Entities",
    shared_topic: "Shared Topic",
    shared_citation: "Shared Citations",
  };
  const totalConnections = Object.values(connectionsByType).reduce((s, arr) => s + arr.length, 0);

  // Build breadcrumb (use English keys for PART_SHORT_NAMES lookup)
  const partEn = paraData?.part ? t(paraData.part, "en") : "";
  const breadcrumb = [
    partEn && PART_SHORT_NAMES[partEn],
    paraData?.section && t(paraData.section, lang),
    paraData?.chapter && t(paraData.chapter, lang),
    paraData?.article && t(paraData.article, lang),
  ].filter(Boolean);

  const hasMultipleLangs = paraData
    ? typeof paraData.text === "object" &&
      Object.keys(paraData.text).filter((k) => (paraData.text as Record<string, string>)[k]).length > 1
    : false;

  return (
    <div className="absolute right-0 top-0 h-full w-96 overflow-y-auto bg-white shadow-lg dark:bg-zinc-900 z-20">
      <div className="sticky top-0 flex items-center justify-between border-b bg-white px-4 py-3 dark:border-zinc-700 dark:bg-zinc-900">
        <div className="flex items-center gap-1 min-w-0">
          {canGoBack && (
            <button
              onClick={onGoBack}
              className="rounded p-1 hover:bg-zinc-100 dark:hover:bg-zinc-800 shrink-0"
              title="Go back"
            >
              <svg className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M12.79 5.23a.75.75 0 01-.02 1.06L8.832 10l3.938 3.71a.75.75 0 11-1.04 1.08l-4.5-4.25a.75.75 0 010-1.08l4.5-4.25a.75.75 0 011.06.02z" clipRule="evenodd" />
              </svg>
            </button>
          )}
          <h2 className="text-lg font-semibold truncate">{attrs.label}</h2>
        </div>
        <button
          onClick={onClose}
          className="rounded p-1 hover:bg-zinc-100 dark:hover:bg-zinc-800 shrink-0"
        >
          <svg className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
            <path d="M6.28 5.22a.75.75 0 00-1.06 1.06L8.94 10l-3.72 3.72a.75.75 0 101.06 1.06L10 11.06l3.72 3.72a.75.75 0 101.06-1.06L11.06 10l3.72-3.72a.75.75 0 00-1.06-1.06L10 8.94 6.28 5.22z" />
          </svg>
        </button>
      </div>

      <div className="space-y-4 p-4">
        {/* Breadcrumb */}
        {breadcrumb.length > 0 && (
          <div className="text-xs text-zinc-500">
            {breadcrumb.map((item, i) => (
              <span key={i}>
                {i > 0 && <span className="mx-1">&rsaquo;</span>}
                <span className="truncate">{item}</span>
              </span>
            ))}
          </div>
        )}

        {/* Node color indicator */}
        <div className="flex items-center gap-2">
          <span
            className="inline-block h-3 w-3 rounded-full"
            style={{ backgroundColor: attrs.color }}
          />
          <span className="text-sm text-zinc-600 dark:text-zinc-400">
            {isParagraph
              ? PART_SHORT_NAMES[t(attrs.part)] || t(attrs.part)
              : isSource
                ? nodeType === "bible"
                  ? "Bible Book"
                  : nodeType === "author"
                    ? "Church Father"
                    : nodeType === "patristic-work"
                      ? "Patristic Work"
                      : "Ecclesiastical Document"
                : `Structure (${attrs.level})`}
          </span>
        </div>

        {/* Theme badges */}
        {paraData && paraData.themes.length > 0 && (
          <div className="flex flex-wrap gap-1">
            {paraData.themes.map((theme) => (
              <button
                key={theme}
                onClick={() => onThemeFilter(theme)}
                className="rounded-full px-2 py-0.5 text-xs font-medium text-white hover:opacity-80 cursor-pointer"
                style={{ backgroundColor: THEME_COLORS[theme] || "#999" }}
              >
                {theme}
              </button>
            ))}
          </div>
        )}

        {/* Entity badges */}
        {paraData && paraData.entities && paraData.entities.length > 0 && (
          <div>
            <div className="mb-1 text-xs text-zinc-400">Entities</div>
            <div className="flex flex-wrap gap-1">
              {paraData.entities.map((eid) => {
                const def = entityDefs.get(eid);
                return (
                  <span
                    key={eid}
                    className="rounded-full bg-orange-50 px-2 py-0.5 text-xs font-medium text-orange-800 dark:bg-orange-900/30 dark:text-orange-300"
                  >
                    {def?.label || eid}
                  </span>
                );
              })}
            </div>
          </div>
        )}

        {/* Topic badges */}
        {paraData && paraData.topics && paraData.topics.length > 0 && (
          <div>
            <div className="mb-1 text-xs text-zinc-400">Topics</div>
            <div className="flex flex-wrap gap-1">
              {paraData.topics.map((tid) => {
                const def = topicDefs.get(tid);
                const label = def ? def.terms.slice(0, 4).join(", ") : `Topic ${tid}`;
                return (
                  <span
                    key={tid}
                    className="rounded-full bg-teal-50 px-2 py-0.5 text-xs font-medium text-teal-800 dark:bg-teal-900/30 dark:text-teal-300"
                  >
                    {label}
                  </span>
                );
              })}
            </div>
          </div>
        )}

        {/* Language selector */}
        {loaded && hasMultipleLangs && (
          <LangSelector />
        )}

        {/* Paragraph text */}
        {loaded && paraData ? (
          <div className="text-sm leading-relaxed text-zinc-800 dark:text-zinc-200">
            {t(paraData.text, lang)}
          </div>
        ) : isParagraph && !loaded ? (
          <div className="text-sm text-zinc-400">Loading...</div>
        ) : isSource ? (
          <SourcePreview
            nodeType={nodeType}
            nodeId={nodeId}
            label={attrs.label}
            bibleSources={bibleSources}
            documentSources={documentSources}
            authorSources={authorSources}
          />
        ) : !isParagraph ? (
          <div className="text-sm text-zinc-500">
            Structural node: {attrs.label}
          </div>
        ) : null}

        {/* Bible citations (for paragraphs) */}
        {paraData && paraData.bible_citations.length > 0 && (
          <div>
            <h3 className="mb-2 text-xs font-semibold uppercase text-zinc-500">
              Bible Citations ({paraData.bible_citations.length})
            </h3>
            <div className="flex flex-wrap gap-1">
              {paraData.bible_citations.map((bookId) => {
                const sourceNodeId = `bible:${bookId}`;
                const hasNode = graph.hasNode(sourceNodeId);
                const label = bookId.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
                return (
                  <button
                    key={bookId}
                    onClick={() => hasNode && onNavigate(sourceNodeId)}
                    disabled={!hasNode}
                    className="rounded bg-green-50 px-2 py-0.5 text-xs font-medium text-green-800 hover:bg-green-100 disabled:opacity-50 dark:bg-green-900/30 dark:text-green-300 dark:hover:bg-green-900/50"
                    style={{ borderLeft: "3px solid #59A14F" }}
                  >
                    {label}
                  </button>
                );
              })}
            </div>
          </div>
        )}

        {/* Author citations (for paragraphs) */}
        {paraData && paraData.author_citations.length > 0 && (
          <div>
            <h3 className="mb-2 text-xs font-semibold uppercase text-zinc-500">
              Church Fathers ({paraData.author_citations.length})
            </h3>
            <div className="flex flex-wrap gap-1">
              {paraData.author_citations.map((authorId) => {
                const sourceNodeId = `author:${authorId}`;
                const hasNode = graph.hasNode(sourceNodeId);
                const label = authorId.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
                return (
                  <button
                    key={authorId}
                    onClick={() => hasNode && onNavigate(sourceNodeId)}
                    disabled={!hasNode}
                    className="rounded bg-purple-50 px-2 py-0.5 text-xs font-medium text-purple-800 hover:bg-purple-100 disabled:opacity-50 dark:bg-purple-900/30 dark:text-purple-300 dark:hover:bg-purple-900/50"
                    style={{ borderLeft: "3px solid #B07AA1" }}
                  >
                    {label}
                  </button>
                );
              })}
            </div>
          </div>
        )}

        {/* Document citations (for paragraphs) */}
        {paraData && paraData.document_citations.length > 0 && (
          <div>
            <h3 className="mb-2 text-xs font-semibold uppercase text-zinc-500">
              Ecclesiastical Documents ({paraData.document_citations.length})
            </h3>
            <div className="flex flex-wrap gap-1">
              {paraData.document_citations.map((docId) => {
                const sourceNodeId = `document:${docId}`;
                const hasNode = graph.hasNode(sourceNodeId);
                const label = docId.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
                return (
                  <button
                    key={docId}
                    onClick={() => hasNode && onNavigate(sourceNodeId)}
                    disabled={!hasNode}
                    className="rounded bg-amber-50 px-2 py-0.5 text-xs font-medium text-amber-800 hover:bg-amber-100 disabled:opacity-50 dark:bg-amber-900/30 dark:text-amber-300 dark:hover:bg-amber-900/50"
                    style={{ borderLeft: `3px solid ${SOURCE_COLORS.document}` }}
                  >
                    {label}
                  </button>
                );
              })}
            </div>
          </div>
        )}

        {/* Footnotes */}
        {paraData && tArr(paraData.footnotes, lang).length > 0 && (
          <div>
            <h3 className="mb-1 text-xs font-semibold uppercase text-zinc-500">
              {lang === "pt" ? "Notas" : "Footnotes"}
            </h3>
            <ul className="space-y-1 text-xs text-zinc-600 dark:text-zinc-400">
              {tArr(paraData.footnotes, lang).map((fn, i) => (
                <li key={i} className="pl-2 border-l-2 border-zinc-200">
                  {fn}
                </li>
              ))}
            </ul>
          </div>
        )}

        {/* Connections list */}
        {totalConnections > 0 && (
          <div className="border-t pt-3 dark:border-zinc-700">
            <h3 className="mb-2 text-xs font-semibold uppercase text-zinc-500">
              Connections ({totalConnections})
            </h3>
            <div className="space-y-3">
              {Object.entries(connectionsByType).map(([type, neighbors]) => (
                <div key={type}>
                  <div className="mb-1 text-xs text-zinc-400">
                    {EDGE_TYPE_LABELS[type] || type} ({neighbors.length})
                  </div>
                  <div className="flex flex-wrap gap-1">
                    {neighbors.map((nId) => {
                      const nAttrs = graph.getNodeAttributes(nId);
                      return (
                        <button
                          key={nId}
                          onClick={() => onNavigate(nId)}
                          className="rounded bg-zinc-100 px-2 py-0.5 text-xs font-medium hover:bg-zinc-200 dark:bg-zinc-800 dark:hover:bg-zinc-700"
                          style={{ borderLeft: `3px solid ${nAttrs.color}` }}
                        >
                          {nAttrs.label}
                        </button>
                      );
                    })}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Stats */}
        {isParagraph && (
          <div className="border-t pt-3 dark:border-zinc-700">
            <div className="text-xs text-zinc-500">
              Community: {attrs.community}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
