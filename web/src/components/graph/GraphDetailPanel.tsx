"use client";

import { useEffect, useState } from "react";
import Graph from "graphology";
import type { ParagraphData } from "@/lib/types";
import { fetchParagraphs } from "@/lib/graph-data";
import { PART_SHORT_NAMES, SOURCE_COLORS, THEME_COLORS } from "@/lib/colors";

interface GraphDetailPanelProps {
  nodeId: string;
  graph: Graph;
  onClose: () => void;
  onNavigate: (nodeId: string) => void;
}

export default function GraphDetailPanel({
  nodeId,
  graph,
  onClose,
  onNavigate,
}: GraphDetailPanelProps) {
  const [paragraphs, setParagraphs] = useState<Map<number, ParagraphData>>(
    new Map(),
  );
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    fetchParagraphs().then((data) => {
      const map = new Map<number, ParagraphData>();
      for (const p of data) map.set(p.id, p);
      setParagraphs(map);
      setLoaded(true);
    });
  }, []);

  if (!graph.hasNode(nodeId)) return null;

  const attrs = graph.getNodeAttributes(nodeId);
  const nodeType = attrs.node_type;
  const isParagraph = nodeType === "paragraph";
  const isSource = nodeType === "bible" || nodeType === "author" || nodeType === "document";
  const paraId = isParagraph ? parseInt(nodeId.replace("p:", "")) : null;
  const paraData = paraId ? paragraphs.get(paraId) : null;

  // Get cross-reference neighbors (for paragraphs)
  const crossRefNeighbors: string[] = [];
  // Get citing paragraphs (for source nodes)
  const citingParagraphs: string[] = [];

  if (isParagraph) {
    graph.forEachEdge(nodeId, (edge, edgeAttrs, source, target) => {
      if (edgeAttrs.edge_type === "cross_reference") {
        const neighbor = source === nodeId ? target : source;
        crossRefNeighbors.push(neighbor);
      }
    });
  }

  if (isSource) {
    graph.forEachEdge(nodeId, (_edge, edgeAttrs, source, target) => {
      if (edgeAttrs.edge_type === "cites") {
        const neighbor = source === nodeId ? target : source;
        if (neighbor.startsWith("p:")) {
          citingParagraphs.push(neighbor);
        }
      }
    });
    citingParagraphs.sort((a, b) => {
      const aId = parseInt(a.replace("p:", ""));
      const bId = parseInt(b.replace("p:", ""));
      return aId - bId;
    });
  }

  // Build breadcrumb
  const breadcrumb = [
    paraData?.part && PART_SHORT_NAMES[paraData.part],
    paraData?.section,
    paraData?.chapter,
    paraData?.article,
  ].filter(Boolean);

  return (
    <div className="absolute right-0 top-0 h-full w-96 overflow-y-auto bg-white shadow-lg dark:bg-zinc-900 z-20">
      <div className="sticky top-0 flex items-center justify-between border-b bg-white px-4 py-3 dark:border-zinc-700 dark:bg-zinc-900">
        <h2 className="text-lg font-semibold">{attrs.label}</h2>
        <button
          onClick={onClose}
          className="rounded p-1 hover:bg-zinc-100 dark:hover:bg-zinc-800"
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
              ? PART_SHORT_NAMES[attrs.part] || attrs.part
              : isSource
                ? nodeType === "bible"
                  ? "Bible Book"
                  : nodeType === "author"
                    ? "Church Father"
                    : "Ecclesiastical Document"
                : `Structure (${attrs.level})`}
          </span>
        </div>

        {/* Theme badges */}
        {paraData && paraData.themes.length > 0 && (
          <div className="flex flex-wrap gap-1">
            {paraData.themes.map((theme) => (
              <span
                key={theme}
                className="rounded-full px-2 py-0.5 text-xs font-medium text-white"
                style={{ backgroundColor: THEME_COLORS[theme] || "#999" }}
              >
                {theme}
              </span>
            ))}
          </div>
        )}

        {/* Paragraph text */}
        {loaded && paraData ? (
          <div className="text-sm leading-relaxed text-zinc-800 dark:text-zinc-200">
            {paraData.text}
          </div>
        ) : isParagraph && !loaded ? (
          <div className="text-sm text-zinc-400">Loading...</div>
        ) : isSource ? (
          <div className="text-sm text-zinc-500">
            {nodeType === "bible" ? "Bible book" : nodeType === "author" ? "Patristic author" : "Ecclesiastical document"}: {attrs.label}
          </div>
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
        {paraData && paraData.footnotes.length > 0 && (
          <div>
            <h3 className="mb-1 text-xs font-semibold uppercase text-zinc-500">
              Footnotes
            </h3>
            <ul className="space-y-1 text-xs text-zinc-600 dark:text-zinc-400">
              {paraData.footnotes.map((fn, i) => (
                <li key={i} className="pl-2 border-l-2 border-zinc-200">
                  {fn}
                </li>
              ))}
            </ul>
          </div>
        )}

        {/* Cross-references (for paragraphs) */}
        {crossRefNeighbors.length > 0 && (
          <div>
            <h3 className="mb-2 text-xs font-semibold uppercase text-zinc-500">
              Cross-References ({crossRefNeighbors.length})
            </h3>
            <div className="flex flex-wrap gap-1">
              {crossRefNeighbors.sort().map((nId) => {
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
        )}

        {/* Citing paragraphs (for source nodes) */}
        {citingParagraphs.length > 0 && (
          <div>
            <h3 className="mb-2 text-xs font-semibold uppercase text-zinc-500">
              Citing Paragraphs ({citingParagraphs.length})
            </h3>
            <div className="flex flex-wrap gap-1">
              {citingParagraphs.map((nId) => {
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
        )}

        {/* Stats */}
        <div className="border-t pt-3 dark:border-zinc-700">
          <div className="grid grid-cols-2 gap-2 text-xs text-zinc-500">
            <div>Connections: {attrs.degree}</div>
            {isParagraph && <div>Community: {attrs.community}</div>}
          </div>
        </div>
      </div>
    </div>
  );
}
