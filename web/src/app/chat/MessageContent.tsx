"use client";

import Link from "next/link";
import ReactMarkdown, { Components } from "react-markdown";
import remarkGfm from "remark-gfm";
import { ReactNode } from "react";

const BIBLE_BOOK_SLUGS: Record<string, string> = {
  gen: "genesis", genesis: "genesis",
  ex: "exodus", exo: "exodus", exodus: "exodus",
  lev: "leviticus", leviticus: "leviticus",
  num: "numbers", numbers: "numbers",
  dt: "deuteronomy", deut: "deuteronomy", deuteronomy: "deuteronomy",
  josh: "joshua", joshua: "joshua",
  judg: "judges", judges: "judges",
  ruth: "ruth",
  "1sam": "1-samuel", "1-sam": "1-samuel", "1-samuel": "1-samuel",
  "2sam": "2-samuel", "2-sam": "2-samuel", "2-samuel": "2-samuel",
  "1kgs": "1-kings", "1-kgs": "1-kings", "1-kings": "1-kings",
  "2kgs": "2-kings", "2-kgs": "2-kings", "2-kings": "2-kings",
  ps: "psalms", psalm: "psalms", psalms: "psalms",
  prov: "proverbs", proverbs: "proverbs",
  eccl: "ecclesiastes", ecclesiastes: "ecclesiastes",
  isa: "isaiah", isaiah: "isaiah",
  jer: "jeremiah", jeremiah: "jeremiah",
  ezek: "ezekiel", ezekiel: "ezekiel",
  dan: "daniel", daniel: "daniel",
  mt: "matthew", matt: "matthew", matthew: "matthew",
  mk: "mark", mark: "mark",
  lk: "luke", luke: "luke",
  jn: "john", john: "john",
  acts: "acts",
  rom: "romans", romans: "romans",
  "1cor": "1-corinthians", "1-cor": "1-corinthians", "1-corinthians": "1-corinthians",
  "2cor": "2-corinthians", "2-cor": "2-corinthians", "2-corinthians": "2-corinthians",
  gal: "galatians", galatians: "galatians",
  eph: "ephesians", ephesians: "ephesians",
  phil: "philippians", philippians: "philippians",
  col: "colossians", colossians: "colossians",
  "1thess": "1-thessalonians", "1-thess": "1-thessalonians",
  "2thess": "2-thessalonians", "2-thess": "2-thessalonians",
  "1tim": "1-timothy", "1-tim": "1-timothy",
  "2tim": "2-timothy", "2-tim": "2-timothy",
  titus: "titus",
  heb: "hebrews", hebrews: "hebrews",
  jas: "james", james: "james",
  "1pet": "1-peter", "1-pet": "1-peter",
  "2pet": "2-peter", "2-pet": "2-peter",
  "1jn": "1-john", "1-jn": "1-john",
  "2jn": "2-john", "2-jn": "2-john",
  "3jn": "3-john", "3-jn": "3-john",
  jude: "jude",
  rev: "revelation", revelation: "revelation",
};

const CCC_RE = /\bCCC\s?(\d{1,4})(?:[–-](\d{1,4}))?\b/gi;
const BIBLE_RE = /\b((?:[123][-\s]?)?[A-Z][a-z]{1,12}\.?)\s+(\d{1,3}):(\d{1,3})(?:[–-](\d{1,3}))?\b/g;

function normalizeBookKey(raw: string): string {
  return raw.toLowerCase().replace(/\./g, "").replace(/\s+/g, "-");
}

type Token = { type: "text"; value: string } | { type: "link"; href: string; value: string };

function tokenize(text: string): Token[] {
  type Match = { start: number; end: number; href: string; value: string };
  const matches: Match[] = [];

  for (const m of text.matchAll(CCC_RE)) {
    const start = m.index ?? 0;
    matches.push({
      start,
      end: start + m[0].length,
      href: `/paragraph/${m[1]}`,
      value: m[0],
    });
  }

  for (const m of text.matchAll(BIBLE_RE)) {
    const start = m.index ?? 0;
    const bookKey = normalizeBookKey(m[1]);
    const slug = BIBLE_BOOK_SLUGS[bookKey];
    if (!slug) continue;
    matches.push({
      start,
      end: start + m[0].length,
      href: `/bible/${slug}/${m[2]}`,
      value: m[0],
    });
  }

  matches.sort((a, b) => a.start - b.start);

  const tokens: Token[] = [];
  let cursor = 0;
  for (const m of matches) {
    if (m.start < cursor) continue;
    if (m.start > cursor) {
      tokens.push({ type: "text", value: text.slice(cursor, m.start) });
    }
    tokens.push({ type: "link", href: m.href, value: m.value });
    cursor = m.end;
  }
  if (cursor < text.length) {
    tokens.push({ type: "text", value: text.slice(cursor) });
  }
  return tokens;
}

function linkifyChildren(children: ReactNode): ReactNode {
  if (typeof children === "string") {
    const tokens = tokenize(children);
    if (tokens.length === 1 && tokens[0].type === "text") return children;
    return tokens.map((t, i) =>
      t.type === "text" ? (
        <span key={i}>{t.value}</span>
      ) : (
        <Link
          key={i}
          href={t.href}
          className="text-blue-600 underline decoration-blue-300 underline-offset-2 hover:decoration-blue-600 dark:text-blue-400 dark:decoration-blue-700"
        >
          {t.value}
        </Link>
      ),
    );
  }
  if (Array.isArray(children)) {
    return children.map((child, i) => (
      <span key={i}>{linkifyChildren(child)}</span>
    ));
  }
  return children;
}

const markdownComponents: Components = {
  p: ({ children }) => <p className="mb-3 last:mb-0">{linkifyChildren(children)}</p>,
  li: ({ children }) => <li className="mb-1">{linkifyChildren(children)}</li>,
  ul: ({ children }) => <ul className="mb-3 list-disc pl-5">{children}</ul>,
  ol: ({ children }) => <ol className="mb-3 list-decimal pl-5">{children}</ol>,
  h1: ({ children }) => <h1 className="mb-2 mt-4 text-lg font-semibold">{linkifyChildren(children)}</h1>,
  h2: ({ children }) => <h2 className="mb-2 mt-4 text-base font-semibold">{linkifyChildren(children)}</h2>,
  h3: ({ children }) => <h3 className="mb-2 mt-3 text-sm font-semibold">{linkifyChildren(children)}</h3>,
  strong: ({ children }) => <strong className="font-semibold">{linkifyChildren(children)}</strong>,
  em: ({ children }) => <em className="italic">{linkifyChildren(children)}</em>,
  blockquote: ({ children }) => (
    <blockquote className="my-3 border-l-2 border-zinc-300 pl-3 italic text-zinc-600 dark:border-zinc-600 dark:text-zinc-400">
      {children}
    </blockquote>
  ),
  code: ({ children }) => (
    <code className="rounded bg-zinc-100 px-1 py-0.5 font-mono text-xs dark:bg-zinc-700">
      {children}
    </code>
  ),
  a: ({ href, children }) => (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className="text-blue-600 underline hover:text-blue-800 dark:text-blue-400"
    >
      {children}
    </a>
  ),
};

export default function MessageContent({ content }: { content: string }) {
  return (
    <div className="prose-sm max-w-none">
      <ReactMarkdown remarkPlugins={[remarkGfm]} components={markdownComponents}>
        {content}
      </ReactMarkdown>
    </div>
  );
}
