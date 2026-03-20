import Link from "next/link";
import paragraphsData from "../../../public/data/paragraphs.json";
import { PART_COLORS, PART_SHORT_NAMES } from "@/lib/colors";
import { t } from "@/lib/types";

interface TreeNode {
  label: string;
  children: Map<string, TreeNode>;
  paragraphs: number[];
}

function buildTree(): TreeNode {
  const root: TreeNode = { label: "Catechism", children: new Map(), paragraphs: [] };

  for (const p of paragraphsData) {
    const parts = [t(p.part), t(p.section), t(p.chapter), t(p.article)].filter(Boolean);
    let node = root;
    for (const part of parts) {
      if (!node.children.has(part)) {
        node.children.set(part, { label: part, children: new Map(), paragraphs: [] });
      }
      node = node.children.get(part)!;
    }
    node.paragraphs.push(p.id);
  }

  return root;
}

function TreeSection({ node, depth }: { node: TreeNode; depth: number }) {
  const color = depth === 0 ? PART_COLORS[node.label] : undefined;
  const isLeaf = node.children.size === 0;

  return (
    <details open={depth < 2} className={depth > 0 ? "ml-4" : ""}>
      <summary className="cursor-pointer py-1 text-sm hover:text-blue-600">
        {color && (
          <span
            className="mr-2 inline-block h-2.5 w-2.5 rounded-full"
            style={{ backgroundColor: color }}
          />
        )}
        <span className={depth === 0 ? "font-semibold" : ""}>
          {PART_SHORT_NAMES[node.label] || node.label}
        </span>
        {node.paragraphs.length > 0 && (
          <span className="ml-2 text-xs text-zinc-400">
            ({node.paragraphs.length} paragraphs)
          </span>
        )}
      </summary>

      {/* Child sections */}
      {Array.from(node.children.values()).map((child) => (
        <TreeSection key={child.label} node={child} depth={depth + 1} />
      ))}

      {/* Paragraph links (only if leaf node) */}
      {isLeaf && node.paragraphs.length > 0 && (
        <div className="ml-6 mt-1 flex flex-wrap gap-1 pb-2">
          {node.paragraphs.map((id) => (
            <Link
              key={id}
              href={`/paragraph/${id}`}
              className="rounded bg-zinc-100 px-1.5 py-0.5 text-xs text-zinc-600 hover:bg-zinc-200 dark:bg-zinc-800 dark:text-zinc-400 dark:hover:bg-zinc-700"
            >
              {id}
            </Link>
          ))}
        </div>
      )}
    </details>
  );
}

export default function StructurePage() {
  const tree = buildTree();

  return (
    <div className="mx-auto max-w-3xl px-6 py-12">
      <div className="mb-6 flex items-center justify-between">
        <h1 className="text-2xl font-bold">CCC Structure</h1>
        <Link
          href="/"
          className="text-sm text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300"
        >
          Home
        </Link>
      </div>

      <p className="mb-8 text-sm text-zinc-500">
        Browse the Catechism of the Catholic Church by its structural hierarchy.
        Click paragraph numbers to see full text.
      </p>

      <div className="space-y-2">
        {Array.from(tree.children.values()).map((child) => (
          <TreeSection key={child.label} node={child} depth={0} />
        ))}
      </div>
    </div>
  );
}
