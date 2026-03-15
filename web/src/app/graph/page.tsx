"use client";

import { Suspense } from "react";
import dynamic from "next/dynamic";

const GraphExplorer = dynamic(() => import("@/components/graph/GraphExplorer"), {
  ssr: false,
  loading: () => (
    <div className="flex h-screen items-center justify-center">
      <div className="text-center">
        <div className="mb-2 text-lg font-medium">Loading graph...</div>
        <div className="text-sm text-zinc-500">3,260 nodes, 3,928 edges</div>
      </div>
    </div>
  ),
});

export default function GraphPage() {
  return (
    <Suspense>
      <GraphExplorer />
    </Suspense>
  );
}
