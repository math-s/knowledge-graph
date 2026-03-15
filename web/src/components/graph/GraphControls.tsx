"use client";

import { useCallback } from "react";
import { useCamera, useFullScreen, useSigma } from "@react-sigma/core";
import type { GraphFilters } from "./FilterPanel";
import { matchesHighlightFilters, hasHighlightFilters } from "./FilterPanel";

interface GraphControlsProps {
  filters: GraphFilters;
}

export default function GraphControls({ filters }: GraphControlsProps) {
  const { zoomIn, zoomOut, reset } = useCamera();
  const { toggle: toggleFullscreen, isFullScreen } = useFullScreen();
  const sigma = useSigma();

  const showZoomToHighlighted = hasHighlightFilters(filters);

  const zoomToHighlighted = useCallback(() => {
    const graph = sigma.getGraph();
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    let count = 0;

    graph.forEachNode((node, attrs) => {
      if (attrs.node_type !== "paragraph") return;
      if (!matchesHighlightFilters(filters, attrs)) return;
      const display = sigma.getNodeDisplayData(node);
      if (!display) return;
      if (display.x < minX) minX = display.x;
      if (display.y < minY) minY = display.y;
      if (display.x > maxX) maxX = display.x;
      if (display.y > maxY) maxY = display.y;
      count++;
    });

    if (count === 0) return;

    // Compute center in graph coordinates
    const centerX = (minX + maxX) / 2;
    const centerY = (minY + maxY) / 2;

    // Convert center to camera coordinates
    const viewportPos = sigma.graphToViewport({ x: centerX, y: centerY });
    const framedPos = sigma.viewportToFramedGraph(viewportPos);

    // Compute ratio from bounding box extent relative to graph extent
    const graphExtent = sigma.getGraphDimensions();
    const bboxWidth = maxX - minX || 1;
    const bboxHeight = maxY - minY || 1;
    const ratioX = graphExtent.width > 0 ? (bboxWidth / graphExtent.width) : 0.5;
    const ratioY = graphExtent.height > 0 ? (bboxHeight / graphExtent.height) : 0.5;
    const ratio = Math.max(ratioX, ratioY) * 1.3; // 1.3x padding

    sigma.getCamera().animate(
      { x: framedPos.x, y: framedPos.y, ratio: Math.max(0.02, Math.min(ratio, 10)) },
      { duration: 300 },
    );
  }, [sigma, filters]);

  const btnClass =
    "flex h-8 w-8 items-center justify-center rounded bg-white shadow hover:bg-zinc-50 dark:bg-zinc-800 dark:hover:bg-zinc-700 text-zinc-700 dark:text-zinc-200";

  return (
    <div className="absolute bottom-4 right-4 z-10 flex flex-col gap-1">
      <button onClick={() => zoomIn()} className={btnClass} title="Zoom in">
        <svg className="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
          <path d="M10 5a1 1 0 011 1v3h3a1 1 0 110 2h-3v3a1 1 0 11-2 0v-3H6a1 1 0 110-2h3V6a1 1 0 011-1z" />
        </svg>
      </button>
      <button onClick={() => zoomOut()} className={btnClass} title="Zoom out">
        <svg className="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
          <path d="M5 10a1 1 0 011-1h8a1 1 0 110 2H6a1 1 0 01-1-1z" />
        </svg>
      </button>
      <button onClick={() => reset()} className={btnClass} title="Reset view">
        <svg className="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
          <path
            fillRule="evenodd"
            d="M4 2a1 1 0 011 1v2.101a7.002 7.002 0 0111.601 2.566 1 1 0 11-1.885.666A5.002 5.002 0 005.999 7H9a1 1 0 010 2H4a1 1 0 01-1-1V3a1 1 0 011-1zm.008 9.057a1 1 0 011.276.61A5.002 5.002 0 0014.001 13H11a1 1 0 110-2h5a1 1 0 011 1v5a1 1 0 11-2 0v-2.101a7.002 7.002 0 01-11.601-2.566 1 1 0 01.61-1.276z"
            clipRule="evenodd"
          />
        </svg>
      </button>
      {showZoomToHighlighted && (
        <button
          onClick={zoomToHighlighted}
          className={btnClass}
          title="Zoom to highlighted"
        >
          <svg className="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
            <path
              fillRule="evenodd"
              d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z"
              clipRule="evenodd"
            />
          </svg>
        </button>
      )}
      <button
        onClick={toggleFullscreen}
        className={btnClass}
        title={isFullScreen ? "Exit fullscreen" : "Fullscreen"}
      >
        <svg className="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
          {isFullScreen ? (
            <path
              fillRule="evenodd"
              d="M3 4a1 1 0 011-1h4a1 1 0 010 2H6.414l2.293 2.293a1 1 0 01-1.414 1.414L5 6.414V8a1 1 0 01-2 0V4zm9 1a1 1 0 010-2h4a1 1 0 011 1v4a1 1 0 01-2 0V6.414l-2.293 2.293a1 1 0 11-1.414-1.414L13.586 5H12zm-9 7a1 1 0 012 0v1.586l2.293-2.293a1 1 0 011.414 1.414L6.414 15H8a1 1 0 010 2H4a1 1 0 01-1-1v-4zm13-1a1 1 0 011 1v4a1 1 0 01-1 1h-4a1 1 0 010-2h1.586l-2.293-2.293a1 1 0 011.414-1.414L15 13.586V12a1 1 0 011-1z"
              clipRule="evenodd"
            />
          ) : (
            <path
              fillRule="evenodd"
              d="M3 4a1 1 0 011-1h4a1 1 0 010 2H6.414l2.293 2.293a1 1 0 01-1.414 1.414L5 6.414V8a1 1 0 01-2 0V4zm9 1a1 1 0 010-2h4a1 1 0 011 1v4a1 1 0 01-2 0V6.414l-2.293 2.293a1 1 0 11-1.414-1.414L13.586 5H12zm-9 7a1 1 0 012 0v1.586l2.293-2.293a1 1 0 011.414 1.414L6.414 15H8a1 1 0 010 2H4a1 1 0 01-1-1v-4zm13-1a1 1 0 011 1v4a1 1 0 01-1 1h-4a1 1 0 010-2h1.586l-2.293-2.293a1 1 0 011.414-1.414L15 13.586V12a1 1 0 011-1z"
              clipRule="evenodd"
            />
          )}
        </svg>
      </button>
    </div>
  );
}
