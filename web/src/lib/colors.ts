/** CCC Part colors for graph visualization. */
export const PART_COLORS: Record<string, string> = {
  PROLOGUE: "#888888",
  "PART ONE: THE PROFESSION OF FAITH": "#4E79A7",
  "PART TWO: THE CELEBRATION OF THE CHRISTIAN MYSTERY": "#E15759",
  "PART THREE: LIFE IN CHRIST": "#76B7B2",
  "PART FOUR: CHRISTIAN PRAYER": "#F28E2B",
};

export const PART_SHORT_NAMES: Record<string, string> = {
  PROLOGUE: "Prologue",
  "PART ONE: THE PROFESSION OF FAITH": "Profession of Faith",
  "PART TWO: THE CELEBRATION OF THE CHRISTIAN MYSTERY": "Christian Mystery",
  "PART THREE: LIFE IN CHRIST": "Life in Christ",
  "PART FOUR: CHRISTIAN PRAYER": "Christian Prayer",
};

export const STRUCTURE_COLOR = "#CCCCCC";

export function getPartColor(part: string): string {
  return PART_COLORS[part] || "#999999";
}
