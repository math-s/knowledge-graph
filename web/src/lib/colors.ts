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

export const SOURCE_COLORS = {
  bible: "#59A14F",
  author: "#B07AA1",
  document: "#EDC948",
};

export const THEME_COLORS: Record<string, string> = {
  trinity: "#6A3D9A",
  eucharist: "#B15928",
  baptism: "#1F78B4",
  prayer: "#F28E2B",
  creation: "#33A02C",
  salvation: "#E31A1C",
  mary: "#6BAED6",
  sin: "#7F0000",
  grace: "#FFD700",
  commandments: "#A6CEE3",
  resurrection: "#FB9A99",
  church: "#4E79A7",
  sacraments: "#E15759",
  "moral-life": "#76B7B2",
  scripture: "#B2DF8A",
};

export function getPartColor(part: string): string {
  return PART_COLORS[part] || "#999999";
}
