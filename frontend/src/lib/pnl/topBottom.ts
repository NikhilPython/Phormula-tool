// src/lib/pnl/topBottom.ts

export type TopBottomRow = {
  product_name: string;
  sku?: string;
  profit: string;
  profitMix: string;
  salesMix: string;

  // Dynamic:
  // CM1 per unit when CM2 is not available
  // CM2 per unit when CM2 is available
  per_unit: string;
};

export type TopBottomTotals = {
  profit: string;
  profitMix: string;
  salesMix: string;

  // Dynamic:
  // Avg CM1 per unit when CM2 is not available
  // Avg CM2 per unit when CM2 is available
  avg_per_unit: string;
};

export type TopBottomData = {
  rows: TopBottomRow[];
  totals: TopBottomTotals;
};