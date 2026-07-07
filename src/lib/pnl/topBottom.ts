// types/topBottom.ts
export type TopBottomRow = {
  product_name: string;
  profit: string;
  profitMix: string;
  salesMix: string;
  cm1_per_unit: string;
};

export type TopBottomTotals = {
  profit: string;
  profitMix: string;
  salesMix: string;
  avg_cm1: string;
};

export type TopBottomData = {
  rows: TopBottomRow[];
  totals: TopBottomTotals;
};
