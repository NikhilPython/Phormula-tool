export type ProfitChartExportApi = {
  getChartBase64: () => string | null;
  title: string;         
  currencySymbol: string;
};

export type SkuSheetFormat = "int" | "money" | "percent" | "text";


// export type SkuSheetModel = {
//   columns: readonly string[];
//   extraRows: string[][];
//   headerRow: Record<string, string>;
//   signRow: Record<string, string>;
//   rows: Array<Record<string, string | number>>;
// summaryRows: Array<
//   Record<string, string | number> & {
//     __bold?: boolean;
//   }
// >;
//   // formats: Record<string, "int" | "money" | "percent" | "text">;
//   formats?: Record<string, SkuSheetFormat>;
//   summaryValueKey?: string; // e.g. "profit"
// };


export type SkuSheetModel = {
  columns: readonly string[];
  extraRows: string[][];
  headerRow: Record<string, string>;
  signRow?: Record<string, string>;   // ✅ optional
  rows: Array<Record<string, string | number>>;
  summaryRows: Array<Record<string, string | number> & { __bold?: boolean }>;
  formats?: Record<string, SkuSheetFormat>;  // ✅ optional (or keep required if always present)
  summaryValueKey?: string;
};


export type SkuExportPayload = {
  tableData: any[];
  totals: any;
  currencySymbol: string;
  brandName?: string;
  companyName?: string;
  title: string;
  periodLabel: string;
  range: string;
  countryName: string;
  sheetModel?: SkuSheetModel;
};




export type TrendChartExportApi = {
  getChartBase64: () => string | null;
  title: string;
};
