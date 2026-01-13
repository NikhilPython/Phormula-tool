export type ProfitChartExportApi = {
  getChartBase64: () => string | null;
  title: string;         
  currencySymbol: string;
};




export type SkuSheetModel = {
  columns: readonly string[];
  extraRows: string[][];
  headerRow: Record<string, string>;
  signRow: Record<string, string>;
  rows: Array<Record<string, string | number>>;
summaryRows: Array<
  Record<string, string | number> & {
    __bold?: boolean;
  }
>;

  formats: Record<string, "int" | "money" | "percent" | "text">;
  summaryValueKey?: string; // e.g. "profit"
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
