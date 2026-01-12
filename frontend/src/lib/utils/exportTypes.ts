export type ProfitChartExportApi = {
  getChartBase64: () => string | null;
  title: string;         
  currencySymbol: string;
};




export type SkuSheetModel = {
  columns: readonly string[];                 // ["product_name","quantity",...]
  extraRows: string[][];                      // your meta rows
  headerRow: Record<string, string>;          // column labels
  signRow: Record<string, string>;            // (+)/(-) row
  rows: Array<Record<string, string | number>>;
  summaryRows: Array<Record<string, string | number>>;
  formats: Record<string, "int" | "money" | "percent" | "text">; // excel formats per key
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
