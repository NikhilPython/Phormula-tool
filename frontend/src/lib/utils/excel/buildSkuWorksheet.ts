// lib/utils/excel/buildSkuWorksheet.ts
import type ExcelJS from "exceljs";
import type { SkuSheetModel } from "@/lib/utils/exportTypes";

/**
 * ✅ MUST match Dropdowns Sheet-1 builder EXACTLY
 */
export const buildSkuWorksheetFromModel = (
  ws: ExcelJS.Worksheet,
  model: SkuSheetModel
) => {
const {
  columns: originalColumns,
  extraRows,
  headerRow,
  signRow,
  rows,
  summaryRows,
  formats,
  summaryValueKey,
} = model;

const EXCEL_EXCLUDED_COLUMNS = new Set<string>([]);

  // ✅ final columns used ONLY for excel
  const columns = originalColumns.filter(
    (col) => !EXCEL_EXCLUDED_COLUMNS.has(col)
  );

  const colIndex: Record<string, number> = {};
  columns.forEach((k, i) => (colIndex[k] = i + 1)); // 1-based for ExcelJS

  // ✅ define these EARLY (used in table + summary)
  const labelKey = columns.includes("product_name") ? "product_name" : columns[0];
const valueKey =
  summaryValueKey && columns.includes(summaryValueKey)
    ? summaryValueKey
    : columns.includes("cm2_profit")
      ? "cm2_profit"
      : columns.includes("profit")
        ? "profit"
        : columns.includes("net_taxes")
          ? "net_taxes"
          : columns[columns.length - 1];

const fmtFor = (key: string) => {
  const t = formats?.[key];

  if (key === "sno") return "#,##0";

  if (t === "int") return "#,##0";
  if (t === "money") return "#,##0.00";
  if (t === "percent") return "0.00%";
  return undefined;
};

  const boldWholeRow = (rowNumber: number) => {
    const r = ws.getRow(rowNumber);
    r.eachCell({ includeEmpty: true }, (cell) => {
      cell.font = { ...(cell.font || {}), bold: true };
    });
  };

  const italicWholeRow = (rowNumber: number) => {
    const r = ws.getRow(rowNumber);
    r.eachCell({ includeEmpty: true }, (cell) => {
      cell.font = { ...(cell.font || {}), italic: true };
    });
  };

  const tableBorder: Partial<ExcelJS.Borders> = {
  top: { style: "thin", color: { argb: "FF000000" } },
  left: { style: "thin", color: { argb: "FF000000" } },
  bottom: { style: "thin", color: { argb: "FF000000" } },
  right: { style: "thin", color: { argb: "FF000000" } },
};

const addBorderToRow = (rowNumber: number) => {
  const row = ws.getRow(rowNumber);

  for (let col = 1; col <= columns.length; col++) {
    const cell = row.getCell(col);
    cell.border = tableBorder;
  }
};

  const capitalizeWords = (value: string) =>
    String(value || "")
      .toLowerCase()
      .replace(/\b\w/g, (char) => char.toUpperCase());

  // column index where CM1 Profit total exists (used in top section)
  const PROFIT_COL_INDEX = colIndex["profit"] || columns.length;

  /**
   * ===== TOP BLOCK =====
   * Your model.extraRows is:
   * 0: brand
   * 1: company
   * 2: title
   * 3: currency
   * 4: country
   * 5: platform
   */

  // 1️⃣ Title row (use extraRows[2] if present; fallback text)
  ws.addRow([extraRows?.[2]?.[0] ?? "Profit Breakup (SKU Level)"]);

  // 2️⃣ Company + Brand row (brand aligned to PROFIT col)
  const brandName = capitalizeWords((extraRows?.[0]?.[0] || "").toString());
  const companyName = capitalizeWords((extraRows?.[1]?.[0] || "").toString());

  const companyBrandRow = new Array(columns.length).fill("");
  companyBrandRow[0] = `Company Name : ${companyName}`;
  companyBrandRow[Math.max(0, PROFIT_COL_INDEX - 1)] = `${brandName}`;

  const cbRow = ws.addRow(companyBrandRow);
  cbRow.font = { bold: false };

  // 3️⃣ Currency / Country / Platform (extraRows[3..])
  for (let i = 3; i < (extraRows?.length || 0); i++) {
    ws.addRow([extraRows?.[i]?.[0] ?? ""]);
  }

  // spacer
  ws.addRow([""]);

  /**
   * ===== TABLE HEADER + SIGN ROW =====
   * ✅ Don't calculate row index. Use returned row numbers.
   */

type HeaderMeta = {
  group?: string;
  subHeader: string;
};

const isUsSkuLayout =
  headerRow?.amazon_fee === "Total Fees" ||
  headerRow?.sno === "Sno." ||
  !columns.includes("tex_and_credits");

const HEADER_META: Record<string, HeaderMeta> = {
  sno: {
    subHeader: isUsSkuLayout ? "Sno." : "S.No",
  },
  product_name: {
    subHeader: "Product Name",
  },
  sku: {
    group: isUsSkuLayout ? "Units" : undefined,
    subHeader: "SKU",
  },

  units_sold: {
    group: isUsSkuLayout ? "Units" : "Net Units Sold",
    subHeader: "Units Sold",
  },
  return_units: {
    group: isUsSkuLayout ? "Units" : "Net Units Sold",
    subHeader: "Return",
  },
  net_units_sold: {
    group: isUsSkuLayout ? "Units" : "Net Units Sold",
    subHeader: isUsSkuLayout ? "Net Units Sold" : "Total",
  },

  asp: {
    subHeader: "ASP",
  },

  product_sales: {
    group: isUsSkuLayout ? "Sales" : "Net Sales",
    subHeader: "Gross Sales",
  },
  refund_sales: {
    group: isUsSkuLayout ? "Sales" : "Net Sales",
    subHeader: "Sales - Refund",
  },
  tex_and_credits: {
    group: "Net Sales",
    subHeader: "Taxes and Credits",
  },
  net_sales: {
    group: isUsSkuLayout ? "Sales" : "Net Sales",
    subHeader: isUsSkuLayout ? "Net Sales" : "Total",
  },

  promotional_rebates: {
    group: isUsSkuLayout ? "Sales" : "Promotions",
    subHeader: "Promotions",
  },
  promotional_rebates_percentage: {
    group: isUsSkuLayout ? undefined : "Promotions",
    subHeader: "Promotions %",
  },

  cost_of_unit_sold: {
    subHeader: "COGS",
  },

  selling_fees: {
    group: isUsSkuLayout ? "Amazon Fees" : "Marketplace Fees",
    subHeader: "Selling Fees",
  },
  fba_fees: {
    group: isUsSkuLayout ? "Amazon Fees" : "Marketplace Fees",
    subHeader: "FBA Fees",
  },
  amazon_fee: {
    group: isUsSkuLayout ? "Amazon Fees" : "Marketplace Fees",
    subHeader: isUsSkuLayout ? "Total Fees" : "Total",
  },

  net_taxes: {
    group: "Other Transactions",
    subHeader: "Net Taxes",
  },
  net_credits: {
    group: "Other Transactions",
    subHeader: "Net Credits",
  },
  misc_transaction: {
    group: "Other Transactions",
    subHeader: "Misc. Transactions",
  },
  other_transactions: {
    group: "Other Transactions",
    subHeader: "Total",
  },

profit: {
  group: "CM1 Profit",
  subHeader: "Total",
},
unit_wise_profitability: {
  group: "CM1 Profit",
  subHeader: "Per Unit",
},
profit_percentage: {
  group: "CM1 Profit",
  subHeader: "%",
},

product_spend: {
  group: "Ads Spend",
  subHeader: "Sponsored Product",
},
display_spend: {
  group: "Ads Spend",
  subHeader: "Sponsored Display",
},
ads_spend: {
  group: "Ads Spend",
  subHeader: "Total",
},

acos: {
  subHeader: "ACoS %",
},

unit_wise_cm2_profitability: {
  group: "CM2 Profit",
  subHeader: "Per Unit",
},
cm2_margins: {
  group: "CM2 Profit",
  subHeader: "%",
},
cm2_profit: {
  group: "CM2 Profit",
  subHeader: "Total",
},
};

const groupHeaderValues = columns.map((k) => {
  const meta = HEADER_META[k];
  if (!meta) return headerRow?.[k] ?? k;
  return meta.group || meta.subHeader;
});

const subHeaderValues = columns.map((k) => {
  const meta = HEADER_META[k];
  if (!meta) return "";
  return meta.group ? meta.subHeader : "";
});

const signValues = columns.map((k) => signRow?.[k] ?? "");

const groupHeaderExcelRow = ws.addRow(groupHeaderValues);
const subHeaderExcelRow = ws.addRow(subHeaderValues);
const signExcelRow = ws.addRow(signValues);

const groupHeaderRowNo = groupHeaderExcelRow.number;
const subHeaderRowNo = subHeaderExcelRow.number;
const signRowNo = signExcelRow.number;

// Merge grouped headers.
let c = 1;
while (c <= columns.length) {
  const key = columns[c - 1];
  const group = HEADER_META[key]?.group;

  if (!group) {
    ws.mergeCells(groupHeaderRowNo, c, subHeaderRowNo, c);
    c++;
    continue;
  }

  let end = c;
  while (
    end + 1 <= columns.length &&
    HEADER_META[columns[end]]?.group === group
  ) {
    end++;
  }

  if (end > c) {
    ws.mergeCells(groupHeaderRowNo, c, groupHeaderRowNo, end);
  }

  c = end + 1;
}

// Black and white header styling only.
for (const rowNo of [groupHeaderRowNo, subHeaderRowNo, signRowNo]) {
  const row = ws.getRow(rowNo);

  row.eachCell({ includeEmpty: true }, (cell) => {
    cell.font = {
      bold: rowNo !== signRowNo,
      color: { argb: "FF000000" },
      size: rowNo === groupHeaderRowNo ? 11 : 10,
    };

    cell.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "FFFFFFFF" },
    };

cell.alignment = {
  horizontal: "center",
  vertical: "middle",
  wrapText: false,
  shrinkToFit: false,
};

    cell.border = {
      top: { style: "thin", color: { argb: "FF000000" } },
      left: { style: "thin", color: { argb: "FF000000" } },
      bottom: { style: "thin", color: { argb: "FF000000" } },
      right: { style: "thin", color: { argb: "FF000000" } },
    };
  });
}

ws.getRow(groupHeaderRowNo).height = 22;
ws.getRow(subHeaderRowNo).height = 22;
ws.getRow(signRowNo).height = 18;

columns.forEach((key, index) => {
  const col = ws.getColumn(index + 1);

  const label =
    HEADER_META[key]?.subHeader ||
    HEADER_META[key]?.group ||
    headerRow?.[key] ||
    key;

  const minWidthByKey: Record<string, number> = {
    sno: 8,
    product_name: 24,
    sku: 18,

    units_sold: 14,
    return_units: 12,
    net_units_sold: 12,

    asp: 10,

    product_sales: 14,
    refund_sales: 16,
    tex_and_credits: 18,
    net_sales: 14,

    promotional_rebates: 16,
    promotional_rebates_percentage: 18,

    cost_of_unit_sold: 12,

    selling_fees: 16,
    fba_fees: 14,
    amazon_fee: 14,

    net_taxes: 14,
    net_credits: 14,
    misc_transaction: 20,
    other_transactions: 14,

    profit: 14,
    unit_wise_profitability: 14,
    profit_percentage: 10,

product_spend: 18,
display_spend: 18,
ads_spend: 14,
acos: 12,

unit_wise_cm2_profitability: 14,
cm2_margins: 12,
cm2_profit: 14,
  };

  col.width = minWidthByKey[key] || Math.max(String(label).length + 4, 12);
});

  /**
   * ===== TABLE BODY =====
   */
for (const r of rows || []) {
  const excelRow = ws.addRow(columns.map((k) => (r as any)?.[k] ?? ""));

  // ✅ Add border to every product table row
  addBorderToRow(excelRow.number);

  // ✅ Bold Total / Others rows (based on product_name)
  const name = String((r as any)?.[labelKey] ?? "").trim().toLowerCase();
  if (name === "total" || name === "others" || name === "grand total") {
    boldWholeRow(excelRow.number);
  }
}

  // spacer
  ws.addRow([""]);

  /**
   * ===== SUMMARY =====
   */

  // ✅ percent-only summary labels (Dropdowns behavior)
  const PERCENT_SUMMARY_LABELS = new Set([
    "CM2 Margins",
    "CM2 Profit %",
    "TACoS (Total Advertising Cost of Sale)",
    "Reimbursement vs CM2 Margins",
    "Reimbursement vs Sales",
  ]);

  // ✅ rows that should keep title but BLANK value
  const SUMMARY_NO_VALUE_LABELS = new Set([
    "Cost of Advertisement",
    "Other Transactions (-)",
    "Other Transactions",
  ]);

  const percentSummaryRowNumbers: number[] = [];

  for (const sr of summaryRows || []) {
    let label = String((sr as any)?.[labelKey] ?? "").trim();
    let value: any = (sr as any)?.[valueKey] ?? "";

    if (label === "Reimbursement for lost Inventory") {
      label = "Reimbursement for lost Inventory (+)";
    }

    const cleanLabel = label.replace(/^\(\+\)\s*/i, "").trim();
    const isPercentRow = PERCENT_SUMMARY_LABELS.has(cleanLabel);

    // UI gives percent number like 27.37 -> Excel needs 0.2737
    if (isPercentRow && typeof value === "number") {
      value = value / 100;
    }

    // remove value only for parent rows
    if (SUMMARY_NO_VALUE_LABELS.has(cleanLabel)) {
      value = "";
    }

    const line = new Array(columns.length).fill("");
    line[colIndex[labelKey] - 1] = label;
    line[colIndex[valueKey] - 1] = value;

    const excelRow = ws.addRow(line);

    if ((sr as any).__bold) {
      boldWholeRow(excelRow.number);
    }

    if (isPercentRow) {
      percentSummaryRowNumbers.push(excelRow.number);
    }
  }

  /**
   * ===== COLUMN FORMATS =====
   */
  for (const k of columns) {
    const idx = colIndex[k];
    const nf = fmtFor(k);
    if (nf) ws.getColumn(idx).numFmt = nf;
  }

  // ✅ re-apply percent formatting AFTER column formats
  for (const r of percentSummaryRowNumbers) {
    ws.getRow(r).getCell(colIndex[valueKey]).numFmt = "0.00%";
  }
};
