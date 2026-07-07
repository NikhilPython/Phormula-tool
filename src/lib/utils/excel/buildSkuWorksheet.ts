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
  } = model;

  // ❌ columns to REMOVE from Excel (this is what Dropdowns does)
  const EXCEL_EXCLUDED_COLUMNS = new Set(["amazon_fee", "other_transactions"]);

  // ✅ final columns used ONLY for excel
  const columns = originalColumns.filter(
    (col) => !EXCEL_EXCLUDED_COLUMNS.has(col)
  );

  const colIndex: Record<string, number> = {};
  columns.forEach((k, i) => (colIndex[k] = i + 1)); // 1-based for ExcelJS

  // ✅ define these EARLY (used in table + summary)
  const labelKey = columns.includes("product_name") ? "product_name" : columns[0];
  const valueKey =
    columns.includes("profit")
      ? "profit"
      : columns.includes("net_taxes")
      ? "net_taxes"
      : columns[columns.length - 1];

  const fmtFor = (key: string) => {
    const t = formats?.[key];
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

  const capitalizeWords = (value: string) =>
    String(value || "")
      .toLowerCase()
      .replace(/\b\w/g, (char) => char.toUpperCase());

  // column index where CM1 Profit Margin exists (used in top section)
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

  // ---- header row
  const headerExcelRow = ws.addRow(columns.map((k) => headerRow?.[k] ?? k));
  boldWholeRow(headerExcelRow.number);

  // ---- sign row
  const signExcelRow = ws.addRow(columns.map((k) => signRow?.[k] ?? ""));
  italicWholeRow(signExcelRow.number);

  /**
   * ===== TABLE BODY =====
   */
  for (const r of rows || []) {
    const excelRow = ws.addRow(columns.map((k) => (r as any)?.[k] ?? ""));

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
