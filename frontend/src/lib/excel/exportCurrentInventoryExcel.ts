import * as XLSX from "xlsx-js-style";
import ExcelJS from "exceljs";
import { saveAs } from "file-saver";

/* =========================
   Currency helpers
========================= */
const countryToCurrencyCode = (country: string) => {
  const c = (country || "").toLowerCase();
  if (c === "uk") return "GBP";
  if (c === "us") return "USD";
  if (c === "ca") return "CAD";
  if (c === "eu") return "EUR";
  return "";
};

const currencyCodeToSymbol = (code: string) => {
  const c = (code || "").toUpperCase();
  if (c === "USD") return "$";
  if (c === "GBP") return "£";
  if (c === "EUR") return "€";
  if (c === "CAD") return "C$";
  if (c === "AUD") return "A$";
  if (c === "INR") return "₹";
  if (c === "AED") return "د.إ";
  if (c === "SAR") return "﷼";
  return c || "";
};

const getCurrencySymbol = ({
  countryName,
  homeCurrencyCode,
}: {
  countryName: string;
  homeCurrencyCode?: string;
}) => {
  const isGlobal = (countryName || "").toLowerCase() === "global";
  const countryCode = countryToCurrencyCode(countryName);
  const codeToUse = isGlobal ? homeCurrencyCode : countryCode || homeCurrencyCode;
  return currencyCodeToSymbol(codeToUse || "");
};

/* =========================
   Top header block helpers
========================= */
const buildTopAoA = ({
  headerCount,
  title,
  companyName,
  brandName,
  anchorCol1Based,
  extraLines = [],
}: {
  headerCount: number;
  title: string;
  companyName: string;
  brandName: string;
  anchorCol1Based: number;
  extraLines?: string[];
}) => {
  const aoa: any[][] = [];

  // Row 1: Title in A1 only (no merge, no bold)
  const titleRow = new Array(headerCount).fill("");
  titleRow[0] = title || "";
  aoa.push(titleRow);

  // Row 2: Company left, brand on the right (anchored)
  const row2 = new Array(headerCount).fill("");
  row2[0] = `Company Name : ${companyName || ""}`;

  const anchor0 = Math.max(0, anchorCol1Based - 1);
  row2[Math.min(headerCount - 1, anchor0)] = brandName || "";
  aoa.push(row2);

  for (const line of extraLines) {
    const r = new Array(headerCount).fill("");
    r[0] = line;
    aoa.push(r);
  }

  // blank row
  aoa.push(new Array(headerCount).fill(""));
  return aoa;
};

const applyTopStyles = (
  ws: XLSX.WorkSheet,
  headerCount: number,
  anchorCol1Based: number
) => {
  // Title same look as other header text (not bold)
  const a1 = XLSX.utils.encode_cell({ r: 0, c: 0 });
  if (ws[a1]) {
    ws[a1].s = {
      font: { bold: false, sz: 11 },
      alignment: { horizontal: "left", vertical: "center" },
    };
  }

  // Row 2: company left, brand right
  for (let c = 0; c < headerCount; c++) {
    const addr = XLSX.utils.encode_cell({ r: 1, c });
    if (!ws[addr]) continue;
    ws[addr].s = {
      font: { bold: false, sz: 11 },
      alignment: { horizontal: c === 0 ? "left" : "right", vertical: "center" },
    };
  }

  // Brand a bit stronger
  const anchor0 = Math.max(0, anchorCol1Based - 1);
  const bAddr = XLSX.utils.encode_cell({
    r: 1,
    c: Math.min(headerCount - 1, anchor0),
  });
  if (ws[bAddr]) {
    ws[bAddr].s = {
      font: { bold: true, sz: 11 },
      alignment: { horizontal: "right", vertical: "center" },
    };
  }

  ws["!rows"] = ws["!rows"] || [];
  ws["!rows"][0] = { hpt: 18 };
  ws["!rows"][1] = { hpt: 18 };
};

/* =========================
   Shared helpers
========================= */
const safeSheetName = (name: string) => {
  const n = (name || "Sheet1").trim();
  // Excel limits: 31 chars, cannot contain : \ / ? * [ ]
  return (
    n
      .replace(/[:\\/?*\[\]]/g, " ")
      .replace(/\s+/g, " ")
      .slice(0, 31)
      .trim() || "Sheet1"
  );
};

// -------------------------
// Number / percent helpers
// -------------------------
const isNumber = (v: any) => typeof v === "number" && isFinite(v);

const toNumberLoose = (v: any): number | null => {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number" && isFinite(v)) return v;

  const s = String(v).trim().replace(/,/g, "").replace(/%/g, "");
  const n = Number(s);
  return isFinite(n) ? n : null;
};

// Decide if a summary label should be treated as a percentage
const isPercentLabel = (label: string) => {
  const s = (label || "").toLowerCase();
  return (
    s.includes("%") ||
    s.includes("margin") ||
    s.includes("tacos") ||
    s.includes("rate") ||
    s.includes("ratio") ||
    s.includes(" vs ")
  );
};

/* =========================
   Main export
========================= */
export function exportCurrentInventoryExcel(params: {
  filename: string;

  // heading like: "Amazon UK - Current Inventory - Jan'26"
  titleLine: string;

  countryName: string; // "uk" | "us" | "global"
  titleCountry: string; // "UK" | "US" | "Global"
  platformLabel?: string; // "Amazon" etc

  periodLabel: string; // e.g. "Jan'26" or "Jan 2026"
  companyName: string;
  brandName: string;
  pieImageBase64?: string | null;

  homeCurrencyCode?: string; // e.g. "USD"
  dataRows: Record<string, any>[]; // normalized rows (clean headers)
}) {
const {
    filename,
    titleLine = "",
    countryName = "",
    titleCountry = "",
    platformLabel = "Amazon",
    periodLabel = "",
    companyName = "",
    brandName = "",
    homeCurrencyCode,
    dataRows,
    pieImageBase64,
  } = params;
  if (!dataRows?.length) return;

  const headers = Object.keys(dataRows[0] || {});
  const headerCount = headers.length || 1;

  // Anchor brand to last column for inventory
  const ANCHOR_COL_1_BASED = headerCount;

  const currencySymbol = getCurrencySymbol({ countryName, homeCurrencyCode });

  const topExtraLines = [
    `Country : ${titleCountry}`,
    `Platform : ${platformLabel}`,
    `Currency : ${currencySymbol}`,
    `Period : ${periodLabel}`,
  ];

  const topAoA = buildTopAoA({
    headerCount,
    title: titleLine,
    companyName,
    brandName,
    anchorCol1Based: ANCHOR_COL_1_BASED,
    extraLines: topExtraLines,
  });

  const headerRowIndex = topAoA.length;

  const bodyAoA = dataRows.map((r) => headers.map((h) => (r as any)[h] ?? ""));

  const sheetAoA = [...topAoA, headers, ...bodyAoA];
  const ws = XLSX.utils.aoa_to_sheet(sheetAoA);

  // Freeze under table header
  ws["!freeze"] = { xSplit: 0, ySplit: headerRowIndex + 1 };

  // Auto widths
  ws["!cols"] = headers.map((h) => ({
    wch: Math.min(Math.max(String(h).length + 2, 12), 48),
  }));

  applyTopStyles(ws, headerCount, ANCHOR_COL_1_BASED);

  // Force numeric cells to 2 decimals
  const range = XLSX.utils.decode_range(ws["!ref"] || "A1:A1");
  for (let r = range.s.r; r <= range.e.r; r++) {
    for (let c = range.s.c; c <= range.e.c; c++) {
      const addr = XLSX.utils.encode_cell({ r, c });
      const cell = ws[addr];
      if (!cell) continue;
      if (isNumber(cell.v)) cell.z = "#,##0.00";
    }
  }

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Current Inventory");
  XLSX.writeFile(wb, filename);
}


/* =========================
   P&L Productwise Breakdown MTD export
========================= */
export function exportPnLProductwiseBreakdownMtdExcel(params: {
  filename: string;
  titleLine: string;

  countryName: string;
  titleCountry: string;
  platformLabel?: string;

  periodLabel: string;
  companyName: string;
  brandName: string;

  homeCurrencyCode?: string;
  dataRows: Record<string, any>[];

  // ✅ now supports nested-like rows via indent + bold
  summaryRows?: { label: string; value: any; indent?: number; bold?: boolean }[];
}) {
  const {
    filename,
    titleLine,
    countryName,
    titleCountry,
    platformLabel = "Amazon",
    periodLabel,
    companyName,
    brandName,
    homeCurrencyCode,
    dataRows,
    summaryRows,
  } = params;

  if (!dataRows?.length) return;

  const headers = Object.keys(dataRows[0] || {});
  const headerCount = headers.length || 1;

  // Anchor brand above "CM1 Profit" (fallback: last column)
  const cm1ProfitCol0 = headers.indexOf("CM1 Profit");
  const ANCHOR_COL_1_BASED = cm1ProfitCol0 >= 0 ? cm1ProfitCol0 + 1 : headerCount;

  const currencySymbol = getCurrencySymbol({ countryName, homeCurrencyCode });

  const topExtraLines = [
    `Country : ${titleCountry}`,
    `Platform : ${platformLabel}`,
    `Currency : ${currencySymbol}`,
    `Period : ${periodLabel}`,
  ];

  const topAoA = buildTopAoA({
    headerCount,
    title: titleLine,
    companyName,
    brandName,
    anchorCol1Based: ANCHOR_COL_1_BASED,
    extraLines: topExtraLines,
  });

  const headerRowIndex = topAoA.length;
  const bodyAoA = dataRows.map((r) => headers.map((h) => (r as any)[h] ?? ""));

  // Summary: label under "Product Name", value under "CM1 Profit"
  const productNameCol = headers.indexOf("Product Name");
  const cm1ProfitCol = headers.indexOf("CM1 Profit");

  const labelCol = productNameCol >= 0 ? productNameCol : 0;
  const valueCol =
    cm1ProfitCol >= 0 ? cm1ProfitCol : Math.min(1, Math.max(0, headerCount - 1));

  // ✅ mimic SKU behavior
  const PERCENT_SUMMARY_LABELS = new Set([
    "CM2 Margins",
    "TACoS (Total Advertising Cost of Sale)",
    "Reimbursement vs CM2 Margins",
    "Reimbursement vs Sales",
  ]);

  // ✅ parent rows must keep label but blank value
  const SUMMARY_NO_VALUE_LABELS = new Set([
    "Cost of Advertisement",
    "Other Transactions",
  ]);

  const summaryAoA: any[][] = [];
  const percentSummaryRowIndices: number[] = [];
  const boldSummaryRowIndices: number[] = [];

  if (summaryRows?.length) {
    // spacer
    summaryAoA.push(new Array(headerCount).fill(""));

    summaryRows.forEach((s) => {
      const row = new Array(headerCount).fill("");

      const rawLabel = String(s?.label ?? "").trim();
      const cleanLabel = rawLabel.replace(/^\(\+\)\s*/i, "").trim(); // keep consistent with SKU logic
      const indent = Math.max(0, Number(s?.indent ?? 0));
      const excelLabel = `${"  ".repeat(indent)}${rawLabel}`;

      // value parse
      let v: any = s?.value;
      const n = toNumberLoose(v);

      const isPercentRow =
        PERCENT_SUMMARY_LABELS.has(cleanLabel) || isPercentLabel(rawLabel);

      // blank value for parent rows
      if (SUMMARY_NO_VALUE_LABELS.has(cleanLabel)) {
        v = "";
      } else if (n === null) {
        v = "";
      } else if (isPercentRow) {
        // UI gives 27.37 -> excel needs 0.2737
        v = n > 1 ? n / 100 : n;
      } else {
        v = n;
      }

      row[labelCol] = excelLabel;
      row[valueCol] = v;

      summaryAoA.push(row);

      // we'll style these later
      const aoaRowIndexInSheet =
        topAoA.length + 1 + bodyAoA.length + (summaryAoA.length - 1); // 0-based AOA -> we use same index later
      if (isPercentRow) percentSummaryRowIndices.push(aoaRowIndexInSheet);
      if (s?.bold) boldSummaryRowIndices.push(aoaRowIndexInSheet);
    });
  }

  const sheetAoA = [...topAoA, headers, ...bodyAoA, ...summaryAoA];
  const ws = XLSX.utils.aoa_to_sheet(sheetAoA);

  // Freeze under table header
  ws["!freeze"] = { xSplit: 0, ySplit: headerRowIndex + 1 };

  // Auto widths
  ws["!cols"] = headers.map((h) => ({
    wch: Math.min(Math.max(String(h).length + 2, 12), 48),
  }));

  applyTopStyles(ws, headerCount, ANCHOR_COL_1_BASED);

  // Bold table header row
  for (let c = 0; c < headerCount; c++) {
    const addr = XLSX.utils.encode_cell({ r: headerRowIndex, c });
    if (ws[addr]) {
      ws[addr].s = {
        ...(ws[addr].s || {}),
        font: { bold: true, sz: 11 },
        alignment: { horizontal: "center", vertical: "center" },
      };
    }
  }

  // Bold total row (last data row)
  const totalRowIndex = headerRowIndex + 1 + bodyAoA.length - 1;
  for (let c = 0; c < headerCount; c++) {
    const addr = XLSX.utils.encode_cell({ r: totalRowIndex, c });
    if (ws[addr]) {
      ws[addr].s = { ...(ws[addr].s || {}), font: { bold: true, sz: 11 } };
    }
  }

  // Force all numeric cells to 2 decimals (default)
  const range = XLSX.utils.decode_range(ws["!ref"] || "A1:A1");
  for (let r = range.s.r; r <= range.e.r; r++) {
    for (let c = range.s.c; c <= range.e.c; c++) {
      const addr = XLSX.utils.encode_cell({ r, c });
      const cell = ws[addr];
      if (!cell) continue;
      if (isNumber(cell.v)) cell.z = "#,##0.00";
    }
  }

  // ✅ Bold only selected summary rows (parents / key rows)
  for (const r of boldSummaryRowIndices) {
    for (let c = 0; c < headerCount; c++) {
      const addr = XLSX.utils.encode_cell({ r, c });
      if (ws[addr]) {
        ws[addr].s = {
          ...(ws[addr].s || {}),
          font: { ...(ws[addr].s?.font || {}), bold: true, sz: 11 },
        };
      }
    }
  }

  // ✅ Percent format override on summary value cells
  for (const r of percentSummaryRowIndices) {
    const valueAddr = XLSX.utils.encode_cell({ r, c: valueCol });
    const cell = ws[valueAddr];
    if (!cell) continue;
    if (isNumber(cell.v)) cell.z = "0.00%";
  }

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, safeSheetName("P&L Productwise MTD"));
  XLSX.writeFile(wb, filename);
}




export function exportInventoryReconExcel(params: {
  filename: string;
  titleLine: string;

  countryName: string;
  titleCountry: string;
  platformLabel?: string;

  // expects already formatted like Dec'25
  periodLabel: string;

  companyName: string;
  brandName: string;

  dataRows: Record<string, any>[]; // YOU control headers + values
}) {
  const {
    filename,
    titleLine,
    titleCountry,
    platformLabel = "Amazon",
    periodLabel,
    companyName,
    brandName,
    dataRows,
  } = params;

  if (!dataRows?.length) return;

  const headers = Object.keys(dataRows[0] || {});
  const headerCount = headers.length || 1;

  /* =========================
     Top block (same layout)
  ========================= */
  const topAoA: any[][] = [];

  // Row 1: Title (merged later)
  const r1 = new Array(headerCount).fill("");
  r1[0] = titleLine || "";
  topAoA.push(r1);

  // Row 2: Company left, Brand right
  const r2 = new Array(headerCount).fill("");
  r2[0] = `Company Name : ${companyName || ""}`;
  r2[headerCount - 1] = `${brandName || ""}`;
  topAoA.push(r2);

  // Row 3-5: meta
  topAoA.push(Object.assign(new Array(headerCount).fill(""), { 0: `Country : ${titleCountry}` }));
  topAoA.push(Object.assign(new Array(headerCount).fill(""), { 0: `Platform : ${platformLabel}` }));
  topAoA.push(Object.assign(new Array(headerCount).fill(""), { 0: `Period : ${periodLabel}` }));

  // Spacer
  topAoA.push(new Array(headerCount).fill(""));

  const headerRowIndex = topAoA.length; // where column headers will be placed

  /* =========================
     Body
  ========================= */
  const bodyAoA = dataRows.map((r) => headers.map((h) => r?.[h] ?? ""));
  const sheetAoA = [...topAoA, headers, ...bodyAoA];

  const ws = XLSX.utils.aoa_to_sheet(sheetAoA);

  /* =========================
     Sheet options
  ========================= */
  ws["!merges"] = ws["!merges"] || [];
  // merge title row A1 -> last col
  ws["!merges"].push({ s: { r: 0, c: 0 }, e: { r: 0, c: headerCount - 1 } });

  // freeze below column header row
  ws["!freeze"] = { xSplit: 0, ySplit: headerRowIndex + 1 };

  // widths
  ws["!cols"] = headers.map((h) => ({
    wch: Math.min(Math.max(String(h).length + 2, 12), 55),
  }));

  // align company + brand
  const companyAddr = XLSX.utils.encode_cell({ r: 1, c: 0 });
  if (ws[companyAddr]) ws[companyAddr].s = { ...(ws[companyAddr].s || {}), alignment: { horizontal: "left" } };

  const brandAddr = XLSX.utils.encode_cell({ r: 1, c: headerCount - 1 });
  if (ws[brandAddr]) ws[brandAddr].s = { ...(ws[brandAddr].s || {}), alignment: { horizontal: "right" } };

  // bold column headers
  for (let c = 0; c < headerCount; c++) {
    const addr = XLSX.utils.encode_cell({ r: headerRowIndex, c });
    if (ws[addr]) {
      ws[addr].s = {
        ...(ws[addr].s || {}),
        font: { bold: true, sz: 11 },
        alignment: { horizontal: "center", vertical: "center" },
      };
    }
  }

  /* =========================
     Formatting rules
     - Keep S. No. as TEXT (no 1.00)
     - Numbers => #,##0.00
  ========================= */
  const snoCol0 = headers.findIndex((h) => String(h).toLowerCase().includes("s. no"));
  const range = XLSX.utils.decode_range(ws["!ref"] || "A1:A1");

  for (let r = range.s.r; r <= range.e.r; r++) {
    for (let c = range.s.c; c <= range.e.c; c++) {
      // ✅ don't coerce Sno column
      if (c === snoCol0) continue;

      const addr = XLSX.utils.encode_cell({ r, c });
      const cell = ws[addr];
      if (!cell) continue;

      const n = toNumberLoose(cell.v);
      if (n !== null) {
        cell.t = "n";
        cell.v = n;
        cell.z = "#,##0.00";
      }
    }
  }

  /* =========================
     Bold TOTAL / GRAND TOTAL rows (after numeric coercion)
  ========================= */
  const applyBoldRow = (rowIndex: number) => {
    const ref = ws["!ref"];
    if (!ref) return;
    const rr = XLSX.utils.decode_range(ref);

    for (let c = rr.s.c; c <= rr.e.c; c++) {
      const addr = XLSX.utils.encode_cell({ r: rowIndex, c });
      const cell = ws[addr] || { t: "s", v: "" };
      ws[addr] = {
        ...cell,
        s: {
          ...((cell as any).s || {}),
          font: { ...(((cell as any).s)?.font || {}), bold: true, sz: 11 },
        },
      };
    }
  };

  const productNameCol0 = headers.findIndex((h) =>
    String(h).toLowerCase().includes("product name")
  );

  if (productNameCol0 >= 0) {
    const totalCandidates = new Set(["total", "grand total"]);

    const startBodyRow = headerRowIndex + 1; // first data row
    const endBodyRow = headerRowIndex + bodyAoA.length;

    for (let r = startBodyRow; r <= endBodyRow; r++) {
      const addr = XLSX.utils.encode_cell({ r, c: productNameCol0 });
      const v = String(ws[addr]?.v ?? "").trim().toLowerCase();
      if (totalCandidates.has(v)) applyBoldRow(r);
    }
  }

  /* =========================
     Save
  ========================= */
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, safeSheetName("Inventory Recon"));
  XLSX.writeFile(wb, filename);
}
