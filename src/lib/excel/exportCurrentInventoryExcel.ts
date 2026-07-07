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



/** Accepts "data:image/png;base64,...." or raw base64 */
const parseBase64 = (b64?: string | null) => {
  if (!b64) return null;
  const m = b64.match(/^data:(image\/\w+);base64,(.*)$/i);
  if (m) return { mime: m[1], base64: m[2] };
  // assume already base64 png
  return { mime: "image/png", base64: b64 };
};


export async function exportInventoryReconExcel(params: {
  filename: string;
  titleLine: string;

  countryName: string;
  titleCountry: string;
  platformLabel?: string;

  periodLabel: string;

  companyName: string;
  brandName: string;

  dataRows: Record<string, any>[];

  breakupChartBase64?: string | null;
ageingChartBase64?: string | null;// (unused now)
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
  breakupChartBase64 = null,
  ageingChartBase64 = null,
} = params;

  if (!dataRows?.length) return;

  const headers = Object.keys(dataRows[0] || {});
  const headerCount = headers.length || 1;

  const snoCol0 = headers.findIndex((h) =>
    String(h).toLowerCase().includes("s. no")
  );
  const productNameCol0 = headers.findIndex((h) =>
    String(h).toLowerCase().includes("product name")
  );

  const wb = new ExcelJS.Workbook();
  wb.creator = "Skinelements";
  wb.created = new Date();

  /* =========================
     Sheet 1: Inventory Recon
  ========================= */
  const ws1 = wb.addWorksheet(safeSheetName("Inventory Recon"), {
    views: [{ state: "frozen", xSplit: 0, ySplit: 7 }],
  });

  ws1.mergeCells(1, 1, 1, headerCount);
  ws1.getCell(1, 1).value = titleLine || "";
  ws1.getCell(1, 1).font = { bold: false };
  ws1.getCell(1, 1).alignment = { horizontal: "left", vertical: "middle" };

  ws1.getCell(2, 1).value = `Company Name : ${companyName || ""}`;
  ws1.getCell(2, 1).alignment = { horizontal: "left" };

  ws1.getCell(2, headerCount).value = `${brandName || ""}`;
  ws1.getCell(2, headerCount).alignment = { horizontal: "right" };
  ws1.getCell(2, headerCount).font = { bold: true };

  ws1.getCell(3, 1).value = `Country : ${titleCountry}`;
  ws1.getCell(4, 1).value = `Platform : ${platformLabel}`;
  ws1.getCell(5, 1).value = `Period : ${periodLabel}`;

  const headerRowNumber = 7;
  const headerRow = ws1.getRow(headerRowNumber);

  headers.forEach((h, i) => {
    const cell = headerRow.getCell(i + 1);
    cell.value = h;
    cell.font = { bold: true, size: 11 };
    cell.alignment = {
      horizontal: "center",
      vertical: "middle",
      wrapText: true,
    };
  });
  headerRow.height = 18;

  const startDataRow = headerRowNumber + 1;

  dataRows.forEach((r, idx) => {
    const row = ws1.getRow(startDataRow + idx);
    headers.forEach((h, c0) => {
      const cell = row.getCell(c0 + 1);
      const v = r?.[h] ?? "";

      if (c0 === snoCol0) {
        cell.value = v === null || v === undefined ? "" : String(v);
        cell.numFmt = "@";
        cell.alignment = { horizontal: "center" };
        return;
      }

      const n = toNumberLoose(v);
      if (n !== null) {
        cell.value = n;
        cell.numFmt = "#,##0.00";
        cell.alignment = { horizontal: "center" };
      } else {
        cell.value = v;
        cell.alignment = { horizontal: "center" };
      }
    });
  });

  ws1.views = [{ state: "frozen", xSplit: 0, ySplit: headerRowNumber }];

  ws1.columns = headers.map((h) => ({
    width: Math.min(Math.max(String(h).length + 2, 12), 55),
  }));

  if (productNameCol0 >= 0) {
    const totalCandidates = new Set(["total", "grand total"]);
    for (let i = 0; i < dataRows.length; i++) {
      const rowNum = startDataRow + i;
      const cellVal = String(
        ws1.getCell(rowNum, productNameCol0 + 1).value ?? ""
      )
        .trim()
        .toLowerCase();

      if (totalCandidates.has(cellVal)) {
        const row = ws1.getRow(rowNum);
        row.eachCell((cell) => {
          cell.font = { ...(cell.font || {}), bold: true, size: 11 };
        });
      }
    }
  }

  /* =========================
   Sheet 2: Inventory Charts
========================= */
const ws2 = wb.addWorksheet(safeSheetName("Inventory Charts"));

ws2.getCell("A1").value = titleLine || "";
ws2.getCell("A1").font = { bold: false };
ws2.getCell("A2").value = `Company Name : ${companyName || ""}`;
ws2.getCell("N2").value = `${brandName || ""}`;
ws2.getCell("N2").alignment = { horizontal: "right" };
ws2.getCell("N2").font = { bold: true };
ws2.getCell("A3").value = `Country : ${titleCountry}`;
ws2.getCell("A4").value = `Platform : ${platformLabel}`;
ws2.getCell("A5").value = `Period : ${periodLabel}`;

const breakupImg = parseBase64(breakupChartBase64);
const ageingImg = parseBase64(ageingChartBase64);

let rowCursor = 7;

const addImageBlock = (
  label: string,
  img: { mime: string; base64: string } | null
) => {
  ws2.getCell(`A${rowCursor}`).value = label;
  ws2.getCell(`A${rowCursor}`).font = { bold: true, size: 12 };
  rowCursor += 1;

  if (!img) {
    ws2.getCell(`A${rowCursor}`).value = `No chart image available: ${label}`;
    rowCursor += 3;
    return;
  }

  const ext = img.mime.toLowerCase().includes("jpeg") ? "jpeg" : "png";

  const imageId = wb.addImage({
    base64: `data:${img.mime};base64,${img.base64}`,
    extension: ext as "png" | "jpeg",
  });

  ws2.addImage(imageId, {
    tl: { col: 0, row: rowCursor - 1 },
    ext: { width: 1400, height: 520 },
  });

  rowCursor += 26;
};

addImageBlock("Inventory Breakup", breakupImg);
rowCursor += 2;
addImageBlock("Inventory Ageing", ageingImg);

  /* =========================
     Save
  ========================= */
  const buf = await wb.xlsx.writeBuffer();
  saveAs(
    new Blob([buf], {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }),
    filename
  );
}


export async function exportProductwiseTrendsExcel(params: {
  filename: string;

  // Header content (same structure as exportInventoryReconExcel)
  titleLine: string;          // e.g. "Amazon UK - Productwise Trends - Jan'26"
  titleCountry: string;       // "UK" | "US" | "Global"
  platformLabel?: string;     // "Amazon"
  periodLabel: string;        // "Jan'26"

  companyName: string;
  brandName: string;

  currencyLabel?: string;     // e.g. "$" / "£" / "₹" (symbol)

    // ✅ NEW: CountryCard-like tiles (Sheet 1)
  countryCards?: Array<{
    countryKey: string;
    countryLabel: string;

    totalSales: number;
    totalUnits: number;
    totalProfit: number;

    avgMonthlySales: number;
    avgSellingPrice: number;
    cm1ProfitPct: number;

    bestSalesMonth: string;
    bestSalesValue: number;

    bestUnitsMonth: string;
    bestUnitsValue: number;

    bestProfitMonth: string;
    bestProfitValue: number;
  }>;

  // Optional: table data (Sheet 1)
  table?: {
    headers: string[];        // e.g. ["Month", "UK Net Sales", "Global Net Sales", ...]
    rows: (string | number | null)[][]; // each row matches headers length
  };

  // Sheet 2 images (base64)
  salesCm1ChartBase64?: string | null; // "data:image/png;base64,...." OR raw base64
  unitsChartBase64?: string | null;    // "data:image/png;base64,...." OR raw base64

  // Optional: image sizing
  chartWidth?: number;        // default 1400
  chartHeight?: number;       // default 520
}) {
  const {
    filename,
    titleLine,
    titleCountry,
    platformLabel = "Amazon",
    periodLabel,
    companyName,
    brandName,
      currencyLabel = "",
    countryCards = [],
    table,
    salesCm1ChartBase64 = null,
    unitsChartBase64 = null,
    chartWidth = 900,
    chartHeight = 360,
  } = params;

  const wb = new ExcelJS.Workbook();
  wb.creator = "Skinelements";
  wb.created = new Date();

  // -------------------------
  // Sheet 1: Performance (Header + Optional Table)
  // -------------------------
  const ws1 = wb.addWorksheet(safeSheetName("Performance"));

const headerCount = Math.max(13, table?.headers?.length || 0);

const moneyFmt0 = `${currencyLabel || ""}#,##0`;
const moneyFmt2 = `${currencyLabel || ""}#,##0.00`;
const unitsFmt0 = `#,##0`;
const pctFmt = `0.00"%"`;

const setBoxBorder = (ws: ExcelJS.Worksheet, r1: number, c1: number, r2: number, c2: number) => {
  for (let r = r1; r <= r2; r++) {
    for (let c = c1; c <= c2; c++) {
      ws.getCell(r, c).border = {
        top: { style: "thin" },
        left: { style: "thin" },
        bottom: { style: "thin" },
        right: { style: "thin" },
      };
    }
  }
};

const writeTile = (
  ws: ExcelJS.Worksheet,
  topRow: number,
  leftCol: number,
  title: string,
  value: string | number,
  numFmt?: string
) => {
  // each tile spans 2 columns x 2 rows
  ws.mergeCells(topRow, leftCol, topRow, leftCol + 1);
  ws.mergeCells(topRow + 1, leftCol, topRow + 1, leftCol + 1);

  const t = ws.getCell(topRow, leftCol);
  t.value = title;
  t.font = { size: 10 };
  t.alignment = { horizontal: "left", vertical: "middle" };

  const v = ws.getCell(topRow + 1, leftCol);
  v.value = value as any;
  v.font = { bold: true, size: 12 };
  v.alignment = { horizontal: "left", vertical: "middle" };
  if (numFmt) v.numFmt = numFmt;

  setBoxBorder(ws, topRow, leftCol, topRow + 1, leftCol + 1);
};

const SECTION_WIDTH = 6; // each section uses 6 columns (A-F or H-M)

// Writes one Country block starting at startCol
const writeCountrySection = (
  cc: any,
  topRow: number,
  startCol: number
) => {
  // Country heading
  ws1.mergeCells(topRow, startCol, topRow, startCol + SECTION_WIDTH - 1);
  const h = ws1.getCell(topRow, startCol);
  h.value = cc.countryLabel;
  h.font = { bold: true, size: 12 };
  h.alignment = { horizontal: "left", vertical: "middle" };

  let r = topRow + 1;

  // Row 1 tiles
  writeTile(ws1, r, startCol + 0, "Net Sales", cc.totalSales, moneyFmt0);
  writeTile(ws1, r, startCol + 2, "Units", cc.totalUnits, unitsFmt0);
  writeTile(ws1, r, startCol + 4, "CM1 Profit", cc.totalProfit, moneyFmt0);
  r += 3;

  // Row 2 tiles
  writeTile(ws1, r, startCol + 0, "Avg. Monthly Sales", cc.avgMonthlySales, moneyFmt0);
  writeTile(ws1, r, startCol + 2, "Avg. Selling Price", cc.avgSellingPrice, moneyFmt2);

  // cc.cm1ProfitPct is 0-100 → Excel needs 0-1
  writeTile(ws1, r, startCol + 4, "CM1 Profit %", cc.cm1ProfitPct / 100, pctFmt);
  r += 3;

  // Best performance header
  ws1.mergeCells(r, startCol, r, startCol + SECTION_WIDTH - 1);
  const bp = ws1.getCell(r, startCol);
  bp.value = "Best Performance";
  bp.font = { bold: true, size: 11 };
  bp.alignment = { horizontal: "left", vertical: "middle" };
  r += 1;

  // Best performance tiles
  writeTile(ws1, r, startCol + 0, "Sales", `${cc.bestSalesMonth}  ${currencyLabel}${cc.bestSalesValue}`, undefined);
  writeTile(ws1, r, startCol + 2, "Units", `${cc.bestUnitsMonth}  ${cc.bestUnitsValue}`, undefined);
  writeTile(ws1, r, startCol + 4, "Profit", `${cc.bestProfitMonth}  ${currencyLabel}${cc.bestProfitValue}`, undefined);

  // return next row cursor after this section
  return topRow + 11;
};

  // Header block (same layout as exportInventoryReconExcel)
  ws1.mergeCells(1, 1, 1, headerCount);
  ws1.getCell(1, 1).value = titleLine || "";
  ws1.getCell(1, 1).font = { bold: false };
  ws1.getCell(1, 1).alignment = { horizontal: "left", vertical: "middle" };

  ws1.getCell(2, 1).value = `Company Name : ${companyName || ""}`;
  ws1.getCell(2, 1).alignment = { horizontal: "left" };

  ws1.getCell(2, headerCount).value = `${brandName || ""}`;
  ws1.getCell(2, headerCount).alignment = { horizontal: "right" };
  ws1.getCell(2, headerCount).font = { bold: true };

  ws1.getCell(3, 1).value = `Country : ${titleCountry || ""}`;
  ws1.getCell(4, 1).value = `Platform : ${platformLabel || ""}`;
  ws1.getCell(5, 1).value = `Currency : ${currencyLabel || ""}`;
  ws1.getCell(6, 1).value = `Period : ${periodLabel || ""}`;

  const tableHeaderRowNumber = 7;

// ---------- Sheet 1 content: Country tiles OR table ----------
// ws1.columns = [
//   { width: 18 }, { width: 18 }, // tile 1 (A,B)
//   { width: 18 }, { width: 18 }, // tile 2 (C,D)
//   { width: 18 }, { width: 18 }, // tile 3 (E,F)
// ];

ws1.columns = Array.from({ length: headerCount }, () => ({ width: 18 }));
ws1.getColumn(7).width = 4; // column G spacer

const startRow = 8; // first row below header block
let rowCursorCards = startRow;

// 1) If countryCards exists -> render tiles like CountryCard UI
if (countryCards?.length) {
  ws1.views = [{ state: "frozen", xSplit: 0, ySplit: tableHeaderRowNumber }];

  for (let i = 0; i < countryCards.length; i += 2) {
    const left = countryCards[i];
    const right = countryCards[i + 1];

    // Left section = A..F (startCol = 1)
    const nextLeft = writeCountrySection(left, rowCursorCards, 1);

    // Right section = H..M (startCol = 8) because G is spacer
    let nextRight = rowCursorCards;
    if (right) {
      nextRight = writeCountrySection(right, rowCursorCards, 8);
    }

    // move down after the taller one
    rowCursorCards = Math.max(nextLeft, nextRight) + 2;
  }
}
// 2) else fallback to table export (your existing logic)
else if (table?.headers?.length && table?.rows?.length) {
  const hdrRow = ws1.getRow(tableHeaderRowNumber);

  table.headers.forEach((h, i) => {
    const cell = hdrRow.getCell(i + 1);
    cell.value = h;
    cell.font = { bold: true, size: 11 };
    cell.alignment = {
      horizontal: "center",
      vertical: "middle",
      wrapText: true,
    };
  });
  hdrRow.height = 18;

  const startDataRow = tableHeaderRowNumber + 1;

  table.rows.forEach((rowArr, ridx) => {
    const row = ws1.getRow(startDataRow + ridx);
    table.headers.forEach((_, c0) => {
      const cell = row.getCell(c0 + 1);
      const v = rowArr?.[c0] ?? "";

      const n = toNumberLoose(v);
      if (n !== null) {
        cell.value = n;
        cell.numFmt = "#,##0.00";
        cell.alignment = { horizontal: "center" };
      } else {
        cell.value = v as any;
        cell.alignment = { horizontal: "center" };
      }
    });
  });

  ws1.views = [{ state: "frozen", xSplit: 0, ySplit: tableHeaderRowNumber }];

  ws1.columns = table.headers.map((h) => ({
    width: Math.min(Math.max(String(h).length + 2, 12), 55),
  }));
}
// 3) else no data
else {
  ws1.views = [{ state: "frozen", xSplit: 0, ySplit: tableHeaderRowNumber }];
  ws1.getCell("A7").value = "No data provided.";
}

// -------------------------
// Sheet 2: Trend Charts (Header + Labels + IMAGES)
// -------------------------
const ws2 = wb.addWorksheet(safeSheetName("Trend Charts"));

const headerCount2 = 13; // keep consistent like Sheet 1 (or use headerCount)
ws2.columns = Array.from({ length: headerCount2 }, () => ({ width: 18 }));

// ---- Header block (same as Sheet 1) ----
ws2.mergeCells(1, 1, 1, headerCount2);
ws2.getCell(1, 1).value = titleLine || "";
ws2.getCell(1, 1).font = { bold: false };
ws2.getCell(1, 1).alignment = { horizontal: "left", vertical: "middle" };

ws2.getCell(2, 1).value = `Company Name : ${companyName || ""}`;
ws2.getCell(2, 1).alignment = { horizontal: "left" };

ws2.getCell(2, headerCount2).value = `${brandName || ""}`;
ws2.getCell(2, headerCount2).alignment = { horizontal: "right" };
ws2.getCell(2, headerCount2).font = { bold: true };

ws2.getCell(3, 1).value = `Country : ${titleCountry || ""}`;
ws2.getCell(4, 1).value = `Platform : ${platformLabel || ""}`;
ws2.getCell(5, 1).value = `Currency : ${currencyLabel || ""}`;
ws2.getCell(6, 1).value = `Period : ${periodLabel || ""}`;

// Freeze header rows
const CHARTS_HEADER_ROW = 7;
ws2.views = [{ state: "frozen", xSplit: 0, ySplit: CHARTS_HEADER_ROW }];

// Parse images
const img1 = parseBase64(salesCm1ChartBase64);
const img2 = parseBase64(unitsChartBase64);

// Start AFTER header block
let rowCursor = CHARTS_HEADER_ROW + 1; // row 8

const addChartImage = (
  img: { mime: string; base64: string } | null,
  label: string
) => {
  // 1) label row above image
  ws2.mergeCells(rowCursor, 1, rowCursor, headerCount2);
  const labelCell = ws2.getCell(rowCursor, 1);
  labelCell.value = label;
  labelCell.font = { bold: true, size: 12 };
  labelCell.alignment = { horizontal: "left", vertical: "middle" };
  ws2.getRow(rowCursor).height = 18;

  rowCursor += 1;

  // 2) image OR fallback text
  if (!img) {
    ws2.mergeCells(rowCursor, 1, rowCursor, headerCount2);
    ws2.getCell(rowCursor, 1).value = `No chart image available: ${label}`;
    rowCursor += 3;
    return;
  }

  const ext = img.mime.toLowerCase().includes("jpeg") ? "jpeg" : "png";

  const imageId = wb.addImage({
    base64: `data:${img.mime};base64,${img.base64}`,
    extension: ext as "png" | "jpeg",
  });

  ws2.addImage(imageId, {
    tl: { col: 0, row: rowCursor - 1 }, // exceljs uses 0-based row/col for anchors
    ext: { width: chartWidth, height: chartHeight },
  });

  // 3) advance cursor below image (+ some spacing)
  // This is a rough row estimate; tweak if needed
  rowCursor += 24; // image height spacing
  rowCursor += 2;  // extra gap between charts
};

addChartImage(img1, "Net Sales & CM1 Profit");
addChartImage(img2, "Units");

  // -------------------------
  // Save
  // -------------------------
  const buf = await wb.xlsx.writeBuffer();
  saveAs(
    new Blob([buf], {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }),
    filename
  );
}

export async function exportInventoryForecastViewExcel(params: {
  filename: string;
  countryName: string;
  month: string;
  year: string;

  soldLabels: string[];
  forecastLabels: string[];

  tableRows: Array<{
    sNo: number;
    product: string;
    sku: string;
    sold1: any;
    sold2: any;
    sold3: any;
    f1: any;
    f2: any;
    f3: any;
  }>;

  totalsRow: {
    sold1: any;
    sold2: any;
    sold3: any;
    f1: any;
    f2: any;
    f3: any;
  };

  chartImageBase64?: string | null;

  titleLine?: string;
  titleCountry?: string;
  platformLabel?: string;
  periodLabel?: string;
  companyName?: string;
  brandName?: string;
}) {
  const {
    filename,
    soldLabels,
    forecastLabels,
    tableRows,
    totalsRow,
    chartImageBase64,
    titleLine = "Inventory Forecast View",
    titleCountry = "",
    platformLabel = "Amazon",
    periodLabel = "",
    companyName = "",
    brandName = "",
  } = params;

  const workbook = new ExcelJS.Workbook();
  workbook.creator = companyName || "Skinelements";
  workbook.created = new Date();

  const base64DataUrlToArrayBuffer = (dataUrl: string): ArrayBuffer => {
    const base64 = dataUrl.split(",")[1];
    const binary = atob(base64);
    const len = binary.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes.buffer;
  };

  const thinGrayBorder = {
    top: { style: "thin" as const, color: { argb: "FFBFBFBF" } },
    bottom: { style: "thin" as const, color: { argb: "FFBFBFBF" } },
    left: { style: "thin" as const, color: { argb: "FFBFBFBF" } },
    right: { style: "thin" as const, color: { argb: "FFBFBFBF" } },
  };

  const lightGrayFill = {
    type: "pattern" as const,
    pattern: "solid" as const,
    fgColor: { argb: "FFE6E6E6" },
  };

  const whiteFill = {
    type: "pattern" as const,
    pattern: "solid" as const,
    fgColor: { argb: "FFFFFFFF" },
  };

  const header1 = [
    "S.No",
    "Product Name",
    "SKU",
    "Last 3 Months",
    "",
    "",
    "Forecasted Months",
    "",
    "",
  ];

  const header2 = [
    "",
    "",
    "",
    soldLabels[0] || "",
    soldLabels[1] || "",
    soldLabels[2] || "",
    forecastLabels[0] || "",
    forecastLabels[1] || "",
    forecastLabels[2] || "",
  ];

  const rows = tableRows.map((r) => [
    r.sNo,
    r.product,
    r.sku,
    r.sold1,
    r.sold2,
    r.sold3,
    r.f1,
    r.f2,
    r.f3,
  ]);

  const totalsExcelRow = [
    "",
    "Total",
    "",
    totalsRow.sold1,
    totalsRow.sold2,
    totalsRow.sold3,
    totalsRow.f1,
    totalsRow.f2,
    totalsRow.f3,
  ];

  const tableData = [header1, header2, ...rows, totalsExcelRow];
  const sheetHeaderCount = 9;

  const applyExcelHeader = (sheet: ExcelJS.Worksheet, headerCount: number) => {
    sheet.mergeCells(1, 1, 1, headerCount);
    sheet.getCell(1, 1).value = titleLine;
    sheet.getCell(1, 1).font = { bold: false, size: 11 };
    sheet.getCell(1, 1).alignment = { horizontal: "left", vertical: "middle" };

    sheet.getCell(2, 1).value = `Company Name : ${companyName}`;
    sheet.getCell(2, 1).alignment = { horizontal: "left" };

    sheet.getCell(2, headerCount).value = brandName;
    sheet.getCell(2, headerCount).alignment = { horizontal: "right" };
    sheet.getCell(2, headerCount).font = { bold: true };

    sheet.getCell(3, 1).value = `Country : ${titleCountry}`;
    sheet.getCell(4, 1).value = `Platform : ${platformLabel}`;
    sheet.getCell(5, 1).value = `Period : ${periodLabel}`;
  };

  /* =========================
     Sheet 1: Forecast Table
  ========================= */
  const ws1 = workbook.addWorksheet("Forecast Table", {
    views: [{ state: "frozen", xSplit: 0, ySplit: 8 }],
  });

  applyExcelHeader(ws1, sheetHeaderCount);

  const tableStartRow = 7;

  tableData.forEach((row, idx) => {
    ws1.getRow(tableStartRow + idx).values = row;
  });

  ws1.mergeCells(tableStartRow, 4, tableStartRow, 6);
  ws1.mergeCells(tableStartRow, 7, tableStartRow, 9);
  ws1.mergeCells(tableStartRow, 1, tableStartRow + 1, 1);
  ws1.mergeCells(tableStartRow, 2, tableStartRow + 1, 2);
  ws1.mergeCells(tableStartRow, 3, tableStartRow + 1, 3);

  const headerRow1 = ws1.getRow(tableStartRow);
  const headerRow2 = ws1.getRow(tableStartRow + 1);

  // Row 1: white header like screenshot
  headerRow1.eachCell((cell) => {
    cell.alignment = { vertical: "middle", horizontal: "center" };
    cell.fill = whiteFill;
    cell.font = { bold: true, size: 11, color: { argb: "FF000000" } };
    cell.border = thinGrayBorder;
  });

  // Row 2: light gray month row
  headerRow2.eachCell((cell) => {
    cell.alignment = { vertical: "middle", horizontal: "center" };
    cell.fill = whiteFill;
    cell.font = { bold: true, size: 11, color: { argb: "FF000000" } };
    cell.border = thinGrayBorder;
  });

  // Body rows
  // Body rows (WHITE background)
for (let r = tableStartRow + 2; r < tableStartRow + tableData.length; r++) {
  const row = ws1.getRow(r);
  row.eachCell((cell, colNumber) => {
    cell.alignment = {
      vertical: "middle",
      horizontal: colNumber === 2 ? "left" : "center",
    };
    cell.fill = whiteFill;
    cell.font = { size: 11, color: { argb: "FF000000" } };
    cell.border = thinGrayBorder;
  });
}

  // Total row
  const totalRowNumber = tableStartRow + tableData.length - 1;
  ws1.getRow(totalRowNumber).eachCell((cell, colNumber) => {
    cell.alignment = {
      vertical: "middle",
      horizontal: colNumber === 2 ? "left" : "center",
    };
    cell.font = { bold: true, size: 11, color: { argb: "FF000000" } };
   cell.fill = whiteFill;
    cell.border = thinGrayBorder;
  });

  // Column widths
  ws1.columns = [
    { width: 10 },
    { width: 28 },
    { width: 18 },
    { width: 14 },
    { width: 14 },
    { width: 14 },
    { width: 16 },
    { width: 16 },
    { width: 16 },
  ];

  /* =========================
     Sheet 2: Forecast Graph
  ========================= */
  const ws2 = workbook.addWorksheet("Forecast Graph");
  ws2.columns = Array.from({ length: sheetHeaderCount }, () => ({ width: 18 }));

  applyExcelHeader(ws2, sheetHeaderCount);

  if (chartImageBase64) {
    const buffer = base64DataUrlToArrayBuffer(chartImageBase64);
    const imageId = workbook.addImage({
      buffer,
      extension: "png",
    });

    ws2.addImage(imageId, {
      tl: { col: 0, row: 6 },
      ext: { width: 1200, height: 520 },
    });
  } else {
    ws2.getCell("A7").value = "No chart image available.";
  }

  const xlsxBuffer = await workbook.xlsx.writeBuffer();
  const blob = new Blob([xlsxBuffer], {
    type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  });

  saveAs(blob, filename);
}













export async function exportPnLForecastExcel(params: {
  filename: string;

  titleLine?: string;
  titleCountry?: string;
  platformLabel?: string;
  periodLabel?: string;

  companyName?: string;
  brandName?: string;

  productRows: Array<Record<string, any>>;
  summaryRows?: Array<Record<string, any>>;

  chartImageBase64?: string | null;
}) {
  const {
    filename,
    titleLine = "P&L Forecast",
    titleCountry = "",
    platformLabel = "Amazon",
    periodLabel = "",
    companyName = "",
    brandName = "",
    productRows,
    summaryRows = [],
    chartImageBase64 = null,
  } = params;

  const workbook = new ExcelJS.Workbook();
  workbook.creator = companyName || "Skinelements";
  workbook.created = new Date();

  const base64DataUrlToArrayBuffer = (dataUrl: string): ArrayBuffer => {
    const base64 = dataUrl.split(",")[1];
    const binary = atob(base64);
    const len = binary.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes.buffer;
  };

  const thinGrayBorder = {
    top: { style: "thin" as const, color: { argb: "FFBFBFBF" } },
    bottom: { style: "thin" as const, color: { argb: "FFBFBFBF" } },
    left: { style: "thin" as const, color: { argb: "FFBFBFBF" } },
    right: { style: "thin" as const, color: { argb: "FFBFBFBF" } },
  };

  const whiteFill = {
    type: "pattern" as const,
    pattern: "solid" as const,
    fgColor: { argb: "FFFFFFFF" },
  };

  const lightGrayFill = {
    type: "pattern" as const,
    pattern: "solid" as const,
    fgColor: { argb: "FFEFEFEF" },
  };

  const applyExcelHeader = (sheet: ExcelJS.Worksheet, headerCount: number) => {
    sheet.mergeCells(1, 1, 1, headerCount);
    sheet.getCell(1, 1).value = titleLine;
    sheet.getCell(1, 1).font = { bold: false, size: 11 };
    sheet.getCell(1, 1).alignment = { horizontal: "left", vertical: "middle" };

    sheet.getCell(2, 1).value = `Company Name : ${companyName}`;
    sheet.getCell(2, 1).alignment = { horizontal: "left" };

    sheet.getCell(2, headerCount).value = brandName;
    sheet.getCell(2, headerCount).alignment = { horizontal: "right" };
    sheet.getCell(2, headerCount).font = { bold: true };

    sheet.getCell(3, 1).value = `Country : ${titleCountry}`;
    sheet.getCell(4, 1).value = `Platform : ${platformLabel}`;
    sheet.getCell(5, 1).value = `Period : ${periodLabel}`;
  };

  const headers = [
    "Product Name",
    "SKU",
    "Sales M1",
    "CM1 M1",
    "Sales M2",
    "CM1 M2",
    "Sales M3",
    "CM1 M3",
    "Sales Total",
    "CM1 Total",
  ];

  const tableRows = [
    ...(productRows || []),
    ...(summaryRows || []),
  ].map((r) => [
    r.product_name ?? "",
    r.sku ?? "",
    r.Total_Sales_1st ?? "",
    r.profit_1st ?? "",
    r.Total_Sales_2nd ?? "",
    r.profit_2nd ?? "",
    r.Total_Sales_3rd ?? "",
    r.profit_3rd ?? "",
    r.Total_Sales_sum ?? "",
    r.profit_sum ?? "",
  ]);

  /* =========================
     Sheet 1: P&L Forecast Table
  ========================= */
  const ws1 = workbook.addWorksheet("P&L Forecast", {
    views: [{ state: "frozen", xSplit: 0, ySplit: 8 }],
  });

  const headerCount = headers.length;
  applyExcelHeader(ws1, headerCount);

  const tableStartRow = 7;

  ws1.getRow(tableStartRow).values = headers;
  ws1.getRow(tableStartRow).height = 18;

  ws1.getRow(tableStartRow).eachCell((cell) => {
    cell.alignment = { vertical: "middle", horizontal: "center" };
    cell.fill = whiteFill;
    cell.font = { bold: true, size: 11, color: { argb: "FF000000" } };
    cell.border = thinGrayBorder;
  });

  tableRows.forEach((row, idx) => {
    const excelRow = ws1.getRow(tableStartRow + 1 + idx);
    excelRow.values = row;

    const isTotalRow = String(row[0]).trim().toLowerCase() === "total";

    excelRow.eachCell((cell, colNumber) => {
      const isNumberValue = typeof cell.value === "number";
      const numeric =
        typeof cell.value === "string" && cell.value !== "" && !isNaN(Number(cell.value));

      if (isNumberValue || numeric) {
        cell.value = Number(cell.value);
        cell.numFmt = "#,##0.00";
      }

      cell.alignment = {
        vertical: "middle",
        horizontal: colNumber === 1 ? "left" : "center",
      };
      cell.fill = isTotalRow ? lightGrayFill : whiteFill;
      cell.font = {
        bold: isTotalRow,
        size: 11,
        color: { argb: "FF000000" },
      };
      cell.border = thinGrayBorder;
    });
  });

  ws1.columns = [
    { width: 28 },
    { width: 18 },
    { width: 14 },
    { width: 14 },
    { width: 14 },
    { width: 14 },
    { width: 14 },
    { width: 14 },
    { width: 16 },
    { width: 16 },
  ];

  /* =========================
     Sheet 2: P&L Chart
  ========================= */
  const ws2 = workbook.addWorksheet("P&L Chart");
  ws2.columns = Array.from({ length: headerCount }, () => ({ width: 18 }));

  applyExcelHeader(ws2, headerCount);

  if (chartImageBase64) {
    const buffer = base64DataUrlToArrayBuffer(chartImageBase64);
    const imageId = workbook.addImage({
      buffer,
      extension: "png",
    });

    ws2.addImage(imageId, {
      tl: { col: 0, row: 6 },
      ext: { width: 1200, height: 520 },
    });
  } else {
    ws2.getCell("A7").value = "No chart image available.";
  }

  const xlsxBuffer = await workbook.xlsx.writeBuffer();
  const blob = new Blob([xlsxBuffer], {
    type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  });

  saveAs(blob, filename);
}

