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

  chartBase64?: string | null;
  chartMetrics?: { name: string; value: number; pct: number }[] | null; // (unused now)
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
    chartBase64 = null,
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
     Sheet 2: Inventory Breakup (ONLY IMAGE)
  ========================= */
  const ws2 = wb.addWorksheet(safeSheetName("Inventory Breakup"));

  const img = parseBase64(chartBase64);
  if (img) {
    const ext = img.mime.toLowerCase().includes("jpeg") ? "jpeg" : "png";

    const imageId = wb.addImage({
      base64: `data:${img.mime};base64,${img.base64}`,
      extension: ext as "png" | "jpeg",
    });

    // ✅ Place image at A1 (no headings)
    ws2.addImage(imageId, {
      tl: { col: 0, row: 0 },
      ext: { width: 1400, height: 520 },
    });
  } else {
    ws2.getCell("A1").value = "No chart image available.";
  }

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
  // Sheet 2: Trend Charts (ONLY IMAGES)
  // -------------------------
  const ws2 = wb.addWorksheet(safeSheetName("Trend Charts"));

  const img1 = parseBase64(salesCm1ChartBase64);
  const img2 = parseBase64(unitsChartBase64);

  let rowCursor = 0;

 const addChartImage = (
  img: { mime: string; base64: string } | null,
  label: string
) => {
  if (!img) {
    ws2.getCell(rowCursor + 1, 1).value = `No chart image available: ${label}`;
    rowCursor += 3;
    return;
  }

  const ext = img.mime.toLowerCase().includes("jpeg") ? "jpeg" : "png";

  // ---------- WHITE BACKGROUND ----------
  // const bgWidth = chartWidth + 40;
  // const bgHeight = chartHeight + 40;

  // for (let r = rowCursor; r < rowCursor + 20; r++) {
  //   for (let c = 1; c <= 10; c++) {
  //     ws2.getCell(r + 1, c).fill = {
  //       type: "pattern",
  //       pattern: "solid",
  //       fgColor: { argb: "FFFFFFFF" },
  //     };
  //   }
  // }

  // ---------- INSERT CHART IMAGE ----------
  const imageId = wb.addImage({
    base64: `data:${img.mime};base64,${img.base64}`,
    extension: ext as "png" | "jpeg",
  });

  ws2.addImage(imageId, {
    tl: { col: 0, row: rowCursor },
    ext: { width: chartWidth, height: chartHeight },
  });

  rowCursor += 24;
};

  addChartImage(img1, "Net Sales + CM1 Profit");
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