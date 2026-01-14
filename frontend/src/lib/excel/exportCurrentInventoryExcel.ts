import * as XLSX from "xlsx-js-style";

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

const applyTopStyles = (ws: XLSX.WorkSheet, headerCount: number, anchorCol1Based: number) => {
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
  const bAddr = XLSX.utils.encode_cell({ r: 1, c: Math.min(headerCount - 1, anchor0) });
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
   Main export
========================= */
export function exportCurrentInventoryExcel(params: {
  filename: string;

  // heading like: "Amazon UK - Current Inventory - Jan'26"
  titleLine: string;

  countryName: string;       // "uk" | "us" | "global"
  titleCountry: string;      // "UK" | "US" | "Global"
  platformLabel?: string;    // "Amazon" etc

  periodLabel: string;       // e.g. "Jan'26" or "Jan 2026"
  companyName: string;
  brandName: string;

  homeCurrencyCode?: string; // e.g. "USD"
  dataRows: Record<string, any>[]; // normalized rows (clean headers)
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
  } = params;

  if (!dataRows?.length) return;

  const headers = Object.keys(dataRows[0] || []);
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

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Current Inventory");

  XLSX.writeFile(wb, filename);
}
