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

const INVENTORY_AGEING_HEADERS = [
  "Inventory 0-90 Days",
  "Inventory 91-180 Days",
  "Inventory 181-270 Days",
  "Inventory 271-365 Days",
  "Inventory 365+ Days",
];

const isZeroOnlyAgeingColumn = (
  rows: Record<string, any>[],
  header: string
) => {
  if (!INVENTORY_AGEING_HEADERS.includes(header)) return false;

  return (rows || []).every((row) => {
    const n = toNumberLoose(row?.[header]);
    return !n;
  });
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
  titleLine: string;

  countryName: string;
  titleCountry: string;
  platformLabel?: string;

  periodLabel: string;
  companyName: string;
  brandName: string;

  pieImageBase64?: string | null;
  homeCurrencyCode?: string;
  dataRows: Record<string, any>[];
}) {
  const {
    filename,
    titleLine = "",
    countryName = "",
    titleCountry = "",
    platformLabel = "Phormula",
    periodLabel = "",
    companyName = "",
    brandName = "",
    homeCurrencyCode,
    dataRows,
  } = params;

  if (!dataRows?.length) return;

const preferredHeaders = [
  "S.No.",
  "Product Name",
  "SKU",
  "MTD Sales",
  "Sales Last 30 Days",
  "Sales Rank",
  "Current Inventory",
  "Inventory 0-90 Days",
  "Inventory 91-180 Days",
  "Inventory 181-270 Days",
  "Inventory 271-365 Days",
  "Inventory 365+ Days",
  "Estimated Storage Cost",
  "Inventory Coverage Ratio",
  "Inventory Alerts",
];
const sourceHeaders = Object.keys(dataRows[0] || {});
const headers = preferredHeaders.filter((h) => sourceHeaders.includes(h));
const headerCount = headers.length || 1;

const hiddenAgeingHeaders = new Set(
  headers.filter((h) => isZeroOnlyAgeingColumn(dataRows, h))
);

  if (!headers.length) return;

  const ANCHOR_COL_1_BASED = headerCount;

  const tableBorder = {
  top: { style: "thin", color: { rgb: "000000" } },
  bottom: { style: "thin", color: { rgb: "000000" } },
  left: { style: "thin", color: { rgb: "000000" } },
  right: { style: "thin", color: { rgb: "000000" } },
};

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

  const bodyAoA = dataRows.map((row, index) =>
    headers.map((h) => {
      if (h === "S.No.") {
        return row[h] ?? index + 1;
      }
      return row[h] ?? "";
    })
  );

  const sheetAoA = [...topAoA, headers, ...bodyAoA];
  const ws = XLSX.utils.aoa_to_sheet(sheetAoA);

  ws["!freeze"] = { xSplit: 0, ySplit: headerRowIndex + 1 };

ws["!cols"] = headers.map((h) => {
  const hidden = hiddenAgeingHeaders.has(h);

  if (h === "S.No.") return { wch: 8, hidden };
  if (h === "Product Name") return { wch: 24, hidden };
  if (h === "SKU") return { wch: 18, hidden };
  if (h === "MTD Sales") return { wch: 14, hidden };
  if (h === "Sales Last 30 Days") return { wch: 18, hidden };
  if (h === "Sales Rank") return { wch: 14, hidden };
  if (h === "Current Inventory") return { wch: 18, hidden };

  if (INVENTORY_AGEING_HEADERS.includes(h)) {
    return { wch: 20, hidden };
  }

  if (h === "Estimated Storage Cost") return { wch: 26, hidden };
  if (h === "Inventory Coverage Ratio") return { wch: 24, hidden };
  if (h === "Inventory Alerts") return { wch: 24, hidden };

  return {
    wch: Math.min(Math.max(String(h).length + 2, 12), 28),
    hidden,
  };
});

  applyTopStyles(ws, headerCount, ANCHOR_COL_1_BASED);

  for (let c = 0; c < headerCount; c++) {
    const addr = XLSX.utils.encode_cell({ r: headerRowIndex, c });
    if (!ws[addr]) continue;

  ws[addr].s = {
  ...(ws[addr].s || {}),
  font: { bold: true, sz: 11 },
  alignment: {
    horizontal: "center",
    vertical: "center",
    wrapText: true,
  },
  border: tableBorder,
};
  }

const INTEGER_COLUMNS = new Set([
  "S.No.",
  "MTD Sales",
  "Sales Last 30 Days",
  "Sales Rank",
  "Current Inventory",
  "Inventory 0-90 Days",
  "Inventory 91-180 Days",
  "Inventory 181-270 Days",
  "Inventory 271-365 Days",
  "Inventory 365+ Days",
]);

const DECIMAL_COLUMNS = new Set([
  "Estimated Storage Cost",
  "Inventory Coverage Ratio",
]);

const toExcelNumber = (value: any) => {
  if (value === null || value === undefined || value === "") return null;

  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  const cleaned = String(value)
    .replace(/,/g, "")
    .trim();

  if (cleaned === "" || cleaned === "-") return null;

  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
};

const range = XLSX.utils.decode_range(ws["!ref"] || "A1:A1");

for (let r = headerRowIndex + 1; r <= range.e.r; r++) {
  for (let c = 0; c < headerCount; c++) {
    const addr = XLSX.utils.encode_cell({ r, c });
    const cell = ws[addr];

    if (!cell) continue;

    const header = headers[c];
    const n = toExcelNumber(cell.v);

    if (n === null) continue;

    if (INTEGER_COLUMNS.has(header)) {
      cell.v = Math.trunc(n);
      cell.t = "n";
      cell.z = "#,##0";
    } else if (DECIMAL_COLUMNS.has(header)) {
      cell.v = n;
      cell.t = "n";
      cell.z = "#,##0.00";
    }
  }
}

// Add borders + center alignment to full table: header + body rows
const tableStartRow = headerRowIndex;
const tableEndRow = range.e.r;

for (let r = tableStartRow; r <= tableEndRow; r++) {
  for (let c = 0; c < headerCount; c++) {
    const addr = XLSX.utils.encode_cell({ r, c });

    if (!ws[addr]) {
      ws[addr] = { t: "s", v: "" };
    }

    ws[addr].s = {
      ...(ws[addr].s || {}),
      alignment: {
        horizontal: "center",
        vertical: "center",
        wrapText: true,
      },
      border: tableBorder,
    };
  }
}

const wb = XLSX.utils.book_new();
XLSX.utils.book_append_sheet(wb, ws, "Current Inventory");
XLSX.writeFile(wb, filename);
}

export async function exportGlobalCurrentInventoryExcel(params: {
  filename: string;

  titleLine?: string;
  platformLabel?: string;
  periodLabel: string;

  companyName: string;
  brandName: string;
  homeCurrencyCode?: string;

  ukRows: Record<string, any>[];
  usRows: Record<string, any>[];
}) {
  const {
    filename,
    titleLine = "Amazon Global - Current Inventory",
    platformLabel = "Phormula",
    periodLabel,
    companyName,
    brandName,
    homeCurrencyCode,
    ukRows,
    usRows,
  } = params;

  if (!ukRows?.length && !usRows?.length) return;

  const workbook = new ExcelJS.Workbook();
  workbook.creator = companyName || "Skinelements";
  workbook.created = new Date();

const headers = [
  "S.No.",
  "Product Name",
  "SKU",
  "MTD Sales",
  "Sales Last 30 Days",
  "Sales Rank",
  "Current Inventory",
  "Inventory 0-90 Days",
  "Inventory 91-180 Days",
  "Inventory 181-270 Days",
  "Inventory 271-365 Days",
  "Inventory 365+ Days",
  "Estimated Storage Cost",
  "Inventory Coverage Ratio",
  "Inventory Alerts",
];

const tableBorder = {
  top: { style: "thin" as const, color: { argb: "FF000000" } },
  bottom: { style: "thin" as const, color: { argb: "FF000000" } },
  left: { style: "thin" as const, color: { argb: "FF000000" } },
  right: { style: "thin" as const, color: { argb: "FF000000" } },
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

  const addInventorySheet = (
    sheetName: string,
    rows: Record<string, any>[],
    countryLabel: "UK" | "US"
  ) => {
    if (!rows?.length) return;

    const ws = workbook.addWorksheet(safeSheetName(sheetName), {
      views: [{ state: "frozen", xSplit: 0, ySplit: 8 }],
    });

    const headerCount = headers.length;

    const hiddenAgeingHeaders = new Set(
  headers.filter((h) => isZeroOnlyAgeingColumn(rows, h))
);

    const currencySymbol = getCurrencySymbol({
  countryName: "global",
  homeCurrencyCode: homeCurrencyCode || "USD",
});

    ws.mergeCells(1, 1, 1, headerCount);
    ws.getCell(1, 1).value = `${titleLine} - ${countryLabel}`;
    ws.getCell(1, 1).font = { bold: false, size: 11 };
    ws.getCell(1, 1).alignment = { horizontal: "left", vertical: "middle" };

    ws.getCell(2, 1).value = `Company Name : ${companyName || ""}`;
    ws.getCell(2, 1).alignment = { horizontal: "left" };

    ws.getCell(2, headerCount).value = brandName || "";
    ws.getCell(2, headerCount).alignment = { horizontal: "right" };
    ws.getCell(2, headerCount).font = { bold: true };

    ws.getCell(3, 1).value = `Country : ${countryLabel}`;
    ws.getCell(4, 1).value = `Platform : ${platformLabel}`;
    ws.getCell(5, 1).value = `Currency : ${currencySymbol}`;
    ws.getCell(6, 1).value = `Period : ${periodLabel}`;

    const headerRowNumber = 8;
    const headerRow = ws.getRow(headerRowNumber);

    headers.forEach((header, index) => {
      const cell = headerRow.getCell(index + 1);
      cell.value = header;
      cell.font = { bold: true, size: 11, color: { argb: "FF000000" } };
      cell.fill = whiteFill;
      cell.alignment = {
        horizontal: "center",
        vertical: "middle",
        wrapText: true,
      };
      cell.border = tableBorder;
    });

    headerRow.height = 32;

    const startDataRow = headerRowNumber + 1;

    rows.forEach((row, rowIndex) => {
      const excelRow = ws.getRow(startDataRow + rowIndex);
      const isTotal =
        String(row["Product Name"] ?? "").trim().toLowerCase() === "total";

      headers.forEach((header, colIndex) => {
        const cell = excelRow.getCell(colIndex + 1);
        const value = row[header];

        const n = toNumberLoose(value);

        if (n !== null && header !== "SKU" && header !== "Inventory Alerts") {
          cell.value = n;

         if (
  [
    "S.No.",
    "MTD Sales",
    "Sales Last 30 Days",
    "Sales Rank",
    "Current Inventory",
    "Inventory 0-90 Days",
    "Inventory 91-180 Days",
    "Inventory 181-270 Days",
    "Inventory 271-365 Days",
    "Inventory 365+ Days",
  ].includes(header)
) {
  cell.numFmt = "#,##0";
} else {
  cell.numFmt = "#,##0.00";
}
        } else {
          cell.value = value ?? "";
        }

       cell.alignment = {
  horizontal: "center",
  vertical: "middle",
  wrapText: true,
  shrinkToFit: false,
};

        cell.fill = whiteFill;
        cell.font = {
          bold: isTotal,
          size: 11,
          color: { argb: "FF000000" },
        };
       cell.border = tableBorder;
      });
    });

ws.columns = headers.map((h) => {
  const hidden = hiddenAgeingHeaders.has(h);

  if (h === "S.No.") return { width: 8, hidden };
  if (h === "Product Name") return { width: 28, hidden };
  if (h === "SKU") return { width: 18, hidden };
  if (h === "MTD Sales") return { width: 14, hidden };
  if (h === "Sales Last 30 Days") return { width: 18, hidden };
  if (h === "Sales Rank") return { width: 14, hidden };
  if (h === "Current Inventory") return { width: 18, hidden };

  if (INVENTORY_AGEING_HEADERS.includes(h)) {
    return { width: 20, hidden };
  }

  if (h === "Estimated Storage Cost") return { width: 26, hidden };
  if (h === "Inventory Coverage Ratio") return { width: 24, hidden };
  if (h === "Inventory Alerts") return { width: 30, hidden };

  return { width: 16, hidden };
});
  };

  addInventorySheet("UK Current Inventory", ukRows, "UK");
  addInventorySheet("US Current Inventory", usRows, "US");

  const buffer = await workbook.xlsx.writeBuffer();

  saveAs(
    new Blob([buffer], {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }),
    filename
  );
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
  isUsLayout?: boolean;
  dataRows: Record<string, any>[];

  // ✅ now supports nested-like rows via indent + bold
  summaryRows?: { label: string; value: any; indent?: number; bold?: boolean }[];
}) {
  const {
    filename,
    titleLine,
    countryName,
    titleCountry,
    platformLabel = "Phormula",
    periodLabel,
    companyName,
    brandName,
    homeCurrencyCode,
    isUsLayout = false,
    dataRows,
    summaryRows,
  } = params;

  if (!dataRows?.length) return;

  const headers = Object.keys(dataRows[0] || {});
  const headerCount = headers.length || 1;
  const isUsPnlLayout = Boolean(isUsLayout);

  const COLUMN_META: Record<
  string,
  { group?: string; subHeader?: string; sign?: "(+)" | "(-)" }
> = {
"S.No": { subHeader: "S.No" },
"Sno.": { subHeader: "Sno." },
"Product Name": { subHeader: "Product Name" },
"SKU": isUsPnlLayout
  ? { group: "Units", subHeader: "SKU" }
  : { subHeader: "SKU" },

"Units Sold": {
  group: isUsPnlLayout ? "Units" : "Net Units Sold",
  subHeader: "Units Sold",
  sign: "(+)",
},
"Return": {
  group: isUsPnlLayout ? "Units" : "Net Units Sold",
  subHeader: "Return",
  sign: "(-)",
},
"Total Units": {
  group: "Net Units Sold",
  subHeader: "Total",
  sign: "(+)",
},
"Net Units Sold": {
  group: "Units",
  subHeader: "Net Units Sold",
  sign: "(+)",
},

"ASP": { subHeader: "ASP" },

"Gross Sales": {
  group: isUsPnlLayout ? "Sales" : "Net Sales",
  subHeader: "Gross Sales",
  sign: "(+)",
},
"Sales - Refund": {
  group: isUsPnlLayout ? "Sales" : "Net Sales",
  subHeader: "Sales - Refund",
  sign: "(-)",
},
"Taxes and Credits": {
  group: "Net Sales",
  subHeader: "Taxes and Credits",
  sign: "(-)",
},
"Net Sales": {
  group: isUsPnlLayout ? "Sales" : "Net Sales",
  subHeader: isUsPnlLayout ? "Net Sales" : "Total",
  sign: "(+)",
},
"Promotions": {
  group: isUsPnlLayout ? "Sales" : "Promotions",
  subHeader: "Promotions",
  sign: "(-)",
},
"Promotions %": {
  group: isUsPnlLayout ? undefined : "Promotions",
  subHeader: "Promotions %",
},

"COGS": { subHeader: "COGS", sign: "(-)" },

  "Selling Fees": {
    group: isUsPnlLayout ? "Amazon Fees" : "Marketplace Fees",
    subHeader: "Selling Fees",
    sign: "(-)",
  },
  "FBA Fees": {
    group: isUsPnlLayout ? "Amazon Fees" : "Marketplace Fees",
    subHeader: "FBA Fees",
    sign: "(-)",
  },
  "Marketplace Fees": {
    group: "Marketplace Fees",
    subHeader: "Total",
    sign: "(-)",
  },
  "Total Fees": {
    group: "Amazon Fees",
    subHeader: "Total Fees",
    sign: "(-)",
  },

  "Tax": {
    group: "Other Transactions",
    subHeader: "Net Taxes",
    sign: "(-)",
  },
  "Credits": {
    group: "Other Transactions",
    subHeader: "Net Credits",
    sign: "(+)",
  },
  "Tax & Credits": {
    group: "Other Transactions",
    subHeader: "Total",
    sign: "(+)",
  },
  "Net Taxes": {
    group: "Other Transactions",
    subHeader: "Net Taxes",
    sign: "(-)",
  },
  "Net Credits": {
    group: "Other Transactions",
    subHeader: "Net Credits",
    sign: "(+)",
  },
  "Misc. Transactions": {
    group: "Other Transactions",
    subHeader: "Misc. Transactions",
    sign: "(+)",
  },
  "Other Transactions": {
    group: "Other Transactions",
    subHeader: "Total",
    sign: "(+)",
  },

  "CM1 Profit Per Unit": {
    group: "CM1 Profit",
    subHeader: "Per Unit",
  },
  "CM1 Profit %": {
    group: "CM1 Profit",
    subHeader: "%",
  },
  "CM1 Profit": {
    group: "CM1 Profit",
    subHeader: isUsPnlLayout ? "Margin" : "Total",
  },

"Sponsored Product": {
  group: "Ads Spend",
  subHeader: "Sponsored Product",
  sign: "(-)",
},
"Sponsored Display": {
  group: "Ads Spend",
  subHeader: "Sponsored Display",
  sign: "(-)",
},
"Ads Spend": {
  group: "Ads Spend",
  subHeader: "Total",
  sign: "(-)",
},

"ACOS %": {
  subHeader: "ACoS %",
},

  "CM2 Profit Per Unit": {
    group: "CM2 Profit",
    subHeader: "Per Unit",
  },
  "CM2 Profit %": {
    group: "CM2 Profit",
    subHeader: "%",
  },
  "CM2 Profit": {
    group: "CM2 Profit",
    subHeader: isUsPnlLayout ? "Margin" : "Total",
  },
};

const groupHeaderRow = headers.map(
  (h) => COLUMN_META[h]?.group || COLUMN_META[h]?.subHeader || h
);

const subHeaderRow = headers.map((h) =>
  COLUMN_META[h]?.group ? COLUMN_META[h]?.subHeader || h : ""
);

const signRow = headers.map((h) => COLUMN_META[h]?.sign || "");

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

  const groupHeaderRowIndex = topAoA.length;
const subHeaderRowIndex = topAoA.length + 1;
const signRowIndex = topAoA.length + 2;
const firstDataRowIndex = topAoA.length + 3;

  const bodyAoA = dataRows.map((r) => headers.map((h) => (r as any)[h] ?? ""));

  // Summary: label under "Product Name", value under "CM1 Profit"
  const productNameCol = headers.indexOf("Product Name");
 const cm1ProfitCol = headers.indexOf("CM1 Profit");
const cm2ProfitCol = headers.indexOf("CM2 Profit");

const labelCol = productNameCol >= 0 ? productNameCol : 0;

// ✅ Summary value should appear under CM2 Profit Total.
// ✅ If CM2 Profit column does not exist, fallback to CM1 Profit Total.
const valueCol =
  cm2ProfitCol >= 0
    ? cm2ProfitCol
    : cm1ProfitCol >= 0
      ? cm1ProfitCol
      : Math.min(1, Math.max(0, headerCount - 1));

  // ✅ mimic SKU behavior
  const PERCENT_SUMMARY_LABELS = new Set([
    "CM2 Margins",
    "CM2 Profit %",
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
      } else if (String(v).trim() === "-") {
        v = "-";
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
      firstDataRowIndex + bodyAoA.length + (summaryAoA.length - 1);
      if (isPercentRow) percentSummaryRowIndices.push(aoaRowIndexInSheet);
      if (s?.bold) boldSummaryRowIndices.push(aoaRowIndexInSheet);
    });
  }

const sheetAoA = [
  ...topAoA,
  groupHeaderRow,
  subHeaderRow,
  signRow,
  ...bodyAoA,
  ...summaryAoA,
];
  const ws = XLSX.utils.aoa_to_sheet(sheetAoA);

  const merges: XLSX.Range[] = [];

let c = 0;
while (c < headers.length) {
  const header = headers[c];
  const group = COLUMN_META[header]?.group;

  if (!group) {
    merges.push({
      s: { r: groupHeaderRowIndex, c },
      e: { r: subHeaderRowIndex, c },
    });
    c++;
    continue;
  }

  let end = c;

  while (
    end + 1 < headers.length &&
    COLUMN_META[headers[end + 1]]?.group === group
  ) {
    end++;
  }

  if (end > c) {
    merges.push({
      s: { r: groupHeaderRowIndex, c },
      e: { r: groupHeaderRowIndex, c: end },
    });
  }

  c = end + 1;
}

ws["!merges"] = [...(ws["!merges"] || []), ...merges];

  // Freeze under table header
 ws["!freeze"] = { xSplit: 0, ySplit: firstDataRowIndex };

  // Auto widths
  ws["!cols"] = headers.map((h) => ({
    wch: Math.min(Math.max(String(h).length + 2, 12), 48),
  }));

  applyTopStyles(ws, headerCount, ANCHOR_COL_1_BASED);

const headerFill = { fgColor: { rgb: "FFFFFF" } };

for (const r of [groupHeaderRowIndex, subHeaderRowIndex, signRowIndex]) {
  for (let c = 0; c < headerCount; c++) {
    const addr = XLSX.utils.encode_cell({ r, c });
    if (!ws[addr]) continue;

    ws[addr].s = {
      ...(ws[addr].s || {}),
      font: {
        bold: true,
        sz: r === groupHeaderRowIndex ? 11 : 10,
        color: { rgb: "000000" },
      },
      fill: headerFill,
      alignment: {
        horizontal: "center",
        vertical: "center",
        wrapText: true,
      },
      border: {
        top: { style: "thin", color: { rgb: "000000" } },
        bottom: { style: "thin", color: { rgb: "000000" } },
        left: { style: "thin", color: { rgb: "000000" } },
        right: { style: "thin", color: { rgb: "000000" } },
      },
    };
  }
}

ws["!rows"] = ws["!rows"] || [];
ws["!rows"][groupHeaderRowIndex] = { hpt: 28 };
ws["!rows"][subHeaderRowIndex] = { hpt: 34 };
ws["!rows"][signRowIndex] = { hpt: 22 };

  // Bold total row (last data row)
const totalRowIndex = firstDataRowIndex + bodyAoA.length - 1;
  for (let c = 0; c < headerCount; c++) {
    const addr = XLSX.utils.encode_cell({ r: totalRowIndex, c });
    if (ws[addr]) {
      ws[addr].s = { ...(ws[addr].s || {}), font: { bold: true, sz: 11 } };
    }
  }

// Force numeric cells formatting
const range = XLSX.utils.decode_range(ws["!ref"] || "A1:A1");
const unitCols = new Set([
  headers.indexOf("Units Sold"),
  headers.indexOf("Return"),
  headers.indexOf("Total Units"),
  headers.indexOf("Net Units Sold"),
].filter((idx) => idx >= 0));

const serialNoCol = headers.indexOf("S.No");
const usSerialNoCol = headers.indexOf("Sno.");

for (let r = range.s.r; r <= range.e.r; r++) {
  for (let c = range.s.c; c <= range.e.c; c++) {
    const addr = XLSX.utils.encode_cell({ r, c });
    const cell = ws[addr];

    if (!cell) continue;
    if (!isNumber(cell.v)) continue;

if (unitCols.has(c) || c === serialNoCol || c === usSerialNoCol) {
  cell.v = Math.round(Number(cell.v || 0));
  cell.z = "#,##0";
} else {
  cell.z = "#,##0.00";
}
  }
}

// ✅ Add % symbol for percentage columns in main product table
const PERCENT_TABLE_HEADERS = new Set([
  "ACOS %",
  "Promotions %",
  "CM1 Profit %",
  "CM2 Profit %",
]);

for (let c = 0; c < headerCount; c++) {
  const header = String(headers[c] || "").trim();

  if (!PERCENT_TABLE_HEADERS.has(header)) continue;

for (let r = firstDataRowIndex; r <= totalRowIndex; r++) {
    const addr = XLSX.utils.encode_cell({ r, c });
    const cell = ws[addr];

    if (!cell || !isNumber(cell.v)) continue;

    cell.z = '0.00"%"';
  }
}


const tableBorder = {
  top: { style: "thin", color: { rgb: "000000" } },
  bottom: { style: "thin", color: { rgb: "000000" } },
  left: { style: "thin", color: { rgb: "000000" } },
  right: { style: "thin", color: { rgb: "000000" } },
};

// Border only the main product table: headers + body rows
for (let r = groupHeaderRowIndex; r <= totalRowIndex; r++) {
  for (let c = 0; c < headerCount; c++) {
    const addr = XLSX.utils.encode_cell({ r, c });

    if (!ws[addr]) {
      ws[addr] = { t: "s", v: "" };
    }

    ws[addr].s = {
      ...(ws[addr].s || {}),
      border: tableBorder,
    };
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
  lostCompRows?: Record<string, any>[];

  breakupChartBase64?: string | null;
  ageingChartBase64?: string | null;
}) {
  const {
    filename,
    titleLine,
    titleCountry,
    platformLabel = "Phormula",
    periodLabel,
    companyName,
    brandName,
    dataRows,
    lostCompRows = [],
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

  const tableBorder = {
    top: { style: "thin" as const, color: { argb: "FF000000" } },
    bottom: { style: "thin" as const, color: { argb: "FF000000" } },
    left: { style: "thin" as const, color: { argb: "FF000000" } },
    right: { style: "thin" as const, color: { argb: "FF000000" } },
  };

const whiteFill = {
  type: "pattern" as const,
  pattern: "solid" as const,
  fgColor: { argb: "FFFFFFFF" },
};

const totalFill = whiteFill;

  type HeaderMeta = {
    raw: string;
    group: string;
    subHeader: string;
    isGrouped: boolean;
  };

  const parseHeaderMeta = (header: string): HeaderMeta => {
    const value = String(header || "");
    const separator = " - ";
    const index = value.indexOf(separator);

    if (index > -1) {
      return {
        raw: value,
        group: value.slice(0, index).trim(),
        subHeader: value.slice(index + separator.length).trim(),
        isGrouped: true,
      };
    }

    return {
      raw: value,
      group: value,
      subHeader: "",
      isGrouped: false,
    };
  };

  const headerMeta = headers.map(parseHeaderMeta);

  const getSignForExportHeader = (header: string) => {
    const h = String(header || "").toLowerCase();

    if (!h.startsWith("other items - ")) return "";

    if (h.includes(" - total")) return "";
    if (h.includes(" - found")) return "(-)";

    return "(+)";
  };

const getSignFontColor = (sign: string) => {
  if (sign === "(+)") return "FF008000";
  if (sign === "(-)") return "FFFF0000";
  return "FF000000";
};

 const styleHeaderCell = (cell: any) => {
  cell.font = {
    bold: true,
    size: 11,
    color: { argb: "FF000000" },
  };
  cell.fill = whiteFill;
  cell.alignment = {
    horizontal: "center",
    vertical: "middle",
    wrapText: true,
  };
  cell.border = tableBorder;
};

 const styleSignCell = (cell: any, sign: string) => {
  cell.font = {
    bold: true,
    size: 11,
    color: { argb: "FF000000" },
  };
  cell.fill = whiteFill;
  cell.alignment = {
    horizontal: "center",
    vertical: "middle",
    wrapText: false,
  };
  cell.border = tableBorder;
};

  /* =========================
     Sheet 1: Inventory Recon
  ========================= */
  const ws1 = wb.addWorksheet(safeSheetName("Inventory Recon"), {
    views: [{ state: "frozen", xSplit: 0, ySplit: 9 }],
  });

  ws1.mergeCells(1, 1, 1, headerCount);
  ws1.getCell(1, 1).value = titleLine || "";
  ws1.getCell(1, 1).font = { bold: false };
  ws1.getCell(1, 1).alignment = {
    horizontal: "left",
    vertical: "middle",
  };

  ws1.getCell(2, 1).value = `Company Name : ${companyName || ""}`;
  ws1.getCell(2, 1).alignment = { horizontal: "left" };

  ws1.getCell(2, headerCount).value = `${brandName || ""}`;
  ws1.getCell(2, headerCount).alignment = { horizontal: "right" };
  ws1.getCell(2, headerCount).font = { bold: true };

  ws1.getCell(3, 1).value = `Country : ${titleCountry}`;
  ws1.getCell(4, 1).value = `Platform : ${platformLabel}`;
  ws1.getCell(5, 1).value = `Period : ${periodLabel}`;

  const groupHeaderRowNumber = 7;
  const subHeaderRowNumber = 8;
  const signRowNumber = 9;

  const groupHeaderRow = ws1.getRow(groupHeaderRowNumber);
  const subHeaderRow = ws1.getRow(subHeaderRowNumber);
  const signRow = ws1.getRow(signRowNumber);

  groupHeaderRow.height = 30;
  subHeaderRow.height = 42;
  signRow.height = 22;

  for (let c = 1; c <= headerCount; c++) {
    const meta = headerMeta[c - 1];
    const sign = getSignForExportHeader(meta.raw);

    const groupCell = groupHeaderRow.getCell(c);
    const subCell = subHeaderRow.getCell(c);
    const signCell = signRow.getCell(c);

    groupCell.value = meta.group;
    subCell.value = meta.isGrouped ? meta.subHeader : "";
    signCell.value = sign;

    styleHeaderCell(groupCell);
    styleHeaderCell(subCell);
    styleSignCell(signCell, sign);
  }

  // Merge headers to match UI grouped header/sub-header layout
  let c = 1;

  while (c <= headerCount) {
    const meta = headerMeta[c - 1];

    if (!meta.isGrouped) {
      ws1.mergeCells(groupHeaderRowNumber, c, subHeaderRowNumber, c);
      c++;
      continue;
    }

    let end = c;

    while (
      end + 1 <= headerCount &&
      headerMeta[end]?.isGrouped &&
      headerMeta[end]?.group === meta.group
    ) {
      end++;
    }

    if (end > c) {
      ws1.mergeCells(groupHeaderRowNumber, c, groupHeaderRowNumber, end);
    }

    c = end + 1;
  }

  const startDataRow = signRowNumber + 1;

  dataRows.forEach((r, idx) => {
    const row = ws1.getRow(startDataRow + idx);

    headers.forEach((h, c0) => {
      const cell = row.getCell(c0 + 1);
      const v = r?.[h] ?? "";

      if (c0 === snoCol0) {
        cell.value = v === null || v === undefined ? "" : String(v);
        cell.numFmt = "@";
        cell.alignment = {
          horizontal: "center",
          vertical: "middle",
        };
      } else {
        const n = toNumberLoose(v);

        if (n !== null) {
          cell.value = Math.trunc(n);
          cell.numFmt = "#,##0";
          cell.alignment = {
            horizontal: "center",
            vertical: "middle",
          };
        } else {
          cell.value = v;
          cell.alignment = {
            horizontal: c0 === productNameCol0 ? "left" : "center",
            vertical: "middle",
          };
        }
      }

      cell.font = {
        size: 11,
        color: { argb: "FF000000" },
      };
      cell.fill = whiteFill;
      cell.border = tableBorder;
    });
  });

  ws1.views = [{ state: "frozen", xSplit: 0, ySplit: signRowNumber }];

  ws1.columns = headers.map((h) => {
    const meta = parseHeaderMeta(h);
    const label = meta.subHeader || meta.group || String(h);

    if (label.toLowerCase().includes("product name")) return { width: 18 };
    if (label.toLowerCase() === "sku") return { width: 16 };
    if (label.toLowerCase().includes("inventory coverage")) return { width: 18 };
    if (label.toLowerCase().includes("transit")) return { width: 16 };

    return {
      width: Math.min(Math.max(String(label).length + 3, 11), 22),
    };
  });

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

        for (let col = 1; col <= headerCount; col++) {
          const cell = row.getCell(col);

          cell.font = {
            ...(cell.font || {}),
            bold: true,
            size: 11,
            color: { argb: "FF000000" },
          };
          cell.fill = totalFill;
          cell.border = tableBorder;
        }
      }
    }
  }

  /* =========================
     Sheet 2: Lost vs Compensation
  ========================= */
  if (lostCompRows?.length) {
    const lostHeaders = Object.keys(lostCompRows[0] || {}).filter(
      (h) => h !== "__isTotal"
    );

    const lostHeaderCount = lostHeaders.length || 1;

    const productNameCol0Lost = lostHeaders.findIndex((h) =>
      String(h).toLowerCase().includes("product name")
    );

    const snoCol0Lost = lostHeaders.findIndex((h) =>
      String(h).toLowerCase().includes("s. no")
    );

    const wsLost = wb.addWorksheet(safeSheetName("Lost vs Compensation"), {
      views: [{ state: "frozen", xSplit: 0, ySplit: 7 }],
    });

    wsLost.mergeCells(1, 1, 1, lostHeaderCount);
    wsLost.getCell(1, 1).value = `${titleLine} - Lost vs Compensation`;
    wsLost.getCell(1, 1).font = { bold: false };
    wsLost.getCell(1, 1).alignment = {
      horizontal: "left",
      vertical: "middle",
    };

    wsLost.getCell(2, 1).value = `Company Name : ${companyName || ""}`;
    wsLost.getCell(2, 1).alignment = { horizontal: "left" };

    wsLost.getCell(2, lostHeaderCount).value = `${brandName || ""}`;
    wsLost.getCell(2, lostHeaderCount).alignment = { horizontal: "right" };
    wsLost.getCell(2, lostHeaderCount).font = { bold: true };

    wsLost.getCell(3, 1).value = `Country : ${titleCountry}`;
    wsLost.getCell(4, 1).value = `Platform : ${platformLabel}`;
    wsLost.getCell(5, 1).value = `Period : ${periodLabel}`;

    const lostHeaderRowNumber = 7;
    const lostHeaderRow = wsLost.getRow(lostHeaderRowNumber);

    lostHeaders.forEach((h, i) => {
      const cell = lostHeaderRow.getCell(i + 1);

      cell.value = h;
      cell.font = {
        bold: true,
        size: 11,
        color: { argb: "FF000000" },
      };
      cell.fill = whiteFill;
      cell.alignment = {
        horizontal: "center",
        vertical: "middle",
        wrapText: false,
      };
      cell.border = tableBorder;
    });

    lostHeaderRow.height = 18;

    const lostStartDataRow = lostHeaderRowNumber + 1;

    lostCompRows.forEach((r, idx) => {
      const row = wsLost.getRow(lostStartDataRow + idx);

      lostHeaders.forEach((h, c0) => {
        const cell = row.getCell(c0 + 1);
        const v = r?.[h] ?? "";

        if (c0 === snoCol0Lost) {
          cell.value = v === null || v === undefined ? "" : String(v);
          cell.numFmt = "@";
          cell.alignment = {
            horizontal: "center",
            vertical: "middle",
          };
        } else {
          const n = toNumberLoose(v);

          if (n !== null) {
            cell.value = Math.trunc(n);
            cell.numFmt = "#,##0";
            cell.alignment = {
              horizontal: "center",
              vertical: "middle",
            };
          } else {
            cell.value = v;
            cell.alignment = {
              horizontal: c0 === productNameCol0Lost ? "left" : "center",
              vertical: "middle",
            };
          }
        }

        cell.font = {
          size: 11,
          color: { argb: "FF000000" },
        };
        cell.fill = whiteFill;
        cell.border = tableBorder;
      });
    });

    wsLost.columns = lostHeaders.map((h) => ({
      width: Math.min(Math.max(String(h).length + 2, 12), 40),
    }));

    if (productNameCol0Lost >= 0) {
      const totalCandidates = new Set(["total", "grand total"]);

      for (let i = 0; i < lostCompRows.length; i++) {
        const rowNum = lostStartDataRow + i;
        const cellVal = String(
          wsLost.getCell(rowNum, productNameCol0Lost + 1).value ?? ""
        )
          .trim()
          .toLowerCase();

        if (totalCandidates.has(cellVal)) {
          const row = wsLost.getRow(rowNum);

          for (let col = 1; col <= lostHeaderCount; col++) {
            const cell = row.getCell(col);

            cell.font = {
              ...(cell.font || {}),
              bold: true,
              size: 11,
              color: { argb: "FF000000" },
            };
            cell.fill = totalFill;
            cell.border = tableBorder;
          }
        }
      }
    }
  }

  /* =========================
     Sheet 3: Inventory Charts
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
  titleLine: string;          // e.g. "Phormula UK - Productwise Trends - Jan'26"
  titleCountry: string;       // "UK" | "US" | "Global"
  platformLabel?: string;     // "Phormula"
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
    platformLabel = "Phormula",
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
const bestMonth =
  cc.bestSalesMonth ||
  cc.bestUnitsMonth ||
  cc.bestProfitMonth ||
  "";

ws1.mergeCells(r, startCol, r, startCol + SECTION_WIDTH - 1);

const bp = ws1.getCell(r, startCol);
bp.value = bestMonth
  ? `Best Performance (${bestMonth})`
  : "Best Performance";

bp.font = { bold: true, size: 11 };
bp.alignment = { horizontal: "left", vertical: "middle" };

r += 1;

// Best performance metric titles
writeTile(
  ws1,
  r,
  startCol + 0,
  "Sales",
  cc.bestSalesValue,
  moneyFmt2
);

writeTile(
  ws1,
  r,
  startCol + 2,
  "Units",
  cc.bestUnitsValue,
  unitsFmt0
);

writeTile(
  ws1,
  r,
  startCol + 4,
  "Profit",
  cc.bestProfitValue,
  moneyFmt2
);

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

   const isGlobalExport =
  String(titleCountry || "").trim().toLowerCase() === "global";

if (isGlobalExport) {
  const globalCards = countryCards.filter((cc: any) =>
    String(cc.countryLabel || cc.countryKey || "")
      .trim()
      .toLowerCase()
      .startsWith("global")
  );

  const countryWiseCards = countryCards.filter((cc: any) => {
    const label = String(cc.countryLabel || cc.countryKey || "")
      .trim()
      .toLowerCase();

    return !label.startsWith("global");
  });

  // 1. GLOBAL block first
  globalCards.forEach((cc: any) => {
    rowCursorCards = writeCountrySection(cc, rowCursorCards, 1) + 2;
  });

  // 2. Country-wise blocks below GLOBAL
  countryWiseCards.forEach((cc: any) => {
    rowCursorCards = writeCountrySection(cc, rowCursorCards, 1) + 2;
  });
} else {
  // Existing layout for non-global export
  for (let i = 0; i < countryCards.length; i += 2) {
    const left = countryCards[i];
    const right = countryCards[i + 1];

    const nextLeft = writeCountrySection(left, rowCursorCards, 1);

    let nextRight = rowCursorCards;
    if (right) {
      nextRight = writeCountrySection(right, rowCursorCards, 8);
    }

    rowCursorCards = Math.max(nextLeft, nextRight) + 2;
  }
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
        wrapText: false,
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
    platformLabel = "Phormula",
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

const tableBorder = {
  top: { style: "thin" as const, color: { argb: "FF000000" } },
  bottom: { style: "thin" as const, color: { argb: "FF000000" } },
  left: { style: "thin" as const, color: { argb: "FF000000" } },
  right: { style: "thin" as const, color: { argb: "FF000000" } },
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
cell.border = tableBorder;
  });

  // Row 2: light gray month row
  headerRow2.eachCell((cell) => {
    cell.alignment = { vertical: "middle", horizontal: "center" };
    cell.fill = whiteFill;
    cell.font = { bold: true, size: 11, color: { argb: "FF000000" } };
cell.border = tableBorder;
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
      cell.border = tableBorder;
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
  cell.border = tableBorder;
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


export async function exportDispatchExcel(params: {
  filename: string;
  titleLine: string;

  titleCountry: string;
  platformLabel?: string;
  periodLabel: string;

  companyName: string;
  brandName: string;

  dataRows: Record<string, any>[];
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

  const wb = new ExcelJS.Workbook();
  wb.creator = "Skinelements";
  wb.created = new Date();

  const headers = Object.keys(dataRows[0] || {});
  const headerCount = headers.length || 1;

  const snoCol0 = headers.findIndex((h) =>
    String(h).toLowerCase().includes("sno")
  );

  const productNameCol0 = headers.findIndex((h) =>
    String(h).toLowerCase().includes("product name")
  );

  const skuCol0 = headers.findIndex((h) =>
    String(h).toLowerCase() === "sku" || String(h).toLowerCase() === "sku."
  );

  const tableBorder = {
    top: { style: "thin" as const, color: { argb: "FF000000" } },
    bottom: { style: "thin" as const, color: { argb: "FF000000" } },
    left: { style: "thin" as const, color: { argb: "FF000000" } },
    right: { style: "thin" as const, color: { argb: "FF000000" } },
  };

  const whiteFill = {
    type: "pattern" as const,
    pattern: "solid" as const,
    fgColor: { argb: "FFFFFFFF" },
  };

  const totalFill = {
    type: "pattern" as const,
    pattern: "solid" as const,
    fgColor: { argb: "FFEFEFEF" },
  };

  const ws = wb.addWorksheet(safeSheetName("Dispatch"), {
    views: [{ state: "frozen", xSplit: 0, ySplit: 7 }],
  });

  // Header block
  ws.mergeCells(1, 1, 1, headerCount);
  ws.getCell(1, 1).value = titleLine || "";
  ws.getCell(1, 1).font = { bold: false };
  ws.getCell(1, 1).alignment = { horizontal: "left", vertical: "middle" };

  ws.getCell(2, 1).value = `Company Name : ${companyName || ""}`;
  ws.getCell(2, 1).alignment = { horizontal: "left" };

  ws.getCell(2, headerCount).value = `${brandName || ""}`;
  ws.getCell(2, headerCount).alignment = { horizontal: "right" };
  ws.getCell(2, headerCount).font = { bold: true };

  ws.getCell(3, 1).value = `Country : ${titleCountry || ""}`;
  ws.getCell(4, 1).value = `Platform : ${platformLabel || ""}`;
  ws.getCell(5, 1).value = `Period : ${periodLabel || ""}`;

  const headerRowNumber = 7;
  const headerRow = ws.getRow(headerRowNumber);

  headers.forEach((h, i) => {
    const cell = headerRow.getCell(i + 1);

    cell.value = h;
    cell.font = { bold: true, size: 11, color: { argb: "FF000000" } };
    cell.fill = whiteFill;
    cell.alignment = {
      horizontal: "center",
      vertical: "middle",
      wrapText: false,
    };
    cell.border = tableBorder;
  });

  headerRow.height = 18;

  const startDataRow = headerRowNumber + 1;

  dataRows.forEach((r, idx) => {
    const row = ws.getRow(startDataRow + idx);

    headers.forEach((h, c0) => {
      const cell = row.getCell(c0 + 1);
      const v = r?.[h] ?? "";

      if (c0 === snoCol0) {
        cell.value = v === null || v === undefined ? "" : String(v);
        cell.numFmt = "@";
        cell.alignment = {
          horizontal: "center",
          vertical: "middle",
        };
      } else {
        const n = toNumberLoose(v);

        if (n !== null) {
          cell.value = Math.trunc(n);
          cell.numFmt = "#,##0";
          cell.alignment = {
            horizontal: "center",
            vertical: "middle",
          };
        } else {
          cell.value = v;
          cell.alignment = {
            horizontal: c0 === productNameCol0 ? "left" : "center",
            vertical: "middle",
          };
        }
      }

      cell.font = { size: 11, color: { argb: "FF000000" } };
      cell.fill = whiteFill;
      cell.border = tableBorder;
    });
  });

  ws.columns = headers.map((h, idx) => {
    const lower = String(h).toLowerCase();
    let width = Math.min(Math.max(String(h).length + 2, 12), 40);

    if (idx === productNameCol0) width = 28;
    if (idx === skuCol0) width = 18;
    if (lower.includes("coverage")) width = 26;
    if (lower.includes("dispatch")) width = 18;

    return { width };
  });

  // Bold + fill total row
  if (productNameCol0 >= 0) {
    const totalCandidates = new Set(["total", "grand total"]);

    for (let i = 0; i < dataRows.length; i++) {
      const rowNum = startDataRow + i;
      const cellVal = String(
        ws.getCell(rowNum, productNameCol0 + 1).value ?? ""
      )
        .trim()
        .toLowerCase();

      if (totalCandidates.has(cellVal)) {
        const row = ws.getRow(rowNum);

        for (let c = 1; c <= headerCount; c++) {
          const cell = row.getCell(c);

          cell.font = {
            ...(cell.font || {}),
            bold: true,
            size: 11,
            color: { argb: "FF000000" },
          };
          cell.fill = whiteFill;
          cell.border = tableBorder;
        }
      }
    }
  }

  const buf = await wb.xlsx.writeBuffer();

  saveAs(
    new Blob([buf], {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }),
    filename
  );
}

export async function exportPurchaseOrderExcel(params: {
  filename: string;
  titleLine: string;

  titleCountry: string;
  platformLabel?: string;
  periodLabel: string;

  companyName: string;
  brandName: string;

  sheetName?: string;
  dataRows: Record<string, any>[];
}) {
  const {
    filename,
    titleLine,
    titleCountry,
    platformLabel = "Amazon",
    periodLabel,
    companyName,
    brandName,
    sheetName = "PO Data",
    dataRows,
  } = params;

  if (!dataRows?.length) return;

  const wb = new ExcelJS.Workbook();
  wb.creator = "Skinelements";
  wb.created = new Date();

  const headers = Object.keys(dataRows[0] || {});
  const headerCount = headers.length || 1;

  const snoCol0 = headers.findIndex((h) =>
    String(h).toLowerCase().includes("sno")
  );

  const productNameCol0 = headers.findIndex((h) =>
    String(h).toLowerCase().includes("product name")
  );

  const tableBorder = {
    top: { style: "thin" as const, color: { argb: "FF000000" } },
    bottom: { style: "thin" as const, color: { argb: "FF000000" } },
    left: { style: "thin" as const, color: { argb: "FF000000" } },
    right: { style: "thin" as const, color: { argb: "FF000000" } },
  };

  const whiteFill = {
    type: "pattern" as const,
    pattern: "solid" as const,
    fgColor: { argb: "FFFFFFFF" },
  };

  const totalFill = {
    type: "pattern" as const,
    pattern: "solid" as const,
    fgColor: { argb: "FFEFEFEF" },
  };

  const ws = wb.addWorksheet(safeSheetName(sheetName), {
    views: [{ state: "frozen", xSplit: 0, ySplit: 7 }],
  });

  ws.mergeCells(1, 1, 1, headerCount);
  ws.getCell(1, 1).value = titleLine || "";
  ws.getCell(1, 1).font = { bold: false };
  ws.getCell(1, 1).alignment = { horizontal: "left", vertical: "middle" };

  ws.getCell(2, 1).value = `Company Name : ${companyName || ""}`;
  ws.getCell(2, 1).alignment = { horizontal: "left" };

  ws.getCell(2, headerCount).value = `${brandName || ""}`;
  ws.getCell(2, headerCount).alignment = { horizontal: "right" };
  ws.getCell(2, headerCount).font = { bold: true };

  ws.getCell(3, 1).value = `Country : ${titleCountry || ""}`;
  ws.getCell(4, 1).value = `Platform : ${platformLabel || ""}`;
  ws.getCell(5, 1).value = `Period : ${periodLabel || ""}`;

  const headerRowNumber = 7;
  const headerRow = ws.getRow(headerRowNumber);

  headers.forEach((h, i) => {
    const cell = headerRow.getCell(i + 1);

    cell.value = h;
    cell.font = { bold: true, size: 11, color: { argb: "FF000000" } };
    cell.fill = whiteFill;
    cell.alignment = {
      horizontal: "center",
      vertical: "middle",
      wrapText: false,
    };
    cell.border = tableBorder;
  });

  headerRow.height = 18;

  const startDataRow = headerRowNumber + 1;

  dataRows.forEach((r, idx) => {
    const row = ws.getRow(startDataRow + idx);

    headers.forEach((h, c0) => {
      const cell = row.getCell(c0 + 1);
      const v = r?.[h] ?? "";

      if (c0 === snoCol0) {
        cell.value = v === null || v === undefined ? "" : String(v);
        cell.numFmt = "@";
        cell.alignment = {
          horizontal: "center",
          vertical: "middle",
        };
      } else {
        const n = toNumberLoose(v);

        if (n !== null) {
          const headerLower = String(h).toLowerCase();

          cell.value = n;

          if (
            headerLower.includes("cost per unit") ||
            headerLower.includes("po cost") ||
            headerLower.includes("price") ||
            headerLower.includes("amount")
          ) {
            cell.numFmt = "#,##0.00";
          } else {
            cell.numFmt = "#,##0";
          }

          cell.alignment = {
            horizontal: "center",
            vertical: "middle",
          };
        } else {
          cell.value = v;
          cell.alignment = {
            horizontal: c0 === productNameCol0 ? "left" : "center",
            vertical: "middle",
          };
        }
      }

      cell.font = { size: 11, color: { argb: "FF000000" } };
      cell.fill = whiteFill;
      cell.border = tableBorder;
    });
  });

  ws.columns = headers.map((h, idx) => {
    const lower = String(h).toLowerCase();

    if (idx === productNameCol0) return { width: 28 };
    if (lower.includes("dispatch")) return { width: 18 };
    if (lower.includes("inventory")) return { width: 24 };
    if (lower.includes("cost per unit")) return { width: 22 };
    if (lower.includes("po cost")) return { width: 18 };
    if (lower.includes("already raised")) return { width: 18 };
    if (lower.includes("to be raised")) return { width: 18 };

    const maxContentLength = Math.max(
      String(h).length,
      ...dataRows.map((row) => String(row[h] ?? "").length)
    );

    return {
      width: Math.min(Math.max(maxContentLength + 4, 14), 40),
    };
  });

  // Bold + fill total row
  if (productNameCol0 >= 0) {
    const totalCandidates = new Set(["total", "grand total"]);

    for (let i = 0; i < dataRows.length; i++) {
      const rowNum = startDataRow + i;
      const cellVal = String(
        ws.getCell(rowNum, productNameCol0 + 1).value ?? ""
      )
        .trim()
        .toLowerCase();

      if (totalCandidates.has(cellVal)) {
        const row = ws.getRow(rowNum);

        for (let c = 1; c <= headerCount; c++) {
          const cell = row.getCell(c);

          cell.font = {
            ...(cell.font || {}),
            bold: true,
            size: 11,
            color: { argb: "FF000000" },
          };
          cell.fill = whiteFill;
          cell.border = tableBorder;
        }
      }
    }
  }

  const buf = await wb.xlsx.writeBuffer();

  saveAs(
    new Blob([buf], {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }),
    filename
  );
}

export async function exportPnLForecastExcel(params: {
  filename: string;

  titleLine?: string;
  titleCountry?: string;
  platformLabel?: string;
  periodLabel?: string;

  companyName?: string;
  brandName?: string;

  currencyLabel?: string;

  month1Label?: string;
  month2Label?: string;
  month3Label?: string;
  totalLabel?: string;

  productRows: Array<Record<string, any>>;
  summaryRows?: Array<Record<string, any>>;

  chartImageBase64?: string | null;
}) {
  const {
    filename,
    titleLine = "P&L Forecast",
    titleCountry = "",
    platformLabel = "Phormula",
    periodLabel = "",
    companyName = "",
    brandName = "",
    currencyLabel = "",

    month1Label = "P&L Forecast M1",
    month2Label = "P&L Forecast M2",
    month3Label = "P&L Forecast M3",
    totalLabel = "P&L Forecast for 3 months",

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

const tableBorder = {
  top: { style: "thin" as const, color: { argb: "FF000000" } },
  bottom: { style: "thin" as const, color: { argb: "FF000000" } },
  left: { style: "thin" as const, color: { argb: "FF000000" } },
  right: { style: "thin" as const, color: { argb: "FF000000" } },
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

  const headerRow1 = [
    "S. No.",
    "Product Name",
    "SKU",

    month1Label,
    "",
    "",
    "",

    month2Label,
    "",
    "",
    "",

    month3Label,
    "",
    "",
    "",

    totalLabel,
    "",
    "",
    "",
  ];

  const headerRow2 = [
    "",
    "",
    "",

    "Units",
    `Sales (${currencyLabel})`,
    `CM1 (${currencyLabel})`,
    "CM1 %",

    "Units",
    `Sales (${currencyLabel})`,
    `CM1 (${currencyLabel})`,
    "CM1 %",

    "Units",
    `Sales (${currencyLabel})`,
    `CM1 (${currencyLabel})`,
    "CM1 %",

    "Units",
    `Sales (${currencyLabel})`,
    `CM1 (${currencyLabel})`,
    "CM1 %",
  ];

  const headerCount = headerRow1.length;

  const tableRows = [
    ...(productRows || []),
    ...(summaryRows || []),
  ].map((r, index) => {
    const isSummaryRow = !r.sku && r.product_name !== "Total";

    return [
      isSummaryRow || r.product_name === "Total" ? "" : index + 1,
      r.product_name ?? "",
      r.sku ?? "",

      r.forecast_1st ?? "",
      r.Total_Sales_1st ?? "",
      r.profit_1st ?? "",
      r.profit_percentage_1st ?? "",

      r.forecast_2nd ?? "",
      r.Total_Sales_2nd ?? "",
      r.profit_2nd ?? "",
      r.profit_percentage_2nd ?? "",

      r.forecast_3rd ?? "",
      r.Total_Sales_3rd ?? "",
      r.profit_3rd ?? "",
      r.profit_percentage_3rd ?? "",

      r.forecast_sum ?? "",
      r.Total_Sales_sum ?? "",
      r.profit_sum ?? "",
      r.profit_percentage_sum ?? "",
    ];
  });

  /* =========================
     Sheet 1: P&L Forecast Table
  ========================= */
  const ws1 = workbook.addWorksheet("P&L Forecast", {
    views: [{ state: "frozen", xSplit: 0, ySplit: 9 }],
  });

  applyExcelHeader(ws1, headerCount);

  const tableStartRow = 7;

  ws1.getRow(tableStartRow).values = headerRow1;
  ws1.getRow(tableStartRow + 1).values = headerRow2;

  ws1.mergeCells(tableStartRow, 1, tableStartRow + 1, 1);
  ws1.mergeCells(tableStartRow, 2, tableStartRow + 1, 2);
  ws1.mergeCells(tableStartRow, 3, tableStartRow + 1, 3);

  ws1.mergeCells(tableStartRow, 4, tableStartRow, 7);
  ws1.mergeCells(tableStartRow, 8, tableStartRow, 11);
  ws1.mergeCells(tableStartRow, 12, tableStartRow, 15);
  ws1.mergeCells(tableStartRow, 16, tableStartRow, 19);

  [tableStartRow, tableStartRow + 1].forEach((rowNumber) => {
    const row = ws1.getRow(rowNumber);
    row.height = 20;

    for (let c = 1; c <= headerCount; c++) {
      const cell = row.getCell(c);
      cell.alignment = {
        vertical: "middle",
        horizontal: "center",
        wrapText: true,
      };
      cell.fill = whiteFill;
      cell.font = { bold: true, size: 11, color: { argb: "FF000000" } };
      cell.border = tableBorder;
    }
  });

  tableRows.forEach((row, idx) => {
    const excelRow = ws1.getRow(tableStartRow + 2 + idx);
    excelRow.values = row;

    const isTotalRow = String(row[1] ?? "").trim().toLowerCase() === "total";

    excelRow.eachCell((cell, colNumber) => {
      const isNumberValue = typeof cell.value === "number";
      const numeric =
        typeof cell.value === "string" &&
        cell.value !== "" &&
        !isNaN(Number(cell.value));

      if (isNumberValue || numeric) {
        cell.value = Number(cell.value);

        const isPercentColumn = [7, 11, 15, 19].includes(colNumber);
        const isUnitOrSerialColumn = [1, 4, 8, 12, 16].includes(colNumber);

        if (isPercentColumn) {
          cell.numFmt = "0.00%";
          cell.value = Number(cell.value) / 100;
        } else if (isUnitOrSerialColumn) {
          cell.numFmt = "0";
        } else {
          cell.numFmt = "#,##0.00";
        }
      }

      cell.alignment = {
        vertical: "middle",
        horizontal: colNumber === 2 ? "left" : "center",
      };

      cell.fill = whiteFill;
      cell.font = {
        bold: isTotalRow,
        size: 11,
        color: { argb: "FF000000" },
      };
     cell.border = tableBorder;
    });
  });

  ws1.columns = [
    { width: 8 },
    { width: 28 },
    { width: 18 },

    { width: 12 },
    { width: 14 },
    { width: 14 },
    { width: 12 },

    { width: 12 },
    { width: 14 },
    { width: 14 },
    { width: 12 },

    { width: 12 },
    { width: 14 },
    { width: 14 },
    { width: 12 },

    { width: 12 },
    { width: 16 },
    { width: 16 },
    { width: 12 },
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


const normalizeCountryLabel = (countryName: string) => {
  const c = (countryName || "").toLowerCase();
  if (c === "uk") return "UK";
  if (c === "us") return "US";
  if (c === "canada" || c === "ca") return "Canada";
  if (c === "eu") return "EU";
  if (c === "europe") return "Europe";
  if (c === "global") return "Global";
  return countryName || "";
};

const formatSkuHeader = (countryName: string, skuKey: string) => {
  const isGlobal = (countryName || "").toLowerCase() === "global";
  if (!isGlobal) return "SKU";

  const suffix = String(skuKey || "")
    .replace(/^sku_/, "")
    .replace(/_/g, " ")
    .toUpperCase();

  return `SKU ${suffix}`;
};

const formatGrossMarginHeader = (countryName: string, grossKey: string) => {
  const isGlobal = (countryName || "").toLowerCase() === "global";
  if (!isGlobal) return "Gross Margin (%)";

  const suffix = String(grossKey || "")
    .replace(/^gross_margin_/, "")
    .replace(/_/g, " ")
    .toUpperCase();

  return `Gross Margin (%) ${suffix}`;
};

export async function exportWarehouseDataExcel(params: {
  filename: string;
  countryName: string;

  titleLine: string;
  titleCountry: string;
  platformLabel?: string;
  periodLabel: string;

  companyName: string;
  brandName: string;

  dataRows: Record<string, any>[];
}) {
  const {
    filename,
    countryName,
    titleLine,
    titleCountry,
    platformLabel = "Amazon",
    periodLabel,
    companyName,
    brandName,
    dataRows,
  } = params;

  if (!dataRows?.length) return;

  const wb = new ExcelJS.Workbook();
  wb.creator = "Skinelements";
  wb.created = new Date();

  const ws = wb.addWorksheet(safeSheetName("Warehouse Data"), {
    views: [{ state: "frozen", xSplit: 0, ySplit: 7 }],
  });

  const firstRow = dataRows[0] || {};
  const inputHeaders = Object.keys(firstRow);

  const orderedHeaders = [
    "s_no",
    "product_name",
    "sku_uk",
    "sku_us",
    "sku_canada",
    "local_stock",
    "in_transit_units",
    "month",
    "year",
  ].filter((h) => inputHeaders.includes(h));

  const headers = orderedHeaders.length ? orderedHeaders : inputHeaders;
  const headerCount = headers.length || 1;

  const tableBorder = {
  top: { style: "thin" as const, color: { argb: "FF000000" } },
  bottom: { style: "thin" as const, color: { argb: "FF000000" } },
  left: { style: "thin" as const, color: { argb: "FF000000" } },
  right: { style: "thin" as const, color: { argb: "FF000000" } },
};

const whiteFill = {
  type: "pattern" as const,
  pattern: "solid" as const,
  fgColor: { argb: "FFFFFFFF" },
};

  ws.mergeCells(1, 1, 1, headerCount);
  ws.getCell(1, 1).value = titleLine || "";
  ws.getCell(1, 1).font = { bold: false };
  ws.getCell(1, 1).alignment = { horizontal: "left", vertical: "middle" };

  ws.getCell(2, 1).value = `Company Name : ${companyName || ""}`;
  ws.getCell(2, 1).alignment = { horizontal: "left" };

  ws.getCell(2, headerCount).value = `${brandName || ""}`;
  ws.getCell(2, headerCount).alignment = { horizontal: "right" };
  ws.getCell(2, headerCount).font = { bold: true };

  ws.getCell(3, 1).value = `Country : ${titleCountry || normalizeCountryLabel(countryName)}`;
  ws.getCell(4, 1).value = `Platform : ${platformLabel || ""}`;
  ws.getCell(5, 1).value = `Period : ${periodLabel || ""}`;

  const headerRowNumber = 7;
  const headerRow = ws.getRow(headerRowNumber);

  const displayHeaders = headers.map((h) => {
    switch (h) {
      case "s_no":
        return "S.No.";
      case "product_name":
        return "Product Name";
      case "local_stock":
        return "Local Stock";
      case "in_transit_units":
        return "In Transit Units";
      case "month":
        return "Month";
      case "year":
        return "Year";
      case "sku_uk":
      case "sku_us":
      case "sku_canada":
        return formatSkuHeader(countryName, h);
      default:
        if (h.startsWith("sku_")) return formatSkuHeader(countryName, h);
        return h
          .replace(/_/g, " ")
          .replace(/\b\w/g, (c) => c.toUpperCase());
    }
  });

displayHeaders.forEach((h, i) => {
  const cell = headerRow.getCell(i + 1);

  cell.value = h;
  cell.font = { bold: true, size: 11, color: { argb: "FF000000" } };
  cell.fill = whiteFill;
  cell.alignment = {
    horizontal: "center",
    vertical: "middle",
    wrapText: false,
  };
  cell.border = tableBorder;
});

  headerRow.height = 18;

  const startDataRow = headerRowNumber + 1;

dataRows.forEach((r, idx) => {
  const row = ws.getRow(startDataRow + idx);

  headers.forEach((h, c0) => {
    const cell = row.getCell(c0 + 1);
    const v = r?.[h];

    if (h === "s_no") {
      cell.value = v ?? idx + 1;
      cell.numFmt = "#,##0";
      cell.alignment = {
        horizontal: "center",
        vertical: "middle",
      };
    } else if (h === "product_name") {
      cell.value = v ?? "";
      cell.alignment = {
        horizontal: "left",
        vertical: "middle",
      };
    } else if (h === "month") {
      const monthVal =
        typeof v === "string" && v.length
          ? v.charAt(0).toUpperCase() + v.slice(1).toLowerCase()
          : v ?? "";

      cell.value = monthVal;
      cell.alignment = {
        horizontal: "center",
        vertical: "middle",
      };
    } else {
      const n = toNumberLoose(v);

      if (n !== null && !String(h).includes("sku")) {
        cell.value = Number.isInteger(n) ? n : n;
        cell.numFmt = Number.isInteger(n) ? "#,##0" : "#,##0.00";
      } else {
        cell.value = v ?? "";
      }

      cell.alignment = {
        horizontal: "center",
        vertical: "middle",
      };
    }

    cell.font = { size: 11, color: { argb: "FF000000" } };
    cell.fill = whiteFill;
    cell.border = tableBorder;
  });
});

  ws.columns = headers.map((h) => {
    if (h === "s_no") return { width: 10 };
    if (h === "product_name") return { width: 28 };
    if (h.startsWith("sku_")) return { width: 18 };
    if (h === "local_stock") return { width: 16 };
    if (h === "in_transit_units") return { width: 18 };
    if (h === "month") return { width: 14 };
    if (h === "year") return { width: 12 };
    return { width: 16 };
  });

  const buf = await wb.xlsx.writeBuffer();
  saveAs(
    new Blob([buf], {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }),
    filename
  );
}


export async function exportSkuInformationExcel(params: {
  filename: string;
  countryName: string;

  titleLine: string;
  titleCountry: string;
  platformLabel?: string;
  periodLabel: string;

  companyName: string;
  brandName: string;

  dataRows: Record<string, any>[];
}) {
  const {
    filename,
    countryName,
    titleLine,
    titleCountry,
    platformLabel = "Amazon",
    periodLabel,
    companyName,
    brandName,
    dataRows,
  } = params;

  if (!dataRows?.length) return;

  const wb = new ExcelJS.Workbook();
  wb.creator = "Skinelements";
  wb.created = new Date();

  const ws = wb.addWorksheet("SKU Information", {
    views: [{ state: "frozen", xSplit: 0, ySplit: 7 }],
  });

  const headers = Object.keys(dataRows[0] || {});
  const headerCount = headers.length || 1;

  const tableBorder = {
  top: { style: "thin" as const, color: { argb: "FF000000" } },
  bottom: { style: "thin" as const, color: { argb: "FF000000" } },
  left: { style: "thin" as const, color: { argb: "FF000000" } },
  right: { style: "thin" as const, color: { argb: "FF000000" } },
};

const whiteFill = {
  type: "pattern" as const,
  pattern: "solid" as const,
  fgColor: { argb: "FFFFFFFF" },
};

  // Header section
  ws.mergeCells(1, 1, 1, headerCount);
  ws.getCell(1, 1).value = titleLine || "";
  ws.getCell(1, 1).alignment = { horizontal: "left" };

  ws.getCell(2, 1).value = `Company Name : ${companyName}`;
  ws.getCell(2, headerCount).value = brandName;
  ws.getCell(2, headerCount).alignment = { horizontal: "right" };

  ws.getCell(3, 1).value = `Country : ${titleCountry}`;
  ws.getCell(4, 1).value = `Platform : ${platformLabel}`;
  ws.getCell(5, 1).value = `Period : ${periodLabel}`;

  // Table header
  const headerRowNumber = 7;
  const headerRow = ws.getRow(headerRowNumber);

headers.forEach((h, i) => {
  const cell = headerRow.getCell(i + 1);

  cell.value = h
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());

  cell.font = { bold: true, size: 11, color: { argb: "FF000000" } };
  cell.fill = whiteFill;
  cell.alignment = {
    horizontal: "center",
    vertical: "middle",
  };
  cell.border = tableBorder;
});

  // Data
  const startRow = headerRowNumber + 1;

  dataRows.forEach((row, idx) => {
  const excelRow = ws.getRow(startRow + idx);

  headers.forEach((h, colIdx) => {
    const cell = excelRow.getCell(colIdx + 1);
    const value = row[h] ?? "";

    const n = toNumberLoose(value);

    if (n !== null && h !== "product_name" && !String(h).includes("sku")) {
      cell.value = n;
      cell.numFmt = Number.isInteger(n) ? "#,##0" : "#,##0.00";
    } else {
      cell.value = value;
    }

    cell.font = { size: 11, color: { argb: "FF000000" } };
    cell.fill = whiteFill;
    cell.alignment = {
      horizontal: h === "product_name" ? "left" : "center",
      vertical: "middle",
    };
    cell.border = tableBorder;
  });
});

  ws.columns = headers.map((h) => ({
    width: Math.min(Math.max(h.length + 2, 14), 30),
  }));

  const buf = await wb.xlsx.writeBuffer();
  saveAs(
    new Blob([buf], {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }),
    filename
  );
}

/* =========================
   SKU Analysis MTD export - black & white
========================= */
export function exportSkuAnalysisMtdExcel(params: {
  filename: string;
  titleLine: string;

  countryName: string;
  titleCountry: string;
  platformLabel?: string;

  periodLabel: string;
  companyName: string;
  brandName: string;
  homeCurrencyCode?: string;

  month2Label: string;

  categorizedGrowth: {
    all_skus?: any[];
    top_80_skus?: any[];
    new_skus?: any[];
    reviving_skus?: any[];
    new_or_reviving_skus?: any[];
    other_skus?: any[];
  };
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
    month2Label,
    categorizedGrowth,
  } = params;

  const currencySymbol = getCurrencySymbol({ countryName, homeCurrencyCode });

  const allSkuRows =
    categorizedGrowth.all_skus?.length
      ? categorizedGrowth.all_skus
      : [
          ...(categorizedGrowth.top_80_skus || []),
          ...(categorizedGrowth.new_skus || []),
          ...(categorizedGrowth.reviving_skus || []),
          ...(categorizedGrowth.other_skus || []),
        ];

  const tabs = [
    {
      sheetName: "All SKUs",
      rows: allSkuRows,
    },
    {
      sheetName: "Top 80 SKUs",
      rows: categorizedGrowth.top_80_skus || [],
    },
    {
      sheetName: "New SKUs",
      rows: categorizedGrowth.new_skus || [],
    },
    {
      sheetName: "Reviving SKUs",
      rows: categorizedGrowth.reviving_skus || [],
    },
    {
      sheetName: "Other SKUs",
      rows: categorizedGrowth.other_skus || [],
    },
  ];

  const currentMonth = month2Label || "Current";

  const headers = [
    "S.No.",
    "Product Name",
    `Sales Mix (${currentMonth})`,
    `Profit Mix (${currentMonth})`,
    "Sales Mix Change (%)",
    "Unit Growth (%)",
    "ASP Growth (%)",
    "Net Sales Growth (%)",
    "CM1 Profit Per Unit (%)",
    "CM1 Profit Impact (%)",
  ];

  const headerCount = headers.length;
  const ANCHOR_COL_1_BASED = headerCount;

  const topExtraLines = [
    `Country : ${titleCountry}`,
    `Platform : ${platformLabel}`,
    `Currency : ${currencySymbol}`,
    `Period : ${periodLabel}`,
  ];

  const isTotalLikeRow = (row: any) => {
    const name = String(row?.product_name || row?.["Product Name"] || "")
      .trim()
      .toLowerCase();

    return name === "total" || name === "grand total" || name.includes("total");
  };

  const cleanRows = (rows: any[] = []) =>
    rows.filter((row) => {
      const name = String(row?.product_name || row?.["Product Name"] || "").trim();
      return name && !isTotalLikeRow(row);
    });

  const getNumber = (value: any): number => {
    if (value && typeof value === "object") {
      return toNumberLoose(value.value) ?? 0;
    }

    return toNumberLoose(value) ?? 0;
  };

  const getGrowthValue = (row: any, keys: string[]) => {
    for (const key of keys) {
      const value = row?.[key];

      if (value !== undefined && value !== null && value !== "") {
        return getNumber(value);
      }
    }

    return 0;
  };

  const getSalesMix = (row: any) => {
    return getGrowthValue(row, [
      "Sales Mix (Month2)",
      "sales_mix_month2",
      "sales_mix_curr",
      "sales_mix",
    ]);
  };

  const getProfit = (row: any) => {
    return getGrowthValue(row, [
      "profit_month2",
      "profit_curr",
      "profit",
      "CM1 Profit",
    ]);
  };

  const getQtyPrev = (row: any) =>
    getGrowthValue(row, ["quantity_month1", "quantity_prev"]);

  const getQtyCurr = (row: any) =>
    getGrowthValue(row, ["quantity_month2", "quantity_curr", "quantity"]);

  const getNetSalesPrev = (row: any) =>
    getGrowthValue(row, ["net_sales_month1", "net_sales_prev"]);

  const getNetSalesCurr = (row: any) =>
    getGrowthValue(row, ["net_sales_month2", "net_sales_curr", "net_sales"]);

  const getProfitPrev = (row: any) =>
    getGrowthValue(row, ["profit_month1", "profit_prev"]);

  const getProfitCurr = (row: any) =>
    getGrowthValue(row, ["profit_month2", "profit_curr", "profit"]);

  const pct = (oldVal: number, newVal: number) =>
    oldVal ? ((newVal - oldVal) / Math.abs(oldVal)) * 100 : 0;

  const computeProfitMixDenominator = (rows: any[]) => {
    return cleanRows(rows).reduce((sum, row) => sum + getProfit(row), 0);
  };

  const computeTotalRow = (rows: any[], profitMixDenom: number) => {
    const cleaned = cleanRows(rows);

    const qtyPrev = cleaned.reduce((sum, row) => sum + getQtyPrev(row), 0);
    const qtyCurr = cleaned.reduce((sum, row) => sum + getQtyCurr(row), 0);

    const netSalesPrev = cleaned.reduce((sum, row) => sum + getNetSalesPrev(row), 0);
    const netSalesCurr = cleaned.reduce((sum, row) => sum + getNetSalesCurr(row), 0);

    const profitPrev = cleaned.reduce((sum, row) => sum + getProfitPrev(row), 0);
    const profitCurr = cleaned.reduce((sum, row) => sum + getProfitCurr(row), 0);

    const aspPrev = qtyPrev ? netSalesPrev / qtyPrev : 0;
    const aspCurr = qtyCurr ? netSalesCurr / qtyCurr : 0;

    const unitProfitPrev = qtyPrev ? profitPrev / qtyPrev : 0;
    const unitProfitCurr = qtyCurr ? profitCurr / qtyCurr : 0;

    const sectionProfit = cleaned.reduce((sum, row) => sum + getProfit(row), 0);

    return [
      "",
      "Total",
      100,
      profitMixDenom ? (sectionProfit / profitMixDenom) * 100 : 0,
      0,
      pct(qtyPrev, qtyCurr),
      pct(aspPrev, aspCurr),
      pct(netSalesPrev, netSalesCurr),
      pct(unitProfitPrev, unitProfitCurr),
      pct(profitPrev, profitCurr),
    ];
  };

  const buildSheetRows = (rows: any[], profitMixDenom: number) => {
    const cleaned = cleanRows(rows);

    const body = cleaned.map((row, index) => {
      const profit = getProfit(row);

      return [
        index + 1,
        row?.product_name || row?.["Product Name"] || row?.sku || "",
        getSalesMix(row),
        profitMixDenom ? (profit / profitMixDenom) * 100 : 0,
        getGrowthValue(row, ["Sales Mix Change", "Sales Mix Change (%)"]),
        getGrowthValue(row, ["Unit Growth", "Unit Growth (%)"]),
        getGrowthValue(row, ["ASP Growth", "ASP Growth (%)"]),
        getGrowthValue(row, [
          "Sales Growth",
          "Net Sales Growth",
          "Net Sales Growth (%)",
        ]),
        getGrowthValue(row, [
          "Profit Per Unit",
          "Profit Per Unit (%)",
          "CM1 Profit Per Unit (%)",
        ]),
        getGrowthValue(row, [
          "CM1 Profit Impact",
          "CM1 Profit Impact (%)",
        ]),
      ];
    });

    body.push(computeTotalRow(cleaned, profitMixDenom));

    return body;
  };

  const wb = XLSX.utils.book_new();

  tabs.forEach((tab) => {
    const rows = cleanRows(tab.rows);

    if (!rows.length) return;

    const profitMixDenom =
      tab.sheetName === "All SKUs"
        ? computeProfitMixDenominator(allSkuRows)
        : computeProfitMixDenominator(allSkuRows);

    const topAoA = buildTopAoA({
      headerCount,
      title: `${titleLine} - ${tab.sheetName}`,
      companyName,
      brandName,
      anchorCol1Based: ANCHOR_COL_1_BASED,
      extraLines: topExtraLines,
    });

    const headerRowIndex = topAoA.length;
    const bodyAoA = buildSheetRows(rows, profitMixDenom);
    const sheetAoA = [...topAoA, headers, ...bodyAoA];

    const ws = XLSX.utils.aoa_to_sheet(sheetAoA);

    ws["!freeze"] = { xSplit: 0, ySplit: headerRowIndex + 1 };

    ws["!cols"] = [
      { wch: 8 },
      { wch: 32 },
      { wch: 20 },
      { wch: 20 },
      { wch: 22 },
      { wch: 18 },
      { wch: 18 },
      { wch: 22 },
      { wch: 26 },
      { wch: 24 },
    ];

    applyTopStyles(ws, headerCount, ANCHOR_COL_1_BASED);

const tableBorder = {
  top: { style: "thin" as const, color: { argb: "FF000000" } },
  bottom: { style: "thin" as const, color: { argb: "FF000000" } },
  left: { style: "thin" as const, color: { argb: "FF000000" } },
  right: { style: "thin" as const, color: { argb: "FF000000" } },
};

const whiteFill = {
  type: "pattern" as const,
  pattern: "solid" as const,
  fgColor: { argb: "FFFFFFFF" },
};

const totalFill = {
  type: "pattern" as const,
  pattern: "solid" as const,
  fgColor: { argb: "FFEFEFEF" },
};

    // Header row: black & white only
    for (let c = 0; c < headerCount; c++) {
      const addr = XLSX.utils.encode_cell({ r: headerRowIndex, c });
      if (!ws[addr]) continue;

      ws[addr].s = {
        ...(ws[addr].s || {}),
        font: { bold: true, sz: 11, color: { rgb: "000000" } },
        fill: {
          fgColor: { rgb: "FFFFFF" },
        },
        alignment: {
          horizontal: "center",
          vertical: "center",
          wrapText: true,
        },
border: tableBorder,
      };
    }

    const startBodyRow = headerRowIndex + 1;
    const endBodyRow = startBodyRow + bodyAoA.length - 1;

    for (let r = startBodyRow; r <= endBodyRow; r++) {
      const isTotalRow = r === endBodyRow;

      for (let c = 0; c < headerCount; c++) {
        const addr = XLSX.utils.encode_cell({ r, c });
        const cell = ws[addr];
        if (!cell) continue;

        cell.s = {
          ...(cell.s || {}),
          font: {
            ...(cell.s?.font || {}),
            bold: isTotalRow,
            sz: 11,
            color: { rgb: "000000" },
          },
          fill: {
            fgColor: { rgb: "FFFFFF" },
          },
          alignment: {
            horizontal: c === 1 ? "left" : "center",
            vertical: "center",
            wrapText: true,
          },
        border: tableBorder,
        };

        // Percent formatting for all metric columns from Sales Mix onward
        if (c >= 2 && isNumber(cell.v)) {
          cell.v = Number(cell.v) / 100;
          cell.z = "0.00%";
        }
      }
    }

    XLSX.utils.book_append_sheet(wb, ws, safeSheetName(tab.sheetName));
  });

  XLSX.writeFile(wb, filename);
}


type AgeingRiskSalesDataKey = "salesLast30Days" | "unitsSold";

const getAgeingRiskSalesValue = (
  row: Record<string, any>,
  unitSalesDataKey: AgeingRiskSalesDataKey = "salesLast30Days"
) => {
  const primaryValue =
    unitSalesDataKey === "unitsSold"
      ? row.unitsSold ?? row["Units Sold"]
      : row.salesLast30Days ??
      row["Sales Last 30 Days"] ??
      row["Unit Sales in Last 30 Days"];

  const fallbackValue =
    unitSalesDataKey === "unitsSold"
      ? row.salesLast30Days ??
      row["Sales Last 30 Days"] ??
      row["Unit Sales in Last 30 Days"]
      : row.unitsSold ?? row["Units Sold"];

  const primaryNumber = toNumberLoose(primaryValue);

  if (primaryNumber !== null) return primaryNumber;

  const fallbackNumber = toNumberLoose(fallbackValue);

  return fallbackNumber ?? 0;
};

const appendCurrentInventoryAgeingRiskSheet = (params: {
  wb: XLSX.WorkBook;
  sheetName: string;
  filename?: string;
  titleLine?: string;
  countryName?: string;
  countryLabel?: string;
  titleCountry?: string;
  platformLabel?: string;
  periodLabel?: string;
  companyName?: string;
  brandName?: string;
  homeCurrencyCode?: string;
  buckets: {
    key: string;
    label: string;
    color?: string;
  }[];
  dataRows: Record<string, any>[];
  showInventoryAlerts?: boolean;
  salesLast30DaysLabel?: string;
  unitSalesDataKey?: AgeingRiskSalesDataKey;
  storageCostCurrencySymbol?: string;
}) => {
  const {
    wb,
    sheetName,
    titleLine = "Ageing Risk Heatmap",
    countryName = "",
    countryLabel = "",
    titleCountry = "",
    platformLabel = "Phormula",
    periodLabel = "",
    companyName = "",
    brandName = "",
    homeCurrencyCode,
    buckets,
    dataRows,
    showInventoryAlerts = true,
    salesLast30DaysLabel = "Sales Last 30 Days",
    unitSalesDataKey = "salesLast30Days",
    storageCostCurrencySymbol,
  } = params;

  const toNum = (value: any) => toNumberLoose(value) ?? 0;
  const displayCountry = titleCountry || countryLabel || countryName || "";
  const currencySymbol = getCurrencySymbol({
    countryName: countryName || displayCountry,
    homeCurrencyCode,
  });
  const storageCostHeaderLabel = `Est. Storage Cost (${storageCostCurrencySymbol || currencySymbol || "$"})`;

  const rowProductName = (row: Record<string, any>) =>
    String(row?.productName || row?.["Product Name"] || "")
      .trim()
      .toLowerCase();
  const rowSku = (row: Record<string, any>) =>
    String(row?.sku || row?.SKU || "")
      .trim()
      .toLowerCase();
  const isOthersLikeRow = (row: Record<string, any>) =>
    row?.isOthersRow === true || rowProductName(row) === "others";
  const isTotalLikeRow = (row: Record<string, any>) =>
    row?.isTotalRow === true ||
    rowProductName(row) === "total" ||
    rowProductName(row) === "grand total" ||
    rowSku(row) === "total" ||
    rowSku(row) === "grand total";
  const isPercentageLikeRow = (row: Record<string, any>) =>
    row?.isPercentageRow === true ||
    row?.is_percentage_row === true ||
    rowProductName(row) === "% of total" ||
    rowProductName(row) === "percentage" ||
    rowSku(row) === "% of total" ||
    rowSku(row) === "percentage";

  const productRows = [...dataRows]
    .filter((row) => {
      const productName = rowProductName(row);
      const sku = rowSku(row);

      if (!productName && !sku) return false;

      return (
        !isOthersLikeRow(row) &&
        !isTotalLikeRow(row) &&
        !isPercentageLikeRow(row)
      );
    })
    .sort(
      (a, b) =>
        getAgeingRiskSalesValue(b, unitSalesDataKey) -
        getAgeingRiskSalesValue(a, unitSalesDataKey)
    );

  const buildSummaryRow = (
    productName: string,
    rows: Record<string, any>[],
    extra: Record<string, any> = {}
  ) => {
    const summary: Record<string, any> = {
      productName,
      sku: productName === "Total" ? "" : "-",
      salesRank: "",
      inventoryAlert: "",
      ...extra,
    };

    buckets.forEach((bucket) => {
      summary[bucket.key] = rows.reduce(
        (sum, row) => sum + toNum(row[bucket.key]),
        0
      );
    });

    summary.currentFba = rows.reduce(
      (sum, row) => sum + toNum(row.currentFba ?? row.available),
      0
    );
    summary.currentAwd = rows.reduce(
      (sum, row) => sum + toNum(row.currentAwd),
      0
    );
    summary.transitFba = rows.reduce(
      (sum, row) => sum + toNum(row.transitFba ?? row.fcTransfer),
      0
    );
    summary.transitAwd = rows.reduce(
      (sum, row) => sum + toNum(row.transitAwd),
      0
    );
    summary.totalInStock = rows.reduce(
      (sum, row) =>
        sum +
        (toNum(row.totalInStock) ||
          toNum(row.currentFba ?? row.available) + toNum(row.currentAwd)),
      0
    );
    summary.totalInTransit = rows.reduce(
      (sum, row) =>
        sum +
        (toNum(row.totalInTransit) ||
          toNum(row.transitFba ?? row.fcTransfer) + toNum(row.transitAwd)),
      0
    );
    summary.unsellableFba = rows.reduce(
      (sum, row) => sum + toNum(row.unsellableFba ?? row.unsellableUnits),
      0
    );
    summary.unsellableAwd = rows.reduce(
      (sum, row) => sum + toNum(row.unsellableAwd),
      0
    );
    summary.storageCostUsd = rows.reduce(
      (sum, row) => sum + toNum(row.storageCostUsd),
      0
    );
    summary.salesLast30Days = rows.reduce(
      (sum, row) => sum + getAgeingRiskSalesValue(row, "salesLast30Days"),
      0
    );
    summary.unitsSold = rows.reduce(
      (sum, row) => sum + getAgeingRiskSalesValue(row, "unitsSold"),
      0
    );

    const summarySales = getAgeingRiskSalesValue(summary, unitSalesDataKey);

    summary.coverageRatio =
      summarySales > 0 ? toNum(summary.totalInStock) / summarySales : "";
    summary.coverageCurrentAndTransit =
      summarySales > 0
        ? (toNum(summary.totalInStock) + toNum(summary.totalInTransit)) /
        summarySales
        : "";

    return summary;
  };

  const mergeDefinedValues = (
    fallback: Record<string, any>,
    source?: Record<string, any>
  ) => {
    if (!source) return fallback;

    const merged = { ...fallback };

    Object.entries(source).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "") {
        merged[key] = value;
      }
    });

    return merged;
  };

  const existingTotalRow = dataRows.find(isTotalLikeRow);
  const existingPercentageRow = dataRows.find(isPercentageLikeRow);
  const computedTotalRow = buildSummaryRow(
    "Total",
    productRows,
    { isTotalRow: true }
  );
  const totalRow: Record<string, any> = {
    ...mergeDefinedValues(computedTotalRow, existingTotalRow),
    productName: "Total",
    sku: "",
    isTotalRow: true,
    salesRank: "",
    inventoryAlert: "",
  };
  const percentageRow: Record<string, any> | null = existingPercentageRow
    ? {
      ...existingPercentageRow,
      productName: "% of Total",
      sku: "",
      isPercentageRow: true,
    }
    : null;

  const exportRows: Record<string, any>[] = [
    ...productRows,
    totalRow,
    ...(percentageRow ? [percentageRow] : []),
  ];

  const leftCols = [
    { key: "sno", label: "Sno." },
    { key: "productName", label: "Product Name" },
    { key: "sku", label: "SKU" },
    { key: "salesRank", label: "Sales Rank" },
  ];

  const groups = [
    {
      label: "Current Inventory",
      cols: [
        { key: "currentFba", label: "FBA" },
        { key: "currentAwd", label: "AWD" },
      ],
    },
    {
      label: "In Transit Inventory",
      cols: [
        { key: "transitFba", label: "FBA" },
        { key: "transitAwd", label: "AWD" },
      ],
    },
    {
      label: "Total Sellable Inventory",
      cols: [
        { key: "totalInStock", label: "In Stock" },
        { key: "totalInTransit", label: "In transit" },
      ],
    },
    {
      label: "Unsellable Inventory",
      cols: [
        { key: "unsellableFba", label: "FBA" },
        { key: "unsellableAwd", label: "AWD" },
      ],
    },
    {
      label: "Breakup - FBA Inventory",
      cols: buckets.map((bucket) => ({
        key: bucket.key,
        label: bucket.label,
      })),
    },
    {
      label: "Sales & Coverage Ratio",
      cols: [
        { key: "salesValue", label: salesLast30DaysLabel },
        {
          key: "coverageRatio",
          label: "Coverage Ratio (Current Inventory)",
        },
        {
          key: "coverageCurrentAndTransit",
          label: "Coverage Ratio (Current + In transit)",
        },
      ],
    },
  ];

  const rightCols = [
    {
      key: "storageCostUsd",
      label: storageCostHeaderLabel,
    },
    ...(showInventoryAlerts
      ? [{ key: "inventoryAlert", label: "Alerts" }]
      : []),
  ];

  const headers = [
    ...leftCols.map((col) => col.label),
    ...groups.flatMap((group) => group.cols.map((col) => col.label)),
    ...rightCols.map((col) => col.label),
  ];

  const headerTopRow = [
    ...leftCols.map((col) => col.label),
    ...groups.flatMap((group) =>
      group.cols.map((_, index) => (index === 0 ? group.label : ""))
    ),
    ...rightCols.map((col) => col.label),
  ];

  const headerSubRow = [
    ...leftCols.map(() => ""),
    ...groups.flatMap((group) => group.cols.map((col) => col.label)),
    ...rightCols.map(() => ""),
  ];

  const headerCount = headers.length;
  const ANCHOR_COL_1_BASED = headerCount;
  const tableBorder = {
    top: { style: "thin", color: { rgb: "000000" } },
    bottom: { style: "thin", color: { rgb: "000000" } },
    left: { style: "thin", color: { rgb: "000000" } },
    right: { style: "thin", color: { rgb: "000000" } },
  };

  const topAoA = buildTopAoA({
    headerCount,
    title: titleLine,
    companyName,
    brandName,
    anchorCol1Based: ANCHOR_COL_1_BASED,
    extraLines: [
      `Country : ${displayCountry}`,
      `Platform : ${platformLabel}`,
      `Currency : ${currencySymbol}`,
      `Period : ${periodLabel}`,
    ],
  });

  const formatNumber = (value: any, decimals = 0) => {
    const n = toNum(value);

    if (!Number.isFinite(n) || n === 0) return "";

    return decimals > 0 ? Number(n.toFixed(decimals)) : n;
  };
  const formatPercentValue = (value: any) => {
    if (value === null || value === undefined || value === "") return "";

    const n = toNum(value);
    if (!Number.isFinite(n) || n === 0) return "";

    return `${n.toFixed(2)}%`;
  };

  const bodyRows = exportRows.map((row, index) => {
    const isTotalRow = row.isTotalRow;
    const isPercentageRow = isPercentageLikeRow(row);
    const salesValue = getAgeingRiskSalesValue(row, unitSalesDataKey);
    const totalInStock =
      toNum(row.totalInStock) ||
      toNum(row.currentFba ?? row.available) + toNum(row.currentAwd);
    const totalInTransit =
      toNum(row.totalInTransit) ||
      toNum(row.transitFba ?? row.fcTransfer) + toNum(row.transitAwd);
    const coverageCurrentAndTransit =
      toNum(row.coverageCurrentAndTransit) ||
      (salesValue > 0 ? (totalInStock + totalInTransit) / salesValue : 0);

    if (isPercentageRow) {
      return [
        "",
        "% of Total",
        "",
        "",
        formatPercentValue(row.currentFba),
        formatPercentValue(row.currentAwd),
        formatPercentValue(row.transitFba),
        formatPercentValue(row.transitAwd),
        formatPercentValue(row.totalInStock ?? row.totalUnits ?? row.available),
        formatPercentValue(row.totalInTransit),
        formatPercentValue(row.unsellableFba ?? row.unsellableUnits),
        formatPercentValue(row.unsellableAwd),
        ...buckets.map((bucket) => formatPercentValue(row[bucket.key])),
        "",
        "",
        "",
        "",
        ...(showInventoryAlerts ? [""] : []),
      ];
    }

    return [
      isTotalRow ? "" : index + 1,
      row.productName || row["Product Name"] || "",
      isTotalRow ? "" : row.sku || row.SKU || "-",
      isTotalRow ? "" : row.salesRank || row["Sales Rank"] || "",
      formatNumber(row.currentFba ?? row.available),
      formatNumber(row.currentAwd),
      formatNumber(row.transitFba ?? row.fcTransfer),
      formatNumber(row.transitAwd),
      formatNumber(totalInStock),
      formatNumber(totalInTransit),
      formatNumber(row.unsellableFba ?? row.unsellableUnits),
      formatNumber(row.unsellableAwd),
      ...buckets.map((bucket) => formatNumber(row[bucket.key])),
      formatNumber(salesValue),
      formatNumber(row.coverageRatio, 2),
      formatNumber(coverageCurrentAndTransit, 2),
      formatNumber(row.storageCostUsd, 2),
      ...(showInventoryAlerts ? [isTotalRow ? "" : row.inventoryAlert || ""] : []),
    ];
  });

  const sheetAoA = [...topAoA, headerTopRow, headerSubRow, ...bodyRows];
  const ws = XLSX.utils.aoa_to_sheet(sheetAoA);
  const headerTopRowIndex = topAoA.length;
  const headerSubRowIndex = headerTopRowIndex + 1;
  const firstBodyRowIndex = headerSubRowIndex + 1;

  const merges: XLSX.Range[] = [];

  for (let c = 0; c < leftCols.length; c++) {
    merges.push({
      s: { r: headerTopRowIndex, c },
      e: { r: headerSubRowIndex, c },
    });
  }

  let groupStartCol = leftCols.length;

  groups.forEach((group) => {
    const groupEndCol = groupStartCol + group.cols.length - 1;

    merges.push({
      s: { r: headerTopRowIndex, c: groupStartCol },
      e: { r: headerTopRowIndex, c: groupEndCol },
    });

    groupStartCol = groupEndCol + 1;
  });

  for (let c = groupStartCol; c < headerCount; c++) {
    merges.push({
      s: { r: headerTopRowIndex, c },
      e: { r: headerSubRowIndex, c },
    });
  }

  ws["!merges"] = [...(ws["!merges"] || []), ...merges];
  ws["!freeze"] = {
    xSplit: 0,
    ySplit: headerSubRowIndex + 1,
  };
  ws["!cols"] = headers.map((header) => {
    if (header === "Sno.") return { wch: 8 };
    if (header === "Product Name") return { wch: 28 };
    if (header === "SKU") return { wch: 18 };
    if (header === "Sales Rank") return { wch: 14 };
    if (header.includes("Coverage Ratio")) return { wch: 26 };
    if (header === storageCostHeaderLabel) return { wch: 22 };
    if (header === "Alerts") return { wch: 28 };
    if (header === salesLast30DaysLabel) return { wch: 24 };
    return { wch: 14 };
  });

  applyTopStyles(ws, headerCount, ANCHOR_COL_1_BASED);

  const range = XLSX.utils.decode_range(ws["!ref"] || "A1:A1");

  for (let r = headerTopRowIndex; r <= headerSubRowIndex; r++) {
    for (let c = 0; c < headerCount; c++) {
      const addr = XLSX.utils.encode_cell({ r, c });

      if (!ws[addr]) {
        ws[addr] = { t: "s", v: "" };
      }

      ws[addr].s = {
        ...(ws[addr].s || {}),
        font: {
          bold: true,
          sz: 11,
          color: { rgb: "000000" },
        },
        fill: {
          fgColor: { rgb: "FFFFFF" },
        },
        alignment: {
          horizontal: "center",
          vertical: "center",
          wrapText: true,
        },
        border: tableBorder,
      };
    }
  }

  for (let r = firstBodyRowIndex; r <= range.e.r; r++) {
    const productNameAddr = XLSX.utils.encode_cell({ r, c: 1 });
    const productName = String(ws[productNameAddr]?.v || "")
      .trim()
      .toLowerCase();
    const isTotalRow = productName === "total";
    const isPercentageRow = productName === "% of total";

    for (let c = 0; c < headerCount; c++) {
      const addr = XLSX.utils.encode_cell({ r, c });

      if (!ws[addr]) {
        ws[addr] = { t: "s", v: "" };
      }

      const cell = ws[addr];
      const header = headers[c];

      if (
        header !== "Product Name" &&
        header !== "SKU" &&
        header !== "Alerts" &&
        !isPercentageRow &&
        cell.v !== ""
      ) {
        const n = toNumberLoose(cell.v);

        if (n !== null) {
          cell.v = n;
          cell.t = "n";
          cell.z = header.includes("Coverage Ratio") ||
            header === storageCostHeaderLabel
            ? "#,##0.00"
            : "#,##0";
        }
      }

      cell.s = {
        ...(cell.s || {}),
        font: {
          bold: isTotalRow || isPercentageRow,
          sz: 11,
          color: { rgb: "000000" },
        },
        fill: {
          fgColor: { rgb: "FFFFFF" },
        },
        alignment: {
          horizontal: c === 1 ? "left" : "center",
          vertical: "center",
          wrapText: true,
        },
        border: tableBorder,
      };
    }
  }

  XLSX.utils.book_append_sheet(wb, ws, safeSheetName(sheetName));
};

export function exportAgeingRiskHeatmapExcel(params: {
  filename: string;
  titleLine?: string;

  countryName?: string;
  countryLabel?: string;
  titleCountry?: string;

  platformLabel?: string;
  periodLabel?: string;

  companyName?: string;
  brandName?: string;
  homeCurrencyCode?: string;

  buckets: {
    key: string;
    label: string;
    color?: string;
  }[];

  dataRows: Record<string, any>[];
  showInventoryAlerts?: boolean;
  salesLast30DaysLabel?: string;
  useCurrentInventoryTableLayout?: boolean;
  unitSalesDataKey?: AgeingRiskSalesDataKey;
  storageCostCurrencySymbol?: string;
}) {
  const {
    filename,
    titleLine = "Ageing Risk Heatmap",

    countryName = "",
    countryLabel = "",
    titleCountry = "",

    platformLabel = "Phormula",
    periodLabel = "",

    companyName = "",
    brandName = "",
    homeCurrencyCode,

    buckets,
    dataRows,
    showInventoryAlerts = true,
    salesLast30DaysLabel = "Unit Sales in Last 30 Days",
    useCurrentInventoryTableLayout = false,
    unitSalesDataKey = "salesLast30Days",
    storageCostCurrencySymbol,
  } = params;

  if (!dataRows?.length) return;

  if (useCurrentInventoryTableLayout) {
    const wb = XLSX.utils.book_new();

    appendCurrentInventoryAgeingRiskSheet({
      wb,
      sheetName: "Inventory Insights",
      titleLine,
      countryName,
      countryLabel,
      titleCountry,
      platformLabel,
      periodLabel,
      companyName,
      brandName,
      homeCurrencyCode,
      buckets,
      dataRows,
      showInventoryAlerts,
      salesLast30DaysLabel,
      unitSalesDataKey,
      storageCostCurrencySymbol,
    });

    XLSX.writeFile(wb, filename);
    return;
  }

  const toNum = (value: any) => {
    if (value === null || value === undefined || value === "") return 0;
    if (typeof value === "number") return Number.isFinite(value) ? value : 0;

    const n = Number(String(value).replace(/,/g, "").replace("%", "").trim());
    return Number.isFinite(n) ? n : 0;
  };

  const getUnitSalesSortValue = (row: Record<string, any>) => {
  return toNum(
    row.salesLast30Days ??
      row["Sales Last 30 Days"] ??
      row["Unit Sales in Last 30 Days"] ??
      row.unitsSold ??
      row["Units Sold"]
  );
};

  const displayCountry = titleCountry || countryLabel || countryName || "";

  const currencySymbol = getCurrencySymbol({
    countryName: countryName || displayCountry,
    homeCurrencyCode,
  });

const leftHeaders = [
  "S.No.",
  "Product Name",
  "SKU",
  "Sales Rank",
];

const bucketHeaders = buckets.map((bucket) => bucket.label);

const sellableHeaders = [
  "Available",
  "FC Transfer",
  "Total",
];

const rightHeaders = [
  "Inbound Units",
  "Unfulfillable Units",
  salesLast30DaysLabel,
  "Coverage Ratio (in Months)",
  ...(showInventoryAlerts ? ["Inventory Alerts"] : []),
];

const headers = [
  ...leftHeaders,
  ...bucketHeaders,
  ...sellableHeaders,
  ...rightHeaders,
];

const headerTopRow = [
  ...leftHeaders,
  ...bucketHeaders,
  "Sellable Units",
  "",
  "",
  ...rightHeaders,
];

const headerSubRow = [
  ...leftHeaders.map(() => ""),
  ...bucketHeaders.map(() => ""),
  ...sellableHeaders,
  ...rightHeaders.map(() => ""),
];

const headerCount = headers.length;
const ANCHOR_COL_1_BASED = headerCount;

const SELLABLE_START_INDEX = leftHeaders.length + bucketHeaders.length;
const SELLABLE_END_INDEX = SELLABLE_START_INDEX + sellableHeaders.length - 1;

const tableBorder = {
  top: { style: "thin", color: { rgb: "000000" } },
  bottom: { style: "thin", color: { rgb: "000000" } },
  left: { style: "thin", color: { rgb: "000000" } },
  right: { style: "thin", color: { rgb: "000000" } },
};

const backendPercentageRow = dataRows.find((row) => {
  const productName = String(row?.productName || row?.["Product Name"] || "")
    .trim()
    .toLowerCase();

  const sku = String(row?.sku || row?.SKU || "")
    .trim()
    .toLowerCase();

  return (
    row?.isPercentageRow === true ||
    row?.is_percentage_row === true ||
    productName === "% of total" ||
    productName === "percentage" ||
    sku === "% of total" ||
    sku === "percentage"
  );
});

const realRows = [...dataRows]
  .filter((row) => {
    const productName = String(row?.productName || row?.["Product Name"] || "")
      .trim()
      .toLowerCase();

    const sku = String(row?.sku || row?.SKU || "")
      .trim()
      .toLowerCase();

    if (!productName && !sku) return false;

    return (
      !row.isOthersRow &&
      !row.isTotalRow &&
      !row.isPercentageRow &&
      !row.is_percentage_row &&
      productName !== "total" &&
      productName !== "grand total" &&
      productName !== "% of total" &&
      productName !== "percentage" &&
      sku !== "total" &&
      sku !== "grand total" &&
      sku !== "% of total" &&
      sku !== "percentage"
    );
  })
  .sort((a, b) => getUnitSalesSortValue(b) - getUnitSalesSortValue(a));

const totalRow: Record<string, any> = {
  productName: "Total",
  sku: "",
  isTotalRow: true,
};

buckets.forEach((bucket) => {
  totalRow[bucket.key] = realRows.reduce(
    (sum, row) => sum + toNum(row[bucket.key]),
    0
  );
});

totalRow.available = realRows.reduce(
  (sum, row) => sum + toNum(row.available),
  0
);

totalRow.fcTransfer = realRows.reduce(
  (sum, row) => sum + toNum(row.fcTransfer),
  0
);

totalRow.totalUnits = realRows.reduce((sum, row) => {
  const available = toNum(row.available);
  const fcTransfer = toNum(row.fcTransfer);
  const totalUnits = toNum(row.totalUnits);

  return sum + (totalUnits > 0 ? totalUnits : available + fcTransfer);
}, 0);

totalRow.inboundUnits = realRows.reduce(
  (sum, row) => sum + toNum(row.inboundUnits),
  0
);

totalRow.salesRank = "";

totalRow.unsellableUnits = realRows.reduce(
  (sum, row) => sum + toNum(row.unsellableUnits),
  0
);

totalRow.salesLast30Days = realRows.reduce(
  (sum, row) => sum + toNum(row.salesLast30Days),
  0
);

totalRow.coverageRatio = "";
totalRow.inventoryAlert = "";

// ✅ IMPORTANT: use backend/UI percentage row, do not calculate again
const percentageRow: Record<string, any> | null = backendPercentageRow
  ? {
      ...backendPercentageRow,
      productName: "% of Total",
      sku: "",
      isPercentageRow: true,
    }
  : null;

const exportRows = percentageRow
  ? [...realRows, totalRow, percentageRow]
  : [...realRows, totalRow];

  const topExtraLines = [
    `Country : ${displayCountry}`,
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

const headerTopRowIndex = topAoA.length;
const headerSubRowIndex = headerTopRowIndex + 1;
const firstBodyRowIndex = headerSubRowIndex + 1;

const formatPercentValue = (value: any) => {
  if (value === null || value === undefined || value === "") return "";

  const raw = String(value).trim();

  if (raw.endsWith("%")) return raw;

  const n = toNum(value);
  if (!Number.isFinite(n) || n === 0) return "";

  return `${n.toFixed(2)}%`;
};

const bodyRows = exportRows.map((row, index) => {
  const isTotalRow = row.isTotalRow;
  const isPercentageRow = row.isPercentageRow || row.is_percentage_row;

  return [
    isTotalRow || isPercentageRow ? "" : index + 1,
    row.productName || row["Product Name"] || "",
    isTotalRow || isPercentageRow ? "" : row.sku || row.SKU || "-",

    // ✅ Sales Rank after SKU
    isTotalRow || isPercentageRow
      ? ""
      : row.salesRank || row["Sales Rank"] || "",

    ...buckets.map((bucket) => {
      if (isPercentageRow) {
        return formatPercentValue(row[bucket.key]);
      }

      const n = toNum(row[bucket.key]);
      return n > 0 ? n : "";
    }),

// ✅ Sellable Units > Available
isPercentageRow
  ? ""
  : toNum(row.available) > 0
    ? toNum(row.available)
    : "",

// ✅ Sellable Units > FC Transfer
isPercentageRow
  ? ""
  : toNum(row.fcTransfer) > 0
    ? toNum(row.fcTransfer)
    : "",

// ✅ Sellable Units > Total
isPercentageRow
  ? formatPercentValue(row.totalUnits ?? row.available ?? row["Sellable Units"])
  : (() => {
      const available = toNum(row.available);
      const fcTransfer = toNum(row.fcTransfer);
      const totalUnits = toNum(row.totalUnits);

      const finalTotal = totalUnits > 0 ? totalUnits : available + fcTransfer;

      return finalTotal > 0 ? finalTotal : "";
    })(),

    isPercentageRow
      ? ""
      : toNum(row.inboundUnits) > 0
        ? toNum(row.inboundUnits)
        : "",

    isPercentageRow
      ? formatPercentValue(
          row.unsellableUnits ??
            row["unfulfillable-quantity"] ??
            row["Unfulfillable Units"]
        )
      : toNum(row.unsellableUnits) > 0
        ? toNum(row.unsellableUnits)
        : "",

   isPercentageRow
  ? ""
  : toNum(row.salesLast30Days) > 0
    ? toNum(row.salesLast30Days)
    : "",

    isTotalRow || isPercentageRow
      ? ""
      : toNum(row.coverageRatio) > 0
        ? Number(row.coverageRatio).toFixed(2)
        : "",

    ...(showInventoryAlerts
      ? [isTotalRow || isPercentageRow ? "" : row.inventoryAlert || ""]
      : []),
  ];
});

const sheetAoA = [...topAoA, headerTopRow, headerSubRow, ...bodyRows];
const ws = XLSX.utils.aoa_to_sheet(sheetAoA);

const headerMerges = [];

// ✅ Merge "Sellable Units" over Available / FC Transfer / Total
headerMerges.push({
  s: { r: headerTopRowIndex, c: SELLABLE_START_INDEX },
  e: { r: headerTopRowIndex, c: SELLABLE_END_INDEX },
});

// ✅ Merge all other headers vertically across 2 header rows
for (let c = 0; c < headerCount; c++) {
  if (c >= SELLABLE_START_INDEX && c <= SELLABLE_END_INDEX) continue;

  headerMerges.push({
    s: { r: headerTopRowIndex, c },
    e: { r: headerSubRowIndex, c },
  });
}

ws["!merges"] = [...(ws["!merges"] || []), ...headerMerges];

  ws["!freeze"] = {
    xSplit: 0,
    ySplit: headerSubRowIndex + 1,
  };

  ws["!cols"] = headers.map((header) => {
    if (header === "S.No.") return { wch: 8 };
    if (header === "Product Name") return { wch: 28 };
    if (header === "SKU") return { wch: 18 };

    if (header.includes("Days")) return { wch: 18 };

 if (header === "Available") return { wch: 14 };
if (header === "FC Transfer") return { wch: 14 };
if (header === "Total") return { wch: 14 };
    if (header === "Inbound Units") return { wch: 16 };
    if (header === "Sales Rank") return { wch: 14 };
    if (header === "Unfulfillable Units") return { wch: 20 };
  if (header === salesLast30DaysLabel) return { wch: 22 };
    if (header === "Coverage Ratio (in Months)") return { wch: 24 };
    if (header === "Inventory Alerts") return { wch: 32 };

    return { wch: 16 };
  });

  applyTopStyles(ws, headerCount, ANCHOR_COL_1_BASED);

  const range = XLSX.utils.decode_range(ws["!ref"] || "A1:A1");

for (let r = headerTopRowIndex; r <= headerSubRowIndex; r++) {
  for (let c = 0; c < headerCount; c++) {
    const addr = XLSX.utils.encode_cell({ r, c });

    if (!ws[addr]) {
      ws[addr] = { t: "s", v: "" };
    }

    ws[addr].s = {
      ...(ws[addr].s || {}),
      font: {
        bold: true,
        sz: 11,
        color: { rgb: "000000" },
      },
      fill: {
        fgColor: { rgb: "FFFFFF" },
      },
      alignment: {
        horizontal: "center",
        vertical: "center",
        wrapText: true,
      },
      border: tableBorder,
    };
  }
}

  for (let r = firstBodyRowIndex; r <= range.e.r; r++) {
    const productNameAddr = XLSX.utils.encode_cell({ r, c: 1 });
    const productName = String(ws[productNameAddr]?.v || "")
      .trim()
      .toLowerCase();

    const isTotalRow = productName === "total";
    const isPercentageRow = productName === "% of total";

    for (let c = 0; c < headerCount; c++) {
      const addr = XLSX.utils.encode_cell({ r, c });

      if (!ws[addr]) {
        ws[addr] = { t: "s", v: "" };
      }

      const cell = ws[addr];
      const header = headers[c];

      const shouldStayText =
        header === "Product Name" ||
        header === "SKU" ||
        header === "Inventory Alerts" ||
        header === "Coverage Ratio (in Months)" ||
        isPercentageRow;

      if (!shouldStayText && cell.v !== "") {
        const n = toNumberLoose(cell.v);

        if (n !== null) {
          cell.v = n;
          cell.t = "n";
          cell.z = "#,##0";
        }
      }

      if (header === "Coverage Ratio (in Months)" && cell.v !== "") {
        const n = toNumberLoose(cell.v);

        if (n !== null) {
          cell.v = n;
          cell.t = "n";
          cell.z = "#,##0.00";
        }
      }

      cell.s = {
        ...(cell.s || {}),
        font: {
          bold: isTotalRow || isPercentageRow,
          sz: 11,
          color: { rgb: "000000" },
        },
        fill: {
          fgColor: { rgb: "FFFFFF" },
        },
        alignment: {
          horizontal: c === 1 ? "left" : "center",
          vertical: "center",
          wrapText: true,
        },
        border: tableBorder,
      };
    }
  }

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, safeSheetName("Inventory Insights"));
  XLSX.writeFile(wb, filename);
}









export function exportGlobalAgeingRiskHeatmapExcel(params: {
  filename: string;
  titleLine?: string;
  platformLabel?: string;
  periodLabel?: string;
  companyName?: string;
  brandName?: string;
  homeCurrencyCode?: string;
  buckets: {
    key: string;
    label: string;
    color?: string;
  }[];
  ukRows: Record<string, any>[];
  usRows: Record<string, any>[];
  showInventoryAlerts?: boolean;
  salesLast30DaysLabel?: string;
  unitSalesDataKey?: AgeingRiskSalesDataKey;
  storageCostCurrencySymbol?: string;
}) {
  const {
    filename,
    titleLine = "Amazon Global - Inventory Insights",
    platformLabel = "Phormula",
    periodLabel = "",
    companyName = "",
    brandName = "",
    homeCurrencyCode,
    buckets,
    ukRows,
    usRows,
    showInventoryAlerts = true,
    salesLast30DaysLabel = "Sales Last 30 Days",
    unitSalesDataKey = "salesLast30Days",
    storageCostCurrencySymbol,
  } = params;

  if (!ukRows?.length && !usRows?.length) return;

  const buildSheet = (
    wb: XLSX.WorkBook,
    sheetName: string,
    titleCountry: "UK" | "US",
    dataRows: Record<string, any>[]
  ) => {
    if (!dataRows?.length) return;

    if (titleCountry === "US") {
      appendCurrentInventoryAgeingRiskSheet({
        wb,
        sheetName,
        titleLine: `${titleLine} - ${titleCountry}`,
        countryName: "global",
        titleCountry,
        platformLabel,
        periodLabel,
        companyName,
        brandName,
        homeCurrencyCode,
        buckets,
        dataRows,
        showInventoryAlerts,
        salesLast30DaysLabel,
        unitSalesDataKey,
        storageCostCurrencySymbol,
      });

      return;
    }

    const toNum = (value: any) => {
      if (value === null || value === undefined || value === "") return 0;
      if (typeof value === "number") return Number.isFinite(value) ? value : 0;

      const n = Number(String(value).replace(/,/g, "").replace("%", "").trim());
      return Number.isFinite(n) ? n : 0;
    };

    const getUnitSalesSortValue = (row: Record<string, any>) =>
      getAgeingRiskSalesValue(row, unitSalesDataKey);

    const currencySymbol = getCurrencySymbol({
      countryName: "global",
      homeCurrencyCode,
    });

    const headers = [
      "S.No.",
      "Product Name",
      "SKU",
      ...buckets.map((bucket) => bucket.label),
      "Sellable Units",
      "Inbound Units",
      "Sales Rank",
      "Unfulfillable Units",
      salesLast30DaysLabel,
      "Coverage Ratio (in Months)",
      ...(showInventoryAlerts ? ["Inventory Alerts"] : []),
    ];

    const headerCount = headers.length;
    const ANCHOR_COL_1_BASED = headerCount;

    const tableBorder = {
      top: { style: "thin", color: { rgb: "000000" } },
      bottom: { style: "thin", color: { rgb: "000000" } },
      left: { style: "thin", color: { rgb: "000000" } },
      right: { style: "thin", color: { rgb: "000000" } },
    };

const backendPercentageRow = dataRows.find((row) => {
  const productName = String(row?.productName || row?.["Product Name"] || "")
    .trim()
    .toLowerCase();

  const sku = String(row?.sku || row?.SKU || "")
    .trim()
    .toLowerCase();

  return (
    row?.isPercentageRow === true ||
    row?.is_percentage_row === true ||
    productName === "% of total" ||
    productName === "percentage" ||
    sku === "% of total" ||
    sku === "percentage"
  );
});

    const realRows = [...dataRows]
      .filter((row) => {
        const productName = String(row?.productName || "").trim().toLowerCase();
        const sku = String(row?.sku || "").trim().toLowerCase();

        if (!productName && !sku) return false;

        return (
  !row.isOthersRow &&
  !row.isTotalRow &&
  !row.isPercentageRow &&
  !row.is_percentage_row &&
  productName !== "total" &&
  productName !== "grand total" &&
  productName !== "% of total" &&
  productName !== "percentage" &&
  sku !== "total" &&
  sku !== "grand total" &&
  sku !== "% of total" &&
  sku !== "percentage"
);
      })
      .sort((a, b) => getUnitSalesSortValue(b) - getUnitSalesSortValue(a));

    const totalRow: Record<string, any> = {
      productName: "Total",
      sku: "",
      isTotalRow: true,
    };

    buckets.forEach((bucket) => {
      totalRow[bucket.key] = realRows.reduce(
        (sum, row) => sum + toNum(row[bucket.key]),
        0
      );
    });

    totalRow.available = realRows.reduce(
      (sum, row) => sum + toNum(row.available ?? row.totalUnits),
      0
    );

    totalRow.totalUnits = totalRow.available;

    totalRow.inboundUnits = realRows.reduce(
      (sum, row) => sum + toNum(row.inboundUnits),
      0
    );

    totalRow.salesRank = "";

    totalRow.unsellableUnits = realRows.reduce(
      (sum, row) => sum + toNum(row.unsellableUnits),
      0
    );

    totalRow.salesLast30Days = realRows.reduce(
      (sum, row) => sum + getAgeingRiskSalesValue(row, "salesLast30Days"),
      0
    );

    totalRow.unitsSold = realRows.reduce(
      (sum, row) => sum + getAgeingRiskSalesValue(row, "unitsSold"),
      0
    );

    totalRow.coverageRatio = "";
    totalRow.inventoryAlert = "";

const percentageRow: Record<string, any> | null = backendPercentageRow
  ? {
      ...backendPercentageRow,
      productName: "% of Total",
      sku: "",
      isPercentageRow: true,
    }
  : null;

const exportRows = percentageRow
  ? [...realRows, totalRow, percentageRow]
  : [...realRows, totalRow];

    const topExtraLines = [
      `Country : ${titleCountry}`,
      `Platform : ${platformLabel}`,
      `Currency : ${currencySymbol}`,
      `Period : ${periodLabel}`,
    ];

    const topAoA = buildTopAoA({
      headerCount,
      title: `${titleLine} - ${titleCountry}`,
      companyName,
      brandName,
      anchorCol1Based: ANCHOR_COL_1_BASED,
      extraLines: topExtraLines,
    });

const headerTopRowIndex = topAoA.length;
const headerSubRowIndex = headerTopRowIndex + 1;
const firstBodyRowIndex = headerSubRowIndex + 1;

const formatPercentValue = (value: any) => {
  if (value === null || value === undefined || value === "") return "";

  const raw = String(value).trim();

  if (raw.endsWith("%")) return raw;

  const n = toNum(value);
  if (!Number.isFinite(n) || n === 0) return "";

  return `${n.toFixed(2)}%`;
};

    const bodyRows = exportRows.map((row, index) => {
      const isTotalRow = row.isTotalRow;
      const isPercentageRow = row.isPercentageRow;
      const salesValue = getAgeingRiskSalesValue(row, unitSalesDataKey);

      return [
        isTotalRow || isPercentageRow ? "" : index + 1,
        row.productName || "",
        isTotalRow || isPercentageRow ? "" : row.sku || "-",

       ...buckets.map((bucket) => {
  if (isPercentageRow) {
    return formatPercentValue(row[bucket.key]);
  }

  const n = toNum(row[bucket.key]);
  return n > 0 ? n : "";
}),

       isPercentageRow
  ? formatPercentValue(row.available ?? row.totalUnits ?? row["Sellable Units"])
  : toNum(row.available ?? row.totalUnits),

        isPercentageRow
          ? ""
          : toNum(row.inboundUnits) > 0
            ? toNum(row.inboundUnits)
            : "",

        isTotalRow || isPercentageRow
          ? ""
          : row.salesRank || "",

      isPercentageRow
  ? formatPercentValue(
      row.unsellableUnits ??
        row["unfulfillable-quantity"] ??
        row["Unfulfillable Units"]
    )
  : toNum(row.unsellableUnits) > 0
    ? toNum(row.unsellableUnits)
    : "",

        isPercentageRow
          ? ""
          : salesValue > 0
            ? salesValue
            : "",

        isTotalRow || isPercentageRow
          ? ""
          : toNum(row.coverageRatio) > 0
            ? Number(row.coverageRatio).toFixed(2)
            : "",

        ...(showInventoryAlerts
          ? [isTotalRow || isPercentageRow ? "" : row.inventoryAlert || ""]
          : []),
      ];
    });

    const sheetAoA = [...topAoA, headers, ...bodyRows];
    const ws = XLSX.utils.aoa_to_sheet(sheetAoA);

    ws["!freeze"] = {
      xSplit: 0,
      ySplit: headerSubRowIndex + 1,
    };

    ws["!cols"] = headers.map((header) => {
      if (header === "S.No.") return { wch: 8 };
      if (header === "Product Name") return { wch: 28 };
      if (header === "SKU") return { wch: 18 };
      if (header.includes("Days")) return { wch: 18 };
      if (header === "Sellable Units") return { wch: 16 };
      if (header === "Inbound Units") return { wch: 16 };
      if (header === "Sales Rank") return { wch: 14 };
      if (header === "Unfulfillable Units") return { wch: 20 };
      if (header === salesLast30DaysLabel) return { wch: 22 };
      if (header === "Coverage Ratio (in Months)") return { wch: 24 };
      if (header === "Inventory Alerts") return { wch: 32 };
      return { wch: 16 };
    });

    applyTopStyles(ws, headerCount, ANCHOR_COL_1_BASED);

    const range = XLSX.utils.decode_range(ws["!ref"] || "A1:A1");

 for (let r = headerTopRowIndex; r <= headerSubRowIndex; r++) {
  for (let c = 0; c < headerCount; c++) {
    const addr = XLSX.utils.encode_cell({ r, c });

    if (!ws[addr]) {
      ws[addr] = { t: "s", v: "" };
    }

    ws[addr].s = {
      ...(ws[addr].s || {}),
      font: {
        bold: true,
        sz: 11,
        color: { rgb: "000000" },
      },
      fill: {
        fgColor: { rgb: "FFFFFF" },
      },
      alignment: {
        horizontal: "center",
        vertical: "center",
        wrapText: true,
      },
      border: tableBorder,
    };
  }
}

    for (let r = firstBodyRowIndex; r <= range.e.r; r++) {
      const productNameAddr = XLSX.utils.encode_cell({ r, c: 1 });
      const productName = String(ws[productNameAddr]?.v || "")
        .trim()
        .toLowerCase();

      const isTotalRow = productName === "total";
      const isPercentageRow = productName === "% of total";

      for (let c = 0; c < headerCount; c++) {
        const addr = XLSX.utils.encode_cell({ r, c });

        if (!ws[addr]) {
          ws[addr] = { t: "s", v: "" };
        }

        const cell = ws[addr];
        const header = headers[c];

        const shouldStayText =
          header === "Product Name" ||
          header === "SKU" ||
          header === "Inventory Alerts" ||
          header === "Coverage Ratio (in Months)" ||
          isPercentageRow;

        if (!shouldStayText && cell.v !== "") {
          const n = toNumberLoose(cell.v);

          if (n !== null) {
            cell.v = n;
            cell.t = "n";
            cell.z = "#,##0";
          }
        }

        if (header === "Coverage Ratio (in Months)" && cell.v !== "") {
          const n = toNumberLoose(cell.v);

          if (n !== null) {
            cell.v = n;
            cell.t = "n";
            cell.z = "#,##0.00";
          }
        }

        cell.s = {
          ...(cell.s || {}),
          font: {
            bold: isTotalRow || isPercentageRow,
            sz: 11,
            color: { rgb: "000000" },
          },
          fill: {
            fgColor: { rgb: "FFFFFF" },
          },
          alignment: {
            horizontal: c === 1 ? "left" : "center",
            vertical: "center",
            wrapText: true,
          },
          border: tableBorder,
        };
      }
    }

    XLSX.utils.book_append_sheet(wb, ws, safeSheetName(sheetName));
  };

  const wb = XLSX.utils.book_new();

  buildSheet(wb, "UK Inventory Insights", "UK", ukRows);
  buildSheet(wb, "US Inventory Insights", "US", usRows);

  XLSX.writeFile(wb, filename);
}
