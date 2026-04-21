"use client";

import React, { useCallback, useMemo } from "react";
// import * as XLSX from "xlsx-js-style";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import Loader from "@/components/loader/Loader";
import type { RegionKey } from "@/lib/dashboard/types";
import DataTable, { ColumnDef } from "../ui/table/DataTable";
import DownloadIconButton from "../ui/button/DownloadIconButton";
// import { saveAs } from "file-saver";
import { exportPnLProductwiseBreakdownMtdExcel } from "@/lib/excel/exportCurrentInventoryExcel";

type InventoryRow = Record<string, string | number>;

type InventoryUiRow = {
  sno: React.ReactNode;
  productName?: React.ReactNode;
  skuAsin?: React.ReactNode;
  mtdSales?: React.ReactNode;
  sales30?: React.ReactNode;
  salesRank?: React.ReactNode;
  currentInventory?: React.ReactNode;
  inventory180Plus?: React.ReactNode;
  estStorage?: React.ReactNode;
  coverageMonths?: React.ReactNode;
  alert?: React.ReactNode;
  rowType?: "normal" | "others" | "total";
} & Record<string, React.ReactNode>;

type CurrencyCode = "USD" | "GBP" | "CAD" | "INR";

type CurrentInventorySectionProps = {
  region: RegionKey;
  invLoading: boolean;
  invError?: string | null;
  invRows: InventoryRow[];
  inventoryAlerts: Record<string, { alert?: string; alert_type?: string }>;
  userData: any;
  convertToDisplayCurrency: (value: number | null | undefined, from: CurrencyCode) => number;
  displayCurrency: CurrencyCode;
};

/* ========= Shared helpers ========= */

function getISTYearMonth() {
  const optsMonth: Intl.DateTimeFormatOptions = {
    timeZone: "Asia/Kolkata",
    month: "long",
  };
  const optsYear: Intl.DateTimeFormatOptions = {
    timeZone: "Asia/Kolkata",
    year: "numeric",
  };
  const now = new Date();
  const monthName = now.toLocaleString("en-US", optsMonth);
  const yearStr = now.toLocaleString("en-US", optsYear);
  return { monthName, year: Number(yearStr) };
}

const toNumberSafe = (v: any) => {
  if (v === null || v === undefined) return 0;
  if (typeof v === "number") return v;
  const s = String(v).replace(/[, ]+/g, "");
  const n = Number(s);
  return isNaN(n) ? 0 : n;
};

const normalizeSku = (v: any) => String(v || "").trim().toUpperCase();

const normKey = (s: string) =>
  String(s || "")
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[_-]+/g, "");

const getNumberByPossibleKeys = (row: InventoryRow, possible: string[]) => {
  const wanted = possible.map(normKey);
  const foundKey = Object.keys(row).find((k) => wanted.includes(normKey(k)));
  return foundKey ? toNumberSafe(row[foundKey]) : 0;
};

const isInventoryTotalRow = (r: InventoryRow) => {
  const name = String(r["Product Name"] ?? "").trim().toLowerCase();
  const sku = String(r["SKU"] ?? "").trim().toLowerCase();
  if (!name && !sku) return false;
  return (
    name === "total" ||
    name === "grand total" ||
    name.includes("total") ||
    sku === "total" ||
    sku === "grand total" ||
    sku.includes("total")
  );
};

const formatInt = (n: number | null | undefined) => {
  const v = Number(n ?? 0);
  if (!v) return "0";
  return v.toLocaleString("en-IN", { maximumFractionDigits: 0 });
};

const formatRatio = (n: number | null | undefined) => {
  const v = Number(n ?? 0);
  if (!v || !Number.isFinite(v)) return "—";
  return v.toFixed(2);
};

// const capitalizeWords = (value: string) =>
//   (value || "")
//     .toString()
//     .toLowerCase()
//     .replace(/\b\w/g, (char) => char.toUpperCase());

/* ========= Export Header Cleanup ========= */

// const HEADER_RENAME_MAP: Record<string, string | null> = {
//   synced_at: null,

//   available: "Available Inventory",

//   "inv-age-0-to-90-days": "Inventory Age 0 to 90 Days",
//   "inv-age-91-to-180-days": "Inventory Age 91 to 180 Days",
//   "inv-age-181-to-270-days": "Inventory Age 181 to 270 Days",
//   "inv-age-271-to-365-days": "Inventory Age 271 to 365 Days",
//   "inv-age-365-plus-days": "Inventory Age 365 Plus Days",

//   "sales-rank": "Sales Rank",
//   "estimated-storage-cost-next-month": "Estimated Storage Cost Next Month",
// };

// const toTitleCase = (s: string) =>
//   String(s || "")
//     .replace(/[_-]+/g, " ")
//     .replace(/\s+/g, " ")
//     .trim()
//     .toLowerCase()
//     .replace(/\b\w/g, (c) => c.toUpperCase());

// const normalizeExportRows = (rows: InventoryRow[]) => {
//   return rows.map((row) => {
//     const cleanRow: Record<string, any> = {};

//     Object.entries(row).forEach(([key, value]) => {
//       const normalizedKey = key.trim().toLowerCase();
//       if (normalizedKey === "synced_at") return;

//       const mapped = HEADER_RENAME_MAP[normalizedKey];
//       if (mapped === null) return;

//       const newKey = mapped ?? toTitleCase(normalizedKey);
//       cleanRow[newKey] = value;
//     });

//     return cleanRow;
//   });
// };

// const getAbbr = (m?: string) => (m ? m.slice(0, 3) : "");

// const buildTopAoA = ({
//   headerCount,
//   title,
//   companyName,
//   brandName,
//   brandAnchorColIndex1Based,
//   extraLines = [],
// }: {
//   headerCount: number;
//   title: string;
//   companyName: string;
//   brandName: string;
//   brandAnchorColIndex1Based: number;
//   extraLines?: string[];
// }) => {
//   const aoa: any[][] = [];

//   const titleRow = new Array(headerCount).fill("");
//   titleRow[0] = title || "";
//   aoa.push(titleRow);

//   const companyBrandRow = new Array(headerCount).fill("");
//   companyBrandRow[0] = `Company Name : ${companyName || ""}`;

//   const anchor0 = Math.max(0, brandAnchorColIndex1Based - 1);
//   companyBrandRow[Math.min(headerCount - 1, anchor0)] = `${brandName || ""}`;
//   aoa.push(companyBrandRow);

//   for (const line of extraLines) {
//     const r = new Array(headerCount).fill("");
//     r[0] = line;
//     aoa.push(r);
//   }

//   aoa.push(new Array(headerCount).fill(""));
//   return aoa;
// };

// const applyTopStyles = (
//   ws: XLSX.WorkSheet,
//   headerCount: number,
//   brandAnchorColIndex1Based: number
// ) => {
//   ws["!merges"] = ws["!merges"] || [];
//   ws["!rows"] = ws["!rows"] || [];

//   ws["!rows"][0] = { hpt: 18 };
//   ws["!rows"][1] = { hpt: 18 };

//   ws["!merges"].push({
//     s: { r: 0, c: 0 },
//     e: { r: 0, c: headerCount - 1 },
//   });

//   const companyAddr = XLSX.utils.encode_cell({ r: 1, c: 0 });
//   if (ws[companyAddr]) {
//     ws[companyAddr].s = {
//       alignment: { horizontal: "left", vertical: "center" },
//     };
//   }

//   const anchor0 = Math.max(0, brandAnchorColIndex1Based - 1);
//   const brandAddr = XLSX.utils.encode_cell({
//     r: 1,
//     c: Math.min(headerCount - 1, anchor0),
//   });
//   if (ws[brandAddr]) {
//     ws[brandAddr].s = {
//       alignment: { horizontal: "right", vertical: "center" },
//     };
//   }
// };

// const applyBoldRow = (ws: XLSX.WorkSheet, rowIndex: number) => {
//   const ref = ws["!ref"];
//   if (!ref) return;
//   const range = XLSX.utils.decode_range(ref);

//   for (let C = range.s.c; C <= range.e.c; C++) {
//     const addr = XLSX.utils.encode_cell({ r: rowIndex, c: C });
//     const cell = ws[addr];
//     const base = cell || { t: "s", v: "" };

//     ws[addr] = {
//       ...base,
//       s: {
//         ...((base as any).s || {}),
//         font: { ...(((base as any).s)?.font || {}), bold: true },
//       },
//     };
//   }
// };

// const boldHeaderRows = (ws: XLSX.WorkSheet, headerRows: number[]) => {
//   headerRows.forEach((r) => applyBoldRow(ws, r));
// };

// const boldRowsByColValue = (
//   ws: XLSX.WorkSheet,
//   colIndex0: number,
//   labels: string[]
// ) => {
//   const ref = ws["!ref"];
//   if (!ref) return;
//   const range = XLSX.utils.decode_range(ref);
//   const set = new Set(labels.map((s) => s.toLowerCase()));

//   for (let R = range.s.r; R <= range.e.r; R++) {
//     const addr = XLSX.utils.encode_cell({ r: R, c: colIndex0 });
//     const v = String(ws[addr]?.v ?? "").trim().toLowerCase();
//     if (set.has(v)) applyBoldRow(ws, R);
//   }
// };

const getCurrencySymbol = (currency: CurrencyCode) => {
  switch (currency) {
    case "USD":
      return "$";
    case "GBP":
      return "£";
    case "CAD":
      return "CA$";
    case "INR":
      return "₹";
    default:
      return "";
  }
};

const formatMoneyNumberOnly = (n: number | null | undefined) => {
  const v = Number(n ?? 0);
  if (!Number.isFinite(v)) return "—";
  return v.toLocaleString("en-IN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
};

/* ===================== COMPONENT ===================== */

export default function CurrentInventorySection({
  region,
  invLoading,
  invError,
  invRows,
  inventoryAlerts,
  userData,
  convertToDisplayCurrency,
  displayCurrency
}: CurrentInventorySectionProps) {

  const displayRegion = useMemo(() => {
    if (region === "UK") return "UK";
    if (region === "US") return "US";
    if (region === "CA") return "CA";
    return "Global";
  }, [region]);

  // ✅ current month/year in IST
  const invMonthYear = useMemo(() => {
    const { monthName, year } = getISTYearMonth();
    return { month: monthName.toLowerCase(), year: String(year) };
  }, []);

  const homeCurrencyCodeForExcel = useMemo(
    () => (userData as any)?.homeCurrency || (userData as any)?.home_currency || "",
    [userData]
  );

  const storageSourceCurrency: CurrencyCode = useMemo(() => {
    if (region === "UK" || region === "Global") return "GBP";
    if (region === "US") return "USD";
    if (region === "CA") return "CAD";
    return "GBP";
  }, [region]);

  const storageHeaderLabel = useMemo(() => {
    return `Estimated storage cost (${getCurrencySymbol(displayCurrency)})`;
  }, [displayCurrency]);

  // backend month column: "Current Month Units Sold (MonthName)" (dynamic)
  const findMtdKey = useCallback((row: InventoryRow) => {
    const key = Object.keys(row).find((k) =>
      k.toLowerCase().startsWith("current month units sold")
    );
    return key || "";
  }, []);

  // Sales for past 30 days: backend may call it "Others" or something else
  const findSales30Key = useCallback((row: InventoryRow) => {
    const keys = Object.keys(row);

    const exactOthers = keys.find((k) => k.trim().toLowerCase() === "others");
    if (exactOthers) return exactOthers;

    const past30 = keys.find((k) => k.toLowerCase().includes("past 30"));
    if (past30) return past30;

    const days30 = keys.find((k) => k.toLowerCase().includes("30 days"));
    if (days30) return days30;

    const same = keys.find((k) => k.trim().toLowerCase() === "sales for past 30 days");
    if (same) return same;

    return "";
  }, []);

  /* ===================== DOWNLOAD EXCEL (FULL DATA) ===================== */

  // const exportRows = useMemo(() => {
  //   if (!invRows?.length) return [];

  //   return invRows
  //     .filter((r) => {
  //       const name = String(r["Product Name"] ?? "").trim();
  //       const sku = String(r["SKU"] ?? "").trim();
  //       if (!name && !sku) return false;
  //       if (isInventoryTotalRow(r)) return false;
  //       return true;
  //     })
  //     .map((row, index) => {
  //       const sku = String(row["SKU"] ?? "").trim();

  //       const currentInventory =
  //         toNumberSafe(row["Inventory at the end of the month"]) ||
  //         toNumberSafe(row["Available Inventory"]);

  //       const mtdKey = findMtdKey(row);
  //       const currentMonthUnitsSold = toNumberSafe(
  //         mtdKey ? (row as any)[mtdKey] : 0
  //       );

  //       const daysInHand =
  //         currentMonthUnitsSold > 0
  //           ? Math.round(currentInventory / currentMonthUnitsSold)
  //           : "";

  //       const rawAlert = String(inventoryAlerts?.[sku]?.alert ?? "").toLowerCase();

  //       let status =
  //         rawAlert.includes("high")
  //           ? "High Alert"
  //           : rawAlert.includes("low")
  //             ? "Low Stock"
  //             : "Healthy";

  //       return {
  //         "Sno.": index + 1,
  //         "SKU": sku,
  //         "Product Name": row["Product Name"] ?? "",
  //         "Current Inventory": currentInventory,
  //         "Current Month Units Sold": currentMonthUnitsSold,
  //         "Days in Hand": daysInHand,
  //         "Status": status,
  //       } as Record<string, string | number>;
  //     });
  // }, [invRows, inventoryAlerts, findMtdKey]);

  // const downloadInventoryExcel = useCallback(() => {
  //   if (!exportRows.length) return;

  //   const headerOrder = Object.keys(exportRows[0]);
  //   const bodyAoA = exportRows.map((row) =>
  //     headerOrder.map((h) => (row as Record<string, any>)[h] ?? "")
  //   );

  //   const { monthName, year } = getISTYearMonth();
  //   const abbr = monthName.slice(0, 3);
  //   const periodLabel = `${abbr.charAt(0).toUpperCase()}${abbr.slice(1)}'${String(year).slice(2)}`;

  //   const companyNameForExcel = capitalizeWords((userData as any)?.company_name || "");
  //   const brandNameForExcel = capitalizeWords((userData as any)?.brand_name || "");

  //   const topAoA = buildTopAoA({
  //     headerCount: headerOrder.length,
  //     title: `Amazon ${displayRegion} - Current Inventory - ${periodLabel}`,
  //     companyName: companyNameForExcel,
  //     brandName: brandNameForExcel,
  //     brandAnchorColIndex1Based: headerOrder.length,
  //     extraLines: [
  //       `Country : ${displayRegion}`,
  //       `Platform : Amazon`,
  //       `Period : ${periodLabel}`,
  //     ],
  //   });

  //   const sheetAoA = [...topAoA, headerOrder, ...bodyAoA];
  //   const ws = XLSX.utils.aoa_to_sheet(sheetAoA);

  //   const HEADER_ROW_INDEX = topAoA.length;
  //   boldHeaderRows(ws, [HEADER_ROW_INDEX]);

  //   const wb = XLSX.utils.book_new();
  //   XLSX.utils.book_append_sheet(wb, ws, "Current Inventory");

  //   const fileName = `Current-Inventory_${displayRegion}_${monthName}_${year}.xlsx`;
  //   const out = XLSX.write(wb, { bookType: "xlsx", type: "array" });

  //   const blob = new Blob([out], {
  //     type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  //   });

  //   saveAs(blob, fileName);
  // }, [exportRows, displayRegion, userData]);

  /* -------- Transform backend rows → UI rows for DataTable -------- */

  const tableRows: InventoryUiRow[] = useMemo(() => {
    if (!invRows?.length) return [];

    // 1) Filter out empty + backend total rows
    const usable = invRows.filter((r) => {
      const name = String(r["Product Name"] ?? "").trim();
      const sku = String(r["SKU"] ?? "").trim();
      const isEmpty = !name && !sku;
      if (isEmpty) return false;
      if (isInventoryTotalRow(r)) return false;
      return true;
    });

    type CalcRow = {
      index: number;
      row: InventoryRow;
      currentInventory: number;
      mtdSales: number;
      sales30: number;
      coverage: number;
      salesRank: number;
      estStorage: number;
      inventory180Plus: number;
    };

    const calcRows: CalcRow[] = usable.map((r, idx) => {
      const mtdKey = findMtdKey(r);
      const sales30Key = findSales30Key(r);

      const currentInventory = toNumberSafe(r["Inventory at the end of the month"]);
      const mtdSales = toNumberSafe(mtdKey ? (r as any)[mtdKey] : 0);
      const sales30 = toNumberSafe(sales30Key ? (r as any)[sales30Key] : 0);

      const age181to270 = getNumberByPossibleKeys(r, [
        "inv-age-181-to-270-days",
        "inv_age_181_to_270_days",
        "Inventory Age 181 to 270 Days",
        "inv age 181 to 270 days",
      ]);

      const age271to365 = getNumberByPossibleKeys(r, [
        "inv-age-271-to-365-days",
        "inv_age_271_to_365_days",
        "Inventory Age 271 to 365 Days",
        "inv age 271 to 365 days",
      ]);

      const age365plus = getNumberByPossibleKeys(r, [
        "inv-age-365-plus-days",
        "inv_age_365_plus_days",
        "Inventory Age 365+ Days",
        "inv age 365 plus days",
        "inv-age-365+-days",
      ]);

      const inventory180Plus = age181to270 + age271to365 + age365plus;

      const denom = mtdSales + sales30;
      const coverage = denom > 0 ? currentInventory / denom : 0;

      const salesRank = toNumberSafe((r as any)["sales-rank"]);
      const estStorage = toNumberSafe((r as any)["estimated-storage-cost-next-month"]);

      return {
        index: idx,
        row: r,
        currentInventory,
        mtdSales,
        sales30,
        coverage,
        salesRank,
        estStorage,
        inventory180Plus,
      };
    });

    if (!calcRows.length) return [];

    // 3) Top 5 by MTD Sales (desc)
    const sortedByMtd = [...calcRows].sort((a, b) => b.mtdSales - a.mtdSales);
    const top5 = sortedByMtd.slice(0, 5);
    const top5Indices = new Set(top5.map((r) => r.index));

    // 4) Others = all remaining rows
    const othersRows = calcRows.filter((r) => !top5Indices.has(r.index));

    // 5) Build UI rows for top 5
    const uiRows: InventoryUiRow[] = top5.map((c, idx) => {
      const {
        row,
        currentInventory,
        mtdSales,
        sales30,
        coverage,
        salesRank,
        estStorage,
        inventory180Plus,
      } = c;

      const skuKey = normalizeSku((row as any)["SKU"]);
      const alertText = inventoryAlerts?.[skuKey]?.alert || "";

      return {
        rowType: "normal",
        sno: idx + 1,
        productName: (row as any)["Product Name"] || "",
        skuAsin: (row as any)["SKU"] || (row as any)["ASIN"] || "",
        mtdSales: formatInt(mtdSales),
        sales30: formatInt(mtdSales + sales30),
        salesRank: salesRank ? formatInt(salesRank) : "—",
        currentInventory: formatInt(currentInventory),
        inventory180Plus: formatInt(inventory180Plus),
        // estStorage: estStorage ? formatInt(estStorage) : "—",
        estStorage:
          estStorage
            ? formatMoneyNumberOnly(
              convertToDisplayCurrency(estStorage, storageSourceCurrency)
            )
            : "—",
        coverageMonths: formatRatio(coverage),
        alert: alertText || "—",
      };
    });

    // 6) "Others" aggregate row
    if (othersRows.length > 0) {
      const agg = othersRows.reduce(
        (acc, r) => {
          acc.currentInventory += r.currentInventory;
          acc.mtdSales += r.mtdSales;
          acc.sales30 += r.sales30;
          acc.estStorage += r.estStorage;
          acc.inventory180Plus += r.inventory180Plus;
          return acc;
        },
        {
          currentInventory: 0,
          mtdSales: 0,
          sales30: 0,
          estStorage: 0,
          inventory180Plus: 0,
        }
      );

      const denom = agg.mtdSales + agg.sales30;
      const coverage = denom > 0 ? agg.currentInventory / denom : 0;

      uiRows.push({
        rowType: "others",
        sno: 6,
        productName: "Others",
        skuAsin: "",
        mtdSales: formatInt(agg.mtdSales),
        sales30: formatInt(agg.mtdSales + agg.sales30),
        salesRank: "—",
        currentInventory: formatInt(agg.currentInventory),
        inventory180Plus: formatInt(agg.inventory180Plus),
        // estStorage: formatInt(agg.estStorage),
        estStorage: formatMoneyNumberOnly(
          convertToDisplayCurrency(agg.estStorage, storageSourceCurrency)
        ),
        coverageMonths: formatRatio(coverage),
        alert: "",
      });
    }

    // 7) Backend grand total row (if present in the original invRows)
    const inventoryTotalRow = invRows.find((r) => isInventoryTotalRow(r)) || null;
    if (inventoryTotalRow) {
      const mtdKey = findMtdKey(inventoryTotalRow);
      const sales30Key = findSales30Key(inventoryTotalRow);

      const currentInventory = toNumberSafe(
        (inventoryTotalRow as any)["Inventory at the end of the month"]
      );
      const mtdSales = toNumberSafe(mtdKey ? (inventoryTotalRow as any)[mtdKey] : 0);
      const sales30 = toNumberSafe(
        sales30Key ? (inventoryTotalRow as any)[sales30Key] : 0
      );

      const age181to270 = getNumberByPossibleKeys(inventoryTotalRow, [
        "inv-age-181-to-270-days",
        "Inventory Age 181 to 270 Days",
      ]);
      const age271to365 = getNumberByPossibleKeys(inventoryTotalRow, [
        "inv-age-271-to-365-days",
        "Inventory Age 271 to 365 Days",
      ]);
      const age365plus = getNumberByPossibleKeys(inventoryTotalRow, [
        "inv-age-365-plus-days",
        "Inventory Age 365 Plus Days",
        "Inventory Age 365+ Days",
      ]);
      const inventory180Plus = age181to270 + age271to365 + age365plus;

      const denom = mtdSales + sales30;
      const coverage = denom > 0 ? currentInventory / denom : 0;

      const estStorage = getNumberByPossibleKeys(inventoryTotalRow, [
        "estimated-storage-cost-next-month",
        "Estimated Storage Cost Next Month",
      ]);

      uiRows.push({
        rowType: "total",
        sno: "",
        productName: <span className="font-semibold">Total</span>,
        skuAsin: "",
        currentInventory: <span className="font-semibold">{formatInt(currentInventory)}</span>,
        inventory180Plus: <span className="font-semibold">{formatInt(inventory180Plus)}</span>,
        salesRank: "",
        // estStorage: <span className="font-semibold">{formatInt(estStorage)}</span>,
        estStorage: (
          <span className="font-semibold">
            {formatMoneyNumberOnly(
              convertToDisplayCurrency(estStorage, storageSourceCurrency)
            )}
          </span>
        ),
        mtdSales: <span className="font-semibold">{formatInt(mtdSales)}</span>,
        sales30: <span className="font-semibold">{formatInt(mtdSales + sales30)}</span>,
        coverageMonths: <span className="font-semibold">{formatRatio(coverage)}</span>,
        alert: "",
      });
    }

    return uiRows;
    // }, [invRows, findMtdKey, findSales30Key, inventoryAlerts]);
  }, [
    invRows,
    findMtdKey,
    findSales30Key,
    inventoryAlerts,
    convertToDisplayCurrency,
    storageSourceCurrency,
  ]);

  const exportDataRows = useMemo(() => {
    if (!invRows?.length) return [];

    return invRows
      .filter((r) => {
        const name = String(r["Product Name"] ?? "").trim();
        const sku = String(r["SKU"] ?? "").trim();
        const isEmpty = !name && !sku;
        if (isEmpty) return false;

        // optional:
        // keep this false if you DO NOT want total row in excel
        if (isInventoryTotalRow(r)) return false;

        return true;
      })
      .map((row, index) => {
        const sku = normalizeSku((row as any)["SKU"]);
        const mtdKey = findMtdKey(row);
        const sales30Key = findSales30Key(row);

        const currentInventory =
          toNumberSafe((row as any)["Inventory at the end of the month"]) ||
          toNumberSafe((row as any)["Available Inventory"]);

        const mtdSales = toNumberSafe(mtdKey ? (row as any)[mtdKey] : 0);
        const sales30Only = toNumberSafe(sales30Key ? (row as any)[sales30Key] : 0);
        const salesLast30Days = mtdSales + sales30Only;

        const age181to270 = getNumberByPossibleKeys(row, [
          "inv-age-181-to-270-days",
          "inv_age_181_to_270_days",
          "Inventory Age 181 to 270 Days",
          "inv age 181 to 270 days",
        ]);

        const age271to365 = getNumberByPossibleKeys(row, [
          "inv-age-271-to-365-days",
          "inv_age_271_to_365_days",
          "Inventory Age 271 to 365 Days",
          "inv age 271 to 365 days",
        ]);

        const age365plus = getNumberByPossibleKeys(row, [
          "inv-age-365-plus-days",
          "inv_age_365_plus_days",
          "Inventory Age 365+ Days",
          "Inventory Age 365 Plus Days",
          "inv age 365 plus days",
          "inv-age-365+-days",
        ]);

        const inventory180Plus = age181to270 + age271to365 + age365plus;

        const salesRank = toNumberSafe((row as any)["sales-rank"]);
        const estStorage = toNumberSafe((row as any)["estimated-storage-cost-next-month"]);

        const denom = mtdSales + sales30Only;
        const coverage = denom > 0 ? currentInventory / denom : 0;

        return {
          "S.No.": index + 1,
          "Product Name": (row as any)["Product Name"] ?? "",
          "SKU": (row as any)["SKU"] || (row as any)["ASIN"] || "",
          "MTD Sales": mtdSales,
          "Sales Last 30 Days": salesLast30Days,
          "Sales Rank": salesRank || "",
          "Current Inventory": currentInventory,
          "Inventory 180+ Days": inventory180Plus,
          "Estimated Storage Cost": estStorage
            ? Number(
              formatMoneyNumberOnly(
                convertToDisplayCurrency(estStorage, storageSourceCurrency)
              ).replace(/,/g, "")
            )
            : "",
          "Inventory Coverage Ratio": coverage
            ? Number(coverage.toFixed(2))
            : "",
          "Inventory Alerts": inventoryAlerts?.[sku]?.alert || "",
        };
      });
  }, [
    invRows,
    inventoryAlerts,
    findMtdKey,
    findSales30Key,
    convertToDisplayCurrency,
    storageSourceCurrency,
  ]);

  const downloadInventoryExcel = useCallback(() => {
    if (!exportDataRows.length) return;

    const { monthName, year } = getISTYearMonth();
    const abbr = monthName.slice(0, 3);
    const periodLabel = `${abbr.charAt(0).toUpperCase()}${abbr.slice(1)}'${String(year).slice(2)}`;

    const companyNameForExcel = String(
      (userData as any)?.company_name || (userData as any)?.companyName || ""
    )
      .trim()
      .toLowerCase()
      .replace(/\b\w/g, (c) => c.toUpperCase());

    const brandNameForExcel = String(
      (userData as any)?.brand_name || (userData as any)?.brandName || ""
    )
      .trim()
      .toLowerCase()
      .replace(/\b\w/g, (c) => c.toUpperCase());

    exportPnLProductwiseBreakdownMtdExcel({
      filename: `Current-Inventory_${displayRegion}_${monthName}_${year}.xlsx`,
      titleLine: `Amazon ${displayRegion} - Current Inventory - ${periodLabel}`,
      countryName: displayRegion.toLowerCase(),
      titleCountry: displayRegion,
      platformLabel: "Amazon",
      periodLabel,
      companyName: companyNameForExcel,
      brandName: brandNameForExcel,
      homeCurrencyCode: homeCurrencyCodeForExcel,
      dataRows: exportDataRows,
    });
  }, [
    exportDataRows,
    displayRegion,
    userData,
    homeCurrencyCodeForExcel,
  ]);

  /* -------- Build DataTable columns -------- */

  const columns: ColumnDef<InventoryUiRow>[] = useMemo(() => {
    const responsiveWidth = "w-20 lg:min-w-fit";

    return [
      { key: "sno", header: "S.No.", width: "w-[60px]", cellClassName: "text-center" },
      {
        key: "productName",
        header: "Product Name",
        width: "w-[120px]",
        cellClassName: "text-left",
        headerClassName: "text-left break-words",
      },
      { key: "skuAsin", header: "SKU", width: "w-[120px]", cellClassName: "text-center", },
      { key: "mtdSales", header: "MTD Sales", width: responsiveWidth, cellClassName: "text-center" },
      {
        key: "sales30",
        header: "Sales last 30 days",
        width: responsiveWidth,
        cellClassName: "text-center",
        headerClassName: "break-words",
      },
      { key: "salesRank", header: "Sales Rank", width: responsiveWidth, cellClassName: "text-center" },
      {
        key: "currentInventory",
        header: "Current Inventory",
        width: responsiveWidth,
        cellClassName: "text-center",
        headerClassName: "break-words",
      },
      {
        key: "inventory180Plus",
        header: "Inventory 180+ days",
        width: responsiveWidth,
        cellClassName: "text-center",
        headerClassName: "break-words",
      },
      {
        key: "estStorage",
        header: storageHeaderLabel,
        width: responsiveWidth,
        cellClassName: "text-center",
        headerClassName: "break-words",
      },
      {
        key: "coverageMonths",
        header: "Coverage Ratio (In Months)",
        width: responsiveWidth,
        cellClassName: "text-center",
        headerClassName: "break-words",
      },
      {
        key: "alert",
        header: "Inventory Alerts",
        width: "w-32 lg:min-w-fit",
        cellClassName: "text-center font-medium",
        headerClassName: "break-words",
      },
    ];
  }, [storageHeaderLabel]);

  // return (
  //   <div
  //     className="
  //       mt-2 md:mt-4 rounded-2xl border bg-white p-4 shadow-sm
  //       w-full max-w-full overflow-hidden
  //       flex flex-col
  //     "
  //   >
  //     <div className="mb-3 flex items-center justify-between">
  //       <div className="flex items-baseline gap-2">
  //         <PageBreadcrumb pageTitle="Current Inventory" variant="page" align="left" />
  //       </div>

  //       <DownloadIconButton
  //         onClick={downloadInventoryExcel}
  //         disabled={invLoading || !invRows?.length}
  //         className="transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
  //       />
  //     </div>

  //     {invError ? (
  //       <div className="rounded-xl border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700">
  //         {invError}
  //       </div>
  //     ) : (
  //       <div className="w-full min-w-0 rounded-xl overflow-x-auto [-webkit-overflow-scrolling:touch]">
  //         <div className="min-w-0">
  //           <DataTable
  //             columns={columns}
  //             data={invLoading ? [] : tableRows}
  //             loading={false}
  //             paginate={true}
  //             pageSize={15}
  //             scrollY={false}
  //             maxHeight="none"
  //             emptyMessage={invLoading ? "" : "No inventory data."}
  //             rowClassName={(row) => {
  //               if (row.rowType === "total") return "bg-[#EFEFEF] font-semibold";
  //               if (row.rowType === "others") return "!bg-[#FFFFFF]";
  //               return "bg-white";
  //             }}
  //             tableClassName="
  //         table-fixed w-full
  //         [&_th]:whitespace-normal
  //         [&_th]:break-words
  //         [&_th]:leading-snug
  //         [&_th>div]:[display:-webkit-box]
  //         [&_th>div]:[-webkit-box-orient:vertical]
  //         [&_th>div]:[-webkit-line-clamp:3]
  //         [&_th>div]:overflow-hidden
  //         [&_th>div]:text-ellipsis
  //       "
  //           />
  //         </div>
  //       </div>
  //     )}
  //   </div>
  // );

  return (
    <div
      className="
      mt-2 md:mt-4 rounded-2xl border bg-white p-4 shadow-sm
      w-full max-w-full overflow-hidden
      flex flex-col
    "
    >
      <div className="mb-3 flex items-center justify-between">
        <div className="flex items-baseline gap-2">
          <PageBreadcrumb pageTitle="Current Inventory" variant="page" align="left" />
        </div>

        <DownloadIconButton
          onClick={downloadInventoryExcel}
          // disabled={invLoading || !invRows?.length}
          disabled={invLoading || !exportDataRows.length}
          className="transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
        />
      </div>

      {invError ? (
        <div className="rounded-xl border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700">
          {invError}
        </div>
      ) : invLoading ? (
        <div className="w-full rounded-xl  min-h-[280px] flex items-center justify-center">
          <Loader transparent />
        </div>
      ) : (
        <div className="w-full min-w-0 rounded-xl overflow-x-auto [-webkit-overflow-scrolling:touch]">
          <div className="min-w-0">
            <DataTable
              columns={columns}
              data={tableRows}
              loading={false}
              paginate={true}
              pageSize={15}
              scrollY={false}
              maxHeight="none"
              emptyMessage="No inventory data."
              rowClassName={(row) => {
                if (row.rowType === "total") return "bg-[#EFEFEF] font-semibold";
                if (row.rowType === "others") return "!bg-[#FFFFFF]";
                return "bg-white";
              }}
              tableClassName="
              table-fixed w-full
              [&_th]:whitespace-normal
              [&_th]:break-words
              [&_th]:leading-snug
              [&_th>div]:[display:-webkit-box]
              [&_th>div]:[-webkit-box-orient:vertical]
              [&_th>div]:[-webkit-line-clamp:3]
              [&_th>div]:overflow-hidden
              [&_th>div]:text-ellipsis
            "
            />
          </div>
        </div>
      )}
    </div>
  );

}
