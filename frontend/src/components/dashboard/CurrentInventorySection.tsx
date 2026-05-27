"use client";

import React, { useCallback, useMemo, useState } from "react";
// import * as XLSX from "xlsx-js-style";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import Loader from "@/components/loader/Loader";
import type { RegionKey } from "@/lib/dashboard/types";
import DataTable, { ColumnDef } from "../ui/table/DataTable";
import DownloadIconButton from "../ui/button/DownloadIconButton";
// import { saveAs } from "file-saver";
// import { exportPnLProductwiseBreakdownMtdExcel } from "@/lib/excel/exportCurrentInventoryExcel";
import {
  exportCurrentInventoryExcel,
  exportGlobalCurrentInventoryExcel,
} from "@/lib/excel/exportCurrentInventoryExcel";
import SegmentedToggle from "../ui/SegmentedToggle";
import type { InventoryRow } from "@/lib/inventory/fetchCurrentInventoryData";
import InfoTip from "@/components/ui/InfoTip";

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

  const [selectedInventoryCountry, setSelectedInventoryCountry] =
    useState<"uk" | "us">("uk");

  const isGlobalInventory = region === "Global";

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
    if (region === "Global") {
      return selectedInventoryCountry === "us" ? "USD" : "GBP";
    }

    if (region === "UK") return "GBP";
    if (region === "US") return "USD";
    if (region === "CA") return "CAD";

    return "GBP";
  }, [region, selectedInventoryCountry]);

  const visibleInvRows = useMemo(() => {
    if (!Array.isArray(invRows)) return [];

    if (!isGlobalInventory) return invRows;

    return invRows.filter((row) => {
      const rowCountry = String(
        (row as any).country ||
        (row as any).Country ||
        ""
      ).toLowerCase();

      return rowCountry === selectedInventoryCountry;
    });
  }, [invRows, isGlobalInventory, selectedInventoryCountry]);

  const visibleInventoryAlerts = useMemo(() => {
    if (!isGlobalInventory) return inventoryAlerts;

    const prefix = `${selectedInventoryCountry.toUpperCase()}-`;
    const next: Record<string, { alert?: string; alert_type?: string }> = {};

    Object.entries(inventoryAlerts || {}).forEach(([key, value]) => {
      const normalizedKey = String(key).trim().toUpperCase();

      if (normalizedKey.startsWith(prefix)) {
        const skuOnly = normalizedKey.slice(prefix.length);

        // Current table lookup uses SKU-only key.
        next[skuOnly] = value;

        // Keep prefixed key too, in case needed later.
        next[normalizedKey] = value;
      }
    });

    return next;
  }, [inventoryAlerts, isGlobalInventory, selectedInventoryCountry]);

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

  const findSales30Key = useCallback((row: InventoryRow) => {
    const keys = Object.keys(row);

    const exact = keys.find(
      (k) => k.trim().toLowerCase() === "sales last 30 days"
    );
    if (exact) return exact;

    const last30 = keys.find((k) =>
      k.toLowerCase().includes("last 30 days")
    );
    if (last30) return last30;

    const past30 = keys.find((k) =>
      k.toLowerCase().includes("past 30")
    );
    if (past30) return past30;

    const same = keys.find(
      (k) => k.trim().toLowerCase() === "sales for past 30 days"
    );
    if (same) return same;

    return "";
  }, []);

  /* ===================== DOWNLOAD EXCEL (FULL DATA) ===================== */

  /* -------- Transform backend rows → UI rows for DataTable -------- */

  const tableRows: InventoryUiRow[] = useMemo(() => {
    if (!visibleInvRows?.length) return [];

    // 1) Filter out empty + backend total rows
    const usable = visibleInvRows.filter((r) => {
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

      const currentInventory = toNumberSafe(
        (r as any)["Current Inventory"] ??
        (r as any)["Inventory at the end of the month"] ??
        (r as any)["available"] ??
        0
      );
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

      const coverageFromBackend = toNumberSafe(
        (r as any)["Coverage Ratio (In Months)"] ??
        (r as any)["Coverage Ratio"] ??
        (r as any)["coverage_ratio"] ??
        0
      );

      const coverage =
        coverageFromBackend > 0
          ? coverageFromBackend
          : sales30 > 0
            ? currentInventory / sales30
            : 0;

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
      const alertText = visibleInventoryAlerts?.[skuKey]?.alert || "";

      return {
        rowType: "normal",
        sno: idx + 1,
        productName: (row as any)["Product Name"] || "",
        skuAsin: (row as any)["SKU"] || (row as any)["ASIN"] || "",
        mtdSales: formatInt(mtdSales),
        sales30: formatInt(sales30),
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

      const coverage = agg.sales30 > 0 ? agg.currentInventory / agg.sales30 : 0;

      uiRows.push({
        rowType: "others",
        sno: 6,
        productName: "Others",
        skuAsin: "",
        mtdSales: formatInt(agg.mtdSales),
        sales30: formatInt(agg.sales30),
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
    // 7) Total row calculated from all real inventory rows.
    // Backend skuwise_items excludes Product Name === "Total", so calculate it here.
    const totalAgg = calcRows.reduce(
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

    const totalCoverage =
      totalAgg.sales30 > 0
        ? totalAgg.currentInventory / totalAgg.sales30
        : 0;

    uiRows.push({
      rowType: "total",
      sno: "",
      productName: <span className="font-semibold">Total</span>,
      skuAsin: "",
      mtdSales: <span className="font-semibold">{formatInt(totalAgg.mtdSales)}</span>,
      sales30: <span className="font-semibold">{formatInt(totalAgg.sales30)}</span>,
      salesRank: "",
      currentInventory: (
        <span className="font-semibold">
          {formatInt(totalAgg.currentInventory)}
        </span>
      ),
      inventory180Plus: (
        <span className="font-semibold">
          {formatInt(totalAgg.inventory180Plus)}
        </span>
      ),
      estStorage: (
        <span className="font-semibold">
          {formatMoneyNumberOnly(
            convertToDisplayCurrency(totalAgg.estStorage, storageSourceCurrency)
          )}
        </span>
      ),
      coverageMonths: (
        <span className="font-semibold">
          {formatRatio(totalCoverage)}
        </span>
      ),
      alert: "",
    });

    return uiRows;
    // }, [invRows, findMtdKey, findSales30Key, inventoryAlerts]);
  }, [
    visibleInvRows,
    findMtdKey,
    findSales30Key,
    visibleInventoryAlerts,
    convertToDisplayCurrency,
    storageSourceCurrency,
  ]);

  const exportDataRows = useMemo(() => {
    if (!visibleInvRows?.length) return [];

    const normalRows: InventoryRow[] = [];
    let totalRow: InventoryRow | null = null;

    visibleInvRows.forEach((r) => {
      const name = String(r["Product Name"] ?? "").trim();
      const sku = String(r["SKU"] ?? "").trim();
      const isEmpty = !name && !sku;
      if (isEmpty) return;

      if (isInventoryTotalRow(r)) {
        totalRow = r;
      } else {
        normalRows.push(r);
      }
    });

    const buildRow = (row: InventoryRow, index: number, isTotal = false) => {
      const sku = normalizeSku((row as any)["SKU"]);
      const mtdKey = findMtdKey(row);
      const sales30Key = findSales30Key(row);

      const currentInventory =
        toNumberSafe((row as any)["Inventory at the end of the month"]) ||
        toNumberSafe((row as any)["Available Inventory"]);

      const mtdSales = toNumberSafe(mtdKey ? (row as any)[mtdKey] : 0);
      const sales30Only = toNumberSafe(sales30Key ? (row as any)[sales30Key] : 0);
      const salesLast30Days = sales30Only;

      const age181to270 = getNumberByPossibleKeys(row, ["inv-age-181-to-270-days"]);
      const age271to365 = getNumberByPossibleKeys(row, ["inv-age-271-to-365-days"]);
      const age365plus = getNumberByPossibleKeys(row, ["inv-age-365-plus-days"]);

      const inventory180Plus = age181to270 + age271to365 + age365plus;

      const salesRank = toNumberSafe((row as any)["sales-rank"]);
      const estStorage = toNumberSafe((row as any)["estimated-storage-cost-next-month"]);

      const coverage = sales30Only > 0 ? currentInventory / sales30Only : 0;

      return {
        "S.No.": isTotal ? "" : index + 1,
        "Product Name": isTotal ? "Total" : (row as any)["Product Name"] ?? "",
        "SKU": isTotal ? "" : (row as any)["SKU"] || "",
        "MTD Sales": mtdSales,
        "Sales Last 30 Days": salesLast30Days,
        "Sales Rank": isTotal ? "" : salesRank,
        "Current Inventory": currentInventory,
        "Inventory 180+ Days": inventory180Plus,
        "Estimated Storage Cost": estStorage,
        "Inventory Coverage Ratio": coverage,
        "Inventory Alerts": isTotal ? "" : (visibleInventoryAlerts?.[sku]?.alert || ""),
      };
    };

    const sortedNormalRows = [...normalRows].sort((a, b) => {
      const aMtdKey = findMtdKey(a);
      const bMtdKey = findMtdKey(b);

      const aMtdSales = toNumberSafe(aMtdKey ? (a as any)[aMtdKey] : 0);
      const bMtdSales = toNumberSafe(bMtdKey ? (b as any)[bMtdKey] : 0);

      return bMtdSales - aMtdSales;
    });

    const finalRows = sortedNormalRows.map((r, i) => buildRow(r, i));

    type ExportInventoryRow = ReturnType<typeof buildRow>;

    const totalExportRow: ExportInventoryRow = finalRows.reduce<ExportInventoryRow>(
      (acc, row) => {
        acc["MTD Sales"] += toNumberSafe(row["MTD Sales"]);
        acc["Sales Last 30 Days"] += toNumberSafe(row["Sales Last 30 Days"]);
        acc["Current Inventory"] += toNumberSafe(row["Current Inventory"]);
        acc["Inventory 180+ Days"] += toNumberSafe(row["Inventory 180+ Days"]);
        acc["Estimated Storage Cost"] += toNumberSafe(row["Estimated Storage Cost"]);
        return acc;
      },
      {
        "S.No.": "",
        "Product Name": "Total",
        "SKU": "",
        "MTD Sales": 0,
        "Sales Last 30 Days": 0,
        "Sales Rank": "",
        "Current Inventory": 0,
        "Inventory 180+ Days": 0,
        "Estimated Storage Cost": 0,
        "Inventory Coverage Ratio": 0,
        "Inventory Alerts": "",
      }
    );

    totalExportRow["Inventory Coverage Ratio"] =
      totalExportRow["Sales Last 30 Days"] > 0
        ? totalExportRow["Current Inventory"] / totalExportRow["Sales Last 30 Days"]
        : 0;

    finalRows.push(totalExportRow);
    return finalRows;

  }, [visibleInvRows, visibleInventoryAlerts, findMtdKey, findSales30Key]);

  const buildExportRowsForCountry = useCallback(
    (country: "uk" | "us") => {
      const originalSelected = selectedInventoryCountry;

      const countryRows = (invRows || []).filter((row) => {
        const rowCountry = String(
          (row as any).country || (row as any).Country || ""
        ).toLowerCase();

        return rowCountry === country;
      });

      if (!countryRows.length) return [];

      const countryAlerts: Record<string, { alert?: string; alert_type?: string }> = {};
      const prefix = `${country.toUpperCase()}-`;

      Object.entries(inventoryAlerts || {}).forEach(([key, value]) => {
        const normalizedKey = String(key).trim().toUpperCase();

        if (normalizedKey.startsWith(prefix)) {
          const skuOnly = normalizedKey.slice(prefix.length);
          countryAlerts[skuOnly] = value;
        }
      });

      const usableRows = countryRows.filter((r) => {
        const name = String(r["Product Name"] ?? "").trim();
        const sku = String(r["SKU"] ?? "").trim();
        return (name || sku) && !isInventoryTotalRow(r);
      });

      const sourceCurrency: CurrencyCode = country === "us" ? "USD" : "GBP";

      const mappedRows = usableRows
        .map((row) => {
          const sku = normalizeSku((row as any)["SKU"]);
          const mtdKey = findMtdKey(row);
          const sales30Key = findSales30Key(row);

          const currentInventory =
            toNumberSafe((row as any)["Current Inventory"]) ||
            toNumberSafe((row as any)["Inventory at the end of the month"]) ||
            toNumberSafe((row as any)["Available Inventory"]) ||
            toNumberSafe((row as any)["available"]);

          const mtdSales = toNumberSafe(mtdKey ? (row as any)[mtdKey] : 0);
          const salesLast30Days = toNumberSafe(sales30Key ? (row as any)[sales30Key] : 0);

          const inventory180Plus =
            getNumberByPossibleKeys(row, [
              "inv-age-181-to-270-days",
              "inv_age_181_to_270_days",
              "Inventory Age 181 to 270 Days",
            ]) +
            getNumberByPossibleKeys(row, [
              "inv-age-271-to-365-days",
              "inv_age_271_to_365_days",
              "Inventory Age 271 to 365 Days",
            ]) +
            getNumberByPossibleKeys(row, [
              "inv-age-365-plus-days",
              "inv_age_365_plus_days",
              "Inventory Age 365+ Days",
              "inv age 365 plus days",
            ]);

          const estimatedStorageRaw = toNumberSafe(
            (row as any)["estimated-storage-cost-next-month"] ||
            (row as any)["Estimated Storage Cost"] ||
            (row as any)["Estimated Storage Cost ($)"]
          );

          return {
            sku,
            row,
            mtdSales,
            exportRow: {
              "S.No.": 0,
              "Product Name": (row as any)["Product Name"] ?? "",
              "SKU": sku,
              "MTD Sales": mtdSales,
              "Sales Last 30 Days": salesLast30Days,
              "Sales Rank": toNumberSafe((row as any)["sales-rank"] || (row as any)["Sales Rank"]),
              "Current Inventory": currentInventory,
              "Inventory 180+ Days": inventory180Plus,
              "Estimated Storage Cost": convertToDisplayCurrency(
                estimatedStorageRaw,
                sourceCurrency
              ),
              "Inventory Coverage Ratio":
                salesLast30Days > 0 ? currentInventory / salesLast30Days : 0,
              "Inventory Alerts": countryAlerts?.[sku]?.alert || "",
            },
          };
        })
        .sort((a, b) => b.mtdSales - a.mtdSales);

      const finalRows = mappedRows.map((item, index) => ({
        ...item.exportRow,
        "S.No.": index + 1,
      }));

      const totalRow = finalRows.reduce<Record<string, any>>(
        (acc, row) => {
          acc["MTD Sales"] += toNumberSafe(row["MTD Sales"]);
          acc["Sales Last 30 Days"] += toNumberSafe(row["Sales Last 30 Days"]);
          acc["Current Inventory"] += toNumberSafe(row["Current Inventory"]);
          acc["Inventory 180+ Days"] += toNumberSafe(row["Inventory 180+ Days"]);
          acc["Estimated Storage Cost"] += toNumberSafe(row["Estimated Storage Cost"]);
          return acc;
        },
        {
          "S.No.": "",
          "Product Name": "Total",
          "SKU": "",
          "MTD Sales": 0,
          "Sales Last 30 Days": 0,
          "Sales Rank": "",
          "Current Inventory": 0,
          "Inventory 180+ Days": 0,
          "Estimated Storage Cost": 0,
          "Inventory Coverage Ratio": 0,
          "Inventory Alerts": "",
        }
      );

      totalRow["Inventory Coverage Ratio"] =
        totalRow["Sales Last 30 Days"] > 0
          ? totalRow["Current Inventory"] / totalRow["Sales Last 30 Days"]
          : 0;

      return [...finalRows, totalRow];
    },
    [
      invRows,
      inventoryAlerts,
      findMtdKey,
      findSales30Key,
      convertToDisplayCurrency,
    ]
  );

  const downloadInventoryExcel = useCallback(() => {
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

    if (isGlobalInventory) {
      const ukRows = buildExportRowsForCountry("uk");
      const usRows = buildExportRowsForCountry("us");

      void exportGlobalCurrentInventoryExcel({
        filename: `Current-Inventory_Global_${monthName}_${year}.xlsx`,
        titleLine: `Amazon Global - Current Inventory - ${periodLabel}`,
        platformLabel: "Phormula",
        periodLabel,
        companyName: companyNameForExcel,
        brandName: brandNameForExcel,
        homeCurrencyCode: homeCurrencyCodeForExcel,
        ukRows,
        usRows,
      });

      return;
    }

    if (!exportDataRows.length) return;

    exportCurrentInventoryExcel({
      filename: `Current-Inventory_${displayRegion}_${monthName}_${year}.xlsx`,
      titleLine: `Amazon ${displayRegion} - Current Inventory - ${periodLabel}`,
      countryName: displayRegion.toLowerCase(),
      titleCountry: displayRegion,
      platformLabel: "Phormula",
      periodLabel,
      companyName: companyNameForExcel,
      brandName: brandNameForExcel,
      homeCurrencyCode: homeCurrencyCodeForExcel,
      dataRows: exportDataRows,
    });
  }, [
    isGlobalInventory,
    buildExportRowsForCountry,
    exportDataRows,
    displayRegion,
    userData,
    homeCurrencyCodeForExcel,
  ]);

  const INVENTORY_ALERT_CRITERIA = (
    <div className="space-y-1">
      <p>
        <strong>High alert:</strong> Coverage ratio is 2 months or less.
      </p>
      <p>
        <strong>Please send shipment:</strong> Coverage ratio is more than 2 months
        and up to 5 months.
      </p>
      <p>
        <strong>High inventory coverage ratio:</strong> Coverage ratio is 6 months
        or more, and there is no long-term aged inventory.
      </p>
      <p>
        <strong>High storage cost:</strong> Estimated storage cost next month is
        greater than 100.
      </p>
      <p>
        <strong>Long-term aged inventory:</strong> SKU has inventory units in aged
        buckets above 180 days.
      </p>
      <p>
        <strong>No alert:</strong> None of the above criteria are met.
      </p>
    </div>
  );

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
        header: (
          <div className="flex items-center justify-center gap-1 whitespace-nowrap">
            <span>Inventory Alerts</span>
            <InfoTip text={INVENTORY_ALERT_CRITERIA} />
          </div>
        ),
        width: "w-40 lg:min-w-fit",
        cellClassName: "text-center font-medium",
        headerClassName: "break-words",
      },
    ];
  }, [storageHeaderLabel]);

  return (
    <div
      className="
      mt-2 md:mt-4 rounded-2xl border bg-white p-4 shadow-sm
      w-full max-w-full overflow-hidden
      flex flex-col
    "
    >
      <div className="mb-3 flex items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          <PageBreadcrumb pageTitle="Current Inventory" variant="page" align="left" />
        </div>

        <div className="flex items-center gap-2">
          {isGlobalInventory && (
            <SegmentedToggle<"uk" | "us">
              value={selectedInventoryCountry}
              onChange={setSelectedInventoryCountry}
              options={[
                { value: "uk", label: "UK" },
                { value: "us", label: "US" },
              ]}
              compact
              textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
            />
          )}

          <DownloadIconButton
            onClick={downloadInventoryExcel}
            disabled={invLoading || !exportDataRows.length}
            className="transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
          />
        </div>
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
