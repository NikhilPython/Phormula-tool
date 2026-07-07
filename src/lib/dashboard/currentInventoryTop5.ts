// /lib/dashboard/currentInventoryTop5.ts
import React from "react";
import { jsx as _jsx } from "react/jsx-runtime";

export type InventoryRow = Record<string, string | number>;

export type InventoryUiRow = {
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
  return foundKey ? toNumberSafe((row as any)[foundKey]) : 0;
};

const isInventoryTotalRow = (r: InventoryRow) => {
  const name = String((r as any)["Product Name"] ?? "").trim().toLowerCase();
  const sku = String((r as any)["SKU"] ?? "").trim().toLowerCase();
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
  return v.toFixed(1);
};

export function buildCurrentInventoryTop5Rows(args: {
  invRows: InventoryRow[];
  inventoryAlerts: Record<string, { alert?: string; alert_type?: string }>;
}) {
  const { invRows, inventoryAlerts } = args;

  const findMtdKey = (row: InventoryRow) =>
    Object.keys(row).find((k) => k.toLowerCase().startsWith("current month units sold")) || "";

  const findSales30Key = (row: InventoryRow) => {
    const keys = Object.keys(row);
    return (
      keys.find((k) => k.trim().toLowerCase() === "others") ||
      keys.find((k) => k.toLowerCase().includes("past 30")) ||
      keys.find((k) => k.toLowerCase().includes("30 days")) ||
      keys.find((k) => k.trim().toLowerCase() === "sales for past 30 days") ||
      ""
    );
  };

  // usable rows
  const usable = invRows.filter((r) => {
    const name = String((r as any)["Product Name"] ?? "").trim();
    const sku = String((r as any)["SKU"] ?? "").trim();
    if (!name && !sku) return false;
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

    const currentInventory = toNumberSafe((r as any)["Inventory at the end of the month"]);
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
      "Inventory Age 365 Plus Days",
      "Inventory Age 365+ Days",
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

  const sortedByMtd = [...calcRows].sort((a, b) => b.mtdSales - a.mtdSales);
  const top5 = sortedByMtd.slice(0, 5);
  const top5Indices = new Set(top5.map((r) => r.index));
  const othersRows = calcRows.filter((r) => !top5Indices.has(r.index));

  const uiRows: InventoryUiRow[] = top5.map((c, idx) => {
    const skuKey = normalizeSku((c.row as any)["SKU"]);
    const rawAlert = inventoryAlerts?.[skuKey]?.alert || "";

    // ✅ ONLY show "High alert" for Top 5, else blank
    const alertToShow = rawAlert === "High alert" ? "High alert" : "";

    return {
      rowType: "normal",
      sno: idx + 1,
      productName: (c.row as any)["Product Name"] || "",
      skuAsin: (c.row as any)["SKU"] || (c.row as any)["ASIN"] || "",
      mtdSales: formatInt(c.mtdSales),
      sales30: formatInt(c.mtdSales + c.sales30),
      salesRank: c.salesRank ? formatInt(c.salesRank) : "—",
      currentInventory: formatInt(c.currentInventory),
      inventory180Plus: formatInt(c.inventory180Plus),
      estStorage: c.estStorage ? formatInt(c.estStorage) : "—",
      coverageMonths: formatRatio(c.coverage),
      alert: alertToShow, // <— key change
    };
  });

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
      estStorage: formatInt(agg.estStorage),
      coverageMonths: formatRatio(coverage),
      alert: "",
    });
  }

  const totalRow = invRows.find((r) => isInventoryTotalRow(r)) || null;
  if (totalRow) {
    const mtdKey = findMtdKey(totalRow);
    const sales30Key = findSales30Key(totalRow);

    const currentInventory = toNumberSafe((totalRow as any)["Inventory at the end of the month"]);
    const mtdSales = toNumberSafe(mtdKey ? (totalRow as any)[mtdKey] : 0);
    const sales30 = toNumberSafe(sales30Key ? (totalRow as any)[sales30Key] : 0);

    const age181to270 = getNumberByPossibleKeys(totalRow, ["inv-age-181-to-270-days", "Inventory Age 181 to 270 Days"]);
    const age271to365 = getNumberByPossibleKeys(totalRow, ["inv-age-271-to-365-days", "Inventory Age 271 to 365 Days"]);
    const age365plus = getNumberByPossibleKeys(totalRow, ["inv-age-365-plus-days", "Inventory Age 365 Plus Days", "Inventory Age 365+ Days"]);
    const inventory180Plus = age181to270 + age271to365 + age365plus;

    const denom = mtdSales + sales30;
    const coverage = denom > 0 ? currentInventory / denom : 0;

    const estStorage = getNumberByPossibleKeys(totalRow, ["estimated-storage-cost-next-month", "Estimated Storage Cost Next Month"]);

    uiRows.push({
      rowType: "total",
      sno: "",
      productName: (_jsx("span", { className: "font-semibold", children: "Total" })),
      skuAsin: "",
      currentInventory: _jsx("span", { className: "font-semibold", children: formatInt(currentInventory) }),
      inventory180Plus: _jsx("span", { className: "font-semibold", children: formatInt(inventory180Plus) }),
      salesRank: "",
      estStorage: _jsx("span", { className: "font-semibold", children: formatInt(estStorage) }),
      mtdSales: _jsx("span", { className: "font-semibold", children: formatInt(mtdSales) }),
      sales30: _jsx("span", { className: "font-semibold", children: formatInt(mtdSales + sales30) }),
      coverageMonths: _jsx("span", { className: "font-semibold", children: formatRatio(coverage) }),
      alert: "",
    });
  }

  return uiRows;
}
