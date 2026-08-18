"use client";

import React, { useMemo, useState } from "react";

type Priority = "Critical" | "High" | "Medium" | "Opportunity";
type Category = "Inventory & Dispatch" | "Ads" | "Finance" | "Returns";

type ActionItem = {
  id: string;
  category: Category;
  priority: Priority;
  title: string;
  reason: string;
  metrics: { value: string; label: string }[];
  action: string;
};

type IconName =
  | "clipboard"
  | "check"
  | "box"
  | "megaphone"
  | "coin"
  | "star"
  | "target"
  | "trend"
  | "heart"
  | "filter"
  | "chevron"
  | "trophy"
  | "spark";

const iconPaths: Record<IconName, React.ReactNode> = {
  clipboard: <><rect x="5" y="4" width="14" height="17" rx="2" /><path d="M9 4.5V3h6v1.5M8 10h8M8 14h8M8 18h5" /></>,
  check: <path d="m5 12 4 4L19 6" />,
  box: <><path d="m12 3 8 4.5v9L12 21l-8-4.5v-9L12 3Z" /><path d="m4.5 7.5 7.5 4 7.5-4M12 11.5V21" /></>,
  megaphone: <><path d="M4 13h3l8 4V7l-8 4H4v2Z" /><path d="M7 13v5h3M18 9.5a4 4 0 0 1 0 5" /></>,
  coin: <><circle cx="12" cy="12" r="8" /><path d="M14.5 8.5c-.7-.5-1.4-.7-2.4-.7-1.5 0-2.6.8-2.6 2 0 3 5 1.4 5 4.2 0 1.2-1 2.1-2.8 2.1-1 0-1.9-.3-2.7-.8M12 6.5v11" /></>,
  star: <path d="m12 3 2.8 5.7 6.2.9-4.5 4.4 1.1 6.2L12 17.3l-5.6 2.9 1.1-6.2L3 9.6l6.2-.9L12 3Z" />,
  target: <><circle cx="12" cy="12" r="8" /><circle cx="12" cy="12" r="4" /><path d="m15 9 5-5M16 4h4v4" /></>,
  trend: <><path d="M4 17 9 12l3 3 7-8" /><path d="M14 7h5v5" /></>,
  heart: <path d="M20 8.5c0 5-8 10-8 10s-8-5-8-10A4.5 4.5 0 0 1 12 5a4.5 4.5 0 0 1 8 3.5Z" />,
  filter: <path d="M4 6h16l-6 7v5l-4 2v-7L4 6Z" />,
  chevron: <path d="m8 10 4 4 4-4" />,
  trophy: <><path d="M8 4h8v4c0 4-2 7-4 7s-4-3-4-7V4Z" /><path d="M8 6H5v2c0 2 1.5 3 3 3M16 6h3v2c0 2-1.5 3-3 3M12 15v4M9 21h6" /></>,
  spark: <><path d="m12 3 1.2 3.8L17 8l-3.8 1.2L12 13l-1.2-3.8L7 8l3.8-1.2L12 3Z" /><path d="m18 14 .7 2.3L21 17l-2.3.7L18 20l-.7-2.3L15 17l2.3-.7L18 14Z" /></>,
};

function Icon({ name, className = "h-4 w-4" }: { name: IconName; className?: string }) {
  return <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">{iconPaths[name]}</svg>;
}

const categoryIcon: Record<Category, IconName> = { "Inventory & Dispatch": "box", Ads: "megaphone", Finance: "coin", Returns: "box" };

const priorityClass: Record<Priority, string> = {
  Critical: "border-[#FFB8B8] bg-[#FFF2F2] text-[#E83D3D]",
  High: "border-[#FFD0B5] bg-[#FFF6EF] text-[#E56A25]",
  Medium: "border-[#F3D99B] bg-[#FFF9EA] text-[#D59000]",
  Opportunity: "border-[#A9DDCF] bg-[#EFFAF6] text-[#168A70]",
};

export type MonthlyMetricRow = Record<string, string | number | null | undefined> & {
  sku?: string;
  product_name?: string;
  month?: string;
  year?: string | number;
  country?: string;
};

type MovementStatus = "up" | "down" | "stable";
type MetricFormat = "currency" | "percent" | "number" | "decimal";
type MetricCategory = "Revenue & Demand" | "Profitability" | "Advertising & Promotion" | "Fees & Other Costs";

type MonthSnapshot = {
  key: string;
  label: string;
  country: string;
  values: Record<string, number>;
};

type MetricDefinition = {
  key: string;
  title: string;
  category: MetricCategory;
  icon: IconName;
  format: MetricFormat;
  // For cost/efficiency KPIs where a lower value is better (for example TACoS).
  inverseTrend?: boolean;
  detail: (snapshot: MonthSnapshot, format: (value: number, type: MetricFormat) => string) => string;
};

const MONTHS = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"];


// Temporary frontend demo data. Remove this fallback once the backend monthly route is ready.
// Keeping it in the same shape as the database response makes the API swap one-line later.
export const DUMMY_MONTHLY_DATA: MonthlyMetricRow[] = [
  {
    sku: "TOTAL", product_name: "TOTAL", month: "march", year: 2026, country: "uk",
    gross_sales: 126400, net_sales: 117200, quantity: 8120, return_quantity: 188, total_quantity: 7932, asp: 14.78,
    profit: 55500, profit_percentage: 47.35, total_cm2_profit: 32700, total_cm2_margins: 27.90,
    cm1_profit_per_unit: 7.00, cm2_profit_per_unit: 4.12,
    total_ads: 30200, tacos_total_advertising_cost_of_sale: 25.77, ads_acos: 31.8, ads_roas: 3.14, ads_conversion_rate: 9.8,
    promotional_rebates: -6700, promotional_rebates_percentage: -5.72, amazon_fees: 52200,
    current_net_reimbursement: 3100, reimbursement_vs_sales: 2.65, cogs: -36500,
    storage_fee: 720, placement_fee: -1980, shipment_fees: 82, shipping_charges: 2980, lost_total: -1240,
    platform_fee: 430, platform_fee_inventory_storage: 980, disbursement: 35400,
    inventory_units: 22100, inventory_coverage_months: 1.84, stockout_skus: 9, low_stock_skus: 31,
    aged_inventory_percent: 14.8, inbound_units: 5800, dispatched_units: 7700, dispatch_rate: 94.2,
  },
  {
    sku: "TOTAL", product_name: "TOTAL", month: "april", year: 2026, country: "uk",
    gross_sales: 130900, net_sales: 121900, quantity: 8350, return_quantity: 180, total_quantity: 8170, asp: 14.92,
    profit: 57900, profit_percentage: 47.50, total_cm2_profit: 34200, total_cm2_margins: 28.06,
    cm1_profit_per_unit: 7.09, cm2_profit_per_unit: 4.19,
    total_ads: 30800, tacos_total_advertising_cost_of_sale: 25.27, ads_acos: 30.9, ads_roas: 3.24, ads_conversion_rate: 10.1,
    promotional_rebates: -6810, promotional_rebates_percentage: -5.59, amazon_fees: 53800,
    current_net_reimbursement: 3300, reimbursement_vs_sales: 2.71, cogs: -37700,
    storage_fee: 700, placement_fee: -1930, shipment_fees: 79, shipping_charges: 3060, lost_total: -1160,
    platform_fee: 440, platform_fee_inventory_storage: 950, disbursement: 36600,
    inventory_units: 22900, inventory_coverage_months: 1.93, stockout_skus: 8, low_stock_skus: 28,
    aged_inventory_percent: 14.2, inbound_units: 6100, dispatched_units: 8040, dispatch_rate: 95.0,
  },
  {
    sku: "TOTAL", product_name: "TOTAL", month: "may", year: 2026, country: "uk",
    gross_sales: 137600, net_sales: 128500, quantity: 8690, return_quantity: 176, total_quantity: 8514, asp: 15.09,
    profit: 61400, profit_percentage: 47.78, total_cm2_profit: 36750, total_cm2_margins: 28.60,
    cm1_profit_per_unit: 7.21, cm2_profit_per_unit: 4.32,
    total_ads: 31600, tacos_total_advertising_cost_of_sale: 24.59, ads_acos: 30.1, ads_roas: 3.32, ads_conversion_rate: 10.5,
    promotional_rebates: -6960, promotional_rebates_percentage: -5.42, amazon_fees: 56300,
    current_net_reimbursement: 3480, reimbursement_vs_sales: 2.71, cogs: -39200,
    storage_fee: 690, placement_fee: -2010, shipment_fees: 76, shipping_charges: 3180, lost_total: -1080,
    platform_fee: 458, platform_fee_inventory_storage: 925, disbursement: 39000,
    inventory_units: 23750, inventory_coverage_months: 2.02, stockout_skus: 7, low_stock_skus: 25,
    aged_inventory_percent: 13.7, inbound_units: 6450, dispatched_units: 8420, dispatch_rate: 95.7,
  },
  {
    sku: "TOTAL", product_name: "TOTAL", month: "june", year: 2026, country: "uk",
    gross_sales: 142900, net_sales: 133200, quantity: 8920, return_quantity: 171, total_quantity: 8749, asp: 15.22,
    profit: 63900, profit_percentage: 47.97, total_cm2_profit: 38450, total_cm2_margins: 28.87,
    cm1_profit_per_unit: 7.30, cm2_profit_per_unit: 4.39,
    total_ads: 32400, tacos_total_advertising_cost_of_sale: 24.32, ads_acos: 29.7, ads_roas: 3.37, ads_conversion_rate: 10.7,
    promotional_rebates: -7080, promotional_rebates_percentage: -5.32, amazon_fees: 57900,
    current_net_reimbursement: 3690, reimbursement_vs_sales: 2.77, cogs: -40500,
    storage_fee: 670, placement_fee: -1950, shipment_fees: 74, shipping_charges: 3260, lost_total: -1020,
    platform_fee: 470, platform_fee_inventory_storage: 900, disbursement: 40700,
    inventory_units: 24500, inventory_coverage_months: 2.12, stockout_skus: 6, low_stock_skus: 23,
    aged_inventory_percent: 13.1, inbound_units: 6700, dispatched_units: 8670, dispatch_rate: 96.1,
  },
  {
    sku: "TOTAL", product_name: "TOTAL", month: "july", year: 2026, country: "uk",
    gross_sales: 147900, net_sales: 138100, quantity: 9150, return_quantity: 169, total_quantity: 8981, asp: 15.38,
    profit: 66400, profit_percentage: 48.08, total_cm2_profit: 40100, total_cm2_margins: 29.04,
    cm1_profit_per_unit: 7.39, cm2_profit_per_unit: 4.46,
    total_ads: 33400, tacos_total_advertising_cost_of_sale: 24.19, ads_acos: 29.2, ads_roas: 3.42, ads_conversion_rate: 10.9,
    promotional_rebates: -7220, promotional_rebates_percentage: -5.23, amazon_fees: 59600,
    current_net_reimbursement: 3910, reimbursement_vs_sales: 2.83, cogs: -41800,
    storage_fee: 655, placement_fee: -1900, shipment_fees: 72, shipping_charges: 3330, lost_total: -960,
    platform_fee: 486, platform_fee_inventory_storage: 875, disbursement: 42500,
    inventory_units: 25200, inventory_coverage_months: 2.24, stockout_skus: 5, low_stock_skus: 21,
    aged_inventory_percent: 12.6, inbound_units: 7000, dispatched_units: 8940, dispatch_rate: 96.5,
  },
  {
    sku: "TOTAL", product_name: "TOTAL", month: "august", year: 2026, country: "uk",
    gross_sales: 157800, net_sales: 146200, quantity: 9580, return_quantity: 162, total_quantity: 9418, asp: 15.52,
    profit: 70200, profit_percentage: 48.02, total_cm2_profit: 42950, total_cm2_margins: 29.38,
    cm1_profit_per_unit: 7.45, cm2_profit_per_unit: 4.56,
    total_ads: 34500, tacos_total_advertising_cost_of_sale: 23.60, ads_acos: 27.8, ads_roas: 3.60, ads_conversion_rate: 11.6,
    promotional_rebates: -7340, promotional_rebates_percentage: -5.02, amazon_fees: 62400,
    current_net_reimbursement: 4180, reimbursement_vs_sales: 2.86, cogs: -43900,
    storage_fee: 620, placement_fee: -1810, shipment_fees: 70, shipping_charges: 3410, lost_total: -880,
    platform_fee: 505, platform_fee_inventory_storage: 830, disbursement: 45200,
    inventory_units: 26900, inventory_coverage_months: 2.48, stockout_skus: 4, low_stock_skus: 18,
    aged_inventory_percent: 11.7, inbound_units: 7600, dispatched_units: 9460, dispatch_rate: 98.2,
  },
];

function toNumber(value: unknown): number | null {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value !== "string") return null;
  const cleaned = value.replace(/,/g, "").replace(/%/g, "").trim();
  if (!cleaned) return null;
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function isTotalRow(row: MonthlyMetricRow) {
  return String(row.sku ?? "").trim().toUpperCase() === "TOTAL" || String(row.product_name ?? "").trim().toUpperCase() === "TOTAL";
}

function monthInfo(row: MonthlyMetricRow) {
  const monthName = String(row.month ?? "").trim().toLowerCase();
  const monthIndex = MONTHS.indexOf(monthName);
  const year = Number(row.year);
  if (monthIndex < 0 || !Number.isFinite(year)) return null;
  return {
    key: `${year}-${String(monthIndex + 1).padStart(2, "0")}`,
    sortKey: year * 12 + monthIndex,
    label: `${MONTHS[monthIndex].slice(0, 3).replace(/^./, (c) => c.toUpperCase())}'${String(year).slice(-2)}`,
  };
}

function safePercent(part: number, whole: number) {
  return whole === 0 ? 0 : (part / whole) * 100;
}

function buildSnapshot(rows: MonthlyMetricRow[]): MonthSnapshot | null {
  if (!rows.length) return null;
  const info = monthInfo(rows[0]);
  if (!info) return null;

  const totalRow = rows.find(isTotalRow);
  const skuRows = rows.filter((row) => !isTotalRow(row));

  const valueFromTotalOrSum = (...fields: string[]) => {
    if (totalRow) {
      let zeroFallback: number | null = null;
      for (const field of fields) {
        const value = toNumber(totalRow[field]);
        if (value === null) continue;
        if (value !== 0) return value;
        zeroFallback = 0;
      }
      if (zeroFallback !== null) return zeroFallback;
    }
    let zeroFallback: number | null = null;
    for (const field of fields) {
      const values = skuRows.map((row) => toNumber(row[field])).filter((value): value is number => value !== null);
      if (!values.length) continue;
      const total = values.reduce((sum, value) => sum + value, 0);
      if (total !== 0) return total;
      zeroFallback = 0;
    }
    return zeroFallback ?? 0;
  };

  const valueFromTotal = (...fields: string[]) => {
    if (!totalRow) return null;
    let zeroFallback: number | null = null;
    for (const field of fields) {
      const value = toNumber(totalRow[field]);
      if (value === null) continue;
      if (value !== 0) return value;
      zeroFallback = 0;
    }
    return zeroFallback;
  };

  const quantity = valueFromTotalOrSum("quantity");
  const returnQuantity = valueFromTotalOrSum("return_quantity");
  const totalQuantity = valueFromTotalOrSum("total_quantity");
  const grossSales = valueFromTotalOrSum("gross_sales");
  const netSales = valueFromTotalOrSum("net_sales");
  const profit = valueFromTotalOrSum("profit");
  const promotionalRebates = valueFromTotalOrSum("promotional_rebates");
  const cogs = valueFromTotalOrSum("cogs", "cost_of_unit_sold");
  const fbaFees = Math.abs(valueFromTotalOrSum("fba_fees"));
  const sellingFees = Math.abs(valueFromTotalOrSum("selling_fees"));
  const amazonFeesFromApi = Math.abs(valueFromTotalOrSum("amazon_fees", "amazon_fee", "marketplace_fees"));
  const amazonFees = amazonFeesFromApi || (fbaFees + sellingFees);

  const explicitAds = valueFromTotal("total_ads", "advertising_fees", "advertising_total", "ads_spend");
  const adsSpend = explicitAds ?? (
    valueFromTotalOrSum("visible_ads") +
    valueFromTotalOrSum("dealsvouchar_ads") +
    valueFromTotalOrSum("brand_spend")
  );

  const cm2Profit = valueFromTotalOrSum("total_cm2_profit", "cm2_profit");
  const profitMargin = valueFromTotal("profit_percentage", "cm1_profit_per") ?? safePercent(profit, netSales);
  const cm2Margin = valueFromTotal("total_cm2_margins", "cm2_margins", "cm2_profit_percentage", "cm2_profit_per") ?? safePercent(cm2Profit, netSales);
  const tacos = valueFromTotal("tacos_total_advertising_cost_of_sale") ?? safePercent(adsSpend, netSales);
  const acos = valueFromTotal("ads_acos", "acos_percentage", "acos") ?? 0;
  const asp = valueFromTotal("asp") ?? (totalQuantity === 0 ? 0 : netSales / totalQuantity);
  const rebatePercent = valueFromTotal("promotional_rebates_percentage") ?? safePercent(promotionalRebates, netSales);
  const reimbursement = valueFromTotalOrSum("current_net_reimbursement", "rembursement_fee");
  const reimbursementVsSales = valueFromTotal("reimbursement_vs_sales") ?? safePercent(reimbursement, netSales);
  const cm1ProfitPerUnit = valueFromTotal("cm1_profit_per_unit", "unit_wise_profitability") ?? (totalQuantity === 0 ? 0 : profit / totalQuantity);
  const cm2ProfitPerUnit = valueFromTotal("cm2_profit_per_unit") ?? (totalQuantity === 0 ? 0 : cm2Profit / totalQuantity);

  return {
    key: info.key,
    label: info.label,
    country: String(totalRow?.country ?? rows[0]?.country ?? "").toLowerCase(),
    values: {
      grossSales,
      netSales,
      quantity,
      totalQuantity,
      returnQuantity,
      returnRate: safePercent(returnQuantity, quantity),
      asp,
      profit,
      profitMargin,
      cm2Profit,
      cm2Margin,
      cm1ProfitPerUnit,
      cm2ProfitPerUnit,
      adsSpend,
      tacos,
      acos,
      adsRoas: valueFromTotal("ads_roas") ?? 0,
      adsConversionRate: valueFromTotal("ads_conversion_rate", "ad_conversion_rate", "conversion_rate", "ads_cvr", "cvr") ?? 0,
      promotionalRebates: Math.abs(promotionalRebates),
      rebatePercent,
      fbaFees,
      sellingFees,
      amazonFees,
      reimbursement,
      reimbursementVsSales,
      cogs,
      storageFee: valueFromTotalOrSum("storage_fee"),
      placementFee: valueFromTotalOrSum("placement_fee"),
      shipmentFees: valueFromTotalOrSum("shipment_fees"),
      shippingCharges: valueFromTotalOrSum("shipping_charges"),
      lostTotal: valueFromTotalOrSum("lost_total"),
      platformFee: valueFromTotalOrSum("platform_fee"),
      inventoryStorageFee: valueFromTotalOrSum("platform_fee_inventory_storage"),
      disbursement: valueFromTotalOrSum("disbursement"),
      inventoryUnits: valueFromTotalOrSum("inventory_units", "available_inventory_units"),
      inventoryCoverageMonths: valueFromTotal("inventory_coverage_months", "inventory_coverage") ?? 0,
      stockoutSkus: valueFromTotal("stockout_skus") ?? 0,
      lowStockSkus: valueFromTotal("low_stock_skus") ?? 0,
      agedInventoryPercent: valueFromTotal("aged_inventory_percent") ?? 0,
      inboundUnits: valueFromTotalOrSum("inbound_units"),
      dispatchedUnits: valueFromTotalOrSum("dispatched_units", "dispatch_units"),
      dispatchRate: valueFromTotal("dispatch_rate") ?? 0,
    },
  };
}

function buildMonthlySnapshots(rows: MonthlyMetricRow[]) {
  const groups = new Map<string, { sortKey: number; rows: MonthlyMetricRow[] }>();
  rows.forEach((row) => {
    const info = monthInfo(row);
    if (!info) return;
    const current = groups.get(info.key) ?? { sortKey: info.sortKey, rows: [] };
    current.rows.push(row);
    groups.set(info.key, current);
  });
  return [...groups.values()]
    .sort((a, b) => a.sortKey - b.sortKey)
    .map((group) => buildSnapshot(group.rows))
    .filter((snapshot): snapshot is MonthSnapshot => Boolean(snapshot));
}

function percentageChange(current: number, previous?: number) {
  if (previous === undefined || previous === null || previous === 0) return null;
  return ((current - previous) / Math.abs(previous)) * 100;
}

function movementStatus(delta: number | null): MovementStatus {
  if (delta !== null && delta >= 4) return "up";
  if (delta !== null && delta <= -4) return "down";
  return "stable";
}

const movementTheme: Record<MovementStatus, { label: string; text: string; border: string; bg: string; softBg: string }> = {
  up: { label: "Growing", text: "text-[#078B70]", border: "border-[#4FA98D] border-t-4 border-t-[#4FA98D]", bg: "bg-[#078B70]", softBg: "bg-[#F0FAF7]" },
  down: { label: "Down", text: "text-[#D94B4B]", border: "border-[#D97A7A] border-t-4 border-t-[#D97A7A]", bg: "bg-[#D94B4B]", softBg: "bg-[#FFF5F5]" },
  stable: { label: "Stable", text: "text-[#2878B8]", border: "border-[#6EA7D2] border-t-4 border-t-[#6EA7D2]", bg: "bg-[#2878B8]", softBg: "bg-[#F3F9FE]" },
};

function currencyFromCountry(country: string) {
  if (["uk", "gb", "gbr"].includes(country)) return "GBP";
  if (["ca", "can", "canada"].includes(country)) return "CAD";
  if (["in", "ind", "india"].includes(country)) return "INR";
  if (["eu", "de", "fr", "it", "es"].includes(country)) return "EUR";
  return "USD";
}

function makeFormatter(currency: string) {
  return (value: number, type: MetricFormat) => {
    if (!Number.isFinite(value)) return "—";
    if (type === "currency") {
      const abs = Math.abs(value);
      const maximumFractionDigits = abs >= 1000 ? 0 : 2;
      return new Intl.NumberFormat("en-US", { style: "currency", currency, maximumFractionDigits }).format(value);
    }
    if (type === "percent") return `${value.toFixed(2)}%`;
    if (type === "decimal") return value.toFixed(2);
    return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
  };
}


const ACTION_THRESHOLDS = {
  tacosPercent: 20,
  promotionalRebatePercent: 5,
  returnRatePercent: 3,
  minUnitsForReturnSignal: 50,
  amazonFeePercent: 55,
  minSalesForFeeSignal: 100,
  inventoryCoverageMonths: 1.25,
  dispatchRatePercent: 95,
};

// Temporary Action Items demo rows.
// These are intentionally shaped like the future backend response so the UI can
// switch to live data without changing the action-item rendering logic.
export const DUMMY_ACTION_ITEM_DATA: MonthlyMetricRow[] = [
  {
    sku: "TOTAL", product_name: "TOTAL", month: "july", year: 2026, country: "us",
    net_sales: 135932.50, quantity: 8846, return_quantity: 160,
    profit: 77210.59, profit_percentage: 56.80,
    promotional_rebates: -8011.01, promotional_rebates_percentage: -5.89,
    amazon_fees: 61441.08, total_ads: 33808.30, advertising_total: 33808.30, ads_spend: 33808.30,
    tacos_total_advertising_cost_of_sale: 24.87,
    inventory_units: 18240, inventory_coverage_months: 0.82, stockout_skus: 6, low_stock_skus: 23,
    aged_inventory_percent: 14.6, inbound_units: 9500, dispatched_units: 8683, dispatch_rate: 91.40,
  },
  {
    sku: "SEMNIW1", product_name: "Classic", month: "july", year: 2026, country: "us",
    net_sales: 48917.05, quantity: 3744, return_quantity: 74, profit: 27555.36, profit_percentage: 56.33,
    promotional_rebates: -3126.87, amazon_fees: 23116.63,
  },
  {
    sku: "SEIWHCWI", product_name: "Classic + Wipes", month: "july", year: 2026, country: "us",
    net_sales: 2357.93, quantity: 177, return_quantity: 8, profit: 895.39, profit_percentage: 37.97,
    promotional_rebates: -86.26, amazon_fees: 1377.45,
  },
  {
    sku: "SEWIPESLIDCO", product_name: "Wipes Lid", month: "july", year: 2026, country: "us",
    net_sales: 1005.97, quantity: 70, return_quantity: 4, profit: 459.04, profit_percentage: 45.63,
    promotional_rebates: -58.97, amazon_fees: 556.94,
  },
  {
    sku: "SEFMTM", product_name: "Turmeric", month: "july", year: 2026, country: "us",
    net_sales: 210.76, quantity: 24, return_quantity: 0, profit: -45.16, profit_percentage: -21.43,
    promotional_rebates: -18.20, amazon_fees: 126.16,
  },
  {
    sku: "SEWMNIW", product_name: "Women", month: "july", year: 2026, country: "us",
    net_sales: 334.09, quantity: 29, return_quantity: 0, profit: -31.23, profit_percentage: -9.35,
    promotional_rebates: -21.82, amazon_fees: 205.60,
  },
];

function firstNumber(row: MonthlyMetricRow | undefined, ...fields: string[]) {
  if (!row) return null;
  let zeroFallback: number | null = null;
  for (const field of fields) {
    const value = toNumber(row[field]);
    if (value === null) continue;
    if (value !== 0) return value;
    zeroFallback = 0;
  }
  return zeroFallback;
}

function latestActionRows(rows: MonthlyMetricRow[]) {
  const dated = rows
    .map((row) => ({ row, info: monthInfo(row) }))
    .filter((entry): entry is { row: MonthlyMetricRow; info: NonNullable<ReturnType<typeof monthInfo>> } => Boolean(entry.info));

  if (!dated.length) return rows;
  const latestSortKey = Math.max(...dated.map((entry) => entry.info.sortKey));
  return dated.filter((entry) => entry.info.sortKey === latestSortKey).map((entry) => entry.row);
}

function compactCurrency(value: number, currency: string) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency,
    notation: Math.abs(value) >= 1000 ? "compact" : "standard",
    maximumFractionDigits: Math.abs(value) >= 1000 ? 1 : 0,
  }).format(value);
}

function buildActionItems(rows: MonthlyMetricRow[], currencyOverride?: string): ActionItem[] {
  const currentRows = latestActionRows(rows);
  if (!currentRows.length) return [];

  const totalRow = currentRows.find(isTotalRow);
  const skuRows = currentRows.filter((row) => !isTotalRow(row));
  const country = String(totalRow?.country ?? currentRows[0]?.country ?? "").toLowerCase();
  const currency = currencyOverride ?? currencyFromCountry(country);
  const items: ActionItem[] = [];

  const sumSkuField = (...fields: string[]) => skuRows.reduce((sum, row) => sum + (firstNumber(row, ...fields) ?? 0), 0);

  const netSales = firstNumber(totalRow, "net_sales") ?? sumSkuField("net_sales");

  const inventoryUnits = firstNumber(totalRow, "inventory_units", "available_inventory_units") ?? sumSkuField("inventory_units", "available_inventory_units");
  const inventoryCoverage = firstNumber(totalRow, "inventory_coverage_months", "inventory_coverage") ?? 0;
  const stockoutSkus = firstNumber(totalRow, "stockout_skus") ?? 0;
  const lowStockSkus = firstNumber(totalRow, "low_stock_skus") ?? 0;

  if (inventoryUnits > 0 && (inventoryCoverage < ACTION_THRESHOLDS.inventoryCoverageMonths || stockoutSkus > 0 || lowStockSkus > 0)) {
    items.push({
      id: "inventory-coverage-risk",
      category: "Inventory & Dispatch",
      priority: stockoutSkus > 0 || inventoryCoverage < 1 ? "Critical" : "High",
      title: "Inventory coverage needs action",
      reason: `Coverage is ${inventoryCoverage.toFixed(2)} months with ${stockoutSkus} stockout SKU${stockoutSkus === 1 ? "" : "s"} and ${lowStockSkus} low-stock SKU${lowStockSkus === 1 ? "" : "s"}.`,
      metrics: [
        { value: `${inventoryCoverage.toFixed(2)} mo`, label: "Coverage" },
        { value: String(stockoutSkus), label: "Stockout SKUs" },
        { value: String(lowStockSkus), label: "Low-stock SKUs" },
      ],
      action: "Plan replenishment",
    });
  }

  const dispatchRate = firstNumber(totalRow, "dispatch_rate") ?? 0;
  const inboundUnits = firstNumber(totalRow, "inbound_units") ?? sumSkuField("inbound_units");
  const dispatchedUnits = firstNumber(totalRow, "dispatched_units", "dispatch_units") ?? sumSkuField("dispatched_units", "dispatch_units");

  if (dispatchRate > 0 && dispatchRate < ACTION_THRESHOLDS.dispatchRatePercent) {
    items.push({
      id: "dispatch-performance",
      category: "Inventory & Dispatch",
      priority: dispatchRate < 90 ? "High" : "Medium",
      title: "Dispatch performance below target",
      reason: `Dispatch rate is ${dispatchRate.toFixed(2)}%, below the ${ACTION_THRESHOLDS.dispatchRatePercent}% operating target for the latest month.`,
      metrics: [
        { value: `${dispatchRate.toFixed(2)}%`, label: "Dispatch rate" },
        { value: new Intl.NumberFormat("en-US").format(dispatchedUnits), label: "Dispatched" },
        { value: new Intl.NumberFormat("en-US").format(inboundUnits), label: "Inbound" },
      ],
      action: "Review dispatch gaps",
    });
  }
  const explicitAdsSpend = firstNumber(totalRow, "total_ads", "advertising_fees", "advertising_total", "ads_spend");
  const componentAdsSpend = (firstNumber(totalRow, "visible_ads") ?? 0)
    + (firstNumber(totalRow, "dealsvouchar_ads") ?? 0)
    + (firstNumber(totalRow, "brand_spend") ?? 0);
  const adsSpend = explicitAdsSpend ?? (componentAdsSpend || sumSkuField("ads_spend", "advertising_total"));
  const tacos = firstNumber(totalRow, "tacos_total_advertising_cost_of_sale") ?? safePercent(adsSpend, netSales);

  if (adsSpend > 0 && tacos >= ACTION_THRESHOLDS.tacosPercent) {
    items.push({
      id: "ads-efficiency",
      category: "Ads",
      priority: tacos >= 25 ? "Critical" : "High",
      title: "Advertising efficiency needs attention",
      reason: `TACoS is ${tacos.toFixed(2)}% on the latest month, so ad spend is taking a high share of net sales.`,
      metrics: [
        { value: `${tacos.toFixed(2)}%`, label: "TACoS" },
        { value: compactCurrency(Math.abs(adsSpend), currency), label: "Ad spend" },
        { value: compactCurrency(Math.abs(netSales), currency), label: "Net sales" },
      ],
      action: "Optimize campaigns",
    });
  }

  const rebateAmountRaw = firstNumber(totalRow, "promotional_rebates") ?? sumSkuField("promotional_rebates");
  const rebateAmount = Math.abs(rebateAmountRaw);
  const rebatePercent = Math.abs(firstNumber(totalRow, "promotional_rebates_percentage") ?? safePercent(rebateAmount, netSales));
  const topRebateSku = [...skuRows]
    .map((row) => ({ row, amount: Math.abs(firstNumber(row, "promotional_rebates") ?? 0) }))
    .sort((a, b) => b.amount - a.amount)[0];

  if (rebateAmount > 0 && rebatePercent >= ACTION_THRESHOLDS.promotionalRebatePercent) {
    items.push({
      id: "promotional-rebates",
      category: "Finance",
      priority: rebatePercent >= 8 ? "High" : "Medium",
      title: "Promotional rebate leakage",
      reason: `Promotional rebates are ${rebatePercent.toFixed(2)}% of net sales. Review the biggest rebate-heavy SKUs first.`,
      metrics: [
        { value: compactCurrency(rebateAmount, currency), label: "Rebates" },
        { value: `${rebatePercent.toFixed(2)}%`, label: "of net sales" },
        { value: String(topRebateSku?.row.sku ?? "—"), label: "Top rebate SKU" },
      ],
      action: "Review promotions",
    });
  }

  const negativeProfitSkus = skuRows
    .map((row) => ({
      row,
      profit: firstNumber(row, "profit") ?? 0,
      margin: firstNumber(row, "profit_percentage", "cm1_profit_per") ?? 0,
    }))
    .filter((entry) => entry.profit < 0)
    .sort((a, b) => a.profit - b.profit);

  if (negativeProfitSkus.length) {
    const totalLoss = Math.abs(negativeProfitSkus.reduce((sum, entry) => sum + entry.profit, 0));
    const worst = negativeProfitSkus[0];
    items.push({
      id: "negative-profit-skus",
      category: "Finance",
      priority: negativeProfitSkus.length >= 5 || totalLoss >= 1000 ? "High" : "Medium",
      title: "Negative-profit SKUs",
      reason: `${negativeProfitSkus.length} SKU${negativeProfitSkus.length === 1 ? " is" : "s are"} currently below zero profit and should be reviewed before scaling sales.`,
      metrics: [
        { value: String(negativeProfitSkus.length), label: "SKUs" },
        { value: compactCurrency(totalLoss, currency), label: "Total loss" },
        { value: `${worst.margin.toFixed(2)}%`, label: "Worst margin" },
      ],
      action: "Reprice / pause",
    });
  }

  const highReturnSkus = skuRows
    .map((row) => {
      const quantity = firstNumber(row, "quantity") ?? 0;
      const returns = firstNumber(row, "return_quantity") ?? 0;
      return { row, quantity, returns, rate: safePercent(returns, quantity) };
    })
    .filter((entry) => entry.quantity >= ACTION_THRESHOLDS.minUnitsForReturnSignal && entry.rate >= ACTION_THRESHOLDS.returnRatePercent)
    .sort((a, b) => b.rate - a.rate);

  if (highReturnSkus.length) {
    const worst = highReturnSkus[0];
    items.push({
      id: "high-return-rate",
      category: "Returns",
      priority: worst.rate >= 5 ? "High" : "Medium",
      title: "High return-rate SKUs",
      reason: `${highReturnSkus.length} SKU${highReturnSkus.length === 1 ? " has" : "s have"} elevated returns with enough sales volume to warrant investigation.`,
      metrics: [
        { value: String(highReturnSkus.length), label: "SKUs" },
        { value: `${worst.rate.toFixed(2)}%`, label: "Max return rate" },
        { value: String(worst.row.sku ?? "—"), label: "Highest SKU" },
      ],
      action: "Inspect root cause",
    });
  }

  const highFeeSkus = skuRows
    .map((row) => {
      const sales = firstNumber(row, "net_sales") ?? 0;
      const fees = Math.abs(firstNumber(row, "amazon_fees", "amazon_fee", "marketplace_fees") ?? 0);
      return { row, sales, fees, rate: safePercent(fees, sales) };
    })
    .filter((entry) => entry.sales >= ACTION_THRESHOLDS.minSalesForFeeSignal && entry.rate >= ACTION_THRESHOLDS.amazonFeePercent)
    .sort((a, b) => b.rate - a.rate);

  if (highFeeSkus.length) {
    const worst = highFeeSkus[0];
    const impactedFees = highFeeSkus.reduce((sum, entry) => sum + entry.fees, 0);
    items.push({
      id: "amazon-fee-pressure",
      category: "Finance",
      priority: worst.rate >= 60 ? "High" : "Medium",
      title: "Amazon fee pressure",
      reason: `${highFeeSkus.length} SKU${highFeeSkus.length === 1 ? " has" : "s have"} Amazon fees at or above ${ACTION_THRESHOLDS.amazonFeePercent}% of net sales.`,
      metrics: [
        { value: String(highFeeSkus.length), label: "SKUs" },
        { value: `${worst.rate.toFixed(2)}%`, label: "Max fee rate" },
        { value: compactCurrency(impactedFees, currency), label: "Fees on flagged SKUs" },
      ],
      action: "Review fee drivers",
    });
  }

  const priorityOrder: Record<Priority, number> = { Critical: 0, High: 1, Medium: 2, Opportunity: 3 };
  return items.sort((a, b) => priorityOrder[a.priority] - priorityOrder[b.priority]).slice(0, 7);
}

function relationText(label: string, part: number, whole: number, format: (value: number, type: MetricFormat) => string) {
  return `${label}: ${format(safePercent(part, whole), "percent")} of net sales.`;
}

const metricDefinitions: MetricDefinition[] = [
  {
    key: "totalQuantity",
    title: "Units Sold",
    category: "Revenue & Demand",
    icon: "box",
    format: "number",
    detail: (s, f) => `${f(s.values.returnQuantity, "number")} units were returned this month.`,
  },
  {
    key: "netSales",
    title: "Net Sales",
    category: "Revenue & Demand",
    icon: "trend",
    format: "currency",
    detail: (s, f) => `Net sales after refunds, credits and promotions are ${f(s.values.netSales, "currency")}.`,
  },
  {
    key: "asp",
    title: "ASP",
    category: "Revenue & Demand",
    icon: "spark",
    format: "currency",
    detail: (s, f) => `Average selling price is ${f(s.values.asp, "currency")} per unit.`,
  },
  {
    key: "cm2Profit",
    title: "CM2 Profit",
    category: "Profitability",
    icon: "coin",
    format: "currency",
    detail: (s, f) => `CM2 margin is ${f(s.values.cm2Margin, "percent")} on current net sales.`,
  },
  {
    key: "adsSpend",
    title: "Ads",
    category: "Advertising & Promotion",
    icon: "megaphone",
    format: "currency",
    inverseTrend: true,
    detail: (s, f) => `Ad spend is ${f(s.values.tacos, "percent")} of net sales (TACoS). Lower ad spend versus the previous month is treated as favorable.`,
  },
  {
    key: "tacos",
    title: "TACoS",
    category: "Advertising & Promotion",
    icon: "target",
    format: "percent",
    inverseTrend: true,
    detail: (s, f) => `Total advertising cost is ${f(s.values.tacos, "percent")} of net sales. Lower TACoS is better.`,
  },
  {
    key: "platformFee",
    title: "Other",
    category: "Fees & Other Costs",
    icon: "coin",
    format: "currency",
    detail: (s, f) => `Other Transactions are ${f(safePercent(Math.abs(s.values.platformFee), s.values.netSales), "percent")} of net sales.`,
  },
  {
    key: "promotionalRebates",
    title: "Promotional Rebate",
    category: "Advertising & Promotion",
    icon: "spark",
    format: "currency",
    detail: (s, f) => `Promotional rebate is ${f(Math.abs(s.values.rebatePercent), "percent")} of net sales.`,
  },
];

function Sparkline({ values, status }: { values: number[]; status: MovementStatus }) {
  const width = 180;
  const height = 44;
  const raw = values.filter((value) => Number.isFinite(value));
  const startValue = raw[0] ?? 0;
  const endValue = raw[raw.length - 1] ?? startValue;

  // With only current + previous month, a normal sparkline is just one straight segment.
  // Build a small deterministic mini-trend so it looks like the KPI-card sparklines while
  // still preserving the real overall direction. Stable stays visually sideways.
  const makeMiniTrend = () => {
    if (raw.length > 2) return raw;

    const count = 7;
    const scale = Math.max(Math.abs(startValue), Math.abs(endValue), 1);
    const realMove = endValue - startValue;
    const wiggle = Math.max(Math.abs(realMove) * 0.28, scale * 0.018);
    const offsets = [0, 0.55, -0.28, 0.42, -0.18, 0.32, 0];

    if (status === "stable") {
      const center = (startValue + endValue) / 2;
      return offsets.map((offset) => center + offset * wiggle);
    }

    return offsets.map((offset, index) => {
      const progress = index / (count - 1);
      const base = startValue + realMove * progress;
      return base + offset * wiggle;
    });
  };

  const usable = makeMiniTrend();
  const min = Math.min(...usable);
  const max = Math.max(...usable);
  const visualPadding = Math.max((max - min) * 0.18, Math.max(Math.abs(max), 1) * 0.01);
  const low = min - visualPadding;
  const high = max + visualPadding;
  const range = high - low || 1;

  const pointList = usable.map((value, index) => {
    const x = (index / Math.max(usable.length - 1, 1)) * width;
    const y = height - 4 - ((value - low) / range) * (height - 8);
    return { x, y };
  });

  const points = pointList.map(({ x, y }) => `${x},${y}`).join(" ");
  const last = pointList[pointList.length - 1];

  const areaPoints = `0,${height} ${points} ${width},${height}`;

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="h-10 w-full" preserveAspectRatio="none" aria-hidden="true">
      <polygon points={areaPoints} fill="currentColor" opacity="0.055" />
      <polyline
        points={points}
        fill="none"
        stroke="currentColor"
        strokeWidth="5"
        strokeLinecap="round"
        strokeLinejoin="round"
        opacity="0.07"
      />
      <polyline
        points={points}
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      {last ? <circle cx={last.x} cy={last.y} r="2.5" fill="white" stroke="currentColor" strokeWidth="1.6" /> : null}
    </svg>
  );
}

function MetricFlipCard({ definition, snapshots, currency }: { definition: MetricDefinition; snapshots: MonthSnapshot[]; currency: string }) {
  const format = makeFormatter(currency);
  const current = snapshots[snapshots.length - 1];
  const previous = snapshots[snapshots.length - 2];
  const currentValue = current.values[definition.key] ?? 0;
  const previousValue = previous?.values[definition.key];
  const delta = percentageChange(currentValue, previousValue);
  const rawStatus = movementStatus(delta);
  const status: MovementStatus = definition.inverseTrend
    ? rawStatus === "up"
      ? "down"
      : rawStatus === "down"
        ? "up"
        : "stable"
    : rawStatus;
  const theme = movementTheme[status];
  const history = snapshots.map((snapshot) => snapshot.values[definition.key] ?? 0);
  const deltaText = delta === null ? "No previous-month baseline" : `${delta >= 0 ? "+" : ""}${delta.toFixed(2)}%`;
  const movementPoint = delta === null
    ? "Add at least one previous month to activate MoM comparison."
    : `${definition.title} ${Math.abs(delta) < 0.005 ? "was flat" : delta > 0 ? "increased" : "decreased"} by ${Math.abs(delta).toFixed(2)}% month over month.${definition.inverseTrend && Math.abs(delta) >= 0.005 ? (delta > 0 ? " Higher is unfavorable." : " Lower is favorable.") : ""}`;

  return <div className="group h-[158px] [perspective:1400px] transition-[transform,filter] duration-500 ease-out hover:-translate-y-1.5 hover:scale-[1.012] focus-within:-translate-y-1.5 focus-within:scale-[1.012]">
    <div className="relative h-full w-full transition-transform duration-1000 ease-[cubic-bezier(0.22,1,0.36,1)] [transform-style:preserve-3d] group-hover:[transform:rotateY(180deg)_rotateX(2deg)_translateZ(8px)] group-focus-within:[transform:rotateY(180deg)_rotateX(2deg)_translateZ(8px)]">
      <article className={`absolute inset-0 flex h-full flex-col overflow-hidden rounded-[16px] border ${theme.border} bg-white p-3.5 shadow-[0_2px_7px_rgba(16,35,58,0.10)] transition-[box-shadow,border-color] duration-500 group-hover:shadow-[0_16px_34px_rgba(16,35,58,0.16)] [backface-visibility:hidden] [transform:translateZ(1px)]`}>
        <span className={`pointer-events-none absolute -right-8 -top-8 h-20 w-20 rounded-full ${theme.softBg} opacity-75 blur-2xl transition-transform duration-1000 group-hover:scale-125`} />

        <div className="relative z-10 flex items-start justify-between gap-3">
          <div className="flex min-w-0 items-center gap-2.5">
            <span className={`flex h-8 w-8 shrink-0 items-center justify-center rounded-full ${theme.softBg} ${theme.text}`}><Icon name={definition.icon} className="h-4 w-4" /></span>
            <div>
              <h3 className="text-sm font-semibold leading-4 text-[#17304F]">{definition.title}</h3>
              <p className="mt-0.5 text-[9px] text-[#738299]">{definition.category}</p>
            </div>
          </div>
          <span className={`rounded-full px-2 py-1 text-[9px] font-semibold ${theme.softBg} ${theme.text}`}>{theme.label}</span>
        </div>

        <div className=" z-10 mt-8 flex min-h-0  items-start justify-between gap-4">
          <div className="min-w-0 shrink-0">
            <div className="min-[1700px]:text-[24px] text-sm font-semibold tracking-[-0.025em] text-charcoal-500">{format(currentValue, definition.format)}</div>
            <div className=" flex items-center gap-1.5">
              <span className={`text-[12px] font-semibold ${theme.text}`}>{deltaText}</span>
            </div>
          </div>

          <div className={`w-[42%] min-w-[105px] max-w-[145px]   ${theme.text}`}>
            <Sparkline values={history} status={status} />
          </div>
        </div>

        {/* <div className="relative z-10 mt-1 flex items-center justify-between text-[8px] text-[#8793A4]">
          <span>{previous?.label ?? current.label}</span>
          <span>Hover for analysis</span>
          <span>{current.label}</span>
        </div> */}
      </article>

      <article className={`absolute inset-0 h-full overflow-hidden rounded-[16px] border ${theme.border} bg-white p-3.5 shadow-[0_16px_34px_rgba(16,35,58,0.16)] [backface-visibility:hidden] [transform:rotateY(180deg)_translateZ(1px)]`}>
        <span className={`pointer-events-none absolute -right-8 -top-8 h-20 w-20 rounded-full ${theme.softBg} opacity-75 blur-2xl`} />
        <div className="relative z-10 flex items-center gap-2">
          <span className={`flex h-7 w-7 items-center justify-center rounded-full ${theme.softBg} ${theme.text}`}><Icon name="spark" className="h-3.5 w-3.5" /></span>
          <div>
            <h3 className="text-[11px] font-semibold text-[#17304F]">{definition.title} analysis</h3>
            <p className="text-[8px] text-[#75849A]">{current.label} business context</p>
          </div>
        </div>
        <ul className="relative z-10 mt-3 space-y-1.5 text-[9px] leading-[13px] text-[#40536C]">
          <li className="flex gap-2"><span className={`mt-1 h-1.5 w-1.5 shrink-0 rounded-full ${theme.bg}`} /><span>Current: <strong className="text-charcoal-500">{format(currentValue, definition.format)}</strong>{previous ? <> · Previous: <strong className="text-charcoal-500">{format(previousValue ?? 0, definition.format)}</strong></> : null}</span></li>
          <li className="flex gap-2"><span className={`mt-1 h-1.5 w-1.5 shrink-0 rounded-full ${theme.bg}`} /><span>{movementPoint}</span></li>
          <li className="flex gap-2"><span className={`mt-1 h-1.5 w-1.5 shrink-0 rounded-full ${theme.bg}`} /><span>{definition.detail(current, format)}</span></li>
        </ul>
      </article>
    </div>
  </div>;
}


function ActionItemsView({ rows, currency, isDemo = false }: { rows: MonthlyMetricRow[]; currency?: string; isDemo?: boolean }) {
  const [category, setCategory] = useState<"All" | Category | "Completed">("All");
  const [completed, setCompleted] = useState<Set<string>>(new Set());
  const actionItems = useMemo(() => buildActionItems(rows, currency), [rows, currency]);

  const visible = useMemo(() => {
    if (category === "Completed") return actionItems.filter((item) => completed.has(item.id));
    return actionItems.filter((item) => !completed.has(item.id) && (category === "All" || item.category === category));
  }, [actionItems, category, completed]);

  const filters: { label: "All" | Category | "Completed"; icon: IconName }[] = [
    { label: "All", icon: "clipboard" },
    { label: "Inventory & Dispatch", icon: "box" },
    { label: "Ads", icon: "megaphone" },
    { label: "Finance", icon: "coin" },
    { label: "Returns", icon: "box" },
    { label: "Completed", icon: "check" },
  ];

  return <div className="w-full space-y-3">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div className="flex flex-wrap gap-2">
        {filters.map((filter) => <button
          key={filter.label}
          onClick={() => setCategory(filter.label)}
          className={`inline-flex h-9 items-center gap-2 rounded-lg border px-3 text-[12px] font-medium transition ${category === filter.label ? "border-green-500 bg-[#EDF8F5] text-[#087D68]" : "border-[#DDE5E8] bg-white text-[#263750] hover:border-[#AFC9C3]"}`}
        >
          <Icon name={filter.icon} className="h-4 w-4" />{filter.label}
        </button>)}
      </div>
      <div className="flex items-center gap-2 text-[11px] font-medium text-[#6B7A90]">
        {isDemo ? <span className="rounded-full bg-[#FFF7E8] px-2.5 py-1 font-semibold text-[#A16A00]">Demo data</span> : null}
        <span>{actionItems.length - completed.size} open action{actionItems.length - completed.size === 1 ? "" : "s"}</span>
      </div>
    </div>

    {visible.length === 0 ? <div className="rounded-xl border border-dashed border-[#CFE0DC] bg-[#F8FCFB] px-6 py-14 text-center">
      <div className="mx-auto mb-3 flex h-10 w-10 items-center justify-center rounded-full bg-[#E8F6F2] text-green-500"><Icon name="check" className="h-5 w-5" /></div>
      <p className="font-semibold text-[#17304F]">{rows.length ? "No action items in this view" : "SKU-wise data is not loaded"}</p>
      <p className="mt-1 text-sm text-[#728096]">{rows.length ? "Only data-backed signals are shown here." : "Pass the same monthly SKU rows used by Business Analysis."}</p>
    </div> : <div className="overflow-hidden rounded-xl border border-[#DDE5E8] bg-white shadow-[0_1px_2px_rgba(16,35,58,0.03)]">
      <div className="divide-y divide-[#E7ECEF]">
        {visible.map((item) => <div key={item.id} className="grid items-center gap-3 px-4 py-3 text-[12px] lg:grid-cols-[92px_minmax(240px,1fr)_minmax(260px,320px)_145px_28px]">
          <span className={`inline-flex w-fit min-w-[78px] justify-center rounded border px-2 py-1 text-[10px] font-semibold ${priorityClass[item.priority]}`}>{item.priority}</span>

          <div className="min-w-0">
            <div className="flex items-center gap-2.5">
              <span className="shrink-0 text-[#38506F]"><Icon name={categoryIcon[item.category]} className="h-4 w-4" /></span>
              <span className="font-semibold text-[#1B2D49]">{item.title}</span>
              <span className="rounded-full bg-[#F3F6F8] px-2 py-0.5 text-[9px] font-medium text-[#66768C]">{item.category}</span>
            </div>
            <p className="mt-1 line-clamp-2 pl-[26px] text-[11px] leading-4 text-[#65758B]">{item.reason}</p>
          </div>

          <div className="grid grid-cols-3 gap-2 rounded-lg bg-[#FAFBFC] px-2 py-2">
            {item.metrics.map((metric) => <div key={metric.label} className="min-w-0 text-center">
              <div className="truncate font-semibold text-[#1B2D49]" title={metric.value}>{metric.value}</div>
              <div className="mt-0.5 truncate text-[9px] text-[#74839A]" title={metric.label}>{metric.label}</div>
            </div>)}
          </div>

          <span className="inline-flex min-h-8 items-center justify-center rounded-md border border-[#9FD1C6] bg-[#F8FCFB] px-2 text-center text-[10px] font-semibold text-[#07836C]">{item.action}</span>

          <input
            aria-label={`Complete ${item.title}`}
            type="checkbox"
            checked={completed.has(item.id)}
            onChange={() => setCompleted((prev) => {
              const next = new Set(prev);
              next.has(item.id) ? next.delete(item.id) : next.add(item.id);
              return next;
            })}
            className="h-4 w-4 rounded border-[#BFCBD5] accent-green-500"
          />
        </div>)}
      </div>
    </div>}
  </div>;
}

export function BusinessAnalysisView({
  monthlyData,
  currency,
  useDummyFallback,
  loading,
  skuAnalysisContent,
}: {
  monthlyData: MonthlyMetricRow[];
  currency?: string;
  useDummyFallback: boolean;
  loading: boolean;
  skuAnalysisContent?: React.ReactNode;
}) {
  const usingDummyData = useDummyFallback && monthlyData.length === 0;
  const effectiveData = usingDummyData ? DUMMY_MONTHLY_DATA : monthlyData;
  const snapshots = useMemo(() => buildMonthlySnapshots(effectiveData), [effectiveData]);

  if (loading && !snapshots.length) {
    return <div className="rounded-xl border border-[#DDE5E8] bg-white px-6 py-16 text-center"><div className="mx-auto mb-3 h-8 w-8 animate-spin rounded-full border-2 border-[#DDE5E8] border-t-[#2878B8]" /><h2 className="text-sm font-semibold text-[#17304F]">Loading Business Analysis...</h2></div>;
  }

  if (!snapshots.length) {
    return <div className="rounded-xl border border-dashed border-[#B8D7EE] bg-[#F6FAFD] px-6 py-16 text-center"><div className="mx-auto mb-3 flex h-11 w-11 items-center justify-center rounded-full bg-[#EAF4FB] text-[#2878B8]"><Icon name="trend" className="h-5 w-5" /></div><h2 className="text-base font-semibold text-[#17304F]">Business Analysis data is not available yet</h2></div>;
  }

  const current = snapshots[snapshots.length - 1];
  const previous = snapshots[snapshots.length - 2];
  const resolvedCurrency = currency ?? currencyFromCountry(current.country);

  return <div className="min-w-0 space-y-5 mt-4">
    <div className="flex flex-wrap items-center justify-between gap-4">
      <div className="flex items-center gap-3">
        <span className="flex h-11 w-11 items-center justify-center rounded-full border border-[#DDE5E8] bg-white text-[#2878B8]"><Icon name="trend" className="h-6 w-6" /></span>
        <div>
          <h2 className="min-[1700px]:text-2xl text-lg font-semibold text-[#17304F]">Business Analysis</h2>
          <p className="min-[1700px]:text-sm text-xs text-[#50627A]">Key business metrics and month-over-month movement.</p>
        </div>
      </div>
      <div className="flex items-center gap-2 text-[10px] text-[#607188]">
        <span className="rounded-full border border-[#DDE5E8] bg-white px-3 py-1.5">{current.label}</span>
        <span className="text-[#9AA7B7]">vs</span>
        <span className="rounded-full border border-[#DDE5E8] bg-white px-3 py-1.5">{previous?.label ?? "Previous month"}</span>
        {usingDummyData ? <span className="rounded-full bg-[#FFF7E8] px-3 py-1.5 font-semibold text-[#A16A00]">Demo data</span> : null}
      </div>
    </div>

    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
      {metricDefinitions.map((definition) => <MetricFlipCard key={definition.key} definition={definition} snapshots={snapshots} currency={resolvedCurrency} />)}
    </div>

    {skuAnalysisContent ? <div className="pt-1">{skuAnalysisContent}</div> : null}
  </div>;
}

export default function DashboardActionItemsTab({
  monthlyData = [],
  currency,
  useDummyFallback = true,
}: {
  monthlyData?: MonthlyMetricRow[];
  currency?: string;
  useDummyFallback?: boolean;
}) {
  const usingDummyData = useDummyFallback && monthlyData.length === 0;
  const effectiveRows = usingDummyData ? DUMMY_ACTION_ITEM_DATA : monthlyData;

  return <div className="w-full px-0 py-4">
    <ActionItemsView rows={effectiveRows} currency={currency} isDemo={usingDummyData} />
  </div>;
}
