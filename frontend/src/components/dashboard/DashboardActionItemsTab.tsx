"use client";

import React, { useId, useMemo, useState } from "react";
import Loader from "@/components/loader/Loader";
import PageBreadcrumb from "../common/PageBreadCrumb";

export type ActionItemPriority = "Critical" | "High" | "Medium" | "Opportunity";
export type ActionItemCategory = "Inventory & Dispatch" | "Ads" | "Finance" | "Returns";

export type DashboardActionItem = {
  id: string;
  category: ActionItemCategory;
  priority: ActionItemPriority;
  title: string;
  reason: string;
  metrics: { value: string; label: string }[];
  action: string;
  affected_skus?: string[];
};

type Priority = ActionItemPriority;
type Category = ActionItemCategory;
type ActionItem = DashboardActionItem;

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
type MetricCategory = "Revenue & Demand" | "Profitability" | "Advertising & Promotion" | "Fees & Other Costs" | "Inventory & Dispatch";

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
  const agedInventory181To270 = valueFromTotalOrSum("aged_inventory_181_270");
  const agedInventory271To365 = valueFromTotalOrSum("aged_inventory_271_365");
  const agedInventory365Plus = valueFromTotalOrSum("aged_inventory_365_plus");
  const agedInventory180Plus = valueFromTotalOrSum("aged_inventory_180_plus");
  const grossSales = valueFromTotalOrSum("gross_sales");
  const refundSales = Math.abs(valueFromTotalOrSum("refund_sales", "refunded_sales"));
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
      refundSales,
      netSales,
      quantity,
      totalQuantity,
      agedInventory181To270,
      agedInventory271To365,
      agedInventory365Plus,
      agedInventory180Plus,
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
      lostTotal: Math.abs(valueFromTotalOrSum("lost_total")),
      miscTransaction: Math.abs(valueFromTotalOrSum("misc_transaction", "misc_transactions")),
      platformInventoryStorageFee: Math.abs(valueFromTotalOrSum("platform_fee_inventory_storage")),
      subscriptionFees: Math.abs(valueFromTotalOrSum("platformfeenew", "platform_fee_new")),
      platformFee: valueFromTotalOrSum("platform_fee"),
      inventoryStorageFee: valueFromTotalOrSum(
        "storage_fee",
        "platform_fee_inventory_storage",
        "inventory_storage_fees"
      ),
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

function unitsFromRow(row: MonthlyMetricRow) {
  for (const field of ["total_quantity", "quantity"]) {
    const value = toNumber(row[field]);
    if (value !== null) return value;
  }
  return 0;
}

type ContributorMetric = "units" | "netSales" | "asp";

function contributorValue(row: MonthlyMetricRow, metric: ContributorMetric) {
  if (metric === "units") return unitsFromRow(row);
  if (metric === "netSales") return toNumber(row.net_sales) ?? 0;
  return toNumber(row.asp) ?? 0;
}

function buildProductContributorInsight(
  rows: MonthlyMetricRow[],
  metric: ContributorMetric,
  displayedOverallDelta?: number
) {
  const groups = new Map<string, { sortKey: number; rows: MonthlyMetricRow[] }>();

  rows.forEach((row) => {
    const info = monthInfo(row);
    if (!info || isTotalRow(row)) return;
    const group = groups.get(info.key) ?? { sortKey: info.sortKey, rows: [] };
    group.rows.push(row);
    groups.set(info.key, group);
  });

  const periods = [...groups.values()].sort((a, b) => a.sortKey - b.sortKey);
  const currentPeriod = periods[periods.length - 1];
  const previousPeriod = periods[periods.length - 2];

  if (!currentPeriod || !previousPeriod) {
    return "No previous-month SKU baseline is available.";
  }

  type ContributorEntry = { sku: string; productName: string; value: number };
  const aggregateValues = (periodRows: MonthlyMetricRow[]) => {
    const result = new Map<string, ContributorEntry>();

    periodRows.forEach((row) => {
      const sku = String(row.sku ?? row.SKU ?? "").trim();
      const productName = String(
        row.product_name ?? row["Product Name"] ?? ""
      ).trim();
      if (!sku && !productName) return;

      const key = sku
        ? `sku:${sku.toUpperCase()}`
        : `product:${productName.toLowerCase()}`;
      const current = result.get(key);
      result.set(key, {
        sku: sku || current?.sku || "",
        productName: productName || current?.productName || "",
        value: (current?.value ?? 0) + contributorValue(row, metric),
      });
    });

    return result;
  };

  const currentValues = aggregateValues(currentPeriod.rows);
  const previousValues = aggregateValues(previousPeriod.rows);
  const allKeys = new Set([...currentValues.keys(), ...previousValues.keys()]);
  const movements = [...allKeys]
    .map((key) => {
      const current = currentValues.get(key);
      const previous = previousValues.get(key);
      const currentValue = current?.value ?? 0;
      const previousValue = previous?.value ?? 0;
      const delta = currentValue - previousValue;

      return {
        label: (
          current?.productName
          || previous?.productName
          || current?.sku
          || previous?.sku
          || "Product"
        ),
        delta,
        percentage: previousValue
          ? (delta / Math.abs(previousValue)) * 100
          : null,
      };
    })
    .filter((item) => item.delta !== 0);

  const currentTotal = [...currentValues.values()].reduce((sum, item) => sum + item.value, 0);
  const previousTotal = [...previousValues.values()].reduce((sum, item) => sum + item.value, 0);
  const overallDelta = Number.isFinite(displayedOverallDelta)
    ? Number(displayedOverallDelta)
    : currentTotal - previousTotal;
  const direction = overallDelta < 0 ? "decline" : overallDelta > 0 ? "growth" : "movement";
  const directionalMovements = movements.filter((item) =>
    direction === "decline"
      ? item.delta < 0
      : direction === "growth"
        ? item.delta > 0
        : true
  );
  const candidates = directionalMovements.length ? directionalMovements : movements;
  const rankedContributors = [...candidates]
    .sort((a, b) =>
      direction === "decline"
        ? a.delta - b.delta
        : direction === "growth"
          ? b.delta - a.delta
          : Math.abs(b.delta) - Math.abs(a.delta)
    );
  const contributors = rankedContributors.slice(0, 1);
  const secondContributor = rankedContributors[1];

  if (
    secondContributor
    && (
      secondContributor.percentage === null
      || Math.abs(secondContributor.percentage) >= 10
    )
  ) {
    contributors.push(secondContributor);
  }

  if (!contributors.length) {
    return "No product-level movement was recorded.";
  }

  const contributorText = contributors
    .map((item) => {
      const percentageText = item.percentage === null
        ? "new"
        : `${item.percentage > 0 ? "+" : ""}${item.percentage.toFixed(1)}%`;
      return `${item.label} (${percentageText})`;
    })
    .join(", ");

  return `Top ${direction} product${contributors.length === 1 ? "" : "s"}: ${contributorText}.`;
}

function buildAcosContributorInsight(
  rows: MonthlyMetricRow[],
  displayedOverallDelta?: number
) {
  const groups = new Map<string, { sortKey: number; rows: MonthlyMetricRow[] }>();

  rows.forEach((row) => {
    const info = monthInfo(row);
    if (!info || isTotalRow(row)) return;
    const group = groups.get(info.key) ?? { sortKey: info.sortKey, rows: [] };
    group.rows.push(row);
    groups.set(info.key, group);
  });

  const periods = [...groups.values()].sort((a, b) => a.sortKey - b.sortKey);
  const currentPeriod = periods.at(-1);
  const previousPeriod = periods.at(-2);
  if (!currentPeriod || !previousPeriod) {
    return "No previous-month product ACOS baseline is available.";
  }

  type AcosEntry = {
    sku: string;
    productName: string;
    adsSpend: number;
    netSales: number;
    directAcosTotal: number;
    directAcosCount: number;
  };
  const aggregate = (periodRows: MonthlyMetricRow[]) => {
    const result = new Map<string, AcosEntry>();

    periodRows.forEach((row) => {
      const sku = String(row.sku ?? row.SKU ?? "").trim();
      const productName = String(row.product_name ?? row["Product Name"] ?? "").trim();
      if (!sku && !productName) return;

      const key = sku
        ? `sku:${sku.toUpperCase()}`
        : `product:${productName.toLowerCase()}`;
      const current = result.get(key);
      const directAcos = toNumber(row.acos ?? row.ads_acos ?? row.acos_percentage);
      result.set(key, {
        sku: sku || current?.sku || "",
        productName: productName || current?.productName || "",
        adsSpend: (current?.adsSpend ?? 0) + Math.abs(toNumber(row.ads_spend) ?? 0),
        netSales: (current?.netSales ?? 0) + Math.abs(toNumber(row.net_sales) ?? 0),
        directAcosTotal: (current?.directAcosTotal ?? 0) + (directAcos ?? 0),
        directAcosCount: (current?.directAcosCount ?? 0) + (directAcos === null ? 0 : 1),
      });
    });

    return result;
  };

  const currentEntries = aggregate(currentPeriod.rows);
  const previousEntries = aggregate(previousPeriod.rows);
  const acosValue = (entry?: AcosEntry) => {
    if (!entry) return 0;
    if (entry.netSales > 0 && entry.adsSpend > 0) {
      return (entry.adsSpend / entry.netSales) * 100;
    }
    return entry.directAcosCount > 0
      ? entry.directAcosTotal / entry.directAcosCount
      : 0;
  };

  const overallDelta = Number.isFinite(displayedOverallDelta)
    ? Number(displayedOverallDelta)
    : 0;
  const direction = overallDelta < 0 ? "decline" : overallDelta > 0 ? "increase" : "movement";
  const movements = [...new Set([...currentEntries.keys(), ...previousEntries.keys()])]
    .map((key) => {
      const current = currentEntries.get(key);
      const previous = previousEntries.get(key);
      const currentAcos = acosValue(current);
      const previousAcos = acosValue(previous);
      return {
        label: current?.productName || previous?.productName || current?.sku || previous?.sku || "Product",
        currentAcos,
        previousAcos,
        delta: currentAcos - previousAcos,
      };
    })
    .filter((item) =>
      direction === "decline"
        ? item.delta < 0
        : direction === "increase"
          ? item.delta > 0
          : item.delta !== 0
    )
    .sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));

  const strongest = movements[0];
  if (!strongest) {
    return `No product ACOS movement aligned with the overall TACoS ${direction}.`;
  }

  const verb = strongest.delta > 0 ? "increased" : "decreased";
  return `Main ACOS driver of TACoS ${direction}: ${strongest.label} ${verb} from ${strongest.previousAcos.toFixed(1)}% to ${strongest.currentAcos.toFixed(1)}% (${strongest.delta > 0 ? "+" : ""}${strongest.delta.toFixed(1)} pp).`;
}

function buildPromotionalRebateInsight(rows: MonthlyMetricRow[]) {
  const groups = new Map<string, { sortKey: number; rows: MonthlyMetricRow[] }>();

  rows.forEach((row) => {
    const info = monthInfo(row);
    if (!info || isTotalRow(row)) return;
    const group = groups.get(info.key) ?? { sortKey: info.sortKey, rows: [] };
    group.rows.push(row);
    groups.set(info.key, group);
  });

  const periods = [...groups.values()].sort((a, b) => a.sortKey - b.sortKey);
  const currentPeriod = periods.at(-1);
  const previousPeriod = periods.at(-2);
  if (!currentPeriod) {
    return {
      highest: "No product-level promotional rebate data is available.",
      improved: "",
    };
  }

  type RebateEntry = {
    sku: string;
    productName: string;
    rebate: number;
    netSales: number;
    directRateTotal: number;
    directRateCount: number;
  };
  const aggregate = (periodRows: MonthlyMetricRow[]) => {
    const result = new Map<string, RebateEntry>();

    periodRows.forEach((row) => {
      const sku = String(row.sku ?? row.SKU ?? "").trim();
      const productName = String(row.product_name ?? row["Product Name"] ?? "").trim();
      if (!sku && !productName) return;

      const key = sku
        ? `sku:${sku.toUpperCase()}`
        : `product:${productName.toLowerCase()}`;
      const current = result.get(key);
      const directRate = toNumber(
        row.promotional_rebates_percentage ?? row.promotion_rebates_percentage
      );
      result.set(key, {
        sku: sku || current?.sku || "",
        productName: productName || current?.productName || "",
        rebate: (current?.rebate ?? 0) + Math.abs(toNumber(row.promotional_rebates) ?? 0),
        netSales: (current?.netSales ?? 0) + Math.abs(toNumber(row.net_sales) ?? 0),
        directRateTotal: (current?.directRateTotal ?? 0) + Math.abs(directRate ?? 0),
        directRateCount: (current?.directRateCount ?? 0) + (directRate === null ? 0 : 1),
      });
    });

    return result;
  };

  const currentEntries = aggregate(currentPeriod.rows);
  const previousEntries = previousPeriod ? aggregate(previousPeriod.rows) : new Map<string, RebateEntry>();
  const rebateRate = (entry?: RebateEntry) => {
    if (!entry) return 0;
    if (entry.netSales > 0 && entry.rebate > 0) {
      return (entry.rebate / entry.netSales) * 100;
    }
    return entry.directRateCount > 0
      ? entry.directRateTotal / entry.directRateCount
      : 0;
  };
  const labelFor = (entry: RebateEntry) => entry.productName || entry.sku || "Product";

  const highestProducts = [...currentEntries.values()]
    .map((entry) => ({ label: labelFor(entry), rate: rebateRate(entry) }))
    .filter((item) => item.rate > 0)
    .sort((a, b) => b.rate - a.rate)
    .slice(0, 2);
  const highest = highestProducts.length
    ? `Highest promotional rebate rates: ${highestProducts.map((item) => `${item.label} (${item.rate.toFixed(1)}%)`).join(", ")}.`
    : "No product currently has a promotional rebate percentage.";

  if (!previousPeriod) return { highest, improved: "" };

  const improvements = [...currentEntries.entries()]
    .map(([key, currentEntry]) => {
      const previousEntry = previousEntries.get(key);
      if (!previousEntry) return null;
      const currentRate = rebateRate(currentEntry);
      const previousRate = rebateRate(previousEntry);
      if (previousRate <= 0 || currentRate >= previousRate) return null;
      const relativeImprovement = ((previousRate - currentRate) / previousRate) * 100;
      return {
        label: labelFor(currentEntry),
        currentRate,
        previousRate,
        percentagePointChange: currentRate - previousRate,
        relativeImprovement,
      };
    })
    .filter((item): item is NonNullable<typeof item> => Boolean(item))
    .filter((item) => item.relativeImprovement >= 10)
    .sort((a, b) => a.percentagePointChange - b.percentagePointChange);

  const mostImproved = improvements[0];
  const improved = mostImproved
    ? `Material improvement: ${mostImproved.label} fell from ${mostImproved.previousRate.toFixed(1)}% to ${mostImproved.currentRate.toFixed(1)}% (${mostImproved.percentagePointChange.toFixed(1)} pp).`
    : "";

  return { highest, improved };
}

function buildHighestReturnSkuInsight(rows: MonthlyMetricRow[]) {
  const groups = new Map<string, { sortKey: number; rows: MonthlyMetricRow[] }>();

  rows.forEach((row) => {
    const info = monthInfo(row);
    if (!info || isTotalRow(row)) return;
    const group = groups.get(info.key) ?? { sortKey: info.sortKey, rows: [] };
    group.rows.push(row);
    groups.set(info.key, group);
  });

  const currentPeriod = [...groups.values()]
    .sort((a, b) => a.sortKey - b.sortKey)
    .at(-1);
  if (!currentPeriod) return null;

  const bySku = new Map<string, { sku: string; productName: string; soldUnits: number; returnedUnits: number }>();
  currentPeriod.rows.forEach((row) => {
    const sku = String(row.sku ?? row.SKU ?? "").trim();
    const productName = String(row.product_name ?? row["Product Name"] ?? "").trim();
    if (!sku) return;

    const key = sku.toUpperCase();
    const current = bySku.get(key);
    bySku.set(key, {
      sku,
      productName: productName || current?.productName || "",
      soldUnits: (current?.soldUnits ?? 0) + Math.max(0, toNumber(row.quantity) ?? 0),
      returnedUnits: (current?.returnedUnits ?? 0) + Math.abs(toNumber(row.return_quantity) ?? 0),
    });
  });

  const highest = [...bySku.values()]
    .filter((item) => item.soldUnits > 0 && item.returnedUnits > 0)
    .map((item) => ({
      ...item,
      returnRate: (item.returnedUnits / item.soldUnits) * 100,
    }))
    .sort((a, b) => b.returnRate - a.returnRate)[0];

  return highest
    ? { label: highest.productName || highest.sku, returnRate: highest.returnRate }
    : null;
}

function buildHighestRefundSalesSkuInsight(rows: MonthlyMetricRow[]) {
  const groups = new Map<string, { sortKey: number; rows: MonthlyMetricRow[] }>();

  rows.forEach((row) => {
    const info = monthInfo(row);
    if (!info || isTotalRow(row)) return;
    const group = groups.get(info.key) ?? { sortKey: info.sortKey, rows: [] };
    group.rows.push(row);
    groups.set(info.key, group);
  });

  const currentPeriod = [...groups.values()]
    .sort((a, b) => a.sortKey - b.sortKey)
    .at(-1);
  if (!currentPeriod) return null;

  const bySku = new Map<string, { sku: string; productName: string; grossSales: number; refundSales: number }>();
  currentPeriod.rows.forEach((row) => {
    const sku = String(row.sku ?? row.SKU ?? "").trim();
    const productName = String(row.product_name ?? row["Product Name"] ?? "").trim();
    if (!sku) return;

    const key = sku.toUpperCase();
    const current = bySku.get(key);
    bySku.set(key, {
      sku,
      productName: productName || current?.productName || "",
      grossSales: (current?.grossSales ?? 0) + Math.abs(toNumber(row.gross_sales) ?? 0),
      refundSales: (current?.refundSales ?? 0) + Math.abs(toNumber(row.refund_sales ?? row.refunded_sales) ?? 0),
    });
  });

  const highest = [...bySku.values()]
    .filter((item) => item.grossSales > 0 && item.refundSales > 0)
    .map((item) => ({
      ...item,
      refundRate: (item.refundSales / item.grossSales) * 100,
    }))
    .sort((a, b) => b.refundRate - a.refundRate)[0];

  return highest
    ? { label: highest.productName || highest.sku, refundRate: highest.refundRate }
    : null;
}

function buildAspRangeInsight(rows: MonthlyMetricRow[]) {
  const groups = new Map<string, { sortKey: number; rows: MonthlyMetricRow[] }>();

  rows.forEach((row) => {
    const info = monthInfo(row);
    if (!info || isTotalRow(row)) return;
    const group = groups.get(info.key) ?? { sortKey: info.sortKey, rows: [] };
    group.rows.push(row);
    groups.set(info.key, group);
  });

  const currentPeriod = [...groups.values()]
    .sort((a, b) => a.sortKey - b.sortKey)
    .at(-1);
  if (!currentPeriod) return null;

  const products = currentPeriod.rows
    .map((row) => {
      const productName = String(row.product_name ?? row["Product Name"] ?? "").trim();
      const sku = String(row.sku ?? row.SKU ?? "").trim();
      return {
        label: productName || sku,
        asp: toNumber(row.asp) ?? 0,
      };
    })
    .filter((item) => item.label && item.asp > 0);

  if (!products.length) return null;

  const highest = [...products].sort((a, b) => b.asp - a.asp)[0];
  const lowest = [...products].sort((a, b) => a.asp - b.asp)[0];
  return { highest, lowest };
}

function buildCm2FactorInsight(current: MonthSnapshot, previous?: MonthSnapshot) {
  if (!previous) return "No previous-month CM2 factor baseline is available.";

  const cm2Delta = (current.values.cm2Profit ?? 0) - (previous.values.cm2Profit ?? 0);
  if (cm2Delta === 0) return "CM2 Profit was unchanged month over month.";
  const cm2Direction = cm2Delta > 0 ? "growth" : "decline";

  const factors = [
    { label: "CM1 Profit", key: "profit", cm2Effect: 1 },
    { label: "Ads", key: "adsSpend", cm2Effect: -1 },
    { label: "Shipping Charges", key: "shippingCharges", cm2Effect: -1 },
    { label: "Inventory Storage Charges", key: "inventoryStorageFee", cm2Effect: -1 },
  ]
    .map((factor) => {
      const currentValue = current.values[factor.key] ?? 0;
      const previousValue = previous.values[factor.key] ?? 0;
      const delta = currentValue - previousValue;
      return {
        ...factor,
        currentValue,
        previousValue,
        delta,
        cm2Impact: delta * factor.cm2Effect,
        percentage: percentageChange(currentValue, previousValue),
      };
    })
    .filter((factor) =>
      cm2Delta > 0 ? factor.cm2Impact > 0 : factor.cm2Impact < 0
    )
    .sort((a, b) => Math.abs(b.cm2Impact) - Math.abs(a.cm2Impact));

  if (!factors.length) {
    return `None of the selected factors contributed to the CM2 ${cm2Direction}.`;
  }

  const strongest = factors.slice(0, 1);
  const second = factors[1];
  if (
    second &&
    Math.abs(second.cm2Impact) >= Math.abs(strongest[0].cm2Impact) * 0.1
  ) {
    strongest.push(second);
  }

  const factorText = strongest.map((factor) => {
    if (factor.percentage === null) {
      return `${factor.label} moved from zero`;
    }
    const direction = factor.delta > 0 ? "increased" : "decreased";
    return `${factor.label} ${direction} ${Math.abs(factor.percentage).toFixed(1)}%`;
  }).join(", ");

  return `Main factor${strongest.length === 1 ? "" : "s"} behind CM2 ${cm2Direction}: ${factorText}.`;
}

function buildOtherExpenseInsight(
  current: MonthSnapshot,
  previous: MonthSnapshot | undefined,
  format: (value: number, type: MetricFormat) => string
) {
  if (!previous) return "No previous-month expense baseline is available.";

  const totalDelta = (current.values.platformFee ?? 0) - (previous.values.platformFee ?? 0);
  if (totalDelta === 0) return "Other expense was unchanged month over month.";

  const factors = [
    { label: "Misc. Transactions", key: "miscTransaction" },
    { label: "Lost Inventory", key: "lostTotal" },
    { label: "Inventory Charges", key: "platformInventoryStorageFee" },
    { label: "Subscription Fees", key: "subscriptionFees" },
  ].map((factor) => {
    const currentValue = current.values[factor.key] ?? 0;
    const previousValue = previous.values[factor.key] ?? 0;
    return {
      ...factor,
      currentValue,
      previousValue,
      delta: currentValue - previousValue,
      percentage: percentageChange(currentValue, previousValue),
    };
  });

  const describe = (factor: typeof factors[number]) => {
    const movement = factor.percentage === null
      ? "new expense"
      : `${factor.percentage > 0 ? "+" : ""}${factor.percentage.toFixed(1)}%`;
    return `${factor.label}: ${format(factor.previousValue, "currency")} → ${format(factor.currentValue, "currency")} (${movement})`;
  };
  const movingFactors = factors
    .filter((factor) => factor.delta !== 0)
    .sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));

  if (!movingFactors.length) {
    return "The tracked Other expense components were unchanged.";
  }

  const strongest = movingFactors.slice(0, 1);
  const second = movingFactors[1];
  if (second && Math.abs(second.delta) >= Math.abs(strongest[0].delta) * 0.1) {
    strongest.push(second);
  }

  return `Largest component changes affecting Other expense: ${strongest.map(describe).join(", ")}.`;
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
    key: "tacos",
    title: "TACoS",
    category: "Advertising & Promotion",
    icon: "target",
    format: "percent",
    inverseTrend: true,
    detail: (s, f) => `Total advertising cost is ${f(s.values.tacos, "percent")} of net sales. Lower TACoS is better.`,
  },
  {
    key: "agedInventory180Plus",
    title: "Ageing Inventory 180+",
    category: "Inventory & Dispatch",
    icon: "box",
    format: "number",
    inverseTrend: true,
    detail: (s, f) => `181–270 days: ${f(s.values.agedInventory181To270, "number")}; 271–365 days: ${f(s.values.agedInventory271To365, "number")}; 365+ days: ${f(s.values.agedInventory365Plus, "number")}.`,
  },
  {
    key: "platformFee",
    title: "Other",
    category: "Fees & Other Costs",
    icon: "coin",
    format: "currency",
    inverseTrend: true,
    detail: (s, f) => `Other Transactions are ${f(safePercent(Math.abs(s.values.platformFee), s.values.netSales), "percent")} of net sales.`,
  },
  {
    key: "promotionalRebates",
    title: "Promotional Rebate",
    category: "Advertising & Promotion",
    icon: "spark",
    format: "currency",
    inverseTrend: true,
    detail: (s, f) => `Promotional rebate is ${f(Math.abs(s.values.rebatePercent), "percent")} of net sales.`,
  },
];

function Sparkline({
  values,
  status,
}: {
  values: number[];
  status: MovementStatus;
}) {
  const areaClipId = `business-sparkline-area-${useId().replace(/:/g, "")}`;

  const width = 180;
  const height = 44;

  const raw = values.filter((value) => Number.isFinite(value));

  const startValue = raw[0] ?? 0;
  const endValue = raw[raw.length - 1] ?? startValue;

  /*
   * IMPORTANT:
   *
   * Up / Down graph actual metric movement follow karega.
   *
   * Lekin agar CARD status Stable hai (for example +1.85%),
   * to graph ko actual slight increase/decrease follow nahi karna hai.
   * Stable graph always horizontal zig-zag dikhega.
   */
  const actualDirection: MovementStatus =
    endValue > startValue
      ? "up"
      : endValue < startValue
        ? "down"
        : "stable";

  const direction: MovementStatus =
    status === "stable"
      ? "stable"
      : actualDirection;

  const makeMiniTrend = () => {
    const count = 7;

    const scale = Math.max(
      Math.abs(startValue),
      Math.abs(endValue),
      1
    );

    const realMove = endValue - startValue;

    /*
     * STABLE
     *
     * Always create the same sideways zig-zag pattern.
     * We intentionally don't return raw history here,
     * because raw values may have a slight upward/downward slope
     * even though the card is classified as Stable.
     */
    if (direction === "stable") {
      const center =
        raw.length > 0
          ? raw.reduce((sum, value) => sum + value, 0) / raw.length
          : (startValue + endValue) / 2;

      const stableWiggle = Math.max(
        Math.abs(center) * 0.012,
        0.35
      );

      /*
       * Starts and ends at same level.
       * Balanced zig-zag = visually stable.
       */
      const stableOffsets = [
        0,
        0.24,
        -0.22,
        0.2,
        -0.18,
        0.14,
        0,
      ];

      return stableOffsets.map(
        (offset) => center + offset * stableWiggle
      );
    }

    /*
     * UP / DOWN
     *
     * If enough real historical points exist,
     * continue using actual data.
     */
    if (raw.length > 2) {
      return raw;
    }

    /*
     * If only previous + current month exist,
     * create a visually useful mini trend while
     * maintaining the real overall direction.
     */
    const wiggle = Math.max(
      Math.abs(realMove) * 0.28,
      scale * 0.018
    );

    const offsets = [
      0,
      0.55,
      -0.28,
      0.42,
      -0.18,
      0.32,
      0,
    ];

    /*
     * Keep enough visual movement so a very small
     * change still looks directional.
     */
    const visualMove =
      Math.sign(realMove || (direction === "up" ? 1 : -1)) *
      Math.max(
        Math.abs(realMove),
        scale * 0.045
      );

    return offsets.map((offset, index) => {
      const progress = index / (count - 1);

      const base =
        startValue +
        visualMove * progress;

      return base + offset * wiggle;
    });
  };

  const usable = makeMiniTrend();

  const min = Math.min(...usable);
  const max = Math.max(...usable);

  const visualPadding = Math.max(
    (max - min) * 0.18,
    Math.max(Math.abs(max), 1) * 0.01
  );

  const low = min - visualPadding;
  const high = max + visualPadding;
  const range = high - low || 1;

  const pointList = usable.map((value, index) => {
    const x =
      (index / Math.max(usable.length - 1, 1)) *
      width;

    const y =
      height -
      4 -
      ((value - low) / range) *
        (height - 8);

    return {
      x,
      y,
    };
  });

  const toAreaPoints = (linePoints: string) =>
    `0,${height} ${linePoints} ${width},${height}`;

  const points = pointList
    .map(({ x, y }) => `${x},${y}`)
    .join(" ");

  /*
   * Animation starts from a straight horizontal line.
   * Then graph reveals into its final shape.
   *
   * This now also applies to Stable cards.
   */
  const introY =
    direction === "stable"
      ? height / 2
      : pointList[0]?.y ?? height / 2;

  const introPoints = pointList
    .map(({ x }) => `${x},${introY}`)
    .join(" ");

  /*
   * BEFORE:
   * direction !== "stable" && pointList.length > 1
   *
   * NOW:
   * Stable also gets animation.
   */
  const shouldAnimateTrend =
    pointList.length > 1;

  const trendAnimationProps = {
    dur: "2.4s",
    repeatCount: "indefinite",
    keyTimes: "0;0.58;1",
    calcMode: "spline",
    keySplines:
      "0.22 1 0.36 1; 0.4 0 1 1",
  };

  const revealAnimationProps = {
    ...trendAnimationProps,
    values: "1; 0; 0",
  };

  const areaRevealAnimationProps = {
    ...trendAnimationProps,
    values: `0; ${width}; ${width}`,
  };

  const first = pointList[0];
  const last =
    pointList[pointList.length - 1];

  const areaPoints =
    `0,${height} ${points} ${width},${height}`;

  const introAreaPoints =
    toAreaPoints(introPoints);

  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      className="business-sparkline-svg h-10 w-full"
      preserveAspectRatio="none"
      aria-hidden="true"
    >
      <defs>
        <clipPath
          id={areaClipId}
          clipPathUnits="userSpaceOnUse"
        >
          <rect
            x="0"
            y="0"
            width={width}
            height={height}
          >
            {shouldAnimateTrend ? (
              <animate
                className="business-sparkline-animate"
                attributeName="width"
                {...areaRevealAnimationProps}
              />
            ) : null}
          </rect>
        </clipPath>
      </defs>

      {/* Soft area below graph */}
      <polygon
        points={areaPoints}
        fill="currentColor"
        opacity="0.055"
        clipPath={`url(#${areaClipId})`}
      >
        {shouldAnimateTrend ? (
          <animate
            className="business-sparkline-animate"
            attributeName="points"
            values={`${introAreaPoints}; ${areaPoints}; ${areaPoints}`}
            {...trendAnimationProps}
          />
        ) : null}
      </polygon>

      {/* Background / glow line */}
      <polyline
        points={points}
        fill="none"
        stroke="currentColor"
        strokeWidth="5"
        strokeLinecap="round"
        strokeLinejoin="round"
        opacity="0.07"
        pathLength={1}
        strokeDasharray="1"
        strokeDashoffset="0"
      >
        {shouldAnimateTrend ? (
          <animate
            className="business-sparkline-animate"
            attributeName="points"
            values={`${introPoints}; ${points}; ${points}`}
            {...trendAnimationProps}
          />
        ) : null}

        {shouldAnimateTrend ? (
          <animate
            className="business-sparkline-animate"
            attributeName="stroke-dashoffset"
            {...revealAnimationProps}
          />
        ) : null}
      </polyline>

      {/* Main graph line */}
      <polyline
        points={points}
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        pathLength={1}
        strokeDasharray="1"
        strokeDashoffset="0"
      >
        {shouldAnimateTrend ? (
          <animate
            className="business-sparkline-animate"
            attributeName="points"
            values={`${introPoints}; ${points}; ${points}`}
            {...trendAnimationProps}
          />
        ) : null}

        {shouldAnimateTrend ? (
          <animate
            className="business-sparkline-animate"
            attributeName="stroke-dashoffset"
            {...revealAnimationProps}
          />
        ) : null}
      </polyline>

      {/* End point */}
      {last ? (
        <circle
          cx={last.x}
          cy={last.y}
          r="2.5"
          fill="white"
          stroke="currentColor"
          strokeWidth="1.6"
        >
          {shouldAnimateTrend ? (
            <animate
              className="business-sparkline-animate"
              attributeName="cx"
              values={`${first?.x ?? last.x}; ${last.x}; ${last.x}`}
              {...trendAnimationProps}
            />
          ) : null}

          {shouldAnimateTrend ? (
            <animate
              className="business-sparkline-animate"
              attributeName="cy"
              values={`${introY}; ${last.y}; ${last.y}`}
              {...trendAnimationProps}
            />
          ) : null}
        </circle>
      ) : null}
    </svg>
  );
}

function MetricFlipCard({
  definition,
  snapshots,
  currency,
  unitContributorData,
}: {
  definition: MetricDefinition;
  snapshots: MonthSnapshot[];
  currency: string;
  unitContributorData: MonthlyMetricRow[];
}) {
  const format = makeFormatter(currency);
  const current = snapshots[snapshots.length - 1];
  const previous = snapshots[snapshots.length - 2];
  const currentValue = current.values[definition.key] ?? 0;
  const previousValue = previous?.values[definition.key];
  const delta = percentageChange(currentValue, previousValue);
  const rawStatus = definition.key === "agedInventory180Plus" && delta !== null
    ? delta > 0
      ? "up"
      : delta < 0
        ? "down"
        : "stable"
    : movementStatus(delta);
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
  const isUnitsSold = definition.key === "totalQuantity";
  const isNetSales = definition.key === "netSales";
  const isAsp = definition.key === "asp";
  const isCm2Profit = definition.key === "cm2Profit";
  const isTacos = definition.key === "tacos";
  const isOtherExpense = definition.key === "platformFee";
  const isPromotionalRebate = definition.key === "promotionalRebates";
  const hasProductMovementAnalysis = isUnitsSold || isNetSales || isAsp;
  const hasCustomMovementAnalysis = hasProductMovementAnalysis || isCm2Profit || isTacos || isOtherExpense || isPromotionalRebate;
  const productContributorInsight = hasProductMovementAnalysis
    ? buildProductContributorInsight(
      unitContributorData,
      isUnitsSold ? "units" : isNetSales ? "netSales" : "asp",
      previousValue === undefined ? undefined : currentValue - previousValue
    )
    : "";
  const highestReturnSku = isUnitsSold
    ? buildHighestReturnSkuInsight(unitContributorData)
    : null;
  const unitsReturnInsight = highestReturnSku
    ? `${format(current.values.returnQuantity ?? 0, "number")} units returned; ${highestReturnSku.label} had the highest return rate (${highestReturnSku.returnRate.toFixed(1)}%).`
    : definition.detail(current, format);
  const highestRefundSalesSku = isNetSales
    ? buildHighestRefundSalesSkuInsight(unitContributorData)
    : null;
  const refundSalesInsight = highestRefundSalesSku
    ? `${format(Math.abs(current.values.refundSales ?? 0), "currency")} refund sales; ${highestRefundSalesSku.label} had the highest refund-sales rate (${highestRefundSalesSku.refundRate.toFixed(1)}%).`
    : `${format(Math.abs(current.values.refundSales ?? 0), "currency")} refund sales this month.`;
  const aspRange = isAsp ? buildAspRangeInsight(unitContributorData) : null;
  const aspRangeInsight = aspRange
    ? `Highest ASP: ${aspRange.highest.label} (${format(aspRange.highest.asp, "currency")}); lowest ASP: ${aspRange.lowest.label} (${format(aspRange.lowest.asp, "currency")}).`
    : "No product-level ASP data is available.";
  const cm2FactorInsight = isCm2Profit
    ? buildCm2FactorInsight(current, previous)
    : "";
  const acosContributorInsight = isTacos
    ? buildAcosContributorInsight(
      unitContributorData,
      previousValue === undefined ? undefined : currentValue - previousValue
    )
    : "";
  const otherExpenseInsight = isOtherExpense
    ? buildOtherExpenseInsight(current, previous, format)
    : null;
  const promotionalRebateInsight = isPromotionalRebate
    ? buildPromotionalRebateInsight(unitContributorData)
    : null;
  const customSecondInsight = isCm2Profit
    ? cm2FactorInsight
    : isTacos
      ? acosContributorInsight
      : isOtherExpense
        ? otherExpenseInsight ?? ""
        : isPromotionalRebate
          ? promotionalRebateInsight?.highest ?? ""
          : productContributorInsight;
  const customThirdInsight = isUnitsSold
    ? unitsReturnInsight
    : isNetSales
      ? refundSalesInsight
      : isAsp
        ? aspRangeInsight
        : isTacos
          ? ""
          : isOtherExpense
            ? ""
            : isPromotionalRebate
              ? promotionalRebateInsight?.improved ?? ""
              : definition.detail(current, format);

  return <div className="group h-[158px] [perspective:1400px] transition-[transform,filter] duration-500 ease-out hover:-translate-y-1.5 hover:scale-[1.012] focus-within:-translate-y-1.5 focus-within:scale-[1.012]">
    <div className="relative h-full w-full transition-transform duration-1000 ease-[cubic-bezier(0.22,1,0.36,1)] [transform-style:preserve-3d] group-hover:[transform:rotateY(180deg)_rotateX(2deg)_translateZ(8px)] group-focus-within:[transform:rotateY(180deg)_rotateX(2deg)_translateZ(8px)]">
      <article className={`absolute inset-0 flex h-full flex-col overflow-hidden rounded-[16px] border ${theme.border} bg-white p-3.5 shadow-[0_2px_7px_rgba(16,35,58,0.10)] transition-[box-shadow,border-color] duration-500 group-hover:shadow-[0_16px_34px_rgba(16,35,58,0.16)] [backface-visibility:hidden] [transform:translateZ(1px)]`}>
        <span className={`pointer-events-none absolute -right-8 -top-8 h-20 w-20 rounded-full ${theme.softBg} opacity-75 blur-2xl transition-transform duration-1000 group-hover:scale-125`} />

        <div className="relative z-10 flex items-start justify-between gap-3">
          <div className="flex min-w-0 items-center gap-2.5">
            <div>
              <h3 className="text-sm font-semibold leading-4 text-[#17304F]">{definition.title}</h3>
              <p className="mt-0.5 text-[9px] text-[#738299]">{definition.category}</p>
            </div>
          </div>
          <span className={`rounded-full px-2 py-1 text-[9px] font-semibold ${theme.softBg} ${theme.text}`}></span>
        </div>

        <div className=" z-10 mt-8 flex min-h-0  items-start justify-between gap-4">
          <div className="min-w-0 shrink-0">
            <div className="min-[1700px]:text-[24px] text-base font-semibold tracking-[-0.025em] text-charcoal-500">{format(currentValue, definition.format)}</div>
            <div className=" flex items-center gap-1.5">
              <span className={`text-[12px] font-semibold ${theme.text}`}>{deltaText}</span>
            </div>
          </div>

          <div className={`w-[42%] min-w-[105px] max-w-[145px]   ${theme.text}`}>
            <Sparkline
  values={history}
  status={status}
/>
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
            <h3 className="text-sm font-semibold text-[#17304F]">{definition.title} Analysis</h3>
            <p className="min-[1700px]:text-[10px] text-[9px] text-[#75849A]">
              {current.label} business context
            </p>
          </div>
        </div>
        <ul className="relative z-10 min-[1700px]:mt-3 mt-2.5 min-[1700px]:space-y-1.5 space-y-1 min-[1700px]:text-[12px] text-[10px] min-[1700px]:leading-[13px] leading-[11px] text-[#40536C]">
          {hasCustomMovementAnalysis ? <>
            <li className="flex gap-2">
              <span className={`mt-1 h-1.5 w-1.5 shrink-0 rounded-full ${theme.bg}`} />
              <span>
                Previous <strong className="text-charcoal-500">{format(previousValue ?? 0, definition.format)}</strong>
                {" → "}Current <strong className="text-charcoal-500">{format(currentValue, definition.format)}</strong>
                {" "}<strong className={theme.text}>({deltaText})</strong>
              </span>
            </li>
            <li className="flex gap-2">
              <span className={`mt-1 h-1.5 w-1.5 shrink-0 rounded-full ${theme.bg}`} />
              <span>{customSecondInsight}</span>
            </li>
            {customThirdInsight ? <li className="flex gap-2">
              <span className={`mt-1 h-1.5 w-1.5 shrink-0 rounded-full ${theme.bg}`} />
              <span>{customThirdInsight}</span>
            </li> : null}
          </> : <>
            <li className="flex gap-2"><span className={`mt-1 h-1.5 w-1.5 shrink-0 rounded-full ${theme.bg}`} /><span>Current: <strong className="text-charcoal-500">{format(currentValue, definition.format)}</strong>{previous ? <> · Previous: <strong className="text-charcoal-500">{format(previousValue ?? 0, definition.format)}</strong></> : null}</span></li>
            <li className="flex gap-2"><span className={`mt-1 h-1.5 w-1.5 shrink-0 rounded-full ${theme.bg}`} /><span>{movementPoint}</span></li>
            <li className="flex gap-2"><span className={`mt-1 h-1.5 w-1.5 shrink-0 rounded-full ${theme.bg}`} /><span>{definition.detail(current, format)}</span></li>
          </>}
        </ul>
      </article>
    </div>
  </div>;
}


function ActionItemsView({
  items,
}: {
  items: DashboardActionItem[];
}) {
  const [category, setCategory] = useState<"All" | Category | "Completed">("All");
  const [completed, setCompleted] = useState<Set<string>>(new Set());
  const actionItems = useMemo(
    () => items.filter((item) => item.id !== "amazon-fee-pressure"),
    [items]
  );
  const completedCount = useMemo(
    () => actionItems.filter((item) => completed.has(item.id)).length,
    [actionItems, completed]
  );

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
        <span>{actionItems.length - completedCount} open action{actionItems.length - completedCount === 1 ? "" : "s"}</span>
      </div>
    </div>

    {visible.length === 0 ? <div className="rounded-xl border border-dashed border-[#CFE0DC] bg-[#F8FCFB] px-6 py-14 text-center">
      <div className="mx-auto mb-3 flex h-10 w-10 items-center justify-center rounded-full bg-[#E8F6F2] text-green-500"><Icon name="check" className="h-5 w-5" /></div>
      <p className="font-semibold text-[#17304F]">No action items in this view</p>
      <p className="mt-1 text-sm text-[#728096]">Only data-backed signals are shown here.</p>
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
              if (next.has(item.id)) {
                next.delete(item.id);
              } else {
                next.add(item.id);
              }
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
  unitContributorData = [],
  currency,
  loading,
}: {
  monthlyData: MonthlyMetricRow[];
  unitContributorData?: MonthlyMetricRow[];
  currency?: string;
  loading: boolean;
}) {
  const snapshots = useMemo(() => buildMonthlySnapshots(monthlyData), [monthlyData]);

  if (loading && !snapshots.length) {
    return (
      <Loader
        fullscreen
        contained
        backgroundClass="bg-white/40"
      />
    );
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
        <div>
          <PageBreadcrumb
            pageTitle="Business Analysis"
            variant="page"
            align="left"
            textSize="2xl"
          />
          <p className="min-[1700px]:text-sm text-xs text-[#50627A]">Key business metrics and month-over-month movement.</p>
        </div>
      </div>
    </div>

    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
      {metricDefinitions.map((definition) => <MetricFlipCard key={definition.key} definition={definition} snapshots={snapshots} currency={resolvedCurrency} unitContributorData={unitContributorData} />)}
    </div>
  </div>;
}

export default function DashboardActionItemsTab({
  actionItems = [],
  loading = false,
  error,
  onRetry,
}: {
  actionItems?: DashboardActionItem[];
  loading?: boolean;
  error?: string | null;
  onRetry?: () => void;
}) {
  if (loading) {
    return (
      <Loader
        fullscreen
        contained
        backgroundClass="bg-white/40"
      />
    );
  }

  if (error) {
    return <div className="my-4 rounded-xl border border-[#FFD0C7] bg-[#FFF7F5] px-6 py-10 text-center">
      <p className="font-semibold text-[#9F2D20]">Action items could not be loaded</p>
      <p className="mt-1 text-sm text-[#7B514B]">{error}</p>
      {onRetry ? <button type="button" onClick={onRetry} className="mt-4 rounded-md border border-[#D89A90] bg-white px-4 py-2 text-xs font-semibold text-[#9F2D20]">Try again</button> : null}
    </div>;
  }

  return <div className="w-full px-0 py-4">
    <ActionItemsView items={actionItems} />
  </div>;
}
