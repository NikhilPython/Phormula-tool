"use client";

import React, { useMemo, useState } from "react";

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
  currency,
  loading,
  skuAnalysisContent,
}: {
  monthlyData: MonthlyMetricRow[];
  currency?: string;
  loading: boolean;
  skuAnalysisContent?: React.ReactNode;
}) {
  const snapshots = useMemo(() => buildMonthlySnapshots(monthlyData), [monthlyData]);

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
      </div>
    </div>

    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
      {metricDefinitions.map((definition) => <MetricFlipCard key={definition.key} definition={definition} snapshots={snapshots} currency={resolvedCurrency} />)}
    </div>

    {skuAnalysisContent ? <div className="pt-1">{skuAnalysisContent}</div> : null}
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
    return <div className="flex min-h-[260px] w-full items-center justify-center py-12">
      <div className="text-center">
        <div className="mx-auto h-8 w-8 animate-spin rounded-full border-2 border-[#CFE0DC] border-t-[#07836C]" />
        <p className="mt-3 text-sm font-medium text-[#65758B]">Building action items…</p>
      </div>
    </div>;
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
