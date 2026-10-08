"use client";

import React, {
  useEffect,
  useMemo,
  useState,
  useRef,
  type JSX,
  useCallback,
} from "react";
import { useParams, useSearchParams } from "next/navigation";
import { Doughnut } from "react-chartjs-2";
import { Chart as ChartJS, ArcElement, Tooltip, Legend } from "chart.js";
import { jwtDecode } from "jwt-decode";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import DataTable, { ColumnDef, Row } from "@/components/ui/table/DataTable";
import Loader from "@/components/loader/Loader";
import DownloadButton from "@/components/ui/button/DownloadIconButton";
import { useHomeCurrencyContext } from "@/lib/hooks/useHomeCurrencyContext";
import { AiButton } from "@/components/ui/button/AiButton";
import PeriodFiltersTable, { type Range } from "@/components/filters/PeriodFiltersTable";
import GroupedCollapsibleTable from "@/components/ui/table/GroupedCollapsibleTable";
import { IoMdLock, IoMdArrowBack } from "react-icons/io";
import SkuAgeingDonutChart, {
  type DonutChartItem,
} from "@/components/common/inventory/SkuAgeingDonutChart";
import Button from "@/components/ui/button/Button";
import { exportReferralFeesExcel } from "@/lib/excel/exportCurrentInventoryExcel";
import { exportFbaFeesExcel } from "@/lib/excel/exportFbaFeesExcel";
import { useAppSelector } from "@/lib/store";
import SummaryMetricCard from "@/components/dropdowns/SummaryMetricCard";
import { motion, AnimatePresence } from "framer-motion";
import Productinfoinpopup from "@/components/businessInsight/Productinfoinpopup";
import ActionDiagnosisPanel from "@/components/dashboard/ActionDiagnosisPanel";

/* ===================== Overlap Plugin ===================== */
const overlapPlugin = {
  id: "overlapPlugin",
  afterDatasetsDraw(chart: any, _args: any, pluginOptions: any) {
    const opts = pluginOptions || {};
    const salesTotal = Math.max(0, Number(opts.salesTotal || 0));
    if (!salesTotal) return;

    const applicableValue = Math.max(0, Number(opts.applicableValue || 0));
    const overchargedValue = Math.max(0, Number(opts.overchargedValue || 0));

    const showApplicable = opts.showApplicable !== false;
    const showOvercharged = opts.showOvercharged !== false;

    if (!showApplicable && !showOvercharged) return;

    const meta = chart.getDatasetMeta(0);
    if (!meta?.data?.length) return;

    const maskArc = meta.data[0];
    const { x, y, innerRadius, outerRadius, startAngle, endAngle } = maskArc;

    const full = Math.PI * 2;
    const applAngleRaw = showApplicable ? (applicableValue / salesTotal) * full : 0;
    const overAngleRaw = showOvercharged ? (overchargedValue / salesTotal) * full : 0;

    const maskSpan = Math.max(0, endAngle - startAngle);
    const applAngle = Math.min(applAngleRaw, maskSpan);
    const overAngle = Math.min(overAngleRaw, Math.max(0, maskSpan - applAngle));

    const radius = (innerRadius + outerRadius) / 2;
    const thickness = outerRadius - innerRadius;

    const ctx = chart.ctx;
    ctx.save();
    ctx.lineWidth = thickness;
    ctx.lineCap = "butt";

    if (applAngle > 0) {
      ctx.beginPath();
      ctx.strokeStyle = "#14B8A6";
      ctx.arc(x, y, radius, startAngle, startAngle + applAngle);
      ctx.stroke();
    }

    if (overAngle > 0) {
      ctx.beginPath();
      ctx.strokeStyle = "#EF4444";
      ctx.arc(
        x,
        y,
        radius,
        startAngle + applAngle,
        startAngle + applAngle + overAngle
      );
      ctx.stroke();
    }

    ctx.restore();
  },
};

ChartJS.register(ArcElement, Tooltip, Legend, overlapPlugin);

/* ===================== ENV ===================== */
const baseURL = process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

/* ===================== Types ===================== */
type ReferralRow = Partial<{
  sku: string;
  product_name: string;
  category: string;
  asin: string;
  quantity: number | string;
  return_quantity: number | string;
  total_quantity: number | string;
  sales: number | string;
  product_sales: number | string;
  shipping_credits: number | string;
  promotional_rebates: number | string;
  referral_fee_per: number | string;
  referral_fee: number | string;
  gross_sales: number | string;
  refRate: number | string;
  refFeesApplicable: number | string;
  refFeesCharged: number | string;
  overcharged: number | string;
  difference: number | string;
  errorstatus: string;
  selling_fees: number | string;
  fba_fees: number | string;
  fbaanswer: number | string | null;
  other_transaction_fees: number | string;
  platform_fee: number | string;
  answer: number | string;

  net_sales_total_value: number | string;
  status: string;
  total_value: number | string;
}>;

type Summary = {
  ordersUnits: number;
  totalSales: number;
  feeImpact: number;
};

type FeeSummaryRow = {
  label: string;
  units: number;
  sales: number;
  refFeesApplicable: number;
  refFeesCharged: number;
  overcharged: number;
};

type Card6Summary = {
  sales: number;
  units: number;
  productSales: number;
  totalFees: number;            // charged (sum of fee buckets)
  totalFeesApplicable: number;  // applicable (sum of fee buckets)

  refFeesApplied: number;       // charged
  refFeesApplicable: number;    // applicable

  fbaFees: number;              // charged
  fbaFeesApplicable: number;    // applicable

  platformFees: number;         // charged
  platformFeesApplicable: number; // applicable

  otherFees: number;            // charged
  otherFeesApplicable: number;  // applicable
};

type FeePercentageMetric = {
  charged_net_sales_pct: number;
  applicable_net_sales_pct: number;
  charged_vs_applicable_pct: number;
};

type FeePercentages = {
  referral_fees: FeePercentageMetric;
  fba_fees: FeePercentageMetric;
  platform_fees: FeePercentageMetric;
  other_fees: FeePercentageMetric;
};

type ReferralFeeInsight = {
  status: "clear" | "low" | "review" | "action";
  overcharged_amount: number;
  overcharged_units: number;
  overcharge_rate_pct: number;
  affected_units_pct: number;
  accurate_units_pct: number;
  net_variance: number;
};

const EMPTY_FEE_PERCENTAGES: FeePercentages = {
  referral_fees: {
    charged_net_sales_pct: 0,
    applicable_net_sales_pct: 0,
    charged_vs_applicable_pct: 0,
  },
  fba_fees: {
    charged_net_sales_pct: 0,
    applicable_net_sales_pct: 0,
    charged_vs_applicable_pct: 0,
  },
  platform_fees: {
    charged_net_sales_pct: 0,
    applicable_net_sales_pct: 0,
    charged_vs_applicable_pct: 0,
  },
  other_fees: {
    charged_net_sales_pct: 0,
    applicable_net_sales_pct: 0,
    charged_vs_applicable_pct: 0,
  },
};

const EMPTY_REFERRAL_FEE_INSIGHT: ReferralFeeInsight = {
  status: "clear",
  overcharged_amount: 0,
  overcharged_units: 0,
  overcharge_rate_pct: 0,
  affected_units_pct: 0,
  accurate_units_pct: 0,
  net_variance: 0,
};


type SalesStatusSummary = {
  totalSales: number;
  accurateSales: number;
  overchargedSales: number;
  underchargedSales: number;
  noRefFeeSales: number;
};

type RefFeesBreakdown = {
  totalApplicable: number; // Grand Total "answer"
  accurateApplicable: number; // Charge - Accurate "answer"
  overchargedApplicable: number; // Charge - Overcharged "answer"
  underchargedApplicable: number; // Charge - Undercharged "answer"
  noRefFeeApplicable: number; // Charge - noreferallfee "answer"
};


/* ===================== Helpers ===================== */
const formatMonthYear = (month: string, year: string) => {
  const date = new Date(`${month} 1, ${year}`);

  if (Number.isNaN(date.getTime())) {
    return `${month} '${year.slice(-2)}`;
  }

  const shortMonth = date.toLocaleString("en-US", {
    month: "short",
  });

  return `${shortMonth}'${year.slice(-2)}`;
};

const fmtInteger = (n: number): string =>
  typeof n === "number"
    ? n.toLocaleString(undefined, {
      maximumFractionDigits: 0,
      minimumFractionDigits: 0,
    })
    : "-";

const getLastCompletedMonth = () => {
  const now = new Date();
  const lastCompleted = new Date(now.getFullYear(), now.getMonth() - 1, 1);

  return {
    month: lastCompleted
      .toLocaleString("en-US", { month: "long" })
      .toLowerCase(),
    year: String(lastCompleted.getFullYear()),
  };
};

const getQuarterFromMonth = (month: string) => {
  const monthIndex = new Date(`${month} 1, 2000`).getMonth();

  if (monthIndex <= 2) return "Q1";
  if (monthIndex <= 5) return "Q2";
  if (monthIndex <= 8) return "Q3";
  return "Q4";
};

const toNumberSafe = (v: any): number => {
  if (v === null || v === undefined) return 0;
  if (typeof v === "number") return v;
  const num = Number(String(v).replace(/[, ]+/g, ""));
  return Number.isNaN(num) ? 0 : num;
};

// Missing expected fees must propagate through subtotals instead of becoming zero.
const expectedFee = (value: unknown): number =>
  value == null || value === "" ? Number.NaN : Number(value);

const getNetSales = (r: any): number => {
  const net = toNumberSafe(r?.net_sales_total_value);
  if (net) return net;
  const skuNet = toNumberSafe(r?.net_sales);
  if (skuNet) return skuNet;
  return toNumberSafe(r?.product_sales ?? r?.sales);
};

const getGrossSales = (r: any): number => {
  const gross = toNumberSafe(r?.gross_sales);
  if (gross) return gross;
  return toNumberSafe(r?.product_sales ?? r?.sales);
};

const hasNumericValue = (v: any): boolean => {
  if (v === null || v === undefined) return false;
  if (typeof v === "string" && v.trim() === "") return false;
  return !Number.isNaN(Number(String(v).replace(/[, ]+/g, "")));
};

const getDisplayUnits = (r: any): number => {
  if (hasNumericValue(r?.total_quantity)) return toNumberSafe(r.total_quantity);
  const quantity = toNumberSafe(r?.quantity);
  if (hasNumericValue(r?.return_quantity)) {
    return Math.max(quantity - toNumberSafe(r.return_quantity), 0);
  }
  return quantity;
};

const getChargedReferralFees = (r: any): number => {
  // Amazon stores selling/referral fees as negative ledger entries. The UI
  // compares fee magnitudes, matching the backend reconciliation calculation.
  return Math.abs(toNumberSafe(r?.selling_fees));
};

const isGrandTotalLabel = (label: any): boolean =>
  String(label ?? "").trim().toLowerCase() === "grand total";

const roundReferralMoney = (value: any): number =>
  Math.round(toNumberSafe(value) * 100) / 100;

const correctReferralSummaryRows = (rows: FeeSummaryRow[]): FeeSummaryRow[] => {
  const correctedDetails = rows
    .filter((row) => !isGrandTotalLabel(row.label))
    .map((row) => {
      const label = row.label.trim().toLowerCase();
      const applicable = roundReferralMoney(row.refFeesApplicable);
      const charged = roundReferralMoney(
        label === "charge - accurate" ? applicable : row.refFeesCharged
      );
      const absoluteDifference = roundReferralMoney(
        Math.abs(applicable - charged)
      );

      return {
        ...row,
        refFeesApplicable: applicable,
        refFeesCharged: charged,
        overcharged:
          label === "charge - undercharged"
            ? -absoluteDifference
            : label === "charge - overcharged"
              ? absoluteDifference
              : 0,
      };
    });

  const correctedByLabel = new Map(
    correctedDetails.map((row) => [row.label.trim().toLowerCase(), row])
  );
  const totalCharged = roundReferralMoney(
    correctedDetails.reduce((sum, row) => sum + row.refFeesCharged, 0)
  );
  const totalDifference = roundReferralMoney(
    correctedDetails.reduce((sum, row) => sum + row.overcharged, 0)
  );

  return rows.map((row) => {
    if (isGrandTotalLabel(row.label)) {
      return {
        ...row,
        refFeesCharged: totalCharged,
        overcharged: totalDifference,
      };
    }

    return correctedByLabel.get(row.label.trim().toLowerCase()) ?? row;
  });
};

const scaleReferralMoneyBreakdown = (
  values: number[],
  target: number
): number[] => {
  const targetCents = Math.round(toNumberSafe(target) * 100);
  const normalizedValues = values.map((value) =>
    Math.max(toNumberSafe(value), 0)
  );
  const sourceTotal = normalizedValues.reduce((sum, value) => sum + value, 0);

  if (!targetCents) return normalizedValues.map(() => 0);
  if (!sourceTotal) {
    return normalizedValues.map((_, index) =>
      index === 0 ? targetCents / 100 : 0
    );
  }

  const scaledCents = normalizedValues.map(
    (value) => (value * targetCents) / sourceTotal
  );
  const allocatedCents = scaledCents.map((value) => Math.floor(value));
  let remainder =
    targetCents - allocatedCents.reduce((sum, value) => sum + value, 0);

  const allocationOrder = scaledCents
    .map((value, index) => ({ index, fraction: value - Math.floor(value) }))
    .sort((a, b) => b.fraction - a.fraction);

  for (const item of allocationOrder) {
    if (remainder <= 0) break;
    allocatedCents[item.index] += 1;
    remainder -= 1;
  }

  return allocatedCents.map((value) => value / 100);
};

const scaleIntegerBreakdown = (values: number[], target: number): number[] => {
  const roundedTarget = Math.round(toNumberSafe(target));
  const sourceTotal = values.reduce((sum, value) => sum + toNumberSafe(value), 0);

  if (!roundedTarget || !sourceTotal || Math.round(sourceTotal) === roundedTarget) {
    return values.map((value) => Math.round(toNumberSafe(value)));
  }

  const scaled = values.map((value) => (toNumberSafe(value) * roundedTarget) / sourceTotal);
  const floors = scaled.map((value) => Math.floor(value));
  let remainder = roundedTarget - floors.reduce((sum, value) => sum + value, 0);

  const order = scaled
    .map((value, index) => ({ index, fraction: value - Math.floor(value) }))
    .sort((a, b) => b.fraction - a.fraction);

  for (const item of order) {
    if (remainder <= 0) break;
    floors[item.index] += 1;
    remainder -= 1;
  }

  return floors;
};

const scaleMoneyBreakdown = (values: number[], target: number): number[] => {
  const sourceTotal = values.reduce((sum, value) => sum + toNumberSafe(value), 0);
  const targetValue = toNumberSafe(target);

  if (!targetValue || !sourceTotal || Math.abs(sourceTotal - targetValue) < 0.005) {
    return values.map((value) => Number(toNumberSafe(value).toFixed(2)));
  }

  const ratio = targetValue / sourceTotal;
  const scaled = values.map((value) => Number((toNumberSafe(value) * ratio).toFixed(2)));
  const residual = Number((targetValue - scaled.reduce((sum, value) => sum + value, 0)).toFixed(2));
  const adjustIndex = scaled.reduce(
    (bestIndex, value, index) => Math.abs(value) >= Math.abs(scaled[bestIndex] ?? 0) ? index : bestIndex,
    0
  );

  if (scaled.length) {
    scaled[adjustIndex] = Number((scaled[adjustIndex] + residual).toFixed(2));
  }

  return scaled;
};

const currencyFromCountryName = (countryName: string) => {
  const c = (countryName || "").toLowerCase();
  if (c === "uk") return "GBP";
  if (c === "us") return "USD";
  // add more mappings later if needed
  return "USD";
};

const fmtPct = (p: number) => `${Math.abs(p).toFixed(2)}%`;


const pctOfSales = (value: number, sales: number) => {
  const s = toNumberSafe(sales);
  if (s <= 0) return 0;
  return (toNumberSafe(value) / s) * 100;
};

const fmtPctDelta = (p: number) =>
  `${Math.abs(toNumberSafe(p)).toFixed(2)}%`;

const fmtPctPlain = (p: number) => `${toNumberSafe(p).toFixed(2)}%`;



// --- Fee buckets helpers ---

// Customize these patterns to match your data keys/naming
const FBA_KEYS = ["fba_fees", "fulfillment_fees"];
const PLATFORM_KEYS = ["platform_fees", "marketplace_fees"];
const OTHER_KEYS = ["other_fees", "misc_fees"];


/* ===================== Product Detail Drawer ===================== */
type DrawerRecommendation = {
  journey_summary?: string[];
  recommendation?: string;
  inventory_recommendation?: string;
  ads_recommendation?: string;
};

type DrawerRecommendationsMap = Record<string, DrawerRecommendation | any>;

type DrawerMetric = {
  label: string;
  value: string;
  color?: string;
};

type DrawerProductBlock = {
  name: string;
  skuKey?: string;
  metrics: DrawerMetric[];
  drawerOnlyMetrics?: DrawerMetric[];
  journeyBullets: string[];
  recommendationBullets: string[];
  inventoryBullets: string[];
};

type DrawerAiState = {
  blocks: DrawerProductBlock[];
  recommendationsMap: DrawerRecommendationsMap;
  periodText: string;
};

type DrawerBestPerformanceMetric = {
  month?: string;
  year?: string | number;
  units?: number;
  net_sales?: number;
  asp?: number;
  cm1_profit?: number;
  unit_wise_profitability?: number;
};

type DrawerBestPerformanceData = {
  units?: DrawerBestPerformanceMetric;
  net_sales?: DrawerBestPerformanceMetric;
  asp?: DrawerBestPerformanceMetric;
  cm1_profit?: DrawerBestPerformanceMetric;
  unit_wise_profitability?: DrawerBestPerformanceMetric;
};

const normalizeDrawerKey = (value: string) =>
  String(value || "")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[^\w\s-]/g, "");

const drawerMonthIndex: Record<string, number> = {
  january: 0,
  february: 1,
  march: 2,
  april: 3,
  may: 4,
  june: 5,
  july: 6,
  august: 7,
  september: 8,
  october: 9,
  november: 10,
  december: 11,
};

const monthNameToNumberForDrawer = (month: string) => {
  const index = drawerMonthIndex[String(month || "").toLowerCase()];
  return typeof index === "number" ? String(index + 1) : "";
};

const drawerMetricCurrent = (metric: any) => {
  if (metric && typeof metric === "object" && "current" in metric) {
    return metric.current;
  }
  return metric;
};

const drawerMetricDelta = (metric: any) => {
  if (metric && typeof metric === "object" && "delta_pct" in metric) {
    return metric.delta_pct;
  }
  return null;
};

const drawerMetricPrevious = (metric: any) => {
  if (metric && typeof metric === "object" && "previous" in metric) {
    return metric.previous;
  }
  return undefined;
};

const drawerMoney = (value: any, symbol: string, decimals = 2) => {
  const n = toNumberSafe(drawerMetricCurrent(value));
  return `${symbol}${n.toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })}`;
};

const drawerMoneyRounded = (value: any, symbol: string) => {
  const n = Math.round(toNumberSafe(drawerMetricCurrent(value)));
  return `${symbol}${n.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  })}`;
};

const drawerDeltaText = (delta: any) => {
  if (delta === null || delta === undefined || !Number.isFinite(Number(delta))) {
    return "";
  }
  const n = Number(delta);
  return ` (${n >= 0 ? "+" : ""}${n.toFixed(2)}%)`;
};

const formatDrawerMetricValue = (
  metric: any,
  type: "money" | "number",
  symbol: string
) => {
  const current = drawerMetricCurrent(metric);
  const delta = drawerMetricDelta(metric);
  const main =
    type === "money"
      ? drawerMoney(current, symbol)
      : Math.round(toNumberSafe(current)).toLocaleString();
  return `${main}${drawerDeltaText(delta)}`;
};

const formatDrawerCoverage = (value: any) => {
  const n = toNumberSafe(drawerMetricCurrent(value));
  return n > 0 ? n.toFixed(2) : "-";
};

const formatDrawerInventory = (value: any) => {
  const n = toNumberSafe(drawerMetricCurrent(value));
  return Number.isFinite(n) ? Math.round(n).toLocaleString() : "-";
};

const hasRealDrawerCm2 = (row: any) => {
  const cm1 = toNumberSafe(drawerMetricCurrent(row?.profit));
  const cm2Raw = drawerMetricCurrent(row?.cm2_profit);
  if (cm2Raw === null || cm2Raw === undefined || cm2Raw === "") return false;

  const cm2 = toNumberSafe(cm2Raw);
  const ads = Math.abs(
    toNumberSafe(
      drawerMetricCurrent(
        row?.productwise_ads_spend ?? row?.ads_spend ?? row?.advertising_total
      )
    )
  );

  if (Math.abs(cm1 - cm2) < 0.01) return false;
  if (Math.abs(cm2) < 0.01 && ads < 0.01) return false;
  return true;
};

const parseDrawerMdSections = (md?: string | null): Record<string, string[]> => {
  if (!md) return {};

  const sections: Record<string, string[]> = { ROOT: [] };
  let current = "ROOT";

  for (const raw of md.split(/\r?\n/)) {
    const line = raw.trim();
    if (!line) continue;

    if (line.toLowerCase().startsWith("## ")) {
      current = line.replace(/^##\s+/i, "").trim().toUpperCase();
      if (!sections[current]) sections[current] = [];
      continue;
    }

    sections[current].push(line.replace(/^[-*•]\s+/, "").trim());
  }

  return sections;
};

const splitDrawerBullets = (text?: string) => {
  if (!text) return [];
  const clean = String(text).trim();
  if (!clean) return [];

  if (clean.includes("\n")) {
    return clean
      .split("\n")
      .map((line) => line.replace(/^[-*•]\s+/, "").trim())
      .filter(Boolean);
  }

  return clean
    .split(/(?:\.\s+|;\s+|\s\|\s)/g)
    .map((line) => line.trim())
    .filter(Boolean);
};

const cleanDrawerInventoryText = (text?: string) =>
  String(text || "")
    .replace(
      /^Your coverage ratio is\s*[\d.]+\s*months\s*(?:and\s*)?/i,
      ""
    )
    .replace(/^and\s+/i, "")
    .trim();

const finalizeDrawerBlock = (
  block: DrawerProductBlock,
  range: Range
): DrawerProductBlock => {
  const metrics = [...(block.metrics || [])];

  const cm1 = metrics.find(
    (metric) => metric.label.trim().toLowerCase() === "cm1 profit"
  );
  const cm2 = metrics.find(
    (metric) => metric.label.trim().toLowerCase() === "cm2 profit"
  );

  const getMainNumber = (value?: string) => {
    const main = String(value || "").split("(")[0];
    const n = Number(main.replace(/[^0-9.-]/g, ""));
    return Number.isFinite(n) ? n : 0;
  };

  const useCm1 =
    range !== "monthly" ||
    !cm2 ||
    (cm1 && Math.abs(getMainNumber(cm1.value) - getMainNumber(cm2.value)) < 0.01);

  const cleanedMetrics = metrics.filter((metric) => {
    const label = metric.label.trim().toLowerCase();

    if (range !== "monthly" && label === "stock cover") return false;

    if (useCm1 && ["cm2 profit", "cm2 profit per unit"].includes(label)) {
      return false;
    }

    if (!useCm1 && ["cm1 profit", "cm1 profit per unit"].includes(label)) {
      return false;
    }

    return true;
  });

  return { ...block, metrics: cleanedMetrics };
};

const parseDrawerProductBlocks = (
  lines: string[],
  range: Range
): DrawerProductBlock[] => {
  const metricLabels = [
    "ASP",
    "Units",
    "Net sales",
    "CM1 profit",
    "CM1 profit per unit",
    "CM2 profit",
    "CM2 profit per unit",
    "Productwise ads spend",
    "Stock Cover",
    "Coverage ratio",
    "Current inventory",
    "Current Inventory",
  ];

  const isMetric = (line: string) =>
    metricLabels.some((label) =>
      line.toLowerCase().startsWith(`${label.toLowerCase()}:`)
    );

  const blocks: DrawerProductBlock[] = [];
  let current: DrawerProductBlock | null = null;
  let inJourney = false;

  const pushCurrent = () => {
    if (current?.name?.trim()) {
      blocks.push(finalizeDrawerBlock(current, range));
    }
    current = null;
  };

  for (let i = 0; i < lines.length; i += 1) {
    const line = String(lines[i] || "")
      .replace(/^[-*•]\s+/, "")
      .replace(/^\d+\.\s*/, "")
      .trim();

    if (!line) continue;

    const nextLine = String(lines[i + 1] || "")
      .replace(/^[-*•]\s+/, "")
      .replace(/^\d+\.\s*/, "")
      .trim();

    const lower = line.toLowerCase();
    const isHeader =
      !isMetric(line) &&
      !lower.startsWith("sku:") &&
      !lower.startsWith("bucket:") &&
      !lower.startsWith("recommendation:") &&
      !lower.startsWith("ads action:") &&
      !lower.startsWith("inventory action:") &&
      !lower.startsWith("product journey") &&
      Boolean(nextLine) &&
      (isMetric(nextLine) || nextLine.toLowerCase().startsWith("sku:"));

    if (isHeader) {
      pushCurrent();

      const skuFromParen = line.match(/\(([A-Z0-9-]+)\)/i)?.[1]?.trim();
      const skuFromPrefix = line.match(/^([A-Z0-9-]+)\s*[-:]\s*/i)?.[1]?.trim();
      const cleanName = line
        .replace(/\([A-Z0-9-]+\)/i, "")
        .replace(/^([A-Z0-9-]+)\s*[-:]\s*/i, "")
        .trim();

      current = {
        name: cleanName || line,
        skuKey: skuFromParen || skuFromPrefix,
        metrics: [],
        drawerOnlyMetrics: [],
        journeyBullets: [],
        recommendationBullets: [],
        inventoryBullets: [],
      };
      inJourney = false;
      continue;
    }

    if (!current) continue;

    if (lower.startsWith("sku:")) {
      current.skuKey = line.replace(/^sku:\s*/i, "").trim();
      continue;
    }

    if (lower.startsWith("product journey")) {
      inJourney = true;
      continue;
    }

    if (lower.startsWith("recommendation:")) {
      inJourney = false;
      const value = line.replace(/^recommendation:\s*/i, "").trim();
      if (value) current.recommendationBullets.push(value);
      continue;
    }

    if (lower.startsWith("inventory action:")) {
      inJourney = false;
      const value = line.replace(/^inventory action:\s*/i, "").trim();
      if (value) current.inventoryBullets.push(value);
      continue;
    }

    if (lower.startsWith("ads action:")) {
      inJourney = false;
      const value = line.replace(/^ads action:\s*/i, "").trim();
      if (value) current.recommendationBullets.push(value);
      continue;
    }

    if (isMetric(line)) {
      const [rawLabel, ...rest] = line.split(":");
      const rawValue = rest.join(":").trim();
      const normalizedLabel = rawLabel.trim().toLowerCase();
      const label =
        normalizedLabel === "coverage ratio" ? "Stock Cover" : rawLabel.trim();

      const numeric = Number(
        String(rawValue).split("(")[0].replace(/[^0-9.-]/g, "")
      );
      const color = Number.isFinite(numeric)
        ? numeric < 0
          ? "#DC2626"
          : numeric > 0
            ? "#059669"
            : "#414042"
        : "#414042";

      if (normalizedLabel === "current inventory") {
        if (range === "monthly") {
          current.drawerOnlyMetrics?.push({
            label: "Current Inventory",
            value: rawValue,
            color,
          });
        }
        continue;
      }

      if (normalizedLabel === "productwise ads spend") {
        if (range === "monthly") {
          current.drawerOnlyMetrics?.push({
            label: "Ads",
            value: rawValue,
            color: "#414042",
          });
        }
        continue;
      }

      current.metrics.push({ label, value: rawValue, color });
      continue;
    }

    if (inJourney) {
      const value = line.replace(/^-+\s*/, "").trim();
      if (value) current.journeyBullets.push(value);
    }
  }

  pushCurrent();
  return blocks;
};

const drawerRecommendationSource = (recommendations: any): DrawerRecommendationsMap => {
  if (!recommendations || typeof recommendations !== "object") return {};
  return (
    recommendations?.sku_actions ??
    recommendations?.recommendations ??
    recommendations ??
    {}
  );
};

const findDrawerRecommendation = (
  map: DrawerRecommendationsMap,
  block: DrawerProductBlock,
  fallbackSku?: string
) => {
  const sku = String(block.skuKey || fallbackSku || "").trim();
  return (
    (sku && map?.[sku]) ||
    map?.[block.name] ||
    map?.[block.name.trim()] ||
    Object.entries(map || {}).find(
      ([key]) => normalizeDrawerKey(key) === normalizeDrawerKey(block.name)
    )?.[1] ||
    null
  );
};

const mergeDrawerRecommendationsIntoBlocks = (
  blocks: DrawerProductBlock[],
  recommendationsMap: DrawerRecommendationsMap
) =>
  blocks.map((block) => {
    const recObj = findDrawerRecommendation(recommendationsMap, block);

    return {
      ...block,
      journeyBullets:
        block.journeyBullets.length > 0
          ? block.journeyBullets
          : Array.isArray(recObj?.journey_summary)
            ? recObj.journey_summary
            : [],
      recommendationBullets:
        block.recommendationBullets.length > 0
          ? block.recommendationBullets
          : splitDrawerBullets(recObj?.recommendation),
      inventoryBullets:
        block.inventoryBullets.length > 0
          ? block.inventoryBullets
          : splitDrawerBullets(recObj?.inventory_recommendation),
    };
  });

const getGlobalDrawerRow = (source: any, productName: string) => {
  if (!source || typeof source !== "object") return {};

  return (
    source?.[productName] ||
    Object.values(source).find(
      (row: any) =>
        normalizeDrawerKey(String(row?.product_name || "")) ===
        normalizeDrawerKey(productName)
    ) ||
    {}
  );
};

const buildGlobalDrawerAiState = (
  data: any,
  range: Range,
  symbol: string,
  fallbackPeriodText: string
): DrawerAiState => {
  const products = Array.isArray(data?.global_ai?.product_journey_comparison)
    ? data.global_ai.product_journey_comparison
    : [];
  const skuCurrent = data?.metrics?.sku_current ?? {};
  const skuMom = data?.metrics?.sku_mom ?? {};

  const recommendationsMap: DrawerRecommendationsMap = {};

  const blocks = products.map((product: any) => {
    const productName = String(product?.product_name || "Unknown Product").trim();
    const currentRow = getGlobalDrawerRow(skuCurrent, productName);
    const momRow = getGlobalDrawerRow(skuMom, productName);

    const cm1Metric = momRow?.profit ?? currentRow?.profit;
    const cm1PerUnitMetric =
      momRow?.unit_wise_profitability ?? currentRow?.unit_wise_profitability;
    const cm2Metric = momRow?.cm2_profit ?? currentRow?.cm2_profit;
    const cm2PerUnitMetric =
      momRow?.cm2_profit_per_unit ??
      momRow?.cm2_profit_per ??
      currentRow?.cm2_profit_per_unit ??
      currentRow?.cm2_profit_per;

    const metrics: DrawerMetric[] = [
      {
        label: "Units",
        value: formatDrawerMetricValue(
          momRow?.total_quantity ?? currentRow?.total_quantity,
          "number",
          symbol
        ),
      },
      {
        label: "Net sales",
        value: formatDrawerMetricValue(
          momRow?.net_sales ?? currentRow?.net_sales,
          "money",
          symbol
        ),
      },
      {
        label: "ASP",
        value: formatDrawerMetricValue(
          momRow?.asp ?? currentRow?.asp,
          "money",
          symbol
        ),
      },
    ];

    if (range === "monthly" && hasRealDrawerCm2(currentRow)) {
      metrics.push(
        {
          label: "CM2 profit",
          value: formatDrawerMetricValue(cm2Metric, "money", symbol),
        },
        {
          label: "CM2 profit per unit",
          value: formatDrawerMetricValue(cm2PerUnitMetric, "money", symbol),
        }
      );
    } else {
      metrics.push(
        {
          label: "CM1 profit",
          value: formatDrawerMetricValue(cm1Metric, "money", symbol),
        },
        {
          label: "CM1 profit per unit",
          value: formatDrawerMetricValue(cm1PerUnitMetric, "money", symbol),
        }
      );
    }

    if (range === "monthly") {
      metrics.push({
        label: "Stock Cover",
        value: formatDrawerCoverage(currentRow?.selected_period_coverage_ratio),
      });
    }

    const actions = product?.country_actions ?? {};
    const uk = actions?.uk ?? {};
    const us = actions?.us ?? {};

    const recommendation = [
      uk?.recommendation ? `UK: ${uk.recommendation}` : "",
      us?.recommendation ? `US: ${us.recommendation}` : "",
    ]
      .filter(Boolean)
      .join("\n");

    const inventoryRecommendation = [
      uk?.inventory_recommendation
        ? `UK: ${uk.inventory_recommendation}`
        : "",
      us?.inventory_recommendation
        ? `US: ${us.inventory_recommendation}`
        : "",
    ]
      .filter(Boolean)
      .join("\n");

    const adsRecommendation = [
      uk?.ads_recommendation ? `UK: ${uk.ads_recommendation}` : "",
      us?.ads_recommendation ? `US: ${us.ads_recommendation}` : "",
    ]
      .filter(Boolean)
      .join("\n");

    recommendationsMap[productName] = {
      journey_summary: product?.journey_comparison ?? [],
      recommendation,
      inventory_recommendation: inventoryRecommendation,
      ads_recommendation: adsRecommendation,
    };

    return {
      name: productName,
      metrics,
      drawerOnlyMetrics:
        range === "monthly"
          ? [
              {
                label: "Current Inventory",
                value: formatDrawerInventory(currentRow?.current_inventory),
              },
              {
                label: "Ads",
                value: formatDrawerMetricValue(
                  momRow?.productwise_ads_spend ??
                    currentRow?.productwise_ads_spend,
                  "money",
                  symbol
                ),
              },
            ]
          : [],
      journeyBullets: Array.isArray(product?.journey_comparison)
        ? product.journey_comparison
        : [],
      recommendationBullets: splitDrawerBullets(recommendation),
      inventoryBullets: splitDrawerBullets(inventoryRecommendation),
    } as DrawerProductBlock;
  });

  const comparison = data?.comparison ?? {};
  const periodLabel = String(comparison?.period_label || "").trim();
  const previousLabel = String(
    comparison?.previous_period_label || comparison?.previous_label || ""
  ).trim();

  return {
    blocks,
    recommendationsMap,
    periodText:
      periodLabel && previousLabel
        ? `(${periodLabel} vs ${previousLabel})`
        : fallbackPeriodText,
  };
};

const buildDrawerPeriodText = (
  range: Range,
  month: string,
  quarter: string,
  year: string
) => {
  if (range === "yearly") {
    return `(${year} vs ${Number(year) - 1})`;
  }

  if (range === "quarterly") {
    const order = ["Q1", "Q2", "Q3", "Q4"];
    const index = order.indexOf(String(quarter || "").toUpperCase());
    if (index === -1) return `(${quarter} ${year})`;
    const previousQuarter = order[index === 0 ? 3 : index - 1];
    const previousYear = index === 0 ? Number(year) - 1 : Number(year);
    return `(${quarter}'${year.slice(-2)} vs ${previousQuarter}'${String(previousYear).slice(-2)})`;
  }

  const monthIndex = drawerMonthIndex[String(month || "").toLowerCase()];
  if (typeof monthIndex !== "number") return `(${month} ${year})`;
  const currentDate = new Date(Number(year), monthIndex, 1);
  const previousDate = new Date(Number(year), monthIndex - 1, 1);
  const currentLabel = currentDate.toLocaleString("en-US", { month: "short" });
  const previousLabel = previousDate.toLocaleString("en-US", { month: "short" });
  return `(${currentLabel}'${String(currentDate.getFullYear()).slice(-2)} vs ${previousLabel}'${String(previousDate.getFullYear()).slice(-2)})`;
};

const getDrawerSummaryPeriodText = (
  summaryLines: string[],
  fallback: string
) => {
  const first = String(summaryLines?.[0] || "");
  const match = first.match(/\(([^)]+)\)/);
  return match?.[1] ? `(${match[1]})` : fallback;
};

const splitDrawerMetricValue = (value: string) => {
  const text = String(value || "").trim();
  const match = text.match(/^(.+?)\s*(\(([+-]?)[^)]+\))\s*$/);

  if (!match) {
    return { main: text, delta: "", deltaColor: "" };
  }

  return {
    main: match[1].trim(),
    delta: match[2].trim(),
    deltaColor:
      match[3] === "+"
        ? "text-emerald-600"
        : match[3] === "-"
          ? "text-red-600"
          : "text-charcoal-500",
  };
};

const formatDrawerDeltaDisplay = (delta: string) => {
  const clean = String(delta || "").replace(/[()]/g, "").trim();
  if (!clean) return "";
  if (clean.startsWith("+")) return `▲ ${clean.slice(1)}`;
  if (clean.startsWith("-")) return `▼ ${clean.slice(1)}`;
  return clean;
};

const formatDrawerMetricTitle = (label: string) =>
  String(label || "")
    .replace(/\b\w/g, (char) => char.toUpperCase())
    .replace("Cm1", "CM1")
    .replace("Cm2", "CM2");

const formatDrawerMetricLabel = (label: string) =>
  String(label || "").trim().toLowerCase() === "stock cover"
    ? "Stock Cover (Months)"
    : formatDrawerMetricTitle(label);

const formatDrawerMainValue = (label: string, main: string) => {
  const normalized = String(label || "").trim().toLowerCase();
  if (!["net sales", "cm1 profit", "cm2 profit", "ads"].includes(normalized)) {
    return main;
  }

  const currencyMatch = String(main || "").match(/^([^0-9-]*)/);
  const currency = currencyMatch?.[1] ?? "";
  const numberValue = Number(String(main || "").replace(/[^0-9.-]/g, ""));
  if (!Number.isFinite(numberValue)) return main;
  return `${currency}${Math.round(numberValue).toLocaleString()}`;
};

const formatDrawerBestPeriod = (month?: string, year?: string | number) => {
  if (!month) return "-";
  const shortMonth = String(month).slice(0, 3);
  const shortYear = year ? String(year).slice(-2) : "";
  return shortYear ? `${shortMonth.charAt(0).toUpperCase()}${shortMonth.slice(1).toLowerCase()}'${shortYear}` : shortMonth;
};

const drawerMetricColors = [
  "border border-[#FDD36F] border-t-4",
  "border border-[#75BBDA] border-t-4",
  "border border-[#B75A5A] border-t-4",
  "border border-[#C49466] border-t-4",
  "border border-[#7B9A6D] border-t-4",
  "border border-[#C49466] border-t-4",
  "border border-[#7B9A6D] border-t-4",
  "border border-[#C49466] border-t-4",
  "border border-[#7B9A6D] border-t-4",
  "border border-[#C49466] border-t-4",
];

const drawerMetricOrder = [
  "units",
  "net sales",
  "asp",
  "ads",
  "cm2 profit",
  "cm2 profit per unit",
  "cm1 profit",
  "cm1 profit per unit",
  "current inventory",
  "stock cover",
];

type ReferralProductDrawerProps = {
  open: boolean;
  onClose: () => void;
  block: DrawerProductBlock | null;
  productName: string;
  recObj?: any;
  countryName: string;
  month: string;
  year: string;
  range: Range;
  quarter: string;
  drawerPeriodText: string;
  currencySymbol: string;
  homeCurrency: string;
  aiLoading: boolean;
  aiError: string | null;
};

function ReferralProductDrawer({
  open,
  onClose,
  block,
  productName,
  recObj,
  countryName,
  month,
  year,
  range,
  quarter,
  drawerPeriodText,
  currencySymbol,
  homeCurrency,
  aiLoading,
  aiError,
}: ReferralProductDrawerProps) {
  const [bestLoading, setBestLoading] = useState(false);
  const [bestError, setBestError] = useState<string | null>(null);
  const [bestData, setBestData] = useState<DrawerBestPerformanceData | null>(null);

  useEffect(() => {
    // Wait for /summary to resolve and match the clicked product first.
    // This keeps all drawer sections in sync and prevents Best Performance
    // from being the only request fired for an empty product block.
    if (!open || !productName || !block) return;

    const controller = new AbortController();

    const fetchBestPerformance = async () => {
      try {
        setBestLoading(true);
        setBestError(null);
        setBestData(null);

        const token =
          typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
        if (!token) throw new Error("Missing token");

        const response = await fetch(`${baseURL}/ProductBestPerformance`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify({
            product_name: productName,
            country: countryName,
            home_currency:
              countryName.toLowerCase() === "global"
                ? homeCurrency
                : currencySymbol,
          }),
          cache: "no-store",
          signal: controller.signal,
        });

        const json = await response.json().catch(() => ({}));
        if (!response.ok) {
          throw new Error(json?.error || "Failed to fetch best performance");
        }

        setBestData(json?.best_performance ?? null);
      } catch (error: any) {
        if (error?.name === "AbortError") return;
        setBestError(error?.message || "Failed to load best performance");
      } finally {
        setBestLoading(false);
      }
    };

    fetchBestPerformance();
    return () => controller.abort();
  }, [open, productName, block, countryName, homeCurrency, currencySymbol]);

  if (!open) return null;

  const actionBullets =
    block?.recommendationBullets?.length
      ? block.recommendationBullets
      : splitDrawerBullets(recObj?.recommendation);

  const inventoryBullets = (
    block?.inventoryBullets?.length
      ? block.inventoryBullets
      : splitDrawerBullets(recObj?.inventory_recommendation)
  )
    .map(cleanDrawerInventoryText)
    .filter(Boolean);

  const adsBullets = splitDrawerBullets(recObj?.ads_recommendation);
  const journeyBullets =
    block?.journeyBullets?.length
      ? block.journeyBullets
      : Array.isArray(recObj?.journey_summary)
        ? recObj.journey_summary
        : [];

  const hasCm2 = (block?.metrics || []).some((metric) =>
    ["cm2 profit", "cm2 profit per unit"].includes(
      metric.label.trim().toLowerCase()
    )
  );

  const sortedMetrics = [
    ...(block?.metrics || []),
    ...(block?.drawerOnlyMetrics || []),
  ]
    .filter((metric) => {
      const label = metric.label.trim().toLowerCase();
      const isGlobal = countryName.toLowerCase() === "global";

      if (
        range === "monthly" &&
        label === "ads" &&
        !hasCm2
      ) {
        return false;
      }

      if (
        isGlobal &&
        ["stock cover", "current inventory"].includes(label)
      ) {
        return false;
      }

      if (range !== "monthly") {
        return ![
          "ads",
          "stock cover",
          "current inventory",
          "cm2 profit",
          "cm2 profit per unit",
        ].includes(label);
      }

      return true;
    })
    .sort((a, b) => {
      const ai = drawerMetricOrder.indexOf(a.label.toLowerCase());
      const bi = drawerMetricOrder.indexOf(b.label.toLowerCase());
      return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi);
    });

  const getMetricBorder = (label: string, fallbackIndex: number) => {
    const index = drawerMetricOrder.indexOf(label.trim().toLowerCase());
    return drawerMetricColors[
      index === -1 ? fallbackIndex % drawerMetricColors.length : index
    ];
  };

  const previousCompletedMonth = (() => {
    const date = new Date();
    date.setMonth(date.getMonth() - 1);
    return {
      month: date.toLocaleString("en-US", { month: "long" }).toLowerCase(),
      year: String(date.getFullYear()),
      quarter: getQuarterFromMonth(
        date.toLocaleString("en-US", { month: "long" }).toLowerCase()
      ),
    };
  })();

  const showRecommendations =
    range === "yearly"
      ? year === previousCompletedMonth.year
      : range === "quarterly"
        ? year === previousCompletedMonth.year &&
          quarter === previousCompletedMonth.quarter
        : year === previousCompletedMonth.year &&
          month.toLowerCase() === previousCompletedMonth.month;

  return (
    <AnimatePresence>
      <>
        <motion.div
          className="fixed inset-0 z-[999999] h-full bg-black/40"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          onClick={onClose}
        />

        <motion.aside
          className="fixed right-0 top-0 z-[1000000] h-screen w-[95vw] bg-white shadow-2xl sm:w-[75vw] lg:w-[50vw]"
          initial={{ x: 520 }}
          animate={{ x: 0 }}
          exit={{ x: 520 }}
          transition={{ type: "tween", duration: 0.25 }}
        >
          <div className="flex h-full flex-col gap-4">
            <div className="flex shrink-0 items-center justify-between gap-3 border-b border-slate-200 p-3">
              <div className="min-w-0 flex-1">
                <div className="flex flex-col gap-1 sm:flex-row sm:items-center">
                  <PageBreadcrumb
                    pageTitle="Detailed View - "
                    variant="page"
                    textSize="2xl"
                  />
                  <div className="flex flex-wrap items-center gap-1">
                    <span className="text-base font-bold text-green-500 sm:text-xl lg:text-lg 2xl:text-2xl">
                      {productName || block?.name || "Details"}
                    </span>
                    <span className="text-base font-bold text-green-500 sm:text-xl lg:text-lg 2xl:text-2xl">
                      {drawerPeriodText}
                    </span>
                  </div>
                </div>
              </div>

              <button
                type="button"
                onClick={onClose}
                className="shrink-0 rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
                aria-label="Close product detail"
              >
                x
              </button>
            </div>

            <div className="flex-1 space-y-6 overflow-y-auto px-3 pb-4">
              {aiLoading && !block ? (
                <div className="flex min-h-[240px] items-center justify-center">
                  <Loader fullscreen={false} transparent />
                </div>
              ) : aiError && !block ? (
                <div className="rounded-lg border border-red-100 bg-red-50 px-3 py-3 text-sm text-red-600">
                  {aiError}
                </div>
              ) : !block ? (
                <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-charcoal-500">
                  Product insight data is not available for this period.
                </div>
              ) : (
                <>
                  <div>
                    <PageBreadcrumb
                      pageTitle="Metrics"
                      variant="page"
                      align="left"
                      textSize="xl"
                      className="mb-2"
                    />

                    <div
                      className={
                        range === "monthly"
                          ? "grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-4"
                          : "grid grid-cols-2 gap-3 sm:grid-cols-3 min-[1700px]:grid-cols-5"
                      }
                    >
                      {sortedMetrics.map((metric, index) => {
                        const { main, delta, deltaColor } =
                          splitDrawerMetricValue(metric.value);
                        const displayMain = formatDrawerMainValue(
                          metric.label,
                          main
                        );

                        return (
                          <div
                            key={`${metric.label}-${index}`}
                            className={[
                              "flex min-h-[60px] w-full flex-col justify-between rounded-xl bg-white p-1.5 shadow-sm 2xl:p-2",
                              getMetricBorder(metric.label, index),
                            ].join(" ")}
                          >
                            <span className="text-[10px] font-medium text-charcoal-500 2xl:text-xs">
                              {formatDrawerMetricLabel(metric.label)}
                            </span>

                            <div className="mt-1 flex items-baseline justify-between gap-3 leading-tight tabular-nums">
                              <span className="truncate text-sm font-semibold text-charcoal-500 2xl:text-lg">
                                {displayMain}
                              </span>
                              {delta ? (
                                <span
                                  className={`whitespace-nowrap text-right text-[10px] font-semibold 2xl:text-xs ${
                                    metric.label.toLowerCase() === "ads"
                                      ? "text-charcoal-500"
                                      : deltaColor
                                  }`}
                                >
                                  {formatDrawerDeltaDisplay(delta)}
                                </span>
                              ) : [
                                  "cm1 profit",
                                  "cm1 profit per unit",
                                  "cm2 profit",
                                  "cm2 profit per unit",
                                ].includes(metric.label.trim().toLowerCase()) ? (
                                <span className="whitespace-nowrap text-right text-[10px] font-semibold text-charcoal-400 2xl:text-xs">
                                  -
                                </span>
                              ) : null}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>

                  <div>
                    <PageBreadcrumb
                      pageTitle="Overall Best Performance"
                      variant="page"
                      align="left"
                      textSize="xl"
                    />
                    <p className="mb-2 mt-1 text-xs text-charcoal-500 2xl:text-sm">
                      Best performance is calculated from overall historical data, not just the selected period.
                    </p>

                    {bestLoading ? (
                      <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-xs text-charcoal-500 2xl:text-sm">
                        Loading best performance...
                      </div>
                    ) : bestError ? (
                      <div className="rounded-lg border border-red-100 bg-red-50 px-3 py-3 text-xs text-red-600 2xl:text-sm">
                        {bestError}
                      </div>
                    ) : bestData ? (
                      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-5">
                        {[
                          {
                            label: "Units",
                            value: Math.round(
                              toNumberSafe(bestData?.units?.units)
                            ).toLocaleString(),
                            period: formatDrawerBestPeriod(
                              bestData?.units?.month,
                              bestData?.units?.year
                            ),
                          },
                          {
                            label: "Net Sales",
                            value: drawerMoneyRounded(
                              bestData?.net_sales?.net_sales,
                              currencySymbol
                            ),
                            period: formatDrawerBestPeriod(
                              bestData?.net_sales?.month,
                              bestData?.net_sales?.year
                            ),
                          },
                          {
                            label: "ASP",
                            value: drawerMoney(
                              bestData?.asp?.asp,
                              currencySymbol
                            ),
                            period: formatDrawerBestPeriod(
                              bestData?.asp?.month,
                              bestData?.asp?.year
                            ),
                          },
                          {
                            label: "CM1 Profit",
                            value: drawerMoneyRounded(
                              bestData?.cm1_profit?.cm1_profit,
                              currencySymbol
                            ),
                            period: formatDrawerBestPeriod(
                              bestData?.cm1_profit?.month,
                              bestData?.cm1_profit?.year
                            ),
                          },
                          {
                            label: "CM1 Profit Per Unit",
                            value: drawerMoney(
                              bestData?.unit_wise_profitability
                                ?.unit_wise_profitability,
                              currencySymbol
                            ),
                            period: formatDrawerBestPeriod(
                              bestData?.unit_wise_profitability?.month,
                              bestData?.unit_wise_profitability?.year
                            ),
                          },
                        ].map((card, index) => (
                          <div
                            key={card.label}
                            className={[
                              "flex min-h-[78px] w-full flex-col justify-between rounded-xl bg-white p-1.5 shadow-sm 2xl:p-2",
                              getMetricBorder(card.label, index),
                            ].join(" ")}
                          >
                            <span className="text-[10px] font-medium text-charcoal-500 2xl:text-xs">
                              {formatDrawerMetricTitle(card.label)}
                            </span>
                            <div className="mt-1 leading-tight tabular-nums">
                              <div className="text-[10px] font-medium text-charcoal-500 2xl:text-xs">
                                {card.period}
                              </div>
                              <div className="mt-1 whitespace-nowrap text-sm font-semibold text-charcoal-500 2xl:text-lg">
                                {card.value}
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-xs text-charcoal-500 2xl:text-sm">
                        -
                      </div>
                    )}
                  </div>

                  {showRecommendations && (
                    <div>
                      <PageBreadcrumb
                        pageTitle="Recommendations"
                        variant="page"
                        align="left"
                        textSize="xl"
                        className="mb-2"
                      />

                      {actionBullets.length > 0 && (
                        <div>
                          <div className="text-xs font-semibold text-charcoal-500 2xl:text-sm">
                            Action
                          </div>
                          <ul className="list-disc space-y-1 pl-5 text-xs text-charcoal-500 2xl:text-sm">
                            {actionBullets.map((item, index) => (
                              <li key={index}>{item}</li>
                            ))}
                          </ul>
                        </div>
                      )}

                      {inventoryBullets.length > 0 && (
                        <div className="mt-2">
                          <div className="text-xs font-semibold text-charcoal-500 2xl:text-sm">
                            Inventory
                          </div>
                          <ul className="list-disc space-y-1 pl-5 text-xs text-charcoal-500 2xl:text-sm">
                            {inventoryBullets.map((item, index) => (
                              <li key={index}>{item}</li>
                            ))}
                          </ul>
                        </div>
                      )}

                      {adsBullets.length > 0 && (
                        <div className="mt-2">
                          <div className="text-xs font-semibold text-charcoal-500 2xl:text-sm">
                            Ads
                          </div>
                          <ul className="list-disc space-y-1 pl-5 text-xs text-charcoal-500 2xl:text-sm">
                            {adsBullets.map((item, index) => (
                              <li key={index}>{item}</li>
                            ))}
                          </ul>
                        </div>
                      )}

                      {actionBullets.length === 0 &&
                        inventoryBullets.length === 0 &&
                        adsBullets.length === 0 && (
                          <div className="text-xs text-charcoal-500 2xl:text-sm">
                            -
                          </div>
                        )}
                    </div>
                  )}

                  <div className="w-full">
                    <Productinfoinpopup
                      productname={block.name || productName}
                      countryName={countryName}
                      isOtherSkus={false}
                      otherSkuProductNames={[]}
                    />
                  </div>

                  <div>
                    <PageBreadcrumb
                      pageTitle="Product Journey"
                      variant="page"
                      textSize="xl"
                      className="mb-2"
                    />
                    {journeyBullets.length > 0 ? (
                      <ol className="list-decimal space-y-1 pl-3 text-xs text-charcoal-500 marker:font-semibold marker:text-charcoal-400 2xl:text-sm">
                        {journeyBullets.map((item: any, index: React.Key | null | undefined) => (
                          <li key={index}>
                            {String(item)
                              .replace(/^\d+\.\s*-\s*/, "")
                              .replace(/^\d+\.\s*/, "")
                              .replace(/^-+\s*/, "")}
                          </li>
                        ))}
                      </ol>
                    ) : (
                      <div className="text-xs text-charcoal-500 2xl:text-sm">
                        -
                      </div>
                    )}
                  </div>
                </>
              )}
            </div>
          </div>
        </motion.aside>
      </>
    </AnimatePresence>
  );
}


function SalesCard({
  title,
  sales,          // Net
  productSales,   // Gross
  units,
  valueFmt,
  borderColor,
  bgColor,
}: {
  title: string;
  sales: number;
  productSales: number;
  units: number;
  valueFmt: (n: number) => string;
  borderColor?: string;
  bgColor?: string;
}) {
  const net = toNumberSafe(sales);
  const gross = toNumberSafe(productSales);
  const discountPct = gross > 0 ? ((gross - net) / gross) * 100 : 0;

  const asp =
    toNumberSafe(units) > 0 ? net / toNumberSafe(units) : 0;

  const currencySymbol = valueFmt(0).replace(/[\d.,\s]/g, "");

  return (
    <div
      className="rounded-2xl border shadow-sm px-4 py-3 flex flex-col min-h-[110px]"
      style={{ borderColor: borderColor ?? "#cbd5e1", backgroundColor: bgColor ?? "#fff" }}
    >
      <p className="text-xs font-semibold text-charcoal-500">{title}</p>

      {/* 3x2 Grid */}
      <div className="mt-2 grid grid-cols-3 grid-rows-2 gap-2 text-left">
        {/* Row 1 */}
        <div>
          <p className="text-[10px] sm:text-[11px] text-slate-500">Gross</p>
          <p className="text-[11px] sm:text-xs font-bold text-charcoal-500">
            {valueFmt(gross)}
          </p>
        </div>

        <div>
          <p className="text-[10px] sm:text-[11px] text-slate-500">Net</p>
          <p className="text-[11px] sm:text-xs font-bold text-charcoal-500">
            {valueFmt(net)}
          </p>
        </div>

        <div>
          <p className="text-[10px] sm:text-[11px] text-slate-500">Disc.</p>
          <p className="text-[11px] sm:text-xs font-bold text-charcoal-500 tabular-nums">
            {discountPct.toFixed(2)}%
          </p>
        </div>

        {/* Row 2 */}
        <div>
          <p className="text-[10px] sm:text-[11px] text-slate-500">Units Sold</p>
          <p className="text-[11px] sm:text-xs font-semibold text-charcoal-500">
            {fmtInteger(Math.round(toNumberSafe(units)))}
          </p>
        </div>

        <div>
          <p className="text-[10px] sm:text-[11px] text-slate-500">ASP</p>
          <p className="text-[11px] sm:text-xs font-semibold text-charcoal-500 tabular-nums">
            {currencySymbol}
            {asp.toFixed(2)}
          </p>
        </div>

        {/* Empty cell (future use / keeps grid symmetric) */}
        <div />
      </div>
    </div>
  );
}


function FeeCard({
  title,
  sales,
  charged,
  applicable,
  fmtCurrency,
  borderColor,
  bgColor,
}: {
  title: string;
  sales: number;
  charged: number;
  applicable: number;
  fmtCurrency: (n: number) => string;
  borderColor?: string;
  bgColor?: string;
}) {
  const chargedPct = pctOfSales(charged, sales);
  const applicablePct = pctOfSales(applicable, sales);

  // delta = charged% - applicable%
  const deltaPct = chargedPct - applicablePct;

  const deltaCls =
    deltaPct > 0
      ? "text-emerald-600"
      : deltaPct < 0
        ? "text-red-600"
        : "text-slate-500";

  return (
    <div
      className="rounded-2xl border shadow-sm px-4 py-3 flex flex-col min-h-[110px]"
      style={{ borderColor: borderColor ?? "#cbd5e1", backgroundColor: bgColor ?? "#fff" }}
    >
      {/* Heading */}
      <p className="text-xs font-semibold text-charcoal-500">{title}</p>

      {/* Charged label */}
      <p className="mt-1 text-[11px] sm:text-xs text-slate-600">Charged</p>

      {/* Charged row */}
      <div className="flex items-baseline gap-3">
        <p className="text-[11px] sm:text-xs font-bold text-charcoal-500">
          {fmtCurrency(toNumberSafe(charged))}
        </p>
        <p className="text-[11px] sm:text-xs font-bold text-emerald-600 whitespace-nowrap">
          ({fmtPctPlain(chargedPct)})
        </p>
      </div>

      {/* Applicable label */}
      <p className="mt-auto pt-2 text-[11px] sm:text-xs text-slate-600">Applicable</p>

      {/* Applicable row + delta */}
      <div className="grid grid-cols-[1fr_auto] items-end gap-2">
        <div className="flex items-baseline gap-3">
          <p className="text-[11px] sm:text-xs font-semibold text-charcoal-500 whitespace-nowrap">
            {Number.isFinite(applicable) ? fmtCurrency(applicable) : "Unavailable"}
          </p>
          <p className="text-[11px] sm:text-xs font-bold text-emerald-600 whitespace-nowrap">
            {Number.isFinite(applicable) ? `(${fmtPctPlain(applicablePct)})` : ""}
          </p>
        </div>

        <div
          className={`text-[11px] sm:text-xs font-bold whitespace-nowrap flex items-center justify-end ${deltaCls}`}
        >
          {deltaPct > 0 && <span className="text-sm leading-none">▲</span>}
          {deltaPct < 0 && <span className="text-sm leading-none">▼</span>}
          <span>{Number.isFinite(applicable) ? fmtPctDelta(deltaPct) : "—"}</span>
        </div>
      </div>
    </div>
  );
}

function PreviewLockedSection({
  enabled,
  children,
  title,
  description,
  buttonText,
  onAction,
}: {
  enabled: boolean;
  children: React.ReactNode;
  title?: string;
  description?: string;
  buttonText?: string;
  onAction?: () => void;
}) {
  return (
    <div className="relative w-full">
      <div
        className={
          enabled
            ? "pointer-events-none select-none opacity-45 transition-all duration-300"
            : "opacity-100 transition-all duration-300"
        }
      >
        {children}
      </div>

      {enabled && (
        <>
          <div className="absolute inset-0 z-10 rounded-xl bg-white/45" />

          <div className="absolute inset-0 z-20 pointer-events-none">
            <div className="sticky top-[18vh] sm:top-[20vh] lg:top-[22vh] 2xl:top-[24vh] flex justify-center px-4">
              <div className="pointer-events-auto w-full max-w-md rounded-2xl bg-white shadow-2xl p-6 text-center">
                <div className="mb-4 flex justify-center">
                  <div className="flex h-16 w-16 items-center justify-center rounded-full bg-[#37455F]">
                    <IoMdLock className="text-3xl text-[#F8EDCE]" />
                  </div>
                </div>

                <h3 className="text-lg font-semibold text-[#414042]">
                  {title}
                </h3>

                <p className="mt-2 text-sm text-gray-600 leading-6">
                  {description}
                </p>

                {buttonText && (
                  <button
                    onClick={onAction}
                    className="mt-4 rounded-md bg-[#37455F] px-4 py-2 text-sm text-[#F8EDCE] hover:opacity-90 transition"
                  >
                    {buttonText}
                  </button>
                )}

                {/* <p className="mt-3 text-xs text-gray-500">
                  Demo data is shown for preview only.
                </p> */}
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

/* ===================== MAIN DASHBOARD PAGE ===================== */
export default function ReferralFeesDashboard(): JSX.Element {
  const routeParams = useParams();
  const searchParams = useSearchParams();
  const companyName = useAppSelector(
    (state) => state.auth.user?.company_name || ""
  );
  const brandName = useAppSelector(
    (state) => state.auth.user?.brand_name || ""
  );
  // ✅ IMPORTANT: pick platform/country from URL
  const country = ((routeParams?.countryName as string) || "global").toLowerCase();

  const routeMonth = (routeParams?.month as string | undefined) ?? "";
  const routeYear = (routeParams?.year as string | undefined) ?? "";

  const [month, setMonth] = useState<string>(() => {
    return getLastCompletedMonth().month;
  });

  const [year, setYear] = useState<string>(() => {
    return getLastCompletedMonth().year;
  });

  const [range, setRange] = useState<Range>("monthly");

  const [selectedQuarter, setSelectedQuarter] = useState<string>(() => {
    const lastCompletedMonth = getLastCompletedMonth();
    return getQuarterFromMonth(lastCompletedMonth.month);
  });

  const [showAllProductRows, setShowAllProductRows] = useState(false);
  const [showDeepDive, setShowDeepDive] = useState(false);

  useEffect(() => {
    setShowAllProductRows(false);
    setShowDeepDive(false);
  }, [country, month, range, selectedQuarter, year]);

  const isPreviewMode =
    routeMonth.toUpperCase() === "NA" ||
    routeYear.toUpperCase() === "NA";

  const effectiveCountry = isPreviewMode
    ? "global"
    : country;

  // ✅ Global vs country behavior
  const isGlobalPage = effectiveCountry === "global";

  // ✅ get home currency ONLY for global formatting + api param
  const { homeCurrency: rawHomeCurrency } = useHomeCurrencyContext(country);
  const homeCurrency = isGlobalPage ? (rawHomeCurrency || "USD").toUpperCase() : "";

  // ✅ formatting currency code (NO fallback to homeCurrency on country pages)
  const displayCurrencyCode = useMemo(() => {
    if (effectiveCountry === "global") return homeCurrency || "USD";
    return currencyFromCountryName(effectiveCountry);
  }, [effectiveCountry, homeCurrency]);

  const handlePreviewAction = () => {
    window.location.href = "/profile/uk/NA/NA";
  };

  const [card6, setCard6] = useState<Card6Summary>({
    sales: 0,
    units: 0,
    productSales: 0,

    totalFees: 0,
    totalFeesApplicable: 0,

    refFeesApplied: 0,
    refFeesApplicable: 0,

    fbaFees: 0,
    fbaFeesApplicable: 0,

    platformFees: 0,
    platformFeesApplicable: 0,

    otherFees: 0,
    otherFeesApplicable: 0,
  });
  const [feePercentages, setFeePercentages] = useState<FeePercentages>(
    EMPTY_FEE_PERCENTAGES
  );
  const [referralFeeInsight, setReferralFeeInsight] =
    useState<ReferralFeeInsight>(EMPTY_REFERRAL_FEE_INSIGHT);

  const fmtCurrency = useCallback(
    (n: number): string => {
      if (typeof n !== "number" || Number.isNaN(n)) return "-";
      return n.toLocaleString(undefined, {
        style: "currency",
        currency: displayCurrencyCode,
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
    },
    [displayCurrencyCode]
  );

  const fmtCurrencyRounded = useCallback(
    (n: number): string => {
      if (typeof n !== "number" || Number.isNaN(n)) return "-";
      return n.toLocaleString(undefined, {
        style: "currency",
        currency: displayCurrencyCode,
        minimumFractionDigits: 0,
        maximumFractionDigits: 0,
      });
    },
    [displayCurrencyCode]
  );

  const renderFeeCurrentValue = (
    amount: number,
    percentage: number
  ): React.ReactNode => {
    return (
      <div className="flex items-baseline gap-1 leading-tight">
        <span className="text-sm 2xl:text-lg font-semibold">
          {fmtCurrencyRounded(amount)}
        </span>

        <span className="text-[10px] 2xl:text-xs text-charcoal-400 font-medium">
          ({toNumberSafe(percentage).toFixed(2)}%)
        </span>
      </div>
    );
  };

  

  const buildFeeComparison = (
    applicableAmount: number,
    applicablePct: number,
    deltaPct: number | null
  ) => {
    if (!Number.isFinite(applicableAmount)) {
      return [{ label: "Applicable estimate", valueText: "Unavailable", deltaText: "—", deltaClassName: "text-gray-400" }];
    }
    const delta =
      deltaPct == null || !Number.isFinite(Number(deltaPct))
        ? null
        : Number(deltaPct);

    // For fees, reduction is good, increase is bad.
    const deltaClassName =
      delta == null || delta === 0
        ? "text-gray-400"
        : delta < 0
          ? "text-emerald-600"
          : "text-red-600";

    return [
      {
        label: "Applicable",
        valueText: `${fmtCurrencyRounded(applicableAmount)} (${toNumberSafe(
          applicablePct
        ).toFixed(2)}%)`,
        deltaText:
          delta == null
            ? "-"
            : `${delta >= 0 ? "▲" : "▼"} ${Math.abs(delta).toFixed(2)}%`,
        deltaClassName,
      },
    ];
  };

  // const fmtFeeWithBackendPct = useCallback(
  //   (n: number, percentage: number): React.ReactNode => (
  //     <>
  //       <span>{fmtCurrencyRounded(n)}</span>
  //       <span className="text-[11px] 2xl:text-sm ml-1">
  //         ({toNumberSafe(percentage).toFixed(2)}%)
  //       </span>
  //     </>
  //   ),
  //   [fmtCurrencyRounded]
  // );

  const DUMMY_CARD6: Card6Summary = {
    sales: 0,
    units: 0,
    productSales: 0,

    totalFees: 0,
    totalFeesApplicable: 0,

    refFeesApplied: 0,
    refFeesApplicable: 0,

    fbaFees: 0,
    fbaFeesApplicable: 0,

    platformFees: 0,
    platformFeesApplicable: 0,

    otherFees: 0,
    otherFeesApplicable: 0,
  };

  const DUMMY_FEE_SUMMARY_ROWS: FeeSummaryRow[] = [
    {
      label: "Charge - Accurate",
      units: 0,
      sales: 0,
      refFeesApplicable: 0,
      refFeesCharged: 0,
      overcharged: 0,
    },
    {
      label: "Charge - Overcharged",
      units: 0,
      sales: 0,
      refFeesApplicable: 0,
      refFeesCharged: 0,
      overcharged: 0,
    },
    {
      label: "Charge - Undercharged",
      units: 0,
      sales: 0,
      refFeesApplicable: 0,
      refFeesCharged: 0,
      overcharged: 0,
    },
    {
      label: "Charge - noreferallfee",
      units: 0,
      sales: 0,
      refFeesApplicable: 0,
      refFeesCharged: 0,
      overcharged: 0,
    },
    {
      label: "Grand Total",
      units: 0,
      sales: 0,
      refFeesApplicable: 0,
      refFeesCharged: 0,
      overcharged: 0,
    },
  ];

  const DUMMY_ROWS: ReferralRow[] = [
    {
      sku: "Charge - Accurate",
      net_sales_total_value: 0,
      selling_fees: 0,
      answer: 0,
      difference: 0,
    },
    {
      sku: "Charge - Overcharged",
      net_sales_total_value: 0,
      selling_fees: 0,
      answer: 0,
      difference: 0,
    },
    {
      sku: "Charge - Undercharged",
      net_sales_total_value: 0,
      selling_fees: 0,
      answer: 0,
      difference: 0,
    },
    {
      sku: "Charge - noreferallfee",
      net_sales_total_value: 0,
      selling_fees: 0,
      answer: 0,
      difference: 0,
    },
    {
      sku: "Grand Total",
      net_sales_total_value: 0,
      selling_fees: 0,
      answer: 0,
      difference: 0,
    },
  ];

  const DUMMY_SKU_ROWS: ReferralRow[] = [
    {
      sku: "SKU-001",
      product_name: "Demo Product A",
      quantity: 0,
      net_sales_total_value: 0,
      selling_fees: 0,
      answer: 0,
      difference: 0,
    },
    {
      sku: "SKU-002",
      product_name: "Demo Product B",
      quantity: 0,
      net_sales_total_value: 0,
      selling_fees: 0,
      answer: 0,
      difference: 0,
    },
    {
      sku: "SKU-003",
      product_name: "Demo Product C",
      quantity: 0,
      net_sales_total_value: 0,
      selling_fees: 0,
      answer: 0,
      difference: 0,
    },
    {
      sku: "SKU-004",
      product_name: "Demo Product D",
      quantity: 0,
      net_sales_total_value: 0,
      selling_fees: 0,
      answer: 0,
      difference: 0,
    },
    {
      sku: "SKU-005",
      product_name: "Demo Product E",
      quantity: 0,
      net_sales_total_value: 0,
      selling_fees: 0,
      answer: 0,
      difference: 0,
    },
  ];


  // quarter -> representative month (choose what you prefer: start month here)
  const quarterToMonth = (q: string) => {
    switch ((q || "").toUpperCase()) {
      case "Q1": return "january";
      case "Q2": return "april";
      case "Q3": return "july";
      case "Q4": return "october";
      default: return "";
    }
  };


  const [rows, setRows] = useState<ReferralRow[]>([]);
  const [skuwiseRows, setSkuwiseRows] = useState<ReferralRow[]>([]);
  const [skuMonthlyRows, setSkuMonthlyRows] = useState<ReferralRow[]>([]);
  const [skuMonthlySummary, setSkuMonthlySummary] = useState<ReferralRow | null>(null);
  const [summary, setSummary] = useState<Summary>({
    ordersUnits: 0,
    totalSales: 0,
    feeImpact: 0,
  });
  const [feeSummaryRows, setFeeSummaryRows] = useState<FeeSummaryRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [userId, setUserId] = useState<string>("unknown");
  const [allOrdersByStatus, setAllOrdersByStatus] = useState<any[]>([]);
  const [fbaOrders, setFbaOrders] = useState<any[]>([]);
  const [error, setError] = useState<string | null>(null);

  const [drawerAi, setDrawerAi] = useState<DrawerAiState>({
    blocks: [],
    recommendationsMap: {},
    periodText: "",
  });
  const [drawerAiLoading, setDrawerAiLoading] = useState(false);
  const [drawerAiError, setDrawerAiError] = useState<string | null>(null);
  const [drawerAiRequestKey, setDrawerAiRequestKey] = useState(0);
  const [selectedDrawerProduct, setSelectedDrawerProduct] = useState<{
    name: string;
    sku?: string;
  } | null>(null);

  const drawerCurrencySymbol = useMemo(
    () => fmtCurrency(0).replace(/[\d.,\s]/g, ""),
    [fmtCurrency]
  );

  const drawerFallbackPeriodText = useMemo(
    () => buildDrawerPeriodText(range, month, selectedQuarter, year),
    [range, month, selectedQuarter, year]
  );

  const selectedDrawerBlock = useMemo(() => {
    if (!selectedDrawerProduct) return null;

    const selectedName = normalizeDrawerKey(selectedDrawerProduct.name);
    const selectedSku = String(selectedDrawerProduct.sku || "").trim().toLowerCase();
    const canUseSku = selectedSku && selectedSku !== "multiple";

    return (
      (canUseSku
        ? drawerAi.blocks.find(
            (block) =>
              String(block.skuKey || "").trim().toLowerCase() === selectedSku
          )
        : undefined) ||
      drawerAi.blocks.find(
        (block) => normalizeDrawerKey(block.name) === selectedName
      ) ||
      null
    );
  }, [drawerAi.blocks, selectedDrawerProduct]);

  const selectedDrawerRecObj = useMemo(() => {
    if (!selectedDrawerBlock) return null;
    return findDrawerRecommendation(
      drawerAi.recommendationsMap,
      selectedDrawerBlock,
      selectedDrawerProduct?.sku
    );
  }, [drawerAi.recommendationsMap, selectedDrawerBlock, selectedDrawerProduct]);

  const openReferralProductDrawer = useCallback((row: any) => {
    if (!row || row._isTotal || row._isOthers) return;

    const name = String(row.productName || "").trim();
    if (!name) return;

    setDrawerAiError(null);
    setDrawerAiLoading(true);
    setSelectedDrawerProduct({
      name,
      sku: String(row.sku || "").trim(),
    });
    // Force the same /summary API to run on every product click.
    // Previously it only ran when the period changed, so clicking a product
    // could open the drawer with an empty/stale block while only
    // ProductBestPerformance was visible in Network.
    setDrawerAiRequestKey((value) => value + 1);
  }, []);

  useEffect(() => {
    setSelectedDrawerProduct(null);
  }, [effectiveCountry, range, month, selectedQuarter, year]);

  useEffect(() => {
    if (isPreviewMode) {
      setDrawerAi({ blocks: [], recommendationsMap: {}, periodText: drawerFallbackPeriodText });
      setDrawerAiLoading(false);
      setDrawerAiError(null);
      return;
    }

    const timeline =
      range === "monthly"
        ? monthNameToNumberForDrawer(month)
        : range === "quarterly"
          ? selectedQuarter
          : "ALL";

    if (!effectiveCountry || !year) return;
    if (range === "monthly" && !timeline) return;
    if (range === "quarterly" && !selectedQuarter) return;

    const controller = new AbortController();

    const fetchDrawerAiSummary = async () => {
      try {
        setDrawerAiLoading(true);
        setDrawerAiError(null);

        const token =
          typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        const url = new URL(`${baseURL}/summary`);
        url.searchParams.set("country", effectiveCountry);
        url.searchParams.set("period", range);
        url.searchParams.set("timeline", String(timeline));
        url.searchParams.set("year", String(year));

        const response = await fetch(url.toString(), {
          method: "GET",
          headers: token ? { Authorization: `Bearer ${token}` } : {},
          cache: "no-store",
          signal: controller.signal,
        });

        const data: any = await response.json().catch(() => ({}));

        if (!response.ok) {
          throw new Error(data?.error || "Failed to fetch AI product insights");
        }

        if (
          data?.scope === "global" ||
          data?.global_ai ||
          effectiveCountry === "global"
        ) {
          setDrawerAi(
            buildGlobalDrawerAiState(
              data,
              range,
              drawerCurrencySymbol,
              drawerFallbackPeriodText
            )
          );
          return;
        }

        const sections = parseDrawerMdSections(data?.summary);
        const productLines = [
          ...(sections["PRODUCT INSIGHTS"] ?? []),
          ...(sections["ALL SKU INDIVIDUAL INSIGHTS"] ?? []),
        ];

        const fallbackLines = productLines.length
          ? productLines
          : String(data?.summary || "")
              .split(/\r?\n/)
              .map((line) => line.trim())
              .filter(Boolean);

        const recommendationsMap = drawerRecommendationSource(
          data?.recommendations
        );

        const blocks = mergeDrawerRecommendationsIntoBlocks(
          parseDrawerProductBlocks(fallbackLines, range),
          recommendationsMap
        );

        const summaryLines = sections["SUMMARY"] ?? sections["ROOT"] ?? [];

        setDrawerAi({
          blocks,
          recommendationsMap,
          periodText: getDrawerSummaryPeriodText(
            summaryLines,
            drawerFallbackPeriodText
          ),
        });
      } catch (error: any) {
        if (error?.name === "AbortError") return;
        setDrawerAi({
          blocks: [],
          recommendationsMap: {},
          periodText: drawerFallbackPeriodText,
        });
        setDrawerAiError(error?.message || "Failed to load product insights");
      } finally {
        setDrawerAiLoading(false);
      }
    };

    fetchDrawerAiSummary();
    return () => controller.abort();
  }, [
    effectiveCountry,
    range,
    month,
    selectedQuarter,
    year,
    isPreviewMode,
    drawerCurrencySymbol,
    drawerFallbackPeriodText,
    drawerAiRequestKey,
  ]);

  const [salesStatus, setSalesStatus] = useState<SalesStatusSummary>({
    totalSales: 0,
    accurateSales: 0,
    overchargedSales: 0,
    underchargedSales: 0,
    noRefFeeSales: 0,
  });

  const [refBreakdown, setRefBreakdown] = useState<RefFeesBreakdown>({
    totalApplicable: 0,
    accurateApplicable: 0,
    overchargedApplicable: 0,
    underchargedApplicable: 0,
    noRefFeeApplicable: 0,
  });

  /* ======= derive userId from JWT ======= */
  useEffect(() => {
    if (typeof window === "undefined") return;
    const token = localStorage.getItem("jwtToken");
    if (!token) return;
    try {
      const decoded: any = jwtDecode(token);
      const id = decoded?.user_id?.toString() ?? "unknown";
      setUserId(id);
    } catch {
      setUserId("unknown");
    }
  }, []);


  /* ======= fileName ======= */
  const fileName = useMemo(
    () => `user_${userId}_${country}_${month}${year}_data`.toLowerCase(),
    [userId, country, month, year]
  );

  /* ===================== API Fetch ===================== */
  const fetchReferralData = useCallback(async () => {
    if (isPreviewMode) {
      setLoading(false);
      setError(null);

      setRows(DUMMY_ROWS);
      setSkuwiseRows(DUMMY_SKU_ROWS);
      setSkuMonthlyRows([]);
      setSkuMonthlySummary(null);
      setFeeSummaryRows(DUMMY_FEE_SUMMARY_ROWS);
      setAllOrdersByStatus([]);
      setFbaOrders([]);

      setCard6(DUMMY_CARD6);
      setFeePercentages(EMPTY_FEE_PERCENTAGES);
      setReferralFeeInsight(EMPTY_REFERRAL_FEE_INSIGHT);

      return;
    }
    if (!month || !year || !country) {
      setError("Please select both month and year to view referral fees.");
      setRows([]);
      setSkuwiseRows([]);
      setSkuMonthlyRows([]);
      setSkuMonthlySummary(null);
      setFeeSummaryRows([]);
      setAllOrdersByStatus([]);
      setFbaOrders([]);
      setFeePercentages(EMPTY_FEE_PERCENTAGES);
      setReferralFeeInsight(EMPTY_REFERRAL_FEE_INSIGHT);
      setSummary({ ordersUnits: 0, totalSales: 0, feeImpact: 0 });
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const token = typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

      const params = new URLSearchParams({
        country: country,
        month: month,
        year: year,
      });

      // ✅ range → send qtd/ytd flags
      if (range === "quarterly") {
        params.set("qtd", "true");       // or "1" if your backend prefers
        params.set("quarter", selectedQuarter || ""); // optional but useful
      }

      if (range === "yearly") {
        params.set("ytd", "true");       // or "1"
      }

      // ✅ ONLY GLOBAL sends homeCurrency
      if (isGlobalPage && homeCurrency) {
        params.set("homeCurrency", homeCurrency.toLowerCase());
      }

      const url = `${baseURL}/get_table_data/${fileName}?${params.toString()}`;

      const res = await fetch(url, {
        method: "GET",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });

      if (!res.ok) throw new Error(`Failed to fetch referral data (${res.status})`);

      const json: any = await res.json();

      const apiFeePercentages = json?.fee_percentages ?? {};
      setFeePercentages({
        referral_fees: {
          charged_net_sales_pct: toNumberSafe(
            apiFeePercentages?.referral_fees?.charged_net_sales_pct
          ),
          applicable_net_sales_pct: toNumberSafe(
            apiFeePercentages?.referral_fees?.applicable_net_sales_pct
          ),
          charged_vs_applicable_pct: toNumberSafe(
            apiFeePercentages?.referral_fees?.charged_vs_applicable_pct
          ),
        },
        fba_fees: {
          charged_net_sales_pct: toNumberSafe(
            apiFeePercentages?.fba_fees?.charged_net_sales_pct
          ),
          applicable_net_sales_pct: expectedFee(
            apiFeePercentages?.fba_fees?.applicable_net_sales_pct
          ),
          charged_vs_applicable_pct: expectedFee(
            apiFeePercentages?.fba_fees?.charged_vs_applicable_pct
          ),
        },
        platform_fees: {
          charged_net_sales_pct: toNumberSafe(
            apiFeePercentages?.platform_fees?.charged_net_sales_pct
          ),
          applicable_net_sales_pct: toNumberSafe(
            apiFeePercentages?.platform_fees?.applicable_net_sales_pct
          ),
          charged_vs_applicable_pct: toNumberSafe(
            apiFeePercentages?.platform_fees?.charged_vs_applicable_pct
          ),
        },
        other_fees: {
          charged_net_sales_pct: toNumberSafe(
            apiFeePercentages?.other_fees?.charged_net_sales_pct
          ),
          applicable_net_sales_pct: toNumberSafe(
            apiFeePercentages?.other_fees?.applicable_net_sales_pct
          ),
          charged_vs_applicable_pct: toNumberSafe(
            apiFeePercentages?.other_fees?.charged_vs_applicable_pct
          ),
        },
      });

      const apiReferralFeeInsight = json?.referral_fee_insight ?? {};
      const insightStatus = ["clear", "low", "review", "action"].includes(
        apiReferralFeeInsight?.status
      )
        ? apiReferralFeeInsight.status
        : "clear";
      setReferralFeeInsight({
        status: insightStatus,
        overcharged_amount: toNumberSafe(apiReferralFeeInsight?.overcharged_amount),
        overcharged_units: toNumberSafe(apiReferralFeeInsight?.overcharged_units),
        overcharge_rate_pct: toNumberSafe(apiReferralFeeInsight?.overcharge_rate_pct),
        affected_units_pct: toNumberSafe(apiReferralFeeInsight?.affected_units_pct),
        accurate_units_pct: toNumberSafe(apiReferralFeeInsight?.accurate_units_pct),
        net_variance: toNumberSafe(apiReferralFeeInsight?.net_variance),
      });

      const platformFeeTotalFromApi = toNumberSafe(json?.platform_fee_total);
      const otherTotalFromApi = toNumberSafe(json?.other_total);


      const table = json?.table ?? [];
      const arr: ReferralRow[] = Array.isArray(table) ? table : [];
      const monthlyRows: ReferralRow[] = Array.isArray(json?.sku_monthly_rows) ? json.sku_monthly_rows : [];
      const monthlySummary: ReferralRow | null =
        json?.sku_monthly_summary && Object.keys(json.sku_monthly_summary).length
          ? json.sku_monthly_summary
          : null;


      // ✅ Pick summary rows directly from backend table
      const getRowBySku = (skuLabel: string) =>
        arr.find((r) => String(r.sku ?? "").trim().toLowerCase() === skuLabel.trim().toLowerCase());

      const rAcc = getRowBySku("Charge - Accurate");
      const rUnder = getRowBySku("Charge - Undercharged");
      const rOver = getRowBySku("Charge - Overcharged");
      const rNoRef = getRowBySku("Charge - noreferallfee");
      const rGrand = getRowBySku("Grand Total");

      // ✅ Sales & Status Summary MUST use net_sales_total_value from these rows
      const grandSales = toNumberSafe(rGrand?.net_sales_total_value);

      setSalesStatus({
        totalSales: grandSales,
        accurateSales: toNumberSafe(rAcc?.net_sales_total_value),
        underchargedSales: toNumberSafe(rUnder?.net_sales_total_value),
        overchargedSales: toNumberSafe(rOver?.net_sales_total_value),
        noRefFeeSales: toNumberSafe(rNoRef?.net_sales_total_value),
      });

      setRows(arr);
      setSkuwiseRows(arr);
      setSkuMonthlyRows(monthlyRows);
      setSkuMonthlySummary(monthlySummary);

      const accurate = Array.isArray(json?.accurate_data) ? json.accurate_data : [];
      const overcharged = Array.isArray(json?.overcharged_data) ? json.overcharged_data : [];
      const undercharged = Array.isArray(json?.undercharged_data) ? json.undercharged_data : [];

      const mergedAll = [
        { Category: "Overcharged" },
        ...overcharged.map((r: any) => ({ Category: "Overcharged", ...r })),
        {},
        { Category: "Undercharged" },
        ...undercharged.map((r: any) => ({ Category: "Undercharged", ...r })),
        {},
        { Category: "Accurate" },
        ...accurate.map((r: any) => ({ Category: "Accurate", ...r })),
      ];

      setAllOrdersByStatus(mergedAll);
      setFbaOrders(Array.isArray(json?.fba_data) ? json.fba_data :
        [...overcharged, ...undercharged, ...accurate,
        ...(Array.isArray(json?.no_ref_fee_data) ? json.no_ref_fee_data : [])]);

      const summarySource = arr.filter((r) => {
        const sku = String(r.sku ?? "");
        return sku.startsWith("Charge -") || sku === "Grand Total";
      });

      const findChargeRow = (skuLabel: string) =>
        summarySource.find((r) => String(r.sku ?? "").toLowerCase() === skuLabel.toLowerCase());

      const chAcc = findChargeRow("Charge - Accurate");
      const chOver = findChargeRow("Charge - Overcharged");
      const chUnder = findChargeRow("Charge - Undercharged");
      const chNoRef = findChargeRow("Charge - noreferallfee");
      const grand = findChargeRow("Grand Total") ?? arr.find((r) => String(r.sku ?? "").toLowerCase() === "grand total");

      // ✅ LEFT PANEL: SALES split by status (use net sales)
      const totalSalesVal = toNumberSafe(grand?.net_sales_total_value ?? 0);
      setSalesStatus({
        totalSales: totalSalesVal,
        accurateSales: toNumberSafe(chAcc?.net_sales_total_value ?? 0),
        overchargedSales: toNumberSafe(chOver?.net_sales_total_value ?? 0),
        underchargedSales: toNumberSafe(chUnder?.net_sales_total_value ?? 0),
        noRefFeeSales: toNumberSafe(chNoRef?.net_sales_total_value ?? 0),
      });

      // ✅ RIGHT PANEL: REF FEES breakdown (use "answer" i.e. applicable)
      setRefBreakdown({
        totalApplicable: toNumberSafe(grand?.answer ?? 0),
        accurateApplicable: toNumberSafe(chAcc?.answer ?? 0),
        overchargedApplicable: toNumberSafe(chOver?.answer ?? 0),     // <- 216.75
        underchargedApplicable: toNumberSafe(chUnder?.answer ?? 0),   // <- 205.22
        noRefFeeApplicable: toNumberSafe(chNoRef?.answer ?? 0),       // <- 0
      });


      const order = [
        "Charge - Accurate",
        "Charge - Undercharged",
        "Charge - Overcharged",
        "Charge - noreferallfee",
        "Grand Total",
      ];

      summarySource.sort((a, b) => {
        const aSku = String(a.sku ?? "");
        const bSku = String(b.sku ?? "");
        const ai = order.indexOf(aSku);
        const bi = order.indexOf(bSku);
        if (ai === -1 && bi === -1) return 0;
        if (ai === -1) return 1;
        if (bi === -1) return -1;
        return ai - bi;
      });

      const mappedSummaryRaw: FeeSummaryRow[] = summarySource.map((r: ReferralRow) => ({
        label: String(r.sku ?? ""),
        units: Math.round(getDisplayUnits(r)),
        sales: getNetSales(r),
        refFeesApplicable: toNumberSafe(r.answer),
        refFeesCharged: getChargedReferralFees(r),
        overcharged: toNumberSafe(r.difference),
      }));

      const mappedSummary: FeeSummaryRow[] = (() => {
        if (!monthlySummary) return mappedSummaryRaw;

        const targetUnits = Math.round(getDisplayUnits(monthlySummary));
        const targetNetSales = getNetSales(monthlySummary);
        const detailRows = mappedSummaryRaw.filter((r) => !isGrandTotalLabel(r.label));
        const scaledUnits = scaleIntegerBreakdown(detailRows.map((r) => r.units), targetUnits);
        const scaledSales = scaleMoneyBreakdown(detailRows.map((r) => r.sales), targetNetSales);
        let detailIndex = 0;

        return mappedSummaryRaw.map((r) => {
          if (isGrandTotalLabel(r.label)) {
            return {
              ...r,
              units: targetUnits || r.units,
              sales: targetNetSales || r.sales,
            };
          }

          const adjusted = {
            ...r,
            units: scaledUnits[detailIndex] ?? r.units,
            sales: scaledSales[detailIndex] ?? r.sales,
          };
          detailIndex += 1;
          return adjusted;
        });
      })();
      const grandSummary = mappedSummary.find((row) =>
        isGrandTotalLabel(row.label)
      );

      // use only actual SKU/product lines (exclude Charge-/Grand Total lines)
      const lineItems = arr.filter((r) => {
        const skuStr = String(r.sku ?? "");
        if (skuStr === "Grand Total") return false;
        if (skuStr.startsWith("Charge -")) return false;
        return true;
      });

      // const salesTotal = lineItems.reduce((acc, r) => acc + getNetSales(r), 0);


      // const totalFees = lineItems.reduce((acc, r) => acc + toNumberSafe(r.selling_fees), 0);

      // const refFeesApplicable = lineItems.reduce((acc, r) => acc + toNumberSafe(r.answer), 0);

      // const refFeesApplied = totalFees;

      // const fbaFees = lineItems.reduce((acc, r) => {
      //   return acc + FBA_KEYS.reduce((s, k) => s + toNumberSafe((r as any)[k]), 0);
      // }, 0);

      // const platformFees = platformFeeTotalFromApi !== 0 ? platformFeeTotalFromApi : platformFeesDerived;
      // const otherFees = otherTotalFromApi !== 0 ? otherTotalFromApi : otherFeesDerived;


      // const platformFeesDerived = lineItems.reduce((acc, r) => {
      //   return acc + PLATFORM_KEYS.reduce((s, k) => s + toNumberSafe((r as any)[k]), 0);
      // }, 0);

      // const otherFeesDerived = lineItems.reduce((acc, r) => {
      //   return acc + OTHER_KEYS.reduce((s, k) => s + toNumberSafe((r as any)[k]), 0);
      // }, 0);


      const unitsSold = monthlySummary
        ? Math.round(getDisplayUnits(monthlySummary))
        : lineItems.reduce((acc, r) => acc + Math.round(getDisplayUnits(r)), 0);

      const salesTotal = monthlySummary
        ? getNetSales(monthlySummary)
        : lineItems.reduce((acc, r) => acc + getNetSales(r), 0);

      const productSalesTotal = monthlySummary
        ? getGrossSales(monthlySummary)
        : lineItems.reduce(
          (acc, r) => acc + getGrossSales(r),
          0
        );



      // Referral
      const refFeesApplicable = grandSummary
        ? grandSummary.refFeesApplicable
        : lineItems.reduce((acc, r) => acc + toNumberSafe(r.answer), 0);
      const refFeesApplied = grandSummary
        ? grandSummary.refFeesCharged
        : lineItems.reduce((acc, r) => acc + getChargedReferralFees(r), 0);

      const fbaFees = monthlySummary
        ? Math.abs(toNumberSafe((monthlySummary as any).fba_fees))
        : Math.abs(
          lineItems.reduce((acc, r) => {
            return acc + FBA_KEYS.reduce((s, k) => s + toNumberSafe((r as any)[k]), 0);
          }, 0)
        );


      // Platform (charged)
      const platformFeesDerived = lineItems.reduce((acc, r) => {
        return acc + PLATFORM_KEYS.reduce((s, k) => s + toNumberSafe((r as any)[k]), 0);
      }, 0);

      // Other (charged)
      const otherFeesDerived = lineItems.reduce((acc, r) => {
        return acc + OTHER_KEYS.reduce((s, k) => s + toNumberSafe((r as any)[k]), 0);
      }, 0);

      const platformFees = platformFeeTotalFromApi !== 0 ? platformFeeTotalFromApi : platformFeesDerived;
      const otherFees = otherTotalFromApi !== 0 ? otherTotalFromApi : otherFeesDerived;

      const fbaGrand = arr.find((r) => String(r.sku).toLowerCase() === "grand total");
      const fbaFeesApplicable = fbaGrand
        ? expectedFee(fbaGrand.fbaanswer)
        : lineItems.reduce((sum, r) => sum + expectedFee(r.fbaanswer), 0);
      const platformFeesApplicable = platformFees;
      const otherFeesApplicable = otherFees;

      // ✅ Total fees are aggregate of buckets (charged + applicable)
      const totalFees = refFeesApplied + fbaFees + platformFees + otherFees;
      const totalFeesApplicable = refFeesApplicable + fbaFeesApplicable + platformFeesApplicable + otherFeesApplicable;

      setCard6({
        sales: salesTotal,
        units: unitsSold,
        productSales: productSalesTotal,
        totalFees,
        totalFeesApplicable,

        refFeesApplied,
        refFeesApplicable,

        fbaFees,
        fbaFeesApplicable,

        platformFees,
        platformFeesApplicable,

        otherFees,
        otherFeesApplicable,
      });



      setFeeSummaryRows(mappedSummary);

      let totalUnits = 0;
      let totalSales = 0;
      let feeImpact = 0;

      for (const r of arr) {
        totalUnits += Math.round(getDisplayUnits(r));
        totalSales += getNetSales(r);
        feeImpact += toNumberSafe(r.overcharged ?? r.difference);
      }

      setSummary({ ordersUnits: totalUnits, totalSales, feeImpact });
    } catch (e: any) {
      setError(e?.message || "Failed to load data");
      setRows([]);
      setSkuwiseRows([]);
      setSkuMonthlyRows([]);
      setSkuMonthlySummary(null);
      setFeeSummaryRows([]);
      setAllOrdersByStatus([]);
      setFbaOrders([]);
      setFeePercentages(EMPTY_FEE_PERCENTAGES);
      setReferralFeeInsight(EMPTY_REFERRAL_FEE_INSIGHT);
      setSummary({ ordersUnits: 0, totalSales: 0, feeImpact: 0 });
    } finally {
      setLoading(false);
    }
  }, [month, year, country, fileName, isGlobalPage, homeCurrency, range, selectedQuarter]);


  useEffect(() => {
    fetchReferralData();
  }, [fetchReferralData]);

  /* ===================== Derived ===================== */
  const totalFeeRow = useMemo<FeeSummaryRow | null>(() => {
    if (!feeSummaryRows.length) return null;
    const total =
      feeSummaryRows.find((r) => r.label && r.label.toLowerCase() === "total") ||
      feeSummaryRows[feeSummaryRows.length - 1];
    return total || null;
  }, [feeSummaryRows]);

  const cardSummary = useMemo(
    () => ({
      ordersUnits: totalFeeRow?.units ?? 0,
      totalSales: totalFeeRow?.sales ?? 0,
      feeImpact: totalFeeRow?.overcharged ?? 0,
    }),
    [totalFeeRow]
  );

  const summaryTableRows: Row[] = useMemo(() => {
    return feeSummaryRows.map((r, index) => ({
      label: r.label,
      units: r.units,
      sales: r.sales,
      refFeesApplicable: r.refFeesApplicable,
      refFeesCharged: r.refFeesCharged,
      overcharged: r.overcharged,
      _isTotal: index === feeSummaryRows.length - 1,
    })) as Row[];
  }, [feeSummaryRows]);

  const correctedRefChargedBySku = useMemo(() => {
    const corrected = new Map<string, number>();

    for (const status of ["Accurate", "Undercharged", "Overcharged"] as const) {
      const matchingRows = allOrdersByStatus.filter(
        (row) => row?.Category === status && Object.keys(row).length > 1
      );
      const summaryRow = feeSummaryRows.find(
        (row) => row.label === `Charge - ${status}`
      );
      const chargedValues = matchingRows.map((row) =>
        getChargedReferralFees(row)
      );
      const correctedValues =
        status === "Accurate"
          ? matchingRows.map((row) => roundReferralMoney(row.answer))
          : summaryRow
            ? scaleReferralMoneyBreakdown(
              chargedValues,
              summaryRow.refFeesCharged
            )
            : chargedValues;

      matchingRows.forEach((row, index) => {
        const skuKey = String(row?.sku ?? "").trim().toLowerCase();
        if (!skuKey) return;
        corrected.set(
          skuKey,
          roundReferralMoney(
            (corrected.get(skuKey) ?? 0) +
            toNumberSafe(correctedValues[index])
          )
        );
      });
    }

    return corrected;
  }, [allOrdersByStatus, feeSummaryRows]);

  // const skuTableAll: Row[] = useMemo(() => {
  //   if (!skuwiseRows.length) return [];

  //   const filtered = skuwiseRows.filter((r) => {
  //     const skuStr = String(r.sku ?? "");
  //     if (skuStr === "Grand Total") return true;
  //     return !skuStr.startsWith("Charge -");
  //   });

  //   return filtered.map((r) => {
  //     const quantity = Math.round(toNumberSafe(r.quantity));
  //     const sales = getNetSales(r);
  //     const applicable = toNumberSafe(r.answer);
  //     const charged = toNumberSafe(r.selling_fees);
  //     const overcharged = toNumberSafe(r.overcharged ?? r.difference);

  //     const skuStr = String(r.sku ?? "");
  //     const isTotal = skuStr.toLowerCase() === "grand total";

  //     return {
  //       sku: isTotal ? "" : (r.sku ?? ""),                 // ✅ blank SKU for total
  //       productName: isTotal ? "Grand Total" : (r.product_name ?? ""), // ✅ label in Product Name
  //       units: quantity,
  //       sales,
  //       applicable,
  //       charged,
  //       overcharged,
  //       _isTotal: isTotal,
  //     };

  //   });
  // }, [skuwiseRows]);

  const skuTableAll: Row[] = useMemo(() => {
    if (!skuwiseRows.length) return [];

    const monthlyKey = (r: ReferralRow) =>
      `${String(r?.sku ?? "").trim().toLowerCase()}|${String(
        r?.product_name ?? ""
      )
        .trim()
        .toLowerCase()}`;

    const monthlyBySkuProduct = new Map<string, ReferralRow>();
    const monthlyBySku = new Map<string, ReferralRow>();
    for (const r of skuMonthlyRows) {
      const skuStr = String(r?.sku ?? "").trim().toLowerCase();
      const productStr = String(r?.product_name ?? "")
        .trim()
        .toLowerCase();
      if (!skuStr || skuStr === "total" || skuStr === "grand_total" || skuStr === "grand total") continue;
      if (productStr === "total" || productStr === "grand total") continue;
      monthlyBySkuProduct.set(monthlyKey(r), r);
      monthlyBySku.set(skuStr, r);
    }

    const usedMonthlyRows = new Set<string>();

    const filtered = skuwiseRows.filter((r) => {
      const skuStr = String(r.sku ?? "");
      if (skuStr === "Grand Total") return true;
     return !skuStr.startsWith("Charge -");
    });

    return filtered.map((r) => {
      const skuStr = String(r.sku ?? "");
      const isTotal = skuStr.toLowerCase() === "grand total";
      const key = monthlyKey(r);
      const skuLookupKey = skuStr.trim().toLowerCase();
      const monthlyMatch = isTotal ? skuMonthlySummary : monthlyBySkuProduct.get(key) ?? monthlyBySku.get(skuLookupKey);
      const useMonthlyValues = Boolean(monthlyMatch) && (isTotal || !usedMonthlyRows.has(key));

      if (useMonthlyValues && !isTotal) {
        usedMonthlyRows.add(key);
      }

      const quantity = Math.round(useMonthlyValues ? getDisplayUnits(monthlyMatch) : getDisplayUnits(r));
      const sales = useMonthlyValues ? getNetSales(monthlyMatch) : getNetSales(r);
      const grossSales = useMonthlyValues ? getGrossSales(monthlyMatch) : getGrossSales(r);

      const ref_applicable = toNumberSafe(r.answer);
      const ref_charged = getChargedReferralFees(r);
      const overcharged = toNumberSafe(r.overcharged ?? r.difference);

      const fba_charged = Math.abs(
        useMonthlyValues
          ? toNumberSafe((monthlyMatch as any)?.fba_fees)
          : FBA_KEYS.reduce((sum, k) => sum + toNumberSafe((r as any)[k]), 0)
      );
      const fba_applicable = expectedFee(r.fbaanswer);

      // ✅ Other fees from row
      const other_charged = Math.abs(
        OTHER_KEYS.reduce((sum, k) => sum + toNumberSafe((r as any)[k]), 0)
      );
      const other_applicable = other_charged; // placeholder

      // (optional) ✅ Platform fees from row
      const platform_charged = Math.abs(
        PLATFORM_KEYS.reduce((sum, k) => sum + toNumberSafe((r as any)[k]), 0)
      );

      // ✅ Total (you can include platform in total, even if not shown as a column)
      const total_charged = ref_charged + fba_charged + other_charged + platform_charged;
      const total_applicable = ref_applicable + fba_applicable + other_applicable + platform_charged;

      const rawSku = String(r.sku ?? "").trim();
      const rawProductName = String(r.product_name ?? "").trim();

      const displayProductName =
        rawProductName &&
          rawProductName !== "0" &&
          rawProductName.toLowerCase() !== "null" &&
          rawProductName.toLowerCase() !== "undefined"
          ? rawProductName
          : rawSku;

      return {
        sku: isTotal ? "" : rawSku,
        productName: isTotal ? "Grand Total" : displayProductName,
        units: quantity,
        sales,
        grossSales,

        // keep your existing ones if needed elsewhere
        applicable: ref_applicable,
        charged: ref_charged,
        overcharged,

        // ✅ NEW fields for grouped table
        ref_applicable,
        ref_charged,
        fba_applicable,
        fba_charged,
        other_applicable,
        other_charged,
        total_applicable,
        total_charged,

        _isTotal: isTotal,
      };
    });
  }, [skuwiseRows, skuMonthlyRows, skuMonthlySummary]);


  const skuColumns: ColumnDef<Row>[] = [
    { key: "sku", header: "SKU" },
    { key: "productName", header: "Product Name" },
    { key: "units", header: "Units" },
    { key: "sales", header: "Net Sales", render: (_, v) => fmtCurrency(Number(v)) },
    { key: "applicable", header: "Ref Fees Applicable", render: (_, v) => fmtCurrency(Number(v)) },
    { key: "charged", header: "Ref Fees Charged", render: (_, v) => fmtCurrency(Number(v)) },
    { key: "overcharged", header: "Overcharged", render: (_, v) => <span>{fmtCurrency(Number(v))}</span> },
  ];

  const summaryColumns: ColumnDef<Row>[] = [
    { key: "label", header: "Ref. Fees" },
    { key: "units", header: "Units", render: (_, v) => fmtInteger(Number(v)) },
    { key: "sales", header: "Net Sales", render: (_, v) => fmtCurrency(Number(v)) },
    { key: "refFeesApplicable", header: "Ref Fees Applicable", render: (_, v) => fmtCurrency(Number(v)) },
    { key: "refFeesCharged", header: "Ref Fees Charged", render: (_, v) => fmtCurrency(Number(v)) },
    { key: "overcharged", header: "Overcharged", render: (_, v) => fmtCurrency(Number(v)) },
  ];

  const skuTableDisplay: Row[] = useMemo(() => {
    if (!skuTableAll.length) return [];

    const nonTotal = skuTableAll.filter((row) => !(row as any)._isTotal);
    const totalRow = skuTableAll.find((row) => (row as any)._isTotal) || null;

    // ✅ 1) Aggregate rows by DISTINCT productName
    const map = new Map<string, any>();

    for (const r of nonTotal as any[]) {
      const nameRaw = String(r.productName ?? "").trim();
      if (!nameRaw) continue;

      const key = nameRaw.toLowerCase(); // distinct productName (case-insensitive)

      if (!map.has(key)) {
        map.set(key, {
          sno: "",
          productName: nameRaw,
          sku: r.sku ? String(r.sku) : "", // will be fixed below if multiple
          units: 0,
          sales: 0,

          ref_applicable: 0,
          ref_charged: 0,

          fba_applicable: 0,
          fba_charged: 0,

          other_applicable: 0,
          other_charged: 0,

          total_applicable: 0,
          total_charged: 0,

          overcharged: 0,

          _skus: new Set<string>(),
        });
      }

      const acc = map.get(key);

      acc.units += Math.round(toNumberSafe(r.units));
      acc.sales += toNumberSafe(r.sales);

      acc.ref_applicable += toNumberSafe(r.ref_applicable);
      acc.ref_charged += toNumberSafe(r.ref_charged);

      acc.fba_applicable += expectedFee(r.fba_applicable);
      acc.fba_charged += toNumberSafe(r.fba_charged);

      acc.other_applicable += toNumberSafe(r.other_applicable);
      acc.other_charged += toNumberSafe(r.other_charged);

      acc.total_applicable += expectedFee(r.total_applicable);
      acc.total_charged += toNumberSafe(r.total_charged);

      acc.overcharged += toNumberSafe(r.overcharged);

      const skuStr = String(r.sku ?? "").trim();
      if (skuStr) acc._skus.add(skuStr);
    }

    // ✅ 2) Convert to array + decide SKU display
    const aggregated = Array.from(map.values()).map((x) => {
      const skus = Array.from(x._skus);
      return {
        ...x,
        sku: skus.length === 1 ? skus[0] : skus.length > 1 ? "Multiple" : "",
      };
    });

    if (!aggregated.length) return totalRow ? [totalRow] : [];

    // Show the top nine products, with all remaining products aggregated as Others.
    const sorted = [...aggregated].sort(
      (a, b) => toNumberSafe(b.sales) - toNumberSafe(a.sales)
    );

    const top9 = sorted.slice(0, 9);
    const remaining = sorted.slice(9);

    let othersRow: Row | null = null;
    if (remaining.length) {
      const agg = remaining.reduce(
        (acc: any, row: any) => {
          acc.units += Math.round(toNumberSafe(row.units));
          acc.sales += toNumberSafe(row.sales);

          // ✅ YOU WERE MISSING THESE TOO
          acc.ref_applicable += toNumberSafe(row.ref_applicable);
          acc.ref_charged += toNumberSafe(row.ref_charged);

          acc.fba_applicable += expectedFee(row.fba_applicable);
          acc.fba_charged += toNumberSafe(row.fba_charged);

          acc.other_applicable += toNumberSafe(row.other_applicable);
          acc.other_charged += toNumberSafe(row.other_charged);

          acc.total_applicable += expectedFee(row.total_applicable);
          acc.total_charged += toNumberSafe(row.total_charged);

          acc.overcharged += toNumberSafe(row.overcharged);
          return acc;
        },
        {
          units: 0,
          sales: 0,

          ref_applicable: 0,
          ref_charged: 0,

          fba_applicable: 0,
          fba_charged: 0,

          other_applicable: 0,
          other_charged: 0,

          total_applicable: 0,
          total_charged: 0,

          overcharged: 0,
        }
      );


      othersRow = {
        sno: "",
        sku: "",
        productName: "Others",
        units: agg.units,
        sales: agg.sales,
        overcharged: agg.overcharged,

        ref_applicable: agg.ref_applicable,
        ref_charged: agg.ref_charged,
        fba_applicable: agg.fba_applicable,
        fba_charged: agg.fba_charged,
        other_applicable: agg.other_applicable,
        other_charged: agg.other_charged,
        total_applicable: agg.total_applicable,
        total_charged: agg.total_charged,

        _isOthers: true,
      } as Row;
    }

    // ✅ 4) Final rows + serial numbers (not for Grand Total)
    const finalRows: any[] = showAllProductRows ? [...sorted] : [...top9];
    if (!showAllProductRows && othersRow) finalRows.push(othersRow);
    if (totalRow) finalRows.push(totalRow);

    let counter = 1;
    return finalRows.map((row: any) => {
      if (row._isTotal) return { ...row, sno: "" };
      return { ...row, sno: counter++ };
    });
  }, [showAllProductRows, skuTableAll]);

  const groupedSkuTableDisplay: any[] = useMemo(() => {
    return skuTableDisplay.map((row: any) => ({
      sno: row.sno,
      productName: row.productName,
      sku: row.sku,
      units: row.units,
      sales: row.sales,

      ref_applicable: row.ref_applicable,
      ref_charged: row.ref_charged,

      fba_applicable: row.fba_applicable,
      fba_charged: row.fba_charged,

      other_applicable: row.other_applicable,
      other_charged: row.other_charged,

      total_applicable: row.total_applicable,
      total_charged: row.total_charged,

      // ✅ keep total-row styling working
      _isTotal: row._isTotal,
      _isOthers: row._isOthers,
    }));
  }, [skuTableDisplay]);


  const handleDownloadFbaExcel = useCallback(() => {
    exportFbaFeesExcel({ rows: fbaOrders, country, currency: displayCurrencyCode, company: companyName,
      period: range === "yearly" ? year : range === "quarterly" ? `${selectedQuarter} ${year}` : `${month} ${year}` });
  }, [fbaOrders, country, displayCurrencyCode, companyName, range, year, selectedQuarter, month]);

  const handleDownloadExcel = useCallback(() => {
    exportReferralFeesExcel({
      filename:
        range === "yearly"
          ? `Referral Fees ${country.toUpperCase()} ${year}.xlsx`
          : range === "quarterly"
            ? `Referral Fees ${country.toUpperCase()} ${selectedQuarter}'${year.slice(-2)}.xlsx`
            : `Referral Fees ${country.toUpperCase()} ${formatMonthYear(month, year)}.xlsx`,
      countryName: country,
      periodLabel:
        range === "yearly"
          ? year
          : range === "quarterly"
            ? `${selectedQuarter || "Quarter"} ${year}`
            : `${month.charAt(0).toUpperCase() + month.slice(1)} ${year}`,
      currencyCode: displayCurrencyCode,
      titleCountry: country === "global" ? "Global" : country.toUpperCase(),
      platformLabel: "Phormula",
      companyName,
      brandName,
      feeSummaryRows,
      productRows: skuTableAll as Record<string, any>[],
      ordersByStatus: allOrdersByStatus as Record<string, any>[],
      cardSummary: card6,
      totalFeeRow,
    });
  }, [
    allOrdersByStatus,
    card6,
    brandName,
    companyName,
    country,
    displayCurrencyCode,
    feeSummaryRows,
    month,
    range,
    selectedQuarter,
    skuTableAll,
    totalFeeRow,
    year,
  ]);


  const canShowContent = !loading && !error && month && year;

  const currencySymbol = useMemo(
    () => fmtCurrency(0).replace(/[\d.,\s]/g, ""),
    [fmtCurrency]
  );

  const fmtMoneyNoSymbol = useCallback((n: any) => {
    if (n == null || !Number.isFinite(Number(n))) return "Unavailable";
    return Math.round(toNumberSafe(n)).toLocaleString();
  }, []);

  const reconciliationStatuses = useMemo(() => {
    const findStatus = (label: string) =>
      feeSummaryRows.find(
        (row) => row.label.trim().toLowerCase() === label.toLowerCase()
      );
    const accurate = findStatus("Charge - Accurate");
    const overcharged = findStatus("Charge - Overcharged");
    const undercharged = findStatus("Charge - Undercharged");

    return [
      {
        key: "accurate",
        title: "Accurate",
        units: Math.max(0, toNumberSafe(accurate?.units)),
        amount: toNumberSafe(accurate?.overcharged),
        color: "#7B9A6D",
      },
      {
        key: "overcharged",
        title: "Overcharged",
        units: Math.max(0, toNumberSafe(overcharged?.units)),
        amount: toNumberSafe(overcharged?.overcharged),
        color: "#B75A5A",
      },
      {
        key: "undercharged",
        title: "Undercharged",
        units: Math.max(0, toNumberSafe(undercharged?.units)),
        amount: toNumberSafe(undercharged?.overcharged),
        color: "#ED9F50",
      },
    ];
  }, [feeSummaryRows]);

  const reconciliationTotalUnits = useMemo(
    () => reconciliationStatuses.reduce((sum, status) => sum + status.units, 0),
    [reconciliationStatuses]
  );

  const reconciliationDonutData = useMemo<DonutChartItem[]>(
    () =>
      reconciliationStatuses.map((status) => ({
        bucket: status.title,
        units: status.units,
        amount: status.amount,
        color: status.color,
      })),
    [reconciliationStatuses]
  );

  const reconciliationInsight = useMemo(() => {
    const overchargedAmount = referralFeeInsight.overcharged_amount;
    const overchargedUnits = referralFeeInsight.overcharged_units;
    const overchargeRate = referralFeeInsight.overcharge_rate_pct;
    const affectedUnitsRate = referralFeeInsight.affected_units_pct;
    const accurateUnitsRate = referralFeeInsight.accurate_units_pct;
    const netVariance = referralFeeInsight.net_variance;

    if (referralFeeInsight.status === "clear") {
      return {
        label: "No action needed",
        title: "Your referral fees are reconciled",
        message:
          "We found no referral fee overcharges for this period. Fees charged align with our calculated applicable fees, so there is nothing you need to action right now.",
        accent: "#5EA68E",
        surface: "#EEF8F4",
        overchargedAmount,
        overchargedUnits,
        overchargeRate,
        affectedUnitsRate,
        accurateUnitsRate,
        netVariance,
      };
    }

    if (referralFeeInsight.status === "low") {
      return {
        label: "No immediate action needed",
        title: "Your referral fees are broadly on track",
        message: `We found ${fmtCurrency(overchargedAmount)} in potential overcharges, equal to ${overchargeRate.toFixed(2)}% of applicable referral fees. The value is within a low-variance range, so no immediate action is required.`,
        accent: "#5EA68E",
        surface: "#EEF8F4",
        overchargedAmount,
        overchargedUnits,
        overchargeRate,
        affectedUnitsRate,
        accurateUnitsRate,
        netVariance,
      };
    }

    if (referralFeeInsight.status === "review") {
      return {
        label: "Review recommended",
        title: "A referral fee variance is worth reviewing",
        message: `Potential overcharges total ${fmtCurrency(overchargedAmount)}, or ${overchargeRate.toFixed(2)}% of applicable referral fees. Review the affected products and orders to confirm whether follow-up is needed.`,
        accent: "#C58A16",
        surface: "#FFF8E1",
        overchargedAmount,
        overchargedUnits,
        overchargeRate,
        affectedUnitsRate,
        accurateUnitsRate,
        netVariance,
      };
    }

    return {
      label: "Action recommended",
      title: "Referral fee overcharges need attention",
      message: `Fees charged are materially above our calculated applicable fees. We found ${fmtCurrency(overchargedAmount)} in potential overcharges across ${fmtInteger(overchargedUnits)} units. Open the detailed analysis to identify the affected products and orders.`,
      accent: "#B75A5A",
      surface: "#FEF2F2",
      overchargedAmount,
      overchargedUnits,
      overchargeRate,
      affectedUnitsRate,
      accurateUnitsRate,
      netVariance,
    };
  }, [fmtCurrency, referralFeeInsight]);

  const selectedPeriodLabel = useMemo(() => {
    if (range === "yearly") return year;
    if (range === "quarterly") return `${selectedQuarter} ${year}`;
    return `${month.charAt(0).toUpperCase() + month.slice(1)} ${year}`;
  }, [month, range, selectedQuarter, year]);


  const referralActionDiagnosis = useMemo(() => {
    if (String(searchParams.get("actionItem") || "") !== "referral-fee-variance") {
      return null;
    }

    return {
      title: "Review referral fee variance",
      description: `This focused view explains the referral-fee variance that triggered the Action Item for ${selectedPeriodLabel || "the selected period"}.`,
      metrics: [
        { label: "Potential overcharge", value: fmtCurrency(referralFeeInsight.overcharged_amount) },
        { label: "Units to review", value: fmtInteger(referralFeeInsight.overcharged_units) },
        { label: "Overcharge rate", value: `${referralFeeInsight.overcharge_rate_pct.toFixed(2)}%` },
        { label: "Affected units", value: `${referralFeeInsight.affected_units_pct.toFixed(2)}%` },
        { label: "Accurately charged", value: `${referralFeeInsight.accurate_units_pct.toFixed(2)}%` },
        { label: "Net fee variance", value: fmtCurrency(referralFeeInsight.net_variance) },
      ],
      whyItMatters:
        "The action is raised when charged referral fees are above the calculated applicable fees. Reviewing the affected units helps separate genuine overcharges from expected fee differences.",
      recommendedAction:
        "Open the detailed analysis, review the affected products and orders, and confirm the fee basis before raising a reimbursement or support case.",
      triggerRule:
        "Charged referral fees are above the calculated applicable referral fees (positive fee difference).",
    };
  }, [fmtCurrency, referralFeeInsight, searchParams, selectedPeriodLabel]);


  return (
    <div className="space-y-1.5 font-sans text-charcoal-500">

      {/* <div className="sticky top-0 z-40 bg-white border-b border-gray-200">
        <div className="w-full flex flex-col md:flex-row md:items-center md:justify-between gap-4 px-1 py-2 mb-2"> */}
      <div className="sticky top-0 z-40 w-full flex flex-col
  bg-[#F7F7F7]
  sm:flex-row md:items-center md:justify-between gap-1 sm:gap-4
  border-b border-gray-200">

        {/* LEFT: Title */}
        <div className="flex flex-col leading-tight w-full md:w-auto">
          {/* <div className="flex items-baseline gap-2">
            <PageBreadcrumb
              pageTitle="Expense Reconciliation - Amazon"
              variant="page"
              align="left"
              textSize="2xl"
              className="mb-0"
            />
            <span className="text-[#5EA68E] font-bold text-lg sm:text-2xl md:text-2xl">
              {effectiveCountry.toUpperCase()}
            </span>
          </div> */}
          <div className="flex items-baseline gap-2">
            <PageBreadcrumb pageTitle="Expense Reconciliation - Amazon" variant="page" align="left" textSize="2xl" />
            <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
              {/* Amazon{" "} */}
              {effectiveCountry?.toLowerCase() === "global"
                ? "Global"
                : effectiveCountry?.toUpperCase()}
            </span>
          </div>
        </div>

        {/* RIGHT: Filters */}
        <div className="flex w-full md:w-auto justify-start md:justify-end mb-2 ">
          <PeriodFiltersTable
            range={range}
            selectedMonth={month}
            selectedQuarter={selectedQuarter}
            selectedYear={year}
            yearOptions={[new Date().getFullYear(), new Date().getFullYear() - 1]}
            onRangeChange={(v) => {
              let nextMonth = month;
              let nextYear = year;

              if (v === "monthly") {
                const lastCompletedMonth = getLastCompletedMonth();
                nextMonth = lastCompletedMonth.month;
                nextYear = lastCompletedMonth.year;
              }

              if (v === "quarterly") {
                const lastCompletedMonth = getLastCompletedMonth();

                const defaultQuarter = getQuarterFromMonth(
                  lastCompletedMonth.month
                );

                setSelectedQuarter(defaultQuarter);

                nextYear = lastCompletedMonth.year;

                const m = quarterToMonth(defaultQuarter);
                if (m) nextMonth = m;
              }

              if (v === "yearly" && !nextMonth) {
                nextMonth = "january";
              }

              setRange(v);
              if (nextMonth) setMonth(nextMonth);
              if (nextYear) setYear(nextYear);
              setError(null);
            }}

            onMonthChange={(v) => {
              setMonth(v);
              setError(null);
            }}

            onQuarterChange={(q) => {
              setSelectedQuarter(q);
              const m = quarterToMonth(q);
              if (m) {
                setMonth(m);
                setError(null);
              }
            }}

            onYearChange={(v) => {
              const nextYear = String(v);
              setYear(nextYear);
              setError(null);
            }}
          />
          {/* </div> */}

        </div>
      </div>

      {referralActionDiagnosis && (
        <ActionDiagnosisPanel
          title={referralActionDiagnosis.title}
          description={referralActionDiagnosis.description}
          metrics={referralActionDiagnosis.metrics}
          whyItMatters={referralActionDiagnosis.whyItMatters}
          recommendedAction={referralActionDiagnosis.recommendedAction}
          triggerRule={referralActionDiagnosis.triggerRule}
          evidenceNote="Uses the same reconciliation metrics already shown on this page; no fee values are estimated in the diagnosis panel."
        />
      )}

      {
        loading && (
          <div className="flex flex-col items-center justify-center py-12 text-center">
            <Loader fullscreen transparent />
          </div>
        )
      }

      {
        !loading && !!error && (
          <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
            <div className="flex items-center">
              <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
              <span>{error}</span>
            </div>
          </div>
        )
      }

      {canShowContent && (
        <PreviewLockedSection
          enabled={isPreviewMode}
          title="Preview Mode"
          description="To view your real business data and analytics, please complete your profile and connect your Amazon account. This will unlock your performance dashboard and insights."
          buttonText="Complete Setup"
          onAction={handlePreviewAction}
        >
          <>
            <p className="mt-3 text-xs text-slate-600">
              FBA applicable fees are estimated from package measurements and the transaction date and price.
              Storage, inventory surcharges and program discounts are separate.
              {!Number.isFinite(card6.fbaFeesApplicable) && " Some products lack measurements or a supported rate; their fees and combined totals are unavailable."}
            </p>
            {!showDeepDive ? (
              <section className="relative mt-4 overflow-hidden rounded-2xl border border-[#CFE8DF] bg-white shadow-sm">

                {/* Soft background decoration */}
                <div
                  className="pointer-events-none absolute inset-0 opacity-90"

                />

                {/* Decorative wave */}
                <div
                  className="pointer-events-none absolute -right-20 top-16 h-48 w-[55%] rounded-[50%] opacity-40 blur-3xl"
                  style={{
                    background:
                      "linear-gradient(90deg, rgba(94,166,142,0.05), rgba(94,166,142,0.18))",
                  }}
                />

                <div className="relative z-10 px-5 py-6 sm:px-7 sm:py-7 lg:px-9 lg:py-8">

                  {/* =====================================================
    TOP AREA
====================================================== */}
                  <div className="relative">

                    {/* LEFT CONTENT */}
                    <div className="min-w-0 w-full">

                      {/* Summary copy */}
                      <div className="min-w-0 w-full">

                        {/* Status pill */}
                        <div
                          className="inline-flex items-center gap-2 rounded-full px-3 py-1.5 text-xs font-semibold"
                          style={{
                            color: reconciliationInsight.accent,
                            backgroundColor: reconciliationInsight.surface,
                          }}
                        >
                          <span
                            className="h-2 w-2 rounded-full"
                            style={{
                              backgroundColor: reconciliationInsight.accent,
                            }}
                          />

                          {reconciliationInsight.label}
                        </div>

                        {/* Heading */}
                        <h2
                          className="
          mt-2
          text-[24px]
          font-bold
          leading-tight
          tracking-[-0.025em]
          sm:text-[28px]
          lg:text-[31px]
          lg:pr-[250px]
        "
                          style={{ color: reconciliationInsight.accent }}
                        >
                          {reconciliationInsight.title}
                        </h2>

                        {/* Description */}
                        <p
                          className="
          mt-3
          w-full
          max-w-none
          whitespace-normal
          text-sm
          leading-6
          text-charcoal-500
          sm:leading-7
        "
                        >
                          {reconciliationInsight.message}
                        </p>
                      </div>
                    </div>

                    {/* CTA */}
                    <button
                      type="button"
                      onClick={() => setShowDeepDive(true)}
                      className="
      mt-5
      inline-flex
      items-center
      justify-center
      gap-3
      rounded-xl
      bg-[#37455F]
      px-5
      py-3
      text-sm
      font-semibold
      text-[#F8EDCE]
      shadow-[0_6px_14px_rgba(55,69,95,0.16)]
      transition-all
      duration-200

      hover:-translate-y-0.5
      hover:bg-[#2F3B52]
      hover:shadow-[0_10px_20px_rgba(55,69,95,0.20)]

      focus:outline-none
      focus:ring-2
      focus:ring-[#5EA68E]
      focus:ring-offset-2

      lg:absolute
      lg:right-0
      lg:top-0
      lg:mt-0
    "
                    >
                      Explore detailed analysis

                      <svg
                        viewBox="0 0 24 24"
                        className="h-4 w-4"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                      >
                        <path d="M5 12h14" />
                        <path d="m13 6 6 6-6 6" />
                      </svg>
                    </button>

                  </div>


                  {/* =====================================================
          METRIC CARDS
      ====================================================== */}
                  <div className="mt-7 grid grid-cols-1 gap-4 sm:grid-cols-3">
                    <SummaryMetricCard
                      title="Potential overcharge"
                      value={(
                        <div>
                          <div className="text-base font-semibold leading-none text-charcoal-500 sm:text-md">
                            {fmtCurrency(reconciliationInsight.overchargedAmount)}
                          </div>
                          <div className="mt-2 text-xs font-medium text-charcoal-500 sm:text-sm">
                            {reconciliationInsight.overchargeRate.toFixed(2)}% of applicable referral fees
                          </div>
                        </div>
                      )}
                      className="border border-[#B75A5A] border-t-4 border-t-[#B75A5A] bg-white/90 px-5 py-5 backdrop-blur"
                      titleClassName="text-xs text-charcoal-500 sm:text-sm"
                      valueClassName="mt-2"
                    />

                    <SummaryMetricCard
                      title="Units to review"
                      value={(
                        <div>
                          <div className="text-base font-semibold leading-none text-charcoal-500 sm:text-md">
                            {fmtInteger(reconciliationInsight.overchargedUnits)}
                          </div>
                          <div className="mt-2 text-xs font-medium text-charcoal-500 sm:text-sm">
                            {reconciliationInsight.affectedUnitsRate.toFixed(2)}% of reconciled units
                          </div>
                        </div>
                      )}
                      className="border border-[#FDD36F] border-t-4 border-t-[#FDD36F] bg-white/90 px-5 py-5 backdrop-blur"
                      titleClassName="text-xs text-charcoal-500 sm:text-sm"
                      valueClassName="mt-2"
                    />

                    <SummaryMetricCard
                      title="Accurately charged units"
                      value={(
                        <div>
                          <div className="text-base font-semibold leading-none text-charcoal-500 sm:text-md">
                            {reconciliationInsight.accurateUnitsRate.toFixed(2)}%
                          </div>
                          <div className="mt-2 text-xs font-medium text-charcoal-500 sm:text-sm">
                            Net fee variance: {fmtCurrency(reconciliationInsight.netVariance)}
                          </div>
                        </div>
                      )}
                      className="border border-[#75BBDA] border-t-4 border-t-[#75BBDA] bg-white/90 px-5 py-5 backdrop-blur"
                      titleClassName="text-xs text-charcoal-500 sm:text-sm"
                      valueClassName="mt-2"
                    />
                  </div>
                </div>
              </section>
            ) : (
              <>
                <div className="mt-4 flex items-center justify-between gap-3 rounded-xl border border-slate-200 bg-white px-4 py-3 shadow-sm">
                  <div>
                    {/* <p className="text-sm font-semibold text-charcoal-500">Detailed referral fee analysis</p> */}
                    <PageBreadcrumb
                      pageTitle="Detailed Referral Fee Analysis"
                      textSize="lg"
                      variant="page" />
                  </div>
                  {/* <button
                    type="button"
                    onClick={() => setShowDeepDive(false)}
                    className="inline-flex items-center gap-2 rounded-lg border border-[#37455F] bg-white px-4 py-2 text-sm font-semibold text-[#37455F] transition hover:bg-slate-50 focus:outline-none focus:ring-2 focus:ring-[#5EA68E] focus:ring-offset-2"
                  >
                    <span aria-hidden="true">←</span>
                    Back to overview
                  </button> */}
                  <Button
                    type="button"
                    variant="primary"
                    size="sm"
                    onClick={() => setShowDeepDive(false)}
                    startIcon={<IoMdArrowBack className="text-sm" />}
                  >
                    Back to overview
                  </Button>
                </div>

                <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,3fr)_minmax(0,2fr)] xl:items-stretch 2xl:grid-cols-2">
                  <SkuAgeingDonutChart
                    title="Reconciliation Distribution"
                    subtitle="Referral fee status across all units"
                    data={reconciliationDonutData}
                    totalUnits={reconciliationTotalUnits}
                    amountLabel="Fee Variance"
                    amountFormatter={fmtCurrency}
                  />

                  <section className="h-full rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
                    <PageBreadcrumb
                      pageTitle="Fee Type Breakdown"
                      variant="page"
                      align="left"
                      className="mb-3"
                    />

                    {/* <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                  <AmazonStatCard
                    label="Referral Fees"
                    current={card6.refFeesApplied}
                    previous={card6.refFeesApplicable}
                    deltaPct={feePercentages.referral_fees.charged_vs_applicable_pct}
                    inverseDelta
                    loading={false}
                    formatter={(value) => fmtFeeWithBackendPct(
                      value,
                      feePercentages.referral_fees.charged_net_sales_pct
                    )}
                    previousFormatter={(value) => fmtFeeWithBackendPct(
                      value,
                      feePercentages.referral_fees.applicable_net_sales_pct
                    )}
                    bottomLabel="Applicable"
                    className="border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]"
                  />
                  <AmazonStatCard
                    label="FBA Fees"
                    current={card6.fbaFees}
                    previous={card6.fbaFeesApplicable}
                    deltaPct={feePercentages.fba_fees.charged_vs_applicable_pct}
                    inverseDelta
                    loading={false}
                    formatter={(value) => fmtFeeWithBackendPct(
                      value,
                      feePercentages.fba_fees.charged_net_sales_pct
                    )}
                    previousFormatter={(value) => fmtFeeWithBackendPct(
                      value,
                      feePercentages.fba_fees.applicable_net_sales_pct
                    )}
                    bottomLabel="Applicable"
                    className="border-[#FDD36F] border-t-4 border-t-[#FDD36F]"
                  />
                  <AmazonStatCard
                    label="Platform Fees"
                    current={card6.platformFees}
                    previous={card6.platformFeesApplicable}
                    deltaPct={feePercentages.platform_fees.charged_vs_applicable_pct}
                    inverseDelta
                    loading={false}
                    formatter={(value) => fmtFeeWithBackendPct(
                      value,
                      feePercentages.platform_fees.charged_net_sales_pct
                    )}
                    previousFormatter={(value) => fmtFeeWithBackendPct(
                      value,
                      feePercentages.platform_fees.applicable_net_sales_pct
                    )}
                    bottomLabel="Applicable"
                    className="border-[#ED9F50] border-t-4 border-t-[#ED9F50]"
                  />
                  <AmazonStatCard
                    label="Other Fees"
                    current={card6.otherFees}
                    previous={card6.otherFeesApplicable}
                    deltaPct={feePercentages.other_fees.charged_vs_applicable_pct}
                    inverseDelta
                    loading={false}
                    formatter={(value) => fmtFeeWithBackendPct(
                      value,
                      feePercentages.other_fees.charged_net_sales_pct
                    )}
                    previousFormatter={(value) => fmtFeeWithBackendPct(
                      value,
                      feePercentages.other_fees.applicable_net_sales_pct
                    )}
                    bottomLabel="Applicable"
                    className="border-[#3A8EA4] border-t-4 border-t-[#3A8EA4]"
                  />
                </div> */}

                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <SummaryMetricCard
                        title="Referral Fees"
                        value={renderFeeCurrentValue(
                          card6.refFeesApplied,
                          feePercentages.referral_fees.charged_net_sales_pct
                        )}
                        comparisons={buildFeeComparison(
                          card6.refFeesApplicable,
                          feePercentages.referral_fees.applicable_net_sales_pct,
                          feePercentages.referral_fees.charged_vs_applicable_pct
                        )}
                        className="bg-white border border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]"
                      />

                      <SummaryMetricCard
                        title="FBA Fees"
                        value={renderFeeCurrentValue(
                          card6.fbaFees,
                          feePercentages.fba_fees.charged_net_sales_pct
                        )}
                        comparisons={buildFeeComparison(
                          card6.fbaFeesApplicable,
                          feePercentages.fba_fees.applicable_net_sales_pct,
                          feePercentages.fba_fees.charged_vs_applicable_pct
                        )}
                        className="bg-white border border-[#FDD36F] border-t-4 border-t-[#FDD36F]"
                      />

                      <SummaryMetricCard
                        title="Platform Fees"
                        value={renderFeeCurrentValue(
                          card6.platformFees,
                          feePercentages.platform_fees.charged_net_sales_pct
                        )}
                        comparisons={buildFeeComparison(
                          card6.platformFeesApplicable,
                          feePercentages.platform_fees.applicable_net_sales_pct,
                          feePercentages.platform_fees.charged_vs_applicable_pct
                        )}
                        className="bg-white border border-[#ED9F50] border-t-4 border-t-[#ED9F50]"
                      />

                      <SummaryMetricCard
                        title="Other Fees"
                        value={renderFeeCurrentValue(
                          card6.otherFees,
                          feePercentages.other_fees.charged_net_sales_pct
                        )}
                        comparisons={buildFeeComparison(
                          card6.otherFeesApplicable,
                          feePercentages.other_fees.applicable_net_sales_pct,
                          feePercentages.other_fees.charged_vs_applicable_pct
                        )}
                        className="bg-white border border-[#3A8EA4] border-t-4 border-t-[#3A8EA4]"
                      />
                    </div>
                  </section>
                </div>

                {/* ===================== 6 CARDS (UPDATED) ===================== */}

                {false && <div className="grid grid-cols-1 sm:grid-cols-3 lg:grid-cols-3 xl:grid-cols-3 gap-3 mt-4">
                  <SalesCard
                    title="Sales"
                    sales={card6.sales}
                    productSales={card6.productSales}
                    units={card6.units}
                    valueFmt={fmtCurrencyRounded}
                    borderColor="#75BBDA"
                    bgColor="#75BBDA4D"
                  />


                  <FeeCard
                    title="Total Amazon Fees"
                    sales={card6.sales}
                    charged={card6.totalFees}
                    applicable={card6.totalFeesApplicable}
                    fmtCurrency={fmtCurrencyRounded}
                    borderColor="#B75A5A"
                    bgColor="#B75A5A4D"
                  />

                  <FeeCard
                    title="Referral Fees"
                    sales={card6.sales}
                    charged={card6.refFeesApplied}
                    applicable={card6.refFeesApplicable}
                    fmtCurrency={fmtCurrencyRounded}
                    borderColor="#7B9A6D"
                    bgColor="#7B9A6D4D"
                  />

                  <FeeCard
                    title="FBA Fees"
                    sales={card6.sales}
                    charged={card6.fbaFees}
                    applicable={card6.fbaFeesApplicable}
                    fmtCurrency={fmtCurrencyRounded}
                    borderColor="#FDD36F"
                    bgColor="#FDD36F4D"
                  />

                  <FeeCard
                    title="Platform Fees"
                    sales={card6.sales}
                    charged={card6.platformFees}
                    applicable={card6.platformFeesApplicable}
                    fmtCurrency={fmtCurrencyRounded}
                    borderColor="#ED9F50"
                    bgColor="#ED9F504D"
                  />

                  <FeeCard
                    title="Other Fees"
                    sales={card6.sales}
                    charged={card6.otherFees}
                    applicable={card6.otherFeesApplicable}
                    fmtCurrency={fmtCurrencyRounded}
                    borderColor="#3A8EA4"
                    bgColor="#3A8EA44D"
                  />
                </div>}


                {/* ===================== BREAKDOWN SECTION (NEW) ===================== */}
                {false && (() => {
                  /* =========================
                     1) Pull Charge rows + Grand Total
                  ========================= */
                  const chargeAcc = rows.find(
                    (r) => String(r.sku ?? "").toLowerCase() === "charge - accurate"
                  );
                  const chargeOver = rows.find(
                    (r) => String(r.sku ?? "").toLowerCase() === "charge - overcharged"
                  );
                  const chargeUnder = rows.find(
                    (r) => String(r.sku ?? "").toLowerCase() === "charge - undercharged"
                  );
                  const chargeNoRef = rows.find(
                    (r) => String(r.sku ?? "").toLowerCase() === "charge - noreferallfee"
                  );
                  const grand = rows.find(
                    (r) => String(r.sku ?? "").toLowerCase() === "grand total"
                  );

                  const fmtPctSigned = (p: number) => {
                    const v = toNumberSafe(p);
                    const sign = v > 0 ? "+" : v < 0 ? "-" : "";
                    return `${sign}${Math.abs(v).toFixed(2)}%`;
                  };


                  const pctOf = (value: number, total: number) => {
                    const t = Math.max(1e-9, Math.abs(toNumberSafe(total)));
                    return (toNumberSafe(value) / t) * 100;
                  };

                  const totalSalesForPct = Math.max(1, Math.abs(toNumberSafe(grand?.net_sales_total_value)));


                  /* =========================
                     2) LEFT PANEL (Sales based): use net_sales_total_value from Charge lines
                     Requirement: show net_sales_total_value for:
                     Charge - Accurate, Charge - Undercharged, Charge - Overcharged, Charge - noreferallfee, Grand Total
                  ========================= */
                  // const leftList = [
                  //   {
                  //     label: "Total Sales",
                  //     value: toNumberSafe(grand?.net_sales_total_value),
                  //     color: "#F47A00",
                  //   },
                  //   {
                  //     label: "Accurately charged",
                  //     value: toNumberSafe(chargeAcc?.net_sales_total_value),
                  //     color: "#14B8A6",
                  //   },
                  //   {
                  //     label: "Over charged",
                  //     value: toNumberSafe(chargeOver?.net_sales_total_value),
                  //     color: "#EF4444",
                  //   },
                  //   {
                  //     label: "Undercharged",
                  //     value: toNumberSafe(chargeUnder?.net_sales_total_value),
                  //     color: "#F59E0B",
                  //   },
                  //   {
                  //     label: "No ref fee",
                  //     value: toNumberSafe(chargeNoRef?.net_sales_total_value),
                  //     color: "#94A3B8",
                  //   },
                  // ];

                  const totalSalesForLeft = Math.max(1e-9, Math.abs(toNumberSafe(grand?.net_sales_total_value)));

                  const leftList = [
                    {
                      label: "Total Sales",
                      value: toNumberSafe(grand?.net_sales_total_value),
                      pct: pctOf(toNumberSafe(grand?.net_sales_total_value), totalSalesForLeft),
                      color: "#75BBDA",
                    },
                    {
                      label: "Accurately charged",
                      value: toNumberSafe(chargeAcc?.net_sales_total_value),
                      pct: pctOf(toNumberSafe(chargeAcc?.net_sales_total_value), totalSalesForLeft),
                      color: "#C49466",
                    },
                    {
                      label: "Over charged",
                      value: toNumberSafe(chargeOver?.net_sales_total_value),
                      pct: pctOf(toNumberSafe(chargeOver?.net_sales_total_value), totalSalesForLeft),
                      color: "#B75A5A",
                    },
                    {
                      label: "Undercharged",
                      value: toNumberSafe(chargeUnder?.net_sales_total_value),
                      pct: pctOf(toNumberSafe(chargeUnder?.net_sales_total_value), totalSalesForLeft),
                      color: "#FDD36F",
                    },
                    {
                      label: "No ref fee",
                      value: toNumberSafe(chargeNoRef?.net_sales_total_value),
                      pct: pctOf(toNumberSafe(chargeNoRef?.net_sales_total_value), totalSalesForLeft),
                      color: "#ED9F50",
                    },
                  ];


                  /* =========================
                     3) RIGHT PANEL (Ref fee breakdown): use charged referral fee magnitudes
                     Also show difference in brackets next to value
                  ========================= */
                  const rightList = [
                    {
                      label: "Total Ref Fees",
                      value: getChargedReferralFees(grand),
                      diff: toNumberSafe(grand?.difference),
                      pct: pctOf(toNumberSafe(grand?.difference), totalSalesForPct),
                      color: "#7B9A6D",
                    },
                    {
                      label: "Accurately charged",
                      value: getChargedReferralFees(chargeAcc),
                      diff: toNumberSafe(chargeAcc?.difference),
                      pct: pctOf(toNumberSafe(chargeAcc?.difference), totalSalesForPct),
                      color: "#C49466",
                    },
                    {
                      label: "Over charged",
                      value: getChargedReferralFees(chargeOver),
                      diff: toNumberSafe(chargeOver?.difference),
                      pct: pctOf(toNumberSafe(chargeOver?.difference), totalSalesForPct),
                      color: "#B75A5A",
                    },
                    {
                      label: "Undercharged",
                      value: getChargedReferralFees(chargeUnder),
                      diff: toNumberSafe(chargeUnder?.difference),
                      pct: pctOf(toNumberSafe(chargeUnder?.difference), totalSalesForPct),
                      color: "#FDD36F",
                    },
                    {
                      label: "No ref fee",
                      value: getChargedReferralFees(chargeNoRef),
                      diff: toNumberSafe(chargeNoRef?.difference),
                      pct: pctOf(toNumberSafe(chargeNoRef?.difference), totalSalesForPct),
                      color: "#ED9F50",
                    },
                  ];


                  /* =========================
                     4) Bar scaling totals
                     - LEFT: scale by Grand Total sales (so bars are "out of total sales")
                     - RIGHT: scale by Grand Total ref fees (so bars are "out of total ref fees")
                  ========================= */
                  const leftTotalForBars = Math.max(
                    1,
                    Math.abs(toNumberSafe(grand?.net_sales_total_value))
                  );

                  const rightTotalForBars = Math.max(
                    1,
                    Math.abs(getChargedReferralFees(grand))
                  );

                  /* =========================
                     5) Row renderer
                     - shows bar + value (and optional diff in brackets)
                  ========================= */
                  const BarRow = ({
                    label,
                    value,
                    total,
                    color,
                    deltaPct,
                    showDelta = false,
                    pctColor,
                  }: {
                    label: string;
                    value: number;
                    total: number;
                    color: string;
                    deltaPct?: number;
                    showDelta?: boolean;
                    pctColor?: string;
                  }) => {

                    const v = Math.abs(toNumberSafe(value));
                    const t = Math.max(1, Math.abs(toNumberSafe(total)));
                    const barPct = Math.min(100, (v / t) * 100);

                    const deltaCls =
                      typeof deltaPct === "number"
                        ? deltaPct > 0
                          ? "text-emerald-600"
                          : deltaPct < 0
                            ? "text-red-600"
                            : "text-slate-500"
                        : "text-slate-500";

                    return (
                      <div
                        className={`grid items-center gap-3 ${showDelta
                          ? "grid-cols-[180px_1fr_170px]"
                          : "grid-cols-[180px_1fr_120px]"
                          }`}
                      >
                        {/* Label */}
                        <div className="text-sm text-slate-700">{label}</div>

                        {/* Bar */}
                        <div className="h-2 rounded-full bg-slate-100 overflow-hidden">
                          <div
                            className="h-2 rounded-full"
                            style={{ width: `${barPct}%`, backgroundColor: color }}
                          />
                        </div>

                        {/* Value (+ optional %) */}
                        {/* <div className="flex items-baseline justify-end whitespace-nowrap tabular-nums">
                    <span className="text-sm font-semibold text-slate-800 min-w-[90px] text-right">
                      {fmtNumber(value)}
                    </span>

                    {showDelta && (
                      <span className={`ml-1 text-xs font-bold min-w-[55px] text-right ${deltaCls}`}>
                        ({fmtPctSigned(deltaPct ?? 0)})
                      </span>
                    )}
                  </div> */}

                        {/* Value / Right column */}
                        <div className="flex items-baseline justify-end whitespace-nowrap tabular-nums">
                          <span className="text-sm font-semibold text-slate-800 min-w-[90px] text-right">
                            {fmtNumber(value)}
                          </span>

                          {showDelta ? (
                            <span className={`ml-1 text-xs font-bold min-w-[55px] text-right ${deltaCls}`}>
                              ({fmtPctSigned(deltaPct ?? 0)})
                            </span>
                          ) : (
                            <span
                              className={`ml-2 text-xs font-bold min-w-[55px] text-right ${deltaCls}`}
                            >
                              ({fmtPctSigned(deltaPct ?? 0)})
                            </span>
                          )}

                        </div>

                      </div>
                    );
                  };

                  const currencySymbol = fmtCurrency(0).replace(/[\d.,\s]/g, "");
                  const fmtNumber = (n: number) =>
                    toNumberSafe(n).toLocaleString(undefined, {
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    });

                  /* =========================
                     6) UI (wrapped in bordered div with heading)
                  ========================= */
                  return (
                    <div className="mt-4 rounded-2xl border border-slate-200 bg-white shadow-sm p-4">
                      <div className="flex flex-row items-center justify-between gap-2 flex-wrap w-full mb-2 md:mb-0">
                        <PageBreadcrumb
                          pageTitle="Referral Fee Recon"
                          variant="page"
                          align="left"
                          className="mb-0 md:mb-4 text-center"
                        />
                        <button type="button" onClick={handleDownloadFbaExcel} disabled={isPreviewMode || loading || !!error || !fbaOrders.length} className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-medium disabled:opacity-50">Download FBA Excel</button>
                        <DownloadButton
                          onClick={handleDownloadExcel}
                          disabled={isPreviewMode}
                        />
                      </div>

                      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                        {/* LEFT PANEL */}
                        {/* <div className="bg-slate-50 rounded-2xl border border-slate-200 p-4">
                    <div className="text-base font-semibold text-slate-800 mb-4">
                      Sales Summary <span className="text-slate-500">({currencySymbol})</span>
                    </div>


                    <div className="space-y-3">
                      {leftList.map((x) => (
                        <BarRow
                          key={x.label}
                          label={x.label}
                          value={x.value}
                          total={leftTotalForBars}
                          color={x.color}
                          showDelta={false}  
                        />
                      ))}

                    </div>
                  </div> */}

                        <div className="bg-slate-50 rounded-2xl border border-slate-200 p-4">
                          <div className="text-base font-semibold text-slate-800 mb-2">
                            Sales Summary <span className="text-slate-500">({currencySymbol})</span>
                          </div>

                          {/* LEFT headers */}
                          {/* LEFT headers */}
                          <div className="grid grid-cols-[180px_1fr_120px] items-center mb-2 text-[11px] text-slate-500 font-semibold">
                            <div /> {/* label column */}
                            <div /> {/* bar column */}

                            {/* single container for both headings (you control gap) */}
                            <div className="flex justify-end gap-4 pr-2">
                              <span>Net Sales</span>
                              <span>% of Sales</span>
                            </div>
                          </div>


                          <div className="space-y-3">
                            {leftList.map((x) => (
                              <BarRow
                                key={x.label}
                                label={x.label}
                                value={x.value}
                                total={leftTotalForBars}
                                color={x.color}
                                deltaPct={x.pct}
                                showDelta={false}
                              />
                            ))}
                          </div>
                        </div>


                        {/* RIGHT PANEL */}
                        {/* <div className="bg-slate-50 rounded-2xl border border-slate-200 p-4">
                    <div className="text-base font-semibold text-slate-800 mb-4">
                      Referral Fees Breakdown <span className="text-slate-500">(£)</span>
                    </div>


                    <div className="space-y-3">
                      {rightList.map((x) => (
                        <BarRow
                          key={x.label}
                          label={x.label}
                          value={x.value}
                          total={rightTotalForBars}
                          color={x.color}
                          deltaPct={x.pct}
                          showDelta={true}
                        />
                      ))}


                    </div>
                  </div> */}

                        <div className="bg-slate-50 rounded-2xl border border-slate-200 p-4">
                          <div className="text-base font-semibold text-slate-800 mb-2">
                            Referral Fees Breakdown <span className="text-slate-500">(£)</span>
                          </div>

                          {/* RIGHT headers */}
                          {/* Header row */}
                          <div className="grid grid-cols-[180px_1fr_170px] items-center mb-2 text-[11px] font-semibold text-slate-500">
                            {/* Empty label column */}
                            <div />

                            {/* Empty bar column */}
                            <div />

                            {/* Single container for both headings */}
                            <div className="flex justify-end gap-4 pr-2">
                              <span>Fees Charged</span>
                              <span>Delta</span>
                            </div>
                          </div>

                          <div className="space-y-3">
                            {rightList.map((x) => (
                              <BarRow
                                key={x.label}
                                label={x.label}
                                value={x.value}
                                total={rightTotalForBars}
                                color={x.color}
                                deltaPct={x.pct}
                                showDelta={true}
                              />
                            ))}
                          </div>
                        </div>

                      </div>
                    </div>
                  );
                })()}
                <div className="mt-4 bg-white rounded-xl border border-slate-200 shadow-sm px-2 md:px-4 pb-2 md:pb-4 w-full overflow-x-auto">
                  <div className="flex flex-col md:flex-row items-center justify-between gap-2 flex-wrap w-full mb-2 md:mb-0">
                    <PageBreadcrumb
                      pageTitle={(
                        <span>
                          Product-wise breakdown{" "}
                          <span className="text-green-500">({currencySymbol})</span>
                        </span>
                      )}
                      variant="page"
                      align="left"
                      className="mt-4 mb-0 md:mb-4 text-center"
                    />
                    <button type="button" onClick={handleDownloadFbaExcel} disabled={isPreviewMode || loading || !!error || !fbaOrders.length} className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-medium disabled:opacity-50">Download FBA Excel</button>
                    <DownloadButton
                      onClick={handleDownloadExcel}
                      disabled={isPreviewMode}
                    />
                  </div>

                  {/* <AiButton /> */}

                  <div className="w-full max-w-full overflow-hidden rounded-xl border border-gray-300 [&_table]:w-full">
                    <GroupedCollapsibleTable<any>
                      rows={groupedSkuTableDisplay}
                      getRowKey={(row, index) =>
                        row._isTotal
                          ? "TOTAL"
                          : row._isOthers
                            ? "OTHERS"
                            : row.sku || `${row.productName}-${index}`
                      }
                      leftCols={[
                        { key: "sno", label: "S.No.", align: "center", width: 60 },
                        { key: "productName", label: "Product Name", align: "left", width: 190 },
                      ]}
                      singleCols={[
                        { key: "sku", label: "SKU", align: "center", width: 120 },
                        { key: "units", label: "Units", align: "center", width: 90 },
                        { key: "sales", label: "Net Sales", align: "center", width: 110 },
                      ]}
                      groups={[
                        {
                          id: "referralFees",
                          label: "Referral Fees",
                          expandable: true,
                          collapsedCols: [
                            { key: "ref_charged", label: "Charged", align: "center", width: 110 },
                          ],
                          expandedCols: [
                            { key: "ref_applicable", label: "Applicable", align: "center", width: 110 },
                            { key: "ref_charged", label: "Charged", align: "center", width: 110 },
                          ],
                        },
                        {
                          id: "fbaFees",
                          label: "FBA Fees",
                          expandable: true,
                          collapsedCols: [
                            { key: "fba_charged", label: "Charged", align: "center", width: 110 },
                          ],
                          expandedCols: [
                            { key: "fba_applicable", label: "Applicable", align: "center", width: 110 },
                            { key: "fba_charged", label: "Charged", align: "center", width: 110 },
                          ],
                        },
                        {
                          id: "otherFees",
                          label: "Other Fees",
                          expandable: true,
                          collapsedCols: [
                            { key: "other_charged", label: "Charged", align: "center", width: 110 },
                          ],
                          expandedCols: [
                            { key: "other_applicable", label: "Applicable", align: "center", width: 110 },
                            { key: "other_charged", label: "Charged", align: "center", width: 110 },
                          ],
                        },
                        {
                          id: "totalFees",
                          label: "Total Fees",
                          expandable: true,
                          collapsedCols: [
                            { key: "total_charged", label: "Charged", align: "center", width: 110 },
                          ],
                          expandedCols: [
                            { key: "total_applicable", label: "Applicable", align: "center", width: 110 },
                            { key: "total_charged", label: "Charged", align: "center", width: 110 },
                          ],
                        },
                      ]}
                      layout={[
                        { type: "single", key: "sku" },
                        { type: "single", key: "units" },
                        { type: "single", key: "sales" },
                        { type: "group", id: "referralFees" },
                        { type: "group", id: "fbaFees" },
                        { type: "group", id: "otherFees" },
                        { type: "group", id: "totalFees" },
                      ]}
                      initialCollapsed={{
                        referralFees: false,
                        fbaFees: false,
                        otherFees: false,
                        totalFees: false,
                      }}
                      preserveColumnWidths="responsive"
                      tableClassName="w-full table-fixed border-separate border-spacing-0 bg-white text-[#414042] text-[12px] lg:text-[12px] min-[1700px]:text-[14px]"
                      isTotalRow={(row) => Boolean(row._isTotal)}
                      getRowClassName={(row, index) => {
                        if (row._isTotal) return "bg-[#EFEFEF] font-semibold";
                        if (row._isOthers) return "bg-white cursor-pointer";
                        return index % 2 === 0 ? "bg-white" : "bg-gray-50";
                      }}
                      onRowClick={(row) => {
                        if (row._isOthers) setShowAllProductRows(true);
                      }}
                      getValue={(row, columnKey) => {
                        if (columnKey === "sno") return row._isTotal ? "" : row.sno ?? "";

                        if (columnKey === "productName") {
                          if (row._isTotal) {
                            return <span className="font-semibold text-charcoal-500">Total</span>;
                          }

                          if (row._isOthers) {
                            return (
                              <button
                                type="button"
                                onClick={(event) => {
                                  event.stopPropagation();
                                  setShowAllProductRows(true);
                                }}
                                className="w-full text-left font-medium text-green-500"
                                title="Expand all products"
                              >
                                Others
                              </button>
                            );
                          }

                          return (
                            <button
                              type="button"
                              onClick={(event) => {
                                event.stopPropagation();
                                openReferralProductDrawer(row);
                              }}
                              className="w-full cursor-pointer text-left font-medium text-green-500 hover:underline"
                              title={`View details for ${row.productName}`}
                            >
                              {row.productName}
                            </button>
                          );
                        }

                        if (columnKey === "sku") {
                          return row._isOthers || row._isTotal ? "-" : row.sku || "-";
                        }

                        if (columnKey === "units") return fmtInteger(Number(row.units));

                        if (
                          columnKey === "sales" ||
                          columnKey === "ref_applicable" ||
                          columnKey === "ref_charged" ||
                          columnKey === "fba_applicable" ||
                          columnKey === "fba_charged" ||
                          columnKey === "other_applicable" ||
                          columnKey === "other_charged" ||
                          columnKey === "total_applicable" ||
                          columnKey === "total_charged"
                        ) {
                          return fmtMoneyNoSymbol(row[columnKey]);
                        }

                        return row[columnKey] ?? "";
                      }}
                    />

                  </div>
                </div>
              </>
            )}
          </>
        </PreviewLockedSection>
      )}

      <ReferralProductDrawer
        open={Boolean(selectedDrawerProduct)}
        onClose={() => setSelectedDrawerProduct(null)}
        block={selectedDrawerBlock}
        productName={selectedDrawerProduct?.name || ""}
        recObj={selectedDrawerRecObj}
        countryName={effectiveCountry}
        month={range === "monthly" ? month : ""}
        year={year}
        range={range}
        quarter={range === "quarterly" ? selectedQuarter : ""}
        drawerPeriodText={drawerAi.periodText || drawerFallbackPeriodText}
        currencySymbol={drawerCurrencySymbol}
        homeCurrency={homeCurrency}
        aiLoading={drawerAiLoading}
        aiError={drawerAiError}
      />

    </div >
  );
}
