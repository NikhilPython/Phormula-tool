"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Bargraph from "./BarGraph";
import GraphPage from "./GraphPage";
import CircleChart from "./CircleChart";
import CMchartofsku from "./CMchartofsku";
import SKUtable from "./SKUtable";
import IntegrationDashboard from "@/features/integration/IntegrationDashboard";
import PageBreadcrumb from "../common/PageBreadCrumb";
import { Modal } from "@/components/ui/modal";
import FileUploadForm from "@/app/(admin)/(ui-elements)/modals/FileUploadForm";
import PeriodFiltersTable from "../filters/PeriodFiltersTable";
import { IoMdLock } from "react-icons/io";
import Loader from "@/components/loader/Loader";
import { useGetUserDataQuery } from "@/lib/api/profileApi";
import ExcelJS from "exceljs";
import { saveAs } from "file-saver";
import type { ProfitChartExportApi, SkuExportPayload, TrendChartExportApi } from "@/lib/utils/exportTypes";
import PerformanceTrendChart from "./PerformanceTrendChart";
import SummaryMetricCard from "./SummaryMetricCard";
import { buildSkuWorksheetFromModel } from "@/lib/utils/excel/buildSkuWorksheet";
import SkuTopBottomTables from "./SkuTopBottomTables";
import type { TopBottomData } from "@/lib/pnl/topBottom";
import type { TableRow } from "./SKUtable";
import { motion, AnimatePresence } from "framer-motion";
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";
import Productinfoinpopup from '@/components/businessInsight/Productinfoinpopup';
import { useRouter, useSearchParams } from "next/navigation";
import SegmentedToggle from "@/components/ui/SegmentedToggle";
import ProductwisePerformance from "@/features/productwiseperformance/ProductwisePerformance";
import CashFlowPage from "@/app/(admin)/cashflow/[countryName]/[month]/[year]/CashFlowClient";
import { jwtDecode } from "jwt-decode";
import AmazonAdsConnect from "@/features/integration/AmazonAdsConnectLegacy";
import AmazonFetchSuccessModal from "@/features/integration/AmazonFetchSuccessModal";
import InventoryInsightsSection from "@/components/common/inventory/InventoryInsightsSection";
import { createZeroInventoryInsightsData } from "@/components/common/inventory/createZeroInventoryInsightsData";

import type {
  AgeingBucket,
  AgeingRiskHeatmapRow,
  AgeingRiskUnitSalesDataKey,
} from "@/components/common/inventory/AgeingRiskHeatmap";

import type {
  DonutChartItem,
} from "@/components/common/inventory/SkuAgeingDonutChart";

import type {
  AgeingTrendItem,
  AgeingTrendAllSeriesItem,
} from "@/components/common/inventory/AgeingTrendChart";

import type {
  ActionCardItem,
  ActionLogicItem,
} from "@/components/common/inventory/ActionBasedDashboard";
/* ---------------------- Types ---------------------- */
type Summary = {
  unit_sold: number;
  total_sales: number;
  asp?: number;
  gross_sales?: number;
  total_product_sales?: number;
  total_expense: number;
  cm2_profit: number;
  cm2_profit_total?: number;
  cm2_margins?: number;
  cm2_profit_percentage?: number;
  cm2_profit_per?: number;
  promotional_rebates?: number;
  promotional_rebates_percentage?: number;
  total_cous?: number;
  otherwplatform?: number;
  advertising_total?: number;
  advertising_total_final?: number;
  advertising_fees?: number;
  ads_spend?: number;
  total_ads?: number;
  total_amazon_fee?: number;
  tacos?: number;
};

type UploadRow = {
  country: string;
  month: string;
  year: string | number;
  total_sales: number;
  total_amazon_fee: number;
  total_cous: number;
  advertising_total: number;
  advertising_total_final?: number;
  advertising_fees?: number;
  ads_spend?: number;
  total_ads?: number;
  otherwplatform: number;
  taxncredit?: number;
  profit?: number;
  cm2_profit: number;
  cm2_profit_total?: number;
  cm2_margins?: number;
  cm2_profit_percentage?: number;
  cm2_profit_per?: number;
  promotional_rebates?: number;
  promotional_rebates_percentage?: number;
  total_profit: number;
  tacos?: number;
};

type SummaryComparisons = {
  lastMonth?: Summary;
  lastQuarter?: Summary;
  lastYear?: Summary;
};

type UploadHistoryResponse = {
  summary: Summary;
  summaryComparisons?: SummaryComparisons;
  [key: string]: unknown;
};

type PerformanceTrendSeries = {
  label: string;          // "Dec'25"
  net_sales: number[];    // per-sku OR per-day array (as your API gives)
  units: number[];
};

type PerformanceTrendPayload = {
  x: number[];            // [1..31] etc.
  xType: string;          // "day"
  series: PerformanceTrendSeries[];
};

type ObjectivePayload = {
  business_context?: string;
  country?: string;
  growth_intent?: string;
  inventory_clearance_priority?: boolean;
  profit_priority?: string;
  time_horizon?: string;
};

type RecommendationsMap = Record<
  string,
  {
    journey_summary?: string[];
    recommendation?: string;
    inventory_recommendation?: string;
    ads_recommendation?: string;
  }
> & {
  remaining_skus_recommendation?: string;
  remaining_skus_inventory_recommendation?: string;
  remaining_skus_ads_recommendation?: string;
};

type AiSummaryResponse = {
  summary?: string | null;
  recommendations?: string | RecommendationsMap | null;
  objective?: ObjectivePayload;
  objective_changed?: boolean;
  performance_trend?: PerformanceTrendPayload;
  performance_trend_metric?: "net_sales" | "units";
  portfolio_recommendation?: string | null;
};

type AiPanelData = {
  summaryBullets: string[];
  skuInsightsBullets: string[];
  recommendationBullets: string[];
  inventoryBullets: string[];
  recommendationsMap?: RecommendationsMap;
  objective?: ObjectivePayload;
  rawSummary?: string | null;
  rawRecommendations?: string | null;
  remainingSkusRecommendation?: string;
  portfolioRecommendation?: string | null;

  otherSkuIncludedProducts?: OtherSkuItem[];
};

type JwtPayload = {
  user_id?: string | number;
  [k: string]: unknown;
};

type RangeType = "monthly" | "quarterly" | "yearly";

type Quarter = "Q1" | "Q2" | "Q3" | "Q4";
const isQuarter = (v: string): v is Quarter =>
  (["Q1", "Q2", "Q3", "Q4"] as const).includes(v as Quarter);

type DropdownsProps = {
  initialRanged: string;
  initialCountryName: string;
  initialMonth: string;
  initialYear: string;
};
type ComparisonItem = {
  label: string;
  value?: number;
  diffPct?: number | null;
};

type InventoryCurrentRow = Record<string, any>;

type InventoryCurrentApiResponse = {
  success: boolean;
  table_name?: string;
  columns?: string[];
  rows?: InventoryCurrentRow[];
  total_rows?: number;

  categories?: Record<
    string,
    {
      items?: any[];
      product_count?: number;
      sku_count?: number;
      name?: string;
      label?: string;
      description?: string;
    }
  > | null;

  category_counts?: Record<string, number> | null;

  // ✅ ADD THIS
  high_alert_coverage_summary?: {
    average_coverage_ratio?: number;
    high_alert_sku_count?: number;
    high_alert_threshold?: number;
    items?: {
      alert?: string;
      coverage_ratio_months?: number;
      high_alert_threshold?: number;
      product_name?: string;
      sku?: string;
    }[];
  };

  // ✅ ADD THESE
  month?: string;
  year?: number;
  requested_month?: string;
  requested_year?: number;

  country_key?: string;

  combined_countries?: string[];
  country_results?: Record<string, InventoryCurrentApiResponse>;

  inventory_age_summary?: {
    total?: number;
    current_month_units_sold_total?: number;
    percentage_base_total?: number;
    sellable_total?: number;
    unfulfillable_total?: number;
    total_units_summary?: {
      current_month_units_sold?: {
        total?: number;
        percentage_share?: number;
      };
      sellable?: {
        total?: number;
        percentage_share?: number;
      };
      unfulfillable?: {
        total?: number;
        percentage_share?: number;
      };
    };
    columns?: Record<
      string,
      {
        total?: number;
        percentage_share?: number;
      }
    >;
  };

  message?: string;
};

type InventoryInsightsData = {
  heatmapBuckets: AgeingBucket[];
  heatmapData: AgeingRiskHeatmapRow[];
  donutSku: string;
  donutData: DonutChartItem[];
  donutTotalUnits: number;
  trendSelectedBucket: string;
  trendData: AgeingTrendItem[];
  trendLineColor: string;

  trendAllSeriesData: AgeingTrendAllSeriesItem[];

  trendBucketOptions: {
    label: string;
    value: string;
    color: string;
  }[];

  actions: ActionCardItem[];
  actionLogic: ActionLogicItem[];
  inventoryAgeSummary?: InventoryCurrentApiResponse["inventory_age_summary"];
};

type InventoryAgeSummaryItem = {
  month: string;
  month_number?: number;
  year: number;
  age_bucket: string;
  column: string;
  units: number;
};

type InventoryAgeMonthSummaryItem = {
  month: string;
  month_number: number;
  year: number;
  source?: string;
  totals: Record<string, number>;
};

type InventoryAgeSummaryApiResponse = {
  success: boolean;
  table_name?: string;
  month?: string;
  year?: number;
  country_key?: string;
  totals?: Record<string, number>;
  age_summary?: InventoryAgeSummaryItem[];
  month_summary?: InventoryAgeMonthSummaryItem[];
  message?: string;

  // ✅ ADD for global inventory trend response
  combined_countries?: string[];
  country_results?: Record<string, InventoryAgeSummaryApiResponse>;
};

const normalizeKey = (s: string) =>
  (s || "")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[^\w\s-]/g, "");


const monthIndexMap: Record<string, number> = {
  january: 0, february: 1, march: 2, april: 3, may: 4, june: 5,
  july: 6, august: 7, september: 8, october: 9, november: 10, december: 11,
};

type FetchedPeriods = Record<string, string[]>;
type UploadPeriodSource = {
  month?: string | number | null;
  month_num?: string | number | null;
  year?: string | number | null;
};


const readFetchedPeriods = (): FetchedPeriods => {
  if (typeof window === "undefined") return {};

  try {
    const raw = localStorage.getItem("fetchedPeriods");
    return raw ? (JSON.parse(raw) as FetchedPeriods) : {};
  } catch {
    return {};
  }
};


const writeFetchedPeriods = (fp: FetchedPeriods) => {
  if (typeof window === "undefined") return;
  localStorage.setItem("fetchedPeriods", JSON.stringify(fp));
};

const markFetched = (year: string, month?: string) => {
  if (typeof window === "undefined") return;

  const y = String(year).trim();
  const m = month ? month.toLowerCase() : "";

  const fp = readFetchedPeriods();

  if (!fp[y]) fp[y] = [];

  if (m && !fp[y].includes(m)) {
    fp[y].push(m);
  }

  fp[y] = fp[y]
    .filter(Boolean)
    .sort((a, b) => (monthIndexMap[a] ?? 99) - (monthIndexMap[b] ?? 99));

  writeFetchedPeriods(fp);

  if (m) {
    localStorage.setItem(
      "latestFetchedPeriod",
      JSON.stringify({ year: y, month: m })
    );
  }
};

const monthNames = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

const normalizeFetchedMonth = (
  month?: string | number | null,
  monthNum?: string | number | null
) => {
  const monthKey = String(month ?? "").trim().toLowerCase();

  if (monthKey && monthIndexMap[monthKey] !== undefined) {
    return monthKey;
  }

  const numericMonth = Number(monthNum ?? month);

  if (
    Number.isInteger(numericMonth) &&
    numericMonth >= 1 &&
    numericMonth <= 12
  ) {
    return monthNames[numericMonth - 1].toLowerCase();
  }

  return "";
};

const buildFetchedPeriodsFromUploads = (uploads: unknown): FetchedPeriods => {
  if (!Array.isArray(uploads)) return {};

  const periods: FetchedPeriods = {};

  uploads.forEach((upload) => {
    const row = upload as UploadPeriodSource;
    const year = String(row.year ?? "").trim();
    const month = normalizeFetchedMonth(row.month, row.month_num);

    if (!year || !month) return;

    if (!periods[year]) periods[year] = [];
    if (!periods[year].includes(month)) periods[year].push(month);
  });

  Object.keys(periods).forEach((year) => {
    periods[year] = periods[year].sort(
      (a, b) => (monthIndexMap[a] ?? 99) - (monthIndexMap[b] ?? 99)
    );
  });

  return periods;
};

const getLatestFetchedPeriodFromMap = (periods: FetchedPeriods) => {
  const latest = Object.entries(periods)
    .flatMap(([year, yearMonths]) =>
      yearMonths.map((month) => ({
        year,
        month,
        monthIndex: monthIndexMap[month] ?? -1,
      }))
    )
    .filter((period) => period.monthIndex >= 0)
    .sort((a, b) => {
      const yearDiff = Number(b.year) - Number(a.year);
      if (yearDiff !== 0) return yearDiff;
      return b.monthIndex - a.monthIndex;
    })[0];

  return latest ? { year: latest.year, month: latest.month } : null;
};

const getLastCompletedMonth = () => {
  const now = new Date();

  // current month - 1
  const lastCompleted = new Date(now.getFullYear(), now.getMonth() - 1, 1);

  return {
    month: monthNames[lastCompleted.getMonth()],
    year: String(lastCompleted.getFullYear()),
    monthIndex: lastCompleted.getMonth(),
  };
};

const getQuarterFromMonthIndex = (monthIndex: number): Quarter => {
  if (monthIndex <= 2) return "Q1";
  if (monthIndex <= 5) return "Q2";
  if (monthIndex <= 8) return "Q3";
  return "Q4";
};

const computeDefaultYearlyYear = () => {
  return String(new Date().getFullYear());
};

const computeDefaultMonthlyPeriod = () => {
  const lastCompleted = getLastCompletedMonth();

  return {
    month: lastCompleted.month.toLowerCase(),
    year: lastCompleted.year,
  };
};

const computeDefaultQuarterlyPeriod = () => {
  const lastCompleted = getLastCompletedMonth();

  return {
    quarter: getQuarterFromMonthIndex(lastCompleted.monthIndex),
    year: lastCompleted.year,
  };
};

const getPrevMonthLabel = (selectedMonth: string, selectedYear: number) => {
  const idx = monthIndexMap[selectedMonth.toLowerCase()];
  if (idx === undefined) return "Last month";

  const prev = new Date(selectedYear, idx - 1, 1);
  const mon = prev.toLocaleString("en-US", { month: "short" });
  const yy = String(prev.getFullYear()).slice(-2);
  return `${mon}'${yy}`;
};

const getPreviousMonthName = (monthName?: string, year?: string | number) => {
  const monthKey = String(monthName || "").toLowerCase();
  const monthIndex = monthIndexMap[monthKey];

  if (monthIndex === undefined) {
    return {
      month: "",
      shortMonth: "",
      year: Number(year || new Date().getFullYear()),
    };
  }

  const selectedYear = Number(year || new Date().getFullYear());
  const prevDate = new Date(selectedYear, monthIndex - 1, 1);

  const month = prevDate.toLocaleString("en-US", {
    month: "long",
  });

  return {
    month: month.toLowerCase(),
    shortMonth: month.slice(0, 3).toLowerCase(),
    year: prevDate.getFullYear(),
  };
};

const formatSalesLast30DaysMonthLabel = (
  range: RangeType,
  selectedMonth: string,
  selectedQuarter: Quarter | "",
  selectedYear: string
) => {
  const yearNumber = Number(selectedYear);
  const currentYear = new Date().getFullYear();

  const monthFullNames = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
  ];

  const quarterEndMonthMap: Record<Quarter, string> = {
    Q1: "March",
    Q2: "June",
    Q3: "September",
    Q4: "December",
  };

  if (range === "monthly") {
    const monthName = String(selectedMonth || "").trim();

    return monthName
      ? monthName.charAt(0).toUpperCase() + monthName.slice(1).toLowerCase()
      : "";
  }

  if (range === "quarterly") {
    return selectedQuarter ? quarterEndMonthMap[selectedQuarter] : "";
  }

  if (range === "yearly") {
    if (yearNumber === currentYear) {
      const previousCompletedMonth = new Date(
        new Date().getFullYear(),
        new Date().getMonth() - 1,
        1
      );

      return monthFullNames[previousCompletedMonth.getMonth()];
    }

    return "December";
  }

  return "";
};

const buildCurrentMonthUnitsSoldLabel = (
  range: RangeType,
  selectedMonth: string,
  selectedQuarter: Quarter | "",
  selectedYear: string
) => {
  const monthLabel = formatSalesLast30DaysMonthLabel(
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear
  );

  if (!monthLabel) {
    return "Unit Sales";
  }

  return `${monthLabel}'s Unit Sales`;
};

const getPreviousSalesRankForSelectedMonth = (
  row: InventoryCurrentRow,
  selectedMonth?: string,
  selectedYear?: string | number
) => {
  const keys = Object.keys(row || {});

  const previousMonth = getPreviousMonthName(selectedMonth, selectedYear);

  // ✅ For selected June, first prefer:
  // Previous Month Sales Rank (May)
  const exactPreviousMonthKey = keys.find((key) => {
    const lowerKey = key.toLowerCase();

    return (
      lowerKey.startsWith("previous month sales rank") &&
      (
        lowerKey.includes(previousMonth.month) ||
        lowerKey.includes(previousMonth.shortMonth)
      )
    );
  });

  if (exactPreviousMonthKey) {
    return row?.[exactPreviousMonthKey];
  }

  // ✅ fallback: if backend sends any key like:
  // Previous Month Sales Rank (June)
  const anyPreviousRankKey = keys.find((key) =>
    key.toLowerCase().startsWith("previous month sales rank")
  );

  return anyPreviousRankKey
    ? row?.[anyPreviousRankKey]
    : row?.previous_sales_rank ??
    row?.previousSalesRank ??
    row?.["Previous Month Sales Rank"] ??
    "";
};

const getCurrencySymbol = (codeOrCountry: string) => {
  const v = (codeOrCountry || "").toLowerCase();

  switch (v) {
    case "usd":
    case "us":
    case "global":
      return "$";
    case "inr":
    case "india":
      return "₹";
    case "gbp":
    case "uk":
      return "£";
    case "eur":
    case "europe":
    case "eu":
      return "€";
    case "cad":
    case "ca":
    case "canada":
      return "C$";
    default:
      return "¤";
  }
};

const getPrevQuarterLabel = (q: Quarter, selectedYear: number) => {
  const order: Quarter[] = ["Q1", "Q2", "Q3", "Q4"];
  const idx = order.indexOf(q);
  if (idx === -1) return "Prev quarter";

  const prevIdx = (idx - 1 + 4) % 4;
  const prevQuarter = order[prevIdx];

  const prevYear = idx === 0 ? selectedYear - 1 : selectedYear; // if Q1 -> prev is Q4 of prev year
  const yy = String(prevYear).slice(-2);

  return `${prevQuarter}'${yy}`; // Q4'25
};

const getPrevYearLabel = (selectedYear: number) => {
  return String(selectedYear - 1); // 2024
};

const splitMetricValue = (value: string) => {
  const v = String(value || "").trim();

  // Supports:
  // $862 (+0.00%)
  // $439 (-97.07%)
  // $0.00 (0.00%)
  const match = v.match(/^(.+?)\s*(\(([+-]?)[^)]+\))\s*$/);

  if (!match) {
    return {
      main: v,
      delta: "",
      deltaColor: "",
    };
  }

  const main = match[1].trim();
  const delta = match[2].trim();
  const sign = match[3];

  return {
    main,
    delta,
    deltaColor:
      sign === "+"
        ? "text-emerald-600"
        : sign === "-"
          ? "text-red-600"
          : "text-charcoal-500",
  };
};

const formatMetricDelta = (delta: string) => {
  const cleanDelta = String(delta || "")
    .replace(/[()]/g, "")
    .trim();

  if (!cleanDelta) return "";

  const isPositive = cleanDelta.startsWith("+");
  const isNegative = cleanDelta.startsWith("-");
  const valueWithoutSign = cleanDelta.replace(/^[-+]/, "");

  if (isPositive) return `▲ ${valueWithoutSign}`;
  if (isNegative) return `▼ ${valueWithoutSign}`;

  return valueWithoutSign;
};


const RecommendationMetricCard = ({
  metric,
}: {
  metric: { label: string; value: string; color?: string };
}) => {
  const { main, delta, deltaColor } = splitMetricValue(metric.value);

  const displayMain = formatRecommendationCardMainValue(
    metric.label,
    main
  );

  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50 px-2 py-2 min-w-0">
      <div
        title={metric.label}
        className="text-xs font-medium text-charcoal-500 leading-tight truncate"
      >
        {formatDisplayMetricLabel(metric.label)}
      </div>

      <div className="mt-1 min-[1700px]:flex min-[1700px]:w-full min-[1700px]:items-baseline min-[1700px]:justify-between min-[1700px]:gap-2 leading-tight tabular-nums">
        <div className="text-xs font-semibold text-charcoal-500 truncate">
          {displayMain}
        </div>

        {delta ? (
  <div
    className={[
      "mt-0.5 min-[1700px]:mt-0 text-xs font-semibold whitespace-nowrap",
      "min-[1700px]:shrink-0 min-[1700px]:text-right",
      deltaColor,
    ].join(" ")}
  >
    {formatMetricDelta(delta)}
  </div>
) : (
  ["cm1 profit", "cm1 profit per unit", "cm2 profit", "cm2 profit per unit"]
    .includes(metric.label.trim().toLowerCase()) && (
      <div className="mt-0.5 min-[1700px]:mt-0 text-xs font-semibold whitespace-nowrap text-charcoal-400">
        -
      </div>
    )
)}
      </div>
    </div>
  );
};

const formatRecommendationCardMainValue = (label: string, main: string) => {
  const normalizedLabel = String(label || "").trim().toLowerCase();

  if (
    normalizedLabel !== "net sales" &&
    normalizedLabel !== "cm1 profit" &&
    normalizedLabel !== "cm2 profit"
  ) {
    return main;
  }

  const currencyMatch = main.match(/^([^0-9-]*)/);
  const currency = currencyMatch?.[1] ?? "";

  const numberPart = main.replace(/[^0-9.-]/g, "");
  const numberValue = Number(numberPart);

  if (!Number.isFinite(numberValue)) return main;

  return `${currency}${Math.round(numberValue).toLocaleString()}`;
};

const formatMetricTitle = (label: string) => {
  return String(label || "")
    .replace(/\b\w/g, (char) => char.toUpperCase())
    .replace("Cm1", "CM1")
    .replace("Cm2", "CM2");
};

const formatDisplayMetricLabel = (label: string) => {
  const normalized = String(label || "").trim().toLowerCase();

  if (normalized === "stock cover") {
    return "Stock Cover (Months)";
  }

  return formatMetricTitle(label);
};

const monthNameToNumber = (m: string): string => {
  const idx = monthIndexMap[(m || "").toLowerCase()];
  return typeof idx === "number" ? String(idx + 1) : "";
};

type OtherSkuItem = {
  product_name: string;
  sku: string;
};

type ProductInsightMetric = {
  label: string;
  value: string;
  color?: string;
};

type ProductInsightBlock = {
  name: string;
  skuKey?: string;
  metrics: ProductInsightMetric[];

  // ✅ Only drawer metrics. These will NOT show on recommendation cards.
  drawerOnlyMetrics?: ProductInsightMetric[];

  journeyBullets: string[];
  recommendationBullets: string[];
  inventoryBullets: string[];
  isOtherSkus?: boolean;
  includedSkus?: OtherSkuItem[];
};


const parseProductInsightsBlocks = (
  lines: string[],
  range: RangeType = "monthly"
): ProductInsightBlock[] => {
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

  const isMetric = (s: string) =>
    metricLabels.some((m) => s.toLowerCase().startsWith(m.toLowerCase() + ":"));

  const blocks: ProductInsightBlock[] = [];
  let current: ProductInsightBlock | null = null;

  const getMetricNumberValue = (value?: string) => {
    const main = String(value || "").split("(")[0] || "";
    const n = Number(main.replace(/[^0-9.-]/g, ""));
    return Number.isFinite(n) ? n : 0;
  };

  const cleanProfitMetricsForDisplay = (
    block: ProductInsightBlock
  ): ProductInsightBlock => {
    const metrics = block.metrics || [];

    const cm1Profit = metrics.find(
  (m) => m.label.trim().toLowerCase() === "cm1 profit"
);

const cm2Profit = metrics.find(
  (m) => m.label.trim().toLowerCase() === "cm2 profit"
);

const cm1ProfitValue = getMetricNumberValue(cm1Profit?.value);
const cm2ProfitValue = getMetricNumberValue(cm2Profit?.value);

const profitsAreSame =
  Math.abs(cm1ProfitValue - cm2ProfitValue) < 0.01;

// ✅ Quarterly / Yearly me always CM1
// ✅ Monthly me:
//    CM1 === CM2 -> CM1
//    CM1 !== CM2 -> CM2
const useCm1 =
  !isMonthlyRange(range) ||
  !cm2Profit ||
  profitsAreSame;

    const cleanedMetrics = metrics
      .map((m) => {
        const label = m.label.trim();
        const lower = label.toLowerCase();

        if (lower === "coverage ratio") {
          return {
            ...m,
            label: "Stock Cover",
            value:
              m.value && m.value !== "0.00"
                ? getMetricNumberValue(m.value).toFixed(2)
                : "-",
          };
        }

        return m;
      })
      .filter((m) => {
        const lower = m.label.trim().toLowerCase();

        // ✅ Ads sirf drawer me dikhega, card me nahi
        if (lower === "productwise ads spend" || lower === "ads") return false;

        // ✅ Current inventory sirf drawer me dikhega, card me nahi
        if (lower === "current inventory") return false;

        // ✅ Stock Cover sirf monthly range me show hoga
        if (!isMonthlyRange(range) && lower === "stock cover") return false;

        // ✅ Quarterly / Yearly me CM2 card se remove
        if (useCm1 && (lower === "cm2 profit" || lower === "cm2 profit per unit")) {
          return false;
        }

        // ✅ Monthly me agar CM2 valid hai to CM1 remove
        if (!useCm1 && (lower === "cm1 profit" || lower === "cm1 profit per unit")) {
          return false;
        }

        return true;
      });

    return {
      ...block,
      metrics: cleanedMetrics,
    };
  };

  const pushCurrent = () => {
    if (current && current.name.trim()) {
      blocks.push(cleanProfitMetricsForDisplay(current));
    }

    current = null;
  };

  let inJourney = false;
  let inIncludedSkus = false;

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];

    const line = raw
      .replace(/^[-•]\s+/, "")
      .replace(/^\d+\.\s*/, "")
      .trim();

    if (!line) continue;

    const nextLine = lines[i + 1]
      ?.replace(/^[-•]\s+/, "")
      .replace(/^\d+\.\s*/, "")
      .trim();

    const isProductHeader =
      !isMetric(line) &&
      !line.toLowerCase().startsWith("sku:") &&
      !line.toLowerCase().startsWith("bucket:") &&
      !line.toLowerCase().startsWith("recommendation:") &&
      !line.toLowerCase().startsWith("ads action:") &&
      !line.toLowerCase().startsWith("product journey") &&
      !line.toLowerCase().startsWith("inventory action:") &&
      !!nextLine &&
      (
        isMetric(nextLine) ||
        nextLine.toLowerCase().startsWith("sku:")
      );

    if (isProductHeader) {
      pushCurrent();

      const skuFromParen = line.match(/\(([A-Z0-9-]+)\)/i)?.[1]?.trim();
      const skuFromPrefix = line.match(/^([A-Z0-9-]+)\s*[-–]\s*/i)?.[1]?.trim();

      const cleanName = line
        .replace(/\([A-Z0-9-]+\)/i, "")
        .replace(/^([A-Z0-9-]+)\s*[-–]\s*/i, "")
        .trim();

      const isOther = (cleanName || line).trim().toLowerCase() === "other skus";

      current = {
        name: cleanName || line,
        skuKey: skuFromParen || skuFromPrefix,
        metrics: [],
        drawerOnlyMetrics: [],
        journeyBullets: [],
        recommendationBullets: [],
        inventoryBullets: [],
        isOtherSkus: isOther,
        includedSkus: [],
      };

      inJourney = false;
      inIncludedSkus = false;
      continue;
    }

    if (!current) continue;

    const lowerLine = line.toLowerCase();

    if (lowerLine.startsWith("sku:")) {
      current.skuKey = line.replace(/^sku:\s*/i, "").trim();
      continue;
    }

    if (lowerLine.startsWith("bucket:")) {
      continue;
    }

    if (
      lowerLine.startsWith("included skus") ||
      lowerLine.startsWith("products included")
    ) {
      inJourney = false;
      inIncludedSkus = true;
      continue;
    }

    if (inIncludedSkus) {
      const lowerLine = line.toLowerCase();

      const isNextSection =
        lowerLine.startsWith("product journey") ||
        lowerLine.startsWith("recommendation:") ||
        lowerLine.startsWith("inventory action:") ||
        lowerLine.startsWith("ads action:");

      if (!isNextSection) {
        const match = line.match(/^(.+?)\s*\(([^)]+)\)$/);

        if (match) {
          current.includedSkus?.push({
            product_name: match[1].trim(),
            sku: match[2].trim(),
          });
        }

        continue;
      }

      inIncludedSkus = false;
    }

    if (line.toLowerCase().startsWith("product journey")) {
      inIncludedSkus = false;
      inJourney = true;
      continue;
    }

    if (line.toLowerCase().startsWith("recommendation:")) {
      inJourney = false;
      const reco = line.replace(/^recommendation:\s*/i, "").trim();
      if (reco) current.recommendationBullets.push(reco);
      continue;
    }

    if (line.toLowerCase().startsWith("ads action:")) {
      inJourney = false;
      const ads = line.replace(/^ads action:\s*/i, "").trim();
      if (ads) current.recommendationBullets.push(ads);
      continue;
    }

    if (line.toLowerCase().startsWith("inventory action:")) {
      inJourney = false;
      const inv = line.replace(/^inventory action:\s*/i, "").trim();
      if (inv) current.inventoryBullets.push(inv);
      continue;
    }

    if (isMetric(line)) {
      const [label, ...rest] = line.split(":");
      const value = rest.join(":").trim();

      const num = value.match(/[-+]?[\d,.]+/g)?.[0]?.replace(/,/g, "");
      const n = num ? Number(num) : NaN;
      const color =
        !isNaN(n) ? (n < 0 ? "#DC2626" : n > 0 ? "#059669" : "#414042") : "#414042";

      const cleanLabel = label.trim();
      const normalizedMetricLabel = cleanLabel.toLowerCase();

      const finalLabel =
        normalizedMetricLabel === "coverage ratio"
          ? "Stock Cover"
          : cleanLabel;

      const finalValue =
        normalizedMetricLabel === "coverage ratio" ||
          normalizedMetricLabel === "stock cover"
          ? getMetricNumberValue(value).toFixed(2)
          : value;

      const metricItem = {
        label: finalLabel,
        value: finalValue,
        color,
      };

      // ✅ Current Inventory sirf drawer me dikhega
      // ✅ Current Inventory sirf monthly drawer me dikhega
      if (normalizedMetricLabel === "current inventory") {
        if (isMonthlyRange(range)) {
          current.drawerOnlyMetrics = [
            ...(current.drawerOnlyMetrics || []),
            {
              label: "Current Inventory",
              value,
              color,
            },
          ];
        }

        continue;
      }

      // ✅ Productwise ads spend sirf monthly drawer me dikhega
      if (normalizedMetricLabel === "productwise ads spend") {
        if (isMonthlyRange(range)) {
          current.drawerOnlyMetrics = [
            ...(current.drawerOnlyMetrics || []),
            {
              label: "Ads",
              value: formatMoneyStringNoDecimal(value),
              color: "#414042",
            },
          ];
        }

        continue;
      }

      current.metrics.push(metricItem);

      continue;
    }

    if (inJourney) {
      const cleaned = line.replace(/^-+\s*/, "").trim();
      if (cleaned) current.journeyBullets.push(cleaned);
      continue;
    }
  }

  pushCurrent();
  return blocks;
};

const parseMdSections = (md?: string | null): Record<string, string[]> => {
  if (!md) return {};

  const lines = md.split(/\r?\n/);

  const sections: Record<string, string[]> = {};
  let current = "ROOT";
  sections[current] = [];

  for (const raw of lines) {
    const line = raw.trim();

    if (line.toLowerCase().startsWith("## ")) {
      current = line.replace(/^##\s+/i, "").trim().toUpperCase();
      if (!sections[current]) sections[current] = [];
      continue;
    }

    if (line) {
      sections[current].push(line);
    }
  }

  const normalize = (l: string) =>
    l.replace(/^[-•]\s+/, "").trim();

  const out: Record<string, string[]> = {};
  for (const [k, arr] of Object.entries(sections)) {
    out[k] = arr
      .filter((l) => !l.startsWith("##"))
      .map(normalize)
      .filter(Boolean);
  }

  return out;
};

const extractRecoAndInventoryBullets = (
  mdOrObj?: string | RecommendationsMap | null
) => {
  // ✅ Case 1: object-based recommendations (new API)
  if (mdOrObj && typeof mdOrObj === "object") {
    return {
      recommendationBullets: [],
      inventoryBullets: [],
      recommendationsMap: mdOrObj as RecommendationsMap,
    };
  }

  // ✅ Case 2: markdown (old API)
  if (!mdOrObj || typeof mdOrObj !== "string") {
    return { recommendationBullets: [], inventoryBullets: [], recommendationsMap: undefined };
  }

  const sections = parseMdSections(mdOrObj);

  return {
    recommendationBullets: sections["ROOT"] ?? [],
    inventoryBullets: sections["INVENTORY"] ?? [],
    recommendationsMap: undefined,
  };
};

const toBullets = (text?: string) => {
  if (!text) return [];
  const t = text.trim();

  if (t.includes("\n")) {
    return t
      .split("\n")
      .map((x) => x.replace(/^[-•]\s+/, "").trim())
      .filter(Boolean);
  }

  return t
    .split(/(?:\.\s+|;\s+|\s\|\s)/g)
    .map((x) => x.trim())
    .filter(Boolean);
};

const toNum = (v: any) => {
  if (v === null || v === undefined) return 0;
  if (typeof v === "number") return Number.isFinite(v) ? v : 0;
  const n = Number(String(v).replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : 0;
};

const calculateTacos = (netSales: any, adsCost: any) => {
  const sales = toNum(netSales);
  const ads = Math.abs(toNum(adsCost));

  return sales > 0 ? (ads / sales) * 100 : 0;
};

const pickFirstNonZeroNum = (...values: any[]) => {
  let fallback = 0;
  let hasFallback = false;

  for (const value of values) {
    if (value === null || value === undefined || value === "") continue;

    const n = toNum(value);

    if (!hasFallback) {
      fallback = n;
      hasFallback = true;
    }

    if (n !== 0) return n;
  }

  return fallback;
};

const pickFirstNonZeroOrNull = (...values: any[]) => {
  for (const value of values) {
    if (value === null || value === undefined || value === "") continue;

    const n = toNum(value);
    if (n !== 0) return n;
  }

  return null;
};

const hasOwnAdSpendField = (row: any) =>
  row &&
  typeof row === "object" &&
  ["advertising_fees", "ads_spend", "total_ads"].some((key) =>
    Object.prototype.hasOwnProperty.call(row, key)
  );

const getNoCm2AdvertisingValue = (row: any) => {
  const requestedValue = pickFirstNonZeroOrNull(
    row?.advertising_fees,
    row?.ads_spend,
    row?.total_ads
  );

  if (requestedValue !== null) return requestedValue;
  if (hasOwnAdSpendField(row)) return 0;

  return pickFirstNonZeroNum(
    row?.advertising_total_final,
    row?.advertising_total,
    row?.cost_of_advertisement
  );
};

const hasSkuCm2Data = (rows: any[] = []) => {
  const productRows = rows.filter((row) => {
    const productName = String(row?.product_name || "").trim().toLowerCase();
    const sku = String(row?.sku || "").trim().toLowerCase();

    return (
      productName !== "total" &&
      productName !== "grand total" &&
      productName !== "others" &&
      sku !== "total" &&
      sku !== "grand_total" &&
      sku !== "others"
    );
  });

  if (!productRows.length) return false;

  return productRows.some((row) => {
    const adsSpend = toNum(row?.ads_spend ?? row?.advertising_total);
    const backendAcos = toNum(row?.acos);
    const netSales = toNum(row?.net_sales);
    const acos = backendAcos || (netSales !== 0 ? (adsSpend / netSales) * 100 : 0);
    const cm1Profit = toNum(row?.profit);
    const cm2Profit = toNum(row?.cm2_profit ?? row?.cm2_profit_total);

    return adsSpend !== 0 || acos !== 0 || Math.abs(cm2Profit - cm1Profit) > 0.01;
  });
};

const getAdvertisingTotalValue = (row: any, hasCm2Columns = true) =>
  hasCm2Columns
    ? pickFirstNonZeroNum(
      row?.advertising_total_final,
      row?.advertising_total,
      row?.total_ads,
      row?.ads_spend,
      row?.cost_of_advertisement
    )
    : getNoCm2AdvertisingValue(row);

const getAdvertisingBaseValue = (row: any, hasCm2Columns = true) => {
  if (!hasCm2Columns) return getNoCm2AdvertisingValue(row);

  const directValue = pickFirstNonZeroNum(
    row?.advertising_total,
    row?.cost_of_advertisement
  );

  if (directValue !== 0) return directValue;

  const childTotal =
    toNum(row?.brand_spend) +
    toNum(
      row?.dealsvouchar_ads ??
      row?.dealvouchars_ads ??
      row?.dealsvoucher_ads ??
      row?.deals_voucher_ads
    );

  return childTotal !== 0 ? childTotal : directValue;
};

const getCm2ProfitValue = (row: any) =>
  pickFirstNonZeroNum(
    row?.cm2_profit_total,
    row?.total_cm2_profit,
    row?.cm2_profit,
    row?.profit
  );

const getCm2MarginValue = (row: any, netSales: any, cm2Profit: any) =>
  pickFirstNonZeroNum(
    row?.cm2_margins,
    row?.total_cm2_margins,
    row?.cm2_profit_percentage,
    row?.cm2_profit_percent,
    row?.cm2_profit_percentage_value,
    toNum(netSales) !== 0 ? (toNum(cm2Profit) / toNum(netSales)) * 100 : 0
  );

const getCm2PerValue = (row: any, netSales: any, cm2Profit: any) =>
  pickFirstNonZeroNum(
    row?.cm2_profit_per,
    row?.cm2_profit_per_unit,
    row?.cm2_profit_percentage,
    row?.cm2_profit_percent,
    row?.cm2_profit_percentage_value,
    toNum(netSales) !== 0 ? (toNum(cm2Profit) / toNum(netSales)) * 100 : 0
  );

const mergeToSingleBullet = (arr: string[]) => {
  const cleaned = (arr || []).map(s => String(s).trim()).filter(Boolean);
  if (!cleaned.length) return [];
  return [cleaned.join(" ")];
};

const formatPct = (v: any) => {
  if (v === null || v === undefined || !Number.isFinite(Number(v))) return "";
  const n = Number(v);
  return `(${n >= 0 ? "+" : ""}${n.toFixed(2)}%)`;
};

const formatMoney = (v: any, symbol = "$") => {
  const n = Number(v ?? 0);

  return `${symbol}${n.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
};

const formatMoneyNoDecimal = (v: any, symbol = "$") => {
  const n = Math.round(toNum(v));

  return `${symbol}${n.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  })}`;
};

const formatMoneyStringNoDecimal = (value: string, symbol = "$") => {
  const main = String(value || "").split("(")[0] || "";
  const deltaMatch = String(value || "").match(/\([^)]*\)/)?.[0] || "";

  const numberValue = Number(main.replace(/[^0-9.-]/g, ""));

  if (!Number.isFinite(numberValue)) return value;

  return `${formatMoneyNoDecimal(numberValue, symbol)}${deltaMatch ? ` ${deltaMatch}` : ""
    }`;
};

const formatBestPerformancePeriod = (month?: string, year?: string | number) => {
  if (!month) return "-";

  const monthMap: Record<string, string> = {
    january: "Jan",
    february: "Feb",
    march: "Mar",
    april: "Apr",
    may: "May",
    june: "Jun",
    july: "Jul",
    august: "Aug",
    september: "Sep",
    october: "Oct",
    november: "Nov",
    december: "Dec",
  };

  const shortMonth =
    monthMap[String(month).toLowerCase()] || String(month).slice(0, 3);

  const shortYear = year ? String(year).slice(-2) : "";

  return shortYear ? `${shortMonth}'${shortYear}` : shortMonth;
};

const buildOtherSkusInsightLines = (
  apiData: any,
  currencySymbol = "$",
  countryName = "global",
  range: RangeType = "monthly"
): string[] => {
  const otherComparison = apiData?.global_ai?.other_skus_comparison;

  const remainingAgg =
    apiData?.metrics?.remaining_agg ||
    apiData?.remaining_agg;

  if (!otherComparison && !remainingAgg) return [];

  const countryKey = String(countryName || "global").toLowerCase();

  const actionObj =
    otherComparison?.country_actions?.[countryKey] ||
    otherComparison?.country_actions?.global ||
    {};

  const journeyLines = Array.isArray(otherComparison?.journey_comparison)
    ? otherComparison.journey_comparison
    : [];

  const productName =
    otherComparison?.product_name ||
    remainingAgg?.product_name ||
    "Other SKUs";

  const includedSkuLines = Array.isArray(remainingAgg?.included_products)
    ? [
      "Included SKUs",
      ...remainingAgg.included_products.map(
        (p: any) => `- ${p.product_name} (${p.sku})`
      ),
    ]
    : [];

  return [
    productName,

    `ASP: ${formatMoney(remainingAgg?.asp?.current, currencySymbol)} ${formatPct(
      remainingAgg?.asp?.delta_pct
    )}`,

    `Units: ${Number(
      remainingAgg?.total_quantity?.current ?? 0
    ).toLocaleString()} ${formatPct(
      remainingAgg?.total_quantity?.delta_pct
    )}`,

    `Net sales: ${formatMoney(
      remainingAgg?.net_sales?.current,
      currencySymbol
    )} ${formatPct(remainingAgg?.net_sales?.delta_pct)}`,

    (() => {
      const profitConfig = getProfitMetricConfig(remainingAgg, range);

      return `${profitConfig.profitLabel}: ${formatMetricObjectWithDelta(
        profitConfig.profitMetric,
        "money",
        currencySymbol
      )}`;
    })(),

    (() => {
      const profitConfig = getProfitMetricConfig(remainingAgg, range);

      return `${profitConfig.profitPerUnitLabel}: ${formatMetricObjectWithDelta(
        profitConfig.profitPerUnitMetric,
        "money",
        currencySymbol
      )}`;
    })(),

    ...(isMonthlyRange(range)
      ? [
        `Stock Cover: ${formatCoverageValue(
          remainingAgg?.selected_period_coverage_ratio
        )}`,
      ]
      : []),

    `Current Inventory: ${formatInventoryUnitsValue(
      remainingAgg?.current_inventory
    )}`,

    ...(isMonthlyRange(range)
      ? [
        `Productwise ads spend: ${formatMoneyNoDecimal(
          getMetricCurrent(remainingAgg?.productwise_ads_spend),
          currencySymbol
        )}${formatPct(getMetricDeltaPct(remainingAgg?.productwise_ads_spend))}`,
      ]
      : []),

    ...includedSkuLines,

    "Product Journey",

    ...journeyLines.map((line: string) => `- ${line}`),

    actionObj?.recommendation
      ? `Recommendation: ${actionObj.recommendation}`
      : "",

    actionObj?.inventory_recommendation
      ? `Inventory Action: ${actionObj.inventory_recommendation}`
      : "",
  ].filter(Boolean);
};

const getOtherSkuIncludedProducts = (data: any): OtherSkuItem[] => {
  const includedProducts =
    data?.remaining_agg?.included_products ||
    data?.metrics?.remaining_agg?.included_products ||
    [];

  if (!Array.isArray(includedProducts)) return [];

  return includedProducts
    .map((p: any) => ({
      product_name: String(p?.product_name || "").trim(),
      sku: String(p?.sku || "").trim(),
    }))
    .filter((p) => p.product_name && p.sku);
};

const formatMetricValueWithDelta = (
  current: any,
  deltaPct: any,
  type: "money" | "number",
  currencySymbol = "$"
) => {
  const value = toNum(current);

  const main =
    type === "money"
      ? formatMoney(value, currencySymbol)
      : Math.round(value).toLocaleString();

  const delta =
    deltaPct === null || deltaPct === undefined || !Number.isFinite(Number(deltaPct))
      ? ""
      : ` (${Number(deltaPct) >= 0 ? "+" : ""}${Number(deltaPct).toFixed(2)}%)`;

  return `${main}${delta}`;
};

const getMetricCurrent = (metric: any) => {
  if (metric && typeof metric === "object" && "current" in metric) {
    return metric.current;
  }

  return metric;
};

const getMetricDeltaPct = (metric: any) => {
  if (metric && typeof metric === "object" && "delta_pct" in metric) {
    return metric.delta_pct;
  }

  return null;
};

const getMetricPrevious = (metric: any) => {
  if (metric && typeof metric === "object" && "previous" in metric) {
    return metric.previous;
  }

  return undefined;
};

const areValuesSame = (
  firstValue: any,
  secondValue: any,
  tolerance = 0.01
) => {
  const first = toNum(firstValue);
  const second = toNum(secondValue);

  return Math.abs(first - second) < tolerance;
};

const hasRealCm2ForPeriod = (
  cm1Metric: any,
  cm2Metric: any,
  period: "current" | "previous"
) => {
  if (!cm2Metric || typeof cm2Metric !== "object") {
    return false;
  }

  const cm1Value =
    period === "current"
      ? getMetricCurrent(cm1Metric)
      : getMetricPrevious(cm1Metric);

  const cm2Value =
    period === "current"
      ? getMetricCurrent(cm2Metric)
      : getMetricPrevious(cm2Metric);

  if (
    cm2Value === null ||
    cm2Value === undefined ||
    cm2Value === ""
  ) {
    return false;
  }

  // Backend fallback me CM2 ko CM1 ke equal bhej raha hai.
  // Equal hone ka matlab genuine CM2 available nahi hai.
  return !areValuesSame(cm1Value, cm2Value);
};

const removeMetricDelta = (metric: any) => {
  if (!metric || typeof metric !== "object") {
    return metric;
  }

  return {
    ...metric,
    delta_pct: null,
  };
};

const areMetricValuesSame = (
  firstMetric: any,
  secondMetric: any,
  tolerance = 0.01
) => {
  const firstValue = toNum(getMetricCurrent(firstMetric));
  const secondValue = toNum(getMetricCurrent(secondMetric));

  return Math.abs(firstValue - secondValue) < tolerance;
};

const getProfitMetricConfig = (
  row: any,
  range: RangeType = "monthly"
) => {
  const cm1ProfitMetric = row?.profit;
  const cm2ProfitMetric = row?.cm2_profit;

  const currentHasRealCm2 = hasRealCm2ForPeriod(
    cm1ProfitMetric,
    cm2ProfitMetric,
    "current"
  );

  const previousHasRealCm2 = hasRealCm2ForPeriod(
    cm1ProfitMetric,
    cm2ProfitMetric,
    "previous"
  );

  // Quarterly / Yearly -> CM1
  // Monthly current period me genuine CM2 ho -> CM2
  const useCm1 =
    !isMonthlyRange(range) ||
    !currentHasRealCm2;

  if (useCm1) {
    return {
      useCm1: true,
      canShowDelta: true,

      profitLabel: "CM1 profit",
      profitMetric: row?.profit,

      profitPerUnitLabel: "CM1 profit per unit",
      profitPerUnitMetric: row?.unit_wise_profitability,
    };
  }

  /*
   * Current period me CM2 available hai.
   *
   * Lekin previous period me genuine CM2 nahi hai:
   * CM2 value show hogi, delta nahi.
   */
  return {
    useCm1: false,
    canShowDelta: previousHasRealCm2,

    profitLabel: "CM2 profit",

    profitMetric: previousHasRealCm2
      ? row?.cm2_profit
      : removeMetricDelta(row?.cm2_profit),

    profitPerUnitLabel: "CM2 profit per unit",

    profitPerUnitMetric: previousHasRealCm2
      ? row?.cm2_profit_per_unit ?? row?.cm2_profit_per
      : removeMetricDelta(
          row?.cm2_profit_per_unit ?? row?.cm2_profit_per
        ),
  };
};

const formatCoverageValue = (value: any) => {
  const n = toNum(getMetricCurrent(value));

  if (!Number.isFinite(n) || n <= 0) return "-";

  return n.toFixed(2);
};

const formatInventoryUnitsValue = (value: any) => {
  const n = toNum(getMetricCurrent(value));

  if (!Number.isFinite(n)) return "-";

  return Math.round(n).toLocaleString();
};

const isMonthlyRange = (range?: RangeType) => range === "monthly";

const cleanInventoryRecommendationText = (text?: string) => {
  return String(text || "")
    .replace(
      /^Your coverage ratio is\s*[\d.]+\s*months\s*(?:and\s*)?/i,
      ""
    )
    .replace(/^and\s+/i, "")
    .trim();
};

const getActionTextFromRecObj = (recObj: any) => {
  return String(recObj?.recommendation || "").trim();
};

const getInventoryTextFromRecObj = (recObj: any) => {
  return cleanInventoryRecommendationText(recObj?.inventory_recommendation);
};

const getAdsTextFromRecObj = (recObj: any) => {
  return String(recObj?.ads_recommendation || "").trim();
};

const dedupeRecommendationLines = (lines: any[]) => {
  const seen = new Set<string>();

  return (lines || [])
    .map((line) => String(line || "").replace(/^\.+\s*/, "").trim())
    .filter(Boolean)
    .filter((line) => {
      const key = line.toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
};

const formatMetricObjectWithDelta = (
  metric: any,
  type: "money" | "number",
  currencySymbol = "$"
) => {
  return formatMetricValueWithDelta(
    getMetricCurrent(metric),
    getMetricDeltaPct(metric),
    type,
    currencySymbol
  );
};

const buildDrawerBlockFromSkuRow = (
  sku: string,
  row: any,
  recObj: any,
  currencySymbol = "$",
  range: RangeType = "monthly"
): ProductInsightBlock => {
  const name = String(row?.product_name || sku).trim();
  const profitConfig = getProfitMetricConfig(row, range);

  return {
    name,
    skuKey: sku,
    metrics: [
      {
        label: "Units",
        value: formatMetricObjectWithDelta(
          row?.total_quantity ?? row?.quantity,
          "number",
          currencySymbol
        ),
      },
      {
        label: "Net sales",
        value: formatMetricObjectWithDelta(
          row?.net_sales,
          "money",
          currencySymbol
        ),
      },
      {
        label: "ASP",
        value: formatMetricObjectWithDelta(
          row?.asp,
          "money",
          currencySymbol
        ),
      },
      {
        label: profitConfig.profitLabel,
        value: formatMetricObjectWithDelta(
          profitConfig.profitMetric,
          "money",
          currencySymbol
        ),
      },
      {
        label: profitConfig.profitPerUnitLabel,
        value: formatMetricObjectWithDelta(
          profitConfig.profitPerUnitMetric,
          "money",
          currencySymbol
        ),
      },
      ...(isMonthlyRange(range)
        ? [
          {
            label: "Stock Cover",
            value: formatCoverageValue(row?.selected_period_coverage_ratio),
          },
        ]
        : []),
    ],

    // ✅ Sirf drawer me show hoga
    drawerOnlyMetrics: [
      ...(isMonthlyRange(range)
        ? [
          {
            label: "Current Inventory",
            value: formatInventoryUnitsValue(row?.current_inventory),
          },
          {
            label: "Ads",
            value: `${formatMoneyNoDecimal(
              getMetricCurrent(row?.productwise_ads_spend),
              currencySymbol
            )}${formatPct(getMetricDeltaPct(row?.productwise_ads_spend))}`,
            color: "#414042",
          },
        ]
        : []),
    ],

    journeyBullets: Array.isArray(recObj?.journey_summary)
      ? recObj.journey_summary
      : [],
    recommendationBullets: recObj?.recommendation
      ? [recObj.recommendation]
      : [],
    inventoryBullets: recObj?.inventory_recommendation
      ? [recObj.inventory_recommendation]
      : [],
    isOtherSkus: name.toLowerCase() === "other skus",
  };
};

const getPeriodBadge = (range: RangeType, year: string, month?: string, quarter?: string) => {
  const yy = String(year || "").slice(-2);

  if (range === "monthly" && month) {
    const shortMonth = month.slice(0, 3);
    return `${shortMonth}'${yy}`;
  }

  if (range === "quarterly" && quarter) {
    return `${quarter}'${yy}`;
  }

  if (range === "yearly" && year) {
    return String(year);
  }

  return "";
};

type BestPerformanceMetric = {
  month?: string;
  year?: string | number;

  units?: number;
  net_sales?: number;
  asp?: number;
  cm1_profit?: number;
  unit_wise_profitability?: number;
};

type ProductBestPerformanceData = {
  units?: BestPerformanceMetric;
  net_sales?: BestPerformanceMetric;
  asp?: BestPerformanceMetric;
  cm1_profit?: BestPerformanceMetric;
  unit_wise_profitability?: BestPerformanceMetric;
};

type RightProductDrawerProps = {
  open: boolean;
  onClose: () => void;
  block: ProductInsightBlock | null;
  objective?: ObjectivePayload;
  recObj?: any;
  countryName: string;
  month: string;
  year: string;
  range: RangeType;
  quarter?: string;
  drawerPeriodText?: string;

  // ✅ ADD THIS
  currencySymbol?: string;

  // ✅ NEW: ProductwisePerformance graph data
  perfLoading?: boolean;
  perfError?: string | null;
  perfData?: {
    rows: {
      x: string;
      selectedValue?: number;
      otherValue?: number;
      value?: number;
    }[];
  } | null;
  perfMetric?: "net_sales" | "units";
  onPerfMetricChange?: (metric: "net_sales" | "units") => void;
  bestPerformanceLoading?: boolean;
  bestPerformanceError?: string | null;
  bestPerformanceData?: ProductBestPerformanceData | null;
};

const metricColors = [
  "border border-[#FDD36F] border-t-4", // Units
  "border border-[#75BBDA] border-t-4", // Net Sales
  "border border-[#B75A5A] border-t-4", // ASP
  "border border-[#C49466] border-t-4", // Ads
  "border border-[#7B9A6D] border-t-4", // CM2 Profit
  "border border-[#C49466] border-t-4", // CM2 Profit Per Unit
  "border border-[#7B9A6D] border-t-4", // CM1 Profit
  "border border-[#C49466] border-t-4", // CM1 Profit Per Unit
  "border border-[#7B9A6D] border-t-4", // Current Inventory
  "border border-[#C49466] border-t-4", // Stock Cover
  "border border-[#C49466] border-t-4", // Productwise Ads Spend
];

const metricOrder = [
  "units",
  "net sales",
  "asp",
  "ads", // ✅ Ads will now come beside ASP in drawer
  "cm2 profit",
  "cm2 profit per unit",
  "cm1 profit",
  "cm1 profit per unit",
  "current inventory",
  "stock cover",
  "productwise ads spend",
];

const RightProductDrawer: React.FC<RightProductDrawerProps> = ({
  open,
  onClose,
  block,
  objective,
  recObj,
  countryName,
  month,
  year,
  range,
  quarter,
  drawerPeriodText,
  currencySymbol,
  perfLoading = false,
  perfError = null,
  perfData = null,
  perfMetric = "net_sales",
  onPerfMetricChange,
  bestPerformanceLoading = false,
  bestPerformanceError = null,
  bestPerformanceData = null,
}) => {
  const inventoryText =
    recObj?.inventory_recommendation || block?.inventoryBullets?.join(" ");

  const inventoryRecoBullets = toBullets(inventoryText);
  const adsRecoBullets = toBullets(recObj?.ads_recommendation);
  const periodBadge = getPeriodBadge(range, year, month, quarter);

  const drawerCurrencySymbol =
    currencySymbol || getCurrencySymbol(countryName || "");

  const isOtherSkusBlock = !!block?.isOtherSkus;
  const sortedMetrics = [
    ...(block?.metrics || []),
    ...(block?.drawerOnlyMetrics || []),
  ]
    .filter((m) => {
      const lower = m.label.trim().toLowerCase();
      const isGlobal = String(countryName || "").toLowerCase() === "global";

      // ✅ Global me stock cover/current inventory kabhi mat dikhao
      if (
        isGlobal &&
        [
          "stock cover",
          "stock cover (months)",
          "current inventory",
          "current inventory units",
        ].includes(lower)
      ) {
        return false;
      }

      // ✅ Quarterly / Yearly me drawer metric cards se ye hide
      if (!isMonthlyRange(range)) {
        return ![
          "ads",
          "productwise ads spend",
          "stock cover",
          "stock cover (months)",
          "current inventory",
          "current inventory units",
          "cm2 profit",
          "cm2 profit per unit",
        ].includes(lower);
      }

      return true;
    })
    .sort((a, b) => {
      const aIndex = metricOrder.indexOf(a.label.toLowerCase());
      const bIndex = metricOrder.indexOf(b.label.toLowerCase());

      const safeAIndex = aIndex === -1 ? Number.MAX_SAFE_INTEGER : aIndex;
      const safeBIndex = bIndex === -1 ? Number.MAX_SAFE_INTEGER : bIndex;

      return safeAIndex - safeBIndex;
    });

  const metricsGridClass = isMonthlyRange(range)
    ? "grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-4 min-[1700px]:grid-cols-4"
    : "grid grid-cols-2 gap-3 sm:grid-cols-3 min-[1700px]:grid-cols-5";

  const getMetricBorderColorByLabel = (label: string, fallbackIndex = 0) => {
    const normalizedLabel = label.trim().toLowerCase();
    const metricIndex = metricOrder.indexOf(normalizedLabel);

    return metricColors[
      metricIndex !== -1 ? metricIndex : fallbackIndex % metricColors.length
    ];
  };

  const getRecentQuarterFromPreviousMonth = () => {
    const previousMonth = getPreviousCompletedMonth();

    const monthIndex = monthIndexMap[previousMonth.month];

    return {
      quarter: getQuarterFromMonthIndex(monthIndex),
      year: previousMonth.year,
    };
  };

  const getPreviousCompletedMonth = () => {
    const prevMonthDate = new Date();
    prevMonthDate.setMonth(prevMonthDate.getMonth() - 1);

    return {
      month: prevMonthDate.toLocaleString("en-US", {
        month: "long",
      }).toLowerCase(),
      year: String(prevMonthDate.getFullYear()),
    };
  };

  const showCurrentPeriodRecommendations = (() => {
    const previousMonth = getPreviousCompletedMonth();

    if (range === "yearly") {
      return String(year) === previousMonth.year;
    }
    if (range === "quarterly") {
      const recentQuarter = getRecentQuarterFromPreviousMonth();

      return (
        String(year) === recentQuarter.year &&
        String(quarter) === recentQuarter.quarter
      );
    }

    if (range === "monthly") {
      return (
        String(year) === previousMonth.year &&
        String(month).toLowerCase() === previousMonth.month
      );
    }

    return false;
  })();

  if (!open || !block) return null;

  return (
    <AnimatePresence>
      {open && (
        <>
          <motion.div
            className="fixed inset-0 z-[999999] h-full bg-black/40"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
          />


          <motion.aside
            className="fixed right-0 top-0 z-[1000000] h-screen w-[95vw] sm:w-[75vw] md-[60vw] lg:w-[50vw] min-[1700px]:w-[50vw] bg-white shadow-2xl"
            initial={{ x: 520 }}
            animate={{ x: 0 }}
            exit={{ x: 520 }}
            transition={{ type: "tween", duration: 0.25 }}
          >
            <div className="flex h-full flex-col gap-4">
              <div className="flex shrink-0 items-center justify-between gap-3 border-b border-slate-200 p-3">

                <div className="min-w-0 flex-1">
                  <div className="flex flex-col sm:flex-row sm:items-center gap-1">

                    <div className="flex items-center gap-1 flex-wrap">
                      <PageBreadcrumb
                        pageTitle="Detailed View - "
                        variant="page"
                        textSize="2xl"
                      />
                    </div>

                    {/* Green section */}
                    <div className="flex flex-wrap items-center gap-1 ">
                      <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
                        {block.name || "Details"}
                      </span>

                      {drawerPeriodText ? (
                        <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
                          {drawerPeriodText}
                        </span>
                      ) : periodBadge ? (
                        <span className="rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[11px] text-slate-700">
                          {periodBadge}
                        </span>
                      ) : null}
                    </div>

                  </div>
                </div>

                <button
                  onClick={onClose}
                  className="shrink-0 rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
                >
                  ✕
                </button>

              </div>

              <div className="flex-1 space-y-6 overflow-y-auto px-3 pb-4">
                <div>
                  <PageBreadcrumb
                    pageTitle="Metrics"
                    variant="page"
                    align="left"
                    textSize="xl"
                    className="mb-2"
                  />

                  <div className={metricsGridClass}>
                    {sortedMetrics.map((m, i) => {
                      const { main, delta, deltaColor } = splitMetricValue(m.value);

                      const displayMain =
                        m.label.trim().toLowerCase() === "ads"
                          ? formatMoneyStringNoDecimal(main, drawerCurrencySymbol)
                          : formatRecommendationCardMainValue(m.label, main);

                      const isAdsMetric = m.label.trim().toLowerCase() === "ads";

                      return (
                        <div
                          key={`${m.label}-${i}`}
                          className={[
                            "w-full rounded-xl bg-white shadow-sm p-1.5 2xl:p-2",
                            "flex flex-col justify-between min-h-[60px] min-[1700px]:min-h-[72px]",
                            getMetricBorderColorByLabel(m.label, i),
                          ].join(" ")}
                        >
                          <div className="flex justify-between items-center">
                            <span className="text-[10px] 2xl:text-xs font-medium text-charcoal-500">
                              {formatDisplayMetricLabel(m.label)}
                            </span>
                          </div>

                          <div className="mt-1 flex items-baseline justify-between gap-3 leading-tight tabular-nums">
                            <span className="text-sm 2xl:text-lg font-semibold text-charcoal-500 truncate">
                              {displayMain}
                            </span>

                            {delta ? (
  <span
    className={[
      "text-[10px] 2xl:text-xs font-semibold whitespace-nowrap text-right",
      isAdsMetric ? "text-charcoal-500" : deltaColor,
    ].join(" ")}
  >
    {formatMetricDelta(delta)}
  </span>
) : (
  [
    "cm1 profit",
    "cm1 profit per unit",
    "cm2 profit",
    "cm2 profit per unit",
  ].includes(m.label.trim().toLowerCase()) && (
    <span className="text-[10px] 2xl:text-xs font-semibold whitespace-nowrap text-right text-charcoal-400">
      -
    </span>
  )
)}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>

                {!isOtherSkusBlock && (
                  <div>
                    <PageBreadcrumb
                      pageTitle="Overall Best Performance"
                      variant="page"
                      align="left"
                      textSize="xl"
                    />

                    <p className="mb-2 text-xs 2xl:text-sm text-charcoal-500 mt-1">
                      Best performance is calculated from overall historical data, not just the selected period.
                    </p>

                    {bestPerformanceLoading ? (
                      <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-xs text-charcoal-500 2xl:text-sm">
                        Loading best performance...
                      </div>
                    ) : bestPerformanceError ? (
                      <div className="rounded-lg border border-red-100 bg-red-50 px-3 py-3 text-xs text-red-600 2xl:text-sm">
                        {bestPerformanceError}
                      </div>
                    ) : bestPerformanceData ? (
                      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-5">
                        {[
                          {
                            label: "Units",
                            value: Math.round(toNum(bestPerformanceData?.units?.units)).toLocaleString(),
                            period: formatBestPerformancePeriod(
                              bestPerformanceData?.units?.month,
                              bestPerformanceData?.units?.year
                            ),
                          },
                          {
                            label: "Net Sales",
                            value: formatMoneyNoDecimal(
                              bestPerformanceData?.net_sales?.net_sales,
                              drawerCurrencySymbol
                            ),
                            period: formatBestPerformancePeriod(
                              bestPerformanceData?.net_sales?.month,
                              bestPerformanceData?.net_sales?.year
                            ),
                          },
                          {
                            label: "ASP",
                            value: formatMoney(
                              bestPerformanceData?.asp?.asp,
                              drawerCurrencySymbol
                            ),
                            period: formatBestPerformancePeriod(
                              bestPerformanceData?.asp?.month,
                              bestPerformanceData?.asp?.year
                            ),
                          },
                          {
                            label: "CM1 Profit",
                            value: formatMoneyNoDecimal(
                              bestPerformanceData?.cm1_profit?.cm1_profit,
                              drawerCurrencySymbol
                            ),
                            period: formatBestPerformancePeriod(
                              bestPerformanceData?.cm1_profit?.month,
                              bestPerformanceData?.cm1_profit?.year
                            ),
                          },
                          {
                            label: "CM1 Profit Per Unit",
                            value: formatMoney(
                              bestPerformanceData?.unit_wise_profitability?.unit_wise_profitability,
                              drawerCurrencySymbol
                            ),
                            period: formatBestPerformancePeriod(
                              bestPerformanceData?.unit_wise_profitability?.month,
                              bestPerformanceData?.unit_wise_profitability?.year
                            ),
                          },
                        ].map((card, index) => (
                          <div
                            key={card.label}
                            className={[
                              "w-full rounded-xl bg-white shadow-sm p-1.5 2xl:p-2",
                              "flex flex-col justify-between min-h-[78px]",
                              getMetricBorderColorByLabel(card.label, index),
                            ].join(" ")}
                          >
                            <div className="flex justify-between items-center">
                              <span className="text-[10px] 2xl:text-xs font-medium text-charcoal-500">
                                {formatMetricTitle(card.label)}
                              </span>
                            </div>

                            <div className="mt-1 flex items-end justify-between gap-3 leading-tight tabular-nums">
                              <div className="min-w-0">
                                <div className="text-[10px] 2xl:text-xs font-medium text-charcoal-500 whitespace-nowrap">
                                  {card.period}
                                </div>

                                <div className="mt-1 text-sm 2xl:text-lg font-semibold text-charcoal-500 whitespace-nowrap">
                                  {card.value}
                                </div>
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-xs text-charcoal-500 2xl:text-sm">
                        —
                      </div>
                    )}
                  </div>
                )}

                {/* {block.isOtherSkus && block.includedSkus?.length ? (
                  <div>
                    <div className="mb-2 text-xs font-semibold text-charcoal-500 sm:text-sm 2xl:text-lg">
                      Other SKUs include these products
                    </div>

                    <div className="">
                      <ul className="grid grid-cols-1 sm:grid-cols-3 gap-x-8 gap-y-2 list-disc pl-5 text-xs text-charcoal-500 2xl:text-sm">
                        {block.includedSkus.map((item, index) => (
                          <li key={`${item.product_name}-${index}`} className="leading-relaxed">
                            {item.product_name}
                          </li>
                        ))}
                      </ul>
                    </div>
                  </div>
                ) : null} */}

                {block.isOtherSkus && block.includedSkus?.length ? (
                  <div>
                    <div className="mb-2 text-xs font-semibold text-charcoal-500 sm:text-sm 2xl:text-lg">
                      Other SKUs include these SKUs
                    </div>

                    <div className="">
                      <div className="flex flex-wrap gap-2">
                        {block.includedSkus.map((item) => (
                          <div
                            key={item.sku}
                            className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-[11px] text-slate-700 2xl:text-xs"
                            title={item.product_name}
                          >
                            <span className="font-semibold text-slate-800">
                              {item.product_name}
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                ) : null}

                {showCurrentPeriodRecommendations && (
                  <div>
                    {/* <div className="mb-2 text-xs font-semibold text-charcoal-500 sm:text-sm 2xl:text-lg">
                      Recommendations
                    </div> */}

                    <PageBreadcrumb
                      pageTitle="Recommendations"
                      variant="page"
                      align="left"
                      textSize="xl"
                      className="mb-2"
                    />

                    {block.recommendationBullets?.length ? (
                      <div>
                        <div className="text-xs font-semibold text-charcoal-500 2xl:text-sm">
                          Action
                        </div>
                        <ul className="list-disc space-y-1 pl-5 text-xs text-charcoal-500 2xl:text-sm">
                          {block.recommendationBullets.map((pt, i) => (
                            <li key={i}>{pt}</li>
                          ))}
                        </ul>
                      </div>
                    ) : null}

                    {inventoryRecoBullets.length ? (
                      <div className="mt-2">
                        <div className="text-xs font-semibold text-charcoal-500 2xl:text-sm">
                          Inventory
                        </div>
                        <ul className="list-disc space-y-1 pl-5 text-xs text-charcoal-500 2xl:text-sm">
                          {inventoryRecoBullets
                            .map(cleanInventoryRecommendationText)
                            .filter(Boolean)
                            .map((pt, i) => (
                              <li key={i}>{pt}</li>
                            ))}
                        </ul>
                      </div>
                    ) : null}

                    {adsRecoBullets.length ? (
                      <div className="mt-2">
                        <div className="text-xs font-semibold text-charcoal-500 2xl:text-sm">
                          Ads
                        </div>
                        <ul className="list-disc space-y-1 pl-5 text-xs text-charcoal-500 2xl:text-sm">
                          {adsRecoBullets.map((pt, i) => (
                            <li key={i}>{pt}</li>
                          ))}
                        </ul>
                      </div>
                    ) : null}

                    {!block.recommendationBullets?.length &&
                      !inventoryRecoBullets.length &&
                      !adsRecoBullets.length && (
                        <div className="text-xs text-charcoal-500 2xl:text-sm">—</div>
                      )}
                  </div>
                )}

                <div className="w-full">
                  <Productinfoinpopup
                    productname={
                      block.isOtherSkus
                        ? block.includedSkus?.[0]?.product_name || block.name
                        : block.name
                    }
                    countryName={countryName}
                    isOtherSkus={!!block.isOtherSkus}
                    otherSkuProductNames={
                      block.isOtherSkus
                        ? (block.includedSkus || []).map((item) => item.product_name)
                        : []
                    }
                  />
                </div>

                <div className="">
                  <div className="flex items-center gap-1 flex-wrap">
                    <PageBreadcrumb
                      pageTitle="Product Journey"
                      variant="page"
                      textSize="xl"
                      className="mb-2"
                    />
                  </div>

                  {block.journeyBullets?.length ? (
                    <ol className="list-decimal pl-3 space-y-1 text-xs text-charcoal-500 2xl:text-sm marker:font-semibold marker:text-charcoal-400">
                      {block.journeyBullets.map((p, i) => (
                        <li key={i}>
                          {p
                            .replace(/^\d+\.\s*-\s*/, "")
                            .replace(/^\d+\.\s*/, "")
                            .replace(/^-+\s*/, "")}
                        </li>
                      ))}
                    </ol>
                  ) : (
                    <div className="text-xs text-charcoal-500 2xl:text-sm">—</div>
                  )}
                </div>
              </div>
            </div>
          </motion.aside>
        </>
      )}
    </AnimatePresence>
  );
};

const PreviewLockedSection = ({
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
}) => {
  return (
    <div className="relative w-full">
      <div
        className={
          enabled
            ? "pointer-events-none select-none opacity-45  transition-all duration-300"
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
                  <div className="flex h-16 w-16 items-center justify-center rounded-full  bg-[#37455F]">
                    <IoMdLock className="text-3xl text-[#F8EDCE]" />
                  </div>
                </div>

                <h3 className="text-lg font-semibold text-[#414042]">
                  {title}
                </h3>

                <p className="mt-2 text-sm text-gray-600 leading-6">
                  {description}
                </p>

                <button
                  onClick={onAction}
                  className="mt-4 rounded-md bg-[#37455F] px-4 py-2 text-sm text-[#F8EDCE] hover:opacity-90 transition"
                >
                  {buttonText}
                </button>

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
};

const parseMoneyFromMetricValue = (value?: string) => {
  if (!value) return 0;

  const main = value.split("(")[0] || "";
  const cleaned = main.replace(/[^0-9.-]/g, "");
  const n = Number(cleaned);

  return Number.isFinite(n) ? n : 0;
};

const getBlockNetSales = (block: ProductInsightBlock) => {
  const metric = block.metrics.find(
    (m) => m.label.trim().toLowerCase() === "net sales"
  );

  return parseMoneyFromMetricValue(metric?.value);
};

const ProductInsightsSection = ({
  blocks,
  objective,
  recommendationsMap,
  nameToSkuMap,
  range,
  selectedYear,
  selectedQuarter,
  homeCurrency,
  countryName,
  currencySymbol,
  drawerPeriodText,
  selectedMonth,
  otherSkuIncludedProducts,
}: {
  blocks: ProductInsightBlock[];
  objective?: ObjectivePayload;
  recommendationsMap?: RecommendationsMap;
  nameToSkuMap?: Record<string, string>;
  drawerPeriodText?: string;
  selectedMonth?: string;
  range: RangeType;
  selectedYear: string;
  selectedQuarter: Quarter | "";
  homeCurrency?: string;
  countryName: string;
  currencySymbol?: string;
  otherSkuIncludedProducts?: OtherSkuItem[];
}) => {
  const [selectedBlock, setSelectedBlock] = useState<ProductInsightBlock | null>(null);
  const [selectedRecObj, setSelectedRecObj] = useState<any>(null);
  const [bestPerformanceLoading, setBestPerformanceLoading] = useState(false);
  const [bestPerformanceError, setBestPerformanceError] = useState<string | null>(null);
  const [bestPerformanceData, setBestPerformanceData] =
    useState<ProductBestPerformanceData | null>(null);

  const [perfLoading, setPerfLoading] = useState(false);
  const [perfError, setPerfError] = useState<string | null>(null);
  const [perfData, setPerfData] = useState<any>(null);

  const [perfMetric, setPerfMetric] = useState<"net_sales" | "units">("net_sales");

  const hasBlocks = blocks.length > 0; // ✅ compute instead of early return

  const enrichedBlocks = useMemo(() => {
    return (blocks || []).map((b) => {
      const isOther =
        b.isOtherSkus || b.name.trim().toLowerCase() === "other skus";

      if (!isOther) return b;

      return {
        ...b,
        isOtherSkus: true,
        includedSkus:
          b.includedSkus?.length
            ? b.includedSkus
            : otherSkuIncludedProducts || [],
      };
    });
  }, [blocks, otherSkuIncludedProducts]);

  const sortedBlocks = useMemo(() => {
    const deduped = new Map<string, ProductInsightBlock>();

    const getResolvedSku = (block: ProductInsightBlock) => {
      return (
        String(block.skuKey || "").trim() ||
        String(nameToSkuMap?.[normalizeKey(block.name)] || "").trim()
      );
    };

    const getBlockScore = (block: ProductInsightBlock) => {
      return (
        block.metrics.length * 5 +
        block.journeyBullets.length +
        block.recommendationBullets.length * 3 +
        block.inventoryBullets.length * 2 +
        (block.skuKey ? 10 : 0)
      );
    };

    for (const block of enrichedBlocks || []) {
      const isOther =
        block.isOtherSkus ||
        normalizeKey(block.name) === "other skus" ||
        normalizeKey(block.name) === "others";

      const resolvedSku = getResolvedSku(block);

      const key = isOther
        ? "other-skus"
        : resolvedSku
          ? `sku-${resolvedSku.toLowerCase()}`
          : `name-${normalizeKey(block.name)}`;

      const normalizedBlock: ProductInsightBlock = {
        ...block,
        skuKey: resolvedSku || block.skuKey,
        name: isOther ? "Other SKUs" : block.name,
        isOtherSkus: isOther,
      };

      const existing = deduped.get(key);

      if (!existing) {
        deduped.set(key, normalizedBlock);
        continue;
      }

      // Merge duplicate SKU blocks instead of showing both cards
      deduped.set(key, {
        ...existing,
        ...normalizedBlock,

        // keep richer arrays
        metrics:
          normalizedBlock.metrics.length >= existing.metrics.length
            ? normalizedBlock.metrics
            : existing.metrics,

        journeyBullets:
          normalizedBlock.journeyBullets.length >= existing.journeyBullets.length
            ? normalizedBlock.journeyBullets
            : existing.journeyBullets,

        recommendationBullets:
          normalizedBlock.recommendationBullets.length >= existing.recommendationBullets.length
            ? normalizedBlock.recommendationBullets
            : existing.recommendationBullets,

        inventoryBullets:
          normalizedBlock.inventoryBullets.length >= existing.inventoryBullets.length
            ? normalizedBlock.inventoryBullets
            : existing.inventoryBullets,

        includedSkus:
          normalizedBlock.includedSkus?.length
            ? normalizedBlock.includedSkus
            : existing.includedSkus,

        skuKey: normalizedBlock.skuKey || existing.skuKey,
        name: existing.name || normalizedBlock.name,
      });

      // If one version is clearly richer overall, prefer its base fields
      if (getBlockScore(normalizedBlock) > getBlockScore(existing)) {
        const merged = deduped.get(key)!;
        deduped.set(key, {
          ...normalizedBlock,
          metrics: merged.metrics,
          journeyBullets: merged.journeyBullets,
          recommendationBullets: merged.recommendationBullets,
          inventoryBullets: merged.inventoryBullets,
          includedSkus: merged.includedSkus,
        });
      }
    }

    const uniqueBlocks = Array.from(deduped.values());

    const otherBlock = uniqueBlocks.find(
      (b) =>
        b.isOtherSkus ||
        normalizeKey(b.name) === "other skus" ||
        normalizeKey(b.name) === "others"
    );

    const topFive = uniqueBlocks
      .filter(
        (b) =>
          !b.isOtherSkus &&
          normalizeKey(b.name) !== "other skus" &&
          normalizeKey(b.name) !== "others"
      )
      .sort((a, b) => getBlockNetSales(b) - getBlockNetSales(a))
      .slice(0, 5);

    return otherBlock ? [...topFive, otherBlock] : topFive;
  }, [enrichedBlocks, nameToSkuMap]);

  const topBorderColors = ["border-t-blue-500", "border-t-amber-500", "border-t-emerald-500", "border-t-rose-500"];

  const resolvedCurrencySymbol =
    currencySymbol ||
    getCurrencySymbol(
      countryName.toLowerCase() === "global"
        ? homeCurrency || "usd"
        : countryName
    );

  const skuActions =
    (recommendationsMap as any)?.sku_actions ??
    (recommendationsMap as any)?.recommendations ??
    recommendationsMap ??
    {};

  const getOtherSkusRecObj = () => {
    const source: any = skuActions || {};

    const remainingObj =
      source?.remaining_skus ||
      source?.other_skus ||
      source?.["Other SKUs"] ||
      source?.["other skus"] ||
      {};

    return {
      recommendation:
        source?.remaining_skus_recommendation ||
        remainingObj?.recommendation ||
        "",

      inventory_recommendation:
        source?.remaining_skus_inventory_recommendation ||
        remainingObj?.inventory_recommendation ||
        "",

      ads_recommendation:
        source?.remaining_skus_ads_recommendation ||
        remainingObj?.ads_recommendation ||
        "",

      journey_summary:
        remainingObj?.journey_summary || [],
    };
  };

  const getRecObjForBlock = (b: ProductInsightBlock) => {
    const isOther =
      b.isOtherSkus ||
      normalizeKey(b.name) === "other skus" ||
      normalizeKey(b.name) === "others";

    if (isOther) {
      return getOtherSkusRecObj();
    }

    const mappedSku = nameToSkuMap?.[normalizeKey(b.name)];
    const skuKey = b.skuKey || mappedSku;

    return (
      (skuKey && (skuActions as any)[skuKey]) ||
      (skuActions as any)[b.name] ||
      (skuActions as any)[b.name.trim()] ||
      null
    );
  };

  const getCountryLabelFromText = (text: string) => {
    const match = String(text || "").trim().match(/^(UK|US|CA|India|IN)\s*:/i);
    return match?.[1]?.toUpperCase() || "";
  };

  const cleanCountryPrefix = (text: string) => {
    return String(text || "")
      .replace(/^(UK|US|CA|India|IN)\s*:\s*/i, "")
      .trim();
  };

  const buildGlobalCardRecommendationLines = (
    block: ProductInsightBlock,
    recObj: any
  ) => {
    const rawLines = dedupeRecommendationLines([
      ...(block.recommendationBullets || []),
      getActionTextFromRecObj(recObj),
    ]);

    const countryMap = new Map<string, string>();

    rawLines.forEach((line) => {
      const country = getCountryLabelFromText(line);

      // ✅ Agar country prefix hai, per country sirf first normal recommendation rakho
      if (country && !countryMap.has(country)) {
        countryMap.set(country, `${country}: ${cleanCountryPrefix(line)}`);
        return;
      }

      // ✅ Agar country prefix nahi hai, single fallback point
      if (!country && !countryMap.has("GLOBAL")) {
        countryMap.set("GLOBAL", line);
      }
    });

    return Array.from(countryMap.values()).filter(Boolean);
  };

  useEffect(() => {
    if (!hasBlocks) return;
    if (!selectedBlock) return;

    const ac = new AbortController();

    (async () => {
      try {
        setPerfLoading(true);
        setPerfError(null);
        setPerfData(null);

        const token =
          typeof window !== "undefined"
            ? localStorage.getItem("jwtToken")
            : null;

        if (!token) throw new Error("Missing token");

        const isOtherSkusBlock =
          selectedBlock.isOtherSkus ||
          selectedBlock.name.trim().toLowerCase() === "other skus";

        /**
         * IMPORTANT:
         * For Other SKUs, backend still needs a real product_name
         * so it can resolve SKU and calculate other_skus_graph_data.
         *
         * We use first non-Other product as anchor.
         */
        const anchorProductName =
          isOtherSkusBlock
            ? sortedBlocks.find(
              (b) =>
                !b.isOtherSkus &&
                b.name.trim().toLowerCase() !== "other skus"
            )?.name
            : selectedBlock.name;

        if (!anchorProductName) {
          setPerfData({ rows: [] });
          return;
        }

        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductwisePerformance`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Authorization: `Bearer ${token}`,
            },
            body: JSON.stringify({
              country: countryName,
              product_name: anchorProductName,
              time_range: "Yearly",
              year: Number(selectedYear),
              quarter: undefined,
              home_currency: homeCurrency,
            }),
            cache: "no-store",
            signal: ac.signal,
          }
        );

        const json = await res.json().catch(() => ({}));

        if (!res.ok) {
          throw new Error(json?.error || "Failed to fetch product performance");
        }

        const pickSeries = (j: any, key: "data" | "other_skus_graph_data") => {
          const d = j?.[key];

          if (!d || typeof d !== "object") return null;

          const country = (countryName || "").toLowerCase();
          const keys = Object.keys(d);

          if (country && country !== "global") {
            const match =
              keys.find((k) => k.toLowerCase() === country) ||
              keys.find((k) => k.toLowerCase().startsWith(country + "_")) ||
              keys.find((k) => k.toLowerCase().startsWith(country));

            if (match) return d[match];
          }

          const globalMatch = keys.find((k) =>
            k.toLowerCase().startsWith("global")
          );

          return globalMatch ? d[globalMatch] : d[keys[0]];
        };

        const selectedRows = pickSeries(json, "data") || [];
        const otherRows = pickSeries(json, "other_skus_graph_data") || [];

        const rows = selectedRows.map((selectedRow: any) => {
          const otherRow = Array.isArray(otherRows)
            ? otherRows.find(
              (x: any) =>
                String(x?.month_num) === String(selectedRow?.month_num) &&
                String(x?.year) === String(selectedRow?.year)
            )
            : null;

          return {
            x: selectedRow?.month ?? selectedRow?.label ?? "-",

            selectedValue:
              perfMetric === "units"
                ? toNum(selectedRow?.quantity ?? selectedRow?.units ?? 0)
                : toNum(selectedRow?.net_sales ?? 0),

            otherValue:
              perfMetric === "units"
                ? toNum(otherRow?.quantity ?? otherRow?.units ?? 0)
                : toNum(otherRow?.net_sales ?? 0),

            value: isOtherSkusBlock
              ? perfMetric === "units"
                ? toNum(otherRow?.quantity ?? otherRow?.units ?? 0)
                : toNum(otherRow?.net_sales ?? 0)
              : perfMetric === "units"
                ? toNum(selectedRow?.quantity ?? selectedRow?.units ?? 0)
                : toNum(selectedRow?.net_sales ?? 0),
          };
        });

        setPerfData({ rows });
      } catch (e: any) {
        if (e?.name === "AbortError") return;
        setPerfError(e?.message || "Failed to load product chart");
      } finally {
        setPerfLoading(false);
      }
    })();

    return () => ac.abort();
  }, [
    selectedBlock,
    hasBlocks,
    sortedBlocks,
    selectedYear,
    homeCurrency,
    countryName,
    perfMetric,
  ]);

  useEffect(() => {
    if (!hasBlocks) return;
    if (!selectedBlock) return;

    const ac = new AbortController();

    const fetchBestPerformance = async () => {
      try {
        setBestPerformanceLoading(true);
        setBestPerformanceError(null);
        setBestPerformanceData(null);

        const token =
          typeof window !== "undefined"
            ? localStorage.getItem("jwtToken")
            : null;

        if (!token) throw new Error("Missing token");

        const isOtherSkusBlock =
          selectedBlock.isOtherSkus ||
          selectedBlock.name.trim().toLowerCase() === "other skus";

        const productName =
          isOtherSkusBlock
            ? sortedBlocks.find(
              (b) =>
                !b.isOtherSkus &&
                b.name.trim().toLowerCase() !== "other skus"
            )?.name
            : selectedBlock.name;

        if (!productName) return;

        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductBestPerformance`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Authorization: `Bearer ${token}`,
            },
            body: JSON.stringify({
              product_name: productName,
              country: countryName,
              home_currency: homeCurrency || resolvedCurrencySymbol,
            }),
            cache: "no-store",
            signal: ac.signal,
          }
        );

        const json = await res.json().catch(() => ({}));

        if (!res.ok) {
          throw new Error(json?.error || "Failed to fetch best performance");
        }

        setBestPerformanceData(json?.best_performance ?? null);
      } catch (e: any) {
        if (e?.name === "AbortError") return;
        setBestPerformanceError(e?.message || "Failed to load best performance");
      } finally {
        setBestPerformanceLoading(false);
      }
    };

    fetchBestPerformance();

    return () => ac.abort();
  }, [
    selectedBlock,
    hasBlocks,
    sortedBlocks,
    countryName,
    homeCurrency,
    resolvedCurrencySymbol,
  ]);
  if (!hasBlocks) return null;

  const openDrawer = (b: ProductInsightBlock) => {
    const recObj = getRecObjForBlock(b);

    setSelectedRecObj(recObj);
    setSelectedBlock(b);
  };

  return (
    <div className="space-y-5">
      <div>
        <PageBreadcrumb pageTitle="Recommendations" variant="page" align="left" textSize="2xl" />
      </div>

      <div className="mt-3 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
        {sortedBlocks.map((b, idx) => {
          const borderColor = topBorderColors[idx % topBorderColors.length];

          const sortedCardMetrics = [...(b.metrics || [])]
            .filter((m) => {
              const lower = m.label.trim().toLowerCase();
              const isGlobal = String(countryName || "").toLowerCase() === "global";

              // ✅ Global cards me stock cover/current inventory hide
              if (
                isGlobal &&
                [
                  "stock cover",
                  "stock cover (months)",
                  "current inventory",
                  "current inventory units",
                ].includes(lower)
              ) {
                return false;
              }

              return isMonthlyRange(range) || lower !== "stock cover";
            })
            .sort((a, b) => {
              const aIndex = metricOrder.indexOf(a.label.trim().toLowerCase());
              const bIndex = metricOrder.indexOf(b.label.trim().toLowerCase());

              const safeAIndex = aIndex === -1 ? Number.MAX_SAFE_INTEGER : aIndex;
              const safeBIndex = bIndex === -1 ? Number.MAX_SAFE_INTEGER : bIndex;

              return safeAIndex - safeBIndex;
            });

          return (
            <motion.div
              key={b.isOtherSkus ? "other-skus" : b.skuKey || normalizeKey(b.name)}
              initial={{ opacity: 0, y: 16 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.35, delay: idx * 0.06 }}
              className={[
                "bg-white rounded-xl  border border-slate-200 shadow-sm hover:shadow-md transition-shadow",
                "border-t-4",
                "p-3 space-y-3",
              ].join(" ")}
            >
              <div className="flex items-start justify-between gap-3">
                <div className="text-sm font-semibold text-charcoal-500">
                  {idx + 1}. {b.name}
                </div>

                <button
                  onClick={() => openDrawer(b)}
                  className="px-3 py-1.5 rounded-lg text-xs font-semibold bg-slate-800 text-yellow-200 hover:bg-slate-700 transition"
                >
                  Detailed View
                </button>
              </div>

              {sortedCardMetrics.length > 0 && (
                <div className="grid grid-cols-3 gap-2">
                  {sortedCardMetrics.map((m, i) => (
                    <RecommendationMetricCard
                      key={`${m.label}-${i}`}
                      metric={m}
                    />
                  ))}
                </div>
              )}
              {(() => {
                const recObj = getRecObjForBlock(b);
                const isGlobal = String(countryName || "").toLowerCase() === "global";

                const actionText = getActionTextFromRecObj(recObj);
                const inventoryText = getInventoryTextFromRecObj(recObj);

                const cardLines = isGlobal
                  ? buildGlobalCardRecommendationLines(b, recObj)
                  : dedupeRecommendationLines([
                    ...(b.recommendationBullets || []),
                    actionText,
                    inventoryText,
                  ]);

                if (!cardLines.length) return null;

                return (
                  <div className="space-y-1 text-xs 2xl:text-sm text-slate-700 leading-relaxed">
                    {cardLines.map((line, i) => (
                      <p key={i}>
                        <span className="font-semibold">{i + 1}.</span> {line}
                      </p>
                    ))}
                  </div>
                );
              })()}
            </motion.div>
          );
        })}
      </div>

      <RightProductDrawer
        open={!!selectedBlock}
        onClose={() => {
          setSelectedBlock(null);
          setSelectedRecObj(null);
          setPerfData(null);
          setPerfError(null);
          setBestPerformanceData(null);
          setBestPerformanceError(null);
        }}
        block={selectedBlock}
        objective={objective}
        recObj={selectedRecObj}
        countryName={countryName}
        range={range}
        year={selectedYear}
        month={range === "monthly" ? (selectedMonth ?? "") : ""}
        quarter={range === "quarterly" ? selectedQuarter : ""}
        drawerPeriodText={drawerPeriodText}
        currencySymbol={resolvedCurrencySymbol}
        perfLoading={perfLoading}
        perfError={perfError}
        perfData={perfData}
        perfMetric={perfMetric}
        onPerfMetricChange={setPerfMetric}
        bestPerformanceLoading={bestPerformanceLoading}
        bestPerformanceError={bestPerformanceError}
        bestPerformanceData={bestPerformanceData}
      />
    </div>
  );
};

const MonthlyObjectiveStrip = ({
  objective,
  className = "",
  targetSummary,
  currencySymbol = "$",
  countryName = "",
  range = "yearly",
}: {
  objective?: ObjectivePayload;
  className?: string;
  targetSummary?: {
    target_sales?: number;
    shortfall_total?: number;
    cashflow_total?: number;
  } | null;
  currencySymbol?: string;
  countryName?: string;
  range?: RangeType;
}) => {
  const isGlobal = countryName.toLowerCase() === "global";

  const objectiveTitle =
    range === "monthly"
      ? "Monthly Objectives & Targets"
      : range === "quarterly"
        ? "Quarterly Objectives & Targets"
        : range === "yearly"
          ? "Yearly Objectives & Targets"
          : "Objectives & Targets";

  const growth = isGlobal
    ? "-"
    : objective?.growth_intent?.replaceAll("_", " ") || "Not Defined";

  const profit = isGlobal
    ? "-"
    : objective?.profit_priority?.replaceAll("_", " ") || "Not Defined";

  const inventory = isGlobal
    ? "-"
    : typeof objective?.inventory_clearance_priority === "boolean"
      ? objective.inventory_clearance_priority
        ? "Yes"
        : "No"
      : "Not Defined";

  const formatMoneyCompact = (value?: number) => {
    const num = Number(value ?? 0);
    const sign = num < 0 ? "-" : "";
    const abs = Math.abs(num);

    let compact = "";
    if (abs >= 1_000_000) {
      compact = `${(abs / 1_000_000).toFixed(1)}M`;
    } else if (abs >= 1_000) {
      compact = `${(abs / 1_000).toFixed(1)}K`;
    } else {
      compact = `${abs.toFixed(0)}`;
    }

    return `${sign}${currencySymbol}${compact}`;
  };

  const targetSet = formatMoneyCompact(targetSummary?.target_sales ?? 0);
  const shortfallValue = Number(targetSummary?.shortfall_total ?? 0);
  const shortfall = formatMoneyCompact(-(shortfallValue || 0));
  const cashFlow = formatMoneyCompact(targetSummary?.cashflow_total ?? 0);

  const objectiveCards = [
    {
      label: "Growth",
      value: growth,
      accent: "bg-sky-500",
      valueClass: "text-charcoal-500",
    },
    {
      label: "Profit",
      value: profit,
      accent: "bg-amber-500",
      valueClass: "text-charcoal-500",
    },
    {
      label: "Inventory Dilution",
      value: inventory,
      accent: "bg-violet-500",
      valueClass: "text-charcoal-500",
    },
    {
      label: "Target Set",
      value: targetSet,
      accent: "bg-emerald-500",
      valueClass: "text-charcoal-500",
    },
    {
      label: "Shortfall",
      value: shortfall,
      accent: "bg-rose-500",
      valueClass: shortfallValue > 0 ? "text-rose-600" : "text-charcoal-500",
    },
    {
      label: "Cash Flow",
      value: cashFlow,
      accent: "bg-cyan-500",
      valueClass: "text-charcoal-500",
    },
  ];

  return (
    <div
      className={`rounded-xl border border-slate-200 bg-white shadow-sm p-4 h-full ${className}`}
    >
      <div className="flex items-center justify-between gap-3 mb-4">
        <div>
          <PageBreadcrumb
            pageTitle={objectiveTitle}
            variant="page"
            textSize="2xl"
            align="left"
          />
        </div>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-3 gap-3">
        {objectiveCards.map((item) => (
          <div
            key={item.label}
            className="relative overflow-hidden rounded-xl border border-slate-200 bg-slate-50/80 p-2 transition-all duration-200 hover:shadow-sm"
          >
            <div className={`absolute left-0 top-0 h-full w-1`} />

            <div className="pl-2">
              <div className="2xl:text-xs text-[10px] text-charcoal-500 leading-tight">
                {item.label}
              </div>

              <div
                className={`mt-2 text-sm 2xl:text-base font-semibold leading-snug capitalize break-words ${item.valueClass}`}
              >
                {item.value}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

type AiSingleInsightCardProps = {
  loading: boolean;
  error: string | null;
  summaryBullets: string[];
  recommendationBullets: string[];
  skuInsightsBullets: string[];
  inventoryBullets: string[];

  recommendationsMap?: RecommendationsMap;
  remainingSkusRecommendation?: string;
  nameToSkuMap?: Record<string, string>;
  objective?: ObjectivePayload;

  range: RangeType;
  selectedYear: string;
  selectedQuarter: Quarter | "";
  selectedMonth?: string;

  homeCurrency?: string;
  countryName: string;
  portfolioRecommendation?: string | null;

  targetSummary?: {
    target_sales?: number;
    shortfall_total?: number;
    cashflow_total?: number;
  } | null;

  currencySymbol?: string;
  otherSkuIncludedProducts?: OtherSkuItem[];
};

const formatSummaryPeriod = (text?: string) => {
  if (!text) return "";

  const m = text.match(/\(([^)]+)\)/);
  if (!m) return "";

  const inside = m[1].trim();
  const [leftRaw, rightRaw] = inside.split(/\s*vs\s*/i);

  if (!leftRaw || !rightRaw) return `(${inside})`;

  const formatPart = (part: string) => {
    const p = part.trim();

    // Yearly: 2026
    if (/^\d{4}$/.test(p)) return p;

    // Quarterly: Q2 2026 -> Q2’26
    const qMatch = p.match(/^(Q[1-4])\s+(\d{4})$/i);
    if (qMatch) {
      return `${qMatch[1].toUpperCase()}’${qMatch[2].slice(-2)}`;
    }

    // Monthly: April 2026 -> Apr’26
    const monthYearMatch = p.match(/^([A-Za-z]+)\s+(\d{4})$/);
    if (monthYearMatch) {
      const shortMonth = monthYearMatch[1].slice(0, 3);
      const shortYear = monthYearMatch[2].slice(-2);
      return `${shortMonth}’${shortYear}`;
    }

    return p;
  };

  return `(${formatPart(leftRaw)} vs ${formatPart(rightRaw)})`;
};

const splitSummaryIntoBulletPoints = (lines: string[]) => {
  return lines
    .flatMap((line) =>
      String(line || "")
        .trim()
        // Split only after sentence-ending punctuation followed by a new sentence.
        // This avoids breaking decimals like 2.16 or 167.65.
        .split(/(?<=[.!?])\s+(?=[A-Z])/g)
    )
    .map((line) => line.trim())
    .filter(Boolean);
};

const formatMonthName = (month?: string) => {
  if (!month) return "";
  return month.charAt(0).toUpperCase() + month.slice(1).toLowerCase();
};

const AiSingleInsightCard: React.FC<AiSingleInsightCardProps> = ({
  loading,
  error,
  summaryBullets,
  recommendationBullets,
  skuInsightsBullets,
  inventoryBullets,
  remainingSkusRecommendation,
  objective,
  recommendationsMap,
  nameToSkuMap,
  countryName,
  portfolioRecommendation,
  range,
  selectedYear,
  selectedQuarter,
  selectedMonth,
  homeCurrency,
  targetSummary,
  currencySymbol = "$",
  otherSkuIncludedProducts,
}) => {
  const router = useRouter();

  const goToInventoryReconciliation = () => {
    const routeCountry = String(countryName || "")
      .trim()
      .toLowerCase();

    const monthForRoute =
      range === "monthly" && selectedMonth
        ? selectedMonth
        : new Date().toLocaleString("en-US", { month: "long" });

    const routeMonth =
      monthForRoute.charAt(0).toUpperCase() +
      monthForRoute.slice(1).toLowerCase();

    const routeYear = String(selectedYear || new Date().getFullYear()).trim();

    router.push(
      `/inventory-reconciliation/${routeCountry}/${routeMonth}/${routeYear}`
    );
  };

  if (
    !summaryBullets.length &&
    !recommendationBullets.length &&
    !skuInsightsBullets.length &&
    !inventoryBullets.length &&
    !objective
  ) {
    return null;
  }

  const cleanedSummaryLines = (summaryBullets || [])
    .map((line) => String(line || "").trim())
    .filter(Boolean);

  const summaryTitleLine = cleanedSummaryLines[0] || "";

  const summaryBodyLines = cleanedSummaryLines
    .slice(1)
    .filter((line) => {
      const value = line.trim();

      // remove markdown headings / metadata only, not real summary sentences
      if (/^##\s+/i.test(value)) return false;
      if (/^period\s*:/i.test(value)) return false;

      return true;
    });

  const summaryBulletPoints = splitSummaryIntoBulletPoints(summaryBodyLines);

  const drawerPeriodText = summaryTitleLine
    ? formatSummaryPeriod(summaryTitleLine)
    : "";

  const hasNoAiData =
    !summaryBullets.length &&
    !skuInsightsBullets.length &&
    !inventoryBullets.length &&
    !recommendationBullets.length;

  return (
    <div className="flex flex-col gap-5">
      <div className="w-full space-y-4">
        {!hasNoAiData && (summaryTitleLine || summaryBulletPoints.length > 0 || objective) && (

          <div className="grid grid-cols-1 xl:grid-cols-12 gap-4 items-stretch">
            {(summaryTitleLine || summaryBulletPoints.length > 0) && (
              <div className="xl:col-span-7 rounded-xl border border-slate-200 bg-white shadow-sm p-4 h-full">
                <div className="space-y-3 h-full">
                  <h2 className="text-base sm:text-xl lg:text-lg 2xl:text-2xl text-charcoal-500 font-bold leading-snug">
                    {summaryTitleLine?.split("(")[0]?.trim()}
                    <span className="text-green-500 font-bold ml-2 text-base sm:text-xl lg:text-lg 2xl:text-2xl">
                      {formatSummaryPeriod(summaryTitleLine)}
                    </span>
                  </h2>

                  <ul className="list-disc pl-5 text-xs 2xl:text-sm text-charcoal-500 leading-relaxed space-y-2">
                    {summaryBulletPoints.map((line, i) => (
                      <li key={i}>{line}</li>
                    ))}
                  </ul>

                  {portfolioRecommendation ? (
                    <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50 p-3">
                      <div className="2xl:text-sm text-xs font-semibold text-charcoal-500 mb-1">
                        Portfolio Recommendation
                      </div>
                      <div className="text-xs 2xl:text-sm text-slate-700 leading-relaxed">
                        {portfolioRecommendation}
                      </div>
                    </div>
                  ) : null}
                </div>
              </div>
            )}

            {/* Right: Monthly Objective */}
            {objective ? (
              <div className="xl:col-span-5 h-full">
                <MonthlyObjectiveStrip
                  objective={objective}
                  targetSummary={targetSummary}
                  currencySymbol={currencySymbol}
                  countryName={countryName}
                  range={range}
                  className="h-full"
                />
              </div>
            ) : null}
          </div>
        )}

        {/* Product Insights */}
        {!hasNoAiData && parseProductInsightsBlocks(skuInsightsBullets, range).length > 0 && (
          <div className="w-full rounded-xl border border-slate-200 bg-white shadow-sm p-4">
            <div className="space-y-5">
              <ProductInsightsSection
                blocks={parseProductInsightsBlocks(skuInsightsBullets, range)}
                objective={objective}
                recommendationsMap={recommendationsMap}
                nameToSkuMap={nameToSkuMap}
                range={range}
                selectedYear={selectedYear}
                selectedQuarter={selectedQuarter}
                selectedMonth={range === "monthly" ? selectedMonth : ""}
                homeCurrency={homeCurrency}
                countryName={countryName}
                currencySymbol={currencySymbol}
                drawerPeriodText={drawerPeriodText}
                otherSkuIncludedProducts={otherSkuIncludedProducts}
              />
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

type FocusedChart = "trend" | "pnl" | null;

type DashboardTab =
  | "graphs"
  | "skuBreakdown"
  | "cashFlow"
  | "skuwiseProfit"
  | "businessSummary"
  | "inventoryInsights";

const TAB_LABELS: Record<DashboardTab, string> = {
  graphs: "Finance Dashboard",
  skuBreakdown: "P&L Breakdown",
  cashFlow: "Cash Flow",
  skuwiseProfit: "SKU Journey",
  businessSummary: "AI Insights & Recommendations",
  inventoryInsights: "Inventory Insights",
};

const TAB_OPTIONS: { value: DashboardTab; label: string }[] = [
  { value: "businessSummary", label: TAB_LABELS.businessSummary },
  { value: "graphs", label: TAB_LABELS.graphs },
  { value: "skuBreakdown", label: TAB_LABELS.skuBreakdown },
  { value: "cashFlow", label: TAB_LABELS.cashFlow },
  { value: "skuwiseProfit", label: TAB_LABELS.skuwiseProfit },
  { value: "inventoryInsights", label: TAB_LABELS.inventoryInsights },
];

const HASH_TO_FINANCE_TAB: Record<string, DashboardTab> = {
  "finance-dashboard": "graphs",
  "ai-insights": "businessSummary",
  "inventory-insights": "inventoryInsights",
  "pnl-breakdown": "skuBreakdown",
  "skuwise-profit": "skuwiseProfit",
  "cash-flow": "cashFlow",
};

const DEMO_SUMMARY: Summary = {
  unit_sold: 0,
  total_sales: 0,
  gross_sales: 0,
  total_product_sales: 0,
  total_expense: 0,
  asp: 0,
  cm2_profit: 0,
  cm2_profit_total: 0,
  cm2_margins: 0,
  cm2_profit_percentage: 0,
  promotional_rebates: 0,
  promotional_rebates_percentage: 0,
  total_cous: 0,
  otherwplatform: 0,
  advertising_total: 0,
  total_amazon_fee: 0,
};

const DEMO_SUMMARY_COMPARISONS: SummaryComparisons = {
  lastMonth: {
    unit_sold: 0,
    total_sales: 0,
    gross_sales: 0,
    total_product_sales: 0,
    total_expense: 0,
    cm2_profit: 0,
    cm2_margins: 0,
    cm2_profit_percentage: 0,
    promotional_rebates: 0,
    promotional_rebates_percentage: 0,
    total_cous: 0,
    otherwplatform: 0,
    advertising_total: 0,
    total_amazon_fee: 0,
    asp: 0,
  },
  lastQuarter: {
    unit_sold: 0,
    total_sales: 0,
    gross_sales: 0,
    total_product_sales: 0,
    total_expense: 0,
    cm2_profit: 0,
    cm2_margins: 0,
    cm2_profit_percentage: 0,
    promotional_rebates: 0,
    promotional_rebates_percentage: 0,
    total_cous: 0,
    otherwplatform: 0,
    advertising_total: 0,
    total_amazon_fee: 0,
    asp: 0,
  },
  lastYear: {
    unit_sold: 0,
    total_sales: 0,
    gross_sales: 0,
    total_product_sales: 0,
    total_expense: 0,
    cm2_profit: 0,
    cm2_margins: 0,
    cm2_profit_percentage: 0,
    promotional_rebates: 0,
    promotional_rebates_percentage: 0,
    total_cous: 0,
    otherwplatform: 0,
    advertising_total: 0,
    total_amazon_fee: 0,
    asp: 0,
  },
};

const DEMO_UPLOAD_HISTORY: UploadHistoryResponse = {
  summary: DEMO_SUMMARY,
  summaryComparisons: DEMO_SUMMARY_COMPARISONS,
};

const DEMO_AI_PANEL: AiPanelData = {
  summaryBullets: [
    "Business Summary (Jan 26 vs Dec 25)",
    "Net sales improved due to better conversion and stronger contribution from hero SKUs.",
    "CM2 remained positive, though ad costs increased slightly as spend was pushed on scaling products.",
  ],
  skuInsightsBullets: [
    "Demo Product A",
    "ASP: $0.00 (0%)",
    "Units: 0 (0%)",
    "Net sales: $0 (+0%)",
    "CM1 profit: $0 (+0%)",
    "CM1 profit per unit: $0 (+0%)",
    "Product Journey",
    "- Demo Product A maintained momentum with better pricing and stable returns.",
    "Recommendation: Push top-performing campaigns and monitor TACoS closely.",
    "Inventory Action: Replenish aggressively to avoid stock-outs.",

    "Demo Product B",
    "ASP: $0 (+0%)",
    "Units: 0 (+0%)",
    "Net sales: $0 (+0%)",
    "CM1 profit: $0 (+0%)",
    "CM1 profit per unit: $0 (+0%)",
    "Product Journey",
    "- Demo Product B showed strong revenue growth and healthy profitability.",
    "Recommendation: Push top-performing campaigns and monitor TACoS closely.",
    "Inventory Action: Replenish aggressively to avoid stock-outs.",

    "Demo Product C",
    "ASP: $0 (+0%)",
    "Units: 0 (+0%)",
    "Net sales: $0 (+0%)",
    "CM1 profit: $0 (+0%)",
    "CM1 profit per unit: $0 (+0%)",
    "Product Journey",
    "- Demo Product C showed strong revenue growth and healthy profitability.",
    "Recommendation: Push top-performing campaigns and monitor TACoS closely.",
    "Inventory Action: Replenish aggressively to avoid stock-outs.",

    "Demo Product D",
    "ASP: $0 (+0%)",
    "Units: 0 (+0%)",
    "Net sales: $0 (+0%)",
    "CM1 profit: $0 (+0%)",
    "CM1 profit per unit: $0 (+0%)",
    "Product Journey",
    "- Demo Product C showed strong revenue growth and healthy profitability.",
    "Recommendation: Push top-performing campaigns and monitor TACoS closely.",
    "Inventory Action: Replenish aggressively to avoid stock-outs.",

    "Other SKUs",
    "ASP: $0 (+0%)",
    "Units: 0 (+0%)",
    "Net sales: $0 (+0%)",
    "CM1 profit: $0 (+0%)",
    "CM1 profit per unit: $0 (+0%)",
    "Product Journey",
    "- Other SKUs showed mixed performance with varying revenue and profitability.",
    "Recommendation: Focus on high-performing SKUs and optimize underperforming ones.",
    "Inventory Action: Replenish aggressively to avoid stock-outs.",
  ],
  recommendationBullets: [],
  inventoryBullets: [
    "Estimated healthy stock cover available for the next cycle.",
    "Unfulfillable inventory (Low and manageable)",
    "For detailed SKU-level replenishment planning, refer to inventory module.",
  ],
  objective: {
    growth_intent: "growth",
    profit_priority: "balanced",
    inventory_clearance_priority: false,
  },
  recommendationsMap: {
    "Demo Product A": {
      recommendation: "Continue scaling while keeping TACoS in control.",
      inventory_recommendation: "Maintain 30-45 days cover.",
      ads_recommendation: "Increase spend only on high-converting search terms.",
    },
    "Demo Product B": {
      recommendation: "Prioritize hero placement and conversion-led traffic.",
      inventory_recommendation: "Replenish early to prevent stock-outs.",
      ads_recommendation: "Defend branded traffic and scale profitable campaigns.",
    },
  },
  portfolioRecommendation:
    "Focus on scaling hero SKUs while maintaining margin discipline and healthy stock depth.",
};

const DEMO_PERFORMANCE_TREND: PerformanceTrendPayload = {
  x: [1, 2, 3, 4, 5, 6],
  xType: "month",
  series: [
    {
      label: "Sales Trend",
      net_sales: [0, 0, 0, 0, 0, 0],
      units: [0, 0, 0, 0, 0, 0],
    },
  ],
};

const DEMO_UPLOADS: UploadRow[] = [
  {
    country: "global",
    month: "january",
    year: 2026,
    total_sales: 0,
    total_amazon_fee: 0,
    total_cous: 0,
    advertising_total: 0,
    otherwplatform: 0,
    cm2_profit: 0,
    total_profit: 0,
  },
  {
    country: "global",
    month: "february",
    year: 2026,
    total_sales: 0,
    total_amazon_fee: 0,
    total_cous: 0,
    advertising_total: 0,
    otherwplatform: 0,
    cm2_profit: 0,
    total_profit: 0,
  },
  {
    country: "global",
    month: "march",
    year: 2026,
    total_sales: 0,
    total_amazon_fee: 0,
    total_cous: 0,
    advertising_total: 0,
    otherwplatform: 0,
    cm2_profit: 0,
    total_profit: 0,
  },
];

const DEMO_SKU_ROWS: TableRow[] = [
  {
    product_name: "Dummy Product 1",
    sku: "SKU-DEMO-1",
    quantity: 0,
    return_quantity: 0,
    total_quantity: 0,
    units_sold: 0,
    return_units: 0,
    net_units_sold: 0,
    asp: 0,
    product_sales: 0,
    refund_sales: 0,
    net_sales: 0,
    lost_total: 0,
    cost_of_unit_sold: 0,
    shipment_charges: 0,
    selling_fees: 0,
    fba_fees: 0,
    amazon_fee: 0,
    tex_and_credits: 0,
    net_taxes: 0,
    net_credits: 0,
    promotional_rebates: 0,
    promotional_rebates_percentage: 0,
    misc_transaction: 0,
    other_transaction_fees: 0,
    other_transactions: 0,
    profit: 0,
    profit_percentage: 0,
    unit_wise_profitability: 0,
    profit_mix: 0,
    sales_mix: 0,
  },
  {
    product_name: "Dummy Product 2",
    sku: "SKU-DEMO-2",
    quantity: 0,
    return_quantity: 0,
    total_quantity: 0,
    units_sold: 0,
    return_units: 0,
    net_units_sold: 0,
    asp: 0,
    product_sales: 0,
    refund_sales: 0,
    net_sales: 0,
    lost_total: 0,
    cost_of_unit_sold: 0,
    shipment_charges: 0,
    selling_fees: 0,
    fba_fees: 0,
    amazon_fee: 0,
    tex_and_credits: 0,
    net_taxes: 0,
    net_credits: 0,
    promotional_rebates: 0,
    promotional_rebates_percentage: 0,
    misc_transaction: 0,
    other_transaction_fees: 0,
    other_transactions: 0,
    profit: 0,
    profit_percentage: 0,
    unit_wise_profitability: 0,
    profit_mix: 0,
    sales_mix: 0,
  },
  {
    product_name: "Dummy Product 3",
    sku: "SKU-DEMO-3",
    quantity: 0,
    return_quantity: 0,
    total_quantity: 0,
    units_sold: 0,
    return_units: 0,
    net_units_sold: 0,
    asp: 0,
    product_sales: 0,
    refund_sales: 0,
    net_sales: 0,
    lost_total: 0,
    cost_of_unit_sold: 0,
    shipment_charges: 0,
    selling_fees: 0,
    fba_fees: 0,
    amazon_fee: 0,
    tex_and_credits: 0,
    net_taxes: 0,
    net_credits: 0,
    promotional_rebates: 0,
    promotional_rebates_percentage: 0,
    misc_transaction: 0,
    other_transaction_fees: 0,
    other_transactions: 0,
    profit: 0,
    profit_percentage: 0,
    unit_wise_profitability: 0,
    profit_mix: 0,
    sales_mix: 0,
  },
  {
    product_name: "Dummy Product 4",
    sku: "SKU-DEMO-4",
    quantity: 0,
    return_quantity: 0,
    total_quantity: 0,
    units_sold: 0,
    return_units: 0,
    net_units_sold: 0,
    asp: 0,
    product_sales: 0,
    refund_sales: 0,
    net_sales: 0,
    lost_total: 0,
    cost_of_unit_sold: 0,
    shipment_charges: 0,
    selling_fees: 0,
    fba_fees: 0,
    amazon_fee: 0,
    tex_and_credits: 0,
    net_taxes: 0,
    net_credits: 0,
    promotional_rebates: 0,
    promotional_rebates_percentage: 0,
    misc_transaction: 0,
    other_transaction_fees: 0,
    other_transactions: 0,
    profit: 0,
    profit_percentage: 0,
    unit_wise_profitability: 0,
    profit_mix: 0,
    sales_mix: 0,
  },
  {
    product_name: "Dummy Product 5",
    sku: "SKU-DEMO-5",
    quantity: 0,
    return_quantity: 0,
    total_quantity: 0,
    units_sold: 0,
    return_units: 0,
    net_units_sold: 0,
    asp: 0,
    product_sales: 0,
    refund_sales: 0,
    net_sales: 0,
    lost_total: 0,
    cost_of_unit_sold: 0,
    shipment_charges: 0,
    selling_fees: 0,
    fba_fees: 0,
    amazon_fee: 0,
    tex_and_credits: 0,
    net_taxes: 0,
    net_credits: 0,
    promotional_rebates: 0,
    promotional_rebates_percentage: 0,
    misc_transaction: 0,
    other_transaction_fees: 0,
    other_transactions: 0,
    profit: 0,
    profit_percentage: 0,
    unit_wise_profitability: 0,
    profit_mix: 0,
    sales_mix: 0,
  },
  {
    product_name: "Total",
    sku: "Total",
    quantity: 0,
    return_quantity: 0,
    total_quantity: 0,
    units_sold: 0,
    return_units: 0,
    net_units_sold: 0,
    asp: 0,
    product_sales: 0,
    refund_sales: 0,
    net_sales: 0,
    lost_total: 0,
    cost_of_unit_sold: 0,
    shipment_charges: 0,
    selling_fees: 0,
    fba_fees: 0,
    amazon_fee: 0,
    tex_and_credits: 0,
    net_taxes: 0,
    net_credits: 0,
    promotional_rebates: 0,
    promotional_rebates_percentage: 0,
    misc_transaction: 0,
    other_transaction_fees: 0,
    other_transactions: 0,
    profit: 0,
    profit_percentage: 0,
    unit_wise_profitability: 0,
    profit_mix: 0,
    sales_mix: 0,
  },
];

const DEMO_TARGET_SUMMARY = {
  target_sales: 0,
  shortfall_total: 0,
  cashflow_total: 0,
};

const heatmapBuckets: AgeingBucket[] = [
  { key: "zeroToOneEighty", label: "0–180 Days", color: "#7B9A6D" },
  { key: "oneEightyOneToTwoSeventy", label: "181–270 Days", color: "#ED9F50" },
  { key: "twoSeventyOneToThreeSixtyFive", label: "271–365 Days", color: "#C49466" },
  { key: "threeSixtyFivePlus", label: "365+ Days", color: "#B75A5A" },
];

const heatmapData: AgeingRiskHeatmapRow[] = [
  {
    productName: "Demo Product A",
    sku: "SKU-A",
    zeroToOneEighty: 530,
    oneEightyOneToTwoSeventy: 60,
    twoSeventyOneToThreeSixtyFive: 25,
    threeSixtyFivePlus: 10,
    available: 625,
    fcTransfer: 0,
    inboundUnits: 40,
    totalUnits: 625,
    unsellableUnits: 0,
  },
  {
    productName: "Demo Product B",
    sku: "SKU-B",
    zeroToOneEighty: 360,
    oneEightyOneToTwoSeventy: 180,
    twoSeventyOneToThreeSixtyFive: 90,
    threeSixtyFivePlus: 130,
    available: 760,
    fcTransfer: 0,
    inboundUnits: 65,
    totalUnits: 760,
    unsellableUnits: 0,
  },
  {
    productName: "Demo Product C",
    sku: "SKU-C",
    zeroToOneEighty: 635,
    oneEightyOneToTwoSeventy: 20,
    twoSeventyOneToThreeSixtyFive: 5,
    threeSixtyFivePlus: 0,
    available: 660,
    fcTransfer: 0,
    inboundUnits: 25,
    totalUnits: 660,
    unsellableUnits: 0,
  },
  {
    productName: "Demo Product D",
    sku: "SKU-D",
    zeroToOneEighty: 210,
    oneEightyOneToTwoSeventy: 160,
    twoSeventyOneToThreeSixtyFive: 220,
    threeSixtyFivePlus: 310,
    available: 900,
    fcTransfer: 0,
    inboundUnits: 90,
    totalUnits: 900,
    unsellableUnits: 0,
  },
  {
    productName: "Demo Product E",
    sku: "SKU-E",
    zeroToOneEighty: 510,
    oneEightyOneToTwoSeventy: 95,
    twoSeventyOneToThreeSixtyFive: 40,
    threeSixtyFivePlus: 35,
    available: 680,
    fcTransfer: 0,
    inboundUnits: 35,
    totalUnits: 680,
    unsellableUnits: 0,
  },
  {
    productName: "Demo Product F",
    sku: "SKU-F",
    zeroToOneEighty: 340,
    oneEightyOneToTwoSeventy: 120,
    twoSeventyOneToThreeSixtyFive: 80,
    threeSixtyFivePlus: 60,
    available: 600,
    fcTransfer: 0,
    inboundUnits: 55,
    totalUnits: 600,
    unsellableUnits: 0,
  },
];

const selectedDonutSku = "SKU-B";

const INVENTORY_BUCKETS: AgeingBucket[] = [
  { key: "zeroToOneEighty", label: "0–180 Days", color: "#7B9A6D" },
  { key: "oneEightyOneToTwoSeventy", label: "181–270 Days", color: "#ED9F50" },
  { key: "twoSeventyOneToThreeSixtyFive", label: "271–365 Days", color: "#C49466" },
  { key: "threeSixtyFivePlus", label: "365+ Days", color: "#B75A5A" },
];

const AGEING_TREND_BUCKET_OPTIONS = [
  {
    label: "0–180 Days",
    value: "0-180 days",
    column: "inv-age-0-to-180-days",
    color: "#7B9A6D",
  },
  {
    label: "181–270 Days",
    value: "181-270 days",
    column: "inv-age-181-to-270-days",
    color: "#ED9F50",
  },
  {
    label: "271–365 Days",
    value: "271-365 days",
    column: "inv-age-271-to-365-days",
    color: "#C49466",
  },
  {
    label: "365+ Days",
    value: "365+ days",
    column: "inv-age-365-plus-days",
    color: "#B75A5A",
  },
];

const INVENTORY_ACTION_LOGIC: ActionLogicItem[] = [
  {
    key: "healthy",
    label: "Healthy",
    description: "Stock covers 0–180 days",
    color: "#7B9A6D",
  },
  {
    key: "high_alert",
    label: "High Alert",
    description: "Shipment Required",
    color: "#B75A5A",
  },
  // {
  //   key: "discount",
  //   label: "Discount",
  //   description: "Stock aged 0–180 days",
  //   color: "#FDD36F",
  // },
  {
    key: "liquidate",
    label: "Liquidate",
    description: "Stock older than 180 days",
    color: "#ED9F50",
  },
  {
    key: "unfulfillable",
    label: "Unfulfillable",
    description: "Remove or dispose stock",
    color: "#3A8EA4",
  },
  {
    key: "estimated_storage_cost",
    label: "Estimate Storage",
    description: "Monthly storage estimate",
    color: "#C49466",
  },
];

const INVENTORY_ACTION_META: Record<
  string,
  {
    label: string;
    description: string;
    color: string;
    backgroundColor: string;
  }
> = {
  healthy: {
    label: "Healthy",
    description: "Stock covers 0–180 days",
    color: "#7B9A6D",
    backgroundColor: "#ffffff",
  },
  high_alert: {
    label: "High Alert",
    description: "Shipment Required",
    color: "#B75A5A",
    backgroundColor: "#ffffff",
  },
  // discount: {
  //   label: "Discount",
  //   description: "Stock aged 0–180 days",
  //   color: "#FDD36F",
  //   backgroundColor: "#ffffff",
  // },
  liquidate: {
    label: "Liquidate",
    description: "Stock older than 180 days",
    color: "#ED9F50",
    backgroundColor: "#ffffff",
  },
  unfulfillable: {
    label: "Unfulfillable",
    description: "Remove or dispose stock",
    color: "#3A8EA4",
    backgroundColor: "#ffffff",
  },
  estimated_storage_cost: {
    label: "Estimate Storage",
    description: "Monthly storage estimate",
    color: "#C49466",
    backgroundColor: "#ffffff",
  },
};

const getInventoryRowSku = (row: InventoryCurrentRow) =>
  String(row?.SKU ?? row?.sku ?? "").trim();

const getInventoryRowProductName = (row: InventoryCurrentRow) =>
  String(row?.["Product Name"] ?? row?.product_name ?? "").trim();

const isInventoryTotalRow = (row: InventoryCurrentRow) => {
  const sku = getInventoryRowSku(row).toLowerCase();
  const product = getInventoryRowProductName(row).toLowerCase();

  return (
    sku === "total" ||
    product === "total" ||
    sku === "grand total" ||
    product === "grand total"
  );
};

const isInventoryPercentageRow = (row: InventoryCurrentRow) => {
  const sku = getInventoryRowSku(row).toLowerCase();
  const product = getInventoryRowProductName(row).toLowerCase();
  const rowType = String(row?.row_type || "").trim().toLowerCase();

  return (
    row?.is_percentage_row === true ||
    rowType === "percentage" ||
    product === "percentage" ||
    product === "% of total" ||
    sku === "percentage" ||
    sku === "% of total"
  );
};

const getInventoryAgeValue = (row: InventoryCurrentRow, key: string) =>
  toNum(row?.[key]);

const hasAnyAgeingBucketValue = (row: InventoryCurrentRow) => {
  return (
    getInventoryAgeValue(row, "inv-age-0-to-90-days") > 0 ||
    getInventoryAgeValue(row, "inv-age-91-to-180-days") > 0 ||
    getInventoryAgeValue(row, "inv-age-0-to-180-days") > 0 ||
    getInventoryAgeValue(row, "inv-age-181-to-270-days") > 0 ||
    getInventoryAgeValue(row, "inv-age-271-to-365-days") > 0 ||
    getInventoryAgeValue(row, "inv-age-365-plus-days") > 0
  );
};

const getEstimatedStorageCostTotal = (
  latestResponse?: InventoryCurrentApiResponse
) => {
  const storageItems =
    latestResponse?.categories?.estimated_storage_cost?.items ?? [];

  const totalRow = storageItems.find((item: any) => {
    const productName = String(item?.product_name || "")
      .trim()
      .toLowerCase();

    const sku = String(item?.sku || "")
      .trim()
      .toLowerCase();

    return productName === "total" || sku === "total" || sku === "";
  });

  if (totalRow) {
    return toNum(totalRow?.["estimated-storage-cost-next-month"]);
  }

  return storageItems.reduce(
    (sum: number, item: any) =>
      sum + toNum(item?.["estimated-storage-cost-next-month"]),
    0
  );
};

const formatInventoryStorageCost = (
  value: number,
  countryName: string,
  homeCurrency?: string
) => {
  const symbol =
    String(countryName || "").toLowerCase() === "global"
      ? getCurrencySymbol(homeCurrency || "usd")
      : getCurrencySymbol(countryName);

  return `${symbol}${value.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
};

const getInventoryRowAvailableUnits = (row: InventoryCurrentRow) => {
  return toNum(
    row?.available ??
    row?.available_quantity ??
    row?.fulfillable_quantity
  );
};

const getInventoryRowFcTransferUnits = (row: InventoryCurrentRow) => {
  return toNum(
    row?.["fc-transfer"] ??
    row?.fc_transfer ??
    row?.reserved_fc_transfer
  );
};

const getInventoryRowSellableUnits = (row: InventoryCurrentRow) => {
  const available = getInventoryRowAvailableUnits(row);
  const fcTransfer = getInventoryRowFcTransferUnits(row);

  return (
    toNum(
      row?.["Sellable Units"] ??
      row?.sellable_units ??
      row?.sellableUnits
    ) || available + fcTransfer
  );
};

const monthLabelFromMonthName = (monthName: string) => {
  const clean = String(monthName || "").trim();
  return clean ? clean.slice(0, 3) : "-";
};

const getUniqueSkuCount = (rows: InventoryCurrentRow[]) => {
  const skus = new Set<string>();

  rows.forEach((row) => {
    const sku = getInventoryRowSku(row);
    if (sku) skus.add(sku);
  });

  return skus.size;
};

const sumInventoryUnitsByKeys = (
  rows: InventoryCurrentRow[],
  keys: string[]
) => {
  return rows.reduce((sum, row) => {
    return (
      sum +
      keys.reduce((rowSum, key) => rowSum + toNum(row?.[key]), 0)
    );
  }, 0);
};

const getRowAgeingTotalUnits = (row: InventoryCurrentRow) => {
  const sellableUnits = getInventoryRowSellableUnits(row);

  if (sellableUnits > 0) return sellableUnits;

  const splitFirst180 =
    toNum(row?.["inv-age-0-to-90-days"]) +
    toNum(row?.["inv-age-91-to-180-days"]);

  const combinedFirst180 = toNum(row?.["inv-age-0-to-180-days"]);

  return (
    (splitFirst180 > 0 ? splitFirst180 : combinedFirst180) +
    toNum(row?.["inv-age-181-to-270-days"]) +
    toNum(row?.["inv-age-271-to-365-days"]) +
    toNum(row?.["inv-age-365-plus-days"])
  );
};

const hasInventoryValue = (row: InventoryCurrentRow, key: string) => {
  return toNum(row?.[key]) > 0;
};

const hasOlderThan180Inventory = (row: InventoryCurrentRow) => {
  return (
    toNum(row?.["inv-age-181-to-270-days"]) > 0 ||
    toNum(row?.["inv-age-271-to-365-days"]) > 0 ||
    toNum(row?.["inv-age-365-plus-days"]) > 0
  );
};

const hasHighAlert = (row: InventoryCurrentRow) => {
  return String(row?.["Inventory Alerts"] || "")
    .trim()
    .toLowerCase() === "high alert";
};

const getShortMonthLabel = (monthName?: string) => {
  const clean = String(monthName || "").trim();
  return clean ? clean.slice(0, 3) : "-";
};

// const flattenGlobalAgeSummaryResponses = (
//   responses: InventoryAgeSummaryApiResponse[]
// ): InventoryAgeSummaryApiResponse[] => {
//   return (responses || []).flatMap((res: any) => {
//     if (
//       String(res?.country_key || "").toLowerCase() === "global" &&
//       res?.country_results &&
//       typeof res.country_results === "object"
//     ) {
//       return Object.entries(res.country_results).map(([countryKey, countryRes]: any) => ({
//         ...countryRes,
//         country_key: countryKey,
//       }));
//     }

//     return [res];
//   });
// };

const getSelectedCountryAgeSummaryResponses = (
  responses: InventoryAgeSummaryApiResponse[],
  selectedCountry: string
): InventoryAgeSummaryApiResponse[] => {
  const selectedKey = String(selectedCountry || "uk").toLowerCase();

  return (responses || []).map((res: any) => {
    if (
      String(res?.country_key || "").toLowerCase() === "global" &&
      res?.country_results &&
      typeof res.country_results === "object"
    ) {
      const countryRes =
        res.country_results?.[selectedKey] ||
        res.country_results?.uk ||
        Object.values(res.country_results || {})[0];

      return {
        ...(countryRes || {}),
        country_key: selectedKey,
      };
    }

    return res;
  });
};

const buildAgeingTrendDataFromSummary = (
  ageSummaryResponses: InventoryAgeSummaryApiResponse[],
  bucketColumn: string
): AgeingTrendItem[] => {
  const monthMap = new Map<
    string,
    {
      month: string;
      month_number: number;
      year: number;
      value: number;
    }
  >();

  for (const res of ageSummaryResponses || []) {
    if (!res?.success) continue;

    // Preferred: your new /inventory_current_age_summary response
    if (Array.isArray(res.month_summary) && res.month_summary.length > 0) {
      for (const item of res.month_summary) {
        const key = `${item.year}-${item.month_number}`;

        const previous = monthMap.get(key);

        monthMap.set(key, {
          month: item.month,
          month_number: item.month_number,
          year: item.year,
          // ✅ For global: UK + US same month should be summed
          value: toNum(previous?.value) + toNum(item.totals?.[bucketColumn]),
        });
      }

      continue;
    }

    // Fallback: group age_summary if month_summary is not present
    if (Array.isArray(res.age_summary) && res.age_summary.length > 0) {
      for (const item of res.age_summary) {
        if (item.column !== bucketColumn) continue;

        const monthNumber =
          item.month_number ?? monthIndexMap[item.month.toLowerCase()] + 1;

        const key = `${item.year}-${monthNumber}`;

        monthMap.set(key, {
          month: item.month,
          month_number: monthNumber,
          year: item.year,
          value: toNum(item.units),
        });
      }

      continue;
    }

    // Old response fallback
    if (res.month && res.year && res.totals) {
      const monthNumber = monthIndexMap[res.month.toLowerCase()] + 1;
      const key = `${res.year}-${monthNumber}`;

      monthMap.set(key, {
        month: res.month,
        month_number: monthNumber,
        year: res.year,
        value: toNum(res.totals?.[bucketColumn]),
      });
    }
  }

  return Array.from(monthMap.values())
    .filter((item) => item.month && item.year && item.month_number)
    .sort((a, b) => {
      if (a.year !== b.year) return a.year - b.year;
      return a.month_number - b.month_number;
    })
    .map((item) => ({
      label: getShortMonthLabel(item.month),
      value: item.value,
    }));
};

const getMonthYearFromInventoryTableName = (tableName?: string) => {
  // Example: currentinventory_1_uk_june2026_table
  const match = String(tableName || "").match(/_([a-z]+)(\d{4})_table$/i);

  if (!match) {
    return {
      month: "",
      year: 0,
      month_number: 0,
    };
  }

  const month = match[1].toLowerCase();
  const year = Number(match[2]);
  const month_number = (monthIndexMap[month] ?? -1) + 1;

  return {
    month,
    year,
    month_number,
  };
};

const buildAgeingTrendDataFromInventoryCurrent = (
  inventoryResponses: InventoryCurrentApiResponse[],
  bucketColumn: string
): AgeingTrendItem[] => {
  return (inventoryResponses || [])
    .filter((res) => res?.success)
    .map((res) => {
      const parsed = getMonthYearFromInventoryTableName(res.table_name);

      const month =
        String(res.month || "").toLowerCase() ||
        parsed.month;

      const year =
        Number(res.year || 0) ||
        parsed.year;

      const month_number =
        (monthIndexMap[month] ?? -1) + 1 || parsed.month_number;

      return {
        month,
        year,
        month_number,
        value: toNum(
          res.inventory_age_summary?.columns?.[bucketColumn]?.total
        ),
      };
    })
    .filter((item) => item.month && item.year && item.month_number)
    .sort((a, b) => {
      if (a.year !== b.year) return a.year - b.year;
      return a.month_number - b.month_number;
    })
    .map((item) => ({
      label: getShortMonthLabel(item.month),
      value: item.value,
    }));
};

const isGlobalInventoryResponse = (res?: InventoryCurrentApiResponse) => {
  return (
    String(res?.country_key || "").toLowerCase() === "global" &&
    !!res?.country_results &&
    typeof res.country_results === "object"
  );
};

const getSelectedCountryInventoryResponse = (
  res: InventoryCurrentApiResponse | undefined,
  selectedCountry: string
): InventoryCurrentApiResponse | undefined => {
  if (!res) return undefined;

  if (!isGlobalInventoryResponse(res)) return res;

  const selectedKey = String(selectedCountry || "uk").toLowerCase();

  const countryRes =
    res.country_results?.[selectedKey] ||
    res.country_results?.uk ||
    Object.values(res.country_results || {})[0];

  if (!countryRes) return undefined;

  return {
    ...countryRes,
    country_key: countryRes.country_key || selectedKey,
  };
};

const getMonthNameAliases = (monthName?: string) => {
  const normalizedMonth = String(monthName || "").trim().toLowerCase();
  const numericMonth = Number(normalizedMonth);
  const monthIndex =
    monthIndexMap[normalizedMonth] ??
    (
      Number.isInteger(numericMonth) &&
        numericMonth >= 1 &&
        numericMonth <= 12
        ? numericMonth - 1
        : undefined
    );

  if (monthIndex === undefined) return [];

  const fullMonth = monthNames[monthIndex].toLowerCase();

  return [fullMonth, fullMonth.slice(0, 3)];
};

const getCurrentMonthUnitsSoldKeyForResponse = (
  res?: InventoryCurrentApiResponse,
  sampleRow?: InventoryCurrentRow,
  preferredMonth?: string
) => {
  const keys = Array.from(
    new Set([...(res?.columns ?? []), ...Object.keys(sampleRow ?? {})])
  );

  const normalizeColumnName = (key: string) =>
    String(key || "")
      .toLowerCase()
      .replace(/[_-]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();

  const candidates = keys.filter((key) =>
    normalizeColumnName(key).startsWith("current month units sold")
  );

  const preferredMonths = [
    res?.requested_month,
    res?.month,
    preferredMonth,
  ].filter(Boolean) as string[];

  for (const monthName of preferredMonths) {
    const aliases = getMonthNameAliases(monthName);
    const matchedKey = candidates.find((key) => {
      const normalizedKey = normalizeColumnName(key);

      return aliases.some((alias) => normalizedKey.includes(alias));
    });

    if (matchedKey) return matchedKey;
  }

  return candidates[0];
};

const getCurrentMonthUnitsSoldValue = (
  row: InventoryCurrentRow,
  columnKey?: string
) => {
  if (!columnKey) return undefined;

  const value = row?.[columnKey];

  if (value === null || value === undefined || value === "") {
    return undefined;
  }

  return toNum(value);
};

const getSalesLast30DaysValue = (row: InventoryCurrentRow) => {
  const keys = Object.keys(row || {});

  const salesLast30DaysKey = keys.find((key) => {
    const normalized = String(key).toLowerCase().trim();

    return (
      normalized === "sales last 30 days" ||
      normalized.includes("sales last 30 days")
    );
  });

  return toNum(
    salesLast30DaysKey
      ? row?.[salesLast30DaysKey]
      : row?.salesLast30Days ??
      row?.sales_last_30_days ??
      row?.last_30_days_sales ??
      0
  );
};

const firstInventoryNumberValue = (
  row: InventoryCurrentRow,
  keys: string[]
) => {
  for (const key of keys) {
    const value = row?.[key];

    if (value !== null && value !== undefined && value !== "") {
      return toNum(value);
    }
  }

  return 0;
};

const getInventoryCurrentFbaValue = (row: InventoryCurrentRow) =>
  firstInventoryNumberValue(row, [
    "Sellable Units",
    "available",
    "Current Inventory FBA",
    "current_inventory_fba",
    "current-fba",
    "fulfillable_quantity",
  ]);

const getInventoryCurrentAwdValue = (row: InventoryCurrentRow) =>
  firstInventoryNumberValue(row, [
    "total_onhand_quantity",
    "Current Inventory AWD",
    "current_inventory_awd",
    "current-awd",
    "available_awd",
    "awd_available",
  ]);

const getInventoryTransitFbaValue = (row: InventoryCurrentRow) =>
  firstInventoryNumberValue(row, [
    "inbound-shipped\r",
    "inbound-shipped",
    "In Transit FBA",
    "in_transit_fba",
    "in-transit-fba",
    "fc-transfer",
    "fc_transfer",
    "inbound-working",
  ]);

const getInventoryTransitAwdValue = (row: InventoryCurrentRow) =>
  firstInventoryNumberValue(row, [
    "total_inbound_quantity",
    "In Transit AWD",
    "in_transit_awd",
    "in-transit-awd",
    "inbound_quantity",
    "inbound-quantity",
    "Inbound Units",
    "inbound-shipped",
  ]);

const getInventoryUnsellableFbaValue = (row: InventoryCurrentRow) =>
  firstInventoryNumberValue(row, [
    "Unsellable Inventory FBA",
    "Unsellable FBA",
    "unfulfillable-quantity",
    "unfulfillable_quantity",
    "unfulfillableUnits",
    "unfulfillable_units",
  ]);

const getInventoryUnsellableAwdValue = (row: InventoryCurrentRow) =>
  firstInventoryNumberValue(row, [
    "Unsellable Inventory AWD",
    "Unsellable AWD",
    "unsellable_awd",
    "unfulfillable_awd",
  ]);

const getInventoryStorageCostValue = (row: InventoryCurrentRow) =>
  firstInventoryNumberValue(row, [
    "Storage Cost (Est) - in USD",
    "Storage Cost (Est) in USD",
    "estimated-storage-cost-next-month",
    "Estimated Storage Cost",
    "Estimated Storage Cost (Est)",
    "storage_cost_est",
  ]);

const getInventoryCoverageCurrentAndTransitValue = (
  row: InventoryCurrentRow
) =>
  firstInventoryNumberValue(row, [
    "Coverage Ratio (Current + In Transit)",
    "Coverage Ratio (Current + In transit)",
    "Coverage Ratio (Current + Intransit)",
    "Coverage Ratio (Current + Inventory)",
    "coverage_ratio_current_in_transit",
    "coverage_ratio_current_intransit",
    "coverage_ratio_current_plus_in_transit",
  ]);

const SPLIT_FIRST_180_INVENTORY_BUCKETS: AgeingBucket[] = [
  { key: "zeroToNinety", label: "0–90 Days", color: "#7B9A6D" },
  { key: "ninetyOneToOneEighty", label: "91–180 Days", color: "#FDD36F" },
  { key: "oneEightyOneToTwoSeventy", label: "181–270 Days", color: "#ED9F50" },
  { key: "twoSeventyOneToThreeSixtyFive", label: "271–365 Days", color: "#C49466" },
  { key: "threeSixtyFivePlus", label: "365+ Days", color: "#B75A5A" },
];

const COMBINED_FIRST_180_INVENTORY_BUCKETS: AgeingBucket[] = [
  { key: "zeroToOneEighty", label: "0–180 Days", color: "#7B9A6D" },
  { key: "oneEightyOneToTwoSeventy", label: "181–270 Days", color: "#ED9F50" },
  { key: "twoSeventyOneToThreeSixtyFive", label: "271–365 Days", color: "#C49466" },
  { key: "threeSixtyFivePlus", label: "365+ Days", color: "#B75A5A" },
];

const getDynamicInventoryBuckets = (
  rows: InventoryCurrentRow[]
): AgeingBucket[] => {
  const splitFirst180Total = rows.reduce((sum, row) => {
    return (
      sum +
      getInventoryAgeValue(row, "inv-age-0-to-90-days") +
      getInventoryAgeValue(row, "inv-age-91-to-180-days")
    );
  }, 0);

  const combinedFirst180Total = rows.reduce((sum, row) => {
    return sum + getInventoryAgeValue(row, "inv-age-0-to-180-days");
  }, 0);

  if (splitFirst180Total > 0) {
    return SPLIT_FIRST_180_INVENTORY_BUCKETS;
  }

  if (combinedFirst180Total > 0) {
    return COMBINED_FIRST_180_INVENTORY_BUCKETS;
  }

  return COMBINED_FIRST_180_INVENTORY_BUCKETS;
};

const buildDonutDataFromInventoryAgeSummary = (
  inventoryAgeSummary?: InventoryCurrentApiResponse["inventory_age_summary"]
): DonutChartItem[] => {
  const columns = inventoryAgeSummary?.columns || {};

  const hasSplitFirst180 =
    toNum(columns["inv-age-0-to-90-days"]?.total) > 0 ||
    toNum(columns["inv-age-91-to-180-days"]?.total) > 0;


  const first180Buckets = hasSplitFirst180
    ? [
      {
        bucket: "0–90 Days",
        column: "inv-age-0-to-90-days",
        color: "#7B9A6D",
      },
      {
        bucket: "91–180 Days",
        column: "inv-age-91-to-180-days",
        color: "#FDD36F",
      },
    ]
    : [
      {
        bucket: "0–180 Days",
        column: "inv-age-0-to-180-days",
        color: "#7B9A6D",
      },
    ];


  const summaryBuckets = [
    ...first180Buckets,
    {
      bucket: "181–270 Days",
      column: "inv-age-181-to-270-days",
      color: "#ED9F50",
    },
    {
      bucket: "271–365 Days",
      column: "inv-age-271-to-365-days",
      color: "#C49466",
    },
    {
      bucket: "365+ Days",
      column: "inv-age-365-plus-days",
      color: "#B75A5A",
    },
  ];


  return summaryBuckets
    .map((bucket) => {
      const item = columns[bucket.column];

      return {
        bucket: bucket.bucket,
        units: toNum(item?.total),
        percentageShare:
          typeof item?.percentage_share === "number"
            ? item.percentage_share
            : undefined,
        color: bucket.color,
      };
    })
    .filter((item) => item.units > 0);
};


const buildBackendPercentageHeatmapRow = (
  row: InventoryCurrentRow | undefined
): AgeingRiskHeatmapRow | null => {
  if (!row) return null;

  return {
    productName: "% of Total",
    sku: "-",

    zeroToNinety: toNum(row?.["inv-age-0-to-90-days"]),
    ninetyOneToOneEighty: toNum(row?.["inv-age-91-to-180-days"]),
    zeroToOneEighty: toNum(row?.["inv-age-0-to-180-days"]),

    oneEightyOneToTwoSeventy: toNum(row?.["inv-age-181-to-270-days"]),
    twoSeventyOneToThreeSixtyFive: toNum(row?.["inv-age-271-to-365-days"]),
    threeSixtyFivePlus: toNum(row?.["inv-age-365-plus-days"]),

    available: toNum(row?.["Sellable Units"]),
    totalUnits: toNum(row?.["Sellable Units"]),
    unsellableUnits: toNum(row?.["unfulfillable-quantity"]),

    inboundUnits: undefined,
    unitsSold: undefined,
    coverageRatio: undefined,
    inventoryAlert: "",

    isPercentageRow: true,
  };
};

const buildInventoryInsightsFromResponses = (
  responses: InventoryCurrentApiResponse[],
  ageSummaryResponses: InventoryAgeSummaryApiResponse[] = [],
  countryName: string,
  homeCurrency?: string,
  selectedTrendBucketValue: string = "365+ days",
  selectedGlobalInventoryCountry: string = "uk",
  selectedRange: RangeType = "monthly",
  selectedMonthForRank: string = "",
  selectedYearForRank: string | number = ""
): InventoryInsightsData => {
  const validResponses = responses.filter((res) => res?.success);
  const latestRawResponse = validResponses[validResponses.length - 1];

  const isGlobalInventory = isGlobalInventoryResponse(latestRawResponse);

  // ✅ Countrywise: latestResponse stays same.
  // ✅ Global: latestResponse becomes only UK or only US based on toggle.
  const latestResponse = getSelectedCountryInventoryResponse(
    latestRawResponse,
    selectedGlobalInventoryCountry
  );

  const rawRows = latestResponse?.rows ?? [];

  const backendPercentageRawRow = rawRows.find((row) =>
    isInventoryPercentageRow(row)
  );

  const backendTotalRawRow = rawRows.find((row) =>
    isInventoryTotalRow(row)
  );

  const latestRows = rawRows.filter(
    (row) =>
      !isInventoryTotalRow(row) &&
      !isInventoryPercentageRow(row) &&
      hasAnyAgeingBucketValue(row)
  );

  const dynamicHeatmapBuckets = getDynamicInventoryBuckets(latestRows);

  const isUsingSplitFirst180 = dynamicHeatmapBuckets.some(
    (bucket) => bucket.key === "zeroToNinety"
  );

  const currentMonthUnitsSoldKey = getCurrentMonthUnitsSoldKeyForResponse(
    latestResponse,
    latestRows?.[0],
    selectedMonthForRank
  );

  const heatmapData: AgeingRiskHeatmapRow[] = latestRows.map((row) => {
    const sku = getInventoryRowSku(row);
    const productName = getInventoryRowProductName(row);

    const zeroToNinety = getInventoryAgeValue(row, "inv-age-0-to-90-days");
    const ninetyOneToOneEighty = getInventoryAgeValue(
      row,
      "inv-age-91-to-180-days"
    );

    const zeroToOneEighty = getInventoryAgeValue(
      row,
      "inv-age-0-to-180-days"
    );

    const oneEightyOneToTwoSeventy = getInventoryAgeValue(
      row,
      "inv-age-181-to-270-days"
    );

    const twoSeventyOneToThreeSixtyFive = getInventoryAgeValue(
      row,
      "inv-age-271-to-365-days"
    );

    const threeSixtyFivePlus = getInventoryAgeValue(
      row,
      "inv-age-365-plus-days"
    );

    const first180Total = isUsingSplitFirst180
      ? zeroToNinety + ninetyOneToOneEighty
      : zeroToOneEighty;

    const bucketTotal =
      first180Total +
      oneEightyOneToTwoSeventy +
      twoSeventyOneToThreeSixtyFive +
      threeSixtyFivePlus;

    const available = getInventoryRowAvailableUnits(row);

    const fcTransfer = getInventoryRowFcTransferUnits(row);

    const sellableUnits = getInventoryRowSellableUnits(row);

    const inboundUnits = toNum(
      row?.["Inbound Units"] ??
      row?.inbound_units ??
      row?.inboundUnits ??
      row?.transit_total ??
      row?.inbound_quantity ??
      row?.["inbound_quantity"] ??
      row?.inboundQuantity ??
      row?.["Inbound Quantity"]
    );

    const totalUnits = sellableUnits || bucketTotal;

    const unsellableUnits = toNum(
      row?.["unfulfillable-quantity"] ??
      row?.unfulfillable_quantity ??
      row?.unfulfillableUnits ??
      row?.unfulfillable_units
    );

    const currentFba = getInventoryCurrentFbaValue(row);
    const currentAwd = getInventoryCurrentAwdValue(row);
    const transitFba = getInventoryTransitFbaValue(row);
    const transitAwd = getInventoryTransitAwdValue(row);
    const totalInStock =
      firstInventoryNumberValue(row, [
        "total_stock",
        "Total Sellable Inventory In Stock",
        "Total Sellable In Stock",
      ]) ||
      currentFba + currentAwd;
    const totalInTransit =
      firstInventoryNumberValue(row, [
        "total_transit",
        "Total Sellable Inventory In Transit",
        "Total Sellable In Transit",
      ]) ||
      transitFba + transitAwd;
    const salesLast30Days = getSalesLast30DaysValue(row);
    const currentMonthUnitsSold =
      getCurrentMonthUnitsSoldValue(row, currentMonthUnitsSoldKey) ??
      salesLast30Days;
    const coverageCurrentAndTransit =
      getInventoryCoverageCurrentAndTransitValue(row) ||
      (
        currentMonthUnitsSold > 0
          ? (totalInStock + totalInTransit) / currentMonthUnitsSold
          : 0
      );

    return {
      productName: productName || sku || "-",
      sku,

      countryKey: String(
        latestResponse?.country_key || countryName || ""
      ).toLowerCase(),

      rawProductName: productName || sku || "-",
      rawSku: sku,

      // Latest periods use these two columns
      zeroToNinety,
      ninetyOneToOneEighty,

      // Older / yearly periods use this column
      zeroToOneEighty,

      oneEightyOneToTwoSeventy,
      twoSeventyOneToThreeSixtyFive,
      threeSixtyFivePlus,

      available,
      fcTransfer,
      inboundUnits,
      totalUnits,
      unsellableUnits,
      currentFba,
      currentAwd,
      transitFba,
      transitAwd,
      totalInStock,
      totalInTransit,
      unsellableFba: getInventoryUnsellableFbaValue(row),
      unsellableAwd: getInventoryUnsellableAwdValue(row),
      storageCostUsd: getInventoryStorageCostValue(row),
      coverageCurrentAndTransit,

      unitsSold: currentMonthUnitsSold,

      salesRank:
        row?.["sales-rank"] ??
        row?.sales_rank ??
        row?.salesRank ??
        row?.["Sales Rank"] ??
        row?.["sales rank"] ??
        "",

      previousSalesRank:
        selectedRange === "monthly"
          ? getPreviousSalesRankForSelectedMonth(
            row,
            selectedMonthForRank,
            selectedYearForRank
          )
          : "",

      salesLast30Days,
      coverageRatio: toNum(row?.["Coverage Ratio (In Months)"]),
      inventoryAlert: String(row?.["Inventory Alerts"] || "").trim(),
    };
  });

  const backendTotalHeatmapRow: AgeingRiskHeatmapRow | null = backendTotalRawRow
    ? {
      productName: "Total",
      sku: "-",

      zeroToNinety: toNum(backendTotalRawRow?.["inv-age-0-to-90-days"]),
      ninetyOneToOneEighty: toNum(backendTotalRawRow?.["inv-age-91-to-180-days"]),
      zeroToOneEighty: toNum(backendTotalRawRow?.["inv-age-0-to-180-days"]),

      oneEightyOneToTwoSeventy: toNum(backendTotalRawRow?.["inv-age-181-to-270-days"]),
      twoSeventyOneToThreeSixtyFive: toNum(backendTotalRawRow?.["inv-age-271-to-365-days"]),
      threeSixtyFivePlus: toNum(backendTotalRawRow?.["inv-age-365-plus-days"]),

      available: getInventoryRowAvailableUnits(backendTotalRawRow),
      fcTransfer: getInventoryRowFcTransferUnits(backendTotalRawRow),
      totalUnits: getInventoryRowSellableUnits(backendTotalRawRow),

      inboundUnits: toNum(
        backendTotalRawRow?.["Inbound Units"] ??
        backendTotalRawRow?.inbound_units ??
        backendTotalRawRow?.inboundUnits ??
        backendTotalRawRow?.inbound_quantity ??
        backendTotalRawRow?.["inbound_quantity"] ??
        backendTotalRawRow?.["inbound-quantity"]
      ),

      unsellableUnits: toNum(backendTotalRawRow?.["unfulfillable-quantity"]),
      currentFba: getInventoryCurrentFbaValue(backendTotalRawRow),
      currentAwd: getInventoryCurrentAwdValue(backendTotalRawRow),
      transitFba: getInventoryTransitFbaValue(backendTotalRawRow),
      transitAwd: getInventoryTransitAwdValue(backendTotalRawRow),
      totalInStock:
        firstInventoryNumberValue(backendTotalRawRow, [
          "total_stock",
          "Total Sellable Inventory In Stock",
          "Total Sellable In Stock",
        ]) ||
        getInventoryCurrentFbaValue(backendTotalRawRow) +
        getInventoryCurrentAwdValue(backendTotalRawRow),
      totalInTransit:
        firstInventoryNumberValue(backendTotalRawRow, [
          "total_transit",
          "Total Sellable Inventory In Transit",
          "Total Sellable In Transit",
        ]) ||
        getInventoryTransitFbaValue(backendTotalRawRow) +
        getInventoryTransitAwdValue(backendTotalRawRow),
      unsellableFba: getInventoryUnsellableFbaValue(backendTotalRawRow),
      unsellableAwd: getInventoryUnsellableAwdValue(backendTotalRawRow),
      storageCostUsd: getInventoryStorageCostValue(backendTotalRawRow),

      unitsSold: getCurrentMonthUnitsSoldValue(
        backendTotalRawRow,
        currentMonthUnitsSoldKey
      ),

      salesLast30Days: getSalesLast30DaysValue(backendTotalRawRow),

      coverageRatio: toNum(
        backendTotalRawRow?.["Coverage Ratio (In Months)"] ??
        backendTotalRawRow?.inventory_coverage_ratio
      ),
      coverageCurrentAndTransit:
        getInventoryCoverageCurrentAndTransitValue(backendTotalRawRow) ||
        (() => {
          const currentMonthUnitsSold =
            getCurrentMonthUnitsSoldValue(
              backendTotalRawRow,
              currentMonthUnitsSoldKey
            ) ?? getSalesLast30DaysValue(backendTotalRawRow);
          const totalInStock =
            firstInventoryNumberValue(backendTotalRawRow, [
              "total_stock",
              "Total Sellable Inventory In Stock",
              "Total Sellable In Stock",
            ]) ||
            getInventoryCurrentFbaValue(backendTotalRawRow) +
            getInventoryCurrentAwdValue(backendTotalRawRow);
          const totalInTransit =
            firstInventoryNumberValue(backendTotalRawRow, [
              "total_transit",
              "Total Sellable Inventory In Transit",
              "Total Sellable In Transit",
            ]) ||
            getInventoryTransitFbaValue(backendTotalRawRow) +
            getInventoryTransitAwdValue(backendTotalRawRow);

          return currentMonthUnitsSold > 0
            ? (totalInStock + totalInTransit) / currentMonthUnitsSold
            : 0;
        })(),

      inventoryAlert: "",
      salesRank: "",
      previousSalesRank: "",

      isTotalRow: true,
    }
    : null;

  const backendPercentageHeatmapRow =
    buildBackendPercentageHeatmapRow(backendPercentageRawRow);

  const sortedHeatmapData = [...heatmapData].sort(
    (a, b) =>
      toNum((b as any).unitsSold ?? (b as any).salesLast30Days) -
      toNum((a as any).unitsSold ?? (a as any).salesLast30Days)
  );

  const finalHeatmapData = [
    ...sortedHeatmapData,
    ...(backendTotalHeatmapRow ? [backendTotalHeatmapRow] : []),
    ...(backendPercentageHeatmapRow ? [backendPercentageHeatmapRow] : []),
  ];

  const overallAgeing = latestRows.reduce(
    (acc, row) => {
      acc.zeroToNinety += getInventoryAgeValue(
        row,
        "inv-age-0-to-90-days"
      );

      acc.ninetyOneToOneEighty += getInventoryAgeValue(
        row,
        "inv-age-91-to-180-days"
      );

      acc.zeroToOneEighty += getInventoryAgeValue(
        row,
        "inv-age-0-to-180-days"
      );

      acc.oneEightyOneToTwoSeventy += getInventoryAgeValue(
        row,
        "inv-age-181-to-270-days"
      );

      acc.twoSeventyOneToThreeSixtyFive += getInventoryAgeValue(
        row,
        "inv-age-271-to-365-days"
      );

      acc.threeSixtyFivePlus += getInventoryAgeValue(
        row,
        "inv-age-365-plus-days"
      );

      return acc;
    },
    {
      zeroToNinety: 0,
      ninetyOneToOneEighty: 0,
      zeroToOneEighty: 0,
      oneEightyOneToTwoSeventy: 0,
      twoSeventyOneToThreeSixtyFive: 0,
      threeSixtyFivePlus: 0,
    }
  );

  const backendSummaryDonutData = buildDonutDataFromInventoryAgeSummary(
    latestResponse?.inventory_age_summary
  );

  const fallbackDonutData: DonutChartItem[] = isUsingSplitFirst180
    ? [
      {
        bucket: "0–90 Days",
        units: overallAgeing.zeroToNinety,
        color: "#7B9A6D",
      },
      {
        bucket: "91–180 Days",
        units: overallAgeing.ninetyOneToOneEighty,
        color: "#FDD36F",
      },
      {
        bucket: "181–270 Days",
        units: overallAgeing.oneEightyOneToTwoSeventy,
        color: "#ED9F50",
      },
      {
        bucket: "271–365 Days",
        units: overallAgeing.twoSeventyOneToThreeSixtyFive,
        color: "#C49466",
      },
      {
        bucket: "365+ Days",
        units: overallAgeing.threeSixtyFivePlus,
        color: "#B75A5A",
      },
    ]
    : [
      {
        bucket: "0–180 Days",
        units: overallAgeing.zeroToOneEighty,
        color: "#7B9A6D",
      },
      {
        bucket: "181–270 Days",
        units: overallAgeing.oneEightyOneToTwoSeventy,
        color: "#ED9F50",
      },
      {
        bucket: "271–365 Days",
        units: overallAgeing.twoSeventyOneToThreeSixtyFive,
        color: "#C49466",
      },
      {
        bucket: "365+ Days",
        units: overallAgeing.threeSixtyFivePlus,
        color: "#B75A5A",
      },
    ];


  const donutData: DonutChartItem[] =
    backendSummaryDonutData.length > 0
      ? backendSummaryDonutData
      : fallbackDonutData.filter((item) => item.units > 0);

  const donutTotalUnits =
    latestResponse?.inventory_age_summary?.sellable_total ??
    latestResponse?.inventory_age_summary?.total_units_summary?.sellable?.total ??
    donutData.reduce((sum, item) => sum + toNum(item.units), 0);

  const trendData: AgeingTrendItem[] = [];

  const trendAgeSummaryResponses = isGlobalInventory
    ? getSelectedCountryAgeSummaryResponses(
      ageSummaryResponses,
      selectedGlobalInventoryCountry
    )
    : ageSummaryResponses;

  const trendAllSeriesData = AGEING_TREND_BUCKET_OPTIONS.map((bucket) => {
    const dataFromSummary = buildAgeingTrendDataFromSummary(
      trendAgeSummaryResponses,
      bucket.column
    );

    const dataFromInventoryCurrent = buildAgeingTrendDataFromInventoryCurrent(
      validResponses,
      bucket.column
    );

    return {
      bucketValue: bucket.value,
      bucketLabel: bucket.label,
      color: bucket.color,
      data: dataFromSummary.length > 0
        ? dataFromSummary
        : dataFromInventoryCurrent,
    };
  });

  const healthyRows = latestRows.filter((row) =>
    hasInventoryValue(row, "inv-age-0-to-180-days") ||
    hasInventoryValue(row, "inv-age-0-to-90-days") ||
    hasInventoryValue(row, "inv-age-91-to-180-days")
  );

  const highAlertRows = latestRows.filter((row) => hasHighAlert(row));

  // ✅ ADD POINT 3 HERE
  const highAlertAvgCoverageRatio =
    typeof latestResponse?.high_alert_coverage_summary?.average_coverage_ratio === "number"
      ? latestResponse.high_alert_coverage_summary.average_coverage_ratio
      : (() => {
        const validCoverageRows = highAlertRows
          .map((row) => toNum(row?.["Coverage Ratio (In Months)"]))
          .filter((value) => value > 0);

        if (!validCoverageRows.length) return 0;

        return (
          validCoverageRows.reduce((sum, value) => sum + value, 0) /
          validCoverageRows.length
        );
      })();

  const discountRows = latestRows.filter((row) =>
    hasInventoryValue(row, "inv-age-0-to-180-days")
  );

  const liquidateRows = latestRows.filter((row) =>
    hasOlderThan180Inventory(row)
  );

  const unfulfillableRows = latestRows.filter((row) =>
    toNum(row?.["unfulfillable-quantity"]) > 0
  );

  const latestCategories = latestResponse?.categories || {};

  const estimatedStorageCategory =
    latestCategories?.estimated_storage_cost as any;

  const storageCostTotal =
    getEstimatedStorageCostTotal(latestResponse) ||
    latestRows.reduce(
      (sum, row) =>
        sum + toNum(row?.["estimated-storage-cost-next-month"]),
      0
    );

  const previousStorageCostTotal = toNum(
    estimatedStorageCategory?.previous_storage_cost
  );

  const storageCostDelta = storageCostTotal - previousStorageCostTotal;

  const storageCostDeltaPercentage =
    previousStorageCostTotal > 0
      ? (storageCostDelta / Math.abs(previousStorageCostTotal)) * 100
      : null;

  const storageCostRows = latestRows.filter((row) =>
    toNum(row?.["estimated-storage-cost-next-month"]) > 0
  );

  const hasSplitFirst180Ageing = (rows: InventoryCurrentRow[]) => {
    const splitTotal = rows.reduce((sum, row) => {
      return (
        sum +
        getInventoryAgeValue(row, "inv-age-0-to-90-days") +
        getInventoryAgeValue(row, "inv-age-91-to-180-days")
      );
    }, 0);

    return splitTotal > 0;
  };

  const getSkuCountForAgeColumn = (
    rows: InventoryCurrentRow[],
    column: string
  ) => {
    return rows.filter((row) => {
      const productName = getInventoryRowProductName(row);
      const sku = getInventoryRowSku(row);

      if (!productName && !sku) return false;

      return getInventoryAgeValue(row, column) > 0;
    }).length;
  };

  const getUnitCountForAgeColumn = (
    rows: InventoryCurrentRow[],
    column: string
  ) => {
    return rows.reduce((sum, row) => {
      return sum + getInventoryAgeValue(row, column);
    }, 0);
  };

  const actions: ActionCardItem[] = [
    ...(isUsingSplitFirst180
      ? [
        {
          key: "age_0_90",
          label: "Healthy",
          description: "Stock aged 0–90 days",
          count: getSkuCountForAgeColumn(latestRows, "inv-age-0-to-90-days"),
          skuCount: getSkuCountForAgeColumn(latestRows, "inv-age-0-to-90-days"),
          unitCount: getUnitCountForAgeColumn(latestRows, "inv-age-0-to-90-days"),
          color: "#7B9A6D",
          backgroundColor: "#ffffff",
        },

        // ✅ High Alert comes before Discount / 91–180 Days
        {
          key: "high_alert",
          label: INVENTORY_ACTION_META.high_alert.label,
          description: INVENTORY_ACTION_META.high_alert.description,
          count: getUniqueSkuCount(highAlertRows),
          displayValue: getUniqueSkuCount(highAlertRows),
          skuCount:
            latestResponse?.high_alert_coverage_summary?.high_alert_sku_count ??
            getUniqueSkuCount(highAlertRows),

          avgCoverageRatio: highAlertAvgCoverageRatio,

          color: INVENTORY_ACTION_META.high_alert.color,
          backgroundColor: INVENTORY_ACTION_META.high_alert.backgroundColor,
        },

        {
          key: "age_91_180",
          label: "Discount",
          description: "Stock aged 91–180 days",
          count: getSkuCountForAgeColumn(latestRows, "inv-age-91-to-180-days"),
          skuCount: getSkuCountForAgeColumn(latestRows, "inv-age-91-to-180-days"),
          unitCount: getUnitCountForAgeColumn(latestRows, "inv-age-91-to-180-days"),
          color: "#FDD36F",
          backgroundColor: "#ffffff",
        },
      ]
      : [
        {
          key: "healthy",
          label: "Healthy",
          description: "Stock covers 0–180 days",
          count: getSkuCountForAgeColumn(latestRows, "inv-age-0-to-180-days"),
          skuCount: getSkuCountForAgeColumn(latestRows, "inv-age-0-to-180-days"),
          unitCount: getUnitCountForAgeColumn(latestRows, "inv-age-0-to-180-days"),
          color: INVENTORY_ACTION_META.healthy.color,
          backgroundColor: INVENTORY_ACTION_META.healthy.backgroundColor,
        },

        // ✅ High Alert comes after Healthy when 0–180 combined exists
        {
          key: "high_alert",
          label: INVENTORY_ACTION_META.high_alert.label,
          description: INVENTORY_ACTION_META.high_alert.description,
          count: getUniqueSkuCount(highAlertRows),
          displayValue: getUniqueSkuCount(highAlertRows),
          skuCount:
            latestResponse?.high_alert_coverage_summary?.high_alert_sku_count ??
            getUniqueSkuCount(highAlertRows),

          avgCoverageRatio: highAlertAvgCoverageRatio,

          color: INVENTORY_ACTION_META.high_alert.color,
          backgroundColor: INVENTORY_ACTION_META.high_alert.backgroundColor,
        },
      ]),

    {
      key: "liquidate",
      label: INVENTORY_ACTION_META.liquidate.label,
      description: INVENTORY_ACTION_META.liquidate.description,
      count: getUniqueSkuCount(liquidateRows),
      displayValue: getUniqueSkuCount(liquidateRows),
      skuCount: getUniqueSkuCount(liquidateRows),
      unitCount: sumInventoryUnitsByKeys(liquidateRows, [
        "inv-age-181-to-270-days",
        "inv-age-271-to-365-days",
        "inv-age-365-plus-days",
      ]),
      color: INVENTORY_ACTION_META.liquidate.color,
      backgroundColor: INVENTORY_ACTION_META.liquidate.backgroundColor,
    },

    {
      key: "unfulfillable",
      label: INVENTORY_ACTION_META.unfulfillable.label,
      description: INVENTORY_ACTION_META.unfulfillable.description,
      count: getUniqueSkuCount(unfulfillableRows),
      displayValue: getUniqueSkuCount(unfulfillableRows),
      skuCount: getUniqueSkuCount(unfulfillableRows),
      unitCount: unfulfillableRows.reduce(
        (sum, row) => sum + toNum(row?.["unfulfillable-quantity"]),
        0
      ),
      color: INVENTORY_ACTION_META.unfulfillable.color,
      backgroundColor: INVENTORY_ACTION_META.unfulfillable.backgroundColor,
    },

    {
      key: "estimated_storage_cost",
      label: INVENTORY_ACTION_META.estimated_storage_cost.label,
      description: INVENTORY_ACTION_META.estimated_storage_cost.description,
      count: getUniqueSkuCount(storageCostRows),
      displayValue: formatInventoryStorageCost(
        storageCostTotal,
        countryName,
        homeCurrency
      ),
      deltaValue:
        previousStorageCostTotal > 0
          ? formatInventoryStorageCost(
            storageCostDelta,
            countryName,
            homeCurrency
          )
          : undefined,
      deltaPercentage: storageCostDeltaPercentage,
      skuCount: getUniqueSkuCount(storageCostRows),
      unitCount: storageCostRows.reduce(
        (sum, row) => sum + getRowAgeingTotalUnits(row),
        0
      ),
      color: INVENTORY_ACTION_META.estimated_storage_cost.color,
      backgroundColor:
        INVENTORY_ACTION_META.estimated_storage_cost.backgroundColor,
    },
  ];

  return {
    heatmapBuckets: dynamicHeatmapBuckets,
    heatmapData: finalHeatmapData,
    donutSku: "Overall",
    donutData,
    donutTotalUnits,
    trendSelectedBucket: "all",
    trendData,
    trendLineColor: "#B75A5A",
    trendAllSeriesData,
    trendBucketOptions: AGEING_TREND_BUCKET_OPTIONS.map((bucket) => ({
      label: bucket.label,
      value: bucket.value,
      color: bucket.color,
    })),
    actions,
    actionLogic: INVENTORY_ACTION_LOGIC,
    inventoryAgeSummary: latestResponse?.inventory_age_summary,
  };
};

const donutData: DonutChartItem[] = [
  { bucket: "0–180 Days", units: 360, color: "#7B9A6D" },
  { bucket: "181–270 Days", units: 180, color: "#ED9F50" },
  { bucket: "271–365 Days", units: 90, color: "#C49466" },
  { bucket: "365+ Days", units: 130, color: "#B75A5A" },
];

const donutTotalUnits = 760;

const trendSelectedBucket = "all";

const trendData: AgeingTrendItem[] = [
  { label: "Jan", value: 80 },
  { label: "Feb", value: 120 },
  { label: "Mar", value: 180 },
  { label: "Apr", value: 260 },
  { label: "May", value: 310 },
  { label: "Jun", value: 280 },
  { label: "Jul", value: 260 },
  { label: "Aug", value: 210 },
];

const trendAllSeriesData = [
  {
    bucketValue: "0-180 days",
    bucketLabel: "0–180 Days",
    color: "#7B9A6D",
    data: [
      { label: "Jan", value: 80 },
      { label: "Feb", value: 120 },
      { label: "Mar", value: 150 },
      { label: "Apr", value: 190 },
      { label: "May", value: 220 },
      { label: "Jun", value: 210 },
      { label: "Jul", value: 180 },
      { label: "Aug", value: 160 },
    ],
  },
  {
    bucketValue: "181-270 days",
    bucketLabel: "181–270 Days",
    color: "#ED9F50",
    data: [
      { label: "Jan", value: 40 },
      { label: "Feb", value: 70 },
      { label: "Mar", value: 110 },
      { label: "Apr", value: 150 },
      { label: "May", value: 170 },
      { label: "Jun", value: 160 },
      { label: "Jul", value: 145 },
      { label: "Aug", value: 130 },
    ],
  },
  {
    bucketValue: "271-365 days",
    bucketLabel: "271–365 Days",
    color: "#C49466",
    data: [
      { label: "Jan", value: 60 },
      { label: "Feb", value: 90 },
      { label: "Mar", value: 120 },
      { label: "Apr", value: 180 },
      { label: "May", value: 210 },
      { label: "Jun", value: 200 },
      { label: "Jul", value: 190 },
      { label: "Aug", value: 170 },
    ],
  },
  {
    bucketValue: "365+ days",
    bucketLabel: "365+ Days",
    color: "#B75A5A",
    data: trendData,
  },
];


const trendLineColor = "#B75A5A";

const inventoryActions: ActionCardItem[] = [
  {
    key: "healthy",
    label: "Healthy",
    description: "Stock covers 0–180 days",
    count: 6,
    displayValue: 6,
    skuCount: 6,
    unitCount: 405,
    color: "#7B9A6D",
    backgroundColor: "#ffffff",
  },
  {
    key: "high_alert",
    label: "High Alert",
    description: "Shipment Required",
    count: 3,
    displayValue: 3,
    skuCount: 3,
    unitCount: 324,
    color: "#B75A5A",
    backgroundColor: "#ffffff",
  },
  // {
  //   key: "discount",
  //   label: "Discount",
  //   description: "Stock aged 91–180 days",
  //   count: 3,
  //   displayValue: 3,
  //   skuCount: 3,
  //   unitCount: 4,
  //   color: "#FDD36F",
  //   backgroundColor: "#ffffff",
  // },
  {
    key: "liquidate",
    label: "Liquidate",
    description: "Stock older than 180 days",
    count: 8,
    displayValue: 8,
    skuCount: 8,
    unitCount: 2548,
    color: "#ED9F50",
    backgroundColor: "#ffffff",
  },
  {
    key: "unfulfillable",
    label: "Unfulfillable",
    description: "Remove or dispose stock",
    count: 3,
    displayValue: 3,
    skuCount: 3,
    unitCount: 148,
    color: "#3A8EA4",
    backgroundColor: "#ffffff",
  },
  {
    key: "estimated_storage_cost",
    label: "Estimate Storage",
    description: "Monthly storage estimate",
    count: 11,
    displayValue: "£428.12",
    deltaValue: "£194.42",
    deltaPercentage: 83.19,
    skuCount: 11,
    unitCount: 2877,
    color: "#C49466",
    backgroundColor: "#ffffff",
  },
];

const inventoryActionLogic: ActionLogicItem[] = [
  {
    key: "healthy",
    label: "Healthy",
    description: "Stock covers 0–180 days",
    color: "#7B9A6D",
  },
  {
    key: "high_alert",
    label: "High Alert",
    description: "Shipment Required",
    color: "#B75A5A",
  },
  // {
  //   key: "discount",
  //   label: "Discount",
  //   description: "Stock aged 91–180 days",
  //   color: "#FDD36F",
  // },
  {
    key: "liquidate",
    label: "Liquidate",
    description: "Stock older than 180 days",
    color: "#ED9F50",
  },
  {
    key: "unfulfillable",
    label: "Unfulfillable",
    description: "Remove or dispose stock",
    color: "#3A8EA4",
  },
  {
    key: "estimated_storage_cost",
    label: "Estimate Storage",
    description: "Monthly storage estimate",
    color: "#C49466",
  },
];

/* ---------------------- Component ---------------------- */
const Dropdowns: React.FC<DropdownsProps> = ({
  initialRanged,
  initialCountryName,
  initialMonth,
  initialYear,
}) => {
  const { data: userData } = useGetUserDataQuery();

  const homeCurrency = (userData?.homeCurrency || "USD").toLowerCase();

  const router = useRouter();
  const searchParams = useSearchParams();

  const ranged = initialRanged;
  const countryName = initialCountryName || "";
  const month = initialMonth || "";
  const year = initialYear || "";

  // const isGlobalPage = countryName.toLowerCase() === "global";
  const isGlobalPage = (countryName || "").toLowerCase() === "global";

  const globalHomeCurrency = isGlobalPage ? homeCurrency : undefined;

  const adsCountry = useMemo<"UK" | "US" | "CA">(() => {
    const c = String(initialCountryName || "").toLowerCase();

    if (c === "us" || c === "usa" || c === "united states") return "US";
    if (c === "ca" || c === "canada") return "CA";
    return "UK";
  }, [initialCountryName]);

  const token = useMemo(() => {
    if (typeof window === "undefined") return null;
    return localStorage.getItem("jwtToken");
  }, []);

  const userid = useMemo(() => {
    if (!token) return "";
    try {
      const decoded = jwtDecode<JwtPayload>(token);
      return decoded?.user_id ?? "";
    } catch {
      return "";
    }
  }, [token]);

  const quarterMapping: Record<string, string> = {
    Q1: "quarter1",
    Q2: "quarter2",
    Q3: "quarter3",
    Q4: "quarter4",
  };

  const buildParentSkuUrl = (
    rangeType: RangeType,
    monthVal: string,
    quarterVal: string,
    yearVal: string,
    countryVal: string
  ) => {
    const isGlobal = countryVal.toLowerCase() === "global";

    if (rangeType === "monthly") {
      const url = new URL(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/skutableprofit`
      );

      url.searchParams.set("country", countryVal);
      url.searchParams.set("month", monthVal.toLowerCase());
      url.searchParams.set("year", String(yearVal));

      if (isGlobal && homeCurrency) {
        url.searchParams.set("homeCurrency", homeCurrency);
      }

      return url.toString();
    }

    if (rangeType === "quarterly") {
      const backendQuarter = quarterMapping[quarterVal] || "";

      const url = new URL(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/quarterlyskutable`
      );

      url.searchParams.set("quarter", backendQuarter);
      url.searchParams.set("country", countryVal);
      url.searchParams.set("year", String(yearVal));
      url.searchParams.set("userid", String(userid));

      if (isGlobal && homeCurrency) {
        url.searchParams.set("homeCurrency", homeCurrency);
      }

      return url.toString();
    }

    const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/YearlySKU`);

    url.searchParams.set("country", countryVal);
    url.searchParams.set("year", String(yearVal));

    if (isGlobal && homeCurrency) {
      url.searchParams.set("homeCurrency", homeCurrency);
    }

    return url.toString();
  };

  const isDemoMode =
    String(month).toUpperCase() === "NA" ||
    String(year).toUpperCase() === "NA";

  const effectiveCountryName = isDemoMode ? "global" : countryName;
  const effectiveHomeCurrency = isDemoMode ? "usd" : globalHomeCurrency;

  const currencySymbol = isDemoMode
    ? getCurrencySymbol("global")
    : isGlobalPage
      ? getCurrencySymbol(homeCurrency)
      : getCurrencySymbol(countryName || "");

  const getInitialSelectedYear = () => {
    if (isDemoMode) return String(new Date().getFullYear());

    const routeYear = String(initialYear || "").trim();

    if (routeYear && routeYear.toUpperCase() !== "NA") {
      return routeYear;
    }

    const routeRange = String(initialRanged || "").toLowerCase();

    if (routeRange === "monthly") {
      return computeDefaultMonthlyPeriod().year;
    }

    if (routeRange === "quarterly") {
      return computeDefaultQuarterlyPeriod().year;
    }

    return computeDefaultYearlyYear();
  };

  const getInitialRange = (): RangeType => {
    const routeRange = String(initialRanged || "").toLowerCase();

    if (routeRange === "monthly") return "monthly";
    if (routeRange === "quarterly") return "quarterly";
    return "yearly";
  };

  const [range, setRange] = useState<RangeType>(() => getInitialRange());
  const [selectedAgeingTrendBucket, setSelectedAgeingTrendBucket] =
    useState<string>("365+ days");

  const [selectedGlobalInventoryCountry, setSelectedGlobalInventoryCountry] =
    useState<string>("uk");

  const [selectedMonth, setSelectedMonth] = useState<string>("");

  const [selectedYear, setSelectedYear] = useState<string>(() => {
    return getInitialSelectedYear();
  });

  const [selectedQuarter, setSelectedQuarter] = useState<Quarter | "">("");
  const [availablePeriods, setAvailablePeriods] =
    useState<FetchedPeriods | null>(null);

  const salesLast30DaysLabel = buildCurrentMonthUnitsSoldLabel(
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear
  );
  const inventoryHeatmapUnitSalesDataKey: AgeingRiskUnitSalesDataKey = "unitsSold";

  const [uploadsData, setUploadsData] = useState<UploadHistoryResponse | null>(
    isDemoMode ? DEMO_UPLOAD_HISTORY : null
  );

  const [cmPieTab, setCmPieTab] = useState<"cm1" | "cm2">("cm1");



  const [inventoryInsightsData, setInventoryInsightsData] =
    useState<InventoryInsightsData | null>(
      isDemoMode
        ? createZeroInventoryInsightsData(currencySymbol)
        : null
    );

  const [inventoryInsightsLoading, setInventoryInsightsLoading] = useState(false);
  const [inventoryInsightsError, setInventoryInsightsError] = useState<string | null>(null);
  const [inventoryRawResponses, setInventoryRawResponses] = useState<{
    inventory: InventoryCurrentApiResponse[];
    ageSummary: InventoryAgeSummaryApiResponse[];
  } | null>(null);
  const [allDropdownsSelected, setAllDropdownsSelected] = useState(() => {
    if (isDemoMode) return true;

    const initialRange = getInitialRange();
    const initialSelectedYear = getInitialSelectedYear();

    if (initialRange === "yearly") return !!initialSelectedYear;
    if (initialRange === "monthly") return !!initialSelectedYear && !!initialMonth;
    if (initialRange === "quarterly") return !!initialSelectedYear && isQuarter(initialMonth);

    return false;
  });
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [loading, setLoading] = useState(false);
  const [showNoDataOverlay, setShowNoDataOverlay] = useState(false);
  const [performanceTrend, setPerformanceTrend] = useState<PerformanceTrendPayload | null>(
    isDemoMode ? DEMO_PERFORMANCE_TREND : null
  );
  const [performanceTrendMetric, setPerformanceTrendMetric] = useState<"net_sales" | "units">("net_sales");
  const [performanceTrendBase64, setPerformanceTrendBase64] = useState<string | null>(null);
  const [trendExportApi, setTrendExportApi] = useState<TrendChartExportApi | null>(null);
  const [focusedChart, setFocusedChart] = useState<FocusedChart>(null);
  const [bargraphUploads, setBargraphUploads] = useState<UploadRow[]>(
    isDemoMode ? DEMO_UPLOADS : []
  );
  const [bargraphLoading, setBargraphLoading] = useState(false);
  const [bargraphUserMeta, setBargraphUserMeta] = useState<{ company_name?: string; brand_name?: string } | null>(null);
  const [graphPageUploads, setGraphPageUploads] = useState<UploadRow[]>(
    isDemoMode ? DEMO_UPLOADS : []
  );
  const [graphPageLoading, setGraphPageLoading] = useState(false);
  const [graphPageUserMeta, setGraphPageUserMeta] = useState<{ company_name?: string; brand_name?: string } | null>(null);
  const [graphPageError, setGraphPageError] = useState<string | null>(null);
  const [skuRows, setSkuRows] = useState<TableRow[]>(
    isDemoMode ? DEMO_SKU_ROWS : []
  );

  const [skuRowsLoading, setSkuRowsLoading] = useState(false);
  const [skuRowsError, setSkuRowsError] = useState<string | null>(null);
  const [skuNoDataFound, setSkuNoDataFound] = useState(false);

  const shouldShowPreviewData =
    isDemoMode ||
    !allDropdownsSelected;

  const displaySkuRows = shouldShowPreviewData ? DEMO_SKU_ROWS : skuRows;
  const displaySkuLoading = shouldShowPreviewData ? false : skuRowsLoading;
  const displaySkuError = shouldShowPreviewData ? null : skuRowsError;
  const displaySkuNoDataFound = shouldShowPreviewData ? false : skuNoDataFound;

  const [showAmazonFetchSuccess, setShowAmazonFetchSuccess] = useState(false);
  const [showAmazonAdsConnect, setShowAmazonAdsConnect] = useState(false);

  const [adsStatusLoading] = useState(false);
  const [adsStatus] = useState<any | null>(null);
  const [adsConnecting, setAdsConnecting] = useState(false);
  const [adsError, setAdsError] = useState<string | null>(null);


  const [activeTab, setActiveTab] = useState<DashboardTab>("businessSummary");
  const shouldScrollTabTopRef = useRef(false);
  const [pendingHash, setPendingHash] = useState<string>("");
  const [targetSummary, setTargetSummary] = useState<{
    target_sales?: number;
    shortfall_total?: number;
    cashflow_total?: number;
  } | null>(isDemoMode ? DEMO_TARGET_SUMMARY : null);

  const [targetSummaryLoading, setTargetSummaryLoading] = useState(false);

  useEffect(() => {
    if (typeof window !== "undefined" && window.location.hash) return;
    setActiveTab("businessSummary");
  }, [range, selectedMonth, selectedQuarter, selectedYear, countryName]);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const applyHash = (rawHash?: string) => {
      const hash = (rawHash ?? window.location.hash).replace("#", "");
      if (!hash) return;

      const targetTab = HASH_TO_FINANCE_TAB[hash];
      if (!targetTab) return;

      setPendingHash(hash);
      setActiveTab(targetTab);
    };

    const onHashChange = () => {
      applyHash(window.location.hash);
    };

    const onCustomHashNavigate = (event: Event) => {
      const customEvent = event as CustomEvent<{ hash?: string }>;
      if (!customEvent.detail?.hash) return;
      applyHash(`#${customEvent.detail.hash}`);
    };

    applyHash(window.location.hash);

    window.addEventListener("hashchange", onHashChange);
    window.addEventListener("page-hash-navigate", onCustomHashNavigate as EventListener);

    return () => {
      window.removeEventListener("hashchange", onHashChange);
      window.removeEventListener("page-hash-navigate", onCustomHashNavigate as EventListener);
    };
  }, []);

  useEffect(() => {
    if (!pendingHash) return;
    if (!allDropdownsSelected) return;

    const timer = setTimeout(() => {
      window.scrollTo({
        top: 0,
        behavior: "auto",
      });

      setPendingHash("");
    }, 50);

    return () => clearTimeout(timer);
  }, [activeTab, pendingHash, allDropdownsSelected]);

  useEffect(() => {
    if (!shouldScrollTabTopRef.current) return;

    shouldScrollTabTopRef.current = false;

    const scrollNow = () => {
      scrollFinancePageToTop();
    };

    scrollNow();

    const r1 = requestAnimationFrame(scrollNow);
    const t1 = window.setTimeout(scrollNow, 50);
    const t2 = window.setTimeout(scrollNow, 150);
    const t3 = window.setTimeout(scrollNow, 350);

    return () => {
      cancelAnimationFrame(r1);
      window.clearTimeout(t1);
      window.clearTimeout(t2);
      window.clearTimeout(t3);
    };
  }, [activeTab]);

  const formatInventoryExcelCountryLabel = (country?: string) => {
    const value = String(country || "").trim().toLowerCase();

    if (value === "uk") return "UK";
    if (value === "us") return "US";
    if (value === "global") return "Global";
    if (value === "ca" || value === "canada") return "Canada";
    if (value === "eu" || value === "europe") return "Europe";

    return value ? value.charAt(0).toUpperCase() + value.slice(1) : "";
  };

  const formatInventoryExcelMonthName = (month?: string) => {
    const value = String(month || "").trim();

    if (!value) return "";

    return value.charAt(0).toUpperCase() + value.slice(1).toLowerCase();
  };

  const getInventoryInsightsPeriodLabel = () => {
    if (range === "monthly") {
      return `${formatInventoryExcelMonthName(selectedMonth)} ${selectedYear}`;
    }

    if (range === "quarterly") {
      return `${selectedQuarter} ${selectedYear}`;
    }

    return `${selectedYear}`;
  };

  const getInventoryInsightsReportCountry = () => {
    return String(countryName || "").toLowerCase() === "global"
      ? selectedGlobalInventoryCountry
      : countryName;
  };

  const getInventoryInsightsFileName = () => {
    return `Inventory Insights Report - ${formatInventoryExcelCountryLabel(
      getInventoryInsightsReportCountry()
    )} - ${getInventoryInsightsPeriodLabel()}.xlsx`;
  };

  const tabsDisabled: Partial<Record<DashboardTab, boolean>> = useMemo(() => {
    if (isDemoMode) {
      return {
        graphs: false,
        businessSummary: false,
        inventoryInsights: false,
        skuBreakdown: false,
        skuwiseProfit: false,
        cashFlow: false,
      };
    }

    const disabled = !allDropdownsSelected;

    return {
      graphs: disabled,
      businessSummary: disabled,
      inventoryInsights: disabled,
      skuBreakdown: disabled,
      skuwiseProfit: disabled,
      cashFlow: disabled,
    };
  }, [allDropdownsSelected, isDemoMode]);

  const nameToSkuMap = useMemo(() => {
    const map: Record<string, string> = {};

    for (const r of displaySkuRows || []) {
      const name = normalizeKey(String((r as any).product_name ?? ""));
      const sku = String((r as any).sku ?? "").trim();

      if (name && sku) map[name] = sku;
    }

    return map;
  }, [displaySkuRows]);



  const toggleFocus = (which: Exclude<FocusedChart, null>) => {
    setFocusedChart((prev) => (prev === which ? null : which));
  };

  const pnlCollapsed = focusedChart !== "pnl";

  // ---------------- AI Summary Panel state ----------------
  const [aiPanel, setAiPanel] = useState<AiPanelData | null>(
    isDemoMode ? DEMO_AI_PANEL : null
  );
  const [aiPanelLoading, setAiPanelLoading] = useState(false);
  const [aiPanelError, setAiPanelError] = useState<string | null>(null);

  const aiRequestIdRef = useRef(0);
  const uploadHistoryRequestIdRef = useRef(0);

  const [selectedAiProductBlock, setSelectedAiProductBlock] =
    useState<ProductInsightBlock | null>(null);

  const [selectedAiProductRecObj, setSelectedAiProductRecObj] =
    useState<any>(null);
  const [aiBestPerformanceLoading, setAiBestPerformanceLoading] = useState(false);
  const [aiBestPerformanceError, setAiBestPerformanceError] = useState<string | null>(null);
  const [aiBestPerformanceData, setAiBestPerformanceData] =
    useState<ProductBestPerformanceData | null>(null);

  const aiProductBlocks = useMemo(() => {
    return parseProductInsightsBlocks(aiPanel?.skuInsightsBullets ?? [], range);
  }, [aiPanel?.skuInsightsBullets, range]);

  const aiSkuActions = useMemo(() => {
    const recommendationsMap = aiPanel?.recommendationsMap;

    return (
      (recommendationsMap as any)?.sku_actions ??
      (recommendationsMap as any)?.recommendations ??
      recommendationsMap ??
      {}
    );
  }, [aiPanel?.recommendationsMap]);

  const openAiProductDrawerByName = useCallback(
    (productName: string, sku?: string) => {
      const cleanName = String(productName || "").trim();
      const cleanSku = String(sku || "").trim();

      if (!cleanName && !cleanSku) return;

      const normalizedClickedName = normalizeKey(cleanName);

      const block =
        // 1. Best match: exact SKU
        (cleanSku
          ? aiProductBlocks.find(
            (b) =>
              String(b.skuKey || "").trim().toLowerCase() ===
              cleanSku.toLowerCase()
          )
          : undefined) ||

        // 2. Exact product name only
        aiProductBlocks.find(
          (b) => normalizeKey(b.name) === normalizedClickedName
        );

      if (!block) {
        console.warn("No AI insight block found for:", {
          productName: cleanName,
          sku: cleanSku,
        });
        return;
      }

      const skuKey =
        block.skuKey ||
        cleanSku ||
        nameToSkuMap?.[normalizeKey(block.name)];

      const recObj =
        (skuKey && (aiSkuActions as any)[skuKey]) ||
        (aiSkuActions as any)[block.name] ||
        (aiSkuActions as any)[block.name.trim()] ||
        null;

      setSelectedAiProductRecObj(recObj);
      setSelectedAiProductBlock(block);
    },
    [aiProductBlocks, aiSkuActions, nameToSkuMap]
  );


  const handleHeatmapProductClick = useCallback(
    (heatmapRow: AgeingRiskHeatmapRow) => {
      if (!heatmapRow || heatmapRow.isTotalRow || heatmapRow.isOthersRow) {
        return;
      }

      const productName = String(heatmapRow.productName || "").trim();
      const sku = String(heatmapRow.sku || "").trim();

      if (!productName && !sku) {
        return;
      }

      openAiProductDrawerByName(productName, sku);
    },
    [openAiProductDrawerByName]
  );

  useEffect(() => {
    if (!selectedAiProductBlock) return;

    const ac = new AbortController();

    const fetchBestPerformance = async () => {
      try {
        setAiBestPerformanceLoading(true);
        setAiBestPerformanceError(null);
        setAiBestPerformanceData(null);

        const token =
          typeof window !== "undefined"
            ? localStorage.getItem("jwtToken")
            : null;

        if (!token) throw new Error("Missing token");

        const isOtherSkusBlock =
          selectedAiProductBlock.isOtherSkus ||
          selectedAiProductBlock.name.trim().toLowerCase() === "other skus";

        const productName =
          isOtherSkusBlock
            ? aiProductBlocks.find(
              (b) =>
                !b.isOtherSkus &&
                b.name.trim().toLowerCase() !== "other skus"
            )?.name
            : selectedAiProductBlock.name;

        if (!productName) return;

        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductBestPerformance`,
          {
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
            signal: ac.signal,
          }
        );

        const json = await res.json().catch(() => ({}));

        if (!res.ok) {
          throw new Error(json?.error || "Failed to fetch best performance");
        }

        setAiBestPerformanceData(json?.best_performance ?? null);
      } catch (e: any) {
        if (e?.name === "AbortError") return;
        setAiBestPerformanceError(e?.message || "Failed to load best performance");
      } finally {
        setAiBestPerformanceLoading(false);
      }
    };

    fetchBestPerformance();

    return () => ac.abort();
  }, [
    selectedAiProductBlock,
    aiProductBlocks,
    countryName,
    homeCurrency,
    currencySymbol,
  ]);

  const [chartExportApi, setChartExportApi] = useState<ProfitChartExportApi | null>(null);
  const [skuExportPayload, setSkuExportPayload] = useState<SkuExportPayload | null>(null);

  const lastSkuExportPayloadSignatureRef = useRef<string>("");

  const handleSkuExportPayloadChange = useCallback((payload: SkuExportPayload | null) => {
    if (!payload) {
      if (lastSkuExportPayloadSignatureRef.current !== "") {
        lastSkuExportPayloadSignatureRef.current = "";
        setSkuExportPayload(null);
      }
      return;
    }

    let signature = "";

    try {
      signature = JSON.stringify(payload);
    } catch {
      signature = String(Date.now());
    }

    if (signature === lastSkuExportPayloadSignatureRef.current) return;

    lastSkuExportPayloadSignatureRef.current = signature;
    setSkuExportPayload(payload);
  }, []);

  const [expenseBreakdownPieBase64, setExpenseBreakdownPieBase64] = useState<string | null>(null);

  const [productWiseCm1PieBase64, setProductWiseCm1PieBase64] = useState<string | null>(null);
  const [productWiseCm2PieBase64, setProductWiseCm2PieBase64] =
    useState<string | null>(null);
  const [showSummaryModal, setShowSummaryModal] = useState(false);

  const [primaryGoal, setPrimaryGoal] = useState("");
  const [riskLevel, setRiskLevel] = useState("");
  const [maxTacos, setMaxTacos] = useState<number | "">("");
  const [adBudget, setAdBudget] = useState<number | "">("");
  const [summaryNotes, setSummaryNotes] = useState("");
  const [maxPriceIncreasePct, setMaxPriceIncreasePct] = useState<number | "">("");

  const layoutRef = useRef<HTMLDivElement | null>(null);
  const tabTopRef = useRef<HTMLDivElement | null>(null);
  const [overlayBounds, setOverlayBounds] = useState<{
    left: number;
    width: number;
  }>({
    left: 0,
    width: 0,
  });

  const emptyTopBottomData: TopBottomData = {
    rows: [],
    totals: {
      profit: "0.00",
      profitMix: "0.00",
      salesMix: "0.00",
      avg_per_unit: "0.00",
    },
  };

  const computeTopBottom5 = (
    rows: TableRow[]
  ): {
    topData: TopBottomData;
    bottomData: TopBottomData;
    hasCm2Data: boolean;
  } => {
    const clean = (rows || []).filter(Boolean);

    if (!clean.length) {
      return {
        topData: emptyTopBottomData,
        bottomData: emptyTopBottomData,
        hasCm2Data: false,
      };
    }

    const num = (v: any) => {
      if (v === null || v === undefined) return 0;
      if (typeof v === "number") return Number.isFinite(v) ? v : 0;

      const n = Number(String(v).replace(/,/g, "").trim());
      return Number.isFinite(n) ? n : 0;
    };

    const lower = (v: any) => String(v || "").trim().toLowerCase();

    const withoutTotal = clean.filter((r) => {
      const name = lower((r as any).product_name);
      const sku = lower((r as any).sku);

      return (
        name !== "total" &&
        sku !== "total" &&
        name !== "others" &&
        sku !== "others"
      );
    });

    const topBottomHasCm2Data = (() => {
      const currentRange = String(range || "").trim().toLowerCase();

      if (currentRange === "yearly" || currentRange === "quarterly") {
        return false;
      }

      return withoutTotal.some((row: any) => {
        const adsSpend = num(row.ads_spend ?? row.advertising_total);
        const backendAcos = num(row.acos);
        const netSales = num(row.net_sales);

        const acos =
          backendAcos || (netSales !== 0 ? (adsSpend / netSales) * 100 : 0);

        const cm1Profit = num(row.profit);
        const cm2Profit = num(row.cm2_profit ?? row.cm2_profit_total);

        return (
          adsSpend !== 0 ||
          acos !== 0 ||
          Math.abs(cm2Profit - cm1Profit) > 0.01
        );
      });
    })();



    const getProfitValue = (row: TableRow) => {
      return topBottomHasCm2Data
        ? num((row as any).cm2_profit ?? (row as any).cm2_profit_total)
        : num((row as any).profit);
    };

    const getPerUnitValue = (row: TableRow) => {
      const units = num(
        (row as any).net_units_sold ?? (row as any).total_quantity
      );

      return units > 0 ? getProfitValue(row) / units : 0;
    };

    const sortByProfitDesc = [...withoutTotal].sort(
      (a, b) => getProfitValue(b) - getProfitValue(a)
    );

    const sortByProfitAsc = [...withoutTotal].sort(
      (a, b) => getProfitValue(a) - getProfitValue(b)
    );

    const top5 = sortByProfitDesc.slice(0, 5);
    const bottom5 = sortByProfitAsc.slice(0, 5);

    const mapRows = (arr: TableRow[]) =>
      arr.map((item) => {
        const profitValue = getProfitValue(item);
        const perUnitValue = getPerUnitValue(item);

        const skuValue =
          (item as any).sku ??
          (item as any).SKU ??
          (item as any).Sku ??
          (item as any).seller_sku ??
          (item as any).sellerSku ??
          (item as any).asin ??
          (item as any).ASIN;

        return {
          product_name: String((item as any).product_name ?? ""),
          sku: String(skuValue ?? ""),

          profit: profitValue.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          }),

          profitMix: num((item as any).profit_mix).toFixed(2),
          salesMix: num((item as any).sales_mix).toFixed(2),

          per_unit: perUnitValue.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          }),
        };
      });

    const totalsFor = (arr: TableRow[]) => {
      const totalProfit = arr.reduce((s, r) => s + getProfitValue(r), 0);

      const totalProfitMix = arr.reduce(
        (s, r) => s + num((r as any).profit_mix),
        0
      );

      const totalSalesMix = arr.reduce(
        (s, r) => s + num((r as any).sales_mix),
        0
      );

      const totalNetUnits = arr.reduce(
        (s, r) =>
          s + num((r as any).net_units_sold ?? (r as any).total_quantity),
        0
      );

      const avgPerUnit = totalNetUnits > 0 ? totalProfit / totalNetUnits : 0;

      return {
        profit: totalProfit.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }),
        profitMix: totalProfitMix.toFixed(2),
        salesMix: totalSalesMix.toFixed(2),
        avg_per_unit: avgPerUnit.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }),
      };
    };

    return {
      topData: {
        rows: mapRows(top5),
        totals: totalsFor(top5),
      },
      bottomData: {
        rows: mapRows(bottom5),
        totals: totalsFor(bottom5),
      },
      hasCm2Data: topBottomHasCm2Data,
    };
  };

  const {
    topData,
    bottomData,
    hasCm2Data: topBottomHasCm2Data,
  } = useMemo(
    () => computeTopBottom5(displaySkuRows),
    [displaySkuRows, range]
  );

  useEffect(() => {
    if (!topBottomHasCm2Data) {
      setCmPieTab("cm1");
    }
  }, [topBottomHasCm2Data]);

  const defaultTopProductName = useMemo(() => {
    const rows = Array.isArray(topData?.rows) ? topData.rows : [];
    const first = rows[0];

    return String(first?.product_name || "").trim();
  }, [topData]);

  useEffect(() => {
    setShowNoDataOverlay(false);
    setFocusedChart(null);
    setChartExportApi(null);
    setSkuExportPayload(null);
    setExpenseBreakdownPieBase64(null);
    setProductWiseCm1PieBase64(null);
    setProductWiseCm2PieBase64(null);
    setPerformanceTrendBase64(null);
    setTrendExportApi(null);

    if (isDemoMode) {
      setPerformanceTrend(DEMO_PERFORMANCE_TREND);
      setSkuRows(DEMO_SKU_ROWS);
      return;
    }

    setPerformanceTrend(null);
    setSkuRows([]);
  }, [range, selectedMonth, selectedQuarter, selectedYear, isDemoMode]);

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") setFocusedChart(null);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);


  useEffect(() => {
    if (!showNoDataOverlay) return;

    const updateBounds = () => {
      if (!layoutRef.current) return;
      const rect = layoutRef.current.getBoundingClientRect();
      setOverlayBounds({
        left: rect.left,
        width: rect.width,
      });
    };

    updateBounds();
    window.addEventListener("resize", updateBounds);
    return () => window.removeEventListener("resize", updateBounds);
  }, [showNoDataOverlay]);

  const yearOptions = useMemo(
    () => [new Date().getFullYear(), new Date().getFullYear() - 1].map(String),
    []
  );

  const zeroData: Summary = {
    unit_sold: 0,
    total_sales: 0,
    asp: 0,
    gross_sales: 0,
    total_product_sales: 0,
    total_expense: 0,
    cm2_profit: 0,
    cm2_profit_total: 0,
    cm2_margins: 0,
    cm2_profit_percentage: 0,
    promotional_rebates: 0,
    promotional_rebates_percentage: 0,
    total_cous: 0,
    otherwplatform: 0,
    advertising_total: 0,
    total_amazon_fee: 0,
  };

  const zeroComparisons: SummaryComparisons = {
    lastMonth: zeroData,
    lastQuarter: zeroData,
    lastYear: zeroData,
  };

  const displayData: Summary = isDemoMode
    ? DEMO_SUMMARY
    : uploadsData?.summary ?? zeroData;

  const displayComparisons: SummaryComparisons = isDemoMode
    ? DEMO_SUMMARY_COMPARISONS
    : uploadsData?.summaryComparisons ?? zeroComparisons;


  const handleRangeChange = (v: "monthly" | "quarterly" | "yearly") => {
    // ✅ Important: if user is already on this range, don't reset filters.
    // This fixes yearly 2026 -> yearly 2025 getting forced back to 2026.
    if (v === range) return;

    setRange(v);

    if (isDemoMode) {
      setSelectedMonth("");
      setSelectedQuarter("");
      setSelectedYear(String(new Date().getFullYear()));
      setUploadsData(DEMO_UPLOAD_HISTORY);
      setAiPanel(DEMO_AI_PANEL);
      setPerformanceTrend(DEMO_PERFORMANCE_TREND);
      setBargraphUploads(DEMO_UPLOADS);
      setGraphPageUploads(DEMO_UPLOADS);
      setSkuRows(DEMO_SKU_ROWS);
      setTargetSummary(DEMO_TARGET_SUMMARY);
      return;
    }

    if (v === "yearly") {
      setSelectedMonth("");
      setSelectedQuarter("");

      // ✅ Only set a default if year is empty.
      // Do not overwrite user's selected year.
      setSelectedYear((prev) => prev || computeDefaultYearlyYear());
    }

    if (v === "monthly") {
      const defaultMonthly = computeDefaultMonthlyPeriod();

      setSelectedMonth(defaultMonthly.month.toLowerCase());
      setSelectedQuarter("");
      setSelectedYear(defaultMonthly.year);
    }

    if (v === "quarterly") {
      const defaultQuarterly = computeDefaultQuarterlyPeriod();

      setSelectedMonth("");
      setSelectedQuarter(defaultQuarterly.quarter);
      setSelectedYear(defaultQuarterly.year);
    }

    setUploadsData(null);
    setBargraphUploads([]);
  };

  const getSkuTotalRow = (rows: any[] = []) =>
    Array.isArray(rows)
      ? rows.find((r) => {
        const sku = String(r?.sku || "").trim().toLowerCase();
        const name = String(r?.product_name || "").trim().toLowerCase();
        return sku === "total" || name === "total";
      }) ?? rows[rows.length - 1] ?? null
      : null;


  const buildMonthlySkuUrl = (
    monthVal: string,
    yearVal: string,
    countryVal: string
  ) => {
    const isGlobal = countryVal.toLowerCase() === "global";

    const url = new URL(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/skutableprofit`
    );

    url.searchParams.set("country", countryVal);
    url.searchParams.set("month", monthVal.toLowerCase());
    url.searchParams.set("year", String(yearVal));

    if (isGlobal && homeCurrency) {
      url.searchParams.set("homeCurrency", homeCurrency);
    }

    return url;
  };

  const getPreviousMonthYear = (monthVal: string, yearVal: string) => {
    const idx = monthIndexMap[monthVal.toLowerCase()];
    const d = new Date(Number(yearVal), idx - 1, 1);

    return {
      prevMonth: d.toLocaleString("en-US", { month: "long" }).toLowerCase(),
      prevYear: String(d.getFullYear()),
    };
  };

  const mapSkuTotalToSummary = (row: any, hasCm2Columns = true): Summary => {
    const netSales = toNum(row?.net_sales);
    const advertisingTotal = Math.abs(getAdvertisingBaseValue(row, hasCm2Columns));
    const advertisingTotalFinal = Math.abs(getAdvertisingTotalValue(row, hasCm2Columns));
    const cm2ProfitTotal = getCm2ProfitValue(row);
    const cm2Margins = getCm2MarginValue(row, netSales, cm2ProfitTotal);
    const cm2ProfitPer = getCm2PerValue(row, netSales, cm2ProfitTotal);

    const promotionalRebates = toNum(row?.promotional_rebates);
    const promotionalRebatesPercentage =
      netSales !== 0
        ? (promotionalRebates / netSales) * 100
        : toNum(row?.promotional_rebates_percentage);

    return {
      unit_sold: toNum(row?.total_quantity),
      total_sales: netSales,
      asp: toNum(row?.asp),
      gross_sales: toNum(row?.gross_sales),
      total_product_sales: toNum(row?.gross_sales),

      total_expense:
        toNum(row?.amazon_fee) +
        toNum(row?.cost_of_unit_sold) +
        Math.abs(advertisingTotalFinal),

      cm2_profit: cm2ProfitTotal,
      cm2_profit_total: cm2ProfitTotal,

      cm2_margins: cm2Margins,
      cm2_profit_percentage: cm2Margins,
      cm2_profit_per: cm2ProfitPer,
      promotional_rebates: promotionalRebates,
      promotional_rebates_percentage: promotionalRebatesPercentage,

      total_cous: toNum(row?.cost_of_unit_sold),

      otherwplatform: Math.abs(toNum(row?.platform_fee)),

      advertising_total: advertisingTotal,
      advertising_total_final: advertisingTotalFinal,
      advertising_fees: toNum(row?.advertising_fees),
      ads_spend: toNum(row?.ads_spend),
      total_ads: toNum(row?.total_ads),

      total_amazon_fee: toNum(row?.amazon_fee),

      tacos: calculateTacos(netSales, advertisingTotalFinal),
    };
  };

  const mapSkuTotalToUploadRow = (
    row: any,
    rangeType: RangeType,
    monthVal: string,
    quarterVal: string,
    yearVal: string,
    country: string,
    hasCm2Columns = true
  ): UploadRow => {
    const netSales = toNum(row?.net_sales);
    const advertisingTotal = Math.abs(getAdvertisingBaseValue(row, hasCm2Columns));
    const advertisingTotalFinal = Math.abs(getAdvertisingTotalValue(row, hasCm2Columns));

    const cm1Profit = toNum(row?.profit);

    const cm2ProfitTotal = getCm2ProfitValue(row);
    const cm2Margins = getCm2MarginValue(row, netSales, cm2ProfitTotal);
    const cm2ProfitPer = getCm2PerValue(row, netSales, cm2ProfitTotal);

    const promotionalRebates = toNum(row?.promotional_rebates);
    const promotionalRebatesPercentage =
      netSales !== 0
        ? (promotionalRebates / netSales) * 100
        : toNum(row?.promotional_rebates_percentage);

    return {
      country,
      month:
        rangeType === "monthly"
          ? monthVal
          : rangeType === "quarterly"
            ? quarterVal
            : "ALL",
      year: yearVal,

      total_sales: netSales,
      total_amazon_fee: toNum(row?.amazon_fee),
      total_cous: toNum(row?.cost_of_unit_sold),

      advertising_total: advertisingTotal,
      advertising_total_final: advertisingTotalFinal,
      advertising_fees: toNum(row?.advertising_fees),
      ads_spend: toNum(row?.ads_spend),
      total_ads: toNum(row?.total_ads),

      otherwplatform: toNum(row?.platform_fee),

      taxncredit: toNum(row?.tex_and_credits),

      // CM1 Profit
      profit: cm1Profit,
      total_profit: cm1Profit,

      // CM2 Profit
      cm2_profit: cm2ProfitTotal,
      cm2_profit_total: cm2ProfitTotal,

      cm2_margins: cm2Margins,
      cm2_profit_percentage: cm2Margins,
      cm2_profit_per: cm2ProfitPer,
      promotional_rebates: promotionalRebates,
      promotional_rebates_percentage: promotionalRebatesPercentage,

      tacos: calculateTacos(netSales, advertisingTotalFinal),
    };
  };

  const buildUploadHistoryFromSkuApi = (
    data: any,
    rangeType: RangeType,
    monthVal: string,
    quarterVal: string,
    yearVal: string,
    country: string
  ): UploadHistoryResponse => {
    const currentTotal = getSkuTotalRow(data?.current_data);
    const previousTotal = getSkuTotalRow(data?.previous_data);
    const currentHasCm2 = hasSkuCm2Data(data?.current_data ?? []);
    const previousHasCm2 = hasSkuCm2Data(data?.previous_data ?? []);

    return {
      summary: mapSkuTotalToSummary(currentTotal, currentHasCm2),
      summaryComparisons: {
        ...(rangeType === "monthly" ? { lastMonth: mapSkuTotalToSummary(previousTotal, previousHasCm2) } : {}),
        ...(rangeType === "quarterly" ? { lastQuarter: mapSkuTotalToSummary(previousTotal, previousHasCm2) } : {}),
        ...(rangeType === "yearly" ? { lastYear: mapSkuTotalToSummary(previousTotal, previousHasCm2) } : {}),
      },
      current_data: data?.current_data ?? [],
      previous_data: data?.previous_data ?? [],
      current_table_name: data?.current_table_name,
      previous_table_name: data?.previous_table_name,
    };
  };

  const fetchUploadHistory = async (
    rangeType: RangeType,
    monthVal: string,
    quarterVal: string,
    yearVal: string,
    country: string
  ) => {
    if (isDemoMode) {
      setLoading(false);
      setSkuRowsLoading(false);
      setBargraphLoading(false);
      setSkuRowsError(null);
      setSkuNoDataFound(false);

      setUploadsData(DEMO_UPLOAD_HISTORY);
      setSkuRows(DEMO_SKU_ROWS);
      setBargraphUploads(DEMO_UPLOADS);
      return;
    }

    if (!rangeType || !yearVal) return;

    // Point 3: every request gets its own id
    const requestId = ++uploadHistoryRequestIdRef.current;

    // Point 4: start all loaders together
    setLoading(true);
    setSkuRowsLoading(true);
    setBargraphLoading(true);
    setSkuRowsError(null);
    setSkuNoDataFound(false);

    try {
      const token =
        typeof window !== "undefined"
          ? localStorage.getItem("jwtToken")
          : null;

      const url = new URL(
        buildParentSkuUrl(rangeType, monthVal, quarterVal, yearVal, country)
      );

      const res = await fetch(url.toString(), {
        method: "GET",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        cache: "no-store",
      });

      // If another request started after this one, ignore this response
      if (requestId !== uploadHistoryRequestIdRef.current) return;

      if (!res.ok) {
        const err = await res.json().catch(() => ({}));

        if (requestId !== uploadHistoryRequestIdRef.current) return;

        console.error(`API Error: ${err?.error ?? res.statusText}`);

        setSkuRows([]);
        setSkuNoDataFound(true);
        setSkuRowsError(null);

        setUploadsData({
          summary: zeroData,
          summaryComparisons: zeroComparisons,
        });

        setBargraphUploads([]);
        return;
      }

      const raw = await res.json();

      if (requestId !== uploadHistoryRequestIdRef.current) return;

      let finalRaw = raw;

      if (
        rangeType === "monthly" &&
        country.toLowerCase() === "global" &&
        (!Array.isArray(raw?.previous_data) || raw.previous_data.length === 0)
      ) {
        const { prevMonth, prevYear } = getPreviousMonthYear(monthVal, yearVal);

        const prevUrl = buildMonthlySkuUrl(prevMonth, prevYear, country);

        const prevRes = await fetch(prevUrl.toString(), {
          method: "GET",
          headers: token ? { Authorization: `Bearer ${token}` } : {},
          cache: "no-store",
        });

        if (requestId !== uploadHistoryRequestIdRef.current) return;

        if (prevRes.ok) {
          const prevRaw = await prevRes.json();

          if (requestId !== uploadHistoryRequestIdRef.current) return;

          finalRaw = {
            ...raw,
            previous_data: prevRaw?.current_data ?? prevRaw?.data ?? [],
            previous_table_name: prevRaw?.current_table_name,
          };
        }
      }

      const data = buildUploadHistoryFromSkuApi(
        finalRaw,
        rangeType,
        monthVal,
        quarterVal,
        yearVal,
        country
      );

      const normalizedCurrentRows = normalizeRowsForParent(
        finalRaw?.current_data ?? finalRaw?.data ?? []
      );

      const normalizedPreviousRows = normalizeRowsForParent(
        finalRaw?.previous_data ?? []
      );

      const rowsWithNetSalesDelta = attachNetSalesDeltaToRows(
        normalizedCurrentRows,
        normalizedPreviousRows
      );

      if (requestId !== uploadHistoryRequestIdRef.current) return;

      if (!rowsWithNetSalesDelta.length) {
        setSkuRows([]);
        setSkuNoDataFound(true);
        setSkuRowsError(null);

        setUploadsData({
          summary: zeroData,
          summaryComparisons: data?.summaryComparisons ?? zeroComparisons,
          current_data: [],
          previous_data: finalRaw?.previous_data ?? [],
        });

        setBargraphUploads([]);
        return;
      }

      setUploadsData(data);
      setSkuRows(rowsWithNetSalesDelta);
      setSkuNoDataFound(false);
      setSkuRowsError(null);

      setBargraphUploads(
        buildUploadRowFromSkuRows(rowsWithNetSalesDelta, {
          rangeType,
          monthVal,
          quarterVal,
          yearVal,
          countryVal: country,
        })
      );

      if (data?.summary) {
        if (rangeType === "monthly" && yearVal && monthVal) {
          markFetched(yearVal, monthVal);
        }

        if (rangeType === "quarterly" && yearVal) {
          markFetched(yearVal);
        }

        if (rangeType === "yearly" && yearVal) {
          markFetched(yearVal);
        }
      }
    } catch (error: any) {
      if (requestId !== uploadHistoryRequestIdRef.current) return;

      console.error("Error fetching data: ", error);

      setSkuRows([]);
      setSkuNoDataFound(true);
      setSkuRowsError(error?.message || "Error fetching SKU data");
      setUploadsData(null);
      setBargraphUploads([]);
    } finally {
      // Only the newest request is allowed to stop loaders
      if (requestId === uploadHistoryRequestIdRef.current) {
        setLoading(false);
        setSkuRowsLoading(false);
        setBargraphLoading(false);
      }
    }
  };

  const formatMoneyValue = (value: any, currency = "$") => {
    const n = toNum(value);
    const sign = n < 0 ? "-" : "";
    return `${sign}${currency}${Math.abs(n).toLocaleString(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: 2,
    })}`;
  };

  const formatNumberValue = (value: any) => {
    const n = toNum(value);
    return n.toLocaleString(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    });
  };

  const formatDelta = (deltaPct: any) => {
    if (deltaPct === null || deltaPct === undefined || Number.isNaN(Number(deltaPct))) {
      return "";
    }

    const n = Number(deltaPct);
    const sign = n >= 0 ? "+" : "";
    return ` (${sign}${n.toFixed(2)}%)`;
  };

  const getMetricDelta = (momRow: any, key: string) => {
    return momRow?.[key]?.delta_pct;
  };

  const buildGlobalProductInsightLines = (
    data: any,
    range: RangeType = "monthly"
  ): string[] => {
    const products = data?.global_ai?.product_journey_comparison ?? [];
    const skuCurrent = data?.metrics?.sku_current ?? {};
    const skuMom = data?.metrics?.sku_mom ?? {};

    const lines: string[] = [];

    products.forEach((product: any) => {
      const productName = product?.product_name || "Unknown Product";

      const currentRow =
        skuCurrent?.[productName] ||
        Object.values(skuCurrent || {}).find(
          (r: any) =>
            String(r?.product_name || "").trim().toLowerCase() ===
            String(productName).trim().toLowerCase()
        ) ||
        {};

      const momRow =
        skuMom?.[productName] ||
        Object.values(skuMom || {}).find(
          (r: any) =>
            String(r?.product_name || "").trim().toLowerCase() ===
            String(productName).trim().toLowerCase()
        ) ||
        {};

      lines.push(productName);

      lines.push(
        `ASP: ${formatMoneyValue(currentRow?.asp)}${formatDelta(
          getMetricDelta(momRow, "asp")
        )}`
      );

      lines.push(
        `Units: ${formatNumberValue(currentRow?.total_quantity)}${formatDelta(
          getMetricDelta(momRow, "total_quantity")
        )}`
      );

      lines.push(
        `Net sales: ${formatMoneyValue(currentRow?.net_sales)}${formatDelta(
          getMetricDelta(momRow, "net_sales")
        )}`
      );

      const currentCm1Profit = currentRow?.profit;
const currentCm2Profit = currentRow?.cm2_profit;

const previousCm1Profit =
  getMetricPrevious(momRow?.profit);

const previousCm2Profit =
  getMetricPrevious(momRow?.cm2_profit);

const currentHasRealCm2 =
  !areValuesSame(currentCm1Profit, currentCm2Profit);

const previousHasRealCm2 =
  previousCm2Profit !== null &&
  previousCm2Profit !== undefined &&
  !areValuesSame(previousCm1Profit, previousCm2Profit);

const useCm1Profit =
  !isMonthlyRange(range) ||
  !currentHasRealCm2;

const profitLabel = useCm1Profit
  ? "CM1 profit"
  : "CM2 profit";

const profitValue = useCm1Profit
  ? currentRow?.profit
  : currentRow?.cm2_profit;

const profitDelta = useCm1Profit
  ? getMetricDelta(momRow, "profit")
  : previousHasRealCm2
    ? getMetricDelta(momRow, "cm2_profit")
    : null;

const profitPerUnitLabel = useCm1Profit
  ? "CM1 profit per unit"
  : "CM2 profit per unit";

const profitPerUnitValue = useCm1Profit
  ? currentRow?.unit_wise_profitability
  : currentRow?.cm2_profit_per_unit ??
    currentRow?.cm2_profit_per;

const profitPerUnitDelta = useCm1Profit
  ? getMetricDelta(momRow, "unit_wise_profitability")
  : previousHasRealCm2
    ? getMetricDelta(momRow, "cm2_profit_per_unit") ??
      getMetricDelta(momRow, "cm2_profit_per")
    : null;

lines.push(
  `${profitLabel}: ${formatMoneyValue(profitValue)}${
    profitDelta === null || profitDelta === undefined
      ? ""
      : formatDelta(profitDelta)
  }`
);

lines.push(
  `${profitPerUnitLabel}: ${formatMoneyValue(profitPerUnitValue)}${
    profitPerUnitDelta === null ||
    profitPerUnitDelta === undefined
      ? ""
      : formatDelta(profitPerUnitDelta)
  }`
);

      lines.push(
        `Stock Cover: ${formatCoverageValue(
          currentRow?.selected_period_coverage_ratio
        )}`
      );

      lines.push(
        `Current Inventory: ${formatInventoryUnitsValue(
          currentRow?.current_inventory
        )}`
      );

      if (isMonthlyRange(range)) {
        lines.push(
          `Productwise ads spend: ${formatMetricObjectWithDelta(
            momRow?.productwise_ads_spend ?? currentRow?.productwise_ads_spend,
            "money",
            currencySymbol
          )}`
        );
      }

      lines.push("Product Journey");

      (product?.journey_comparison ?? []).forEach((journeyLine: string) => {
        lines.push(`- ${journeyLine}`);
      });

      const countryActions = product?.country_actions ?? {};

      const usRecommendation = countryActions?.us?.recommendation;
      const ukRecommendation = countryActions?.uk?.recommendation;

      if (ukRecommendation) {
        lines.push(`Recommendation: UK: ${ukRecommendation}`);
      }

      if (usRecommendation) {
        lines.push(`Recommendation: US: ${usRecommendation}`);
      }

      const usInventory = countryActions?.us?.inventory_recommendation;
      const ukInventory = countryActions?.uk?.inventory_recommendation;

      if (ukInventory) {
        lines.push(`Inventory Action: UK: ${ukInventory}`);
      }

      if (usInventory) {
        lines.push(`Inventory Action: US: ${usInventory}`);
      }
    });

    return lines;
  };

  const buildGlobalRecommendationsMap = (data: any): RecommendationsMap => {
    const products = data?.global_ai?.product_journey_comparison ?? [];

    const map: RecommendationsMap = {};

    products.forEach((product: any) => {
      const productName = product?.product_name;
      if (!productName) return;

      const actions = product?.country_actions ?? {};
      const us = actions?.us ?? {};
      const uk = actions?.uk ?? {};

      map[productName] = {
        journey_summary: product?.journey_comparison ?? [],
        recommendation: [
          uk?.recommendation ? `UK: ${uk.recommendation}` : "",
          us?.recommendation ? `US: ${us.recommendation}` : "",
        ]
          .filter(Boolean)
          .join("\n"),
        inventory_recommendation: [
          uk?.inventory_recommendation ? `UK: ${uk.inventory_recommendation}` : "",
          us?.inventory_recommendation ? `US: ${us.inventory_recommendation}` : "",
        ]
          .filter(Boolean)
          .join("\n"),

        ads_recommendation: [
          uk?.ads_recommendation ? `UK: ${uk.ads_recommendation}` : "",
          us?.ads_recommendation ? `US: ${us.ads_recommendation}` : "",
        ]
          .filter(Boolean)
          .join("\n"),
      };
    });

    return map;
  };

  const buildGlobalInventoryLines = (data: any): string[] => {
    const alerts = data?.inventory_alerts ?? {};
    const lines: string[] = [];

    const uk = alerts?.uk;
    const us = alerts?.us;

    if (uk) {
      lines.push(
        `UK ageing inventory: ${uk?.ageing_inventory?.total_units ?? 0} units across ${uk?.ageing_inventory?.total_skus ?? 0
        } SKUs`
      );

      lines.push(
        `UK estimated storage cost: ${formatMoneyValue(
          uk?.estimated_storage_cost?.value
        )}`
      );

      lines.push(
        `UK unfulfillable inventory: ${uk?.unfulfillable?.units ?? 0} units · ${uk?.unfulfillable?.percentage ?? 0
        }%`
      );
    }

    if (us) {
      lines.push(
        `US ageing inventory: ${us?.ageing_inventory?.total_units ?? 0} units across ${us?.ageing_inventory?.total_skus ?? 0
        } SKUs`
      );

      lines.push(
        `US estimated storage cost: ${formatMoneyValue(
          us?.estimated_storage_cost?.value
        )}`
      );

      lines.push(
        `US unfulfillable inventory: ${us?.unfulfillable?.units ?? 0} units · ${us?.unfulfillable?.percentage ?? 0
        }%`
      );
    }

    return lines;
  };

  const mapGlobalAiResponseToPanel = (
    data: any,
    rangeType: RangeType = "monthly"
  ): AiPanelData => {
    const globalAi = data?.global_ai ?? {};

    const comparison = data?.comparison;
    const periodLabel = comparison?.period_label || "Selected Period";

    const getPreviousComparisonLabel = () => {
      if (comparison?.previous_period_label) return comparison.previous_period_label;
      if (comparison?.previous_label) return comparison.previous_label;

      const period = String(comparison?.period || "").toLowerCase();
      const currentPeriodLabel = String(comparison?.period_label || "");

      // Monthly: April 2026 -> March 2026
      const monthYearMatch = currentPeriodLabel.match(/^([A-Za-z]+)\s+(\d{4})$/);

      if (period === "monthly" && monthYearMatch) {
        const monthName = monthYearMatch[1].toLowerCase();
        const yearNum = Number(monthYearMatch[2]);
        const monthIndex = monthIndexMap[monthName];

        if (typeof monthIndex === "number") {
          const previousDate = new Date(yearNum, monthIndex - 1, 1);

          return previousDate.toLocaleString("en-US", {
            month: "long",
            year: "numeric",
          });
        }

        return "Previous Month";
      }

      // Quarterly: Q2 2026 -> Q1 2026
      const quarterYearMatch = currentPeriodLabel.match(/^(Q[1-4])\s+(\d{4})$/i);

      if (period === "quarterly" && quarterYearMatch) {
        const currentQuarter = quarterYearMatch[1].toUpperCase();
        const yearNum = Number(quarterYearMatch[2]);

        const quarterOrder = ["Q1", "Q2", "Q3", "Q4"];
        const currentIndex = quarterOrder.indexOf(currentQuarter);

        if (currentIndex !== -1) {
          const previousIndex = currentIndex === 0 ? 3 : currentIndex - 1;
          const previousYear = currentIndex === 0 ? yearNum - 1 : yearNum;

          return `${quarterOrder[previousIndex]} ${previousYear}`;
        }

        return "Previous Quarter";
      }

      // Yearly: 2026 -> 2025
      if (period === "yearly" && /^\d{4}$/.test(currentPeriodLabel)) {
        return String(Number(currentPeriodLabel) - 1);
      }

      return "Previous Period";
    };

    const previousLabel = getPreviousComparisonLabel();


    const summaryBullets = [
      `Global Business Summary (${periodLabel} vs ${previousLabel})`,
      globalAi?.global_summary || data?.summary || "",
      ...(globalAi?.uk_vs_us_comparison ?? []),
    ].filter(Boolean);

    return {
      summaryBullets,
      skuInsightsBullets: [
        ...buildGlobalProductInsightLines(data, rangeType),
        ...buildOtherSkusInsightLines(data, currencySymbol, "global", rangeType),
      ],
      recommendationBullets: [],
      inventoryBullets: buildGlobalInventoryLines(data),
      recommendationsMap: buildGlobalRecommendationsMap(data),
      objective: {
        country: "global",
        growth_intent:
          data?.objectives?.uk?.growth_intent ||
          data?.objectives?.us?.growth_intent ||
          "balanced",
        profit_priority:
          data?.objectives?.uk?.profit_priority ||
          data?.objectives?.us?.profit_priority ||
          "protect_growth",
        inventory_clearance_priority:
          Boolean(data?.objectives?.uk?.inventory_clearance_priority) ||
          Boolean(data?.objectives?.us?.inventory_clearance_priority),
        time_horizon:
          data?.objectives?.uk?.time_horizon ||
          data?.objectives?.us?.time_horizon ||
          "1_month",
      },
      rawSummary: data?.summary ?? globalAi?.global_summary ?? null,
      rawRecommendations: null,
      portfolioRecommendation:
        globalAi?.global_overall_recommendation ||
        data?.overall_recommendation ||
        null,

      otherSkuIncludedProducts: getOtherSkuIncludedProducts(data),
    };
  };

  const fetchAiSummary = async (rangeType: RangeType) => {
    if (isDemoMode) {
      setAiPanelLoading(false);
      setAiPanelError(null);
      setAiPanel(DEMO_AI_PANEL);
      return;
    }

    const isGlobalAiSummary = countryName.toLowerCase() === "global";

    if (!countryName) return;

    // ✅ For Global AI Insights, force only this route:
    // /summary?country=global&period=monthly&timeline=4&year=2026
    const aiCountry = isGlobalAiSummary ? "global" : countryName;

    const aiPeriod: RangeType = rangeType;

    const aiTimeline =
      rangeType === "monthly"
        ? monthNameToNumber(selectedMonth)
        : rangeType === "quarterly"
          ? selectedQuarter
          : "ALL";

    const aiYear = selectedYear;

    if (!aiPeriod || !aiYear) return;
    if (aiPeriod === "monthly" && !aiTimeline) return;
    if (aiPeriod === "quarterly" && !selectedQuarter) return;

    const requestId = ++aiRequestIdRef.current;

    setAiPanelLoading(true);
    setAiPanelError(null);
    setAiPanel(null);

    try {
      const token =
        typeof window !== "undefined"
          ? localStorage.getItem("jwtToken")
          : null;

      const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/summary`);

      url.searchParams.set("country", aiCountry);
      url.searchParams.set("period", aiPeriod);
      url.searchParams.set("timeline", String(aiTimeline));
      url.searchParams.set("year", String(aiYear));

      const res = await fetch(url.toString(), {
        method: "GET",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        cache: "no-store",
      });

      if (!res.ok) {
        if (requestId !== aiRequestIdRef.current) return;
        setAiPanel(null);
        setAiPanelError("Failed to fetch AI summary");
        return;
      }

      const data: any = await res.json();

      if (requestId !== aiRequestIdRef.current) return;

      // ✅ Global AI response has a different backend shape.
      // Map it into the existing AiPanelData UI format.
      if (
        data?.scope === "global" ||
        data?.global_ai ||
        countryName.toLowerCase() === "global"
      ) {
        setAiPanel(mapGlobalAiResponseToPanel(data, rangeType));
        return;
      }

      // Existing non-global mapping
      const sections = parseMdSections(data.summary);

      const summaryLines = sections["SUMMARY"] ?? [];
      const inventoryLines = sections["INVENTORY"] ?? [];
      const productLines = [
        ...(sections["PRODUCT INSIGHTS"] ?? []),
        ...(sections["ALL SKU INDIVIDUAL INSIGHTS"] ?? []),
      ];

      const { recommendationBullets, inventoryBullets, recommendationsMap } =
        extractRecoAndInventoryBullets(data.recommendations as any);

      let remainingSkusRecommendation: string | undefined;

      if (
        data.recommendations &&
        typeof data.recommendations === "object" &&
        "remaining_skus_recommendation" in data.recommendations
      ) {
        remainingSkusRecommendation =
          (data.recommendations as any).remaining_skus_recommendation;
      }

      setAiPanel({
        summaryBullets: summaryLines,
        skuInsightsBullets: productLines,
        recommendationBullets,
        inventoryBullets: inventoryLines,
        recommendationsMap,
        objective: data.objective,
        rawSummary: data.summary ?? null,
        rawRecommendations:
          typeof data.recommendations === "string" ? data.recommendations : null,
        remainingSkusRecommendation,
        portfolioRecommendation: data.portfolio_recommendation ?? null,

        otherSkuIncludedProducts: getOtherSkuIncludedProducts(data),
      });
    } catch (e: any) {
      if (requestId !== aiRequestIdRef.current) return;
      setAiPanel(null);
      setAiPanelError(e?.message || "Failed to fetch AI summary");
    } finally {
      if (requestId === aiRequestIdRef.current) {
        setAiPanelLoading(false);
      }
    }
  };

  useEffect(() => {
    if (isDemoMode) {
      setTargetSummary(DEMO_TARGET_SUMMARY);
      return;
    }

    const ready =
      (range === "monthly" && !!selectedMonth && !!selectedYear) ||
      (range === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (range === "yearly" && !!selectedYear);

    if (!ready) {
      setTargetSummary(null);
      return;
    }

    fetchTargetSummary();
  }, [range, selectedMonth, selectedQuarter, selectedYear, initialCountryName, isDemoMode]);


  const fetchPerformanceTrendFromHistory = async (rangeType: RangeType) => {
    if (isDemoMode) {
      setPerformanceTrend(DEMO_PERFORMANCE_TREND);
      setPerformanceTrendMetric("net_sales");
      return;
    }

    if (!countryName || !rangeType || !selectedYear) return;

    const timeline =
      rangeType === "monthly"
        ? monthNameToNumber(selectedMonth)
        : rangeType === "quarterly"
          ? selectedQuarter
          : "ALL";

    if (rangeType === "monthly" && !timeline) return;
    if (rangeType === "quarterly" && !selectedQuarter) return;

    try {
      const token =
        typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

      const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_history`);

      url.searchParams.set("country", countryName);
      url.searchParams.set("period", rangeType);
      url.searchParams.set("timeline", String(timeline));
      url.searchParams.set("year", String(selectedYear));
      url.searchParams.set("metric", performanceTrendMetric);

      if (countryName.toLowerCase() === "global" && homeCurrency) {
        url.searchParams.set("homeCurrency", homeCurrency);
      }

      const res = await fetch(url.toString(), {
        method: "GET",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        cache: "no-store",
      });

      if (!res.ok) {
        setPerformanceTrend(null);
        return;
      }

      const data = await res.json();

      setPerformanceTrend(data.performance_trend ?? null);
      setPerformanceTrendMetric(data.performance_trend_metric ?? "net_sales");
    } catch (e) {
      setPerformanceTrend(null);
    }
  };

  const quarterToMonths: Record<Quarter, string[]> = {
    Q1: ["January", "February", "March"],
    Q2: ["April", "May", "June"],
    Q3: ["July", "August", "September"],
    Q4: ["October", "November", "December"],
  };

  const allMonths = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
  ];

  const fetchSingleMonthTargetSummary = async (monthName: string) => {
    const token =
      typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

    const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/target-summary`);
    url.searchParams.set("month", monthName);
    url.searchParams.set("year", selectedYear);
    url.searchParams.set("country", initialCountryName.toLowerCase());

    if (initialCountryName.toLowerCase() === "global" && homeCurrency) {
      url.searchParams.set("currency", homeCurrency.toLowerCase());
    }

    const res = await fetch(url.toString(), {
      method: "GET",
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      cache: "no-store",
    });

    if (!res.ok) {
      return {
        target_sales: 0,
        shortfall_total: 0,
        cashflow_total: 0,
      };
    }

    const responseJson = await res.json();
    const payload = responseJson?.data ?? {};

    return {
      target_sales: Number(payload?.target_sales ?? 0),
      shortfall_total: Number(payload?.shortfall_total ?? 0),
      cashflow_total: Number(payload?.cashflow_total ?? 0),
    };
  };

  const fetchTargetSummary = async () => {
    if (isDemoMode) {
      setTargetSummaryLoading(false);
      setTargetSummary(DEMO_TARGET_SUMMARY);
      return;
    }

    if (!selectedYear || !initialCountryName) return;

    const ready =
      (range === "monthly" && !!selectedMonth && !!selectedYear) ||
      (range === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (range === "yearly" && !!selectedYear);

    if (!ready) {
      setTargetSummary(null);
      return;
    }

    try {
      setTargetSummaryLoading(true);

      let monthsToFetch: string[] = [];

      if (range === "monthly") {
        monthsToFetch = [
          selectedMonth.charAt(0).toUpperCase() + selectedMonth.slice(1).toLowerCase(),
        ];
      } else if (range === "quarterly" && selectedQuarter) {
        monthsToFetch = quarterToMonths[selectedQuarter];
      } else if (range === "yearly") {
        const currentDate = new Date();
        const selectedYearNum = Number(selectedYear);
        const currentYear = currentDate.getFullYear();
        const currentMonthIndex = currentDate.getMonth(); // 0 = Jan, 11 = Dec

        if (selectedYearNum === currentYear) {
          // Current year: fetch only Jan through current month
          monthsToFetch = allMonths.slice(0, currentMonthIndex + 1);
        } else if (selectedYearNum < currentYear) {
          // Past year: fetch all 12 months
          monthsToFetch = allMonths;
        } else {
          // Future year: fetch nothing
          monthsToFetch = [];
        }
      }

      if (!monthsToFetch.length) {
        setTargetSummary({
          target_sales: 0,
          shortfall_total: 0,
          cashflow_total: 0,
        });
        return;
      }

      const monthlyResults = await Promise.all(
        monthsToFetch.map((monthName) => fetchSingleMonthTargetSummary(monthName))
      );

      const totals = monthlyResults.reduce(
        (acc, curr) => {
          acc.target_sales += Number(curr.target_sales ?? 0);
          acc.shortfall_total += Number(curr.shortfall_total ?? 0);
          acc.cashflow_total += Number(curr.cashflow_total ?? 0);
          return acc;
        },
        {
          target_sales: 0,
          shortfall_total: 0,
          cashflow_total: 0,
        }
      );

      setTargetSummary(totals);
    } catch (error) {
      console.error("Failed to fetch target summary:", error);
      setTargetSummary(null);
    } finally {
      setTargetSummaryLoading(false);
    }
  };

  // useEffect(() => {
  //   const amazonFetch = searchParams.get("amazonFetch");
  //   const promptAmazonAds = searchParams.get("promptAmazonAds");

  //   if (amazonFetch === "success" && promptAmazonAds === "1") {
  //     setShowAmazonFetchSuccess(true);
  //   }
  // }, [searchParams]);

  // useEffect(() => {
  //   if (!userData) return; // wait until data is loaded

  //   const amazonFetch = searchParams.get("amazonFetch");
  //   const promptAmazonAds = searchParams.get("promptAmazonAds");

  //   if (
  //     amazonFetch === "success" &&
  //     promptAmazonAds === "1" &&
  //     !userData.amazon_ads_exists
  //   ) {
  //     setShowAmazonFetchSuccess(true);
  //   }
  // }, [searchParams, userData]);

  const AMAZON_ADS_PROMPT_KEY = "pendingAmazonAdsPrompt";

  const isAmazonAdsConnected = (value: unknown) => {
    if (value === true) return true;
    if (value === 1) return true;

    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      return normalized === "true" || normalized === "1" || normalized === "yes";
    }

    return false;
  };

  useEffect(() => {
    const amazonFetch = searchParams.get("amazonFetch");
    const promptAmazonAds = searchParams.get("promptAmazonAds");

    const hasPromptInUrl =
      amazonFetch === "success" && promptAmazonAds === "1";

    if (hasPromptInUrl && typeof window !== "undefined") {
      sessionStorage.setItem(AMAZON_ADS_PROMPT_KEY, "1");
    }

    const hasPromptInSession =
      typeof window !== "undefined" &&
      sessionStorage.getItem(AMAZON_ADS_PROMPT_KEY) === "1";

    if (!userData) return;

    const shouldPrompt = hasPromptInUrl || hasPromptInSession;

    if (!shouldPrompt) return;

    const adsConnected =
      isAmazonAdsConnected((userData as any).amazon_ads_exists) ||
      isAmazonAdsConnected((userData as any).amazon_ads_connected) ||
      isAmazonAdsConnected((userData as any).ads_connected) ||
      isAmazonAdsConnected((userData as any).amazonAdsConnected);

    if (!adsConnected) {
      setShowAmazonFetchSuccess(true);
    } else {
      sessionStorage.removeItem(AMAZON_ADS_PROMPT_KEY);
      clearAmazonFetchQueryParams();
    }
  }, [searchParams, userData]);

  useEffect(() => {
    console.log(
      "[Amazon Ads Prompt Debug] showAmazonFetchSuccess changed:",
      showAmazonFetchSuccess
    );
  }, [showAmazonFetchSuccess]);

  useEffect(() => {
    if (isDemoMode) {
      setTargetSummary(DEMO_TARGET_SUMMARY);
      return;
    }

    const ready =
      (range === "monthly" && !!selectedMonth && !!selectedYear) ||
      (range === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (range === "yearly" && !!selectedYear);

    if (!ready) {
      setTargetSummary(null);
      return;
    }

    fetchTargetSummary();
  }, [
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    initialCountryName,
    homeCurrency,
    isDemoMode,
  ]);

  const clearAmazonFetchQueryParams = () => {
    if (typeof window === "undefined") return;

    const url = new URL(window.location.href);

    console.log("[Amazon Ads Prompt Debug] clearing query params from:", url.href);

    url.searchParams.delete("amazonFetch");
    url.searchParams.delete("promptAmazonAds");

    const nextUrl = url.pathname + url.search + url.hash;

    console.log("[Amazon Ads Prompt Debug] router.replace to:", nextUrl);

    router.replace(nextUrl, { scroll: false });
  };

  const handleCloseAmazonFetchSuccess = () => {
    if (typeof window !== "undefined") {
      sessionStorage.removeItem(AMAZON_ADS_PROMPT_KEY);
    }

    setShowAmazonFetchSuccess(false);
    clearAmazonFetchQueryParams();
  };

  const handleOpenAmazonAdsFromSuccess = (country: "UK" | "US" | "CA") => {
    if (typeof window !== "undefined") {
      sessionStorage.removeItem(AMAZON_ADS_PROMPT_KEY);
    }

    setShowAmazonFetchSuccess(false);
    clearAmazonFetchQueryParams();
    setShowAmazonAdsConnect(true);
  };

  const onConnectOrSyncAds = async () => {
    try {
      setAdsConnecting(true);
      setAdsError(null);
    } catch (err) {
      console.error(err);
      setAdsError("Amazon Ads action failed");
    } finally {
      setAdsConnecting(false);
    }
  };

  const handleMonthChange = (v: string) => {
    setSelectedMonth(v);

    if (isDemoMode) {
      setUploadsData(DEMO_UPLOAD_HISTORY);
      return;
    }

    setUploadsData(null);
    setBargraphUploads([]);
  };

  const handleQuarterChange = (v: string) => {
    const q = isQuarter(v) ? v : "";
    setSelectedQuarter(q);

    if (isDemoMode) {
      setUploadsData(DEMO_UPLOAD_HISTORY);
      return;
    }

    setUploadsData(null);
    setBargraphUploads([]);
  };

  const handleYearChange = (v: string) => {
    setSelectedYear(String(v));

    if (isDemoMode) {
      setUploadsData(DEMO_UPLOAD_HISTORY);
      return;
    }

    setUploadsData(null);
    setBargraphUploads([]);
  };

  const didApplyLandingDefault = useRef(false);

  useEffect(() => {
    if (isDemoMode) {
      setRange("yearly");
      setSelectedMonth("");
      setSelectedQuarter("");
      setSelectedYear(String(new Date().getFullYear()));
      setUploadsData(DEMO_UPLOAD_HISTORY);
      setAiPanel(DEMO_AI_PANEL);
      setPerformanceTrend(DEMO_PERFORMANCE_TREND);
      setBargraphUploads(DEMO_UPLOADS);
      setGraphPageUploads(DEMO_UPLOADS);
      setSkuRows(DEMO_SKU_ROWS);
      setTargetSummary(DEMO_TARGET_SUMMARY);
      setAllDropdownsSelected(true);
      return;
    }

    if (didApplyLandingDefault.current) return;
    didApplyLandingDefault.current = true;

    const routeRange = getInitialRange();

    setRange(routeRange);
    setSelectedYear(getInitialSelectedYear());

    if (routeRange === "monthly") {
      setSelectedMonth(
        (initialMonth || computeDefaultMonthlyPeriod().month).toLowerCase()
      );
      setSelectedQuarter("");
      return;
    }

    if (routeRange === "quarterly") {
      setSelectedMonth("");
      setSelectedQuarter(
        isQuarter(initialMonth) ? initialMonth : computeDefaultQuarterlyPeriod().quarter
      );
      return;
    }

    setSelectedMonth("");
    setSelectedQuarter("");
  }, [isDemoMode, initialRanged, initialMonth, initialYear]);


  const fetchCurrencyKey = isGlobalPage ? homeCurrency : "country";

  useEffect(() => {
    if (isDemoMode) {
      setAvailablePeriods(null);
      return;
    }

    if (!countryName) {
      setAvailablePeriods(null);
      return;
    }

    let ignore = false;
    setAvailablePeriods(null);

    const fetchAvailablePeriods = async () => {
      try {
        const authToken =
          token ??
          (typeof window !== "undefined"
            ? localStorage.getItem("jwtToken")
            : null);

        const url = new URL(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_history`
        );

        url.searchParams.set("country", countryName);
        url.searchParams.set("period", "monthly");

        if (countryName.toLowerCase() === "global" && homeCurrency) {
          url.searchParams.set("homeCurrency", homeCurrency);
        }

        const res = await fetch(url.toString(), {
          method: "GET",
          headers: authToken ? { Authorization: `Bearer ${authToken}` } : {},
          cache: "no-store",
        });

        if (ignore) return;

        if (!res.ok) {
          setAvailablePeriods(null);
          return;
        }

        const data = await res.json();
        const nextPeriods = buildFetchedPeriodsFromUploads(data?.uploads);

        if (ignore) return;

        setAvailablePeriods(nextPeriods);
        writeFetchedPeriods(nextPeriods);

        const latestFetchedPeriod =
          getLatestFetchedPeriodFromMap(nextPeriods);

        if (typeof window !== "undefined") {
          if (latestFetchedPeriod) {
            localStorage.setItem(
              "latestFetchedPeriod",
              JSON.stringify(latestFetchedPeriod)
            );
          } else {
            localStorage.removeItem("latestFetchedPeriod");
          }
        }
      } catch (error) {
        if (!ignore) {
          console.error("Error fetching available periods:", error);
          setAvailablePeriods(null);
        }
      }
    };

    fetchAvailablePeriods();

    return () => {
      ignore = true;
    };
  }, [countryName, fetchCurrencyKey, homeCurrency, isDemoMode, token]);

  useEffect(() => {
    if (isDemoMode) return;
    if (!countryName) return;

    const safeRange: RangeType =
      activeTab === "skuwiseProfit" && range === "monthly"
        ? "yearly"
        : range;

    if (!selectedYear) return;

    const ready =
      (safeRange === "monthly" && !!selectedMonth && !!selectedYear) ||
      (safeRange === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (safeRange === "yearly" && !!selectedYear);

    if (!ready) return;

    fetchUploadHistory(
      safeRange,
      safeRange === "monthly" ? selectedMonth : "",
      safeRange === "quarterly" ? selectedQuarter || "" : "",
      selectedYear,
      countryName
    );
  }, [
    activeTab,
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    countryName,
    fetchCurrencyKey,
    isDemoMode,
  ]);

  useEffect(() => {
    if (isDemoMode) return;

    const safeRange: RangeType =
      activeTab === "skuwiseProfit" && range === "monthly"
        ? "yearly"
        : range;

    if (!countryName || !safeRange || !selectedYear) {
      setAiPanel(null);
      return;
    }

    const ready =
      (safeRange === "monthly" && !!selectedMonth && !!selectedYear) ||
      (safeRange === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (safeRange === "yearly" && !!selectedYear);

    if (!ready) {
      setAiPanel(null);
      return;
    }

    fetchAiSummary(safeRange);

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    activeTab,
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    countryName,
    isDemoMode,
  ]);

  useEffect(() => {
    if (isDemoMode) return;

    const safeRange: RangeType =
      activeTab === "skuwiseProfit" && range === "monthly"
        ? "yearly"
        : range;

    if (!safeRange || !selectedYear) {
      setPerformanceTrend(null);
      return;
    }

    const ready =
      (safeRange === "monthly" && !!selectedMonth && !!selectedYear) ||
      (safeRange === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (safeRange === "yearly" && !!selectedYear);

    if (!ready) {
      setPerformanceTrend(null);
      return;
    }

    fetchPerformanceTrendFromHistory(safeRange);
  }, [
    activeTab,
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    countryName,
    homeCurrency,
    performanceTrendMetric,
    isDemoMode,
  ]);

  useEffect(() => {
    if (isDemoMode) return;
    if (!inventoryRawResponses) return;

    setInventoryInsightsData(
      buildInventoryInsightsFromResponses(
        inventoryRawResponses.inventory,
        inventoryRawResponses.ageSummary,
        effectiveCountryName,
        effectiveHomeCurrency,
        selectedAgeingTrendBucket,
        selectedGlobalInventoryCountry,
        range,
        selectedMonth,
        selectedYear
      )
    );
  }, [
    selectedAgeingTrendBucket,
    inventoryRawResponses,
    effectiveCountryName,
    effectiveHomeCurrency,
    isDemoMode,
    selectedGlobalInventoryCountry,
  ]);

  useEffect(() => {
    if (isDemoMode) {
      setAllDropdownsSelected(true);
      return;
    }
    if (range === "monthly") {
      setAllDropdownsSelected(!!selectedMonth && !!selectedYear);
    } else if (range === "quarterly") {
      setAllDropdownsSelected(!!selectedQuarter && !!selectedYear);
    } else if (range === "yearly") {
      setAllDropdownsSelected(!!selectedYear);
    } else {
      setAllDropdownsSelected(false);
    }
  }, [
    selectedAgeingTrendBucket,
    inventoryRawResponses,
    effectiveCountryName,
    effectiveHomeCurrency,
    isDemoMode,
    selectedGlobalInventoryCountry,
    range,
    selectedMonth,
    selectedYear,
  ]);


  useEffect(() => {
    if (typeof document === "undefined") return;

    const body = document.body;

    if (showNoDataOverlay) {
      body.style.overflow = "hidden";
    } else {
      body.style.overflow = "";
    }

    return () => {
      body.style.overflow = "";
    };
  }, [showNoDataOverlay]);

  useEffect(() => {
    if (isDemoMode) {
      setBargraphUploads(DEMO_UPLOADS);
      setBargraphUserMeta({
        company_name: "Demo Company",
        brand_name: "Demo Brand",
      });
      setBargraphLoading(false);
      return;
    }

    setBargraphLoading(false);
  }, [isDemoMode]);

  useEffect(() => {
    if (isDemoMode) {
      setGraphPageUploads(DEMO_UPLOADS);
      setGraphPageUserMeta({
        company_name: "Demo Company",
        brand_name: "Demo Brand",
      });
      setGraphPageError(null);
      setGraphPageLoading(false);
      return;
    }

    const safeRange: RangeType =
      activeTab === "skuwiseProfit" && range === "monthly"
        ? "yearly"
        : range;

    if (!safeRange || !selectedYear || !countryName) return;

    const ready =
      (safeRange === "monthly" && !!selectedMonth && !!selectedYear) ||
      (safeRange === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (safeRange === "yearly" && !!selectedYear);

    if (!ready) {
      setGraphPageUploads([]);
      return;
    }

    const fetchGraphPageUploadsFromSku = async () => {
      setGraphPageLoading(true);
      setGraphPageError(null);

      try {
        const token =
          typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        if (safeRange === "monthly") {
          const rows = await fetchMonthlySkuRowsForGraph(
            selectedMonth,
            selectedYear,
            countryName,
            token
          );

          setGraphPageUploads(rows);
          setGraphPageLoading(false);
          return;
        }

        if (safeRange === "quarterly" && selectedQuarter) {
          const months = quarterToMonths[selectedQuarter];

          const rowsNested = await Promise.all(
            months.map((m) =>
              fetchMonthlySkuRowsForGraph(
                m.toLowerCase(),
                selectedYear,
                countryName,
                token
              )
            )
          );

          setGraphPageUploads(rowsNested.flat());
          setGraphPageLoading(false);
          return;
        }

        if (safeRange === "yearly") {
          const rowsNested = await Promise.all(
            allMonths.map((m) =>
              fetchMonthlySkuRowsForGraph(
                m.toLowerCase(),
                selectedYear,
                countryName,
                token
              )
            )
          );

          setGraphPageUploads(rowsNested.flat());
          setGraphPageLoading(false);
          return;
        }
      } catch (e: any) {
        setGraphPageUploads([]);
        setGraphPageError(e?.message || "Failed to fetch graph data from SKU tables");
        setGraphPageLoading(false);
      }
    };

    fetchGraphPageUploadsFromSku();
  }, [
    activeTab,
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    countryName,
    homeCurrency,
    isDemoMode,
  ]);

  useEffect(() => {
    if (activeTab !== "skuwiseProfit") return;
    if (range !== "monthly") return;

    setRange("yearly");
    setSelectedMonth("");
    setSelectedQuarter("");
  }, [activeTab, range]);

  const fetchSingleMonthInventoryCurrent = async (
    monthName: string,
    yearValue: string,
    countryValue: string,
    signal?: AbortSignal
  ): Promise<InventoryCurrentApiResponse> => {
    const token =
      typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

    if (!token) {
      throw new Error("Missing token");
    }

    const url = new URL(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/inventory_current`
    );

    url.searchParams.set("country_key", String(countryValue).toLowerCase());
    url.searchParams.set("month_name", String(monthName).toLowerCase());
    url.searchParams.set("year", String(yearValue));

    const res = await fetch(url.toString(), {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
      },
      cache: "no-store",
      signal,
    });

    const json = await res.json().catch(() => ({}));

    if (!res.ok) {
      throw new Error(
        json?.message ||
        json?.error ||
        `Failed to fetch inventory for ${monthName} ${yearValue}`
      );
    }

    return json;
  };

  const fetchInventoryCurrentByPeriod = async (
    signal?: AbortSignal
  ): Promise<InventoryCurrentApiResponse> => {
    const token =
      typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

    if (!token) {
      throw new Error("Missing token");
    }

    const url = new URL(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/inventory_current`
    );

    url.searchParams.set("country_key", String(countryName).toLowerCase());
    url.searchParams.set("year", String(selectedYear));

    if (range === "monthly") {
      url.searchParams.set("range_type", "monthly");
      url.searchParams.set("month_name", String(selectedMonth).toLowerCase());
    }

    if (range === "quarterly") {
      url.searchParams.set("range_type", "quarter_months");
      url.searchParams.set("quarter", String(selectedQuarter));
    }

    if (range === "yearly") {
      url.searchParams.set("range_type", "yearly");
    }

    const res = await fetch(url.toString(), {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
      },
      cache: "no-store",
      signal,
    });

    const json = await res.json().catch(() => ({}));

    if (!res.ok) {
      throw new Error(
        json?.message ||
        json?.error ||
        `Failed to fetch inventory data for ${range}`
      );
    }

    return json;
  };

  const fetchSingleMonthInventoryAgeSummary = async (
    monthName: string,
    yearValue: string,
    countryValue: string,
    signal?: AbortSignal
  ): Promise<InventoryAgeSummaryApiResponse> => {
    const token =
      typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

    if (!token) {
      throw new Error("Missing token");
    }

    const url = new URL(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/inventory_current_age_summary`
    );

    url.searchParams.set("country_key", String(countryValue).toLowerCase());
    url.searchParams.set("month_name", String(monthName).toLowerCase());
    url.searchParams.set("year", String(yearValue));

    const res = await fetch(url.toString(), {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
      },
      cache: "no-store",
      signal,
    });

    const json = await res.json().catch(() => ({}));

    if (!res.ok) {
      throw new Error(
        json?.message ||
        json?.error ||
        `Failed to fetch inventory age summary for ${monthName} ${yearValue}`
      );
    }

    return json;
  };

  const getInventoryMonthsForSelectedRange = () => {
    if (range === "monthly") {
      return selectedMonth ? [selectedMonth] : [];
    }

    if (range === "quarterly" && selectedQuarter) {
      return quarterToMonths[selectedQuarter].map((m) => m.toLowerCase());
    }

    if (range === "yearly") {
      return allMonths.map((m) => m.toLowerCase());
    }

    return [];
  };

  const getInventoryAgeSummaryMonthsForTrend = () => {
    const selectedYearNum = Number(selectedYear);

    if (!selectedYearNum || Number.isNaN(selectedYearNum)) {
      return [];
    }

    const now = new Date();
    const currentYear = now.getFullYear();

    // ✅ current month - 1
    const lastCompletedDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
    const lastCompletedMonth = monthNames[
      lastCompletedDate.getMonth()
    ].toLowerCase();

    // ✅ Future year: no historic trend
    if (selectedYearNum > currentYear) {
      return [];
    }

    // ✅ Past year: full historic year
    if (selectedYearNum < currentYear) {
      return ["december"];
    }

    // ✅ Current year:
    // Monthly / Quarterly / Yearly all should show Jan -> current month - 1
    // Example: current month is July, trend should go till June.
    return [lastCompletedMonth];
  };

  useEffect(() => {
    if (isDemoMode) {
      setInventoryRawResponses(null);
      setInventoryInsightsData(createZeroInventoryInsightsData(currencySymbol));

      return;
    }

    if (activeTab !== "inventoryInsights") return;

    const ready =
      (range === "monthly" && !!selectedMonth && !!selectedYear) ||
      (range === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (range === "yearly" && !!selectedYear);

    if (!ready || !countryName) {
      setInventoryInsightsData(null);
      setInventoryRawResponses(null);
      setInventoryInsightsError(null);
      return;
    }

    const ageSummaryMonthsToFetch: string[] =
      getInventoryAgeSummaryMonthsForTrend();

    const ac = new AbortController();

    const fetchInventoryInsights = async () => {
      try {
        setInventoryInsightsLoading(true);
        setInventoryInsightsError(null);

        const [inventoryResult, ageSummaryResults]: [
          InventoryCurrentApiResponse,
          PromiseSettledResult<InventoryAgeSummaryApiResponse>[]
        ] = await Promise.all([
          fetchInventoryCurrentByPeriod(ac.signal),

          Promise.allSettled(
            ageSummaryMonthsToFetch.map((monthName: string) =>
              fetchSingleMonthInventoryAgeSummary(
                monthName,
                selectedYear,
                countryName,
                ac.signal
              )
            )
          ),
        ]);

        const fulfilledInventory: InventoryCurrentApiResponse[] =
          inventoryResult?.success ? [inventoryResult] : [];

        const fulfilledAgeSummary: InventoryAgeSummaryApiResponse[] =
          ageSummaryResults
            .filter(
              (
                result: PromiseSettledResult<InventoryAgeSummaryApiResponse>
              ): result is PromiseFulfilledResult<InventoryAgeSummaryApiResponse> =>
                result.status === "fulfilled"
            )
            .map(
              (result: PromiseFulfilledResult<InventoryAgeSummaryApiResponse>) =>
                result.value
            );

        if (!fulfilledInventory.length) {
          throw new Error("No inventory data found");
        }

        setInventoryRawResponses({
          inventory: fulfilledInventory,
          ageSummary: fulfilledAgeSummary,
        });

        setInventoryInsightsData(
          buildInventoryInsightsFromResponses(
            fulfilledInventory,
            fulfilledAgeSummary,
            effectiveCountryName,
            effectiveHomeCurrency,
            "all",
            selectedGlobalInventoryCountry,
            range,
            selectedMonth,
            selectedYear
          )
        );
      } catch (error: any) {
        if (error?.name === "AbortError") return;
        setInventoryInsightsError(
          error?.message || "Failed to fetch inventory insights"
        );
        setInventoryInsightsData(null);
        setInventoryRawResponses(null);
      } finally {
        setInventoryInsightsLoading(false);
      }
    };

    fetchInventoryInsights();

    return () => ac.abort();
  }, [
    activeTab,
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    countryName,
    isDemoMode,
    selectedGlobalInventoryCountry,
    effectiveCountryName,
    effectiveHomeCurrency,
    currencySymbol,
  ]);


  const normalizeRowsForParent = (data: any[]): TableRow[] => {
    const hasCm2Columns = hasSkuCm2Data(data);

    return data.map((row) => {
      const productName =
        row?.product_name && String(row.product_name).trim() !== ""
          ? String(row.product_name)
          : row?.sku && String(row.sku).trim() !== ""
            ? String(row.sku)
            : "-";

      const isTotalRow = productName.trim().toLowerCase() === "total";
      const netSales = toNum(row.net_sales);
      const advertisingTotal = Math.abs(getAdvertisingBaseValue(row, hasCm2Columns));
      const advertisingTotalFinal = Math.abs(getAdvertisingTotalValue(row, hasCm2Columns));
      const cm2Profit = getCm2ProfitValue(row);
      const cm2Margin = getCm2MarginValue(row, netSales, cm2Profit);
      const cm2ProfitPer = getCm2PerValue(row, netSales, cm2Profit);

      return {
        ...row,
        product_name: isTotalRow ? "Total" : productName,
        sku: row.sku ?? "-",

        quantity: toNum(row.quantity),
        return_quantity: toNum(row.return_quantity),
        total_quantity: toNum(row.total_quantity),

        units_sold: toNum(row.quantity),
        return_units: toNum(row.return_quantity),
        net_units_sold: toNum(row.total_quantity),

        asp: toNum(row.asp ?? row.ASP),
        product_sales: toNum(row.gross_sales ?? row.product_sales),
        gross_sales: toNum(row.gross_sales ?? row.product_sales),
        refund_sales: toNum(row.refund_sales),
        net_sales: netSales,

        cost_of_unit_sold: toNum(row.cost_of_unit_sold),
        selling_fees: toNum(row.selling_fees),
        fba_fees: toNum(row.fba_fees),
        amazon_fee: toNum(row.amazon_fee),

        advertising_total: advertisingTotal,
        advertising_total_final: advertisingTotalFinal,
        advertising_fees: toNum(row.advertising_fees),
        ads_spend: toNum(row.ads_spend),
        total_ads: toNum(row.total_ads),
        visible_ads: toNum(row.visible_ads),
        dealsvouchar_ads: toNum(row.dealsvouchar_ads),

        platform_fee: toNum(row.platform_fee),
        platformfeenew: toNum(row.platformfeenew),
        platform_fee_inventory_storage: toNum(row.platform_fee_inventory_storage),

        tex_and_credits: toNum(row.tex_and_credits),
        net_taxes: toNum(row.net_taxes),
        net_credits: toNum(row.net_credits),

        promotional_rebates: toNum(row.promotional_rebates),
        promotional_rebates_percentage: toNum(row.promotional_rebates_percentage),

        misc_transaction: toNum(row.misc_transaction),
        other_transaction_fees: toNum(row.other_transaction_fees),
        other_transactions: toNum(row.other_transaction_fees),

        profit: toNum(row.profit),
        profit_percentage: toNum(row.profit_percentage),
        unit_wise_profitability: toNum(row.unit_wise_profitability),
        cm2_profit: cm2Profit,
        cm2_profit_total: cm2Profit,

        cm2_profit_per: cm2ProfitPer,

        cm2_profit_percentage: cm2Margin,

        cm2_margins: cm2Margin,

        acos: toNum(row.acos),

        profit_mix: toNum(row.profit_mix),
        sales_mix: toNum(row.sales_mix),
      } as TableRow;
    });
  };

  const normalizeProductDeltaKey = (value: unknown) =>
    String(value || "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, " ")
      .replace(/[^\w\s+&-]/g, "");

  const attachNetSalesDeltaToRows = (
    currentRows: TableRow[],
    previousRows: TableRow[]
  ): TableRow[] => {
    const previousMap = new Map<string, TableRow>();

    previousRows.forEach((row) => {
      const name = normalizeProductDeltaKey(row.product_name);
      const sku = normalizeProductDeltaKey(row.sku);

      if (name && name !== "total") previousMap.set(name, row);
      if (sku && sku !== "total" && sku !== "-") previousMap.set(sku, row);
    });

    return currentRows.map((row): TableRow => {
      const name = normalizeProductDeltaKey(row.product_name);
      const sku = normalizeProductDeltaKey(row.sku);

      const isTotal = name === "total" || sku === "total";

      if (isTotal) return row;

      const previousRow = previousMap.get(name) || previousMap.get(sku);

      const currentNetSales = toNum(row.net_sales);
      const previousNetSales = toNum(previousRow?.net_sales);

      const delta = currentNetSales - previousNetSales;

      const deltaPct =
        previousNetSales !== 0
          ? (delta / previousNetSales) * 100
          : undefined;

      return {
        ...row,
        previous_net_sales: previousNetSales,
        net_sales_delta: delta,
        net_sales_delta_percentage: deltaPct,
      };
    });
  };

  const sumSkuRows = (rows: TableRow[], key: string) =>
    rows
      .filter((r) => String(r.product_name || "").trim().toLowerCase() !== "total")
      .reduce((sum, r) => sum + toNum((r as any)[key]), 0);

  const buildSummaryFromSkuRows = (rows: TableRow[]): Summary => {
    const totalRow = getSkuTotalRow(rows);
    const hasCm2Columns = hasSkuCm2Data(rows);

    const netSales = toNum(totalRow?.net_sales) || sumSkuRows(rows, "net_sales");
    const grossSales =
      toNum((totalRow as any)?.gross_sales) ||
      sumSkuRows(rows, "gross_sales");

    const units =
      toNum(totalRow?.total_quantity) ||
      toNum(totalRow?.net_units_sold) ||
      sumSkuRows(rows, "total_quantity");

    const advertisingTotal = Math.abs(getAdvertisingBaseValue(totalRow, hasCm2Columns));
    const advertisingTotalFinal = Math.abs(getAdvertisingTotalValue(totalRow, hasCm2Columns));

    const cm2 = getCm2ProfitValue(totalRow);

    const promotionalRebates =
      toNum((totalRow as any)?.promotional_rebates) ||
      sumSkuRows(rows, "promotional_rebates");

    const promotionalRebatesPercentage =
      netSales !== 0
        ? (promotionalRebates / netSales) * 100
        : toNum((totalRow as any)?.promotional_rebates_percentage);

    const cm2Margin = getCm2MarginValue(totalRow, netSales, cm2);

    return {
      unit_sold: units,
      total_sales: netSales,
      gross_sales: grossSales,
      total_product_sales: grossSales,
      total_expense: 0,

      cm2_profit: cm2,
      cm2_profit_total: cm2,
      cm2_margins: cm2Margin,
      cm2_profit_percentage: cm2Margin,
      promotional_rebates: promotionalRebates,
      promotional_rebates_percentage: promotionalRebatesPercentage,

      total_cous:
        toNum((totalRow as any)?.cost_of_unit_sold) ||
        sumSkuRows(rows, "cost_of_unit_sold"),

      advertising_total: advertisingTotal,
      advertising_total_final: advertisingTotalFinal,
      advertising_fees: toNum((totalRow as any)?.advertising_fees),
      ads_spend: toNum((totalRow as any)?.ads_spend),
      total_ads: toNum((totalRow as any)?.total_ads),

      total_amazon_fee:
        toNum((totalRow as any)?.amazon_fee) ||
        sumSkuRows(rows, "amazon_fee"),

      otherwplatform: Math.abs(toNum((totalRow as any)?.platform_fee)),
    };
  };

  const buildUploadRowFromSkuRows = (
    rows: TableRow[],
    period: {
      rangeType: RangeType;
      monthVal: string;
      quarterVal: string;
      yearVal: string;
      countryVal: string;
    }
  ): UploadRow[] => {
    const totalRow = getSkuTotalRow(rows);
    const hasCm2Columns = hasSkuCm2Data(rows);

    if (!totalRow) return [];

    const netSales = toNum((totalRow as any)?.net_sales);
    const advertisingTotal = Math.abs(getAdvertisingBaseValue(totalRow, hasCm2Columns));
    const advertisingTotalFinal = Math.abs(getAdvertisingTotalValue(totalRow, hasCm2Columns));

    const cm1Profit = toNum((totalRow as any)?.profit);

    const cm2ProfitTotal = getCm2ProfitValue(totalRow);

    const promotionalRebates = toNum((totalRow as any)?.promotional_rebates);
    const promotionalRebatesPercentage =
      netSales !== 0
        ? (promotionalRebates / netSales) * 100
        : toNum((totalRow as any)?.promotional_rebates_percentage);

    const cm2Margins = getCm2MarginValue(totalRow, netSales, cm2ProfitTotal);
    const cm2ProfitPer = getCm2PerValue(totalRow, netSales, cm2ProfitTotal);

    return [
      {
        country: period.countryVal,
        month:
          period.rangeType === "monthly"
            ? period.monthVal.toLowerCase()
            : period.rangeType === "quarterly"
              ? period.quarterVal
              : "ALL",
        year: period.yearVal,

        total_sales: netSales,
        total_amazon_fee: toNum((totalRow as any)?.amazon_fee),
        total_cous: toNum((totalRow as any)?.cost_of_unit_sold),

        advertising_total: advertisingTotal,
        advertising_total_final: advertisingTotalFinal,
        advertising_fees: toNum((totalRow as any)?.advertising_fees),
        ads_spend: toNum((totalRow as any)?.ads_spend),
        total_ads: toNum((totalRow as any)?.total_ads),

        otherwplatform:
          toNum((totalRow as any)?.platform_fee),

        taxncredit: toNum((totalRow as any)?.tex_and_credits),

        // CM1 Profit
        profit: cm1Profit,
        total_profit: cm1Profit,

        // CM2 Profit
        cm2_profit: cm2ProfitTotal,
        cm2_profit_total: cm2ProfitTotal,
        cm2_margins: cm2Margins,
        cm2_profit_percentage: cm2Margins,
        cm2_profit_per: cm2ProfitPer,
        promotional_rebates: promotionalRebates,
        promotional_rebates_percentage: promotionalRebatesPercentage,

        tacos: calculateTacos(netSales, advertisingTotalFinal),
      },
    ];
  };

  const monthNumberToName = (monthNum: number | string) => {
    const n = Number(monthNum);
    const names = [
      "january", "february", "march", "april", "may", "june",
      "july", "august", "september", "october", "november", "december",
    ];
    return names[n - 1] || "";
  };

  const buildGraphUploadRowsFromSkuRows = (
    rows: TableRow[],
    monthVal: string,
    yearVal: string,
    countryVal: string
  ): UploadRow[] => {
    const totalRow = getSkuTotalRow(rows);
    const hasCm2Columns = hasSkuCm2Data(rows);

    if (!totalRow) return [];

    const cm1Profit = toNum((totalRow as any)?.profit);

    const netSales = toNum((totalRow as any)?.net_sales);
    const advertisingTotal = Math.abs(getAdvertisingBaseValue(totalRow, hasCm2Columns));
    const advertisingTotalFinal = Math.abs(getAdvertisingTotalValue(totalRow, hasCm2Columns));
    const cm2ProfitTotal = getCm2ProfitValue(totalRow);
    const cm2Margins = getCm2MarginValue(totalRow, netSales, cm2ProfitTotal);
    const cm2ProfitPer = getCm2PerValue(totalRow, netSales, cm2ProfitTotal);
    const promotionalRebates = toNum((totalRow as any)?.promotional_rebates);
    const promotionalRebatesPercentage =
      netSales !== 0
        ? (promotionalRebates / netSales) * 100
        : toNum((totalRow as any)?.promotional_rebates_percentage);

    return [
      {
        country: countryVal,
        month: monthVal.toLowerCase(),
        year: yearVal,

        total_sales: netSales,
        total_amazon_fee: toNum((totalRow as any)?.amazon_fee),
        total_cous: toNum((totalRow as any)?.cost_of_unit_sold),

        advertising_total: advertisingTotal,
        advertising_total_final: advertisingTotalFinal,
        advertising_fees: toNum((totalRow as any)?.advertising_fees),
        ads_spend: toNum((totalRow as any)?.ads_spend),
        total_ads: toNum((totalRow as any)?.total_ads),

        otherwplatform:
          toNum((totalRow as any)?.platform_fee),

        taxncredit: toNum((totalRow as any)?.tex_and_credits),

        // CM1 Profit
        profit: cm1Profit,
        total_profit: cm1Profit,

        // CM2 Profit
        cm2_profit: cm2ProfitTotal,
        cm2_profit_total: cm2ProfitTotal,

        cm2_margins: cm2Margins,
        cm2_profit_percentage: cm2Margins,
        cm2_profit_per: cm2ProfitPer,
        promotional_rebates: promotionalRebates,
        promotional_rebates_percentage: promotionalRebatesPercentage,
      },
    ];
  };

  const fetchMonthlySkuRowsForGraph = async (
    monthVal: string,
    yearVal: string,
    countryVal: string,
    token: string | null
  ): Promise<UploadRow[]> => {
    const url = buildMonthlySkuUrl(monthVal, yearVal, countryVal);

    const res = await fetch(url.toString(), {
      method: "GET",
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      cache: "no-store",
    });

    if (!res.ok) return [];

    const raw = await res.json();
    const normalizedRows = normalizeRowsForParent(raw?.current_data ?? []);

    return buildGraphUploadRowsFromSkuRows(
      normalizedRows,
      monthVal,
      yearVal,
      countryVal
    );
  };

  const marketplaceFeesFromTable = useMemo(() => {
    if (!skuRows?.length) return 0;

    const totalRow = skuRows.find(
      (row) => String(row.product_name || "").trim().toLowerCase() === "total"
    );

    if (totalRow) {
      return Number(totalRow.amazon_fee || 0);
    }

    return skuRows.reduce((sum, row) => {
      const isTotal = String(row.product_name || "").trim().toLowerCase() === "total";
      if (isTotal) return sum;
      return sum + Number(row.amazon_fee || 0);
    }, 0);
  }, [skuRows]);

  const hasAnyContent = !!uploadsData?.summary;
  const initialLoading = loading && !hasAnyContent;

  if (initialLoading) {
    return (
      // <Loader fullscreen transparent />
      <div className="absolute inset-0 z-20 flex items-center justify-center bg-white/80">
        <Loader backgroundClass="bg-white/40" />
      </div>
    );
  }

  const capitalizeFirstLetter = (str: string) =>
    str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();

  const convertToAbbreviatedMonth = (m?: string) =>
    m ? capitalizeFirstLetter(m).slice(0, 3) : "";

  const getTitle = () => {
    if (range === "quarterly" && selectedQuarter) {
      return `${capitalizeFirstLetter(
        range
      )} Tracking Profitability - ${selectedQuarter}'${String(selectedYear).slice(
        -2
      )}`;
    }
    if (range === "monthly" && selectedMonth) {
      return `${capitalizeFirstLetter(
        range
      )} Tracking Profitability - ${convertToAbbreviatedMonth(
        selectedMonth
      )} ${selectedYear}`;
    }
    return `${capitalizeFirstLetter(range)} Tracking Profitability - ${selectedYear}`;
  };

  const getPeriodLabelShort = () => {
    const yy = String(selectedYear || "").slice(-2);
    if (range === "monthly" && selectedMonth && selectedYear) {
      return `${convertToAbbreviatedMonth(selectedMonth)}'${yy}`;
    }
    if (range === "quarterly" && selectedQuarter && selectedYear) {
      return `${selectedQuarter}'${yy}`;
    }
    if (range === "yearly" && selectedYear) {
      return String(selectedYear);
    }
    return "";
  };

  const getTrendWrapperHeight = () => {
    if (focusedChart === "trend") return "h-[50vh]";
    if (range === "monthly") return "h-[360px]";
    return "h-[375px] 2xl:h-[500px]";
  };

  const FINANCE_TAB_TO_HASH: Record<DashboardTab, string> = {
    graphs: "finance-dashboard",
    businessSummary: "ai-insights",
    inventoryInsights: "inventory-insights",
    skuBreakdown: "pnl-breakdown",
    skuwiseProfit: "sku-journey",
    cashFlow: "cash-flow",
  };

  const syncTabToHash = (tab: DashboardTab) => {
    if (typeof window === "undefined") return;

    const hash = FINANCE_TAB_TO_HASH[tab];
    if (!hash) return;

    const nextUrl = `${window.location.pathname}#${hash}`;

    // Only update URL on manual tab switch.
    // Do not dispatch page-hash-navigate, because that triggers scrollIntoView.
    window.history.replaceState(null, "", nextUrl);
  };


  const scrollFinancePageToTop = () => {
    if (typeof window === "undefined") return;

    const target = tabTopRef.current || layoutRef.current;

    const scrollParents: HTMLElement[] = [];

    let parent = target?.parentElement || null;

    while (parent) {
      const style = window.getComputedStyle(parent);
      const overflowY = style.overflowY;

      const canScroll =
        (overflowY === "auto" || overflowY === "scroll") &&
        parent.scrollHeight > parent.clientHeight;

      if (canScroll) {
        scrollParents.push(parent);
      }

      parent = parent.parentElement;
    }

    // window/body scroll reset
    window.scrollTo({
      top: 0,
      left: 0,
      behavior: "auto",
    });

    document.documentElement.scrollTop = 0;
    document.body.scrollTop = 0;

    // any custom scroll container reset
    scrollParents.forEach((el) => {
      el.scrollTop = 0;
    });
  };



  const handleConnectAmazonPreview = () => {
    router.push(`/profile/${countryName}/NA/NA`);
  };

  const inventoryInsightsReportCountry = getInventoryInsightsReportCountry();
  const showUsCurrentInventoryTable =
    !isDemoMode &&
    ["us", "usa", "united states"].includes(
      String(inventoryInsightsReportCountry || "").trim().toLowerCase()
    );



  return (
    <div ref={layoutRef} className="space-y-3 relative">
      <div ref={tabTopRef} />
      <div className="sticky top-0 z-40 w-full flex flex-col bg-[#F7F7F7] sm:flex-row md:items-center md:justify-between gap-4 ">
        <div className="flex flex-col leading-tight w-full md:w-auto ">
          <div className="flex items-baseline gap-2">
            <PageBreadcrumb pageTitle="Financial Metrics - Amazon" variant="page" align="left" textSize="2xl" />
            <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
              {/* Amazon{" "} */}
              {effectiveCountryName?.toLowerCase() === "global"
                ? "Global"
                : effectiveCountryName?.toUpperCase()}
            </span>
          </div>
          <p className="text-xs 2xl:text-sm text-charcoal-500 mt-1">
            Track your profitability and key metrics
          </p>
        </div>

        <div className="flex w-full mb-2 sm:mb-0 md:w-auto justify-start md:justify-end">
          {/* <PeriodFiltersTable
            range={range === "" ? "yearly" : (range as "monthly" | "quarterly" | "yearly")}
            selectedMonth={selectedMonth}
            selectedQuarter={selectedQuarter || ""}
            selectedYear={selectedYear}
            yearOptions={yearOptions}
            onRangeChange={handleRangeChange}
            onMonthChange={handleMonthChange}
            onQuarterChange={handleQuarterChange}
            onYearChange={handleYearChange}
          /> */}
          <PeriodFiltersTable
            range={activeTab === "skuwiseProfit" && range === "monthly" ? "yearly" : range}
            selectedMonth={activeTab === "skuwiseProfit" ? "" : selectedMonth}
            selectedQuarter={selectedQuarter}
            selectedYear={selectedYear}
            yearOptions={yearOptions}
            onRangeChange={handleRangeChange}
            onMonthChange={handleMonthChange}
            onQuarterChange={handleQuarterChange}
            onYearChange={handleYearChange}
            availablePeriods={availablePeriods}
            allowedRanges={
              activeTab === "skuwiseProfit"
                ? ["quarterly", "yearly"]
                : ["monthly", "quarterly", "yearly"]
            }
          />
        </div>
      </div>

      {/* ===================== NEW: TABS (UNDER HEADER) ===================== */}

      <div className="sticky max-[480px]:top-[97px] max-[640px]:top-[97px] sm:top-[48px] md:top-[48px] 2xl:top-[56px] z-30 bg-[#F7F7F7] border-b border-gray-200 
    max-[480px]:pb-1 max-[640px]:pb-2 sm:py-2">
        <SegmentedToggle<DashboardTab>
          value={activeTab}
          options={TAB_OPTIONS}
          onChange={(t) => {
            if (tabsDisabled?.[t]) return;

            shouldScrollTabTopRef.current = true;

            setActiveTab(t);

            if (t === "skuwiseProfit" && range === "monthly") {
              setRange("yearly");
              setSelectedMonth("");
              setSelectedQuarter("");
              setUploadsData({
                summary: zeroData,
                summaryComparisons: zeroComparisons,
              });
              setSkuRows([]);
              setSkuNoDataFound(false);
              setSkuRowsError(null);
            }

            syncTabToHash(t);
          }}
          className="w-full"
          textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
          compact
        />
      </div>
      <PreviewLockedSection
        enabled={isDemoMode}
        title="Preview Mode"
        description="To view your real business data and analytics, please complete your profile and connect your Amazon account. This will unlock your performance dashboard and insights."
        buttonText="Complete Setup"
        onAction={handleConnectAmazonPreview}
      >
        {/* ===================== SUMMARY CARDS (OPTIONAL: ALWAYS SHOW) ===================== */}
        {activeTab !== "cashFlow" && activeTab !== "skuwiseProfit" && activeTab !== "inventoryInsights" && (

          <div className="flex flex-col gap-5 w-full mt-4">
            {/* Summary Cards */}
            {allDropdownsSelected &&

              (() => {
                const summary = displayData;
                const netSales = summary.total_sales;
                // const rawComparisons =
                //   (uploadsData as any).summaryComparisons ??
                //   (uploadsData as any).summary_comparisons;

                // const comparisons: SummaryComparisons | undefined = rawComparisons
                //   ? (rawComparisons as SummaryComparisons)
                //   : undefined;

                const rawComparisons =
                  (uploadsData as any)?.summaryComparisons ??
                  (uploadsData as any)?.summary_comparisons ??
                  displayComparisons ??
                  zeroComparisons;

                const comparisons: SummaryComparisons = rawComparisons as SummaryComparisons;

                const formatMoney = (
                  val: number,
                  opts?: { showPlus?: boolean; decimals?: number }
                ) => {
                  const num = Number(val || 0);
                  const sign = num < 0 ? "-" : opts?.showPlus && num > 0 ? "+" : "";
                  const abs = Math.abs(num);
                  const decimals = opts?.decimals ?? 2;

                  return `${sign}${currencySymbol}${abs.toLocaleString(undefined, {
                    minimumFractionDigits: decimals,
                    maximumFractionDigits: decimals,
                  })}`;
                };

                const costOfAds =
                  summary.advertising_total_final ??
                  summary.advertising_total ??
                  0;

                const getRoas = (s?: Summary) => {
                  const ns = s?.total_sales ?? 0;            // net sales
                  const ads =
                    s?.advertising_total_final ??
                    s?.advertising_total ??
                    0;                                      // final cost of ads
                  return ns > 0 ? (ads / ns) * 100 : 0;
                };


                const roas = getRoas(summary);

                const formatRoas = (val: number) =>
                  `${val.toLocaleString(undefined, {
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2,
                  })}%`;

                const buildTacosComparisonRows = () => {
                  const yNum = Number(selectedYear);

                  const label =
                    range === "monthly"
                      ? selectedMonth && yNum
                        ? getPrevMonthLabel(selectedMonth, yNum)
                        : "Prev month"
                      : range === "quarterly"
                        ? selectedQuarter && yNum
                          ? getPrevQuarterLabel(selectedQuarter as Quarter, yNum)
                          : "Prev quarter"
                        : yNum
                          ? getPrevYearLabel(yNum)
                          : "Prev year";

                  const prevVal =
                    range === "monthly"
                      ? comparisons?.lastMonth
                        ? getRoas(comparisons.lastMonth)
                        : undefined
                      : range === "quarterly"
                        ? comparisons?.lastQuarter
                          ? getRoas(comparisons.lastQuarter)
                          : undefined
                        : comparisons?.lastYear
                          ? getRoas(comparisons.lastYear)
                          : undefined;

                  const hasPrev = typeof prevVal === "number" && !isNaN(prevVal);
                  const delta =
                    hasPrev && prevVal !== 0
                      ? ((roas - prevVal!) / Math.abs(prevVal!)) * 100
                      : null;

                  const deltaClassName =
                    typeof delta === "number"
                      ? delta > 0
                        ? "text-red-600"       // higher TACoS worse
                        : delta < 0
                          ? "text-emerald-600" // lower TACoS better
                          : "text-gray-400"
                      : "text-gray-400";

                  const arrow =
                    typeof delta === "number"
                      ? roas > prevVal!
                        ? "▲" // increased → bad
                        : roas < prevVal!
                          ? "▼" // decreased → good
                          : ""
                      : "";

                  const deltaText =
                    typeof delta === "number"
                      ? `${arrow} ${Math.abs(delta).toFixed(2)}%`
                      : "-";

                  return [
                    {
                      label,
                      valueText: hasPrev ? formatRoas(prevVal!) : "-",
                      deltaText,
                      deltaClassName,
                    },
                  ];
                };


                const formatUnits = (val: number) =>
                  val.toLocaleString(undefined, { maximumFractionDigits: 0 });

                const safeDiv = (num: number, den: number) => (den > 0 ? num / den : 0);

                const renderMoneyWithPerUnit = (
                  total: number,
                  units: number,
                  roundPerUnit = false,
                  decimals = 2
                ) => {
                  const totalText = formatMoney(total, { decimals });

                  if (!units) {
                    return <span>{totalText}</span>;
                  }

                  const perUnitRaw = total / units;
                  const perUnit = roundPerUnit ? Math.round(perUnitRaw) : perUnitRaw;
                  const perUnitText = formatMoney(perUnit, {
                    decimals: roundPerUnit ? 0 : decimals,
                  });

                  return (
                    <div className="flex items-baseline gap-1 leading-tight">
                      <span className="text-sm 2xl:text-lg font-semibold">
                        {totalText}
                      </span>

                      <span className="text-[10px] 2xl:text-xs text-charcoal-400 font-medium">
                        ({perUnitText}/unit)
                      </span>
                    </div>
                  );
                };

                const formatPercent = (val: number) =>
                  `${val.toLocaleString(undefined, {
                    minimumFractionDigits: 0,
                    maximumFractionDigits: 2,
                  })}%`;

                const getGrossSales = (s?: Summary) =>
                  s?.total_product_sales ?? s?.gross_sales ?? 0;

                const roundMoney = (val: number) => Math.round(val || 0);

                const formatWholeMoney = (val: number) =>
                  formatMoney(roundMoney(val), { decimals: 0 });

                const formatAspMoney = (val: number) =>
                  formatMoney(toNum(val), { decimals: 2 });

                const formatAmountWithPct = (
                  amount: number,
                  pct: number,
                  absoluteAmount = false
                ) => {
                  const displayAmount = absoluteAmount ? Math.abs(amount) : amount;

                  return `${formatWholeMoney(displayAmount)} (${formatPercent(Math.abs(pct))})`;
                };

                const formatCurrentAmountWithPct = (
                  amount: number,
                  pct: number,
                  absoluteAmount = false
                ) => {
                  const displayAmount = absoluteAmount ? Math.abs(amount) : amount;

                  return (
                    <div className="flex items-baseline gap-1 leading-tight">
                      <span className="text-sm 2xl:text-lg font-semibold">
                        {formatWholeMoney(displayAmount)}
                      </span>

                      <span className="text-[10px] 2xl:text-xs text-charcoal-400 font-medium">
                        ({formatPercent(Math.abs(pct))})
                      </span>
                    </div>
                  );
                };

                const isSummaryZero =
                  summary.unit_sold === 0 &&
                  summary.total_sales === 0 &&
                  summary.total_expense === 0 &&
                  summary.cm2_profit === 0;

                // ---------- generic comparisons helper ----------
                const getComparisons = (metric: keyof Summary): ComparisonItem[] => {
                  const current = summary[metric] ?? 0;

                  const lm = comparisons?.lastMonth?.[metric];
                  const lq = comparisons?.lastQuarter?.[metric];
                  const ly = comparisons?.lastYear?.[metric];

                  const makeItem = (label: string, prevVal?: number): ComparisonItem => {
                    if (typeof prevVal !== "number") {
                      return { label, value: undefined, diffPct: null };
                    }

                    const diffPct =
                      prevVal === 0 ? null : ((current - prevVal) / Math.abs(prevVal)) * 100;

                    return { label, value: prevVal, diffPct };
                  };

                  const yNum = Number(selectedYear);

                  if (range === "monthly") {
                    const label = selectedMonth && yNum ? getPrevMonthLabel(selectedMonth, yNum) : "Prev month";
                    return [makeItem(label, lm)];
                  }

                  if (range === "quarterly") {
                    const label =
                      selectedQuarter && yNum
                        ? getPrevQuarterLabel(selectedQuarter as Quarter, yNum)
                        : "Prev quarter";
                    return [makeItem(label, lq)];
                  }

                  if (range === "yearly") {
                    const label = yNum ? getPrevYearLabel(yNum) : "Prev year";
                    return [makeItem(label, ly)];
                  }

                  return [];
                };

                const buildComparisonsRows = (
                  metric: keyof Summary,
                  formatter: (val: number) => string
                ) => {
                  const items = getComparisons(metric);

                  return items.map((item) => {
                    const hasValue = typeof item.value === "number" && !isNaN(item.value);
                    const hasDiff = typeof item.diffPct === "number" && !isNaN(item.diffPct);

                    // ✅ define metric behavior INLINE
                    const isCostMetric =
                      metric === "advertising_total_final" ||
                      metric === "advertising_total" ||
                      metric === "total_amazon_fee";



                    // ✅ FIXED COLOR LOGIC
                    let deltaClassName = "text-gray-400";

                    if (hasDiff) {
                      if (isCostMetric) {
                        // higher cost = BAD
                        deltaClassName =
                          item.diffPct! < 0 ? "text-emerald-600" : "text-red-600";
                      } else {
                        // higher = GOOD
                        deltaClassName =
                          item.diffPct! >= 0 ? "text-emerald-600" : "text-red-600";
                      }
                    }

                    const deltaText = hasDiff
                      ? `${item.diffPct! >= 0 ? "▲" : "▼"} ${Math.abs(item.diffPct!).toFixed(2)}%`
                      : "-";

                    return {
                      label: item.label,
                      valueText: hasValue ? formatter(item.value!) : "-",
                      deltaText,
                      deltaClassName,
                    };
                  });
                };

                const getAspComparisons = (): ComparisonItem[] => {
                  const current = toNum(summary?.asp);
                  const yNum = Number(selectedYear);

                  const prevMonth = comparisons?.lastMonth
                    ? toNum(comparisons.lastMonth.asp)
                    : undefined;

                  const prevQuarter = comparisons?.lastQuarter
                    ? toNum(comparisons.lastQuarter.asp)
                    : undefined;

                  const prevYear = comparisons?.lastYear
                    ? toNum(comparisons.lastYear.asp)
                    : undefined;

                  const makeItem = (label: string, prevVal?: number): ComparisonItem => {
                    if (typeof prevVal !== "number" || !Number.isFinite(prevVal)) {
                      return { label, value: undefined, diffPct: null };
                    }

                    const diffPct =
                      prevVal === 0 ? null : ((current - prevVal) / Math.abs(prevVal)) * 100;

                    return {
                      label,
                      value: prevVal,
                      diffPct,
                    };
                  };

                  if (range === "monthly") {
                    const label =
                      selectedMonth && yNum
                        ? getPrevMonthLabel(selectedMonth, yNum)
                        : "Prev month";

                    return [makeItem(label, prevMonth)];
                  }

                  if (range === "quarterly") {
                    const label =
                      selectedQuarter && yNum
                        ? getPrevQuarterLabel(selectedQuarter as Quarter, yNum)
                        : "Prev quarter";

                    return [makeItem(label, prevQuarter)];
                  }

                  if (range === "yearly") {
                    const label = yNum ? getPrevYearLabel(yNum) : "Prev year";
                    return [makeItem(label, prevYear)];
                  }

                  return [];
                };

                // ---------- Gross Sales comparisons ----------
                const getGrossSalesComparisons = (): ComparisonItem[] => {
                  const current = getGrossSales(summary);
                  const yNum = Number(selectedYear);

                  const prevMonth = comparisons?.lastMonth ? getGrossSales(comparisons.lastMonth) : undefined;
                  const prevQuarter = comparisons?.lastQuarter ? getGrossSales(comparisons.lastQuarter) : undefined;
                  const prevYear = comparisons?.lastYear ? getGrossSales(comparisons.lastYear) : undefined;

                  const makeItem = (label: string, prevVal?: number): ComparisonItem => {
                    if (typeof prevVal !== "number") return { label, value: undefined, diffPct: null };
                    const diffPct = prevVal === 0 ? null : ((current - prevVal) / prevVal) * 100;
                    return { label, value: prevVal, diffPct };
                  };

                  if (range === "monthly") {
                    const label = selectedMonth && yNum ? getPrevMonthLabel(selectedMonth, yNum) : "Prev month";
                    return [makeItem(label, prevMonth)];
                  }

                  if (range === "quarterly") {
                    const label =
                      selectedQuarter && yNum
                        ? getPrevQuarterLabel(selectedQuarter as Quarter, yNum)
                        : "Prev quarter";
                    return [makeItem(label, prevQuarter)];
                  }

                  if (range === "yearly") {
                    const label = yNum ? getPrevYearLabel(yNum) : "Prev year";
                    return [makeItem(label, prevYear)];
                  }

                  return [];
                };

                // ---------- CM2% comparisons ----------
                const getCm2Percent = (s?: Summary) => {
                  const explicitValue = [
                    s?.cm2_margins,
                    s?.cm2_profit_percentage,
                    s?.cm2_profit_per,
                  ]
                    .map((value) => toNum(value))
                    .find((value) => value !== 0);

                  if (typeof explicitValue === "number") {
                    return explicitValue;
                  }

                  const sales = toNum(s?.total_sales);
                  return sales !== 0 ? (toNum(s?.cm2_profit) / sales) * 100 : 0;
                };

                const cm2Margin = getCm2Percent(summary);
                const promotionsAmount = Math.abs(toNum(summary.promotional_rebates));
                const getPromotionsPercent = (s?: Summary) => {
                  const rebates = toNum(s?.promotional_rebates);
                  const sales = toNum(s?.total_sales);
                  const hasRebates =
                    s?.promotional_rebates !== undefined &&
                    s?.promotional_rebates !== null;

                  return Math.abs(
                    hasRebates && sales !== 0
                      ? (rebates / sales) * 100
                      : toNum(s?.promotional_rebates_percentage)
                  );
                };

                const promotionsPercent = getPromotionsPercent(summary);

                const getActivePreviousComparison = () => {
                  const yNum = Number(selectedYear);

                  if (range === "monthly") {
                    return {
                      label:
                        selectedMonth && yNum
                          ? getPrevMonthLabel(selectedMonth, yNum)
                          : "Prev month",
                      summary: comparisons?.lastMonth,
                    };
                  }

                  if (range === "quarterly") {
                    return {
                      label:
                        selectedQuarter && yNum
                          ? getPrevQuarterLabel(selectedQuarter as Quarter, yNum)
                          : "Prev quarter",
                      summary: comparisons?.lastQuarter,
                    };
                  }

                  return {
                    label: yNum ? getPrevYearLabel(yNum) : "Prev year",
                    summary: comparisons?.lastYear,
                  };
                };

                const buildAmountPctComparisonRows = (
                  currentAmount: number,
                  getPreviousAmount: (s?: Summary) => number,
                  getPreviousPct: (s?: Summary) => number,
                  absoluteAmount = false,
                  lowerIsBetter = false
                ) => {
                  const { label, summary: previousSummary } = getActivePreviousComparison();
                  const previousAmount = getPreviousAmount(previousSummary);
                  const previousPct = getPreviousPct(previousSummary);
                  const hasPrev =
                    typeof previousAmount === "number" &&
                    Number.isFinite(previousAmount);
                  const diffPct =
                    hasPrev && previousAmount !== 0
                      ? ((currentAmount - previousAmount) / Math.abs(previousAmount)) * 100
                      : null;

                  const deltaClassName =
                    typeof diffPct === "number"
                      ? diffPct === 0
                        ? "text-gray-400"
                        : lowerIsBetter
                          ? diffPct < 0
                            ? "text-emerald-600"
                            : "text-red-600"
                          : diffPct > 0
                            ? "text-emerald-600"
                            : "text-red-600"
                      : "text-gray-400";

                  const deltaText =
                    typeof diffPct === "number"
                      ? `${diffPct >= 0 ? "\u25B2" : "\u25BC"} ${Math.abs(diffPct).toFixed(2)}%`
                      : "-";

                  return [
                    {
                      label,
                      valueText: hasPrev
                        ? formatAmountWithPct(previousAmount, previousPct, absoluteAmount)
                        : "-",
                      deltaText,
                      deltaClassName,
                    },
                  ];
                };

                const cards = [
                  {
                    key: "units",
                    title: "Units",
                    value: formatUnits(summary.unit_sold),
                    // className: "border border-[#FDD36F] bg-[#FDD36F4D]",
                    className: "bg-white border border-[#FDD36F] border-t-4 border-t-[#FDD36F] ",
                    comparisons: buildComparisonsRows("unit_sold", formatUnits),
                  },
                  {
                    key: "asp",
                    title: "ASP",

                    // ✅ Shows decimals, e.g. £9.92 instead of £10
                    value: formatAspMoney(summary?.asp ?? 0),

                    className: "bg-white border border-[#ED9F50] border-t-4 border-t-[#ED9F50]",

                    comparisons: (() => {
                      const items = getAspComparisons();

                      return items.map((item) => {
                        const hasValue = typeof item.value === "number" && !isNaN(item.value);
                        const hasDiff = typeof item.diffPct === "number" && !isNaN(item.diffPct);

                        const deltaClassName = hasDiff
                          ? item.diffPct! >= 0
                            ? "text-emerald-600"
                            : "text-red-600"
                          : "text-gray-400";

                        const deltaText = hasDiff
                          ? `${item.diffPct! >= 0 ? "▲" : "▼"} ${Math.abs(item.diffPct!).toFixed(2)}%`
                          : "-";

                        return {
                          label: item.label,

                          // ✅ Previous ASP also shows decimals
                          valueText: hasValue ? formatAspMoney(item.value!) : "-",

                          deltaText,
                          deltaClassName,
                        };
                      });
                    })(),
                  },
                  {
                    key: "netSales",
                    title: "Net Sales",
                    value: renderMoneyWithPerUnit(
                      roundMoney(netSales),
                      summary.unit_sold,
                      true,
                      0
                    ),
                    className: "bg-white border border-[#75BBDA] border-t-4 border-t-[#75BBDA]",
                    comparisons: buildComparisonsRows("total_sales", formatWholeMoney),
                  },
                  {
                    key: "expenses",
                    title: "Marketplace Fees",
                    value: renderMoneyWithPerUnit(
                      roundMoney(marketplaceFeesFromTable),
                      summary.unit_sold,
                      true,
                      0
                    ),
                    className: "bg-white border border-[#B75A5A] border-t-4 border-t-[#B75A5A]",
                    comparisons: buildComparisonsRows("total_amazon_fee", formatWholeMoney),
                  },
                  {
                    key: "ads",
                    title: "Cost of Advertisement",
                    value: renderMoneyWithPerUnit(
                      roundMoney(costOfAds),
                      summary.unit_sold,
                      true,
                      0
                    ),
                    className: "bg-white border border-[#C49466] border-t-4 border-t-[#C49466]",
                    comparisons: buildComparisonsRows("advertising_total_final", formatWholeMoney),
                  },
                  {
                    key: "tacos",
                    title: "TACoS",
                    value: formatRoas(roas),
                    // className: "border border-[#3A8EA4] bg-[#3A8EA44D]",
                    className: "bg-white border border-[#3A8EA4] border-t-4 border-t-[#3A8EA4]",
                    comparisons: buildTacosComparisonRows(),
                  },
                  {
                    key: "cm2",
                    title: "CM2 Profit",
                    value: formatCurrentAmountWithPct(
                      roundMoney(summary.cm2_profit),
                      cm2Margin
                    ),
                    className: "bg-white border border-[#B8C78C] border-t-4 border-t-[#B8C78C]",
                    comparisons: buildAmountPctComparisonRows(
                      roundMoney(summary.cm2_profit),
                      (s) => roundMoney(toNum(s?.cm2_profit)),
                      getCm2Percent
                    ),
                  },
                  {
                    key: "promotions",
                    title: "Promotions",
                    value: formatCurrentAmountWithPct(
                      roundMoney(promotionsAmount),
                      promotionsPercent,
                      true
                    ),
                    className: "bg-white border border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]",
                    comparisons: buildAmountPctComparisonRows(
                      roundMoney(promotionsAmount),
                      (s) => roundMoney(Math.abs(toNum(s?.promotional_rebates))),
                      getPromotionsPercent,
                      true,
                      true
                    ),
                  },
                ];

                return (
                  <div
                    className={[
                      "w-full grid gap-2 2xl:gap-3",
                      "grid-cols-2 sm:grid-cols-4 min-[1700px]:grid-cols-8",
                    ].join(" ")}
                  >
                    {cards.map((c) => (
                      <SummaryMetricCard
                        key={c.key}
                        title={c.title}
                        value={c.value}
                        className={c.className}
                        comparisons={c.comparisons}
                      />
                    ))}
                  </div>
                );
              })()}
          </div>

        )}

        {/* ===================== TAB CONTENT AREA ===================== */}
        <div className="w-full mt-4">
          {/* ---------- TAB 1: GRAPHS ---------- */}
          {activeTab === "graphs" && (

            <div id="finance-dashboard" className="scroll-mt-[80px]">
              {/* Monthly */}
              {range === "monthly" && selectedMonth && selectedYear && (
                <>
                  <div className="w-full rounded-xl space-y-4">
                    <div
                      className={[
                        "grid grid-cols-1 gap-4",
                        focusedChart ? "lg:grid-cols-1" : "lg:grid-cols-2",
                      ].join(" ")}
                    >
                      {/* LEFT card */}
                      {(focusedChart === null || focusedChart === "trend") && (
                        <div
                          className={[
                            "rounded-xl border border-slate-200 bg-white shadow-sm p-4",
                            "cursor-default select-none",
                            focusedChart === "trend" ? "cursor-default" : "",
                          ].join(" ")}
                        >
                          <div className={getTrendWrapperHeight()}>
                            <PerformanceTrendChart
                              range={range}
                              month={selectedMonth}
                              year={selectedYear}
                              countryName={initialCountryName}
                              homeCurrency={globalHomeCurrency}
                              currencySymbol={currencySymbol}
                              data={performanceTrend}
                              metric={performanceTrendMetric}
                              onExportApiReady={setTrendExportApi}
                              isExpanded={focusedChart === "trend"}
                              onToggleExpand={() => toggleFocus("trend")}
                              isPreviewMode={isDemoMode}
                            />
                          </div>
                        </div>
                      )}

                      {/* RIGHT card */}
                      {(focusedChart === null || focusedChart === "pnl") && (
                        <div
                          className={[
                            "rounded-xl border border-slate-200 bg-white shadow-sm p-4",
                            "cursor-default select-none",
                            "min-h-0 overflow-hidden",
                            "flex flex-col",
                            focusedChart === "pnl" ? "cursor-default" : "",
                          ].join(" ")}
                        >
                          <div className="shrink-0 flex items-center justify-between gap-3">
                            <div className="flex items-baseline gap-2 min-w-0">
                              <PageBreadcrumb
                                pageTitle="P&L"
                                variant="page"
                                align="left"
                                textSize="2xl"
                              />
                            </div>

                            <button
                              type="button"
                              data-no-expand
                              onClick={(e) => {
                                e.stopPropagation();
                                toggleFocus("pnl");
                              }}
                              aria-label={
                                focusedChart === "pnl"
                                  ? "Collapse P&L chart"
                                  : "Expand P&L chart"
                              }
                              title={focusedChart === "pnl" ? "Collapse" : "Expand"}
                              className="hidden lg:inline-flex rounded-md border border-gray-300 bg-white text-blue-700 p-1.5 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                            >
                              {focusedChart === "pnl" ? (
                                <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                              ) : (
                                <RiExpandDiagonalFill size={18} className="font-extrabold" />
                              )}
                            </button>
                          </div>

                          <div className="flex-1 min-h-0 overflow-hidden mt-4">
                            <Bargraph
                              range={range}
                              selectedMonth={selectedMonth}
                              selectedYear={selectedYear}
                              countryName={initialCountryName}
                              homeCurrency={globalHomeCurrency}
                              hideDownloadButton
                              onExportApiReady={setChartExportApi}
                              onNoDataChange={(noData) => setShowNoDataOverlay(noData)}
                              isCollapsed={pnlCollapsed}
                              uploads={bargraphUploads}
                              loading={bargraphLoading}
                              userMeta={bargraphUserMeta}
                              isPreviewMode={isDemoMode}
                            />
                          </div>
                        </div>
                      )}
                    </div>
                  </div>

                  {allDropdownsSelected && (
                    <div className="mt-4">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 items-stretch">
                        <CircleChart
                          range="monthly"
                          month={selectedMonth}
                          selectedQuarter={undefined}
                          year={selectedYear}
                          countryName={initialCountryName}
                          homeCurrency={globalHomeCurrency}
                          onExportBase64Ready={setExpenseBreakdownPieBase64}
                          isPreviewMode={isDemoMode}
                          pnlRowFromSku={bargraphUploads?.[0] ?? null}
                          summaryFromSku={uploadsData?.summary ?? null}
                        />

                        <CMchartofsku
                          range="monthly"
                          month={isDemoMode ? "NA" : selectedMonth}
                          selectedQuarter={undefined}
                          year={isDemoMode ? "NA" : selectedYear}
                          countryName={isDemoMode ? "global" : initialCountryName}
                          homeCurrency={isDemoMode ? "usd" : globalHomeCurrency}
                          onExportBase64Ready={setProductWiseCm1PieBase64}
                          disableInternalFade={isDemoMode}
                          showCm2Toggle={topBottomHasCm2Data}
                        />
                      </div>
                    </div>
                  )}
                </>
              )}

              {/* Quarterly */}
              {range === "quarterly" && isQuarter(selectedQuarter) && selectedYear && (
                <>
                  <div className="w-full rounded-xl space-y-4">
                    <div
                      className={[
                        "grid grid-cols-1 gap-4",
                        focusedChart ? "lg:grid-cols-1" : "lg:grid-cols-2",
                      ].join(" ")}
                    >
                      {/* LEFT card */}
                      {(focusedChart === null || focusedChart === "trend") && (
                        <div
                          className={[
                            "rounded-xl border border-slate-200 bg-white shadow-sm p-4",
                            "cursor-default select-none",
                            focusedChart === "trend" ? "cursor-default" : "",
                          ].join(" ")}
                        >
                          <div className={getTrendWrapperHeight()}>
                            <PerformanceTrendChart
                              range={range}
                              quarter={selectedQuarter}
                              year={selectedYear}
                              countryName={initialCountryName}
                              homeCurrency={globalHomeCurrency}
                              currencySymbol={currencySymbol}
                              data={performanceTrend}
                              metric={performanceTrendMetric}
                              onExportApiReady={setTrendExportApi}
                              isExpanded={focusedChart === "trend"}
                              onToggleExpand={() => toggleFocus("trend")}
                              isPreviewMode={isDemoMode}
                            />
                          </div>
                        </div>
                      )}

                      {/* RIGHT card */}
                      {(focusedChart === null || focusedChart === "pnl") && (
                        <div
                          className={[
                            "rounded-xl border border-slate-200 bg-white shadow-sm p-4",
                            "cursor-default select-none",
                            "min-h-0 overflow-hidden",
                            "flex flex-col",
                            focusedChart === "pnl" ? "cursor-default" : "",
                          ].join(" ")}
                        >
                          <div className="shrink-0 flex items-center justify-between gap-3">
                            <div className="flex items-baseline gap-2">
                              <PageBreadcrumb
                                pageTitle="P&L"
                                variant="page"
                                align="left"
                                textSize="2xl"
                              />
                            </div>

                            <button
                              type="button"
                              data-no-expand
                              onClick={(e) => {
                                e.stopPropagation();
                                toggleFocus("pnl");
                              }}
                              aria-label={
                                focusedChart === "pnl"
                                  ? "Collapse P&L chart"
                                  : "Expand P&L chart"
                              }
                              title={focusedChart === "pnl" ? "Collapse" : "Expand"}
                              className="hidden lg:inline-flex rounded-md border border-gray-300 bg-white text-blue-700 p-1.5 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                            >
                              {focusedChart === "pnl" ? (
                                <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                              ) : (
                                <RiExpandDiagonalFill size={18} className="font-extrabold" />
                              )}
                            </button>
                          </div>

                          <div className="flex-1 min-h-0 overflow-hidden mt-4">
                            <GraphPage
                              range={range}
                              selectedQuarter={selectedQuarter}
                              selectedYear={selectedYear}
                              countryName={initialCountryName}
                              homeCurrency={globalHomeCurrency}
                              hideDownloadButton
                              onExportApiReady={setChartExportApi}
                              onNoDataChange={(noData) => setShowNoDataOverlay(noData)}
                              isCollapsed={pnlCollapsed}
                              uploads={graphPageUploads}
                              loading={graphPageLoading}
                              userMeta={graphPageUserMeta}
                              error={graphPageError}
                              isPreviewMode={isDemoMode}
                            />
                          </div>
                        </div>
                      )}
                    </div>
                  </div>

                  {allDropdownsSelected && (
                    <div className="mt-4">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 items-stretch">
                        <CircleChart
                          range="quarterly"
                          month={undefined}
                          selectedQuarter={selectedQuarter}
                          year={selectedYear}
                          countryName={initialCountryName}
                          homeCurrency={globalHomeCurrency}
                          onExportBase64Ready={setExpenseBreakdownPieBase64}
                          isPreviewMode={isDemoMode}
                          pnlRowFromSku={bargraphUploads?.[0] ?? null}
                          summaryFromSku={uploadsData?.summary ?? null}
                        />

                        <CMchartofsku
                          range="quarterly"
                          month={undefined}
                          selectedQuarter={isDemoMode ? undefined : selectedQuarter}
                          year={isDemoMode ? "NA" : selectedYear}
                          countryName={isDemoMode ? "global" : initialCountryName}
                          homeCurrency={isDemoMode ? "usd" : globalHomeCurrency}
                          onExportBase64Ready={setProductWiseCm1PieBase64}
                          disableInternalFade={isDemoMode}
                          showCm2Toggle={topBottomHasCm2Data}
                        />
                      </div>
                    </div>
                  )}
                </>
              )}

              {/* Yearly */}
              {allDropdownsSelected && range === "yearly" && selectedYear && (
                <>
                  <div className="w-full rounded-xl space-y-4">
                    <div
                      className={[
                        "grid grid-cols-1 gap-4",
                        focusedChart ? "lg:grid-cols-1" : "lg:grid-cols-2",
                      ].join(" ")}
                    >
                      {/* LEFT card */}
                      {(focusedChart === null || focusedChart === "trend") && (
                        <div
                          className={[
                            "rounded-xl border border-slate-200 bg-white shadow-sm p-4",
                            "cursor-default select-none",
                            focusedChart === "trend" ? "cursor-default" : "",
                          ].join(" ")}
                        >
                          <div className={getTrendWrapperHeight()}>
                            <PerformanceTrendChart
                              range={range}
                              year={selectedYear}
                              countryName={initialCountryName}
                              homeCurrency={globalHomeCurrency}
                              currencySymbol={currencySymbol}
                              data={performanceTrend}
                              metric={performanceTrendMetric}
                              onExportApiReady={setTrendExportApi}
                              isExpanded={focusedChart === "trend"}
                              onToggleExpand={() => toggleFocus("trend")}
                              isPreviewMode={isDemoMode}
                            />
                          </div>
                        </div>
                      )}

                      {/* RIGHT card */}
                      {(focusedChart === null || focusedChart === "pnl") && (
                        <div
                          className={[
                            "rounded-xl border border-slate-200 bg-white shadow-sm p-4",
                            "cursor-default select-none",
                            "min-h-0 overflow-hidden",
                            "flex flex-col",
                            focusedChart === "pnl" ? "cursor-default" : "",
                          ].join(" ")}
                        >
                          <div className="shrink-0 flex items-center justify-between gap-3">
                            <div className="flex items-baseline gap-2">
                              <PageBreadcrumb
                                pageTitle="P&L"
                                variant="page"
                                align="left"
                                textSize="2xl"
                              />
                            </div>

                            <button
                              type="button"
                              data-no-expand
                              onClick={(e) => {
                                e.stopPropagation();
                                toggleFocus("pnl");
                              }}
                              aria-label={
                                focusedChart === "pnl"
                                  ? "Collapse P&L chart"
                                  : "Expand P&L chart"
                              }
                              title={focusedChart === "pnl" ? "Collapse" : "Expand"}
                              className="hidden lg:inline-flex rounded-md border border-gray-300 bg-white text-blue-700 p-1.5 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                            >
                              {focusedChart === "pnl" ? (
                                <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                              ) : (
                                <RiExpandDiagonalFill size={18} className="font-extrabold" />
                              )}
                            </button>
                          </div>

                          <div className="flex-1 min-h-0 overflow-hidden mt-4">
                            <GraphPage
                              range={range}
                              selectedYear={selectedYear}
                              countryName={initialCountryName}
                              homeCurrency={globalHomeCurrency}
                              hideDownloadButton
                              onExportApiReady={setChartExportApi}
                              onNoDataChange={(noData) => setShowNoDataOverlay(noData)}
                              isCollapsed={pnlCollapsed}
                              uploads={graphPageUploads}
                              loading={graphPageLoading}
                              userMeta={graphPageUserMeta}
                              error={graphPageError}
                              isPreviewMode={isDemoMode}
                            />
                          </div>
                        </div>
                      )}
                    </div>
                  </div>

                  <div className="mt-4">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 items-stretch">
                      <CircleChart
                        range="yearly"
                        month={undefined}
                        selectedQuarter={undefined}
                        year={selectedYear}
                        countryName={initialCountryName}
                        homeCurrency={globalHomeCurrency}
                        onExportBase64Ready={setExpenseBreakdownPieBase64}
                        disableInternalFade={isDemoMode}
                        isPreviewMode={isDemoMode}
                        pnlRowFromSku={bargraphUploads?.[0] ?? null}
                        summaryFromSku={uploadsData?.summary ?? null}
                      />

                      <CMchartofsku
                        range="yearly"
                        month={undefined}
                        selectedQuarter={undefined}
                        year={isDemoMode ? "NA" : selectedYear}
                        countryName={isDemoMode ? "global" : initialCountryName}
                        homeCurrency={isDemoMode ? "usd" : globalHomeCurrency}
                        onExportBase64Ready={setProductWiseCm1PieBase64}
                        disableInternalFade={isDemoMode}
                        showCm2Toggle={topBottomHasCm2Data}
                      />
                    </div>
                  </div>
                </>
              )}
            </div>

          )}

          {/* ---------- TAB 2: BUSINESS SUMMARY ---------- */}
          {activeTab === "businessSummary" && allDropdownsSelected && (

            <div
              id="ai-insights"
              className="scroll-mt-[80px] space-y-5"
            >
              {/* ✅ Loader INSIDE the white container */}
              {aiPanelLoading ? (
                <div className="min-h-[420px] flex items-center justify-center">
                  {/* IMPORTANT: force inline, not fullscreen */}
                  <Loader fullscreen={false} transparent />
                </div>
              ) : aiPanelError ? (
                <div className="w-full rounded-2xl border-2 border-red-200 bg-red-50 p-6 space-y-2">
                  <div className="flex items-center gap-2">
                    <span className="text-lg">⚠️</span>
                    <p className="font-semibold text-red-700">Unable to Generate Insights</p>
                  </div>
                  <p className="text-sm text-red-600">{aiPanelError}</p>
                </div>
              ) : (
                <>
                  {/* {aiPanel?.objective && (
  <MonthlyObjectiveStrip
    objective={aiPanel.objective}
    targetSummary={targetSummary}
    currencySymbol={currencySymbol}
  />
)} */}
                  <AiSingleInsightCard
                    loading={false}
                    error={null}
                    summaryBullets={aiPanel?.summaryBullets ?? []}
                    recommendationBullets={aiPanel?.recommendationBullets ?? []}
                    skuInsightsBullets={aiPanel?.skuInsightsBullets ?? []}
                    inventoryBullets={aiPanel?.inventoryBullets ?? []}
                    recommendationsMap={aiPanel?.recommendationsMap}
                    objective={aiPanel?.objective}
                    remainingSkusRecommendation={aiPanel?.remainingSkusRecommendation}
                    nameToSkuMap={nameToSkuMap}
                    range={range}
                    selectedYear={selectedYear}
                    selectedQuarter={selectedQuarter}
                    selectedMonth={selectedMonth}
                    homeCurrency={globalHomeCurrency}
                    countryName={initialCountryName}
                    portfolioRecommendation={aiPanel?.portfolioRecommendation}
                    targetSummary={targetSummary}
                    currencySymbol={currencySymbol}
                    otherSkuIncludedProducts={aiPanel?.otherSkuIncludedProducts}
                  />
                </>
              )}
            </div>

          )}

          {/* ---------- TAB 3: SKU / PRODUCTWISE P&L ---------- */}
          {activeTab === "skuBreakdown" && allDropdownsSelected && (

            <div id="pnl-breakdown" className="mt-4 space-y-4 scroll-mt-[80px]">
              <SKUtable
                range={range || "yearly"}
                month={range === "monthly" ? selectedMonth : ""}
                quarter={range === "quarterly" ? selectedQuarter : ""}
                year={selectedYear}
                countryName={isDemoMode ? "global" : initialCountryName}
                homeCurrency={isDemoMode ? "usd" : globalHomeCurrency}
                rows={displaySkuRows}
                loading={displaySkuLoading}
                error={displaySkuError}
                noDataFound={displaySkuNoDataFound}
                userMeta={{
                  brand_name: userData?.brand_name,
                  company_name: userData?.company_name,
                }}
                onExportPayloadChange={handleSkuExportPayloadChange}
                onProductDetailClick={openAiProductDrawerByName}
              />

              {!displaySkuNoDataFound && displaySkuRows.length > 0 && (
                <SkuTopBottomTables
                  topData={topData}
                  bottomData={bottomData}
                  currencySymbol={currencySymbol}
                  previewMode={isDemoMode}
                  hasCm2Data={topBottomHasCm2Data}
                />
              )}
            </div>

          )}

          {activeTab === "skuwiseProfit" && allDropdownsSelected && (
            <div id="sku-journey" className="mt-4 scroll-mt-[80px]">
              {(() => {
                const productWiseRange =
                  range === "quarterly" ? "quarterly" : "yearly";

                const hasRealSkuRowsForProductWise =
                  !isDemoMode &&
                  !skuNoDataFound &&
                  Array.isArray(skuRows) &&
                  skuRows.some((row: any) => {
                    const productName = String(row?.product_name || "").trim().toLowerCase();
                    const sku = String(row?.sku || "").trim().toLowerCase();

                    const isTotalRow = productName === "total" || sku === "total";
                    if (isTotalRow) return false;

                    return (
                      toNum(row?.net_sales) !== 0 ||
                      toNum(row?.total_quantity) !== 0 ||
                      toNum(row?.quantity) !== 0 ||
                      toNum(row?.profit) !== 0 ||
                      toNum(row?.cm2_profit) !== 0
                    );
                  });

                const firstInsightProductName =
                  parseProductInsightsBlocks(
                    aiPanel?.skuInsightsBullets ?? [],
                    productWiseRange as RangeType
                  )?.[0]?.name || "";

                const productWiseInitialProductName = isDemoMode
                  ? defaultTopProductName || firstInsightProductName || "Demo Product A"
                  : hasRealSkuRowsForProductWise
                    ? defaultTopProductName || firstInsightProductName
                    : firstInsightProductName;

                return (
                  <ProductwisePerformance
                    key={[
                      initialCountryName,
                      productWiseRange,
                      selectedQuarter,
                      selectedYear,
                      productWiseInitialProductName || "no-product",
                      isDemoMode ? "demo" : "live",
                    ].join("-")}
                    embedded
                    countryNameProp={isDemoMode ? "global" : initialCountryName}
                    rangeProp={productWiseRange}
                    selectedMonthProp={isDemoMode ? "NA" : ""}
                    selectedQuarterProp={
                      isDemoMode
                        ? ""
                        : productWiseRange === "quarterly"
                          ? selectedQuarter
                          : ""
                    }
                    selectedYearProp={
                      isDemoMode ? ("NA" as any) : selectedYear ? Number(selectedYear) : ""
                    }
                    initialProductName={productWiseInitialProductName}

                    // ✅ Same source as Dropdown drawer
                    sharedInsightData={{
                      blocks: parseProductInsightsBlocks(
                        aiPanel?.skuInsightsBullets ?? [],
                        productWiseRange as RangeType
                      ),
                      objective: aiPanel?.objective ?? null,
                      recommendationsMap: aiPanel?.recommendationsMap,
                      drawerPeriodText: aiPanel?.summaryBullets?.[0]
                        ? formatSummaryPeriod(aiPanel.summaryBullets[0])
                        : "",
                    }}
                  />
                );
              })()}
            </div>
          )}

          {activeTab === "cashFlow" && allDropdownsSelected && (

            <div id="cash-flow" className="mt-4 scroll-mt-[80px]">
              <CashFlowPage
                embedded
                countryNameProp={isDemoMode ? "global" : initialCountryName}
                rangeProp={range as "monthly" | "quarterly" | "yearly"}
                selectedMonthProp={isDemoMode ? "NA" : range === "monthly" ? selectedMonth : ""}
                selectedQuarterProp={isDemoMode ? "" : range === "quarterly" ? selectedQuarter : ""}
                selectedYearProp={isDemoMode ? "NA" : selectedYear}
              />
            </div>

          )}

          {/* ---------- TAB: INVENTORY INSIGHTS ---------- */}
          {activeTab === "inventoryInsights" && allDropdownsSelected && (
            <div id="inventory-insights" className="mt-4 scroll-mt-[80px]">
              {inventoryInsightsLoading ? (
                <div className="min-h-[420px] flex items-center justify-center">
                  <Loader fullscreen={false} transparent />
                </div>
              ) : inventoryInsightsError ? (
                <div className="w-full rounded-2xl border-2 border-red-200 bg-red-50 p-6 space-y-2">
                  <div className="flex items-center gap-2">
                    <span className="text-lg">⚠️</span>
                    <p className="font-semibold text-red-700">
                      Unable to Load Inventory Insights
                    </p>
                  </div>

                  <p className="text-sm text-red-600">{inventoryInsightsError}</p>
                </div>
              ) : inventoryInsightsData ? (
                <>
                  <div className="mb-4 flex items-center justify-between gap-4">
                    <PageBreadcrumb
                      pageTitle="Inventory Insights"
                      variant="page"
                      align="left"
                      textSize="2xl"
                    />

                    {isGlobalPage && (
                      <SegmentedToggle
                        value={selectedGlobalInventoryCountry}
                        onChange={(val) => setSelectedGlobalInventoryCountry(String(val))}
                        options={[
                          { value: "uk", label: "UK" },
                          { value: "us", label: "US" },
                        ]}
                        compact
                        textSizeClass="text-xs"
                      />
                    )}
                  </div>

                  <InventoryInsightsSection
                    heatmapBuckets={inventoryInsightsData.heatmapBuckets}
                    heatmapData={inventoryInsightsData.heatmapData}
                    donutData={inventoryInsightsData.donutData}
                    donutTotalUnits={inventoryInsightsData.donutTotalUnits}
                    trendSelectedBucket={inventoryInsightsData.trendSelectedBucket}
                    trendData={inventoryInsightsData.trendData}
                    trendLineColor={inventoryInsightsData.trendLineColor}
                    trendBucketOptions={inventoryInsightsData.trendBucketOptions}
                    trendAllSeriesData={inventoryInsightsData.trendAllSeriesData}
                    actions={inventoryInsightsData.actions}
                    actionLogic={inventoryInsightsData.actionLogic}
                    onHeatmapProductClick={handleHeatmapProductClick}
                    showInventoryAlerts={false}
                    inventoryAgeSummary={inventoryInsightsData.inventoryAgeSummary}
                    heatmapExcelFilename={getInventoryInsightsFileName()}
                    heatmapExcelTitleLine="Inventory Insights Report"
                    heatmapExcelCountryLabel={formatInventoryExcelCountryLabel(
                      getInventoryInsightsReportCountry()
                    )}
                    heatmapExcelPeriodLabel={getInventoryInsightsPeriodLabel()}
                    salesLast30DaysLabel={salesLast30DaysLabel}
                    unitSalesDataKey={inventoryHeatmapUnitSalesDataKey}
                    useCurrentInventoryTableLayout={showUsCurrentInventoryTable}
                    storageCostCurrencySymbol={currencySymbol}
                  />
                </>
              ) : (
                <div className="rounded-xl border border-slate-200 bg-white p-6 text-sm text-slate-600">
                  No inventory insights found for the selected period.
                </div>
              )}
            </div>
          )}
        </div>
      </PreviewLockedSection>

      {/* ===================== YOUR EXISTING OVERLAYS / MODALS (KEEP) ===================== */}
      {/* {showNoDataOverlay && (
        <div
          className="fixed inset-y-0 z-[9999] flex items-center justify-center pointer-events-none"
          style={{ left: overlayBounds.left, width: overlayBounds.width || "100%" }}
        >
    
        </div>
      )} */}

      <Modal
        isOpen={showUploadModal}
        onClose={() => setShowUploadModal(false)}
        className="max-w-3xl w-[90vw] mx-auto p-0 shadow-[6px_6px_7px_0px_#00000026] border border-[#D9D9D9]"
        showCloseButton
      >
        <div className="max-h-[85vh] overflow-y-auto">
          <FileUploadForm
            initialCountry={initialCountryName}
            onClose={() => setShowUploadModal(false)}
            onComplete={() => {
              setShowUploadModal(false);
              fetchUploadHistory(range, selectedMonth, selectedQuarter || "", selectedYear, initialCountryName);
            }}
          />
        </div>
      </Modal>



      <AmazonFetchSuccessModal
        isOpen={showAmazonFetchSuccess}
        onClose={handleCloseAmazonFetchSuccess}
        onConnectAds={handleOpenAmazonAdsFromSuccess}
        country={adsCountry}
      />

      {showAmazonAdsConnect && (
        <AmazonAdsConnect
          onClose={() => setShowAmazonAdsConnect(false)}
          onConnected={onConnectOrSyncAds}
          country={adsCountry}
        />
      )}

      <RightProductDrawer
        open={!!selectedAiProductBlock}
        onClose={() => {
          setSelectedAiProductBlock(null);
          setSelectedAiProductRecObj(null);
          setAiBestPerformanceData(null);
          setAiBestPerformanceError(null);
        }}
        block={selectedAiProductBlock}
        objective={aiPanel?.objective}
        recObj={selectedAiProductRecObj}
        countryName={countryName}
        range={range}
        year={selectedYear}
        month={range === "monthly" ? selectedMonth : ""}
        quarter={range === "quarterly" ? selectedQuarter : ""}
        drawerPeriodText={
          aiPanel?.summaryBullets?.[0]
            ? formatSummaryPeriod(aiPanel.summaryBullets[0])
            : ""
        }
        currencySymbol={currencySymbol}
        bestPerformanceLoading={aiBestPerformanceLoading}
        bestPerformanceError={aiBestPerformanceError}
        bestPerformanceData={aiBestPerformanceData}
      />
    </div>
  );

};

export default Dropdowns;
