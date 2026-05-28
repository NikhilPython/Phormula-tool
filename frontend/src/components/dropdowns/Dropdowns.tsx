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

/* ---------------------- Types ---------------------- */
type Summary = {
  unit_sold: number;
  total_sales: number;
  gross_sales?: number;
  total_product_sales?: number;
  total_expense: number;
  cm2_profit: number;
  total_cous?: number;
  otherwplatform?: number;
  advertising_total?: number;
  total_amazon_fee?: number;
};

type UploadRow = {
  country: string;
  month: string;
  year: string | number;
  total_sales: number;
  total_amazon_fee: number;
  total_cous: number;
  advertising_total: number;
  otherwplatform: number;
  taxncredit?: number;
  cm2_profit: number;
  total_profit: number;
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

// const markFetched = (year: string, month?: string) => {
//   if (typeof window === "undefined") return;
//   const y = String(year);
//   const m = month ? month.toLowerCase() : "";

//   const fp = readFetchedPeriods();
//   if (!fp[y]) fp[y] = [];
//   if (m && !fp[y].includes(m)) fp[y].push(m);

//   // keep months sorted
//   fp[y] = fp[y]
//     .filter(Boolean)
//     .sort((a, b) => (monthIndexMap[a] ?? 99) - (monthIndexMap[b] ?? 99));

//   writeFetchedPeriods(fp);

//   if (m) {
//     localStorage.setItem("latestFetchedPeriod", JSON.stringify({ year: y, month: m }));
//   }
// };

// const computeDefaultYearlyYear = () => {
//   if (typeof window !== "undefined") {
//     try {
//       const raw = localStorage.getItem("latestFetchedPeriod");
//       if (raw) {
//         const parsed = JSON.parse(raw);
//         const y = String(parsed?.year || "").trim();
//         if (y) return y;
//       }
//     } catch { }
//   }

//   // Fallback: latest completed month, not current calendar year
//   const now = new Date();
//   const latestCompleted = new Date(now.getFullYear(), now.getMonth() - 1, 1);
//   return String(latestCompleted.getFullYear());
// };

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
    month: lastCompleted.month,
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
  const v = (value || "").trim();
  const m = v.match(/^(.+?)\s*(\(([-+])[^)]+\))\s*$/);

  if (!m) {
    return { main: v, delta: "", deltaColor: "" };
  }

  const main = m[1].trim();
  const delta = m[2].trim();
  const sign = m[3];

  const deltaColor = sign === "+" ? "text-emerald-600" : "text-red-600";
  return { main, delta, deltaColor };
};


const monthNameToNumber = (m: string): string => {
  const idx = monthIndexMap[(m || "").toLowerCase()];
  return typeof idx === "number" ? String(idx + 1) : "";
};

type ProductInsightBlock = {
  name: string;
  skuKey?: string;
  metrics: { label: string; value: string; color?: string }[];
  journeyBullets: string[];
  recommendationBullets: string[];
  inventoryBullets: string[];
  isOtherSkus?: boolean;
};


const parseProductInsightsBlocks = (lines: string[]): ProductInsightBlock[] => {
  const metricLabels = ["ASP", "Units", "Net sales", "CM1 profit", "CM1 profit per unit"];

  const isMetric = (s: string) =>
    metricLabels.some((m) => s.toLowerCase().startsWith(m.toLowerCase() + ":"));

  const blocks: ProductInsightBlock[] = [];
  let current: ProductInsightBlock | null = null;

  const pushCurrent = () => {
    if (current && current.name.trim()) blocks.push(current);
    current = null;
  };

  let inJourney = false;

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
      !line.toLowerCase().startsWith("recommendation:") &&
      !line.toLowerCase().startsWith("product journey") &&
      !line.toLowerCase().startsWith("inventory action:") &&
      !!nextLine &&
      isMetric(nextLine);

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
        journeyBullets: [],
        recommendationBullets: [],
        inventoryBullets: [],
        isOtherSkus: isOther,
      };

      inJourney = false;
      continue;
    }

    if (!current) continue;

    if (line.toLowerCase().startsWith("product journey")) {
      inJourney = true;
      continue;
    }

    if (line.toLowerCase().startsWith("recommendation:")) {
      inJourney = false;
      const reco = line.replace(/^recommendation:\s*/i, "").trim();
      if (reco) current.recommendationBullets.push(reco);
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

      current.metrics.push({ label: label.trim(), value, color });
      continue;
    }

    // journey bullets
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

const buildOtherSkusInsightLines = (
  apiData: any,
  currencySymbol = "$",
  countryName = "global"
): string[] => {
  const otherComparison = apiData?.global_ai?.other_skus_comparison;
  const remainingAgg = apiData?.metrics?.remaining_agg;

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

    `CM1 profit: ${formatMoney(
      remainingAgg?.profit?.current,
      currencySymbol
    )} ${formatPct(remainingAgg?.profit?.delta_pct)}`,

    `CM1 profit per unit: ${formatMoney(
      remainingAgg?.unit_wise_profitability?.current,
      currencySymbol
    )} ${formatPct(remainingAgg?.unit_wise_profitability?.delta_pct)}`,

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
};

const metricColors = [
  "border border-[#FDD36F] border-t-[#FDD36F]",
  "border border-[#75BBDA] border-t-[#75BBDA]",
  "border border-[#B75A5A] border-t-[#B75A5A]",
  "border border-[#7B9A6D] border-t-[#7B9A6D]",
  "border border-[#C49466] border-t-[#C49466]",
];

const metricOrder = [
  "units",
  "net sales",
  "asp",
  "cm1 profit",
  "cm1 profit per unit",
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
}) => {
  const inventoryText =
    recObj?.inventory_recommendation || block?.inventoryBullets?.join(" ");

  const inventoryRecoBullets = toBullets(inventoryText);
  const adsRecoBullets = toBullets(recObj?.ads_recommendation);
  const periodBadge = getPeriodBadge(range, year, month, quarter);
  const shouldHideGraphForOtherSkus = !!block?.isOtherSkus;
  const sortedMetrics = [...(block?.metrics || [])].sort((a, b) => {
    const aIndex = metricOrder.indexOf(a.label.toLowerCase());
    const bIndex = metricOrder.indexOf(b.label.toLowerCase());

    const safeAIndex = aIndex === -1 ? Number.MAX_SAFE_INTEGER : aIndex;
    const safeBIndex = bIndex === -1 ? Number.MAX_SAFE_INTEGER : bIndex;

    return safeAIndex - safeBIndex;
  });

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
                    <div className="flex flex-wrap items-center gap-1 sm:ml-1">
                      <span className="text-base font-bold text-green-500 sm:text-xl lg:text-lg 2xl:text-2xl">
                        {block.name || "Details"}
                      </span>

                      {drawerPeriodText ? (
                        <span className="text-base font-bold text-green-500 sm:text-xl lg:text-lg 2xl:text-2xl">
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

              <div className="flex-1 space-y-6 overflow-y-auto px-3">
                <div>
                  <div className="mb-2 text-xs font-semibold text-charcoal-700 sm:text-sm 2xl:text-lg text-charcoal-700">
                    Metrics
                  </div>

                  <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 2xl:grid-cols-5">
                    {sortedMetrics.map((m, i) => (
                      <div
                        key={i}
                        className={`rounded-lg border border-t-4 ${metricColors[i % metricColors.length]} px-3 py-2`}
                      >
                        <div className="text-[10px] text-charcoal-400 2xl:text-xs">
                          {m.label
                            .replace(/\b\w/g, (char) => char.toUpperCase())
                            .replace("Cm1", "CM1")}
                        </div>

                        <div className="flex flex-col leading-tight">
                          {(() => {
                            const { main, delta, deltaColor } = splitMetricValue(m.value);

                            return (
                              <>
                                {/* 2nd line: value */}
                                <span
                                  className="text-sm font-bold 2xl:text-lg"
                                  style={{ color: "#414042" }}
                                >
                                  {main}
                                </span>

                                {delta && (
                                  <span
                                    className="text-[10px] 2xl:text-xs font-semibold"
                                    style={{
                                      color:
                                        deltaColor === "text-emerald-600"
                                          ? "#5EA68E"
                                          : "#FF5C5C",
                                    }}
                                  >
                                    {delta}
                                  </span>
                                )}
                              </>
                            );
                          })()}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                <div>
                  <div className="mb-2 text-xs font-semibold text-charcoal-500 sm:text-sm 2xl:text-lg">
                    Recommendations
                  </div>

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

                  {adsRecoBullets.length ? (
                    <div className="mt-2">
                      <div className="text-xs font-semibold text-charcoal-500 2xl:text-sm">
                        Advertising
                      </div>
                      <ul className="list-disc space-y-1 pl-5 text-xs text-charcoal-500 2xl:text-sm">
                        {adsRecoBullets.map((pt, i) => (
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
                        {inventoryRecoBullets.map((pt, i) => (
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

                {!shouldHideGraphForOtherSkus && (
                  <div className="w-full">
                    <Productinfoinpopup
                      productname={block.name}
                      countryName={countryName}
                    />
                  </div>
                )}

                <div className="pb-4">
                  <div className="flex items-center gap-1 flex-wrap">
                    <PageBreadcrumb
                      pageTitle="Product Journey"
                      variant="page"
                      textSize="lg"
                    />
                  </div>

                  {block.journeyBullets?.length ? (
                    <ul className="list-disc pl-5 space-y-1 text-xs text-charcoal-500 2xl:text-sm">
                      {block.journeyBullets.map((p, i) => (
                        <li key={i}>
                          {p
                            .replace(/^\d+\.\s*-\s*/, "")
                            .replace(/^\d+\.\s*/, "")
                            .replace(/^-+\s*/, "")}
                        </li>
                      ))}
                    </ul>
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
  drawerPeriodText,
  selectedMonth,
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
}) => {
  const [selectedBlock, setSelectedBlock] = useState<ProductInsightBlock | null>(null);
  const [selectedRecObj, setSelectedRecObj] = useState<any>(null);

  const [perfLoading, setPerfLoading] = useState(false);
  const [perfError, setPerfError] = useState<string | null>(null);
  const [perfData, setPerfData] = useState<any>(null);

  const [perfMetric, setPerfMetric] = useState<"net_sales" | "units">("net_sales");

  const hasBlocks = blocks.length > 0; // ✅ compute instead of early return

  const sortedBlocks = useMemo(() => {
    return [...blocks].sort((a, b) => {
      const aIsOther = a.isOtherSkus || a.name.trim().toLowerCase() === "other skus";
      const bIsOther = b.isOtherSkus || b.name.trim().toLowerCase() === "other skus";

      // Always keep Other SKUs at the end
      if (aIsOther && !bIsOther) return 1;
      if (!aIsOther && bIsOther) return -1;

      // Sort all normal products by Net Sales descending
      return getBlockNetSales(b) - getBlockNetSales(a);
    });
  }, [blocks]);

  const topBorderColors = ["border-t-blue-500", "border-t-amber-500", "border-t-emerald-500", "border-t-rose-500"];

  const skuActions =
    (recommendationsMap as any)?.sku_actions ??
    (recommendationsMap as any)?.recommendations ??
    recommendationsMap ??
    {};

  useEffect(() => {

    if (!hasBlocks) return;
    if (!selectedBlock) return;
    if (selectedBlock.isOtherSkus) return;

    const ac = new AbortController();

    (async () => {
      try {
        setPerfLoading(true);
        setPerfError(null);
        setPerfData(null);

        const token = typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
        if (!token) throw new Error("Missing token");

        const productKeyForApi = selectedBlock.name;
        const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductwisePerformance`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify({
            country: countryName,
            product_name: selectedBlock.name,
            time_range: "Yearly",
            year: Number(selectedYear),
            quarter: undefined,
            home_currency: homeCurrency,
          }),
          cache: "no-store",
          signal: ac.signal,
        });

        const json = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(json?.error || "Failed to fetch product performance");

        const pickSeries = (j: any) => {
          const d = j?.data;
          if (!d || typeof d !== "object") return null;

          const country = (countryName || "").toLowerCase();
          const keys = Object.keys(d);

          if (country && country !== "global") {
            const match =
              keys.find(k => k.toLowerCase() === country) ||
              keys.find(k => k.toLowerCase().startsWith(country + "_")) ||
              keys.find(k => k.toLowerCase().startsWith(country));

            if (match) return d[match];
          }

          const g = keys.find(k => k.toLowerCase().startsWith("global"));
          return g ? d[g] : d[keys[0]];
        };

        const rows = pickSeries(json);

        setPerfData({
          rows: Array.isArray(rows)
            ? rows.map((r: any) => ({
              x: r?.month ?? r?.label ?? "-",
              y: perfMetric === "units"
                ? toNum(r?.quantity ?? r?.units ?? 0)
                : toNum(r?.net_sales ?? 0),
            }))
            : [],
        });
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
    range,
    selectedYear,
    selectedQuarter,
    homeCurrency,
    nameToSkuMap,
    countryName,
    perfMetric,
  ]);

  if (!hasBlocks) return null;

  const openDrawer = (b: ProductInsightBlock) => {
    const mappedSku = nameToSkuMap?.[normalizeKey(b.name)];
    const skuKey = b.skuKey || mappedSku;

    const recObj =
      (skuKey && (skuActions as any)[skuKey]) ||
      (skuActions as any)[b.name] ||
      (skuActions as any)[b.name.trim()] ||
      null;

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

          const sortedCardMetrics = [...(b.metrics || [])].sort((a, b) => {
            const aIndex = metricOrder.indexOf(a.label.trim().toLowerCase());
            const bIndex = metricOrder.indexOf(b.label.trim().toLowerCase());

            const safeAIndex = aIndex === -1 ? Number.MAX_SAFE_INTEGER : aIndex;
            const safeBIndex = bIndex === -1 ? Number.MAX_SAFE_INTEGER : bIndex;

            return safeAIndex - safeBIndex;
          });

          return (
            <motion.div
              key={idx}
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
                <div className="text-sm font-semibold text-slate-800">
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
                    <div
                      key={i}
                      className="rounded-lg border border-slate-200 bg-slate-50 py-2 px-1 min-w-0"
                    >
                      <div className="text-[10px] 2xl:text-xs text-slate-500 leading-none truncate">
                        {m.label}
                      </div>

                      {(() => {
                        const { main, delta, deltaColor } = splitMetricValue(m.value);

                        return (
                          <div className="mt-1 flex flex-col min-[1700px]:flex-row  2xl:items-baseline gap-0.5 2xl:gap-1 min-w-0">
                            <span className="text-[10px] 2xl:text-xs font-bold text-slate-900 truncate">
                              {main}
                            </span>

                            {delta ? (
                              <span className={`text-[10px] 2xl:text-xs font-semibold shrink-0 ${deltaColor}`}>
                                {delta}
                              </span>
                            ) : null}
                          </div>
                        );
                      })()}
                    </div>
                  ))}
                </div>
              )}
              {b.recommendationBullets?.length > 0 && (
                <div className="space-y-1 text-xs 2xl:text-sm text-slate-700 leading-relaxed">
                  {b.recommendationBullets.map((line, i) => (
                    <p key={i}>{line}</p>
                  ))}
                </div>
              )}
            </motion.div>
          );
        })}
      </div>

      <RightProductDrawer
        open={!!selectedBlock}
        onClose={() => {
          setSelectedBlock(null);
          setSelectedRecObj(null);
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
      valueClass: "text-slate-800",
    },
    {
      label: "Profit",
      value: profit,
      accent: "bg-amber-500",
      valueClass: "text-slate-800",
    },
    {
      label: "Inventory Dilution",
      value: inventory,
      accent: "bg-violet-500",
      valueClass: "text-slate-800",
    },
    {
      label: "Target Set",
      value: targetSet,
      accent: "bg-emerald-500",
      valueClass: "text-slate-800",
    },
    {
      label: "Shortfall",
      value: shortfall,
      accent: "bg-rose-500",
      valueClass: shortfallValue > 0 ? "text-rose-600" : "text-slate-800",
    },
    {
      label: "Cash Flow",
      value: cashFlow,
      accent: "bg-cyan-500",
      valueClass: "text-slate-800",
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
              <div className="text-[11px] 2xl:text-xs font-medium text-slate-500 leading-tight">
                {item.label}
              </div>

              <div
                className={`mt-2 text-sm font-semibold leading-snug capitalize break-words ${item.valueClass}`}
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


  const narrativeInsights = summaryBullets.filter((l) => !l.includes(":"));

  const summaryBulletPoints = splitSummaryIntoBulletPoints(
    narrativeInsights.slice(1)
  );

  const drawerPeriodText =
    narrativeInsights?.[0] ? formatSummaryPeriod(narrativeInsights[0]) : "";

  const hasNoAiData =
    !summaryBullets.length &&
    !skuInsightsBullets.length &&
    !inventoryBullets.length &&
    !recommendationBullets.length;

  return (
    <div className="flex flex-col gap-5">
      <div className="w-full space-y-4">
        {!hasNoAiData && (narrativeInsights.length > 0 || objective) && (

          <div className="grid grid-cols-1 xl:grid-cols-12 gap-4 items-stretch">
            {narrativeInsights.length > 0 && (
              <div className="xl:col-span-7 rounded-xl border border-slate-200 bg-white shadow-sm p-4 h-full">
                <div className="space-y-3 h-full">
                  <h2 className="text-base sm:text-xl lg:text-lg 2xl:text-2xl text-charcoal-500 font-bold leading-snug">
                    {narrativeInsights[0]?.split("(")[0]?.trim()}
                    <span className="text-[#5EA68E] font-bold ml-2 text-base sm:text-xl lg:text-lg 2xl:text-2xl">
                      {formatSummaryPeriod(narrativeInsights[0])}
                    </span>
                  </h2>

                  <ul className="list-disc pl-5 text-xs 2xl:text-sm text-slate-700 leading-relaxed space-y-2">
                    {summaryBulletPoints.map((line, i) => (
                      <li key={i}>{line}</li>
                    ))}
                  </ul>

                  {portfolioRecommendation ? (
                    <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50 p-3">
                      <div className="text-xs font-semibold text-slate-700 mb-1">
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
        {!hasNoAiData && parseProductInsightsBlocks(skuInsightsBullets).length > 0 && (
          <div className="w-full rounded-xl border border-slate-200 bg-white shadow-sm p-4">
            <div className="space-y-5">
              <ProductInsightsSection
                blocks={parseProductInsightsBlocks(skuInsightsBullets)}
                objective={objective}
                recommendationsMap={recommendationsMap}
                nameToSkuMap={nameToSkuMap}
                range={range}
                selectedYear={selectedYear}
                selectedQuarter={selectedQuarter}
                selectedMonth={range === "monthly" ? selectedMonth : ""}
                homeCurrency={homeCurrency}
                countryName={countryName}
                drawerPeriodText={drawerPeriodText}
              />
            </div>
          </div>
        )}



        {/* Inventory Section */}
        {!hasNoAiData && inventoryBullets.length > 0 && (
          <div className="space-y-4 rounded-xl border border-slate-200 bg-white shadow-sm p-4">
            <div className="flex items-center gap-2">
              <span className="text-base 2xl:text-2xl font-bold text-slate-800">
                Inventory Insights
              </span>
            </div>

            {(() => {
              const isGlobalInventory = countryName.toLowerCase() === "global";

              const getCountryTitle = () => {
                const c = countryName.toLowerCase();

                if (c === "uk") return "UK Inventory";
                if (c === "us") return "US Inventory";
                if (c === "global") return "Global Inventory";

                return `${countryName.toUpperCase()} Inventory`;
              };

              const toTitleLabel = (value: string) => {
                return String(value || "")
                  .trim()
                  .replace(/\s+/g, " ")
                  .replace(/\b\w/g, (char) => char.toUpperCase())
                  .replace(/\bSkus\b/g, "SKUs")
                  .replace(/\bSku\b/g, "SKU")
                  .replace(/\bUk\b/g, "UK")
                  .replace(/\bUs\b/g, "US");
              };

              const parseInventoryLine = (line: string) => {
                const raw = String(line || "").trim();

                if (
                  raw.toLowerCase().includes("for detailed inventory insights") ||
                  raw.toLowerCase().includes("inventory reconciliation tab")
                ) {
                  return null;
                }

                const colonIdx = raw.indexOf(":");

                const left = colonIdx > -1 ? raw.slice(0, colonIdx).trim() : raw;
                const right = colonIdx > -1 ? raw.slice(colonIdx + 1).trim() : "";

                const country = /^UK\b/i.test(raw)
                  ? "UK"
                  : /^US\b/i.test(raw)
                    ? "US"
                    : "";

                const cleanLabel = toTitleLabel(
                  left
                    .replace(/^UK\s+/i, "")
                    .replace(/^US\s+/i, "")
                    .trim()
                );

                return {
                  country,
                  label: cleanLabel,
                  value: right,
                };
              };

              type InventoryItem = NonNullable<ReturnType<typeof parseInventoryLine>>;

              const CountryInventoryCard = ({
                title,
                items,
                accentClass,
              }: {
                title: string;
                items: InventoryItem[];
                accentClass: string;
              }) => {
                if (!items.length) return null;

                const getItemRank = (label: string) => {
                  const value = label.toLowerCase();

                  if (value.includes("ageing")) return 1;
                  if (value.includes("estimated storage") || value.includes("est. storage")) return 2;
                  if (value.includes("unfulfillable")) return 3;
                  if (value.includes("high coverage")) return 4;

                  return 99;
                };

                const orderedItems = [...items].sort(
                  (a, b) => getItemRank(a.label) - getItemRank(b.label)
                );

                return (
                  <div
                    className={`w-full rounded-xl border bg-white shadow-sm overflow-hidden ${accentClass}`}
                  >
                    <div className="border-b border-slate-100 bg-slate-50 px-4 py-2">
                      <div className="text-sm font-bold text-slate-800">
                        {title}
                      </div>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-2 p-3">
                      {orderedItems.map((item, i) => (
                        <div
                          key={i}
                          className="flex min-h-[38px] items-center justify-between gap-3 rounded-lg border border-amber-100 bg-white px-3 py-2"
                        >
                          <span className="text-sm font-medium text-slate-700">
                            {item.label}
                          </span>

                          {item.value ? (
                            <span className="text-sm font-bold text-[#414042] text-right whitespace-nowrap">
                              {item.value}
                            </span>
                          ) : null}
                        </div>
                      ))}

                      <button
                        type="button"
                        onClick={goToInventoryReconciliation}
                        className="md:col-span-2 flex min-h-[38px] items-center rounded-lg border border-amber-100 bg-white px-3 py-2 text-left text-sm font-medium text-slate-700 transition hover:text-[#5EA68E]"
                      >
                        For Detailed Inventory Insights, Please Refer To The{" "}
                        <span className="ml-1 font-bold text-[#5EA68E] underline underline-offset-2">
                          Inventory Reconciliation Tab
                        </span>
                        .
                      </button>
                    </div>
                  </div>
                );
              };

              const parsedItems = inventoryBullets
                .map(parseInventoryLine)
                .filter((item): item is InventoryItem => item !== null);

              // ✅ Country-wise page: render one country card only
              if (!isGlobalInventory) {
                return (
                  <div className="grid grid-cols-1 gap-4">
                    <CountryInventoryCard
                      title={getCountryTitle()}
                      items={parsedItems}
                      accentClass={
                        countryName.toLowerCase() === "uk"
                          ? "border-l-4 border-l-[#7B9A6D]"
                          : "border-l-4 border-l-[#3A8EA4]"
                      }
                    />
                  </div>
                );
              }

              // ✅ Global page: group UK and US separately
              const grouped = parsedItems.reduce(
                (acc, item) => {
                  if (item.country === "UK") acc.uk.push(item);
                  else if (item.country === "US") acc.us.push(item);
                  else acc.other.push(item);

                  return acc;
                },
                {
                  uk: [] as InventoryItem[],
                  us: [] as InventoryItem[],
                  other: [] as InventoryItem[],
                }
              );

              const hasUkInventory = grouped.uk.length > 0;
              const hasUsInventory = grouped.us.length > 0;
              const hasOtherInventory = grouped.other.length > 0;

              const visibleCountryCount =
                Number(hasUkInventory) + Number(hasUsInventory);

              return (
                <div
                  className={[
                    "grid grid-cols-1 gap-4",
                    visibleCountryCount > 1 ? "lg:grid-cols-2" : "lg:grid-cols-1",
                  ].join(" ")}
                >
                  {hasUkInventory && (
                    <CountryInventoryCard
                      title="UK Inventory"
                      items={grouped.uk}
                      accentClass="border-l-4 border-l-[#7B9A6D]"
                    />
                  )}

                  {hasUsInventory && (
                    <CountryInventoryCard
                      title="US Inventory"
                      items={grouped.us}
                      accentClass="border-l-4 border-l-[#3A8EA4]"
                    />
                  )}

                  {hasOtherInventory && (
                    <CountryInventoryCard
                      title="Other Inventory"
                      items={grouped.other}
                      accentClass="border-l-4 border-l-slate-400 lg:col-span-2"
                    />
                  )}
                </div>
              );
            })()}
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
  | "businessSummary";

const TAB_LABELS: Record<DashboardTab, string> = {
  graphs: "Finance Dashboard",
  skuBreakdown: "P&L Breakdown",
  cashFlow: "Cash Flow",
  skuwiseProfit: "SKU wise Profit",
  businessSummary: "AI Insights & Recommendations",
};

const TAB_OPTIONS: { value: DashboardTab; label: string }[] = [
  { value: "graphs", label: TAB_LABELS.graphs },
  { value: "skuBreakdown", label: TAB_LABELS.skuBreakdown },
  { value: "cashFlow", label: TAB_LABELS.cashFlow },
  { value: "skuwiseProfit", label: TAB_LABELS.skuwiseProfit },
  { value: "businessSummary", label: TAB_LABELS.businessSummary },
];

const HASH_TO_FINANCE_TAB: Record<string, DashboardTab> = {
  "finance-dashboard": "graphs",
  "ai-insights": "businessSummary",
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
  cm2_profit: 0,
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
    total_cous: 0,
    otherwplatform: 0,
    advertising_total: 0,
    total_amazon_fee: 0,
  },
  lastQuarter: {
    unit_sold: 0,
    total_sales: 0,
    gross_sales: 0,
    total_product_sales: 0,
    total_expense: 0,
    cm2_profit: 0,
    total_cous: 0,
    otherwplatform: 0,
    advertising_total: 0,
    total_amazon_fee: 0,
  },
  lastYear: {
    unit_sold: 0,
    total_sales: 0,
    gross_sales: 0,
    total_product_sales: 0,
    total_expense: 0,
    cm2_profit: 0,
    total_cous: 0,
    otherwplatform: 0,
    advertising_total: 0,
    total_amazon_fee: 0,
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

  const [range, setRange] = useState<RangeType>("yearly");

  const [selectedMonth, setSelectedMonth] = useState<string>("");

  const [selectedYear, setSelectedYear] = useState<string>(() => {
    return computeDefaultYearlyYear();
  });

  const [selectedQuarter, setSelectedQuarter] = useState<Quarter | "">("");

  const [uploadsData, setUploadsData] = useState<UploadHistoryResponse | null>(
    isDemoMode ? DEMO_UPLOAD_HISTORY : null
  );
  const [allDropdownsSelected, setAllDropdownsSelected] = useState(isDemoMode);
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


  const [activeTab, setActiveTab] = useState<DashboardTab>("graphs");
  const [pendingHash, setPendingHash] = useState<string>("");
  const [targetSummary, setTargetSummary] = useState<{
    target_sales?: number;
    shortfall_total?: number;
    cashflow_total?: number;
  } | null>(isDemoMode ? DEMO_TARGET_SUMMARY : null);

  const [targetSummaryLoading, setTargetSummaryLoading] = useState(false);

  useEffect(() => {
    if (typeof window !== "undefined" && window.location.hash) return;
    setActiveTab("graphs");
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

    if (isDemoMode) {
      setPendingHash("");
      return;
    }

    const timer = setTimeout(() => {
      const el = document.getElementById(pendingHash);
      if (el) {
        el.scrollIntoView({
          behavior: "smooth",
          block: "start",
        });
      }
      setPendingHash("");
    }, 250);

    return () => clearTimeout(timer);
  }, [activeTab, pendingHash, allDropdownsSelected, isDemoMode]);

  const tabsDisabled: Partial<Record<DashboardTab, boolean>> = useMemo(() => {
    if (isDemoMode) {
      return {
        graphs: false,
        businessSummary: false,
        skuBreakdown: false,
        skuwiseProfit: false,
        cashFlow: false,
      };
    }

    const disabled = !allDropdownsSelected;
    return {
      graphs: disabled,
      businessSummary: disabled,
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

  // const [chartExportApi, setChartExportApi] = useState<ProfitChartExportApi | null>(null);
  // const [skuExportPayload, setSkuExportPayload] = useState<SkuExportPayload | null>(null);
  // const [expenseBreakdownPieBase64, setExpenseBreakdownPieBase64] = useState<string | null>(null);

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
  const [showSummaryModal, setShowSummaryModal] = useState(false);

  const [primaryGoal, setPrimaryGoal] = useState("");
  const [riskLevel, setRiskLevel] = useState("");
  const [maxTacos, setMaxTacos] = useState<number | "">("");
  const [adBudget, setAdBudget] = useState<number | "">("");
  const [summaryNotes, setSummaryNotes] = useState("");
  const [maxPriceIncreasePct, setMaxPriceIncreasePct] = useState<number | "">("");

  const layoutRef = useRef<HTMLDivElement | null>(null);
  const [overlayBounds, setOverlayBounds] = useState<{
    left: number;
    width: number;
  }>({
    left: 0,
    width: 0,
  });

  const computeTopBottom5 = (
    rows: TableRow[]
  ): { topData: TopBottomData; bottomData: TopBottomData } => {
    const clean = (rows || []).filter(Boolean);

    const num = (v: any) => {
      if (v === null || v === undefined) return 0;
      if (typeof v === "number") return isFinite(v) ? v : 0;
      const s = String(v).replace(/,/g, "").trim();
      const n = Number(s);
      return isFinite(n) ? n : 0;
    };

    const lower = (v: any) => String(v || "").trim().toLowerCase();

    const withoutTotal = clean.filter((r) => {
      const name = lower((r as any).product_name ?? (r as any).sku);
      return name !== "total";
    });

    const sortByProfitDesc = [...withoutTotal].sort((a, b) => num(b.profit) - num(a.profit));
    const sortByProfitAsc = [...withoutTotal].sort((a, b) => num(a.profit) - num(b.profit));

    const top5 = sortByProfitDesc.slice(0, 5);
    const bottom5 = sortByProfitAsc.slice(0, 5);

    const mapRows = (arr: TableRow[]) =>
      arr.map((item) => {
        const netUnits = num(item.net_units_sold);
        const profit = num(item.profit);
        const cm1PerUnit = netUnits > 0 ? profit / netUnits : 0;

        return {
          product_name: String((item as any).product_name || (item as any).sku || "-"),
          profit: profit.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
          profitMix: num(item.profit_mix).toFixed(2),
          salesMix: num(item.sales_mix).toFixed(2),
          cm1_per_unit: cm1PerUnit.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
        };
      });

    const totalsFor = (arr: TableRow[]) => {
      const totalProfit = arr.reduce((s, r) => s + num(r.profit), 0);
      const totalProfitMix = arr.reduce((s, r) => s + num(r.profit_mix), 0);
      const totalSalesMix = arr.reduce((s, r) => s + num(r.sales_mix), 0);
      const totalNetUnits = arr.reduce((s, r) => s + num(r.net_units_sold), 0);
      const avgCm1 = totalNetUnits > 0 ? totalProfit / totalNetUnits : 0;

      return {
        profit: totalProfit.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
        profitMix: totalProfitMix.toFixed(2),
        salesMix: totalSalesMix.toFixed(2),
        avg_cm1: avgCm1.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      };
    };

    return {
      topData: { rows: mapRows(top5), totals: totalsFor(top5) },
      bottomData: { rows: mapRows(bottom5), totals: totalsFor(bottom5) },
    };
  };

  const { topData, bottomData } = useMemo(
    () => computeTopBottom5(displaySkuRows),
    [displaySkuRows]
  );

  const defaultTopProductName = useMemo(() => {
    const first = topData?.rows?.[0];
    return first?.product_name?.trim() || "";
  }, [topData]);

  useEffect(() => {
    setShowNoDataOverlay(false);
    setFocusedChart(null);
    setChartExportApi(null);
    setSkuExportPayload(null);
    setExpenseBreakdownPieBase64(null);
    setProductWiseCm1PieBase64(null);
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
    gross_sales: 0,
    total_product_sales: 0,
    total_expense: 0,
    cm2_profit: 0,
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
      setSelectedYear(computeDefaultYearlyYear());
    }

    if (v === "monthly") {
      const defaultMonthly = computeDefaultMonthlyPeriod();

      setSelectedMonth(defaultMonthly.month);
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

  const mapSkuTotalToSummary = (row: any): Summary => ({
    unit_sold: toNum(row?.total_quantity),
    total_sales: toNum(row?.net_sales),
    gross_sales: toNum(row?.gross_sales),
    total_product_sales: toNum(row?.gross_sales),
    total_expense:
      toNum(row?.amazon_fee) +
      toNum(row?.cost_of_unit_sold) +
      Math.abs(toNum(row?.advertising_total)),
    cm2_profit: toNum(row?.cm2_profit || row?.profit),
    total_cous: toNum(row?.cost_of_unit_sold),
    otherwplatform:
      toNum(row?.platform_fee) +
      toNum(row?.platform_fee_inventory_storage) +
      toNum(row?.platformfeenew),
    advertising_total: Math.abs(toNum(row?.advertising_total || row?.visible_ads)),
    total_amazon_fee: toNum(row?.amazon_fee),
  });

  const mapSkuTotalToUploadRow = (
    row: any,
    rangeType: RangeType,
    monthVal: string,
    quarterVal: string,
    yearVal: string,
    country: string
  ): UploadRow => ({
    country,
    month: rangeType === "monthly" ? monthVal : rangeType === "quarterly" ? quarterVal : "ALL",
    year: yearVal,
    total_sales: toNum(row?.net_sales),
    total_amazon_fee: toNum(row?.amazon_fee),
    total_cous: toNum(row?.cost_of_unit_sold),
    advertising_total: Math.abs(toNum(row?.advertising_total || row?.visible_ads)),
    otherwplatform:
      toNum(row?.platform_fee) +
      toNum(row?.platform_fee_inventory_storage) +
      toNum(row?.platformfeenew),
    taxncredit: toNum(row?.tex_and_credits),
    cm2_profit: toNum(row?.cm2_profit || row?.profit),
    total_profit: toNum(row?.profit),
  });

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

    return {
      summary: mapSkuTotalToSummary(currentTotal),
      summaryComparisons: {
        ...(rangeType === "monthly" ? { lastMonth: mapSkuTotalToSummary(previousTotal) } : {}),
        ...(rangeType === "quarterly" ? { lastQuarter: mapSkuTotalToSummary(previousTotal) } : {}),
        ...(rangeType === "yearly" ? { lastYear: mapSkuTotalToSummary(previousTotal) } : {}),
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

  const buildGlobalProductInsightLines = (data: any): string[] => {
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

      lines.push(
        `CM1 profit: ${formatMoneyValue(currentRow?.profit)}${formatDelta(
          getMetricDelta(momRow, "profit")
        )}`
      );

      lines.push(
        `CM1 profit per unit: ${formatMoneyValue(
          currentRow?.unit_wise_profitability
        )}${formatDelta(getMetricDelta(momRow, "unit_wise_profitability"))}`
      );

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

  const mapGlobalAiResponseToPanel = (data: any): AiPanelData => {
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
        ...buildGlobalProductInsightLines(data),
        ...buildOtherSkusInsightLines(data, currencySymbol, "global"),
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
        setAiPanel(mapGlobalAiResponseToPanel(data));
        return;
      }

      // Existing non-global mapping
      const sections = parseMdSections(data.summary);

      const summaryLines = sections["SUMMARY"] ?? [];
      const inventoryLines = sections["INVENTORY"] ?? [];
      const productLines = sections["PRODUCT INSIGHTS"] ?? [];

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

    setRange("yearly");
    setSelectedMonth("");
    setSelectedQuarter("");
    setSelectedYear(computeDefaultYearlyYear());
  }, [isDemoMode]);


  const fetchCurrencyKey = isGlobalPage ? homeCurrency : "country";

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
  }, [range, selectedMonth, selectedQuarter, selectedYear, isDemoMode]);


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

  const normalizeRowsForParent = (data: any[]): TableRow[] => {
    return data.map((row) => {
      const productName =
        row?.product_name && String(row.product_name).trim() !== ""
          ? String(row.product_name)
          : row?.sku && String(row.sku).trim() !== ""
            ? String(row.sku)
            : "-";

      const isTotalRow = productName.trim().toLowerCase() === "total";

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
        net_sales: toNum(row.net_sales),

        cost_of_unit_sold: toNum(row.cost_of_unit_sold),
        selling_fees: toNum(row.selling_fees),
        fba_fees: toNum(row.fba_fees),
        amazon_fee: toNum(row.amazon_fee),

        advertising_total: toNum(row.advertising_total),
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
        cm2_profit: toNum(row.cm2_profit),

        // ✅ Backend global total row sends this as cm2_profit_percentage
        cm2_profit_percentage: toNum(
          row.cm2_profit_percentage ??
          row.cm2_margins ??
          row.cm2_profit_percent ??
          row.cm2_profit_percentage_value
        ),

        // ✅ SKUtable summary uses cm2_margins
        cm2_margins: toNum(
          row.cm2_margins ??
          row.cm2_profit_percentage ??
          row.cm2_profit_percent ??
          row.cm2_profit_percentage_value
        ),

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

    const netSales = toNum(totalRow?.net_sales) || sumSkuRows(rows, "net_sales");
    const grossSales =
      toNum((totalRow as any)?.gross_sales) ||
      sumSkuRows(rows, "gross_sales");

    const units =
      toNum(totalRow?.total_quantity) ||
      toNum(totalRow?.net_units_sold) ||
      sumSkuRows(rows, "total_quantity");

    const cm2 =
      toNum((totalRow as any)?.cm2_profit) ||
      toNum((totalRow as any)?.profit) ||
      sumSkuRows(rows, "profit");

    return {
      unit_sold: units,
      total_sales: netSales,
      gross_sales: grossSales,
      total_product_sales: grossSales,
      total_expense: 0,
      cm2_profit: cm2,
      total_cous:
        toNum((totalRow as any)?.cost_of_unit_sold) ||
        sumSkuRows(rows, "cost_of_unit_sold"),
      advertising_total: toNum((totalRow as any)?.advertising_total),
      total_amazon_fee:
        toNum((totalRow as any)?.amazon_fee) ||
        sumSkuRows(rows, "amazon_fee"),
      otherwplatform:
        toNum((totalRow as any)?.platform_fee) ||
        toNum((totalRow as any)?.other_transaction_fees),
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
    const summary = buildSummaryFromSkuRows(rows);

    const cm1Profit =
      summary.total_sales -
      (summary.total_cous ?? 0) -
      (summary.total_amazon_fee ?? 0) +
      toNum((totalRow as any)?.tex_and_credits);

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

        total_sales: summary.total_sales,
        total_amazon_fee: summary.total_amazon_fee ?? 0,
        total_cous: summary.total_cous ?? 0,
        advertising_total: summary.advertising_total ?? 0,
        otherwplatform: summary.otherwplatform ?? 0,
        taxncredit: toNum((totalRow as any)?.tex_and_credits),
        cm2_profit: summary.cm2_profit,
        total_profit: cm1Profit,
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
    const summary = buildSummaryFromSkuRows(rows);

    const cm1Profit =
      summary.total_sales -
      (summary.total_cous ?? 0) -
      (summary.total_amazon_fee ?? 0) +
      toNum((totalRow as any)?.tex_and_credits);

    return [
      {
        country: countryVal,
        month: monthVal.toLowerCase(),
        year: yearVal,
        total_sales: summary.total_sales,
        total_amazon_fee: summary.total_amazon_fee ?? 0,
        total_cous: summary.total_cous ?? 0,
        advertising_total: summary.advertising_total ?? 0,
        otherwplatform: summary.otherwplatform ?? 0,
        taxncredit: toNum((totalRow as any)?.tex_and_credits),
        cm2_profit: summary.cm2_profit,
        total_profit: cm1Profit,
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
    skuBreakdown: "pnl-breakdown",
    skuwiseProfit: "skuwise-profit",
    cashFlow: "cash-flow",
  };

  const syncTabToHash = (tab: DashboardTab) => {
    if (typeof window === "undefined") return;

    const hash = FINANCE_TAB_TO_HASH[tab];
    if (!hash) return;

    const nextUrl = `${window.location.pathname}#${hash}`;

    if (isDemoMode) {
      window.history.replaceState(null, "", nextUrl);
      return;
    }

    window.history.pushState(null, "", nextUrl);

    window.dispatchEvent(
      new CustomEvent("page-hash-navigate", {
        detail: { hash },
      })
    );
  };

  const handleConnectAmazonPreview = () => {
    router.push(`/profile/${countryName}/NA/NA`);
  };



  return (
    <div ref={layoutRef} className="space-y-3 relative">
      <div className="sticky top-0 z-40 w-full flex flex-col bg-[#F7F7F7] sm:flex-row md:items-center md:justify-between gap-4 ">
        <div className="flex flex-col leading-tight w-full md:w-auto ">
          <div className="flex items-baseline gap-2">
            <PageBreadcrumb pageTitle="Financial Metrics -" variant="page" align="left" textSize="2xl" />
            <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
              Amazon{" "}
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
        {activeTab !== "cashFlow" && (

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

                const costOfAds = summary.advertising_total ?? 0;

                const getRoas = (s?: Summary) => {
                  const ns = s?.total_sales ?? 0;            // net sales
                  const ads = s?.advertising_total ?? 0;     // cost of ads
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

                const isSummaryZero =
                  summary.unit_sold === 0 &&
                  summary.total_sales === 0 &&
                  summary.total_expense === 0 &&
                  summary.cm2_profit === 0;

                const cm2Percent =
                  netSales > 0 ? (summary.cm2_profit / netSales) * 100 : 0;

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
                const getCm2Percent = (s?: Summary) =>
                  s && s.total_sales > 0 ? (s.cm2_profit / s.total_sales) * 100 : 0;
                const renderCm2PercentComparisons = () => {
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
                        ? getCm2Percent(comparisons.lastMonth)
                        : undefined
                      : range === "quarterly"
                        ? comparisons?.lastQuarter
                          ? getCm2Percent(comparisons.lastQuarter)
                          : undefined
                        : comparisons?.lastYear
                          ? getCm2Percent(comparisons.lastYear)
                          : undefined;

                  const hasPrev = typeof prevVal === "number" && !isNaN(prevVal);

                  const diffPct =
                    hasPrev && prevVal !== 0 ? ((cm2Percent - prevVal) / prevVal) * 100 : null;

                  const diffClass =
                    typeof diffPct === "number"
                      ? diffPct >= 0
                        ? "text-emerald-600"
                        : "text-red-600"
                      : "text-gray-400";

                  return (
                    <div className="mt-3 space-y-1.5">
                      <div className="flex items-end text-charcoal-500 justify-between gap-3 text-[10px] 2xl:text-xs leading-tight tabular-nums">
                        <div className="min-w-0">
                          <div className="whitespace-nowrap">
                            {label}:
                          </div>
                          <div className="whitespace-nowrap">
                            {hasPrev ? formatPercent(prevVal!) : "-"}
                          </div>
                        </div>

                        <span className={`font-bold whitespace-nowrap ${diffClass}`}>
                          {typeof diffPct === "number" ? (
                            <>
                              {diffPct >= 0 ? "▲" : "▼"} {Math.abs(diffPct).toFixed(1)}%
                            </>
                          ) : (
                            "-"
                          )}
                        </span>
                      </div>
                    </div>
                  );
                };

                const buildCm2PercentComparisonRows = () => {
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
                        ? getCm2Percent(comparisons.lastMonth)
                        : undefined
                      : range === "quarterly"
                        ? comparisons?.lastQuarter
                          ? getCm2Percent(comparisons.lastQuarter)
                          : undefined
                        : comparisons?.lastYear
                          ? getCm2Percent(comparisons.lastYear)
                          : undefined;

                  const hasPrev = typeof prevVal === "number" && !isNaN(prevVal);

                  const diffPct =
                    hasPrev && prevVal !== 0
                      ? ((cm2Percent - prevVal) / Math.abs(prevVal)) * 100
                      : null;

                  const deltaClassName =
                    typeof diffPct === "number"
                      ? diffPct >= 0
                        ? "text-emerald-600"
                        : "text-red-600"
                      : "text-gray-400";

                  const deltaText =
                    typeof diffPct === "number"
                      ? `${diffPct >= 0 ? "▲" : "▼"} ${Math.abs(diffPct).toFixed(1)}%`
                      : "-";

                  return [
                    {
                      label,
                      valueText: hasPrev ? formatPercent(prevVal!) : "-",
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
                    key: "grossSales",
                    title: "Gross Sales",
                    value: renderMoneyWithPerUnit(
                      roundMoney(getGrossSales(summary)),
                      summary.unit_sold,
                      true,
                      0
                    ),
                    className: "bg-white border border-[#ED9F50] border-t-4 border-t-[#ED9F50]",
                    comparisons: (() => {
                      const items = getGrossSalesComparisons();

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
                          valueText: hasValue ? formatWholeMoney(item.value!) : "-",
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
                    comparisons: buildComparisonsRows("advertising_total", formatWholeMoney),
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
                    value: renderMoneyWithPerUnit(
                      roundMoney(summary.cm2_profit),
                      summary.unit_sold,
                      true,
                      0
                    ),
                    className: "bg-white border border-[#B8C78C] border-t-4 border-t-[#B8C78C]",
                    comparisons: buildComparisonsRows("cm2_profit", formatWholeMoney),
                  },
                  {
                    key: "cm2Pct",
                    title: "CM2 Profit %",
                    value: formatPercent(cm2Percent),
                    // className: "border border-[#7B9A6D] bg-[#7B9A6D4D]",
                    className: "bg-white border border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]",
                    comparisons: buildCm2PercentComparisonRows(),
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
                // metricSortMetrics={["units", "sales", "profit", "marketplace_fees"]}
                // hideDownloadButton
                disableInternalFade={shouldShowPreviewData}
              />

              {!displaySkuNoDataFound && displaySkuRows.length > 0 && (
                <SkuTopBottomTables
                  topData={topData}
                  bottomData={bottomData}
                  currencySymbol={currencySymbol}
                  previewMode={shouldShowPreviewData}
                />
              )}
            </div>

          )}

          {/* {activeTab === "skuwiseProfit" && allDropdownsSelected && (
            <div id="skuwise-profit" className="mt-4 scroll-mt-[80px]">
              {(() => {
                const productWiseRange =
                  range === "quarterly" ? "quarterly" : "yearly";

                return (
                  <ProductwisePerformance
                    key={[
                      initialCountryName,
                      productWiseRange,
                      selectedQuarter,
                      selectedYear,
                      defaultTopProductName,
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
                    initialProductName={defaultTopProductName || "Demo Product A"}
                  />
                );
              })()}
            </div>
          )} */}

          {activeTab === "skuwiseProfit" && allDropdownsSelected && (
            <div id="skuwise-profit" className="mt-4 scroll-mt-[80px]">
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

                const productWiseInitialProductName = isDemoMode
                  ? defaultTopProductName || "Demo Product A"
                  : hasRealSkuRowsForProductWise
                    ? defaultTopProductName
                    : "";

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
    </div>
  );

};

export default Dropdowns;