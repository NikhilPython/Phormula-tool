"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
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
import {
  TrendingUp,
  DollarSign,
  Package,
  Target,
  AlertCircle,
  Wallet,
} from "lucide-react";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
} from "recharts";
import Productinfoinpopup from '@/components/businessInsight/Productinfoinpopup';
import { useRouter } from "next/navigation";
import SegmentedToggle from "@/components/ui/SegmentedToggle";

/* ---------------------- Types ---------------------- */
type Summary = {
  unit_sold: number;
  total_sales: number;      // (your current "Sales")
  gross_sales?: number;     // ✅ ADD THIS
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

    // ✅ ADD THESE (new API fields)
    inventory_recommendation?: string;
    ads_recommendation?: string;
  }
> & {
  remaining_skus_recommendation?: string;
};

type AiSummaryResponse = {
  summary?: string | null;

  // ✅ now recommendations can be OBJECT (new API) OR markdown string (old)
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

  // ✅ ADD THIS
  remainingSkusRecommendation?: string;
  portfolioRecommendation?: string | null;
};



type RangeType = "monthly" | "quarterly" | "yearly" | "";

/** Quarter union and helpers */
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

type FetchedPeriods = Record<string, string[]>; // { "2024": ["january","february"], ... }

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
  const y = String(year);
  const m = month ? month.toLowerCase() : "";

  const fp = readFetchedPeriods();
  if (!fp[y]) fp[y] = [];
  if (m && !fp[y].includes(m)) fp[y].push(m);

  // keep months sorted
  fp[y] = fp[y]
    .filter(Boolean)
    .sort((a, b) => (monthIndexMap[a] ?? 99) - (monthIndexMap[b] ?? 99));

  writeFetchedPeriods(fp);

  // keep latestFetchedPeriod updated (used by PeriodFiltersTable too)
  if (m) {
    localStorage.setItem("latestFetchedPeriod", JSON.stringify({ year: y, month: m }));
  }
};

const computeDefaultYearlyYear = () => {
  const now = new Date();
  const cy = now.getFullYear();
  const cm = now.getMonth(); // 0..11, current month

  const fp = readFetchedPeriods();
  const monthsFetchedThisYear = fp[String(cy)] || [];

  const hasHistoricMonthInCurrentYear = monthsFetchedThisYear.some((m) => {
    const idx = monthIndexMap[m.toLowerCase()];
    return typeof idx === "number" && idx < cm; // strictly historic
  });

  return hasHistoricMonthInCurrentYear ? String(cy) : String(cy - 1);
};

const getPrevMonthLabel = (selectedMonth: string, selectedYear: number) => {
  const idx = monthIndexMap[selectedMonth.toLowerCase()];
  if (idx === undefined) return "Last month";

  const prev = new Date(selectedYear, idx - 1, 1);
  const mon = prev.toLocaleString("en-US", { month: "short" }); // Nov
  const yy = String(prev.getFullYear()).slice(-2); // 25
  return `${mon}'${yy}`; // Nov'25
};

const getCurrencySymbol = (codeOrCountry: string) => {
  const v = (codeOrCountry || "").toLowerCase();

  switch (v) {
    // Home currency / common codes
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

  // matches: "£10.55 (+7.54%)" OR "80.00 (-31.62%)"
  const m = v.match(/^(.+?)\s*(\(([-+])[^)]+\))\s*$/);

  if (!m) {
    return { main: v, delta: "", deltaColor: "" };
  }

  const main = m[1].trim();
  const delta = m[2].trim();     // "(+7.54%)"
  const sign = m[3];             // "+" | "-"

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
  isOtherSkus?: boolean; // ✅ ADD
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
      !line.toLowerCase().startsWith("inventory action:") &&   // ✅ ADD
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
        inventoryBullets: [],          // ✅ ADD
        isOtherSkus: isOther,
      };

      inJourney = false;
      continue;
    }

    if (!current) continue;

    // section switches
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

    // ✅ ADD: inventory action lines
    if (line.toLowerCase().startsWith("inventory action:")) {
      inJourney = false;
      const inv = line.replace(/^inventory action:\s*/i, "").trim();
      if (inv) current.inventoryBullets.push(inv);
      continue;
    }

    // metrics
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
    // We will render these inside Product Insights, so keep reco bullets empty here
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

  // if already multiline / bullets
  if (t.includes("\n")) {
    return t
      .split("\n")
      .map((x) => x.replace(/^[-•]\s+/, "").trim())
      .filter(Boolean);
  }

  // otherwise split by sentences
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
  return [cleaned.join(" ")]; // single bullet line
};

const getPeriodBadge = (range: RangeType, year: string, month?: string, quarter?: string) => {
  const yy = String(year || "").slice(-2);

  if (range === "monthly" && month) {
    const shortMonth = month.slice(0, 3);
    return `${shortMonth}'${yy}`; // Jan'26
  }

  if (range === "quarterly" && quarter) {
    return `${quarter}'${yy}`;    // Q1'26
  }

  if (range === "yearly" && year) {
    return String(year);         // 2026
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
  "border border-[#2DA49A] border-t-[#2DA49A]",
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

  const inventoryRecoBullets = mergeToSingleBullet(toBullets(inventoryText));
  const adsRecoBullets = toBullets(recObj?.ads_recommendation);
  const periodBadge = getPeriodBadge(range, year, month, quarter);

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
            className="fixed inset-0 z-[999999] bg-black/40"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
          />

          <motion.aside
            className="fixed right-0 top-0 z-[1000000] h-screen w-[95vw] max-w-[900px] bg-white shadow-2xl"
            initial={{ x: 520 }}
            animate={{ x: 0 }}
            exit={{ x: 520 }}
            transition={{ type: "tween", duration: 0.25 }}
          >
            <div className="flex h-full flex-col gap-4">
              <div className="flex shrink-0 items-start justify-between gap-3 border-b border-slate-200 p-3">
                <div>
                  <div className="flex items-center gap-1 flex-wrap">
                    <PageBreadcrumb
                      pageTitle="Detailed View - "
                      variant="page"
                      textSize="2xl"
                    />

                    <span className="text-base font-bold text-green-500 sm:text-xl lg:text-lg 2xl:text-2xl">
                      {block.name || "Details"}
                    </span>

                    {drawerPeriodText ? (
                      <span className="ml-1 text-base font-bold text-green-500 sm:text-xl lg:text-lg 2xl:text-2xl ">
                        {drawerPeriodText}
                      </span>
                    ) : periodBadge ? (
                      <span className="ml-1 rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[11px] text-slate-700">
                        {periodBadge}
                      </span>
                    ) : null}
                  </div>
                </div>

                <button
                  onClick={onClose}
                  className="rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
                >
                  ✕
                </button>
              </div>

              <div className="flex-1 space-y-6 overflow-y-auto px-3">
                <div>
                  <div className="mb-2 text-xs font-semibold text-charcoal-700 sm:text-sm 2xl:text-lg">
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

                                {/* 3rd line: percentage */}
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

                <div className="w-full">
                  <Productinfoinpopup
                    productname={block.name}
                    countryName={countryName}
                  />
                </div>

                <div className="pb-4">
                  <div className="flex items-center gap-1 flex-wrap">
                    <PageBreadcrumb
                      pageTitle="Product Journey"
                      variant="page"
                      textSize="lg"
                    />
                  </div>

                  {block.journeyBullets?.length ? (
                    <ul className="space-y-1 text-xs text-charcoal-600 2xl:text-sm">
                      {block.journeyBullets.map((p, i) => (
                        <li key={i} className="flex items-start gap-2">
                          <span className="text-charcoal-400">→</span>
                          <span>
                            {p
                              .replace(/^\d+\.\s*-\s*/, "")
                              .replace(/^\d+\.\s*/, "")
                              .replace(/^-+\s*/, "")}
                          </span>
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


type ProductJourneyModalProps = {
  open: boolean;
  onClose: () => void;
  block: ProductInsightBlock | null;
};

const ProductJourneyModal: React.FC<ProductJourneyModalProps> = ({
  open,
  onClose,
  block,
}) => {
  if (!open || !block) return null;

  return (
    <div className="fixed inset-0 z-999999 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl w-[95%] max-w-2xl shadow-xl p-6 space-y-5 relative">

        {/* Close */}
        <button
          onClick={onClose}
          className="absolute right-4 top-4 text-slate-400 hover:text-slate-600"
        >
          ✕
        </button>

        {/* Product Name */}
        <h2 className="text-lg font-semibold text-slate-800">
          {block.name}
        </h2>

        {/* Metrics */}
        {block.metrics.length > 0 && (
          <div className="flex flex-wrap gap-3">
            {block.metrics.map((m, i) => (
              <div
                key={i}
                className="px-3 py-1 rounded-full text-xs font-medium border"
                style={{
                  color: m.color || "#414042",
                  borderColor: "#E2E8F0",
                }}
              >
                {m.label}: {m.value}
              </div>
            ))}
          </div>
        )}

        {/* Action Summary */}
        {block.recommendationBullets.length > 0 && (
          <div className="bg-blue-50 border-l-4 border-blue-500 p-4 rounded-md">
            <p className="text-sm text-blue-900">
              💡 {block.recommendationBullets.join(" ")}
            </p>
          </div>
        )}

        {/* Product Journey */}
        {block.journeyBullets.length > 0 && (
          <div>
            <h3 className="font-semibold text-slate-700 mb-2">
              Product Journey
            </h3>
            <ul className="space-y-2 text-sm text-slate-600">
              {block.journeyBullets.map((j, i) => (
                <li key={i} className="flex gap-2">
                  <span className="text-slate-400">→</span>
                  <span>{j}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </div>
  );
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

  // If you later add metric toggle, keep this state (and include in deps)
  const [perfMetric, setPerfMetric] = useState<"net_sales" | "units">("net_sales");

  const hasBlocks = blocks.length > 0; // ✅ compute instead of early return

  // top border colors (rotate)
  const topBorderColors = ["border-t-blue-500", "border-t-amber-500", "border-t-emerald-500", "border-t-rose-500"];

  const skuActions =
    (recommendationsMap as any)?.sku_actions ??
    (recommendationsMap as any)?.recommendations ??
    recommendationsMap ??
    {};

  useEffect(() => {
    // ✅ guard inside effect
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

        const productKeyForApi = selectedBlock.name; // ✅ Always product name

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
    perfMetric, // ✅ include, since you read it inside effect
  ]);

  // ✅ NOW you can early return (after hooks)
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

      <div className="grid grid-cols-3 gap-6">
        {blocks.map((b, idx) => {
          const borderColor = topBorderColors[idx % topBorderColors.length];

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
              {/* Header */}
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

              {/* Metrics */}
              {b.metrics?.length > 0 && (
                <div className="grid grid-cols-3 gap-2">
                  {b.metrics.map((m, i) => (
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

              {/* Short inline recommendation */}
              {b.recommendationBullets?.length > 0 && (
                <p className="text-xs 2xl:text-sm text-slate-700 leading-relaxed">
                  {b.recommendationBullets.join(" ")}
                </p>
              )}
            </motion.div>
          );
        })}
      </div>

      {/* Right Drawer */}
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

const capitalizeFirst = (text?: string) => {
  if (!text) return "";
  return text.charAt(0).toUpperCase() + text.slice(1);
};

// const MonthlyObjectiveStrip = ({
//   objective,
// }: {
//   objective?: ObjectivePayload;
// }) => {
//   const Item = ({
//     label,
//     value,
//     icon,
//     topColor,
//     iconBg,
//     iconColor,
//     valueClass = "text-slate-800",
//   }: {
//     label: string;
//     value: string;
//     icon: React.ReactNode;
//     topColor: string;
//     iconBg: string;
//     iconColor?: string;
//     valueClass?: string;
//   }) => (
//     <div className="relative flex flex-col justify-center px-6 py-4 bg-white border border-slate-200 rounded-xl">

//       {/* Top Color Bar */}
//       <div
//         className="absolute top-0 left-0 w-full h-1"
//       />

//       <div className="flex items-center gap-3">




//         <div className="flex flex-col">
//           <span className="text-xs text-slate-500">{label}</span>
//           <span className={`text-sm font-semibold ${valueClass}`}>
//             {value}
//           </span>
//         </div>
//       </div>
//     </div>
//   );


//   return (
//     <div className="rounded-xl border border-slate-200 bg-white shadow-sm p-3 mt-4">

//       {/* Title */}
//       <div className="mb-2">
//         <PageBreadcrumb
//           pageTitle=" Monthly Objectives & Targets"
//           variant="page"
//           textSize="2xl"
//           align="left"
//         />
//       </div>

//       {/* Strip Grid */}
//       <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-5  rounded-sm">

//         <Item
//           label="Growth"
//           value={capitalizeFirst(objective?.growth_intent) || "Growth"}
//           icon={<TrendingUp size={16} />}
//           topColor="#3A8EA4"
//           iconBg="#E0F2F1"
//         />

//         <Item
//           label="Profit"
//           value={capitalizeFirst(objective?.profit_priority?.replaceAll("_", " ")) || "Profit"}
//           icon={<DollarSign size={16} />}
//           topColor="#ED9F50"
//           iconBg="#FFF3E0"
//         />

//         <Item
//           label="Inventory Dilution"
//           value={objective?.inventory_clearance_priority ? "Yes" : "No"}
//           icon={<Package size={16} />}
//           topColor="#C0BFC1"
//           iconBg="#F3F4F6"
//         />

//         <Item
//           label="Target Set"
//           value="$140K"
//           icon={<Target size={16} />}
//           topColor="#5EA68E"
//           iconBg="#E6F4EA"
//         />

//         <Item
//           label="Shortfall"
//           value="-$3.6K"
//           icon={<AlertCircle size={16} />}
//           topColor="#B75A5A"
//           iconBg="#FDECEA"
//           valueClass="text-red-600"
//         />

//         <Item
//           label="Cash Flow"
//           value="$130K"
//           icon={<Wallet size={16} />}
//           topColor="#75BBDA"
//           iconBg="#E3F2FD"
//         />


//       </div>
//     </div>
//   );
// };

const MonthlyObjectiveStrip = ({
  objective,
  className = "",
}: {
  objective?: ObjectivePayload;
  className?: string;
}) => {
  const growth =
    objective?.growth_intent?.replaceAll("_", " ") || "Not Defined";

  const profit =
    objective?.profit_priority?.replaceAll("_", " ") || "Not Defined";

  const inventory =
    typeof objective?.inventory_clearance_priority === "boolean"
      ? objective.inventory_clearance_priority
        ? "Yes"
        : "No"
      : "Not Defined";

  const targetSet = "$140K";
  const shortfall = "-$3.6K";
  const cashFlow = "$130K";

  const Item = ({
    label,
    value,
    valueClass = "text-charcoal-500",
  }: {
    label: string;
    value: string;
    valueClass?: string;
  }) => (
    <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
      <div className="2xl:text-xs text-[10px] text-charcoal-500">
        {label}
      </div>
      <div
        className={`mt-1 text-sm 2xl:text-lg font-semibold capitalize ${valueClass}`}
      >
        {value}
      </div>
    </div>
  );

  return (
    <div className={`rounded-xl border border-slate-200 bg-white shadow-sm p-4 mt-4 ${className}`}>
      <div className="mb-2">
        <PageBreadcrumb
          pageTitle="Monthly Objectives & Targets"
          variant="page"
          textSize="2xl"
          align="left"
        />
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-5">
        <Item label="Growth" value={growth} />
        <Item label="Profit" value={profit} />
        <Item label="Inventory Dilution" value={inventory} />
        <Item label="Target Set" value={targetSet} />
        <Item label="Shortfall" value={shortfall} valueClass="text-red-600" />
        <Item label="Cash Flow" value={cashFlow} />
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

  // ✅ ADD THESE
  range: RangeType;
  selectedYear: string;
  selectedQuarter: Quarter | "";
  homeCurrency?: string;
  countryName: string; // ✅ ADD
  portfolioRecommendation?: string | null; // ✅ ADD
};

const formatSummaryPeriod = (text?: string) => {
  if (!text) return "";

  const m = text.match(/\(([^)]+)\)/);     // pick "(...)" safely
  if (!m) return "";

  const inside = m[1].trim();             // e.g. "2026 vs 2025" OR "Jan 2026 vs Jan 2025"
  const [leftRaw, rightRaw] = inside.split(/\s*vs\s*/i);
  if (!leftRaw || !rightRaw) return `(${inside})`;

  const formatPart = (part: string) => {
    const p = part.trim();

    // ✅ Case 1: Year only ("2026")
    if (/^\d{4}$/.test(p)) return p;

    // ✅ Case 2: Month Year ("January 2026")
    const [month, year] = p.split(/\s+/);
    if (!month || !year) return p;

    const shortMonth = month.slice(0, 3);
    const shortYear = year.slice(-2);
    return `${shortMonth}’${shortYear}`;
  };

  return `(${formatPart(leftRaw)} vs ${formatPart(rightRaw)})`;
};

const AiSingleInsightCard: React.FC<AiSingleInsightCardProps> = ({
  loading, // kept for prop compatibility but NOT used for UI
  error,   // kept for prop compatibility but NOT used for UI
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
  homeCurrency,
}) => {

  if (
    !summaryBullets.length &&
    !recommendationBullets.length &&
    !skuInsightsBullets.length &&
    !inventoryBullets.length
  ) {
    return null;
  }

  const narrativeInsights = summaryBullets.filter((l) => !l.includes(":"));

  const drawerPeriodText =
    narrativeInsights?.[0] ? formatSummaryPeriod(narrativeInsights[0]) : "";

  return (
    <div className="flex flex-col gap-5">
      <div className="w-full space-y-4">

        <div className="space-y-4 rounded-xl border border-slate-200 bg-white shadow-sm p-4">
          {/* Narrative Summary */}
          {narrativeInsights.length > 0 && (
            <>
              <div className="space-y-3">
                <h2 className="text-lg 2xl:text-2xl text-[#414042] font-bold">
                  {narrativeInsights[0]?.split("(")[0]?.trim()}
                  <span className="text-[#5EA68E] font-semibold ml-2 2xl:text-xl">
                    {formatSummaryPeriod(narrativeInsights[0])}
                  </span>
                </h2>

                <div className="text-xs 2xl:text-sm text-slate-700 leading-relaxed space-y-2">
                  {narrativeInsights.slice(1).map((line, i) => (
                    <p key={i}>{line}</p>
                  ))}
                </div>

                {/* ✅ ADD THIS AT THE END */}
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
            </>
          )}
        </div>

        {/* Product Insights */}
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
              homeCurrency={homeCurrency}
              countryName={countryName}
              drawerPeriodText={drawerPeriodText}
            />
          </div>
        </div>

        {/* ================= INVENTORY SECTION ================= */}
        {inventoryBullets.length > 0 && (
          <div className="space-y-4 rounded-xl border border-slate-200 bg-white shadow-sm p-4">
            <div className="flex items-center gap-2">
              <span className="text-base 2xl:text-2xl font-bold text-slate-800">
                Inventory Insights
              </span>
            </div>

            {(() => {
              const detailLines = inventoryBullets.filter((b) => /for detailed/i.test(b));
              const mainLines = inventoryBullets.filter((b) => !/for detailed/i.test(b));

              return (
                <>
                  <div className="grid grid-cols-2 gap-3">
                    {mainLines.map((b, i) => {
                      const raw = String(b || "").trim();

                      const isUnfulfillable = /unfulfillable/i.test(raw);
                      if (isUnfulfillable) {
                        const match = raw.match(/\(([^)]+)\)/);
                        const value = match?.[1]?.trim();
                        const label = raw.replace(/\([^)]+\)/, "").trim();

                        return (
                          <div
                            key={i}
                            className="flex justify-between items-center bg-white rounded-lg p-3 border border-amber-100"
                          >
                            <span className="text-sm font-medium text-slate-700">{label}</span>
                            {value ? (
                              <span className="font-bold text-[#414042] text-sm whitespace-nowrap">
                                {value}
                              </span>
                            ) : null}
                          </div>
                        );
                      }

                      const colonIdx = raw.indexOf(":");
                      const hasColon = colonIdx > -1;

                      const left = hasColon ? raw.slice(0, colonIdx).trim() : raw;
                      const right = hasColon ? raw.slice(colonIdx + 1).trim() : "";

                      return (
                        <div
                          key={i}
                          className="flex justify-between items-center bg-white rounded-lg p-3 border border-amber-100"
                        >
                          <span className="text-sm font-medium text-slate-700">{left}</span>
                          {right ? (
                            <span className="font-bold text-[#414042] text-sm whitespace-nowrap">
                              {right}
                            </span>
                          ) : null}
                        </div>
                      );
                    })}
                  </div>

                  {detailLines.map((line, idx) => (
                    <p key={idx} className="text-xs text-slate-500 italic mt-2">
                      {line}
                    </p>
                  ))}
                </>
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
  | "businessSummary"
  // | "breakdowns"    
  | "skuBreakdown";

const TAB_LABELS: Record<DashboardTab, string> = {
  graphs: "Finance Dashboard",
  businessSummary: "AI Insights & Recommendations",
  // breakdowns: "Breakdowns",    
  skuBreakdown: "P&L Breakdown",
};

const TAB_OPTIONS: { value: DashboardTab; label: string }[] = [
  { value: "graphs", label: TAB_LABELS.graphs },
  { value: "businessSummary", label: TAB_LABELS.businessSummary },
  // { value: "breakdowns", label: TAB_LABELS.breakdowns },
  { value: "skuBreakdown", label: TAB_LABELS.skuBreakdown },
];

const HASH_TO_FINANCE_TAB: Record<string, DashboardTab> = {
  "finance-dashboard": "graphs",
  "ai-insights": "businessSummary",
  "pnl-breakdown": "skuBreakdown",
};

/* ---------------------- Component ---------------------- */
const Dropdowns: React.FC<DropdownsProps> = ({
  initialRanged,
  initialCountryName,
  initialMonth,
  initialYear,
}) => {
  const { data: userData } = useGetUserDataQuery();

  // Normalized home currency from profile (e.g. "usd", "inr")
  const homeCurrency = (userData?.homeCurrency || "USD").toLowerCase();

  const router = useRouter();

  // params from parent
  const ranged = initialRanged;
  const countryName = initialCountryName;
  const month = initialMonth;
  const year = initialYear;

  // Global vs Country page
  const isGlobalPage = countryName.toLowerCase() === "global";

  // For child components: only pass homeCurrency when global
  const globalHomeCurrency = isGlobalPage ? homeCurrency : undefined;

  // Symbol for summary cards
  const currencySymbol = isGlobalPage
    ? getCurrencySymbol(homeCurrency) // GLOBAL → homeCurrency
    : getCurrencySymbol(countryName || ""); // Country → country currency
  const [collapsed, setCollapsed] = useState(false);

  const [range, setRange] = useState<RangeType>("");
  const [selectedMonth, setSelectedMonth] = useState<string>("");
  const [selectedYear, setSelectedYear] = useState<string>("");
  const [selectedQuarter, setSelectedQuarter] = useState<Quarter | "">("");
  const [uploadsData, setUploadsData] = useState<UploadHistoryResponse | null>(
    null
  );
  const [allDropdownsSelected, setAllDropdownsSelected] = useState(false);
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [loading, setLoading] = useState(false);
  const [showNoDataOverlay, setShowNoDataOverlay] = useState(false);
  const [performanceTrend, setPerformanceTrend] = useState<PerformanceTrendPayload | null>(null);
  const [performanceTrendMetric, setPerformanceTrendMetric] = useState<"net_sales" | "units">("net_sales");
  const [performanceTrendBase64, setPerformanceTrendBase64] = useState<string | null>(null);
  const [trendExportApi, setTrendExportApi] = useState<TrendChartExportApi | null>(null);
  const [focusedChart, setFocusedChart] = useState<FocusedChart>(null);
  const [bargraphUploads, setBargraphUploads] = useState<UploadRow[]>([]);
  const [bargraphLoading, setBargraphLoading] = useState(false);
  const [bargraphUserMeta, setBargraphUserMeta] = useState<{ company_name?: string; brand_name?: string } | null>(null);

  // ✅ GraphPage (Line chart) data from parent
  const [graphPageUploads, setGraphPageUploads] = useState<UploadRow[]>([]);
  const [graphPageLoading, setGraphPageLoading] = useState(false);
  const [graphPageUserMeta, setGraphPageUserMeta] = useState<{ company_name?: string; brand_name?: string } | null>(null);
  const [graphPageError, setGraphPageError] = useState<string | null>(null);
  const [skuRows, setSkuRows] = useState<TableRow[]>([]);

  const [activeTab, setActiveTab] = useState<DashboardTab>("graphs");
  const [pendingHash, setPendingHash] = useState<string>("");

  useEffect(() => {
    if (typeof window !== "undefined" && window.location.hash) return;
    setActiveTab("graphs");
  }, [range, selectedMonth, selectedQuarter, selectedYear, countryName]);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const applyHash = () => {
      const hash = window.location.hash.replace("#", "");
      if (!hash) return;

      const targetTab = HASH_TO_FINANCE_TAB[hash];
      if (!targetTab) return;

      setPendingHash(hash);

      setActiveTab((prev) => {
        if (prev === targetTab) return prev;
        return targetTab;
      });
    };

    applyHash();
    window.addEventListener("hashchange", applyHash);

    return () => window.removeEventListener("hashchange", applyHash);
  }, []);

  useEffect(() => {
    if (!pendingHash) return;
    if (!allDropdownsSelected) return;

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
  }, [activeTab, pendingHash, allDropdownsSelected]);

  const tabsDisabled: Partial<Record<DashboardTab, boolean>> = useMemo(() => {
    const disabled = !allDropdownsSelected;
    return {
      graphs: disabled,
      businessSummary: disabled,
      // breakdowns: disabled,
      skuBreakdown: disabled,
    };
  }, [allDropdownsSelected]);

  const nameToSkuMap = useMemo(() => {
    const map: Record<string, string> = {};

    for (const r of skuRows || []) {
      const name = normalizeKey(String((r as any).product_name ?? ""));
      const sku = String((r as any).sku ?? "").trim();

      if (name && sku) map[name] = sku;
    }

    return map;
  }, [skuRows]);



  const toggleFocus = (which: Exclude<FocusedChart, null>) => {
    setFocusedChart((prev) => (prev === which ? null : which));
  };

  // ✅ PnL is "collapsed" when it's NOT in focused full view
  const pnlCollapsed = focusedChart !== "pnl";

  // ---------------- AI Summary Panel state ----------------
  const [aiPanel, setAiPanel] = useState<AiPanelData | null>(null);
  const [aiPanelLoading, setAiPanelLoading] = useState(false);
  const [aiPanelError, setAiPanelError] = useState<string | null>(null);

  // ✅ ADD THIS (request version guard)
  const aiRequestIdRef = useRef(0);

  const [chartExportApi, setChartExportApi] = useState<ProfitChartExportApi | null>(null);
  const [skuExportPayload, setSkuExportPayload] = useState<SkuExportPayload | null>(null);
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

    // ✅ robust number parser (handles "1,234.56", null, undefined, "")
    const num = (v: any) => {
      if (v === null || v === undefined) return 0;
      if (typeof v === "number") return isFinite(v) ? v : 0;
      const s = String(v).replace(/,/g, "").trim();
      const n = Number(s);
      return isFinite(n) ? n : 0;
    };

    const lower = (v: any) => String(v || "").trim().toLowerCase();

    // remove Total if present (supports product_name or sku)
    const withoutTotal = clean.filter((r) => {
      const name = lower((r as any).product_name ?? (r as any).sku);
      return name !== "total";
    });

    // ✅ sort using parsed numbers (prevents string-sorting bugs)
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

  const { topData, bottomData } = useMemo(() => computeTopBottom5(skuRows), [skuRows]);


  useEffect(() => {
    setShowNoDataOverlay(false);
    setFocusedChart(null);
    setChartExportApi(null);
    setSkuExportPayload(null);
    setExpenseBreakdownPieBase64(null);
    setProductWiseCm1PieBase64(null);
    setPerformanceTrend(null);
    setPerformanceTrendBase64(null);
    setTrendExportApi(null);
    setSkuRows([]);
  }, [range, selectedMonth, selectedQuarter, selectedYear]);

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
    gross_sales: 0, // ✅ ADD THIS
    total_product_sales: 0,
    total_expense: 0,
    cm2_profit: 0,
    total_cous: 0,
    otherwplatform: 0,
    advertising_total: 0,
    total_amazon_fee: 0,
  };


  const displayData: Summary =
    allDropdownsSelected && uploadsData?.summary
      ? uploadsData.summary
      : zeroData;


  console.log("🔍 displayData:", displayData);

  // range: "monthly" | "quarterly" | "yearly"
  const handleRangeChange = (v: "monthly" | "quarterly" | "yearly") => {
    setRange(v);
    setSelectedMonth("");
    setSelectedQuarter("");
    setSelectedYear("");
    setUploadsData(null);
  };

  const fetchUploadHistory = async (
    rangeType: RangeType,
    monthVal: string,
    quarterVal: string, // safe for the API as plain string
    yearVal: string,
    country: string
  ) => {
    if (!rangeType || !yearVal) return;

    setLoading(true);
    try {
      const token =
        typeof window !== "undefined"
          ? localStorage.getItem("jwtToken")
          : null;

      const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_history2`);
      url.searchParams.set("range", rangeType);
      url.searchParams.set("month", monthVal);
      url.searchParams.set("quarter", quarterVal);
      url.searchParams.set("year", yearVal);
      url.searchParams.set("country", country);

      // ✅ Only for GLOBAL send homeCurrency
      if (country.toLowerCase() === "global" && homeCurrency) {
        url.searchParams.set("homeCurrency", homeCurrency);
      }

      const res = await fetch(url.toString(), {
        method: "GET",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        cache: "no-store",
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        console.error(`API Error: ${err?.error ?? res.statusText}`);
        // setUploadsData(null);
        return;
      }

      const data: UploadHistoryResponse = await res.json();
      setUploadsData(data);

      // ✅ Persist fetched periods so older months/years remain selectable later
      if (data?.summary) {
        if (rangeType === "monthly" && yearVal && monthVal) {
          markFetched(yearVal, monthVal);
        }
        if (rangeType === "quarterly" && yearVal) {
          // optional: mark year as seen (no month)
          markFetched(yearVal);
        }
        if (rangeType === "yearly" && yearVal) {
          // optional: mark year as seen (no month)
          markFetched(yearVal);
        }
      }

    } catch (error) {
      console.error("Error fetching data: ", error);
      // setUploadsData(null);
    } finally {
      setLoading(false);
    }
  };


  const fetchAiSummary = async (rangeType: RangeType) => {
    if (!countryName || !rangeType || !selectedYear) return;

    const requestId = ++aiRequestIdRef.current; // ✅ 1️⃣ request version

    const timeline =
      rangeType === "monthly"
        ? monthNameToNumber(selectedMonth)
        : rangeType === "quarterly"
          ? selectedQuarter
          : "ALL";

    if (rangeType === "monthly" && !timeline) return;
    if (rangeType === "quarterly" && !selectedQuarter) return;

    setAiPanelLoading(true);
    setAiPanelError(null);
    setAiPanel(null); // ✅ 2️⃣ clear stale summary

    try {
      const token =
        typeof window !== "undefined"
          ? localStorage.getItem("jwtToken")
          : null;

      const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/summary`);
      url.searchParams.set("country", countryName);
      url.searchParams.set("period", rangeType);
      url.searchParams.set("timeline", String(timeline));
      url.searchParams.set("year", String(selectedYear));

      const res = await fetch(url.toString(), {
        method: "GET",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        cache: "no-store",
      });

      if (!res.ok) {
        if (requestId !== aiRequestIdRef.current) return; // ✅ 3️⃣ guard
        setAiPanel(null);
        setAiPanelError("Failed to fetch AI summary");
        return;
      }

      const data: AiSummaryResponse = await res.json();

      if (requestId !== aiRequestIdRef.current) return; // ✅ 3️⃣ guard

      // setPerformanceTrend(data.performance_trend ?? null);
      // setPerformanceTrendMetric(data.performance_trend_metric ?? "net_sales");

      const sections = parseMdSections(data.summary);

      const summaryLines = sections["SUMMARY"] ?? [];
      const inventoryLines = sections["INVENTORY"] ?? [];
      const productLines = sections["PRODUCT INSIGHTS"] ?? [];
      const { recommendationBullets, inventoryBullets, recommendationsMap } =
        extractRecoAndInventoryBullets(data.recommendations as any);

      // ✅ extract remaining_skus_recommendation safely
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

        // ✅ NEW
        remainingSkusRecommendation,
        portfolioRecommendation: data.portfolio_recommendation ?? null,
      });



    } catch (e: any) {
      if (requestId !== aiRequestIdRef.current) return; // ✅ 3️⃣ guard
      setAiPanel(null);
      setAiPanelError(e?.message || "Failed to fetch AI summary");
    } finally {
      if (requestId === aiRequestIdRef.current) {
        setAiPanelLoading(false); // ✅ 3️⃣ guard
      }
    }
  };



  const fetchPerformanceTrendFromHistory = async (rangeType: RangeType) => {
    if (!countryName || !rangeType || !selectedYear) return;

    // build timeline exactly like your summary route did
    const timeline =
      rangeType === "monthly"
        ? monthNameToNumber(selectedMonth)               // 1..12
        : rangeType === "quarterly"
          ? selectedQuarter                              // "Q1".."Q4"
          : "ALL";                                       // yearly

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

      // (optional) if you want to control metric from FE:
      url.searchParams.set("metric", performanceTrendMetric); // "net_sales" | "units"

      // ✅ Only for GLOBAL send homeCurrency
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



  const handleMonthChange = (v: string) => {
    setSelectedMonth(v);

    if (selectedYear) {
      fetchUploadHistory(range, v, selectedQuarter || "", selectedYear, countryName);
    } else {
      setUploadsData(null);
    }
  };

  // quarter is "Q1" | "Q2" | "Q3" | "Q4"
  const handleQuarterChange = (v: string) => {
    const q = isQuarter(v) ? v : "";
    setSelectedQuarter(q);

    if (selectedYear && q) {
      fetchUploadHistory(range, selectedMonth, q, selectedYear, countryName);
    } else {
      setUploadsData(null);
    }
  };

  const handleYearChange = (v: string) => {
    setSelectedYear(v);

    if (
      (range === "monthly" && selectedMonth) ||
      (range === "quarterly" && selectedQuarter) ||
      range === "yearly"
    ) {
      fetchUploadHistory(range, selectedMonth, selectedQuarter || "", v, countryName);
    } else {
      setUploadsData(null);
    }
  };

  useEffect(() => {
    setRange("yearly");
    setSelectedMonth("");
    setSelectedQuarter("");

    const y = computeDefaultYearlyYear();
    setSelectedYear(y);
  }, []);


  // ✅ only change when global currency changes (prevents country pages going 0)
  const fetchCurrencyKey = isGlobalPage ? homeCurrency : "country";

  useEffect(() => {
    if (!countryName) return;
    if (range === "" || !selectedYear) return;

    fetchUploadHistory(
      range,
      selectedMonth,
      selectedQuarter || "",
      selectedYear,
      countryName
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [range, selectedMonth, selectedQuarter, selectedYear, countryName, fetchCurrencyKey]);

  // Fetch AI summary/recommendations for the selected period
  useEffect(() => {
    if (!range || !selectedYear) {
      setAiPanel(null);
      return;
    }

    // align with the same dropdown validity rules
    const ready =
      (range === "monthly" && !!selectedMonth && !!selectedYear) ||
      (range === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (range === "yearly" && !!selectedYear);

    if (!ready) {
      setAiPanel(null);
      return;
    }

    fetchAiSummary(range);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [range, selectedMonth, selectedQuarter, selectedYear, countryName]);


  useEffect(() => {
    if (!range || !selectedYear) {
      setPerformanceTrend(null);
      return;
    }

    const ready =
      (range === "monthly" && !!selectedMonth && !!selectedYear) ||
      (range === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (range === "yearly" && !!selectedYear);

    if (!ready) {
      setPerformanceTrend(null);
      return;
    }

    fetchPerformanceTrendFromHistory(range);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [range, selectedMonth, selectedQuarter, selectedYear, countryName, homeCurrency, performanceTrendMetric]);


  const cropPngBase64WithSize = async (
    base64: string,
    pad = 0,
    opts?: {
      // how close to white counts as background (0-255)
      whiteThreshold?: number;     // default 253
      // how much non-bg must exist in a row/col to keep it (0..1)
      minContentRatio?: number;    // default 0.002 (0.2%)
    }
  ): Promise<{ base64: string; w: number; h: number }> => {
    const isDataUrl = base64.startsWith("data:image/");
    const raw = base64.includes("base64,") ? base64.split("base64,")[1] : base64;

    const img = new Image();

    // ✅ Use original data URL if present (jpeg/png)
    // ✅ Otherwise assume png (your older charts send raw png base64)
    img.src = isDataUrl ? base64 : `data:image/png;base64,${raw}`;


    await new Promise<void>((res, rej) => {
      img.onload = () => res();
      img.onerror = () => rej(new Error("Failed to load image for cropping"));
    });

    const canvas = document.createElement("canvas");
    const ctx = canvas.getContext("2d");
    if (!ctx) return { base64: raw, w: img.width, h: img.height };

    canvas.width = img.width;
    canvas.height = img.height;
    ctx.drawImage(img, 0, 0);

    const { data, width, height } = ctx.getImageData(0, 0, canvas.width, canvas.height);

    const whiteThreshold = opts?.whiteThreshold ?? 253;
    const minContentRatio = opts?.minContentRatio ?? 0.002; // 0.2%

    // Background: transparent OR near-white
    const isBg = (r: number, g: number, b: number, a: number) => {
      if (a === 0) return true;
      return r >= whiteThreshold && g >= whiteThreshold && b >= whiteThreshold;
    };

    // Count non-bg pixels in a row
    const rowContentRatio = (y: number) => {
      let nonBg = 0;
      for (let x = 0; x < width; x++) {
        const i = (y * width + x) * 4;
        const r = data[i], g = data[i + 1], b = data[i + 2], a = data[i + 3];
        if (!isBg(r, g, b, a)) nonBg++;
      }
      return nonBg / width;
    };

    // Count non-bg pixels in a col
    const colContentRatio = (x: number) => {
      let nonBg = 0;
      for (let y = 0; y < height; y++) {
        const i = (y * width + x) * 4;
        const r = data[i], g = data[i + 1], b = data[i + 2], a = data[i + 3];
        if (!isBg(r, g, b, a)) nonBg++;
      }
      return nonBg / height;
    };

    // Trim top/bottom by density
    let top = 0;
    while (top < height && rowContentRatio(top) < minContentRatio) top++;

    let bottom = height - 1;
    while (bottom >= 0 && rowContentRatio(bottom) < minContentRatio) bottom--;

    // Trim left/right by density
    let left = 0;
    while (left < width && colContentRatio(left) < minContentRatio) left++;

    let right = width - 1;
    while (right >= 0 && colContentRatio(right) < minContentRatio) right--;

    // If nothing meaningful found, return original
    if (right <= left || bottom <= top) return { base64: raw, w: img.width, h: img.height };

    // Apply pad
    left = Math.max(0, left - pad);
    top = Math.max(0, top - pad);
    right = Math.min(width - 1, right + pad);
    bottom = Math.min(height - 1, bottom + pad);

    const cropW = right - left + 1;
    const cropH = bottom - top + 1;

    const out = document.createElement("canvas");
    const outCtx = out.getContext("2d");
    if (!outCtx) return { base64: raw, w: img.width, h: img.height };

    out.width = cropW;
    out.height = cropH;

    outCtx.drawImage(canvas, left, top, cropW, cropH, 0, 0, cropW, cropH);

    return {
      base64: out.toDataURL("image/png").split("base64,")[1],
      w: cropW,
      h: cropH,
    };
  };

  const toJpegBase64 = async (
    base64: string,
    quality = 0.98,
    opts?: { scale?: number; bg?: string }
  ): Promise<{ base64: string; w: number; h: number }> => {
    const raw = base64.includes("base64,") ? base64.split("base64,")[1] : base64;

    // allow passing either png or jpeg data (we always load as image/*)
    const img = new Image();
    img.src = base64.startsWith("data:image/")
      ? base64
      : `data:image/png;base64,${raw}`;

    await new Promise<void>((res, rej) => {
      img.onload = () => res();
      img.onerror = () => rej(new Error("Failed to load image for JPEG conversion"));
    });

    const scale = opts?.scale ?? 1;          // ✅ upscale to reduce Excel seams
    const bg = opts?.bg ?? "#FFFFFF";        // ✅ solid bg

    const canvas = document.createElement("canvas");
    canvas.width = Math.max(1, Math.round(img.width * scale));
    canvas.height = Math.max(1, Math.round(img.height * scale));

    const ctx = canvas.getContext("2d");
    if (!ctx) return { base64: raw, w: img.width, h: img.height };

    // ✅ high quality scaling
    ctx.imageSmoothingEnabled = true;
    ctx.imageSmoothingQuality = "high";

    // ✅ solid white background
    ctx.fillStyle = bg;
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    ctx.drawImage(img, 0, 0, canvas.width, canvas.height);

    return {
      base64: canvas.toDataURL("image/jpeg", quality).split("base64,")[1],
      w: canvas.width,
      h: canvas.height,
    };
  };

  const handleDownloadSkuSheet1 = async () => {
    try {
      const wb = new ExcelJS.Workbook();
      const wsSku = wb.addWorksheet("SKU Profitability");

      if (skuExportPayload?.sheetModel) {
        buildSkuWorksheetFromModel(wsSku, skuExportPayload.sheetModel);
      } else {
        wsSku.addRow(["SKU sheet model not available"]);
      }

      const buffer = await wb.xlsx.writeBuffer();
      const periodLabel = getPeriodLabelShort();
      const fileName = `SKU-wise Profitability - ${periodLabel || String(selectedYear)}.xlsx`;

      saveAs(
        new Blob([buffer], {
          type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }),
        fileName
      );
    } catch (e) {
      console.error("SKU Sheet 1 export failed:", e);
    }
  };


  useEffect(() => {
    if (range === "monthly") {
      setAllDropdownsSelected(!!selectedMonth && !!selectedYear);
    } else if (range === "quarterly") {
      setAllDropdownsSelected(!!selectedQuarter && !!selectedYear);
    } else if (range === "yearly") {
      setAllDropdownsSelected(!!selectedYear);
    } else {
      setAllDropdownsSelected(false);
    }
  }, [range, selectedMonth, selectedQuarter, selectedYear]);


  useEffect(() => {
    if (typeof document === "undefined") return;

    const body = document.body;

    if (showNoDataOverlay) {
      body.style.overflow = "hidden"; // lock both X/Y scroll
    } else {
      body.style.overflow = ""; // restore default
    }

    return () => {
      body.style.overflow = ""; // cleanup on unmount
    };
  }, [showNoDataOverlay]);


  useEffect(() => {
    if (!range || !selectedYear) return;

    const ready =
      (range === "monthly" && !!selectedMonth && !!selectedYear) ||
      (range === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (range === "yearly" && !!selectedYear);

    if (!ready) {
      setBargraphUploads([]);
      return;
    }

    const fetchBargraphData = async () => {
      setBargraphLoading(true);

      try {
        const token =
          typeof window !== "undefined"
            ? localStorage.getItem("jwtToken")
            : null;

        const timeline =
          range === "monthly"
            ? monthNameToNumber(selectedMonth)
            : range === "quarterly"
              ? selectedQuarter
              : "ALL";

        const url = new URL(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_history` // 🔁 replace with your actual endpoint
        );

        url.searchParams.set("country", countryName);
        url.searchParams.set("period", range);
        url.searchParams.set("timeline", String(timeline));
        url.searchParams.set("year", String(selectedYear));

        if (countryName.toLowerCase() === "global" && homeCurrency) {
          url.searchParams.set("homeCurrency", homeCurrency);
        }

        const res = await fetch(url.toString(), {
          method: "GET",
          headers: token ? { Authorization: `Bearer ${token}` } : {},
          cache: "no-store",
        });

        if (!res.ok) {
          setBargraphUploads([]);
          return;
        }

        const data = await res.json();

        setBargraphUploads(data.uploads ?? []);
        setBargraphUserMeta(data.userMeta ?? null);
      } catch (err) {
        setBargraphUploads([]);
      } finally {
        setBargraphLoading(false);
      }
    };

    fetchBargraphData();
  }, [
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    countryName,
    homeCurrency,
  ]);

  useEffect(() => {
    if (!range || !selectedYear) return;

    const ready =
      (range === "monthly" && !!selectedMonth && !!selectedYear) ||
      (range === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (range === "yearly" && !!selectedYear);

    if (!ready) {
      setGraphPageUploads([]);
      return;
    }

    const fetchGraphPageUploads = async () => {
      setGraphPageLoading(true);
      setGraphPageError(null);

      try {
        const token =
          typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_history`);

        // ✅ Only send homeCurrency for GLOBAL
        if (countryName.toLowerCase() === "global" && homeCurrency) {
          url.searchParams.set("homeCurrency", homeCurrency);
        }

        const res = await fetch(url.toString(), {
          method: "GET",
          headers: token ? { Authorization: `Bearer ${token}` } : {},
          cache: "no-store",
        });

        if (!res.ok) {
          const j = await res.json().catch(() => ({}));
          throw new Error(j?.error || "Failed to fetch upload history");
        }

        const json = await res.json();
        const rows: UploadRow[] = json?.uploads ?? [];

        const isGlobal = countryName.toLowerCase() === "global";
        const normalizedHomeCurrency = (homeCurrency || "").trim().toLowerCase();
        const isUsd = normalizedHomeCurrency === "usd";

        const filtered = rows.filter((r) => {
          const c = (r.country || "").toLowerCase();

          if (isGlobal) {
            if (isUsd) return c === "global" || c === "global_usd";
            return c === `global_${normalizedHomeCurrency}`;
          }

          return c === countryName.toLowerCase();
        });

        setGraphPageUploads(filtered);

        // ✅ If you already have userData in parent, prefer that:
        // setGraphPageUserMeta({ company_name: userData?.company_name, brand_name: userData?.brand_name });

        // Otherwise if API returns it:
        setGraphPageUserMeta(json?.userMeta ?? null);
      } catch (e: any) {
        setGraphPageUploads([]);
        setGraphPageError(e?.message || "Failed to fetch upload history");
      } finally {
        setGraphPageLoading(false);
      }
    };

    fetchGraphPageUploads();
  }, [range, selectedMonth, selectedQuarter, selectedYear, countryName, homeCurrency]);


  const goBack = () => router.push("/pnl-dashboard/QTD/global/NA/NA");

  if (month === "NA" || year === "NA") {
    return <IntegrationDashboard />;
  }

  /* 🌟 Initial fullscreen loader for this page */
  const hasAnyContent = !!uploadsData?.summary;
  const initialLoading = loading && !hasAnyContent;



  if (initialLoading) {
    return (
      <Loader fullscreen transparent />
    );
  }

  // 🔹 4) TITLE HELPERS FOR THE OVERLAY
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


  const getCountryLabel = () => {
    const c = (countryName || "").toLowerCase();
    return c === "global" ? "GLOBAL" : (countryName || "").toUpperCase();
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

  const getPnLTitleParts = () => {
    return {
      country: getCountryLabel(),
      period: getPeriodLabelShort(),
    };
  };


  const getTrendWrapperHeight = () => {
    if (focusedChart === "trend") return "h-[50vh]";
    // monthly stays perfect
    if (range === "monthly") return "h-[360px]";
    // restore previous intended size for quarterly/yearly
    return "h-[375px] 2xl:h-[500px]";
  };

  return (
    <div ref={layoutRef} className="space-y-3 relative">
      {/* ===================== STICKY HEADER (EXISTING) ===================== */}
      <div className="sticky top-0 z-40 w-full flex flex-col bg-[#F7F7F7] sm:flex-row md:items-center md:justify-between gap-4 ">
        {/* LEFT: Title + Subtitle */}
        <div className="flex flex-col leading-tight w-full md:w-auto ">
          <div className="flex items-baseline gap-2">
            <PageBreadcrumb pageTitle="Financial Metrics -" variant="page" align="left" textSize="2xl" />
            <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
              Amazon{" "}
              {countryName?.toLowerCase() === "global" ? "Global" : countryName?.toUpperCase()}
            </span>
          </div>
          <p className="text-xs 2xl:text-sm text-charcoal-500 mt-1">
            Track your profitability and key metrics
          </p>
        </div>

        {/* RIGHT: Filters */}
        <div className="flex w-full mb-2 sm:mb-0 md:w-auto justify-start md:justify-end">
          <PeriodFiltersTable
            range={range === "" ? "yearly" : (range as "monthly" | "quarterly" | "yearly")}
            selectedMonth={selectedMonth}
            selectedQuarter={selectedQuarter || ""}
            selectedYear={selectedYear}
            yearOptions={yearOptions}
            onRangeChange={handleRangeChange}
            onMonthChange={handleMonthChange}
            onQuarterChange={handleQuarterChange}
            onYearChange={handleYearChange}
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
            // optional: prevent switching if disabled
            if (tabsDisabled?.[t]) return;
            setActiveTab(t);
          }}
          className="w-full"
          textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
          compact
        />
      </div>

      {/* ===================== SUMMARY CARDS (OPTIONAL: ALWAYS SHOW) ===================== */}
      {/* If you want summary cards ALWAYS visible regardless of tab, keep this block here */}
      <div className="flex flex-col gap-5 w-full mt-4">
        {/* Summary Cards */}
        {uploadsData?.summary &&
          (() => {
            const summary = displayData;
            const netSales = summary.total_sales;


            // ✅ comparisons (camelCase OR snake_case)
            const rawComparisons =
              (uploadsData as any).summaryComparisons ??
              (uploadsData as any).summary_comparisons;

            const comparisons: SummaryComparisons | undefined = rawComparisons
              ? (rawComparisons as SummaryComparisons)
              : undefined;

            const formatMoney = (val: number, opts?: { showPlus?: boolean }) => {
              const num = Number(val || 0);
              const sign = num < 0 ? "-" : opts?.showPlus && num > 0 ? "+" : "";
              const abs = Math.abs(num);

              return `${sign}${currencySymbol}${abs.toLocaleString(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              })}`;
            };


            // ✅ Cost of Ads
            const costOfAds = summary.advertising_total ?? 0;

            // ✅ "ROAS" as you defined: (Cost of Ads / Net Sales) * 100
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
              const delta = hasPrev ? roas - prevVal! : null;

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
                  ? delta > 0
                    ? "▼"
                    : delta < 0
                      ? "▲"
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

            const formatMoneyPerUnit = (total: number, units: number) => {
              const perUnit = safeDiv(total, units);

              const totalText = formatMoney(total);
              const perUnitText = formatMoney(perUnit);

              // If no units, show dash (avoid misleading /unit)
              if (!units) return `${totalText} (-/unit)`;

              return `${totalText} (${perUnitText}/unit)`;
            };

            const renderMoneyWithPerUnit = (total: number, units: number) => {
              const totalText = formatMoney(total);

              if (!units) {
                return <span>{totalText}</span>;
              }

              const perUnit = total / units;
              const perUnitText = formatMoney(perUnit);

              return (
                <div className="flex items-baseline gap-1 leading-tight">
                  {/* Main value */}
                  <span className="text-sm 2xl:text-lg font-semibold">
                    {totalText}
                  </span>

                  {/* Per-unit (smaller, muted) */}
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

            const isSummaryZero =
              summary.unit_sold === 0 &&
              summary.total_sales === 0 &&
              summary.total_expense === 0 &&
              summary.cm2_profit === 0;

            // const cm2Percent =
            //   summary.total_sales > 0 ? (summary.cm2_profit / summary.total_sales) * 100 : 0;

            const cm2Percent =
              netSales > 0 ? (summary.cm2_profit / netSales) * 100 : 0;

            // ---------- generic comparisons helper ----------
            const getComparisons = (metric: keyof Summary): ComparisonItem[] => {
              const current = summary[metric] ?? 0;

              const lm = comparisons?.lastMonth?.[metric];
              const lq = comparisons?.lastQuarter?.[metric];
              const ly = comparisons?.lastYear?.[metric];

              const makeItem = (label: string, prevVal?: number): ComparisonItem => {
                if (typeof prevVal !== "number") return { label, value: undefined, diffPct: null };
                const diffPct = prevVal === 0 ? null : ((current - prevVal) / prevVal) * 100;
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

            const renderComparisons = (metric: keyof Summary, formatter: (val: number) => string) => {
              const items = getComparisons(metric);
              if (!items.length) return null;

              return (
                <div className="2xl:mt-3 space-y-2">
                  {items.map((item) => {
                    const hasValue = typeof item.value === "number" && !isNaN(item.value);
                    const hasDiff = typeof item.diffPct === "number" && !isNaN(item.diffPct);

                    const diffClass = hasDiff
                      ? item.diffPct! >= 0
                        ? "text-emerald-600"
                        : "text-red-600"
                      : "text-gray-400";

                    return (
                      <div
                        key={item.label}
                        className="flex items-end text-charcoal-500 justify-between gap-3 text-[10px] 2xl:text-xs leading-tight tabular-nums"
                      >
                        <div className="min-w-0">
                          <div className=" whitespace-nowrap">
                            {item.label}:
                          </div>
                          <div className=" whitespace-nowrap">
                            {hasValue ? formatter(item.value!) : "-"}
                          </div>
                        </div>

                        <div className={`font-bold whitespace-nowrap ${diffClass}`}>
                          {hasDiff ? (
                            <>
                              {item.diffPct! >= 0 ? "▲" : "▼"}{" "}
                              {Math.abs(item.diffPct!).toFixed(2)}%
                            </>
                          ) : (
                            "-"
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              );
            };

            const buildComparisonsRows = (
              metric: keyof Summary,
              formatter: (val: number) => string
            ) => {
              const items = getComparisons(metric);

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
                  valueText: hasValue ? formatter(item.value!) : "-",
                  deltaText,
                  deltaClassName,
                };
              });
            };


            const pickNum = (obj: any, keys: string[]) => {
              for (const k of keys) {
                const v = obj?.[k];
                if (v === 0) return 0;
                if (typeof v === "number" && !isNaN(v)) return v;
                if (typeof v === "string" && v.trim() !== "" && !isNaN(Number(v))) return Number(v);
              }
              return 0;
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

            const renderGrossSalesComparisons = () => {
              const items = getGrossSalesComparisons();
              if (!items.length) return null;
              return (
                <div className="mt-3 space-y-2">
                  {items.map((item) => {
                    const hasValue = typeof item.value === "number" && !isNaN(item.value);
                    const hasDiff = typeof item.diffPct === "number" && !isNaN(item.diffPct);

                    const diffClass = hasDiff
                      ? item.diffPct! >= 0
                        ? "text-emerald-600"
                        : "text-red-600"
                      : "text-gray-400";

                    return (
                      <div
                        key={item.label}
                        className="flex items-end text-charcoal-500 justify-between gap-3 text-[10px] 2xl:text-xs leading-tight tabular-nums"
                      >
                        <div className="min-w-0">
                          <div className="whitespace-nowrap">
                            {item.label}:
                          </div>
                          <div className="whitespace-nowrap">
                            {hasValue ? formatMoney(item.value!) : "-"}
                          </div>
                        </div>

                        <div className={`font-bold whitespace-nowrap ${diffClass}`}>
                          {hasDiff ? (
                            <>
                              {item.diffPct! >= 0 ? "▲" : "▼"}{" "}
                              {Math.abs(item.diffPct!).toFixed(2)}%
                            </>
                          ) : (
                            "-"
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              );
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
                hasPrev && prevVal !== 0 ? ((cm2Percent - prevVal) / prevVal) * 100 : null;

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
                value: renderMoneyWithPerUnit(getGrossSales(summary), summary.unit_sold),
                // className: "border border-[#ED9F50] bg-[#ED9F504D]",
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
                      valueText: hasValue ? formatMoney(item.value!) : "-",
                      deltaText,
                      deltaClassName,
                    };
                  });
                })(),
              },
              {
                key: "netSales",
                title: "Net Sales",
                value: renderMoneyWithPerUnit(netSales, summary.unit_sold),
                // className: "border border-[#75BBDA] bg-[#75BBDA4D]",
                className: "bg-white border border-[#75BBDA] border-t-4 border-t-[#75BBDA]",
                comparisons: buildComparisonsRows("total_sales", formatMoney),
              },

              {
                key: "expenses",
                title: "Marketplace Fees",
                value: renderMoneyWithPerUnit(summary.total_expense, summary.unit_sold),
                // className: "border border-[#B75A5A] bg-[#B75A5A4D]",
                className: "bg-white border border-[#B75A5A] border-t-4 border-t-[#B75A5A]",
                comparisons: buildComparisonsRows("total_expense", formatMoney),
              },
              {
                key: "ads",
                title: "Cost of Advertisement",
                value: renderMoneyWithPerUnit(costOfAds, summary.unit_sold),
                // className: "border border-[#C49466] bg-[#C494664D]",
                className: "bg-white border border-[#C49466] border-t-4 border-t-[#C49466]",
                comparisons: buildComparisonsRows("advertising_total", formatMoney),
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
                value: renderMoneyWithPerUnit(summary.cm2_profit, summary.unit_sold),
                // className: "border border-[#B8C78C] bg-[#B8C78C4D]",
                className: "bg-white border border-[#B8C78C] border-t-4 border-t-[#B8C78C]",
                comparisons: buildComparisonsRows("cm2_profit", formatMoney),
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
                  isSummaryZero ? "opacity-30" : "opacity-100",
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

      {/* ===================== TAB CONTENT AREA ===================== */}
      <div className="w-full">
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
                      />

                      <CMchartofsku
                        range="monthly"
                        month={selectedMonth}
                        selectedQuarter={undefined}
                        year={selectedYear}
                        countryName={initialCountryName}
                        homeCurrency={globalHomeCurrency}
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
                      />

                      <CMchartofsku
                        range="quarterly"
                        month={undefined}
                        selectedQuarter={selectedQuarter}
                        year={selectedYear}
                        countryName={initialCountryName}
                        homeCurrency={globalHomeCurrency}
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
                    />

                    <CMchartofsku
                      range="yearly"
                      month={undefined}
                      selectedQuarter={undefined}
                      year={selectedYear}
                      countryName={initialCountryName}
                      homeCurrency={globalHomeCurrency}
                      onExportBase64Ready={setProductWiseCm1PieBase64}
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
                {aiPanel?.objective && <MonthlyObjectiveStrip objective={aiPanel.objective} />}

                <AiSingleInsightCard
                  loading={false} // ✅ parent controls loader now
                  error={null}    // ✅ parent controls error now
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
                  homeCurrency={globalHomeCurrency}
                  countryName={initialCountryName}
                  portfolioRecommendation={aiPanel?.portfolioRecommendation} // ✅ ADD
                />
              </>
            )}
          </div>
        )}


        {/* ---------- TAB 3: EXPENSE BREAKUP ---------- */}
        {/* {activeTab === "breakdowns" && allDropdownsSelected && (
          <div className="mt-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6 items-stretch">
            
              <div className="w-full rounded-xl border border-gray-200 bg-white p-4">
                <CircleChart
                  range={range as Exclude<RangeType, "">}
                  month={range === "monthly" ? selectedMonth : undefined}
                  selectedQuarter={range === "quarterly" && selectedQuarter ? selectedQuarter : undefined}
                  year={selectedYear}
                  countryName={initialCountryName}
                  homeCurrency={globalHomeCurrency}
                  onExportBase64Ready={setExpenseBreakdownPieBase64}
                />
              </div>

             
              <div className="w-full rounded-xl border border-gray-200 bg-white p-4">
                <CMchartofsku
                  range={range as Exclude<RangeType, "">}
                  month={range === "monthly" ? selectedMonth : undefined}
                  selectedQuarter={range === "quarterly" && selectedQuarter ? selectedQuarter : undefined}
                  year={selectedYear}
                  countryName={initialCountryName}
                  homeCurrency={globalHomeCurrency}
                  onExportBase64Ready={setProductWiseCm1PieBase64}
                />
              </div>
            </div>
          </div>
        )} */}

        {/* ---------- TAB 4: SKU / PRODUCTWISE P&L ---------- */}
        {activeTab === "skuBreakdown" && allDropdownsSelected && (
          <div id="pnl-breakdown" className="mt-4 space-y-4 scroll-mt-[80px]">
            <SKUtable
              range={range as Exclude<RangeType, "">}
              month={range === "monthly" ? selectedMonth : undefined}
              quarter={range === "quarterly" ? selectedQuarter : undefined}
              year={selectedYear}
              countryName={initialCountryName}
              homeCurrency={globalHomeCurrency}
              hideDownloadButton={false}
              onExportPayloadChange={setSkuExportPayload}
              onDownload={handleDownloadSkuSheet1}
              onRowsChange={setSkuRows}
            />

            {skuRows.length > 0 && (
              <SkuTopBottomTables
                topData={topData}
                bottomData={bottomData}
                currencySymbol={currencySymbol}
              />
            )}
          </div>
        )}
      </div>

      {/* ===================== YOUR EXISTING OVERLAYS / MODALS (KEEP) ===================== */}
      {showNoDataOverlay && (
        <div
          className="fixed inset-y-0 z-[9999] flex items-center justify-center pointer-events-none"
          style={{ left: overlayBounds.left, width: overlayBounds.width || "100%" }}
        >
          {/* keep your existing overlay card */}
        </div>
      )}

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
    </div>
  );

};

export default Dropdowns;