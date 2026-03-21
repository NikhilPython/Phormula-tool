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
import Productinfoinpopup from '@/components/businessInsight/Productinfoinpopup';
import { useRouter } from "next/navigation";
import SegmentedToggle from "@/components/ui/SegmentedToggle";
import ProductwisePerformance from "@/features/productwiseperformance/ProductwisePerformance";
import CashFlowPage from "@/app/(admin)/cashflow/[countryName]/[month]/[year]/CashFlowClient";
import { jwtDecode } from "jwt-decode";

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

type RangeType = "monthly" | "quarterly" | "yearly" | "";

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

  const inventoryRecoBullets = mergeToSingleBullet(toBullets(inventoryText));
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

                <p className="mt-3 text-xs text-gray-500">
                  Demo data is shown for preview only.
                </p>
              </div>
            </div>
          </div>
        </>
      )}
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

  const [perfMetric, setPerfMetric] = useState<"net_sales" | "units">("net_sales");

  const hasBlocks = blocks.length > 0; // ✅ compute instead of early return

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
              {b.recommendationBullets?.length > 0 && (
                <p className="text-xs 2xl:text-sm text-slate-700 leading-relaxed">
                  {b.recommendationBullets.join(" ")}
                </p>
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
}: {
  objective?: ObjectivePayload;
  className?: string;
  targetSummary?: {
    target_sales?: number;
    shortfall_total?: number;
    cashflow_total?: number;
  } | null;
  currencySymbol?: string;
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
            pageTitle="Monthly Objectives & Targets"
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
            <div className={`absolute left-0 top-0 h-full w-1 `} />

            <div className="pl-2">
              <div className="text-[11px] 2xl:text-xs font-medium text-slate-500 leading-tight">
                {item.label}
              </div>

              <div
                className={`mt-2  text-sm font-semibold leading-snug capitalize break-words ${item.valueClass}`}
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

    if (/^\d{4}$/.test(p)) return p;

    const [month, year] = p.split(/\s+/);
    if (!month || !year) return p;

    const shortMonth = month.slice(0, 3);
    const shortYear = year.slice(-2);
    return `${shortMonth}’${shortYear}`;
  };

  return `(${formatPart(leftRaw)} vs ${formatPart(rightRaw)})`;
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
  homeCurrency,
  targetSummary,
  currencySymbol = "$",
}) => {
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

  const drawerPeriodText =
    narrativeInsights?.[0] ? formatSummaryPeriod(narrativeInsights[0]) : "";

  return (
    <div className="flex flex-col gap-5">
      <div className="w-full space-y-4">
        {(narrativeInsights.length > 0 || objective) && (
          <div className="grid grid-cols-1 xl:grid-cols-12 gap-4 items-stretch">
            {narrativeInsights.length > 0 && (
              <div className="xl:col-span-7 rounded-xl border border-slate-200 bg-white shadow-sm p-4 h-full">
                <div className="space-y-3 h-full">
                  <h2 className="text-lg 2xl:text-2xl text-[#414042] font-bold leading-snug">
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
                  className="h-full"
                />
              </div>
            ) : null}
          </div>
        )}

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

        {/* Inventory Section */}
        {inventoryBullets.length > 0 && (
          <div className="space-y-4 rounded-xl border border-slate-200 bg-white shadow-sm p-4">
            <div className="flex items-center gap-2">
              <span className="text-base 2xl:text-2xl font-bold text-slate-800">
                Inventory Insights
              </span>
            </div>

            {(() => {
              const detailLines = inventoryBullets.filter((b) =>
                /for detailed/i.test(b)
              );
              const mainLines = inventoryBullets.filter(
                (b) => !/for detailed/i.test(b)
              );

              return (
                <>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
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
                            className="flex items-start justify-between gap-3 bg-white rounded-lg p-3 border border-amber-100"
                          >
                            <span className="text-sm font-medium text-slate-700">
                              {label}
                            </span>
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
                      const right = hasColon
                        ? raw.slice(colonIdx + 1).trim()
                        : "";

                      return (
                        <div
                          key={i}
                          className="flex justify-between items-center bg-white rounded-lg p-3 border border-amber-100"
                        >
                          <span className="text-sm font-medium text-slate-700">
                            {left}
                          </span>
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
    "Business performance remained healthy (Jan 26 vs Dec 25)",
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
    "Recommendation: Continue scaling on high-converting keywords and protect margin.",
    "Inventory Action: Maintain healthy stock cover for next cycle.",

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
    product_name: "Sample Product A",
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
    product_name: "Sample Product B",
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

  // const ranged = initialRanged;
  // const countryName = initialCountryName;
  // const month = initialMonth;
  // const year = initialYear;

  const ranged = initialRanged;
  const countryName = initialCountryName || "";
  const month = initialMonth || "";
  const year = initialYear || "";

  // const isGlobalPage = countryName.toLowerCase() === "global";
  const isGlobalPage = (countryName || "").toLowerCase() === "global";

  const globalHomeCurrency = isGlobalPage ? homeCurrency : undefined;

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

  const buildParentSkuUrl = () => {
    if (range === "monthly") {
      const skuwiseFileName =
        initialCountryName.toLowerCase() === "global"
          ? `skuwisemonthly_${userid}_${initialCountryName}_${(selectedMonth || "").toLowerCase()}${selectedYear}_table`
          : `skuwisemonthly_${userid}_${initialCountryName.toLowerCase()}_${(selectedMonth || "").toLowerCase()}${selectedYear}`;

      const url = new URL(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/skutableprofit/${skuwiseFileName}`
      );

      url.searchParams.set("country", initialCountryName);
      url.searchParams.set("month", (selectedMonth || "").toLowerCase());
      url.searchParams.set("year", String(selectedYear));

      if (initialCountryName.toLowerCase() === "global" && globalHomeCurrency) {
        url.searchParams.set("homeCurrency", globalHomeCurrency);
      }

      return url.toString();
    }

    if (range === "quarterly") {
      const backendQuarter = quarterMapping[selectedQuarter] || "";

      const url = new URL(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/quarterlyskutable`
      );

      url.searchParams.set("quarter", backendQuarter);
      url.searchParams.set("country", initialCountryName);
      url.searchParams.set("year", String(selectedYear));
      url.searchParams.set("userid", String(userid));

      if (initialCountryName.toLowerCase() === "global" && globalHomeCurrency) {
        url.searchParams.set("homeCurrency", globalHomeCurrency);
      }

      return url.toString();
    }

    const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/YearlySKU`);
    url.searchParams.set("country", initialCountryName);
    url.searchParams.set("year", String(selectedYear));

    if (initialCountryName.toLowerCase() === "global" && globalHomeCurrency) {
      url.searchParams.set("homeCurrency", globalHomeCurrency);
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



  const [collapsed, setCollapsed] = useState(false);

  const [range, setRange] = useState<RangeType>("");
  const [selectedMonth, setSelectedMonth] = useState<string>("");
  const [selectedYear, setSelectedYear] = useState<string>("");
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

  const pnlCollapsed = focusedChart !== "pnl";

  // ---------------- AI Summary Panel state ----------------
  const [aiPanel, setAiPanel] = useState<AiPanelData | null>(
    isDemoMode ? DEMO_AI_PANEL : null
  );
  const [aiPanelLoading, setAiPanelLoading] = useState(false);
  const [aiPanelError, setAiPanelError] = useState<string | null>(null);

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

  const { topData, bottomData } = useMemo(() => computeTopBottom5(skuRows), [skuRows]);

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


  const displayData: Summary =
    isDemoMode
      ? DEMO_SUMMARY
      : allDropdownsSelected && uploadsData?.summary
        ? uploadsData.summary
        : zeroData;

  // range: "monthly" | "quarterly" | "yearly"
  const handleRangeChange = (v: "monthly" | "quarterly" | "yearly") => {
    setRange(v);
    setSelectedMonth("");
    setSelectedQuarter("");
    setSelectedYear(isDemoMode ? String(new Date().getFullYear()) : "");

    if (isDemoMode) {
      setUploadsData(DEMO_UPLOAD_HISTORY);
      setAiPanel(DEMO_AI_PANEL);
      setPerformanceTrend(DEMO_PERFORMANCE_TREND);
      setBargraphUploads(DEMO_UPLOADS);
      setGraphPageUploads(DEMO_UPLOADS);
      setSkuRows(DEMO_SKU_ROWS);
      setTargetSummary(DEMO_TARGET_SUMMARY);
      return;
    }

    setUploadsData(null);
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
      setUploadsData(DEMO_UPLOAD_HISTORY);
      return;
    }
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
        return;
      }

      const data: UploadHistoryResponse = await res.json();
      setUploadsData(data);

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

    } catch (error) {
      console.error("Error fetching data: ", error);
    } finally {
      setLoading(false);
    }
  };


  const fetchAiSummary = async (rangeType: RangeType) => {
    if (isDemoMode) {
      setAiPanelLoading(false);
      setAiPanelError(null);
      setAiPanel(DEMO_AI_PANEL);
      return;
    }
    if (!countryName || !rangeType || !selectedYear) return;

    const requestId = ++aiRequestIdRef.current;

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
    setAiPanel(null);

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

    let apiMonth = "";

    if (range === "monthly" && selectedMonth) {
      apiMonth =
        selectedMonth.charAt(0).toUpperCase() + selectedMonth.slice(1).toLowerCase();
    } else {
      const now = new Date();
      apiMonth = now.toLocaleString("en-US", { month: "long" });
    }

    try {
      setTargetSummaryLoading(true);

      const token =
        typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

      const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/target-summary`);
      url.searchParams.set("month", apiMonth);
      url.searchParams.set("year", selectedYear);
      url.searchParams.set("country", initialCountryName.toLowerCase());

      const res = await fetch(url.toString(), {
        method: "GET",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        cache: "no-store",
      });

      if (!res.ok) {
        setTargetSummary(null);
        return;
      }

      const data = await res.json();

      setTargetSummary({
        target_sales: Number(data?.target_sales ?? 0),
        shortfall_total: Number(data?.shortfall_total ?? 0),
        cashflow_total: Number(data?.cashflow_total ?? 0),
      });
    } catch (error) {
      console.error("Failed to fetch target summary:", error);
      setTargetSummary(null);
    } finally {
      setTargetSummaryLoading(false);
    }
  };

  const handleMonthChange = (v: string) => {
    setSelectedMonth(v);

    if (isDemoMode) {
      setUploadsData(DEMO_UPLOAD_HISTORY);
      return;
    }

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

    if (isDemoMode) {
      setUploadsData(DEMO_UPLOAD_HISTORY);
      return;
    }

    if (selectedYear && q) {
      fetchUploadHistory(range, selectedMonth, q, selectedYear, countryName);
    } else {
      setUploadsData(null);
    }
  };

  const handleYearChange = (v: string) => {
    setSelectedYear(v);

    if (isDemoMode) {
      setUploadsData(DEMO_UPLOAD_HISTORY);
      return;
    }

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

    setRange("yearly");
    setSelectedMonth("");
    setSelectedQuarter("");

    const y = computeDefaultYearlyYear();
    setSelectedYear(y);
  }, [isDemoMode]);


  const fetchCurrencyKey = isGlobalPage ? homeCurrency : "country";

  useEffect(() => {
    if (isDemoMode) return;
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
  }, [range, selectedMonth, selectedQuarter, selectedYear, countryName, fetchCurrencyKey, isDemoMode]);

  useEffect(() => {
    if (isDemoMode) return;

    if (!range || !selectedYear) {
      setAiPanel(null);
      return;
    }

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
  }, [range, selectedMonth, selectedQuarter, selectedYear, countryName, isDemoMode]);


  useEffect(() => {
    if (isDemoMode) return;

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
  }, [range, selectedMonth, selectedQuarter, selectedYear, countryName, homeCurrency, performanceTrendMetric, isDemoMode]);

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
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_history`
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
    isDemoMode,
  ]);

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
        setGraphPageUserMeta(json?.userMeta ?? null);
      } catch (e: any) {
        setGraphPageUploads([]);
        setGraphPageError(e?.message || "Failed to fetch upload history");
      } finally {
        setGraphPageLoading(false);
      }
    };

    fetchGraphPageUploads();
  }, [range, selectedMonth, selectedQuarter, selectedYear, countryName, homeCurrency, isDemoMode]);


  useEffect(() => {
    if (isDemoMode) {
      setSkuRows(DEMO_SKU_ROWS);
      return;
    }
    const ready =
      (range === "monthly" && !!selectedMonth && !!selectedYear) ||
      (range === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (range === "yearly" && !!selectedYear);

    if (!ready || !initialCountryName) {
      setSkuRows([]);
      return;
    }

    const ac = new AbortController();

    const normalizeRowsForParent = (data: any[]): TableRow[] => {
      return data.map((row) => {
        const productName =
          row?.product_name && String(row.product_name).trim() !== ""
            ? String(row.product_name)
            : row?.sku && String(row.sku).trim() !== ""
              ? String(row.sku)
              : "-";

        const isTotalRow = productName.trim().toLowerCase() === "total";

        const toNumber = (v: any) => {
          if (v === undefined || v === null || v === "") return 0;
          if (typeof v === "number") return Number.isFinite(v) ? v : 0;
          const n = Number(String(v).replace(/,/g, "").trim());
          return Number.isFinite(n) ? n : 0;
        };

        return {
          ...row,
          product_name: isTotalRow ? "Total" : productName,
          sku: row.sku ?? "-",

          quantity: toNumber(row.quantity),
          return_quantity: toNumber(row.return_quantity),
          total_quantity: toNumber(row.total_quantity),

          units_sold: toNumber(row.quantity),
          return_units: toNumber(row.return_quantity),
          net_units_sold: toNumber(row.total_quantity),

          asp: toNumber(row.asp ?? row.ASP),
          product_sales: toNumber(row.gross_sales ?? row.product_sales),
          refund_sales: toNumber(row.refund_sales),
          net_sales: toNumber(row.net_sales),
          lost_total: toNumber(row.lost_total),

          cost_of_unit_sold: toNumber(row.cost_of_unit_sold),
          shipment_charges: toNumber(row.shipment_charges),
          selling_fees: toNumber(row.selling_fees),
          fba_fees: toNumber(row.fba_fees),
          amazon_fee: toNumber(row.amazon_fee),

          tex_and_credits: toNumber(row.tex_and_credits),
          net_taxes: toNumber(row.net_taxes),
          net_credits: toNumber(row.net_credits),

          promotional_rebates: toNumber(row.promotional_rebates),
          promotional_rebates_percentage: toNumber(row.promotional_rebates_percentage),

          misc_transaction: toNumber(row.misc_transaction),
          other_transaction_fees: toNumber(row.other_transaction_fees),
          other_transactions: toNumber(row.other_transaction_fees),

          profit: toNumber(row.profit),
          profit_percentage: toNumber(row.profit_percentage),
          unit_wise_profitability: toNumber(row.unit_wise_profitability),

          profit_mix: toNumber(row.profit_mix),
          sales_mix: toNumber(row.sales_mix),
        } as TableRow;
      });
    };

    const fetchSkuRows = async () => {
      try {
        if (!token) {
          setSkuRows([]);
          return;
        }

        const url = buildParentSkuUrl();

        const res = await fetch(url, {
          method: "GET",
          headers: { Authorization: `Bearer ${token}` },
          cache: "no-store",
          signal: ac.signal,
        });

        if (!res.ok) {
          setSkuRows([]);
          return;
        }

        const data = await res.json();

        if (!Array.isArray(data) || data.length === 0) {
          setSkuRows([]);
          return;
        }

        const normalized = normalizeRowsForParent(data);
        setSkuRows(normalized);
      } catch (e: any) {
        if (e?.name === "AbortError") return;
        setSkuRows([]);
      }
    };

    fetchSkuRows();

    return () => ac.abort();
  }, [
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    initialCountryName,
    globalHomeCurrency,
    userid,
    token,
    isDemoMode,
  ]);


  // if (month === "NA" || year === "NA") {
  //   return <IntegrationDashboard />;
  // }




  const hasAnyContent = !!uploadsData?.summary;
  const initialLoading = loading && !hasAnyContent;

  if (initialLoading) {
    return (
      <Loader fullscreen transparent />
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
    const connectCountry = effectiveCountryName === "global" ? "uk" : effectiveCountryName;
    router.push(`/integration-dashboard/${connectCountry}/NA/NA`);
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
            if (tabsDisabled?.[t]) return;

            setActiveTab(t);
            syncTabToHash(t);
          }}
          className="w-full"
          textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
          compact
        />
      </div>
      <PreviewLockedSection
        enabled={isDemoMode}
        title="Preview mode"
        description="You're not seeing your real data yet.Connect your Amazon account now to unlock complete visibility into your business performance."
        buttonText="Connect Amazon"
        onAction={handleConnectAmazonPreview}
      >
        {/* ===================== SUMMARY CARDS (OPTIONAL: ALWAYS SHOW) ===================== */}
        {activeTab !== "cashFlow" && (

          <div className="flex flex-col gap-5 w-full mt-4">
            {/* Summary Cards */}
            {uploadsData?.summary &&
              (() => {
                const summary = displayData;
                const netSales = summary.total_sales;
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

                const renderMoneyWithPerUnit = (total: number, units: number) => {
                  const totalText = formatMoney(total);

                  if (!units) {
                    return <span>{totalText}</span>;
                  }

                  const perUnit = total / units;
                  const perUnitText = formatMoney(perUnit);

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
                        disableInternalFade={isDemoMode}
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
                disableInternalFade={isDemoMode}
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

          {activeTab === "skuwiseProfit" && allDropdownsSelected && (

            <div id="skuwise-profit" className="mt-4 scroll-mt-[80px]">
              {/* <ProductwisePerformance
              embedded
              countryNameProp={initialCountryName}
              rangeProp={range as "monthly" | "quarterly" | "yearly"}
              selectedMonthProp={range === "monthly" ? selectedMonth : ""}
              selectedQuarterProp={range === "quarterly" ? selectedQuarter : ""}
              selectedYearProp={selectedYear ? Number(selectedYear) : ""}
              initialProductName={defaultTopProductName}
            /> */}
              <ProductwisePerformance
                key={[
                  initialCountryName,
                  range,
                  selectedMonth,
                  selectedQuarter,
                  selectedYear,
                  defaultTopProductName,
                  isDemoMode ? "demo" : "live",
                ].join("-")}
                embedded
                countryNameProp={isDemoMode ? "global" : initialCountryName}
                rangeProp={range as "monthly" | "quarterly" | "yearly"}
                selectedMonthProp={isDemoMode ? "NA" : range === "monthly" ? selectedMonth : ""}
                selectedQuarterProp={isDemoMode ? "" : range === "quarterly" ? selectedQuarter : ""}
                selectedYearProp={isDemoMode ? "NA" as any : selectedYear ? Number(selectedYear) : ""}
                initialProductName={defaultTopProductName || "Demo Product A"}
              />
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