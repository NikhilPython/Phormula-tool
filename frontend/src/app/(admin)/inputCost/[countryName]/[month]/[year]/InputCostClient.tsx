'use client';

import React, { use, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import * as XLSX from 'xlsx';
import '@/app/(admin)/pnlforecast/[countryName]/[month]/[year]/Styles.css';
import Modalmsg from '@/components/ui/modal/Modalmsg';
import SkuMultiuseCountryUpload from '@/components/ui/modal/SkuMultiCountryUpload';
import DataTable, { ColumnDef } from '@/components/ui/table/DataTable';
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import SegmentedToggle from '@/components/ui/SegmentedToggle';
import DownloadIconButton from '@/components/ui/button/DownloadButton';
import { useRouter } from 'next/navigation';
import { IoMdLock } from "react-icons/io";
import WarehouseMultiCountryUpload from "@/components/ui/modal/WarehouseMultiCountryUpload";
import Loader from '@/components/loader/Loader';
import { exportSkuInformationExcel, exportWarehouseDataExcel } from '@/lib/excel/exportCurrentInventoryExcel';
import { useGetUserDataQuery } from '@/lib/api/profileApi';
import InventoryInsightsSection from "@/components/common/inventory/InventoryInsightsSection";
import GroupedCollapsibleTable, {
  ColGroup,
  LeafCol,
} from '@/components/ui/table/GroupedCollapsibleTable';

import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";

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
import PeriodFiltersTable from "@/components/filters/PeriodFiltersTable";
import { AnimatePresence, motion } from "framer-motion";
import Productinfoinpopup from "@/components/businessInsight/Productinfoinpopup";


type InventoryActionCardItem = ActionCardItem & {
  delta?: number;
  deltaPercentage?: number | null;

  // ✅ ADD THIS
  avgCoverageRatio?: number;
};
// =========================
// Warehouse upload format
// =========================
const WAREHOUSE_FIXED_COLUMNS = [
  's_no',
  'sku_us',
  'sku_uk',
  'local_stock',
  'in_transit_units',
  'month',
  'year',
];

const normalizeWarehouseHeader = (key: string) => {
  const raw = key.trim().toLowerCase().replace(/\s+/g, '_');

  const map: Record<string, string> = {
    's.no': 's_no',
    's.no.': 's_no',
    's._no': 's_no',
    's._no.': 's_no',
    's_no': 's_no',
    'serial_no': 's_no',
    'serial_number': 's_no',

    'sku_us': 'sku_us',
    'sku_(us)': 'sku_us',
    'sku-us': 'sku_us',
    'sku us': 'sku_us',

    'sku_uk': 'sku_uk',
    'sku_(uk)': 'sku_uk',
    'sku-uk': 'sku_uk',
    'sku uk': 'sku_uk',

    'stock': 'local_stock',
    'local_stock': 'local_stock',
    'local stock': 'local_stock',

    'in_transit': 'in_transit_units',
    'transit_units': 'in_transit_units',
    'in_transit_units': 'in_transit_units',
    'in transit units': 'in_transit_units',

    'month': 'month',
    'year': 'year',
  };

  return map[raw] || raw;
};

const validateWarehouseHeaders = (row: Record<string, any>) => {
  const normalized = Object.keys(row).map(normalizeWarehouseHeader);

  const missing = WAREHOUSE_FIXED_COLUMNS.filter(
    (col) => !normalized.includes(col)
  );

  if (missing.length) {
    return 'Invalid file format. Please upload a file using the provided template.';
  }

  return '';
};

// Types
interface Params {
  params: Promise<{
    countryName: string;
    month: string;
    year: string;
  }>;
}

interface SkuRow {
  s_no: number;
  product_name: string;
  sku_uk?: string;
  sku_us?: string;
  sku_canada?: string;
  asin?: string;
  product_barcode?: string;
  price?: number;
  currency?: string;
  month?: string;
  year?: string | number;
  local_stock?: number;
  in_transit_units?: number;
  [key: string]: any;
}

type TableRow = {
  id: string;
  s_no: React.ReactNode;
  product_name: React.ReactNode;
  sku_uk?: React.ReactNode;
  sku_us?: React.ReactNode;
  sku_canada?: React.ReactNode;
  asin?: React.ReactNode;
  product_barcode?: React.ReactNode;
  month_year?: React.ReactNode;
  price?: React.ReactNode;
  gross_margin_uk?: React.ReactNode;
  gross_margin_us?: React.ReactNode;
  gross_margin_canada?: React.ReactNode;
  gross_margin_eu?: React.ReactNode;
  gross_margin_europe?: React.ReactNode;
  gross_margin_global?: React.ReactNode;
  [key: string]: React.ReactNode;
};

type RangeType = 'monthly' | 'quarterly' | 'yearly';
type Quarter = 'Q1' | 'Q2' | 'Q3' | 'Q4';

type InventoryCurrentRow = Record<string, any>;

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

type OtherSkuItem = {
  product_name: string;
  sku: string;
};

type ProductInsightBlock = {
  name: string;
  skuKey?: string;
  metrics: { label: string; value: string; color?: string }[];

  // ✅ Only drawer metrics. Cards me nahi, drawer me show honge.
  drawerOnlyMetrics?: { label: string; value: string; color?: string }[];

  journeyBullets: string[];
  recommendationBullets: string[];
  inventoryBullets: string[];
  isOtherSkus?: boolean;
  includedSkus?: OtherSkuItem[];
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

const normalizeKey = (s: string) =>
  String(s || "")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[^\w\s-]/g, "");

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

  const out: Record<string, string[]> = {};

  for (const [key, arr] of Object.entries(sections)) {
    out[key] = arr
      .filter((line) => !line.startsWith("##"))
      .map((line) => line.replace(/^[-•]\s+/, "").trim())
      .filter(Boolean);
  }

  return out;
};

const extractRecoAndInventoryBullets = (
  mdOrObj?: string | RecommendationsMap | null
) => {
  if (mdOrObj && typeof mdOrObj === "object") {
    return {
      recommendationBullets: [],
      inventoryBullets: [],
      recommendationsMap: mdOrObj as RecommendationsMap,
    };
  }

  if (!mdOrObj || typeof mdOrObj !== "string") {
    return {
      recommendationBullets: [],
      inventoryBullets: [],
      recommendationsMap: undefined,
    };
  }

  const sections = parseMdSections(mdOrObj);

  return {
    recommendationBullets: sections["ROOT"] ?? [],
    inventoryBullets: sections["INVENTORY"] ?? [],
    recommendationsMap: undefined,
  };
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
    metricLabels.some((m) =>
      s.toLowerCase().startsWith(m.toLowerCase() + ":")
    );

  const blocks: ProductInsightBlock[] = [];
  let current: ProductInsightBlock | null = null;
  let inJourney = false;
  let inIncludedSkus = false;

  const getMetricNumberValue = (value?: string) => {
    const main = String(value || "").split("(")[0] || "";
    const n = Number(main.replace(/[^0-9.-]/g, ""));
    return Number.isFinite(n) ? n : 0;
  };

  const cleanProfitMetricsForDisplay = (
    block: ProductInsightBlock
  ): ProductInsightBlock => {
    const metrics = block.metrics || [];

    const cm2Profit = metrics.find(
      (m) => m.label.trim().toLowerCase() === "cm2 profit"
    );

    const cm2ProfitValue = getMetricNumberValue(cm2Profit?.value);

    // ✅ Monthly me CM2 tabhi show jab CM2 non-zero ho
    // ✅ CM2 0 / missing ho to CM1 show
    // ✅ Quarterly / Yearly me always CM1
    const useCm1 = !isMonthlyRange(range) || !cm2Profit || cm2ProfitValue === 0;

    const cleanedMetrics = metrics.filter((m) => {
      const lower = m.label.trim().toLowerCase();

      if (useCm1 && (lower === "cm2 profit" || lower === "cm2 profit per unit")) {
        return false;
      }

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

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];

    const line = String(raw || "")
      .replace(/^[-•]\s+/, "")
      .replace(/^\d+\.\s*/, "")
      .trim();

    if (!line) continue;

    const nextLine = String(lines[i + 1] || "")
      .replace(/^[-•]\s+/, "")
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
      (isMetric(nextLine) || nextLine.toLowerCase().startsWith("sku:"));

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
        name: isOther ? "Other SKUs" : cleanName || line,
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

    if (lowerLine.startsWith("product journey")) {
      inJourney = true;
      inIncludedSkus = false;
      continue;
    }

    if (lowerLine.startsWith("recommendation:")) {
      inJourney = false;
      const reco = line.replace(/^recommendation:\s*/i, "").trim();
      if (reco) current.recommendationBullets.push(reco);
      continue;
    }

    if (lowerLine.startsWith("ads action:")) {
      inJourney = false;
      const ads = line.replace(/^ads action:\s*/i, "").trim();
      if (ads) current.recommendationBullets.push(ads);
      continue;
    }

    if (lowerLine.startsWith("inventory action:")) {
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
        !Number.isNaN(n)
          ? n < 0
            ? "#DC2626"
            : n > 0
              ? "#059669"
              : "#414042"
          : "#414042";

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

      // ✅ Current Inventory drawer-only, monthly only
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

      // ✅ Ads drawer-only, monthly only
      if (normalizedMetricLabel === "productwise ads spend") {
        if (isMonthlyRange(range)) {
          current.drawerOnlyMetrics = [
            ...(current.drawerOnlyMetrics || []),
            {
              label: "Ads",
              value,
              color: "#414042",
            },
          ];
        }

        continue;
      }

      // ✅ Stock Cover monthly only
      if (!isMonthlyRange(range) && finalLabel.toLowerCase() === "stock cover") {
        continue;
      }

      current.metrics.push({
        label: finalLabel,
        value: finalValue,
        color,
      });

      continue;
    }

    if (inJourney) {
      const cleaned = line.replace(/^-+\s*/, "").trim();
      if (cleaned) current.journeyBullets.push(cleaned);
    }
  }

  pushCurrent();
  return blocks;
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

const toBullets = (text?: string) => {
  if (!text) return [];

  const clean = String(text).trim();

  if (!clean) return [];

  if (clean.includes("\n")) {
    return clean
      .split("\n")
      .map((x) => x.replace(/^[-•]\s+/, "").trim())
      .filter(Boolean);
  }

  return clean
    .split(/(?:\.\s+|;\s+|\s\|\s)/g)
    .map((x) => x.trim())
    .filter(Boolean);
};

const splitMetricValue = (value: string) => {
  const v = String(value || "").trim();
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

const isMonthlyRange = (range?: RangeType) => range === "monthly";

const formatUnitsNoDecimal = (value: any) => {
  return Math.round(Number(value ?? 0)).toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
};

const formatRecommendationCardMainValue = (label: string, main: string) => {
  const normalizedLabel = label.trim().toLowerCase();

  if (normalizedLabel !== "net sales" && normalizedLabel !== "cm1 profit") {
    return main;
  }

  const currencyMatch = main.match(/^([^0-9-]*)/);
  const currency = currencyMatch?.[1] ?? "";

  const numberPart = main.replace(/[^0-9.-]/g, "");
  const numberValue = Number(numberPart);

  if (!Number.isFinite(numberValue)) return main;

  return `${currency}${Math.round(numberValue).toLocaleString()}`;
};

const formatMoney = (value: any, symbol = "$") => {
  const n = Number(value ?? 0);

  return `${symbol}${n.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
};

const formatMoneyNoDecimal = (value: any, symbol = "$") => {
  const n = Math.round(Number(value ?? 0));

  return `${symbol}${n.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  })}`;
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
    return "Current Month Units Sold";
  }

  return `Current Month Units Sold (${monthLabel})`;
};

const formatBestPerformancePeriod = (
  month?: string,
  year?: string | number
) => {
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

const getPeriodBadge = (
  range: RangeType,
  year: string,
  month?: string,
  quarter?: string
) => {
  const yy = String(year || "").slice(-2);

  if (range === "monthly" && month) {
    return `${month.slice(0, 3)}'${yy}`;
  }

  if (range === "quarterly" && quarter) {
    return `${quarter}'${yy}`;
  }

  if (range === "yearly" && year) {
    return String(year);
  }

  return "";
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

    const qMatch = p.match(/^(Q[1-4])\s+(\d{4})$/i);
    if (qMatch) {
      return `${qMatch[1].toUpperCase()}’${qMatch[2].slice(-2)}`;
    }

    const monthYearMatch = p.match(/^([A-Za-z]+)\s+(\d{4})$/);
    if (monthYearMatch) {
      return `${monthYearMatch[1].slice(0, 3)}’${monthYearMatch[2].slice(-2)}`;
    }

    return p;
  };

  return `(${formatPart(leftRaw)} vs ${formatPart(rightRaw)})`;
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
  "ads",
  "cm2 profit",
  "cm2 profit per unit",
  "cm1 profit",
  "cm1 profit per unit",
  "current inventory",
  "stock cover",
  "productwise ads spend",
];

type InventoryCurrentApiResponse = {
  success: boolean;
  rows?: InventoryCurrentRow[];
  columns?: string[];
  table_name?: string;
  total_rows?: number;
  combined_countries?: string[];
  country_results?: Record<string, InventoryCurrentApiResponse>;
  categories?: Record<
    string,
    {
      items?: any[];
      product_count?: number;
      sku_count?: number;

      value?: number;
      total?: number;
      total_value?: number;
      estimated_storage_cost?: number;
      storage_cost?: number;
      next_month_storage_cost?: number;
      previous_storage_cost?: number;
    }
  >;

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

  month?: string;
  year?: number;
  requested_month?: string;
  requested_year?: number;
  country_key?: string;
  inventory_age_summary?: {
    total?: number;
    percentage_base_total?: number;
    sellable_total?: number;
    unfulfillable_total?: number;
    current_month_units_sold_total?: number;
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

type InventoryAgeSummaryApiResponse = {
  success: boolean;
  month?: string;
  year?: number;
  country_key?: string;
  combined_countries?: string[];
  country_results?: Record<string, InventoryAgeSummaryApiResponse>;
  totals?: Record<string, number>;
  age_summary?: {
    month: string;
    month_number?: number;
    year: number;
    age_bucket: string;
    column: string;
    units: number;
  }[];
  month_summary?: {
    month: string;
    month_number: number;
    year: number;
    source?: string;
    totals: Record<string, number>;
  }[];
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
  actions: InventoryActionCardItem[];
  actionLogic: ActionLogicItem[];
  inventoryAgeSummary?: InventoryCurrentApiResponse["inventory_age_summary"];
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
  currencySymbol?: string;
  bestPerformanceLoading?: boolean;
  bestPerformanceError?: string | null;
  bestPerformanceData?: ProductBestPerformanceData | null;
  sharedInsightData?: {
    blocks: ProductInsightBlock[];
    objective?: ObjectivePayload | null;
    recommendationsMap?: RecommendationsMap;
    drawerPeriodText?: string;
    nameToSkuMap?: Record<string, string>;
  };
};

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
  bestPerformanceLoading = false,
  bestPerformanceError = null,
  bestPerformanceData = null,
  sharedInsightData,
}) => {
  if (!open || !block) return null;

  const inventoryText =
    recObj?.inventory_recommendation || block?.inventoryBullets?.join(" ");

  const inventoryRecoBullets = toBullets(inventoryText);
  const adsRecoBullets = toBullets(recObj?.ads_recommendation);

  const periodBadge = getPeriodBadge(range, year, month, quarter);

  const drawerCurrencySymbol =
    currencySymbol || getCurrencySymbol(getCurrencyForCountry(countryName));

  const isOtherSkusBlock = !!block?.isOtherSkus;

  const sortedMetrics = [
    ...(block?.metrics || []),
    ...(block?.drawerOnlyMetrics || []),
  ]
    .filter((m) => {
      const lower = m.label.trim().toLowerCase();

      // ✅ Monthly me show, Quarterly/Yearly me hide
      if (!isMonthlyRange(range)) {
        return ![
          "ads",
          "productwise ads spend",
          "stock cover",
          "current inventory",
        ].includes(lower);
      }

      return true;
    })
    // ✅ duplicate metrics remove: Stock Cover do baar nahi aayega
    .filter((metric, index, arr) => {
      const key = metric.label.trim().toLowerCase();

      return (
        arr.findIndex(
          (item) => item.label.trim().toLowerCase() === key
        ) === index
      );
    })
    .sort((a, b) => {
      const aIndex = metricOrder.indexOf(a.label.toLowerCase());
      const bIndex = metricOrder.indexOf(b.label.toLowerCase());

      const safeAIndex = aIndex === -1 ? Number.MAX_SAFE_INTEGER : aIndex;
      const safeBIndex = bIndex === -1 ? Number.MAX_SAFE_INTEGER : bIndex;

      return safeAIndex - safeBIndex;
    });

  const getMetricBorderColorByLabel = (label: string, fallbackIndex = 0) => {
    const normalizedLabel = label.trim().toLowerCase();
    const metricIndex = metricOrder.indexOf(normalizedLabel);

    return metricColors[
      metricIndex !== -1 ? metricIndex : fallbackIndex % metricColors.length
    ];
  };

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
            className="fixed right-0 top-0 z-[1000000] h-screen w-[95vw] sm:w-[75vw] md:w-[60vw] lg:w-[50vw] min-[1700px]:w-[50vw] bg-white shadow-2xl"
            initial={{ x: 520 }}
            animate={{ x: 0 }}
            exit={{ x: 520 }}
            transition={{ type: "tween", duration: 0.25 }}
          >
            <div className="flex flex-col gap-4 h-full">
              {/* Header */}
              <div className="shrink-0 border-b border-slate-200 p-3 flex items-center justify-between gap-3">
                <div className="min-w-0">
                  <div className="flex items-center gap-1 flex-wrap">
                    <PageBreadcrumb
                      pageTitle="Detailed View - "
                      variant="page"
                      textSize="2xl"
                    />

                    <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
                      {block.name || "Details"}
                    </span>

                    {drawerPeriodText ? (
                      <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
                        {drawerPeriodText}
                      </span>
                    ) : periodBadge ? (
                      <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
                        ( {periodBadge} )
                      </span>
                    ) : null}
                  </div>
                </div>

                <button
                  onClick={onClose}
                  className="rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-charcoal-500 hover:bg-slate-50"
                >
                  ✕
                </button>
              </div>

              <div className="flex-1 overflow-y-auto space-y-6 px-3">
                {/* Metrics */}
                <div>
                  <PageBreadcrumb
                    pageTitle="Metrics"
                    variant="page"
                    align="left"
                    textSize="xl"
                    className="mb-2"
                  />

                  <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-5">
                    {sortedMetrics.map((m, i) => {
                      const { main, delta, deltaColor } = splitMetricValue(m.value);

                      const displayMain = formatRecommendationCardMainValue(
                        m.label,
                        main
                      );

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
                                  deltaColor,
                                ].join(" ")}
                              >
                                {formatMetricDelta(delta)}
                              </span>
                            ) : null}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>

                {/* Overall Best Performance */}
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
                      <div className="grid grid-cols-1 gap-3 sm:grid-cols-3 xl:grid-cols-5">
                        {[
                          {
                            label: "Units",
                            value: formatUnitsNoDecimal(
                              bestPerformanceData?.units?.units
                            ),
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
                              bestPerformanceData?.unit_wise_profitability
                                ?.unit_wise_profitability,
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

                {/* Recommendations */}
                <div>
                  <div className="mb-2 text-xs font-semibold text-charcoal-500 sm:text-sm 2xl:text-lg">
                    Recommendations
                  </div>

                  {block.recommendationBullets?.length ? (
                    <div>
                      <div className="text-xs 2xl:text-sm font-semibold text-charcoal-500">
                        Action
                      </div>

                      <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
                        {block.recommendationBullets.map((pt, i) => (
                          <li key={i}>{pt}</li>
                        ))}
                      </ul>
                    </div>
                  ) : (
                    <div className="text-xs 2xl:text-sm text-charcoal-500">—</div>
                  )}

                  {adsRecoBullets.length ? (
                    <div className="mt-2">
                      <div className="text-xs 2xl:text-sm font-semibold text-charcoal-500">
                        Advertising
                      </div>

                      <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
                        {adsRecoBullets.map((pt, i) => (
                          <li key={i}>{pt}</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}

                  {inventoryRecoBullets.length ? (
                    <div className="mt-2">
                      <div className="text-xs 2xl:text-sm font-semibold text-charcoal-500">
                        Inventory
                      </div>

                      <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
                        {inventoryRecoBullets.map((pt, i) => (
                          <li key={i}>{pt.replace(/^•\s*/, "")}</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}
                </div>

                {/* Chart / Product Info */}
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
                        ? (block.includedSkus || []).map(
                          (item) => item.product_name
                        )
                        : []
                    }
                  />
                </div>

                {/* Product Journey */}
                <div className="pb-4">
                  <div className="flex items-center gap-1 flex-wrap">
                    <PageBreadcrumb
                      pageTitle="Product Journey"
                      variant="page"
                      textSize="lg"
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
                    <div className="text-xs 2xl:text-sm text-charcoal-500">
                      —
                    </div>
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

const allMonths = [
  'january',
  'february',
  'march',
  'april',
  'may',
  'june',
  'july',
  'august',
  'september',
  'october',
  'november',
  'december',
];

const getPreviousCompletedPeriod = () => {
  const now = new Date();
  const previousMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);

  return {
    month: allMonths[previousMonthDate.getMonth()],
    year: String(previousMonthDate.getFullYear()),
    monthIndex: previousMonthDate.getMonth(),
  };
};

const getInventoryAgeSummaryMonthsForTrend = (
  rangeValue: RangeType,
  monthValue: string,
  quarterValue: Quarter | "",
  yearValue: string
) => {
  const selectedYearNum = Number(yearValue);

  if (!selectedYearNum || Number.isNaN(selectedYearNum)) {
    return [];
  }

  const previousCompleted = getPreviousCompletedPeriod();
  const previousCompletedYear = Number(previousCompleted.year);

  // ✅ Future year: no historic trend
  if (selectedYearNum > previousCompletedYear) {
    return [];
  }

  // ✅ Past year: full year trend
  if (selectedYearNum < previousCompletedYear) {
    return ["december"];
  }

  // ✅ Current year:
  // Ignore selected month/quarter.
  // Always show Jan -> current month - 1.
  return [previousCompleted.month];
};

const isSameOrBeforePreviousCompletedMonth = (
  monthNumber: number,
  yearValue: string | number
) => {
  const previousCompleted = getPreviousCompletedPeriod();

  const yearNumber = Number(yearValue);
  const previousCompletedYear = Number(previousCompleted.year);
  const previousCompletedMonthNumber = previousCompleted.monthIndex + 1;

  if (!Number.isFinite(yearNumber) || !Number.isFinite(monthNumber)) {
    return false;
  }

  if (yearNumber < previousCompletedYear) return true;
  if (yearNumber > previousCompletedYear) return false;

  return monthNumber <= previousCompletedMonthNumber;
};

const isCurrentOrFutureMonth = (monthName: string, yearValue: string) => {
  const monthIndex = allMonths.indexOf(String(monthName || '').toLowerCase());
  const yearNumber = Number(yearValue);

  if (monthIndex < 0 || !Number.isFinite(yearNumber)) return false;

  const now = new Date();
  const currentYear = now.getFullYear();
  const currentMonthIndex = now.getMonth();

  return (
    yearNumber > currentYear ||
    (yearNumber === currentYear && monthIndex >= currentMonthIndex)
  );
};

const getSafeInventoryMonth = (monthName: string, yearValue: string) => {
  const previousCompleted = getPreviousCompletedPeriod();

  if (!monthName || !allMonths.includes(String(monthName).toLowerCase())) {
    return previousCompleted.month;
  }

  if (isCurrentOrFutureMonth(monthName, yearValue)) {
    return previousCompleted.month;
  }

  return String(monthName).toLowerCase();
};

const getSafeInventoryYear = (yearValue: string) => {
  const parsed = Number(yearValue);
  const previousCompleted = getPreviousCompletedPeriod();

  if (!Number.isFinite(parsed) || parsed > new Date().getFullYear()) {
    return previousCompleted.year;
  }

  return String(yearValue);
};

const quarterToMonths: Record<Quarter, string[]> = {
  Q1: ['january', 'february', 'march'],
  Q2: ['april', 'may', 'june'],
  Q3: ['july', 'august', 'september'],
  Q4: ['october', 'november', 'december'],
};

const SPLIT_FIRST_180_INVENTORY_BUCKETS: AgeingBucket[] = [
  { key: 'zeroToNinety', label: '0–90 Days', color: '#7B9A6D' },
  { key: 'ninetyOneToOneEighty', label: '91–180 Days', color: '#FDD36F' },
  { key: 'oneEightyOneToTwoSeventy', label: '181–270 Days', color: '#ED9F50' },
  { key: 'twoSeventyOneToThreeSixtyFive', label: '271–365 Days', color: '#C49466' },
  { key: 'threeSixtyFivePlus', label: '365+ Days', color: '#B75A5A' },
];

const COMBINED_FIRST_180_INVENTORY_BUCKETS: AgeingBucket[] = [
  { key: 'zeroToOneEighty', label: '0–180 Days', color: '#7B9A6D' },
  { key: 'oneEightyOneToTwoSeventy', label: '181–270 Days', color: '#ED9F50' },
  { key: 'twoSeventyOneToThreeSixtyFive', label: '271–365 Days', color: '#C49466' },
  { key: 'threeSixtyFivePlus', label: '365+ Days', color: '#B75A5A' },
];

// Keep this for demo/default fallback.
const INVENTORY_BUCKETS: AgeingBucket[] = COMBINED_FIRST_180_INVENTORY_BUCKETS;

const getDynamicInventoryBuckets = (
  rows: InventoryCurrentRow[]
): AgeingBucket[] => {
  const splitFirst180Total = rows.reduce((sum, row) => {
    return (
      sum +
      getInventoryAgeValue(row, 'inv-age-0-to-90-days') +
      getInventoryAgeValue(row, 'inv-age-91-to-180-days')
    );
  }, 0);

  const combinedFirst180Total = rows.reduce((sum, row) => {
    return sum + getInventoryAgeValue(row, 'inv-age-0-to-180-days');
  }, 0);

  if (splitFirst180Total > 0) {
    return SPLIT_FIRST_180_INVENTORY_BUCKETS;
  }

  if (combinedFirst180Total > 0) {
    return COMBINED_FIRST_180_INVENTORY_BUCKETS;
  }

  return COMBINED_FIRST_180_INVENTORY_BUCKETS;
};

const AGEING_TREND_BUCKET_OPTIONS = [
  {
    label: '0–180 Days',
    value: '0-180 days',
    column: 'inv-age-0-to-180-days',
    color: '#7B9A6D',
  },
  {
    label: '181–270 Days',
    value: '181-270 days',
    column: 'inv-age-181-to-270-days',
    color: '#ED9F50',
  },
  {
    label: '271–365 Days',
    value: '271-365 days',
    column: 'inv-age-271-to-365-days',
    color: '#C49466',
  },
  {
    label: '365+ Days',
    value: '365+ days',
    column: 'inv-age-365-plus-days',
    color: '#B75A5A',
  },
];

const INVENTORY_ACTION_LOGIC: ActionLogicItem[] = [
  {
    key: 'healthy',
    label: 'Healthy',
    description: 'Stock covers 0–180 days',
    color: '#7B9A6D',
  },
  {
    key: 'high_alert',
    label: 'High Alert',
    description: 'Shipment Required',
    color: '#B75A5A',
  },
  // {
  //   key: 'discount',
  //   label: 'Discount',
  //   description: 'Stock aged 0–180 days',
  //   color: '#FDD36F',
  // },
  {
    key: 'liquidate',
    label: 'Liquidate',
    description: 'Stock older than 180 days',
    color: '#ED9F50',
  },
  {
    key: 'unfulfillable',
    label: 'Unfulfillable',
    description: 'Remove or dispose stock',
    color: '#3A8EA4',
  },
  {
    key: 'estimated_storage_cost',
    label: 'Estimate Storage',
    description: 'Monthly storage estimate',
    color: '#C49466',
  },
];

const toNum = (v: any) => {
  if (v === null || v === undefined) return 0;
  if (typeof v === 'number') return Number.isFinite(v) ? v : 0;
  const n = Number(String(v).replace(/,/g, '').trim());
  return Number.isFinite(n) ? n : 0;
};

const normalizeInventoryKey = (key: string) =>
  String(key || '')
    .toLowerCase()
    .trim()
    .replace(/[%()]/g, '')
    .replace(/[-\s]+/g, '_')
    .replace(/__+/g, '_');

const pickInventoryNumber = (
  row: InventoryCurrentRow,
  keys: string[]
): number => {
  if (!row) return 0;

  for (const key of keys) {
    const directValue = row?.[key];

    if (directValue !== null && directValue !== undefined && directValue !== '') {
      const value = toNum(directValue);
      if (value !== 0) return value;
    }
  }

  const normalizedTargetKeys = keys.map(normalizeInventoryKey);

  for (const [rowKey, rowValue] of Object.entries(row)) {
    const normalizedRowKey = normalizeInventoryKey(rowKey);

    if (normalizedTargetKeys.includes(normalizedRowKey)) {
      const value = toNum(rowValue);
      if (value !== 0) return value;
    }
  }

  return 0;
};

const getInventoryRowProductName = (row: InventoryCurrentRow) => {
  const possibleKeys = [
    'product_name',
    'Product Name',
    'product name',
    'productName',
    'product_name_x',
    'product_name_y',
    'parent_product_name',
    'item_name',
    'item-name',
    'itemName',
    'title',
    'product',
    'Product',
    'asin_title',
    'item-title',
    'item_title',
    'name',
  ];

  for (const key of possibleKeys) {
    const value = row?.[key];

    if (
      value !== null &&
      value !== undefined &&
      String(value).trim() !== '' &&
      String(value).trim().toLowerCase() !== 'nan' &&
      String(value).trim().toLowerCase() !== 'none' &&
      String(value).trim().toLowerCase() !== 'null'
    ) {
      return String(value).trim();
    }
  }

  return getInventoryRowSku(row) || 'Unknown Product';
};

const getInventoryRowSku = (row: InventoryCurrentRow) =>
  String(row?.sku || row?.SKU || row?.seller_sku || row?.fnsku || '').trim();

const isInventoryPercentageRow = (row: InventoryCurrentRow) => {
  const productName = getInventoryRowProductName(row).trim().toLowerCase();
  const sku = getInventoryRowSku(row).trim().toLowerCase();
  const rowType = String(row?.row_type || "").trim().toLowerCase();

  return (
    row?.is_percentage_row === true ||
    rowType === "percentage" ||
    productName === "percentage" ||
    productName === "% of total" ||
    sku === "percentage" ||
    sku === "% of total"
  );
};

const isInventoryTotalRow = (row: InventoryCurrentRow) => {
  const productName = getInventoryRowProductName(row).trim().toLowerCase();
  const sku = getInventoryRowSku(row).trim().toLowerCase();
  const rowType = String(row?.row_type || "").trim().toLowerCase();

  return (
    row?.is_total_row === true ||
    row?.is_total === true ||
    row?.__isTotal === true ||
    rowType === "total" ||
    productName === "total" ||
    productName === "grand total" ||
    sku === "total" ||
    sku === "grand total"
  );
};

const getInventoryAgeValue = (row: InventoryCurrentRow, key: string) =>
  toNum(row?.[key]);

const getInventoryRowAvailableUnits = (row: InventoryCurrentRow) => {
  return pickInventoryNumber(row, [
    "available",
    "available_quantity",
    "fulfillable_quantity",
    "afn_fulfillable_quantity",
    "afn-fulfillable-quantity",
  ]);
};

const getInventoryRowFcTransferUnits = (row: InventoryCurrentRow) => {
  return pickInventoryNumber(row, [
    "fc-transfer",
    "fc_transfer",
    "fcTransfer",
    "reserved_fc_transfer",
    "reserved-fc-transfer",
  ]);
};

const getInventoryRowTotalUnits = (row: InventoryCurrentRow) => {
  const available = getInventoryRowAvailableUnits(row);
  const fcTransfer = getInventoryRowFcTransferUnits(row);

  return (
    pickInventoryNumber(row, [
      "Sellable Units",
      "sellable_units",
      "sellableUnits",
      "sellable_sum_last",
      "totalUnits",
      "total_units",
      "total_quantity",
    ]) || available + fcTransfer
  );
};

const getInventoryRowUnfulfillableUnits = (row: InventoryCurrentRow) => {
  return pickInventoryNumber(row, [
    "unfulfillable-quantity",
    "Unfulfillable Units",
    "unfulfillableUnits",
    "unfulfillable_units",
    "unfulfillable_quantity",
    "unfulfillable units",
    "unsellableUnits",
    "unsellable_units",
    "unsellable-quantity",
    "unsellable_quantity",
    "afn_unsellable_quantity",
    "afn-unsellable-quantity",
  ]);
};

const getInventoryRowInboundUnits = (row: InventoryCurrentRow) => {
  return pickInventoryNumber(row, [
    "Inbound Units",
    "inbound_units",
    "inboundUnits",
    "transit_total",
    "inbound_quantity",
    "inboundQuantity",
    "Inbound Quantity",
    "inbound units",
    "inbound-quantity",
    "inbound-units",
    "afn_inbound_quantity",
    "afn-inbound-quantity",
  ]);
};

const getInventoryRowCoverageRatio = (row: InventoryCurrentRow) => {
  return pickInventoryNumber(row, [
    'coverageRatio',
    'coverage_ratio',
    'coverage-ratio',
    'coverage_ratio_in_months',
    'coverage-ratio-in-months',
    'coverage_ratio_months',
    'coverage-ratio-months',
    'Coverage Ratio (in Months)',
    'Coverage Ratio (In Months)',
    'Coverage Ratio In Months',
    'coverage ratio in months',
    'coverage ratio',
    'months_of_cover',
    'months-of-cover',
    'month_cover',
    'month-cover',
    'stock_cover_months',
    'stock-cover-months',
    'cover_months',
    'cover-months',
  ]);
};

const getInventoryCurrentFbaValue = (row: InventoryCurrentRow) =>
  pickInventoryNumber(row, [
    "available",
    "Current Inventory FBA",
    "current_inventory_fba",
    "current-fba",
    "fulfillable_quantity",
    "afn_fulfillable_quantity",
    "afn-fulfillable-quantity",
  ]);

const getInventoryCurrentAwdValue = (row: InventoryCurrentRow) =>
  pickInventoryNumber(row, [
    "total_onhand_quantity",
    "Current Inventory AWD",
    "current_inventory_awd",
    "current-awd",
    "available_awd",
    "awd_available",
  ]);

const getInventoryTransitFbaValue = (row: InventoryCurrentRow) =>
  pickInventoryNumber(row, [
    "inbound-shipped\r",
    "inbound-shipped",
    "In Transit FBA",
    "in_transit_fba",
    "in-transit-fba",
    "fc-transfer",
    "fc_transfer",
    "fcTransfer",
    "reserved_fc_transfer",
    "reserved-fc-transfer",
    "inbound-working",
  ]);

const getInventoryTransitAwdValue = (row: InventoryCurrentRow) =>
  pickInventoryNumber(row, [
    "total_inbound_quantity",
    "In Transit AWD",
    "in_transit_awd",
    "in-transit-awd",
    "inbound_quantity",
    "inbound-quantity",
    "Inbound Units",
    "inbound_units",
    "inboundUnits",
    "inbound-shipped",
  ]);

const getInventoryUnsellableFbaValue = (row: InventoryCurrentRow) =>
  pickInventoryNumber(row, [
    "Unsellable Inventory FBA",
    "Unsellable FBA",
    "unfulfillable-quantity",
    "unfulfillable_quantity",
    "unfulfillableUnits",
    "unfulfillable_units",
    "Unfulfillable Units",
  ]);

const getInventoryUnsellableAwdValue = (row: InventoryCurrentRow) =>
  pickInventoryNumber(row, [
    "Unsellable Inventory AWD",
    "Unsellable AWD",
    "unsellable_awd",
    "unfulfillable_awd",
  ]);

const getInventoryStorageCostValue = (row: InventoryCurrentRow) =>
  pickInventoryNumber(row, [
    "Storage Cost (Est) - in USD",
    "Storage Cost (Est) in USD",
    "estimated-storage-cost-next-month",
    "estimated_storage_cost_next_month",
    "estimatedStorageCostNextMonth",
    "Estimated Storage Cost",
    "estimated_storage_cost",
    "storage_cost_est",
    "storage_cost",
  ]);

const getInventoryCoverageCurrentAndTransitValue = (row: InventoryCurrentRow) =>
  pickInventoryNumber(row, [
    "Coverage Ratio (Current + In Transit)",
    "Coverage Ratio (Current + In transit)",
    "Coverage Ratio (Current + Intransit)",
    "Coverage Ratio (Current + Inventory)",
    "coverage_ratio_current_in_transit",
    "coverage_ratio_current_intransit",
    "coverage_ratio_current_plus_in_transit",
  ]);

const getInventoryRowSalesLast30Days = (row: InventoryCurrentRow) => {
  return pickInventoryNumber(row, [
    'Sales Last 30 Days',
    'sales_last_30_days',
    'sales-last-30-days',
    'salesLast30Days',
    'last_30_days_sales',
    'last-30-days-sales',
    'Last 30 Days Sales',
  ]);
};

const getInventoryRowSalesRank = (row: InventoryCurrentRow) => {
  const directValue =
    row?.["sales-rank"] ??
    row?.sales_rank ??
    row?.salesRank ??
    row?.["Sales Rank"] ??
    row?.["sales rank"] ??
    row?.rank ??
    "";

  if (
    directValue === null ||
    directValue === undefined ||
    String(directValue).trim() === "" ||
    String(directValue).trim().toLowerCase() === "nan"
  ) {
    return "";
  }

  return directValue;
};

const getInventoryRowPreviousSalesRank = (
  row: InventoryCurrentRow,
  selectedMonth?: string
) => {
  const keys = Object.keys(row || {});

  const selectedMonthFull = String(selectedMonth || "").trim().toLowerCase();
  const selectedMonthShort = selectedMonthFull.slice(0, 3);

  // Example:
  // Previous Month Sales Rank (June)
  // Previous Month Sales Rank (Jun)
  const selectedMonthRankKey = keys.find((key) => {
    const lowerKey = key.toLowerCase();

    return (
      lowerKey.startsWith("previous month sales rank") &&
      (
        lowerKey.includes(selectedMonthFull) ||
        lowerKey.includes(selectedMonthShort)
      )
    );
  });

  if (selectedMonthRankKey) {
    return row?.[selectedMonthRankKey];
  }

  // Fallback: any backend key starting with Previous Month Sales Rank
  const anyPreviousRankKey = keys.find((key) =>
    key.toLowerCase().startsWith("previous month sales rank")
  );

  if (anyPreviousRankKey) {
    return row?.[anyPreviousRankKey];
  }

  return (
    row?.previous_sales_rank ??
    row?.previousSalesRank ??
    row?.["Previous Month Sales Rank"] ??
    row?.["previous sales rank"] ??
    ""
  );
};

const getInventoryRowEstimatedStorageCost = (row: InventoryCurrentRow) => {
  return pickInventoryNumber(row, [
    // actual backend key used by dropdown/dashboard
    'estimated-storage-cost-next-month',
    'estimated_storage_cost_next_month',
    'estimatedStorageCostNextMonth',
    'Estimated Storage Cost Next Month',
    'estimated storage cost next month',

    // existing fallback keys
    'estimatedStorageCost',
    'estimated_storage_cost',
    'estimated-storage-cost',
    'Estimated Storage Cost',
    'estimated storage cost',
    'estimate_storage',
    'estimated_storage',
    'monthly_storage_cost',
    'monthly_storage_fee',
    'storage_cost',
    'storage_fee',
  ]);
};

const getShortMonthLabel = (monthName?: string) => {
  const clean = String(monthName || '').trim();
  return clean ? clean.slice(0, 3) : '-';
};

const getUniqueInventorySkuCount = (rows: any[]) => {
  const set = new Set<string>();

  rows.forEach((row) => {
    const sku = getInventoryRowSku(row);
    const productName = getInventoryRowProductName(row);

    const key = sku || productName;

    if (key && key.toLowerCase() !== 'total') {
      set.add(key.toLowerCase());
    }
  });

  return set.size;
};

const getRowAgeingTotalUnits = (row: any) => {
  const sellableUnits = getInventoryRowTotalUnits(row);

  if (sellableUnits > 0) return sellableUnits;

  return (
    toNum(
      row?.zeroToOneEighty ??
      row?.['inv-age-0-to-180-days'] ??
      (
        toNum(row?.zeroToNinety ?? row?.['inv-age-0-to-90-days']) +
        toNum(row?.ninetyOneToOneEighty ?? row?.['inv-age-91-to-180-days'])
      )
    ) +
    toNum(row?.oneEightyOneToTwoSeventy ?? row?.['inv-age-181-to-270-days']) +
    toNum(row?.twoSeventyOneToThreeSixtyFive ?? row?.['inv-age-271-to-365-days']) +
    toNum(row?.threeSixtyFivePlus ?? row?.['inv-age-365-plus-days'])
  );
};

const hasHighAlertInventory = (row: any) => {
  return String(
    row?.inventoryAlert ??
    row?.["Inventory Alerts"] ??
    row?.inventory_alerts ??
    row?.alert ??
    ""
  )
    .trim()
    .toLowerCase() === "high alert";
};

const formatInventoryStorageCost = (
  value: number,
  countryName: string
) => {
  const symbol = getCurrencySymbol(getCurrencyForCountry(countryName));

  return `${symbol}${toNum(value).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
};

const getEstimatedStorageCostTotal = (
  response?: InventoryCurrentApiResponse
) => {
  const estimatedStorageCategory =
    response?.categories?.estimated_storage_cost as any;

  const categoryValue = toNum(
    estimatedStorageCategory?.value ??
    estimatedStorageCategory?.total ??
    estimatedStorageCategory?.total_value ??
    estimatedStorageCategory?.estimated_storage_cost ??
    estimatedStorageCategory?.storage_cost ??
    estimatedStorageCategory?.next_month_storage_cost
  );

  if (categoryValue > 0) return categoryValue;

  const storageItems = estimatedStorageCategory?.items ?? [];

  const totalRow = storageItems.find((item: any) => {
    const productName = String(
      item?.product_name ??
      item?.["Product Name"] ??
      ""
    )
      .trim()
      .toLowerCase();

    const sku = String(item?.sku ?? item?.SKU ?? "")
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
    allMonths.indexOf(normalizedMonth) >= 0
      ? allMonths.indexOf(normalizedMonth)
      : Number.isInteger(numericMonth) &&
        numericMonth >= 1 &&
        numericMonth <= 12
        ? numericMonth - 1
        : -1;

  if (monthIndex < 0) return [];

  const fullMonth = allMonths[monthIndex];

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
  inventoryResponses: InventoryCurrentApiResponse[],
  ageSummaryResponses: InventoryAgeSummaryApiResponse[],
  selectedTrendBucket: string,
  countryName: string,
  selectedGlobalInventoryCountry: string = "uk",
  selectedRange: RangeType = "monthly",
  selectedMonthForRank: string = "",
  selectedQuarterForTrend: Quarter | "" = ""
): InventoryInsightsData => {
  const isGlobalInventory =
    String(countryName || "").toLowerCase() === "global";

  const selectedInventoryCountry =
    isGlobalInventory ? selectedGlobalInventoryCountry : countryName;

  const selectedInventoryResponses = isGlobalInventory
    ? inventoryResponses
      .map((res) =>
        getSelectedCountryInventoryResponse(res, selectedGlobalInventoryCountry)
      )
      .filter(Boolean) as InventoryCurrentApiResponse[]
    : inventoryResponses;

  const selectedAgeSummaryResponses = isGlobalInventory
    ? getSelectedCountryAgeSummaryResponses(
      ageSummaryResponses,
      selectedGlobalInventoryCountry
    )
    : ageSummaryResponses;

  const backendPercentageRawRow = selectedInventoryResponses
    .flatMap((res) => (Array.isArray(res?.rows) ? res.rows : []))
    .find((row) => isInventoryPercentageRow(row));

  const backendTotalRawRow = selectedInventoryResponses
    .flatMap((res) => (Array.isArray(res?.rows) ? res.rows : []))
    .find((row) => isInventoryTotalRow(row));

  const rows = selectedInventoryResponses.flatMap((res) => {
    const categoryRows = res?.categories
      ? Object.values(res.categories).flatMap((category) =>
        Array.isArray(category?.items) ? category.items : []
      )
      : [];

    const directRows = Array.isArray(res?.rows) ? res.rows : [];

    // IMPORTANT:
    // directRows me coverage ratio aa sakta hai
    // categoryRows me estimated-storage-cost-next-month aa sakta hai
    // isliye dono ko combine karna hai, either/or nahi.
    const merged = [...directRows, ...categoryRows];

    const unique = new Map<string, InventoryCurrentRow>();

    merged.forEach((row) => {
      const sku = getInventoryRowSku(row);
      const productName = getInventoryRowProductName(row);

      const key = `${sku || productName}`.trim().toLowerCase();

      if (
        !key ||
        key === "total" ||
        key === "grand total" ||
        key === "percentage" ||
        key === "% of total" ||
        isInventoryPercentageRow(row)
      ) {
        return;
      }

      const previous = unique.get(key) || {};

      // Merge fields without losing earlier values.
      // Agar same SKU directRows me coverage ratio hai aur categoryRows me storage cost,
      // dono same final row me aa jayenge.
      const next: InventoryCurrentRow = { ...previous };

      Object.entries(row).forEach(([fieldKey, fieldValue]) => {
        const isEmpty =
          fieldValue === null ||
          fieldValue === undefined ||
          fieldValue === '' ||
          String(fieldValue).trim().toLowerCase() === 'nan' ||
          String(fieldValue).trim().toLowerCase() === 'none' ||
          String(fieldValue).trim().toLowerCase() === 'null';

        if (!isEmpty) {
          next[fieldKey] = fieldValue;
        }
      });

      unique.set(key, next);
    });

    return Array.from(unique.values());
  });

  const productRows = rows.filter((row) => {
    const productName = getInventoryRowProductName(row).trim().toLowerCase();
    const sku = getInventoryRowSku(row).trim().toLowerCase();

    return (
      productName !== "total" &&
      sku !== "total" &&
      productName !== "grand total" &&
      sku !== "grand total" &&
      !isInventoryPercentageRow(row)
    );
  });

  const latestInventoryResponse = selectedInventoryResponses.find((res) => res?.success);

  const dynamicHeatmapBuckets = getDynamicInventoryBuckets(productRows);

  const isUsingSplitFirst180 = dynamicHeatmapBuckets.some(
    (bucket) => bucket.key === 'zeroToNinety'
  );

  const currentMonthUnitsSoldKey = getCurrentMonthUnitsSoldKeyForResponse(
    latestInventoryResponse,
    productRows?.[0],
    selectedMonthForRank
  );

  const heatmapData: AgeingRiskHeatmapRow[] = productRows
    .map((row) => {
      const zeroToNinety = getInventoryAgeValue(row, 'inv-age-0-to-90-days');
      const ninetyOneToOneEighty = getInventoryAgeValue(row, 'inv-age-91-to-180-days');
      const zeroToOneEighty = getInventoryAgeValue(row, 'inv-age-0-to-180-days');

      const oneEightyOneToTwoSeventy = getInventoryAgeValue(row, 'inv-age-181-to-270-days');
      const twoSeventyOneToThreeSixtyFive = getInventoryAgeValue(row, 'inv-age-271-to-365-days');
      const threeSixtyFivePlus = getInventoryAgeValue(row, 'inv-age-365-plus-days');

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

      const sellableUnits = getInventoryRowTotalUnits(row);

      const inboundUnits = getInventoryRowInboundUnits(row);

      const totalUnits = sellableUnits || bucketTotal;

      // ✅ Needed for Others coverage ratio
      const salesLast30Days = getInventoryRowSalesLast30Days(row);
      const unitsSold =
        getCurrentMonthUnitsSoldValue(row, currentMonthUnitsSoldKey) ??
        salesLast30Days;
      const currentFba = getInventoryCurrentFbaValue(row);
      const currentAwd = getInventoryCurrentAwdValue(row);
      const transitFba = getInventoryTransitFbaValue(row);
      const transitAwd = getInventoryTransitAwdValue(row);
      const totalInStock =
        pickInventoryNumber(row, [
          "total_stock",
          "Total Sellable Inventory In Stock",
          "Total Sellable In Stock",
        ]) ||
        currentFba + currentAwd;
      const totalInTransit =
        pickInventoryNumber(row, [
          "total_transit",
          "Total Sellable Inventory In Transit",
          "Total Sellable In Transit",
        ]) ||
        transitFba + transitAwd;
      const coverageCurrentAndTransit =
        getInventoryCoverageCurrentAndTransitValue(row) ||
        (unitsSold > 0 ? (totalInStock + totalInTransit) / unitsSold : 0);

      return {
        productName: getInventoryRowProductName(row),
        sku: getInventoryRowSku(row),

        inventoryAlert: String(
          row?.['Inventory Alerts'] ??
          row?.inventory_alerts ??
          row?.alert ??
          ''
        ).trim(),

        // Latest periods use these two columns
        zeroToNinety,
        ninetyOneToOneEighty,

        // Older / yearly periods use this column
        zeroToOneEighty,

        oneEightyOneToTwoSeventy,
        twoSeventyOneToThreeSixtyFive,
        threeSixtyFivePlus,

        available,      // backend available
        fcTransfer,     // backend fc-transfer
        inboundUnits,
        totalUnits,     // backend Sellable Units

        unsellableUnits: getInventoryRowUnfulfillableUnits(row),
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
        unitsSold,
        salesLast30Days,
        salesRank: getInventoryRowSalesRank(row),
        previousSalesRank:
          selectedRange === 'monthly'
            ? getInventoryRowPreviousSalesRank(row, selectedMonthForRank)
            : '',

        coverageRatio: getInventoryRowCoverageRatio(row),
        estimatedStorageCost: getInventoryRowEstimatedStorageCost(row),
      };
    })
    .filter(
      (row) =>
        toNum(row.available) > 0 ||
        toNum((row as any).inboundUnits) !== 0 ||
        toNum(row.totalUnits) > 0 ||
        toNum((row as any).unsellableUnits) > 0 ||
        toNum((row as any).coverageRatio) > 0 ||
        toNum((row as any).estimatedStorageCost) > 0 ||
        toNum((row as any).unitsSold) > 0 ||
        toNum((row as any).salesLast30Days) > 0 ||
        String((row as any).salesRank || '').trim() !== ''
    );

  const backendTotalHeatmapRow: AgeingRiskHeatmapRow | null = backendTotalRawRow
    ? {
      productName: "Total",
      sku: "-",

      zeroToNinety: toNum(backendTotalRawRow?.["inv-age-0-to-90-days"]),
      ninetyOneToOneEighty: toNum(backendTotalRawRow?.["inv-age-91-to-180-days"]),
      zeroToOneEighty: toNum(backendTotalRawRow?.["inv-age-0-to-180-days"]),

      oneEightyOneToTwoSeventy: toNum(
        backendTotalRawRow?.["inv-age-181-to-270-days"]
      ),
      twoSeventyOneToThreeSixtyFive: toNum(
        backendTotalRawRow?.["inv-age-271-to-365-days"]
      ),
      threeSixtyFivePlus: toNum(
        backendTotalRawRow?.["inv-age-365-plus-days"]
      ),

      available: getInventoryRowAvailableUnits(backendTotalRawRow),
      fcTransfer: getInventoryRowFcTransferUnits(backendTotalRawRow),
      totalUnits: getInventoryRowTotalUnits(backendTotalRawRow),
      inboundUnits: getInventoryRowInboundUnits(backendTotalRawRow),
      unsellableUnits: getInventoryRowUnfulfillableUnits(backendTotalRawRow),
      currentFba: getInventoryCurrentFbaValue(backendTotalRawRow),
      currentAwd: getInventoryCurrentAwdValue(backendTotalRawRow),
      transitFba: getInventoryTransitFbaValue(backendTotalRawRow),
      transitAwd: getInventoryTransitAwdValue(backendTotalRawRow),
      totalInStock:
        pickInventoryNumber(backendTotalRawRow, [
          "total_stock",
          "Total Sellable Inventory In Stock",
          "Total Sellable In Stock",
        ]) ||
        getInventoryCurrentFbaValue(backendTotalRawRow) +
        getInventoryCurrentAwdValue(backendTotalRawRow),
      totalInTransit:
        pickInventoryNumber(backendTotalRawRow, [
          "total_transit",
          "Total Sellable Inventory In Transit",
          "Total Sellable In Transit",
        ]) ||
        getInventoryTransitFbaValue(backendTotalRawRow) +
        getInventoryTransitAwdValue(backendTotalRawRow),
      unsellableFba: getInventoryUnsellableFbaValue(backendTotalRawRow),
      unsellableAwd: getInventoryUnsellableAwdValue(backendTotalRawRow),
      storageCostUsd: getInventoryStorageCostValue(backendTotalRawRow),

      unitsSold:
        getCurrentMonthUnitsSoldValue(
          backendTotalRawRow,
          currentMonthUnitsSoldKey
        ) ?? getInventoryRowSalesLast30Days(backendTotalRawRow),

      // ✅ backend total Sales Last 30 Days
      salesLast30Days: getInventoryRowSalesLast30Days(backendTotalRawRow),

      // ✅ backend total Coverage Ratio only
      coverageRatio: getInventoryRowCoverageRatio(backendTotalRawRow),
      coverageCurrentAndTransit:
        getInventoryCoverageCurrentAndTransitValue(backendTotalRawRow) ||
        (() => {
          const unitsSold =
            getCurrentMonthUnitsSoldValue(
              backendTotalRawRow,
              currentMonthUnitsSoldKey
            ) ?? getInventoryRowSalesLast30Days(backendTotalRawRow);
          const totalInStock =
            pickInventoryNumber(backendTotalRawRow, [
              "total_stock",
              "Total Sellable Inventory In Stock",
              "Total Sellable In Stock",
            ]) ||
            getInventoryCurrentFbaValue(backendTotalRawRow) +
            getInventoryCurrentAwdValue(backendTotalRawRow);
          const totalInTransit =
            pickInventoryNumber(backendTotalRawRow, [
              "total_transit",
              "Total Sellable Inventory In Transit",
              "Total Sellable In Transit",
            ]) ||
            getInventoryTransitFbaValue(backendTotalRawRow) +
            getInventoryTransitAwdValue(backendTotalRawRow);

          return unitsSold > 0
            ? (totalInStock + totalInTransit) / unitsSold
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

  const overallAgeing = heatmapData.reduce(
    (acc, row) => {
      acc.zeroToNinety += toNum((row as any).zeroToNinety);
      acc.ninetyOneToOneEighty += toNum((row as any).ninetyOneToOneEighty);
      acc.zeroToOneEighty += toNum((row as any).zeroToOneEighty);

      acc.oneEightyOneToTwoSeventy += toNum(row.oneEightyOneToTwoSeventy);
      acc.twoSeventyOneToThreeSixtyFive += toNum(row.twoSeventyOneToThreeSixtyFive);
      acc.threeSixtyFivePlus += toNum(row.threeSixtyFivePlus);

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

  const selectedInventoryResponseForDonut =
    selectedInventoryResponses.find((res) => res?.success) ||
    selectedInventoryResponses[0];

  const backendSummaryDonutData = buildDonutDataFromInventoryAgeSummary(
    selectedInventoryResponseForDonut?.inventory_age_summary
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
    selectedInventoryResponseForDonut?.inventory_age_summary?.sellable_total ??
    donutData.reduce((sum, item) => sum + toNum(item.units), 0);

  const selectedTrendOption =
    AGEING_TREND_BUCKET_OPTIONS.find(
      (option) => option.value === selectedTrendBucket
    ) || AGEING_TREND_BUCKET_OPTIONS[2];

  const monthSummaryMap = new Map<
    string,
    {
      month: string;
      month_number: number;
      year: number;
      totals: Record<string, number>;
    }
  >();

  selectedAgeSummaryResponses.forEach((res) => {
    if (!res?.success) return;

    /**
     * IMPORTANT:
     * inventory_current_age_summary can return full year month_summary.
     * InputCost calls this API for multiple months, so the same Jan-May
     * data can come back 12 times.
     *
     * Do NOT add duplicate month_summary values.
     * Keep one row per year-month, same as Dropdown page behavior.
     */
    if (Array.isArray(res.month_summary) && res.month_summary.length > 0) {
      res.month_summary.forEach((item) => {
        const monthNumber =
          Number(item.month_number) ||
          allMonths.indexOf(String(item.month || '').toLowerCase()) + 1;

        if (!monthNumber || !item.year) return;

        const key = `${item.year}-${monthNumber}`;

        monthSummaryMap.set(key, {
          month: item.month,
          month_number: monthNumber,
          year: Number(item.year),
          totals: item.totals || {},
        });
      });

      return;
    }

    /**
     * Fallback only if month_summary is not present.
     * Here we aggregate age_summary rows within that one response.
     */
    if (Array.isArray(res.age_summary)) {
      res.age_summary.forEach((item) => {
        const monthNumber =
          Number(item.month_number) ||
          allMonths.indexOf(String(item.month || '').toLowerCase()) + 1;

        if (!monthNumber || !item.year || !item.column) return;

        const key = `${item.year}-${monthNumber}`;
        const previous = monthSummaryMap.get(key);

        monthSummaryMap.set(key, {
          month: item.month,
          month_number: monthNumber,
          year: Number(item.year),
          totals: {
            ...(previous?.totals || {}),
            [item.column]:
              toNum(previous?.totals?.[item.column]) + toNum(item.units),
          },
        });
      });
    }
  });

  const maxTrendMonthNumber = (() => {
    const previousCompleted = getPreviousCompletedPeriod();

    const selectedYearFromSummary =
      selectedAgeSummaryResponses
        ?.flatMap((res) => res.month_summary || [])
        ?.find((item) => item?.year)?.year;

    const trendYear = Number(selectedYearFromSummary || previousCompleted.year);
    const previousCompletedYear = Number(previousCompleted.year);

    // ✅ Past year: show full year
    if (trendYear < previousCompletedYear) {
      return 12;
    }

    // ✅ Future year: show nothing
    if (trendYear > previousCompletedYear) {
      return 0;
    }

    // ✅ Current year: show till current month - 1
    return previousCompleted.monthIndex + 1;
  })();

  const sortedMonthSummaryValues = Array.from(monthSummaryMap.values())
    .filter((item) => Number(item.month_number) <= maxTrendMonthNumber)
    .sort((a, b) => a.year - b.year || a.month_number - b.month_number);

  const trendData: AgeingTrendItem[] = sortedMonthSummaryValues.map((item) => ({
    label: getShortMonthLabel(item.month),
    value: toNum(item.totals?.[selectedTrendOption.column]),
  }));

  const trendAllSeriesData: AgeingTrendAllSeriesItem[] =
    AGEING_TREND_BUCKET_OPTIONS.map((bucket) => ({
      bucketValue: bucket.value,
      bucketLabel: bucket.label,
      color: bucket.color,
      data: sortedMonthSummaryValues.map((item) => ({
        label: getShortMonthLabel(item.month),
        value: toNum(item.totals?.[bucket.column]),
      })),
    }));

  const selectedInventoryResponse =
    latestInventoryResponse || selectedInventoryResponses[0];

  const estimatedStorageCategory =
    selectedInventoryResponse?.categories?.estimated_storage_cost as any;

  const totalEstimatedStorageCost =
    getEstimatedStorageCostTotal(selectedInventoryResponse) ||
    heatmapData.reduce(
      (sum, row) => sum + toNum((row as any).estimatedStorageCost),
      0
    );

  const previousStorageCostTotal = toNum(
    estimatedStorageCategory?.previous_storage_cost
  );

  const storageCostDelta =
    previousStorageCostTotal > 0
      ? totalEstimatedStorageCost - previousStorageCostTotal
      : 0;

  const storageCostDeltaPercentage =
    previousStorageCostTotal > 0
      ? (storageCostDelta / Math.abs(previousStorageCostTotal)) * 100
      : null;

  const healthyRows = heatmapData.filter(
    (row) =>
      toNum((row as any).zeroToOneEighty) > 0 ||
      toNum((row as any).zeroToNinety) > 0 ||
      toNum((row as any).ninetyOneToOneEighty) > 0
  );

  const highAlertRows = heatmapData.filter((row) =>
    hasHighAlertInventory(row)
  );

  const highAlertAvgCoverageRatio =
    typeof selectedInventoryResponse?.high_alert_coverage_summary?.average_coverage_ratio === "number"
      ? selectedInventoryResponse.high_alert_coverage_summary.average_coverage_ratio
      : (() => {
        const validCoverageRows = highAlertRows
          .map((row) => toNum((row as any).coverageRatio))
          .filter((value) => value > 0);

        if (!validCoverageRows.length) return 0;

        return (
          validCoverageRows.reduce((sum, value) => sum + value, 0) /
          validCoverageRows.length
        );
      })();

  const highAlertSkuCount =
    selectedInventoryResponse?.high_alert_coverage_summary?.high_alert_sku_count ??
    getUniqueInventorySkuCount(highAlertRows);

  const discountRows = heatmapData.filter(
    (row) => toNum((row as any).zeroToOneEighty) > 0
  );

  const liquidateRows = heatmapData.filter(
    (row) =>
      toNum(row.oneEightyOneToTwoSeventy) > 0 ||
      toNum(row.twoSeventyOneToThreeSixtyFive) > 0 ||
      toNum(row.threeSixtyFivePlus) > 0
  );

  const unfulfillableRows = heatmapData.filter(
    (row) => toNum((row as any).unsellableUnits) > 0
  );

  const storageCostRows = heatmapData.filter(
    (row) => toNum((row as any).estimatedStorageCost) > 0
  );

  const actions: InventoryActionCardItem[] = [
    ...(isUsingSplitFirst180
      ? [
        {
          key: 'age_0_90',
          label: 'Healthy',
          count: getUniqueInventorySkuCount(
            heatmapData.filter((row) => toNum((row as any).zeroToNinety) > 0)
          ),
          displayValue: getUniqueInventorySkuCount(
            heatmapData.filter((row) => toNum((row as any).zeroToNinety) > 0)
          ),
          skuCount: getUniqueInventorySkuCount(
            heatmapData.filter((row) => toNum((row as any).zeroToNinety) > 0)
          ),
          unitCount: heatmapData.reduce(
            (sum, row) => sum + toNum((row as any).zeroToNinety),
            0
          ),
          description: 'Stock aged 0–90 days',
          color: '#7B9A6D',
          backgroundColor: '#ffffff',
        },

        // ✅ High Alert comes before 91–180 Days
        {
          key: 'high_alert',
          label: 'High Alert',
          count: highAlertSkuCount,
          displayValue: highAlertSkuCount,
          skuCount: highAlertSkuCount,
          avgCoverageRatio: highAlertAvgCoverageRatio,
          description: 'Shipment Required',
          color: '#B75A5A',
          backgroundColor: '#ffffff',
        },

        {
          key: 'age_91_180',
          label: 'Discount',
          count: getUniqueInventorySkuCount(
            heatmapData.filter((row) => toNum((row as any).ninetyOneToOneEighty) > 0)
          ),
          displayValue: getUniqueInventorySkuCount(
            heatmapData.filter((row) => toNum((row as any).ninetyOneToOneEighty) > 0)
          ),
          skuCount: getUniqueInventorySkuCount(
            heatmapData.filter((row) => toNum((row as any).ninetyOneToOneEighty) > 0)
          ),
          unitCount: heatmapData.reduce(
            (sum, row) => sum + toNum((row as any).ninetyOneToOneEighty),
            0
          ),
          description: 'Stock aged 91–180 days',
          color: '#FDD36F',
          backgroundColor: '#ffffff',
        },
      ]
      : [
        {
          key: 'healthy',
          label: 'Healthy',
          count: getUniqueInventorySkuCount(healthyRows),
          displayValue: getUniqueInventorySkuCount(healthyRows),
          skuCount: getUniqueInventorySkuCount(healthyRows),
          unitCount: healthyRows.reduce(
            (sum, row) => sum + toNum((row as any).zeroToOneEighty),
            0
          ),
          description: 'Stock covers 0–180 days',
          color: '#7B9A6D',
          backgroundColor: '#ffffff',
        },

        // ✅ High Alert comes after Healthy
        {
          key: 'high_alert',
          label: 'High Alert',
          count: highAlertSkuCount,
          displayValue: highAlertSkuCount,
          skuCount: highAlertSkuCount,
          avgCoverageRatio: highAlertAvgCoverageRatio,
          description: 'Shipment Required',
          color: '#B75A5A',
          backgroundColor: '#ffffff',
        },
      ]),

    {
      key: 'liquidate',
      label: 'Liquidate',
      count: getUniqueInventorySkuCount(liquidateRows),
      displayValue: getUniqueInventorySkuCount(liquidateRows),
      skuCount: getUniqueInventorySkuCount(liquidateRows),
      unitCount: liquidateRows.reduce(
        (sum, row) =>
          sum +
          toNum(row.oneEightyOneToTwoSeventy) +
          toNum(row.twoSeventyOneToThreeSixtyFive) +
          toNum(row.threeSixtyFivePlus),
        0
      ),
      description: 'Stock older than 180 days',
      color: '#ED9F50',
      backgroundColor: '#ffffff',
    },

    {
      key: 'unfulfillable',
      label: 'Unfulfillable',
      count: getUniqueInventorySkuCount(unfulfillableRows),
      displayValue: getUniqueInventorySkuCount(unfulfillableRows),
      skuCount: getUniqueInventorySkuCount(unfulfillableRows),
      unitCount: unfulfillableRows.reduce(
        (sum, row) => sum + toNum((row as any).unsellableUnits),
        0
      ),
      description: 'Remove or dispose stock',
      color: '#3A8EA4',
      backgroundColor: '#ffffff',
    },

    {
      key: 'estimated_storage_cost',
      label: 'Estimate Storage',
      count: totalEstimatedStorageCost,
      displayValue: formatInventoryStorageCost(
        totalEstimatedStorageCost,
        selectedInventoryCountry
      ),
      skuCount: getUniqueInventorySkuCount(storageCostRows),
      unitCount: storageCostRows.reduce(
        (sum, row) => sum + getRowAgeingTotalUnits(row),
        0
      ),
      delta: storageCostDelta,
      deltaPercentage: storageCostDeltaPercentage,
      description: 'Monthly storage estimate',
      color: '#C49466',
      backgroundColor: '#ffffff',
    },
  ];

  return {
    heatmapBuckets: dynamicHeatmapBuckets,

    // ✅ use backend percentage row only for display
    heatmapData: finalHeatmapData,

    donutSku: '',
    donutData,
    donutTotalUnits,
    trendSelectedBucket: selectedTrendOption.value,
    trendData,
    trendLineColor: selectedTrendOption.color,
    trendAllSeriesData,
    trendBucketOptions: AGEING_TREND_BUCKET_OPTIONS.map((bucket) => ({
      label: bucket.label,
      value: bucket.value,
      color: bucket.color,
    })),
    actions,
    actionLogic: INVENTORY_ACTION_LOGIC,

    // ✅ pass backend summary to AgeingRiskHeatmap for total row
    inventoryAgeSummary: selectedInventoryResponseForDonut?.inventory_age_summary,
  };
};

const getCurrencySymbol = (country: string | undefined): string => {
  switch (country) {
    case 'GBP':
      return '£';
    case 'INR':
      return '₹';
    case 'USD':
      return '$';
    case 'europe':
    case 'eu':
    case 'EUR':
      return '€';
    case 'CAD':
      return '$';
    case 'global':
      return '$';
    default:
      return '$';
  }
};

function getCurrencyForCountry(country: string): string {
  switch (country.toLowerCase()) {
    case 'uk':
      return 'GBP';
    case 'us':
      return 'USD';
    case 'canada':
      return 'CAD';
    case 'eu':
    case 'europe':
      return 'EUR';
    default:
      return 'USD';
  }
}

const DUMMY_SKU_DATA: SkuRow[] = [
  {
    s_no: 1,
    product_name: 'Sample Product A',
    sku_uk: 'UK-SKU-001',
    // sku_us: 'US-SKU-001',
    // sku_canada: 'CA-SKU-001',
    asin: 'B0DUMMY001',
    product_barcode: '1234567890123',
    price: 0,
    currency: 'GBP',
    month: 'January',
    year: '2026',
    landing_cost: 0,
  },
  {
    s_no: 2,
    product_name: 'Sample Product B',
    sku_uk: 'UK-SKU-002',
    // sku_us: 'US-SKU-002',
    // sku_canada: 'CA-SKU-002',
    asin: 'B0DUMMY002',
    product_barcode: '2234567890123',
    price: 0,
    currency: 'GBP',
    month: 'January',
    year: '2026',
    landing_cost: 0,
  },
  {
    s_no: 3,
    product_name: 'Sample Product C',
    sku_uk: 'UK-SKU-003',
    // sku_us: 'US-SKU-003',
    // sku_canada: 'CA-SKU-003',
    asin: 'B0DUMMY003',
    product_barcode: '3234567890123',
    price: 0,
    currency: 'GBP',
    month: 'January',
    year: '2026',
    landing_cost: 0,
  },
  {
    s_no: 4,
    product_name: 'Sample Product D',
    sku_uk: 'UK-SKU-004',
    // sku_us: 'US-SKU-004',
    // sku_canada: 'CA-SKU-004',
    asin: 'B0DUMMY004',
    product_barcode: '4234567890123',
    price: 0,
    currency: 'GBP',
    month: 'January',
    year: '2026',
    landing_cost: 0,
  },
  {
    s_no: 5,
    product_name: 'Sample Product E',
    sku_uk: 'UK-SKU-005',
    // sku_us: 'US-SKU-005',
    // sku_canada: 'CA-SKU-005',
    asin: 'B0DUMMY005',
    product_barcode: '5234567890123',
    price: 0,
    currency: 'GBP',
    month: 'January',
    year: '2026',
    landing_cost: 0,
  },
];

const DUMMY_ASP_DATA: Record<string, number> = {
  'Sample Product A': 0,
  'Sample Product B': 0,
  'Sample Product C': 0,
  'Sample Product A_uk': 0,
  'Sample Product B_uk': 0,
  'Sample Product C_uk': 0,
  'Sample Product A_us': 0,
  'Sample Product B_us': 0,
  'Sample Product C_us': 0,
  'Sample Product A_canada': 0,
  'Sample Product B_canada': 0,
  'Sample Product C_canada': 0,
};

const DUMMY_CURRENCY_RATES: Record<string, number> = {
  GBP: 1,
  USD: 1,
  CAD: 1,
  EUR: 1,
  INR: 1,
  gbp: 1,
  usd: 1,
  cad: 1,
  eur: 1,
  inr: 1,
  GBP_uk: 1,
  USD_us: 1,
  CAD_canada: 1,
  EUR_europe: 1,
  EUR_eu: 1,
  USD_global: 1,
};

const DUMMY_WAREHOUSE_DATA = [
  {
    s_no: 1,
    // sku_us: 'US-SKU-001',
    sku_uk: 'UK-SKU-001',
    local_stock: 0,
    in_transit_units: 0,
    month: 'January',
    year: '2026',
  },
  {
    s_no: 2,
    // sku_us: 'US-SKU-002',
    sku_uk: 'UK-SKU-002',
    local_stock: 0,
    in_transit_units: 0,
    month: 'January',
    year: '2026',
  },
  {
    s_no: 3,
    // sku_us: 'US-SKU-002',
    sku_uk: 'UK-SKU-003',
    local_stock: 0,
    in_transit_units: 0,
    month: 'January',
    year: '2026',
  },
  {
    s_no: 4,
    // sku_us: 'US-SKU-002',
    sku_uk: 'UK-SKU-004',
    local_stock: 0,
    in_transit_units: 0,
    month: 'January',
    year: '2026',
  },
  {
    s_no: 5,
    // sku_us: 'US-SKU-002',
    sku_uk: 'UK-SKU-005',
    local_stock: 0,
    in_transit_units: 0,
    month: 'January',
    year: '2026',
  },
];

type AnyRow = Record<string, any>;

type LedgerDBReadParams =
  | { range: 'monthly'; month: string; year: string; country?: string; marketplaceId?: string | null }
  | { range: 'quarterly'; quarter: string; year: string; country?: string; marketplaceId?: string | null }
  | { range: 'yearly'; year: string; country?: string; marketplaceId?: string | null };

const MARKETPLACE_ID_BY_COUNTRY: Record<string, string> = {
  uk: "A1F83G8C2ARO7P",
  gb: "A1F83G8C2ARO7P",
  us: "ATVPDKIKX0DER",
  usa: "ATVPDKIKX0DER",
  ca: "A2EUQ1WTGCTBG2",
  canada: "A2EUQ1WTGCTBG2",
};

const getMarketplaceIdForCountry = (country?: string) => {
  const normalizedCountry = String(country || "").trim().toLowerCase();

  return MARKETPLACE_ID_BY_COUNTRY[normalizedCountry] || null;
};

const monthNameToNumber = (m: string) => {
  const idx = allMonths.indexOf((m || '').toLowerCase());
  return idx === -1 ? null : idx + 1;
};

const quarterToNumber = (q: string) => {
  const v = (q || '').toUpperCase().trim();
  const n = Number(v.replace('Q', ''));
  return [1, 2, 3, 4].includes(n) ? n : null;
};



const buildQuery = (obj: Record<string, string | number | undefined | null>) => {
  const sp = new URLSearchParams();

  Object.entries(obj).forEach(([k, v]) => {
    if (v === undefined || v === null || v === '') return;
    sp.set(k, String(v));
  });

  return sp.toString();
};

const isNumericLike = (v: any) => {
  if (v === null || v === undefined) return false;
  if (typeof v === 'number') return Number.isFinite(v);
  if (typeof v === 'string' && v.trim() !== '') return !Number.isNaN(Number(v));
  return false;
};

const toInventoryInt = (v: any) => {
  if (v === null || v === undefined || v === '') return 0;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : 0;
};



const formatReconCell = (v: any) => {
  if (v === null || v === undefined || v === "") return "-";
  if (typeof v === "boolean") return v ? "Yes" : "No";

  if (isNumericLike(v)) {
    const n = Math.abs(Math.trunc(Number(v)));
    if (n === 0) return "-";
    return n.toLocaleString();
  }

  return String(v);
};

const isTotalRow = (row: AnyRow) => {
  const msku = String(row?.msku || "").trim().toUpperCase();
  const pn = String(row?.product_name || "").trim().toUpperCase();

  return (
    msku === "TOTAL" ||
    pn === "TOTAL" ||
    msku === "GRAND TOTAL" ||
    pn === "GRAND TOTAL" ||
    row?.is_total === true ||
    row?.__isTotal === true
  );
};

const sumRowForKeys = (rowsToSum: AnyRow[], keys: string[], base: AnyRow = {}) => {
  const out: AnyRow = { ...base };

  keys.forEach((k) => {
    out[k] = rowsToSum.reduce((acc, r) => acc + toInventoryInt(r?.[k]), 0);
  });

  return out;
};

const DUMMY_RECON_ROWS: AnyRow[] = [
  {
    id: "dummy-1",
    product_name: "Sample Product A",
    msku: "SKU-001",
    sellable_sum_first: 0,
    expired_sum_first: 0,
    beginning_total: 0,
    sum_receipts: 0,
    transit_total: 0,
    sum_disposed: 0,
    sum_damaged: 0,
    sum_lost: 0,
    sum_found: 0,
    sold_total: 0,
    ending_total: 0,
    difference_total: 0,
    sellable_sum_last: 0,
    expired_sum_last: 0,
    inventory_coverage_ratio: 0,
  },
  {
    id: "__TOTAL__",
    product_name: "GRAND TOTAL",
    msku: "GRAND TOTAL",
    __isTotal: true,
    sellable_sum_first: 0,
    expired_sum_first: 0,
    beginning_total: 0,
    sum_receipts: 0,
    transit_total: 0,
    sum_disposed: 0,
    sum_damaged: 0,
    sum_lost: 0,
    sum_found: 0,
    sold_total: 0,
    ending_total: 0,
    difference_total: 0,
    sellable_sum_last: 0,
    expired_sum_last: 0,
    inventory_coverage_ratio: 0,
  },
];

const DUMMY_LOST_COMP_ROWS: AnyRow[] = [
  {
    id: "dummy-lc-1",
    product_name: "Sample Product A",
    msku: "SKU-001",
    lost_units: 0,
    damaged_units: 0,
    total_lost_units: 0,
    compensation_units: 0,
    compensation_value: 0,
    settlement_loss_event_amount: 0,
    net_value: 0,
    net_units: 0,
  },
  {
    id: "dummy-lc-total",
    product_name: "Total",
    msku: "-",
    __isTotal: true,
    lost_units: 0,
    damaged_units: 0,
    total_lost_units: 0,
    compensation_units: 0,
    compensation_value: 0,
    settlement_loss_event_amount: 0,
    net_value: 0,
    net_units: 0,
  },
];



export default function InputCostPage({ params }: Params) {
  const { countryName: countryNameRaw, month: monthRaw, year: yearRaw } = use(params);
  const countryName = decodeURIComponent(countryNameRaw ?? '').toLowerCase();
  const monthParam = decodeURIComponent(monthRaw ?? '');
  const yearParam = decodeURIComponent(yearRaw ?? '');

  const [skuData, setSkuData] = useState<SkuRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isEditing, setIsEditing] = useState(false);
  const [editedPrices, setEditedPrices] = useState<Record<string, number>>({});
  const [showModal, setShowModal] = useState(false);
  const [modalMessage, setModalMessage] = useState('');
  const [visibleColumns, setVisibleColumns] = useState<string[]>([]);
  const [currencyRates, setCurrencyRates] = useState<Record<string, number>>({});
  const [aspData, setAspData] = useState<Record<string, number>>({});
  const [showMultiuseCountry, setShowMultiuseCountry] = useState(false);
  const [warehouseData, setWarehouseData] = useState<Record<string, any>[]>([]);
  const [warehouseColumns, setWarehouseColumns] = useState<string[]>([]);
  const [warehouseLoading, setWarehouseLoading] = useState(false);
  const [showWarehouseUpload, setShowWarehouseUpload] = useState(false);
  const [selectedWarehouseFile, setSelectedWarehouseFile] = useState<File | null>(null);
  const [aiPanel, setAiPanel] = useState<AiPanelData | null>(null);
  const [aiPanelLoading, setAiPanelLoading] = useState(false);
  const [aiPanelError, setAiPanelError] = useState<string | null>(null);

  const aiRequestIdRef = useRef(0);

  const [selectedAiProductBlock, setSelectedAiProductBlock] =
    useState<ProductInsightBlock | null>(null);

  const [selectedAiProductRecObj, setSelectedAiProductRecObj] =
    useState<any>(null);

  const [aiBestPerformanceLoading, setAiBestPerformanceLoading] =
    useState(false);

  const [aiBestPerformanceError, setAiBestPerformanceError] =
    useState<string | null>(null);

  const [aiBestPerformanceData, setAiBestPerformanceData] =
    useState<ProductBestPerformanceData | null>(null);

  const { data: userData } = useGetUserDataQuery();
  const router = useRouter();

  const companyName =
    (userData as any)?.companyName ||
    (userData as any)?.company_name ||
    (userData as any)?.company ||
    "";

  const brandName =
    (userData as any)?.brandName ||
    (userData as any)?.brand_name ||
    (userData as any)?.brand ||
    "";


  const isNA =
    monthParam?.toLowerCase() === 'na' ||
    yearParam?.toLowerCase() === 'na';

  type InputCostTab =
    | 'inventory-insights'
    | 'sku-info'
    | 'recon-table'
    | 'lost-compensation'
    | 'extra';
  const [activeTab, setActiveTab] = useState<InputCostTab>('inventory-insights');
  const pageTopRef = useRef<HTMLDivElement | null>(null);
  const tabTopRef = useRef<HTMLDivElement | null>(null);
  const shouldScrollTabTopRef = useRef(false);

  const INPUT_COST_TAB_HASHES: InputCostTab[] = [
    'inventory-insights',
    'sku-info',
    'extra',
    'recon-table',
    'lost-compensation',
  ];

  const scrollInputCostPageToTop = useCallback(() => {
    if (typeof window === "undefined") return;

    const target = tabTopRef.current || pageTopRef.current;

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

    window.scrollTo({
      top: 0,
      left: 0,
      behavior: "auto",
    });

    document.documentElement.scrollTop = 0;
    document.body.scrollTop = 0;

    scrollParents.forEach((el) => {
      el.scrollTop = 0;
    });
  }, []);

  useEffect(() => {
    if (!shouldScrollTabTopRef.current) return;

    shouldScrollTabTopRef.current = false;

    const scrollNow = () => {
      scrollInputCostPageToTop();
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
  }, [activeTab, scrollInputCostPageToTop]);

  const syncTabFromHash = useCallback(() => {
    if (typeof window === "undefined") return;

    const hash = window.location.hash.replace("#", "") as InputCostTab;

    if (INPUT_COST_TAB_HASHES.includes(hash)) {
      shouldScrollTabTopRef.current = true;
      setActiveTab(hash);
    }
  }, []);

  useEffect(() => {
    syncTabFromHash();

    const handleHashChange = () => {
      syncTabFromHash();
    };

    const handleSidebarHashNavigate = (event: Event) => {
      const customEvent = event as CustomEvent<{ hash?: string }>;
      const hash = customEvent.detail?.hash as InputCostTab | undefined;

      if (hash && INPUT_COST_TAB_HASHES.includes(hash)) {
        shouldScrollTabTopRef.current = true;
        setActiveTab(hash);
      } else {
        syncTabFromHash();
      }
    };

    window.addEventListener("hashchange", handleHashChange);
    window.addEventListener("page-hash-navigate", handleSidebarHashNavigate);

    return () => {
      window.removeEventListener("hashchange", handleHashChange);
      window.removeEventListener("page-hash-navigate", handleSidebarHashNavigate);
    };
  }, [syncTabFromHash]);

  const getDefaultMonth = () => {
    const clean = String(monthParam || '').toLowerCase();
    const safeYear = getDefaultYear();

    return getSafeInventoryMonth(clean, safeYear);
  };

  const getDefaultYear = () => {
    const parsed = Number(yearParam);
    const previousCompleted = getPreviousCompletedPeriod();

    if (!Number.isFinite(parsed) || parsed <= 2000) {
      return previousCompleted.year;
    }

    if (parsed > new Date().getFullYear()) {
      return previousCompleted.year;
    }

    return String(parsed);
  };

  const [range, setRange] = useState<RangeType>('monthly');
  const [selectedMonth, setSelectedMonth] = useState<string>(getDefaultMonth());
  const [selectedQuarter, setSelectedQuarter] = useState<Quarter | ''>('');
  const [selectedYear, setSelectedYear] = useState<string>(getDefaultYear());
  const [selectedAgeingTrendBucket, setSelectedAgeingTrendBucket] =
    useState<string>('365+ days');
  const salesLast30DaysLabel = buildCurrentMonthUnitsSoldLabel(
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear
  );
  const inventoryHeatmapUnitSalesDataKey: AgeingRiskUnitSalesDataKey = "unitsSold";
  const [selectedGlobalInventoryCountry, setSelectedGlobalInventoryCountry] =
    useState<"uk" | "us">("uk");
  const inventoryInsightsReportCountry =
    countryName === "global" ? selectedGlobalInventoryCountry : countryName;
  const showUsCurrentInventoryTable =
    ["us", "usa", "united states"].includes(
      String(inventoryInsightsReportCountry || "").trim().toLowerCase()
    );


  const [inventoryInsightsData, setInventoryInsightsData] =
    useState<InventoryInsightsData | null>(null);

  const [inventoryInsightsLoading, setInventoryInsightsLoading] = useState(true);

  const [inventoryInsightsError, setInventoryInsightsError] =
    useState<string | null>(null);

  const [inventoryRawResponses, setInventoryRawResponses] = useState<{
    inventory: InventoryCurrentApiResponse[];
    ageSummary: InventoryAgeSummaryApiResponse[];
  } | null>(null);

  const formatMonthName = (month?: string) => {
    const value = String(month || "").trim();

    if (!value) return "";

    return value.charAt(0).toUpperCase() + value.slice(1).toLowerCase();
  };

  const formatCountryLabel = (country?: string) => {
    const value = String(country || "").trim().toLowerCase();

    if (!value) return "";

    if (value === "uk") return "UK";
    if (value === "us") return "US";
    if (value === "global") return "Global";
    if (value === "ca" || value === "canada") return "Canada";
    if (value === "eu" || value === "europe") return "Europe";

    return value.charAt(0).toUpperCase() + value.slice(1);
  };

  const getInventoryInsightsPeriodLabel = () => {
    if (range === "monthly") {
      return `${formatMonthName(selectedMonth)} ${selectedYear}`;
    }

    if (range === "quarterly") {
      return `${selectedQuarter} ${selectedYear}`;
    }

    return `${selectedYear}`;
  };

  const getInventoryInsightsFileName = () => {
    return `Inventory Insights Report - ${formatCountryLabel(
      inventoryInsightsReportCountry
    )} - ${getInventoryInsightsPeriodLabel()}.xlsx`;
  };

  const [reconRows, setReconRows] = useState<AnyRow[]>([]);
  const [reconFetching, setReconFetching] = useState(false);
  const [reconLoadedOnce, setReconLoadedOnce] = useState(false);

  const [lostCompRows, setLostCompRows] = useState<AnyRow[]>([]);
  const [lostCompLoading, setLostCompLoading] = useState(false);

  const [showAllReconRows, setShowAllReconRows] = useState(false);
  const [showAllLostCompRows, setShowAllLostCompRows] = useState(false);

  const [anyExpanded, setAnyExpanded] = useState(false);
  const anyExpandedRef = useRef(false);

  const handleAnyGroupExpandedChange = useCallback((v: boolean) => {
    anyExpandedRef.current = v;
    setAnyExpanded(v);
  }, []);

  const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL;

  const LEDGER_DB_STORE_MONTH = `${API_BASE}/amazon_api/inventory/ledger-summary/db/store-month`;
  const LEDGER_DB_STORE_QUARTER = `${API_BASE}/amazon_api/inventory/ledger-summary/db/store-quarter`;
  const LEDGER_DB_STORE_YEAR = `${API_BASE}/amazon_api/inventory/ledger-summary/db/store-year`;

  const marketplaceId = useMemo(
    () => getMarketplaceIdForCountry(countryName),
    [countryName]
  );

  const inputCostNameToSkuMap = useMemo(() => {
    const map: Record<string, string> = {};

    for (const row of skuData || []) {
      const name = normalizeKey(String(row.product_name || ""));

      const sku =
        countryName === "uk"
          ? row.sku_uk
          : countryName === "us"
            ? row.sku_us
            : countryName === "canada"
              ? row.sku_canada
              : row.sku_uk || row.sku_us || row.sku_canada;

      if (name && sku) {
        map[name] = String(sku).trim();
      }
    }

    return map;
  }, [skuData, countryName]);

  const aiProductBlocks = useMemo(() => {
    return parseProductInsightsBlocks(aiPanel?.skuInsightsBullets ?? [], range)
  }, [aiPanel?.skuInsightsBullets]);

  const aiSkuActions = useMemo(() => {
    const recommendationsMap = aiPanel?.recommendationsMap;

    return (
      (recommendationsMap as any)?.sku_actions ??
      (recommendationsMap as any)?.recommendations ??
      recommendationsMap ??
      {}
    );
  }, [aiPanel?.recommendationsMap]);

  const drawerPeriodText = useMemo(() => {
    return aiPanel?.summaryBullets?.[0]
      ? formatSummaryPeriod(aiPanel.summaryBullets[0])
      : "";
  }, [aiPanel?.summaryBullets]);

  const fetchAiSummary = useCallback(async () => {
    const ready =
      (range === "monthly" && !!selectedMonth && !!selectedYear) ||
      (range === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (range === "yearly" && !!selectedYear);

    if (!ready || !countryName || isNA) return;

    const aiTimeline =
      range === "monthly"
        ? monthNameToNumber(selectedMonth)
        : range === "quarterly"
          ? selectedQuarter
          : "ALL";

    if (range === "monthly" && !aiTimeline) return;
    if (range === "quarterly" && !selectedQuarter) return;

    const requestId = ++aiRequestIdRef.current;

    setAiPanelLoading(true);
    setAiPanelError(null);

    try {
      const token =
        typeof window !== "undefined"
          ? localStorage.getItem("jwtToken")
          : null;

      const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/summary`);

      url.searchParams.set("country", countryName);
      url.searchParams.set("period", range);
      url.searchParams.set("timeline", String(aiTimeline));
      url.searchParams.set("year", String(selectedYear));

      const res = await fetch(url.toString(), {
        method: "GET",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        cache: "no-store",
      });

      const data: any = await res.json().catch(() => ({}));

      if (!res.ok) {
        throw new Error(data?.error || data?.message || "Failed to fetch AI summary");
      }

      if (requestId !== aiRequestIdRef.current) return;

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
        inventoryBullets: inventoryLines.length ? inventoryLines : inventoryBullets,
        recommendationsMap,
        objective: data.objective,
        rawSummary: data.summary ?? null,
        rawRecommendations:
          typeof data.recommendations === "string"
            ? data.recommendations
            : null,
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
  }, [
    countryName,
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    isNA,
  ]);

  const authHeaders = () => {
    const token = localStorage.getItem('jwtToken');
    if (!token) throw new Error('Missing jwtToken');
    return { Authorization: `Bearer ${token}` };
  };

  const yearOptions = useMemo(() => {
    const currentYear = new Date().getFullYear();
    return Array.from({ length: 6 }, (_, index) => String(currentYear - index));
  }, []);



  const handleRangeChange = (nextRange: RangeType) => {
    setRange(nextRange);

    if (nextRange === 'monthly') {
      setSelectedQuarter('');
      if (!selectedMonth) setSelectedMonth(getDefaultMonth());
    }

    if (nextRange === 'quarterly') {
      setSelectedMonth('');
      if (!selectedQuarter) setSelectedQuarter('Q1');
    }

    if (nextRange === 'yearly') {
      setSelectedMonth('');
      setSelectedQuarter('');
    }
  };

  const handleMonthChange = (nextMonth: string) => {
    setSelectedMonth(getSafeInventoryMonth(nextMonth, selectedYear));
  };

  const handleQuarterChange = (nextQuarter: string) => {
    setSelectedQuarter(nextQuarter as Quarter);
  };

  const handleYearChange = (nextYear: string) => {
    const safeYear = getSafeInventoryYear(nextYear);

    setSelectedYear(safeYear);

    if (range === 'monthly') {
      setSelectedMonth((prev) => getSafeInventoryMonth(prev, safeYear));
    }

    if (range === 'quarterly') {
      const now = new Date();
      const currentYear = now.getFullYear();
      const selectedYearNumber = Number(safeYear);

      if (selectedYearNumber >= currentYear) {
        const previousCompleted = getPreviousCompletedPeriod();
        const monthIndex = previousCompleted.monthIndex;

        const safeQuarter: Quarter =
          monthIndex <= 2 ? 'Q1' : monthIndex <= 5 ? 'Q2' : monthIndex <= 8 ? 'Q3' : 'Q4';

        setSelectedQuarter(safeQuarter);
      }
    }
  };

  const isEmptyCellValue = (value: any) => {
    if (value === null || value === undefined) return true;

    if (typeof value === 'number') {
      return Number.isNaN(value);
    }

    if (typeof value === 'string') {
      const normalized = value.trim().toLowerCase();
      return (
        normalized === '' ||
        normalized === 'nan' ||
        normalized === 'none' ||
        normalized === 'null' ||
        normalized === 'undefined' ||
        normalized === '-'
      );
    }

    return false;
  };

  const isColumnEmpty = (data: SkuRow[], columnName: string) => {
    return data.every((row) => isEmptyCellValue(row[columnName]));
  };

  // Remove local_stock and in_transit_units from SKU tab
  const getVisibleColumns = (data: SkuRow[]) => {
    if (!data || data.length === 0) return [] as string[];

    const baseColumns: string[] = ['s_no', 'product_name'];
    let skuColumns: string[] = [];
    let grossMarginColumns: string[] = [];

    if (countryName === 'global') {
      const potentialSkuColumns = ['sku_uk', 'sku_us', 'sku_canada'];

      skuColumns = potentialSkuColumns.filter((col) => !isColumnEmpty(data, col));

      grossMarginColumns = skuColumns.map((skuCol) => {
        const c = skuCol.replace('sku_', '');
        return `gross_margin_${c}`;
      });
    } else {
      const skuColumn = `sku_${countryName}`;
      if (!isColumnEmpty(data, skuColumn)) {
        skuColumns.push(skuColumn);
      }
      grossMarginColumns.push(`gross_margin_${countryName}`);
    }

    const otherColumns = [
      'asin',
      'product_barcode',
      'month_year',
      'price',
    ];

    const visibleOtherColumns = otherColumns.filter((col) => {
      if (col === 'month_year') {
        return data.some((row) => row.month || row.year);
      }
      return !isColumnEmpty(data, col);
    });

    return [...baseColumns, ...skuColumns, ...visibleOtherColumns, ...grossMarginColumns];
  };

  const getColumnDisplayName = (column: string): React.ReactNode => {
    switch (column) {
      case 's_no':
        return 'S.No.';
      case 'product_name':
        return 'Product Name';
      case 'sku_uk':
        return countryName === 'global' ? 'SKU (UK)' : 'SKU';
      case 'sku_us':
        return countryName === 'global' ? 'SKU (US)' : 'SKU';
      case 'sku_canada':
        return countryName === 'global' ? 'SKU (CANADA)' : 'SKU';
      case 'asin':
        return 'ASIN';
      case 'product_barcode':
        return 'Product Barcode';
      case 'month_year':
        return 'Month / Year';
      case 'price':
        return 'Landing Cost';
      default:
        if (column.startsWith('sku_')) {
          if (countryName !== 'global') return 'SKU';

          const c = column.replace('sku_', '').toUpperCase();
          return `SKU (${c})`;
        }
        if (column.startsWith('gross_margin_')) {
          const c = column.replace('gross_margin_', '').toUpperCase();

          return (
            <div className="text-center leading-tight">
              <span className="hidden xl:inline whitespace-nowrap">
                {countryName === 'global'
                  ? `Gross Margin (%) ${c}`
                  : 'Gross Margin (%)'}
              </span>

              <span className="xl:hidden flex flex-col items-center justify-center whitespace-normal">
                <span>Gross Margin</span>
                <span>
                  (%) {countryName === 'global' ? c : ''}
                </span>
              </span>

              <span
                className="mt-0.5 inline-flex cursor-pointer align-middle"
                title="*Gross Margin calculation is based on previous month’s ASP"
              >
                <i className="fa-solid fa-circle-info ml-1" style={{ color: '#f8edcf' }}></i>
              </span>
            </div>
          );
        }
        return column.charAt(0).toUpperCase() + column.slice(1);
    }
  };

  const getCurrencyRate = (currency: string | undefined, country: string) => {
    if (!currency || !currencyRates || Object.keys(currencyRates).length === 0) return 1;
    const possibleKeys = [
      `${currency}_${country}`,
      `${currency.toLowerCase()}_${country.toLowerCase()}`,
      `${currency.toUpperCase()}_${country.toLowerCase()}`,
      currency,
      currency.toLowerCase(),
      currency.toUpperCase(),
    ];
    for (const key of possibleKeys) {
      if (currencyRates[key] !== undefined) return currencyRates[key];
    }
    return 1;
  };

  const getAspForProduct = (productName: string, targetCountry: string | null = null) => {
    if (!aspData || Object.keys(aspData).length === 0) return null;

    if (targetCountry && countryName === 'global') {
      const countrySpecificKey = `${productName}_${targetCountry}`;
      if (aspData[countrySpecificKey] !== undefined) return aspData[countrySpecificKey];
      for (const key in aspData) {
        if (key.includes(`_${targetCountry}`) && key.includes(productName)) return aspData[key];
      }
    }

    if (countryName === 'global') {
      if (aspData[productName] !== undefined) return aspData[productName];
      for (const key in aspData) {
        if (key.includes(productName) || productName.includes(key)) return aspData[key];
      }
    } else {
      return aspData[productName] ?? null;
    }

    return null;
  };

  const calculateGrossMargin = (
    price: number | undefined,
    sourceCurrency: string | undefined,
    targetCountry: string,
    productName: string
  ): string => {
    try {
      const asp = getAspForProduct(productName, targetCountry);

      // ✅ Preview / dummy case → show 0 instead of N/A
      if (isNA) return '0.00';

      if (!asp || asp === 0) return '0.00';

      const safePrice = price ?? 0;

      let convertedPrice: number;

      if (countryName === 'global') {
        const targetCurrency = getCurrencyForCountry(targetCountry);

        if (sourceCurrency === targetCurrency) {
          convertedPrice = safePrice;
        } else {
          const sourceToUsdRate = getCurrencyRate(sourceCurrency, 'global') || 1;
          const usdToTargetRate = getCurrencyRate(targetCurrency, targetCountry) || 1;
          convertedPrice = safePrice * sourceToUsdRate * usdToTargetRate;
        }
      } else {
        const currencyRate = getCurrencyRate(sourceCurrency, targetCountry);
        convertedPrice = safePrice * currencyRate;
      }

      const grossMargin = ((asp - convertedPrice) / asp) * 100;

      // ✅ Handle NaN/Infinity safely
      if (!isFinite(grossMargin)) return '0.00';

      return grossMargin.toFixed(2);
    } catch {
      return '0.00';
    }
  };

  const fetchCurrencyRates = async () => {
    if (isNA) {
      setCurrencyRates(DUMMY_CURRENCY_RATES);
      return;
    }

    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) return;

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/currency-rates`, {
        method: 'GET',
        headers: { Authorization: `Bearer ${token}` },
      });
      if (response.ok) {
        const rates: Array<{ user_currency: string; country: string; conversion_rate: number }> =
          await response.json();
        const map: Record<string, number> = {};
        rates.forEach((rate) => {
          const keys = [
            `${rate.user_currency}_${rate.country}`,
            `${rate.user_currency.toLowerCase()}_${rate.country.toLowerCase()}`,
            `${rate.user_currency.toUpperCase()}_${rate.country.toLowerCase()}`,
            rate.user_currency,
            rate.user_currency.toLowerCase(),
            rate.user_currency.toUpperCase(),
          ];
          keys.forEach((k) => (map[k] = rate.conversion_rate));
        });
        setCurrencyRates(map);
      }
    } catch (e) {
      console.error('Error fetching currency rates', e);
    }
  };

  const fetchAspData = async () => {
    if (isNA) {
      setAspData(DUMMY_ASP_DATA);
      return;
    }

    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) return;

    const monthNames = [
      'january', 'february', 'march', 'april', 'may', 'june',
      'july', 'august', 'september', 'october', 'november', 'december',
    ];

    const normalizedMonth = (() => {
      const m = monthParam.toLowerCase();
      if (/^\d+$/.test(m)) {
        const idx = Math.min(Math.max(parseInt(m, 10) - 1, 0), 11);
        return monthNames[idx];
      }
      return monthNames.includes(m) ? m : monthNames[new Date().getMonth()];
    })();

    const normalizedYear = (() => {
      const y = parseInt(yearParam, 10);
      if (!isNaN(y) && y > 2000 && y < 2100) return y;
      return new Date().getFullYear();
    })();

    try {
      let currentMonthIndex = monthNames.indexOf(normalizedMonth);
      let currentYear = normalizedYear;

      for (let attempt = 0; attempt < 12; attempt++) {
        const monthName = monthNames[currentMonthIndex];
        try {
          const response = await fetch(
            `${process.env.NEXT_PUBLIC_API_BASE_URL}/asp-data?country=${countryName}&month=${monthName}&year=${currentYear}`,
            {
              method: 'GET',
              headers: { Authorization: `Bearer ${token}` },
            }
          );
          if (response.ok) {
            const aspArray: Array<{ product_name: string; asp: number; source_country?: string }> =
              await response.json();
            const map: Record<string, number> = {};
            aspArray.forEach((item) => {
              map[item.product_name] = item.asp;
            });
            setAspData(map);
            return;
          }
        } catch {
          // continue
        }
        currentMonthIndex--;
        if (currentMonthIndex < 0) {
          currentMonthIndex = 11;
          currentYear--;
        }
      }
      setAspData({});
    } catch (e) {
      console.error('Error in fetchAspData', e);
      setAspData({});
    }
  };

  const fetchSkuData = async () => {
    if (isNA) {
      const previewCurrency =
        countryName === 'uk'
          ? 'GBP'
          : countryName === 'us'
            ? 'USD'
            : countryName === 'canada'
              ? 'CAD'
              : countryName === 'eu' || countryName === 'europe'
                ? 'EUR'
                : 'USD';

      const previewRows = DUMMY_SKU_DATA.map((row) => ({
        ...row,
        currency: countryName === 'global' ? row.currency : previewCurrency,
        month: 'January',
        year: '2026',
      }));

      const sorted = [...previewRows].sort((a, b) => (a.s_no ?? 0) - (b.s_no ?? 0));
      setSkuData(sorted);
      setVisibleColumns(getVisibleColumns(sorted));
      setLoading(false);
      setError(null);
      return;
    }

    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) {
      setError('Authorization token is missing');
      setLoading(false);
      return;
    }

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/skuprice`, {
        method: 'GET',
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!response.ok) throw new Error('Failed to fetch data');
      const data: SkuRow[] = await response.json();
      const sorted = [...data].sort((a, b) => (a.s_no ?? 0) - (b.s_no ?? 0));
      setSkuData(sorted);
      setVisibleColumns(getVisibleColumns(sorted));
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchSkuData();
    fetchCurrencyRates();
    fetchAspData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [countryName, monthParam, yearParam, isNA]);

  useEffect(() => {
    void fetchAiSummary();
  }, [fetchAiSummary]);

  const handlePriceChange = (productName: string, value: string) => {
    setEditedPrices((prev) => ({
      ...prev,
      [productName]: value === '' ? NaN : parseFloat(value),
    }));
  };

  const saveChanges = async () => {
    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) {
      alert('Authorization token is missing');
      return;
    }
    if (Object.keys(editedPrices).length === 0) {
      alert('No changes to save.');
      return;
    }
    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/updatePrices`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ prices: editedPrices }),
      });
      if (response.ok) {
        const result = await response.json();
        setModalMessage('Prices updated successfully');
        setShowModal(true);
        setIsEditing(false);
        setEditedPrices({});
        if (result.data) {
          const sortedData: SkuRow[] = result.data.sort((a: SkuRow, b: SkuRow) => (a.s_no ?? 0) - (b.s_no ?? 0));
          setSkuData(sortedData);
          setVisibleColumns(getVisibleColumns(sortedData));
        } else {
          window.location.reload();
        }
      } else {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to update prices');
      }
    } catch (e: any) {
      alert(`Error: ${e.message}`);
      console.error('Update prices error:', e);
    }
  };

  const getOrderedWarehouseColumns = (cols: string[]) => {
    const preferredOrder = [
      's_no',
      'product_name',
      'sku_us',
      'sku_uk',
      'local_stock',
      'in_transit_units',
      'month',
      'year',
    ];

    const preferred = preferredOrder.filter((c) => cols.includes(c));
    const remaining = cols.filter((c) => !preferredOrder.includes(c));

    return [...preferred, ...remaining];
  };

  const fetchInventoryCurrentByPeriod = async (
    signal?: AbortSignal
  ): Promise<InventoryCurrentApiResponse> => {
    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    if (!token) throw new Error('Missing token');

    const url = new URL(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/inventory_current`
    );

    url.searchParams.set('country_key', String(countryName).toLowerCase());
    url.searchParams.set('year', String(selectedYear));

    if (range === 'monthly') {
      url.searchParams.set('range_type', 'monthly');
      url.searchParams.set(
        'month_name',
        getSafeInventoryMonth(selectedMonth, selectedYear)
      );
    }

    if (range === 'quarterly') {
      url.searchParams.set('range_type', 'quarter_months');
      url.searchParams.set('quarter', String(selectedQuarter));
    }

    if (range === 'yearly') {
      url.searchParams.set('range_type', 'yearly');
    }

    const res = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`,
      },
      cache: 'no-store',
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
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    if (!token) throw new Error('Missing token');

    const url = new URL(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/inventory_current_age_summary`
    );

    url.searchParams.set('country_key', String(countryValue).toLowerCase());
    url.searchParams.set('month_name', String(monthName).toLowerCase());
    url.searchParams.set('year', String(yearValue));

    const res = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`,
      },
      cache: 'no-store',
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


  async function fetchLedgerSummaryDB(params: LedgerDBReadParams) {
    const { range, year, country, marketplaceId } = params;

    const q: Record<string, any> = {
      year,
      sort: "desc",
    };

    if (country) q.country = country;
    if (marketplaceId) q.marketplace_id = marketplaceId;

    let endpoint = LEDGER_DB_STORE_YEAR;

    if (range === 'monthly') {
      const mm = monthNameToNumber(params.month);
      if (!mm) throw new Error('Invalid month selected');

      q.month = mm;
      endpoint = LEDGER_DB_STORE_MONTH;
    }

    if (range === 'quarterly') {
      const qq = quarterToNumber(params.quarter);
      if (!qq) throw new Error('Invalid quarter selected');

      q.quarter = qq;
      endpoint = LEDGER_DB_STORE_QUARTER;
    }

    const url = `${endpoint}?${buildQuery(q)}`;

    const res = await fetch(url, {
      headers: authHeaders(),
      cache: 'no-store',
    });

    const json = await res.json().catch(() => ({}));

    if (!res.ok || json?.success === false) {
      throw new Error(json?.error || 'Failed to fetch ledger summary from DB');
    }

    return Array.isArray(json?.items) ? json.items : [];
  }

  const fetchReconTableData = async () => {
    if (isNA) {
      setReconRows(DUMMY_RECON_ROWS);
      setReconFetching(false);
      setReconLoadedOnce(true);
      return;
    }

    setReconFetching(true);

    try {
      let payload: LedgerDBReadParams;

      if (range === 'monthly') {
        payload = {
          range: 'monthly',
          month: selectedMonth,
          year: selectedYear,
          country: countryName,
          marketplaceId,
        };
      } else if (range === 'quarterly') {
        payload = {
          range: 'quarterly',
          quarter: selectedQuarter,
          year: selectedYear,
          country: countryName,
          marketplaceId,
        };
      } else {
        payload = {
          range: 'yearly',
          year: selectedYear,
          country: countryName,
          marketplaceId,
        };
      }

      const items = await fetchLedgerSummaryDB(payload);

      setReconRows(items);
      setReconLoadedOnce(true);
    } catch (e) {
      console.error(e);
      setReconRows([]);
      setReconLoadedOnce(true);
    } finally {
      setReconFetching(false);
    }
  };


  async function fetchInventoryLostCompensation() {
    if (isNA) {
      setLostCompRows(DUMMY_LOST_COMP_ROWS);
      setLostCompLoading(false);
      return;
    }

    setLostCompLoading(true);

    try {
      const mode =
        range === "monthly" ? "month" : range === "quarterly" ? "quarter" : "year";

      const q: Record<string, any> = {
        country: countryName,
        year: selectedYear,
        mode,
      };

      if (mode === "month") {
        q.month = selectedMonth;
      }

      if (mode === "quarter") {
        q.quarter = String(selectedQuarter).toLowerCase();
      }

      const url = `${API_BASE}/api/inventory_lost_compensation?${buildQuery(q)}`;

      const res = await fetch(url, {
        headers: authHeaders(),
        cache: 'no-store',
      });

      const json = await res.json().catch(() => ({}));

      if (!res.ok || json?.success === false) {
        throw new Error(json?.error || "Failed to fetch inventory lost compensation");
      }

      let rows: AnyRow[] = Array.isArray(json?.data) ? json.data : [];

      rows = rows.filter((r) => {
        const name = String(r?.product_name || "").toUpperCase();
        const sku = String(r?.msku || "").toUpperCase();

        return name !== "GRAND TOTAL" && sku !== "GRAND TOTAL";
      });

      if (!rows.length) {
        setLostCompRows([]);
        return;
      }

      const total = rows.reduce(
        (acc: AnyRow, r: AnyRow) => {
          acc.lost_units += Number(r?.lost_units || 0);
          acc.damaged_units += Number(r?.damaged_units || 0);
          acc.total_lost_units += Number(r?.total_lost_units || 0);
          acc.compensation_units += Number(r?.compensation_units || 0);
          acc.compensation_value += Number(r?.compensation_value || 0);
          acc.compensation_reimbursement_amount += Number(
            r?.compensation_reimbursement_amount || 0
          );
          acc.settlement_loss_event_units += Number(
            r?.settlement_loss_event_units || 0
          );
          acc.settlement_loss_event_amount += Number(
            r?.settlement_loss_event_amount || 0
          );
          acc.loss_value += Number(r?.loss_value || 0);
          acc.net_units += Number(r?.net_units || 0);
          acc.net_value += Number(r?.net_value || 0);
          return acc;
        },
        {
          product_name: "Total",
          msku: "-",
          __isTotal: true,
          lost_units: 0,
          damaged_units: 0,
          total_lost_units: 0,
          compensation_units: 0,
          compensation_value: 0,
          compensation_reimbursement_amount: 0,
          settlement_loss_event_units: 0,
          settlement_loss_event_amount: 0,
          loss_value: 0,
          net_units: 0,
          net_value: 0,
        }
      );

      setLostCompRows([...rows, total]);
    } catch (e) {
      console.error(e);
      setLostCompRows([]);
    } finally {
      setLostCompLoading(false);
    }
  }

  const fetchWarehouseData = async () => {
    if (isNA) {
      setWarehouseData(DUMMY_WAREHOUSE_DATA);
      setWarehouseColumns(getOrderedWarehouseColumns(Object.keys(DUMMY_WAREHOUSE_DATA[0] || {})));
      setWarehouseLoading(false);
      return;
    }

    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    const skuFallbackRows = skuData
      .filter((row) => row.local_stock !== undefined || row.in_transit_units !== undefined)
      .map((row, index) => ({
        s_no: row.s_no ?? index + 1,
        product_name: row.product_name ?? '',
        sku_us: row.sku_us ?? '',
        sku_uk: row.sku_uk ?? '',
        local_stock: row.local_stock ?? '',
        in_transit_units: row.in_transit_units ?? '',
        month: row.month ?? '',
        year: row.year ?? '',
      }));

    if (!token) {
      setWarehouseData(skuFallbackRows);
      setWarehouseColumns(
        skuFallbackRows.length > 0
          ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
          : []
      );
      return;
    }

    try {
      setWarehouseLoading(true);

      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/uploadWarehouseData?country=${countryName}`,
        {
          method: 'GET',
          headers: {
            Authorization: `Bearer ${token}`,
          },
        }
      );

      const result = await response.json().catch(() => ({}));

      if (!response.ok) {
        setWarehouseData(skuFallbackRows);
        setWarehouseColumns(
          skuFallbackRows.length > 0
            ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
            : []
        );
        return;
      }

      const rows = Array.isArray(result?.data) ? result.data : [];

      if (rows.length > 0) {
        const enrichedRows = rows.map((row: Record<string, any>, index: number) => {
          const matchedSku = skuData.find((sku) => {
            if (countryName === 'global') {
              return (
                (row.sku_uk && sku.sku_uk === row.sku_uk) ||
                (row.sku_us && sku.sku_us === row.sku_us) ||
                (row.sku_canada && sku.sku_canada === row.sku_canada)
              );
            }

            const skuKey = `sku_${countryName}`;
            return sku[skuKey] && row[skuKey] && sku[skuKey] === row[skuKey];
          });

          return {
            s_no: row.s_no ?? matchedSku?.s_no ?? index + 1,
            product_name: row.product_name ?? matchedSku?.product_name ?? '',
            ...row,
          };
        });

        const cols = Array.isArray(result?.columns)
          ? result.columns
          : Object.keys(enrichedRows[0]);

        setWarehouseData(enrichedRows);
        setWarehouseColumns(
          getOrderedWarehouseColumns(
            cols.includes('product_name') ? cols : ['product_name', ...cols]
          )
        );
      } else {
        setWarehouseData(skuFallbackRows);
        setWarehouseColumns(
          skuFallbackRows.length > 0
            ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
            : []
        );
      }
    } catch (e) {
      console.error('Failed to fetch warehouse data', e);

      setWarehouseData(skuFallbackRows);
      setWarehouseColumns(
        skuFallbackRows.length > 0
          ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
          : []
      );
    } finally {
      setWarehouseLoading(false);
    }
  };

  const uploadWarehouseToServer = async (file: File) => {
    if (isNA) {
      setModalMessage('Preview mode only. Connect your account to upload warehouse data.');
      setShowModal(true);
      return;
    }

    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    if (!token) {
      setModalMessage('Authorization token is missing');
      setShowModal(true);
      return;
    }

    try {
      setWarehouseLoading(true);

      const formData = new FormData();
      formData.append('file', file);
      formData.append('country', countryName);

      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/uploadWarehouseData`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
        },
        body: formData,
      });

      const result = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(result?.error || result?.message || 'Failed to upload warehouse file');
      }

      const rows = Array.isArray(result?.data) ? result.data : [];
      setWarehouseData(rows);

      const cols = Array.isArray(result?.columns)
        ? result.columns
        : rows.length > 0
          ? Object.keys(rows[0])
          : [];

      setWarehouseColumns(getOrderedWarehouseColumns(cols));

      setSelectedWarehouseFile(null);
      setShowWarehouseUpload(false);
      setModalMessage(result?.message || 'Warehouse file uploaded successfully');
      setShowModal(true);
    } catch (e: any) {
      setModalMessage(e?.message || 'Failed to upload warehouse file');
      setShowModal(true);
    } finally {
      setWarehouseLoading(false);
    }
  };

  const handleWarehouseUpload = async (file: File) => {
    try {
      const reader = new FileReader();

      reader.onload = async (e) => {
        try {
          const wb = XLSX.read(e.target?.result as ArrayBuffer, {
            type: 'array',
          });

          const firstSheetName = wb.SheetNames[0];
          if (!firstSheetName) {
            setModalMessage('The uploaded Excel file has no sheets.');
            setShowModal(true);
            return;
          }

          const sheet = wb.Sheets[firstSheetName];
          const json = XLSX.utils.sheet_to_json(sheet);

          if (!json.length) {
            setModalMessage('The uploaded file is empty.');
            setShowModal(true);
            return;
          }

          const headerError = validateWarehouseHeaders(json[0] as Record<string, any>);
          if (headerError) {
            setModalMessage(headerError);
            setShowModal(true);
            return;
          }

          await uploadWarehouseToServer(file);
        } catch (err) {
          console.error(err);
          setModalMessage('Invalid warehouse file format.');
          setShowModal(true);
        }
      };

      reader.onerror = () => {
        setModalMessage('Failed to read the selected file.');
        setShowModal(true);
      };

      reader.readAsArrayBuffer(file);
    } catch {
      setModalMessage('Invalid warehouse file');
      setShowModal(true);
    }
  };

  useEffect(() => {
    if (activeTab === 'extra') {
      void fetchWarehouseData();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, countryName, skuData]);

  useEffect(() => {
    if (activeTab !== 'inventory-insights') return;

    if (isNA) {
      setInventoryInsightsLoading(false);
      setInventoryInsightsData(null);
      setInventoryRawResponses(null);
      setInventoryInsightsError(null);
      return;
    }

    const ready =
      (range === 'monthly' && !!selectedMonth && !!selectedYear) ||
      (range === 'quarterly' && !!selectedQuarter && !!selectedYear) ||
      (range === 'yearly' && !!selectedYear);

    if (!ready || !countryName) {
      setInventoryInsightsLoading(false);
      setInventoryInsightsData(null);
      setInventoryRawResponses(null);
      setInventoryInsightsError(null);
      return;
    }

    const ac = new AbortController();

    const fetchInventoryInsights = async () => {
      // ✅ IMPORTANT: turn loader ON immediately before clearing old data / calling APIs
      setInventoryInsightsLoading(true);
      setInventoryInsightsError(null);
      setInventoryInsightsData(null);
      setInventoryRawResponses(null);

      try {
        const trendMonthsToFetch: string[] =
          getInventoryAgeSummaryMonthsForTrend(
            range,
            selectedMonth,
            selectedQuarter,
            selectedYear
          );

        const [inventoryResult, ageSummaryResults]: [
          InventoryCurrentApiResponse,
          PromiseSettledResult<InventoryAgeSummaryApiResponse>[]
        ] = await Promise.all([
          fetchInventoryCurrentByPeriod(ac.signal),

          Promise.allSettled(
            trendMonthsToFetch.map((monthName: string) =>
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

        const fulfilledAgeSummary = ageSummaryResults
          .filter(
            (
              result
            ): result is PromiseFulfilledResult<InventoryAgeSummaryApiResponse> =>
              result.status === 'fulfilled'
          )
          .map((result) => result.value);

        if (!fulfilledInventory.length) {
          throw new Error('No inventory data found');
        }

        const nextRawResponses = {
          inventory: fulfilledInventory,
          ageSummary: fulfilledAgeSummary,
        };

        const nextInventoryInsightsData = buildInventoryInsightsFromResponses(
          fulfilledInventory,
          fulfilledAgeSummary,
          selectedAgeingTrendBucket,
          countryName,
          selectedGlobalInventoryCountry,
          range,
          selectedMonth,
          selectedQuarter
        );

        if (ac.signal.aborted) return;

        setInventoryRawResponses(nextRawResponses);
        setInventoryInsightsData(nextInventoryInsightsData);
      } catch (e: any) {
        if (e?.name === 'AbortError' || ac.signal.aborted) return;

        setInventoryInsightsData(null);
        setInventoryRawResponses(null);
        setInventoryInsightsError(
          e?.message || 'Failed to load inventory insights'
        );
      } finally {
        if (!ac.signal.aborted) {
          setInventoryInsightsLoading(false);
        }
      }
    };

    void fetchInventoryInsights();

    return () => ac.abort();
  }, [
    activeTab,
    isNA,
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    countryName,
    // selectedAgeingTrendBucket,
    // selectedGlobalInventoryCountry,
  ]);

  useEffect(() => {
    if (countryName !== "global") return;
    if (!inventoryRawResponses) return;

    const rebuiltInventoryInsightsData = buildInventoryInsightsFromResponses(
      inventoryRawResponses.inventory,
      inventoryRawResponses.ageSummary,
      selectedAgeingTrendBucket,
      countryName,
      selectedGlobalInventoryCountry,
      range,
      selectedMonth,
      selectedQuarter
    );

    setInventoryInsightsData(rebuiltInventoryInsightsData);
  }, [
    countryName,
    inventoryRawResponses,
    selectedAgeingTrendBucket,
    selectedGlobalInventoryCountry,
    range,
    selectedMonth,
    selectedQuarter
  ]);

  useEffect(() => {
    setShowAllReconRows(false);
    setShowAllLostCompRows(false);
  }, [range, selectedMonth, selectedQuarter, selectedYear, countryName]);

  useEffect(() => {
    if (activeTab !== 'recon-table') return;

    const ready =
      (range === 'monthly' && !!selectedMonth && !!selectedYear) ||
      (range === 'quarterly' && !!selectedQuarter && !!selectedYear) ||
      (range === 'yearly' && !!selectedYear);

    if (!ready) return;

    void fetchReconTableData();

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    activeTab,
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    countryName,
    marketplaceId,
    isNA,
  ]);

  useEffect(() => {
    if (activeTab !== 'lost-compensation') return;

    const ready =
      (range === 'monthly' && !!selectedMonth && !!selectedYear) ||
      (range === 'quarterly' && !!selectedQuarter && !!selectedYear) ||
      (range === 'yearly' && !!selectedYear);

    if (!ready) return;

    void fetchInventoryLostCompensation();

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, range, selectedMonth, selectedQuarter, selectedYear, countryName, isNA]);

  // useEffect(() => {
  //   if (!inventoryRawResponses) return;

  //   setInventoryInsightsData(
  //     buildInventoryInsightsFromResponses(
  //       inventoryRawResponses.inventory,
  //       inventoryRawResponses.ageSummary,
  //       selectedAgeingTrendBucket,
  //       countryName
  //     )
  //   );
  // }, [
  //   selectedAgeingTrendBucket,
  //   inventoryRawResponses,
  //   countryName,
  // ]);

  const renderGrossMarginCell = (row: SkuRow, column: string) => {
    const targetCountry = column.replace('gross_margin_', '');
    const currentPrice =
      editedPrices[row.product_name] !== undefined ? editedPrices[row.product_name] : row.price;
    const currency = row.currency;
    const grossMargin = calculateGrossMargin(currentPrice, currency, targetCountry, row.product_name);

    if (grossMargin === 'N/A') return <span className="gross-margin-na">N/A</span>;
    const marginValue = parseFloat(grossMargin);
    const className = marginValue >= 0 ? 'gross-margin-positive' : 'gross-margin-negative';
    return <span className={className}>{grossMargin}%</span>;
  };

  const getMonthYearDisplay = (row: SkuRow) => {
    const month = row.month ?? '';
    const year = row.year ?? '';
    if (month && year) return `${month} ${year}`;
    if (month) return String(month);
    if (year) return String(year);
    return '—';
  };

  const handleDownloadXLSX = async () => {
    if (!skuData || skuData.length === 0) {
      alert('No data available to download.');
      return;
    }

    const dataToExport = skuData.map((row) => {
      const updatedPrice =
        editedPrices[row.product_name] !== undefined
          ? editedPrices[row.product_name]
          : row.price;

      const exportRow: Record<string, any> = {
        s_no: row.s_no ?? '',
        product_name: row.product_name ?? '',
      };

      visibleColumns.forEach((col) => {
        if (col.startsWith('gross_margin_')) {
          const targetCountry = col.replace('gross_margin_', '');
          const gm = calculateGrossMargin(
            updatedPrice,
            row.currency,
            targetCountry,
            row.product_name
          );

          exportRow[col] = gm !== 'N/A' ? Number(gm) : '';
        } else if (col === 'price') {
          exportRow.price = updatedPrice ?? '';
        } else if (col === 'month_year') {
          exportRow.month_year = getMonthYearDisplay(row);
        } else {
          exportRow[col] = row[col] ?? '';
        }
      });

      return exportRow;
    });

    await exportSkuInformationExcel({
      filename: `SKU_Information_${countryName?.toUpperCase() || 'EXPORT'}.xlsx`,
      countryName,
      titleLine: 'SKU Information',
      titleCountry: countryName === 'global' ? 'Global' : countryName.toUpperCase(),
      platformLabel: 'Phormula',
      periodLabel: `${monthParam} ${yearParam}`,
      companyName,
      brandName,
      dataRows: dataToExport,
    });
  };

  const tableData: TableRow[] = useMemo(() => {
    return skuData.map((row, index) => {
      const item: TableRow = {
        id: `${row.product_name}-${index}`,
        s_no: row.s_no ?? index + 1,
        product_name: row.product_name ?? '—',
        sku_uk: isEmptyCellValue(row.sku_uk) ? '—' : row.sku_uk,
        sku_us: isEmptyCellValue(row.sku_us) ? '—' : row.sku_us,
        sku_canada: isEmptyCellValue(row.sku_canada) ? '—' : row.sku_canada,
        asin: row.asin ?? '—',
        product_barcode: row.product_barcode ?? '—',
        month_year: getMonthYearDisplay(row),
        price: row.price ?? '',
      };

      visibleColumns.forEach((col) => {
        if (col.startsWith('gross_margin_')) {
          item[col] = '';
        } else if (col in row) {
          item[col] = isEmptyCellValue(row[col]) ? '—' : row[col];
        }
      });

      return item;
    });
  }, [skuData, visibleColumns]);

  const openAiProductDrawerByName = useCallback(
    (productName: string, sku?: string, inventoryRow?: any) => {
      const cleanName = String(productName || "").trim();
      const cleanSku = String(sku || "").trim();

      if (!cleanName && !cleanSku) return;

      const normalizedClickedName = normalizeKey(cleanName);

      const block =
        (cleanSku
          ? aiProductBlocks.find(
            (b) =>
              String(b.skuKey || "").trim().toLowerCase() ===
              cleanSku.toLowerCase()
          )
          : undefined) ||
        aiProductBlocks.find(
          (b) => normalizeKey(b.name) === normalizedClickedName
        );

      if (!block) {
        console.warn("No AI insight block found for:", {
          productName: cleanName,
          sku: cleanSku,
          aiProductBlocks,
        });
        return;
      }

      const skuKey =
        block.skuKey ||
        cleanSku ||
        inputCostNameToSkuMap?.[normalizeKey(block.name)];

      const recObj =
        (skuKey && (aiSkuActions as any)[skuKey]) ||
        (aiSkuActions as any)[block.name] ||
        (aiSkuActions as any)[block.name.trim()] ||
        null;

      setSelectedAiProductRecObj(recObj);
      const monthlyDrawerMetrics =
        isMonthlyRange(range) && inventoryRow
          ? [
            {
              label: "Current Inventory",
              value: formatUnitsNoDecimal(
                inventoryRow.available ??
                inventoryRow.totalUnits ??
                inventoryRow.total_units ??
                inventoryRow.total_quantity ??
                0
              ),
            },
            {
              label: "Stock Cover",
              value:
                Number(inventoryRow.coverageRatio ?? 0) > 0
                  ? Number(inventoryRow.coverageRatio).toFixed(2)
                  : "-",
            },
          ]
          : [];

      const existingDrawerOnlyMetrics = block.drawerOnlyMetrics || [];

      const nextBlock: ProductInsightBlock = {
        ...block,
        drawerOnlyMetrics: [
          ...existingDrawerOnlyMetrics,
          ...monthlyDrawerMetrics.filter(
            (metric) =>
              !existingDrawerOnlyMetrics.some(
                (existing) =>
                  existing.label.trim().toLowerCase() ===
                  metric.label.trim().toLowerCase()
              )
          ),
        ],
      };

      setSelectedAiProductBlock(nextBlock);
    },
    [aiProductBlocks, aiSkuActions, inputCostNameToSkuMap]
  );

  const handleHeatmapProductClick = useCallback(
    (heatmapRow: AgeingRiskHeatmapRow) => {
      if (!heatmapRow || heatmapRow.isTotalRow || heatmapRow.isOthersRow) return;

      const productName = String(heatmapRow.productName || "").trim();
      const sku = String(heatmapRow.sku || "").trim();

      if (!productName && !sku) return;

      openAiProductDrawerByName(productName, sku, heatmapRow);
    },
    [openAiProductDrawerByName]
  );

  const getSkuFromAnyRow = useCallback(
    (row: any) => {
      if (!row) return "";

      if (countryName === "uk") {
        return (
          row.sku_uk ||
          row.sku ||
          row.SKU ||
          row.msku ||
          row.seller_sku ||
          row.fnsku ||
          ""
        );
      }

      if (countryName === "us") {
        return (
          row.sku_us ||
          row.sku ||
          row.SKU ||
          row.msku ||
          row.seller_sku ||
          row.fnsku ||
          ""
        );
      }

      if (countryName === "canada") {
        return (
          row.sku_canada ||
          row.sku ||
          row.SKU ||
          row.msku ||
          row.seller_sku ||
          row.fnsku ||
          ""
        );
      }

      return (
        row.sku_uk ||
        row.sku_us ||
        row.sku_canada ||
        row.sku ||
        row.SKU ||
        row.msku ||
        row.seller_sku ||
        row.fnsku ||
        ""
      );
    },
    [countryName]
  );

  const renderClickableProductName = useCallback(
    (
      productNameValue: any,
      skuValue?: any,
      options?: {
        disabled?: boolean;
        displayName?: string;
      }
    ) => {
      const productName = String(productNameValue || "").trim();
      const sku = String(skuValue || "").trim();

      const displayName = options?.displayName || productName || "—";
      const normalizedName = productName.toLowerCase();

      const isSpecialRow =
        options?.disabled ||
        !productName ||
        normalizedName === "total" ||
        normalizedName === "grand total" ||
        normalizedName === "others" ||
        normalizedName === "other skus" ||
        normalizedName === "-";

      if (isSpecialRow) {
        return <span>{displayName}</span>;
      }

      return (
        <button
          type="button"
          onClick={() => openAiProductDrawerByName(productName, sku)}
          className="text-left text-green-500"
          title="Open detailed product view"
        >
          {displayName}
        </button>
      );
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
              home_currency: getCurrencyForCountry(countryName),
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
  }, [selectedAiProductBlock, aiProductBlocks, countryName]);

  const columns: ColumnDef<TableRow>[] = useMemo(() => {
    return visibleColumns.map((column) => {
      const col: ColumnDef<TableRow> = {
        key: column,
        header: getColumnDisplayName(column),
      };

      if (column === 's_no') col.width = '60px';

      if (column === "product_name") {
        col.width = "160px";
        col.cellClassName = "text-left";

        col.render = (tableRow) => {
          const productName = String(tableRow.product_name || "").trim();

          const originalRow = skuData.find(
            (r) => String(r.product_name || "").trim() === productName
          );

          if (!originalRow) {
            return <span>{tableRow.product_name}</span>;
          }

          const sku =
            countryName === "uk"
              ? originalRow.sku_uk
              : countryName === "us"
                ? originalRow.sku_us
                : countryName === "canada"
                  ? originalRow.sku_canada
                  : originalRow.sku_uk ||
                  originalRow.sku_us ||
                  originalRow.sku_canada;

          return (
            <button
              type="button"
              onClick={() =>
                openAiProductDrawerByName(
                  originalRow.product_name,
                  String(sku || "")
                )
              }
              className="text-left text-green-500 "
            >
              {tableRow.product_name}
            </button>
          );
        };
      }

      if (column === 'sku_uk' || column === 'sku_us' || column === 'sku_canada') {
        col.width = '130px';
        col.cellClassName = 'text-left';
        col.headerClassName = 'text-center';
      }

      if (column === 'asin') col.width = '140px';
      if (column === 'product_barcode') col.width = '160px';
      if (column === 'month_year') col.width = '140px';

      if (column === 'price') {
        col.width = '100px';
        col.render = (tableRow) => {
          const originalRow = skuData.find((r) => r.product_name === String(tableRow.product_name));
          if (!originalRow) return '—';

          return isEditing ? (
            <div className="flex items-center justify-center gap-1">
              <span>{getCurrencySymbol(originalRow.currency)}</span>
              <input
                type="number"
                className="border border-gray-300 rounded px-2 py-1 w-[90px] text-center"
                value={
                  editedPrices[originalRow.product_name] !== undefined
                    ? editedPrices[originalRow.product_name]
                    : originalRow.price ?? ''
                }
                onChange={(e) => handlePriceChange(originalRow.product_name, e.target.value)}
              />
            </div>
          ) : (
            <span>
              {getCurrencySymbol(originalRow.currency)} {originalRow.price ?? '—'}
            </span>
          );
        };
      }

      if (column.startsWith('gross_margin_')) {
        col.width = '150px';
        col.render = (tableRow) => {
          const originalRow = skuData.find((r) => r.product_name === String(tableRow.product_name));
          if (!originalRow) return 'N/A';
          return renderGrossMarginCell(originalRow, column);
        };
      }

      return col;
    });
  }, [
    visibleColumns,
    skuData,
    isEditing,
    editedPrices,
    countryName,
    openAiProductDrawerByName,
  ]);

  const periodLabel = useMemo(() => {
    if (range === "monthly") return "month";
    if (range === "quarterly") return "quarter";
    return "year";
  }, [range]);

  const beginningInventoryLabel = useMemo(
    () => `Inventory at the beginning of the ${periodLabel}`,
    [periodLabel]
  );

  const endingInventoryLabel = useMemo(() => {
    if (range === "monthly") return "Inventory at month end";
    if (range === "quarterly") return "Inventory at quarter end";
    return "Inventory at year end";
  }, [range]);

  const leftCols: LeafCol<AnyRow>[] = [
    { key: '__sno', label: 'S. No.', width: 70, align: 'center' },
    { key: 'product_name', label: 'Product Name', width: 120, align: 'left' },
    { key: 'msku', label: 'SKU', width: 110, align: 'left' },
  ];

  const groups: ColGroup<AnyRow>[] = useMemo(
    () => [
      {
        id: 'beginning',
        label: beginningInventoryLabel,
        headerClassName: 'min-w-[120px]',
        collapsedCols: [
          { key: '__beginning_total', label: 'Total', width: 140, align: 'center' },
        ],
        expandedCols: [
          { key: 'sellable_sum_first', label: 'Sellable', width: 110, align: 'center' },
          { key: '__beginning_damaged_total', label: 'Damaged', width: 110, align: 'center' },
          { key: 'expired_sum_first', label: 'Expired', width: 110, align: 'center' },
          { key: 'sum_in_transit_between_warehouses', label: 'Transit (Between WH)', width: 110, align: 'center' },
          { key: 'beginning_total', label: 'Total', width: 110, align: 'center' },
        ],
      },
      {
        id: 'units_in_transit',
        label: 'Units in transit',
        headerClassName: 'min-w-[120px]',
        collapsedCols: [
          { key: '__transit_total', label: 'Total', width: 100, align: 'center' },
        ],
        expandedCols: [
          { key: 'transit_total', label: 'In Transit', width: 110, align: 'center' },
          { key: 'sum_receipts', label: 'Delivered', width: 110, align: 'center' },
          { key: '__transit_total', label: 'Total', width: 110, align: 'center' },
        ],
      },
      {
        id: 'other_items',
        label: 'Other Items',
        headerClassName: 'min-w-[120px]',
        collapsedCols: [
          { key: '__other_items_total', label: 'Total', width: 90, align: 'center' },
        ],
        expandedCols: [
          { key: 'sum_disposed', label: 'Units Disposed', width: 110, align: 'center' },
          { key: 'sum_damaged', label: 'Damaged', width: 110, align: 'center' },
          { key: 'sum_unknown_events', label: 'Unknown Event', width: 110, align: 'center' },
          { key: 'sum_other_events', label: 'Other Events', width: 110, align: 'center' },
          { key: 'sum_vendor_returns', label: 'Vendor Return', width: 110, align: 'center' },
          { key: 'sum_lost', label: 'Lost', width: 110, align: 'center' },
          { key: 'sum_found', label: 'Found', width: 110, align: 'center' },
          { key: '__other_items_total', label: 'Total', width: 110, align: 'center' },
        ],
      },
      {
        id: 'units_sold',
        label: 'Units Sold',
        headerClassName: 'min-w-[120px]',
        collapsedCols: [
          { key: '__units_sold_net', label: 'Net Units', width: 90, align: 'center' },
        ],
        expandedCols: [
          { key: '__units_sold_gross', label: 'Gross Sales', width: 110, align: 'center' },
          { key: '__units_sold_returns', label: 'Return', width: 110, align: 'center' },
          { key: '__units_sold_net', label: 'Net Units', width: 110, align: 'center' },
        ],
      },
      {
        id: 'open_orders',
        label: 'Open orders',
        headerClassName: 'min-w-[120px]',
        collapsedCols: [
          { key: '__open_orders_total', label: 'Total', width: 110, align: 'center' },
        ],
        expandedCols: [
          { key: '__open_orders_beginning', label: 'Beginning', width: 110, align: 'center' },
          { key: '__open_orders_end', label: 'End', width: 110, align: 'center' },
          { key: '__open_orders_total', label: 'Total', width: 110, align: 'center' },
        ],
      },
      {
        id: 'ending',
        label: endingInventoryLabel,
        headerClassName: 'min-w-[120px]',
        collapsedCols: [
          { key: '__ending_total', label: 'Total', width: 110, align: 'center' },
        ],
        expandedCols: [
          { key: 'sellable_sum_last', label: 'Sellable', width: 110, align: 'center' },
          { key: '__ending_damaged_lost_total', label: 'Damaged/Lost', width: 110, align: 'center' },
          { key: 'expired_sum_last', label: 'Expired', width: 110, align: 'center' },
          { key: '__ending_transit_placeholder', label: 'Transit (Between WH)', width: 110, align: 'center' },
          { key: 'ending_total', label: 'Total', width: 110, align: 'center' },
        ],
      },
    ],
    [beginningInventoryLabel, endingInventoryLabel]
  );

  const singleCols: LeafCol<AnyRow>[] = useMemo(
    () => [
      {
        key: "inventory_coverage_ratio",
        label: "Inventory Coverage Ratio",
        width: 140,
        align: "center",
      },
      {
        key: "difference_total",
        label: "Difference",
        width: 90,
        align: "center",
      },
    ],
    []
  );

  const reconDisplayRows = useMemo(() => {
    if (!reconRows || reconRows.length === 0) return [];

    const grandTotalRow = reconRows.find(isTotalRow) || null;
    const dataRows = reconRows.filter((r) => !isTotalRow(r));

    const sortedDataRows = [...dataRows].sort((a, b) => {
      return Math.abs(toInventoryInt(b?.sold_total)) - Math.abs(toInventoryInt(a?.sold_total));
    });

    const top = showAllReconRows ? sortedDataRows : sortedDataRows.slice(0, 9);
    const remaining = showAllReconRows ? [] : sortedDataRows.slice(9);

    const keys = Array.from(
      new Set(
        dataRows.flatMap((r) =>
          Object.keys(r || {}).filter(
            (k) => isNumericLike(r?.[k]) && k !== "inventory_coverage_ratio"
          )
        )
      )
    );

    const out: AnyRow[] = [...top];

    if (remaining.length > 0) {
      const others = sumRowForKeys(remaining, keys, {
        id: "__OTHERS__",
        msku: "OTHERS",
        product_name: "OTHERS",
        __isOthers: true,
      });

      const endingTotal = toInventoryInt(others?.ending_total);
      const soldTotalAbs = Math.abs(toInventoryInt(others?.sold_total));

      others.inventory_coverage_ratio =
        soldTotalAbs > 0 ? endingTotal / soldTotalAbs : 0;

      out.push(others);
    }

    const total =
      grandTotalRow
        ? {
          ...grandTotalRow,
          id: "__TOTAL__",
          __isTotal: true,
        }
        : (() => {
          const totalRow = sumRowForKeys(dataRows, keys, {
            id: "__TOTAL__",
            msku: "GRAND TOTAL",
            product_name: "GRAND TOTAL",
            __isTotal: true,
          });

          const endingTotal = toInventoryInt(totalRow?.ending_total);
          const soldTotalAbs = Math.abs(toInventoryInt(totalRow?.sold_total));

          totalRow.inventory_coverage_ratio =
            soldTotalAbs > 0 ? endingTotal / soldTotalAbs : 0;

          return totalRow;
        })();

    out.push(total);

    return out;
  }, [reconRows, showAllReconRows]);

  const effectiveReconRows = useMemo(() => {
    if (isNA) return DUMMY_RECON_ROWS;
    return reconDisplayRows;
  }, [isNA, reconDisplayRows]);

  const getReconRowClassName = (row: AnyRow) => {
    const msku = String(row?.msku || '').trim().toUpperCase();
    const isGrand = isTotalRow(row) || msku === 'TOTAL' || row?.__isTotal === true;
    const isOthers = msku === 'OTHERS' || row?.__isOthers === true;

    if (isGrand) return 'bg-[#EFEFEF] font-semibold';
    if (isOthers) return '';
    return '';
  };

  const getReconValue = (row: AnyRow, colKey: string, exportIndex?: number) => {
    const isSpecialRow =
      row?.__isTotal === true ||
      row?.__isOthers === true ||
      isTotalRow(row);

    if (colKey === 'msku' && isSpecialRow) return '-';

    const pn = String(row?.product_name || '').trim().toUpperCase();

    if (colKey === "product_name") {
      if (pn === "OTHERS") {
        return <span className="text-green-500">Others</span>;
      }

      if (pn === "TOTAL" || pn === "GRAND TOTAL") return "Total";

      return renderClickableProductName(row?.product_name, row?.msku, {
        disabled:
          row?.__isTotal === true ||
          row?.__isOthers === true ||
          isTotalRow(row),
      });
    }

    if (colKey === "__sno") {
      if (row?.__isOthers === true || String(row?.msku || "").trim().toUpperCase() === "OTHERS") {
        return "10";
      }

      if (row?.__isTotal === true || isTotalRow(row)) {
        return "";
      }

      if (typeof exportIndex === "number") {
        return String(exportIndex + 1);
      }

      const visibleRows = reconDisplayRows.filter(
        (r) => !(r?.__isTotal || isTotalRow(r) || r?.__isOthers)
      );

      const idx = visibleRows.findIndex(
        (r) => (r?.id ?? r?.msku) === (row?.id ?? row?.msku)
      );

      return idx >= 0 ? String(idx + 1) : "";
    }

    if (
      colKey === 'sum_in_transit_between_warehouses' ||
      colKey === '__open_orders_beginning' ||
      colKey === '__open_orders_end' ||
      colKey === '__open_orders_total' ||
      colKey === '__ending_transit_placeholder'
    ) {
      return '-';
    }

    if (colKey === '__beginning_damaged_total') {
      return formatReconCell(
        toInventoryInt(row?.warehouse_damaged_sum_first) +
        toInventoryInt(row?.customer_damaged_sum_first) +
        toInventoryInt(row?.distributor_damaged_sum_first) +
        toInventoryInt(row?.defective_sum_first)
      );
    }

    if (colKey === '__beginning_total') return formatReconCell(row?.beginning_total);

    if (colKey === 'transit_total') return '-';

    if (colKey === 'sum_receipts') return formatReconCell(row?.sum_receipts);

    if (colKey === '__transit_total') return formatReconCell(row?.transit_total);

    if (colKey === '__other_items_total') {
      return formatReconCell(row?.other_total);
    }

    if (colKey === '__units_sold_gross') {
      return formatReconCell(Math.abs(toInventoryInt(row?.sum_customer_shipments)));
    }

    if (colKey === '__units_sold_returns') {
      return formatReconCell(Math.abs(toInventoryInt(row?.sum_customer_returns)));
    }

    if (colKey === '__units_sold_net') {
      return formatReconCell(Math.abs(toInventoryInt(row?.sold_total)));
    }

    if (colKey === '__ending_total') {
      return formatReconCell(row?.ending_total);
    }

    if (colKey === '__ending_damaged_lost_total') {
      const total =
        toInventoryInt(row?.defective_sum_last) +
        toInventoryInt(row?.warehouse_damaged_sum_last) +
        toInventoryInt(row?.customer_damaged_sum_last) +
        toInventoryInt(row?.distributor_damaged_sum_last);

      return formatReconCell(total);
    }

    if (colKey === "inventory_coverage_ratio") {
      const raw = Number(row?.inventory_coverage_ratio);
      if (!Number.isFinite(raw) || raw === 0) return "-";
      return raw.toFixed(2);
    }

    return formatReconCell(row?.[colKey]);
  };

  const SIGN_PLUS = useMemo(
    () =>
      new Set([
        "sum_disposed",
        "sum_damaged",
        "sum_unknown_events",
        "sum_other_events",
        "sum_vendor_returns",
        "sum_lost",
      ]),
    []
  );

  const SIGN_MINUS = useMemo(
    () =>
      new Set([
        "sum_found",
      ]),
    []
  );

  const getSignForCol = useCallback(
    (colKey: string) => {
      if (SIGN_PLUS.has(colKey)) return { text: "(+)", className: "text-green-700" };
      if (SIGN_MINUS.has(colKey)) return { text: "(-)", className: "text-[#ff5c5c]" };
      return null;
    },
    [SIGN_PLUS, SIGN_MINUS]
  );


  const effectiveLostCompRows = useMemo(() => {
    if (isNA) return DUMMY_LOST_COMP_ROWS;
    return lostCompRows;
  }, [isNA, lostCompRows]);

  const lostCompDisplayRows = useMemo(() => {
    const rows = effectiveLostCompRows || [];

    const totalRow = rows.find((r) => r?.__isTotal);
    const dataRows = rows.filter((r) => !r?.__isTotal);

    const sorted = [...dataRows].sort(
      (a, b) =>
        Math.abs(toInventoryInt(b?.total_lost_units)) -
        Math.abs(toInventoryInt(a?.total_lost_units))
    );

    const top9 = showAllLostCompRows ? sorted : sorted.slice(0, 9);
    const others = showAllLostCompRows ? [] : sorted.slice(9);

    const out: AnyRow[] = [...top9];

    if (others.length > 0) {
      out.push({
        product_name: "Others",
        msku: "-",
        __isOthers: true,
        lost_units: others.reduce((a, r) => a + toInventoryInt(r?.lost_units), 0),
        damaged_units: others.reduce((a, r) => a + toInventoryInt(r?.damaged_units), 0),
        total_lost_units: others.reduce((a, r) => a + toInventoryInt(r?.total_lost_units), 0),
        compensation_units: others.reduce((a, r) => a + toInventoryInt(r?.compensation_units), 0),
        compensation_value: others.reduce((a, r) => a + toInventoryInt(r?.compensation_value), 0),
        settlement_loss_event_amount: others.reduce(
          (a, r) => a + toInventoryInt(r?.settlement_loss_event_amount),
          0
        ),
        net_units: others.reduce((a, r) => a + toInventoryInt(r?.net_units), 0),
        net_value: others.reduce((a, r) => a + toInventoryInt(r?.net_value), 0),
      });
    }

    if (totalRow) out.push(totalRow);

    return out;
  }, [effectiveLostCompRows, showAllLostCompRows]);

  const lostCompTableData = useMemo<Record<string, React.ReactNode>[]>(() => {
    return (lostCompDisplayRows || []).map((row, idx) => ({
      __isTotal: row?.__isTotal,
      __isOthers: row?.__isOthers,
      __sno: row?.__isTotal ? "" : idx + 1,

      product_name:
        row?.__isTotal || row?.__isOthers
          ? formatReconCell(row?.product_name)
          : renderClickableProductName(row?.product_name, row?.msku),

      msku: formatReconCell(row?.msku),

      lost_units: formatReconCell(row?.lost_units),
      damaged_units: formatReconCell(row?.damaged_units),
      compensation_units: formatReconCell(row?.compensation_units),
      net_units: formatReconCell(row?.net_units),
      total_lost_units: formatReconCell(row?.total_lost_units),
      compensation_value: formatReconCell(row?.compensation_value),
      settlement_loss_event_amount: formatReconCell(row?.settlement_loss_event_amount),
      net_value: formatReconCell(row?.net_value),
    }));
  }, [lostCompDisplayRows, renderClickableProductName]);

  const countryCurrencySymbol = useMemo(() => {
    const c = String(countryName || "").trim().toLowerCase();

    const map: Record<string, string> = {
      uk: "£",
      gb: "£",
      us: "$",
      usa: "$",
      india: "₹",
      in: "₹",
      canada: "C$",
      ca: "C$",
    };

    return map[c] || "";
  }, [countryName]);

  const lostCompTableColumns = useMemo<
    ColumnDef<Record<string, React.ReactNode>>[]
  >(() => {
    const countryCurrencySuffix = countryCurrencySymbol
      ? ` (${countryCurrencySymbol})`
      : "";

    return [
      {
        key: "__sno",
        header: "S. No.",
        width: "w-[70px]",
        cellClassName: "text-center",
      },
      {
        key: "product_name",
        header: "Product Name",
        width: "w-[220px]",
        cellClassName: "text-left",
        headerClassName: "text-left break-words",
      },
      {
        key: "msku",
        header: "SKU",
        width: "w-[120px]",
        cellClassName: "text-left",
        headerClassName: "text-center",
      },
      {
        key: "lost_units",
        header: "Lost Units",
        width: "w-[120px]",
        cellClassName: "text-center",
        headerClassName: "break-words",
      },
      {
        key: "damaged_units",
        header: "Damaged Units",
        width: "w-[120px]",
        cellClassName: "text-center",
        headerClassName: "break-words",
      },
      {
        key: "compensation_units",
        header: "Compensation Units",
        width: "w-[150px]",
        cellClassName: "text-center",
        headerClassName: "break-words",
      },
      {
        key: "net_units",
        header: "Remaining Compensation Units",
        width: "w-[160px]",
        cellClassName: "text-center",
        headerClassName: "break-words",
      },
      {
        key: "total_lost_units",
        header: "Total Lost Units",
        width: "w-[130px]",
        cellClassName: "text-center",
        headerClassName: "break-words",
      },
      {
        key: "compensation_value",
        header: `Compensation Value Amount${countryCurrencySuffix}`,
        width: "w-[170px]",
        cellClassName: "text-center",
        headerClassName: "break-words",
      },
      {
        key: "settlement_loss_event_amount",
        header: `Remaining Compensation Amount${countryCurrencySuffix}`,
        width: "w-[200px]",
        cellClassName: "text-center",
        headerClassName: "break-words",
      },
      {
        key: "net_value",
        header: `Total Compensation Value${countryCurrencySuffix}`,
        width: "w-[170px]",
        cellClassName: "text-center",
        headerClassName: "break-words",
      },
    ];
  }, [countryCurrencySymbol]);

  const tabOptions = useMemo(
    () => [
      { value: 'inventory-insights' as const, label: 'Inventory Insights' },
      { value: 'sku-info' as const, label: 'SKU Information' },
      { value: 'extra' as const, label: 'Upload Warehouse' },
      { value: 'recon-table' as const, label: 'Recon Table' },
      { value: 'lost-compensation' as const, label: 'Lost vs Compensation' },
    ],
    []
  );

  // 4) Make warehouse header label nicer
  const getWarehouseHeaderLabel = (col: string) => {
    if (col === 's_no') return 'S.No.';
    if (col === 'product_name') return 'Product Name';

    if (col === 'sku_uk' || col === 'sku_us' || col === 'sku_canada') {
      if (countryName === 'global') {
        const c = col.replace('sku_', '').toUpperCase();
        return `SKU (${c})`;
      }

      return 'SKU';
    }

    const formatted = col
      .replaceAll('_', ' ')
      .replace(/\b\w/g, (c) => c.toUpperCase());

    return formatted;
  };

  // 5) Optional: make Product Name column a bit wider
  const warehouseTableColumns: ColumnDef<Record<string, any>>[] = useMemo(() => {
    const filteredWarehouseColumns = warehouseColumns.filter((col) => {
      // Global should show all country SKU columns
      if (countryName === 'global') return true;

      // For country-wise pages, show only that country's SKU column
      if (col.startsWith('sku_')) {
        return col === `sku_${countryName}`;
      }

      return true;
    });

    return filteredWarehouseColumns.map((col) => ({
      key: col,
      header: getWarehouseHeaderLabel(col),
      width:
        col === 's_no'
          ? '70px'
          : col === 'product_name'
            ? '220px'
            : col === 'sku_us' || col === 'sku_uk' || col === 'sku_canada'
              ? '120px'
              : col === 'local_stock' || col === 'in_transit_units'
                ? '150px'
                : col === 'month' || col === 'year'
                  ? '110px'
                  : '140px',
      cellClassName:
        col === 'product_name' ||
          col === 'sku_us' ||
          col === 'sku_uk' ||
          col === 'sku_canada'
          ? 'text-left'
          : 'text-center',
      render: (row) => {
        const value = row[col];

        if (col === "product_name") {
          const sku = getSkuFromAnyRow(row);

          return renderClickableProductName(value, sku);
        }

        if (col === "month" && typeof value === "string") {
          return value.charAt(0).toUpperCase() + value.slice(1).toLowerCase();
        }

        return value === null || value === undefined || value === ""
          ? "—"
          : String(value);
      },
    }));
  }, [
    warehouseColumns,
    countryName,
    getSkuFromAnyRow,
    renderClickableProductName,
  ]);

  if (error) return <div>Error: {error}</div>;

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

  const handleConnectAmazonPreview = () => {
    router.push(`/profile/${countryName}/NA/NA`);
  };

  const handleWarehouseDownload = async () => {
    if (!warehouseData || warehouseData.length === 0) {
      alert('No warehouse data available to download.');
      return;
    }

    const exportWarehouseColumns = warehouseColumns.filter((col) => {
      if (countryName === 'global') return true;

      if (col.startsWith('sku_')) {
        return col === `sku_${countryName}`;
      }

      return true;
    });

    const exportData = warehouseData.map((row, index) => {
      const exportRow: Record<string, any> = {};

      exportWarehouseColumns.forEach((col) => {
        if (col === 's_no') {
          exportRow.s_no = row.s_no ?? index + 1;
        } else if (col === 'product_name') {
          exportRow.product_name = row.product_name ?? '';
        } else if (col === 'month' && typeof row[col] === 'string') {
          exportRow.month =
            row[col].charAt(0).toUpperCase() + row[col].slice(1).toLowerCase();
        } else {
          exportRow[col] = isEmptyCellValue(row[col]) ? '' : row[col];
        }
      });

      return exportRow;
    });

    await exportWarehouseDataExcel({
      filename: `Warehouse_Data_${countryName?.toUpperCase() || 'EXPORT'}.xlsx`,
      countryName,
      titleLine: 'Warehouse Data',
      titleCountry: countryName === 'global' ? 'Global' : countryName.toUpperCase(),
      platformLabel: 'Phormula',
      periodLabel: `${monthParam} ${yearParam}`,
      companyName,
      brandName,
      dataRows: exportData,
    });
  };

  const INPUT_COST_VISIBLE_ROWS = 15;
  const INPUT_COST_ROW_HEIGHT = 40;

  const RECON_VISIBLE_ROWS = 15;
  const LOST_COMP_VISIBLE_ROWS = 15;
  const TABLE_ROW_HEIGHT = 40;

  const shouldScrollReconTable =
    !showAllReconRows &&
    effectiveReconRows.filter((row) => !isTotalRow(row)).length > RECON_VISIBLE_ROWS;

  const shouldScrollLostCompTable =
    !showAllLostCompRows &&
    lostCompTableData.filter((row: any) => !row.__isTotal).length > LOST_COMP_VISIBLE_ROWS;

  const shouldScrollSkuInfoTable = tableData.length > INPUT_COST_VISIBLE_ROWS;

  const shouldScrollWarehouseTable =
    warehouseData.length > INPUT_COST_VISIBLE_ROWS;

  return (
    <div ref={pageTopRef} className="space-y-3 relative">
      <div ref={tabTopRef} />
      <style>{`
        div { font-family: 'Lato', sans-serif; }
        .gross-margin-positive { color: #5EA68E; font-weight: bold; }
        .gross-margin-negative { color: #E7000B; font-weight: bold; }
        .gross-margin-na { color: #6c757d; font-style: italic; }
      `}</style>

      <div className="sticky top-0 z-40 w-full flex flex-col bg-[#F7F7F7] sm:flex-row md:items-center md:justify-between gap-4">
        <div className="flex flex-col leading-tight w-full md:w-auto">
          <div className="flex items-baseline gap-2">
            <PageBreadcrumb
              pageTitle="Current Inventory - Amazon"
              variant="page"
              align="left"
              textSize="2xl"
            />

            <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
              {/* Amazon{" "} */}
              {countryName?.toLowerCase() === "global"
                ? "Global"
                : countryName?.toUpperCase()}
            </span>
          </div>

          <p className="text-xs 2xl:text-sm text-charcoal-500 mt-1">
            Track your inventory
          </p>
        </div>

        <div className="flex w-full mb-2 sm:mb-0 md:w-auto justify-start md:justify-end">
          {activeTab === "sku-info" || activeTab === "extra" ? (
            <div className="flex flex-wrap items-center gap-3">
              {activeTab === "sku-info" ? (
                <>
                  {isEditing && (
                    <button
                      className="ml-auto cursor-pointer rounded-[5px] bg-[#2c3e50] px-4 py-2 font-['Lato'] text-[clamp(12px,0.729vw,16px)] font-bold text-[#f8edcf] hover:bg-[#34495e]"
                      onClick={saveChanges}
                    >
                      Save Changes
                    </button>
                  )}

                  <button
                    className="ml-auto cursor-pointer rounded-[5px] bg-[#2c3e50] px-4 py-2 font-['Lato'] text-[clamp(12px,0.729vw,16px)] font-bold text-[#f8edcf] hover:bg-[#34495e]"
                    onClick={() => setShowMultiuseCountry(true)}
                    disabled={isNA}
                  >
                    Upload File
                  </button>

                  <DownloadIconButton
                    onClick={handleDownloadXLSX}
                    size="md"
                    disabled={isNA}
                  />
                </>
              ) : (
                <>
                  <button
                    className="ml-auto cursor-pointer rounded-[5px] bg-[#2c3e50] px-4 py-2 font-['Lato'] text-[clamp(12px,0.729vw,16px)] font-bold text-[#f8edcf] hover:bg-[#34495e]"
                    onClick={() => setShowWarehouseUpload(true)}
                    disabled={isNA}
                  >
                    Upload File
                  </button>

                  <DownloadIconButton
                    onClick={handleWarehouseDownload}
                    size="md"
                    disabled={isNA || warehouseData.length === 0}
                  />
                </>
              )}
            </div>
          ) : (
            <div className="flex flex-wrap items-center gap-3">
              <PeriodFiltersTable
                range={range}
                selectedMonth={selectedMonth}
                selectedQuarter={selectedQuarter}
                selectedYear={selectedYear}
                yearOptions={yearOptions}
                onRangeChange={handleRangeChange}
                onMonthChange={handleMonthChange}
                onQuarterChange={handleQuarterChange}
                onYearChange={handleYearChange}
                allowedRanges={["monthly", "quarterly", "yearly"]}
              />

              {activeTab === "recon-table" &&
                effectiveReconRows.filter((r) => !isTotalRow(r)).length > 9 && (
                  <button
                    type="button"
                    onClick={() => setShowAllReconRows((prev) => !prev)}
                    title={showAllReconRows ? "Collapse rows" : "Expand all rows"}
                    aria-label={showAllReconRows ? "Collapse rows" : "Expand all rows"}
                    disabled={isNA || reconFetching}
                    className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {showAllReconRows ? (
                      <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                    ) : (
                      <RiExpandDiagonalFill size={18} className="font-extrabold" />
                    )}
                  </button>
                )}

              {activeTab === "lost-compensation" &&
                effectiveLostCompRows.filter((r) => !r?.__isTotal).length > 9 && (
                  <button
                    type="button"
                    onClick={() => setShowAllLostCompRows((prev) => !prev)}
                    title={showAllLostCompRows ? "Collapse rows" : "Expand all rows"}
                    aria-label={showAllLostCompRows ? "Collapse rows" : "Expand all rows"}
                    disabled={isNA || lostCompLoading}
                    className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {showAllLostCompRows ? (
                      <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                    ) : (
                      <RiExpandDiagonalFill size={18} className="font-extrabold" />
                    )}
                  </button>
                )}
            </div>
          )}
        </div>
      </div>

      <div className="sticky max-[480px]:top-[97px] max-[640px]:top-[97px] sm:top-[48px] md:top-[48px] 2xl:top-[56px] z-30 bg-[#F7F7F7] border-b border-gray-200 max-[480px]:pb-1 max-[640px]:pb-2 sm:py-2">
        <SegmentedToggle<InputCostTab>
          value={activeTab}
          onChange={(nextTab) => {
            shouldScrollTabTopRef.current = true;

            setActiveTab(nextTab);

            if (typeof window !== "undefined") {
              const nextUrl = `${window.location.pathname}#${nextTab}`;

              // Only update URL. Do not dispatch page-hash-navigate,
              // because that can trigger hash scroll behavior.
              window.history.replaceState(null, "", nextUrl);
            }
          }}
          options={tabOptions}
          compact
          className="w-full"
          textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
        />
      </div>

      <PreviewLockedSection
        enabled={isNA}
        title="Preview Mode"
        description="To view your real business data and analytics, please complete your profile and connect your Amazon account. This will unlock your performance dashboard and insights."
        buttonText="Complete Setup"
        onAction={handleConnectAmazonPreview}
      >
        <>

          {activeTab === 'inventory-insights' && (
            <div className="mt-5">
              {inventoryInsightsLoading ? (
                <div className="rounded-xl border border-slate-200 bg-white min-h-[420px] flex items-center justify-center">
                  <Loader transparent />
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

                    {countryName === "global" && (
                      <SegmentedToggle
                        value={selectedGlobalInventoryCountry}
                        onChange={(val) =>
                          setSelectedGlobalInventoryCountry(String(val) as "uk" | "us")
                        }
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
                    onTrendBucketChange={setSelectedAgeingTrendBucket}
                    actions={inventoryInsightsData.actions}
                    actionLogic={inventoryInsightsData.actionLogic}
                    onHeatmapProductClick={handleHeatmapProductClick}
                    showInventoryAlerts={false}
                    inventoryAgeSummary={inventoryInsightsData.inventoryAgeSummary}
                    showHeatmapExcelDownload={true}
                    heatmapExcelFilename={getInventoryInsightsFileName()}
                    heatmapExcelTitleLine="Inventory Insights Report"
                    // heatmapExcelCountryLabel={formatCountryLabel(countryName)}
                    heatmapExcelCountryLabel={formatCountryLabel(
                      inventoryInsightsReportCountry
                    )}
                    heatmapExcelPlatformLabel="Phormula"
                    heatmapExcelPeriodLabel={getInventoryInsightsPeriodLabel()}
                    heatmapExcelCompanyName={userData?.company_name || ""}
                    heatmapExcelBrandName={userData?.brand_name || ""}
                    salesLast30DaysLabel={salesLast30DaysLabel}
                    unitSalesDataKey={inventoryHeatmapUnitSalesDataKey}
                    useCurrentInventoryTableLayout={showUsCurrentInventoryTable}
                    storageCostCurrencySymbol={getCurrencySymbol(
                      getCurrencyForCountry(inventoryInsightsReportCountry)
                    )}
                  />
                </>
              ) : (
                <div className="rounded-xl border border-slate-200 bg-white p-6 text-sm text-charcoal-500">
                  No inventory insights found for the selected period.
                </div>
              )}
            </div>
          )}
          {activeTab === 'sku-info' && (
            <>
              {loading ? (
                <div className="mt-5 rounded-xl border border-slate-200 bg-white min-h-[420px] flex items-center justify-center">
                  <Loader transparent />
                </div>
              ) : skuData.length > 0 ? (
                <div className="mt-5">
                  <DataTable<TableRow>
                    columns={columns}
                    data={tableData}
                    loading={false}
                    paginate={false}
                    pageSize={10}
                    stickyHeader={true}
                    zebra={true}
                    scrollY={false}
                    maxHeight="none"
                    bodyMaxHeight={
                      shouldScrollSkuInfoTable
                        ? INPUT_COST_ROW_HEIGHT * INPUT_COST_VISIBLE_ROWS
                        : undefined
                    }
                    emptyMessage="No data available"
                    tableClassName="text-sm"
                    className="rounded-xl"
                  />
                </div>
              ) : (
                <p className="mt-5">No data available</p>
              )}
            </>
          )}

          {activeTab === 'extra' && (
            <div className="mt-5">
              {warehouseLoading ? (
                <div className="rounded-xl border border-slate-200 bg-white min-h-[320px] flex items-center justify-center">
                  <Loader transparent />
                </div>
              ) : warehouseData.length > 0 ? (
                <DataTable<Record<string, any>>
                  columns={warehouseTableColumns}
                  data={warehouseData}
                  paginate={false}
                  pageSize={10}
                  stickyHeader
                  scrollY={false}
                  maxHeight="none"
                  bodyMaxHeight={
                    shouldScrollWarehouseTable
                      ? INPUT_COST_ROW_HEIGHT * INPUT_COST_VISIBLE_ROWS
                      : undefined
                  }
                  emptyMessage="No warehouse data available"
                  tableClassName="text-sm"
                  className="rounded-xl"
                />
              ) : (
                <div className="rounded-xl border border-slate-200 bg-white p-6 text-sm text-charcoal-500">
                  Upload a warehouse file to view data here.
                </div>
              )}
            </div>
          )}
          {activeTab === 'recon-table' && (
            <div
              className={[
                "mt-5 w-full rounded-xl border border-gray-200 bg-white",
                "overflow-x-auto",
                "[-webkit-overflow-scrolling:touch]",
              ].join(" ")}
            >
              {reconFetching || !reconLoadedOnce ? (
                <div className="flex min-h-[220px] w-full items-center justify-center rounded-lg bg-white">
                  <Loader transparent />
                </div>
              ) : effectiveReconRows.length === 0 ? (
                <div className="p-6 text-sm text-neutral-600">
                  No data available
                </div>
              ) : (
                <GroupedCollapsibleTable
                  rows={effectiveReconRows}
                  getRowKey={(r, idx) => r?.id ?? r?.msku ?? idx}
                  leftCols={leftCols}
                  groups={groups}
                  singleCols={singleCols}
                  getValue={getReconValue}
                  getRowClassName={getReconRowClassName}
                  onAnyGroupExpandedChange={handleAnyGroupExpandedChange}
                  isTotalRow={isTotalRow}
                  bodyMaxHeight={
                    shouldScrollReconTable
                      ? TABLE_ROW_HEIGHT * RECON_VISIBLE_ROWS
                      : undefined
                  }
                  tableClassName={
                    anyExpanded
                      ? "min-w-[900px] w-full table-auto border-collapse bg-white text-[#414042] text-xs 2xl:text-sm"
                      : "w-full table-fixed border-collapse bg-white text-[#414042] text-xs 2xl:text-sm"
                  }
                  // headerRow1ClassName="bg-[#5EA68E] text-[#f8edcf]"
                  // headerRow2ClassName="bg-[#5EA68E] text-[#f8edcf]"
                  showSignRowInBody
                  getSignForCol={getSignForCol}
                />
              )}
            </div>
          )}
          {activeTab === 'lost-compensation' && (
            <div className="mt-5">
              {lostCompLoading ? (
                <div className="flex min-h-[220px] w-full items-center justify-center rounded-lg bg-white">
                  <Loader transparent />
                </div>
              ) : (
                <DataTable<Record<string, React.ReactNode>>
                  columns={lostCompTableColumns}
                  data={lostCompTableData}
                  loading={false}
                  paginate={false}
                  stickyHeader
                  scrollY={false}
                  maxHeight="none"
                  emptyMessage="No data available"
                  tableClassName="text-xs 2xl:text-sm"
                  className="rounded-lg"
                  isTotalRow={(row) => !!(row as any).__isTotal}
                  bodyMaxHeight={
                    shouldScrollLostCompTable
                      ? TABLE_ROW_HEIGHT * LOST_COMP_VISIBLE_ROWS
                      : undefined
                  }
                  rowClassName={(row) =>
                    (row as any).__isTotal
                      ? "bg-[#EFEFEF] font-semibold"
                      : (row as any).__isOthers
                        ? ""
                        : ""
                  }
                />
              )}
            </div>
          )}
        </>
      </PreviewLockedSection>

      {showWarehouseUpload && (
        <div
          onClick={() => setShowWarehouseUpload(false)}
          className="fixed inset-0 z-[10001] flex items-center justify-center bg-black/50 p-4"
        >
          <div
            onClick={(e) => e.stopPropagation()}
            className="relative m-4 w-full max-w-[500px] rounded-xl border border-[#D9D9D9] bg-white shadow-[6px_6px_7px_0px_#00000026]"
          >
            <button
              onClick={() => setShowWarehouseUpload(false)}
              type="button"
              className="absolute right-4 top-3 z-10 text-2xl leading-none text-neutral-500 hover:text-neutral-800"
            >
              &times;
            </button>

            <div className="relative w-full rounded-xl bg-white/30 p-4 no-scrollbar lg:p-9">
              <WarehouseMultiCountryUpload
                countryName={countryName}
                onClose={() => setShowWarehouseUpload(false)}
                onComplete={() => {
                  setShowWarehouseUpload(false);
                  void fetchWarehouseData();
                  setModalMessage("Warehouse file uploaded successfully");
                  setShowModal(true);
                }}
              />
            </div>
          </div>
        </div>
      )}

      {showMultiuseCountry && (
        <div
          onClick={() => setShowMultiuseCountry(false)}
          className="fixed inset-0 z-[10001] flex items-center justify-center bg-black/50 p-4"
        >
          <div
            onClick={(e) => e.stopPropagation()}
            className="relative m-4 w-full max-w-[500px] rounded-xl border border-[#D9D9D9] bg-white shadow-[6px_6px_7px_0px_#00000026]"
          >
            <button
              onClick={() => setShowMultiuseCountry(false)}
              type="button"
              className="absolute right-4 top-3 z-10 text-2xl leading-none text-neutral-500 hover:text-neutral-800"
            >
              &times;
            </button>

            <div className="relative w-full rounded-xl bg-white/30 p-4 no-scrollbar lg:p-9">
              <SkuMultiuseCountryUpload
                onClose={() => setShowMultiuseCountry(false)}
                onComplete={() => {
                  setShowMultiuseCountry(false);
                  void fetchSkuData();
                }}
              />
            </div>
          </div>
        </div>
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
        drawerPeriodText={drawerPeriodText}
        currencySymbol={getCurrencySymbol(getCurrencyForCountry(countryName))}
        bestPerformanceLoading={aiBestPerformanceLoading}
        bestPerformanceError={aiBestPerformanceError}
        bestPerformanceData={aiBestPerformanceData}
        sharedInsightData={{
          blocks: aiProductBlocks,
          objective: aiPanel?.objective ?? null,
          recommendationsMap: aiPanel?.recommendationsMap,
          drawerPeriodText,
          nameToSkuMap: inputCostNameToSkuMap,
        }}
      />

      <Modalmsg
        show={showModal}
        message={modalMessage}
        onClose={() => setShowModal(false)}
        onCancel={() => setShowModal(false)}
      />
    </div>
  );
}
