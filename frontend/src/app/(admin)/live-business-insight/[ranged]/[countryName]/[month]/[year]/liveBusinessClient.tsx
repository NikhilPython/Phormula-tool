'use client';

import React, { useState, useEffect, useMemo } from 'react';
import axios from 'axios';
import * as XLSX from "xlsx-js-style";
import { FaThumbsUp, FaThumbsDown } from 'react-icons/fa';
import { IoDownload } from 'react-icons/io5';
import { BsStars } from 'react-icons/bs';
import { FaArrowUp, FaArrowDown } from 'react-icons/fa';
import Loader from '@/components/loader/Loader';
import DataTable, { ColumnDef } from '@/components/ui/table/DataTable';
import DownloadIconButton from '@/components/ui/button/DownloadIconButton';
import SegmentedToggle from '@/components/ui/SegmentedToggle';
import { AiButton } from '@/components/ui/button/AiButton';
import PageBreadcrumb from '@/components/common/PageBreadCrumb';
import { useGetUserDataQuery } from '@/lib/api/profileApi';
import { motion } from "framer-motion";
import SkuRecommendationDrawer from '@/components/dashboard/SkuRecommendationDrawer';
import InventoryInsightsSection from "@/components/businessInsight/InventoryInsightsSection";
import {
  exportSkuAnalysisMtdExcel
} from "@/lib/excel/exportCurrentInventoryExcel";

type CurrencyCode = "USD" | "GBP" | "INR" | "CAD";

const GBP_TO_USD_ENV = Number(process.env.NEXT_PUBLIC_GBP_TO_USD || "1.25");
const INR_TO_USD_ENV = Number(process.env.NEXT_PUBLIC_INR_TO_USD || "0.01128");
const CAD_TO_USD_ENV = Number(process.env.NEXT_PUBLIC_CAD_TO_USD || "0.74");

const currencyForCountry = (countryName: string): CurrencyCode => {
  const c = (countryName || "").toLowerCase();
  if (c === "uk") return "GBP";
  if (c === "us") return "USD";
  if (c === "ca") return "CAD";
  if (c === "india") return "INR";
  return "USD";
};

const toNumberSafe = (v: any) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

type MonthsforBIProps = {
  countryName: string;
  sourceCountryName?: string;
  ranged: string;
  month: string;
  year: string;
  initialData?: ApiResponse | null;
  disableAutoFetch?: boolean;
  onGenerateInsights?: () => Promise<void>;

  // ✅ NEW: optional global live MTD params
  asOf?: string;      // "2026-05-11"
  startDay?: number; // 1
  endDay?: number;   // 11
};

// =========================
// Types/Interfaces
// =========================

interface GrowthCategory {
  category: string;
  value: number;
}

interface SkuItem {
  product_name: string;
  sku?: string;
  'Sales Mix (Month2)'?: number;
  quantity?: number;
  asp?: number;
  net_sales?: number;
  sales_mix?: number;
  unit_wise_profitability?: number;
  profit?: number;
  quantity_prev?: number;
  quantity_curr?: number;
  asp_prev?: number;
  asp_curr?: number;
  net_sales_prev?: number;
  net_sales_curr?: number;
  sales_mix_prev?: number;
  sales_mix_curr?: number;
  unit_wise_profitability_prev?: number;
  unit_wise_profitability_curr?: number;
  profit_prev?: number;
  profit_curr?: number;
  quantity_month1?: number;
  quantity_month2?: number;
  asp_month1?: number;
  asp_month2?: number;
  net_sales_month1?: number;
  net_sales_month2?: number;
  sales_mix_month1?: number;
  sales_mix_month2?: number;
  unit_wise_profitability_month1?: number;
  unit_wise_profitability_month2?: number;
  profit_month1?: number;
  profit_month2?: number;
  product_sales_prev?: number;
  product_sales_curr?: number;
  product_sales_month1?: number;
  product_sales_month2?: number;
  profit_percentage_month1?: number;
  profit_percentage_month2?: number;
  'Gross Sales Growth (%)'?: {
    category: string;
    value: number;
  };
  [key: string]: any;
}

interface CategorizedGrowth {
  top_80_skus: SkuItem[];

  // keep combined for existing export/chart/backward compatibility
  new_or_reviving_skus: SkuItem[];

  // new separated groups from backend
  new_skus: SkuItem[];
  reviving_skus: SkuItem[];

  other_skus: SkuItem[];

  top_80_total?: SkuItem | null;
  new_or_reviving_total?: SkuItem | null;

  new_skus_total?: SkuItem | number | null;
  reviving_skus_total?: SkuItem | number | null;

  other_total?: SkuItem | null;
  all_skus_total?: SkuItem | null;
}

interface SkuInsight {
  product_name: string;
  sku?: string;
  insight?: string;

  recommendation?: string | string[] | number;
  product_journey?: string[] | string;
  advertising?: string | string[];
  inventory_recommendation?: string | string[] | number;

  [key: string]: any;
}

interface PeriodInfo {
  label: string;
  start_date?: string;
  end_date?: string;
  start?: string;
  end?: string;
}

interface ApiResponse {
  message?: string;
  periods?: {
    previous?: PeriodInfo;
    current_mtd?: PeriodInfo;
  };
  categorized_growth?: CategorizedGrowth;
  insights?: Record<string, SkuInsight>;
  ai_insights?: Record<string, SkuInsight>;
  overall_summary?: {
    summary_text: string;
    metric_bullets: string[];
  };
  objective_context?: {
    growth_intent?: string;
    inventory_clearance_priority?: boolean;
    profit_priority?: string;
    inventory_summary?: {
      alert_bullets?: string[];
      summary_text?: string;
    };
    ads_recommendation?: string;
    journey_summary?: string[];
    recommendation?: string;
  };
  ads_recommendation?: string;
  inventory_summary?: {
    alert_bullets?: string[];
    summary_text?: string;
  };
  overall_actions?: string[] | Record<string, string>;
  recommended_actions_mtd?: Record<string, any>;
  portfolio_inventory_block?: string | number | Record<string, string>;
  product_journey?: Record<string, any>;
  portfolio_inventory_alerts?: Record<string, any>;
  remaining_skus_recommendation?: string | number;
  remaining_skus_block?: string | number;
  portfolio_recommendation?: string | number;
}

// =========================
// Config
// =========================
const API_BASE = `${process.env.NEXT_PUBLIC_API_BASE_URL}`;

// const STORAGE_KEY = 'live_bi_insight_data';
// const INSIGHTS_KEY = 'live_bi_sku_insights';

// Axios instance with JWT
const api = axios.create({ baseURL: API_BASE });
api.interceptors.request.use((cfg) => {
  const t =
    typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
  if (t) cfg.headers.Authorization = `Bearer ${t}`;
  return cfg;
});

// =========================
// Small helpers
// =========================

const getShortPeriodLabel = (label?: string) =>
  label ? label.split(' ')[0] || label : '';

const getTodayKey = (): string => {
  const today = new Date();
  const yyyy = today.getFullYear();
  const mm = String(today.getMonth() + 1).padStart(2, '0');
  const dd = String(today.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
};

const capitalizeWords = (value: string) =>
  (value || "")
    .toString()
    .toLowerCase()
    .replace(/\b\w/g, (char) => char.toUpperCase());

const normalizeTextBlock = (value: unknown): string => {
  if (typeof value === "string") return value;
  if (typeof value === "number") return String(value);

  // ✅ Global returns { uk: "...", us: "..." }
  if (value && typeof value === "object") {
    return Object.entries(value as Record<string, unknown>)
      .map(([country, text]) => {
        if (!text) return "";
        return `## ${country.toUpperCase()}\n${String(text)}`;
      })
      .filter(Boolean)
      .join("\n\n");
  }

  return "";
};

const isOthersCardName = (name: string) => {
  const value = String(name || "").trim().toLowerCase();
  return (
    value === "others" ||
    value === "other skus" ||
    value === "other sku" ||
    value === "other"
  );
};

const getLiveMtdParams = ({
  isGlobal,
  asOf,
  startDay,
  endDay,
}: {
  isGlobal: boolean;
  asOf?: string;
  startDay?: number;
  endDay?: number;
}) => {
  if (!isGlobal) return null;

  const finalAsOf = asOf || getTodayKey();
  const parsedDay = Number(finalAsOf.split("-")[2]);

  return {
    as_of: finalAsOf,
    start_day: startDay ?? 1,
    end_day: endDay ?? parsedDay,
  };
};

// =========================
// Main Component
// =========================

export default function LiveBusinessClient({
  countryName,
  sourceCountryName,
  ranged,
  month,
  year,
  initialData,
  disableAutoFetch = false,
  onGenerateInsights,
  asOf,
  startDay,
  endDay,
}: MonthsforBIProps) {
  const { data: userData } = useGetUserDataQuery();


  const [gbpToUsd, setGbpToUsd] = useState(GBP_TO_USD_ENV);
  const [inrToUsd, setInrToUsd] = useState(INR_TO_USD_ENV);
  const [cadToUsd, setCadToUsd] = useState(CAD_TO_USD_ENV);

  type CurrencyRateRow = {
    conversion_rate: number;
    country: string;
    month: string;
    selected_currency: string;
    user_currency: string;
    year: number;
  };

  const FX_RATES_GET_ENDPOINT = `${API_BASE}/currency-rates`;

  const normalizeCurrency = (v: string) => String(v || "").trim().toLowerCase();
  const normalizeMonth = (v: string) => String(v || "").trim().toLowerCase();
  const normalizeYear = (v: string | number) => Number(v);

  const fetchFxRates = async () => {
    try {
      const token =
        typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

      const headers: HeadersInit = { Accept: "application/json" };
      if (token) (headers as any).Authorization = `Bearer ${token}`;

      const res = await fetch(FX_RATES_GET_ENDPOINT, { method: "GET", headers });
      if (!res.ok) throw new Error(`FX rates fetch failed: ${res.status}`);

      const data = await res.json();
      const rows: CurrencyRateRow[] = Array.isArray(data) ? data : data?.data || [];

      const currentMonth = normalizeMonth(month);
      const currentYear = normalizeYear(year);

      const currentRows = rows.filter(
        (r) =>
          normalizeMonth(r.month) === currentMonth &&
          normalizeYear(r.year) === currentYear
      );

      const getDirectRate = (from: string, to: string) => {
        const fromCur = normalizeCurrency(from);
        const toCur = normalizeCurrency(to);

        const row = currentRows.find(
          (r) =>
            normalizeCurrency(r.user_currency) === fromCur &&
            normalizeCurrency(r.selected_currency) === toCur
        );

        const rate = Number(row?.conversion_rate);
        return Number.isFinite(rate) && rate > 0 ? rate : null;
      };

      const getInverseRate = (from: string, to: string) => {
        const inverse = getDirectRate(to, from);
        if (!inverse || inverse <= 0) return null;
        return 1 / inverse;
      };

      const getRate = (from: string, to: string) => {
        return getDirectRate(from, to) ?? getInverseRate(from, to);
      };

      const gbpUsd = getRate("gbp", "usd");
      const inrUsd = getRate("inr", "usd");
      const cadUsd = getRate("cad", "usd");

      if (gbpUsd != null) setGbpToUsd(gbpUsd);
      if (inrUsd != null) setInrToUsd(inrUsd);
      if (cadUsd != null) setCadToUsd(cadUsd);
    } catch (err) {
      console.error("Failed to fetch FX from DB, keeping env defaults", err);
    }
  };

  useEffect(() => {
    fetchFxRates();
  }, [month, year]);

  const profileHomeCurrency = ((userData?.homeCurrency || "USD").toUpperCase() as CurrencyCode);

  const sourceCurrency: CurrencyCode = useMemo(() => {
    return currencyForCountry(sourceCountryName || countryName);
  }, [sourceCountryName, countryName]);

  const displayCurrency: CurrencyCode = useMemo(() => {
    const c = (countryName || "").toLowerCase();

    if (c === "global") return profileHomeCurrency;
    if (c === "uk") return "GBP";
    if (c === "us") return "USD";
    if (c === "ca") return "CAD";

    return profileHomeCurrency;
  }, [countryName, profileHomeCurrency]);

  const convertToDisplayCurrency = useMemo(() => {
    return (value: number | null | undefined, from: CurrencyCode) => {
      const n = toNumberSafe(value ?? 0);
      if (!n) return 0;

      let usd = n;
      if (from === "GBP") usd = n * gbpToUsd;
      if (from === "INR") usd = n * inrToUsd;
      if (from === "CAD") usd = n * cadToUsd;

      if (displayCurrency === "USD") return usd;
      if (displayCurrency === "GBP") return gbpToUsd ? usd / gbpToUsd : usd;
      if (displayCurrency === "INR") return inrToUsd ? usd / inrToUsd : usd;
      if (displayCurrency === "CAD") return cadToUsd ? usd / cadToUsd : usd;

      return usd;
    };
  }, [displayCurrency, gbpToUsd, inrToUsd, cadToUsd]);

  const formatDisplayAmount = useMemo(() => {
    return (value: number | null | undefined) => {
      const n = toNumberSafe(value ?? 0);

      switch (displayCurrency) {
        case "USD":
          return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(n);
        case "GBP":
          return new Intl.NumberFormat("en-GB", { style: "currency", currency: "GBP" }).format(n);
        case "CAD":
          return new Intl.NumberFormat("en-CA", { style: "currency", currency: "CAD" }).format(n);
        case "INR":
          return new Intl.NumberFormat("en-IN", { style: "currency", currency: "INR" }).format(n);
        default:
          return n.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          });
      }
    };
  }, [displayCurrency]);

  const detectCurrencyFromMetric = (raw: string): CurrencyCode => {
    const v = String(raw || "").trim().toUpperCase();

    if (v.startsWith("C$") || v.startsWith("CAD")) return "CAD";
    if (v.startsWith("£") || v.startsWith("GBP")) return "GBP";
    if (v.startsWith("₹") || v.startsWith("INR")) return "INR";
    if (v.startsWith("$") || v.startsWith("USD")) return "USD";

    return sourceCurrency; // fallback
  };

  const convertMetricValueString = useMemo(() => {
    return (rawValue: string, label: string) => {
      const value = String(rawValue || "").trim();
      if (!value) return value;

      if (label.toLowerCase() === "units") return value;

      const match = value.match(
        /^((?:C\$|£|\$|₹|USD|GBP|CAD|INR)?\s*[-+]?[0-9,]*\.?[0-9]+)\s*(\(.+\))?$/i
      );
      if (!match) return value;

      const mainPart = match[1] || "";
      const percentPart = match[2] || "";

      const numeric = Number(mainPart.replace(/[^\d.-]/g, ""));
      if (!Number.isFinite(numeric)) return value;

      const fromCurrency = detectCurrencyFromMetric(mainPart);
      const converted = convertToDisplayCurrency(numeric, fromCurrency);
      const formatted = formatDisplayAmount(converted);

      return `${formatted}${percentPart ? ` ${percentPart}` : ""}`;
    };
  }, [convertToDisplayCurrency, formatDisplayAmount, sourceCurrency]);

  const [categorizedGrowth, setCategorizedGrowth] = useState<CategorizedGrowth>({
    top_80_skus: [],
    new_or_reviving_skus: [],
    new_skus: [],
    reviving_skus: [],
    other_skus: [],

    top_80_total: null,
    new_or_reviving_total: null,
    new_skus_total: null,
    reviving_skus_total: null,
    other_total: null,
    all_skus_total: null,
  });

  const getCurrencySymbolForExcel = () => {
    return currencyCodeToSymbol(displayCurrency);
  };

  const titleCountry = useMemo(() => {
    const c = (countryName || "").toLowerCase();
    if (c === "global") return "Global";
    return c.toUpperCase(); // UK / US / CA
  }, [countryName]);

  const titleMonth = useMemo(() => {
    // parent sends "january", "february" etc
    const abbr = (month || "").slice(0, 3);
    const yy = String(year || "").slice(2);
    return `${abbr.charAt(0).toUpperCase()}${abbr.slice(1)}'${yy}`; // Jan'26
  }, [month, year]);

  type TabKey =
    | "all_skus"
    | "top_80_skus"
    | "new_skus"
    | "reviving_skus"
    | "other_skus";

  const [activeTab, setActiveTab] = useState<TabKey>("all_skus");

  const tabOptions = useMemo(
    () => [
      { value: "all_skus" as const, label: "All SKUs" },
      { value: "top_80_skus" as const, label: "Top 80% SKUs" },
      { value: "new_skus" as const, label: "New SKUs" },
      { value: "reviving_skus" as const, label: "Reviving SKUs" },
      { value: "other_skus" as const, label: "Other SKUs" },
    ],
    []
  );

  const handleTabChange = (val: TabKey) => setActiveTab(val);


  const normalizedCountry = (countryName || '').toLowerCase();
  const [periods, setPeriods] = useState<ApiResponse['periods'] | null>(null);
  const [month2Label, setMonth2Label] = useState<string>('');
  const [error, setError] = useState<string | null>(null);
  const [portfolioRecommendation, setPortfolioRecommendation] = useState<string>("");

  // overall bullets from backend
  const [summaryText, setSummaryText] = useState<string>("");
  const [overallSummary, setOverallSummary] = useState<string[]>([]);

  const [overallActions, setOverallActions] = useState<any[]>([]);
  const [recommendedActions, setRecommendedActions] = useState<Record<string, string>>({});
  const [remainingSkusBlock, setRemainingSkusBlock] = useState<string>("");

  const [insightDate, setInsightDate] = useState<string | null>(null);

  // Insights + modal
  const [loadingInsight, setLoadingInsight] = useState<boolean>(false);
  const [skuInsights, setSkuInsights] = useState<Record<string, SkuInsight>>(
    {}
  );
  const [selectedSku, setSelectedSku] = useState<string | null>(null);
  const [modalOpen, setModalOpen] = useState<boolean>(false);
  const [adsRecommendation, setAdsRecommendation] = useState<string>("");
  const [inventorySummary, setInventorySummary] = useState<any>(null);
  const [selectedSkuItem, setSelectedSkuItem] = useState<SkuItem | null>(null);
  const [portfolioInventoryBlock, setPortfolioInventoryBlock] = useState<string>("");
  // Feedback
  const [fbType, setFbType] = useState<'like' | 'dislike' | null>(null);
  const [fbText, setFbText] = useState<string>('');
  const [fbSubmitting, setFbSubmitting] = useState<boolean>(false);
  const [fbSuccess, setFbSuccess] = useState<boolean>(false);
  const [objectiveContext, setObjectiveContext] = useState<{
    growth_intent?: string;
    inventory_clearance_priority?: boolean;
    profit_priority?: string;
  } | null>(null);

  const [pageLoading, setPageLoading] = useState<boolean>(true);
  const [recDrawerOpen, setRecDrawerOpen] = useState(false);
  const [selectedRec, setSelectedRec] = useState<{
    productName: string;
    metrics: { label: string; value: string; color?: string }[];
    journeyPoints: string[];
    recommendationPoints: string[];
    advertisingPoints?: string[];
    inventoryPoints?: string[];
    showChart?: boolean; // ✅ NEW
  } | null>(null);

  const isGlobalData = () => normalizedCountry === 'global';

  const getMonthYearFromLabel = (label?: string) => {
    if (!label) return { month: '', year: '' };
    const parts = label.split(' ');
    return {
      month: parts[0] ?? '',
      year: parts[1] ?? '',
    };
  };

  const prevPeriod = getMonthYearFromLabel(periods?.previous?.label);
  const currPeriod = getMonthYearFromLabel(periods?.current_mtd?.label);

  const splitIntoPoints = (para: string) =>
    (para || "")
      .split(/(?<=\.)\s+/)   // sentence split
      .map((s) => s.trim())
      .filter(Boolean);

  const extractSections = (text: string) => {
    const raw = (text || "").trim();

    const getBlock = (label: string, nextLabels: string[]) => {
      const next = nextLabels.map((l) => l.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|");
      const re = new RegExp(`${label}\\s*:\\s*([\\s\\S]*?)(?=\\n\\s*(?:${next})\\s*:|$)`, "i");
      const m = raw.match(re);
      return (m?.[1] || "").trim();
    };

    const journeyText = getBlock("Product\\s*Journey", ["Recommendation", "Advertising", "Inventory"]);
    const recText = getBlock("Recommendation", ["Advertising", "Inventory", "Product\\s*Journey"]);
    const adsText = getBlock("Advertising", ["Inventory", "Recommendation", "Product\\s*Journey"]);
    const invText = getBlock("Inventory", ["Advertising", "Recommendation", "Product\\s*Journey"]);

    return {
      journeyPoints: splitIntoPoints(journeyText),
      recommendationPoints: splitIntoPoints(recText),
      advertisingPoints: splitIntoPoints(adsText),
      inventoryPoints: splitIntoPoints(invText),
    };
  };



  const fmtPct = (v?: any) => {
    const n = Number(v);
    if (!Number.isFinite(n)) return "";
    const sign = n > 0 ? "+" : "";
    return `${sign}${n.toFixed(2)}%`;
  };

  const buildMetricsForSku = (item: SkuItem) => {
    const m: { label: string; value: string; color?: string }[] = [];

    const getGrowth = (key: string) => {
      const raw = (item as any)?.[key];
      const n = Number(typeof raw === "object" ? raw?.value : raw);
      return Number.isFinite(n) ? n : 0;
    };

    const formatPct = (v: number) => {
      const sign = v > 0 ? "+" : "";
      return `${sign}${v.toFixed(2)}%`;
    };

    const formatValueWithGrowth = (
      actualValue: number,
      growthValue: number,
      type: "money" | "number" = "money"
    ) => {
      const mainValue =
        type === "number"
          ? Number(actualValue || 0).toLocaleString()
          : formatDisplayAmount(
            convertToDisplayCurrency(Number(actualValue || 0), sourceCurrency)
          );

      return `${mainValue} (${formatPct(growthValue)})`;
    };

    m.push({
      label: "Units",
      value: formatValueWithGrowth(
        Number(
          (item as any).quantity_month2 ??
          (item as any).quantity_curr ??
          item.quantity ??
          0
        ),
        getGrowth("Unit Growth"),
        "number"
      ),
      color: getGrowth("Unit Growth") < 0 ? "#FF5C5C" : "#5EA68E",
    });

    m.push({
      label: "Net sales",
      value: formatValueWithGrowth(
        Number(
          (item as any).net_sales_month2 ??
          (item as any).net_sales_curr ??
          item.net_sales ??
          0
        ),
        getGrowth("Sales Growth") || getGrowth("Net Sales Growth"),
        "money"
      ),
      color:
        (getGrowth("Sales Growth") || getGrowth("Net Sales Growth")) < 0
          ? "#FF5C5C"
          : "#5EA68E",
    });

    m.push({
      label: "ASP",
      value: formatValueWithGrowth(
        Number(
          (item as any).asp_month2 ??
          (item as any).asp_curr ??
          item.asp ??
          0
        ),
        getGrowth("ASP Growth"),
        "money"
      ),
      color: getGrowth("ASP Growth") < 0 ? "#FF5C5C" : "#5EA68E",
    });

    m.push({
      label: "CM1 profit",
      value: formatValueWithGrowth(
        Number(
          (item as any).profit_month2 ??
          (item as any).profit_curr ??
          item.profit ??
          0
        ),
        getGrowth("CM1 Profit Impact"),
        "money"
      ),
      color: getGrowth("CM1 Profit Impact") < 0 ? "#FF5C5C" : "#5EA68E",
    });

    m.push({
      label: "CM1 profit per unit",
      value: formatValueWithGrowth(
        Number(
          (item as any).unit_wise_profitability_month2 ??
          (item as any).unit_wise_profitability_curr ??
          item.unit_wise_profitability ??
          0
        ),
        getGrowth("Profit Per Unit"),
        "money"
      ),
      color: getGrowth("Profit Per Unit") < 0 ? "#FF5C5C" : "#5EA68E",
    });

    return m;
  };

  const toPoints = (value: unknown): string[] => {
    if (!value) return [];

    if (Array.isArray(value)) {
      return value
        .map((v) => String(v || "").trim())
        .filter(Boolean);
    }

    return String(value)
      .split(/\r?\n|(?<=\.)\s+/)
      .map((v) => v.trim())
      .filter(Boolean);
  };

  const getCountryActionFromGlobalJourney = (
    journey: any,
    country: "uk" | "us" | "ca" | "india"
  ) => {
    const blocks = journey?.[country] || {};
    return Object.values(blocks || {})[0] as any;
  };

  const buildSelectedRecFromSkuInsight = (
    item: SkuItem,
    insight?: SkuInsight | null
  ) => {
    const productName = item?.product_name || insight?.product_name || "Details";

    return {
      productName,
      metrics: buildMetricsForSku(item),

      // ✅ Single-country /live_mtd_bi response
      journeyPoints: toPoints(insight?.product_journey),
      recommendationPoints: toPoints(insight?.recommendation),
      advertisingPoints: toPoints(insight?.advertising),
      inventoryPoints: toPoints(insight?.inventory_recommendation),

      showChart: true,
    };
  };

  const buildSelectedRecFromGlobalSkuInsight = (
    item: SkuItem,
    insight?: SkuInsight | null
  ) => {
    const productName = item?.product_name || insight?.product_name || "Details";
    const journey = insight?.raw_global_journey;

    const ukAction = getCountryActionFromGlobalJourney(journey, "uk");
    const usAction = getCountryActionFromGlobalJourney(journey, "us");
    const caAction = getCountryActionFromGlobalJourney(journey, "ca");
    const indiaAction = getCountryActionFromGlobalJourney(journey, "india");

    const countryActions = [
      ["UK", ukAction],
      ["US", usAction],
      ["CA", caAction],
      ["India", indiaAction],
    ] as const;

    return {
      productName,
      metrics: buildMetricsForSku(item),

      // ✅ Global product journey comparison
      journeyPoints: Array.isArray(journey?.journey_comparison)
        ? journey.journey_comparison
        : toPoints(insight?.product_journey),

      // ✅ Countrywise recommendations
      recommendationPoints: countryActions
        .map(([country, action]) =>
          action?.recommendation ? `${country}: ${action.recommendation}` : ""
        )
        .filter(Boolean),

      advertisingPoints: countryActions
        .map(([country, action]) =>
          action?.ads_recommendation ? `${country}: ${action.ads_recommendation}` : ""
        )
        .filter(Boolean),

      inventoryPoints: countryActions
        .map(([country, action]) =>
          action?.inventory_recommendation
            ? `${country}: ${action.inventory_recommendation}`
            : ""
        )
        .filter(Boolean),

      showChart: true,
    };
  };

  const buildSelectedRecFromInsight = (
    item: SkuItem | null,
    insightText: string,
    productName: string
  ) => {
    const sections = extractSections(insightText || "");

    return {
      productName: productName || item?.product_name || "Details",
      metrics: item ? buildMetricsForSku(item) : [],
      journeyPoints: sections.journeyPoints,
      recommendationPoints: sections.recommendationPoints,
      advertisingPoints: sections.advertisingPoints,
      inventoryPoints: sections.inventoryPoints,
      showChart: true,
    };
  };

  const getTabRows = (tab: TabKey): SkuItem[] => {
    if (tab === "all_skus") return getAllSkusForExport();
    if (tab === "new_skus") return categorizedGrowth.new_skus || [];
    if (tab === "reviving_skus") return categorizedGrowth.reviving_skus || [];
    return categorizedGrowth[tab] || [];
  };

  const getTabLabel = (key: TabKey): string =>
    key === "top_80_skus"
      ? "Top 80% SKUs"
      : key === "new_skus"
        ? "New SKUs"
        : key === "reviving_skus"
          ? "Reviving SKUs"
          : key === "other_skus"
            ? "Other SKUs"
            : "All SKUs";

  const getTabNumberForFeedback = (key: TabKey): number =>
    key === "top_80_skus"
      ? 1
      : key === "new_skus"
        ? 2
        : key === "reviving_skus"
          ? 3
          : key === "other_skus"
            ? 4
            : 5;

  // =========================
  // Persistence helpers
  // =========================

  // Normalize backend growth field names -> existing frontend keys
  const normalizeCategorizedGrowth = (raw?: any): CategorizedGrowth => {
    const mapRow = (row: any): SkuItem => {
      const clone: any = { ...row };

      if (row['Sales Mix (Current)'] != null) {
        clone['Sales Mix (Month2)'] = row['Sales Mix (Current)'];
      }

      const fieldMap: Record<string, string> = {
        'Unit Growth (%)': 'Unit Growth',
        'ASP Growth (%)': 'ASP Growth',
        'Net Sales Growth (%)': 'Net Sales Growth',
        'Sales Mix Change (%)': 'Sales Mix Change',
        'Profit Per Unit (%)': 'Profit Per Unit',
        'CM1 Profit Impact (%)': 'CM1 Profit Impact',
      };

      Object.entries(fieldMap).forEach(([backendKey, frontKey]) => {
        if (row[backendKey] != null) clone[frontKey] = row[backendKey];
      });

      // Handles 0 correctly
      if (clone['Net Sales Growth'] != null && clone['Sales Growth'] == null) {
        clone['Sales Growth'] = clone['Net Sales Growth'];
      }

      clone.quantity_month1 = row.quantity_prev ?? null;
      clone.quantity_month2 = row.quantity_curr ?? null;
      clone.asp_month1 = row.asp_prev ?? null;
      clone.asp_month2 = row.asp_curr ?? null;
      clone.net_sales_month1 = row.net_sales_prev ?? null;
      clone.net_sales_month2 = row.net_sales_curr ?? null;
      clone.product_sales_month1 = row.product_sales_prev ?? null;
      clone.product_sales_month2 = row.product_sales_curr ?? null;
      if (row['Gross Sales Growth (%)'] != null) {
        clone['Gross Sales Growth'] = row['Gross Sales Growth (%)'];
      }
      clone.sales_mix_month1 = row.sales_mix_prev ?? null;
      clone.sales_mix_month2 = row.sales_mix_curr ?? row['Sales Mix (Current)'] ?? null;
      clone.unit_wise_profitability_month1 = row.unit_wise_profitability_prev ?? null;
      clone.unit_wise_profitability_month2 = row.unit_wise_profitability_curr ?? null;
      clone.profit_month1 = row.profit_prev ?? null;
      clone.profit_month2 = row.profit_curr ?? null;
      clone.profit_percentage_month1 = row.profit_pct_prev ?? null;
      clone.profit_percentage_month2 = row.profit_pct_curr ?? null;
      return clone;
    };

    const empty: CategorizedGrowth = {
      top_80_skus: [],
      new_or_reviving_skus: [],
      new_skus: [],
      reviving_skus: [],
      other_skus: [],

      top_80_total: null,
      new_or_reviving_total: null,
      new_skus_total: null,
      reviving_skus_total: null,
      other_total: null,
      all_skus_total: null,
    };

    if (!raw) return empty;

    const newSkus = (raw.new_skus || []).map(mapRow);
    const revivingSkus = (raw.reviving_skus || []).map(mapRow);

    const combinedNewReviving =
      raw.new_or_reviving_skus?.length
        ? raw.new_or_reviving_skus.map(mapRow)
        : [...newSkus, ...revivingSkus];

    return {
      top_80_skus: (raw.top_80_skus || []).map(mapRow),

      // combined key remains for export/old logic
      new_or_reviving_skus: combinedNewReviving,

      // separated table tabs
      new_skus: newSkus,
      reviving_skus: revivingSkus,

      other_skus: (raw.other_skus || []).map(mapRow),

      top_80_total: raw.top_80_total ? mapRow(raw.top_80_total) : null,
      new_or_reviving_total: raw.new_or_reviving_total ? mapRow(raw.new_or_reviving_total) : null,

      new_skus_total: raw.new_skus_total ?? null,
      reviving_skus_total: raw.reviving_skus_total ?? null,

      other_total: raw.other_total ? mapRow(raw.other_total) : null,
      all_skus_total: raw.all_skus_total ? mapRow(raw.all_skus_total) : null,
    };
  };

  // const hydrateFromPayload = (payload: ApiResponse) => {
  //   const newPeriods = payload.periods || null;

  //   const rawCat = payload.categorized_growth || {
  //     top_80_skus: [],
  //     new_or_reviving_skus: [],
  //     other_skus: [],
  //   };

  //   const normalized = normalizeCategorizedGrowth(rawCat);
  //   setPeriods(newPeriods);
  //   setCategorizedGrowth(normalized);
  //   const currentLabel = newPeriods?.current_mtd?.label || '';
  //   setMonth2Label(currentLabel);
  //   const summaryObj = payload.overall_summary;
  //   setSummaryText(summaryObj?.summary_text || "");
  //   setOverallSummary(summaryObj?.metric_bullets || []);
  //   setOverallActions(payload.overall_actions || []);
  //   setRecommendedActions(payload.recommended_actions_mtd || {});
  //   setRemainingSkusBlock(payload.remaining_skus_block || payload.remaining_skus_recommendation || "");
  //   setPortfolioRecommendation((payload as any).portfolio_recommendation || "");
  //   setPortfolioInventoryBlock((payload as any).portfolio_inventory_block || "");
  //   setObjectiveContext(payload.objective_context || null);
  //   setAdsRecommendation((payload as any).ads_recommendation || "");
  //   setInventorySummary((payload as any).inventory_summary || null);
  //   const liveInsights = payload.ai_insights || {};
  //   if (liveInsights && Object.keys(liveInsights).length) {
  //     setSkuInsights(liveInsights);
  //     // saveInsightsToStorage(liveInsights);
  //   }
  // };

  const normalizeGlobalCategorizedGrowth = (raw?: any): CategorizedGrowth => {
    return normalizeCategorizedGrowth({
      top_80_skus: raw?.top_80_skus || raw?.top_80_products || [],

      new_skus: raw?.new_skus || [],
      reviving_skus: raw?.reviving_skus || [],
      new_or_reviving_skus: raw?.new_or_reviving_skus || [],

      other_skus: raw?.other_skus || raw?.other_products || [],

      top_80_total: raw?.top_80_total || null,
      new_or_reviving_total: raw?.new_or_reviving_total || null,
      new_skus_total: raw?.new_skus_total ?? null,
      reviving_skus_total: raw?.reviving_skus_total ?? null,
      other_total: raw?.other_total || null,
      all_skus_total: raw?.all_skus_total || null,
    });
  };

  const flattenGlobalRecommendedActions = (recommended?: Record<string, any>) => {
    const flat: Record<string, any> = {};

    Object.entries(recommended || {}).forEach(([country, actions]) => {
      Object.entries(actions || {}).forEach(([sku, action]: [string, any]) => {
        const key = `${country}:${sku}`;

        // Existing UI expects parseRecommendedAction(text), so keep strings as strings.
        // If backend returns object, convert it to a readable block.
        if (typeof action === "string") {
          flat[key] = action;
          return;
        }

        const growth = action?.growth_row || {};
        const productName = growth?.product_name || action?.product_name || sku;

        flat[key] = [
          `Product Journey: ${(action?.journey_summary || []).join(" ")}`,
          `Recommendation: ${action?.recommendation || "Monitor performance"}`,
          `Advertising: ${action?.ads_recommendation || "Monitor current advertising."}`,
          `Inventory: ${action?.inventory_recommendation || "Inventory position is stable."}`,
          `Product: ${productName}`,
        ].join("\n");
      });
    });

    return flat;
  };

  const buildGlobalSkuInsights = (payload: ApiResponse): Record<string, SkuInsight> => {
    const insights: Record<string, SkuInsight> = {};

    Object.entries(payload.product_journey || {}).forEach(([productKey, journey]: [string, any]) => {
      const productName = journey?.product_name || productKey;

      const insightText = [
        `Product Journey: ${(journey?.journey_comparison || []).join(" ")}`,
        `Recommendation: ${extractGlobalRecommendation(journey)}`,
        `Advertising: ${extractGlobalAdvertising(journey)}`,
        `Inventory: ${extractGlobalInventory(journey)}`,
      ].join("\n");

      insights[productKey] = {
        product_name: productName,
        insight: insightText,
        product_journey: journey?.journey_comparison || [],
        recommendation: extractGlobalRecommendation(journey),
        advertising: extractGlobalAdvertising(journey),
        inventory_recommendation: extractGlobalInventory(journey),
        raw_global_journey: journey,
      };
    });

    return insights;
  };

  const extractGlobalRecommendation = (journey: any): string => {
    const countries = ["uk", "us"];

    for (const country of countries) {
      const skuBlocks = journey?.[country] || {};
      const first = Object.values(skuBlocks)[0] as any;
      if (first?.recommendation) return first.recommendation;
    }

    return "Monitor performance across UK and US.";
  };

  const extractGlobalAdvertising = (journey: any): string => {
    const countries = ["uk", "us"];

    for (const country of countries) {
      const skuBlocks = journey?.[country] || {};
      const first = Object.values(skuBlocks)[0] as any;
      if (first?.ads_recommendation) return first.ads_recommendation;
    }

    return "Monitor current advertising.";
  };

  const extractGlobalInventory = (journey: any): string => {
    const countries = ["uk", "us"];

    for (const country of countries) {
      const skuBlocks = journey?.[country] || {};
      const first = Object.values(skuBlocks)[0] as any;
      if (first?.inventory_recommendation) return first.inventory_recommendation;
    }

    return "Inventory position is stable.";
  };

  const hydrateFromPayload = (payload: ApiResponse) => {
    const newPeriods = payload.periods || null;

    const rawCat = payload.categorized_growth || {
      top_80_skus: [],
      new_or_reviving_skus: [],
      other_skus: [],
    };

    const normalized = isGlobalData()
      ? normalizeGlobalCategorizedGrowth(rawCat)
      : normalizeCategorizedGrowth(rawCat);

    setPeriods(newPeriods);
    setCategorizedGrowth(normalized);
    setMonth2Label(newPeriods?.current_mtd?.label || "");

    const summaryObj = payload.overall_summary;

    setSummaryText(summaryObj?.summary_text || "");
    setOverallSummary(summaryObj?.metric_bullets || []);
    setOverallActions(
      Array.isArray(payload.overall_actions)
        ? payload.overall_actions
        : []
    );
    setRecommendedActions(
      isGlobalData()
        ? flattenGlobalRecommendedActions(payload.recommended_actions_mtd || {})
        : payload.recommended_actions_mtd || {}
    );
    setRemainingSkusBlock(normalizeTextBlock(payload.remaining_skus_block || payload.remaining_skus_recommendation));
    setPortfolioRecommendation(normalizeTextBlock(payload.portfolio_recommendation));
    setPortfolioInventoryBlock(normalizeTextBlock(payload.portfolio_inventory_block));
    setObjectiveContext(payload.objective_context || null);
    setAdsRecommendation(payload.ads_recommendation || "");
    setInventorySummary(payload.inventory_summary || null);

    const liveInsights = isGlobalData()
      ? buildGlobalSkuInsights(payload)
      : payload.ai_insights || {};

    setSkuInsights(liveInsights);
  };

  useEffect(() => {
    if (initialData) {
      hydrateFromPayload(initialData);
      setPageLoading(false);
    }
  }, [initialData]);

  // =========================
  // Initial load (cached + live)
  // =========================

  const parsePortfolioInventoryBlock = (raw: string) => {
    if (!raw || typeof raw !== "string") {
      return { inventoryBullets: [], summaryText: "" };
    }

    const lines = raw
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter(Boolean)
      .filter((l) => !/^##\s*inventory/i.test(l));

    const inventoryBullets = lines
      .filter((l) => /^[-•]/.test(l))
      .map((l) => l.replace(/^[-•]\s*/, "").trim());

    const summaryText = lines
      .filter((l) => !/^[-•]/.test(l))
      .join(" ")
      .trim();

    return { inventoryBullets, summaryText };
  };

  const parsedPortfolioInventory = useMemo(() => {
    return parsePortfolioInventoryBlock(portfolioInventoryBlock);
  }, [portfolioInventoryBlock]);

  const fetchLiveBi = async (generateInsights: boolean = false) => {
    setError(null);

    if (!generateInsights) {
      setSkuInsights({});
      setSelectedSku(null);
      setModalOpen(false);
      setPageLoading(true);
    }

    try {
      const liveMtdParams = getLiveMtdParams({
        isGlobal: normalizedCountry === "global",
        asOf,
        startDay,
        endDay,
      });

      const res = await api.get<ApiResponse>('/live_mtd_bi', {
        params: normalizedCountry === "global"
          ? {
            countryName: "global",
            ...liveMtdParams,
            generate_ai_insights: "false",
          }
          : {
            countryName: normalizedCountry,
            ranged,
            month,
            year,
            generate_ai_insights: generateInsights ? "true" : "false",
          },
      });

      const newPeriods = res.data.periods || null;
      const rawCat = res.data.categorized_growth || {
        top_80_skus: [],
        new_or_reviving_skus: [],
        other_skus: [],
      };

      const normalized = normalizedCountry === "global"
        ? normalizeGlobalCategorizedGrowth(rawCat)
        : normalizeCategorizedGrowth(rawCat);

      setPeriods(newPeriods);
      setCategorizedGrowth(normalized);
      setMonth2Label(newPeriods?.current_mtd?.label || "");

      const summaryObj = res.data.overall_summary;
      const summaryTextFromApi = summaryObj?.summary_text || "";
      const summaryBulletsFromApi = summaryObj?.metric_bullets || [];
      const adsRecommendationFromApi = res.data.ads_recommendation || "";
      const inventoryFromApi = res.data.inventory_summary || null;
      const remainingBlock = normalizeTextBlock(
        res.data.remaining_skus_block || res.data.remaining_skus_recommendation
      );
      const portfolioRecFromApi = normalizeTextBlock(res.data.portfolio_recommendation);
      const portfolioInventoryFromApi = normalizeTextBlock(res.data.portfolio_inventory_block);

      setSummaryText(summaryTextFromApi);
      setOverallSummary(summaryBulletsFromApi);
      setOverallActions(Array.isArray(res.data.overall_actions) ? res.data.overall_actions : []);
      setRecommendedActions(
        normalizedCountry === "global"
          ? flattenGlobalRecommendedActions(res.data.recommended_actions_mtd || {})
          : res.data.recommended_actions_mtd || {}
      );
      setAdsRecommendation(adsRecommendationFromApi);
      setInventorySummary(inventoryFromApi);
      setRemainingSkusBlock(remainingBlock);
      setPortfolioRecommendation(portfolioRecFromApi);
      setPortfolioInventoryBlock(portfolioInventoryFromApi);
      setObjectiveContext(res.data.objective_context || null);
      setInsightDate(getTodayKey());

      const liveInsights = normalizedCountry === "global"
        ? buildGlobalSkuInsights(res.data)
        : res.data.ai_insights || {};

      if (normalizedCountry === "global" || generateInsights) {
        setSkuInsights(liveInsights);
      }
    } catch (err: any) {
      console.error('live_mtd_bi error:', err?.response?.data || err.message);
      setError(
        err?.response?.data?.error ||
        'An error occurred while fetching live BI data.'
      );
    } finally {
      if (!generateInsights) setPageLoading(false);
    }
  };

  useEffect(() => {
    if (initialData) {
      hydrateFromPayload(initialData);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialData]);

  // useEffect(() => {
  //   if (!normalizedCountry || normalizedCountry === 'global') return;
  //   fetchLiveBi(false);
  //   // eslint-disable-next-line react-hooks/exhaustive-deps
  // }, [normalizedCountry, ranged, month, year]);

  useEffect(() => {
    if (disableAutoFetch) return;
    if (!normalizedCountry) return;

    fetchLiveBi(false);

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    normalizedCountry,
    ranged,
    month,
    year,
    disableAutoFetch,
    asOf,
    startDay,
    endDay,
  ]);
  // =========================
  // AI insights generate (button)
  // =========================

  // const analyzeSkus = async () => {
  //   setLoadingInsight(true);
  //   try {
  //     await fetchLiveBi(true);
  //   } catch (err: any) {
  //     console.error('generate insights error:', err?.response?.data || err.message);
  //   } finally {
  //     setLoadingInsight(false);
  //   }
  // };

  const analyzeSkus = async () => {
    setLoadingInsight(true);
    try {
      if (onGenerateInsights) {
        await onGenerateInsights();
        return;
      }
      await fetchLiveBi(normalizedCountry === "global" ? false : true);
    } catch (err: any) {
      console.error('generate insights error:', err?.response?.data || err.message);
    } finally {
      setLoadingInsight(false);
    }
  };

  // =========================
  // Insight helpers
  // =========================

  const getInsightByProductName = (
    productName: string
  ): [string, SkuInsight] | null => {
    if (!productName) return null;
    const needle = productName.toLowerCase().trim();

    let entry = Object.entries(skuInsights).find(
      ([, d]) => d.product_name?.toLowerCase().trim() === needle
    );

    if (!entry && isGlobalData()) {
      entry = Object.entries(skuInsights).find(([, d]) => {
        const n = d.product_name?.toLowerCase().trim();
        return n && (n.includes(needle) || needle.includes(n));
      });
    }

    return entry ? (entry as [string, SkuInsight]) : null;
  };

  const getInsightForItem = (item: SkuItem): [string, SkuInsight] | null => {
    if (isGlobalData()) return getInsightByProductName(item.product_name);
    if (item.sku && skuInsights[item.sku]) return [item.sku, skuInsights[item.sku]];
    return getInsightByProductName(item.product_name);
  };


  // ---------- TOP SECTION HELPERS (add above exportToExcel) ----------


  const buildTopAoA = ({
    headerCount,
    title,
    companyName,
    brandName,
    profitColIndex1Based,
    extraLines = [],
  }: {
    headerCount: number;
    title: string;
    companyName: string;
    brandName: string;
    profitColIndex1Based: number; // Excel-style 1-based
    extraLines?: string[];
  }) => {
    const aoa: any[][] = [];

    // Row 1: Title
    const titleRow = new Array(headerCount).fill("");
    titleRow[0] = title || "";
    aoa.push(titleRow);

    // Row 2: Company left + Brand right (anchored near profit column)
    const companyBrandRow = new Array(headerCount).fill("");
    companyBrandRow[0] = `Company Name : ${companyName || ""}`;

    const profit0Based = Math.max(0, profitColIndex1Based - 1);
    companyBrandRow[Math.min(headerCount - 1, profit0Based)] = `${brandName || ""}`;
    aoa.push(companyBrandRow);

    // Row 3+: extra lines (Currency/Country/Platform etc.)
    for (const line of extraLines) {
      const r = new Array(headerCount).fill("");
      r[0] = line;
      aoa.push(r);
    }

    // blank row
    aoa.push(new Array(headerCount).fill(""));

    return aoa;
  };

  const applyTopStyles = (
    ws: XLSX.WorkSheet,
    headerCount: number,
    profitColIndex1Based: number
  ) => {
    ws["!merges"] = ws["!merges"] || [];



    // Row heights (make room for 4 lines)
    ws["!rows"] = ws["!rows"] || [];
    ws["!rows"][0] = { hpt: 18 };
    ws["!rows"][1] = { hpt: 18 };


    // Company/brand row alignment
    for (let c = 0; c < headerCount; c++) {
      const addr = XLSX.utils.encode_cell({ r: 1, c });
      if (!ws[addr]) continue;
      ws[addr].s = {
        font: { bold: false, sz: 11 },
        alignment: { horizontal: c === 0 ? "left" : "right", vertical: "center" },
      };
    }

    // Brand cell stronger
    const profit0Based = Math.max(0, profitColIndex1Based - 1);
    const brandAddr = XLSX.utils.encode_cell({
      r: 1,
      c: Math.min(headerCount - 1, profit0Based),
    });
    if (ws[brandAddr]) {
      ws[brandAddr].s = {
        font: { bold: false, sz: 11 },
        alignment: { horizontal: "right", vertical: "center" },
      };
    }

    // Row heights
    ws["!rows"] = ws["!rows"] || [];
    ws["!rows"][0] = { hpt: 24 };
    ws["!rows"][1] = { hpt: 18 };
  };


  const isTotalLikeRow = (r: any) => {
    const name = String(r?.product_name || "").toLowerCase().trim();
    return name === "total" || name.includes("total (top 80");
  };

  const cleanSkuRows = (rows: SkuItem[] = []) => rows.filter((r) => !isTotalLikeRow(r));



  // ---------- Excel Header Values (from userData) ----------
  const companyNameForExcel = capitalizeWords(userData?.company_name || "");
  const brandNameForExcel = capitalizeWords(userData?.brand_name || "");

  // ---------- Currency helpers ----------
  const countryToCurrencyCode = (country: string) => {
    const c = (country || "").toLowerCase();
    if (c === "uk") return "GBP";
    if (c === "us") return "USD";
    if (c === "ca") return "CAD";
    if (c === "eu") return "EUR";
    // add more if needed
    return ""; // unknown
  };

  const currencyCodeToSymbol = (code: string) => {
    const c = (code || "").toUpperCase();
    if (c === "USD") return "$";
    if (c === "GBP") return "£";
    if (c === "EUR") return "€";
    if (c === "CAD") return "C$";
    if (c === "AUD") return "A$";
    if (c === "INR") return "₹";
    if (c === "AED") return "د.إ";
    if (c === "SAR") return "﷼";
    return c; // fallback: show code if symbol unknown
  };

  // const getCurrencySymbolForExcel = () => {
  //   const isGlobal = (countryName || "").toLowerCase() === "global";

  //   // your profile field (adjust the key if your API uses a different name)
  //   const homeCode =
  //     (userData as any)?.homeCurrency ||
  //     (userData as any)?.home_currency ||
  //     "";

  //   const countryCode = countryToCurrencyCode(countryName);

  //   const codeToUse = isGlobal ? homeCode : countryCode || homeCode;
  //   return currencyCodeToSymbol(codeToUse);
  // };

  const parseISODateSafe = (iso?: string) => {
    if (!iso) return null;
    const d = new Date(iso);
    return Number.isNaN(d.getTime()) ? null : d;
  };

  const formatRangeLabel = (p?: PeriodInfo) => {
    const s = parseISODateSafe(p?.start_date || p?.start);
    const e = parseISODateSafe(p?.end_date || p?.end);
    if (!s || !e) return "";

    const sm = s.toLocaleString("en-US", { month: "short" });
    const em = e.toLocaleString("en-US", { month: "short" });

    const sd = s.getDate();
    const ed = e.getDate();

    const sameMonth = s.getMonth() === e.getMonth() && s.getFullYear() === e.getFullYear();

    if (sameMonth) return `${sm} ${sd}-${ed}`;

    const sameYear = s.getFullYear() === e.getFullYear();
    if (sameYear) return `${sm} ${sd}-${em} ${ed}`;

    return `${sm} ${sd}, ${s.getFullYear()}-${em} ${ed}, ${e.getFullYear()}`;
  };

  const applyBoldRow = (ws: XLSX.WorkSheet, rowIndex: number) => {
    const ref = ws["!ref"];
    if (!ref) return;
    const range = XLSX.utils.decode_range(ref);

    for (let C = range.s.c; C <= range.e.c; C++) {
      const addr = XLSX.utils.encode_cell({ r: rowIndex, c: C });
      const cell = ws[addr];
      if (!cell) continue;

      ws[addr] = {
        ...cell,
        s: {
          ...(cell.s || {}),
          font: { ...((cell.s as any)?.font || {}), bold: true },
        },
      };
    }
  };

  const boldHeaderRows = (ws: XLSX.WorkSheet, headerRows: number[]) => {
    headerRows.forEach((r) => applyBoldRow(ws, r));
  };

  const boldTotalRowsByProductColumn = (
    ws: XLSX.WorkSheet,
    productColIndex0Based: number = 1,  // col B by default
    totalLabels: string[] = ["total", "grand total", "others"]
  ) => {
    const ref = ws["!ref"];
    if (!ref) return;
    const range = XLSX.utils.decode_range(ref);

    for (let R = range.s.r; R <= range.e.r; R++) {
      const productAddr = XLSX.utils.encode_cell({ r: R, c: productColIndex0Based });
      const v = String(ws[productAddr]?.v ?? "").trim().toLowerCase();
      if (!v) continue;

      if (totalLabels.includes(v)) {
        applyBoldRow(ws, R);
      }
    }
  };

  // =========================
  // Export to Excel
  // =========================

  const exportToExcel = (rows: SkuItem[] = [], filename = "sku_analysis.xlsx") => {
    const currentMonthLabel = month2Label.split(" ")[0] || "Current";

    // ✅ Always export ALL rows, not visible/sliced rows
    const exportRows =
      activeTab === "all_skus"
        ? allSkuRows
        : cleanSkuRows(getTabRows(activeTab));

    const totalCm1ProfitMonth2 = allSkuRows.reduce(
      (s, r: any) =>
        s +
        convertToDisplayCurrency(
          r?.profit_month2 ?? r?.profit_curr ?? r?.profit ?? 0,
          sourceCurrency
        ),
      0
    );

    const totalNetSalesMonth2 = allSkuRows.reduce(
      (s, r: any) =>
        s +
        convertToDisplayCurrency(
          r?.net_sales_month2 ?? r?.net_sales_curr ?? r?.net_sales ?? 0,
          sourceCurrency
        ),
      0
    );

    const getGrowthValue = (row: any, key: string) => {
      const raw = row?.[key];
      const value = typeof raw === "object" ? raw?.value : raw;
      const n = Number(value);
      return Number.isFinite(n) ? n : 0;
    };

    const fmtPctValue = (value: any) => {
      const n = Number(value);
      if (!Number.isFinite(n)) return "0.00%";
      const sign = n > 0 ? "+" : "";
      return `${sign}${n.toFixed(2)}%`;
    };

    const getSalesMix = (row: any) => {
      const mix =
        row?.["Sales Mix (Month2)"] ??
        row?.sales_mix_month2 ??
        row?.sales_mix_curr ??
        row?.sales_mix;

      if (mix != null && Number.isFinite(Number(mix))) {
        return `${Number(mix).toFixed(2)}%`;
      }

      const ns = convertToDisplayCurrency(
        row?.net_sales_month2 ?? row?.net_sales_curr ?? row?.net_sales ?? 0,
        sourceCurrency
      );

      return totalNetSalesMonth2 > 0
        ? `${((ns / totalNetSalesMonth2) * 100).toFixed(2)}%`
        : "0.00%";
    };

    const getProfitMix = (row: any) => {
      const profit = convertToDisplayCurrency(
        row?.profit_month2 ?? row?.profit_curr ?? row?.profit ?? 0,
        sourceCurrency
      );

      return totalCm1ProfitMonth2 > 0
        ? `${((profit / totalCm1ProfitMonth2) * 100).toFixed(2)}%`
        : "0.00%";
    };

    type ExcelRow = {
      "S.No.": number | string;
      "Product Name": string;
      [key: string]: string | number;
    };

    const excelRows: ExcelRow[] = exportRows.map((item, index) => ({
      "S.No.": index + 1,
      "Product Name": capitalizeWords(item.product_name || item.sku || "N/A"),
      [`Sales Mix (${currentMonthLabel})`]: getSalesMix(item),
      [`Profit Mix (${currentMonthLabel})`]: getProfitMix(item),
      "Sales Mix Change (%)": fmtPctValue(getGrowthValue(item, "Sales Mix Change")),
      "Unit Growth (%)": fmtPctValue(getGrowthValue(item, "Unit Growth")),
      "ASP Growth (%)": fmtPctValue(getGrowthValue(item, "ASP Growth")),
      "Net Sales Growth (%)": fmtPctValue(
        getGrowthValue(item, "Sales Growth") || getGrowthValue(item, "Net Sales Growth")
      ),
      "CM1 Profit Per Unit (%)": fmtPctValue(getGrowthValue(item, "Profit Per Unit")),
      "CM1 Profit Impact (%)": fmtPctValue(getGrowthValue(item, "CM1 Profit Impact")),
    }));


    // ✅ Total row
    const qtyPrev = allSkuRows.reduce(
      (s: number, r: any) => s + Number(r?.quantity_month1 ?? r?.quantity_prev ?? 0),
      0
    );

    const qtyCurr = allSkuRows.reduce(
      (s: number, r: any) =>
        s + Number(r?.quantity_month2 ?? r?.quantity_curr ?? r?.quantity ?? 0),
      0
    );

    const nsPrev = allSkuRows.reduce(
      (s: number, r: any) =>
        s +
        convertToDisplayCurrency(
          r?.net_sales_month1 ?? r?.net_sales_prev ?? 0,
          sourceCurrency
        ),
      0
    );

    const nsCurr = allSkuRows.reduce(
      (s: number, r: any) =>
        s +
        convertToDisplayCurrency(
          r?.net_sales_month2 ?? r?.net_sales_curr ?? r?.net_sales ?? 0,
          sourceCurrency
        ),
      0
    );

    const profitPrev = allSkuRows.reduce(
      (s: number, r: any) =>
        s +
        convertToDisplayCurrency(
          r?.profit_month1 ?? r?.profit_prev ?? 0,
          sourceCurrency
        ),
      0
    );

    const profitCurr = allSkuRows.reduce(
      (s: number, r: any) =>
        s +
        convertToDisplayCurrency(
          r?.profit_month2 ?? r?.profit_curr ?? r?.profit ?? 0,
          sourceCurrency
        ),
      0
    );

    const aspPrev = qtyPrev > 0 ? nsPrev / qtyPrev : 0;
    const aspCurr = qtyCurr > 0 ? nsCurr / qtyCurr : 0;

    const unitProfitPrev = qtyPrev > 0 ? profitPrev / qtyPrev : 0;
    const unitProfitCurr = qtyCurr > 0 ? profitCurr / qtyCurr : 0;

    const pct = (prev: number, curr: number) =>
      prev ? ((curr - prev) / prev) * 100 : 0;

    excelRows.push({
      "S.No.": 0,
      "Product Name": "Total",
      [`Sales Mix (${currentMonthLabel})`]: "100.00%",
      [`Profit Mix (${currentMonthLabel})`]: "100.00%",
      "Sales Mix Change (%)": "0.00%",
      "Unit Growth (%)": fmtPctValue(pct(qtyPrev, qtyCurr)),
      "ASP Growth (%)": fmtPctValue(pct(aspPrev, aspCurr)),
      "Net Sales Growth (%)": fmtPctValue(pct(nsPrev, nsCurr)),
      "CM1 Profit Per Unit (%)": fmtPctValue(pct(unitProfitPrev, unitProfitCurr)),
      "CM1 Profit Impact (%)": fmtPctValue(pct(profitPrev, profitCurr)),
    });

    const ws = XLSX.utils.json_to_sheet(excelRows);

    // ✅ Column widths
    ws["!cols"] = [
      { wch: 8 },
      { wch: 28 },
      { wch: 20 },
      { wch: 20 },
      { wch: 22 },
      { wch: 18 },
      { wch: 18 },
      { wch: 22 },
      { wch: 26 },
      { wch: 24 },
    ];

    // ✅ Style header and total row
    const range = XLSX.utils.decode_range(ws["!ref"] || "A1:J1");

    for (let C = range.s.c; C <= range.e.c; C++) {
      const headerAddr = XLSX.utils.encode_cell({ r: 0, c: C });
      if (ws[headerAddr]) {
        ws[headerAddr].s = {
          font: { bold: true, color: { rgb: "FFF2C2" } },
          fill: { fgColor: { rgb: "5EA68E" } },
          alignment: { horizontal: "center", vertical: "center" },
        };
      }
    }

    const totalRowIndex = excelRows.length;
    for (let C = range.s.c; C <= range.e.c; C++) {
      const addr = XLSX.utils.encode_cell({ r: totalRowIndex, c: C });
      if (ws[addr]) {
        ws[addr].s = {
          font: { bold: true },
          fill: { fgColor: { rgb: "D9D9D9" } },
          alignment: { horizontal: "center", vertical: "center" },
        };
      }
    }

    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "SKU Analysis");
    XLSX.writeFile(wb, filename);
  };

  // =====================

  // =========================
  // Feedback submit
  // =========================

  const submitSummaryFeedback = async () => {
    try {
      if (!selectedSku) return;
      if (!fbType) {
        setError('Please choose 👍 or 👎 before submitting.');
        return;
      }

      setFbSubmitting(true);
      setError(null);

      const insightData =
        skuInsights[selectedSku as keyof typeof skuInsights] ||
        getInsightByProductName(selectedSku as string)?.[1];

      const productName = insightData?.product_name || selectedSku;
      const fullInsightText = insightData?.insight || '';

      const currentRows = getTabRows(activeTab);

      const rowIndex = Math.max(
        currentRows.findIndex(
          (r) =>
            (r.sku && r.sku === selectedSku) ||
            (r.product_name &&
              r.product_name.toLowerCase().trim() ===
              String(productName).toLowerCase().trim())
        ),
        -1
      );

      const payload = {
        country: countryName,
        rowIndex: rowIndex === -1 ? 0 : rowIndex,
        tab: getTabNumberForFeedback(activeTab),
        type: fbType,
        text: fbText || '',
        productData: {
          product_name: productName,
          combined_text: fullInsightText,
          raw_ai_response: fullInsightText,
        },
      };

      await api.post('/row-feedback', payload, {
        headers: { 'Content-Type': 'application/json' },
      });

      setFbSuccess(true);
      setTimeout(() => setFbSuccess(false), 2500);
      setFbText('');
      setFbType(null);
    } catch (err: any) {
      console.error('row-feedback error:', err?.response?.data || err.message);
      setError(
        err?.response?.data?.error ||
        err?.response?.data?.message ||
        'Failed to submit feedback. Please try again.'
      );
    } finally {
      setFbSubmitting(false);
    }
  };

  // =========================
  // Insight formatting helpers
  // =========================

  const highlightInsightText = (text: string) => {
    const greenWords = [
      'profit',
      'profits',
      'increase',
      'growth',
      'improvement',
      'gain',
      'gains',
      'up',
      'higher',
    ];

    const redWords = ['loss', 'losses', 'decrease', 'decline', 'drop', 'down', 'lower'];

    const regex = new RegExp(`\\b(${[...greenWords, ...redWords].join('|')})\\b`, 'gi');
    const parts = text.split(regex);

    return parts.map((part, idx) => {
      const lower = part.toLowerCase();
      if (greenWords.includes(lower)) {
        return (
          <span key={idx} style={{ color: '#5EA68E', fontWeight: 600 }}>
            {part}
          </span>
        );
      }
      if (redWords.includes(lower)) {
        return (
          <span key={idx} style={{ color: '#FF5C5C', fontWeight: 600 }}>
            {part}
          </span>
        );
      }
      return <span key={idx}>{part}</span>;
    });
  };

  const asGrowth = (v: number | null): GrowthCategory | undefined =>
    v == null ? undefined : { value: v, category: "" };

  const renderFormattedInsight = (raw: string) => {
    if (!raw) return null;

    const lines = raw
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter(Boolean);

    const SECTION_ORDER = [
      'Details',
      'Observations',
      'Improvements',
      'Unit Growth',
      'ASP',
      'Sales',
      'Profit',
      'Unit Profitability',
      'Summary',
    ];

    const LIST_SECTIONS = new Set([
      'Observations',
      'Improvements',
      'Unit Growth',
      'ASP',
      'Sales',
      'Profit',
      'Unit Profitability',
    ]);

    const headingOf = (line: string): string | null => {
      const m =
        line.match(/^details\s+for/i)
          ? ['Details']
          : line.match(/^(observations)\s*:?\s*$/i)
            ? ['Observations']
            : line.match(/^(improvements)\s*:?\s*$/i)
              ? ['Improvements']
              : line.match(/^(unit\s+growth)\s*:?\s*$/i)
                ? ['Unit Growth']
                : line.match(/^(asp)\s*:?\s*$/i)
                  ? ['ASP']
                  : line.match(/^(sales)\s*:?\s*$/i)
                    ? ['Sales']
                    : line.match(/^(profit)\s*:?\s*$/i)
                      ? ['Profit']
                      : line.match(/^(unit\s+profitability)\s*:?\s*$/i)
                        ? ['Unit Profitability']
                        : line.match(/^(summary)\s*:?\s*$/i)
                          ? ['Summary']
                          : null;
      return m ? m[0] : null;
    };

    const sections: Record<string, string[]> = {};
    let current: string | null = null;

    for (const line of lines) {
      const hd = headingOf(line);
      if (hd) {
        current = hd;
        if (!sections[current]) sections[current] = [];
        if (current === 'Details') sections[current].push(line);
        continue;
      }
      if (!current) current = 'Details';
      if (!sections[current]) sections[current] = [];
      const isLabel = !!line.match(
        /^(observations|improvements|unit\s+growth|asp|sales|profit|unit\s+profitability|summary)\s*:?\s*$/i
      );
      if (isLabel) continue;
      sections[current].push(line);
    }

    const clean = (s: string) =>
      s.replace(/^[•\-\u2013\u2014]\s+/, '').replace(/^\d+\.\s+/, '');

    return SECTION_ORDER.filter((sec) => sections[sec]?.length).map((sec, idx) => {
      const content = sections[sec];
      const isList = LIST_SECTIONS.has(sec);

      return (
        <div key={idx} className="insight-section" style={{ marginBottom: 12 }}>
          {(isList || sec === 'Summary') && (
            <strong style={{ display: 'block', marginBottom: 6 }}>
              {sec}
            </strong>
          )}

          {isList ? (
            <ul className="list-disc" style={{ margin: '6px 0 10px 20px', padding: 0 }}>
              {content.map((line, i) => {
                const trimmed = clean(line);

                const isSubHeading =
                  /^[A-Za-z][A-Za-z\s\/]+:?$/i.test(trimmed) &&
                  !trimmed.match(/\d|%|,/) &&
                  trimmed.split(/\s+/).length <= 5;

                if (isSubHeading) {
                  const label = trimmed.replace(/:$/, '').trim();
                  return (
                    <li
                      key={i}
                      style={{
                        listStyle: 'none',
                        marginTop: 10,
                        marginBottom: 4,
                      }}
                    >
                      <span
                        style={{
                          fontWeight: 700,
                          fontSize: 14,
                          color: '#374151',
                          borderLeft: '3px solid #60a68e',
                          paddingLeft: 8,
                        }}
                      >
                        {label}
                      </span>
                    </li>
                  );
                }

                return (
                  <li
                    key={i}
                    style={{
                      marginBottom: 4,
                      lineHeight: 1.6,
                      fontSize: 13,
                    }}
                  >
                    {highlightInsightText(trimmed)}
                  </li>
                );
              })}
            </ul>
          ) : (
            <div>
              {content.map((line, i) => (
                <p
                  key={i}
                  style={{
                    margin: '4px 0',
                    lineHeight: 1.6,
                    fontSize: 13,
                  }}
                >
                  {highlightInsightText(line)}
                </p>
              ))}
            </div>
          )}

          {sec === 'Summary' && (
            <div style={{ marginTop: 10 }}>
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'center',
                  gap: 12,
                }}
              >
                <button
                  type="button"
                  onClick={() => setFbType('like')}
                  title="Like"
                  style={{
                    background: 'none',
                    border: 'none',
                    cursor: 'pointer',
                    opacity: fbType === 'like' ? 1 : 0.6,
                  }}
                >
                  <FaThumbsUp size={18} />
                </button>
                <button
                  type="button"
                  onClick={() => setFbType('dislike')}
                  title="Dislike"
                  style={{
                    background: 'none',
                    border: 'none',
                    cursor: 'pointer',
                    opacity: fbType === 'dislike' ? 1 : 0.6,
                  }}
                >
                  <FaThumbsDown size={18} />
                </button>
              </div>

              <div
                style={{
                  marginTop: 10,
                  backgroundColor: '#f1f1f1',
                  padding: '10px 12px',
                  borderRadius: 8,
                  display: 'flex',
                  gap: 10,
                  alignItems: 'center',
                }}
              >
                <input
                  type="text"
                  placeholder="Add a Comment......"
                  value={fbText}
                  onChange={(e) => setFbText(e.target.value)}
                  style={{
                    flex: 1,
                    border: 'none',
                    outline: 'none',
                    background: 'transparent',
                  }}
                />
                <button
                  type="button"
                  onClick={submitSummaryFeedback}
                  disabled={fbSubmitting}
                  className="styled-button"
                  style={{ whiteSpace: 'nowrap' }}
                >
                  {fbSubmitting ? 'Submitting...' : 'Submit'}
                </button>
              </div>

              {fbSuccess && (
                <div style={{ color: '#2e7d32', fontWeight: 600, marginTop: 6 }}>
                  Feedback submitted!
                </div>
              )}
            </div>
          )}
        </div>
      );
    });
  };

  const sortMetricsByOrder = (
    metrics: { label: string; value: string; color?: string }[]
  ) => {
    const order = [
      "units",
      "net sales",
      "asp",
      "cm1 profit",
      "cm1 profit per unit",
    ];

    return [...metrics].sort((a, b) => {
      const aIndex = order.indexOf(a.label.trim().toLowerCase());
      const bIndex = order.indexOf(b.label.trim().toLowerCase());

      return (
        (aIndex === -1 ? Number.MAX_SAFE_INTEGER : aIndex) -
        (bIndex === -1 ? Number.MAX_SAFE_INTEGER : bIndex)
      );
    });
  };

  const parseRecommendedAction = (raw: string) => {
    const lines = (raw || "")
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean);

    const productName = lines[0] || "";

    const metrics: { label: string; value: string; color?: string }[] = [];
    const metricRegex =
      /^(ASP|Units|Net sales|CM1 profit per unit|CM1 profit)\s*:\s*(.+)$/i;

    const insightParts: string[] = [];

    for (const line of lines.slice(1)) {
      const metricMatch = line.match(metricRegex);
      if (metricMatch) {
        const label = metricMatch[1];
        const rawMetricValue = metricMatch[2];
        const value = convertMetricValueString(rawMetricValue, label);

        metrics.push({
          label,
          value,
          color: value.includes("-") ? "#FF5C5C" : "#5EA68E",
        });
        continue;
      }
      insightParts.push(line);
    }

    const insightText = insightParts.join("\n").trim();
    const sections = extractSections(insightText);

    return {
      productName,
      metrics: sortMetricsByOrder(metrics),
      insightText,
      journeyPoints: sections.journeyPoints,
      recommendationPoints: sections.recommendationPoints,
      advertisingPoints: sections.advertisingPoints,
      inventoryPoints: sections.inventoryPoints,
    };
  };

  const parseOtherSkusBlock = (raw: string) => {
    const lines = (raw || "")
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean);

    const productName = lines[0] || "Other SKUs";

    const metrics: { label: string; value: string; color?: string }[] = [];
    const metricRegex =
      /^(ASP|Units|Net sales|CM1 profit per unit|CM1 profit)\s*:\s*(.+)$/i;

    const insightParts: string[] = [];

    for (const line of lines.slice(1)) {
      const metricMatch = line.match(metricRegex);
      if (metricMatch) {
        const label = metricMatch[1];
        const rawMetricValue = metricMatch[2];
        const value = convertMetricValueString(rawMetricValue, label);

        metrics.push({
          label,
          value,
          color: value.includes("-") ? "#FF5C5C" : "#5EA68E",
        });
        continue;
      }
      insightParts.push(line);
    }

    const insightText = insightParts.join("\n").trim();
    const sections = extractSections(insightText);

    return {
      productName,
      metrics: sortMetricsByOrder(metrics),
      insightText,
      journeyPoints: sections.journeyPoints,
      recommendationPoints: sections.recommendationPoints,
      advertisingPoints: sections.advertisingPoints,
      inventoryPoints: sections.inventoryPoints,
    };
  };

  const formatBulletLine = (line: string) => {
    if (!line) return null;

    const reDelta = /(increased|decreased)\s+by\s+(-?\d+(?:\.\d+)?)\s*(%?)/gi;
    const out: any[] = [];
    let lastIndex = 0;

    let match: RegExpExecArray | null;
    while ((match = reDelta.exec(line)) !== null) {
      const [full, verb, num, suffixRaw] = match;
      const start = match.index;
      const end = start + full.length;

      if (start > lastIndex) out.push({ type: 'text', value: line.slice(lastIndex, start) });

      out.push({ type: 'text', value: `${verb} by ` });

      const isIncrease = String(verb).toLowerCase() === 'increased';
      const suffix = suffixRaw || '';

      out.push({
        type: 'num',
        value: `${Number(num).toFixed(2)}${suffix}`,
        color: isIncrease ? '#5EA68E' : '#FF5C5C',
      });

      lastIndex = end;
    }

    if (lastIndex < line.length) out.push({ type: 'text', value: line.slice(lastIndex) });

    const hasDelta = out.some((p) => p.type === 'num');
    if (!hasDelta) return highlightInsightText(line);

    return out.map((p, i) => {
      if (p.type === 'num') {
        return (
          <span key={i} style={{ color: p.color, fontWeight: 700 }}>
            {p.value}
          </span>
        );
      }
      return <span key={i}>{p.value}</span>;
    });
  };






  // =========================
  // Data for table
  // =========================

  const getAllSkusForExport = (): SkuItem[] => [
    ...(categorizedGrowth.top_80_skus || []),
    ...(categorizedGrowth.new_skus || []),
    ...(categorizedGrowth.reviving_skus || []),
    ...(categorizedGrowth.other_skus || []),
  ];

  const currentTabData = getTabRows(activeTab);

  const allSkuRows = categorizedGrowth
    ? [
      ...cleanSkuRows(categorizedGrowth.top_80_skus || []),
      ...cleanSkuRows(categorizedGrowth.new_skus || []),
      ...cleanSkuRows(categorizedGrowth.reviving_skus || []),
      ...cleanSkuRows(categorizedGrowth.other_skus || []),
    ]
    : [];

  const [showAllSkus, setShowAllSkus] = useState(false);

  useEffect(() => {
    if (activeTab === 'all_skus') setShowAllSkus(false);
  }, [activeTab]);

  const rowsToRender =
    activeTab === 'all_skus'
      ? showAllSkus
        ? allSkuRows
        : allSkuRows.slice(0, 5)
      : currentTabData;

  const hasAnySkus =
    categorizedGrowth.top_80_skus.length > 0 ||
    categorizedGrowth.new_skus.length > 0 ||
    categorizedGrowth.reviving_skus.length > 0 ||
    categorizedGrowth.other_skus.length > 0;

  const segmentTotal =
    activeTab === "all_skus"
      ? categorizedGrowth.all_skus_total
      : activeTab === "top_80_skus"
        ? categorizedGrowth.top_80_total
        : activeTab === "new_skus"
          ? categorizedGrowth.new_skus_total
          : activeTab === "reviving_skus"
            ? categorizedGrowth.reviving_skus_total
            : categorizedGrowth.other_total;

  const manualTotalsForAll = (() => {
    if (activeTab !== 'all_skus' || !currentTabData.length) {
      return {
        salesMix: 0,
        quantity: 0,
        asp: 0,
        net_sales: 0,
        unit_wise_profitability: 0,
        profit: 0,
      };
    }

    let quantity = 0;
    let net_sales = 0;
    let profit = 0;
    let salesMix = 0;

    let aspWeighted = 0;
    let unitProfitWeighted = 0;

    currentTabData.forEach((r) => {
      const q = Number((r as any).quantity_month2 ?? (r as any).quantity_curr ?? r.quantity ?? 0) || 0;
      // const ns = Number((r as any).net_sales_month2 ?? (r as any).net_sales_curr ?? r.net_sales ?? 0) || 0;
      // const p = Number((r as any).profit_month2 ?? (r as any).profit_curr ?? r.profit ?? 0) || 0;

      const ns = convertToDisplayCurrency(
        (r as any).net_sales_month2 ?? (r as any).net_sales_curr ?? r.net_sales ?? 0,
        sourceCurrency
      );

      const p = convertToDisplayCurrency(
        (r as any).profit_month2 ?? (r as any).profit_curr ?? r.profit ?? 0,
        sourceCurrency
      );

      const mix =
        Number(
          (r as any).sales_mix_month2 ??
          (r as any).sales_mix_curr ??
          (r as any)['Sales Mix (Month2)'] ??
          (r as any).sales_mix ??
          0
        ) || 0;

      quantity += q;
      net_sales += ns;
      profit += p;
      salesMix += mix;

      const aspVal = convertToDisplayCurrency(
        (r as any).asp_month2 ?? (r as any).asp_curr ?? r.asp ?? 0,
        sourceCurrency
      );

      const upVal = convertToDisplayCurrency(
        (r as any).unit_wise_profitability_month2 ??
        (r as any).unit_wise_profitability_curr ??
        r.unit_wise_profitability ??
        0,
        sourceCurrency
      );

      aspWeighted += aspVal * q;
      unitProfitWeighted += upVal * q;
    });

    const asp = quantity > 0 ? (net_sales !== 0 ? net_sales / quantity : aspWeighted / quantity) : 0;
    const unit_wise_profitability =
      quantity > 0 ? (profit !== 0 ? profit / quantity : unitProfitWeighted / quantity) : 0;

    return {
      salesMix,
      quantity,
      asp,
      net_sales,
      unit_wise_profitability,
      profit,
    };
  })();

  const manualTotalsForNewRev = (() => {
    const isNewOrRevivingTab =
      activeTab === "new_skus" || activeTab === "reviving_skus";

    if (!isNewOrRevivingTab || !currentTabData.length) {
      return {
        salesMix: 0,
        quantity: 0,
        asp: 0,
        net_sales: 0,
        unit_wise_profitability: 0,
        profit: 0,
      };
    }

    let quantity = 0;
    let net_sales = 0;
    let profit = 0;
    let aspWeighted = 0;
    let unitProfitWeighted = 0;

    const totalNetSalesMonth1 = allSkuRows.reduce(
      (s, r: any) =>
        s + convertToDisplayCurrency(r?.net_sales_month1 ?? r?.net_sales_prev ?? 0, sourceCurrency),
      0
    );

    // ✅ compute sales mix ONCE (and correctly)
    const totalNetSalesAll = allSkuRows.reduce(
      (s, r: any) => s + Number(r?.net_sales_month2 ?? r?.net_sales_curr ?? r?.net_sales ?? 0),
      0
    );

    const segmentNetSales = currentTabData.reduce(
      (s, r: any) => s + Number(r?.net_sales_month2 ?? r?.net_sales_curr ?? r?.net_sales ?? 0),
      0
    );

    const salesMix = totalNetSalesAll > 0 ? (segmentNetSales / totalNetSalesAll) * 100 : 0;

    currentTabData.forEach((r) => {
      const q = Number(r.quantity ?? 0) || 0;
      const ns = Number(r.net_sales ?? 0) || 0;
      const p = Number(r.profit ?? 0) || 0;

      const aspVal = Number(r.asp ?? 0) || 0;
      const upVal = Number(r.unit_wise_profitability ?? 0) || 0;

      quantity += q;
      net_sales += ns;
      profit += p;

      aspWeighted += aspVal * q;
      unitProfitWeighted += upVal * q;
    });

    const asp = quantity > 0 ? aspWeighted / quantity : 0;
    const unit_wise_profitability = quantity > 0 ? unitProfitWeighted / quantity : 0;

    return {
      salesMix,
      quantity,
      asp,
      net_sales,
      unit_wise_profitability,
      profit,
    };
  })();


  const prevShort = getShortPeriodLabel(periods?.previous?.label);
  const currShort = getShortPeriodLabel(periods?.current_mtd?.label);

  const handleSkuAnalysisDownload = () => {
    if (!userData) {
      setError("User profile not loaded yet. Please try again.");
      return;
    }

    const prevShortName = prevShort || "Prev";
    const currShortName = currShort || "Curr";

    exportSkuAnalysisMtdExcel({
      filename: `SKU_Analysis_${titleCountry}_${prevShortName}vs${currShortName}.xlsx`,

      titleLine: `Amazon ${titleCountry} - SKU Analysis - MTD ${titleMonth}`,
      countryName,
      titleCountry,
      platformLabel: "Amazon",
      periodLabel: `${prevShortName} vs ${currShortName}`,

      companyName: companyNameForExcel,
      brandName: brandNameForExcel,
      homeCurrencyCode: profileHomeCurrency,

      month2Label: month2Label.split(" ")[0] || currShortName || "Current",

      categorizedGrowth: {
        all_skus: allSkuRows,
        top_80_skus: categorizedGrowth.top_80_skus || [],
        new_skus: categorizedGrowth.new_skus || [],
        reviving_skus: categorizedGrowth.reviving_skus || [],
        new_or_reviving_skus: categorizedGrowth.new_or_reviving_skus || [],
        other_skus: categorizedGrowth.other_skus || [],
      },
    });
  };

  // =========================
  // DataTable wiring
  // =========================

  type BIGridRow = {
    __isTotal?: boolean;
    sNo?: React.ReactNode;
    product?: React.ReactNode;
    salesMix?: React.ReactNode;
    profitMix?: React.ReactNode;
    unit?: React.ReactNode;
    asp?: React.ReactNode;
    sales?: React.ReactNode;
    mixChange?: React.ReactNode;
    unitProfit?: React.ReactNode;
    ai?: React.ReactNode;
    profit?: React.ReactNode;
  };

  const calcGrowthValue = (prev: number, curr: number) => {
    if (!prev || prev === 0 || curr == null) return null;
    return ((curr - prev) / prev) * 100;
  };

  const safePct = (prev: number, curr: number) => {
    if (!prev || prev === 0 || curr == null) return null;
    return ((curr - prev) / prev) * 100;
  };

  const makeGrowth = (prev: number, curr: number): GrowthCategory | undefined => {
    const v = calcGrowthValue(prev, curr);
    if (v == null) return undefined;
    return { value: v, category: "" };
  };

  const CenterCell = ({ value }: { value: React.ReactNode }) => (
    <div className="w-full flex items-center justify-center">
      <span className="tabular-nums inline-block min-w-[60px] text-center">
        {value}
      </span>
    </div>
  );

  const GrowthCell = ({
    val,
    color,
    showArrow,
  }: {
    val: number;
    color: string;
    showArrow: boolean;
  }) => {
    const text = `${val > 0 ? "+" : ""}${val.toFixed(2)}%`; // keeps + only for positive
    const Icon = val > 0 ? FaArrowUp : FaArrowDown;

    return (
      <span
        className="inline-flex items-center justify-center gap-2 w-full font-semibold"
        style={{ color }}
      >
        <span className="w-4 flex justify-center shrink-0">
          {showArrow ? (
            <Icon size={12} />
          ) : (
            <Icon size={12} style={{ visibility: "hidden" }} />
          )}
        </span>

        <span className="tabular-nums inline-block w-[60px] text-center">
          {val === 0 ? "0.00%" : text}
        </span>
      </span>
    );
  };

  const renderGrowthOrNA = (g?: GrowthCategory) => {
    const val = !g || g.value == null ? 0 : Number(g.value);

    let color = "#414042";
    if (val > 5) color = "#5EA68E";
    else if (val < -5) color = "#FF5C5C";

    return <GrowthCell val={val} color={color} showArrow={val !== 0} />;
  };

  const renderNewRevGrowthOrDash = (g?: GrowthCategory) => {
    const val =
      g && g.value != null && g.category && g.category !== "No Data"
        ? g.value
        : 0;

    return renderGrowthOrNA({ value: Number(val), category: "" });
  };

  const buildAiCell = (item: SkuItem) => {
    if (!Object.keys(skuInsights).length) return null;

    const entry = getInsightForItem(item);

    if (entry) {
      return (
        <button
          type="button"
          onClick={() => {
            const insightEntry = getInsightForItem(item);
            const insight = insightEntry?.[1] || null;

            const selected = isGlobalData()
              ? buildSelectedRecFromGlobalSkuInsight(item, insight)
              : buildSelectedRecFromSkuInsight(item, insight);

            setSelectedSkuItem(item);
            setSelectedSku(item.sku || item.product_name);
            setSelectedRec(selected);
            setRecDrawerOpen(true);
          }}
          className="font-semibold underline"
        >
          View Insights
        </button>
      );
    }

    return (
      <em style={{ color: "#888" }}>
        Not analyzed
        <br />
        <small style={{ fontSize: 10 }}>
          ({isGlobalData() ? "Global/Product Name" : "SKU"}:{" "}
          {item.product_name || item.sku || "N/A"})
        </small>
      </em>
    );
  };

  const columns: ColumnDef<BIGridRow>[] = useMemo(() => {
    const isNewRev = activeTab === "new_skus" || activeTab === "reviving_skus";
    const showAI = Object.keys(skuInsights).length > 0;

    const SNO_WIDTH = '65px';
    const COMMON_WIDTH = '160px';

    const cols: ColumnDef<BIGridRow>[] = [
      {
        key: 'sNo',
        header: 'S.No.',
        width: SNO_WIDTH,
      },
      {
        key: 'product',
        header: 'Product Name',
        width: COMMON_WIDTH,
        cellClassName: 'text-left',
        headerClassName: 'text-left',
      },
      {
        key: 'salesMix',
        header: `Sales Mix (${month2Label.split(' ')[0] || 'Current'})`,
        width: COMMON_WIDTH,
      },
      {
        key: 'profitMix',
        header: `Profit Mix (${month2Label.split(' ')[0] || 'Current'})`,
        width: COMMON_WIDTH,
      },


      ...(isNewRev
        ? []
        : [
          {
            key: 'mixChange',
            header: 'Sales Mix Change (%)',
            width: COMMON_WIDTH,
          },
        ]),

      {
        key: 'unit',
        header: isNewRev ? 'Units (%)' : 'Unit Growth (%)',
        width: COMMON_WIDTH,
      },
      {
        key: 'asp',
        header: isNewRev ? 'ASP (%)' : 'ASP Growth (%)',
        width: COMMON_WIDTH,
      },
      {
        key: 'sales',
        header: isNewRev ? 'Sales (%)' : 'Net Sales Growth (%)',
        width: COMMON_WIDTH,
      },
      {
        key: 'unitProfit',
        header: isNewRev ? 'Unit Profit (%)' : 'CM1 Profit Per Unit (%)',
        width: '190px',
      },
      {
        key: 'profit',
        header: isNewRev ? 'Profit (%)' : 'CM1 Profit Impact (%)',
        width: '200px',
      },
      ...(showAI
        ? [
          {
            key: 'ai',
            header: 'AI Insight',
            width: '150px',
          },
        ]
        : []),

    ];

    return cols;
  }, [activeTab, month2Label, skuInsights]);



  const tableData: BIGridRow[] = useMemo(() => {
    const isNewRev = activeTab === "new_skus" || activeTab === "reviving_skus";
    const showAI = Object.keys(skuInsights).length > 0;

    const totalNetSalesMonth1 = allSkuRows.reduce(
      (s, r: any) => s + Number(r?.net_sales_month1 ?? r?.net_sales_prev ?? 0),
      0
    );

    const totalCm1ProfitMonth2 = allSkuRows.reduce(
      (s, r: any) =>
        s + convertToDisplayCurrency(r?.profit_month2 ?? r?.profit_curr ?? r?.profit ?? 0, sourceCurrency),
      0
    );

    const getProfitMonth2 = (r: any) =>
      Number(r?.profit_month2 ?? r?.profit_curr ?? r?.profit ?? 0) || 0;

    const tabProfitMonth2 =
      activeTab === "all_skus"
        ? totalCm1ProfitMonth2
        : (rowsToRender || []).reduce((s, r: any) => s + getProfitMonth2(r), 0);

    const totalProfitMix =
      totalCm1ProfitMonth2 > 0
        ? `${((tabProfitMonth2 / totalCm1ProfitMonth2) * 100).toFixed(2)}%`
        : "0.00%";


    const totalNetSalesMonth2 =
      activeTab === 'all_skus'
        ? allSkuRows.reduce(
          (s, r: any) =>
            s + Number(r?.net_sales_month2 ?? r?.net_sales_curr ?? r?.net_sales ?? 0),
          0
        )
        : 0;

    const rows: BIGridRow[] = (rowsToRender || []).map((item, idx) => {
      const mixVal =
        (item as any)['Sales Mix (Month2)'] ??
        (item as any).sales_mix_month2 ??
        (item as any).sales_mix_curr ??
        (item as any).sales_mix ??
        null;

      const salesMix = mixVal != null ? `${Number(mixVal).toFixed(2)}%` : "N/A";
      const rowProfit =
        Number((item as any).profit_month2 ?? (item as any).profit_curr ?? item.profit ?? 0) || 0;

      const profitMix =
        totalCm1ProfitMonth2 > 0
          ? `${((rowProfit / totalCm1ProfitMonth2) * 100).toFixed(2)}%`
          : "0.00%";

      const itemAny = item as any;

      return {
        sNo: <CenterCell value={idx + 1} />,
        product: capitalizeWords(item.product_name || item.sku || 'N/A'),
        salesMix: <CenterCell value={salesMix} />,
        profitMix: <CenterCell value={profitMix} />,

        unit: isNewRev
          ? renderNewRevGrowthOrDash(itemAny["Unit Growth"])
          : renderGrowthOrNA(itemAny["Unit Growth"]),

        asp: isNewRev
          ? renderNewRevGrowthOrDash(itemAny["ASP Growth"])
          : renderGrowthOrNA(itemAny["ASP Growth"]),

        sales: isNewRev
          ? renderNewRevGrowthOrDash(itemAny["Sales Growth"])
          : renderGrowthOrNA(itemAny["Sales Growth"]),

        ...(isNewRev
          ? {}
          : {
            mixChange: renderGrowthOrNA(itemAny["Sales Mix Change"]),
          }),

        unitProfit: isNewRev
          ? renderNewRevGrowthOrDash(itemAny["Profit Per Unit"])
          : renderGrowthOrNA(itemAny["Profit Per Unit"]),

        profit: isNewRev
          ? renderNewRevGrowthOrDash(itemAny["CM1 Profit Impact"])
          : renderGrowthOrNA(itemAny["CM1 Profit Impact"]),

        ...(showAI ? { ai: buildAiCell(item) } : {}),
      };
    });

    const hasAIInsights = Object.keys(skuInsights).length > 0;

    if (
      activeTab === 'all_skus' &&
      allSkuRows.length > 5 &&
      !showAllSkus
    ) {
      const others = allSkuRows.slice(5);

      const sum = (keyPrev: string, keyCurr: string) => {
        let prev = 0;
        let curr = 0;
        others.forEach((r) => {
          prev += Number((r as any)[keyPrev] ?? 0);
          curr += Number((r as any)[keyCurr] ?? 0);
        });
        return { prev, curr };
      };

      const qty = sum('quantity_month1', 'quantity_month2');
      const sales = sum('net_sales_month1', 'net_sales_month2');
      const profit = sum('profit_month1', 'profit_month2');
      const othersProfitMix =
        totalCm1ProfitMonth2 > 0
          ? `${((profit.curr / totalCm1ProfitMonth2) * 100).toFixed(2)}%`
          : "0.00%";

      const aspPrev = qty.prev ? sales.prev / qty.prev : 0;
      const aspCurr = qty.curr ? sales.curr / qty.curr : 0;

      const othersNetSales = others.reduce(
        (s, r: any) =>
          s + convertToDisplayCurrency(r?.net_sales_month2 ?? r?.net_sales_curr ?? r?.net_sales ?? 0, sourceCurrency),
        0
      );

      const othersNetSalesMonth1 = others.reduce(
        (s, r: any) =>
          s + convertToDisplayCurrency(r?.net_sales_month1 ?? r?.net_sales_prev ?? 0, sourceCurrency),
        0
      );

      const totalNetSalesMonth1 = allSkuRows.reduce(
        (s, r: any) => s + Number(r?.net_sales_month1 ?? r?.net_sales_prev ?? 0),
        0
      );


      const othersMix1 =
        totalNetSalesMonth1 > 0 ? (othersNetSalesMonth1 / totalNetSalesMonth1) * 100 : 0;

      const othersMix2 =
        totalNetSalesMonth2 > 0 ? (othersNetSales / totalNetSalesMonth2) * 100 : 0;

      rows.push({
        sNo: <CenterCell value={6} />,
        product: 'Others',
        salesMix: <CenterCell value={
          totalNetSalesMonth2 > 0
            ? `${((othersNetSales / totalNetSalesMonth2) * 100).toFixed(2)}%`
            : '0.00%'
        } />,
        profitMix: <CenterCell value={othersProfitMix} />,
        unit: renderGrowthOrNA(makeGrowth(qty.prev, qty.curr)),
        asp: renderGrowthOrNA(makeGrowth(aspPrev, aspCurr)),
        sales: renderGrowthOrNA(makeGrowth(sales.prev, sales.curr)),
        mixChange: renderGrowthOrNA({
          value: othersMix2 - othersMix1,
          category: '',
        }),
        unitProfit: renderGrowthOrNA(
          makeGrowth(
            profit.prev / (qty.prev || 1),
            profit.curr / (qty.curr || 1)
          )
        ),
        profit: renderGrowthOrNA(makeGrowth(profit.prev, profit.curr)),

        // ✅ AI column
        ...(hasAIInsights
          ? {
            ai: (
              <button
                className="font-semibold underline text-[#5EA68E]"
                onClick={() => setShowAllSkus(true)}
              >
                Expand SKUs
              </button>
            ),
          }
          : {}),
      });

    }

    const isNewOrRevivingTab =
      activeTab === "new_skus" || activeTab === "reviving_skus";

    if (isNewOrRevivingTab && rows.length === 0) {
      return [];
    }


    const pct = (prev: number, curr: number) => {
      if (!prev || prev === 0 || curr == null) return null;
      return ((curr - prev) / prev) * 100;
    };

    const pickFirstNumber = (r: any, keys: string[]) => {
      for (const k of keys) {
        const v = r?.[k];
        if (v != null && v !== "" && !Number.isNaN(Number(v))) return Number(v);
      }
      return null; // important: null means "not available"
    };

    const sum = (rows: any[], keys: string[]) =>
      rows.reduce((acc, r) => acc + Number(pickFirstNumber(r, keys) ?? 0), 0);

    const sumMoneyPrevOnly = (rows: any[], keys: string[]) =>
      rows.reduce(
        (acc, r) => acc + convertToDisplayCurrency(pickPrevNumber(r, keys) ?? 0, sourceCurrency),
        0
      );

    const sumMoneyCurrOnly = (rows: any[], keys: string[]) =>
      rows.reduce(
        (acc, r) => acc + convertToDisplayCurrency(pickCurrNumber(r, keys) ?? 0, sourceCurrency),
        0
      );

    const pickPrevNumber = (r: any, keys: string[]) => {
      for (const k of keys) {
        const v = r?.[k];
        if (v !== null && v !== undefined && v !== "" && Number.isFinite(Number(v))) {
          return Number(v);
        }
      }
      return null; // null = missing prev
    };

    const pickCurrNumber = (r: any, keys: string[]) => {
      for (const k of keys) {
        const v = r?.[k];
        if (v !== null && v !== undefined && v !== "" && Number.isFinite(Number(v))) {
          return Number(v);
        }
      }
      return null;
    };

    const sumPrevOnly = (rows: any[], keys: string[]) =>
      rows.reduce((acc, r) => acc + Number(pickPrevNumber(r, keys) ?? 0), 0);

    const sumCurrOnly = (rows: any[], keys: string[]) =>
      rows.reduce((acc, r) => acc + Number(pickCurrNumber(r, keys) ?? 0), 0);

    // TOTAL row appended
    const isAll = activeTab === 'all_skus';

    const totalSalesMix = isAll
      ? totalNetSalesMonth2 > 0
        ? '100.00%'
        : '0.00%'
      : segmentTotal && (segmentTotal as any)['Sales Mix (Month2)'] != null
        ? `${Number((segmentTotal as any)['Sales Mix (Month2)']).toFixed(2)}%`
        : activeTab === "new_skus" || activeTab === "reviving_skus"
          ? `${manualTotalsForNewRev.salesMix.toFixed(2)}%`
          : "N/A";

    const totalRow: BIGridRow = {
      __isTotal: true,
      sNo: <CenterCell value="" />,
      product: 'Total',
      salesMix: <CenterCell value={totalSalesMix} />,
      profitMix: <CenterCell value={totalProfitMix} />,
      ...(activeTab === "all_skus"
        ? (() => {
          // Use allSkuRows so totals are correct even when UI shows only 5 + "Others"
          const rows = allSkuRows;


          const qtyPrev = sumPrevOnly(rows, ["quantity_month1", "quantity_prev"]);
          const qtyCurr = sumCurrOnly(rows, ["quantity_month2", "quantity_curr", "quantity"]);

          const nsPrev = sumMoneyPrevOnly(rows, ["net_sales_month1", "net_sales_prev"]);
          const nsCurr = sumMoneyCurrOnly(rows, ["net_sales_month2", "net_sales_curr", "net_sales"]);

          const profitPrev = sumMoneyPrevOnly(rows, ["profit_month1", "profit_prev"]);
          const profitCurr = sumMoneyCurrOnly(rows, ["profit_month2", "profit_curr", "profit"]);


          // ✅ PREVIOUS: ONLY previous keys (no current fallback)
          // const qtyPrev = sumPrevOnly(rows, ["quantity_month1", "quantity_prev"]);
          // const nsPrev = sumPrevOnly(rows, ["net_sales_month1", "net_sales_prev"]);
          // const profitPrev = sumPrevOnly(rows, ["profit_month1", "profit_prev"]);

          // ✅ CURRENT: current keys + allowed generic current fallbacks
          // const qtyCurr = sumCurrOnly(rows, ["quantity_month2", "quantity_curr", "quantity"]);
          // const nsCurr = sumCurrOnly(rows, ["net_sales_month2", "net_sales_curr", "net_sales"]);
          // const profitCurr = sumCurrOnly(rows, ["profit_month2", "profit_curr", "profit"]);

          // ✅ ASP must be weighted: ΣNetSales / ΣQty
          const aspPrev = qtyPrev > 0 ? nsPrev / qtyPrev : null;
          const aspCurr = qtyCurr > 0 ? nsCurr / qtyCurr : null;

          // ✅ Unit profitability must be weighted: ΣProfit / ΣQty
          const unitProfitPrev = qtyPrev > 0 ? profitPrev / qtyPrev : null;
          const unitProfitCurr = qtyCurr > 0 ? profitCurr / qtyCurr : null;

          // pct already exists in your scope; keep it
          const unitGrowthPct = pct(qtyPrev, qtyCurr);
          const aspGrowthPct = aspPrev != null && aspCurr != null ? pct(aspPrev, aspCurr) : null;
          const salesGrowthPct = pct(nsPrev, nsCurr);
          const unitProfitGrowthPct =
            unitProfitPrev != null && unitProfitCurr != null
              ? pct(unitProfitPrev, unitProfitCurr)
              : null;

          const profitGrowthPct = pct(profitPrev, profitCurr);

          return {
            unit: renderGrowthOrNA(asGrowth(unitGrowthPct)),
            asp: renderGrowthOrNA(asGrowth(aspGrowthPct)),
            sales: renderGrowthOrNA(asGrowth(salesGrowthPct)),

            // Total mix is always 100% if there are sales; change is 0
            mixChange: renderGrowthOrNA(asGrowth(0)),

            unitProfit: renderGrowthOrNA(asGrowth(unitProfitGrowthPct)),
            profit: renderGrowthOrNA(asGrowth(profitGrowthPct)),
          };
        })()
        : activeTab !== "new_skus" && activeTab !== "reviving_skus"
          ? {
            unit: renderGrowthOrNA((segmentTotal as SkuItem | null | undefined)?.["Unit Growth"]),
            asp: renderGrowthOrNA((segmentTotal as SkuItem | null | undefined)?.["ASP Growth"]),
            sales: renderGrowthOrNA((segmentTotal as SkuItem | null | undefined)?.["Sales Growth"]),
            mixChange: renderGrowthOrNA((segmentTotal as SkuItem | null | undefined)?.["Sales Mix Change"]),
            unitProfit: renderGrowthOrNA((segmentTotal as SkuItem | null | undefined)?.["Profit Per Unit"]),
            profit: renderGrowthOrNA((segmentTotal as SkuItem | null | undefined)?.["CM1 Profit Impact"]),
          }
          : {
            unit: "-",
            asp: "-",
            sales: "-",
            unitProfit: "-",
            profit: "-",
          })
      ,
      ...(Object.keys(skuInsights).length > 0 ? { ai: '' } : {}),
    };

    return [...rows, totalRow];
  }, [
    rowsToRender,
    activeTab,
    month2Label,
    skuInsights,
    segmentTotal,
    manualTotalsForAll,
    manualTotalsForNewRev,
    showAllSkus,
    allSkuRows,
    currentTabData,
  ]);


  const rowClassNameForDataTable = (row: BIGridRow) => {
    if (row.__isTotal) {
      return 'bg-[#D9D9D9] font-bold';
    }
    return 'bg-white';
  };

  const extractActionLines = (text: string): string[] => {
    if (!text) return [];

    return text
      .split(/\r?\n/)
      .map(l => l.trim())
      .filter(l =>
        /^Action\s*:|^(Increase|Maintain|Decrease|Check|Review)/i.test(l)
      );
  };


  const summaryMetricPoints = overallSummary.filter(
    (b) => /%|£|\$/.test(b)
  );

  const summaryNarrative =
    overallSummary.find((b) =>
      b.toLowerCase().includes("business experienced")
    ) ||
    overallSummary[overallSummary.length - 1]; // safe fallback

  const ObjectiveCards = ({
    objective,
    isGlobal = false,
    className = "",
  }: {
    objective?: {
      growth_intent?: string;
      profit_priority?: string;
      inventory_clearance_priority?: boolean;
    } | null;
    isGlobal?: boolean;
    className?: string;
  }) => {
    const growth = isGlobal
      ? "-"
      : objective?.growth_intent?.replaceAll("_", " ") || "Not Defined";

    const profit = isGlobal
      ? "-"
      : objective?.profit_priority?.replaceAll("_", " ") || "Not Defined";

    const inv = isGlobal
      ? "-"
      : typeof objective?.inventory_clearance_priority === "boolean"
        ? objective.inventory_clearance_priority
          ? "Yes"
          : "No"
        : "Not Defined";

    const Card = ({ label, value }: { label: string; value: string }) => (
      <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
        <div className="2xl:text-xs text-[10px] text-charcoal-500">
          {label}
        </div>

        <div className="mt-1 text-sm 2xl:text-base font-semibold text-charcoal-500 capitalize">
          {value}
        </div>
      </div>
    );

    return (
      <div className={`grid grid-cols-1 gap-5 ${className}`}>
        <Card label="Growth" value={growth} />
        <Card label="Profit" value={profit} />
        <Card label="Inventory Dilution" value={inv} />
      </div>
    );
  };

  // =========================
  // Render
  // =========================

  const topBorderColors = ["border-t-blue-500", "border-t-amber-500", "border-t-emerald-500", "border-t-rose-500"];

  const parseMetricNumber = (value?: string): number => {
    if (!value) return 0;

    // Takes only the main numeric part before the percentage bracket
    const mainPart = value.split("(")[0].trim();

    // Remove currency symbols, commas, spaces etc.
    const numeric = Number(mainPart.replace(/[^\d.-]/g, ""));

    return Number.isFinite(numeric) ? numeric : 0;
  };

  const toTitleCase = (value: string) =>
    String(value || "")
      .toLowerCase()
      .replace(/\b\w/g, (char) => char.toUpperCase());

  const getGrowthValue = (row: any, key: string) => {
    const raw = row?.[key];
    const value = typeof raw === "object" ? raw?.value : raw;
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
  };

  const formatGrowth = (value: number) => {
    const sign = value > 0 ? "+" : "";
    return `(${sign}${value.toFixed(2)}%)`;
  };

  const formatGlobalMetricValue = (
    value: number,
    growth: number,
    type: "money" | "number" = "money"
  ) => {
    const main =
      type === "number"
        ? Number(value || 0).toLocaleString()
        : formatDisplayAmount(Number(value || 0));

    return `${main} ${formatGrowth(growth)}`;
  };

  const getGlobalProductJourney = (productName: string) => {
    const key = String(productName || "").trim().toLowerCase();

    const direct = (skuInsights as any)?.[key]?.raw_global_journey;
    if (direct) return direct;

    const entry = Object.values(skuInsights || {}).find((item: any) => {
      const product = String(item?.product_name || "").trim().toLowerCase();
      return product === key || product.includes(key) || key.includes(product);
    }) as any;

    return entry?.raw_global_journey || null;
  };

  const getFirstCountryAction = (journey: any, country: "uk" | "us") => {
    const countryObj = journey?.[country] || {};
    return Object.values(countryObj || {})[0] as any;
  };

  const globalRecommendationCards = useMemo(() => {
    if (!isGlobalData()) return [];

    const isOthersCardName = (name: string) => {
      const value = String(name || "").trim().toLowerCase();

      return (
        value === "others" ||
        value === "other" ||
        value === "other skus" ||
        value === "other sku" ||
        value === "other products" ||
        value === "other product"
      );
    };

    const getNetSalesValue = (row: any) => {
      return Number(
        row?.net_sales_curr ??
        row?.net_sales_month2 ??
        row?.net_sales ??
        0
      );
    };

    // ✅ only Top 80 SKUs for top cards
    const top80Rows = [...(categorizedGrowth.top_80_skus || [])]
      .filter((row) => !isTotalLikeRow(row))
      .filter((row) => !isOthersCardName(row.product_name || ""))
      .sort((a, b) => getNetSalesValue(b) - getNetSalesValue(a))
      .slice(0, 5);

    // ✅ build one Other SKUs row from other_total if available,
    // otherwise manually aggregate categorizedGrowth.other_skus
    const otherRows = [...(categorizedGrowth.other_skus || [])].filter(
      (row) => !isTotalLikeRow(row)
    );

    const otherCardRow =
      categorizedGrowth.other_total ||
      otherRows.find((row) => isOthersCardName(row.product_name || "")) ||
      (otherRows.length
        ? otherRows.reduce(
          (acc: any, row: any) => {
            acc.product_name = "Other Skus";

            acc.quantity_curr += Number(
              row?.quantity_curr ??
              row?.quantity_month2 ??
              row?.quantity ??
              0
            );

            acc.net_sales_curr += Number(
              row?.net_sales_curr ??
              row?.net_sales_month2 ??
              row?.net_sales ??
              0
            );

            acc.profit_curr += Number(
              row?.profit_curr ??
              row?.profit_month2 ??
              row?.profit ??
              0
            );

            const qty = Number(
              row?.quantity_curr ??
              row?.quantity_month2 ??
              row?.quantity ??
              0
            );

            const asp = Number(
              row?.asp_curr ??
              row?.asp_month2 ??
              row?.asp ??
              0
            );

            const unitProfit = Number(
              row?.unit_wise_profitability_curr ??
              row?.unit_wise_profitability_month2 ??
              row?.unit_wise_profitability ??
              0
            );

            acc._aspWeighted += asp * qty;
            acc._unitProfitWeighted += unitProfit * qty;

            return acc;
          },
          {
            product_name: "Other Skus",
            quantity_curr: 0,
            net_sales_curr: 0,
            profit_curr: 0,
            _aspWeighted: 0,
            _unitProfitWeighted: 0,
          }
        )
        : null);

    if (otherCardRow && otherCardRow.quantity_curr) {
      otherCardRow.asp_curr =
        Number(otherCardRow.quantity_curr) > 0
          ? Number(otherCardRow.net_sales_curr || 0) /
          Number(otherCardRow.quantity_curr)
          : 0;

      otherCardRow.unit_wise_profitability_curr =
        Number(otherCardRow.quantity_curr) > 0
          ? Number(otherCardRow.profit_curr || 0) /
          Number(otherCardRow.quantity_curr)
          : 0;
    }

    // ✅ final output: Top 5 + Other Skus at end
    const finalRows = otherCardRow
      ? [...top80Rows, otherCardRow]
      : top80Rows;

    return finalRows.map((row) => {
      const productName = row.product_name || "";
      const journey = getGlobalProductJourney(productName);

      const ukAction = getFirstCountryAction(journey, "uk");
      const usAction = getFirstCountryAction(journey, "us");

      const metrics = [
        {
          label: "Units",
          value: formatGlobalMetricValue(
            Number(row.quantity_curr || row.quantity_month2 || row.quantity || 0),
            getGrowthValue(row, "Unit Growth (%)"),
            "number"
          ),
        },
        {
          label: "Net sales",
          value: formatGlobalMetricValue(
            Number(row.net_sales_curr || row.net_sales_month2 || row.net_sales || 0),
            getGrowthValue(row, "Net Sales Growth (%)")
          ),
        },
        {
          label: "ASP",
          value: formatGlobalMetricValue(
            Number(row.asp_curr || row.asp_month2 || row.asp || 0),
            getGrowthValue(row, "ASP Growth (%)")
          ),
        },
        {
          label: "CM1 profit",
          value: formatGlobalMetricValue(
            Number(row.profit_curr || row.profit_month2 || row.profit || 0),
            getGrowthValue(row, "CM1 Profit Impact (%)")
          ),
        },
        {
          label: "CM1 profit per unit",
          value: formatGlobalMetricValue(
            Number(
              row.unit_wise_profitability_curr ||
              row.unit_wise_profitability_month2 ||
              row.unit_wise_profitability ||
              0
            ),
            getGrowthValue(row, "Profit Per Unit (%)")
          ),
        },
      ];

      const recommendationPoints = isOthersCardName(productName)
        ? [
          remainingSkusBlock ||
          "Monitor the remaining SKUs and prioritize actions based on visibility, ASP, units, net sales, and CM1 profit.",
        ].filter(Boolean)
        : [
          ukAction?.recommendation ? `UK: ${ukAction.recommendation}` : "",
          usAction?.recommendation ? `US: ${usAction.recommendation}` : "",
        ].filter(Boolean);

      const advertisingPoints = [
        ukAction?.ads_recommendation ? `UK: ${ukAction.ads_recommendation}` : "",
        usAction?.ads_recommendation ? `US: ${usAction.ads_recommendation}` : "",
      ].filter(Boolean);

      const inventoryPoints = [
        ukAction?.inventory_recommendation ? `UK: ${ukAction.inventory_recommendation}` : "",
        usAction?.inventory_recommendation ? `US: ${usAction.inventory_recommendation}` : "",
      ].filter(Boolean);

      const journeyPoints = Array.isArray(journey?.journey_comparison)
        ? journey.journey_comparison
        : [];

      return {
        key: productName,
        productName: toTitleCase(productName),
        metrics,
        journeyPoints,
        recommendationPoints,
        advertisingPoints,
        inventoryPoints,
      };
    });
  }, [
    categorizedGrowth.top_80_skus,
    categorizedGrowth.other_skus,
    categorizedGrowth.other_total,
    skuInsights,
    displayCurrency,
    remainingSkusBlock,
  ]);

  const sortedRecommendations = useMemo(() => {
    const cards = Object.entries(recommendedActions)
      .map(([key, text]) => {
        const parsed = parseRecommendedAction(text);

        const netSalesMetric = parsed.metrics.find(
          (m) => m.label.trim().toLowerCase() === "net sales"
        );

        const netSales = parseMetricNumber(netSalesMetric?.value);

        return {
          key,
          text,
          parsed,
          netSales,
          isOthers: isOthersCardName(parsed.productName),
        };
      });

    const othersCard = cards.find((card) => card.isOthers);

    const top5Cards = cards
      .filter((card) => !card.isOthers)
      .sort((a, b) => b.netSales - a.netSales)
      .slice(0, 5);

    return othersCard ? [...top5Cards, othersCard] : top5Cards;
  }, [recommendedActions, convertMetricValueString, displayCurrency]);

  const parseGlobalInventoryItems = (inventoryText: string) => {
    const lines = String(inventoryText || "")
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);

    const result: Record<
      "uk" | "us",
      {
        ageingInventory?: string;
        estimatedStorageCost?: string;
        unfulfillableInventory?: string;
        highCoverage?: string;
      }
    > = {
      uk: {},
      us: {},
    };

    let currentCountry: "uk" | "us" | null = null;

    for (const line of lines) {
      const clean = line.replace(/^[-•]\s*/, "").trim();

      if (/^##\s*UK/i.test(clean)) {
        currentCountry = "uk";
        continue;
      }

      if (/^##\s*US/i.test(clean)) {
        currentCountry = "us";
        continue;
      }

      if (/^##\s*INVENTORY/i.test(clean)) continue;
      if (!currentCountry) continue;

      if (/ageing inventory/i.test(clean)) {
        const match = clean.match(/:\s*(.+)$/);
        result[currentCountry].ageingInventory = match?.[1]?.trim() || clean;
        continue;
      }

      if (/est\.?\s*storage cost|estimated storage cost/i.test(clean)) {
        const match = clean.match(/:\s*(.+)$/);
        result[currentCountry].estimatedStorageCost = match?.[1]?.trim() || clean;
        continue;
      }

      if (/unfulfillable/i.test(clean)) {
        const match = clean.match(/:\s*(.+)$/);
        result[currentCountry].unfulfillableInventory =
          match?.[1]?.trim() ||
          clean.replace(/^Unfulfillable inventory\s*/i, "").trim();
        continue;
      }

      if (/high coverage/i.test(clean)) {
        const match = clean.match(/:\s*(.+)$/);
        result[currentCountry].highCoverage = match?.[1]?.trim() || clean;
        continue;
      }
    }

    return result;
  };

  const parseSingleCountryInventoryItems = (inventoryText: string) => {
    const lines = String(inventoryText || "")
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => line.replace(/^[-•]\s*/, "").trim());

    const result: {
      ageingInventory?: string;
      highCoverage?: string;
      unfulfillableInventory?: string;
      estimatedStorageCost?: string;
      detailNote?: string;
    } = {};

    for (const line of lines) {
      if (/^##\s*INVENTORY/i.test(line)) continue;

      if (/ageing inventory/i.test(line)) {
        const match = line.match(/:\s*(.+)$/);
        result.ageingInventory = match?.[1]?.trim() || line.replace(/^Ageing inventory\s*/i, "").trim();
        continue;
      }

      if (/high coverage/i.test(line)) {
        const match = line.match(/:\s*(.+)$/);
        result.highCoverage = match?.[1]?.trim() || line.replace(/^High coverage SKUs\s*/i, "").trim();
        continue;
      }

      if (/unfulfillable/i.test(line)) {
        const match = line.match(/:\s*(.+)$/);
        result.unfulfillableInventory = match?.[1]?.trim() || line;
        continue;
      }

      if (/storage cost|est\.?\s*storage/i.test(line)) {
        const match = line.match(/:\s*(.+)$/);
        result.estimatedStorageCost = match?.[1]?.trim() || line.replace(/^Est\.?\s*storage cost next month\s*/i, "").trim();
        continue;
      }

      if (/inventory reconciliation/i.test(line) || /for detailed inventory insights/i.test(line)) {
        result.detailNote = line;
        continue;
      }
    }

    return result;
  };

  const getInventoryAccentClass = (country?: string) => {
    const c = String(country || "").toLowerCase();

    if (c === "uk") return "border-l-[#7B9A6D]";
    if (c === "us") return "border-l-[#3A8EA4]";
    if (c === "ca") return "border-l-[#D97706]";
    if (c === "india") return "border-l-[#8B5CF6]";

    return "border-l-[#5EA68E]";
  };

  const SingleCountryInventoryInsights = () => {
    const inventory = parseSingleCountryInventoryItems(portfolioInventoryBlock);

    const rows = [
      {
        label: "Ageing Inventory (181+ Days)",
        value: inventory.ageingInventory,
      },
      {
        label: "High Coverage SKUs",
        value: inventory.highCoverage,
      },
      {
        label: "Unfulfillable Inventory Remains Below 1%",
        value: inventory.unfulfillableInventory,
      },
      {
        label: "Est. Storage Cost Next Month",
        value: inventory.estimatedStorageCost,
      },
      {
        label: "For Detailed Inventory Insights, Please Refer To The Inventory Reconciliation Tab.",
        value: inventory.detailNote ? "" : undefined,
        fullWidth: true,
      },
    ].filter((row) => row.value !== undefined);

    if (!rows.length) return null;

    const countryTitle =
      normalizedCountry === "uk"
        ? "UK Inventory"
        : normalizedCountry === "us"
          ? "US Inventory"
          : normalizedCountry === "ca"
            ? "CA Inventory"
            : `${titleCountry} Inventory`;

    return (
      <div className="space-y-4 rounded-xl border border-slate-200 bg-white shadow-sm p-4">
        <div className="flex items-center gap-2">
          <span className="text-base 2xl:text-2xl font-bold text-slate-800">
            Inventory Insights
          </span>
        </div>

        <div
          className={[
            "rounded-xl border border-slate-200 bg-white shadow-sm overflow-hidden border-l-4",
            getInventoryAccentClass(normalizedCountry),
          ].join(" ")}
        >
          <div className="border-b border-slate-100 bg-slate-50 px-4 py-2">
            <div className="text-sm font-bold text-slate-800">
              {countryTitle}
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-2 p-3">
            {rows.map((item, idx) => (
              <div
                key={idx}
                className={[
                  "flex min-h-[38px] items-center justify-between gap-3 rounded-lg border border-amber-100 bg-white px-3 py-2",
                  item.fullWidth ? "md:col-span-1" : "",
                ].join(" ")}
              >
                <span className="text-sm font-medium text-slate-700">
                  {item.label}
                </span>

                {item.value && (
                  <span className="text-sm font-bold text-[#414042] text-right">
                    {item.value}
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  };

  const GlobalInventoryInsights = () => {
    const inventory = parseGlobalInventoryItems(portfolioInventoryBlock);

    const InventoryCountryCard = ({
      title,
      items,
      accentClass,
    }: {
      title: string;
      items: {
        ageingInventory?: string;
        estimatedStorageCost?: string;
        unfulfillableInventory?: string;
        highCoverage?: string;
      };
      accentClass: string;
    }) => {
      const rows = [
        {
          label: "Ageing Inventory",
          value: items.ageingInventory,
        },
        {
          label: "Estimated Storage Cost",
          value: items.estimatedStorageCost,
        },
        {
          label: "Unfulfillable Inventory",
          value: items.unfulfillableInventory,
        },
        {
          label: "High Coverage SKUs",
          value: items.highCoverage,
        },
      ].filter((row) => row.value);

      if (!rows.length) return null;

      return (
        <div
          className={`rounded-xl border border-slate-200 bg-white shadow-sm overflow-hidden border-l-4 ${accentClass}`}
        >
          <div className="border-b border-slate-100 bg-slate-50 px-4 py-2">
            <div className="text-sm font-bold text-slate-800">{title}</div>
          </div>

          <div className="grid grid-cols-1 min-[1700px]:grid-cols-2 gap-2 p-3">
            {rows.map((item, idx) => (
              <div
                key={idx}
                className="flex min-h-[38px] items-center justify-between gap-3 rounded-lg border border-amber-100 bg-white px-3 py-2"
              >
                <span className="text-sm font-medium text-slate-700">
                  {item.label}
                </span>

                <span className="text-sm font-bold text-[#414042] text-right">
                  {item.value}
                </span>
              </div>
            ))}
          </div>
        </div>
      );
    };

    const hasInventory =
      Object.values(inventory.uk).some(Boolean) ||
      Object.values(inventory.us).some(Boolean);

    if (!hasInventory) return null;

    return (
      <div className="space-y-4 rounded-xl border border-slate-200 bg-white shadow-sm p-4">
        <div className="flex items-center gap-2">
          <span className="text-base 2xl:text-2xl font-bold text-slate-800">
            Inventory Insights
          </span>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <InventoryCountryCard
            title="UK Inventory"
            items={inventory.uk}
            accentClass={getInventoryAccentClass("uk")}
          />

          <InventoryCountryCard
            title="US Inventory"
            items={inventory.us}
            accentClass={getInventoryAccentClass("us")}
          />
        </div>
      </div>
    );
  };

  const getSkuEmptyMessage = () => {
    if (activeTab === "new_skus") {
      return "No new SKU has been launched within the last 6 months.";
    }

    if (activeTab === "reviving_skus") {
      return "No reviving SKU was identified.";
    }

    return "No data found.";
  };

  return (
    <>
      <style>{`
        div{ font-family: 'Lato', sans-serif; }
        select{ outline: none; }

        .styled-button, .compare-button{
          padding:8px 16px; font-size:.9rem; border:none; border-radius:6px; cursor:pointer;
          transition:background-color .2s ease; box-shadow:0 3px 6px rgba(0,0,0,.15);
          background-color:#2c3e50; color:#f8edcf; font-weight:bold;
        }
        .styled-button:hover, .compare-button:hover{ background-color:#1f2a36; }
      `}</style>

      {pageLoading ? (
        <div className="flex flex-col items-center justify-center py-12 text-center">
          <Loader fullscreen transparent />
        </div>
      ) : (
        <div className="mt-2 md:mt-4 flex flex-col ">
          {error && <p style={{ color: 'red' }}>{error}</p>}

          {(summaryText ||
            overallSummary.length > 0 ||
            Object.keys(recommendedActions).length > 0 ||
            overallActions.length > 0 ||
            parsedPortfolioInventory.inventoryBullets.length > 0 ||
            !!parsedPortfolioInventory.summaryText) && (

              <div className="flex gap-4 flex-col">

                {/* 1) Monthly Objective */}
                <div className="flex flex-col lg:flex-row gap-4 items-stretch">
                  <div className="flex-1">
                    {(summaryText || overallSummary.length > 0 || portfolioRecommendation) && (
                      <div className="bg-white border border-[#D9D9D9] rounded-xl shadow-sm p-4 text-xs 2xl:text-sm text-charcoal-500 w-full h-full flex flex-col">
                        <PageBreadcrumb
                          pageTitle={isGlobalData() ? "Global Business Summary" : "Business Summary"}
                          variant="page"
                          align="left"
                        />

                        {summaryText && (
                          <div className="mt-3 2xl:text-sm text-xs text-charcoal-500 border-slate-300 flex-1">
                            {summaryText}
                          </div>
                        )}

                        {portfolioRecommendation && (
                          <div className="mt-3 p-3 rounded-lg bg-slate-50 border border-slate-200 flex flex-col sm:flex-row items-start sm:items-center gap-1">
                            <span className="2xl:text-sm text-xs font-semibold text-charcoal-500">
                              Portfolio Recommendation:
                            </span>
                            <span className="2xl:text-sm text-xs text-charcoal-600 leading-relaxed">
                              {portfolioRecommendation}
                            </span>
                          </div>
                        )}
                      </div>
                    )}
                  </div>

                  <div className="lg:w-1/3 flex">
                    <div className="bg-white border border-[#D9D9D9] rounded-xl shadow-sm p-4 text-xs 2xl:text-sm text-charcoal-600 w-full h-full flex flex-col">
                      <PageBreadcrumb
                        pageTitle="Monthly Objective"
                        variant="page"
                        align="left"
                      />

                      <ObjectiveCards
                        objective={objectiveContext}
                        isGlobal={isGlobalData()}
                        className="mt-3 flex-1"
                      />
                    </div>
                  </div>
                </div>

                {/* 3) Recommended Actions (cards) */}
                {(
                  isGlobalData()
                    ? globalRecommendationCards.length > 0
                    : recommendedActions && Object.keys(recommendedActions).length > 0
                ) && (
                    <div className="bg-white border border-[#D9D9D9] rounded-xl shadow-sm p-4 text-xs 2xl:text-sm text-charcoal-600 w-full">
                      <PageBreadcrumb pageTitle="Recommendations" variant="page" align="left" />

                      <div className="mt-3 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
                        {isGlobalData()
                          ? globalRecommendationCards.map((card, idx) => {
                            return (
                              <motion.div
                                key={card.key}
                                initial={{ opacity: 0, y: 16 }}
                                animate={{ opacity: 1, y: 0 }}
                                transition={{ duration: 0.35, delay: idx * 0.06 }}
                                className={[
                                  "bg-white rounded-xl border border-slate-200 shadow-sm hover:shadow-md transition-shadow",
                                  "border-t-4",
                                  "p-3 space-y-3",
                                ].join(" ")}
                              >
                                <div className="flex items-start justify-between gap-3">
                                  <div className="text-sm font-semibold text-slate-800 line-clamp-2">
                                    {idx + 1}. {card.productName}
                                  </div>

                                  <button
                                    type="button"
                                    onClick={() => {
                                      setSelectedRec({
                                        productName: card.productName,
                                        metrics: card.metrics,
                                        journeyPoints: card.journeyPoints,
                                        recommendationPoints: card.recommendationPoints,
                                        advertisingPoints: card.advertisingPoints,
                                        inventoryPoints: card.inventoryPoints,
                                        showChart: !isOthersCardName(card.productName),
                                      });
                                      setRecDrawerOpen(true);
                                    }}
                                    className="px-3 py-1.5 rounded-lg text-xs font-semibold bg-slate-800 text-yellow-200 hover:bg-slate-700 transition whitespace-nowrap"
                                  >
                                    Detailed View
                                  </button>
                                </div>

                                {card.metrics?.length > 0 && (
                                  <div className="grid grid-cols-3 gap-2">
                                    {card.metrics.map((m, i) => (
                                      <div
                                        key={i}
                                        className="rounded-lg border border-slate-200 bg-slate-50 py-2 px-1 min-w-0"
                                      >
                                        <div className="text-[10px] 2xl:text-xs text-slate-500 leading-none truncate">
                                          {m.label}
                                        </div>

                                        <div className="mt-1 flex flex-col min-[1700px]:flex-row 2xl:items-baseline gap-0.5 2xl:gap-1 min-w-0 font-bold text-[10px] 2xl:text-xs">
                                          {(() => {
                                            const match = m.value.match(/^([^\(]+)\s*(\(.+\))?$/);
                                            const mainValue = match?.[1]?.trim() || m.value;
                                            const percentPart = match?.[2] || "";
                                            const isNegative = percentPart.includes("-");
                                            const percentColor = isNegative ? "#FF5C5C" : "#5EA68E";

                                            return (
                                              <>
                                                <span className="text-slate-900 truncate">
                                                  {mainValue}
                                                </span>

                                                {percentPart && (
                                                  <span className="shrink-0" style={{ color: percentColor }}>
                                                    {percentPart}
                                                  </span>
                                                )}
                                              </>
                                            );
                                          })()}
                                        </div>
                                      </div>
                                    ))}
                                  </div>
                                )}

                                {card.recommendationPoints?.length > 0 && (
                                  <div className="space-y-1 text-xs 2xl:text-sm text-slate-700 leading-relaxed">
                                    {card.recommendationPoints.map((line, i) => (
                                      <p key={i}>{line}</p>
                                    ))}
                                  </div>
                                )}
                              </motion.div>
                            );
                          })
                          : sortedRecommendations.map(({ key, text, parsed, netSales }, idx) => {
                            const recommendationPoints = parsed.recommendationPoints;

                            return (
                              <motion.div
                                key={key}
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
                                  <div className="text-sm font-semibold text-slate-800 line-clamp-2">
                                    {idx + 1}. {parsed.productName}
                                  </div>

                                  <button
                                    type="button"
                                    onClick={() => {
                                      setSelectedRec({
                                        productName: parsed.productName,
                                        metrics: parsed.metrics,
                                        journeyPoints: parsed.journeyPoints,
                                        recommendationPoints: parsed.recommendationPoints,
                                        advertisingPoints: parsed.advertisingPoints,
                                        inventoryPoints: parsed.inventoryPoints,
                                        showChart: true,
                                      });
                                      setRecDrawerOpen(true);
                                    }}
                                    className="px-3 py-1.5 rounded-lg text-xs font-semibold bg-blue-700 text-yellow-200 hover:bg-slate-700 transition whitespace-nowrap"
                                  >
                                    Detailed View
                                  </button>
                                </div>

                                {parsed.metrics?.length > 0 && (
                                  <div className="grid grid-cols-3 gap-2">
                                    {parsed.metrics.map((m, i) => (
                                      <div
                                        key={i}
                                        className="rounded-lg border border-slate-200 bg-slate-50 py-2 px-1 min-w-0"
                                      >
                                        <div className="text-[10px] 2xl:text-xs text-slate-500 leading-none truncate">
                                          {m.label}
                                        </div>

                                        <div className="mt-1 flex flex-col min-[1700px]:flex-row 2xl:items-baseline gap-0.5 2xl:gap-1 min-w-0 font-bold text-[10px] 2xl:text-xs">
                                          {(() => {
                                            const match = m.value.match(/^([^\(]+)\s*(\(.+\))?$/);
                                            const mainValue = match?.[1]?.trim() || m.value;
                                            const percentPart = match?.[2] || "";
                                            const isNegative = percentPart.includes("-");
                                            const percentColor = isNegative ? "#FF5C5C" : "#5EA68E";

                                            return (
                                              <>
                                                <span className="text-slate-900 truncate">
                                                  {mainValue}
                                                </span>

                                                {percentPart && (
                                                  <span className="shrink-0" style={{ color: percentColor }}>
                                                    {percentPart}
                                                  </span>
                                                )}
                                              </>
                                            );
                                          })()}
                                        </div>
                                      </div>
                                    ))}
                                  </div>
                                )}

                                {recommendationPoints?.length > 0 && (
                                  <div className="text-xs 2xl:text-sm text-slate-700 leading-relaxed">
                                    <div className="line-clamp-2">
                                      {recommendationPoints[0]}
                                    </div>
                                  </div>
                                )}
                              </motion.div>
                            );
                          })}

                        {!isGlobalData() && remainingSkusBlock?.trim() && (() => {
                          const parsedOther = parseOtherSkusBlock(remainingSkusBlock);
                          const otherIdx = Object.keys(recommendedActions).length;
                          const borderColor = topBorderColors[otherIdx % topBorderColors.length];

                          return (
                            <motion.div
                              key="other-skus-card"
                              initial={{ opacity: 0, y: 16 }}
                              animate={{ opacity: 1, y: 0 }}
                              transition={{ duration: 0.35, delay: 0.06 * otherIdx }}
                              className={[
                                "bg-white rounded-xl border border-slate-200 shadow-sm hover:shadow-md transition-shadow",
                                "border-t-4",
                                "p-3 space-y-3",
                              ].join(" ")}
                            >
                              <div className="flex items-start justify-between gap-3">
                                <div className="text-sm font-semibold text-slate-800 line-clamp-2">
                                  {otherIdx + 1}. {parsedOther.productName}
                                </div>

                                <button
                                  type="button"
                                  onClick={() => {
                                    setSelectedRec({
                                      productName: parsedOther.productName,
                                      metrics: parsedOther.metrics,
                                      journeyPoints: parsedOther.journeyPoints,
                                      recommendationPoints: parsedOther.recommendationPoints,
                                      advertisingPoints: parsedOther.advertisingPoints,
                                      inventoryPoints: parsedOther.inventoryPoints,
                                      showChart: false,
                                    });
                                    setRecDrawerOpen(true);
                                  }}
                                  className="px-3 py-1.5 rounded-lg text-xs font-semibold bg-blue-700 text-yellow-200 hover:bg-slate-700 transition whitespace-nowrap"
                                >
                                  Detailed View
                                </button>
                              </div>

                              {parsedOther.metrics?.length > 0 && (
                                <div className="grid grid-cols-3 gap-2">
                                  {parsedOther.metrics.map((m, i) => (
                                    <div
                                      key={i}
                                      className="rounded-lg border border-slate-200 bg-slate-50 py-2 px-1 min-w-0"
                                    >
                                      <div className="text-[10px] 2xl:text-xs text-slate-500 leading-none truncate">
                                        {m.label}
                                      </div>

                                      <div className="mt-1 flex flex-col min-[1700px]:flex-row 2xl:items-baseline gap-0.5 2xl:gap-1 min-w-0 font-bold text-[10px] 2xl:text-xs">
                                        {(() => {
                                          const match = m.value.match(/^([^\(]+)\s*(\(.+\))?$/);
                                          const mainValue = match?.[1]?.trim() || m.value;
                                          const percentPart = match?.[2] || "";
                                          const isNegative = percentPart.includes("-");
                                          const percentColor = isNegative ? "#FF5C5C" : "#5EA68E";

                                          return (
                                            <>
                                              <span className="text-slate-900 truncate">
                                                {mainValue}
                                              </span>
                                              {percentPart && (
                                                <span
                                                  className="shrink-0"
                                                  style={{ color: percentColor }}
                                                >
                                                  {percentPart}
                                                </span>
                                              )}
                                            </>
                                          );
                                        })()}
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              )}

                              {parsedOther.recommendationPoints?.length > 0 && (
                                <div className="text-xs 2xl:text-sm text-slate-700 leading-relaxed">
                                  <div className="line-clamp-2">
                                    {parsedOther.recommendationPoints[0]}
                                  </div>
                                </div>
                              )}
                            </motion.div>
                          );
                        })()}
                      </div>
                    </div>
                  )}

                {/* 4) Inventory Insight */}
                {isGlobalData() ? (
                  <GlobalInventoryInsights />
                ) : (
                  <SingleCountryInventoryInsights />
                )}

              </div>
            )}

          <div>
            <div className="mt-4 rounded-xl border bg-white p-4 shadow-sm">

              <div className="flex flex-col gap-4">
                {/* MOBILE HEADER */}
                <div className="flex items-center justify-between xl:hidden">
                  <PageBreadcrumb
                    pageTitle="SKU Analysis"
                    variant="page"
                    align="left"
                  />

                  <div className="flex items-center gap-2">
                    <AiButton
                      onClick={analyzeSkus}
                      disabled={
                        loadingInsight ||
                        !["top_80_skus", "new_skus", "reviving_skus", "other_skus"].some(
                          (k) =>
                            (categorizedGrowth[k as keyof CategorizedGrowth] as SkuItem[])?.length > 0
                        )
                      }
                    >
                      {loadingInsight ? "Generating..." : "AI Insights"}
                    </AiButton>

                    <DownloadIconButton
                      onClick={handleSkuAnalysisDownload}
                      className="transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                    />
                  </div>
                </div>

                {/* MOBILE TABS */}
                <div className="xl:hidden">
                  <SegmentedToggle<TabKey>
                    value={activeTab}
                    options={tabOptions}
                    onChange={handleTabChange}
                    className="bg-white"
                    textSizeClass="text-xs 2xl:text-sm"
                  />
                </div>

                {/* DESKTOP HEADER */}
                <div className="hidden xl:flex xl:items-center xl:justify-between xl:gap-6">
                  <PageBreadcrumb
                    pageTitle="SKU Analysis"
                    variant="page"
                    align="left"
                  />

                  <div className="flex items-center gap-3">
                    <SegmentedToggle<TabKey>
                      value={activeTab}
                      options={tabOptions}
                      onChange={handleTabChange}
                      className="bg-white"
                      textSizeClass="text-xs 2xl:text-sm"
                    />

                    <AiButton
                      onClick={analyzeSkus}
                      disabled={
                        loadingInsight ||
                        !["top_80_skus", "new_or_reviving_skus", "other_skus"].some(
                          (k) =>
                            (categorizedGrowth[k as keyof CategorizedGrowth] as SkuItem[])?.length > 0
                        )
                      }
                    >
                      {loadingInsight ? "Generating..." : "AI Insights"}
                    </AiButton>

                    <DownloadIconButton
                      onClick={handleSkuAnalysisDownload}
                      className="transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                    />
                  </div>
                </div>
              </div>

              {hasAnySkus ? (
                <div className="pt-6">
                  <DataTable<BIGridRow>
                    columns={columns}
                    data={tableData}
                    stickyHeader
                    zebra
                    paginate
                    pageSize={10}
                    maxHeight="60vh"
                    loading={false}
                    headerMaxWidth={140}
                    emptyMessage={getSkuEmptyMessage()}
                    rowClassName={rowClassNameForDataTable}
                  />
                </div>
              ) : (
                <div className="pt-6 text-sm text-gray-500">
                  No SKUs found for this period / country. Try changing the period or checking if orders exist.
                </div>

              )}
              <div className="flex justify-center mt-2">
                <div
                  className="
      grid grid-cols-2 gap-x-6 gap-y-2
      sm:grid-cols-4
      lg:flex lg:items-center lg:gap-10 lg:flex-wrap
      text-xs 2xl:text-sm text-[#414042] mt-1
      justify-items-start
    "
                >

                  <span className="inline-flex items-center gap-2">
                    <span className="inline-flex items-center gap-2 text-[#5EA68E] font-bold">
                      <FaArrowUp className="text-[10px] 2xl:text-xs" /> High growth
                    </span>
                  </span>

                  <span className="inline-flex items-center gap-2">
                    <span className="inline-flex items-center gap-2 text-[#FF5C5C] font-bold">
                      <FaArrowDown className="text-[10px] 2xl:text-xs" /> Negative growth
                    </span>
                  </span>

                  <span className="inline-flex items-center gap-2 font-bold whitespace-nowrap">
                    <span className="text-[#414042] inline-flex items-center gap-1">
                      <FaArrowUp className="text-[10px] 2xl:text-xs" /> + /
                      <FaArrowDown className="text-[10px] 2xl:text-xs" /> -
                    </span>
                    Low growth
                  </span>

                  <span className="inline-flex items-center gap-2 font-bold">
                    <span className="text-sm 2xl:text-base leading-none">-</span>
                    Past data for SKU is not available
                  </span>
                </div>
              </div>


            </div>
          </div>
        </div>
      )}
      {/* <SkuRecommendationDrawer
        open={recDrawerOpen}
        onClose={() => setRecDrawerOpen(false)}
        selectedRec={selectedRec}
        objectiveContext={objectiveContext}
        countryName={countryName}
      /> */}
      <SkuRecommendationDrawer
        open={recDrawerOpen}
        onClose={() => setRecDrawerOpen(false)}
        selectedRec={selectedRec}
        objectiveContext={objectiveContext}
        countryName={countryName}
        sourceCountryName={sourceCountryName}
        displayCurrency={displayCurrency}
      />
    </>
  );
};

