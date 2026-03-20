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
  countryName: string; // "uk" | "us" | "ca" | "global"
  sourceCountryName?: string;
  ranged: string;      // "QTD", "MTD", etc
  month: string;       // "november"
  year: string;        // "2025"
  initialData?: ApiResponse | null;
  disableAutoFetch?: boolean; // when true, LiveBusinessClient will NOT fetch
 onGenerateInsights?: () => Promise<void>;
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
  new_or_reviving_skus: SkuItem[];
  other_skus: SkuItem[];
  top_80_total?: SkuItem | null;
  new_or_reviving_total?: SkuItem | null;
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
  start_date: string;
  end_date: string;
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
  overall_actions?: string[];
  recommended_actions_mtd?: Record<string, string>;
  remaining_skus_recommendation?: string;
  remaining_skus_block?: string;
  portfolio_recommendation?: string;
}

// =========================
// Config
// =========================
const API_BASE = `${process.env.NEXT_PUBLIC_API_BASE_URL}`;

const STORAGE_KEY = 'live_bi_insight_data';
const INSIGHTS_KEY = 'live_bi_sku_insights';

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
}: MonthsforBIProps) {
  const { data: userData } = useGetUserDataQuery();

  const [gbpToUsd, setGbpToUsd] = useState(GBP_TO_USD_ENV);
  const [inrToUsd, setInrToUsd] = useState(INR_TO_USD_ENV);
  const [cadToUsd, setCadToUsd] = useState(CAD_TO_USD_ENV);

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

  const convertMetricValueString = useMemo(() => {
    return (rawValue: string, label: string) => {
      const value = String(rawValue || "").trim();
      if (!value) return value;

      // Units should not be currency-converted
      if (label.toLowerCase() === "units") return value;

      // Match: "£10.32 (+8.19%)" or "$433.54 (-9.67%)" or "10.32 (+8.19%)"
      const match = value.match(/^([£$₹€A-Z$C]*\s*[-+]?[0-9,]*\.?[0-9]+)\s*(\(.+\))?$/i);
      if (!match) return value;

      const mainPart = match[1] || "";
      const percentPart = match[2] || "";

      const numeric = Number(mainPart.replace(/[^\d.-]/g, ""));
      if (!Number.isFinite(numeric)) return value;

      const converted = convertToDisplayCurrency(numeric, sourceCurrency);
      const formatted = formatDisplayAmount(converted);

      return `${formatted}${percentPart ? ` ${percentPart}` : ""}`;
    };
  }, [convertToDisplayCurrency, formatDisplayAmount, sourceCurrency]);

  const [categorizedGrowth, setCategorizedGrowth] = useState<CategorizedGrowth>(
    {
      top_80_skus: [],
      new_or_reviving_skus: [],
      other_skus: [],
      top_80_total: null,
      new_or_reviving_total: null,
      other_total: null,
      all_skus_total: null,
    }
  );

  type CurrencyRateRow = {
    conversion_rate: number;
    country: string;
    month: string;
    selected_currency: string;
    user_currency: string;
    year: number;
  };

  const FX_RATES_GET_ENDPOINT = `${API_BASE}/currency-rates`;

  const fetchFxRates = async () => {
    try {
      const token =
        typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

      const headers: HeadersInit = { Accept: "application/json" };
      if (token) (headers as any).Authorization = `Bearer ${token}`;

      const res = await fetch(FX_RATES_GET_ENDPOINT, { method: "GET", headers });
      if (!res.ok) throw new Error(`FX rates fetch failed: ${res.status}`);

      const rows: CurrencyRateRow[] = await res.json();

      const currentMonth = String(month || "").toLowerCase();
      const currentYear = Number(year);

      const cur = (rows || []).filter(
        (r) =>
          String(r.month || "").toLowerCase() === currentMonth &&
          Number(r.year) === currentYear
      );

      const getRate = (from: string, to: string) => {
        const row = cur.find(
          (r) =>
            String(r.user_currency).toLowerCase() === from &&
            String(r.selected_currency).toLowerCase() === to
        );
        const rate = Number(row?.conversion_rate);
        return Number.isFinite(rate) && rate > 0 ? rate : null;
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

  const [activeTab, setActiveTab] = useState<
    'all_skus' | 'top_80_skus' | 'new_or_reviving_skus' | 'other_skus'
  >('all_skus');

  const tabOptions = useMemo(
    () => [
      { value: "all_skus" as const, label: "All SKUs" },
      { value: "top_80_skus" as const, label: "Top 80% SKUs" },
      { value: "new_or_reviving_skus" as const, label: "New/Reviving SKUs" },
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

    const unit = (item as any)?.["Unit Growth"]?.value;
    const asp = (item as any)?.["ASP Growth"]?.value;
    const sales =
      (item as any)?.["Sales Growth"]?.value ??
      (item as any)?.["Net Sales Growth"]?.value;
    const unitProfit = (item as any)?.["Profit Per Unit"]?.value;
    const profitImpact = (item as any)?.["CM1 Profit Impact"]?.value;

    const push = (label: string, v: any) => {
      if (v == null) return;
      const pct = fmtPct(v);
      const isNeg = String(pct).includes("-");
      m.push({
        label,
        value: `(${pct})`,
        color: isNeg ? "#FF5C5C" : "#5EA68E",
      });
    };

    push("Units", unit);
    push("ASP", asp);
    push("Net sales", sales);
    push("CM1 profit per unit", unitProfit);
    push("CM1 profit", profitImpact);

    return m;
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




  type TabKey =
    | 'top_80_skus'
    | 'new_or_reviving_skus'
    | 'other_skus'
    | 'all_skus';

  const getTabRows = (tab: TabKey): SkuItem[] => {
    if (tab === 'all_skus') return getAllSkusForExport();
    return categorizedGrowth[tab];
  };

  const getTabLabel = (key: TabKey): string =>
    key === 'top_80_skus'
      ? 'Top 80% SKUs'
      : key === 'new_or_reviving_skus'
        ? 'New/Reviving SKUs'
        : key === 'other_skus'
          ? 'Other SKUs'
          : 'All SKUs';

  const getTabNumberForFeedback = (key: TabKey): number =>
    key === 'top_80_skus'
      ? 1
      : key === 'new_or_reviving_skus'
        ? 2
        : key === 'other_skus'
          ? 3
          : 4;

  // =========================
  // Persistence helpers
  // =========================

  const saveCompareToStorage = (payload: any) => {
    if (typeof window === 'undefined') return;
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
    } catch (e) {
      console.warn('Failed to save BI compare state:', e);
    }
  };

  const loadCompareFromStorage = (): any => {
    if (typeof window === 'undefined') return null;
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch (e) {
      console.warn('Failed to load BI compare state:', e);
      return null;
    }
  };

  const saveInsightsToStorage = (insights: Record<string, SkuInsight>) => {
    if (typeof window === 'undefined') return;
    try {
      localStorage.setItem(INSIGHTS_KEY, JSON.stringify(insights || {}));
    } catch (e) {
      console.warn('Failed to save insights:', e);
    }
  };

  const loadInsightsFromStorage = (): Record<string, SkuInsight> => {
    if (typeof window === 'undefined') return {};
    try {
      const raw = localStorage.getItem(INSIGHTS_KEY);
      return raw ? JSON.parse(raw) : {};
    } catch (e) {
      console.warn('Failed to load insights:', e);
      return {};
    }
  };

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

      // ✅ keep UI working (your table uses Sales Growth, Excel uses Net Sales Growth)
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
      other_skus: [],
      top_80_total: null,
      new_or_reviving_total: null,
      other_total: null,
      all_skus_total: null,
    };

    if (!raw) return empty;

    return {
      top_80_skus: (raw.top_80_skus || []).map(mapRow),
      new_or_reviving_skus: (raw.new_or_reviving_skus || []).map(mapRow),
      other_skus: (raw.other_skus || []).map(mapRow),

      top_80_total: raw.top_80_total ? mapRow(raw.top_80_total) : null,
      new_or_reviving_total: raw.new_or_reviving_total ? mapRow(raw.new_or_reviving_total) : null,
      other_total: raw.other_total ? mapRow(raw.other_total) : null,
      all_skus_total: raw.all_skus_total ? mapRow(raw.all_skus_total) : null,
    };
  };

  const hydrateFromPayload = (payload: ApiResponse) => {
    const newPeriods = payload.periods || null;

    const rawCat = payload.categorized_growth || {
      top_80_skus: [],
      new_or_reviving_skus: [],
      other_skus: [],
    };

    const normalized = normalizeCategorizedGrowth(rawCat);
    setPeriods(newPeriods);
    setCategorizedGrowth(normalized);
    const currentLabel = newPeriods?.current_mtd?.label || '';
    setMonth2Label(currentLabel);
    const summaryObj = payload.overall_summary;
    setSummaryText(summaryObj?.summary_text || "");
    setOverallSummary(summaryObj?.metric_bullets || []);
    setOverallActions(payload.overall_actions || []);
    setRecommendedActions(payload.recommended_actions_mtd || {});
    setRemainingSkusBlock(payload.remaining_skus_block || payload.remaining_skus_recommendation || "");
    setPortfolioRecommendation((payload as any).portfolio_recommendation || "");
    setObjectiveContext(payload.objective_context || null);
    setAdsRecommendation((payload as any).ads_recommendation || "");
    setInventorySummary((payload as any).inventory_summary || null);
    const liveInsights = payload.ai_insights || {};
    if (liveInsights && Object.keys(liveInsights).length) {
      setSkuInsights(liveInsights);
      saveInsightsToStorage(liveInsights);
    }
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

  useEffect(() => {
    const saved = loadCompareFromStorage();
    const todayKey = getTodayKey();

    if (saved) {
      if (saved.categorizedGrowth) setCategorizedGrowth(saved.categorizedGrowth);
      if (saved.periods) setPeriods(saved.periods);
      if (saved.month2Label) setMonth2Label(saved.month2Label);
      if (saved.activeTab) setActiveTab(saved.activeTab);
      if (saved.portfolioRecommendation) setPortfolioRecommendation(saved.portfolioRecommendation);

      if (saved.insightDate === todayKey) {
        if (saved.overallActions) setOverallActions(saved.overallActions);
        if (saved.summaryText) setSummaryText(saved.summaryText);
        if (saved.overallSummary) setOverallSummary(saved.overallSummary);
        if (saved.objectiveContext) {
          setObjectiveContext(saved.objectiveContext);
        }

        setInsightDate(todayKey);
      }
    }

    const cachedInsights = loadInsightsFromStorage();
    if (cachedInsights && Object.keys(cachedInsights).length) {
      setSkuInsights(cachedInsights);
    }
  }, []);

  useEffect(() => {
    const saved = loadCompareFromStorage();
    if (saved) saveCompareToStorage({ ...saved, activeTab });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab]);

  // =========================
  // Fetch live BI (current MTD vs previous)
  // =========================

  const fetchLiveBi = async (generateInsights: boolean = false) => {
    setError(null);
    if (!generateInsights) {
      setSkuInsights({});
      saveInsightsToStorage({});
      setSelectedSku(null);
      setModalOpen(false);
      setPageLoading(true);
    }

    try {
      const res = await api.get<ApiResponse>('/live_mtd_bi', {
        params: {
          countryName: normalizedCountry,
          ranged,
          month,
          year,
          generate_ai_insights: generateInsights ? 'true' : 'false',
        },
      });


      const newPeriods = res.data.periods || null;
      const rawCat = res.data.categorized_growth || {
        top_80_skus: [],
        new_or_reviving_skus: [],
        other_skus: [],
      };
      const normalized = normalizeCategorizedGrowth(rawCat);

      setPeriods(newPeriods);
      setCategorizedGrowth(normalized);

      const currentLabel = newPeriods?.current_mtd?.label || '';
      setMonth2Label(currentLabel);

      const summaryObj = res.data.overall_summary;

      const summaryTextFromApi = summaryObj?.summary_text || "";
      const summaryBulletsFromApi = summaryObj?.metric_bullets || [];
      const adsRecommendation = res.data.ads_recommendation || "";
      const inventoryFromApi = res.data.inventory_summary || null;
      const remainingBlock =
        res.data.remaining_skus_block || res.data.remaining_skus_recommendation || "";

      const actionsFromApi = res.data.overall_actions || [];
      const recommendedActionsFromApi = res.data.recommended_actions_mtd || {};
      const objectiveFromApi = res.data.objective_context || null;
      const portfolioRecFromApi = (res.data as any).portfolio_recommendation || "";
      setPortfolioRecommendation(portfolioRecFromApi);
      setObjectiveContext(objectiveFromApi);

      setSummaryText(summaryTextFromApi);
      setOverallSummary(summaryBulletsFromApi);
      setOverallActions(actionsFromApi);              // ✅ ADD THIS
      setRecommendedActions(recommendedActionsFromApi);
      setAdsRecommendation(adsRecommendation);
      setInventorySummary(inventoryFromApi);
      setRemainingSkusBlock(remainingBlock);

      let finalSummary = overallSummary;
      let finalActions = overallActions;

      const todayKey = getTodayKey();

      if (!generateInsights) {
        const isNewDay = insightDate !== todayKey;
        const hasNoExisting =
          overallSummary.length === 0 && overallActions.length === 0;

        if (isNewDay || hasNoExisting) {
          finalSummary = summaryBulletsFromApi;
          finalActions = actionsFromApi;

          setSummaryText(summaryTextFromApi);
          setOverallSummary(summaryBulletsFromApi);
          setOverallActions(actionsFromApi);

          setInsightDate(todayKey);
        }
      }

      const liveInsights = res.data.ai_insights || {};
      if (generateInsights && Object.keys(liveInsights).length) {
        setSkuInsights(liveInsights);
        saveInsightsToStorage(liveInsights);
      }

      saveCompareToStorage({
        categorizedGrowth: normalized,
        periods: newPeriods,
        month2Label: currentLabel,
        activeTab,
        countryName,
        // overallSummary: finalSummary,
        overallActions: finalActions,
        recommendedActions: recommendedActionsFromApi, // ✅ ADD
        overallSummary: summaryBulletsFromApi,
        summaryText: summaryTextFromApi,
        insightDate: todayKey,
        objectiveContext: objectiveFromApi,
        portfolioRecommendation: portfolioRecFromApi,
      });
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
    if (!normalizedCountry || normalizedCountry === 'global') return;
    fetchLiveBi(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [normalizedCountry, ranged, month, year, disableAutoFetch]);

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
    await fetchLiveBi(true);
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
    const s = parseISODateSafe(p?.start_date);
    const e = parseISODateSafe(p?.end_date);
    if (!s || !e) return "";

    const sm = s.toLocaleString("en-US", { month: "short" });
    const em = e.toLocaleString("en-US", { month: "short" });

    const sd = s.getDate();
    const ed = e.getDate();

    const sameMonth = s.getMonth() === e.getMonth() && s.getFullYear() === e.getFullYear();

    // Most MTD cases: "Jan 1-19"
    if (sameMonth) return `${sm} ${sd}-${ed}`;

    // Spans months within same year: "Dec 25-Jan 19"
    const sameYear = s.getFullYear() === e.getFullYear();
    if (sameYear) return `${sm} ${sd}-${em} ${ed}`;

    // Spans years: "Dec 25, 2025-Jan 19, 2026"
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

  const exportToExcel = (rows: SkuItem[], filename = "export.xlsx") => {
    // ✅ IMPORTANT: backend fields are tied to month1(old) / month2(new). Keep fixed mapping.
    const newLbl = formatRangeLabel(periods?.current_mtd) || currPeriod.month; // e.g. "Jan 1-19"
    const oldLbl = formatRangeLabel(periods?.previous) || prevPeriod.month; // e.g. "Dec 1-19"

    // 1) remove any existing total rows coming from API/data
    const cleanRows = (rows || []).filter((r) => {
      const name = String(r?.product_name || "").toLowerCase().trim();
      return (
        name !== "total" &&
        !name.includes("total (top 80") &&
        name !== "total (top 80%)"
      );
    });

    const num = (v: any) => {
      const n = Number(v);
      return Number.isFinite(n) ? n : 0;
    };

    const round2 = (v: any) => {
      const n = Number(v);
      return Number.isFinite(n) ? Number(n.toFixed(2)) : null;
    };

    // ✅ Live BI mapping is fixed: month1 = previous, month2 = current
    const pickNew = (row: any, keyMonth1: string, keyMonth2: string) => row?.[keyMonth2];
    const pickOld = (row: any, keyMonth1: string, keyMonth2: string) => row?.[keyMonth1];

    const pct = (oldV: number, newV: number) => (oldV ? ((newV - oldV) / oldV) * 100 : null);

    // ✅ EXACT column order (NEW month first, then OLD month)
    const headerOrder = [
      "SKU",
      "Product",
      `Qty ${newLbl}`,
      `Qty ${oldLbl}`,
      "Change in Qty (%age)",
      `Gross Sales ${newLbl}`,
      `Gross Sales ${oldLbl}`,
      "Change in Gross Sales (%age)",
      `Net Sales ${newLbl}`,
      `Net Sales ${oldLbl}`,
      "Change in Net Sales (%age)",
      `ASP ${newLbl}`,
      `ASP ${oldLbl}`,
      "Change in ASP (%age)",
      `Sales Mix ${newLbl}`,
      `Profit Mix ${newLbl}`,
      `Sales Mix ${oldLbl}`,
      `Profit Mix ${oldLbl}`,
      "Change in Sales Mix (%age)",
      `CM1 Profit ${newLbl}`,
      `CM1 Profit ${oldLbl}`,
      "Change in CM1 Profit",
      `CM1 Profit %age(${newLbl})`,
      `CM1 Profit %age(${oldLbl})`,
      `CM1 Unit Profit ${newLbl}`,
      `CM1 Unit Profit ${oldLbl}`,
      "Change in CM1 Unit Profit (%age)",
    ];

    // ---------- TOP BLOCK CONFIG ----------
    const PROFIT_COL_INDEX_1_BASED = (() => {
      const idx0 = headerOrder.findIndex((h) =>
        String(h).toLowerCase().includes("cm1 profit %age")
      );
      return (idx0 >= 0 ? idx0 : headerOrder.length - 1) + 1;
    })();

    const currencySymbol = getCurrencySymbolForExcel();

    const prevShortLocal = getShortPeriodLabel(periods?.previous?.label);
    const currShortLocal = getShortPeriodLabel(periods?.current_mtd?.label);

    const topExtraLines = [
      `Country : ${titleCountry}`,
      `Platform : Amazon`,
      `Currency : ${currencySymbol}`,
      `Period : ${prevShortLocal || ""} vs ${currShortLocal || ""}`,
    ];

    const excelTitle = `Amazon ${titleCountry} - SKU Analysis - MTD ${titleMonth}`;

    const topAoA = buildTopAoA({
      headerCount: headerOrder.length,
      title: excelTitle,
      companyName: companyNameForExcel,
      brandName: brandNameForExcel,
      profitColIndex1Based: PROFIT_COL_INDEX_1_BASED,
      extraLines: topExtraLines,
    });

    const WS1_HEADER_ROW_INDEX = topAoA.length;

    /**
     * ✅ Percent formatting:
     * formats columns whose header contains "%" OR starts with "Sales Mix " OR "Profit Mix "
     */
    const addPercentToPercentColumns = (ws: XLSX.WorkSheet, headerRowIndexes: number[] = [0]) => {
      const ref = ws["!ref"];
      if (!ref) return;

      const range = XLSX.utils.decode_range(ref);
      const isSalesMixHeader = (h: string) => h.trim().toLowerCase().startsWith("sales mix ");
      const isProfitMixHeader = (h: string) => h.trim().toLowerCase().startsWith("profit mix ");

      for (const headerRow of headerRowIndexes) {
        if (headerRow < range.s.r || headerRow > range.e.r) continue;

        for (let C = range.s.c; C <= range.e.c; C++) {
          const headerCell = ws[XLSX.utils.encode_cell({ r: headerRow, c: C })];
          const header = String(headerCell?.v ?? "");

          const shouldFormatAsPercent =
            header.includes("%") || isSalesMixHeader(header) || isProfitMixHeader(header);
          if (!shouldFormatAsPercent) continue;

          for (let R = headerRow + 1; R <= range.e.r; R++) {
            // stop at blank separator row
            const a0 = ws[XLSX.utils.encode_cell({ r: R, c: 0 })];
            const b0 = ws[XLSX.utils.encode_cell({ r: R, c: 1 })];
            const rowLooksBlank =
              (!a0 || a0.v == null || a0.v === "") &&
              (!b0 || b0.v == null || b0.v === "");
            if (rowLooksBlank) break;

            const addr = XLSX.utils.encode_cell({ r: R, c: C });
            const cell = ws[addr];
            if (!cell || cell.v == null || cell.v === "") continue;

            const n = Number(cell.v);
            if (!Number.isFinite(n)) continue;

            ws[addr] = { ...cell, t: "n", v: n, z: '0.00"%"' };
          }
        }
      }
    };

    // =========================
    // ✅ FIXED: formatter for a section (adds a Total row at bottom)
    // Profit Mix in Sheet 2 must be computed vs GRAND TOTAL (all SKUs), not section total.
    // So we allow passing a ProfitMix denominator via opts.
    // =========================
    const formatRowsWithTotals = (
      inputRows: SkuItem[],
      opts?: { profitMixDenomNew?: number; profitMixDenomOld?: number }
    ) => {
      const clean = (inputRows || []).filter((r) => {
        const name = String(r?.product_name || "").toLowerCase().trim();
        return (
          name !== "total" &&
          !name.includes("total (top 80") &&
          name !== "total (top 80%)"
        );
      });

      // ✅ Sales Mix computed from section Net Sales totals (OK for Sheet 1; your logic expects this)
      const totalNsNew = clean.reduce(
        (s, r) => s + num(pickNew(r, "net_sales_month1", "net_sales_month2")),
        0
      );
      const totalNsOld = clean.reduce(
        (s, r) => s + num(pickOld(r, "net_sales_month1", "net_sales_month2")),
        0
      );

      // Section profit totals
      const sectionProfitNew = clean.reduce(
        (s, r) => s + num(pickNew(r, "profit_month1", "profit_month2")),
        0
      );
      const sectionProfitOld = clean.reduce(
        (s, r) => s + num(pickOld(r, "profit_month1", "profit_month2")),
        0
      );

      // ✅ If provided (Sheet 2), use GRAND profit denominator; otherwise fallback to section totals (Sheet 1)
      const denomProfitNew = opts?.profitMixDenomNew ?? sectionProfitNew;
      const denomProfitOld = opts?.profitMixDenomOld ?? sectionProfitOld;

      const formatted = clean.map((row) => {
        const unitGrowth = row["Unit Growth"] as GrowthCategory | undefined;
        const aspGrowth = row["ASP Growth"] as GrowthCategory | undefined;
        const grossSalesGrowth = row["Gross Sales Growth"] as GrowthCategory | undefined;
        const netSalesGrowth = row["Net Sales Growth"] as GrowthCategory | undefined;
        const unitProfitGrowth = row["Profit Per Unit"] as GrowthCategory | undefined;

        const qtyOld = pickOld(row, "quantity_month1", "quantity_month2");
        const qtyNew = pickNew(row, "quantity_month1", "quantity_month2");

        const gsOldRaw = pickOld(row, "product_sales_month1", "product_sales_month2");
        const gsNewRaw = pickNew(row, "product_sales_month1", "product_sales_month2");

        const nsOldRaw = pickOld(row, "net_sales_month1", "net_sales_month2");
        const nsNewRaw = pickNew(row, "net_sales_month1", "net_sales_month2");

        const aspOldRaw = pickOld(row, "asp_month1", "asp_month2");
        const aspNewRaw = pickNew(row, "asp_month1", "asp_month2");

        const cm1OldRaw = pickOld(row, "profit_month1", "profit_month2");
        const cm1NewRaw = pickNew(row, "profit_month1", "profit_month2");

        const upOldRaw = pickOld(row, "unit_wise_profitability_month1", "unit_wise_profitability_month2");
        const upNewRaw = pickNew(row, "unit_wise_profitability_month1", "unit_wise_profitability_month2");

        const gsOld = convertToDisplayCurrency(gsOldRaw, sourceCurrency);
        const gsNew = convertToDisplayCurrency(gsNewRaw, sourceCurrency);

        const nsOld = convertToDisplayCurrency(nsOldRaw, sourceCurrency);
        const nsNew = convertToDisplayCurrency(nsNewRaw, sourceCurrency);

        const aspOld = convertToDisplayCurrency(aspOldRaw, sourceCurrency);
        const aspNew = convertToDisplayCurrency(aspNewRaw, sourceCurrency);

        const cm1Old = convertToDisplayCurrency(cm1OldRaw, sourceCurrency);
        const cm1New = convertToDisplayCurrency(cm1NewRaw, sourceCurrency);

        const upOld = convertToDisplayCurrency(upOldRaw, sourceCurrency);
        const upNew = convertToDisplayCurrency(upNewRaw, sourceCurrency);

        const mixOld = totalNsOld ? (num(nsOld) / totalNsOld) * 100 : null;
        const mixNew = totalNsNew ? (num(nsNew) / totalNsNew) * 100 : null;

        const profitMixNew = denomProfitNew ? (num(cm1New) / denomProfitNew) * 100 : null;
        const profitMixOld = denomProfitOld ? (num(cm1Old) / denomProfitOld) * 100 : null;

        const cm1PctOld = pickOld(row, "profit_percentage_month1", "profit_percentage_month2");
        const cm1PctNew = pickNew(row, "profit_percentage_month1", "profit_percentage_month2");

        return {
          SKU: row.sku || "",
          Product: row.product_name || "",

          [`Qty ${newLbl}`]: qtyNew ?? null,
          [`Qty ${oldLbl}`]: qtyOld ?? null,
          "Change in Qty (%age)": unitGrowth?.value ?? null,

          [`Gross Sales ${newLbl}`]: gsNew ?? null,
          [`Gross Sales ${oldLbl}`]: gsOld ?? null,
          "Change in Gross Sales (%age)": grossSalesGrowth?.value ?? null,

          [`Net Sales ${newLbl}`]: nsNew ?? null,
          [`Net Sales ${oldLbl}`]: nsOld ?? null,
          "Change in Net Sales (%age)": netSalesGrowth?.value ?? null,

          [`ASP ${newLbl}`]: round2(aspNew ?? null),
          [`ASP ${oldLbl}`]: round2(aspOld ?? null),
          "Change in ASP (%age)": aspGrowth?.value ?? null,

          [`Sales Mix ${newLbl}`]: mixNew ?? null,
          [`Profit Mix ${newLbl}`]: profitMixNew ?? null,
          [`Sales Mix ${oldLbl}`]: mixOld ?? null,
          [`Profit Mix ${oldLbl}`]: profitMixOld ?? null,

          "Change in Sales Mix (%age)":
            mixOld != null && mixNew != null ? mixNew - mixOld : null,

          [`CM1 Profit ${newLbl}`]: cm1New ?? null,
          [`CM1 Profit ${oldLbl}`]: cm1Old ?? null,
          "Change in CM1 Profit":
            cm1New != null && cm1Old != null ? Number(cm1New) - Number(cm1Old) : null,

          [`CM1 Profit %age(${newLbl})`]: cm1PctNew ?? null,
          [`CM1 Profit %age(${oldLbl})`]: cm1PctOld ?? null,

          [`CM1 Unit Profit ${newLbl}`]: upNew ?? null,
          [`CM1 Unit Profit ${oldLbl}`]: upOld ?? null,
          "Change in CM1 Unit Profit (%age)": unitProfitGrowth?.value ?? null,
        };
      });

      const totals = clean.reduce(
        (acc, r) => {
          acc.qtyOld += num(pickOld(r, "quantity_month1", "quantity_month2"));
          acc.qtyNew += num(pickNew(r, "quantity_month1", "quantity_month2"));

          acc.gsOld += convertToDisplayCurrency(
            pickOld(r, "product_sales_month1", "product_sales_month2"),
            sourceCurrency
          );
          acc.gsNew += convertToDisplayCurrency(
            pickNew(r, "product_sales_month1", "product_sales_month2"),
            sourceCurrency
          );

          acc.nsOld += convertToDisplayCurrency(
            pickOld(r, "net_sales_month1", "net_sales_month2"),
            sourceCurrency
          );
          acc.nsNew += convertToDisplayCurrency(
            pickNew(r, "net_sales_month1", "net_sales_month2"),
            sourceCurrency
          );

          acc.cm1Old += convertToDisplayCurrency(
            pickOld(r, "profit_month1", "profit_month2"),
            sourceCurrency
          );
          acc.cm1New += convertToDisplayCurrency(
            pickNew(r, "profit_month1", "profit_month2"),
            sourceCurrency
          );

          acc.upOld += convertToDisplayCurrency(
            pickOld(r, "unit_wise_profitability_month1", "unit_wise_profitability_month2"),
            sourceCurrency
          );
          acc.upNew += convertToDisplayCurrency(
            pickNew(r, "unit_wise_profitability_month1", "unit_wise_profitability_month2"),
            sourceCurrency
          );

          return acc;
        },
        {
          qtyOld: 0, qtyNew: 0,
          gsOld: 0, gsNew: 0,
          nsOld: 0, nsNew: 0,
          cm1Old: 0, cm1New: 0,
          upOld: 0, upNew: 0,
        }
      );

      const safeDiv = (a: number, b: number) => (b ? a / b : null);
      const totalAspOld = round2(safeDiv(totals.nsOld, totals.qtyOld));
      const totalAspNew = round2(safeDiv(totals.nsNew, totals.qtyNew));

      const profitPct = (profit: number, sales: number) => (sales ? (profit / sales) * 100 : null);
      const totalCm1PctOld = profitPct(totals.cm1Old, totals.nsOld);
      const totalCm1PctNew = profitPct(totals.cm1New, totals.nsNew);

      const totalSalesMixOld = totalNsOld ? 100 : null;
      const totalSalesMixNew = totalNsNew ? 100 : null;

      // ✅ Profit mix totals relative to denom
      const totalProfitMixOld = denomProfitOld ? (totals.cm1Old / denomProfitOld) * 100 : null;
      const totalProfitMixNew = denomProfitNew ? (totals.cm1New / denomProfitNew) * 100 : null;

      const totalSalesMixChange =
        totalSalesMixOld != null && totalSalesMixNew != null ? pct(totalSalesMixOld, totalSalesMixNew) : null;

      formatted.push({
        SKU: "",
        Product: "Total",

        [`Qty ${newLbl}`]: totals.qtyNew,
        [`Qty ${oldLbl}`]: totals.qtyOld,
        "Change in Qty (%age)": pct(totals.qtyOld, totals.qtyNew),

        [`Gross Sales ${newLbl}`]: totals.gsNew,
        [`Gross Sales ${oldLbl}`]: totals.gsOld,
        "Change in Gross Sales (%age)": pct(totals.gsOld, totals.gsNew),

        [`Net Sales ${newLbl}`]: totals.nsNew,
        [`Net Sales ${oldLbl}`]: totals.nsOld,
        "Change in Net Sales (%age)": pct(totals.nsOld, totals.nsNew),

        [`ASP ${newLbl}`]: totalAspNew,
        [`ASP ${oldLbl}`]: totalAspOld,
        "Change in ASP (%age)":
          totalAspOld != null && totalAspNew != null ? pct(totalAspOld, totalAspNew) : null,

        [`Sales Mix ${newLbl}`]: totalSalesMixNew,
        [`Profit Mix ${newLbl}`]: totalProfitMixNew,
        [`Sales Mix ${oldLbl}`]: totalSalesMixOld,
        [`Profit Mix ${oldLbl}`]: totalProfitMixOld,

        "Change in Sales Mix (%age)": totalSalesMixChange,

        [`CM1 Profit ${newLbl}`]: totals.cm1New,
        [`CM1 Profit ${oldLbl}`]: totals.cm1Old,
        "Change in CM1 Profit": totals.cm1New - totals.cm1Old,

        [`CM1 Profit %age(${newLbl})`]: totalCm1PctNew,
        [`CM1 Profit %age(${oldLbl})`]: totalCm1PctOld,

        [`CM1 Unit Profit ${newLbl}`]: totals.upNew,
        [`CM1 Unit Profit ${oldLbl}`]: totals.upOld,
        "Change in CM1 Unit Profit (%age)": pct(totals.upOld, totals.upNew),
      });

      return formatted;
    };

    // -------------------------
    // Sheet 1: All SKUs (Growth Comparison)  ✅ unchanged behavior
    // -------------------------
    const formattedAll = formatRowsWithTotals(cleanRows);

    const bodyAoA1 = formattedAll.map((obj) =>
      headerOrder.map((h) => (obj as any)[h] ?? null)
    );

    const sheet1AoA = [...topAoA, headerOrder, ...bodyAoA1];
    const ws1 = XLSX.utils.aoa_to_sheet(sheet1AoA);

    addPercentToPercentColumns(ws1, [WS1_HEADER_ROW_INDEX]);
    ws1["!freeze"] = { xSplit: 0, ySplit: WS1_HEADER_ROW_INDEX + 1 };
    applyTopStyles(ws1, headerOrder.length, PROFIT_COL_INDEX_1_BASED);
    boldHeaderRows(ws1, [WS1_HEADER_ROW_INDEX]);
    boldTotalRowsByProductColumn(ws1, 1, ["total", "others", "grand total"]);

    // -------------------------
    // Sheet 2: SKU Split (3 sections + ✅ only Grand Total row)
    // ✅ FIX: Profit Mix must be computed vs GRAND TOTAL profit, not each section
    // -------------------------
    const splitHeader = [...headerOrder];
    const aoa: any[][] = [];

    const top80 = categorizedGrowth.top_80_skus || [];
    const newRev = categorizedGrowth.new_or_reviving_skus || [];
    const other = categorizedGrowth.other_skus || [];

    // ✅ Build all rows (without Total-like rows) for grand-denominators
    const allRowsForMix = [...top80, ...newRev, ...other].filter((r: any) => {
      const name = String(r?.product_name || "").toLowerCase().trim();
      return (
        name !== "total" &&
        !name.includes("total (top 80") &&
        name !== "total (top 80%)"
      );
    });

    const grandProfitNew = allRowsForMix.reduce(
      (s: number, r: any) => s + num(pickNew(r, "profit_month1", "profit_month2")),
      0
    );

    const grandProfitOld = allRowsForMix.reduce(
      (s: number, r: any) => s + num(pickOld(r, "profit_month1", "profit_month2")),
      0
    );

    const makeSectionAoA = (sectionTitle: string, sectionRows: SkuItem[]) => {
      const formatted = formatRowsWithTotals(sectionRows, {
        profitMixDenomNew: grandProfitNew, // ✅ key fix
        profitMixDenomOld: grandProfitOld, // ✅ key fix
      });

      const body = formatted.map((obj) =>
        headerOrder.map((h) => (obj as any)[h] ?? null)
      );

      const titleRow = [sectionTitle];
      while (titleRow.length < splitHeader.length) titleRow.push("");

      return { titleRow, headerRow: splitHeader, body };
    };

    const pushSection = (title: string, sectionRows: SkuItem[]) => {
      const { titleRow, headerRow, body } = makeSectionAoA(title, sectionRows);
      aoa.push(titleRow);
      aoa.push(headerRow);
      aoa.push(...body);
      aoa.push([]); // blank row gap
    };

    pushSection("Top 80% SKUs", top80);
    pushSection("New/Reviving SKUs", newRev);
    pushSection("Other SKUs", other);

    // ✅ Append Grand Total title + header + ONLY the grand total row
    {
      const grandTitleRow = ["Grand Total"];
      while (grandTitleRow.length < splitHeader.length) grandTitleRow.push("");

      const grandFormatted = formatRowsWithTotals([...top80, ...newRev, ...other], {
        profitMixDenomNew: grandProfitNew,
        profitMixDenomOld: grandProfitOld,
      });

      const grandTotalObj = grandFormatted[grandFormatted.length - 1]; // "Total" row
      const grandTotalRow = headerOrder.map((h) => (grandTotalObj as any)?.[h] ?? null);

      aoa.push(grandTitleRow);
      aoa.push(splitHeader);
      aoa.push(grandTotalRow);
    }

    const ws2 = XLSX.utils.aoa_to_sheet([...topAoA, ...aoa]);
    applyTopStyles(ws2, headerOrder.length, PROFIT_COL_INDEX_1_BASED);
    ws2["!freeze"] = { xSplit: 0, ySplit: topAoA.length + 2 };

    const findHeaderRows = (ws: XLSX.WorkSheet) => {
      const ref = ws["!ref"];
      if (!ref) return [0];
      const range = XLSX.utils.decode_range(ref);

      const rows: number[] = [];
      for (let R = range.s.r; R <= range.e.r; R++) {
        const a = ws[XLSX.utils.encode_cell({ r: R, c: 0 })];
        if (String(a?.v ?? "").trim() === "SKU") rows.push(R);
      }
      return rows.length ? rows : [0];
    };

    const ws2HeaderRows = findHeaderRows(ws2);
    addPercentToPercentColumns(ws2, ws2HeaderRows);
    boldHeaderRows(ws2, ws2HeaderRows);
    boldTotalRowsByProductColumn(ws2, 1, ["total"]);

    const boldRowsByColValue = (ws: XLSX.WorkSheet, colIndex0: number, labels: string[]) => {
      const ref = ws["!ref"];
      if (!ref) return;
      const range = XLSX.utils.decode_range(ref);
      const set = new Set(labels.map((s) => s.toLowerCase()));
      for (let R = range.s.r; R <= range.e.r; R++) {
        const addr = XLSX.utils.encode_cell({ r: R, c: colIndex0 });
        const v = String(ws[addr]?.v ?? "").trim().toLowerCase();
        if (set.has(v)) applyBoldRow(ws, R);
      }
    };
    boldRowsByColValue(ws2, 0, ["Grand Total"]);

    // -------------------------
    // Build workbook with 2 sheets
    // -------------------------
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws1, "All SKUs");
    XLSX.utils.book_append_sheet(wb, ws2, "SKU Split");

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
      metrics,
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
      metrics,
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
    ...(categorizedGrowth.new_or_reviving_skus || []),
    ...(categorizedGrowth.other_skus || []),
  ];

  const currentTabData = getTabRows(activeTab);

  const allSkuRows = categorizedGrowth
    ? [
      ...cleanSkuRows(categorizedGrowth.top_80_skus || []),
      ...cleanSkuRows(categorizedGrowth.new_or_reviving_skus || []),
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
    categorizedGrowth.new_or_reviving_skus.length > 0 ||
    categorizedGrowth.other_skus.length > 0;

  const segmentTotal =
    activeTab === 'all_skus'
      ? categorizedGrowth.all_skus_total
      : activeTab === 'top_80_skus'
        ? categorizedGrowth.top_80_total
        : activeTab === 'new_or_reviving_skus'
          ? categorizedGrowth.new_or_reviving_total
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
    if (activeTab !== 'new_or_reviving_skus' || !currentTabData.length) {
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

  // =========================
  // DataTable wiring
  // =========================

  type BIGridRow = {
    __isTotal?: boolean;
    sNo?: number | string;
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
        {/* ✅ fixed icon slot (never collapses) */}
        <span className="w-4 flex justify-center shrink-0">
          {showArrow ? (
            <Icon size={12} />
          ) : (
            <Icon size={12} style={{ visibility: "hidden" }} />
          )}
        </span>

        <span className="tabular-nums inline-block w-[50px] 2xl:w-[60px] text-right">
          {val === 0 ? "0.00%" : text}
        </span>
      </span>
    );
  };

  const renderGrowthOrNA = (g?: GrowthCategory) => {
    if (!g || g.value == null) return <span>N/A</span>;

    const val = Number(g.value);

    let color = "#414042";
    if (val > 5) color = "#5EA68E";
    else if (val < -5) color = "#FF5C5C";

    return <GrowthCell val={val} color={color} showArrow={val !== 0} />;
  };


  const renderNewRevGrowthOrDash = (g?: GrowthCategory) => {
    if (g && g.value != null && g.category && g.category !== 'No Data') {
      return renderGrowthOrNA(g);
    }
    return <span>-</span>;
  };

  const buildAiCell = (item: SkuItem) => {
    if (!Object.keys(skuInsights).length) return null;

    const entry = getInsightForItem(item);
    if (entry) {
      return (
        <button
          className="font-semibold underline"
          onClick={() => {
            setFbType(null);
            setFbText('');
            setFbSuccess(false);

            setSelectedSku(entry[0]);      // optional (agar feedback etc chahiye)
            setSelectedSkuItem(item);      // ✅ store clicked row

            const insightData =
              skuInsights[entry[0] as keyof typeof skuInsights] ||
              getInsightByProductName(item.product_name)?.[1];

            const insightText = insightData?.insight || "";
            const prodName = insightData?.product_name || item.product_name || "";

            // ✅ Open SAME "Detailed View" drawer
            setSelectedRec(buildSelectedRecFromInsight(item, insightText, prodName));
            setRecDrawerOpen(true);

            // ❌ do NOT open old AI drawer
            // setModalOpen(true);
          }}
        >
          View Insights
        </button>
      );
    }

    return (
      <em style={{ color: '#888' }}>
        Not analyzed
        <br />
        <small style={{ fontSize: 10 }}>
          ({isGlobalData() ? 'Global/Product Name' : 'SKU'}: {item.product_name || item.sku || 'N/A'})
        </small>
      </em>
    );
  };

  const columns: ColumnDef<BIGridRow>[] = useMemo(() => {
    const isNewRev = activeTab === 'new_or_reviving_skus';
    const showAI = Object.keys(skuInsights).length > 0;

    const SNO_WIDTH = '70px';
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
    const isNewRev = activeTab === 'new_or_reviving_skus';
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

      return {
        sNo: idx + 1,
        product: item.product_name || item.sku || 'N/A',
        salesMix,
        profitMix,
        unit: isNewRev ? renderNewRevGrowthOrDash(item['Unit Growth']) : renderGrowthOrNA(item['Unit Growth']),
        asp: isNewRev ? renderNewRevGrowthOrDash(item['ASP Growth']) : renderGrowthOrNA(item['ASP Growth']),
        sales: isNewRev ? renderNewRevGrowthOrDash(item['Sales Growth']) : renderGrowthOrNA(item['Sales Growth']),
        ...(isNewRev ? {} : { mixChange: renderGrowthOrNA(item['Sales Mix Change']) }),
        unitProfit: isNewRev ? renderNewRevGrowthOrDash(item['Profit Per Unit']) : renderGrowthOrNA(item['Profit Per Unit']),
        profit: isNewRev ? renderNewRevGrowthOrDash(item['CM1 Profit Impact']) : renderGrowthOrNA(item['CM1 Profit Impact']),
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
        sNo: 6,
        product: 'Others',
        salesMix:
          totalNetSalesMonth2 > 0
            ? `${((othersNetSales / totalNetSalesMonth2) * 100).toFixed(2)}%`
            : '0.00%',
        profitMix: othersProfitMix,
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
        : activeTab === 'new_or_reviving_skus'
          ? `${manualTotalsForNewRev.salesMix.toFixed(2)}%`
          : 'N/A';



    const totalRow: BIGridRow = {
      __isTotal: true,
      sNo: '',
      product: 'Total',
      salesMix: totalSalesMix,
      profitMix: totalProfitMix,
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
        : activeTab !== "new_or_reviving_skus"
          ? {
            unit: renderGrowthOrNA(segmentTotal?.["Unit Growth"]),
            asp: renderGrowthOrNA(segmentTotal?.["ASP Growth"]),
            sales: renderGrowthOrNA(segmentTotal?.["Sales Growth"]),
            mixChange: renderGrowthOrNA(segmentTotal?.["Sales Mix Change"]),
            unitProfit: renderGrowthOrNA(segmentTotal?.["Profit Per Unit"]),
            profit: renderGrowthOrNA(segmentTotal?.["CM1 Profit Impact"]),
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
    className = "",
  }: {
    objective?: {
      growth_intent?: string;
      profit_priority?: string;
      inventory_clearance_priority?: boolean;
    } | null;
    className?: string;
  }) => {
    const growth = objective?.growth_intent?.replaceAll("_", " ") || "Not Defined";
    const profit = objective?.profit_priority?.replaceAll("_", " ") || "Not Defined";
    const inv = objective?.inventory_clearance_priority ? "Yes" : "No";

    const Card = ({ label, value }: { label: string; value: string }) => (
      <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
        <div className="2xl:text-xs text-[10px] text-charcoal-500">{label}</div>
        <div className="mt-1 text-sm 2xl:text-base font-semibold text-charcoal-500 capitalize">
          {value}
        </div>
      </div>
    );

    return (
      <div className={`grid grid-cols-1  gap-5 ${className}`}>
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
            overallActions.length > 0) && (

              <div className="flex gap-4 flex-col">

                {/* 1) Monthly Objective */}
                <div className="flex flex-col lg:flex-row gap-4 items-stretch">
                  <div className="flex-1">
                    {(summaryText || overallSummary.length > 0 || portfolioRecommendation) && (
                      <div className="bg-white border border-[#D9D9D9] rounded-xl shadow-sm p-4 text-xs 2xl:text-sm text-charcoal-500 w-full h-full flex flex-col">
                        <PageBreadcrumb pageTitle="Business Summary" variant="page" align="left" />

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
                    {objectiveContext && (
                      <div className="bg-white border border-[#D9D9D9] rounded-xl shadow-sm p-4 text-xs 2xl:text-sm text-charcoal-600 w-full h-full flex flex-col">
                        <PageBreadcrumb pageTitle="Monthly Objective" variant="page" align="left" />
                        <ObjectiveCards objective={objectiveContext} className="mt-3 flex-1" />
                      </div>
                    )}
                  </div>
                </div>



                {/* 3) Recommended Actions (cards) */}
                {recommendedActions && Object.keys(recommendedActions).length > 0 && (
                  <div className="bg-white border border-[#D9D9D9] rounded-xl shadow-sm p-4 text-xs 2xl:text-sm text-charcoal-600 w-full">
                    <PageBreadcrumb pageTitle="Recommendations" variant="page" align="left" />

                    <div className="mt-3 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
                      {Object.entries(recommendedActions).map(([_, text], idx) => {
                        const parsed = parseRecommendedAction(text);
                        const recommendationPoints = parsed.recommendationPoints;

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
                                  <div key={i} className="rounded-lg border border-slate-200 bg-slate-50 py-2 px-1 min-w-0">
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
                        )
                      })}

                      {remainingSkusBlock?.trim() && (() => {
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
                {(inventorySummary?.alert_bullets?.length > 0 || inventorySummary?.summary_text) && (
                  <div className="bg-white border border-[#D9D9D9] rounded-xl shadow-sm p-3 text-xs 2xl:text-sm text-charcoal-600 w-full">
                    <PageBreadcrumb pageTitle="Inventory Insight" variant="page" align="left" />

                    {inventorySummary?.alert_bullets?.length > 0 && (
                      <ul className="list-disc pl-5 space-y-1 pt-2">
                        {inventorySummary.alert_bullets.map((bullet: string, idx: number) => (
                          <li key={idx}>{bullet}</li>
                        ))}
                      </ul>
                    )}

                    {inventorySummary?.summary_text && (
                      <div className="mt-3 text-xs 2xl:text-sm text-charcoal-500 italic border-l-2 border-green-500 pl-3">
                        {inventorySummary.summary_text}
                      </div>
                    )}
                  </div>
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
                        !['top_80_skus', 'new_or_reviving_skus', 'other_skus'].some(
                          (k) =>
                            (categorizedGrowth[k as keyof CategorizedGrowth] as SkuItem[])?.length > 0
                        )
                      }
                    >
                      {loadingInsight ? "Generating..." : "AI Insights"}
                    </AiButton>

                    <DownloadIconButton
                      onClick={() => {
                        if (!userData) {
                          setError("User profile not loaded yet. Please try again.");
                          return;
                        }

                        const prevShortName = prevShort || "Prev";
                        const currShortName = currShort || "Curr";
                        const file = `AllSKUs-${prevShortName}vs${currShortName}.xlsx`;
                        const allRows = getAllSkusForExport();
                        exportToExcel(allRows, file);
                      }}
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
                        !['top_80_skus', 'new_or_reviving_skus', 'other_skus'].some(
                          (k) =>
                            (categorizedGrowth[k as keyof CategorizedGrowth] as SkuItem[])?.length > 0
                        )
                      }
                    >
                      {loadingInsight ? "Generating..." : "AI Insights"}
                    </AiButton>

                    <DownloadIconButton
                      onClick={() => {
                        if (!userData) {
                          setError("User profile not loaded yet. Please try again.");
                          return;
                        }

                        const prevShortName = prevShort || "Prev";
                        const currShortName = currShort || "Curr";
                        const file = `AllSKUs-${prevShortName}vs${currShortName}.xlsx`;
                        const allRows = getAllSkusForExport();
                        exportToExcel(allRows, file);
                      }}
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
                    scrollY
                    // maxHeight="60vh"
                    paginate={false} // ✅ total row always visible at bottom
                    className="rounded-xl"
                    tableClassName="w-full"
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

