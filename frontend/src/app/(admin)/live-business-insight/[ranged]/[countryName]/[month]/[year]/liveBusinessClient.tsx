'use client';

import React, { useState, useEffect, useMemo, useCallback } from 'react';
import axios from 'axios';
import * as XLSX from "xlsx-js-style";
import { IoDownload, IoRefresh } from 'react-icons/io5';
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
import {
  exportSkuAnalysisMtdExcel
} from "@/lib/excel/exportCurrentInventoryExcel";
import { useRouter } from "next/navigation";
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";

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

  // ✅ ADD THIS
  onManualAiRefresh?: () => Promise<ApiResponse | null | any>;

  asOf?: string;
  startDay?: number;
  endDay?: number;

  formattedMonthYear?: string;
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
  coverage_ratio_months?: number;
  current_inventory?: number;
  'Gross Sales Growth (%)'?: {
    category: string;
    value: number;
  };
  [key: string]: any;
  cm2_profit_curr?: number;
  cm2_profit_prev?: number;
  cm2_profit_per_unit_curr?: number;
  cm2_profit_per_unit_prev?: number;
  ads_spend_curr?: number;
  ads_spend_growth_pct?: number;
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
  ai_last_refreshed_at?: {
    iso?: string | null;
    display?: string | null;
    date?: string | null;
    time?: string | null;
    timezone?: string | null;
    timezone_label?: string | null;
  };

  periods?: {
    previous?: PeriodInfo;
    current_mtd?: PeriodInfo;
  };
  all_action_rows?: SkuItem[];
  remaining_skus_aggregate?: SkuItem | null;
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
  if (value === null || value === undefined) return "";

  if (typeof value === "number") {
    return value === 0 ? "" : String(value);
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed || trimmed === "0" || trimmed === "0.0") return "";
    return trimmed;
  }

  if (value && typeof value === "object") {
    return Object.entries(value as Record<string, unknown>)
      .map(([country, text]) => {
        const normalized = normalizeTextBlock(text);
        if (!normalized) return "";
        return `## ${country.toUpperCase()}\n${normalized}`;
      })
      .filter(Boolean)
      .join("\n\n");
  }

  return "";
};



const normalizeSkuGroupName = (name: string) =>
  String(name || "")
    .trim()
    .toLowerCase()
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/sku's/g, "skus")
    .replace(/product's/g, "products");

const isOthersCardName = (name: string) => {
  const value = normalizeSkuGroupName(name);

  return (
    value === "others" ||
    value === "other" ||
    value === "other sku" ||
    value === "other skus" ||
    value === "other product" ||
    value === "other products" ||
    value === "remaining" ||
    value === "remaining sku" ||
    value === "remaining skus" ||
    value === "remaining product" ||
    value === "remaining products" ||
    value === "rest sku" ||
    value === "rest skus" ||
    value === "rest product" ||
    value === "rest products" ||
    value === "leftover sku" ||
    value === "leftover skus" ||
    value === "leftover product" ||
    value === "leftover products" ||
    value === "all other skus" ||
    value === "all remaining skus"
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

  // ✅ ADD THIS
  onManualAiRefresh,

  asOf,
  startDay,
  endDay,
  formattedMonthYear,
}: MonthsforBIProps) {
  const { data: userData } = useGetUserDataQuery();
  const router = useRouter();

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

  const formatDisplayAmountNoDecimals = useMemo(() => {
    return (value: number | null | undefined) => {
      const n = toNumberSafe(value ?? 0);

      switch (displayCurrency) {
        case "USD":
          return new Intl.NumberFormat("en-US", {
            style: "currency",
            currency: "USD",
            minimumFractionDigits: 0,
            maximumFractionDigits: 0,
          }).format(n);

        case "GBP":
          return new Intl.NumberFormat("en-GB", {
            style: "currency",
            currency: "GBP",
            minimumFractionDigits: 0,
            maximumFractionDigits: 0,
          }).format(n);

        case "CAD":
          return new Intl.NumberFormat("en-CA", {
            style: "currency",
            currency: "CAD",
            minimumFractionDigits: 0,
            maximumFractionDigits: 0,
          }).format(n);

        case "INR":
          return new Intl.NumberFormat("en-IN", {
            style: "currency",
            currency: "INR",
            minimumFractionDigits: 0,
            maximumFractionDigits: 0,
          }).format(n);

        default:
          return n.toLocaleString(undefined, {
            minimumFractionDigits: 0,
            maximumFractionDigits: 0,
          });
      }
    };
  }, [displayCurrency]);

  const hasValue = (value: any) => {
    if (value === null || value === undefined) return false;
    if (typeof value === "string" && value.trim() === "") return false;

    const n = Number(value);
    return Number.isFinite(n);
  };

  const getPctGrowth = (prevValue: any, currValue: any) => {
    const prev = Number(prevValue || 0);
    const curr = Number(currValue || 0);

    if (!prev) return 0;

    return ((curr - prev) / Math.abs(prev)) * 100;
  };

  const hasCm2ProfitData = (row: any) => {
    return (
      hasValue(row?.cm2_profit_curr) &&
      hasValue(row?.cm2_profit_per_unit_curr)
    );
  };

  const formatMetricValueWithGrowth = (
    actualValue: number,
    growthValue: number,
    type: "money" | "number" = "money"
  ) => {
    const sign = growthValue > 0 ? "+" : "";
    const growthText = `${sign}${growthValue.toFixed(2)}%`;

    const mainValue =
      type === "number"
        ? Number(actualValue || 0).toLocaleString()
        : formatDisplayAmount(
          convertToDisplayCurrency(Number(actualValue || 0), sourceCurrency)
        );

    return `${mainValue} (${growthText})`;
  };

  const buildProfitMetricCards = (
    item: any,
    getGrowth?: (key: string) => number
  ) => {
    if (hasCm2ProfitData(item)) {
      const cm2ProfitCurr = Number(item?.cm2_profit_curr || 0);
      const cm2ProfitPrev = Number(item?.cm2_profit_prev || 0);

      const cm2ProfitPerUnitCurr = Number(item?.cm2_profit_per_unit_curr || 0);
      const cm2ProfitPerUnitPrev = Number(item?.cm2_profit_per_unit_prev || 0);

      const cm2ProfitGrowth = hasValue(item?.cm2_profit_growth_pct)
        ? Number(item.cm2_profit_growth_pct)
        : getPctGrowth(cm2ProfitPrev, cm2ProfitCurr);
      const cm2ProfitPerUnitGrowth = getPctGrowth(
        cm2ProfitPerUnitPrev,
        cm2ProfitPerUnitCurr
      );

      return [
        {
          label: "CM2 profit",
          value: formatMetricValueWithGrowth(
            cm2ProfitCurr,
            cm2ProfitGrowth,
            "money"
          ),
          color: cm2ProfitGrowth < 0 ? "#FF5C5C" : "#5EA68E",
        },
        {
          label: "CM2 profit per unit",
          value: formatMetricValueWithGrowth(
            cm2ProfitPerUnitCurr,
            cm2ProfitPerUnitGrowth,
            "money"
          ),
          color: cm2ProfitPerUnitGrowth < 0 ? "#FF5C5C" : "#5EA68E",
        },
      ];
    }

    const cm1ProfitCurr = Number(
      item?.profit_month2 ??
      item?.profit_curr ??
      item?.profit ??
      0
    );

    const cm1ProfitPerUnitCurr = Number(
      item?.unit_wise_profitability_month2 ??
      item?.unit_wise_profitability_curr ??
      item?.unit_wise_profitability ??
      0
    );

    const cm1ProfitGrowth = getGrowth?.("CM1 Profit Impact") ?? 0;
    const cm1ProfitPerUnitGrowth = getGrowth?.("Profit Per Unit") ?? 0;

    return [
      {
        label: "CM1 profit",
        value: formatMetricValueWithGrowth(
          cm1ProfitCurr,
          cm1ProfitGrowth,
          "money"
        ),
        color: cm1ProfitGrowth < 0 ? "#FF5C5C" : "#5EA68E",
      },
      {
        label: "CM1 profit per unit",
        value: formatMetricValueWithGrowth(
          cm1ProfitPerUnitCurr,
          cm1ProfitPerUnitGrowth,
          "money"
        ),
        color: cm1ProfitPerUnitGrowth < 0 ? "#FF5C5C" : "#5EA68E",
      },
    ];
  };

  const buildAdsMetric = (item: any) => {
    const adsSpendCurr = Number(
      item?.ads_spend_curr ??
      item?.ads_spend_month2 ??
      item?.ads_spend ??
      item?.total_ads ??
      item?.advertising_fees ??
      0
    );

    const adsSpendPrev = Number(
      item?.ads_spend_prev ??
      item?.ads_spend_month1 ??
      item?.ads_spend_previous ??
      0
    );

    const adsSpendGrowthPct =
      item?.ads_spend_growth_pct != null
        ? Number(item.ads_spend_growth_pct)
        : adsSpendPrev
          ? ((adsSpendCurr - adsSpendPrev) / Math.abs(adsSpendPrev)) * 100
          : 0;

    const sign = adsSpendGrowthPct > 0 ? "+" : "";
    const growthText = `${sign}${adsSpendGrowthPct.toFixed(2)}%`;

    return {
      label: "Ads",
      value: `${formatDisplayAmount(
        convertToDisplayCurrency(adsSpendCurr, sourceCurrency)
      )} (${growthText})`,
      color: "#414042", // growth black
    };
  };

  const replaceProfitMetricsWithCm2IfAvailable = (
    metrics: { label: string; value: string; color?: string }[],
    sourceRow: any
  ) => {
    if (!sourceRow || !hasCm2ProfitData(sourceRow)) return metrics;

    const filteredMetrics = metrics.filter((m) => {
      const label = m.label.trim().toLowerCase();

      return (
        label !== "cm1 profit" &&
        label !== "cm1 profit per unit" &&
        label !== "cm2 profit" &&
        label !== "cm2 profit per unit"
      );
    });

    return [
      ...filteredMetrics,
      ...buildProfitMetricCards(sourceRow),
    ];
  };

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

      const normalizedLabel = label.trim().toLowerCase();

      const formatted =
        normalizedLabel === "net sales" ||
          normalizedLabel === "cm1 profit" ||
          normalizedLabel === "cm2 profit"
          ? formatDisplayAmountNoDecimals(converted)
          : formatDisplayAmount(converted);

      return `${formatted}${percentPart ? ` ${percentPart}` : ""}`;
    };
  }, [
    convertToDisplayCurrency,
    formatDisplayAmount,
    formatDisplayAmountNoDecimals,
    sourceCurrency,
  ]);

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
  const [allActionRows, setAllActionRows] = useState<SkuItem[]>([]);
  const [remainingSkusAggregate, setRemainingSkusAggregate] = useState<SkuItem | null>(null);


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

  const drawerMonthYear = formattedMonthYear || titleMonth;

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
  const [aiLastRefreshedAt, setAiLastRefreshedAt] = useState<string>("");
  const [manualAiRefreshing, setManualAiRefreshing] = useState<boolean>(false);

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

  // const [pageLoading, setPageLoading] = useState<boolean>(true);
  const [pageLoading, setPageLoading] = useState<boolean>(
    disableAutoFetch ? !initialData : true
  );
  const [recDrawerOpen, setRecDrawerOpen] = useState(false);

  const [selectedRec, setSelectedRec] = useState<{
    productName: string;
    metrics: { label: string; value: string; color?: string }[];
    journeyPoints: string[];
    recommendationPoints: string[];
    advertisingPoints?: string[];
    inventoryPoints?: string[];
    showChart?: boolean;

    // ✅ aggregate SKU group support: Other SKUs, Remaining SKUs, Rest SKUs, etc.
    isOtherSkus?: boolean;
    otherSkuProductNames?: string[];
  } | null>(null);
  const isGlobalData = () => normalizedCountry === 'global';


  const getOtherSkuProductNames = () => {
    return (categorizedGrowth.other_skus || [])
      .filter((row: any) => !isTotalLikeRow(row))
      .filter((row: any) => !isOthersCardName(row?.product_name || ""))
      .map((row: any) => String(row?.product_name || "").trim())
      .filter(Boolean);
  };


  const splitIntoPoints = (value: string): string[] => {
    if (!value) return [];

    return String(value)
      // normalize escaped newlines if backend sends "\n" as text
      .replace(/\\n/g, "\n")

      // make inline bullets start on a new line
      .replace(/\s+-\s+/g, "\n- ")
      .replace(/\s+•\s+/g, "\n• ")

      // split bullet/newline format
      .split(/\r?\n+/)

      // fallback: split long paragraph into journey-style sentences
      .flatMap((line) =>
        line.split(
          /(?<=[.!?])\s+(?=(?:From|Between|In|This|ASP|Units|CM1|Net|Inventory|Long-term)\b)/g
        )
      )

      .map(cleanPoint)
      .filter(Boolean);
  };

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

    m.push(...buildProfitMetricCards(item, getGrowth));
    //  m.push(buildAdsMetric(item));

    const coverageRatio = Number(
      (item as any).coverage_ratio_months ??
      (item as any).coverageRatioMonths ??
      0
    );

    const currentInventory = Number(
      (item as any).current_inventory ??
      (item as any).currentInventory ??
      0
    );

    m.push(buildStockCoverMetric(item));
    m.push(buildCurrentInventoryUnitsMetric(item));

    return sortMetricsByOrder(m);
  };

  const cleanPoint = (value: string) =>
    value
      .replace(/^\s*[-•]\s*/, "")
      .replace(/^\s*\d+\.\s*-\s*/, "")
      .replace(/^\s*\d+\.\s*/, "")
      .trim();

  const toPoints = (value: unknown): string[] => {
    if (!value) return [];

    const rawItems = Array.isArray(value) ? value : [String(value)];

    return rawItems
      .flatMap((item) =>
        String(item || "")
          // split real bullet/newline format first
          .split(/\r?\n+/)
          .flatMap((line) =>
            line
              // split when multiple "- ..." bullets are inside one string
              .split(/\s+(?=[-•]\s+)/g)
          )
          .flatMap((line) =>
            line
              // fallback: split long paragraph into sentences
              .split(/(?<=[.!?])\s+(?=(?:From|Between|In|This|Long-term|ASP|Units|CM1|Net)\b)/g)
          )
      )
      .map(cleanPoint)
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
      metrics: sortMetricsByOrder([
        ...buildMetricsForSku(item),
        buildAdsMetric(item),
      ]),
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
      metrics: sortMetricsByOrder([
        ...buildMetricsForSku(item),
        buildAdsMetric(item),
      ]),

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
    setAllActionRows(payload.all_action_rows || []);
    setRemainingSkusAggregate(payload.remaining_skus_aggregate || null);
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
    setAiLastRefreshedAt(payload.ai_last_refreshed_at?.display || "");

    const liveInsights = isGlobalData()
      ? buildGlobalSkuInsights(payload)
      : payload.ai_insights || {};

    setSkuInsights(liveInsights);
  };

  // useEffect(() => {
  //   if (initialData) {
  //     hydrateFromPayload(initialData);
  //     setPageLoading(false);
  //   }
  // }, [initialData]);

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

  const fetchLiveBi = async (
    generateInsights: boolean = false,
    manualAiRefresh: boolean = false
  ) => {
    setError(null);

    if (!generateInsights && !manualAiRefresh) {
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
            manual_ai_refresh: manualAiRefresh ? "true" : "false",
          }
          : {
            countryName: normalizedCountry,
            ranged,
            month,
            year,
            generate_ai_insights: generateInsights ? "true" : "false",
            manual_ai_refresh: manualAiRefresh ? "true" : "false",
          },
      });

      setAllActionRows(res.data.all_action_rows || []);
      setRemainingSkusAggregate(res.data.remaining_skus_aggregate || null);

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
      setAiLastRefreshedAt(res.data.ai_last_refreshed_at?.display || "");
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
      if (!generateInsights && !manualAiRefresh) setPageLoading(false);
    }
  };

  // useEffect(() => {
  //   if (initialData) {
  //     hydrateFromPayload(initialData);
  //   }
  //   // eslint-disable-next-line react-hooks/exhaustive-deps
  // }, [initialData]);

  // useEffect(() => {
  //   if (!normalizedCountry || normalizedCountry === 'global') return;
  //   fetchLiveBi(false);
  //   // eslint-disable-next-line react-hooks/exhaustive-deps
  // }, [normalizedCountry, ranged, month, year]);

  useEffect(() => {
    if (!initialData) {
      if (disableAutoFetch) {
        setPageLoading(true);
      }
      return;
    }

    hydrateFromPayload(initialData);
    setPageLoading(false);

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialData, disableAutoFetch]);

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

  const handleManualAiRefresh = async () => {
    setManualAiRefreshing(true);
    setError(null);

    try {
      // ✅ Parent dashboard controls initialData when disableAutoFetch is true.
      // So update parent payload first, then hydrate child from the fresh payload.
      if (onManualAiRefresh) {
        const freshPayload = await onManualAiRefresh();

        if (freshPayload) {
          hydrateFromPayload(freshPayload);
        }

        return;
      }

      // Fallback for standalone usage
      await fetchLiveBi(false, true);
    } catch (err: any) {
      console.error("manual ai refresh error:", err?.response?.data || err.message);
      setError(
        err?.response?.data?.error ||
        err?.response?.data?.message ||
        "Failed to refresh AI summary and recommendations."
      );
    } finally {
      setManualAiRefreshing(false);
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


  const getInventoryMetricValues = (row: any) => {
    const coverageRatio = Number(
      row?.coverage_ratio_months ??
      row?.coverageRatioMonths ??
      0
    );

    const currentInventory = Number(
      row?.current_inventory ??
      row?.currentInventory ??
      0
    );

    return {
      coverageRatio: Number.isFinite(coverageRatio) ? coverageRatio : 0,
      currentInventory: Number.isFinite(currentInventory) ? currentInventory : 0,
    };
  };

  const buildStockCoverMetric = (row: any) => {
    const { coverageRatio } = getInventoryMetricValues(row);

    return {
      label: "Current inventory",
      value: `${coverageRatio.toFixed(2)} months`,
    };
  };

  const buildCurrentInventoryUnitsMetric = (row: any) => {
    const { currentInventory } = getInventoryMetricValues(row);

    return {
      label: "Current Inventory",
      value: `${Math.round(currentInventory).toLocaleString()} units`,
    };
  };

  const buildDrawerMetrics = (
    metrics: { label: string; value: string; color?: string }[],
    sourceRow: any
  ) => {
    const baseMetrics = metrics.filter((m) => {
      const label = m.label.trim().toLowerCase();
      return (
        label !== "current inventory" &&
        label !== "stock cover" &&
        label !== "stock cover (months)" &&
        label !== "current inventory units"
      );
    });

    return sortMetricsByOrder([
      ...baseMetrics,
      ...(sourceRow ? [buildAdsMetric(sourceRow)] : []),
      buildStockCoverMetric(sourceRow),
    ]);
  };

  const sortMetricsByOrder = (
    metrics: { label: string; value: string; color?: string }[]
  ) => {
    const order = [
      "units",
      "net sales",
      "asp",
      "ads",
      "cm2 profit",
      "cm2 profit per unit",
      "cm1 profit",
      "cm1 profit per unit",
      "current inventory",
      "stock cover (months)",
      "stock cover",
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
      /^(ASP|Units|Net sales|Ads|CM2 profit per unit|CM2 profit|CM1 profit per unit|CM1 profit|Current inventory)\s*:\s*(.+)$/i;

    const insightParts: string[] = [];

    for (const line of lines.slice(1)) {
      const metricMatch = line.match(metricRegex);

      if (metricMatch) {
        const label = metricMatch[1];
        const rawMetricValue = metricMatch[2];

        const value =
          label.trim().toLowerCase() === "current inventory"
            ? rawMetricValue
            : convertMetricValueString(rawMetricValue, label);

        metrics.push({
          label,
          value,
          color:
            label.trim().toLowerCase() === "ads"
              ? "#414042"
              : value.includes("-")
                ? "#FF5C5C"
                : "#5EA68E",
        });

        continue;
      }

      insightParts.push(line);
    }

    const sourceRow = getRecommendationSourceRow(productName) as any;

    const hasStockCoverMetric = metrics.some((m) => {
      const label = m.label.trim().toLowerCase();
      return (
        label === "stock cover" ||
        label === "stock cover (months)" ||
        label === "current inventory"
      );
    });

    if (sourceRow && !hasStockCoverMetric) {
      const coverageRatio = Number(
        sourceRow.coverage_ratio_months ??
        sourceRow.coverageRatioMonths ??
        0
      );

      const currentInventory = Number(
        sourceRow.current_inventory ??
        sourceRow.currentInventory ??
        0
      );

      metrics.push(buildStockCoverMetric(sourceRow));
    }

    const finalMetrics = replaceProfitMetricsWithCm2IfAvailable(
      metrics,
      sourceRow
    );



    const insightText = insightParts.join("\n").trim();
    const sections = extractSections(insightText);

    return {
      productName,
      metrics: sortMetricsByOrder(finalMetrics),
      insightText,
      journeyPoints: sections.journeyPoints,
      recommendationPoints: sections.recommendationPoints,
      advertisingPoints: sections.advertisingPoints,
      inventoryPoints: sections.inventoryPoints,
    };
  };

  const getRecommendedActionForItem = (item: SkuItem) => {
    const itemProductName = normalizeProductKey(item.product_name || "");
    const itemSku = normalizeProductKey(item.sku || "");

    if (!itemProductName && !itemSku) return null;

    const matchedEntry = Object.entries(recommendedActions || {}).find(
      ([key, text]) => {
        const parsed = parseRecommendedAction(String(text || ""));

        const keyName = normalizeProductKey(key);
        const parsedProductName = normalizeProductKey(parsed.productName || "");

        return (
          keyName === itemSku ||
          keyName === itemProductName ||
          parsedProductName === itemProductName ||
          parsedProductName === itemSku
        );
      }
    );

    if (!matchedEntry) return null;

    const [, text] = matchedEntry;
    return parseRecommendedAction(String(text || ""));
  };

  const cleanInventoryCardPoint = (point: string) => {
    return String(point || "")
      .replace(/^•\s*/, "")
      .replace(/^Inventory action:\s*Your coverage ratio is\s*[\d.]+\s*months\.?\s*/i, "")
      .replace(/^and\s+/i, "")
      .trim();
  };




  const buildOtherSkusAggregateItem = useCallback((): SkuItem | null => {
    const rows = categorizedGrowth.other_skus || [];
    const total = (remainingSkusAggregate || categorizedGrowth.other_total) as any;

    if (!rows.length && !total) return null;

    const qtyPrev =
      Number(total?.quantity_month1 ?? total?.quantity_prev ?? 0) ||
      rows.reduce(
        (s, r: any) => s + Number(r.quantity_month1 ?? r.quantity_prev ?? 0),
        0
      );

    const qtyCurr =
      Number(total?.quantity_month2 ?? total?.quantity_curr ?? total?.quantity ?? 0) ||
      rows.reduce(
        (s, r: any) => s + Number(r.quantity_month2 ?? r.quantity_curr ?? r.quantity ?? 0),
        0
      );

    const netSalesPrev =
      Number(total?.net_sales_month1 ?? total?.net_sales_prev ?? 0) ||
      rows.reduce(
        (s, r: any) => s + Number(r.net_sales_month1 ?? r.net_sales_prev ?? 0),
        0
      );

    const netSalesCurr =
      Number(total?.net_sales_month2 ?? total?.net_sales_curr ?? total?.net_sales ?? 0) ||
      rows.reduce(
        (s, r: any) => s + Number(r.net_sales_month2 ?? r.net_sales_curr ?? r.net_sales ?? 0),
        0
      );

    const profitPrev =
      Number(total?.profit_month1 ?? total?.profit_prev ?? 0) ||
      rows.reduce(
        (s, r: any) => s + Number(r.profit_month1 ?? r.profit_prev ?? 0),
        0
      );

    const profitCurr =
      Number(total?.profit_month2 ?? total?.profit_curr ?? total?.profit ?? 0) ||
      rows.reduce(
        (s, r: any) => s + Number(r.profit_month2 ?? r.profit_curr ?? r.profit ?? 0),
        0
      );

    const aspPrev =
      Number(total?.asp_prev ?? total?.asp_month1 ?? 0) ||
      (qtyPrev > 0 ? netSalesPrev / qtyPrev : 0);

    const aspCurr =
      Number(total?.asp_curr ?? total?.asp_month2 ?? total?.asp ?? 0) ||
      (qtyCurr > 0 ? netSalesCurr / qtyCurr : 0);

    const unitProfitPrev =
      Number(total?.unit_wise_profitability_prev ?? total?.unit_wise_profitability_month1 ?? 0) ||
      (qtyPrev > 0 ? profitPrev / qtyPrev : 0);

    const unitProfitCurr =
      Number(total?.unit_wise_profitability_curr ?? total?.unit_wise_profitability_month2 ?? total?.unit_wise_profitability ?? 0) ||
      (qtyCurr > 0 ? profitCurr / qtyCurr : 0);

    const adsSpendPrev =
      Number(
        total?.ads_spend_prev ??
        total?.ads_spend_month1 ??
        total?.ads_spend_previous ??
        0
      ) ||
      rows.reduce(
        (s, r: any) =>
          s +
          Number(
            r?.ads_spend_prev ??
            r?.ads_spend_month1 ??
            r?.ads_spend_previous ??
            0
          ),
        0
      );

    const adsSpendCurr =
      Number(
        total?.ads_spend_curr ??
        total?.ads_spend_month2 ??
        total?.ads_spend ??
        total?.total_ads ??
        total?.advertising_fees ??
        0
      ) ||
      rows.reduce(
        (s, r: any) =>
          s +
          Number(
            r?.ads_spend_curr ??
            r?.ads_spend_month2 ??
            r?.ads_spend ??
            r?.total_ads ??
            r?.advertising_fees ??
            0
          ),
        0
      );

    const adsSpendGrowthPct =
      Number(total?.ads_spend_growth_pct ?? total?.["Ads Growth"] ?? total?.["Ads Growth (%)"]) ||
      (adsSpendPrev ? ((adsSpendCurr - adsSpendPrev) / Math.abs(adsSpendPrev)) * 100 : 0);

    const hasCm2Data =
      hasCm2ProfitData(total) ||
      rows.some((r: any) => hasCm2ProfitData(r));

    const cm2ProfitPrev = hasCm2Data
      ? Number(total?.cm2_profit_prev ?? 0) ||
      rows.reduce((s, r: any) => s + Number(r.cm2_profit_prev ?? 0), 0)
      : 0;

    const cm2ProfitCurr = hasCm2Data
      ? Number(total?.cm2_profit_curr ?? 0) ||
      rows.reduce((s, r: any) => s + Number(r.cm2_profit_curr ?? 0), 0)
      : 0;

    const cm2ProfitPerUnitPrev = hasCm2Data
      ? Number(total?.cm2_profit_per_unit_prev ?? 0) ||
      (qtyPrev > 0 ? cm2ProfitPrev / qtyPrev : 0)
      : 0;

    const cm2ProfitPerUnitCurr = hasCm2Data
      ? Number(total?.cm2_profit_per_unit_curr ?? 0) ||
      (qtyCurr > 0 ? cm2ProfitCurr / qtyCurr : 0)
      : 0;

    const pct = (prev: number, curr: number) =>
      prev ? ((curr - prev) / prev) * 100 : 0;

    const getGrowthValueFromTotal = (frontKey: string, backendKey: string) => {
      const raw = total?.[frontKey] ?? total?.[backendKey];
      const value = typeof raw === "object" ? raw?.value : raw;
      const n = Number(value);
      return Number.isFinite(n) ? n : null;
    };

    const otherSkuItem: any = {
      product_name: "Other SKUs",
      ads_spend_prev: adsSpendPrev,
      ads_spend_curr: adsSpendCurr,
      ads_spend_growth_pct: adsSpendGrowthPct,

      quantity_month1: qtyPrev,
      quantity_month2: qtyCurr,
      quantity_prev: qtyPrev,
      quantity_curr: qtyCurr,

      net_sales_month1: netSalesPrev,
      net_sales_month2: netSalesCurr,
      net_sales_prev: netSalesPrev,
      net_sales_curr: netSalesCurr,

      profit_month1: profitPrev,
      profit_month2: profitCurr,
      profit_prev: profitPrev,
      profit_curr: profitCurr,

      asp_month1: aspPrev,
      asp_month2: aspCurr,
      asp_prev: aspPrev,
      asp_curr: aspCurr,

      unit_wise_profitability_month1: unitProfitPrev,
      unit_wise_profitability_month2: unitProfitCurr,
      unit_wise_profitability_prev: unitProfitPrev,
      unit_wise_profitability_curr: unitProfitCurr,

      coverage_ratio_months: Number(total?.coverage_ratio_months ?? 0),
      current_inventory: Number(total?.current_inventory ?? 0),
      included_product_count: Number(total?.included_product_count ?? rows.length ?? 0),

      ["Unit Growth"]: {
        value: getGrowthValueFromTotal("Unit Growth", "Unit Growth (%)") ?? pct(qtyPrev, qtyCurr),
        category: "",
      },
      ["ASP Growth"]: {
        value: getGrowthValueFromTotal("ASP Growth", "ASP Growth (%)") ?? pct(aspPrev, aspCurr),
        category: "",
      },
      ["Sales Growth"]: {
        value:
          getGrowthValueFromTotal("Sales Growth", "Net Sales Growth (%)") ??
          getGrowthValueFromTotal("Net Sales Growth", "Net Sales Growth (%)") ??
          pct(netSalesPrev, netSalesCurr),
        category: "",
      },
      ["CM1 Profit Impact"]: {
        value:
          getGrowthValueFromTotal("CM1 Profit Impact", "CM1 Profit Impact (%)") ??
          pct(profitPrev, profitCurr),
        category: "",
      },
      ["Profit Per Unit"]: {
        value:
          getGrowthValueFromTotal("Profit Per Unit", "Profit Per Unit (%)") ??
          pct(unitProfitPrev, unitProfitCurr),
        category: "",
      },
      ["Sales Mix Change"]: {
        value: getGrowthValueFromTotal("Sales Mix Change", "Sales Mix Change (%)") ?? 0,
        category: "",
      },

    };
    if (hasCm2Data) {
      otherSkuItem.cm2_profit_prev = cm2ProfitPrev;
      otherSkuItem.cm2_profit_curr = cm2ProfitCurr;
      otherSkuItem.cm2_profit_per_unit_prev = cm2ProfitPerUnitPrev;
      otherSkuItem.cm2_profit_per_unit_curr = cm2ProfitPerUnitCurr;
    }

    return otherSkuItem;
  }, [
    categorizedGrowth.other_skus,
    categorizedGrowth.other_total,
    remainingSkusAggregate,
  ]);

  const normalizeProductKey = (value: any) =>
    String(value || "")
      .trim()
      .toLowerCase()
      .replace(/^product\s*:\s*/i, "")
      .replace(/^\d+\.\s*/, "")
      .replace(/\s*\+\s*/g, " + ")
      .replace(/\s+/g, " ")
      .trim();

  const isComboProductName = (value: string) =>
    normalizeProductKey(value).includes(" + ");

  const getRecommendationSourceRow = useCallback(
    (productName: string) => {
      if (isOthersCardName(productName)) {
        return buildOtherSkusAggregateItem();
      }

      const normalized = normalizeProductKey(productName);
      if (!normalized) return null;

      const allRows = [
        ...(allActionRows || []),
        ...(categorizedGrowth.top_80_skus || []),
        ...(categorizedGrowth.new_skus || []),
        ...(categorizedGrowth.reviving_skus || []),
        ...(categorizedGrowth.other_skus || []),
      ];

      // 1) Exact product_name match first
      const exactMatch = allRows.find((row) => {
        const rowName = normalizeProductKey(row.product_name || "");
        return rowName === normalized;
      });

      if (exactMatch) return exactMatch;

      // 2) Exact SKU match, if backend/card ever uses SKU as key
      const skuMatch = allRows.find((row) => {
        const rowSku = normalizeProductKey(row.sku || "");
        return rowSku && rowSku === normalized;
      });

      if (skuMatch) return skuMatch;

      // 3) For combo products like Classic + Passion Fruit,
      // do NOT use includes matching, otherwise Classic will be picked incorrectly.
      if (isComboProductName(productName)) {
        return null;
      }

      // 4) Safe fallback only for non-combo product names
      return (
        allRows.find((row) => {
          const rowName = normalizeProductKey(row.product_name || "");
          if (!rowName) return false;

          if (isComboProductName(rowName)) return false;

          return rowName === normalized;
        }) || null
      );
    },
    [
      allActionRows,
      categorizedGrowth.top_80_skus,
      categorizedGrowth.new_skus,
      categorizedGrowth.reviving_skus,
      categorizedGrowth.other_skus,
      buildOtherSkusAggregateItem,
    ]
  );

  const parseOtherSkusBlock = (raw: string) => {
    const lines = (raw || "")
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean);

    const productName = lines[0] || "Other SKUs";

    const metrics: { label: string; value: string; color?: string }[] = [];
    const metricRegex =
      /^(ASP|Units|Net sales|Ads|CM2 profit per unit|CM2 profit|CM1 profit per unit|CM1 profit|Current inventory)\s*:\s*(.+)$/i;

    const insightParts: string[] = [];

    for (const line of lines.slice(1)) {
      const metricMatch = line.match(metricRegex);
      if (metricMatch) {
        const label = metricMatch[1];
        const rawMetricValue = metricMatch[2];
        const value =
          label.trim().toLowerCase() === "current inventory"
            ? rawMetricValue
            : convertMetricValueString(rawMetricValue, label);

        metrics.push({
          label,
          value,
          color:
            label.trim().toLowerCase() === "ads"
              ? "#414042"
              : value.includes("-")
                ? "#FF5C5C"
                : "#5EA68E",
        });
        continue;
      }
      insightParts.push(line);
    }

    const sourceRow = getRecommendationSourceRow(productName) as any;

    const hasStockCoverMetric = metrics.some((m) => {
      const label = m.label.trim().toLowerCase();
      return (
        label === "stock cover" ||
        label === "stock cover (months)" ||
        label === "current inventory"
      );
    });

    if (sourceRow && !hasStockCoverMetric) {
      const coverageRatio = Number(
        sourceRow.coverage_ratio_months ??
        sourceRow.coverageRatioMonths ??
        0
      );

      const currentInventory = Number(
        sourceRow.current_inventory ??
        sourceRow.currentInventory ??
        0
      );

      metrics.push(buildStockCoverMetric(sourceRow));
    }

    const finalMetrics = replaceProfitMetricsWithCm2IfAvailable(
      metrics,
      sourceRow
    );



    const insightText = insightParts.join("\n").trim();
    const sections = extractSections(insightText);

    return {
      productName,
      metrics: sortMetricsByOrder(finalMetrics),
      insightText,
      journeyPoints: sections.journeyPoints,
      recommendationPoints: sections.recommendationPoints,
      advertisingPoints: sections.advertisingPoints,
      inventoryPoints: sections.inventoryPoints,
    };
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
    if (activeTab !== "all_skus") {
      setShowAllSkus(false);
    }
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
    profit?: React.ReactNode;
  };

  const calcGrowthValue = (prev: number, curr: number) => {
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

  const openRecommendationDrawerForSku = (item: SkuItem) => {
    const insightEntry = getInsightForItem(item);
    const insight = insightEntry?.[1] || null;

    const parsedRecommendation = getRecommendedActionForItem(item);

    const insightSelected = isGlobalData()
      ? buildSelectedRecFromGlobalSkuInsight(item, insight)
      : buildSelectedRecFromSkuInsight(item, insight);

    const selected = parsedRecommendation
      ? {
        productName:
          item.product_name ||
          parsedRecommendation.productName ||
          insightSelected.productName,

        metrics: buildDrawerMetrics(
          parsedRecommendation.metrics?.length
            ? parsedRecommendation.metrics
            : insightSelected.metrics,
          item
        ),

        journeyPoints:
          parsedRecommendation.journeyPoints?.length
            ? parsedRecommendation.journeyPoints
            : insightSelected.journeyPoints,

        recommendationPoints:
          parsedRecommendation.recommendationPoints?.length
            ? parsedRecommendation.recommendationPoints
            : insightSelected.recommendationPoints,

        advertisingPoints:
          parsedRecommendation.advertisingPoints?.length
            ? parsedRecommendation.advertisingPoints
            : insightSelected.advertisingPoints,

        inventoryPoints:
          parsedRecommendation.inventoryPoints?.length
            ? parsedRecommendation.inventoryPoints
            : insightSelected.inventoryPoints,

        showChart: true,
      }
      : insightSelected;

    setSelectedSkuItem(item);
    setSelectedSku(item.sku || item.product_name);
    setSelectedRec(selected);
    setRecDrawerOpen(true);
  };

  const ProductNameCell = ({ item }: { item: SkuItem }) => {
    const productName = item.product_name || item.sku || "N/A";
    const isOthers = isOthersCardName(productName);

    return (
      <button
        type="button"
        onClick={() => openRecommendationDrawerForSku(item)}
        className="text-left text-green-500 underline-offset-2"
      >
        {isOthers ? "Others" : capitalizeWords(productName)}
      </button>
    );
  };

  const hideAdsFromRecommendationCard = (
    metrics: { label: string; value: string; color?: string }[] = []
  ) => {
    return metrics.filter(
      (m) => m.label.trim().toLowerCase() !== "ads"
    );
  };

  const columns: ColumnDef<BIGridRow>[] = useMemo(() => {
    const isNewRev = activeTab === "new_skus" || activeTab === "reviving_skus";
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
    ];

    return cols;
  }, [activeTab, month2Label]);



  const tableData: BIGridRow[] = useMemo(() => {
    const isNewRev = activeTab === "new_skus" || activeTab === "reviving_skus";
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
        product: <ProductNameCell item={item} />,
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

      };
    });

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
        product: <span className="text-green-500">Others</span>,
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
      return 'bg-[#EFEFEF] font-semibold';
    }
    return 'bg-white';
  };

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

  const splitMetricValue = (value: string) => {
    const v = String(value || "").trim();

    const match = v.match(/^(.+?)\s*(\(([-+])[^)]+\))\s*$/);

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
      deltaColor: sign === "+" ? "text-emerald-600" : "text-red-600",
    };
  };

  const formatRecommendationCardMainValue = (
    label: string,
    main: string
  ) => {
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

  const formatMetricDelta = (delta: string) => {
    const cleanDelta = String(delta || "")
      .replace(/[()]/g, "")
      .trim();

    if (!cleanDelta) return "";

    const isNegative = cleanDelta.startsWith("-");
    const valueWithoutSign = cleanDelta.replace(/^[-+]/, "");

    return `${isNegative ? "▼" : "▲"} ${valueWithoutSign}`;
  };

  const formatGlobalMetricValue = (
    value: number,
    growth: number,
    type: "money" | "number" = "money",
    label?: string
  ) => {
    const normalizedLabel = String(label || "").trim().toLowerCase();

    const main =
      type === "number"
        ? Number(value || 0).toLocaleString()
        : normalizedLabel === "net sales" || normalizedLabel === "cm1 profit"
          ? formatDisplayAmountNoDecimals(Number(value || 0))
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
      const rawProductName = row.product_name || "";

      const isOtherSkuCard =
        isOthersCardName(rawProductName) ||
        row === otherCardRow;

      const productName = isOtherSkuCard ? "Other SKUs" : rawProductName;

      const journey = getGlobalProductJourney(rawProductName);

      const ukAction = getFirstCountryAction(journey, "uk");
      const usAction = getFirstCountryAction(journey, "us");

      const coverageRatio = Number(
        row.coverage_ratio_months ??
        row.coverageRatioMonths ??
        0
      );

      const currentInventory = Number(
        row.current_inventory ??
        row.currentInventory ??
        0
      );

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
            getGrowthValue(row, "Net Sales Growth (%)"),
            "money",
            "Net sales"
          ),
        },
        {
          label: "ASP",
          value: formatGlobalMetricValue(
            Number(row.asp_curr || row.asp_month2 || row.asp || 0),
            getGrowthValue(row, "ASP Growth (%)")
          ),
        },
        ...(hasCm2ProfitData(row)
          ? [
            {
              label: "CM2 profit",
              value: formatGlobalMetricValue(
                Number(row.cm2_profit_curr || 0),
                getPctGrowth(row.cm2_profit_prev, row.cm2_profit_curr),
                "money",
                "CM2 profit"
              ),
            },
            {
              label: "CM2 profit per unit",
              value: formatGlobalMetricValue(
                Number(row.cm2_profit_per_unit_curr || 0),
                getPctGrowth(
                  row.cm2_profit_per_unit_prev,
                  row.cm2_profit_per_unit_curr
                )
              ),
            },
          ]
          : [
            {
              label: "CM1 profit",
              value: formatGlobalMetricValue(
                Number(row.profit_curr || row.profit_month2 || row.profit || 0),
                getGrowthValue(row, "CM1 Profit Impact (%)"),
                "money",
                "CM1 profit"
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
          ]),
        {
          label: "Current inventory",
          value: `${Number.isFinite(coverageRatio) ? coverageRatio.toFixed(2) : "0.00"} months`,
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
        productName: isOtherSkuCard ? "Other SKUs" : toTitleCase(productName),
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

  const buildOthersActionFromCategorizedGrowth = useCallback(() => {
    const total = categorizedGrowth.other_total;
    const rows = categorizedGrowth.other_skus || [];

    if (!total && rows.length === 0) return "";

    const qty =
      Number((total as any)?.quantity_curr ?? (total as any)?.quantity_month2 ?? 0) ||
      rows.reduce((s, r: any) => s + Number(r.quantity_curr ?? r.quantity_month2 ?? 0), 0);

    const netSales =
      Number((total as any)?.net_sales_curr ?? (total as any)?.net_sales_month2 ?? 0) ||
      rows.reduce((s, r: any) => s + Number(r.net_sales_curr ?? r.net_sales_month2 ?? 0), 0);

    const profit =
      Number((total as any)?.profit_curr ?? (total as any)?.profit_month2 ?? 0) ||
      rows.reduce((s, r: any) => s + Number(r.profit_curr ?? r.profit_month2 ?? 0), 0);

    const asp =
      Number((total as any)?.asp_curr ?? (total as any)?.asp_month2 ?? 0) ||
      (qty > 0 ? netSales / qty : 0);

    const unitProfit =
      Number(
        (total as any)?.unit_wise_profitability_curr ??
        (total as any)?.unit_wise_profitability_month2 ??
        0
      ) || (qty > 0 ? profit / qty : 0);

    const getGrowth = (key: string) => {
      const raw = (total as any)?.[key];
      const value = typeof raw === "object" ? raw?.value : raw;
      const n = Number(value);
      return Number.isFinite(n) ? n : 0;
    };

    const fmt = (n: number) =>
      formatDisplayAmount(convertToDisplayCurrency(n, sourceCurrency));

    const fmtNoDec = (n: number) =>
      formatDisplayAmountNoDecimals(convertToDisplayCurrency(n, sourceCurrency));

    const pct = (n: number) => `${n > 0 ? "+" : ""}${n.toFixed(2)}%`;

    return [
      `Other SKUs`,
      ``,
      `ASP: ${fmt(asp)} (${pct(getGrowth("ASP Growth") || getGrowth("ASP Growth (%)"))})`,
      `Units: ${qty.toLocaleString()} (${pct(getGrowth("Unit Growth") || getGrowth("Unit Growth (%)"))})`,
      `Net sales: ${fmtNoDec(netSales)} (${pct(getGrowth("Sales Growth") || getGrowth("Net Sales Growth") || getGrowth("Net Sales Growth (%)"))})`,
      `CM1 profit: ${fmtNoDec(profit)} (${pct(getGrowth("CM1 Profit Impact") || getGrowth("CM1 Profit Impact (%)"))})`,
      `CM1 profit per unit: ${fmt(unitProfit)} (${pct(getGrowth("Profit Per Unit") || getGrowth("Profit Per Unit (%)"))})`,
      ``,
      `Recommendation: Monitor the remaining SKUs and prioritize actions based on units, net sales, ASP, and CM1 profit.`,
    ].join("\n");
  }, [
    categorizedGrowth.other_total,
    categorizedGrowth.other_skus,
    convertToDisplayCurrency,
    formatDisplayAmount,
    formatDisplayAmountNoDecimals,
    sourceCurrency,
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

  const formatUnfulfillableInventoryText = (line: string) => {
    const raw = String(line || "").trim();

    const pctMatch = raw.match(/\(([^)]+%)\)/);
    const isAbove = raw.toLowerCase().includes("above");
    const isBelow = raw.toLowerCase().includes("below");

    if (isAbove) {
      return `Above 1% of Total Inventory${pctMatch?.[1] ? ` (${pctMatch[1]})` : ""}`;
    }

    if (isBelow) {
      return `Below 1% of Total Inventory${pctMatch?.[1] ? ` (${pctMatch[1]})` : ""}`;
    }

    const colonMatch = raw.match(/:\s*(.+)$/);
    return colonMatch?.[1]?.trim() || raw;
  };

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
        result[currentCountry].unfulfillableInventory =
          formatUnfulfillableInventoryText(clean);
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
        result.unfulfillableInventory = formatUnfulfillableInventoryText(line);
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

    if (c === "uk") return "border-t-[#5EA68E]";
    if (c === "us") return "border-t-[#3A8EA4]";
    if (c === "ca") return "border-t-[#D97706]";
    if (c === "india") return "border-t-[#8B5CF6]";

    return "border-t-[#5EA68E]";
  };

  const splitInventoryValue = (label: string, value?: string) => {
    const rawValue = String(value || "").trim();
    const lowerLabel = label.toLowerCase();

    if (!rawValue) {
      return {
        main: "—",
        sub: "",
      };
    }

    if (lowerLabel.includes("ageing")) {
      const match = rawValue.match(/^(.+?)\s+across\s+(.+)$/i);

      if (match) {
        return {
          main: match[1].trim(),
          sub: `across ${match[2].trim()}`,
        };
      }
    }

    return {
      main: rawValue,
      sub: "",
    };
  };

  const InventoryMetricCard = ({
    label,
    value,
  }: {
    label: string;
    value?: string;
  }) => {
    const { main, sub } = splitInventoryValue(label, value);

    return (
      <div className="min-h-[72px] rounded-lg border border-slate-200 bg-slate-50/70 p-3">
        <div className="text-xs font-medium text-slate-500">
          {label}
        </div>

        <div className="mt-1 flex flex-wrap items-baseline gap-1 leading-tight">
          <span className="text-base font-bold text-slate-900">
            {main}
          </span>

          {sub ? (
            <span className="text-xs font-medium text-slate-500">
              {sub}
            </span>
          ) : null}
        </div>
      </div>
    );
  };

  const InventoryCard = ({
    title,
    countryCode,
    rows,
    accentClass,
    showHeader = true,
  }: {
    title: string;
    countryCode?: string;
    rows: {
      label: string;
      value?: string;
    }[];
    accentClass: string;
    showHeader?: boolean;
  }) => {
    const visibleRows = rows.filter((row) => row.value !== undefined);

    if (!visibleRows.length) return null;

    return (
      <div
        className={[
          "w-full overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm",
          "border-t-4 transition hover:shadow-md",
          accentClass,
        ].join(" ")}
      >
        <div className="p-4">
          {showHeader && (
            <div className="mb-4 flex items-center gap-2">
              <h3 className="text-sm font-bold text-slate-800">
                {title}
              </h3>
            </div>
          )}

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            {visibleRows.map((item, idx) => (
              <InventoryMetricCard
                key={`${item.label}-${idx}`}
                label={item.label}
                value={item.value}
              />
            ))}
          </div>

          <button
            type="button"
            onClick={goToInventoryReconciliation}
            className="mt-4 inline-flex items-center text-sm font-semibold text-[#5EA68E] transition hover:text-[#4B8F7A] hover:underline"
          >
            View inventory reconciliation
            <span className="ml-1">→</span>
          </button>
        </div>
      </div>
    );
  };

  const effectiveRemainingSkusBlock = useMemo(() => {
    return normalizeTextBlock(remainingSkusBlock) || buildOthersActionFromCategorizedGrowth();
  }, [remainingSkusBlock, buildOthersActionFromCategorizedGrowth]);

  const GlobalInventoryInsights = () => {
    const inventory = parseGlobalInventoryItems(portfolioInventoryBlock);

    const hasUkInventory = Object.values(inventory.uk).some(Boolean);
    const hasUsInventory = Object.values(inventory.us).some(Boolean);

    const hasInventory = hasUkInventory || hasUsInventory;
    if (!hasInventory) return null;

    const visibleCountryCount = Number(hasUkInventory) + Number(hasUsInventory);

    const buildRows = (items: {
      ageingInventory?: string;
      estimatedStorageCost?: string;
      unfulfillableInventory?: string;
      highCoverage?: string;
    }) => [
        {
          label: "Ageing Inventory",
          value: items.ageingInventory,
        },
        {
          label: "Est. Storage Cost",
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
      ];

    return (
      <div className="w-full space-y-4 rounded-xl border border-slate-200 bg-white shadow-sm p-4">
        <div className="flex items-center gap-2">
          <span className="text-base 2xl:text-2xl font-bold text-slate-800">
            Inventory Insights
          </span>
        </div>

        <div
          className={[
            "grid grid-cols-1 gap-4",
            visibleCountryCount > 1 ? "lg:grid-cols-2" : "lg:grid-cols-1",
          ].join(" ")}
        >
          {hasUkInventory && (
            <InventoryCard
              title="UK Inventory"
              countryCode="GB"
              rows={buildRows(inventory.uk)}
              accentClass={getInventoryAccentClass("uk")}
            />
          )}

          {hasUsInventory && (
            <InventoryCard
              title="US Inventory"
              countryCode="US"
              rows={buildRows(inventory.us)}
              accentClass={getInventoryAccentClass("us")}
            />
          )}
        </div>
      </div>
    );
  };

  const goToInventoryReconciliation = () => {
    const routeCountry = String(sourceCountryName || countryName || "")
      .trim()
      .toLowerCase();

    const routeMonth =
      String(month || "")
        .trim()
        .charAt(0)
        .toUpperCase() + String(month || "").trim().slice(1).toLowerCase();

    const routeYear = String(year || "").trim();

    router.push(
      `/inventory-reconciliation/${routeCountry}/${routeMonth}/${routeYear}`
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

  const businessSummaryPoints = useMemo(() => {
    const metricPoints = Array.isArray(overallSummary)
      ? overallSummary.map(String).filter(Boolean)
      : [];

    if (metricPoints.length) return metricPoints;

    return splitIntoPoints(summaryText);
  }, [overallSummary, summaryText]);

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
                      <div className="relative overflow-hidden bg-white border border-[#D9D9D9] rounded-xl shadow-sm p-4 text-xs 2xl:text-sm text-charcoal-500 w-full h-full flex flex-col">
                        <div className="flex items-start justify-between gap-3">
                          <div className="flex flex-col sm:flex-row sm:items-end gap-1 sm:gap-2">
                            <PageBreadcrumb
                              pageTitle={isGlobalData() ? "Global Business Summary" : "Business Summary"}
                              variant="page"
                              align="left"
                            />

                            {aiLastRefreshedAt && (
                              <span className="text-[10px] 2xl:text-xs text-slate-500 leading-5">
                                Last updated: {aiLastRefreshedAt}
                              </span>
                            )}
                          </div>

                          <button
                            type="button"
                            onClick={handleManualAiRefresh}
                            disabled={manualAiRefreshing}
                            className="shrink-0 inline-flex items-center justify-center rounded-md border border-slate-300 bg-white p-2 text-charcoal-600 shadow-sm hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
                            title={manualAiRefreshing ? "Refreshing..." : "Refresh business summary and recommendations"}
                            aria-label="Refresh business summary and recommendations"
                          >
                            <IoRefresh
                              size={16}
                            // className={manualAiRefreshing ? "animate-spin" : ""}
                            />
                          </button>
                        </div>

                        {manualAiRefreshing && (
                          <div className="absolute inset-0 z-20 flex items-center justify-center rounded-xl bg-white/70 backdrop-blur-[1px]">
                            <Loader transparent />
                          </div>
                        )}


                        {businessSummaryPoints.length > 0 && (
                          <ul className="mt-3 list-disc pl-5 2xl:text-sm text-xs text-charcoal-500 border-slate-300 flex-1 leading-relaxed space-y-2">
                            {businessSummaryPoints.map((point, index) => (
                              <li key={index}>{point}</li>
                            ))}
                          </ul>
                        )}

                        {portfolioRecommendation && (
                          <div className="mt-3 p-3 rounded-lg bg-slate-50 border border-slate-200 flex flex-col items-start gap-2">
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
                  Object.keys(recommendedActions || {}).length > 0 ||
                  !!effectiveRemainingSkusBlock
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
                                      const isSkuGroup = isOthersCardName(card.productName);

                                      setSelectedRec({
                                        productName: card.productName,
                                        metrics: card.metrics,
                                        journeyPoints: card.journeyPoints,
                                        recommendationPoints: card.recommendationPoints,
                                        advertisingPoints: card.advertisingPoints,
                                        inventoryPoints: card.inventoryPoints,

                                        // ✅ keep chart visible for Other/Remaining SKU groups too
                                        showChart: true,

                                        // ✅ tells drawer/chart to use aggregate graph mode
                                        isOtherSkus: isSkuGroup,
                                        otherSkuProductNames: isSkuGroup ? getOtherSkuProductNames() : [],
                                      });

                                      setRecDrawerOpen(true);
                                    }}
                                    className="px-3 py-1.5 rounded-lg text-xs font-semibold bg-slate-800 text-yellow-200 hover:bg-slate-700 transition whitespace-nowrap"
                                  >
                                    Detailed View
                                  </button>
                                </div>

                                {hideAdsFromRecommendationCard(card.metrics)?.length > 0 && (
                                  <div className="grid grid-cols-3 gap-2">
                                    {hideAdsFromRecommendationCard(card.metrics).map((m, i) => (
                                      <div
                                        key={i}
                                        className="rounded-lg px-2 border border-slate-200 bg-slate-50 py-2 min-w-0"
                                      >
                                        <div className="text-[10px] 2xl:text-xs font-medium text-charcoal-500 leading-none truncate">
                                          {m.label}
                                        </div>

                                        {(() => {
                                          const { main, delta, deltaColor } = splitMetricValue(m.value);
                                          const displayMain = formatRecommendationCardMainValue(
                                            m.label,
                                            main
                                          );

                                          return (
                                            <div className="mt-1 flex w-full items-baseline justify-between gap-2 min-w-0">
                                              <span className="text-[10px] 2xl:text-xs font-semibold text-charcoal-500 truncate whitespace-pre-line">
                                                {displayMain}
                                              </span>

                                              {delta ? (
                                                <span
                                                  className={`text-[10px] 2xl:text-xs font-semibold shrink-0 whitespace-nowrap text-right ${deltaColor}`}
                                                >
                                                  {formatMetricDelta(delta)}
                                                </span>
                                              ) : null}
                                            </div>
                                          );
                                        })()}
                                      </div>
                                    ))}
                                  </div>
                                )}

                                {(() => {
                                  const cardInventoryPoints = [
                                    ...(card.inventoryPoints || []),
                                    ...(card.recommendationPoints || []).filter((p) => /inventory/i.test(p)),
                                    ...(card.advertisingPoints || []).filter((p) => /inventory/i.test(p)),
                                  ];

                                  const cardActionPoints = (card.recommendationPoints || []).filter(
                                    (p) => !/inventory/i.test(p)
                                  );

                                  return (
                                    <div className="space-y-1 font-lato text-xs 2xl:text-sm leading-[20px] font-normal text-slate-900">
                                      {cardActionPoints[0] && (
                                        <div className="flex items-start gap-1.5">
                                          <span className="shrink-0 text-xs 2xl:text-sm leading-[20px] font-normal text-slate-900">
                                            1.
                                          </span>

                                          <span className="line-clamp-2 text-xs 2xl:text-sm leading-[20px] font-normal text-slate-900">
                                            {cardActionPoints[0]}
                                          </span>
                                        </div>
                                      )}

                                      {cardInventoryPoints[0] && (
                                        <div className="flex items-start gap-1.5">
                                          <span className="shrink-0 text-xs 2xl:text-sm leading-[20px] font-normal text-slate-900">
                                            2.
                                          </span>

                                          <span className="line-clamp-2 text-xs 2xl:text-sm leading-[20px] font-normal text-slate-900">
                                            {cleanInventoryCardPoint(cardInventoryPoints[0])}
                                          </span>
                                        </div>
                                      )}
                                    </div>
                                  );
                                })()}
                              </motion.div>
                            );
                          })
                          : sortedRecommendations.map(({ key, text, parsed, netSales }, idx) => {
                            const recommendationPoints = parsed.recommendationPoints || [];

                            const inventoryPoints = [
                              ...(parsed.inventoryPoints || []),
                              ...(parsed.recommendationPoints || []).filter((p) => /inventory/i.test(p)),
                              ...(parsed.advertisingPoints || []).filter((p) => /inventory/i.test(p)),
                            ];

                            const actionPoints = recommendationPoints.filter(
                              (p) => !/inventory/i.test(p)
                            );

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
                                      const sourceRow = getRecommendationSourceRow(parsed.productName);
                                      const isSkuGroup = isOthersCardName(parsed.productName);

                                      setSelectedSkuItem(sourceRow);
                                      setSelectedSku(sourceRow?.sku || parsed.productName);

                                      setSelectedRec({
                                        productName: parsed.productName,
                                        metrics: buildDrawerMetrics(parsed.metrics, sourceRow),
                                        journeyPoints: parsed.journeyPoints,
                                        recommendationPoints: parsed.recommendationPoints,
                                        advertisingPoints: parsed.advertisingPoints,
                                        inventoryPoints: parsed.inventoryPoints,
                                        showChart: true,

                                        // ✅ supports Other SKUs / Remaining SKUs / Rest SKUs etc.
                                        isOtherSkus: isSkuGroup,
                                        otherSkuProductNames: isSkuGroup ? getOtherSkuProductNames() : [],
                                      });

                                      setRecDrawerOpen(true);
                                    }}
                                    className="px-3 py-1.5 rounded-lg text-xs font-semibold bg-blue-700 text-yellow-200 hover:bg-slate-700 transition whitespace-nowrap"
                                  >
                                    Detailed View
                                  </button>
                                </div>

                                {hideAdsFromRecommendationCard(parsed.metrics)?.length > 0 && (
                                  <div className="grid grid-cols-3 gap-2">
                                    {hideAdsFromRecommendationCard(parsed.metrics).map((m, i) => (
                                      <div
                                        key={i}
                                        className="rounded-lg px-2 border border-slate-200 bg-slate-50 py-2 min-w-0"
                                      >
                                        <div className="text-[10px] 2xl:text-xs font-medium text-charcoal-500 leading-none truncate">
                                          {m.label}
                                        </div>

                                        {(() => {
                                          const { main, delta, deltaColor } = splitMetricValue(m.value);
                                          const displayMain = formatRecommendationCardMainValue(
                                            m.label,
                                            main
                                          );

                                          return (
                                            <div className="mt-1 flex w-full items-baseline justify-between gap-2 min-w-0">
                                              <span className="text-[10px] 2xl:text-xs font-semibold text-charcoal-500 truncate">
                                                {displayMain}
                                              </span>

                                              {delta ? (
                                                <span
                                                  className={`text-[10px] 2xl:text-xs font-semibold shrink-0 whitespace-nowrap text-right ${deltaColor}`}
                                                >
                                                  {formatMetricDelta(delta)}
                                                </span>
                                              ) : null}
                                            </div>
                                          );
                                        })()}
                                      </div>
                                    ))}
                                  </div>
                                )}

                                {(actionPoints.length > 0 || inventoryPoints.length > 0) && (
                                  <div className="space-y-1  text-xs 2xl:text-sm text-charcoal-500 leading-relaxed">
                                    {actionPoints[0] && (
                                      <div className="flex gap-2">
                                        <span className="shrink-0 font-semibold">1.</span>
                                        <span className="line-clamp-2">
                                          {actionPoints[0]}
                                        </span>
                                      </div>
                                    )}

                                    {inventoryPoints[0] && (
                                      <div className="flex gap-2">
                                        <span className="shrink-0 font-semibold">2.</span>
                                        <span className="line-clamp-2">
                                          {cleanInventoryCardPoint(inventoryPoints[0])}
                                        </span>
                                      </div>
                                    )}
                                  </div>
                                )}
                              </motion.div>
                            );
                          })}

                        {!isGlobalData() && effectiveRemainingSkusBlock && (() => {
                          const parsedOther = parseOtherSkusBlock(effectiveRemainingSkusBlock);

                          const otherIdx = sortedRecommendations.length;
                          const displayNumber = otherIdx + 1;
                          const otherCardName = "Other SKUs";

                          const borderColor = topBorderColors[otherIdx % topBorderColors.length];

                          return (
                            <motion.div
                              key="other-skus-card"
                              initial={{ opacity: 0, y: 16 }}
                              animate={{ opacity: 1, y: 0 }}
                              transition={{ duration: 0.35, delay: 0.06 * otherIdx }}
                              className={[
                                "global-recommendation-card",
                                "bg-white rounded-xl border border-slate-200 shadow-sm hover:shadow-md transition-shadow",
                                "border-t-4",
                                "p-3 space-y-3",
                              ].join(" ")}
                            >
                              <div className="flex items-start justify-between gap-3">
                                <div className="text-sm font-semibold text-slate-800 line-clamp-2">
                                  {displayNumber}. {otherCardName}
                                </div>

                                <button
                                  type="button"
                                  onClick={() => {
                                    const parsedOther = parseOtherSkusBlock(effectiveRemainingSkusBlock);
                                    const otherSourceRow = buildOtherSkusAggregateItem();

                                    setSelectedSkuItem(otherSourceRow);
                                    setSelectedSku("__OTHER_SKUS__");

                                    setSelectedRec({
                                      productName: otherCardName,
                                      metrics: buildDrawerMetrics(parsedOther.metrics, otherSourceRow),
                                      journeyPoints: parsedOther.journeyPoints,
                                      recommendationPoints: parsedOther.recommendationPoints,
                                      advertisingPoints: parsedOther.advertisingPoints,
                                      inventoryPoints: parsedOther.inventoryPoints,
                                      showChart: true,

                                      isOtherSkus: true,
                                      otherSkuProductNames: getOtherSkuProductNames(),
                                    });

                                    setRecDrawerOpen(true);
                                  }}
                                  className="px-3 py-1.5 rounded-lg text-xs font-semibold bg-blue-700 text-yellow-200 hover:bg-slate-700 transition whitespace-nowrap"
                                >
                                  Detailed View
                                </button>
                              </div>

                              {hideAdsFromRecommendationCard(parsedOther.metrics)?.length > 0 && (
                                <div className="grid grid-cols-3 gap-2">
                                  {hideAdsFromRecommendationCard(parsedOther.metrics).map((m, i) => (
                                    <div
                                      key={i}
                                      className="rounded-lg px-2 border border-slate-200 bg-slate-50 py-2 min-w-0"
                                    >
                                      <div className="text-[10px] 2xl:text-xs font-medium text-charcoal-500 leading-none truncate">
                                        {m.label}
                                      </div>

                                      {(() => {
                                        const { main, delta, deltaColor } = splitMetricValue(m.value);
                                        const displayMain = formatRecommendationCardMainValue(
                                          m.label,
                                          main
                                        );

                                        return (
                                          <div className="mt-1 flex w-full items-baseline justify-between gap-2 min-w-0">
                                            <span className="text-[10px] 2xl:text-xs font-semibold text-charcoal-500 truncate">
                                              {displayMain}
                                            </span>

                                            {delta ? (
                                              <span
                                                className={`text-[10px] 2xl:text-xs font-semibold shrink-0 whitespace-nowrap text-right ${deltaColor}`}
                                              >
                                                {formatMetricDelta(delta)}
                                              </span>
                                            ) : null}
                                          </div>
                                        );
                                      })()}
                                    </div>
                                  ))}
                                </div>
                              )}

                              {(() => {
                                const otherInventoryPoints = [
                                  ...(parsedOther.inventoryPoints || []),
                                  ...(parsedOther.recommendationPoints || []).filter((p) => /inventory/i.test(p)),
                                  ...(parsedOther.advertisingPoints || []).filter((p) => /inventory/i.test(p)),
                                ];

                                const otherActionPoints = (parsedOther.recommendationPoints || []).filter(
                                  (p) => !/inventory/i.test(p)
                                );

                                return (
                                  <div className="space-y-1 text-[10px] 2xl:text-xs text-slate-700 leading-relaxed">
                                    {otherActionPoints[0] && (
                                      <div className="flex gap-2">
                                        <span className="shrink-0 font-semibold">1.</span>
                                        <span className="line-clamp-2">
                                          {otherActionPoints[0]}
                                        </span>
                                      </div>
                                    )}

                                    {otherInventoryPoints[0] && (
                                      <div className="flex gap-2">
                                        <span className="shrink-0 font-semibold">2.</span>
                                        <span className="line-clamp-2">
                                          {cleanInventoryCardPoint(otherInventoryPoints[0])}
                                        </span>
                                      </div>
                                    )}
                                  </div>
                                );
                              })()}
                            </motion.div>
                          );
                        })()}
                      </div>
                    </div>
                  )}

                {/* 4) Inventory Insight */}
                {/* {isGlobalData() ? (
                  <GlobalInventoryInsights />
                ) : (
                  <SingleCountryInventoryInsights />
                )} */}

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
                    {activeTab === "all_skus" && allSkuRows.length > 5 && (
                      <button
                        type="button"
                        onClick={() => setShowAllSkus((prev) => !prev)}
                        title={showAllSkus ? "Collapse rows" : "Expand all rows"}
                        aria-label={showAllSkus ? "Collapse rows" : "Expand all rows"}
                        className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                      >
                        {showAllSkus ? (
                          <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                        ) : (
                          <RiExpandDiagonalFill size={18} className="font-extrabold" />
                        )}
                      </button>
                    )}

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

                    {activeTab === "all_skus" && allSkuRows.length > 5 && (
                      <button
                        type="button"
                        onClick={() => setShowAllSkus((prev) => !prev)}
                        title={showAllSkus ? "Collapse rows" : "Expand all rows"}
                        aria-label={showAllSkus ? "Collapse rows" : "Expand all rows"}
                        className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                      >
                        {showAllSkus ? (
                          <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                        ) : (
                          <RiExpandDiagonalFill size={18} className="font-extrabold" />
                        )}
                      </button>
                    )}

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
                    paginate={false}
                    scrollY={false}
                    maxHeight="none"
                    loading={false}
                    headerMaxWidth={140}
                    emptyMessage={getSkuEmptyMessage()}
                    rowClassName={rowClassNameForDataTable}
                    isTotalRow={(row) => !!row.__isTotal}
                    bodyMaxHeight={
                      showAllSkus && activeTab === "all_skus" && allSkuRows.length > 15
                        ? 40 * 15
                        : undefined
                    }
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
        sourceCountryName={sourceCountryName || countryName}
        displayCurrency={displayCurrency}
        formattedMonthYear={drawerMonthYear}
      />
    </>
  );
};

