'use client';

import React, { useState, useEffect, useMemo } from 'react';
import axios from 'axios';
import * as XLSX from "xlsx-js-style";
import { FaThumbsUp, FaThumbsDown } from 'react-icons/fa';
import Productinfoinpopup from '@/components/businessInsight/Productinfoinpopup';
import { IoDownload } from 'react-icons/io5';
import { BsStars } from 'react-icons/bs';
import Drawer from '@mui/material/Drawer';
import IconButton from '@mui/material/IconButton';
import { FaArrowUp, FaArrowDown } from 'react-icons/fa';
import Loader from '@/components/loader/Loader';
import DataTable, { ColumnDef } from '@/components/ui/table/DataTable';
import DownloadIconButton from '@/components/ui/button/DownloadIconButton';
import SegmentedToggle from '@/components/ui/SegmentedToggle';
import { AiButton } from '@/components/ui/button/AiButton';
import PageBreadcrumb from '@/components/common/PageBreadCrumb';
import { useGetUserDataQuery } from '@/lib/api/profileApi';


type MonthsforBIProps = {
  countryName: string; // "uk" | "us" | "ca"
  ranged: string; // "QTD", "MTD", etc
  month: string; // "november"
  year: string; // "2025"
  initialData?: any;
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

  // raw fields for Excel (may be null with live API)
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

  // mapped fields for Excel export
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
  insight: string;
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

  overall_actions?: string[];
    recommended_actions_mtd?: Record<string, string>;
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

const getAbbr = (m?: string) => {
  if (!m) return '';
  return m.slice(0, 3);
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
  ranged,
  month,
  year,
  initialData,
}: MonthsforBIProps) {
  const { data: userData } = useGetUserDataQuery();

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

  // overall bullets from backend
  const [summaryText, setSummaryText] = useState<string>("");
  const [overallSummary, setOverallSummary] = useState<string[]>([]);

  const [overallActions, setOverallActions] = useState<any[]>([]);
  const [recommendedActions, setRecommendedActions] = useState<Record<string, string>>({});

  const [insightDate, setInsightDate] = useState<string | null>(null);

  // Insights + modal
  const [loadingInsight, setLoadingInsight] = useState<boolean>(false);
  const [skuInsights, setSkuInsights] = useState<Record<string, SkuInsight>>(
    {}
  );
  const [selectedSku, setSelectedSku] = useState<string | null>(null);
  const [modalOpen, setModalOpen] = useState<boolean>(false);

  // Feedback
  const [fbType, setFbType] = useState<'like' | 'dislike' | null>(null);
  const [fbText, setFbText] = useState<string>('');
  const [fbSubmitting, setFbSubmitting] = useState<boolean>(false);
  const [fbSuccess, setFbSuccess] = useState<boolean>(false);

  const [pageLoading, setPageLoading] = useState<boolean>(false);
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

      // ✅ ADD THIS
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

      if (saved.insightDate === todayKey) {
        if (saved.overallActions) setOverallActions(saved.overallActions);
        if (saved.summaryText) setSummaryText(saved.summaryText);
        if (saved.overallSummary) setOverallSummary(saved.overallSummary);

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

    // normal fetch: clear per-SKU AI insights & show full loader
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

      const actionsFromApi = res.data.overall_actions || [];
      const recommendedActionsFromApi = res.data.recommended_actions_mtd || {};



      setSummaryText(summaryTextFromApi);
      setOverallSummary(summaryBulletsFromApi);
      setRecommendedActions(recommendedActionsFromApi);

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
    if (!normalizedCountry || normalizedCountry === 'global') return;
    fetchLiveBi(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [normalizedCountry, ranged, month, year]);

  // =========================
  // AI insights generate (button)
  // =========================

  const analyzeSkus = async () => {
    setLoadingInsight(true);
    try {
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

  const getCurrencySymbolForExcel = () => {
    const isGlobal = (countryName || "").toLowerCase() === "global";

    // your profile field (adjust the key if your API uses a different name)
    const homeCode =
      (userData as any)?.homeCurrency ||
      (userData as any)?.home_currency ||
      "";

    const countryCode = countryToCurrencyCode(countryName);

    const codeToUse = isGlobal ? homeCode : countryCode || homeCode;
    return currencyCodeToSymbol(codeToUse);
  };

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

        const gsOld = pickOld(row, "product_sales_month1", "product_sales_month2");
        const gsNew = pickNew(row, "product_sales_month1", "product_sales_month2");

        const nsOld = pickOld(row, "net_sales_month1", "net_sales_month2");
        const nsNew = pickNew(row, "net_sales_month1", "net_sales_month2");

        const aspOld = pickOld(row, "asp_month1", "asp_month2");
        const aspNew = pickNew(row, "asp_month1", "asp_month2");

        // ✅ Sales mix from section net sales totals
        const mixOld = totalNsOld ? (num(nsOld) / totalNsOld) * 100 : null;
        const mixNew = totalNsNew ? (num(nsNew) / totalNsNew) * 100 : null;

        const cm1Old = pickOld(row, "profit_month1", "profit_month2");
        const cm1New = pickNew(row, "profit_month1", "profit_month2");

        // ✅ Profit mix from denom (grand in Sheet 2; section in Sheet 1)
        const profitMixNew = denomProfitNew ? (num(cm1New) / denomProfitNew) * 100 : null;
        const profitMixOld = denomProfitOld ? (num(cm1Old) / denomProfitOld) * 100 : null;

        const cm1PctOld = pickOld(row, "profit_percentage_month1", "profit_percentage_month2");
        const cm1PctNew = pickNew(row, "profit_percentage_month1", "profit_percentage_month2");

        const upOld = pickOld(row, "unit_wise_profitability_month1", "unit_wise_profitability_month2");
        const upNew = pickNew(row, "unit_wise_profitability_month1", "unit_wise_profitability_month2");

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

      // totals
      const totals = clean.reduce(
        (acc, r) => {
          acc.qtyOld += num(pickOld(r, "quantity_month1", "quantity_month2"));
          acc.qtyNew += num(pickNew(r, "quantity_month1", "quantity_month2"));

          acc.gsOld += num(pickOld(r, "product_sales_month1", "product_sales_month2"));
          acc.gsNew += num(pickNew(r, "product_sales_month1", "product_sales_month2"));

          acc.nsOld += num(pickOld(r, "net_sales_month1", "net_sales_month2"));
          acc.nsNew += num(pickNew(r, "net_sales_month1", "net_sales_month2"));

          acc.cm1Old += num(pickOld(r, "profit_month1", "profit_month2"));
          acc.cm1New += num(pickNew(r, "profit_month1", "profit_month2"));

          acc.upOld += num(pickOld(r, "unit_wise_profitability_month1", "unit_wise_profitability_month2"));
          acc.upNew += num(pickNew(r, "unit_wise_profitability_month1", "unit_wise_profitability_month2"));

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

  const renderAiActionLine = (line: string) => {
    if (!line) return null;

    // 1) Remove leading "Product name -"
    const cleaned = line.replace(/^\s*Product\s*name\s*[-–:]\s*/i, "").trim();

    // 2) Extract product name safely:
    //    take text from start until we hit common sentence starters like "The/There/A/An/Increase/Decrease/..."
    const productMatch = cleaned.match(
      /^(.+?)(?=\s+(?:The|There|A|An|Increase|Decreased|Decreasing|Increased|Increasing)\b|$)/i
    );

    const productName = (productMatch?.[1] || "").trim();

    // 3) Remaining text (everything after product name)
    let rest = cleaned;
    if (productName) {
      // remove ONLY first occurrence at the start
      rest = rest.replace(new RegExp("^" + productName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "\\s*", "i"), "");
    }
    rest = rest.trim();

    // 4) Split into sentences (keeps simple, works for your AI text)
    const sentences = rest
      .split(/(?<=[.!?])\s+/)
      .map((s) => s.trim())
      .filter(Boolean);

    const verbs = [
      "Check",
      "Review",
      "Monitor",
      "Increase",
      "Reduce",
      "Maintain",
      "Push",
      "If your objective"
    ];

    const isAction = (s: string) =>
      new RegExp(`^(?:⚠\\s*)?(?:${verbs.join("|")})\\b`, "i").test(s);

    const isInventory = (s: string) =>
      /inventory\s*:/i.test(s) || /^\s*⚠\s*inventory\s*:/i.test(s);

    const inventoryLines: string[] = [];
    const actionLines: string[] = [];
    const descLines: string[] = [];

    for (const s of sentences) {
      if (isInventory(s)) inventoryLines.push(s);
      else if (isAction(s)) actionLines.push(s);
      else descLines.push(s);
    }

    const description = descLines.join(" ");

    return (
      <div className="space-y-2">
        {/* Product Name */}
        {productName && (
          <div className="font-bold text-charcoal-500">
            Product name – {productName}
          </div>
        )}

        {/* Description */}
        {description && (
          <div className="text-xs 2xl:text-sm text-charcoal-500">
            {formatBulletLine(description)}
          </div>
        )}

        {/* Actions (bold, separate lines) */}
        {actionLines.map((a, i) => (
          <div key={i} className="font-bold text-xs 2xl:text-sm text-charcoal-500">
            {a.replace(/^⚠\s*/, "")}
          </div>
        ))}

        {/* Inventory (always last, bold) */}
        {inventoryLines.map((inv, i) => (
          <div key={i} className="font-bold text-xs 2xl:text-sm text-charcoal-500">
            ⚠ {inv.replace(/^⚠?\s*Inventory\s*:\s*/i, "Inventory: ")}
          </div>
        ))}
      </div>
    );
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
      const ns = Number((r as any).net_sales_month2 ?? (r as any).net_sales_curr ?? r.net_sales ?? 0) || 0;
      const p = Number((r as any).profit_month2 ?? (r as any).profit_curr ?? r.profit ?? 0) || 0;

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

      const aspVal = Number((r as any).asp_month2 ?? (r as any).asp_curr ?? r.asp ?? 0) || 0;
      const upVal =
        Number(
          (r as any).unit_wise_profitability_month2 ??
          (r as any).unit_wise_profitability_curr ??
          r.unit_wise_profitability ??
          0
        ) || 0;

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
            setSelectedSku(entry[0]);
            setModalOpen(true);
            setFbType(null);
            setFbText('');
            setFbSuccess(false);
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
      ...(showAI
        ? [
          {
            key: 'ai',
            header: 'AI Insight',
            width: '150px',
          },
        ]
        : []),

      {
        key: 'profit',
        header: isNewRev ? 'Profit (%)' : 'CM1 Profit Impact (%)',
        width: '200px',
      },

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
        s + Number(r?.profit_month2 ?? r?.profit_curr ?? r?.profit ?? 0),
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
          s + Number(r?.net_sales_month2 ?? r?.net_sales_curr ?? r?.net_sales ?? 0),
        0
      );

      // ✅ month1 totals (for mix change)
      const totalNetSalesMonth1 = allSkuRows.reduce(
        (s, r: any) => s + Number(r?.net_sales_month1 ?? r?.net_sales_prev ?? 0),
        0
      );

      const othersNetSalesMonth1 = others.reduce(
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

          // ✅ PREVIOUS: ONLY previous keys (no current fallback)
          const qtyPrev = sumPrevOnly(rows, ["quantity_month1", "quantity_prev"]);
          const nsPrev = sumPrevOnly(rows, ["net_sales_month1", "net_sales_prev"]);
          const profitPrev = sumPrevOnly(rows, ["profit_month1", "profit_prev"]);

          // ✅ CURRENT: current keys + allowed generic current fallbacks
          const qtyCurr = sumCurrOnly(rows, ["quantity_month2", "quantity_curr", "quantity"]);
          const nsCurr = sumCurrOnly(rows, ["net_sales_month2", "net_sales_curr", "net_sales"]);
          const profitCurr = sumCurrOnly(rows, ["profit_month2", "profit_curr", "profit"]);

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

  // =========================
  // Render
  // =========================

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
        <div className="flex flex-col mt-4">
          {error && <p style={{ color: 'red' }}>{error}</p>}

          {(overallSummary.length > 0 || overallActions.length > 0) && (
            <div className="flex gap-4 flex-col md:flex-row">
              {(summaryText || overallSummary.length > 0) && (
                <div className="bg-[#D9D9D94D] border border-[#D9D9D9] rounded-md p-3 text-xs 2xl:text-sm text-charcoal-500 w-full">
                  <PageBreadcrumb pageTitle="Business Summary MTD" variant="page" align="left" />

                  {/* ✅ Executive paragraph */}
                  {summaryText && (
                    <p className="mb-2 text-sm text-charcoal-500">
                      {summaryText}
                    </p>
                  )}

                  {/* ✅ Metric bullets */}
                  {overallSummary.length > 0 && (
                    <ul className="list-disc pl-5 space-y-1 pt-2">
                      {overallSummary.map((line, idx) => (
                        <li key={idx}>{formatBulletLine(line)}</li>
                      ))}
                    </ul>
                  )}
                </div>
              )}

              {((recommendedActions && Object.keys(recommendedActions).length > 0) ||
                (skuInsights && Object.keys(skuInsights).length > 0)) && (


                  <div className="bg-[#F7F9FC] border border-[#D9D9D9] rounded-md p-3 text-xs 2xl:text-sm text-charcoal-600 w-full">
                    <PageBreadcrumb
                      pageTitle="Recommended Actions (MTD)"
                      variant="page"
                      align="left"
                    />

                    <div className="space-y-3 pt-2">

                      {/* ✅ PREFERRED: AI Insights (rich text) */}
                      {skuInsights && Object.keys(skuInsights).length > 0 ? (
                        Object.values(skuInsights).map((insight, idx) => (
                          <div key={idx} className="border-b pb-2 last:border-b-0">
                            <div className="font-bold text-charcoal-500 mb-1">
                              {insight.product_name}
                            </div>
                            <div className="text-sm">
                              {renderAiActionLine(insight.insight)}
                            </div>
                          </div>
                        ))
                      ) : (

                        /* ✅ FALLBACK: overallActions (SKU → action) */
                        Object.entries(recommendedActions || {}).map(([sku, action], idx) => (
                          <div key={idx} className="text-sm text-charcoal-500">
                            {renderAiActionLine(action)}
                          </div>
                        ))

                      )}

                    </div>
                  </div>
                )}

            </div>
          )}

          <div>
            <div className="mt-4 rounded-2xl border bg-white p-5 shadow-sm">

              <div className="flex flex-col 2xl:flex-row gap-4  xl:items-left xl:justify-between">
                {/* <PageBreadcrumb pageTitle="SKU Analysis MTD" variant="page" align="left" /> */}
                <div className="flex flex-wrap items-baseline gap-2 font-bold ">
                  {/* <PageBreadcrumb
                    pageTitle={`Amazon`}
                    variant="page"
                    align="left"
                  />
                  <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl text-[#5EA68E]">{titleCountry}</span> */}
                  <PageBreadcrumb
                    pageTitle={`SKU Analysis MTD`}
                    variant="page"
                    align="left"
                  />
                  {/* <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl text-[#5EA68E]">{titleMonth}</span> */}
                </div>

                <div className="flex flex-col gap-3 sm:flex-row sm:items-center xl:justify-between">
                  <SegmentedToggle<TabKey>
                    value={activeTab}
                    options={tabOptions}
                    onChange={handleTabChange}
                    className="bg-white border border-[#D9D9D9E5] shadow-sm"
                    textSizeClass="text-xs 2xl:text-sm"
                  />


                  <div className="flex gap-3">


                    <AiButton onClick={analyzeSkus}
                      disabled={
                        !['top_80_skus', 'new_or_reviving_skus', 'other_skus'].some(
                          (k) =>
                            (categorizedGrowth[k as keyof CategorizedGrowth] as SkuItem[])?.length > 0
                        )
                      } >  {loadingInsight ? "Generating..." : "AI Insights"}</AiButton>




                    <DownloadIconButton
                      onClick={() => {
                        if (!userData) {
                          // optionally show toast instead of return
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
                    maxHeight="60vh"
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
                <div className="flex items-center gap-10 flex-wrap text-xs 2xl:text-sm text-[#414042] mt-1">
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

      {(() => {
        if (!modalOpen || !selectedSku) return null;

        const insightData =
          skuInsights[selectedSku as keyof typeof skuInsights] ||
          getInsightByProductName(selectedSku as string)?.[1];

        if (!insightData) return null;

        return (
          <Drawer
            anchor="right"
            open={modalOpen}
            onClose={() => setModalOpen(false)}
            PaperProps={{
              sx: {
                width: { xs: '100vw', sm: '80vw', md: '60vw', lg: '50vw' },
                maxWidth: 900,
                padding: 2,
              },
            }}
          >
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12, height: '100%' }}>
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  marginBottom: 8,
                }}
              >
                <h2 style={{ margin: 0, fontSize: 18 }}>
                  AI Insight for{' '}
                  <span style={{ color: '#60a68e' }}>
                    {insightData.product_name || selectedSku}
                  </span>
                </h2>

                <IconButton size="small" onClick={() => setModalOpen(false)} aria-label="Close">
                  x
                </IconButton>
              </div>

              <div style={{ border: '1px solid #e5e7eb', borderRadius: 8, padding: 8 }}>
                <Productinfoinpopup
                  productname={insightData.product_name}
                  countryName={countryName}   // ✅ PASS COUNTRY
                />
              </div>

              <div style={{ flex: 1, overflowY: 'auto', marginTop: 8, paddingRight: 4 }}>
                {renderFormattedInsight(insightData.insight)}
              </div>
            </div>
          </Drawer>
        );
      })()}
    </>
  );
};

