'use client'

import React, { useState, useEffect, useMemo } from 'react';
import axios from 'axios';
import { useParams, useRouter } from 'next/navigation';
import * as XLSX from 'xlsx';
import { FaThumbsUp, FaThumbsDown } from 'react-icons/fa';
import Productinfoinpopup from '@/components/businessInsight/Productinfoinpopup';
import Drawer from '@mui/material/Drawer';
import IconButton from '@mui/material/IconButton';
import { IoDownload } from "react-icons/io5";
import { BsStars } from "react-icons/bs";
import { FaArrowUp, FaArrowDown } from 'react-icons/fa';
import * as echarts from 'echarts';
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  LineElement,
  PointElement,
  Tooltip,
  Legend,
} from "chart.js";
import introJs from 'intro.js';
import 'intro.js/introjs.css';
import DataTable, { ColumnDef, Row } from '@/components/ui/table/DataTable';
import DownloadIconButton from '@/components/ui/button/DownloadIconButton';
import PageBreadcrumb from '@/components/common/PageBreadCrumb';
import { AnimatePresence, motion } from "framer-motion";
import { AiButton } from '@/components/ui/button/AiButton';
import SegmentedToggle from '@/components/ui/SegmentedToggle';
import { IoMdLock } from "react-icons/io";
import SkuAnalysisSection from '@/components/businessInsight/SkuAnalysisSection';

ChartJS.register(CategoryScale, LinearScale, LineElement, PointElement, Tooltip, Legend);

// =========================
// Types/Interfaces
// =========================
interface MonthOption {
  value: string;
  label: string;
}
interface CurrencyRateRow {
  user_currency: string;
  country: string;
  selected_currency: string;
  conversion_rate: number;
  month: string;
  year: number;
}

interface GrowthCategory {
  category: string;
  value: number;
}

interface SkuItem {
  product_name: string;
  sku?: string;
  'Sales Mix (Month2)'?: number;
  total_quantity?: number;
  asp?: number;
  net_sales?: number;
  sales_mix?: number;
  unit_wise_profitability?: number;
  profit?: number;

  // ✅ new raw fields from backend for Excel:
  total_quantity_month1?: number;
  total_quantity_month2?: number;
  asp_month1?: number;
  asp_month2?: number;
  net_sales_month1?: number;
  net_sales_month2?: number;
  gross_sales_month1?: number;
  gross_sales_month2?: number;
  sales_mix_month1?: number;
  sales_mix_month2?: number;
  profit_percentage_month1?: number;
  profit_percentage_month2?: number;
  unit_wise_profitability_month1?: number;
  unit_wise_profitability_month2?: number;
  profit_month1?: number;
  profit_month2?: number;

  [key: string]: any; // For growth fields like 'Unit Growth'
}

interface CategorizedGrowth {
  top_80_skus: SkuItem[];
  new_or_reviving_skus: SkuItem[];
  other_skus: SkuItem[];

  // ✅ add
  all_skus?: SkuItem[];

  top_80_total?: SkuItem | null;
  new_or_reviving_total?: SkuItem | null;
  other_total?: SkuItem | null;
  all_skus_total?: SkuItem | null;
}



interface SkuInsight {
  product_name: string;
  insight?: string;

  inventory_recommendation?: string;
  recommendation?: string;
  product_journey?: string[]; // ✅ ADD THIS

  objective?: {
    growth_intent?: string;
    inventory_clearance_priority?: string;
    profit_priority?: string;
  } | string; // fallback if backend still sends string
}

type ObjectiveObj = {
  growth_intent?: string;
  inventory_clearance_priority?: string;
  profit_priority?: string;
};

const isObjectiveObj = (v: unknown): v is ObjectiveObj =>
  !!v && typeof v === "object" && !Array.isArray(v);



interface ApiResponse {
  comparison_range?: { month2_label: string };
  categorized_growth?: CategorizedGrowth;
  insights?: Record<string, SkuInsight>;
  reimbursement_totals?: { month1: number; month2: number };

  // ✅ add
  advertising_totals?: { month1: number; month2: number };
  expense_totals?: { month1: number; month2: number };
}


// =========================
// Config
// =========================
const API_BASE = `${process.env.NEXT_PUBLIC_API_BASE_URL}`;

type TabKey = 'top_80_skus' | 'new_or_reviving_skus' | 'other_skus' | 'all_skus';

// Persist keys
const STORAGE_KEY = 'bi_insight_data';        // compare inputs + results
const INSIGHTS_KEY = 'bi_sku_insights';       // AI insights (optional persist)

// Axios instance with JWT
const api = axios.create({ baseURL: API_BASE });
api.interceptors.request.use((cfg) => {
  const t = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
  if (t) cfg.headers.Authorization = `Bearer ${t}`;
  return cfg;
});

type GrowthChartsProps = {
  unitsChartRef: React.RefObject<HTMLDivElement | null>;
  chartRef: React.RefObject<HTMLDivElement | null>;
  profitChartRef: React.RefObject<HTMLDivElement | null>;
  aspChartRef: React.RefObject<HTMLDivElement | null>;
};

const GrowthCharts = React.memo(function GrowthCharts({
  unitsChartRef,
  chartRef,
  profitChartRef,
  aspChartRef,
}: GrowthChartsProps) {
  return (
    <div className="mt-4 mb-3 rounded-xl border border-gray-200 bg-white p-4">
      <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
        <div>
          <PageBreadcrumb pageTitle="Units Sold" variant="page" align="left" textSize="2xl" />
          <div ref={unitsChartRef} className="h-[320px] w-full" />
        </div>

        <div>
          <PageBreadcrumb pageTitle="Net Sales" variant="page" align="left" textSize="2xl" />
          <div ref={chartRef} className="h-[320px] w-full" />
        </div>

        <div className="mt-3">
          <PageBreadcrumb pageTitle="CM1 Profit" variant="page" align="left" textSize="2xl" />
          <div ref={profitChartRef} className="h-[320px] w-full" />
        </div>

        <div className="mt-3">
          <PageBreadcrumb pageTitle="Average Selling Price" variant="page" align="left" textSize="2xl" />
          <div ref={aspChartRef} className="h-[320px] w-full" />
        </div>
      </div>

      <div className="mt-3 flex flex-wrap justify-center gap-4 text-[10px] font-semibold text-[#414042] 2xl:text-xs">
        <span className="inline-flex items-center gap-2">
          <span className="inline-block h-[10px] w-[10px] bg-[#ED9F50]" />
          Top 80%
        </span>

        <span className="inline-flex items-center gap-2">
          <span className="inline-block h-[10px] w-[10px] bg-[#3A8EA4]" />
          Other SKUs
        </span>

        <span className="inline-flex items-center gap-2">
          <span className="inline-block h-[10px] w-[10px] bg-[#7B9A6D]" />
          New/Reviving
        </span>
      </div>
    </div>
  );
});


const MonthsforBI: React.FC = () => {
  const params = useParams();
  const countryName = params?.countryName as string | undefined;

  const hexToRgba = (hex: string, a: number) => {
    const h = hex.replace("#", "");
    const full = h.length === 3 ? h.split("").map(c => c + c).join("") : h;
    const n = parseInt(full, 16);
    const r = (n >> 16) & 255;
    const g = (n >> 8) & 255;
    const b = n & 255;
    return `rgba(${r}, ${g}, ${b}, ${a})`;
  };

  const TOP_80_DUMMY: SkuItem[] = [
    {
      product_name: "Demo Product A",
      sku: "DEMO-A",

      total_quantity_month1: 0,
      total_quantity_month2: 0,

      net_sales_month1: 0,
      net_sales_month2: 0,

      asp_month1: 0,
      asp_month2: 0,

      profit_month1: 0,
      profit_month2: 0,

      "Sales Mix (Month2)": 0,

      "Unit Growth": { category: "High Growth", value: 0 },
      "ASP Growth": { category: "Low Growth", value: 0 },
      "Net Sales Growth": { category: "High Growth", value: 0 },
      "CM1 Profit Impact": { category: "High Growth", value: 0 },
      "Profit Per Unit": { category: "High Growth", value: 0 },
    },
    {
      product_name: "Demo Product B",
      sku: "DEMO-B",

      total_quantity_month1: 0,
      total_quantity_month2: 0,

      net_sales_month1: 0,
      net_sales_month2: 0,

      asp_month1: 0,
      asp_month2: 0,

      profit_month1: 0,
      profit_month2: 0,

      "Sales Mix (Month2)": 0,

      "Unit Growth": { category: "High Growth", value: 0 },
      "ASP Growth": { category: "Low Growth", value: 0 },
      "Net Sales Growth": { category: "High Growth", value: 0 },
      "CM1 Profit Impact": { category: "High Growth", value: 0 },
      "Profit Per Unit": { category: "High Growth", value: 0 },
    },
  ];

  const NEW_REV_DUMMY: SkuItem[] = [
    {
      product_name: "Demo Product C",
      sku: "DEMO-C",

      total_quantity_month1: 0,
      total_quantity_month2: 0,

      net_sales_month1: 0,
      net_sales_month2: 0,

      asp_month1: 0,
      asp_month2: 0,

      profit_month1: 0,
      profit_month2: 0,

      "Sales Mix (Month2)": 0,

      "Unit Growth": { category: "High Growth", value: 0 },
      "ASP Growth": { category: "Low Growth", value: 0 },
      "Net Sales Growth": { category: "High Growth", value: 0 },
      "CM1 Profit Impact": { category: "High Growth", value: 0 },
      "Profit Per Unit": { category: "High Growth", value: 0 },
    },
    {
      product_name: "Demo Product D",
      sku: "DEMO-D",

      total_quantity_month1: 0,
      total_quantity_month2: 0,

      net_sales_month1: 0,
      net_sales_month2: 0,

      asp_month1: 0,
      asp_month2: 0,

      profit_month1: 0,
      profit_month2: 0,

      "Sales Mix (Month2)": 0,

      "Unit Growth": { category: "High Growth", value: 0 },
      "ASP Growth": { category: "Low Growth", value: 0 },
      "Net Sales Growth": { category: "High Growth", value: 0 },
      "CM1 Profit Impact": { category: "High Growth", value: 0 },
      "Profit Per Unit": { category: "High Growth", value: 0 },
    },
  ];

  const OTHER_DUMMY: SkuItem[] = [
    {
      product_name: "Demo Product E",
      sku: "DEMO-E",

      total_quantity_month1: 0,
      total_quantity_month2: 0,

      net_sales_month1: 0,
      net_sales_month2: 0,

      asp_month1: 0,
      asp_month2: 0,

      profit_month1: 0,
      profit_month2: 0,

      "Sales Mix (Month2)": 0,

      "Unit Growth": { category: "Negative Growth", value: 0 },
      "ASP Growth": { category: "Low Growth", value: 0 },
      "Net Sales Growth": { category: "Negative Growth", value: 0 },
      "CM1 Profit Impact": { category: "Negative Growth", value: 0 },
      "Profit Per Unit": { category: "Negative Growth", value: 0 },
    },
  ];

  const DUMMY_CATEGORIZED_GROWTH: CategorizedGrowth = {
    top_80_skus: TOP_80_DUMMY,
    new_or_reviving_skus: NEW_REV_DUMMY,
    other_skus: OTHER_DUMMY,

    // ✅ merged list for All SKUs tab
    all_skus: [
      ...TOP_80_DUMMY,
      ...NEW_REV_DUMMY,
      ...OTHER_DUMMY,
    ],

    top_80_total: null,
    new_or_reviving_total: null,
    other_total: null,
    all_skus_total: null,
  };

  const PREVIEW_MONTH1 = "01";
  const PREVIEW_YEAR1 = "2025";
  const PREVIEW_MONTH2 = "02";
  const PREVIEW_YEAR2 = "2025";


  const isPreviewMode =
    params?.month === "NA" || params?.year === "NA";


  // Month/year form
  const [month1, setMonth1] = useState<string>(isPreviewMode ? PREVIEW_MONTH1 : '');
  const [year1, setYear1] = useState<string>(isPreviewMode ? PREVIEW_YEAR1 : '');
  const [month2, setMonth2] = useState<string>(isPreviewMode ? PREVIEW_MONTH2 : '');
  const [year2, setYear2] = useState<string>(isPreviewMode ? PREVIEW_YEAR2 : '');
  const [isMobile, setIsMobile] = useState(false);
  const [loadingCompare, setLoadingCompare] = useState(false);
  const [is2xlUp, setIs2xlUp] = useState(false);

  const setEmptyChartOption = (
    instance: echarts.EChartsType,
  ) => {
    instance.clear();

    instance.setOption({
      animation: false,
      tooltip: { show: false },
      legend: { show: false },
      grid: { left: 20, right: 20, top: 40, bottom: 40 },

      xAxis: {
        show: false,
        type: 'category',
        data: [],
      },

      yAxis: {
        show: false,
        type: 'value',
      },

      graphic: [
        {
          type: 'text',
          left: 'center',
          top: 'middle',
          style: {
            text: 'No data Available',
            fontSize: 14,
            fontWeight: 400,
            fill: '#6B7280',
            textAlign: 'center',
            textVerticalAlign: 'middle',
          },
        },
      ],

      series: [],
    }, true);
  };

  useEffect(() => {
    const check = () => {
      setIsMobile(window.innerWidth < 768);
      setIs2xlUp(window.innerWidth >= 1536);
    };
    check();
    window.addEventListener("resize", check);
    return () => window.removeEventListener("resize", check);
  }, []);

  const axisTickFontSize = is2xlUp ? 14 : 12;
  const axisNameFontSize = is2xlUp ? 14 : 12;
  useEffect(() => {
    const check = () => setIsMobile(window.innerWidth < 768);
    check();
    window.addEventListener("resize", check);
    return () => window.removeEventListener("resize", check);
  }, []);

  // Data + UI state
  const [categorizedGrowth, setCategorizedGrowth] = useState<CategorizedGrowth>({
    top_80_skus: [],
    new_or_reviving_skus: [],
    other_skus: [],
    all_skus: [],              // ✅ add
    top_80_total: null,
    new_or_reviving_total: null,
    other_total: null,
    all_skus_total: null,
  });


  // const [activeTab, setActiveTab] = useState<TabKey>('all_skus');
  const [month2Label, setMonth2Label] = useState<string>('');
  const [error, setError] = useState<string | null>(null);

  // Insights + modal
  // const [loadingInsight, setLoadingInsight] = useState<boolean>(false);
  // const [skuInsights, setSkuInsights] = useState<Record<string, SkuInsight>>({});
  // const [selectedSku, setSelectedSku] = useState<string | null>(null);
  // const [modalOpen, setModalOpen] = useState<boolean>(false);

  // Feedback (Summary)
  const [fbType, setFbType] = useState<'like' | 'dislike' | null>(null);
  const [fbText, setFbText] = useState<string>('');
  const [fbSubmitting, setFbSubmitting] = useState<boolean>(false);
  const [fbSuccess, setFbSuccess] = useState<boolean>(false);
  // const [expandAllSkusOthers, setExpandAllSkusOthers] = useState(true);
  const [reimbursementTotals, setReimbursementTotals] = useState<{ month1: number; month2: number } | null>(null);
  const [advertisingTotals, setAdvertisingTotals] = useState<{ month1: number; month2: number } | null>(null);
  const [expenseTotals, setExpenseTotals] = useState<{ month1: number; month2: number } | null>(null);
  const [introReady, setIntroReady] = useState(false);
  const router = useRouter();

  useEffect(() => {
    if (!isPreviewMode) return;

    setMonth1(PREVIEW_MONTH1);
    setYear1(PREVIEW_YEAR1);
    setMonth2(PREVIEW_MONTH2);
    setYear2(PREVIEW_YEAR2);

    setCategorizedGrowth(DUMMY_CATEGORIZED_GROWTH);
    setAdvertisingTotals({ month1: 0, month2: 0 });
    setExpenseTotals({ month1: 0, month2: 0 });
    setReimbursementTotals({ month1: 0, month2: 0 });
    setMonth2Label("Preview");
    setError(null);
  }, [isPreviewMode]);

  const [currencyRates, setCurrencyRates] = useState<CurrencyRateRow[]>([]);
  const [ratesLoading, setRatesLoading] = useState(false);


  const monthName = (date = new Date()) =>
    date.toLocaleString("en-US", { month: "long" }).toLowerCase();

  const currentYearNum = (date = new Date()) => date.getFullYear();

  const rateMap = useMemo(() => {
    const map = new Map<string, number>();

    for (const r of currencyRates) {
      const key = [
        (r.user_currency || "").toLowerCase(),
        (r.selected_currency || "").toLowerCase(),
        (r.country || "").toLowerCase(),
        (r.month || "").toLowerCase(),
        Number(r.year),
      ].join("|");

      map.set(key, Number(r.conversion_rate));
    }

    return map;
  }, [currencyRates]);

  const getFxDb = (
    from: string,
    to: string,
    country: string,
    month: string = monthName(),
    year: number = currentYearNum()
  ) => {
    const f = (from || "").toLowerCase();
    const t = (to || "").toLowerCase();
    const c = (country || "").toLowerCase();
    const m = (month || "").toLowerCase();
    const y = Number(year);

    if (!f || !t) return 1;
    if (f === t) return 1;

    const directKey = `${f}|${t}|${c}|${m}|${y}`;
    const inverseKey = `${t}|${f}|${c}|${m}|${y}`;

    const direct = rateMap.get(directKey);
    if (direct != null) return direct;

    const inverse = rateMap.get(inverseKey);
    if (inverse != null && inverse !== 0) return 1 / inverse;

    return 1;
  };

  const getFxLookupCountry = (country?: string, currencyCode?: string) => {
    const c = (country || "").toLowerCase();
    const code = (currencyCode || "").toUpperCase();

    if (c && c !== "global") return c;

    if (code === "GBP") return "uk";
    if (code === "USD") return "us";
    if (code === "CAD") return "ca";
    if (code === "INR") return "india";

    return "us";
  };

  const safeNumber = (value: any, fallback = 0) => {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
  };

  const getCurrencyCodeByCountry = (country?: string) => {
    const c = (country || "").toLowerCase();
    if (c === "uk") return "GBP";
    if (c === "us") return "USD";
    if (c === "ca") return "CAD";
    if (c === "india" || c === "in") return "INR";
    if (c === "global") return "USD";
    return "USD";
  };

  const getCurrencySymbolFromCode = (code?: string) => {
    const c = (code || "").toUpperCase();
    if (c === "GBP") return "£";
    if (c === "USD") return "$";
    if (c === "CAD") return "C$";
    if (c === "EUR") return "€";
    if (c === "INR") return "₹";
    return c || "$";
  };

  const effectiveCountry = isPreviewMode ? "global" : countryName;

  const selectedCountryCurrency = useMemo(() => {
    return getCurrencyCodeByCountry(effectiveCountry);
  }, [effectiveCountry]);

  const homeCurrencyCode = useMemo(() => {
    if (!currencyRates.length) return selectedCountryCurrency;

    const row = currencyRates.find(
      (r) => (r.country || "").toLowerCase() === (effectiveCountry || "").toLowerCase()
    );

    return (row?.user_currency || selectedCountryCurrency).toUpperCase();
  }, [currencyRates, effectiveCountry, selectedCountryCurrency]);

  const displayCurrencyCode = useMemo(() => {
    return (effectiveCountry || "").toLowerCase() === "global"
      ? homeCurrencyCode
      : selectedCountryCurrency;
  }, [effectiveCountry, homeCurrencyCode, selectedCountryCurrency]);

  const displayCurrencySymbol = useMemo(() => {
    return getCurrencySymbolFromCode(displayCurrencyCode);
  }, [displayCurrencyCode]);

  const convertAmount = (
    value: number,
    sourceCountry?: string,
    month?: string,
    year?: string | number
  ) => {
    const numericValue = safeNumber(value, 0);
    if (!numericValue) return 0;

    const fromCurrency = getCurrencyCodeByCountry(sourceCountry || effectiveCountry);
    const toCurrency = displayCurrencyCode;

    if (!fromCurrency || !toCurrency || fromCurrency === toCurrency) {
      return numericValue;
    }

    const fxCountry = getFxLookupCountry(sourceCountry || effectiveCountry, toCurrency);

    const monthLabel =
      month
        ? (months.find((m) => m.value === String(month).padStart(2, "0"))?.label || "").toLowerCase()
        : monthName();

    const fx = getFxDb(
      fromCurrency,
      toCurrency,
      fxCountry,
      monthLabel || monthName(),
      year ? Number(year) : currentYearNum()
    );

    const converted = numericValue * safeNumber(fx, 1);
    return Number.isFinite(converted) ? converted : numericValue;
  };

  // ✅ NEW: available periods from backend (['YYYY-MM'])
  const [availablePeriods, setAvailablePeriods] = useState<string[]>([]);
  const [showTotalsModal, setShowTotalsModal] = useState(false);

  const [selectedTotals, setSelectedTotals] = useState<Record<string, boolean>>({
    netSales: true,
    cm1Profit: true,
    otherExpense: true,
    advertising: true,
    reimbursement: true,
  });

  const year1Ref = React.useRef<HTMLSelectElement | null>(null);
  const month1Ref = React.useRef<HTMLDivElement | null>(null);
  const compareBtnRef = React.useRef<HTMLButtonElement | null>(null);
  const didInitialFetchRef = React.useRef(false);

  useEffect(() => {
    if (!isPreviewMode) return;

    setMonth1(PREVIEW_MONTH1);
    setYear1(PREVIEW_YEAR1);
    setMonth2(PREVIEW_MONTH2);
    setYear2(PREVIEW_YEAR2);

    setCategorizedGrowth(DUMMY_CATEGORIZED_GROWTH);
    setAdvertisingTotals({ month1: 0, month2: 0 });
    setExpenseTotals({ month1: 0, month2: 0 });
    setReimbursementTotals({ month1: 0, month2: 0 });
    setMonth2Label("Preview");
    // setActiveTab("all_skus");
    setError(null);
  }, [isPreviewMode]);

  useEffect(() => {
    const token =
      typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

    if (!token) return;

    const fetchRates = async () => {
      try {
        setRatesLoading(true);

        const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/currency-rates`, {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        });

        if (!res.ok) {
          const errText = await res.text();
          throw new Error(`Failed to fetch rates: ${res.status} ${errText}`);
        }

        const json = await res.json();
        setCurrencyRates(Array.isArray(json) ? json : []);
      } catch (e) {
        console.error("Failed to fetch currency rates:", e);
        setCurrencyRates([]);
      } finally {
        setRatesLoading(false);
      }
    };

    fetchRates();
  }, []);



  const isUsingDummyData = isPreviewMode;

  const DummyBlurWrapper = ({
    enabled,
    badgeText = "Dummy Preview",
    children,
    className = "",
  }: {
    enabled: boolean;
    badgeText?: string;
    children: React.ReactNode;
    className?: string;
  }) => {
    return (
      <div className={`relative w-full ${className}`}>
        {/* {enabled && (
        <div className="absolute right-2 top-2 z-20 rounded-md bg-black/70 px-2 py-1 text-[10px] sm:text-xs text-white shadow">
          {badgeText}
        </div>
      )} */}

        <div
          className={
            enabled
              ? "opacity-40 pointer-events-none select-none transition-opacity duration-300"
              : "opacity-100 transition-opacity duration-300"
          }
        >
          {children}
        </div>
      </div>
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

  // =========================
  // PREVIEW / DUMMY DATA
  // =========================







  useEffect(() => {
    if (!introReady) return;

    const done = localStorage.getItem('bi_intro_done');
    if (done) return;

    // 🔥 SAFETY: agar koi purana instance ho
    introJs().exit();

    const intro = introJs();
    intro.setOptions({
      showProgress: true,
      showBullets: false,
      exitOnOverlayClick: false,
      scrollToElement: false, // 🔥 IMPORTANT
      steps: [
        {
          element: '#intro-year1',
          intro: 'Select the year you want to analyze.',
        },
        {
          element: '#intro-month1',
          intro: 'Choose the corresponding month for that year.',
        },
        {
          element: '#intro-compare',
          intro: 'Click Compare to view insights and performance trends.',
        },
      ],

    });

    intro.oncomplete(() => localStorage.setItem('bi_intro_done', '1'));
    intro.onexit(() => localStorage.setItem('bi_intro_done', '1'));

    intro.start();
  }, [introReady]);






  const toggleTotalsMetric = (key: string) => {
    const selectedCount = Object.values(selectedTotals).filter(Boolean).length;
    const isChecked = !!selectedTotals[key];

    // prevent turning off the last metric (same logic as GraphPage) :contentReference[oaicite:2]{index=2}
    if (isChecked && selectedCount === 1) {
      setShowTotalsModal(true);
      return;
    }

    setSelectedTotals((prev) => ({ ...prev, [key]: !isChecked }));
  };





  const chartRef = React.useRef<HTMLDivElement | null>(null);
  const chartInstanceRef = React.useRef<echarts.EChartsType | null>(null);

  const profitChartRef = React.useRef<HTMLDivElement | null>(null);
  const profitChartInstanceRef = React.useRef<echarts.EChartsType | null>(null);

  const unitsChartRef = React.useRef<HTMLDivElement | null>(null);
  const unitsChartInstanceRef = React.useRef<echarts.EChartsType | null>(null);

  const aspChartRef = React.useRef<HTMLDivElement | null>(null);
  const aspChartInstanceRef = React.useRef<echarts.EChartsType | null>(null);

  const totalsChartRef = React.useRef<HTMLDivElement | null>(null);
  const totalsChartInstanceRef = React.useRef<echarts.EChartsType | null>(null);


  const periodToDate = (p: string) => {
    // p = "YYYY-MM"
    const [yy, mm] = p.split('-').map(Number);
    return new Date(yy, (mm || 1) - 1, 1).getTime();
  };

  const pickDefaultComparePeriods = (periods: string[]) => {
    if (!periods?.length) return null;

    const now = new Date();
    const cy = now.getFullYear();
    const cm = pad2(now.getMonth() + 1);
    const currentKey = `${cy}-${cm}`;

    // exclude current month (even if available)
    const filtered = periods.filter((p) => p !== currentKey);

    // sort descending (latest first)
    const sorted = [...filtered].sort((a, b) => periodToDate(b) - periodToDate(a));

    if (sorted.length < 2) return null;

    return { newer: sorted[0], older: sorted[1] }; // month2=newer, month1=older
  };


  const buildCompareSeries = (
    metricKeyBase: 'net_sales' | 'profit' | 'total_quantity' | 'rembursement_fee' | 'asp'
  ) => {
    const safeM1 = month1 || (isPreviewMode ? PREVIEW_MONTH1 : "");
    const safeY1 = year1 || (isPreviewMode ? PREVIEW_YEAR1 : "");
    const safeM2 = month2 || (isPreviewMode ? PREVIEW_MONTH2 : "");
    const safeY2 = year2 || (isPreviewMode ? PREVIEW_YEAR2 : "");

    const m1Label = `${getAbbr(safeM1)}'${String(safeY1).slice(2)}`;
    const m2Label = `${getAbbr(safeM2)}'${String(safeY2).slice(2)}`;
    const x = [m1Label, "", m2Label];

    if (isPreviewMode) {
      return {
        x,
        values: {
          top80_m1: 0,
          top80_m2: 0,
          newRev_m1: 0,
          newRev_m2: 0,
          other_m1: 0,
          other_m2: 0,
        },
      };
    }

    const top80Rows = categorizedGrowth.top_80_skus || [];
    const newRevRows = categorizedGrowth.new_or_reviving_skus || [];
    const otherRows = categorizedGrowth.other_skus || [];

    const sumFieldConverted = (
      rows: any[],
      key: string,
      monthVal: string,
      yearVal: string
    ) =>
      (rows || []).reduce((a, r) => {
        const raw = safeNumber(r?.[key], 0);
        return a + safeNumber(convertAmount(raw, effectiveCountry, monthVal, yearVal), 0);
      }, 0);

    const sumFieldRaw = (rows: any[], key: string) =>
      (rows || []).reduce((a, r) => a + safeNumber(r?.[key], 0), 0);

    const safeDiv = (a: number, b: number) => {
      const result = b ? a / b : 0;
      return Number.isFinite(result) ? result : 0;
    };

    if (metricKeyBase === 'asp') {
      const top80_ns_m1 = sumFieldConverted(top80Rows, 'net_sales_month1', safeM1, safeY1);
      const top80_ns_m2 = sumFieldConverted(top80Rows, 'net_sales_month2', safeM2, safeY2);
      const top80_q_m1 = sumFieldRaw(top80Rows, 'total_quantity_month1');
      const top80_q_m2 = sumFieldRaw(top80Rows, 'total_quantity_month2');

      const newRev_ns_m1 = sumFieldConverted(newRevRows, 'net_sales_month1', safeM1, safeY1);
      const newRev_ns_m2 = sumFieldConverted(newRevRows, 'net_sales_month2', safeM2, safeY2);
      const newRev_q_m1 = sumFieldRaw(newRevRows, 'total_quantity_month1');
      const newRev_q_m2 = sumFieldRaw(newRevRows, 'total_quantity_month2');

      const other_ns_m1 = sumFieldConverted(otherRows, 'net_sales_month1', safeM1, safeY1);
      const other_ns_m2 = sumFieldConverted(otherRows, 'net_sales_month2', safeM2, safeY2);
      const other_q_m1 = sumFieldRaw(otherRows, 'total_quantity_month1');
      const other_q_m2 = sumFieldRaw(otherRows, 'total_quantity_month2');

      const top80_m1 = safeDiv(top80_ns_m1, top80_q_m1);
      const top80_m2 = safeDiv(top80_ns_m2, top80_q_m2);

      const newRev_m1 = safeDiv(newRev_ns_m1, newRev_q_m1);
      const newRev_m2 = safeDiv(newRev_ns_m2, newRev_q_m2);

      const other_m1 = safeDiv(other_ns_m1, other_q_m1);
      const other_m2 = safeDiv(other_ns_m2, other_q_m2);

      return {
        x,
        values: {
          top80_m1: safeNumber(top80_m1),
          top80_m2: safeNumber(top80_m2),
          newRev_m1: safeNumber(newRev_m1),
          newRev_m2: safeNumber(newRev_m2),
          other_m1: safeNumber(other_m1),
          other_m2: safeNumber(other_m2),
        },
      };
    }

    if (metricKeyBase === 'total_quantity') {
      const m1Key = `${metricKeyBase}_month1`;
      const m2Key = `${metricKeyBase}_month2`;

      const top80_m1 = sumFieldRaw(top80Rows, m1Key);
      const top80_m2 = sumFieldRaw(top80Rows, m2Key);

      const newRev_m1 = sumFieldRaw(newRevRows, m1Key);
      const newRev_m2 = sumFieldRaw(newRevRows, m2Key);

      const other_m1 = sumFieldRaw(otherRows, m1Key);
      const other_m2 = sumFieldRaw(otherRows, m2Key);

      return {
        x,
        values: {
          top80_m1: safeNumber(top80_m1),
          top80_m2: safeNumber(top80_m2),
          newRev_m1: safeNumber(newRev_m1),
          newRev_m2: safeNumber(newRev_m2),
          other_m1: safeNumber(other_m1),
          other_m2: safeNumber(other_m2),
        },
      };
    }

    const m1Key = `${metricKeyBase}_month1`;
    const m2Key = `${metricKeyBase}_month2`;

    const top80_m1 = sumFieldConverted(top80Rows, m1Key, safeM1, safeY1);
    const top80_m2 = sumFieldConverted(top80Rows, m2Key, safeM2, safeY2);

    const newRev_m1 = sumFieldConverted(newRevRows, m1Key, safeM1, safeY1);
    const newRev_m2 = sumFieldConverted(newRevRows, m2Key, safeM2, safeY2);

    const other_m1 = sumFieldConverted(otherRows, m1Key, safeM1, safeY1);
    const other_m2 = sumFieldConverted(otherRows, m2Key, safeM2, safeY2);
    return {
      x,
      values: {
        top80_m1: safeNumber(top80_m1),
        top80_m2: safeNumber(top80_m2),
        newRev_m1: safeNumber(newRev_m1),
        newRev_m2: safeNumber(newRev_m2),
        other_m1: safeNumber(other_m1),
        other_m2: safeNumber(other_m2),
      },
    };
  };

  const getAllRows = () => ([
    ...(categorizedGrowth.top_80_skus || []),
    ...(categorizedGrowth.new_or_reviving_skus || []),
    ...(categorizedGrowth.other_skus || []),
  ]);

  const totalOf = (key: string) => {
    const monthVal = key.endsWith("_month1") ? month1 : month2;
    const yearVal = key.endsWith("_month1") ? year1 : year2;

    return getAllRows().reduce((a, r) => {
      const raw = safeNumber(r?.[key], 0);

      if (key.startsWith("total_quantity")) {
        return a + raw;
      }

      return a + safeNumber(convertAmount(raw, effectiveCountry, monthVal, yearVal), 0);
    }, 0);
  };

  useEffect(() => {
    const el = chartRef.current;
    if (!el) return;

    let ro: ResizeObserver | null = null;
    const onResize = () => chartInstanceRef.current?.resize();

    const raf = requestAnimationFrame(() => {
      if (chartInstanceRef.current && chartInstanceRef.current.getDom() !== el) {
        chartInstanceRef.current.dispose();
        chartInstanceRef.current = null;
      }
      if (!chartInstanceRef.current) {
        chartInstanceRef.current = echarts.init(el);
      }

      const { x, values } = buildCompareSeries('net_sales');
      const { top80_m1, top80_m2, newRev_m1, newRev_m2, other_m1, other_m2 } = values;
      const currency = displayCurrencySymbol;

      const hasAny =
        top80_m1 || top80_m2 || newRev_m1 || newRev_m2 || other_m1 || other_m2;

      if (!hasAny && !isPreviewMode && chartInstanceRef.current) {
        setEmptyChartOption(chartInstanceRef.current);
        chartInstanceRef.current.resize();
        return;
      }

      const option: echarts.EChartsOption = {
        tooltip: {
          trigger: 'axis',
          formatter: (params: any) => {
            const axisLabel = params?.[0]?.axisValueLabel ?? '';
            const lines: string[] = [];
            lines.push(`<div style="font-weight:700;margin-bottom:6px;">Net Sales ${axisLabel}</div>`);

            const map = new Map(params.map((p: any) => [p.seriesName, p]));
            const ordered = SERIES_ORDER.map(s => map.get(s.name)).filter(Boolean);

            ordered.forEach((p: any) => {
              lines.push(
                `<div style="display:flex;align-items:center;gap:8px;margin:2px 0;">
                <span style="display:inline-block;width:10px;height:10px;border-radius:999px;background:${p.color};"></span>
                <span style="flex:1;">${p.seriesName}</span>
                <span style="font-weight:700;">${currency}${fmtNum(p.data)}</span>
              </div>`
              );
            });

            return `<div style="min-width:180px;">${lines.join('')}</div>`;
          },
        },
        legend: { show: false },
        grid: { left: 50, right: 20, top: 40, bottom: 35 },
        color: ['#7B9A6D', '#3a8ea4', '#ed9F50'],
        xAxis: {
          type: 'category',
          boundaryGap: false,
          data: x,
          axisLabel: {
            interval: 0,
            fontSize: axisTickFontSize,
            margin: 20,
            align: 'center',
            formatter: (v: string, idx: number) => {
              if (v === '') return '';
              if (idx === 0) return `{m1|${v}}`;
              if (idx === 2) return `{m2|${v}}`;
              return v;
            },
            rich: {
              m1: { align: 'right', padding: [0, 0, 0, 80] },
              m2: { align: 'left', padding: [0, 80, 0, 0] },
            },
          },
          splitLine: { show: true, lineStyle: { type: 'dashed', opacity: 0.35 } },
        },
        yAxis: {
          type: 'value',
          name: `Amount (${currency})`,
          nameLocation: 'middle',
          nameGap: 45,
          axisLabel: {
            fontSize: axisTickFontSize,
            formatter: (v: number) => `${Math.round(v).toLocaleString()}`
          }
        },
        series: [
          {
            name: 'New/Reviving',
            type: 'line',
            smooth: true,
            stack: 'Total',
            symbol: 'none',
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                { offset: 0.0, color: hexToRgba("#7B9A6D", 0.12) },
                { offset: 0.49, color: hexToRgba("#7B9A6D", 0.12) },
                { offset: 0.51, color: hexToRgba("#7B9A6D", 0.28) },
                { offset: 1.0, color: hexToRgba("#7B9A6D", 0.28) },
              ]),
            },
            data: [newRev_m1, newRev_m1, newRev_m2],
          },
          {
            name: 'Other SKUs',
            type: 'line',
            smooth: true,
            stack: 'Total',
            symbol: 'none',
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                { offset: 0.0, color: hexToRgba("#3A8ea4", 0.12) },
                { offset: 0.49, color: hexToRgba("#3A8ea4", 0.12) },
                { offset: 0.51, color: hexToRgba("#3A8ea4", 0.28) },
                { offset: 1.0, color: hexToRgba("#3A8ea4", 0.28) },
              ]),
            },
            data: [other_m1, other_m1, other_m2],
          },
          {
            name: 'Top 80%',
            type: 'line',
            smooth: true,
            stack: 'Total',
            symbol: 'none',
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                { offset: 0.0, color: hexToRgba("#ed9f50", 0.12) },
                { offset: 0.49, color: hexToRgba("#ed9f50", 0.12) },
                { offset: 0.51, color: hexToRgba("#ed9f50", 0.28) },
                { offset: 1.0, color: hexToRgba("#ed9f50", 0.28) },
              ]),
            },
            data: [top80_m1, top80_m1, top80_m2],
            markLine: {
              symbol: "none",
              silent: true,
              data: [{ xAxis: "" }],
              lineStyle: { color: "#111827", width: 1, opacity: 0.35 },
              label: { show: false },
            },
          },
        ],
      };

      chartInstanceRef.current.setOption(option, true);
      chartInstanceRef.current.resize();

      window.addEventListener("resize", onResize);
      ro = new ResizeObserver(() => chartInstanceRef.current?.resize());
      ro.observe(el);
    });

    return () => {
      cancelAnimationFrame(raf);
      window.removeEventListener("resize", onResize);
      ro?.disconnect();
    };
  }, [
    month1, year1, month2, year2,
    categorizedGrowth.top_80_skus,
    categorizedGrowth.new_or_reviving_skus,
    categorizedGrowth.other_skus,
    rateMap,
    displayCurrencySymbol,
    displayCurrencyCode,
    effectiveCountry,
    axisTickFontSize,
    isPreviewMode,
  ]);

  useEffect(() => {
    const el = profitChartRef.current;
    if (!el) return;

    let ro: ResizeObserver | null = null;
    const onResize = () => profitChartInstanceRef.current?.resize();

    const raf = requestAnimationFrame(() => {
      if (profitChartInstanceRef.current && profitChartInstanceRef.current.getDom() !== el) {
        profitChartInstanceRef.current.dispose();
        profitChartInstanceRef.current = null;
      }
      if (!profitChartInstanceRef.current) {
        profitChartInstanceRef.current = echarts.init(el);
      }

      const { x, values } = buildCompareSeries('profit');
      const { top80_m1, top80_m2, newRev_m1, newRev_m2, other_m1, other_m2 } = values;
      const currency = displayCurrencySymbol;

      const hasAny =
        top80_m1 || top80_m2 || newRev_m1 || newRev_m2 || other_m1 || other_m2;

      if (!hasAny && !isPreviewMode && profitChartInstanceRef.current) {
        setEmptyChartOption(profitChartInstanceRef.current);
        profitChartInstanceRef.current.resize();
        return;
      }

      const option: echarts.EChartsOption = {
        tooltip: {
          trigger: 'axis',
          formatter: (params: any) => {
            const axisLabel = params?.[0]?.axisValueLabel ?? '';
            const lines: string[] = [];
            lines.push(`<div style="font-weight:700;margin-bottom:6px;">CM1 Profit ${axisLabel}</div>`);

            const map = new Map(params.map((p: any) => [p.seriesName, p]));
            const ordered = SERIES_ORDER.map(s => map.get(s.name)).filter(Boolean);

            ordered.forEach((p: any) => {
              lines.push(
                `<div style="display:flex;align-items:center;gap:8px;margin:2px 0;">
                <span style="display:inline-block;width:10px;height:10px;border-radius:999px;background:${p.color};"></span>
                <span style="flex:1;">${p.seriesName}</span>
                <span style="font-weight:700;">${currency}${fmtNum(p.data)}</span>
              </div>`
              );
            });

            return `<div style="min-width:180px;">${lines.join('')}</div>`;
          },
        },
        legend: { show: false },
        grid: { left: 50, right: 20, top: 40, bottom: 35 },
        color: ['#7B9A6D', '#3A8ea4', '#ed9f50'],
        xAxis: {
          type: 'category',
          boundaryGap: false,
          data: x,
          axisLabel: {
            interval: 0,
            fontSize: axisTickFontSize,
            margin: 20,
            align: 'center',
            formatter: (v: string, idx: number) => {
              if (v === '') return '';
              if (idx === 0) return `{m1|${v}}`;
              if (idx === 2) return `{m2|${v}}`;
              return v;
            },
            rich: {
              m1: { align: 'right', padding: [0, 0, 0, 80] },
              m2: { align: 'left', padding: [0, 80, 0, 0] },
            },
          },
          splitLine: { show: true, lineStyle: { type: 'dashed', opacity: 0.35 } },
        },
        yAxis: {
          type: 'value',
          name: `Amount (${currency})`,
          nameLocation: 'middle',
          nameGap: 45,
          axisLabel: {
            fontSize: axisNameFontSize,
            formatter: (v: number) => `${Math.round(v).toLocaleString()}`
          }
        },
        series: [
          {
            name: 'New/Reviving',
            type: 'line',
            smooth: true,
            stack: 'Total',
            symbol: 'none',
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                { offset: 0.0, color: hexToRgba("#7B9A6D", 0.12) },
                { offset: 0.49, color: hexToRgba("#7B9A6D", 0.12) },
                { offset: 0.51, color: hexToRgba("#7B9A6D", 0.28) },
                { offset: 1.0, color: hexToRgba("#7B9A6D", 0.28) },
              ]),
            },
            data: [newRev_m1, newRev_m1, newRev_m2],
          },
          {
            name: 'Other SKUs',
            type: 'line',
            smooth: true,
            stack: 'Total',
            symbol: 'none',
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                { offset: 0.0, color: hexToRgba("#3A8ea4", 0.12) },
                { offset: 0.49, color: hexToRgba("#3A8ea4", 0.12) },
                { offset: 0.51, color: hexToRgba("#3A8ea4", 0.28) },
                { offset: 1.0, color: hexToRgba("#3A8ea4", 0.28) },
              ]),
            },
            data: [other_m1, other_m1, other_m2],
          },
          {
            name: 'Top 80%',
            type: 'line',
            smooth: true,
            stack: 'Total',
            symbol: 'none',
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                { offset: 0.0, color: hexToRgba("#ed9f50", 0.12) },
                { offset: 0.49, color: hexToRgba("#ed9f50", 0.12) },
                { offset: 0.51, color: hexToRgba("#ed9f50", 0.28) },
                { offset: 1.0, color: hexToRgba("#ed9f50", 0.28) },
              ]),
            },
            data: [top80_m1, top80_m1, top80_m2],
            markLine: {
              symbol: "none",
              silent: true,
              data: [{ xAxis: "" }],
              lineStyle: { color: "#111827", width: 1, opacity: 0.35 },
              label: { show: false },
            },
          },
        ],
      };

      profitChartInstanceRef.current.setOption(option, true);
      profitChartInstanceRef.current.resize();

      window.addEventListener("resize", onResize);
      ro = new ResizeObserver(() => profitChartInstanceRef.current?.resize());
      ro.observe(el);
    });

    return () => {
      cancelAnimationFrame(raf);
      window.removeEventListener("resize", onResize);
      ro?.disconnect();
    };
  }, [
    month1,
    year1,
    month2,
    year2,
    categorizedGrowth.top_80_skus,
    categorizedGrowth.new_or_reviving_skus,
    categorizedGrowth.other_skus,
    rateMap,
    displayCurrencySymbol,
    displayCurrencyCode,
    effectiveCountry,
    axisTickFontSize,
    axisNameFontSize,
    isPreviewMode,
  ]);
  useEffect(() => {
    const el = unitsChartRef.current;
    if (!el) return;

    let ro: ResizeObserver | null = null;
    const onResize = () => unitsChartInstanceRef.current?.resize();

    const raf = requestAnimationFrame(() => {
      if (unitsChartInstanceRef.current && unitsChartInstanceRef.current.getDom() !== el) {
        unitsChartInstanceRef.current.dispose();
        unitsChartInstanceRef.current = null;
      }
      if (!unitsChartInstanceRef.current) {
        unitsChartInstanceRef.current = echarts.init(el);
      }

      const { x, values } = buildCompareSeries('total_quantity');
      const { top80_m1, top80_m2, newRev_m1, newRev_m2, other_m1, other_m2 } = values;

      const hasAny =
        top80_m1 || top80_m2 || newRev_m1 || newRev_m2 || other_m1 || other_m2;

      if (!hasAny && !isPreviewMode && unitsChartInstanceRef.current) {
        setEmptyChartOption(unitsChartInstanceRef.current);
        unitsChartInstanceRef.current.resize();
        return;
      }

      const option: echarts.EChartsOption = {
        tooltip: {
          trigger: 'axis',
          formatter: (params: any) => {
            const axisLabel = params?.[0]?.axisValueLabel ?? '';
            const lines: string[] = [];
            lines.push(`<div style="font-weight:700;margin-bottom:6px;">Units ${axisLabel}</div>`);

            const map = new Map(params.map((p: any) => [p.seriesName, p]));
            const ordered = SERIES_ORDER.map(s => map.get(s.name)).filter(Boolean);

            ordered.forEach((p: any) => {
              lines.push(
                `<div style="display:flex;align-items:center;gap:8px;margin:2px 0;">
                <span style="display:inline-block;width:10px;height:10px;border-radius:999px;background:${p.color};"></span>
                <span style="flex:1;">${p.seriesName}</span>
                <span style="font-weight:700;">${fmtNum(p.data)}</span>
              </div>`
              );
            });

            return `<div style="min-width:180px;">${lines.join('')}</div>`;
          },
        },
        legend: { show: false },
        grid: { left: 50, right: 20, top: 40, bottom: 35 },
        color: ['#7B9A6D', '#3A8ea4', '#ed9f50'],
        xAxis: {
          type: 'category',
          boundaryGap: false,
          data: x,
          axisLabel: {
            interval: 0,
            fontSize: axisTickFontSize,
            margin: 20,
            align: 'center',
            formatter: (v: string, idx: number) => {
              if (v === '') return '';
              if (idx === 0) return `{m1|${v}}`;
              if (idx === 2) return `{m2|${v}}`;
              return v;
            },
            rich: {
              m1: { align: 'right', padding: [0, 0, 0, 80] },
              m2: { align: 'left', padding: [0, 80, 0, 0] },
            },
          },
          splitLine: { show: true, lineStyle: { type: 'dashed', opacity: 0.35 } },
        },
        yAxis: {
          type: 'value',
          name: 'Nos.',
          nameLocation: 'middle',
          nameGap: 45,
          axisLabel: {
            fontSize: axisTickFontSize,
            formatter: (v: number) => `${Math.round(v).toLocaleString()}`
          }
        },
        series: [
          {
            name: 'New/Reviving',
            type: 'line',
            smooth: true,
            stack: 'Total',
            symbol: 'none',
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                { offset: 0.0, color: hexToRgba("#7B9A6D", 0.12) },
                { offset: 0.49, color: hexToRgba("#7B9A6D", 0.12) },
                { offset: 0.51, color: hexToRgba("#7B9A6D", 0.28) },
                { offset: 1.0, color: hexToRgba("#7B9A6D", 0.28) },
              ]),
            },
            data: [newRev_m1, newRev_m1, newRev_m2],
          },
          {
            name: 'Other SKUs',
            type: 'line',
            smooth: true,
            stack: 'Total',
            symbol: 'none',
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                { offset: 0.0, color: hexToRgba("#3A8ea4", 0.12) },
                { offset: 0.49, color: hexToRgba("#3A8ea4", 0.12) },
                { offset: 0.51, color: hexToRgba("#3A8ea4", 0.28) },
                { offset: 1.0, color: hexToRgba("#3A8ea4", 0.28) },
              ]),
            },
            data: [other_m1, other_m1, other_m2],
          },
          {
            name: 'Top 80%',
            type: 'line',
            smooth: true,
            stack: 'Total',
            symbol: 'none',
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                { offset: 0.0, color: hexToRgba("#ed9f50", 0.12) },
                { offset: 0.49, color: hexToRgba("#ed9f50", 0.12) },
                { offset: 0.51, color: hexToRgba("#ed9f50", 0.28) },
                { offset: 1.0, color: hexToRgba("#ed9f50", 0.28) },
              ]),
            },
            data: [top80_m1, top80_m1, top80_m2],
            markLine: {
              symbol: "none",
              silent: true,
              data: [{ xAxis: "" }],
              lineStyle: { color: "#111827", width: 1, opacity: 0.35 },
              label: { show: false },
            },
          },
        ],
      };

      unitsChartInstanceRef.current.setOption(option, true);
      unitsChartInstanceRef.current.resize();

      window.addEventListener("resize", onResize);
      ro = new ResizeObserver(() => unitsChartInstanceRef.current?.resize());
      ro.observe(el);
    });

    return () => {
      cancelAnimationFrame(raf);
      window.removeEventListener("resize", onResize);
      ro?.disconnect();
    };
  }, [
    month1, year1, month2, year2,
    categorizedGrowth.top_80_skus,
    categorizedGrowth.new_or_reviving_skus,
    categorizedGrowth.other_skus,
    rateMap,
    displayCurrencySymbol,
    displayCurrencyCode,
    effectiveCountry,
    axisTickFontSize,
    isPreviewMode,
  ]);

  useEffect(() => {
    const el = aspChartRef.current;
    if (!el) return;

    let ro: ResizeObserver | null = null;
    const onResize = () => aspChartInstanceRef.current?.resize();

    const raf = requestAnimationFrame(() => {
      if (aspChartInstanceRef.current && aspChartInstanceRef.current.getDom() !== el) {
        aspChartInstanceRef.current.dispose();
        aspChartInstanceRef.current = null;
      }
      if (!aspChartInstanceRef.current) {
        aspChartInstanceRef.current = echarts.init(el);
      }

      const { x, values } = buildCompareSeries('asp');
      const { top80_m1, top80_m2, newRev_m1, newRev_m2, other_m1, other_m2 } = values;
      const currency = displayCurrencySymbol;

      const hasAny =
        top80_m1 || top80_m2 || newRev_m1 || newRev_m2 || other_m1 || other_m2;

      if (!hasAny && !isPreviewMode && aspChartInstanceRef.current) {
        setEmptyChartOption(aspChartInstanceRef.current);
        aspChartInstanceRef.current.resize();
        return;
      }

      const option: echarts.EChartsOption = {
        tooltip: {
          trigger: 'axis',
          formatter: (params: any) => {
            const axisLabel = params?.[0]?.axisValueLabel ?? '';
            const lines: string[] = [];
            lines.push(`<div style="font-weight:700;margin-bottom:6px;">ASP ${axisLabel}</div>`);

            const map = new Map(params.map((p: any) => [p.seriesName, p]));
            const ordered = SERIES_ORDER.map(s => map.get(s.name)).filter(Boolean);

            ordered.forEach((p: any) => {
              lines.push(
                `<div style="display:flex;align-items:center;gap:8px;margin:2px 0;">
                <span style="display:inline-block;width:10px;height:10px;border-radius:999px;background:${p.color};"></span>
                <span style="flex:1;">${p.seriesName}</span>
                <span style="font-weight:700;">${currency}${Number(p.data ?? 0).toFixed(2)}</span>
              </div>`
              );
            });

            return `<div style="min-width:180px;">${lines.join('')}</div>`;
          },
        },
        legend: { show: false },
        grid: { left: 50, right: 20, top: 40, bottom: 35 },
        color: ['#7B9A6D', '#3A8ea4', '#ed9f50'],
        xAxis: {
          type: 'category',
          boundaryGap: false,
          data: x,
          axisLabel: {
            interval: 0,
            fontSize: axisTickFontSize,
            margin: 20,
            align: 'center',
            formatter: (v: string, idx: number) => {
              if (v === '') return '';
              if (idx === 0) return `{m1|${v}}`;
              if (idx === 2) return `{m2|${v}}`;
              return v;
            },
            rich: {
              m1: { align: 'right', padding: [0, 0, 0, 80] },
              m2: { align: 'left', padding: [0, 80, 0, 0] },
            },
          },
          splitLine: { show: true, lineStyle: { type: 'dashed', opacity: 0.35 } },
        },
        yAxis: {
          type: 'value',
          name: `Amount (${currency})`,
          nameLocation: 'middle',
          nameGap: 45,
          axisLabel: {
            fontSize: axisNameFontSize,
            formatter: (value: number) => {
              if (!value) return '0';
              return Number.isInteger(value) ? value.toString() : value.toFixed(0);
            }
          },
        },
        series: [
          {
            name: 'New/Reviving',
            type: 'line',
            smooth: true,
            stack: 'Total',
            symbol: 'none',
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                { offset: 0.0, color: hexToRgba("#7B9A6D", 0.12) },
                { offset: 0.49, color: hexToRgba("#7B9A6D", 0.12) },
                { offset: 0.51, color: hexToRgba("#7B9A6D", 0.28) },
                { offset: 1.0, color: hexToRgba("#7B9A6D", 0.28) },
              ]),
            },
            data: [newRev_m1, newRev_m1, newRev_m2],
          },
          {
            name: 'Other SKUs',
            type: 'line',
            smooth: true,
            stack: 'Total',
            symbol: 'none',
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                { offset: 0.0, color: hexToRgba("#3A8ea4", 0.12) },
                { offset: 0.49, color: hexToRgba("#3A8ea4", 0.12) },
                { offset: 0.51, color: hexToRgba("#3A8ea4", 0.28) },
                { offset: 1.0, color: hexToRgba("#3A8ea4", 0.28) },
              ]),
            },
            data: [other_m1, other_m1, other_m2],
          },
          {
            name: 'Top 80%',
            type: 'line',
            smooth: true,
            stack: 'Total',
            symbol: 'none',
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                { offset: 0.0, color: hexToRgba("#ed9f50", 0.12) },
                { offset: 0.49, color: hexToRgba("#ed9f50", 0.12) },
                { offset: 0.51, color: hexToRgba("#ed9f50", 0.28) },
                { offset: 1.0, color: hexToRgba("#ed9f50", 0.28) },
              ]),
            },
            data: [top80_m1, top80_m1, top80_m2],
            markLine: {
              symbol: "none",
              silent: true,
              data: [{ xAxis: "" }],
              lineStyle: { color: "#111827", width: 1, opacity: 0.35 },
              label: { show: false },
            },
          },
        ],
      };

      aspChartInstanceRef.current.setOption(option, true);
      aspChartInstanceRef.current.resize();

      window.addEventListener("resize", onResize);
      ro = new ResizeObserver(() => aspChartInstanceRef.current?.resize());
      ro.observe(el);
    });

    return () => {
      cancelAnimationFrame(raf);
      window.removeEventListener("resize", onResize);
      ro?.disconnect();
    };
  }, [
    month1, year1, month2, year2,
    categorizedGrowth.top_80_skus,
    categorizedGrowth.new_or_reviving_skus,
    categorizedGrowth.other_skus,
    rateMap,
    displayCurrencySymbol,
    displayCurrencyCode,
    effectiveCountry,
    axisTickFontSize,
    axisNameFontSize,
    isPreviewMode,
  ]);




  const months: MonthOption[] = [
    { value: '01', label: 'January' }, { value: '02', label: 'February' },
    { value: '03', label: 'March' }, { value: '04', label: 'April' },
    { value: '05', label: 'May' }, { value: '06', label: 'June' },
    { value: '07', label: 'July' }, { value: '08', label: 'August' },
    { value: '09', label: 'September' }, { value: '10', label: 'October' },
    { value: '11', label: 'November' }, { value: '12', label: 'December' },
  ];
  const currentYear = new Date().getFullYear();
  const years = Array.from({ length: 3 }, (_, i) => String(currentYear - i));
  const pad2 = (m: string | number) => String(m).padStart(2, '0');
  const getAbbr = (m: string | number) => months.find(x => x.value === pad2(m))?.label.slice(0, 3) || '';

  const isGlobalData = () => (countryName || '').toLowerCase() === 'global';
  const getTabLabel = (key: TabKey): string =>
    key === 'top_80_skus' ? 'Top 80% SKUs'
      : key === 'new_or_reviving_skus' ? 'New/Reviving SKUs'
        : key === 'other_skus' ? 'Other SKUs'
          : 'All SKUs';

  const getTabNumberForFeedback = (key: keyof CategorizedGrowth): number =>
    key === 'top_80_skus' ? 1 : key === 'new_or_reviving_skus' ? 2 : 3;

  // ✅ NEW helper: check if (year, month) allowed by backend
  const isPeriodAvailable = (year: string, month: string) => {
    if (!year || !month) return false;
    if (!availablePeriods.length) return true; // if API failed, don't block UI
    const key = `${year}-${month}`;
    return availablePeriods.includes(key);
  };

  // =========================
  // Persistence: helpers
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

  const normalizeCategorizedGrowth = (raw?: any): CategorizedGrowth => {
    const mapRow = (row: any): SkuItem => {
      const clone: any = { ...row };

      // sales mix key mapping (if needed)
      if (row['Sales Mix (Current)'] != null) clone['Sales Mix (Month2)'] = row['Sales Mix (Current)'];

      // growth field mapping (if your backend uses (%) keys)
      const fieldMap: Record<string, string> = {
        'Unit Growth (%)': 'Unit Growth',
        'ASP Growth (%)': 'ASP Growth',
        'Gross Sales Growth (%)': 'Gross Sales Growth',
        'Net Sales Growth (%)': 'Net Sales Growth',
        'Sales Mix Change (%)': 'Sales Mix Change',
        'Profit Per Unit (%)': 'Profit Per Unit',
        'CM1 Profit Impact (%)': 'CM1 Profit Impact',
      };
      Object.entries(fieldMap).forEach(([bk, fk]) => {
        if (row[bk] != null) clone[fk] = row[bk];
      });

      // prev/curr → month1/month2 (aapke months compare page me bhi same keys use ho rahe)
      clone.total_quantity_month1 = row.total_quantity_month1 ?? row.total_quantity_prev ?? null;
      clone.total_quantity_month2 = row.total_quantity_month2 ?? row.total_quantity_curr ?? null;

      clone.asp_month1 = row.asp_month1 ?? row.asp_prev ?? null;
      clone.asp_month2 = row.asp_month2 ?? row.asp_curr ?? null;

      clone.net_sales_month1 = row.net_sales_month1 ?? row.net_sales_prev ?? null;
      clone.net_sales_month2 = row.net_sales_month2 ?? row.net_sales_curr ?? null;

      clone.gross_sales_month1 = row.gross_sales_month1 ?? row.product_sales_prev ?? null;
      clone.gross_sales_month2 = row.gross_sales_month2 ?? row.product_sales_curr ?? null;


      clone.sales_mix_month1 = row.sales_mix_month1 ?? row.sales_mix_prev ?? null;
      clone.sales_mix_month2 = row.sales_mix_month2 ?? row.sales_mix_curr ?? row['Sales Mix (Current)'] ?? null;

      clone.profit_percentage_month1 = row.profit_percentage_month1 ?? row.profit_percentage_prev ?? null;
      clone.profit_percentage_month2 = row.profit_percentage_month2 ?? row.profit_percentage_curr ?? null;


      clone.unit_wise_profitability_month1 =
        row.unit_wise_profitability_month1 ?? row.unit_wise_profitability_prev ?? null;
      clone.unit_wise_profitability_month2 =
        row.unit_wise_profitability_month2 ?? row.unit_wise_profitability_curr ?? null;

      clone.profit_month1 = row.profit_month1 ?? row.profit_prev ?? null;
      clone.profit_month2 = row.profit_month2 ?? row.profit_curr ?? null;

      clone.rembursement_fee_month1 = row.rembursement_fee_month1 ?? row.rembursement_fee_prev ?? null;
      clone.rembursement_fee_month2 = row.rembursement_fee_month2 ?? row.rembursement_fee_curr ?? null;


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

      all_skus: (raw.all_skus || [
        ...(raw.top_80_skus || []),
        ...(raw.new_or_reviving_skus || []),
        ...(raw.other_skus || []),
      ]).map(mapRow),

      top_80_total: raw.top_80_total ? mapRow(raw.top_80_total) : null,
      new_or_reviving_total: raw.new_or_reviving_total ? mapRow(raw.new_or_reviving_total) : null,
      other_total: raw.other_total ? mapRow(raw.other_total) : null,
      all_skus_total: raw.all_skus_total ? mapRow(raw.all_skus_total) : null,
    };
  };


  // =========================
  // Load persisted state on mount
  // =========================
  useEffect(() => {
    if (isPreviewMode) return;

    const saved = loadCompareFromStorage();
    if (saved) {
      setMonth1(saved.month1 || '');
      setYear1(saved.year1 || '');
      setMonth2(saved.month2 || '');
      setYear2(saved.year2 || '');
      setCategorizedGrowth(
        saved.categorizedGrowth || {
          top_80_skus: [],
          new_or_reviving_skus: [],
          other_skus: [],
          all_skus: [],
          top_80_total: null,
          new_or_reviving_total: null,
          other_total: null,
          all_skus_total: null,
        }
      );
      setMonth2Label(saved.month2Label || '');
    }

    // const cachedInsights = loadInsightsFromStorage();
    // if (cachedInsights && Object.keys(cachedInsights).length) {
    //   setSkuInsights(cachedInsights);
    // }
  }, [isPreviewMode]);

  // ✅ NEW: fetch available periods from backend
  useEffect(() => {
    if (!countryName) return;
    const fetchAvailable = async () => {
      try {
        const res = await api.get<{ periods: string[] }>('/MonthsforBI/available-periods', {
          params: { countryName },
        });
        setAvailablePeriods(res.data?.periods || []);
      } catch (err: any) {
        console.error('Failed to load available periods:', err?.response?.data || err.message);
      }
    };
    fetchAvailable();
  }, [countryName]);

  // ✅ NEW: if year change ke baad combination invalid ho jaye to month reset
  useEffect(() => {
    if (year1 && month1 && !isPeriodAvailable(year1, month1)) {
      setMonth1('');
    }
  }, [year1, availablePeriods]);

  useEffect(() => {
    if (year2 && month2 && !isPeriodAvailable(year2, month2)) {
      setMonth2('');
    }
  }, [year2, availablePeriods]);

  useEffect(() => {
    if (isPreviewMode) return; // 🔥 ADD THIS LINE
    if (!availablePeriods?.length) return;

    const hasValid =
      month1 && year1 && month2 && year2 &&
      isPeriodAvailable(year1, month1) &&
      isPeriodAvailable(year2, month2);

    if (hasValid) return;

    const def = pickDefaultComparePeriods(availablePeriods);
    if (!def) return;

    const [y2, m2] = def.newer.split("-");
    const [y1, m1] = def.older.split("-");

    setYear1(y1);
    setMonth1(m1);
    setYear2(y2);
    setMonth2(m2);
  }, [availablePeriods, isPreviewMode]);



  // =====================
  // Fetch compare result
  // =====================
  const handleSubmit = async (e?: React.FormEvent) => {
    if (isPreviewMode) {
      setError(null);
      setCategorizedGrowth(DUMMY_CATEGORIZED_GROWTH);
      setAdvertisingTotals({ month1: 0, month2: 0 });
      setExpenseTotals({ month1: 0, month2: 0 });
      setReimbursementTotals({ month1: 0, month2: 0 });
      setMonth2Label("Preview");
      return;
    }

    e?.preventDefault?.();
    setError(null);
    setLoadingCompare(true);

    // keep old chart data visible while fetching
    // do NOT clear categorizedGrowth here
    // do NOT clear month2Label here unless you really want blank header during loading

    // setSkuInsights({});
    // setModalOpen(false);
    // saveInsightsToStorage({});

    if (!month1 || !year1 || !month2 || !year2) {
      setError('Please select both months and years.');
      setLoadingCompare(false);
      return;
    }

    if (!isPeriodAvailable(year1, month1) || !isPeriodAvailable(year2, month2)) {
      setError('Selected month ka data available nahi hai. Sirf highlighted months select karein.');
      setLoadingCompare(false);
      return;
    }

    try {
      const res = await api.get<ApiResponse>('/MonthsforBI', {
        params: { month1, year1, month2, year2, countryName },
      });

      const newMonth2Label = res.data?.comparison_range?.month2_label || '';
      const raw = res.data?.categorized_growth;
      const newCategorized = normalizeCategorizedGrowth(raw);

      setMonth2Label(newMonth2Label);
      setCategorizedGrowth(newCategorized);
      setReimbursementTotals(res.data?.reimbursement_totals ?? null);
      setAdvertisingTotals(res.data?.advertising_totals ?? null);
      setExpenseTotals(res.data?.expense_totals ?? null);

      saveCompareToStorage({
        month1,
        year1,
        month2,
        year2,
        categorizedGrowth: newCategorized,
        month2Label: newMonth2Label,
        countryName,
      });
    } catch (err: any) {
      console.error('MonthsforBI error:', err?.response?.data || err.message);
      setError(err?.response?.data?.error || 'An error occurred');
    } finally {
      setLoadingCompare(false);
    }
  };

  useEffect(() => {
    if (isPreviewMode) return;
    if (!countryName) return;

    if (!month1 || !year1 || !month2 || !year2) return;

    if (!isPeriodAvailable(year1, month1) || !isPeriodAvailable(year2, month2)) return;

    if (didInitialFetchRef.current) return;

    didInitialFetchRef.current = true;
    handleSubmit();
  }, [countryName, month1, year1, month2, year2, isPreviewMode, availablePeriods]);


  // =====================
  // AI insights generate
  // =====================
  // const analyzeSkus = async (
  //   e?: React.MouseEvent<HTMLButtonElement> | React.FormEvent
  // ) => {
  //   e?.preventDefault?.();
  //   e?.stopPropagation?.();

  //   setLoadingInsight(true);
  //   try {
  //     const allSkus: SkuItem[] = [
  //       ...categorizedGrowth.top_80_skus,
  //       ...categorizedGrowth.new_or_reviving_skus,
  //       ...categorizedGrowth.other_skus,
  //     ];

  //     const res = await api.post<{ insights: Record<string, SkuInsight> }>(
  //       "/analyze_skus",
  //       {
  //         month1,
  //         year1,
  //         month2,
  //         year2,
  //         country: countryName,
  //         skus: allSkus,
  //       }
  //     );

  //     const insights = res.data?.insights || {};
  //     setSkuInsights(insights);
  //     saveInsightsToStorage(insights);
  //   } catch (err: any) {
  //     console.error("analyze_skus error:", err?.response?.data || err.message);
  //   } finally {
  //     setLoadingInsight(false);
  //   }
  // };

  // const analyzeSkus = async (
  //   e?: React.MouseEvent<HTMLButtonElement> | React.FormEvent
  // ) => {
  //   e?.preventDefault?.();
  //   e?.stopPropagation?.();

  //   if (loadingInsight) return;

  //   setLoadingInsight(true);

  //   try {
  //     const allSkus: SkuItem[] = [
  //       ...(categorizedGrowth.top_80_skus || []),
  //       ...(categorizedGrowth.new_or_reviving_skus || []),
  //       ...(categorizedGrowth.other_skus || []),
  //     ];

  //     const res = await api.post<{ insights: Record<string, SkuInsight> }>(
  //       "/analyze_skus",
  //       {
  //         month1,
  //         year1,
  //         month2,
  //         year2,
  //         country: countryName,
  //         skus: allSkus,
  //       }
  //     );

  //     const insights = res.data?.insights || {};
  //     setSkuInsights(insights);
  //     saveInsightsToStorage(insights);
  //   } catch (err: any) {
  //     console.error("analyze_skus error:", err?.response?.data || err.message);
  //   } finally {
  //     setLoadingInsight(false);
  //   }
  // };

  // useEffect(() => {
  //   requestAnimationFrame(() => {
  //     unitsChartInstanceRef.current?.resize();
  //     chartInstanceRef.current?.resize();
  //     profitChartInstanceRef.current?.resize();
  //     aspChartInstanceRef.current?.resize();
  //   });
  // }, [loadingInsight, skuInsights]);

  // =====================
  // Insight lookups
  // =====================
  // const getInsightByProductName = (productName: string): [string, SkuInsight] | null => {
  //   if (!productName) return null;
  //   const needle = productName.toLowerCase().trim();

  //   // Prefer exact match
  //   let entry = Object.entries(skuInsights).find(
  //     ([, d]) => d.product_name?.toLowerCase().trim() === needle
  //   );
  //   // For GLOBAL, allow partial fallback
  //   if (!entry && isGlobalData()) {
  //     entry = Object.entries(skuInsights).find(([, d]) => {
  //       const n = d.product_name?.toLowerCase().trim();
  //       return n && (n.includes(needle) || needle.includes(n));
  //     });
  //   }
  //   return entry ? entry as [string, SkuInsight] : null; // [key, value]
  // };

  // const getInsightForItem = (item: SkuItem): [string, SkuInsight] | null => {
  //   if (isGlobalData()) return getInsightByProductName(item.product_name);
  //   if (item.sku && skuInsights[item.sku]) return [item.sku, skuInsights[item.sku]];
  //   return getInsightByProductName(item.product_name);
  // };


  const fmtNum = (v: any) => Math.round(Number(v || 0)).toLocaleString();

  const SERIES_ORDER = [
    { name: 'Top 80%', color: '#ed9f50' },
    { name: 'Other SKUs', color: '#3A8ea4' },
    { name: 'New/Reviving', color: '#7B9A6D' },
  ];


  // =====================
  // Export to Excel
  // =====================
  // =====================
  const exportToExcel = (rows: SkuItem[], filename = 'export.xlsx') => {
    // ✅ IMPORTANT: backend fields are tied to month1(old) / month2(new). Keep fixed mapping.
    const isM2New = true;

    const newMonth = isM2New ? month2 : month1;
    const newYear = isM2New ? year2 : year1;
    const oldMonth = isM2New ? month1 : month2;
    const oldYear = isM2New ? year1 : year2;

    const newAbbr = `${getAbbr(newMonth)}'${String(newYear).slice(2)}`;
    const oldAbbr = `${getAbbr(oldMonth)}'${String(oldYear).slice(2)}`;

    // 1) remove any existing total rows coming from API/data
    const cleanRows = (rows || []).filter((r) => {
      const name = String(r?.product_name || '').toLowerCase().trim();
      return name !== 'total' && !name.includes('total (top 80') && name !== 'total (top 80%)';
    });

    const num = (v: any) => {
      const n = Number(v);
      return Number.isFinite(n) ? n : 0;
    };

    const round2 = (v: any) => {
      const n = Number(v);
      return Number.isFinite(n) ? Number(n.toFixed(2)) : null;
    };

    // helper to pick month1/month2 value based on which is new/old
    const pickNew = (row: any, keyMonth1: string, keyMonth2: string) =>
      isM2New ? row?.[keyMonth2] : row?.[keyMonth1];

    const pickOld = (row: any, keyMonth1: string, keyMonth2: string) =>
      isM2New ? row?.[keyMonth1] : row?.[keyMonth2];

    const pct = (oldV: number, newV: number) => (oldV ? ((newV - oldV) / oldV) * 100 : null);

    // ✅ EXACT column order (NEW month first, then OLD month)
    const headerOrder = [
      'SKU',
      'Product',

      `Qty ${newAbbr}`,
      `Qty ${oldAbbr}`,
      'Change in Qty (%age)',

      `Gross Sales ${newAbbr}`,
      `Gross Sales ${oldAbbr}`,
      'Change in Gross Sales (%age)',

      `Net Sales ${newAbbr}`,
      `Net Sales ${oldAbbr}`,
      'Change in Net Sales (%age)',

      `ASP ${newAbbr}`,
      `ASP ${oldAbbr}`,
      'Change in ASP (%age)',

      `Sales Mix ${newAbbr}`,
      `Sales Mix ${oldAbbr}`,
      'Change in Sales Mix (%age)',

      `CM1 Profit ${newAbbr}`,
      `CM1 Profit ${oldAbbr}`,
      'Change in CM1 Profit',

      `CM1 Profit %age(${newAbbr})`,
      `CM1 Profit %age(${oldAbbr})`,

      `CM1 Unit Profit ${newAbbr}`,
      `CM1 Unit Profit ${oldAbbr}`,
      'Change in CM1 Unit Profit (%age)',
    ];

    /**
     * ✅ Percent formatting:
     * formats columns whose header contains "%" OR starts with "Sales Mix "
     * for rows below the provided header row until it hits a blank separator row.
     */
    const addPercentToPercentColumns = (ws: XLSX.WorkSheet, headerRowIndexes: number[] = [0]) => {
      const ref = ws["!ref"];
      if (!ref) return;

      const range = XLSX.utils.decode_range(ref);
      const isSalesMixHeader = (h: string) => h.trim().toLowerCase().startsWith("sales mix ");

      for (const headerRow of headerRowIndexes) {
        if (headerRow < range.s.r || headerRow > range.e.r) continue;

        for (let C = range.s.c; C <= range.e.c; C++) {
          const headerCell = ws[XLSX.utils.encode_cell({ r: headerRow, c: C })];
          const header = String(headerCell?.v ?? "");

          const shouldFormatAsPercent = header.includes("%") || isSalesMixHeader(header);
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

    // -------------------------
    // Shared: formatter for a section (adds a Total row at bottom)
    // -------------------------
    const formatRowsWithTotals = (inputRows: SkuItem[]) => {
      const clean = (inputRows || []).filter((r) => {
        const name = String(r?.product_name || '').toLowerCase().trim();
        return name !== 'total' && !name.includes('total (top 80') && name !== 'total (top 80%)';
      });

      // ✅ FIX: compute Sales Mix from Net Sales totals (prevents totals > 100 due to rounding)
      const totalNsNew = clean.reduce((s, r) => s + num(pickNew(r, 'net_sales_month1', 'net_sales_month2')), 0);
      const totalNsOld = clean.reduce((s, r) => s + num(pickOld(r, 'net_sales_month1', 'net_sales_month2')), 0);

      const formatted = clean.map((row) => {
        const unitGrowth = row['Unit Growth'] as GrowthCategory | undefined;
        const aspGrowth = row['ASP Growth'] as GrowthCategory | undefined;
        const grossSalesGrowth = row['Gross Sales Growth'] as GrowthCategory | undefined;
        const netSalesGrowth = row['Net Sales Growth'] as GrowthCategory | undefined;
        const unitProfitGrowth = row['Profit Per Unit'] as GrowthCategory | undefined;

        const qtyOld = pickOld(row, 'total_quantity_month1', 'total_quantity_month2');
        const qtyNew = pickNew(row, 'total_quantity_month1', 'total_quantity_month2');

        const gsOld = pickOld(row, 'gross_sales_month1', 'gross_sales_month2');
        const gsNew = pickNew(row, 'gross_sales_month1', 'gross_sales_month2');

        const nsOld = pickOld(row, 'net_sales_month1', 'net_sales_month2');
        const nsNew = pickNew(row, 'net_sales_month1', 'net_sales_month2');

        const aspOld = pickOld(row, 'asp_month1', 'asp_month2');
        const aspNew = pickNew(row, 'asp_month1', 'asp_month2');

        // ✅ FIX: recompute mix from Net Sales instead of using stored % (which may be rounded)
        const mixOld = totalNsOld ? (num(nsOld) / totalNsOld) * 100 : null;
        const mixNew = totalNsNew ? (num(nsNew) / totalNsNew) * 100 : null;

        const cm1Old = pickOld(row, 'profit_month1', 'profit_month2');
        const cm1New = pickNew(row, 'profit_month1', 'profit_month2');

        const cm1PctOld = pickOld(row, 'profit_percentage_month1', 'profit_percentage_month2');
        const cm1PctNew = pickNew(row, 'profit_percentage_month1', 'profit_percentage_month2');

        const upOld = pickOld(row, 'unit_wise_profitability_month1', 'unit_wise_profitability_month2');
        const upNew = pickNew(row, 'unit_wise_profitability_month1', 'unit_wise_profitability_month2');

        return {
          SKU: row.sku || '',
          Product: row.product_name || '',

          [`Qty ${newAbbr}`]: qtyNew ?? null,
          [`Qty ${oldAbbr}`]: qtyOld ?? null,
          'Change in Qty (%age)': unitGrowth?.value ?? null,

          [`Gross Sales ${newAbbr}`]: gsNew ?? null,
          [`Gross Sales ${oldAbbr}`]: gsOld ?? null,
          'Change in Gross Sales (%age)': grossSalesGrowth?.value ?? null,

          [`Net Sales ${newAbbr}`]: nsNew ?? null,
          [`Net Sales ${oldAbbr}`]: nsOld ?? null,
          'Change in Net Sales (%age)': netSalesGrowth?.value ?? null,

          [`ASP ${newAbbr}`]: round2(aspNew ?? null),
          [`ASP ${oldAbbr}`]: round2(aspOld ?? null),
          'Change in ASP (%age)': aspGrowth?.value ?? null,

          [`Sales Mix ${newAbbr}`]: mixNew ?? null,
          [`Sales Mix ${oldAbbr}`]: mixOld ?? null,

          // ✅ FIX: compute change from recomputed mixes (keeps columns consistent)
          'Change in Sales Mix (%age)': mixOld != null && mixNew != null ? mixNew - mixOld : null,

          [`CM1 Profit ${newAbbr}`]: cm1New ?? null,
          [`CM1 Profit ${oldAbbr}`]: cm1Old ?? null,
          'Change in CM1 Profit': cm1New != null && cm1Old != null ? Number(cm1New) - Number(cm1Old) : null,

          [`CM1 Profit %age(${newAbbr})`]: cm1PctNew ?? null,
          [`CM1 Profit %age(${oldAbbr})`]: cm1PctOld ?? null,

          [`CM1 Unit Profit ${newAbbr}`]: upNew ?? null,
          [`CM1 Unit Profit ${oldAbbr}`]: upOld ?? null,
          'Change in CM1 Unit Profit (%age)': unitProfitGrowth?.value ?? null,
        };
      });

      // totals
      const totals = clean.reduce(
        (acc, r) => {
          acc.qtyOld += num(pickOld(r, 'total_quantity_month1', 'total_quantity_month2'));
          acc.qtyNew += num(pickNew(r, 'total_quantity_month1', 'total_quantity_month2'));

          acc.gsOld += num(pickOld(r, 'gross_sales_month1', 'gross_sales_month2'));
          acc.gsNew += num(pickNew(r, 'gross_sales_month1', 'gross_sales_month2'));

          acc.nsOld += num(pickOld(r, 'net_sales_month1', 'net_sales_month2'));
          acc.nsNew += num(pickNew(r, 'net_sales_month1', 'net_sales_month2'));

          // ✅ FIX: do NOT sum Sales Mix % values

          acc.cm1Old += num(pickOld(r, 'profit_month1', 'profit_month2'));
          acc.cm1New += num(pickNew(r, 'profit_month1', 'profit_month2'));

          acc.upOld += num(pickOld(r, 'unit_wise_profitability_month1', 'unit_wise_profitability_month2'));
          acc.upNew += num(pickNew(r, 'unit_wise_profitability_month1', 'unit_wise_profitability_month2'));

          return acc;
        },
        { qtyOld: 0, qtyNew: 0, gsOld: 0, gsNew: 0, nsOld: 0, nsNew: 0, cm1Old: 0, cm1New: 0, upOld: 0, upNew: 0 }
      );

      const safeDiv = (a: number, b: number) => (b ? a / b : null);
      const totalAspOld = round2(safeDiv(totals.nsOld, totals.qtyOld));
      const totalAspNew = round2(safeDiv(totals.nsNew, totals.qtyNew));

      const profitPct = (profit: number, sales: number) => (sales ? (profit / sales) * 100 : null);
      const totalCm1PctOld = profitPct(totals.cm1Old, totals.nsOld);
      const totalCm1PctNew = profitPct(totals.cm1New, totals.nsNew);

      // ✅ FIX: Total Sales Mix should be exactly 100% only when there is net sales
      const totalSalesMixOld = totalNsOld ? 100 : null;
      const totalSalesMixNew = totalNsNew ? 100 : null;

      // ✅ Total mix is 100% in both months (if there is sales), so change should be 0%
      const totalSalesMixChange =
        totalSalesMixOld != null && totalSalesMixNew != null ? pct(totalSalesMixOld, totalSalesMixNew) : null;

      formatted.push({
        SKU: '',
        Product: 'Total',

        [`Qty ${newAbbr}`]: totals.qtyNew,
        [`Qty ${oldAbbr}`]: totals.qtyOld,
        'Change in Qty (%age)': pct(totals.qtyOld, totals.qtyNew),

        [`Gross Sales ${newAbbr}`]: totals.gsNew,
        [`Gross Sales ${oldAbbr}`]: totals.gsOld,
        'Change in Gross Sales (%age)': pct(totals.gsOld, totals.gsNew),

        [`Net Sales ${newAbbr}`]: totals.nsNew,
        [`Net Sales ${oldAbbr}`]: totals.nsOld,
        'Change in Net Sales (%age)': pct(totals.nsOld, totals.nsNew),

        [`ASP ${newAbbr}`]: totalAspNew,
        [`ASP ${oldAbbr}`]: totalAspOld,
        'Change in ASP (%age)': totalAspOld != null && totalAspNew != null ? pct(totalAspOld, totalAspNew) : null,

        [`Sales Mix ${newAbbr}`]: totalSalesMixNew,
        [`Sales Mix ${oldAbbr}`]: totalSalesMixOld,
        'Change in Sales Mix (%age)': totalSalesMixChange,

        [`CM1 Profit ${newAbbr}`]: totals.cm1New,
        [`CM1 Profit ${oldAbbr}`]: totals.cm1Old,
        'Change in CM1 Profit': totals.cm1New - totals.cm1Old,

        [`CM1 Profit %age(${newAbbr})`]: totalCm1PctNew,
        [`CM1 Profit %age(${oldAbbr})`]: totalCm1PctOld,

        [`CM1 Unit Profit ${newAbbr}`]: totals.upNew,
        [`CM1 Unit Profit ${oldAbbr}`]: totals.upOld,
        'Change in CM1 Unit Profit (%age)': pct(totals.upOld, totals.upNew),
      });

      return formatted;
    };

    // -------------------------
    // Sheet 1: All SKUs (Growth Comparison)
    // -------------------------
    const formattedAll = formatRowsWithTotals(cleanRows);

    const ws1 = XLSX.utils.json_to_sheet(formattedAll, { header: headerOrder });
    XLSX.utils.sheet_add_aoa(ws1, [headerOrder], { origin: 'A1' });
    addPercentToPercentColumns(ws1, [0]);
    ws1['!freeze'] = { xSplit: 0, ySplit: 1 };

    // -------------------------
    // Sheet 2: SKU Split (3 sections + ✅ only Grand Total row)
    // -------------------------
    const splitHeader = [...headerOrder];
    const aoa: any[][] = [];

    const makeSectionAoA = (sectionTitle: string, sectionRows: SkuItem[]) => {
      const formatted = formatRowsWithTotals(sectionRows);

      const body = formatted.map((obj) => {
        const rowArr: any[] = [];
        for (const h of headerOrder) rowArr.push((obj as any)[h] ?? null);
        return rowArr;
      });

      const titleRow = [sectionTitle];
      while (titleRow.length < splitHeader.length) titleRow.push('');

      return { titleRow, headerRow: splitHeader, body };
    };

    const top80 = categorizedGrowth.top_80_skus || [];
    const newRev = categorizedGrowth.new_or_reviving_skus || [];
    const other = categorizedGrowth.other_skus || [];

    const pushSection = (title: string, sectionRows: SkuItem[]) => {
      const { titleRow, headerRow, body } = makeSectionAoA(title, sectionRows);
      aoa.push(titleRow);
      aoa.push(headerRow);
      aoa.push(...body);
      aoa.push([]); // blank row gap
    };

    pushSection('Top 80% SKUs', top80);
    pushSection('New/Reviving SKUs', newRev);
    pushSection('Other SKUs', other);

    // ✅ Append Grand Total title + header + ONLY the grand total row (not full table)
    {
      const grandTitleRow = ['Grand Total'];
      while (grandTitleRow.length < splitHeader.length) grandTitleRow.push('');

      // formatRowsWithTotals adds many rows + a Total at the end.
      // We only want the final "Total" row.
      const grandFormatted = formatRowsWithTotals([...top80, ...newRev, ...other]);
      const grandTotalObj = grandFormatted[grandFormatted.length - 1]; // the "Total" row

      const grandTotalRow = headerOrder.map((h) => (grandTotalObj as any)?.[h] ?? null);

      aoa.push(grandTitleRow);
      aoa.push(splitHeader);
      aoa.push(grandTotalRow);
    }

    const ws2 = XLSX.utils.aoa_to_sheet(aoa);
    ws2['!freeze'] = { xSplit: 0, ySplit: 2 };

    // ✅ apply percent formatting for EVERY repeated header row (3 sections + grand total)
    // In SKU Split, each table header row is the row where col A is "SKU".
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

    // -------------------------
    // Build workbook with 2 sheets
    // -------------------------
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws1, 'All SKUs');
    XLSX.utils.book_append_sheet(wb, ws2, 'SKU Split');

    XLSX.writeFile(wb, filename);
  };
  // =====================

  const allRows = useMemo(() => ([
    ...(categorizedGrowth.top_80_skus || []),
    ...(categorizedGrowth.new_or_reviving_skus || []),
    ...(categorizedGrowth.other_skus || []),
  ]), [categorizedGrowth]);

  const sumKey = (k: string) =>
    allRows.reduce((a, r: any) => a + Number(r?.[k] ?? 0), 0);

  const m1Label = `${getAbbr(month1)}'${String(year1).slice(2)}`;
  const m2Label = `${getAbbr(month2)}'${String(year2).slice(2)}`;
  const currency = displayCurrencySymbol;


  const totalsLine = useMemo(() => {

    const netSales_m1 = totalOf("net_sales_month1");
    const netSales_m2 = totalOf("net_sales_month2");

    const profit_m1 = totalOf("profit_month1");
    const profit_m2 = totalOf("profit_month2");

    const otherExp_m1 = convertAmount(expenseTotals?.month1 ?? 0, effectiveCountry, month1, year1);
    const otherExp_m2 = convertAmount(expenseTotals?.month2 ?? 0, effectiveCountry, month2, year2);

    const adv_m1 = convertAmount(advertisingTotals?.month1 ?? 0, effectiveCountry, month1, year1);
    const adv_m2 = convertAmount(advertisingTotals?.month2 ?? 0, effectiveCountry, month2, year2);

    const reimb_m1 = convertAmount(reimbursementTotals?.month1 ?? 0, effectiveCountry, month1, year1);
    const reimb_m2 = convertAmount(reimbursementTotals?.month2 ?? 0, effectiveCountry, month2, year2);

    // ✅ YAHAN ADD KARO (return se just pehle)
    const ds = [
      { key: "netSales", label: "Net Sales", data: [netSales_m1, netSales_m2], color: "#75bbda" },
      { key: "cm1Profit", label: "CM1 Profit", data: [profit_m1, profit_m2], color: "#7b9a6d" },
      { key: "otherExpense", label: "Other Expense", data: [otherExp_m1, otherExp_m2], color: "#3A8EA4" },
      { key: "advertising", label: "Advertising Total", data: [adv_m1, adv_m2], color: "#C49466" },
      { key: "reimbursement", label: "Reimbursement", data: [reimb_m1, reimb_m2], color: "#FDD36F" },
    ];

    const datasets = ds
      .filter((d) => selectedTotals[d.key])
      .map((d) => ({
        label: d.label,
        data: d.data,
        fill: false,
        borderColor: d.color,
        backgroundColor: d.color,
        tension: 0.4,
      }));

    const isProfitabilityEmpty = datasets.every(
      (ds: any) => (ds.data || []).every((v: any) => Number(v) === 0)
    );

    return {
      data: {
        labels: [m1Label, m2Label],
        datasets,
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { intersect: false, mode: "index" as const },
        plugins: {
          legend: { display: false },
          tooltip: {
            mode: "index" as const,
            intersect: false,
            backgroundColor: "#ffffff",
            titleColor: "#111827",
            bodyColor: "#111827",
            borderColor: "#e5e7eb",
            borderWidth: 1,
            cornerRadius: 8,
            padding: 10,
            callbacks: {
              label: (tooltipItem: any) => {
                const label = tooltipItem.dataset.label || "";
                const value = tooltipItem.raw as number;
                return `${label}: ${currency}${Number(value ?? 0).toLocaleString(undefined, {
                  minimumFractionDigits: 2,
                  maximumFractionDigits: 2,
                })}`;
              },
            },
          },
        },
        scales: {
          x: { title: { display: false, text: "Month" } },
          y: {
            min: isProfitabilityEmpty ? 0 : undefined,
            suggestedMax: isProfitabilityEmpty ? 1 : undefined,
            ticks: {
              stepSize: isProfitabilityEmpty ? 0.2 : undefined,
              callback: (value: any) =>
                isProfitabilityEmpty
                  ? Number(value).toFixed(1)
                  : Number(value).toLocaleString(),
            },
            title: {
              display: true,
              text: `Amount (${currency})`,
            },
          },
        },
      },
    };
  }, [
    m1Label,
    m2Label,
    expenseTotals,
    advertisingTotals,
    reimbursementTotals,
    selectedTotals,
    allRows,
    rateMap,
    displayCurrencySymbol,
    displayCurrencyCode,
    effectiveCountry,
    month1,
    year1,
    month2,
    year2,
  ]);

  const renderSection = (title: string, raw?: string) => {
    if (!raw) return null;

    const sentences = raw
      .split(/(?<=\.)\s+|[\n\r]+/g)     // dot-space OR new lines
      .map(s => s.replace(/^-+\s*/, "").trim())
      .filter(Boolean);

    return (
      <div style={{ marginTop: 14 }}>
        <div style={{ fontSize: 15, fontWeight: 700, color: "#111827", marginBottom: 10 }}>
          {title}
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          {sentences.map((sentence, idx) => (
            <div
              key={idx}
              style={{
                display: "flex",
                alignItems: "flex-start",
                gap: 10,
                fontSize: 14,
                lineHeight: 1.6,
                color: "#374151",
              }}
            >
              <span
                style={{
                  width: 6,
                  height: 6,
                  marginTop: 7,
                  borderRadius: "50%",
                  backgroundColor: "#9CA3AF",
                  flexShrink: 0,
                }}
              />
              <span>{sentence}</span>
            </div>
          ))}
        </div>
      </div>
    );
  };

  const normalizeBullets = (raw?: string) => {
    if (!raw) return [];

    let parts = raw
      .split(/\r?\n+/g)
      .map(s => s.trim())
      .filter(Boolean);

    if (parts.length <= 1) {
      parts = raw
        .split(/(?<=[.!?])\s+/g)
        .map(s => s.trim())
        .filter(Boolean);
    }

    return parts
      .map((s) =>
        s
          .replace(/^[-•*]+\s*/, "")
          .replace(/^\d+[\).\]]\s*/, "")
          .trim()
      )
      .filter(Boolean);
  };

  const renderBullets = (raw?: string) => {
    const bullets = normalizeBullets(raw);
    if (!bullets.length) return null;

    return (
      <div style={{ marginTop: 14 }}>
        <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 8 }}>
          Product Journey
        </div>

        <div style={{ border: "1px solid #E5E7EB", borderRadius: 16, padding: 14 }}>
          <ul
            style={{
              margin: 0,
              paddingLeft: 22,
              listStyleType: "disc",         // ✅ THIS LINE fixes Tailwind reset
              listStylePosition: "outside",
            }}
          >
            {bullets.map((b, i) => (
              <li key={i} style={{ marginBottom: 10, display: "list-item" }}>
                {b}
              </li>
            ))}
          </ul>
        </div>
      </div>
    );
  };

  const pill = (label: string, value: any) => {
    const displayValue = (() => {
      if (value === null || value === undefined) return "-";
      if (typeof value === "boolean") return value ? "true" : "false"; // 👈 ye important
      if (value === "") return "-";
      return String(value);
    })();

    return (
      <div
        style={{
          border: "1px solid #E5E7EB",
          borderRadius: 12,
          padding: "10px 12px",
          background: "#fff",
        }}
      >
        <div style={{ fontSize: 12, color: "#6B7280", marginBottom: 4 }}>
          {label}
        </div>
        <div style={{ fontSize: 16, fontWeight: 700, color: "#111827" }}>
          {displayValue}
        </div>
      </div>
    );
  };

  const bigBox = (title: string, text?: string) => (
    <div style={{ marginTop: 14 }}>
      <div style={{ fontSize: 14, fontWeight: 700, color: "#111827", marginBottom: 8 }}>{title}</div>
      <div style={{ border: "1px solid #E5E7EB", borderRadius: 14, padding: "12px 14px", background: "#fff", color: "#374151", lineHeight: 1.6, fontSize: 14 }}>
        {text || "--"}
      </div>
    </div>
  );

  // useEffect(() => {
  //   setExpandAllSkusOthers(false);
  // }, [categorizedGrowth]);

  const renderFormattedInsight = (raw: string) => {
    if (!raw) return null;

    const sentences = raw
      .split(/(?<=\.)\s+/)
      .map(s => s.replace(/^-+\s*/, "").trim()) // remove leading "-"
      .filter(Boolean);

    return (
      <div style={{ marginTop: 8 }}>

        {/* Heading */}
        <div
          style={{
            fontSize: 16,
            fontWeight: 600,
            color: "#111827",
            marginBottom: 12,
          }}
        >
          AI Insight
        </div>

        {/* Bullet Points */}
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {sentences.map((sentence, idx) => (
            <div
              key={idx}
              style={{
                display: "flex",
                alignItems: "flex-start",
                gap: 10,
                fontSize: 14,
                lineHeight: 1.6,
                color: "#374151",
              }}
            >
              {/* Clean subtle bullet */}
              <span
                style={{
                  width: 6,
                  height: 6,
                  marginTop: 7,
                  borderRadius: "50%",
                  backgroundColor: "#9CA3AF",
                  flexShrink: 0,
                }}
              />
              <span>{sentence}</span>
            </div>
          ))}
        </div>
      </div>
    );
  };

  const getAllSkusForExport = (): SkuItem[] => {
    return [
      ...(categorizedGrowth.top_80_skus || []),
      ...(categorizedGrowth.new_or_reviving_skus || []),
      ...(categorizedGrowth.other_skus || []),
    ];
  };


  type TableRow = Row & {
    __isTotal?: boolean;
  };


  const renderGrowthCell = (growth: any) => {
    // growth can be GrowthCategory {category,value} OR a number (for total row)
    if (growth == null) return "N/A";

    // Total row sends number; normal rows send {category,value}
    const isObj = typeof growth === "object" && "value" in growth;
    const val = Number(isObj ? growth.value : growth);

    if (!Number.isFinite(val)) return "N/A";

    const sign = val > 0 ? "+" : "";
    const text = `${sign}${val.toFixed(2)}%`;

    // classify like your UI:
    const category = isObj ? growth.category : val >= 5 ? "High Growth" : val < 0 ? "Negative Growth" : "Low Growth";

    if (category === "High Growth") {
      return (
        <span className="flex items-center justify-center gap-2 w-full font-semibold text-[#5EA68E]">
          <span className="w-4 flex justify-center shrink-0">
            <FaArrowUp size={12} />
          </span>
          <span className="tabular-nums inline-block w-[50px] 2xl:w-[60px] text-right">{text}</span>
        </span>
      );
    }

    if (category === "Negative Growth") {
      return (
        <span className="flex items-center justify-center gap-2 w-full font-semibold text-[#FF5C5C]">
          <span className="w-4 flex justify-center shrink-0">
            <FaArrowDown size={12} />
          </span>
          <span className="tabular-nums inline-block w-[50px] 2xl:w-[60px] text-right">{text}</span>
        </span>
      );
    }

    return (
      <span className="flex items-center justify-center gap-2 w-full font-semibold text-[#414042]">
        <span className="w-4 flex justify-center shrink-0">
          {val > 0 ? <FaArrowUp size={12} /> : val < 0 ? <FaArrowDown size={12} /> : null}
        </span>
        <span className="tabular-nums inline-block w-[50px] 2xl:w-[60px] text-right">{text}</span>
      </span>
    );
  };

  const formatCountryLabel = (country: string) => {
    const lower = country.toLowerCase();
    if (lower === "global") return "Global"; // special case
    return country.toUpperCase(); // UK, US, etc.
  };

  const currentYearStr = String(new Date().getFullYear());
  const currentMonthStr = pad2(new Date().getMonth() + 1); // "01".."12"
  const currentPeriodKey = `${currentYearStr}-${currentMonthStr}`;

  const isCurrentPeriodAvailable = availablePeriods.includes(currentPeriodKey);

  const isLockedCurrent = (year: string, month: string) => {
    if (!year || !month) return false;
    // Lock ONLY if backend has current month AND the option is current month
    return isCurrentPeriodAvailable && `${year}-${month}` === currentPeriodKey;
  };

  // Prevent selecting the same month for both periods
  useEffect(() => {
    if (year1 && year2 && year1 === year2 && month1 && month2 && month1 === month2) {
      // keep Month 1, reset Month 2
      setMonth2('');
    }
  }, [year1, year2, month1, month2]);

  return (
    <>
      <style>{`
  div{ font-family: 'Lato', sans-serif; }
  select{ outline: none; }
 
  .styled-button, .compare-button{
    padding:8px 16px;  border:none; border-radius:6px; cursor:pointer;
    transition:background-color .2s ease; box-shadow:0 3px 6px rgba(0,0,0,.15);
    background-color:#2c3e50; color:#f8edcf; font-weight:bold;
  }
  .styled-button:hover, .compare-button:hover{ background-color:#1f2a36; }
  .month-form{ max-width:100%; margin:15px 0; border:1px solid #e4e7ec ; padding:10px; background:#fff; }
  .month-tag{ font-size:12px; font-weight:bold; color:#414042; position:absolute; top:-25px; }
  .highlight{ color:#60a68e; }
  .subtitle{ margin-top:0; color:#414042; font-size:14px; }

  .month-row{
    display:flex;
    align-items:center;
    margin-top:20px;
    gap:10px;
  }
  .year-dropdown{
    margin-right:10px;
    padding:6px;
    font-size:14px;
    border-radius:4px;
    border:1px solid #ccc;
  }
  .month-slider{
    margin-top:30px;
    display:flex;
    flex-grow:1;
    justify-content:space-between;
    padding:0 10px;
    position:relative;
    border-top:2px solid #ccc;
  }
  .month-dot{
    display:flex;
    flex-direction:column;
    align-items:center;
    cursor:pointer;
    position:relative;
    top:-6px;
  }
  .month-dot .dot{
    width:12px;
    height:12px;
    background:#ccc;
    border-radius:50%;
    margin-bottom:4px;
  }


.introjs-progressbar {
    background-color: #5EA68E;
     }

  .month-dot.selected .dot{ background:#5EA68E; }

  .month-dot.selected .month-label{
    color:#5EA68E;
    font-weight:600;
  }

  .month-dot.disabled{
    opacity:0.3;
    cursor:not-allowed;
  }
  .month-dot.disabled .dot{
    background:#eee;
  }

  .month-label{
    font-size:12px;
    color:#414042;
    white-space:nowrap;
  }
  .month-label-short{ display:none; }   
  .month-label-full{ display:inline; }

  @keyframes pulseRing {
  0%   { transform: scale(1);   box-shadow: 0 0 0 0 rgba(94,166,142,.45); }
  70%  { transform: scale(1.05); box-shadow: 0 0 0 10px rgba(94,166,142,0); }
  100% { transform: scale(1);   box-shadow: 0 0 0 0 rgba(94,166,142,0); }
}

.month-slider.needs-pick .month-dot:not(.disabled):not(.selected) .dot {
  animation: pulseRing 1.3s infinite;
}

.month-help {
  font-size: 12px;
  color: #5EA68E;
  font-weight: 700;
  margin-top: 6px;
}

  /* ======= Responsive changes (under lg) ======= */
  @media (max-width: 1023.98px){
    .month-row{
      flex-direction:column;
      align-items:stretch;
    }
    .year-dropdown{
      width:100%;
      margin-right:0;
    }
    .month-slider{
      margin-top:20px;
      padding:0 4px;
    }
    .month-label{
      font-size:10px;
    }
    .month-label-full{ display:none; }   /* mobile: sirf 3 letter show */
    .month-label-short{ display:inline; }

    .month-tag{
      top:-20px;
      font-size:11px;
    }
  }

  .compare-button-container{ margin-top:20px; text-align:right; }
  .theadc{ background:#5EA68E; color:#f8edcf; }
  .tablec{ width:100%; border-collapse:collapse;  table-layout: fixed; }
  .tablec td, .tablec th{ border:1px solid #414042; padding:10px 8px; text-align:center;  white-space: nowrap;  text-overflow: ellipsis; vertical-align: middle; }
  .insight-section-title{  color:#414042; }
  .insight-list{ margin: 6px 0 10px 20px; padding:0; }
  .insight-list-item{ line-height:1.6; }
  .insight-paragraphs p{ margin:4px 0; line-height:1.6; }

  .table-wrapper{
  width:100%;
  overflow-x:auto;
  -webkit-overflow-scrolling: touch;
}



.tour-overlay{
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,.35);
  z-index: 9998;        /* 🔥 sidebar se upar */
}

.tour-box{
  position: fixed;     /* 🔥 absolute ❌ → fixed ✅ */
  background: #ffffff;
  padding: 14px 16px;
  border-radius: 10px;
  width: 260px;
  box-shadow: 0 12px 40px rgba(0,0,0,.3);
  z-index: 9999;       /* 🔥 overlay se bhi upar */
}

.tour-actions{
  display: flex;
  justify-content: space-between;
  margin-top: 10px;
}

.tour-actions button{
  padding: 6px 12px;
  border-radius: 6px;
  border: none;
  cursor: pointer;
  background: #5EA68E;
  color: #fff;
  font-size: 13px;
}

.month-dot.selected .dot {
  background: #5EA68E;
  box-shadow: 0 0 0 6px rgba(22,163,74,0.25);
}

`}</style>

      <div className="w-full font-[Lato,sans-serif]">
        <div className="sticky top-0 z-40 flex w-full flex-col gap-1 border-b border-gray-200 bg-[#F7F7F7] sm:flex-row sm:gap-4 md:items-center md:justify-between">
          <div className="mb-3">
            {/* <h2 className="text-[18px] font-bold text-[#414042] 2xl:text-2xl">
              Business Insights - AI Analyst&nbsp;-
              <span className="pl-1 text-[#5EA68E]">
                Amazon {effectiveCountry && formatCountryLabel(effectiveCountry)}
                <span className="px-2 text-[#5EA68E]"></span>
              </span>
            </h2> */}
            <PageBreadcrumb
              variant="page"
              align="left"
              textSize="2xl"
              pageTitle={
                <div className="flex flex-wrap items-baseline gap-2">
                  <span className="text-[#414042] font-bold">
                    Business Insights - AI Analyst&nbsp;- Amazon
                  </span>
                  <span className="text-green-500 font-bold">
                    {countryName?.toUpperCase()}
                  </span>
                </div>
              }
            />
            <p>
              <i className="text-xs 2xl:text-sm">
                Select the year and month for both periods to compare growth metrics.
              </i>
            </p>
          </div>
        </div>

        <PreviewLockedSection
          enabled={isUsingDummyData}
          title="Preview Mode"
          description="To view your real business data and analytics, please complete your profile and connect your Amazon account. This will unlock your performance dashboard and insights."
          buttonText="Complete Setup"
          onAction={handleConnectAmazonPreview}
        >

          <form onSubmit={handleSubmit} className="month-form ">
            {/* Row 1 */}
            <div className="month-row">
              <select id="intro-year1" value={year1} ref={year1Ref} onChange={(e) => setYear1(e.target.value)} className="year-dropdown">
                <option value="">Year 1</option>
                {years.map(y => <option key={y} value={y}>{y}</option>)}
              </select>
              <div id="intro-month1" ref={month1Ref} className={`month-slider ${year1 && !month1 ? 'needs-pick' : ''}`}>
                {months.map(m => {
                  const disabled =
                    !year1 ||
                    !isPeriodAvailable(year1, m.value) ||
                    isLockedCurrent(year1, m.value) ||
                    (year2 && year1 === year2 && month2 === m.value);
                  const selected = month1 === m.value;
                  return (
                    <div
                      key={m.value}

                      className={`month-dot  ${selected ? 'selected' : ''} ${disabled ? 'disabled' : ''}`}
                      onClick={() => {
                        if (disabled) return;
                        setMonth1(m.value);
                      }}
                    >
                      {selected && !disabled && <div className="month-tag text-nowrap"></div>}
                      <span className="dot"></span>
                      <div className="month-label">
                        <span className="month-label-full">{m.label}</span>
                        <span className="month-label-short">{m.label.slice(0, 3)}</span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
            {/* Row 2 */}
            <div className="month-row">
              <select value={year2} onChange={(e) => setYear2(e.target.value)} className="year-dropdown">
                <option value="">Year 2</option>
                {years.map(y => <option key={y} value={y}>{y}</option>)}
              </select>
              <div className={`month-slider ${year2 && !month2 ? 'needs-pick' : ''}`}>
                {months.map(m => {
                  const disabled =
                    !year2 ||
                    !isPeriodAvailable(year2, m.value) ||
                    isLockedCurrent(year2, m.value) ||
                    (year1 && year1 === year2 && month1 === m.value);
                  const selected = month2 === m.value;
                  return (
                    <div
                      key={m.value}
                      className={`month-dot ${selected ? 'selected' : ''} ${disabled ? 'disabled' : ''}`}
                      onClick={() => {
                        if (disabled) return;
                        setMonth2(m.value);
                      }}
                    >
                      {selected && !disabled && <div className="month-tag text-nowrap"></div>}
                      <span className="dot"></span>
                      <div className="month-label">
                        <span className="month-label-full">{m.label}</span>
                        <span className="month-label-short">{m.label.slice(0, 3)}</span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </form>

          <div className="mt-5 text-right">
            <button
              id="intro-compare"
              type="submit"
              onClick={(e) => {
                handleSubmit(e);
              }}
              ref={compareBtnRef}
              className="rounded-md bg-[#2c3e50] px-4 py-2 text-xs font-bold text-[#f8edcf] shadow-[0_3px_6px_rgba(0,0,0,.15)] transition-colors duration-200 ease-in-out hover:bg-[#1f2a36] 2xl:text-sm"
            >
              Compare
            </button>
          </div>

          {error && <p className="text-red-500">{error}</p>}

          <div className="mt-4 mb-3 w-full rounded-xl border border-gray-200 bg-white p-4">
            <div className="text-[18px] font-bold text-[#414042] 2xl:text-2xl">
              Profitability
            </div>

            <div className="mx-auto mt-3 flex w-full flex-wrap items-center justify-center gap-3 transition-opacity duration-300 sm:mt-4 sm:gap-4 md:gap-5">
              {[
                { key: "netSales", label: "Net Sales", color: "#75bbda" },
                { key: "cm1Profit", label: "CM1 Profit", color: "#7b9a6d" },
                { key: "otherExpense", label: "Other Expense", color: "#3A8EA4" },
                { key: "advertising", label: "Advertising Total", color: "#C49466" },
                { key: "reimbursement", label: "Reimbursement", color: "#FDD36F" },
              ].map(({ key, label, color }) => {
                const isChecked = !!selectedTotals[key];

                return (
                  <label
                    key={key}
                    className={[
                      "flex shrink-0 cursor-pointer select-none items-center gap-1 whitespace-nowrap font-semibold text-[10px] text-charcoal-500 sm:gap-1.5 2xl:text-xs",
                      isChecked ? "opacity-100" : "opacity-40",
                    ].join(" ")}
                  >
                    <span
                      className="flex h-3 w-3 items-center justify-center rounded-sm border transition sm:h-3.5 sm:w-3.5"
                      style={{
                        borderColor: color,
                        backgroundColor: isChecked ? color : "white",
                      }}
                      onClick={() => toggleTotalsMetric(key)}
                    >
                      {isChecked && (
                        <svg viewBox="0 0 24 24" width="14" height="14" className="text-white">
                          <path
                            fill="currentColor"
                            d="M20.285 6.709a1 1 0 0 0-1.414-1.414L9 15.168l-3.879-3.88a1 1 0 0 0-1.414 1.415l4.586 4.586a1 1 0 0 0 1.414 0l10-10Z"
                          />
                        </svg>
                      )}
                    </span>

                    <span className="capitalize">{label}</span>
                  </label>
                );
              })}
            </div>

            <div className="mt-2 h-[320px] w-full sm:mt-3 sm:h-[360px] md:h-[400px] lg:h-[420px]">
              <Line data={totalsLine.data as any} options={totalsLine.options as any} />
            </div>
          </div>

          {/* <div className="mt-4 mb-3 rounded-xl border border-gray-200 bg-white p-4">
            <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
              <div>
                <PageBreadcrumb
                  pageTitle="Units Sold"
                  variant="page"
                  align="left"
                  textSize="2xl"
                />
                <div ref={unitsChartRef} className="h-[320px] w-full" />
              </div>

              <div>
                <PageBreadcrumb
                  pageTitle="Net Sales"
                  variant="page"
                  align="left"
                  textSize="2xl"
                />
                <div ref={chartRef} className="h-[320px] w-full" />
              </div>

              <div className="mt-3">
                <PageBreadcrumb
                  pageTitle="CM1 Profit"
                  variant="page"
                  align="left"
                  textSize="2xl"
                />
                <div ref={profitChartRef} className="h-[320px] w-full" />
              </div>

              <div className="mt-3">
                <PageBreadcrumb
                  pageTitle="Average Selling Price"
                  variant="page"
                  align="left"
                  textSize="2xl"
                />
                <div ref={aspChartRef} className="h-[320px] w-full" />
              </div>
            </div>

            <div className="mt-3 flex flex-wrap justify-center gap-4 text-[10px] font-semibold text-[#414042] 2xl:text-xs">
              <span className="inline-flex items-center gap-2">
                <span className="inline-block h-[10px] w-[10px] bg-[#ED9F50]" />
                Top 80%
              </span>

              <span className="inline-flex items-center gap-2">
                <span className="inline-block h-[10px] w-[10px] bg-[#3A8EA4]" />
                Other SKUs
              </span>

              <span className="inline-flex items-center gap-2">
                <span className="inline-block h-[10px] w-[10px] bg-[#7B9A6D]" />
                New/Reviving
              </span>
            </div>
          </div> */}

          <GrowthCharts
            unitsChartRef={unitsChartRef}
            chartRef={chartRef}
            profitChartRef={profitChartRef}
            aspChartRef={aspChartRef}
          />

          {/* <SkuAnalysisSection
            categorizedGrowth={categorizedGrowth}
            month1={month1}
            year1={year1}
            month2={month2}
            year2={year2}
            month2Label={month2Label}
            skuInsights={skuInsights}
            loadingInsight={loadingInsight}
            analyzeSkus={analyzeSkus}
            exportToExcel={exportToExcel}
            getAllSkusForExport={getAllSkusForExport}
            getAbbr={getAbbr}
            getInsightForItem={getInsightForItem}
            setSelectedSku={setSelectedSku}
            setModalOpen={setModalOpen}
            setFbType={setFbType}
            setFbText={setFbText}
            setFbSuccess={setFbSuccess}
            isPreviewMode={isPreviewMode}
          /> */}

          <SkuAnalysisSection
            categorizedGrowth={categorizedGrowth}
            month1={month1}
            year1={year1}
            month2={month2}
            year2={year2}
            month2Label={month2Label}
            countryName={countryName}
            isGlobalData={isGlobalData}
            exportToExcel={exportToExcel}
            getAllSkusForExport={getAllSkusForExport}
            getAbbr={getAbbr}
            isPreviewMode={isPreviewMode}
          />
        </PreviewLockedSection>
      </div>
    </>
  );
};

export default MonthsforBI;



