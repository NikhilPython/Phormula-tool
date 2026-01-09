"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSelector } from "react-redux";
import ExcelJS from "exceljs";
import { saveAs } from "file-saver";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import Loader from "@/components/loader/Loader";
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
import SegmentedToggle from "@/components/ui/SegmentedToggle";
import DashboardBargraphCard from "@/components/dashboard/DashboardBargraphCard";
import SalesTargetCard from "@/components/dashboard/SalesTargetCard";
import SalesTargetStatsCard from "@/components/dashboard/SalesTargetStatsCard";
import AmazonStatCard from "@/components/dashboard/AmazonStatCard";
import CurrentInventorySection from "@/components/dashboard/CurrentInventorySection";
import { RootState } from "@/lib/store";
import { useAmazonConnections } from "@/lib/utils/useAmazonConnections";
import HashScroll from "@/components/common/HashScroll";
import {
  getISTYearMonth,
  getPrevISTYearMonth,
  getPrevMonthShortLabel,
  getISTDayInfo,
} from "@/lib/dashboard/date";

import {
  fmtGBP,
  fmtUSD,
  fmtNum,
  fmtPct,
  fmtInt,
  toNumberSafe,
} from "@/lib/dashboard/format";

import type { RegionKey, RegionMetrics } from "@/lib/dashboard/types";

import { useGetUserDataQuery } from "@/lib/api/profileApi";
import { usePlatform } from "@/components/context/PlatformContext";
import type { PlatformId } from "@/lib/utils/platforms";
import LiveBiLineGraph from "@/components/businessInsight/LiveBiLineChartPanel";

// ✅ moved range picker deps here
import { DateRange } from "react-date-range";
import "react-date-range/dist/styles.css";
import "react-date-range/dist/theme/default.css";
import { FaCalendarAlt } from "react-icons/fa";
import MonthsforBI from "@/app/(admin)/live-business-insight/[ranged]/[countryName]/[month]/[year]/page";

type CurrencyCode = "USD" | "GBP" | "INR" | "CAD";

/* ===================== ENV & ENDPOINTS ===================== */
const baseURL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

// const API_URL = `${baseURL}/amazon_api/orders`;
const FIN_MTD_TX_ENDPOINT = `${baseURL}/amazon_api/finances/mtd_transactions`;
const SHOPIFY_DROPDOWN_ENDPOINT = `${baseURL}/shopify/dropdown`;
// const FX_ENDPOINT = `${baseURL}/currency-rate`;

// ✅ BI endpoint (same one your graph uses)
const LIVE_MTD_BI_ENDPOINT = `${baseURL}/live_mtd_bi`;

/** 💵 FX defaults (used until backend answers) */
const GBP_TO_USD_ENV = Number(process.env.NEXT_PUBLIC_GBP_TO_USD || "1.25");
const INR_TO_USD_ENV = Number(process.env.NEXT_PUBLIC_INR_TO_USD || "0.01128");
const CAD_TO_USD_ENV = Number(process.env.NEXT_PUBLIC_CAD_TO_USD || "0.74");

const USE_MANUAL_LAST_MONTH =
  (process.env.NEXT_PUBLIC_USE_MANUAL_LAST_MONTH || "false").toLowerCase() ===
  "true";

const MANUAL_LAST_MONTH_USD_GLOBAL = Number(
  process.env.NEXT_PUBLIC_MANUAL_LAST_MONTH_USD_GLOBAL || "0"
);
const MANUAL_LAST_MONTH_USD_UK = Number(
  process.env.NEXT_PUBLIC_MANUAL_LAST_MONTH_USD_UK || "0"
);
const MANUAL_LAST_MONTH_USD_US = Number(
  process.env.NEXT_PUBLIC_MANUAL_LAST_MONTH_USD_US || "0"
);
const MANUAL_LAST_MONTH_USD_CA = Number(
  process.env.NEXT_PUBLIC_MANUAL_LAST_MONTH_USD_CA || "0"
);

/* ===================== BI TYPES (for shared cards + graph) ===================== */
type ChartMetric = "net_sales" | "quantity";

type DailyPoint = {
  date: string;
  quantity?: number;
  net_sales?: number;
  product_sales?: number;
  profit?: number;
  cm2_profit?: number; // ✅ add
};


type DailySeries = {
  previous: DailyPoint[];
  current_mtd: DailyPoint[];
};

type PeriodInfo = {
  label: string;
  start_date: string;
  end_date: string;
};

type BiApiResponse = {
  message?: string;
  periods?: {
    previous?: PeriodInfo;
    current_mtd?: PeriodInfo;
  };
  daily_series?: DailySeries;

  aligned_totals?: BiAlignedTotals;

  categorized_growth?: any;
  insights?: Record<string, any>;
  ai_insights?: Record<string, any>;
  overall_summary?: string[];
  overall_actions?: string[];
};


type BiAlignedTotals = {
  // existing fields you already have...
  current_cm2_profit?: number;
  previous_cm2_profit?: number;
  total_current_profit_percentage?: number;
  total_previous_profit_percentage?: number;

  total_previous_net_sales_full_month?: number;
  total_previous_net_sales?: number;
  total_current_net_sales?: number;

  // ✅ NEW (from your API response)
  total_current_advertising?: number;
  total_previous_advertising?: number;

  total_current_platform_fees?: number;
  total_previous_platform_fees?: number;

  total_current_profit?: number;
  total_previous_profit?: number;
};


/* ===================== SMALL HELPERS ===================== */
const getShort = (label?: string) => (label ? label.split(" ")[0] || label : "");

const currencyForCountry = (countryName: string): CurrencyCode => {
  const c = (countryName || "").toLowerCase();
  if (c === "uk") return "GBP";
  if (c === "us") return "USD";
  if (c === "ca") return "CAD";
  // fallback (if you ever use india/shopify here)
  if (c === "india") return "INR";
  return "USD";
};

const safeDeltaPctFromPct = (currentPct: number, previousPct: number) => {
  const c = Number(currentPct) || 0;
  const p = Number(previousPct) || 0;
  if (!p) return null;
  return ((c - p) / Math.abs(p)) * 100;
};

const fmtPct2 = (v: number) => `${(Number(v) || 0).toFixed(2)}%`;


/* ===================== RANGE PICKER (moved above graph) ===================== */
function RangePicker({
  selectedStartDay,
  selectedEndDay,
  onSubmit,
  onClear,
  onCloseReset,
}: {
  selectedStartDay: number | null;
  selectedEndDay: number | null;
  onSubmit: (s: number | null, e: number | null) => void;
  onClear: () => void;
  onCloseReset: () => void;
}) {

  // ✅ LOCK CALENDAR TO CURRENT MONTH ONLY
  const today = new Date();
  const monthStart = new Date(today.getFullYear(), today.getMonth(), 1);
  const monthEnd = new Date(today.getFullYear(), today.getMonth() + 1, 0);

  const [shownDate, setShownDate] = useState<Date>(monthStart);

  const [showCalendar, setShowCalendar] = useState(false);

  const [calendarRange, setCalendarRange] = useState<any>([
    { startDate: null, endDate: null, key: "selection" },
  ]);

  const [pendingStartDay, setPendingStartDay] = useState<number | null>(null);
  const [pendingEndDay, setPendingEndDay] = useState<number | null>(null);

  const handleCalendarChange = (ranges: any) => {
    const range = ranges.selection;
    setCalendarRange([range]);

    if (range.startDate && range.endDate) {
      setPendingStartDay(range.startDate.getDate());
      setPendingEndDay(range.endDate.getDate());
    } else {
      setPendingStartDay(null);
      setPendingEndDay(null);
    }
  };

  const applyRange = () => {
    onSubmit(pendingStartDay, pendingEndDay);
    setShowCalendar(false);
  };

  const clearRange = () => {
    setCalendarRange([{ startDate: null, endDate: null, key: "selection" }]);
    setPendingStartDay(null);
    setPendingEndDay(null);
    onClear();
  };

  const closeAndReset = () => {
    setCalendarRange([{ startDate: null, endDate: null, key: "selection" }]);
    setPendingStartDay(null);
    setPendingEndDay(null);
    setShowCalendar(false);
    onCloseReset();
  };

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setShowCalendar((s) => !s)}
        className="flex items-center gap-2 text-xs 2xl:text-sm"
        style={{
          padding: "6px 10px",
          borderRadius: 8,
          border: "1px solid #D9D9D9E5",
          backgroundColor: "#ffffff",
          // fontSize: 12,
        }}
      >
        <FaCalendarAlt className="text-sm 2xl:text-md" />
        {selectedStartDay && selectedEndDay
          ? `Day ${selectedStartDay} – ${selectedEndDay}`
          : "Select Date Range"}
      </button>

      {showCalendar && (
        <div
          style={{
            position: "absolute",
            right: 0,
            top: "110%",
            zIndex: 50,
            backgroundColor: "#ffffff",
            boxShadow: "0 4px 12px rgba(0,0,0,0.15)",
            padding: 8,
            borderRadius: 8,
            minWidth: 320,
          }}
        >
          {/* <DateRange
            ranges={calendarRange}
            onChange={handleCalendarChange}
            moveRangeOnFirstSelection={false}
            showMonthAndYearPickers={false}
            rangeColors={["#5EA68E"]}
          /> */}

          <DateRange
            ranges={calendarRange}
            onChange={handleCalendarChange}
            moveRangeOnFirstSelection={false}
            showMonthAndYearPickers={false}
            rangeColors={["#5EA68E"]}

            // ✅ Only allow selecting dates from current month
            minDate={monthStart}
            maxDate={monthEnd}

            // ✅ Always show current month (prevents switching)
            shownDate={shownDate}
            onShownDateChange={() => {
              // snap back to the same month even if user tries to navigate
              setShownDate(monthStart);
            }}
          />
          <style jsx global>{`
  /* Remove left/right month navigation arrows */
  .rdrNextPrevButton {
    display: none !important;
  }
`}</style>


          <div className="flex justify-between mt-2 gap-2">
            <button
              type="button"
              onClick={clearRange}
              className="text-xs px-2 py-1 border rounded"
            >
              Clear
            </button>

            <div className="flex gap-2">
              <button
                type="button"
                onClick={applyRange}
                disabled={pendingStartDay == null || pendingEndDay == null}
                className="text-xs px-2 py-1 rounded text-yellow-200"
                style={{
                  background: "#37455F",
                  opacity: pendingStartDay == null ? 0.6 : 1,
                }}
              >
                Submit
              </button>
              <button
                type="button"
                onClick={closeAndReset}
                className="text-xs px-2 py-1 rounded text-charcoal-500 border border-charcoal-500"
              // style={{ background: "#5EA68E" }}
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}



const sliceByDayRange = (
  points: DailyPoint[] = [],
  startDay: number | null,
  endDay: number | null
) => {
  if (startDay == null || endDay == null) return points;

  const s = Math.min(startDay, endDay);
  const e = Math.max(startDay, endDay);

  return points.filter((p) => {
    const day = Number(p.date?.slice(8, 10)); // "YYYY-MM-DD" -> DD
    return day >= s && day <= e;
  });
};


export default function DashboardPage() {

  const { platform } = usePlatform();
  const { data: userData } = useGetUserDataQuery();
  const isCountryMode = platform !== "global" && platform !== "shopify";

  const countryName = useMemo(() => {
    switch (platform) {
      case "amazon-uk":
        return "uk";
      case "amazon-us":
        return "us";
      case "amazon-ca":
        return "ca";
      default:
        return "global";
    }
  }, [platform]);

  const showLiveBI = isCountryMode || platform === "global";


  const brandName = useSelector(
    (state: RootState) => state.auth.user?.brand_name
  );

  const biCountryName = useMemo(() => {
    if (platform === "global") return "uk";
    return countryName;
  }, [platform, countryName]);

  const biDataCurrency = useMemo(() => currencyForCountry(biCountryName), [biCountryName]);

  const biSourceCurrency: CurrencyCode = useMemo(
    () => currencyForCountry(biCountryName),
    [biCountryName]
  );

  const amazonDataCurrency: CurrencyCode = useMemo(() => {
    // your fetchAmazon uses UK when platform is "global"
    if (platform === "amazon-us") return "USD";
    if (platform === "amazon-ca") return "CAD";
    return "GBP"; // amazon-uk OR global default
  }, [platform]);




  /* ===================== PLATFORM → DISPLAY CURRENCY ===================== */
  const profileHomeCurrency = ((userData?.homeCurrency || "USD").toUpperCase() as CurrencyCode);

  const displayCurrency: CurrencyCode = useMemo(() => {
    switch (platform as PlatformId) {
      case "global":
        return profileHomeCurrency;
      case "amazon-uk":
        return "GBP";
      case "amazon-us":
        return "USD";
      case "amazon-ca":
        return "CAD";
      case "shopify":
        return "INR";
      default:
        return profileHomeCurrency;
    }
  }, [platform, profileHomeCurrency]);

  /* ===================== AMAZON / SHOPIFY STATE ===================== */
  const [loading, setLoading] = useState(false);
  const [unauthorized, setUnauthorized] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<any>(null);

  const { connections: amazonConnections } = useAmazonConnections();

  // Shopify (current month)
  const [shopifyLoading, setShopifyLoading] = useState(false);
  const [shopifyError, setShopifyError] = useState<string | null>(null);
  const [shopifyRows, setShopifyRows] = useState<any[]>([]);
  const shopify = shopifyRows?.[0] || null;

  // Shopify (previous month)
  const [shopifyPrevRows, setShopifyPrevRows] = useState<any[]>([]);

  // Shopify store info
  const [shopifyStore, setShopifyStore] = useState<any | null>(null);

  // which region tab is selected in the Amazon card
  const [amazonRegion, setAmazonRegion] = useState<RegionKey>("Global");

  // which region is selected in the P&L graph
  const [graphRegion, setGraphRegion] = useState<RegionKey>("Global");


  const chartRef = React.useRef<HTMLDivElement | null>(null);
  const prevLabel = useMemo(() => getPrevMonthShortLabel(), []);

  // ✅ put near other helpers
  const getDayOfMonthIST = () => {
    const now = new Date();
    const ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
    return ist.getDate(); // 1..31
  };

  // ✅ add state to "lock" today's sales (in BI currency)
  const [todaySalesRaw, setTodaySalesRaw] = useState<number>(0);


  /* ===================== ✅ SHARED RANGE STATE (PARENT) ===================== */
  const [selectedStartDay, setSelectedStartDay] = useState<number | null>(null);
  const [selectedEndDay, setSelectedEndDay] = useState<number | null>(null);

  const [biLoading, setBiLoading] = useState(false);
  const [biError, setBiError] = useState<string | null>(null);
  const [biDailySeries, setBiDailySeries] = useState<DailySeries | null>(null);
  const [biPeriods, setBiPeriods] = useState<BiApiResponse["periods"] | null>(null);
  const [liveBiPayload, setLiveBiPayload] = useState<BiApiResponse | null>(null);
  const [biAlignedTotals, setBiAlignedTotals] = useState<BiAlignedTotals | null>(null);

  /* ===================== FX RATES ===================== */
  const [gbpToUsd, setGbpToUsd] = useState(GBP_TO_USD_ENV);
  const [inrToUsd, setInrToUsd] = useState(INR_TO_USD_ENV);
  const [cadToUsd, setCadToUsd] = useState(CAD_TO_USD_ENV);
  const [fxLoading, setFxLoading] = useState(false);

  const fetchFxRates = useCallback(async () => {
    try {
      setFxLoading(true);

      const token =
        typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

      const headers: HeadersInit = {
        "Content-Type": "application/json",
        Accept: "application/json",
      };
      if (token) (headers as any).Authorization = `Bearer ${token}`;

      const { monthName, year } = getISTYearMonth();
      const month = monthName.toLowerCase();

      const commonBody = {
        month,
        year,
        fetch_if_missing: true,
        seed_all: true,
      };

      const [ukRes, inrRes, cadRes] = await Promise.all([
        fetch(FX_ENDPOINT, {
          method: "POST",
          headers,
          body: JSON.stringify({
            ...commonBody,
            user_currency: "gbp",
            country: "uk",
            selected_currency: "usd",
          }),
        }),
        fetch(FX_ENDPOINT, {
          method: "POST",
          headers,
          body: JSON.stringify({
            ...commonBody,
            user_currency: "inr",
            country: "india",
            selected_currency: "usd",
          }),
        }),
        fetch(FX_ENDPOINT, {
          method: "POST",
          headers,
          body: JSON.stringify({
            ...commonBody,
            user_currency: "cad",
            country: "ca",
            selected_currency: "usd",
          }),
        }),
      ]);

      if (ukRes.ok) {
        const json = await ukRes.json();
        console.log("💱 GBP → USD FX response:", json);

        const rate = json?.record?.conversion_rate;
        if (json?.success && rate != null) {
          setGbpToUsd(Number(rate));
          console.log("✅ GBP → USD rate used:", Number(rate));
        }
      }

      if (inrRes.ok) {
        const json = await inrRes.json();
        console.log("💱 INR → USD FX response:", json);

        const rate = json?.record?.conversion_rate;
        if (json?.success && rate != null) {
          setInrToUsd(Number(rate));
          console.log("✅ INR → USD rate used:", Number(rate));
        }
      }

      if (cadRes.ok) {
        const json = await cadRes.json();
        console.log("💱 CAD → USD FX response:", json);

        const rate = json?.record?.conversion_rate;
        if (json?.success && rate != null) {
          setCadToUsd(Number(rate));
          console.log("✅ CAD → USD rate used:", Number(rate));
        }
      }
    } catch (err) {
      console.error("Failed to fetch FX rates", err);
    } finally {
      setFxLoading(false);
    }
  }, []);

  useEffect(() => {
    console.log("📊 FINAL FX RATES IN USE", {
      GBP_TO_USD: gbpToUsd,
      INR_TO_USD: inrToUsd,
      CAD_TO_USD: cadToUsd,
      displayCurrency,
    });
  }, [gbpToUsd, inrToUsd, cadToUsd, displayCurrency]);


  useEffect(() => {
    fetchFxRates();
  }, [fetchFxRates]);

  const forcedRegion: RegionKey = useMemo(() => {
    switch (platform) {
      case "amazon-uk":
        return "UK";
      case "amazon-us":
        return "US";
      case "amazon-ca":
        return "CA";
      default:
        return "Global";
    }
  }, [platform]);

  const graphRegionToUse: RegionKey = isCountryMode ? forcedRegion : graphRegion;

  useEffect(() => {
    if (!isCountryMode) return;
    setGraphRegion(forcedRegion);
    setAmazonRegion(forcedRegion);
  }, [isCountryMode, forcedRegion]);

  // ✅ which region is selected in the Sales Target card
  const [targetRegion, setTargetRegion] = useState<RegionKey>(
    isCountryMode ? forcedRegion : "Global"
  );

  useEffect(() => {
    if (isCountryMode) setTargetRegion(forcedRegion);
  }, [isCountryMode, forcedRegion]);


  /* ===================== CONVERSION + FORMATTING (DISPLAY CURRENCY) ===================== */
  const convertToDisplayCurrency = useCallback(
    (value: number | null | undefined, from: CurrencyCode) => {
      const n = toNumberSafe(value ?? 0);
      if (!n) return 0;

      // from -> USD
      let usd = n;
      if (from === "GBP") usd = n * gbpToUsd;
      if (from === "INR") usd = n * inrToUsd;
      if (from === "CAD") usd = n * cadToUsd;

      // USD -> displayCurrency
      if (displayCurrency === "USD") return usd;
      if (displayCurrency === "GBP") return gbpToUsd ? usd / gbpToUsd : usd;
      if (displayCurrency === "INR") return inrToUsd ? usd / inrToUsd : usd;
      if (displayCurrency === "CAD") return cadToUsd ? usd / cadToUsd : usd;

      return usd;
    },
    [displayCurrency, gbpToUsd, inrToUsd, cadToUsd]
  );

  const userMonthlyTargetGBP = useMemo(() => {
    return toNumberSafe(userData?.target_sales ?? 0);
  }, [userData?.target_sales]);

  const userMonthlyTargetHome = useMemo(() => {
    if (!userMonthlyTargetGBP) return 0;
    return convertToDisplayCurrency(userMonthlyTargetGBP, "GBP");
  }, [userMonthlyTargetGBP, convertToDisplayCurrency]);


  const prevFullMonthNetSalesDisp = useMemo(() => {
    const v = liveBiPayload?.aligned_totals?.total_previous_net_sales_full_month;
    if (v == null) return 0;
    return convertToDisplayCurrency(Number(v) || 0, biSourceCurrency);
  }, [liveBiPayload, convertToDisplayCurrency, biSourceCurrency]);

  /* ===================== INTEGRATION FLAGS ===================== */
  const shopifyDeriv = useMemo(() => {
    if (!shopify) return null;
    const totalOrders = toNumberSafe(shopify.total_orders);
    const netSales = toNumberSafe(shopify.net_sales);
    return { totalOrders, netSales };
  }, [shopify]);

  const shopifyPrevDeriv = useMemo(() => {
    const row = shopifyPrevRows?.[0];
    if (!row) return null;
    const netSales = toNumberSafe(row.net_sales);
    const totalOrders = toNumberSafe(row.total_orders);
    return { netSales, totalOrders };
  }, [shopifyPrevRows]);

  // ✅ Global FULL month target = Amazon(previous full month from BI) + Shopify(previous month total)
  const globalPrevFullMonthNetSalesDisp = useMemo(() => {
    const amazonFull = prevFullMonthNetSalesDisp; // already in display currency
    const shopifyFull = convertToDisplayCurrency(shopifyPrevDeriv?.netSales ?? 0, "INR");
    return amazonFull + shopifyFull;
  }, [prevFullMonthNetSalesDisp, shopifyPrevDeriv?.netSales, convertToDisplayCurrency]);


  const formatDisplayAmount = useCallback(
    (value: number | null | undefined) => {
      const n = toNumberSafe(value ?? 0);

      switch (displayCurrency) {
        case "USD":
          return fmtUSD(n);
        case "GBP":
          return fmtGBP(n);
        case "CAD":
          return new Intl.NumberFormat("en-CA", {
            style: "currency",
            currency: "CAD",
          }).format(n);
        case "INR":
          return new Intl.NumberFormat("en-IN", {
            style: "currency",
            currency: "INR",
          }).format(n);
        default:
          return fmtNum(n);
      }
    },
    [displayCurrency]
  );




  const formatDisplayK = useCallback(
    (value: number | null | undefined) => {
      const n = toNumberSafe(value ?? 0);
      const abs = Math.abs(n);
      const isK = abs >= 1000;

      const displayVal = isK ? n / 1000 : n;
      const suffix = isK ? "k" : "";

      return `${formatDisplayAmount(displayVal)}${suffix}`;
    },
    [formatDisplayAmount]
  );

  const currencySymbol =
    displayCurrency === "USD"
      ? "$"
      : displayCurrency === "GBP"
        ? "£"
        : displayCurrency === "CAD"
          ? "CA$"
          : displayCurrency === "INR"
            ? "₹"
            : "¤";

  const biDailySeriesHome = useMemo(() => {
    if (!biDailySeries) return null;

    const convPoint = (p: DailyPoint): DailyPoint => ({
      ...p,
      net_sales: p.net_sales != null ? convertToDisplayCurrency(p.net_sales, biDataCurrency) : p.net_sales,
      product_sales: p.product_sales != null ? convertToDisplayCurrency(p.product_sales, biDataCurrency) : p.product_sales,
      profit: p.profit != null ? convertToDisplayCurrency(p.profit, biDataCurrency) : p.profit,
      cm2_profit: p.cm2_profit != null ? convertToDisplayCurrency(p.cm2_profit, biDataCurrency) : p.cm2_profit,
    });

    return {
      previous: (biDailySeries.previous || []).map(convPoint),
      current_mtd: (biDailySeries.current_mtd || []).map(convPoint),
    };
  }, [biDailySeries, convertToDisplayCurrency, biDataCurrency]);


  /* ===================== AMAZON FETCH ===================== */
  const fetchAmazon = useCallback(async () => {
    setLoading(true);
    setUnauthorized(false);
    setError(null);

    try {
      const token =
        typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

      if (!token) {
        setUnauthorized(true);
        throw new Error("No token found. Please sign in.");
      }

      // ✅ decide country from platform
      const uiCountry =
        platform === "amazon-us" ? "us" : platform === "amazon-ca" ? "ca" : "uk";

      // ✅ marketplace id (fallback to UK one you provided)
      const marketplaceId =
        (amazonConnections?.find?.((c: any) => (c?.country || "").toLowerCase() === uiCountry)
          ?.marketplace_id) ||
        (uiCountry === "uk"
          ? "A1F83G8C2ARO7P"
          : uiCountry === "us"
            ? "ATVPDKIKX0DER"
            : uiCountry === "ca"
              ? "A2EUQ1WTGCTBG2"
              : "A1F83G8C2ARO7P");

      const params = new URLSearchParams({
        marketplace_id: marketplaceId,
        store_in_db: "true",
        country: uiCountry,
      });

      const url = `${FIN_MTD_TX_ENDPOINT}?${params.toString()}`;

      const res = await fetch(url, {
        method: "GET",
        headers: {
          Accept: "application/json",
          Authorization: `Bearer ${token}`,
        },
        credentials: "omit",
      });

      if (res.status === 401) {
        setUnauthorized(true);
        throw new Error("Unauthorized — token missing/invalid/expired.");
      }

      const json = await res.json();
      if (!res.ok || json?.success === false) {
        throw new Error(json?.error || `Request failed: ${res.status}`);
      }

      setData(json); // ✅ data now matches your new response shape
    } catch (e: any) {
      setError(e?.message || "Failed to load data");
      setData(null);
    } finally {
      setLoading(false);
    }
  }, [platform, amazonConnections]);


  /* ===================== SHOPIFY STORE INFO ===================== */
  useEffect(() => {
    const fetchShopifyStore = async () => {
      try {
        const token =
          typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
        if (!token) return;

        const res = await fetch(`${baseURL}/shopify/store`, {
          headers: { Authorization: `Bearer ${token}` },
        });

        const ct = res.headers.get("content-type") || "";
        if (!ct.includes("application/json")) return;

        const d = await res.json();
        if (!res.ok || d?.error) return;

        setShopifyStore(d);
      } catch (err) {
        console.error("Error fetching Shopify store in Dashboard:", err);
      }
    };
    fetchShopifyStore();
  }, []);

  /* ===================== SHOPIFY CURRENT MONTH ===================== */
  const fetchShopify = useCallback(async () => {
    setShopifyLoading(true);
    setShopifyError(null);
    try {
      const user_token =
        typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
      if (!user_token) throw new Error("No token found. Please sign in.");

      if (!shopifyStore?.shop_name || !shopifyStore?.access_token) {
        throw new Error("Shopify store not connected.");
      }

      const { monthName, year } = getISTYearMonth();

      const params = new URLSearchParams({
        range: "monthly",
        month: monthName.toLowerCase(),
        year: String(year),
        user_token,
        shop: shopifyStore.shop_name,
        token: shopifyStore.access_token,
      });

      const url = `${SHOPIFY_DROPDOWN_ENDPOINT}?${params.toString()}`;

      const res = await fetch(url, {
        method: "GET",
        headers: { Accept: "application/json", Authorization: `Bearer ${user_token}` },
        credentials: "omit",
      });

      if (res.status === 401) throw new Error("Unauthorized — token missing/invalid/expired.");
      if (!res.ok) throw new Error(`Shopify request failed: ${res.status}`);

      const json = await res.json();
      const row = json?.last_row_data ? json.last_row_data : null;
      setShopifyRows(row ? [row] : []);
    } catch (e: any) {
      setShopifyError(e?.message || "Failed to load Shopify data");
      setShopifyRows([]);
    } finally {
      setShopifyLoading(false);
    }
  }, [shopifyStore]);

  /* ===================== SHOPIFY PREVIOUS MONTH ===================== */
  const fetchShopifyPrev = useCallback(async () => {
    try {
      const user_token =
        typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
      if (!user_token) throw new Error("No token found. Please sign in.");

      if (!shopifyStore?.shop_name || !shopifyStore?.access_token) {
        throw new Error("Shopify store not connected.");
      }

      const { year, monthName } = getPrevISTYearMonth();

      const params = new URLSearchParams({
        range: "monthly",
        month: monthName.toLowerCase(),
        year: String(year),
        user_token,
        shop: shopifyStore.shop_name,
        token: shopifyStore.access_token,
      });

      const url = `${SHOPIFY_DROPDOWN_ENDPOINT}?${params.toString()}`;

      const res = await fetch(url, {
        method: "GET",
        headers: { Accept: "application/json", Authorization: `Bearer ${user_token}` },
        credentials: "omit",
      });

      if (res.status === 401) throw new Error("Unauthorized — token missing/invalid/expired.");
      if (!res.ok) throw new Error(`Shopify (prev) request failed: ${res.status}`);

      const json = await res.json();
      const row = json?.last_row_data ? json.last_row_data : null;
      setShopifyPrevRows(row ? [row] : []);
    } catch (e: any) {
      console.warn("Shopify prev-month fetch failed:", e?.message);
      setShopifyPrevRows([]);
    }
  }, [shopifyStore]);






  /* ===================== ✅ SHARED BI FETCH (FOR CARDS + GRAPH) ===================== */
  const { monthName: currMonthName, year: currYear } = getISTYearMonth();

  const lastBiKeyRef = useRef<string>("");



  const fetchBiSeries = useCallback(
    async (startDay?: number | null, endDay?: number | null) => {
      if (!showLiveBI) return;

      const normalized = (biCountryName || "").toLowerCase();

      if (!normalized || normalized === "global") return;


      const rangeActive = startDay != null && endDay != null;

      const key = JSON.stringify({
        country: normalized,
        ranged: "MTD",
        month: currMonthName.toLowerCase(),
        year: currYear,
        startDay: rangeActive ? startDay : null,
        endDay: rangeActive ? endDay : null,
      });

      if (lastBiKeyRef.current === key) return;
      lastBiKeyRef.current = key;

      setBiLoading(true);
      setBiError(null);

      // if (rangeActive) {
      //   setBiAlignedTotals(null);
      // }

      try {
        const token =
          typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        const params = new URLSearchParams({
          countryName: normalized,
          ranged: "MTD",
          month: currMonthName.toLowerCase(),
          year: String(currYear),
          generate_ai_insights: "false",
        });

        // ✅ only send range params when rangeActive
        if (rangeActive) {
          params.set("start_day", String(startDay));
          params.set("end_day", String(endDay));
        }

        const res = await fetch(`${LIVE_MTD_BI_ENDPOINT}?${params.toString()}`, {
          headers: token ? { Authorization: `Bearer ${token}` } : undefined,
        });

        const json: BiApiResponse = await res.json();
        if (!res.ok) throw new Error((json as any)?.error || "Failed to load BI series");

        setLiveBiPayload(json);
        setBiPeriods(json?.periods || null);
        setBiDailySeries(json?.daily_series || null);

        // setBiAlignedTotals(json?.aligned_totals || null);
        const alignedFromNested = (json as any)?.aligned_totals;

        const alignedFromTopLevel: BiAlignedTotals = {
          total_current_advertising: (json as any)?.total_current_advertising,
          total_previous_advertising: (json as any)?.total_previous_advertising,

          total_current_net_sales: (json as any)?.total_current_net_sales,
          total_previous_net_sales: (json as any)?.total_previous_net_sales,
          total_previous_net_sales_full_month: (json as any)?.total_previous_net_sales_full_month,

          total_current_platform_fees: (json as any)?.total_current_platform_fees,
          total_previous_platform_fees: (json as any)?.total_previous_platform_fees,

          total_current_profit: (json as any)?.total_current_profit,
          total_previous_profit: (json as any)?.total_previous_profit,
        };

        // ✅ prefer nested if backend sends it, else fallback to top-level
        setBiAlignedTotals(alignedFromNested ?? alignedFromTopLevel ?? null);

      } catch (e: any) {
        setBiPeriods(null);
        setBiDailySeries(null);
        setBiAlignedTotals(null);
        setBiError(e?.message || "Failed to load BI series");
      } finally {
        setBiLoading(false);
      }
    },
    [showLiveBI, biCountryName, currMonthName, currYear]

  );


  useEffect(() => {
    if (!showLiveBI) return;
    fetchBiSeries(selectedStartDay, selectedEndDay);
  }, [showLiveBI, fetchBiSeries, selectedStartDay, selectedEndDay]);

  /* ===================== REFRESH ALL ===================== */
  const refreshAll = useCallback(async () => {
    await fetchAmazon();
    if (shopifyStore?.shop_name && shopifyStore?.access_token) {
      await Promise.all([fetchShopify(), fetchShopifyPrev()]);
    }
    // also refresh BI (keep current selected range)
    // if (showLiveBI) {
    //   await fetchBiSeries(selectedStartDay, selectedEndDay);
    // }
  }, [
    fetchAmazon,
    fetchShopify,
    fetchShopifyPrev,
    shopifyStore,
    // showLiveBI,
    // fetchBiSeries,
    // selectedStartDay,
    // selectedEndDay,
  ]);

  // useEffect(() => {
  //   refreshAll();
  // }, [refreshAll]);

  const didRefreshRef = useRef(false);

  useEffect(() => {
    if (didRefreshRef.current) return;
    didRefreshRef.current = true;

    refreshAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);


  /* ===================== AMAZON DERIVED DATA ===================== */
  const totals = data?.totals || null;
  const derived = data?.derived_totals || null;

  const uk = useMemo(() => {
    const netSalesGBP = derived?.net_sales != null ? toNumberSafe(derived.net_sales) : null;
    const aspGBP = derived?.asp != null ? toNumberSafe(derived.asp) : null;
    const cm2ProfitGBP =
      derived?.cm2_profit != null ? toNumberSafe(derived.cm2_profit) : null;

    const cogsGBP = totals?.cogs != null ? toNumberSafe(totals.cogs) : 0;
    const fbaFeesGBP = totals?.fba_fees != null ? toNumberSafe(totals.fba_fees) : 0;
    const sellingFeesGBP = totals?.selling_fees != null ? toNumberSafe(totals.selling_fees) : 0;

    // ✅ your backend already computed amazon_fees = selling + fba, but we can compute too
    const amazonFeesGBP =
      derived?.amazon_fees != null
        ? toNumberSafe(derived.amazon_fees)
        : (fbaFeesGBP + sellingFeesGBP);

    const profitGBP = derived?.profit != null ? toNumberSafe(derived.profit) : null;

    const unitsGBP = totals?.quantity != null ? toNumberSafe(totals.quantity) : null;


    let profitPctGBP: number | null = null;
    if (cm2ProfitGBP !== null && netSalesGBP && netSalesGBP !== 0) {
      profitPctGBP = (cm2ProfitGBP / netSalesGBP) * 100;
    }


    const grossSalesGBP =
      totals?.product_sales != null ? toNumberSafe(totals.product_sales) : null; // ✅ current gross

    const advertisingGBP =
      derived?.advertising_fees != null ? toNumberSafe(derived.advertising_fees) : 0;

    const platformFeeGBP =
      derived?.platform_fee != null ? toNumberSafe(derived.platform_fee) : 0;


    return {
      unitsGBP,
      netSalesGBP,
      grossSalesGBP,
      aspGBP,
      profitGBP,
      cm2ProfitGBP,
      profitPctGBP,
      cogsGBP,
      amazonFeesGBP,
      advertisingGBP,
      platformFeeGBP,
    };
  }, [totals, derived]);

  const safeDeltaPct = (current: number, previous: number) => {
    const c = Number(current) || 0;
    const p = Number(previous) || 0;
    if (!p) return null;
    return ((c - p) / p) * 100;
  };



  const prevTotals = data?.previous_period?.totals || null;

  const prev = useMemo(() => {
    return {
      quantity: toNumberSafe(prevTotals?.quantity ?? 0),
      netSales: toNumberSafe(prevTotals?.net_sales ?? 0),
      grossSales: toNumberSafe(prevTotals?.gross_sales ?? 0), // ✅ add
      asp: toNumberSafe(prevTotals?.asp ?? 0),
      profit: toNumberSafe(prevTotals?.profit ?? 0),
      cm2Profit: toNumberSafe(prevTotals?.cm2_profit ?? 0),
      profitPct: toNumberSafe(prevTotals?.profit_percentage ?? 0),
    };
  }, [prevTotals]);

  // ✅ AMAZON Ads (display currency)
  const amazonCurrAdsDisp = useMemo(() => {
    const ads = toNumberSafe(derived?.advertising_fees ?? 0);
    return convertToDisplayCurrency(ads, amazonDataCurrency);
  }, [derived?.advertising_fees, convertToDisplayCurrency, amazonDataCurrency]);

  const amazonPrevAdsDisp = useMemo(() => {
    const ads = toNumberSafe(data?.previous_period?.totals?.advertising_fees ?? 0);
    return convertToDisplayCurrency(ads, amazonDataCurrency);
  }, [data?.previous_period?.totals?.advertising_fees, convertToDisplayCurrency, amazonDataCurrency]);

  const amazonAdsDeltaPct = useMemo(
    () => safeDeltaPct(amazonCurrAdsDisp, amazonPrevAdsDisp),
    [amazonCurrAdsDisp, amazonPrevAdsDisp]
  );

  // ✅ AMAZON ROAS% = (Ads / Net Sales) * 100
  const amazonCurrRoasPct = useMemo(() => {
    const sales = toNumberSafe(derived?.net_sales ?? 0);
    const ads = toNumberSafe(derived?.advertising_fees ?? 0);
    return sales > 0 ? (ads / sales) * 100 : 0;
  }, [derived?.net_sales, derived?.advertising_fees]);

  const amazonPrevRoasPct = useMemo(() => {
    const sales = toNumberSafe(data?.previous_period?.totals?.net_sales ?? 0);
    const ads = toNumberSafe(data?.previous_period?.totals?.advertising_fees ?? 0);
    return sales > 0 ? (ads / sales) * 100 : 0;
  }, [data?.previous_period?.totals?.net_sales, data?.previous_period?.totals?.advertising_fees]);

  const amazonRoasDeltaPct = useMemo(
    () => safeDeltaPct(amazonCurrRoasPct, amazonPrevRoasPct),
    [amazonCurrRoasPct, amazonPrevRoasPct]
  );



  const curr = useMemo(() => {
    return {
      quantity: toNumberSafe(totals?.quantity ?? 0),
      netSales: toNumberSafe(derived?.net_sales ?? 0),
      asp: toNumberSafe(derived?.asp ?? 0),
      profit: toNumberSafe(derived?.profit ?? 0),
      profitPct: toNumberSafe(uk.profitPctGBP ?? 0),
    };
  }, [totals, derived, uk.profitPctGBP]);

  const deltas = useMemo(() => {
    return {
      quantityPct: safeDeltaPct(curr.quantity, prev.quantity),
      netSalesPct: safeDeltaPct(curr.netSales, prev.netSales),
      aspPct: safeDeltaPct(curr.asp, prev.asp),
      profitPct: safeDeltaPct(curr.profit, prev.profit),

      // Profit % must be percentage-points (pp)
      profitMarginPctPts:
        curr.profitPct != null && prev.profitPct != null
          ? Number(curr.profitPct) - Number(prev.profitPct)
          : null,
    };
  }, [curr, prev]);

  const deltaPctPoints = (currentPct: number, previousPct: number) => {
    const c = Number(currentPct) || 0;
    const p = Number(previousPct) || 0;
    return c - p; // percentage points
  };

  const deltaPctAbs = (currentPct: number, previousPct: number) => {
    const c = Number(currentPct) || 0;
    const p = Number(previousPct) || 0;
    return c - p;
  };


  /* ===================== ✅ RANGE KPIs FOR CARDS (FROM SAME BI DATA AS GRAPH) ===================== */
  // useEffect(() => {
  //   const pts = biDailySeriesHome?.current_mtd || [];
  //   if (!pts.length) return;

  //   const todayDay = getDayOfMonthIST();

  //   const todayPoint = pts.find((p) => Number(p.date?.slice(8, 10)) === todayDay);

  //   if (todayPoint?.net_sales != null) {
  //     setTodaySalesRaw(Number(todayPoint.net_sales) || 0); // now "raw" is actually HOME currency
  //   }
  // }, [biDailySeriesHome]);

  useEffect(() => {
    const pts = biDailySeriesHome?.current_mtd || [];
    if (!pts.length) return;

    const todayDay = getDayOfMonthIST();

    // try exact today
    const exact = pts.find((p) => Number(p.date?.slice(8, 10)) === todayDay);
    if (exact?.net_sales != null) {
      setTodaySalesRaw(Number(exact.net_sales) || 0);
      return;
    }

    // fallback: latest available day in series
    const latest = [...pts].sort((a, b) => a.date.localeCompare(b.date)).at(-1);
    setTodaySalesRaw(Number(latest?.net_sales) || 0);
  }, [biDailySeriesHome]);


  const biCardKpis = useMemo(() => {
    const currAll = biDailySeriesHome?.current_mtd || [];
    const prevAll = biDailySeriesHome?.previous || [];

    const currPts = sliceByDayRange(currAll, selectedStartDay, selectedEndDay);
    const prevPts = sliceByDayRange(prevAll, selectedStartDay, selectedEndDay);

    const sum = (arr: DailyPoint[], key: keyof DailyPoint) =>
      arr.reduce((a, d) => a + (Number(d[key]) || 0), 0);

    const curr = {
      units: sum(currPts, "quantity"),
      netSales: sum(currPts, "net_sales"),
      grossSales: sum(currPts, "product_sales"),
      profit: sum(currPts, "profit"),
      cm2Profit: sum(currPts, "cm2_profit"),
    };

    const prev = {
      units: sum(prevPts, "quantity"),
      netSales: sum(prevPts, "net_sales"),
      grossSales: sum(prevPts, "product_sales"),
      profit: sum(prevPts, "profit"),
      cm2Profit: sum(prevPts, "cm2_profit"),
    };

    const currAsp = curr.units > 0 ? curr.netSales / curr.units : 0;
    const prevAsp = prev.units > 0 ? prev.netSales / prev.units : 0;

    const currProfitPct = curr.netSales !== 0 ? (curr.cm2Profit / curr.netSales) * 100 : 0;
    const prevProfitPct = prev.netSales !== 0 ? (prev.cm2Profit / prev.netSales) * 100 : 0;

    const deltaPct = (c: number, p: number) => (p ? ((c - p) / p) * 100 : null);

    return {
      curr: { ...curr, asp: currAsp, profitPct: currProfitPct },
      prev: { ...prev, asp: prevAsp, profitPct: prevProfitPct },
      deltas: {
        units: deltaPct(curr.units, prev.units),
        netSales: deltaPct(curr.netSales, prev.netSales),
        grossSales: deltaPct(curr.grossSales, prev.grossSales),
        asp: deltaPct(currAsp, prevAsp),
        profit: deltaPct(curr.profit, prev.profit),
        profitPct: safeDeltaPctFromPct(currProfitPct, prevProfitPct),

      },
    };
  }, [biDailySeriesHome, selectedStartDay, selectedEndDay]);

  // const rangeActive = selectedStartDay != null && selectedEndDay != null;
  const rangeActive = selectedStartDay != null && selectedEndDay != null;

  // use BI only when a range is active
  const useBiCm2 = showLiveBI && rangeActive;

  // BI values are usable only when rangeActive + finished loading + response present
  const cm2Ready = useBiCm2 && !biLoading && !!biAlignedTotals;

  const globalRangeCurrency = currencyForCountry(biCountryName); // global -> "uk" -> "GBP"
  const globalUseBi = platform === "global" && showLiveBI && rangeActive;
  const globalCm2Ready = globalUseBi && !biLoading && !!biAlignedTotals;

  const shopifyNotConnected =
    !shopifyStore?.shop_name ||
    !shopifyStore?.access_token ||
    (shopifyError &&
      (shopifyError.toLowerCase().includes("shopify store not connected") ||
        shopifyError.toLowerCase().includes("no token")));

  const shopifyIntegrated = !shopifyNotConnected && !!shopify;

  const amazonIntegrated =
    Array.isArray(amazonConnections) && amazonConnections.length > 0;

  const noIntegrations = !amazonIntegrated && !shopifyIntegrated;

  /* ===================== GLOBAL / FX COMBINED (BASE USD DATA) ===================== */
  const amazonUK_USD = useMemo(() => {
    const amazonUK_GBP = toNumberSafe(uk.netSalesGBP);
    return amazonUK_GBP * gbpToUsd;
  }, [uk.netSalesGBP, gbpToUsd]);

  const combinedUSD = useMemo(() => {
    const aUK = amazonUK_USD;
    const shopifyUSD = toNumberSafe(shopifyDeriv?.netSales) * inrToUsd;
    return aUK + shopifyUSD;
  }, [amazonUK_USD, shopifyDeriv?.netSales, inrToUsd]);

  const prevAmazonMtdSalesGBP = toNumberSafe(data?.previous_period?.totals?.net_sales ?? 0);
  const prevAmazonMtdSalesUSD = prevAmazonMtdSalesGBP * gbpToUsd;

  const prevAmazonUKTotalUSD = useMemo(() => {
    const prevTotalGBP = toNumberSafe(data?.previous_month_total_net_sales?.total);
    if (prevTotalGBP > 0) return prevTotalGBP * gbpToUsd;

    // fallback: estimate full last-month total from last-month MTD
    const { todayDay, daysInPrevMonth } = getISTDayInfo();
    if (!todayDay || !daysInPrevMonth) return 0;

    // prevAmazonMtdSalesUSD is already last month MTD (USD)
    return (prevAmazonMtdSalesUSD * daysInPrevMonth) / todayDay;
  }, [data?.previous_month_total_net_sales?.total, gbpToUsd, prevAmazonMtdSalesUSD]);


  const amazonUK_Gross_USD = useMemo(() => {
    const grossGBP = toNumberSafe(totals?.product_sales); // ✅ current gross
    return grossGBP * gbpToUsd;
  }, [totals?.product_sales, gbpToUsd]);



  const combinedGrossUSD = useMemo(() => {
    const shopifyUSD = toNumberSafe(shopifyDeriv?.netSales) * inrToUsd;
    return amazonUK_Gross_USD + shopifyUSD;
  }, [amazonUK_Gross_USD, shopifyDeriv?.netSales, inrToUsd]);

  const prevAmazonGrossUSD = useMemo(() => {
    return toNumberSafe(prev.grossSales) * gbpToUsd; // prev gross in GBP → USD
  }, [prev.grossSales, gbpToUsd]);

  const prevGlobalGrossUSD = useMemo(() => {
    const prevShopifyUSD = toNumberSafe(shopifyPrevDeriv?.netSales) * inrToUsd; // shopify gross not available; using net like you do elsewhere
    return prevAmazonGrossUSD + prevShopifyUSD;
  }, [prevAmazonGrossUSD, shopifyPrevDeriv?.netSales, inrToUsd]);


  const fallbackTargetUSD = useMemo(() => {
    return prevAmazonUKTotalUSD > 0 ? prevAmazonUKTotalUSD : 0;
  }, [prevAmazonUKTotalUSD]);


  const prevShopifyTotalUSD = useMemo(() => {
    const prevINRTotal = toNumberSafe(shopifyPrevDeriv?.netSales);
    return prevINRTotal * inrToUsd;
  }, [shopifyPrevDeriv, inrToUsd]);


  const globalPrevTotalUSD = prevShopifyTotalUSD + prevAmazonUKTotalUSD;


  const chooseLastMonthTotal = (manualUSD: number, computedUSD: number) =>
    USE_MANUAL_LAST_MONTH && manualUSD > 0 ? manualUSD : computedUSD;

  const prorateToDate = (lastMonthTotalUSD: number) => {
    const { todayDay, daysInPrevMonth } = getISTDayInfo();
    return daysInPrevMonth > 0 ? (lastMonthTotalUSD * todayDay) / daysInPrevMonth : 0;
  };




  // ---------- NET SALES (DISPLAY CURRENCY) ----------

  // Amazon current & prev net sales (already correct source)
  const amazonCurrNetDisp = useMemo(
    () => convertToDisplayCurrency(uk.netSalesGBP ?? 0, "GBP"),
    [uk.netSalesGBP, convertToDisplayCurrency]
  );

  const amazonPrevNetDisp = useMemo(
    () => convertToDisplayCurrency(prev.netSales ?? 0, "GBP"),
    [prev.netSales, convertToDisplayCurrency]
  );

  // Global = Amazon + Shopify (NET SALES ONLY)
  const globalCurrNetDisp = useMemo(() => {
    const amazon = amazonCurrNetDisp;
    const shopify = convertToDisplayCurrency(
      shopifyDeriv?.netSales ?? 0,
      "INR"
    );
    return amazon + shopify;
  }, [amazonCurrNetDisp, shopifyDeriv?.netSales, convertToDisplayCurrency]);

  const globalPrevNetDisp = useMemo(() => {
    const amazon = amazonPrevNetDisp;
    const shopify = convertToDisplayCurrency(
      shopifyPrevDeriv?.netSales ?? 0,
      "INR"
    );
    return amazon + shopify;
  }, [amazonPrevNetDisp, shopifyPrevDeriv?.netSales, convertToDisplayCurrency]);


  // const regions = useMemo(() => {
  //   const globalLastMonthTotal = chooseLastMonthTotal(
  //     MANUAL_LAST_MONTH_USD_GLOBAL,
  //     globalPrevTotalUSD
  //   );

  //   const globalTarget =
  //     userMonthlyTargetHome > 0
  //       ? userMonthlyTargetHome
  //       : (globalPrevFullMonthNetSalesDisp > 0 ? globalPrevFullMonthNetSalesDisp : globalPrevNetDisp);

  //   const global: RegionMetrics = {
  //     mtdUSD: globalCurrNetDisp,
  //     lastMonthToDateUSD: globalPrevNetDisp,   // prev MTD
  //     lastMonthTotalUSD: globalTarget,         // ✅ prev FULL month total
  //     targetUSD: globalTarget,                 // ✅ target = prev FULL month total
  //     decTargetUSD: globalTarget,
  //   };

  //   const ukTarget =
  //     userMonthlyTargetHome > 0
  //       ? userMonthlyTargetHome
  //       : (prevFullMonthNetSalesDisp > 0 ? prevFullMonthNetSalesDisp : amazonPrevNetDisp);

  //   const ukRegion: RegionMetrics = {
  //     mtdUSD: amazonCurrNetDisp,
  //     lastMonthToDateUSD: amazonPrevNetDisp,
  //     lastMonthTotalUSD: ukTarget,
  //     targetUSD: ukTarget,
  //     // ✅ Dec target
  //     decTargetUSD: ukTarget,
  //   };


  //   const ukLastMonthTotal = chooseLastMonthTotal(
  //     MANUAL_LAST_MONTH_USD_UK,
  //     prevAmazonUKTotalUSD
  //   );



  //   const usLastMonthTotal = chooseLastMonthTotal(MANUAL_LAST_MONTH_USD_US, 0);
  //   const usRegion: RegionMetrics = {
  //     mtdUSD: 0,
  //     lastMonthToDateUSD: prorateToDate(usLastMonthTotal),
  //     lastMonthTotalUSD: usLastMonthTotal,
  //     targetUSD: usLastMonthTotal,
  //     decTargetUSD: usLastMonthTotal,
  //   };

  //   const caLastMonthTotal = chooseLastMonthTotal(MANUAL_LAST_MONTH_USD_CA, 0);
  //   const caRegion: RegionMetrics = {
  //     mtdUSD: 0,
  //     lastMonthToDateUSD: prorateToDate(caLastMonthTotal),
  //     lastMonthTotalUSD: caLastMonthTotal,
  //     targetUSD: caLastMonthTotal,
  //     // ✅ Dec target (fallback)
  //     decTargetUSD: caLastMonthTotal,
  //   };

  //   return {
  //     Global: global,
  //     UK: ukRegion,
  //     US: usRegion,
  //     CA: caRegion,
  //   } as Record<RegionKey, RegionMetrics>;
  // }, [
  //   globalCurrNetDisp,
  //   globalPrevNetDisp,
  //   amazonCurrNetDisp,
  //   amazonPrevNetDisp,
  //   prevFullMonthNetSalesDisp,
  //   globalPrevFullMonthNetSalesDisp,
  //   userMonthlyTargetHome,
  // ]);

  const regions = useMemo(() => {
    // ✅ user target is stored in GBP; convert to display (home) currency
    const userMonthlyTargetGBP = toNumberSafe(userData?.target_sales ?? 0);
    const userMonthlyTargetHome =
      userMonthlyTargetGBP > 0
        ? convertToDisplayCurrency(userMonthlyTargetGBP, "GBP")
        : 0;

    // ✅ GLOBAL: keep "sales" (lastMonthTotalUSD) as your computed prev full-month sales
    const globalPrevFullMonthSales =
      globalPrevFullMonthNetSalesDisp > 0
        ? globalPrevFullMonthNetSalesDisp
        : globalPrevNetDisp;

    // ✅ GLOBAL target: prefer user target, else fallback to sales baseline
    const globalTarget =
      userMonthlyTargetHome > 0 ? userMonthlyTargetHome : globalPrevFullMonthSales;

    const global: RegionMetrics = {
      mtdUSD: globalCurrNetDisp,
      lastMonthToDateUSD: globalPrevNetDisp, // prev MTD
      lastMonthTotalUSD: globalPrevFullMonthSales, // ✅ Dec/prev full-month SALES (as-is)
      targetUSD: globalTarget, // ✅ Target from userData
      decTargetUSD: globalTarget,
    };

    // ✅ UK: keep "sales" (lastMonthTotalUSD) as your computed prev full-month sales
    const ukPrevFullMonthSales =
      prevFullMonthNetSalesDisp > 0
        ? prevFullMonthNetSalesDisp
        : amazonPrevNetDisp;

    // ✅ UK target: prefer user target, else fallback
    const ukTarget =
      userMonthlyTargetHome > 0 ? userMonthlyTargetHome : ukPrevFullMonthSales;

    const ukRegion: RegionMetrics = {
      mtdUSD: amazonCurrNetDisp,
      lastMonthToDateUSD: amazonPrevNetDisp, // prev MTD
      lastMonthTotalUSD: ukPrevFullMonthSales, // ✅ Dec/prev full-month SALES (as-is)
      targetUSD: ukTarget, // ✅ Target from userData
      decTargetUSD: ukTarget,
    };

    // ✅ US/CA unchanged (you currently use manual env fallbacks)
    const usLastMonthTotal = chooseLastMonthTotal(MANUAL_LAST_MONTH_USD_US, 0);
    const usRegion: RegionMetrics = {
      mtdUSD: 0,
      lastMonthToDateUSD: prorateToDate(usLastMonthTotal),
      lastMonthTotalUSD: usLastMonthTotal,
      targetUSD: usLastMonthTotal,
      decTargetUSD: usLastMonthTotal,
    };

    const caLastMonthTotal = chooseLastMonthTotal(MANUAL_LAST_MONTH_USD_CA, 0);
    const caRegion: RegionMetrics = {
      mtdUSD: 0,
      lastMonthToDateUSD: prorateToDate(caLastMonthTotal),
      lastMonthTotalUSD: caLastMonthTotal,
      targetUSD: caLastMonthTotal,
      decTargetUSD: caLastMonthTotal,
    };

    return {
      Global: global,
      UK: ukRegion,
      US: usRegion,
      CA: caRegion,
    } as Record<RegionKey, RegionMetrics>;
  }, [
    // existing deps you already had
    globalCurrNetDisp,
    globalPrevNetDisp,
    amazonCurrNetDisp,
    amazonPrevNetDisp,
    prevFullMonthNetSalesDisp,
    globalPrevFullMonthNetSalesDisp,

    // ✅ add these because we use them inside now
    userData?.target_sales,
    convertToDisplayCurrency,

    // these are referenced by US/CA regions
    chooseLastMonthTotal,
    prorateToDate,
  ]);


  const anyLoading = loading || shopifyLoading;

  const amazonTabs = useMemo<RegionKey[]>(() => {
    const tabs: RegionKey[] = [];
    (["UK", "US", "CA"] as RegionKey[]).forEach((key) => {
      const r = regions[key];
      if (!r) return;
      if (r.mtdUSD || r.lastMonthToDateUSD || r.lastMonthTotalUSD || r.targetUSD) tabs.push(key);
    });
    return tabs;
  }, [regions]);

  useEffect(() => {
    if (amazonTabs.length && !amazonTabs.includes(amazonRegion)) setAmazonRegion(amazonTabs[0]);
  }, [amazonTabs, amazonRegion]);

  const graphRegions = useMemo<RegionKey[]>(() => {
    const list: RegionKey[] = ["Global"];
    (["UK", "US", "CA"] as RegionKey[]).forEach((key) => {
      const r = regions[key];
      if (!r) return;
      if (r.mtdUSD || r.lastMonthToDateUSD || r.lastMonthTotalUSD || r.targetUSD) list.push(key);
    });
    return list;
  }, [regions]);

  useEffect(() => {
    if (!graphRegions.includes(graphRegion)) setGraphRegion("Global");
  }, [graphRegions, graphRegion]);

  const onlyAmazon = amazonIntegrated && !shopifyIntegrated;
  const onlyShopify = shopifyIntegrated && !amazonIntegrated;

  /* ===================== P&L ITEMS (DISPLAY CURRENCY OUTPUT) ===================== */
  const plItems = useMemo(() => {
    const ukPl = () => {
      const sales = convertToDisplayCurrency(uk.netSalesGBP ?? 0, "GBP");
      const fees = convertToDisplayCurrency(uk.amazonFeesGBP ?? 0, "GBP");
      const cogs = convertToDisplayCurrency(uk.cogsGBP ?? 0, "GBP");
      const adv = convertToDisplayCurrency(uk.advertisingGBP ?? 0, "GBP");

      const others = convertToDisplayCurrency(uk.platformFeeGBP ?? 0, "GBP"); // you renamed Platform Fees → Others
      const cm1 = convertToDisplayCurrency(uk.profitGBP ?? 0, "GBP");         // you renamed Profit → CM1 Profit
      const cm2 = convertToDisplayCurrency(uk.cm2ProfitGBP ?? 0, "GBP");

      // ✅ NEW: Tax & Credits from totals.tax_and_credits
      const taxCredits = convertToDisplayCurrency(
        toNumberSafe(totals?.tax_and_credits ?? 0),
        "GBP"
      );

      return [
        { label: "Net Sales", raw: sales, display: formatDisplayAmount(sales) },
        { label: "COGS", raw: cogs, display: formatDisplayAmount(cogs) },
        { label: "Amazon Fees", raw: fees, display: formatDisplayAmount(fees) },
        { label: "Tax & Credits", raw: taxCredits, display: formatDisplayAmount(taxCredits) },
        { label: "CM1 Profit", raw: cm1, display: formatDisplayAmount(cm1) },
        { label: "Advertisements", raw: adv, display: formatDisplayAmount(adv) },
        { label: "Others", raw: others, display: formatDisplayAmount(others) },
        { label: "CM2 Profit", raw: cm2, display: formatDisplayAmount(cm2) },
      ];
    };


    if (graphRegionToUse === "Global") {
      if (onlyAmazon) return ukPl();

      if (onlyShopify) {
        const sales = convertToDisplayCurrency(shopifyDeriv?.netSales ?? 0, "INR");
        return [
          { label: "Net Sales", raw: sales, display: formatDisplayAmount(sales) },
          { label: "Amazon Fees", raw: 0, display: formatDisplayAmount(0) },
          { label: "COGS", raw: 0, display: formatDisplayAmount(0) },
          { label: "Advertisements", raw: 0, display: formatDisplayAmount(0) },
          { label: "Other Charges", raw: 0, display: formatDisplayAmount(0) },
          { label: "Profit", raw: 0, display: formatDisplayAmount(0) },
        ];
      }

      const sales = convertToDisplayCurrency(combinedUSD, "USD");
      return [
        { label: "Sales", raw: sales, display: formatDisplayAmount(sales) },
        { label: "Amazon Fees", raw: 0, display: formatDisplayAmount(0) },
        { label: "COGS", raw: 0, display: formatDisplayAmount(0) },
        { label: "Advertisements", raw: 0, display: formatDisplayAmount(0) },
        { label: "Other Charges", raw: 0, display: formatDisplayAmount(0) },
        { label: "Profit", raw: 0, display: formatDisplayAmount(0) },
      ];
    }

    if (graphRegionToUse === "UK") return ukPl();

    const zero = formatDisplayAmount(0);
    return [
      { label: "Sales", raw: 0, display: zero },
      { label: "Amazon Fees", raw: 0, display: zero },
      { label: "COGS", raw: 0, display: zero },
      { label: "Advertisements", raw: 0, display: zero },
      { label: "Other Charges", raw: 0, display: zero },
      { label: "Profit", raw: 0, display: zero },
    ];
  }, [
    graphRegionToUse,
    onlyAmazon,
    onlyShopify,
    combinedUSD,
    totals?.tax_and_credits,
    uk.netSalesGBP,
    uk.amazonFeesGBP,
    uk.cogsGBP,
    uk.advertisingGBP,
    uk.platformFeeGBP,
    uk.profitGBP,
    shopifyDeriv?.netSales,
    convertToDisplayCurrency,
    formatDisplayAmount,
  ]);

  // ✅ remove empty categories so bars don't get spaced out
  const chartItems = useMemo(() => {
    return (plItems || []).filter((i) => {
      const v = Number(i?.raw ?? 0);
      // keep only meaningful values
      return Math.abs(v) > 1e-9;
    });
  }, [plItems]);

  const labels = chartItems.map((i) => i.label);
  const values = chartItems.map((i) => Number(i.raw ?? 0));

  const colorMapping: Record<string, string> = {
    "Net Sales": "#75BBDA",
    "Amazon Fees": "#B75A5A",
    COGS: "#FDD36F",
    Advertisements: "#C49466",
    "Tax & Credits": "#ED9F50",
    Others: "#3A8EA4",
    "CM1 Profit": "#7B9A6D",
    "CM2 Profit": "#B8C78C",

  };

  const colors = labels.map((label) => colorMapping[label] || "#75BBDA");

  const allValuesZero = values.length === 0 || values.every((v) => !v || v === 0);


  /* ===================== EXCEL EXPORT (USES displayCurrency symbol) ===================== */
  const captureChartPng = useCallback(async () => {
    const container = chartRef.current;
    if (!container) return null;

    const canvas = container.querySelector("canvas") as HTMLCanvasElement | null;
    if (canvas) {
      try {
        const tmpCanvas = document.createElement("canvas");
        tmpCanvas.width = canvas.width;
        tmpCanvas.height = canvas.height;
        const ctx = tmpCanvas.getContext("2d");
        if (!ctx) return null;

        ctx.fillStyle = "#ffffff";
        ctx.fillRect(0, 0, tmpCanvas.width, tmpCanvas.height);
        ctx.drawImage(canvas, 0, 0);

        return tmpCanvas.toDataURL("image/png");
      } catch {
        return null;
      }
    }
    return null;
  }, []);

  const shortMonForGraph = new Date(`${currMonthName} 1, ${currYear}`).toLocaleString("en-US", {
    month: "short",
    timeZone: "Asia/Kolkata",
  });
  const formattedMonthYear = `${shortMonForGraph}'${String(currYear).slice(-2)}`;

  const countryNameForGraph =
    graphRegionToUse === "Global" ? "global" : graphRegionToUse.toLowerCase();

  const handleDownload = useCallback(async () => {
    try {
      const pngDataUrl = await captureChartPng();

      const workbook = new ExcelJS.Workbook();
      const sheet = workbook.addWorksheet("Amazon P&L");

      sheet.addRow([brandName || "Brand"]);
      sheet.addRow([`Amazon P&L - ${formattedMonthYear}`]);
      sheet.addRow([`Country: ${countryNameForGraph.toUpperCase()}`]);
      sheet.addRow([`Currency: ${currencySymbol}`]);
      sheet.addRow([""]);

      sheet.addRow(["Metric", "", `Amount (${currencySymbol})`]);

      const signs: Record<string, string> = {
        "Net Sales": "(+)",
        "Amazon Fees": "(-)",
        COGS: "(-)",
        Advertisements: "(-)",
        "Tax & Credits": "(+/-)",
        "Other Charges": "(-)",
        Others: "(-)",
        "CM1 Profit": "",
        "CM2 Profit": "",
      };

      values.forEach((v, idx) => {
        const label = labels[idx];
        const sign = signs[label] || "";
        const num = Number(v || 0);
        sheet.addRow([label, sign, Number(num.toFixed(2))]);
      });

      const totalValue = values.reduce((acc, v) => acc + (Number(v) || 0), 0);
      sheet.addRow(["Total", "", Number(totalValue.toFixed(2))]);

      if (pngDataUrl) {
        const base64 = pngDataUrl.replace(/^data:image\/png;base64,/, "");
        const imageId = workbook.addImage({ base64, extension: "png" });

        sheet.addImage(
          imageId,
          { tl: { col: 0, row: 9 } as any, br: { col: 8, row: 28 } as any, editAs: "oneCell" } as any
        );
      }

      const buffer = await workbook.xlsx.writeBuffer();
      const blob = new Blob([buffer], {
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      });
      saveAs(blob, `Amazon-PnL-${formattedMonthYear}.xlsx`);
    } catch (err) {
      console.error("Error generating Excel with chart", err);
    }
  }, [
    brandName,
    formattedMonthYear,
    countryNameForGraph,
    currencySymbol,
    captureChartPng,
    labels,
    values,
  ]);

  const todaySalesFromBI = useMemo(() => {
    const points = biDailySeries?.current_mtd || [];
    if (!points.length) return 0;

    // if range active, use sliced series (so "today" = last day in range)
    const pts = rangeActive
      ? sliceByDayRange(points, selectedStartDay, selectedEndDay)
      : points;

    if (!pts.length) return 0;

    // pick last point by date (safe even if API order changes)
    const last = [...pts].sort((a, b) => a.date.localeCompare(b.date)).at(-1);

    return Number(last?.net_sales) || 0;
  }, [biDailySeries, rangeActive, selectedStartDay, selectedEndDay]);

  const useBiForAmazonCards =
    showLiveBI && rangeActive && (isCountryMode || platform === "global");


  /* ===================== ✅ GLOBAL CARD: prev/current + deltas ===================== */

  // Global Units
  const globalCurrUnits = useMemo(() => {
    return toNumberSafe(totals?.quantity ?? 0) + toNumberSafe(shopifyDeriv?.totalOrders ?? 0);
  }, [totals?.quantity, shopifyDeriv?.totalOrders]);

  const globalPrevUnits = useMemo(() => {
    return toNumberSafe(prev.quantity ?? 0) + toNumberSafe(shopifyPrevDeriv?.totalOrders ?? 0);
  }, [prev.quantity, shopifyPrevDeriv?.totalOrders]);

  const globalCurrSalesDisp = useMemo(() => {
    return convertToDisplayCurrency(combinedUSD, "USD");
  }, [combinedUSD, convertToDisplayCurrency]);

  const globalPrevSalesDisp = useMemo(() => {
    return convertToDisplayCurrency(globalPrevTotalUSD, "USD");
  }, [globalPrevTotalUSD, convertToDisplayCurrency]);

  const globalCurrAsp = useMemo(() => {
    return globalCurrUnits > 0 ? globalCurrSalesDisp / globalCurrUnits : 0;
  }, [globalCurrSalesDisp, globalCurrUnits]);

  const globalPrevAsp = useMemo(() => {
    return globalPrevUnits > 0 ? globalPrevSalesDisp / globalPrevUnits : 0;
  }, [globalPrevSalesDisp, globalPrevUnits]);



  // ✅ Global card "Nov'25" should use previous_period (prev.*), not previous_month_total_net_sales/globalPrevTotalUSD.
  // When only Amazon is connected, Global == Amazon UK.

  const globalCurrNetSalesDisp = useMemo(() => {
    if (onlyAmazon) return convertToDisplayCurrency(uk.netSalesGBP ?? 0, "GBP");
    return convertToDisplayCurrency(combinedUSD, "USD");
  }, [onlyAmazon, uk.netSalesGBP, combinedUSD, convertToDisplayCurrency]);

  const globalPrevNetSalesDisp = useMemo(() => {
    if (onlyAmazon) return convertToDisplayCurrency(prev.netSales ?? 0, "GBP");
    // (optional) if Shopify prev exists, add it here later; for now keep your existing globalPrevTotalUSD
    return convertToDisplayCurrency(globalPrevTotalUSD, "USD");
  }, [onlyAmazon, prev.netSales, globalPrevTotalUSD, convertToDisplayCurrency]);

  // ✅ GLOBAL Ads (Amazon Ads only for now)
  const globalCurrAdsDisp = useMemo(() => {
    const ads = toNumberSafe(derived?.advertising_fees ?? 0);
    return convertToDisplayCurrency(ads, amazonDataCurrency);
  }, [derived?.advertising_fees, convertToDisplayCurrency, amazonDataCurrency]);

  const globalPrevAdsDisp = useMemo(() => {
    const ads = toNumberSafe(data?.previous_period?.totals?.advertising_fees ?? 0);
    return convertToDisplayCurrency(ads, amazonDataCurrency);
  }, [data?.previous_period?.totals?.advertising_fees, convertToDisplayCurrency, amazonDataCurrency]);

  const globalAdsDeltaPct = useMemo(
    () => safeDeltaPct(globalCurrAdsDisp, globalPrevAdsDisp),
    [globalCurrAdsDisp, globalPrevAdsDisp]
  );

  // ✅ GLOBAL ROAS% (normalize to USD to avoid GBP+INR mixing)
  const globalCurrRoasPct = useMemo(() => {
    const ads = toNumberSafe(derived?.advertising_fees ?? 0);
    const amazonSales = toNumberSafe(derived?.net_sales ?? 0);
    const shopifySales = toNumberSafe(shopifyDeriv?.netSales ?? 0);

    const amazonSalesUsd =
      amazonDataCurrency === "GBP" ? amazonSales * gbpToUsd :
        amazonDataCurrency === "CAD" ? amazonSales * cadToUsd :
          amazonSales;

    const shopifySalesUsd = shopifySales * inrToUsd;
    const globalSalesUsd = onlyAmazon ? amazonSalesUsd : (amazonSalesUsd + shopifySalesUsd);

    const adsUsd =
      amazonDataCurrency === "GBP" ? ads * gbpToUsd :
        amazonDataCurrency === "CAD" ? ads * cadToUsd :
          ads;

    return globalSalesUsd > 0 ? (adsUsd / globalSalesUsd) * 100 : 0;
  }, [
    derived?.advertising_fees,
    derived?.net_sales,
    shopifyDeriv?.netSales,
    onlyAmazon,
    amazonDataCurrency,
    gbpToUsd,
    cadToUsd,
    inrToUsd,
  ]);

  const globalPrevRoasPct = useMemo(() => {
    const ads = toNumberSafe(data?.previous_period?.totals?.advertising_fees ?? 0);
    const amazonSales = toNumberSafe(data?.previous_period?.totals?.net_sales ?? 0);
    const shopifySales = toNumberSafe(shopifyPrevDeriv?.netSales ?? 0);

    const amazonSalesUsd =
      amazonDataCurrency === "GBP" ? amazonSales * gbpToUsd :
        amazonDataCurrency === "CAD" ? amazonSales * cadToUsd :
          amazonSales;

    const shopifySalesUsd = shopifySales * inrToUsd;
    const globalSalesUsd = onlyAmazon ? amazonSalesUsd : (amazonSalesUsd + shopifySalesUsd);

    const adsUsd =
      amazonDataCurrency === "GBP" ? ads * gbpToUsd :
        amazonDataCurrency === "CAD" ? ads * cadToUsd :
          ads;

    return globalSalesUsd > 0 ? (adsUsd / globalSalesUsd) * 100 : 0;
  }, [
    data?.previous_period?.totals?.advertising_fees,
    data?.previous_period?.totals?.net_sales,
    shopifyPrevDeriv?.netSales,
    onlyAmazon,
    amazonDataCurrency,
    gbpToUsd,
    cadToUsd,
    inrToUsd,
  ]);

  const globalRoasDeltaPct = useMemo(
    () => safeDeltaPct(globalCurrRoasPct, globalPrevRoasPct),
    [globalCurrRoasPct, globalPrevRoasPct]
  );

  const globalCurrAspDisp = useMemo(() => {
    return globalCurrUnits > 0 ? globalCurrNetSalesDisp / globalCurrUnits : 0;
  }, [globalCurrUnits, globalCurrNetSalesDisp]);

  const globalPrevAspDisp = useMemo(() => {
    return globalPrevUnits > 0 ? globalPrevNetSalesDisp / globalPrevUnits : 0;
  }, [globalPrevUnits, globalPrevNetSalesDisp]);


  const globalCurrCm2Disp = useMemo(() => {
    return convertToDisplayCurrency(uk.cm2ProfitGBP ?? 0, "GBP");
  }, [uk.cm2ProfitGBP, convertToDisplayCurrency]);

  const globalPrevCm2Disp = useMemo(() => {
    return convertToDisplayCurrency(prev.cm2Profit ?? 0, "GBP");
  }, [prev.cm2Profit, convertToDisplayCurrency]);


  // Global Profit (you currently show Amazon profit only in global card)
  const globalCurrProfit = useMemo(() => {
    const pUsd = toNumberSafe(uk.profitGBP ?? 0) * gbpToUsd;
    return convertToDisplayCurrency(pUsd, "USD");
  }, [uk.profitGBP, gbpToUsd, convertToDisplayCurrency]);

  const globalPrevProfit = useMemo(() => {
    // previous month MTD Amazon profit in GBP from your API
    const prevProfitGbp = toNumberSafe(prev.profit ?? 0);
    const pUsd = prevProfitGbp * gbpToUsd;
    return convertToDisplayCurrency(pUsd, "USD");
  }, [prev.profit, gbpToUsd, convertToDisplayCurrency]);

  const globalDeltas = useMemo(() => {
    return {
      units: safeDeltaPct(globalCurrUnits, globalPrevUnits),
      sales: safeDeltaPct(globalCurrSalesDisp, globalPrevSalesDisp),
      asp: safeDeltaPct(globalCurrAsp, globalPrevAsp),
      profit: safeDeltaPct(globalCurrProfit, globalPrevProfit),
      profitPct: null as number | null,
    };
  }, [
    globalCurrUnits,
    globalPrevUnits,
    globalCurrSalesDisp,
    globalPrevSalesDisp,
    globalCurrAsp,
    globalPrevAsp,
    globalCurrProfit,
    globalPrevProfit,
  ]);

  const globalCurrGrossDisp = useMemo(() => {
    return convertToDisplayCurrency(combinedGrossUSD, "USD");
  }, [combinedGrossUSD, convertToDisplayCurrency]);

  const globalPrevGrossDisp = useMemo(() => {
    const prevAmazonGrossUSD = toNumberSafe(prev.grossSales) * gbpToUsd; // prev gross comes in GBP
    const prevShopifyUSD = toNumberSafe(shopifyPrevDeriv?.netSales) * inrToUsd;
    return convertToDisplayCurrency(prevAmazonGrossUSD + prevShopifyUSD, "USD");
  }, [prev.grossSales, gbpToUsd, shopifyPrevDeriv?.netSales, inrToUsd, convertToDisplayCurrency]);



  /* ===================== RENDER FLAGS ===================== */
  const hasAnyGraphData = amazonIntegrated || shopifyIntegrated;
  const hasGlobalCard = !noIntegrations;
  const hasAmazonCard = amazonIntegrated;
  const hasShopifyCard = !shopifyNotConnected;

  const leftColumnHeightClass = !hasShopifyCard ? "lg:min-h-[520px]" : "";

  const prevShort = getShort(biPeriods?.previous?.label);
  const currShort = getShort(biPeriods?.current_mtd?.label);

  const rangeCurrency = currencyForCountry(countryName);


  const identityConvert = useCallback((v: number, _from?: any) => v, []);

  // ✅ Reimbursement (current + previous) converted to HOME currency (displayCurrency)
  const reimbursementHome = useMemo(() => {
    // current month reimbursement lives in derived_totals
    const currRaw = toNumberSafe(derived?.current_net_reimbursement ?? 0);

    // previous month reimbursement lives in previous_period.totals (as per your snippet)
    const prevRaw = toNumberSafe(
      data?.previous_period?.totals?.previous_net_reimbursement ?? 0
    );

    return {
      current: convertToDisplayCurrency(currRaw, amazonDataCurrency),
      previous: convertToDisplayCurrency(prevRaw, amazonDataCurrency),

      // optional: delta% in home currency (safe even if fx changes)
      deltaPct: safeDeltaPct(
        convertToDisplayCurrency(currRaw, amazonDataCurrency),
        convertToDisplayCurrency(prevRaw, amazonDataCurrency)
      ),
    };
  }, [
    derived?.current_net_reimbursement,
    data?.previous_period?.totals?.previous_net_reimbursement,
    convertToDisplayCurrency,
    amazonDataCurrency,
  ]);


  const targetData = regions[targetRegion] || regions.Global;

  const stats_mtdHome = identityConvert(targetData.mtdUSD ?? 0);
  const stats_lastMtdHome = identityConvert(targetData.lastMonthToDateUSD ?? 0);
  const stats_lastMonthTotalHome = identityConvert(targetData.lastMonthTotalUSD ?? 0);
  const stats_targetHome = identityConvert(targetData.targetUSD ?? 0);

  const { todayDay: statsTodayDay } = getISTDayInfo();

  const stats_todayHome =
    typeof todaySalesRaw === "number" && !Number.isNaN(todaySalesRaw)
      ? todaySalesRaw
      : statsTodayDay > 0
        ? stats_mtdHome / statsTodayDay
        : 0;

  const stats_salesTrendPct =
    stats_lastMtdHome > 0
      ? ((stats_mtdHome - stats_lastMtdHome) / stats_lastMtdHome) * 100
      : 0;

const getDaysInMonthIST = () => {
  const now = new Date();
  const ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
  return new Date(ist.getFullYear(), ist.getMonth() + 1, 0).getDate(); // 28..31
};

const todayIST = getDayOfMonthIST();        // D
const daysInMonthIST = getDaysInMonthIST(); // N

const proratedTargetToDate = (daysInMonthIST > 0)
  ? (todayIST / daysInMonthIST) * stats_targetHome  // x
  : 0;

const stats_targetTrendPct =
  stats_targetHome > 0
    ? ((proratedTargetToDate - stats_mtdHome) / stats_targetHome) * 100
    : 0;

  return (
    <div className="relative overflow-x-hidden">
      <HashScroll offset={80} />
      {(loading || shopifyLoading) && !data && !shopify && (
        <>
          <div className="fixed inset-0 z-40 bg-white/70" />
          <div className="fixed inset-0 z-50 flex items-center justify-center">
            <Loader
              src="/infinity-unscreen.gif"
              label="Loading sales dashboard…"
              size={120}
              roundedClass="rounded-xl"
              backgroundClass="bg-transparent"
              respectReducedMotion
            />
          </div>
        </>
      )}

      <div className="mx-auto w-full max-w-full ">

        <div className="mb-2 2xl:mb-4 flex flex-col gap-1 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex flex-col leading-tight">
            <p className="text-sm 2xl:text-lg text-charcoal-500 mb-1">
              Let&apos;s get started,{" "}
              <span className="text-green-500">{brandName}!</span>
            </p>

            <div className="flex items-center gap-2">
              <PageBreadcrumb
                pageTitle="Sales Dashboard -"
                variant="page"
                textSize="2xl"
              // className="text-2xl font-semibold"
              />

              <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-semibold text-[#5EA68E]">
                {formattedMonthYear}
              </span>
            </div>
          </div>

          <button
            onClick={refreshAll}
            disabled={loading || shopifyLoading || biLoading}
            className={`w-full rounded-md border px-3 py-1.5 text-xs 2xl:text-sm shadow-sm active:scale-[.99] sm:w-auto ${loading || shopifyLoading || biLoading
              ? "cursor-not-allowed border-gray-200 bg-gray-100 text-gray-400"
              : "border-gray-300 bg-white hover:bg-gray-50"
              }`}
          >
            {loading || shopifyLoading || biLoading ? "Refreshing…" : "Refresh"}
          </button>
        </div>

        {/* <div className={`grid grid-cols-12 gap-6 items-stretch`}> */}
        <div id="live-sales" className="grid grid-cols-12 gap-4 lg:gap-4 2xl:gap-6 items-stretch scroll-mt-[80px]">

          {/* LEFT COLUMN */}
          <div className={`col-span-12 lg:col-span-8 order-2 lg:order-1 flex flex-col gap-4 lg:gap-4 2xl:gap-6 ${leftColumnHeightClass}`}>

            {/* GLOBAL CARD */}
            {!isCountryMode && hasGlobalCard && (
              <div className="flex">
                <div className="w-full rounded-2xl border bg-white p-4 lg:p-3 2xl:p-5 shadow-sm">
                  <div className="mb-4 flex items-start justify-between gap-3">
                    <div className="flex items-baseline gap-2">
                      <PageBreadcrumb pageTitle="Global" variant="page" align="left" />
                    </div>

                    {showLiveBI && platform === "global" && (
                      <RangePicker
                        selectedStartDay={selectedStartDay}
                        selectedEndDay={selectedEndDay}
                        onSubmit={(s, e) => {
                          setSelectedStartDay(s);
                          setSelectedEndDay(e);
                        }}
                        onClear={() => {
                          setSelectedStartDay(null);
                          setSelectedEndDay(null);
                        }}
                        onCloseReset={() => {
                          setSelectedStartDay(null);
                          setSelectedEndDay(null);
                        }}
                      />
                    )}
                  </div>

                  <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-4 2xl:grid-cols-4 gap-3 auto-rows-fr">

                    <AmazonStatCard
                      label="Units"
                      current={globalUseBi ? biCardKpis.curr.units : globalCurrUnits}
                      previous={globalUseBi ? biCardKpis.prev.units : globalPrevUnits}
                      deltaPct={globalUseBi ? biCardKpis.deltas.units : globalDeltas.units}
                      loading={loading || shopifyLoading || biLoading}
                      formatter={fmtInt}
                      bottomLabel={prevLabel}
                      className="border-[#FDD36F] bg-[#FDD36F4D]"
                    />

                    <AmazonStatCard
                      label="Gross Sales"
                      current={globalUseBi ? biCardKpis.curr.grossSales : globalCurrGrossDisp}
                      previous={globalUseBi ? biCardKpis.prev.grossSales : globalPrevGrossDisp}

                      deltaPct={globalUseBi ? biCardKpis.deltas.grossSales : safeDeltaPct(combinedGrossUSD, prevGlobalGrossUSD)}
                      loading={loading || shopifyLoading || biLoading}
                      formatter={formatDisplayAmount}
                      bottomLabel={prevLabel}

                      className="border-[#ED9F50] bg-[#ED9F504D]"
                    />


                    <AmazonStatCard
                      label="Net Sales"
                      current={globalUseBi ? biCardKpis.curr.netSales : globalCurrNetSalesDisp}
                      previous={globalUseBi ? biCardKpis.prev.netSales : globalPrevNetSalesDisp}

                      deltaPct={globalUseBi ? biCardKpis.deltas.netSales : safeDeltaPct(globalCurrNetSalesDisp, globalPrevNetSalesDisp)}
                      loading={loading || shopifyLoading || biLoading}
                      formatter={formatDisplayAmount}
                      bottomLabel={prevLabel}
                      className="border-[#75BBDA] bg-[#75BBDA4D]"
                    />

                    <AmazonStatCard
                      label="ASP"
                      current={globalUseBi ? biCardKpis.curr.asp : globalCurrAspDisp}
                      previous={globalUseBi ? biCardKpis.prev.asp : globalPrevAspDisp}
                      deltaPct={
                        globalUseBi
                          ? biCardKpis.deltas.asp
                          : safeDeltaPct(globalCurrAspDisp, globalPrevAspDisp)
                      }
                      loading={loading || shopifyLoading || biLoading}
                      formatter={formatDisplayAmount}
                      bottomLabel={prevLabel}
                      className="border-[#B75A5A] bg-[#B75A5A4D]"
                    />

                    <AmazonStatCard
                      label="Cost of Ads"
                      current={
                        globalUseBi
                          ? (globalCm2Ready
                            ? convertToDisplayCurrency(
                              biAlignedTotals?.total_current_advertising ?? 0,
                              biSourceCurrency
                            )
                            : 0)
                          : globalCurrAdsDisp
                      }
                      previous={
                        globalUseBi
                          ? (globalCm2Ready
                            ? convertToDisplayCurrency(
                              biAlignedTotals?.total_previous_advertising ?? 0,
                              biSourceCurrency
                            )
                            : 0)
                          : globalPrevAdsDisp
                      }
                      deltaPct={
                        globalUseBi
                          ? (globalCm2Ready
                            ? safeDeltaPct(
                              // delta should be computed on converted values (home currency) to avoid FX noise
                              convertToDisplayCurrency(
                                biAlignedTotals?.total_current_advertising ?? 0,
                                biSourceCurrency
                              ),
                              convertToDisplayCurrency(
                                biAlignedTotals?.total_previous_advertising ?? 0,
                                biSourceCurrency
                              )
                            )
                            : null)
                          : globalAdsDeltaPct
                      }
                      loading={loading || shopifyLoading || (globalUseBi ? biLoading : false)}
                      formatter={formatDisplayAmount}
                      bottomLabel={prevLabel}
                      className="border-[#C49466] bg-[#C494664D]"
                    />

                    <AmazonStatCard
                      label="TACoS"
                      current={
                        globalUseBi
                          ? (globalCm2Ready
                            ? (() => {
                              const ads = biAlignedTotals?.total_current_advertising ?? 0;
                              const sales = biAlignedTotals?.total_current_net_sales ?? 0;
                              return sales > 0 ? (ads / sales) * 100 : 0;
                            })()
                            : 0)
                          : globalCurrRoasPct
                      }
                      previous={
                        globalUseBi
                          ? (globalCm2Ready
                            ? (() => {
                              const ads = biAlignedTotals?.total_previous_advertising ?? 0;
                              const sales = biAlignedTotals?.total_previous_net_sales ?? 0;
                              return sales > 0 ? (ads / sales) * 100 : 0;
                            })()
                            : 0)
                          : globalPrevRoasPct
                      }
                      deltaPct={
                        globalUseBi
                          ? (globalCm2Ready
                            ? deltaPctAbs(
                              (() => {
                                const ads = biAlignedTotals?.total_current_advertising ?? 0;
                                const sales = biAlignedTotals?.total_current_net_sales ?? 0;
                                return sales > 0 ? (ads / sales) * 100 : 0;
                              })(),
                              (() => {
                                const ads = biAlignedTotals?.total_previous_advertising ?? 0;
                                const sales = biAlignedTotals?.total_previous_net_sales ?? 0;
                                return sales > 0 ? (ads / sales) * 100 : 0;
                              })()
                            )
                            : null)
                          : deltaPctAbs(globalCurrRoasPct, globalPrevRoasPct)
                      }
                      inverseDelta
                      loading={loading || shopifyLoading || (globalUseBi ? biLoading : false)}
                      formatter={fmtPct2}
                      bottomLabel={prevLabel}
                      className="border-[#3A8EA4] bg-[#3A8EA44D]"
                    />


                    <AmazonStatCard
                      label="CM2 Profit"
                      current={
                        globalUseBi
                          ? (globalCm2Ready
                            ? convertToDisplayCurrency(biAlignedTotals?.current_cm2_profit ?? 0, biSourceCurrency)
                            : 0)
                          : globalCurrCm2Disp
                      }
                      previous={
                        globalUseBi
                          ? (globalCm2Ready
                            ? convertToDisplayCurrency(biAlignedTotals?.previous_cm2_profit ?? 0, biSourceCurrency)
                            : 0)
                          : globalPrevCm2Disp
                      }

                      deltaPct={
                        globalUseBi
                          ? (globalCm2Ready
                            ? safeDeltaPct(
                              biAlignedTotals?.current_cm2_profit ?? 0,
                              biAlignedTotals?.previous_cm2_profit ?? 0
                            )
                            : null)
                          : safeDeltaPct(globalCurrCm2Disp, globalPrevCm2Disp)
                      }
                      loading={loading || shopifyLoading || (globalUseBi ? biLoading : false)}
                      formatter={formatDisplayAmount}
                      bottomLabel={prevLabel}
                      className="border-[#B8C78C] bg-[#B8C78C4D]"
                    />


                    <AmazonStatCard
                      label="CM2 Profit %"
                      current={
                        globalUseBi
                          ? (globalCm2Ready ? (biAlignedTotals?.total_current_profit_percentage ?? 0) : 0)
                          : curr.profitPct
                      }
                      previous={
                        globalUseBi
                          ? (globalCm2Ready ? (biAlignedTotals?.total_previous_profit_percentage ?? 0) : 0)
                          : prev.profitPct
                      }
                      deltaPct={
                        globalUseBi
                          ? (globalCm2Ready
                            ? deltaPctPoints(
                              biAlignedTotals?.total_current_profit_percentage ?? 0,
                              biAlignedTotals?.total_previous_profit_percentage ?? 0
                            )
                            : null)
                          : deltaPctPoints(curr.profitPct ?? 0, prev.profitPct ?? 0)
                      }


                      loading={loading || shopifyLoading || (globalUseBi ? biLoading : false)}
                      formatter={fmtPct}
                      bottomLabel={prevLabel}
                      className="border-[#7B9A6D] bg-[#7B9A6D4D]"
                    />


                  </div>
                </div>
              </div>
            )}

            {/* AMAZON SECTION */}
            {hasAmazonCard && (
              <div className="flex flex-col lg:flex-1 gap-4 2xl:gap-6">
                {/* Amazon KPI Box */}
                <div className="w-full rounded-2xl border bg-white p-3 2xl:p-5 shadow-sm">
                  <div className="mb-3 lg:mb-2 2xl:mb-4 flex flex-row gap-3 items-start md:items-start md:justify-between">
                    <div className="flex flex-col flex-1 min-w-0">
                      <div className="flex flex-wrap items-baseline gap-2">
                        <PageBreadcrumb pageTitle="Amazon" variant="page" align="left" />

                        {countryName !== "global" && (
                          <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
                            {countryName.toUpperCase()}
                          </span>
                        )}
                      </div>


                    </div>

                    <div className="flex items-center gap-2">
                      {showLiveBI && isCountryMode && (
                        <RangePicker
                          selectedStartDay={selectedStartDay}
                          selectedEndDay={selectedEndDay}
                          onSubmit={(s, e) => {
                            setSelectedStartDay(s);
                            setSelectedEndDay(e);
                          }}
                          onClear={() => {
                            setSelectedStartDay(null);
                            setSelectedEndDay(null);
                          }}
                          onCloseReset={() => {
                            setSelectedStartDay(null);
                            setSelectedEndDay(null);
                          }}
                        />
                      )}

                    </div>
                  </div>

                  <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-4 2xl:grid-cols-4 gap-2 lg:gap-2 2xl:gap-3 auto-rows-fr">

                    <AmazonStatCard
                      label="Units"
                      current={useBiForAmazonCards ? biCardKpis.curr.units : (totals?.quantity ?? 0)}
                      previous={useBiForAmazonCards ? biCardKpis.prev.units : prev.quantity}
                      deltaPct={useBiForAmazonCards ? biCardKpis.deltas.units : deltas.quantityPct}
                      loading={loading || biLoading}
                      formatter={fmtInt}
                      bottomLabel={prevLabel}
                      className="border-[#FDD36F] bg-[#FDD36F4D]"

                    />

                    <AmazonStatCard
                      label="Gross Sales"
                      current={
                        showLiveBI && rangeActive
                          ? biCardKpis.curr.grossSales                 // ✅ no conversion
                          : convertToDisplayCurrency(uk.grossSalesGBP ?? 0, amazonDataCurrency)
                      }
                      previous={
                        showLiveBI && rangeActive
                          ? biCardKpis.prev.grossSales                 // ✅ no conversion
                          : convertToDisplayCurrency(prev.grossSales ?? 0, amazonDataCurrency)
                      }
                      deltaPct={useBiForAmazonCards ? biCardKpis.deltas.grossSales : safeDeltaPct(uk.grossSalesGBP ?? 0, prev.grossSales ?? 0)}
                      loading={loading || biLoading}
                      formatter={formatDisplayAmount}
                      bottomLabel={prevLabel}
                      className="border-[#ED9F50] bg-[#ED9F504D]"
                    />

                    <AmazonStatCard
                      label="Net Sales"
                      current={
                        showLiveBI && rangeActive
                          ? biCardKpis.curr.netSales                   // ✅ no conversion
                          : convertToDisplayCurrency(uk.netSalesGBP ?? 0, amazonDataCurrency)
                      }
                      previous={
                        showLiveBI && rangeActive
                          ? biCardKpis.prev.netSales                   // ✅ no conversion
                          : convertToDisplayCurrency(prev.netSales, amazonDataCurrency)
                      }
                      deltaPct={useBiForAmazonCards ? biCardKpis.deltas.netSales : deltas.netSalesPct}
                      loading={loading || biLoading}
                      formatter={formatDisplayAmount}
                      bottomLabel={prevLabel}
                      className="border-[#75BBDA] bg-[#75BBDA4D]"
                    />






                    <AmazonStatCard
                      label="ASP"
                      current={
                        showLiveBI && rangeActive
                          ? biCardKpis.curr.asp
                          : convertToDisplayCurrency(uk.aspGBP ?? 0, amazonDataCurrency)
                      }
                      previous={
                        showLiveBI && rangeActive
                          ? biCardKpis.prev.asp
                          : convertToDisplayCurrency(prev.asp, amazonDataCurrency)
                      }
                      deltaPct={useBiForAmazonCards ? biCardKpis.deltas.asp : deltas.aspPct}
                      loading={loading || biLoading}
                      formatter={formatDisplayAmount}
                      bottomLabel={prevLabel}
                      className="border-[#B75A5A] bg-[#B75A5A4D]"
                    />

                    <AmazonStatCard
                      label="Cost of Ads"
                      current={
                        useBiForAmazonCards
                          ? (cm2Ready
                            ? convertToDisplayCurrency(
                              biAlignedTotals?.total_current_advertising ?? 0,
                              biSourceCurrency
                            )
                            : 0)
                          : amazonCurrAdsDisp
                      }
                      previous={
                        useBiForAmazonCards
                          ? (cm2Ready
                            ? convertToDisplayCurrency(
                              biAlignedTotals?.total_previous_advertising ?? 0,
                              biSourceCurrency
                            )
                            : 0)
                          : amazonPrevAdsDisp
                      }
                      deltaPct={
                        useBiForAmazonCards
                          ? (cm2Ready
                            ? safeDeltaPct(
                              convertToDisplayCurrency(
                                biAlignedTotals?.total_current_advertising ?? 0,
                                biSourceCurrency
                              ),
                              convertToDisplayCurrency(
                                biAlignedTotals?.total_previous_advertising ?? 0,
                                biSourceCurrency
                              )
                            )
                            : null)
                          : amazonAdsDeltaPct
                      }
                      loading={loading || (useBiForAmazonCards ? biLoading : false)}
                      formatter={formatDisplayAmount}
                      bottomLabel={prevLabel}
                      className="border-[#C49466] bg-[#C494664D]"
                    />

                    <AmazonStatCard
                      label="TACoS"
                      current={
                        useBiForAmazonCards
                          ? (cm2Ready
                            ? (() => {
                              const ads = biAlignedTotals?.total_current_advertising ?? 0;
                              const sales = biAlignedTotals?.total_current_net_sales ?? 0;
                              return sales > 0 ? (ads / sales) * 100 : 0;
                            })()
                            : 0)
                          : amazonCurrRoasPct
                      }
                      previous={
                        useBiForAmazonCards
                          ? (cm2Ready
                            ? (() => {
                              const ads = biAlignedTotals?.total_previous_advertising ?? 0;
                              const sales = biAlignedTotals?.total_previous_net_sales ?? 0;
                              return sales > 0 ? (ads / sales) * 100 : 0;
                            })()
                            : 0)
                          : amazonPrevRoasPct
                      }
                      deltaPct={
                        useBiForAmazonCards
                          ? (cm2Ready
                            ? deltaPctAbs(
                              (() => {
                                const ads = biAlignedTotals?.total_current_advertising ?? 0;
                                const sales = biAlignedTotals?.total_current_net_sales ?? 0;
                                return sales > 0 ? (ads / sales) * 100 : 0;
                              })(),
                              (() => {
                                const ads = biAlignedTotals?.total_previous_advertising ?? 0;
                                const sales = biAlignedTotals?.total_previous_net_sales ?? 0;
                                return sales > 0 ? (ads / sales) * 100 : 0;
                              })()
                            )
                            : null)
                          : deltaPctAbs(amazonCurrRoasPct, amazonPrevRoasPct)
                      }
                      inverseDelta
                      loading={loading || (useBiForAmazonCards ? biLoading : false)}
                      formatter={fmtPct2}
                      bottomLabel={prevLabel}
                      className="border-[#3A8EA4] bg-[#3A8EA44D]"
                    />


                    <AmazonStatCard
                      label="CM2 Profit"
                      current={
                        useBiCm2
                          ? (cm2Ready
                            ? convertToDisplayCurrency(biAlignedTotals?.current_cm2_profit ?? 0, biSourceCurrency)

                            : 0)
                          : convertToDisplayCurrency(uk.cm2ProfitGBP ?? 0, amazonDataCurrency) // ✅ MTD Transactions
                      }
                      previous={
                        useBiCm2
                          ? (cm2Ready
                            ? convertToDisplayCurrency(biAlignedTotals?.previous_cm2_profit ?? 0, rangeCurrency)
                            : 0)
                          : convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency) // ✅ MTD Transactions prev
                      }
                      deltaPct={
                        useBiCm2
                          ? (cm2Ready
                            ? safeDeltaPct(
                              biAlignedTotals?.current_cm2_profit ?? 0,
                              biAlignedTotals?.previous_cm2_profit ?? 0
                            )
                            : null)
                          : safeDeltaPct(uk.cm2ProfitGBP ?? 0, prev.cm2Profit ?? 0) // ✅ MTD Transactions delta
                      }
                      loading={loading || (useBiCm2 ? biLoading : false)}
                      formatter={formatDisplayAmount}
                      bottomLabel={prevLabel}
                      className="border-[#B8C78C] bg-[#B8C78C4D]"
                    />

                    <AmazonStatCard
                      label="CM2 Profit %"
                      current={
                        useBiCm2
                          ? (cm2Ready ? (biAlignedTotals?.total_current_profit_percentage ?? 0) : 0)
                          : (curr.profitPct ?? 0) // ✅ MTD Transactions
                      }
                      previous={
                        useBiCm2
                          ? (cm2Ready ? (biAlignedTotals?.total_previous_profit_percentage ?? 0) : 0)
                          : (prev.profitPct ?? 0) // ✅ MTD Transactions
                      }
                      deltaPct={
                        useBiCm2
                          ? (cm2Ready
                            ? deltaPctPoints(
                              biAlignedTotals?.total_current_profit_percentage ?? 0,
                              biAlignedTotals?.total_previous_profit_percentage ?? 0
                            )
                            : null)
                          : deltaPctPoints(curr.profitPct ?? 0, prev.profitPct ?? 0)
                      }

                      loading={loading || (useBiCm2 ? biLoading : false)}
                      formatter={fmtPct}
                      bottomLabel={prevLabel}
                      className="border-[#7B9A6D] bg-[#7B9A6D4D]"
                    />
                  </div>
                </div>

                {/* {showLiveBI && isCountryMode && (
                  <div className="w-full rounded-2xl border bg-white p-4 sm:p-5 shadow-sm overflow-x-hidden">
                    <div className="w-full max-w-full min-w-0">
                      <LiveBiLineGraph
                        dailySeries={biDailySeries}
                        periods={biPeriods}
                        loading={biLoading}
                        error={biError}
                        selectedStartDay={selectedStartDay}
                        selectedEndDay={selectedEndDay}
                      />
                    </div>
                  </div>
                )} */}


                {/* Live BI graph */}
                {showLiveBI && isCountryMode && (
                  <div className="w-full rounded-2xl border bg-white p-3 lg:p-3 2xl:p-5 shadow-sm overflow-x-hidden">
                    <div className="w-full max-w-full min-w-0">
                      <LiveBiLineGraph
                        dailySeries={biDailySeriesHome}
                        periods={biPeriods}
                        loading={biLoading}
                        error={biError}
                        selectedStartDay={selectedStartDay}
                        selectedEndDay={selectedEndDay}
                        currencySymbol={currencySymbol}
                      />
                    </div>
                  </div>
                )}

              </div>
            )}

            {/* Shopify Block */}
            {!isCountryMode && hasShopifyCard && (
              <div className="flex lg:flex-1">
                <div className="w-full rounded-2xl border bg-white p-5 shadow-sm">
                  <div className="mb-4 flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
                    <div className="flex flex-col">
                      <div className="flex items-baseline gap-2">
                        <PageBreadcrumb
                          pageTitle="Shopify"
                          variant="page"
                          align="left"
                          textSize="2xl"
                        />
                      </div>
                      {/* <p className="mt-1 text-sm text-charcoal-500">
                        Real-time data from Shopify
                      </p> */}
                    </div>
                  </div>

                  {shopifyLoading ? (
                    <div className="mt-3 text-sm text-gray-500">Loading Shopify…</div>
                  ) : shopify ? (
                    <div className="grid grid-cols-2 gap-3 sm:grid-cols-2 lg:grid-cols-3">

                      <AmazonStatCard
                        label="Units"
                        current={shopifyDeriv?.totalOrders ?? 0}
                        previous={shopifyPrevDeriv?.totalOrders ?? 0}
                        loading={shopifyLoading}
                        formatter={fmtInt}
                        bottomLabel={prevLabel}
                        className="border-[#FDD36F] bg-[#FDD36F4D]"
                      />
                      <AmazonStatCard
                        label="Sales"
                        current={convertToDisplayCurrency(shopifyDeriv?.netSales ?? 0, "INR")}
                        previous={convertToDisplayCurrency(shopifyPrevDeriv?.netSales ?? 0, "INR")}
                        loading={shopifyLoading}
                        formatter={formatDisplayAmount}
                        bottomLabel={prevLabel}
                        className="border-[#75BBDA] bg-[#75BBDA4D]"

                      />
                      <AmazonStatCard
                        label="ASP"
                        current={(() => {
                          const units = shopifyDeriv?.totalOrders ?? 0;
                          if (!units) return 0;
                          const net = convertToDisplayCurrency(shopifyDeriv?.netSales ?? 0, "INR");
                          return net / units;
                        })()}
                        previous={0}
                        loading={shopifyLoading}
                        formatter={formatDisplayAmount}
                        bottomLabel={prevLabel}
                        className="border-[#B75A5A] bg-[#B75A5A4D]"
                      />
                    </div>
                  ) : (
                    <div className="mt-2 text-sm text-gray-500">
                      No Shopify data for the current month.
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>

          {/* RIGHT COLUMN – Sales Target */}
          <aside className="col-span-12 lg:col-span-4 order-1 lg:order-2 flex flex-col gap-4 lg:gap-4 2xl:gap-6 h-full">
            <div className="w-full">
              <SalesTargetStatsCard
                regions={regions}
                value={targetRegion}
                onChange={setTargetRegion}
                hideTabs={isCountryMode}
                homeCurrency={displayCurrency}
                formatHomeK={formatDisplayK}
                todayHome={stats_todayHome}
                mtdHome={stats_mtdHome}
                targetHome={stats_targetHome}
                lastMonthTotalHome={stats_lastMonthTotalHome}
                salesTrendPct={stats_salesTrendPct}
                targetTrendPct={stats_targetTrendPct}
                currentReimbursement={reimbursementHome.current}
                previousReimbursement={reimbursementHome.previous}
                targetHome={stats_targetHome}
              />
            </div>

            <div className="w-full lg:sticky lg:top-4 2xl:top-6">
              <SalesTargetCard
                data={targetData}
                regions={regions}
                value={targetRegion}
                onChange={setTargetRegion}
                hideTabs={isCountryMode}
                homeCurrency={displayCurrency}
                convertToHomeCurrency={identityConvert}
                formatHomeK={formatDisplayK}
                todaySales={todaySalesRaw}
                targetHome={stats_targetHome}
                mtdHome={stats_mtdHome}
                lastMonthTotalHome={stats_lastMonthTotalHome}
                currentReimbursement={reimbursementHome.current}
                previousReimbursement={reimbursementHome.previous}
              />
            </div>
          </aside>
        </div>



        {/* ✅ Global-only Performance Trend BELOW top section */}
        {platform === "global" && showLiveBI && (
          // <div className="mt-6 w-full rounded-2xl border bg-white p-4 sm:p-5 shadow-sm overflow-x-hidden">
          <div
            id="targets-action-items"
            className="mt-6 w-full rounded-2xl border bg-white p-4 sm:p-5 shadow-sm overflow-x-hidden scroll-mt-[80px]"
          >
            <div className="w-full max-w-full min-w-0">
              <LiveBiLineGraph
                dailySeries={biDailySeriesHome}
                periods={biPeriods}
                loading={biLoading}
                error={biError}
                selectedStartDay={selectedStartDay}
                selectedEndDay={selectedEndDay}
                currencySymbol={currencySymbol}
              />
            </div>
          </div>
        )}


        {/* Months for BI */}
        {/* <div className="w-full overflow-x-hidden"> */}
        <div id="targets-action-items" className="w-full overflow-x-hidden scroll-mt-[80px]">
          {showLiveBI && (
            <div className="w-full max-w-full min-w-0">
              <MonthsforBI
                countryName={countryName}
                ranged="MTD"
                month={currMonthName.toLowerCase()}
                year={String(currYear)}
                initialData={liveBiPayload}
              />

            </div>
          )}
        </div>

        {/* Lower P&L Graph and Inventory */}
        {hasAnyGraphData && (
          <>
            {/* <div className="mt-6 rounded-2xl border bg-[#D9D9D933] p-5 shadow-sm"> */}
            <div
              id="mtd-pl"
              className="mt-4 rounded-2xl border bg-[#D9D9D933] p-5 shadow-sm scroll-mt-[80px]"
            >

              <div className="mb-3 flex items-center justify-between">
                <div className="text-sm text-charcoal-500">
                  <div className="flex flex-wrap items-baseline gap-2 text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold">
                    <PageBreadcrumb
                      pageTitle="MTD P&L - Amazon"
                      align="left"
                      textSize="2xl"
                      variant="page"
                    />

                    <span className="text-green-500 ">  {countryName.toUpperCase()}</span>
                    <span className="text-charcoal-500 "> -</span>
                    
                    <span className=" text-green-500">  {formattedMonthYear}</span>
                  </div>
                </div>


                {!isCountryMode && (
                  <div className="flex items-center gap-3">
                    <SegmentedToggle<RegionKey>
                      value={graphRegion}
                      options={graphRegions.map((r) => ({ value: r }))}
                      onChange={setGraphRegion}
                    />
                    <DownloadIconButton onClick={handleDownload} />
                  </div>
                )}
              </div>

              <div ref={chartRef} className="overflow-x-hidden">
                <div className="w-full max-w-full min-w-0">

                  <DashboardBargraphCard
                    countryName={countryNameForGraph}
                    formattedMonthYear={formattedMonthYear}
                    currencySymbol={currencySymbol}
                    labels={labels}
                    values={values}
                    colors={colors}
                    loading={loading}
                    allValuesZero={allValuesZero}
                  />
                </div>
              </div>
            </div>

            {amazonIntegrated && graphRegionToUse !== "Global" && (
              <div id="current-inventory" className="scroll-mt-[80px]">
                <CurrentInventorySection region={graphRegionToUse} />
              </div>
            )}

          </>
        )}
      </div>
    </div>
  );


}
