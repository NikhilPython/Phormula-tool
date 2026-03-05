"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import "@/lib/chartSetup";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title as ChartTitle,
  Tooltip,
  Legend,
  Filler,
} from "chart.js";

import Loader from "@/components/loader/Loader";

// REMOVE GBP_TO_USD_RATE and formatCurrencyByCountry here
import {
  APIResponse,
  CountryKey,
  MonthDatum,
  Range,
  formatCountryLabel,
  getCountryColor,
  monthOrder,
} from "@/components/productwise/productwiseHelpers";
import { useFx, HomeCurrency, FromCurrency } from "@/components/dashboard/useFx";
import CountryCard from "@/components/productwise/CountryCard";
import TrendChartSection from "@/components/productwise/TrendChartSection";
import ProductwiseHeader from "@/components/productwise/ProductwiseHeader";
import FiltersAndSearchRow from "@/components/productwise/FiltersAndSearchRow";
import { useGetUserDataQuery } from "@/lib/api/profileApi";
import { useConnectedPlatforms } from "@/lib/utils/useConnectedPlatforms";   // 👈 add this
import {
  PlatformId,
  platformToCountryName,
} from "@/lib/utils/platforms";
import InsightSideDrawer, { SkuInsight } from "@/components/productwise/InsightSideDrawer";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  ChartTitle,
  Tooltip,
  Legend,
  Filler
);

interface ProductwisePerformanceProps {
  productname?: string;
}

/* ---------- URL-safe helpers (NO "plus" text conversion) ---------- */
const toSlug = (name: string) => encodeURIComponent(name.trim());

const fromSlug = (slug: string) => decodeURIComponent(slug);

const normalizeProductSlug = (slug?: string) => {
  if (!slug) return undefined;
  try {
    const decoded = decodeURIComponent(slug);
    return decoded.trim() || undefined;
  } catch {
    return slug.trim() || undefined;
  }
};

const normalizeCountryKey = (key: string): CountryKey => {
  const lower = key.toLowerCase();

  if (lower.startsWith("global")) return "global";
  if (lower.startsWith("uk")) return "uk";
  if (lower.startsWith("us")) return "us";
  if (lower.startsWith("ca")) return "ca" as CountryKey;

  return lower as CountryKey;
};

const months = [
  "january",
  "february",
  "march",
  "april",
  "may",
  "june",
  "july",
  "august",
  "september",
  "october",
  "november",
  "december",
];

const cap = (s: string) => (s ? s[0].toUpperCase() + s.slice(1) : "");

const getLatestFetchedMonth = (): string | null => {
  if (typeof window === "undefined") return null;
  try {
    const raw = localStorage.getItem("latestFetchedPeriod");
    if (!raw) return null;
    const parsed = JSON.parse(raw) as { month?: string; year?: string };
    return parsed.month ? parsed.month.toLowerCase() : null;
  } catch {
    return null;
  }
};

const resolveProductKey = (
  productName: string | null | undefined,
  sku: string | null | undefined
): { key: string; isSku: boolean } => {
  if (productName && productName.trim()) {
    return { key: productName.trim(), isSku: false };
  }
  if (sku && sku.trim()) {
    return { key: sku.trim(), isSku: true };
  }
  return { key: "", isSku: false };
};


const buildInsightsCacheKey = (
  identifier: string,
  country: string
) => `productwise_insights:${country}:${identifier.toLowerCase()}`;

type BestPerfItem = { month: string; value: number };
type BestPerformance = { sales?: BestPerfItem; units?: BestPerfItem; profit?: BestPerfItem };

const computeBestPerformance = (monthly: MonthDatum[] = []): BestPerformance | undefined => {
  if (!monthly.length) return undefined;

  const bestSales = monthly.reduce((best, m) => (m.net_sales > best.net_sales ? m : best), monthly[0]);
  const bestUnits = monthly.reduce((best, m) => (m.quantity > best.quantity ? m : best), monthly[0]);
  const bestProfit = monthly.reduce((best, m) => (m.profit > best.profit ? m : best), monthly[0]);

  return {
    sales: { month: bestSales.month, value: bestSales.net_sales },
    units: { month: bestUnits.month, value: bestUnits.quantity },
    profit: { month: bestProfit.month, value: bestProfit.profit },
  };
};

const findCountryKeyFor = (sourceData: Record<string, any>, target: string) => {
  const t = target.toLowerCase();
  const keys = Object.keys(sourceData || {});
  return keys.find((k) => normalizeCountryKey(k) === t) || null;
};

const currencySymbolFromCode = (code: string) => {
  const c = (code || "").toUpperCase();
  if (c === "USD") return "$";
  if (c === "GBP") return "£";
  if (c === "EUR") return "€";
  if (c === "CAD") return "C$";
  if (c === "INR") return "₹";
  return c;
};

const ProductwisePerformance: React.FC<ProductwisePerformanceProps> = ({
  productname: propProductName,
}) => {
  const { homeCurrency, setHomeCurrency, convertToHomeCurrency, formatHomeAmount } =
    useFx();
  const { data: userData } = useGetUserDataQuery();

  // 🔹 which Amazon marketplaces are actually connected?
  const connectedPlatforms = useConnectedPlatforms();

  const connectedCountries = useMemo<CountryKey[]>(() => {
    const arr: CountryKey[] = [];
    if (connectedPlatforms.amazonUk) arr.push("uk");
    if (connectedPlatforms.amazonUs) arr.push("us");
    if (connectedPlatforms.amazonCa) arr.push("ca" as CountryKey);
    return arr;
  }, [connectedPlatforms]);

  const params = useParams();
  const router = useRouter();

  // -----------------------
  // Insights Drawer state
  // -----------------------
  const [isDrawerOpen, setIsDrawerOpen] = useState(false);
  const [insightsLoading, setInsightsLoading] = useState(false);
  const [insightsError, setInsightsError] = useState<string | null>(null);

  const [selectedSku, setSelectedSku] = useState<string | null>(null);
  type BestPerfItem = { month: string; value: number };
  type BestPerformance = { sales?: BestPerfItem; units?: BestPerfItem; profit?: BestPerfItem };

  type SkuInsightExtended = {
    product_name: string;
    insight: string;

    inventory_recommendation?: string;
    objective?: Record<string, any> | null;
    recommendation?: string;
    best_performance?: BestPerformance;
    product_journey?: string[];
  };

  const [skuInsights, setSkuInsights] = useState<Record<string, SkuInsightExtended>>({});


  const handleViewBusinessInsights = async () => {
    const { key: identifier, isSku } = resolveProductKey(productname, selectedSku);

    if (!identifier) return;

    const countryForApi = (platformCountryName || "global").toLowerCase();
    const cacheKey = buildInsightsCacheKey(identifier, countryForApi);

    // 1️⃣ Open drawer immediately
    setIsDrawerOpen(true);
    setInsightsError(null);

    // 2️⃣ Cache check
    if (typeof window !== "undefined") {
      const cached = localStorage.getItem(cacheKey);
      if (cached) {
        try {
          const parsed = JSON.parse(cached) as (SkuInsightExtended & { cachedAt: number });


          setSkuInsights({
            [identifier]: {
              product_name: parsed.product_name,
              insight: parsed.insight,
              inventory_recommendation: parsed.inventory_recommendation,
              objective: parsed.objective ?? null,
              recommendation: parsed.recommendation,
              best_performance: parsed.best_performance,
              product_journey: parsed.product_journey ?? [],
            },
          });

          setSelectedSku(identifier);
          setInsightsLoading(false);
          return;
        } catch {
          localStorage.removeItem(cacheKey);
        }
      }
    }

    // 3️⃣ No cache → call API
    setInsightsLoading(true);

    try {
      const payload: any = {
        country: countryForApi,
      };

      if (isSku) {
        payload.sku = identifier;
      } else {
        payload.product_name = identifier;
      }

      const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductwiseGrowthAI`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${authToken ?? ""}`,
        },
        body: JSON.stringify(payload),
      });

      const json = await res.json().catch(() => null);

      if (!res.ok || !json?.success) {
        throw new Error(json?.error || `HTTP ${res.status}`);
      }

      const returnedName = json.product_name || identifier;
      const insightText = json.ai_insights || "";

      // NEW fields from API
      const inventoryRec = json.inventory_recommendation || "";
      const objective = json.objective ?? null;
      const recommendation = json.recommendation || "";
      const productJourney: string[] = Array.isArray(json.product_journey)
  ? json.product_journey
  : [];

      // Best performance from already fetched ProductwisePerformance data
      let bestPerformance: any = undefined;
      const sourceData = isPreviewMode ? DUMMY_PRODUCTWISE_DATA.data : data?.data;

      if (sourceData) {
        const key =
          countryForApi === "global"
            ? globalKey
            : (findCountryKeyFor(sourceData as any, countryForApi) as any);

        const monthly: MonthDatum[] = key ? (sourceData as any)[key] : [];
        bestPerformance = computeBestPerformance(Array.isArray(monthly) ? monthly : []);
      }

      setSkuInsights({
        [identifier]: {
          product_name: returnedName,
          insight: insightText,

          // NEW
          inventory_recommendation: inventoryRec,
          objective,
          recommendation,
          best_performance: bestPerformance,
          product_journey: productJourney,
        },
      });

      setSelectedSku(identifier);

      // 4️⃣ Save cache
      localStorage.setItem(
        cacheKey,
        JSON.stringify({
          product_name: returnedName,
          insight: insightText,

          inventory_recommendation: inventoryRec,
          objective,
          recommendation,
          best_performance: bestPerformance,
          product_journey: productJourney,

          cachedAt: Date.now(),
        })
      );

    } catch (e: any) {
      console.error("Growth AI Error:", e);
      setInsightsError(e?.message || "Failed to load insights");
    } finally {
      setInsightsLoading(false);
    }
  };

  // NEW: active platform, default "global"
  const [activePlatform, setActivePlatform] = useState<PlatformId>("global");

  // pick currency we want to show on this page
  const viewCurrency: HomeCurrency =
    activePlatform === "amazon-uk"
      ? "GBP"
      : activePlatform === "amazon-ca"
        ? "CAD"
        : activePlatform === "amazon-us"
          ? "USD"
          : (userData?.homeCurrency?.toUpperCase() as HomeCurrency) || "USD";


  useEffect(() => {
    if (typeof window === "undefined") return;
    const saved = window.localStorage.getItem("selectedPlatform") as PlatformId | null;
    if (
      saved &&
      ["global", "amazon-uk", "amazon-us", "amazon-ca", "shopify"].includes(saved)
    ) {
      setActivePlatform(saved);
    }
  }, []);

  const platformCountryName = platformToCountryName(activePlatform); // "global" | "uk" | "us" | "ca"


  const rawSlug = params?.productname as string | undefined;
  const urlProductName = normalizeProductSlug(rawSlug);

  const countryName = (params?.countryName as string) || undefined;
  const monthParam = (params?.month as string) || undefined;
  const yearParam = (params?.year as string) || undefined;

  const DUMMY_PRODUCTWISE_DATA: APIResponse = {
    success: true,
    data: {
      global: [
        { month: "January", net_sales: 42000, quantity: 320, profit: 9800 },
        { month: "February", net_sales: 46000, quantity: 350, profit: 11200 },
        { month: "March", net_sales: 52000, quantity: 380, profit: 13500 },
      ],
      uk: [
        { month: "January", net_sales: 18000, quantity: 140, profit: 4200 },
        { month: "February", net_sales: 20000, quantity: 150, profit: 5100 },
        { month: "March", net_sales: 23000, quantity: 170, profit: 6400 },
      ],
      us: [
        { month: "January", net_sales: 24000, quantity: 180, profit: 5600 },
        { month: "February", net_sales: 26000, quantity: 200, profit: 6100 },
        { month: "March", net_sales: 29000, quantity: 210, profit: 7100 },
      ],
      global_gbp: [],
      global_inr: [],
      global_cad: [],
      ca: [],
      india: []
    },
  };


  const isPreviewMode =
    monthParam?.toUpperCase() === "NA" &&
    yearParam?.toUpperCase() === "NA";

  const productname = propProductName || urlProductName || "";

  const [data, setData] = useState<APIResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");

  const authToken =
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

  // ---------- derive homeCurrency from profile ----------
  const profileHomeCurrency = (userData?.homeCurrency || "USD").toUpperCase() as HomeCurrency;

  // useEffect(() => {
  //   if (profileHomeCurrency && profileHomeCurrency !== homeCurrency) {
  //     setHomeCurrency(profileHomeCurrency);
  //   }
  // }, [profileHomeCurrency, homeCurrency, setHomeCurrency]);

  // ✅ keep useFx's homeCurrency in sync with viewCurrency
  useEffect(() => {
    if (viewCurrency && viewCurrency !== homeCurrency) {
      setHomeCurrency(viewCurrency);
    }
  }, [viewCurrency, homeCurrency, setHomeCurrency]);


  const globalKey: CountryKey =
    viewCurrency === "GBP"
      ? "global_gbp"
      : viewCurrency === "INR"
        ? "global_inr"
        : viewCurrency === "CAD"
          ? "global_cad"
          : "global"; // USD


  // ---------- controls ----------
  const [range, setRange] = useState<Range>("yearly");

  const [selectedMonth, setSelectedMonth] = useState<string>(() => {
    if (monthParam) return monthParam.toLowerCase();

    if (typeof window !== "undefined") {
      const raw = localStorage.getItem("latestFetchedPeriod");
      if (raw) {
        try {
          const parsed = JSON.parse(raw) as { month?: string; year?: string };
          if (parsed.month) return parsed.month.toLowerCase();
        } catch {
          /* ignore */
        }
      }
    }

    return "";
  });

  const [selectedQuarter, setSelectedQuarter] = useState<string>("Q1");

  useEffect(() => {
    if (!selectedMonth) return;
    const idx = months.indexOf(selectedMonth.toLowerCase());
    if (idx >= 0) {
      const q = Math.floor(idx / 3) + 1;
      setSelectedQuarter(`Q${q}`);   // october -> "Q4"
    }
  }, [selectedMonth]);



  const [selectedYear, setSelectedYear] = useState<number | "">(() => {
    if (yearParam) return Number(yearParam);

    if (typeof window !== "undefined") {
      const raw = localStorage.getItem("latestFetchedPeriod");
      if (raw) {
        try {
          const parsed = JSON.parse(raw) as { month?: string; year?: string };
          if (parsed.year) return Number(parsed.year);
        } catch {
          /* ignore */
        }
      }
    }

    return "";
  });

  const [selectedCountries, setSelectedCountries] = useState<
    Record<CountryKey, boolean>
  >({} as Record<CountryKey, boolean>);

  // Seed toggles once we know globalKey + connected countries
  useEffect(() => {
    setSelectedCountries((prev) => {
      // if user already toggled something, keep it
      if (Object.keys(prev).length > 0) return prev;

      const initial: Record<CountryKey, boolean> = {
        [globalKey]: true,
      } as Record<CountryKey, boolean>;

      connectedCountries.forEach((c) => {
        initial[c] = true;
      });

      return initial;
    });
  }, [globalKey, connectedCountries]);


  const years = useMemo(
    () => Array.from({ length: 2 }, (_, i) => new Date().getFullYear() - i),
    []
  );

  // ✅ default to YEARLY + set default year once
  useEffect(() => {
    // force yearly only once (in case URL param set etc.)
    setRange("yearly");

    // if year not selected, pick latest available
    if (selectedYear === "" && years?.length) {
      setSelectedYear(Math.max(...years));
    }

    // optional: also seed quarter/month empty
    setSelectedMonth("");
    setSelectedQuarter("Q1");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const isProductSelected = !!productname;
  const hasYear = selectedYear !== "" && selectedYear !== undefined;

  const isPeriodComplete =
    (range === "yearly" && hasYear) ||
    (range === "quarterly" && hasYear && !!selectedQuarter) ||
    (range === "monthly" && hasYear && !!selectedMonth);

  const canShowResults = isProductSelected && isPeriodComplete;

  /* ---------- country toggle ---------- */
  const handleCountryChange = (country: CountryKey) => {
    setSelectedCountries((prev) => ({
      ...prev,
      [country]: !(prev[country] ?? true),
    }));
  };

  /* ---------- product select handler ---------- */
  const handleProductSelect = (selectedProductName: string) => {
    const base = "/skuwiseprofit";
    const slug = toSlug(selectedProductName);

    let month = selectedMonth;
    let year = selectedYear;

    if ((!month || !year) && typeof window !== "undefined") {
      try {
        const raw = localStorage.getItem("latestFetchedPeriod");
        if (raw) {
          const parsed = JSON.parse(raw) as { month?: string; year?: string };
          if (!month && parsed.month) month = parsed.month.toLowerCase();
          if (!year && parsed.year) year = Number(parsed.year);
        }
      } catch {
        // ignore
      }
    }

    const to = `${base}/${slug}/${countryName ?? ""}/${month || ""}/${year || ""}`;
    router.push(to);
  };

  /* ---------- fetch data ---------- */
  const fetchProductData = async () => {
    if (!canShowResults || isPreviewMode) return;

    setLoading(true);
    setError("");

    try {
      // 🔹 always include globalKey + only *connected* country codes
      const countries: string[] = [globalKey, ...connectedCountries];

      const backendTimeRange =
        range === "yearly"
          ? "Yearly"
          : range === "quarterly"
            ? "Quarterly"
            : "Monthly";

      const payload: any = {
        product_name: productname,
        time_range: backendTimeRange,
        year: selectedYear,
        countries,
        home_currency: viewCurrency,  // 👈 now depends on selected platform
      };


      // if (range === "quarterly") {
      //   // selectedQuarter is like "Q1", "Q2", "Q3", "Q4"
      //   payload.quarter = selectedQuarter.replace("Q", ""); // "Q4" -> "4"
      // }

      if (range === "quarterly") {
        const q = (selectedQuarter || "").match(/Q([1-4])/i)?.[1]; // "4"
        if (q) payload.quarter = q;
      }


      if (range === "monthly") {
        payload.month = selectedMonth;
      }

      const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductwisePerformance`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${authToken ?? ""}`,
        },
        body: JSON.stringify(payload),
      });

      const json: APIResponse | any = await res.json().catch(() => null);

      if (!res.ok || !json?.success) {
        const errMsg =
          (json && (json.error || json.message)) ||
          `HTTP error! status: ${res.status}`;
        throw new Error(errMsg);
      }

      setData(json as APIResponse);
    } catch (e: any) {
      console.error("API Error:", e);
      setError(e?.message || "Failed to fetch data from server");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!canShowResults) return;
    fetchProductData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    productname,
    selectedYear,
    range,
    selectedQuarter,
    selectedMonth,
    canShowResults,
    globalKey,
    profileHomeCurrency,
  ]);

  /* ---------- non-empty countries (excluding globalKey) ---------- */
  const nonEmptyCountriesFromApi = useMemo(() => {
    if (!data?.data) return [] as CountryKey[];

    const connectedSet = new Set(
      connectedCountries.map((c) => c.toLowerCase())
    );

    return (Object.entries(data.data) as [string, any][])
      .filter(([country, countryArray]) => {
        const lower = country.toLowerCase();
        const norm = normalizeCountryKey(lower); // 👈

        // skip global* rows here – they’re handled separately as globalKey
        if (norm === "global") return false;

        // only show if the platform is actually connected
        if (!connectedSet.has(norm)) return false;

        const rows: MonthDatum[] = Array.isArray(countryArray)
          ? (countryArray as MonthDatum[])
          : [];

        return rows.some(
          (m) => m.net_sales !== 0 || m.quantity !== 0 || m.profit !== 0
        );
      })
      // we return the backend key ("uk_usd" or "uk") so data.data[country] works
      .map(([country]) => country as CountryKey);
  }, [data, globalKey, connectedCountries]);

  // helper: sort "January", "january", etc. by calendar month Jan–Dec
  const sortByCalendarMonth = (a: string, b: string) => {
    const idxA = months.indexOf(a.toLowerCase());
    const idxB = months.indexOf(b.toLowerCase());

    // If both not found, keep alphabetical
    if (idxA === -1 && idxB === -1) return a.localeCompare(b);
    // Unknown months go to the end
    if (idxA === -1) return 1;
    if (idxB === -1) return -1;

    return idxA - idxB; // Jan (0) → Dec (11)
  };


  /* ---------- chart data (Last 12 Months + FX) ---------- */
  const chartDataList = useMemo(() => {
    const sourceData = isPreviewMode
      ? DUMMY_PRODUCTWISE_DATA.data
      : data?.data;

    if (!sourceData) return [null, null, null];

    // 1) Collect all months we have from the API
    const allMonthsSet = new Set<string>();
    Object.values(sourceData).forEach((countryArray: any) => {
      const monthly = Array.isArray(countryArray)
        ? (countryArray as MonthDatum[])
        : [];
      monthly.forEach((m) => allMonthsSet.add(m.month));
    });

    const labels = Array.from(allMonthsSet).sort(sortByCalendarMonth);

    const getMetric = (
      country: CountryKey,
      month: string,
      metric: keyof MonthDatum
    ) => {
      const countryArr = (sourceData as any)[country];
      const monthly: MonthDatum[] = Array.isArray(countryArr)
        ? countryArr
        : [];
      const found = monthly.find((m) => m.month === month);

      if (!found) return 0;

      // ✅ Backend has already converted everything into home_currency.
      return found[metric];
    };


    const makeDataset = (
      country: CountryKey,
      metric: keyof MonthDatum,
      labelSuffix: string
    ) => {
      const dataSeries = labels.map((month) =>
        getMetric(country, month, metric)
      );

      const normalized = normalizeCountryKey(country);  // "uk_usd" -> "uk"

      const isGlobalSeries = normalized === "global";

      return {
        label: `${formatCountryLabel(normalized)} ${labelSuffix}`, // "UK Net Sales"
        data: dataSeries,
        borderColor: getCountryColor(normalized),
        backgroundColor: getCountryColor(normalized),
        tension: 0.1,
        pointRadius: 3,
        fill: false,
        borderDash: isGlobalSeries ? [6, 4] : [],
        borderWidth: 2,
        order: isGlobalSeries ? 99 : 0,
      };
    };
    const metrics: { metric: keyof MonthDatum; suffix: string }[] = [
      { metric: "net_sales", suffix: "Net Sales" },
      { metric: "quantity", suffix: "Quantity" },
      { metric: "profit", suffix: "Profit" },
    ];

    return metrics.map(({ metric, suffix }) => {
      const visibleCountries: CountryKey[] = [];

      if (isPreviewMode) {
        // ✅ PREVIEW MODE: hardcode correct keys
        visibleCountries.push("global");
        visibleCountries.push("uk");
      } else {
        // 🔹 NORMAL MODE (existing logic)
        if (selectedCountries[globalKey] ?? true) {
          visibleCountries.push(globalKey);
        }

        visibleCountries.push(
          ...nonEmptyCountriesFromApi.filter(
            (c) => selectedCountries[c] ?? true
          )
        );
      }

      const datasets = visibleCountries.map((country) =>
        makeDataset(country, metric, suffix)
      );

      return { labels, datasets };
    });
  }, [
    data,
    globalKey,
    nonEmptyCountriesFromApi,
    selectedCountries,
  ]);

  // helper to format "October" -> "Oct"  ✅ (NO YEAR)
  const formatAxisMonth = (monthName: string) => {
    if (!monthName) return "";

    const fullNames = [
      "january",
      "february",
      "march",
      "april",
      "may",
      "june",
      "july",
      "august",
      "september",
      "october",
      "november",
      "december",
    ];

    const abbrs = [
      "Jan",
      "Feb",
      "Mar",
      "Apr",
      "May",
      "Jun",
      "Jul",
      "Aug",
      "Sep",
      "Oct",
      "Nov",
      "Dec",
    ];

    const lower = monthName.toLowerCase();
    let idx = fullNames.indexOf(lower);

    if (idx === -1) {
      idx = fullNames.findIndex((full) => lower.startsWith(full.slice(0, 3)));
    }

    return idx >= 0 ? abbrs[idx] : monthName.slice(0, 3);
  };



  /* ---------- chart options (currency-aware) ---------- */
  const yAxisLabel = (() => {
    switch (homeCurrency) {
      case "GBP":
        return "Amount (£)";
      case "INR":
        return "Amount (₹)";
      case "CAD":
        return "Amount (CA$)";
      default:
        return "Amount ($)";
    }
  })();


  const chartOptions = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (context: any) => {
              const value = context.parsed.y as number;
              const datasetLabel = context.dataset.label as string;
              const metricPart = (
                datasetLabel
                  .split(" ")
                  .slice(1)
                  .join(" ") || ""
              ).toLowerCase();

              if (
                metricPart.includes("quantity") ||
                metricPart.includes("units")
              ) {
                return `${datasetLabel}: ${value.toLocaleString()}`;
              }

              // 💰 still using home-currency formatter you already have
              if (isPreviewMode) {
                return `${datasetLabel}: ${value.toLocaleString()}`;
              }
              return `${datasetLabel}: ${formatHomeAmount(value)}`;
            },
          },
        },
      },
      scales: {
        x: {
          title: { display: false, text: "Month" },
          ticks: {
            callback: function (val: any) {
              // "this" is the X scale; get the raw label ("October", etc.)
              // @ts-expect-error -- Chart.js scale context typing is incorrect for getLabelForValue
              const rawLabel = this.getLabelForValue(val) as string;
              return formatAxisMonth(rawLabel);
            },
          },
        },
        y: {
          title: { display: true, text: yAxisLabel },
          min: 0,
          ticks: { padding: 0 },
        },
      },
    }),
    [formatHomeAmount, yAxisLabel, selectedYear]
  );



  const yearShort =
    selectedYear === "" ? "" : selectedYear.toString().slice(-2);

  // const getTitle = () => {
  //   if (range === "yearly") return `Last 12 Months'${yearShort}`;
  //   if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
  //   return selectedMonth
  //     ? `${cap(selectedMonth)}'${yearShort}`
  //     : `Year'${yearShort}`;
  // };

  const getTitle = () => {
  if (range === "yearly") return `${selectedYear}`;
    if (range === "quarterly") return `${selectedQuarter}'${yearShort}`; // selectedQuarter is "Q4"
    return selectedMonth
      ? `${cap(selectedMonth)}'${yearShort}`
      : `Year'${yearShort}`;
  };

  const getHeadingPeriod = () => {
  if (range === "yearly") return `${selectedYear}`;
    if (range === "quarterly") return `${selectedQuarter}'${yearShort}`;
    if (range === "monthly" && selectedMonth) {
      return `${cap(selectedMonth)}'${yearShort}`;
    }
    return "";
  };

  useEffect(() => {
    if (isPreviewMode) {
      setData(DUMMY_PRODUCTWISE_DATA);
    }
  }, [isPreviewMode]);

  /* ---------- summary cards ---------- */
  const cards = useMemo(() => {
    const sourceData = isPreviewMode
      ? DUMMY_PRODUCTWISE_DATA.data
      : data?.data;

    if (!sourceData) return [];

    const connectedSet = new Set(
      connectedCountries.map((c) => c.toLowerCase())
    );

    return Object.entries(sourceData)
      // keep global + only connected real countries (but normalised)
      .filter(([country, rawArray]) => {
        const norm = normalizeCountryKey(country);
        const rows = Array.isArray(rawArray) ? rawArray : [];

        // 🔥 PREVIEW MODE: only GLOBAL + UK with data
        if (isPreviewMode) {
          return (
            (norm === "global" || norm === "uk") &&
            rows.some(
              (m: MonthDatum) =>
                m.net_sales !== 0 || m.quantity !== 0 || m.profit !== 0
            )
          );
        }

        // 🔹 NORMAL MODE (existing logic)
        if (norm === "global") return true;
        return connectedSet.has(norm);
      })

      .map(([country, rawArray]) => {
        const backendKey = country.toLowerCase();
        const normKey = normalizeCountryKey(backendKey);

        const monthly: MonthDatum[] = Array.isArray(rawArray)
          ? (rawArray as MonthDatum[])
          : [];

        const totalSales = monthly.reduce((s, m) => s + m.net_sales, 0);
        const totalProfit = monthly.reduce((s, m) => s + m.profit, 0);
        const totalUnits = monthly.reduce((s, m) => s + m.quantity, 0);

        const gross_margin_avg =
          totalSales > 0 ? (totalProfit / totalSales) * 100 : 0;

        const monthsWithSales = monthly.filter((m) => m.net_sales > 0);
        const avgSales =
          monthsWithSales.length > 0 ? totalSales / monthsWithSales.length : 0;

        const avgSellingPrice = totalUnits > 0 ? totalSales / totalUnits : 0;
        const avgMonthlyProfit =
          monthly.length > 0 ? totalProfit / monthly.length : 0;

        const maxSalesMonth =
          monthly.length > 0
            ? monthly.reduce((max, m) =>
              m.net_sales > max.net_sales ? m : max
            )
            : { month: "", net_sales: 0, quantity: 0, profit: 0 };

        const maxUnitsMonth =
          monthly.length > 0
            ? monthly.reduce((max, m) =>
              m.quantity > max.quantity ? m : max
            )
            : { month: "", net_sales: 0, quantity: 0, profit: 0 };

        const isConnected =
          normKey === "global" || connectedSet.has(normKey);

        return {
          country: backendKey,
          stats: {
            totalSales,
            totalProfit,
            totalUnits,
            gross_margin_avg,
            avgSales,
            avgSellingPrice,
            avgMonthlyProfit,
            maxSalesMonth,
            maxUnitsMonth,
          },
          isConnected,
        };
      });
  }, [data, connectedCountries]);


  const orderedCards = useMemo(() => {
    if (!cards.length) return [];
    const globals = cards.filter((c) => c.country.startsWith("global"));
    const others = cards.filter((c) => !c.country.startsWith("global"));
    return [...globals, ...others];
  }, [cards]);

  const exportCurrencySymbol = useMemo(
    () => currencySymbolFromCode(homeCurrency),
    [homeCurrency]
  );

  const exportTitleCountry = useMemo(() => {
    const c = (platformCountryName || "global").toLowerCase();
    if (c === "uk") return "UK";
    if (c === "us") return "US";
    if (c === "ca") return "CA";
    return "Global";
  }, [platformCountryName]);

  // This is what Sheet 1 should show (same tiles as CountryCard)
  const exportCountryCards = useMemo(() => {
    return orderedCards.map((card) => {
      const norm = normalizeCountryKey(card.country); // "global", "uk", "us", ...
      return {
        countryKey: card.country, // backend key, keep if needed
        countryLabel: formatCountryLabel(norm).toUpperCase(), // "GLOBAL", "UK", etc

        // top tiles
        totalSales: card.stats.totalSales,
        totalUnits: card.stats.totalUnits,
        totalProfit: card.stats.totalProfit,

        avgMonthlySales: card.stats.avgSales,
        avgSellingPrice: card.stats.avgSellingPrice,
        cm1ProfitPct: card.stats.gross_margin_avg, // this is % number already (0-100)

        // best performance tiles (already computed)
        bestSalesMonth: card.stats.maxSalesMonth?.month || "",
        bestSalesValue: card.stats.maxSalesMonth?.net_sales ?? 0,

        bestUnitsMonth: card.stats.maxUnitsMonth?.month || "",
        bestUnitsValue: card.stats.maxUnitsMonth?.quantity ?? 0,

        // profit best month is not in your stats currently; you have maxSalesMonth and maxUnitsMonth only.
        // If you want best profit too, compute it in cards memo and add it to stats.
        // For now, use maxSalesMonth.profit as a fallback.
        bestProfitMonth: card.stats.maxSalesMonth?.month || "",
        bestProfitValue: card.stats.maxSalesMonth?.profit ?? 0,
      };
    });
  }, [orderedCards]);

  /* ---------- render ---------- */
  return (
    <div className="w-full">
      {/* Header + filters row */}
      {/* 🔒 STICKY HEADER */}
      <div className="sticky top-0 z-40 w-full flex flex-col
  bg-[#F7F7F7]
  sm:flex-row md:items-center md:justify-between gap-1 sm:gap-4
  border-b border-gray-200">
        {/* <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between mb-2"> */}
        <ProductwiseHeader
          canShowResults={canShowResults}
          countryName={countryName}
          productname={productname}
          headingPeriod={getHeadingPeriod()}
        />

        <FiltersAndSearchRow
          range={range}
          selectedMonth={selectedMonth}
          selectedQuarter={selectedQuarter}
          selectedYear={selectedYear}
          years={years}
          onRangeChange={setRange}
          onMonthChange={setSelectedMonth}
          onQuarterChange={(val) => {
            setSelectedQuarter(val || "Q1");
          }}
          onYearChange={(val) => {
            setSelectedYear(val ? Number(val) : "");
          }}
        />
        {/* </div> */}
      </div>

      {/* ⬇️ margin moved HERE */}
      <div className="mb-6 mt-5" />


      {!canShowResults && (
        <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
          <div className="flex items-center">
            <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
            <span>
              Search a product and choose the period to view SKU-wise
              performance.
            </span>
          </div>
        </div>
      )}

      {canShowResults && loading && (
        <div className="flex flex-col items-center justify-center py-12 textcenter">
          <Loader fullscreen transparent />
        </div>
      )}

      {canShowResults && !!error && (
        <div className="mb-8 rounded-2xl border border-red-200 bg-red-50 p-6">
          <div className="flex items-center gap-3 text-red-700">
            <span className="text-xl">❌</span>
            <p className="m-0 font-medium">{error}</p>
          </div>
        </div>
      )}

      {canShowResults && data && !loading && (
        <div className="flex flex-col">

          {/* <TrendChartSection
            productname={productname}
            title={getTitle()}
            chartDataList={chartDataList}
            chartOptions={chartOptions}
            nonEmptyCountriesFromApi={nonEmptyCountriesFromApi}
            selectedCountries={selectedCountries}
            onToggleCountry={handleCountryChange}
            authToken={authToken}
            onProductSelect={handleProductSelect}
            onViewBusinessInsights={handleViewBusinessInsights}
            insightsLoading={insightsLoading}
            isPreviewMode={isPreviewMode}
          /> */}

          <TrendChartSection
            productname={productname}
            title={getTitle()}
            chartDataList={chartDataList}
            chartOptions={chartOptions}
            nonEmptyCountriesFromApi={nonEmptyCountriesFromApi}
            selectedCountries={selectedCountries}
            onToggleCountry={handleCountryChange}
            authToken={authToken}
            onProductSelect={handleProductSelect}
            onViewBusinessInsights={handleViewBusinessInsights}
            insightsLoading={insightsLoading}
            isPreviewMode={isPreviewMode}

            // ✅ NEW: sheet-1 payload + header meta
            exportMeta={{
              titleLine: `${productname} - Productwise Performance - ${getHeadingPeriod()}`,
              titleCountry: exportTitleCountry,
              platformLabel: "Amazon", // or derive from activePlatform if you want
              periodLabel: getHeadingPeriod(),
              companyName: userData?.company_name || "",  // adjust to your actual field name
              brandName: userData?.brand_name || "",      // adjust to your actual field name
              currencyLabel: exportCurrencySymbol,
            }}
            exportCountryCards={exportCountryCards}
          />

          <InsightSideDrawer
            open={isDrawerOpen}
            selectedSku={selectedSku}
            skuInsights={skuInsights}
            onClose={() => setIsDrawerOpen(false)}
            enableFeedback={false}
          />

          {isDrawerOpen && insightsError && (
            <div className="fixed right-6 top-16 z-[9999] rounded bg-red-50 px-3 py-2 shadow text-sm text-red-700">
              {insightsError}
            </div>
          )}

          <div className="mt-8">
            <div className="grid gap-5 grid-cols-1 md:grid-cols-[repeat(auto-fit,minmax(260px,1fr))]">
              {orderedCards.map((card) => {
                const key = card.country.toLowerCase();

                return (
                  <CountryCard
                    key={key}
                    country={card.country}
                    stats={card.stats}
                    selectedYear={selectedYear}
                    homeCurrency={homeCurrency}
                    activeCountry={(countryName || "global").toLowerCase()}
                  />
                );
              })}

            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default ProductwisePerformance;
