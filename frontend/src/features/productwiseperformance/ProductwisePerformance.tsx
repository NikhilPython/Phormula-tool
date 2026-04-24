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
import {
  APIResponse,
  CountryKey,
  MonthDatum,
  Range,
  formatCountryLabel,
  getCountryColor,
} from "@/components/productwise/productwiseHelpers";
import { useFx, HomeCurrency } from "@/components/dashboard/useFx";
import CountryCard from "@/components/productwise/CountryCard";
import TrendChartSection from "@/components/productwise/TrendChartSection";
import { useGetUserDataQuery } from "@/lib/api/profileApi";
import { useConnectedPlatforms } from "@/lib/utils/useConnectedPlatforms";
import { PlatformId, platformToCountryName } from "@/lib/utils/platforms";
import InsightSideDrawer from "@/components/productwise/InsightSideDrawer";

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
  embedded?: boolean;
  countryNameProp?: string;
  rangeProp?: Range;
  selectedMonthProp?: string;
  selectedQuarterProp?: string;
  selectedYearProp?: number | "";
  initialProductName?: string;
}

const toSlug = (name: string) => encodeURIComponent(name.trim());

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

  if (lower === "global" || lower.startsWith("global_")) return "global";
  if (lower === "uk" || lower.startsWith("uk")) return "uk";
  if (lower === "us") return "us";
  if (lower === "ca") return "ca" as CountryKey;

  return lower as CountryKey;
};

const resolveSourceDataKey = (
  sourceData: Record<string, any>,
  country: string
): string | null => {
  if (!sourceData) return null;

  if (sourceData[country]) return country;

  const normalizedTarget = normalizeCountryKey(country);

  const exactNormalizedMatch = Object.keys(sourceData).find(
    (key) => normalizeCountryKey(key) === normalizedTarget
  );

  return exactNormalizedMatch || null;
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
  country: string,
  range: Range,
  year: number | "",
  quarter?: string,
  month?: string,
  currency?: string
) => {
  const safeYear = year || "na";
  const safeQuarter = quarter || "na";
  const safeMonth = month || "na";
  const safeCurrency = currency || "na";

  return `productwise_insights:${country}:${identifier.toLowerCase()}:${range}:${safeYear}:${safeQuarter}:${safeMonth}:${safeCurrency}`;
};

type BestPerfItem = { month: string; value: number };
type BestPerformance = {
  sales?: BestPerfItem;
  units?: BestPerfItem;
  profit?: BestPerfItem;
};

type SkuInsightExtended = {
  product_name: string;
  insight: string;
  inventory_recommendation?: string;
  objective?: Record<string, any> | null;
  recommendation?: string;
  best_performance?: BestPerformance;
  product_journey?: string[];
};

const computeBestPerformance = (
  monthly: MonthDatum[] = []
): BestPerformance | undefined => {
  if (!monthly.length) return undefined;

  const bestSales = monthly.reduce(
    (best, m) => (m.net_sales > best.net_sales ? m : best),
    monthly[0]
  );
  const bestUnits = monthly.reduce(
    (best, m) => (m.quantity > best.quantity ? m : best),
    monthly[0]
  );
  const bestProfit = monthly.reduce(
    (best, m) => (m.profit > best.profit ? m : best),
    monthly[0]
  );

  return {
    sales: { month: bestSales.month, value: bestSales.net_sales },
    units: { month: bestUnits.month, value: bestUnits.quantity },
    profit: { month: bestProfit.month, value: bestProfit.profit },
  };
};

const findCountryKeyFor = (
  sourceData: Record<string, any>,
  target: string
) => {
  return resolveSourceDataKey(sourceData, target);
};

const getBestPerformanceForCurrentView = ({
  sourceData,
  countryForApi,
  globalKey,
}: {
  sourceData: Record<string, any> | undefined;
  countryForApi: string;
  globalKey: CountryKey;
}): BestPerformance | undefined => {
  if (!sourceData) return undefined;

  const resolvedKey =
    countryForApi === "global"
      ? resolveSourceDataKey(sourceData, globalKey) ||
      resolveSourceDataKey(sourceData, "global")
      : resolveSourceDataKey(sourceData, countryForApi);

  const monthly: MonthDatum[] =
    resolvedKey && Array.isArray(sourceData[resolvedKey])
      ? sourceData[resolvedKey]
      : [];

  return computeBestPerformance(monthly);
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

const DUMMY_PRODUCTWISE_DATA: APIResponse = {
  success: true,
  data: {
    global: [
      { month: "January", net_sales: 0, quantity: 0, profit: 0 },
      { month: "February", net_sales: 0, quantity: 0, profit: 0 },
      { month: "March", net_sales: 0, quantity: 0, profit: 0 },
    ],
    uk: [
      { month: "January", net_sales: 0, quantity: 0, profit: 0 },
      { month: "February", net_sales: 0, quantity: 0, profit: 0 },
      { month: "March", net_sales: 0, quantity: 0, profit: 0 },
    ],
    us: [
      { month: "January", net_sales: 0, quantity: 0, profit: 0 },
      { month: "February", net_sales: 0, quantity: 0, profit: 0 },
      { month: "March", net_sales: 0, quantity: 0, profit: 0 },
    ],
    global_gbp: [],
    global_inr: [],
    global_cad: [],
    ca: [],
    india: [],
  },
};

const ProductwisePerformance: React.FC<ProductwisePerformanceProps> = ({
  embedded = false,
  countryNameProp,
  rangeProp,
  selectedMonthProp,
  selectedQuarterProp,
  selectedYearProp,
  initialProductName = "",
}) => {
  const { homeCurrency, setHomeCurrency, formatHomeAmount } = useFx();
  const { data: userData } = useGetUserDataQuery();
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

  const [isDrawerOpen, setIsDrawerOpen] = useState(false);
  const [insightsLoading, setInsightsLoading] = useState(false);
  const [insightsError, setInsightsError] = useState<string | null>(null);
  const [selectedSku, setSelectedSku] = useState<string | null>(null);
  const [skuInsights, setSkuInsights] = useState<
    Record<string, SkuInsightExtended>
  >({});
  const [activePlatform, setActivePlatform] =
    useState<PlatformId>("global");
  const [data, setData] = useState<APIResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");
  const [selectedCountries, setSelectedCountries] = useState<
    Record<CountryKey, boolean>
  >({} as Record<CountryKey, boolean>);

  const rawSlug = params?.productname as string | undefined;
  const urlProductName = normalizeProductSlug(rawSlug);

  const [selectedProductName, setSelectedProductName] =
    useState(initialProductName);

  const routeCountryName = (params?.countryName as string) || undefined;
  const monthParam = (params?.month as string) || undefined;
  const yearParam = (params?.year as string) || undefined;

  const isPreviewMode =
    String(embedded ? selectedMonthProp : monthParam).toUpperCase() === "NA" &&
    String(embedded ? selectedYearProp : yearParam).toUpperCase() === "NA";

  useEffect(() => {
    if (!embedded) return;

    setSelectedProductName(
      isPreviewMode ? (initialProductName || "Demo Product") : (initialProductName || "")
    );
    setSelectedSku(null);
    setSkuInsights({});
    setIsDrawerOpen(false);
  }, [
    embedded,
    initialProductName,
    rangeProp,
    selectedMonthProp,
    selectedQuarterProp,
    selectedYearProp,
    countryNameProp,
    isPreviewMode,
  ]);



  const productname = isPreviewMode
    ? (embedded ? selectedProductName || initialProductName || "Demo Product" : "Demo Product")
    : embedded
      ? selectedProductName
      : initialProductName || urlProductName || "";



  const countryName = embedded ? countryNameProp : routeCountryName;

  const authToken =
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

  const viewCurrency: HomeCurrency =
    activePlatform === "amazon-uk"
      ? "GBP"
      : activePlatform === "amazon-ca"
        ? "CAD"
        : activePlatform === "amazon-us"
          ? "USD"
          : ((userData?.homeCurrency?.toUpperCase() as HomeCurrency) || "USD");

  useEffect(() => {
    if (typeof window === "undefined") return;
    const saved = window.localStorage.getItem(
      "selectedPlatform"
    ) as PlatformId | null;
    if (
      saved &&
      ["global", "amazon-uk", "amazon-us", "amazon-ca", "shopify"].includes(
        saved
      )
    ) {
      setActivePlatform(saved);
    }
  }, []);

  const platformCountryName = platformToCountryName(activePlatform);



  const profileHomeCurrency = (
    userData?.homeCurrency || "USD"
  ).toUpperCase() as HomeCurrency;

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
          : "global";

  const [internalRange, setInternalRange] = useState<Range>("yearly");

  const [internalSelectedMonth, setInternalSelectedMonth] = useState<string>(
    () => {
      if (monthParam) return monthParam.toLowerCase();

      if (typeof window !== "undefined") {
        const raw = localStorage.getItem("latestFetchedPeriod");
        if (raw) {
          try {
            const parsed = JSON.parse(raw) as {
              month?: string;
              year?: string;
            };
            if (parsed.month) return parsed.month.toLowerCase();
          } catch { }
        }
      }

      return "";
    }
  );

  const [internalSelectedQuarter, setInternalSelectedQuarter] =
    useState<string>("Q1");

  const [internalSelectedYear, setInternalSelectedYear] = useState<
    number | ""
  >(() => {
    if (yearParam) return Number(yearParam);

    if (typeof window !== "undefined") {
      const raw = localStorage.getItem("latestFetchedPeriod");
      if (raw) {
        try {
          const parsed = JSON.parse(raw) as {
            month?: string;
            year?: string;
          };
          if (parsed.year) return Number(parsed.year);
        } catch { }
      }
    }

    return "";
  });

  const range = embedded ? rangeProp ?? "yearly" : internalRange;
  const selectedMonth = embedded
    ? selectedMonthProp ?? ""
    : internalSelectedMonth;
  const selectedQuarter = embedded
    ? selectedQuarterProp ?? "Q1"
    : internalSelectedQuarter;
  const selectedYear = embedded
    ? selectedYearProp ?? ""
    : internalSelectedYear;

  useEffect(() => {
    if (embedded) return;
    if (!internalSelectedMonth) return;

    const idx = months.indexOf(internalSelectedMonth.toLowerCase());
    if (idx >= 0) {
      const q = Math.floor(idx / 3) + 1;
      setInternalSelectedQuarter(`Q${q}`);
    }
  }, [embedded, internalSelectedMonth]);

  useEffect(() => {
    if (!isDrawerOpen || !selectedSku) return;

    const countryForApi = (platformCountryName || "global").toLowerCase();
    const sourceData = isPreviewMode ? DUMMY_PRODUCTWISE_DATA.data : data?.data;

    const freshBestPerformance = getBestPerformanceForCurrentView({
      sourceData: sourceData as Record<string, any> | undefined,
      countryForApi,
      globalKey,
    });

    setSkuInsights((prev) => {
      const current = prev[selectedSku];
      if (!current) return prev;

      return {
        ...prev,
        [selectedSku]: {
          ...current,
          best_performance: freshBestPerformance,
        },
      };
    });
  }, [
    isDrawerOpen,
    selectedSku,
    data,
    globalKey,
    range,
    selectedYear,
    selectedQuarter,
    selectedMonth,
    platformCountryName,
    isPreviewMode,
  ]);

  useEffect(() => {
    setSelectedCountries((prev) => {
      if (Object.keys(prev).length > 0) return prev;

      const initial: Record<string, boolean> = {};

      connectedCountries.forEach((c) => {
        initial[c] = true;
      });

      return initial as Record<CountryKey, boolean>;
    });
  }, [connectedCountries]);

  const years = useMemo(
    () => Array.from({ length: 2 }, (_, i) => new Date().getFullYear() - i),
    []
  );

  useEffect(() => {
    if (embedded) return;

    setInternalRange("yearly");

    if (internalSelectedYear === "" && years.length) {
      setInternalSelectedYear(Math.max(...years));
    }

    setInternalSelectedMonth("");
    setInternalSelectedQuarter("Q1");
  }, [embedded, years, internalSelectedYear]);

  const isProductSelected = !!productname;
  const hasYear = selectedYear !== "" && selectedYear !== undefined;

  const isPeriodComplete =
    (range === "yearly" && hasYear) ||
    (range === "quarterly" && hasYear && !!selectedQuarter) ||
    (range === "monthly" && hasYear && !!selectedMonth);

  const canShowResults = isPreviewMode || (isProductSelected && isPeriodComplete);
  const shouldShowTrendSection = embedded || canShowResults;

  const handleCountryChange = (country: CountryKey) => {
    setSelectedCountries((prev) => ({
      ...prev,
      [country]: !(prev[country] ?? true),
    }));
  };

  const handleProductSelect = (nextProductName: string) => {
    if (embedded) {
      setSelectedProductName(nextProductName);
      return;
    }

    const base = "/skuwiseprofit";
    const slug = toSlug(nextProductName);

    let month = selectedMonth;
    let year = selectedYear;

    if ((!month || !year) && typeof window !== "undefined") {
      try {
        const raw = localStorage.getItem("latestFetchedPeriod");
        if (raw) {
          const parsed = JSON.parse(raw) as {
            month?: string;
            year?: string;
          };
          if (!month && parsed.month) month = parsed.month.toLowerCase();
          if (!year && parsed.year) year = Number(parsed.year);
        }
      } catch { }
    }

    const to = `${base}/${slug}/${countryName ?? ""}/${month || ""}/${year || ""}`;
    router.push(to);
  };

  const handleViewBusinessInsights = async () => {
    const { key: identifier, isSku } = resolveProductKey(
      productname,
      selectedSku
    );

    if (!identifier) return;

    const countryForApi = (platformCountryName || "global").toLowerCase();
    const cacheKey = buildInsightsCacheKey(
      identifier,
      countryForApi,
      range,
      selectedYear,
      selectedQuarter,
      selectedMonth,
      homeCurrency
    );

    setIsDrawerOpen(true);
    setInsightsError(null);

    if (typeof window !== "undefined") {
      const cached = localStorage.getItem(cacheKey);
      if (cached) {
        try {
          const parsed = JSON.parse(cached) as SkuInsightExtended & {
            cachedAt: number;
          };

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

      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductwiseGrowthAI`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${authToken ?? ""}`,
          },
          body: JSON.stringify(payload),
        }
      );

      const json = await res.json().catch(() => null);

      if (!res.ok || !json?.success) {
        throw new Error(json?.error || `HTTP ${res.status}`);
      }

      const returnedName = json.product_name || identifier;
      const insightText = json.ai_insights || "";
      const inventoryRec = json.inventory_recommendation || "";
      const objective = json.objective ?? null;
      const recommendation = json.recommendation || "";
      const productJourney: string[] = Array.isArray(json.product_journey)
        ? json.product_journey
        : [];

      const sourceData = isPreviewMode ? DUMMY_PRODUCTWISE_DATA.data : data?.data;

      const bestPerformance = getBestPerformanceForCurrentView({
        sourceData: sourceData as Record<string, any> | undefined,
        countryForApi,
        globalKey,
      });

      setSkuInsights({
        [identifier]: {
          product_name: returnedName,
          insight: insightText,
          inventory_recommendation: inventoryRec,
          objective,
          recommendation,
          best_performance: bestPerformance,
          product_journey: productJourney,
        },
      });

      setSelectedSku(identifier);

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

  const fetchProductData = async () => {
    if (!canShowResults || isPreviewMode) return;

    setLoading(true);
    setError("");

    try {
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
        home_currency: viewCurrency,
      };

      if (range === "quarterly") {
        const q = (selectedQuarter || "").match(/Q([1-4])/i)?.[1];
        if (q) payload.quarter = q;
      }

      if (range === "monthly") {
        payload.month = selectedMonth;
      }

      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductwisePerformance`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${authToken ?? ""}`,
          },
          body: JSON.stringify(payload),
        }
      );

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

  const nonEmptyCountriesFromApi = useMemo(() => {
    if (!data?.data) return [] as CountryKey[];

    const connectedSet = new Set(
      connectedCountries.map((c) => c.toLowerCase())
    );

    return (Object.entries(data.data) as [string, any][])
      .filter(([country, countryArray]) => {
        const lower = country.toLowerCase();
        const norm = normalizeCountryKey(lower);

        if (norm === "global") return false;
        if (!connectedSet.has(norm)) return false;

        const rows: MonthDatum[] = Array.isArray(countryArray)
          ? (countryArray as MonthDatum[])
          : [];

        return rows.some(
          (m) => m.net_sales !== 0 || m.quantity !== 0 || m.profit !== 0
        );
      })
      .map(([country]) => country as CountryKey);
  }, [data, connectedCountries]);

  const sortByCalendarMonth = (a: string, b: string) => {
    const idxA = months.indexOf(a.toLowerCase());
    const idxB = months.indexOf(b.toLowerCase());

    if (idxA === -1 && idxB === -1) return a.localeCompare(b);
    if (idxA === -1) return 1;
    if (idxB === -1) return -1;

    return idxA - idxB;
  };

  const chartDataList = useMemo(() => {
    const sourceData = isPreviewMode ? DUMMY_PRODUCTWISE_DATA.data : data?.data;

    if (!sourceData) return [null, null, null];

    const allMonthsSet = new Set<string>();
    Object.values(sourceData).forEach((countryArray: any) => {
      const monthly = Array.isArray(countryArray)
        ? (countryArray as MonthDatum[])
        : [];
      monthly.forEach((m) => allMonthsSet.add(m.month));
    });

    const labels = Array.from(allMonthsSet).sort(sortByCalendarMonth);

    const getMetric = (
      country: string,
      month: string,
      metric: keyof MonthDatum
    ) => {
      const resolvedKey = resolveSourceDataKey(sourceData, country);

      if (!resolvedKey) return 0;

      const monthly: MonthDatum[] = Array.isArray(sourceData[resolvedKey as CountryKey])
        ? sourceData[resolvedKey as CountryKey]
        : [];

      const found = monthly.find((m) => m.month === month);

      if (!found) return 0;
      return found[metric] ?? 0;
    };

    const makeDataset = (
      country: CountryKey,
      metric: keyof MonthDatum,
      labelSuffix: string
    ) => {
      const dataSeries = labels.map((month) =>
        getMetric(country, month, metric)
      );

      const normalized = normalizeCountryKey(country);
      const isGlobalSeries = normalized === "global";

      return {
        label: `${formatCountryLabel(normalized)} ${labelSuffix}`,
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
        visibleCountries.push(
          ...connectedCountries.filter((c) => selectedCountries[c] ?? true)
        );
      } else {
        visibleCountries.push(
          ...nonEmptyCountriesFromApi.filter((c) => selectedCountries[c] ?? true)
        );
      }

      const datasets = visibleCountries.map((country) =>
        makeDataset(country, metric, suffix)
      );

      return { labels, datasets };
    });
  }, [
    data,
    nonEmptyCountriesFromApi,
    selectedCountries,
    isPreviewMode,
    connectedCountries,
  ]);

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
      idx = fullNames.findIndex((full) =>
        lower.startsWith(full.slice(0, 3))
      );
    }

    return idx >= 0 ? abbrs[idx] : monthName.slice(0, 3);
  };

  const yAxisLabel = "Units (in nos.)";

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
              // @ts-expect-error Chart.js typing
              const rawLabel = this.getLabelForValue(val) as string;
              return formatAxisMonth(rawLabel);
            },
          },
          grid: {
            display: false,
            drawBorder: false,
          },
        },
        y: {
          title: { display: true, text: yAxisLabel },
          min: 0,
          ticks: { padding: 0 },
        },
      },
    }),
    [formatHomeAmount, yAxisLabel, selectedYear, isPreviewMode]
  );

  const yearShort =
    selectedYear === "" ? "" : selectedYear.toString().slice(-2);

  const getTitle = () => {
    if (range === "yearly") return `${selectedYear}`;
    if (range === "quarterly") return `${selectedQuarter}'${yearShort}`;
    return selectedMonth ? `${cap(selectedMonth)}'${yearShort}` : `Year'${yearShort}`;
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
    if (!isPreviewMode) return;
    setData(DUMMY_PRODUCTWISE_DATA);
    setError("");
    setLoading(false);
  }, [isPreviewMode]);

  const cards = useMemo(() => {
    const sourceData = isPreviewMode ? DUMMY_PRODUCTWISE_DATA.data : data?.data;

    if (!sourceData) return [];

    const connectedSet = new Set(
      connectedCountries.map((c) => c.toLowerCase())
    );

    return Object.entries(sourceData)
      .filter(([country, rawArray]) => {
        const norm = normalizeCountryKey(country);
        const rows = Array.isArray(rawArray) ? rawArray : [];

        if (norm === "global") return false;

        if (isPreviewMode) {
          return (
            connectedSet.has(norm) &&
            rows.some(
              (m: MonthDatum) =>
                m.net_sales !== 0 || m.quantity !== 0 || m.profit !== 0
            )
          );
        }

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
  }, [data, connectedCountries, isPreviewMode]);

  const orderedCards = useMemo(() => {
    if (!cards.length) return [];
    const globals = cards.filter((c) => c.country.startsWith("global"));
    const others = cards.filter((c) => !c.country.startsWith("global"));
    return [...globals, ...others];
  }, [cards]);

  const visibleCountryCards = useMemo(() => {
    const active = normalizeCountryKey((countryName || "global").toLowerCase());

    if (active === "global") return orderedCards;

    return orderedCards.filter(
      (card) => normalizeCountryKey(card.country) === active
    );
  }, [orderedCards, countryName]);

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

  const exportCountryCards = useMemo(() => {
    return visibleCountryCards.map((card) => {
      const norm = normalizeCountryKey(card.country);
      return {
        countryKey: card.country,
        countryLabel: formatCountryLabel(norm).toUpperCase(),
        totalSales: card.stats.totalSales,
        totalUnits: card.stats.totalUnits,
        totalProfit: card.stats.totalProfit,
        avgMonthlySales: card.stats.avgSales,
        avgSellingPrice: card.stats.avgSellingPrice,
        cm1ProfitPct: card.stats.gross_margin_avg,
        bestSalesMonth: card.stats.maxSalesMonth?.month || "",
        bestSalesValue: card.stats.maxSalesMonth?.net_sales ?? 0,
        bestUnitsMonth: card.stats.maxUnitsMonth?.month || "",
        bestUnitsValue: card.stats.maxUnitsMonth?.quantity ?? 0,
        bestProfitMonth: card.stats.maxSalesMonth?.month || "",
        bestProfitValue: card.stats.maxSalesMonth?.profit ?? 0,
      };
    });
  }, [orderedCards]);

  return (
    <div className="w-full">
      {!canShowResults && !embedded && (
        <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
          <div className="flex items-center">
            <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
            <span>
              Search a product and choose the period to view SKU-wise performance.
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

      {shouldShowTrendSection && (
        <div className="flex flex-col">
          <TrendChartSection
            productname={productname}
            title={getTitle()}
            chartDataList={canShowResults ? chartDataList : [null, null, null]}
            chartOptions={chartOptions}
            nonEmptyCountriesFromApi={canShowResults ? nonEmptyCountriesFromApi : []}
            selectedCountries={selectedCountries}
            onToggleCountry={handleCountryChange}
            authToken={authToken}
            onProductSelect={handleProductSelect}
            onViewBusinessInsights={canShowResults ? handleViewBusinessInsights : undefined}
            insightsLoading={insightsLoading}
            isPreviewMode={isPreviewMode}
            exportMeta={{
              titleLine: `${productname} - Productwise Performance - ${getHeadingPeriod()}`,
              titleCountry: exportTitleCountry,
              platformLabel: "Phormula",
              periodLabel: getHeadingPeriod(),
              companyName: userData?.company_name || "",
              brandName: userData?.brand_name || "",
              currencyLabel: exportCurrencySymbol,
            }}
            exportCountryCards={canShowResults ? exportCountryCards : []}
          />

          {canShowResults && data && !loading && (
            <>
              <InsightSideDrawer
                open={isDrawerOpen}
                selectedSku={selectedSku}
                skuInsights={skuInsights}
                onClose={() => setIsDrawerOpen(false)}
                enableFeedback={false}
                selectedYear={selectedYear}
                homeCurrency={homeCurrency}
                drawerPeriodText={getHeadingPeriod()}
              />

              {isDrawerOpen && insightsError && (
                <div className="fixed right-6 top-16 z-[9999] rounded bg-red-50 px-3 py-2 shadow text-sm text-red-700">
                  {insightsError}
                </div>
              )}

              <div className="mt-4">
                <div className="grid gap-4 grid-cols-1 md:grid-cols-[repeat(auto-fit,minmax(260px,1fr))]">
                  {visibleCountryCards.map((card) => (
                    <CountryCard
                      key={card.country.toLowerCase()}
                      country={card.country}
                      stats={card.stats}
                      selectedYear={selectedYear}
                      homeCurrency={homeCurrency}
                      activeCountry={(countryName || "global").toLowerCase()}
                    />
                  ))}
                </div>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
};

export default ProductwisePerformance;