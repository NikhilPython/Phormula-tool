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
import ProductJourneyInlineGraph from "@/components/businessInsight/ProductJourneyInlineGraph";
import ProductSearchDropdown from "@/components/products/ProductSearchDropdown";

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
  sharedInsightData?: SharedInsightData;
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
  units?: BestPerfItem;
  sales?: BestPerfItem;
  asp?: BestPerfItem;
  profit?: BestPerfItem;
  unitWiseProfitability?: BestPerfItem;
};

type SkuInsightExtended = {
  product_name: string;
  sku?: string;
  insight: string;
  inventory_recommendation?: string;
  objective?: Record<string, any> | null;
  recommendation?: string;
  best_performance?: BestPerformance;
  product_journey?: string[];
  metrics?: ProductInsightMetric[];
  isOtherSkus?: boolean;
  includedSkus?: {
    product_name: string;
    sku: string;
  }[];
};

type ProductInsightMetric = {
  label: string;
  value: string;
  color?: string;
};

type ProductInsightBlockForDrawer = {
  name: string;
  skuKey?: string;
  metrics: ProductInsightMetric[];
  journeyBullets: string[];
  recommendationBullets: string[];
  inventoryBullets: string[];
  isOtherSkus?: boolean;
  includedSkus?: {
    product_name: string;
    sku: string;
  }[];
};

type SharedInsightData = {
  blocks: ProductInsightBlockForDrawer[];
  objective?: Record<string, any> | null;
  recommendationsMap?: Record<string, any>;
  drawerPeriodText?: string;
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
  sharedInsightData,
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

  const [isDrawerOpen, setIsDrawerOpen] = useState(embedded);
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

  const productname = isPreviewMode
    ? (embedded ? selectedProductName || initialProductName || "Demo Product" : "Demo Product")
    : embedded
      ? selectedProductName || ""
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

  const currentInsight = skuInsights[selectedSku];

  // ✅ already loaded, don't call again
  if (currentInsight?.best_performance) return;

  const productName =
    currentInsight?.product_name ||
    productname ||
    selectedSku;

  if (!productName) return;

  const ac = new AbortController();

  const loadBestPerformance = async () => {
    try {
      const countryForApi = (
        platformCountryName ||
        countryName ||
        "global"
      ).toLowerCase();

      const bestPerformance = await fetchProductBestPerformance({
        productName,
        country: countryForApi,
        homeCurrency: viewCurrency,
        signal: ac.signal,
      });

      setSkuInsights((prev) => {
        const current = prev[selectedSku];

        if (!current) return prev;

        // ✅ protect again inside setter
        if (current.best_performance) return prev;

        return {
          ...prev,
          [selectedSku]: {
            ...current,
            best_performance: bestPerformance,
          },
        };
      });
    } catch (e: any) {
      if (e?.name === "AbortError") return;
      console.error("ProductBestPerformance Error:", e);
    }
  };

  loadBestPerformance();

  return () => ac.abort();
}, [
  isDrawerOpen,
  selectedSku,
  productname,
  platformCountryName,
  countryName,
  viewCurrency,
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

  const normalizeTextKey = (value: string) =>
    String(value || "")
      .toLowerCase()
      .trim()
      .replace(/\s+/g, " ")
      .replace(/[^\w\s-]/g, "");

  const blockToSkuInsight = (
    block: ProductInsightBlockForDrawer,
    recObj: any,
    fallbackObjective?: Record<string, any> | null,
    bestPerformance?: BestPerformance
  ): SkuInsightExtended => {
    const recommendationText =
      recObj?.recommendation ||
      block.recommendationBullets?.join(" ") ||
      "";

    const inventoryText =
      recObj?.inventory_recommendation ||
      block.inventoryBullets?.join(" ") ||
      "";

    return {
      product_name: block.name,
      sku: block.skuKey || "",
      insight: block.journeyBullets?.join("\n") || "",
      recommendation: recommendationText,
      inventory_recommendation: inventoryText,
      objective: recObj?.objective ?? fallbackObjective ?? null,
      product_journey: block.journeyBullets || [],
      best_performance: bestPerformance,
      metrics: block.metrics || [],
      isOtherSkus: block.isOtherSkus,
      includedSkus: block.includedSkus || [],
    };
  };

  const findSharedInsightBlock = (
    identifier: string,
    shared?: SharedInsightData
  ) => {
    if (!identifier || !shared?.blocks?.length) return null;

    const normalizedIdentifier = normalizeTextKey(identifier);

    return (
      shared.blocks.find(
        (b) =>
          String(b.skuKey || "").trim().toLowerCase() ===
          identifier.trim().toLowerCase()
      ) ||
      shared.blocks.find(
        (b) => normalizeTextKey(b.name) === normalizedIdentifier
      ) ||
      null
    );
  };

  

  const getSharedRecObj = (
    block: ProductInsightBlockForDrawer,
    shared?: SharedInsightData
  ) => {
    const map = shared?.recommendationsMap as any;

    if (!map) return null;

    const skuActions =
      map?.sku_actions ??
      map?.recommendations ??
      map ??
      {};

    return (
      (block.skuKey && skuActions[block.skuKey]) ||
      skuActions[block.name] ||
      skuActions[block.name?.trim?.()] ||
      null
    );
  };

  const handleInlineProductSelect = async (nextProductName: string) => {
  const cleanProductName = String(nextProductName || "").trim();
  if (!cleanProductName) return;

  setSelectedProductName(cleanProductName);
  setInsightsError(null);
  setIsDrawerOpen(true);

  const sharedBlock = findSharedInsightBlock(cleanProductName, sharedInsightData);

  if (sharedBlock) {
    const key =
      sharedBlock.skuKey ||
      sharedBlock.name ||
      cleanProductName;

    const recObj = getSharedRecObj(sharedBlock, sharedInsightData);

    setSelectedSku(key);

    setSkuInsights((prev) => ({
      ...prev,
      [key]: blockToSkuInsight(
        sharedBlock,
        recObj,
        sharedInsightData?.objective,
        prev[key]?.best_performance
      ),
    }));

    setInsightsLoading(false);
    return;
  }

  setInsightsLoading(true);

  try {
    const countryForApi = (
      platformCountryName ||
      countryName ||
      countryNameProp ||
      "global"
    ).toLowerCase();

    const res = await fetch(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductwiseGrowthAI`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${authToken ?? ""}`,
        },
        body: JSON.stringify({
          country: countryForApi,
          product_name: cleanProductName,
        }),
      }
    );

    const json = await res.json().catch(() => null);

    if (!res.ok || !json?.success) {
      throw new Error(json?.error || `HTTP ${res.status}`);
    }

    const key = cleanProductName;
    const returnedName = json.product_name || cleanProductName;

    setSelectedSku(key);

    setSkuInsights((prev) => ({
      ...prev,
      [key]: {
        product_name: returnedName,
        sku: "",
        insight: json.ai_insights || "",
        inventory_recommendation: json.inventory_recommendation || "",
        objective: json.objective ?? null,
        recommendation: json.recommendation || "",
        best_performance: prev[key]?.best_performance,
        product_journey: Array.isArray(json.product_journey)
          ? json.product_journey
          : [],
        metrics: [],
      },
    }));
  } catch (e: any) {
    console.error("Inline product search insight error:", e);
    setInsightsError(e?.message || "Failed to load selected product insight");
  } finally {
    setInsightsLoading(false);
  }
};

 useEffect(() => {
  if (!embedded) return;
  if (!sharedInsightData?.blocks?.length) return;

  const selectedBlock =
    sharedInsightData.blocks.find(
      (b) =>
        normalizeTextKey(b.name) === normalizeTextKey(initialProductName || "")
    ) ||
    sharedInsightData.blocks.find((b) => !b.isOtherSkus) ||
    sharedInsightData.blocks[0];

  if (!selectedBlock) return;

  const key =
    selectedBlock.skuKey ||
    selectedBlock.name ||
    initialProductName ||
    "selected-product";

  // ✅ same product hai to state dobara set nahi hogi
  if (selectedSku && skuInsights[selectedSku]) return;

  const recObj = getSharedRecObj(selectedBlock, sharedInsightData);

  setSelectedSku(key);

  setSelectedProductName((prev) =>
    prev === selectedBlock.name ? prev : selectedBlock.name
  );

  setSkuInsights((prev) => {
    if (prev[key]) return prev;

    return {
      ...prev,
      [key]: blockToSkuInsight(
        selectedBlock,
        recObj,
        sharedInsightData.objective,
        undefined
      ),
    };
  });

  setInsightsError(null);
  setInsightsLoading(false);
  setIsDrawerOpen(true);
}, [
  embedded,
  sharedInsightData?.blocks?.length,
  initialProductName,
  selectedSku,
]);

 const mapApiBestPerformanceToDrawerShape = (apiBestPerformance: any): BestPerformance | undefined => {
  if (!apiBestPerformance) return undefined;

  return {
    units: apiBestPerformance?.units
      ? {
          month: String(apiBestPerformance.units.month || ""),
          value: Number(apiBestPerformance.units.units ?? 0),
        }
      : undefined,

    sales: apiBestPerformance?.net_sales
      ? {
          month: String(apiBestPerformance.net_sales.month || ""),
          value: Number(apiBestPerformance.net_sales.net_sales ?? 0),
        }
      : undefined,

    asp: apiBestPerformance?.asp
      ? {
          month: String(apiBestPerformance.asp.month || ""),
          value: Number(apiBestPerformance.asp.asp ?? 0),
        }
      : undefined,

    profit: apiBestPerformance?.cm1_profit
      ? {
          month: String(apiBestPerformance.cm1_profit.month || ""),
          value: Number(apiBestPerformance.cm1_profit.cm1_profit ?? 0),
        }
      : undefined,

    unitWiseProfitability: apiBestPerformance?.unit_wise_profitability
      ? {
          month: String(apiBestPerformance.unit_wise_profitability.month || ""),
          value: Number(
            apiBestPerformance.unit_wise_profitability.unit_wise_profitability ?? 0
          ),
        }
      : undefined,
  };
};
  const fetchProductBestPerformance = async ({
    productName,
    country,
    homeCurrency,
    signal,
  }: {
    productName: string;
    country: string;
    homeCurrency?: string;
    signal?: AbortSignal;
  }): Promise<BestPerformance | undefined> => {
    const token =
      typeof window !== "undefined"
        ? localStorage.getItem("jwtToken")
        : null;

    if (!token) throw new Error("Missing token");

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
          country,
          home_currency: homeCurrency,
        }),
        cache: "no-store",
        signal,
      }
    );

    const json = await res.json().catch(() => ({}));

    if (!res.ok) {
      throw new Error(json?.error || "Failed to fetch best performance");
    }

    return mapApiBestPerformanceToDrawerShape(json?.best_performance);
  };

  const splitMetricValue = (value: string) => {
  const v = (value || "").trim();
  const m = v.match(/^(.+?)\s*(\(([-+])[^)]+\))\s*$/);

  if (!m) return { main: v, delta: "", deltaColor: "" };

  const main = m[1].trim();
  const delta = m[2].trim();
  const sign = m[3];

  return {
    main,
    delta,
    deltaColor: sign === "+" ? "#5EA68E" : "#FF5C5C",
  };
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

const getMetricBorderColorByLabel = (label: string, fallbackIndex = 0) => {
  const normalizedLabel = label.trim().toLowerCase();
  const metricIndex = metricOrder.indexOf(normalizedLabel);

  return metricColors[
    metricIndex !== -1 ? metricIndex : fallbackIndex % metricColors.length
  ];
};

const formatPerfMonth = (month?: string) => {
  if (!month) return "-";

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

  const lower = month.toLowerCase();
  let idx = fullNames.indexOf(lower);

  if (idx === -1) {
    idx = fullNames.findIndex((m) => lower.startsWith(m.slice(0, 3)));
  }

  const shortMonth = idx >= 0 ? abbrs[idx] : month;
  const shortYear =
    selectedYear !== undefined && selectedYear !== ""
      ? String(selectedYear).slice(-2)
      : "";

  return shortYear ? `${shortMonth}'${shortYear}` : shortMonth;
};

const formatPerfValue = (label: string, value?: number) => {
  if (typeof value !== "number") return "-";

  const lower = label.toLowerCase();

  if (lower === "units") {
    return Math.round(value).toLocaleString();
  }

  if (lower.includes("asp") || lower.includes("per unit")) {
    return `${currencySymbolFromCode(homeCurrency)}${Number(value).toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })}`;
  }

  return `${currencySymbolFromCode(homeCurrency)}${Math.round(value).toLocaleString()}`;
};

const renderInlineAiInsightSection = () => {

  if (!selectedSku) return null;

  const insightData = skuInsights[selectedSku];
  if (!insightData) return null;

  const sortedMetrics = [...(insightData.metrics || [])].sort((a, b) => {
    const aIndex = metricOrder.indexOf(a.label.trim().toLowerCase());
    const bIndex = metricOrder.indexOf(b.label.trim().toLowerCase());

    const safeAIndex = aIndex === -1 ? Number.MAX_SAFE_INTEGER : aIndex;
    const safeBIndex = bIndex === -1 ? Number.MAX_SAFE_INTEGER : bIndex;

    return safeAIndex - safeBIndex;
  });

  const bestPerformanceCards = [
  {
    label: "Units",
    data: insightData.best_performance?.units,
  },
  {
    label: "Net Sales",
    data: insightData.best_performance?.sales,
  },
  {
    label: "ASP",
    data: insightData.best_performance?.asp,
  },
  {
    label: "CM1 Profit",
    data: insightData.best_performance?.profit,
  },
  {
    label: "CM1 Profit Per Unit",
    data: insightData.best_performance?.unitWiseProfitability,
  },
];

  const journeyBullets = Array.isArray(insightData.product_journey)
    ? insightData.product_journey
    : [];

 const filteredJourneyBullets = journeyBullets;

  const displaySku = insightData.sku || selectedSku || "-";

  const graphProductName = insightData.isOtherSkus
  ? insightData.includedSkus?.[0]?.product_name ||
    insightData.product_name ||
    selectedSku
  : insightData.product_name || selectedSku;

const graphCountryName = String(
  countryName ||
    platformCountryName ||
    countryNameProp ||
    "global"
)
  .trim()
  .toLowerCase();

const graphYear =
  selectedYear !== "" && selectedYear !== undefined
    ? Number(selectedYear)
    : new Date().getFullYear();

  return (
    <div className="">
      <div className="mb-4 flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
        <div className="min-w-0">
          <div className="text-[11px] font-semibold uppercase tracking-wide text-charcoal-400">
            Product Name
          </div>

         <div className="flex flex-wrap items-center gap-3">
  <span className="text-base font-bold text-[#414042] sm:text-xl lg:text-lg 2xl:text-2xl">
    {insightData.product_name || selectedSku}
  </span>

  {sharedInsightData?.drawerPeriodText || getHeadingPeriod() ? (
    <span className="text-base font-bold text-green-500 sm:text-xl lg:text-lg 2xl:text-2xl">
      {sharedInsightData?.drawerPeriodText || getHeadingPeriod()}
    </span>
  ) : null}

  
</div>
        </div>

        <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
          <div className="w-full sm:w-72">
    <ProductSearchDropdown
      authToken={authToken}
      onProductSelect={handleInlineProductSelect}
    />
  </div>
        </div>
      </div>

      {sortedMetrics.length > 0 && (
        <div>
          <div className="mb-2 text-xs font-semibold text-charcoal-700 sm:text-sm 2xl:text-lg">
            Metrics
          </div>

          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-5 ">
            {sortedMetrics.map((m, i) => (
              <div
                key={`${m.label}-${i}`}
                className={`rounded-lg border bg-white shadow-sm border-t-4 ${getMetricBorderColorByLabel(
                  m.label,
                  i
                )} px-3 py-2`}
              >
                <div className="text-[10px] text-charcoal-400 2xl:text-xs">
                  {m.label
                    .replace(/\b\w/g, (char) => char.toUpperCase())
                    .replace("Cm1", "CM1")}
                </div>

                <div className="flex flex-col leading-tight">
                  {(() => {
                    const { main, delta, deltaColor } = splitMetricValue(m.value);
                    const displayMain = formatRecommendationCardMainValue(
                      m.label,
                      main
                    );

                    return (
                      <>
                        <span className="text-sm font-bold 2xl:text-lg text-[#414042]">
                          {displayMain}
                        </span>

                        {delta && (
                          <span
                            className="text-[10px] 2xl:text-xs font-semibold"
                            style={{ color: deltaColor }}
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
      )}

      {insightData.best_performance && (
        <div className="mt-5">
          <div className="mb-1 text-xs sm:text-sm 2xl:text-lg font-semibold text-charcoal-700">
            Overall Best Performance
          </div>

          <div className="mb-2 text-[11px] text-charcoal-400 2xl:text-xs">
            Best performance is calculated from overall historical data.
          </div>

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-5">
            {bestPerformanceCards.map((card, index) => (
              <div
                key={card.label}
                className={`rounded-lg border bg-white shadow-sm border-t-4 ${getMetricBorderColorByLabel(
                  card.label,
                  index
                )} px-3 py-2`}
              >
                <div className="text-[10px] 2xl:text-xs text-charcoal-400">
                  {card.label}
                </div>

                <div className="mt-1 text-[10px] 2xl:text-xs text-[#414042]">
                  {formatPerfMonth(card.data?.month)}
                </div>

                <div className=" text-sm 2xl:text-lg font-bold text-[#414042]">
                  {formatPerfValue(card.label, card.data?.value)}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="mt-5">
       <ProductJourneyInlineGraph
  productname={graphProductName || ""}
  countryName={graphCountryName}
  displayCurrency={homeCurrency as any}
  isOtherSkus={!!insightData.isOtherSkus}
  otherSkuProductNames={
    insightData.isOtherSkus
      ? (insightData.includedSkus || []).map((item) => item.product_name)
      : []
  }
/>
      </div>

      <div className="mt-5 pb-2 w-full rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="mb-2 text-xs font-semibold text-charcoal-700 sm:text-sm 2xl:text-lg">
          Product Journey
        </div>

        {filteredJourneyBullets.length > 0 ? (
          <ol className="list-decimal pl-4 space-y-1 text-xs text-charcoal-500 2xl:text-sm marker:font-semibold marker:text-charcoal-400">
            {filteredJourneyBullets.map((item, index) => (
              <li key={index}>{item}</li>
            ))}
          </ol>
        ) : (
          <div className="text-xs 2xl:text-sm text-charcoal-500">—</div>
        )}
      </div>
    </div>
  );
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

    const sharedBlock = findSharedInsightBlock(identifier, sharedInsightData);

    if (sharedBlock) {

      const sharedRecObj = getSharedRecObj(sharedBlock, sharedInsightData);

      setSkuInsights({
        [identifier]: blockToSkuInsight(
          sharedBlock,
          sharedRecObj,
          sharedInsightData?.objective,
          undefined
        ),
      });

      setSelectedSku(identifier);
      setInsightsLoading(false);
      return;
    }

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
              sku: parsed.sku || identifier,
              insight: parsed.insight,
              inventory_recommendation: parsed.inventory_recommendation,
              objective: parsed.objective ?? null,
              recommendation: parsed.recommendation,
              best_performance: undefined,
              product_journey: parsed.product_journey ?? [],
              metrics: parsed.metrics ?? [],
              isOtherSkus: parsed.isOtherSkus,
              includedSkus: parsed.includedSkus ?? [],
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

      setSkuInsights({
  [identifier]: {
    product_name: returnedName,
    sku: isSku ? identifier : "",
    insight: insightText,
    inventory_recommendation: inventoryRec,
    objective,
    recommendation,
    best_performance: undefined,
    product_journey: productJourney,
    metrics: [],
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
          best_performance: undefined,
          product_journey: productJourney,
          cachedAt: Date.now(),
          sku: isSku ? identifier : "",
          metrics: [],

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
  if (embedded) return;
  if (!canShowResults) return;

  fetchProductData();
}, [
  embedded,
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

  const activeCountryKey = useMemo(() => {
    return normalizeCountryKey((countryName || "global").toLowerCase());
  }, [countryName]);

  const isGlobalPage = activeCountryKey === "global";

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

    let labels = Array.from(allMonthsSet).sort(sortByCalendarMonth);

    // ✅ In yearly view, show only months that are actually fetched from API.
    // This will include May if May exists in API data.
    if (range === "yearly") {
      const fetchedMonthIndexes: number[] = [];

      Object.values(sourceData).forEach((countryArray: any) => {
        const monthly = Array.isArray(countryArray)
          ? (countryArray as MonthDatum[])
          : [];

        monthly.forEach((m) => {
          const idx = months.indexOf(String(m.month || "").toLowerCase());

          const hasFetchedData =
            Number(m.net_sales || 0) !== 0 ||
            Number(m.quantity || 0) !== 0 ||
            Number(m.profit || 0) !== 0;

          if (idx >= 0 && hasFetchedData) {
            fetchedMonthIndexes.push(idx);
          }
        });
      });

      let latestIdx =
        fetchedMonthIndexes.length > 0
          ? Math.max(...fetchedMonthIndexes)
          : -1;

      // fallback only if API data has no non-zero months
      if (latestIdx < 0) {
        const selectedIdx = months.indexOf(
          String(selectedMonth || "").toLowerCase()
        );

        if (selectedIdx >= 0) {
          latestIdx = selectedIdx;
        }
      }

      if (latestIdx >= 0) {
        labels = labels.filter((month) => {
          const monthIdx = months.indexOf(String(month || "").toLowerCase());
          return monthIdx >= 0 && monthIdx <= latestIdx;
        });
      }
    }

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
      let availableCountries: CountryKey[] = [];

      if (isPreviewMode) {
        availableCountries = connectedCountries;
      } else {
        availableCountries = nonEmptyCountriesFromApi;
      }

      const visibleCountries: CountryKey[] = isGlobalPage
        ? availableCountries.filter((c) => selectedCountries[c] ?? true)
        : availableCountries.filter(
          (c) => normalizeCountryKey(c) === activeCountryKey
        );
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
    activeCountryKey,
    isGlobalPage,
    range,
    selectedMonth,
    selectedYear,
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
          suggestedMin: 0,
          grace: "10%",
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

        const hasData = rows.some(
          (m: MonthDatum) =>
            m.net_sales !== 0 || m.quantity !== 0 || m.profit !== 0
        );

        // ✅ Show GLOBAL card only on global page
        if (norm === "global") {
          return isGlobalPage && hasData;
        }

        // ✅ Preview: only show connected countries with data
        if (isPreviewMode) {
          return connectedSet.has(norm) && hasData;
        }

        // ✅ Normal: show connected country cards like UK / US / CA
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

        const maxProfitMonth =
          monthly.length > 0
            ? monthly.reduce((max, m) =>
              m.profit > max.profit ? m : max
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
            maxSalesMonth,
            maxUnitsMonth,
            maxProfitMonth,
          },
          isConnected,
        };
      });
  }, [
    data,
    connectedCountries,
    isPreviewMode,
    isGlobalPage,
  ]);

  const orderedCards = useMemo(() => {
    if (!cards.length) return [];
    const globals = cards.filter((c) => c.country.startsWith("global"));
    const others = cards.filter((c) => !c.country.startsWith("global"));
    return [...globals, ...others];
  }, [cards]);

  const visibleCountryCards = useMemo(() => {
    if (isGlobalPage) return orderedCards;

    return orderedCards.filter(
      (card) => normalizeCountryKey(card.country) === activeCountryKey
    );
  }, [orderedCards, activeCountryKey, isGlobalPage]);

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
        bestProfitMonth: card.stats.maxProfitMonth?.month || "",
        bestProfitValue: card.stats.maxProfitMonth?.profit ?? 0,
      };
    });
  }, [visibleCountryCards]);

  const isMultiCountry = visibleCountryCards.length > 1;

 return (
  <div className="w-full space-y-4">
    {insightsError && (
      <div className="rounded-lg border border-red-100 bg-red-50 px-3 py-2 text-sm text-red-700">
        {insightsError}
      </div>
    )}

    {insightsLoading && (
      <div className="rounded-xl border border-slate-200 bg-white p-6">
        <Loader fullscreen={false} transparent />
      </div>
    )}

    {!insightsLoading && renderInlineAiInsightSection()}

    {!selectedSku && !sharedInsightData?.blocks?.length && (
      <div className="rounded-xl border border-slate-200 bg-white p-6 text-sm text-charcoal-500">
        No SKU-wise insight data available for this period.
      </div>
    )}
  </div>
);
};

export default ProductwisePerformance;