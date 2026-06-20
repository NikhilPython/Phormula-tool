"use client";

import React, { useEffect, useMemo, useState } from "react";
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from "chart.js";
import zoomPlugin from "chartjs-plugin-zoom";

import Loader from "@/components/loader/Loader";
import SegmentedToggle from "@/components/ui/SegmentedToggle";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
  zoomPlugin
);

type CountryKey = "uk" | "global" | "us" | "ca";
type TrendTab = "sales_cm1" | "units_asp" | "mix" | "inventory_units";
type CurrencyCode = "USD" | "GBP" | "INR" | "CAD";

interface ProductMetricPoint {
  month: string;
  net_sales: number;
  cm1_profit: number;
  units_sold: number;
  asp: number;
  sales_mix: number;
  profit_mix: number;
  inventory_units: number;
}

interface ApiMonthRow {
  month: string;
  net_sales?: number;
  cm1_profit?: number;
  cm1?: number;
  profit?: number;
  quantity?: number;
  units_sold?: number;
  units?: number;
  asp?: number;
  sales_mix?: number;
  profit_mix?: number;
  inventory_units?: number;
}

interface ApiResponse {
  success: boolean;
  data?: Record<string, any>;
  other_skus_graph_data?: Record<string, any>;
  message?: string;
  error?: string;
}

interface ProductJourneyInlineGraphProps {
  productname?: string;
  countryName?: string;
  sourceCountryName?: string;
  displayCurrency?: CurrencyCode;
  isOtherSkus?: boolean;
  otherSkuProductNames?: string[];
}

const ProductJourneyInlineGraph: React.FC<ProductJourneyInlineGraphProps> = ({
  productname,
  countryName = "global",
  displayCurrency,
  isOtherSkus = false,
  otherSkuProductNames = [],
}) => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [activeTab, setActiveTab] = useState<TrendTab>("sales_cm1");
  const [isDraggingChart, setIsDraggingChart] = useState(false);

  const [selectedCountries, setSelectedCountries] = useState<Record<CountryKey, boolean>>({
    uk: true,
    global: true,
    us: false,
    ca: false,
  });

  const [journeyData, setJourneyData] = useState<Record<CountryKey, ProductMetricPoint[]>>({
    uk: [],
    global: [],
    us: [],
    ca: [],
  });

  const scope = (countryName || "global").toLowerCase();

  useEffect(() => {
    if (scope === "uk") {
      setSelectedCountries({ uk: true, global: false, us: false, ca: false });
    } else if (scope === "global") {
      setSelectedCountries({ uk: false, global: true, us: false, ca: false });
    } else if (scope === "us") {
      setSelectedCountries({ uk: false, global: false, us: true, ca: false });
    } else if (scope === "ca") {
      setSelectedCountries({ uk: false, global: false, us: false, ca: true });
    } else {
      setSelectedCountries({ uk: true, global: true, us: true, ca: true });
    }
  }, [scope]);

  const chartCurrency: CurrencyCode = useMemo(() => {
    if (displayCurrency) return displayCurrency;

    if (scope === "uk") return "GBP";
    if (scope === "us") return "USD";
    if (scope === "ca") return "CAD";
    if (scope === "india") return "INR";

    return "USD";
  }, [displayCurrency, scope]);

  const currencySymbol = useMemo(() => {
    switch (chartCurrency) {
      case "GBP":
        return "£";
      case "USD":
        return "$";
      case "CAD":
        return "C$";
      case "INR":
        return "₹";
      default:
        return "$";
    }
  }, [chartCurrency]);

  const getMetricColor = (metric: string) => {
    const colors: Record<string, string> = {
      net_sales: "#75BBDA",
      cm1_profit: "#7B9A6D",
      units_sold: "#FDD36F",
      asp: "#B75A5A",
      sales_mix: "#3A8EA4",
      profit_mix: "#ED9F50",
      inventory_units: "#7B9A6D",
    };

    return colors[metric] || "#6b7280";
  };

  const formatCurrency = (value: number) => {
    const locale =
      chartCurrency === "GBP"
        ? "en-GB"
        : chartCurrency === "CAD"
          ? "en-CA"
          : chartCurrency === "INR"
            ? "en-IN"
            : "en-US";

    return new Intl.NumberFormat(locale, {
      style: "currency",
      currency: chartCurrency,
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(Number(value || 0));
  };

  const formatUnits = (value: number) =>
    new Intl.NumberFormat("en-US", {
      maximumFractionDigits: 0,
    }).format(Number(value || 0));

  const formatAsp = (value: number) => {
    const locale =
      chartCurrency === "GBP"
        ? "en-GB"
        : chartCurrency === "CAD"
          ? "en-CA"
          : chartCurrency === "INR"
            ? "en-IN"
            : "en-US";

    return new Intl.NumberFormat(locale, {
      style: "currency",
      currency: chartCurrency,
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(Number(value || 0));
  };

  const formatPercent = (value: number) => `${Number(value || 0).toFixed(2)}%`;

  const monthShort = (d: Date) => d.toLocaleString("en-US", { month: "short" });
  const monthLabel = (d: Date) => `${monthShort(d)}'${String(d.getFullYear()).slice(-2)}`;

  const monthNameToIndex: Record<string, number> = {
    january: 0,
    february: 1,
    march: 2,
    april: 3,
    may: 4,
    june: 5,
    july: 6,
    august: 7,
    september: 8,
    october: 9,
    november: 10,
    december: 11,
  };

  const backendKeysFor = (country: CountryKey) => {
    if (country === "uk") return ["uk"];
    if (country === "us") return ["us"];
    if (country === "ca") return ["ca"];

    if (country === "global") {
      if (chartCurrency === "GBP") return ["global_gbp", "global"];
      if (chartCurrency === "USD") return ["global", "global_gbp"];
      if (chartCurrency === "CAD") return ["global_cad", "global", "global_gbp"];
      if (chartCurrency === "INR") return ["global_inr", "global", "global_gbp"];
      return ["global", "global_gbp"];
    }

    return [country];
  };

  const normalizeRows = (countryBlock: any): ApiMonthRow[] => {
    if (!countryBlock) return [];
    if (Array.isArray(countryBlock)) return countryBlock;
    if (Array.isArray(countryBlock?.Yearly)) return countryBlock.Yearly;

    const firstArray = Object.values(countryBlock).find((v) => Array.isArray(v));
    return Array.isArray(firstArray) ? (firstArray as ApiMonthRow[]) : [];
  };

  const otherSkuKey = useMemo(
    () => otherSkuProductNames.map((x) => String(x || "").trim()).filter(Boolean).join("|"),
    [otherSkuProductNames]
  );

  useEffect(() => {
    const cleanProductName = String(productname || "").trim();

    if (!cleanProductName) {
      setLoading(false);
      setError("");
      setJourneyData({ uk: [], global: [], us: [], ca: [] });
      return;
    }

    const ac = new AbortController();

    const fetchJourneyData = async () => {
  setLoading(true);
  setError("");
  setJourneyData({ uk: [], global: [], us: [], ca: [] });

      try {
        const token =
          typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        if (!token) throw new Error("Missing token");

        const today = new Date();
        const currentYear = today.getFullYear();
        const START_YEAR = 2023;

        const countriesToRequest: CountryKey[] = ["uk", "global", "us", "ca"];
        const yearsToFetch = Array.from(
          { length: currentYear - START_YEAR + 1 },
          (_, i) => START_YEAR + i
        );

        const responses: { year: number; json: ApiResponse }[] = [];

for (const yr of yearsToFetch) {
  if (ac.signal.aborted) return;

  const response = await fetch(
    `${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductwisePerformance`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({
        product_name: cleanProductName,
        time_range: "Yearly",
        year: yr,
        quarter: null,
        countries: countriesToRequest,
        home_currency: chartCurrency,
        other_sku_product_names: isOtherSkus ? otherSkuProductNames : [],
      }),
      cache: "no-store",
      signal: ac.signal,
    }
  );

  const json: ApiResponse = await response
    .json()
    .catch(() => ({} as ApiResponse));

  if (!response.ok) {
    throw new Error(
      json?.error ||
        json?.message ||
        `HTTP error! status: ${response.status}`
    );
  }

  responses.push({ year: yr, json });
}

        const todayEnd = new Date(today.getFullYear(), today.getMonth() - 1, 1);
        const startDate = new Date(START_YEAR, 0, 1);

        const createMaps = () => ({
          net_sales: new Map<string, number>(),
          cm1_profit: new Map<string, number>(),
          units_sold: new Map<string, number>(),
          asp: new Map<string, number>(),
          sales_mix: new Map<string, number>(),
          profit_mix: new Map<string, number>(),
          inventory_units: new Map<string, number>(),
        });

        const valueMaps: Record<CountryKey, ReturnType<typeof createMaps>> = {
          uk: createMaps(),
          global: createMaps(),
          us: createMaps(),
          ca: createMaps(),
        };

        for (const { year: responseYear, json } of responses) {
          if (!json?.success) continue;

          const responseData = isOtherSkus ? json.other_skus_graph_data : json.data;
          if (!responseData) continue;

          (["uk", "global", "us", "ca"] as CountryKey[]).forEach((country) => {
            const keys = backendKeysFor(country);
            let rows: ApiMonthRow[] = [];

            for (const key of keys) {
              const candidate = normalizeRows(responseData?.[key]);
              if (candidate.length) {
                rows = candidate;
                break;
              }
            }

            if (!rows.length) return;

            rows.forEach((row) => {
              const monthIndex = monthNameToIndex[String(row.month || "").toLowerCase()];
              if (monthIndex === undefined) return;

              const date = new Date(responseYear, monthIndex, 1);
              if (date > todayEnd) return;

              const label = monthLabel(date);

              const netSales = Number(row.net_sales ?? 0);
              const cm1Profit = Number(row.cm1_profit ?? row.cm1 ?? row.profit ?? 0);
              const unitsRaw = Number(row.quantity ?? row.units_sold ?? row.units ?? 0);
              const inventoryUnitsRaw = Number(row.inventory_units ?? 0);
              const asp = Number(row.asp ?? 0);

              valueMaps[country].net_sales.set(label, Number.isFinite(netSales) ? netSales : 0);
              valueMaps[country].cm1_profit.set(label, Number.isFinite(cm1Profit) ? cm1Profit : 0);
              valueMaps[country].units_sold.set(label, Number.isFinite(unitsRaw) ? unitsRaw : 0);
              valueMaps[country].inventory_units.set(
                label,
                Number.isFinite(inventoryUnitsRaw) ? inventoryUnitsRaw : 0
              );
              valueMaps[country].asp.set(label, Number.isFinite(asp) ? asp : 0);
              valueMaps[country].sales_mix.set(label, Number(row.sales_mix ?? 0));
              valueMaps[country].profit_mix.set(label, Number(row.profit_mix ?? 0));
            });
          });
        }

        const months: Date[] = [];
        const cursor = new Date(startDate);

        while (cursor <= todayEnd) {
          months.push(new Date(cursor));
          cursor.setMonth(cursor.getMonth() + 1);
        }

        const buildCountryData = (country: CountryKey): ProductMetricPoint[] =>
          months.map((m) => {
            const label = monthLabel(m);

            return {
              month: label,
              net_sales: valueMaps[country].net_sales.get(label) ?? 0,
              cm1_profit: valueMaps[country].cm1_profit.get(label) ?? 0,
              units_sold: valueMaps[country].units_sold.get(label) ?? 0,
              asp: valueMaps[country].asp.get(label) ?? 0,
              sales_mix: valueMaps[country].sales_mix.get(label) ?? 0,
              profit_mix: valueMaps[country].profit_mix.get(label) ?? 0,
              inventory_units: valueMaps[country].inventory_units.get(label) ?? 0,
            };
          });

        setJourneyData({
          uk: buildCountryData("uk"),
          global: buildCountryData("global"),
          us: buildCountryData("us"),
          ca: buildCountryData("ca"),
        });
      } catch (err: any) {
        if (err?.name === "AbortError") return;

        console.error("Journey API Error:", err);
        setError(err?.message || "Failed to fetch data from server");
        setJourneyData({ uk: [], global: [], us: [], ca: [] });
      } finally {
        setLoading(false);
      }
    };

    fetchJourneyData();

    return () => {
      ac.abort();
    };
  }, [
    productname,
    scope,
    chartCurrency,
    isOtherSkus,
    otherSkuKey,
  ]);

  const visibleCountries: CountryKey[] =
    scope === "uk"
      ? ["uk"]
      : scope === "global"
        ? ["global"]
        : scope === "us"
          ? ["us"]
          : scope === "ca"
            ? ["ca"]
            : ["uk", "global", "us", "ca"];

  const trimmedJourneyData = useMemo(() => {
    const activeCountries = (Object.keys(selectedCountries) as CountryKey[])
      .filter((country) => visibleCountries.includes(country))
      .filter((country) => selectedCountries[country]);

    if (!activeCountries.length) {
      return { labels: [], startIndex: 0 };
    }

    const baseSeries = journeyData[activeCountries[0]] || [];

    if (!baseSeries.length) {
      return { labels: [], startIndex: 0 };
    }

    const hasValueAtIndex = (idx: number) => {
      return activeCountries.some((country) => {
        const point = journeyData[country]?.[idx];
        if (!point) return false;

        if (activeTab === "units_asp") {
          return Number(point.units_sold || 0) > 0 || Number(point.asp || 0) > 0;
        }

        if (activeTab === "inventory_units") {
          return Number(point.inventory_units || 0) > 0 || Number(point.units_sold || 0) > 0;
        }

        if (activeTab === "mix") {
          return Number(point.sales_mix || 0) > 0 || Number(point.profit_mix || 0) > 0;
        }

        return Number(point.net_sales || 0) > 0 || Number(point.cm1_profit || 0) > 0;
      });
    };

    const firstMeaningfulIndex = baseSeries.findIndex((_, idx) => hasValueAtIndex(idx));
    const startIndex = firstMeaningfulIndex === -1 ? 0 : firstMeaningfulIndex;

    return {
      labels: baseSeries.slice(startIndex).map((d) => d.month),
      startIndex,
    };
  }, [journeyData, selectedCountries, visibleCountries, activeTab]);

  const allLabels = trimmedJourneyData.labels;

  const formatCountry = (c: string) => {
    const upperCaseCountries = ["uk", "us", "ca"];
    const normalized = String(c || "").toLowerCase();

    if (upperCaseCountries.includes(normalized)) {
      return normalized.toUpperCase();
    }

    return normalized.charAt(0).toUpperCase() + normalized.slice(1);
  };

  const chartJSData = useMemo(() => {
    const labels = allLabels;

    const activeCountries = (Object.keys(selectedCountries) as CountryKey[])
      .filter((country) => visibleCountries.includes(country))
      .filter((country) => selectedCountries[country]);

    if (activeTab === "sales_cm1") {
      return {
        labels,
        datasets: [
          ...activeCountries.map((country) => ({
            label: `${formatCountry(country)} Net Sales`,
            data: labels.map((label) => journeyData[country]?.find((d) => d.month === label)?.net_sales || 0),
            borderColor: getMetricColor("net_sales"),
            backgroundColor: getMetricColor("net_sales"),
            tension: 0.35,
            pointRadius: 3,
            pointHoverRadius: 5,
            pointHitRadius: 12,
            fill: false,
            borderWidth: 2,
            yAxisID: "y",
          })),
          ...activeCountries.map((country) => ({
            label: `${formatCountry(country)} CM1 Profit`,
            data: labels.map((label) => journeyData[country]?.find((d) => d.month === label)?.cm1_profit || 0),
            borderColor: getMetricColor("cm1_profit"),
            backgroundColor: getMetricColor("cm1_profit"),
            tension: 0.35,
            pointRadius: 3,
            pointHoverRadius: 5,
            pointHitRadius: 12,
            fill: false,
            borderWidth: 2,
            yAxisID: "y",
          })),
        ],
      };
    }

    if (activeTab === "inventory_units") {
      return {
        labels,
        datasets: [
          ...activeCountries.map((country) => ({
            label: `${formatCountry(country)} Inventory Units`,
            data: labels.map((label) => journeyData[country]?.find((d) => d.month === label)?.inventory_units || 0),
            borderColor: getMetricColor("inventory_units"),
            backgroundColor: getMetricColor("inventory_units"),
            tension: 0.35,
            pointRadius: 3,
            pointHoverRadius: 5,
            pointHitRadius: 12,
            fill: false,
            borderWidth: 2,
            yAxisID: "y",
          })),
          ...activeCountries.map((country) => ({
            label: `${formatCountry(country)} Unit Sales`,
            data: labels.map((label) => journeyData[country]?.find((d) => d.month === label)?.units_sold || 0),
            borderColor: getMetricColor("units_sold"),
            backgroundColor: getMetricColor("units_sold"),
            tension: 0.35,
            pointRadius: 3,
            pointHoverRadius: 5,
            pointHitRadius: 12,
            fill: false,
            borderWidth: 2,
            yAxisID: "y1",
          })),
        ],
      };
    }

    if (activeTab === "units_asp") {
      return {
        labels,
        datasets: [
          ...activeCountries.map((country) => ({
            label: `${formatCountry(country)} Units`,
            data: labels.map((label) => journeyData[country]?.find((d) => d.month === label)?.units_sold || 0),
            borderColor: getMetricColor("units_sold"),
            backgroundColor: getMetricColor("units_sold"),
            tension: 0.35,
            pointRadius: 3,
            pointHoverRadius: 5,
            pointHitRadius: 12,
            fill: false,
            borderWidth: 2,
            yAxisID: "y",
          })),
          ...activeCountries.map((country) => ({
            label: `${formatCountry(country)} ASP`,
            data: labels.map((label) => journeyData[country]?.find((d) => d.month === label)?.asp || 0),
            borderColor: getMetricColor("asp"),
            backgroundColor: getMetricColor("asp"),
            tension: 0.35,
            pointRadius: 3,
            pointHoverRadius: 5,
            pointHitRadius: 12,
            fill: false,
            borderWidth: 2,
            yAxisID: "y1",
          })),
        ],
      };
    }

    return {
      labels,
      datasets: [
        ...activeCountries.map((country) => ({
          label: `${formatCountry(country)} Sales Mix`,
          data: labels.map((label) => journeyData[country]?.find((d) => d.month === label)?.sales_mix || 0),
          borderColor: getMetricColor("sales_mix"),
          backgroundColor: getMetricColor("sales_mix"),
          tension: 0.35,
          pointRadius: 3,
          pointHoverRadius: 5,
          pointHitRadius: 12,
          fill: false,
          borderWidth: 2,
          yAxisID: "y",
        })),
        ...activeCountries.map((country) => ({
          label: `${formatCountry(country)} CM1 Profit Mix`,
          data: labels.map((label) => journeyData[country]?.find((d) => d.month === label)?.profit_mix || 0),
          borderColor: getMetricColor("profit_mix"),
          backgroundColor: getMetricColor("profit_mix"),
          tension: 0.35,
          pointRadius: 3,
          pointHoverRadius: 5,
          pointHitRadius: 12,
          fill: false,
          borderWidth: 2,
          yAxisID: "y",
        })),
      ],
    };
  }, [activeTab, allLabels, journeyData, selectedCountries, visibleCountries]);

  const initialMinIndex = Math.max(0, allLabels.length - 12);
  const initialMaxIndex = Math.max(0, allLabels.length - 1);

  const isSmallScreen =
    typeof window !== "undefined" && window.innerWidth < 768;

  const chartOptions: any = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: {
        mode: "index",
        intersect: false,
      },
      hover: {
        mode: "index",
        intersect: false,
      },
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          enabled: true,
          mode: "index",
          intersect: false,
          backgroundColor: "#ffffff",
          titleColor: "#414042",
          bodyColor: "#414042",
          borderColor: "#D1D5DB",
          borderWidth: 1,
          cornerRadius: 6,
          padding: 12,
          displayColors: true,
          boxWidth: 10,
          boxHeight: 10,
          titleFont: {
            size: 14,
            weight: "bold",
          },
          bodyFont: {
            size: 13,
            weight: "normal",
          },
          callbacks: {
            title: (items: any[]) => items?.[0]?.label || "",
            label: (context: any) => {
              const value = Number(context.parsed.y || 0);
              const datasetLabel = String(context.dataset.label || "");
              const lowerLabel = datasetLabel.toLowerCase();

              if (lowerLabel.includes("asp")) return `${datasetLabel}: ${formatAsp(value)}`;
              if (lowerLabel.includes("mix")) return `${datasetLabel}: ${formatPercent(value)}`;
              if (lowerLabel.includes("units")) return `${datasetLabel}: ${formatUnits(value)}`;

              return `${datasetLabel}: ${formatCurrency(value)}`;
            },
            labelColor: (context: any) => {
              const color = context.dataset.borderColor || "#414042";

              return {
                borderColor: color,
                backgroundColor: color,
                borderWidth: 2,
                borderRadius: 2,
              };
            },
          },
        },
        zoom: {
          limits: {
            x: {
              min: 0,
              max: Math.max(0, allLabels.length - 1),
              minRange: Math.min(12, Math.max(1, allLabels.length)),
            },
          },
          pan: {
            enabled: true,
            mode: "x",
          },
          zoom: {
            wheel: { enabled: false },
            pinch: { enabled: false },
            drag: { enabled: false },
            mode: "x",
          },
        },
      },
      scales: {
        x: {
          min: initialMinIndex,
          max: initialMaxIndex,
          ticks: {
            autoSkip: true,
            maxTicksLimit: isSmallScreen ? 6 : 12,
            maxRotation: 0,
            minRotation: 0,
            font: { size: isSmallScreen ? 9 : 12 },
          },
          grid: { display: false },
        },
        y: {
          title: {
            display: true,
            text:
              activeTab === "units_asp"
                ? "Unit (in nos.)"
                : activeTab === "inventory_units"
                  ? "Inventory Units"
                  : activeTab === "mix"
                    ? "Mix (%)"
                    : `Amount (${currencySymbol})`,
          },
          min: 0,
          ticks: {
            padding: 0,
            font: { size: isSmallScreen ? 10 : 12 },
            callback: (value: number) => {
              if (activeTab === "units_asp" || activeTab === "inventory_units") {
                return formatUnits(value);
              }

              if (activeTab === "mix") return formatPercent(value);

              return formatCurrency(value);
            },
          },
        },
        y1: {
          display: activeTab === "units_asp" || activeTab === "inventory_units",
          position: "right",
          min: 0,
          grid: { drawOnChartArea: false },
          title: {
            display: activeTab === "units_asp" || activeTab === "inventory_units",
            text:
              activeTab === "inventory_units"
                ? "Unit Sales (in nos.)"
                : `ASP (${currencySymbol})`,
          },
          ticks: {
            font: { size: isSmallScreen ? 10 : 12 },
            callback: (value: number) => {
              if (activeTab === "inventory_units") return formatUnits(Number(value));
              return formatAsp(Number(value));
            },
          },
        },
      },
    }),
    [activeTab, allLabels.length, currencySymbol, initialMaxIndex, initialMinIndex]
  );

  return (
    <div className="w-full rounded-xl border border-slate-200 bg-white p-4 shadow-sm">

      {error && !loading && (
        <div className="rounded-2xl border border-red-200 bg-red-50 p-4">
          <div className="flex items-center">
            <div className="mr-3 text-xl text-red-600">❌</div>
            <p className="m-0 text-sm font-medium text-red-700">{error}</p>
          </div>
        </div>
      )}

      {!error && (
        <div className="flex flex-col gap-8">
          <div>
            <div className="mb-4 w-full">
              <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
                <div className="min-w-0 flex-1">
                  <div className="flex flex-col items-start">
                    <div className="flex flex-wrap items-center gap-1">
                      <PageBreadcrumb
                        pageTitle="Performance Journey"
                        variant="page"
                        textSize="lg"
                      />
                    </div>

                    <p className="mt-1 text-[11px] text-charcoal-500 sm:text-xs lg:text-xs">
                      Drag horizontally to navigate months.
                    </p>
                  </div>
                </div>

                <div className="w-full overflow-x-auto pb-1 xl:w-auto xl:overflow-visible">
                  <div className="min-w-max 2xl:min-w-0 2xl:w-fit">
                    <SegmentedToggle<TrendTab>
                      value={activeTab}
                      onChange={setActiveTab}
                      textSizeClass="text-[10px] sm:text-xs"
                      className="w-full 2xl:w-auto"
                      options={[
                        { value: "sales_cm1", label: "Sales & CM1 Profit" },
                        { value: "units_asp", label: "Units & ASP" },
                        { value: "mix", label: "Sales Mix & CM1 Profit Mix" },
                        { value: "inventory_units", label: "Inventory Units & Unit Sales" },
                      ]}
                    />
                  </div>
                </div>
              </div>
            </div>

            <div
  className={`relative flex h-[380px] max-h-[500px] items-center justify-center rounded-md  bg-white ${
    isDraggingChart ? "cursor-grabbing" : "cursor-grab"
  }`}
  onMouseDown={() => setIsDraggingChart(true)}
  onMouseUp={() => setIsDraggingChart(false)}
  onMouseLeave={() => setIsDraggingChart(false)}
  onTouchStart={() => setIsDraggingChart(true)}
  onTouchEnd={() => setIsDraggingChart(false)}
>
 {loading ? (
  <Loader fullscreen={false} transparent />
) : chartJSData?.labels?.length ? (
  <>
    {!isDraggingChart && allLabels.length > 12 && (
      <div className="pointer-events-none absolute left-1/2 top-3 z-10 -translate-x-1/2 rounded-full border border-slate-200 bg-white/95 px-3 py-1 text-[11px] font-semibold text-slate-600 shadow-sm">
        ← Drag to view more months →
      </div>
    )}

    <Line data={chartJSData} options={chartOptions} />
  </>
) : (
  <p className="text-sm text-charcoal-500">No chart data available</p>
)}
</div>

            <div className="mt-3 flex flex-wrap items-center justify-center gap-5 text-[13px] font-semibold text-gray-700">
              {activeTab === "sales_cm1" && (
                <>
                  <div className="flex items-center gap-2">
                    <span className="h-0 w-9 border-t-2 border-[#75BBDA]" />
                    <span>Net Sales</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="h-0 w-9 border-t-2 border-[#7B9A6D]" />
                    <span>CM1 Profit</span>
                  </div>
                </>
              )}

              {activeTab === "units_asp" && (
                <>
                  <div className="flex items-center gap-2">
                    <span className="h-0 w-9 border-t-2 border-[#FDD36F]" />
                    <span>Units</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="h-0 w-9 border-t-2 border-[#B75A5A]" />
                    <span>ASP</span>
                  </div>
                </>
              )}

              {activeTab === "mix" && (
                <>
                  <div className="flex items-center gap-2">
                    <span className="h-0 w-9 border-t-2 border-[#3A8EA4]" />
                    <span>Sales Mix</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="h-0 w-9 border-t-2 border-[#ED9F50]" />
                    <span>CM1 Profit Mix</span>
                  </div>
                </>
              )}

              {activeTab === "inventory_units" && (
                <>
                  <div className="flex items-center gap-2">
                    <span className="h-0 w-9 border-t-2 border-[#7B9A6D]" />
                    <span>Inventory Units</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="h-0 w-9 border-t-2 border-[#FDD36F]" />
                    <span>Unit Sales</span>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default ProductJourneyInlineGraph;