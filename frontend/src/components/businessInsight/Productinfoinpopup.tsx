"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useParams, usePathname } from "next/navigation";
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
import SegmentedToggle from "../ui/SegmentedToggle";
import PageBreadcrumb from "../common/PageBreadCrumb";

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

  // ✅ NEW: backend returns this for Other SKUs graph
  other_skus_graph_data?: Record<string, any>;

  message?: string;
}

interface ProductinfoinpopupProps {
  productname?: string;
  countryName?: string;
  sourceCountryName?: string;
  displayCurrency?: CurrencyCode;
  onClose?: () => void;
  isOtherSkus?: boolean;

  // ✅ NEW
  otherSkuProductNames?: string[];
}

const Productinfoinpopup: React.FC<ProductinfoinpopupProps> = ({
  productname,
  countryName = "global",
  sourceCountryName,
  displayCurrency,
  isOtherSkus = false,
  otherSkuProductNames = [],
}) => {
  const params = useParams();
  const pathname = usePathname();

  const { month, year } = params as {
    month?: string;
    year?: string;
  };

  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>("");
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

  const authToken =
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

  const scope = (countryName || "").toLowerCase();

  useEffect(() => {
    if (scope === "uk") {
      setSelectedCountries({ uk: true, global: false, us: false, ca: false });
    } else if (scope === "global") {
      setSelectedCountries({ uk: false, global: true, us: false, ca: false });
    } else if (scope === "us") {
      setSelectedCountries({ uk: false, global: false, us: true, ca: false });
    } else if (scope === "ca") {
      setSelectedCountries({ uk: false, global: false, us: false, ca: true });
    }
  }, [scope]);

  const pageScope = (countryName || "global").toLowerCase();

  const chartCurrency: CurrencyCode = useMemo(() => {
    if (displayCurrency) return displayCurrency;

    if (pageScope === "uk") return "GBP";
    if (pageScope === "us") return "USD";
    if (pageScope === "ca") return "CAD";
    if (pageScope === "india") return "INR";

    return "USD";
  }, [displayCurrency, pageScope]);

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
    }).format(value);
  };

  const formatUnits = (value: number) => {
    return new Intl.NumberFormat("en-US", {
      maximumFractionDigits: 0,
    }).format(value);
  };

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

  const formatPercent = (value: number) => {
    return `${Number(value || 0).toFixed(2)}%`;
  };

  const monthShort = (d: Date) =>
    d.toLocaleString("en-US", { month: "short" });

  const monthLabel = (d: Date) =>
    `${monthShort(d)}'${String(d.getFullYear()).slice(-2)}`;

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

  const fetchJourneyData = async () => {
    if (!productname) {
      setJourneyData({
        uk: [],
        global: [],
        us: [],
        ca: [],
      });
      return;
    }

    setLoading(true);
    setError("");

    try {
      const today = new Date();
      const currentYear = today.getFullYear();
      const START_YEAR = 2023;

      const countriesToRequest: CountryKey[] = ["uk", "global", "us", "ca"];
      const yearsToFetch = Array.from(
        { length: currentYear - START_YEAR + 1 },
        (_, i) => START_YEAR + i
      );

      const responses = await Promise.all(
        yearsToFetch.map(async (yr) => {
          const requestPayload = {
            product_name: productname,
            time_range: "Yearly",
            year: yr,
            quarter: null,
            countries: countriesToRequest,
            home_currency: chartCurrency,

            // ✅ NEW
            other_sku_product_names: isOtherSkus ? otherSkuProductNames : [],
          };

          const response = await fetch(
            `${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductwisePerformance`,
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${authToken}`,
              },
              body: JSON.stringify(requestPayload),
            }
          );

          if (!response.ok) {
            let msg = `HTTP error! status: ${response.status}`;
            try {
              const errJson = await response.json();
              msg = errJson.error || errJson.message || msg;
            } catch {
              // ignore
            }
            throw new Error(msg);
          }

          const json: ApiResponse = await response.json();
          console.log("ProductwisePerformance graph response", {
            productname,
            isOtherSkus,
            year: yr,
            normalData: json.data,
            otherSkusData: json.other_skus_graph_data,
          });
          return { year: yr, json };
        })
      );

      const todayEnd = new Date(today.getFullYear(), today.getMonth() - 1, 1);
      const startDate = new Date(START_YEAR, 0, 1);

      const valueMaps: Record<
        CountryKey,
        {
          net_sales: Map<string, number>;
          cm1_profit: Map<string, number>;
          units_sold: Map<string, number>;
          asp: Map<string, number>;
          sales_mix: Map<string, number>;
          profit_mix: Map<string, number>;
          inventory_units: Map<string, number>;
        }
      > = {
        uk: {
          net_sales: new Map(),
          cm1_profit: new Map(),
          units_sold: new Map(),
          asp: new Map(),
          sales_mix: new Map(),
          profit_mix: new Map(),
          inventory_units: new Map(),
        },
        global: {
          net_sales: new Map(),
          cm1_profit: new Map(),
          units_sold: new Map(),
          asp: new Map(),
          sales_mix: new Map(),
          profit_mix: new Map(),
          inventory_units: new Map(),
        },
        us: {
          net_sales: new Map(),
          cm1_profit: new Map(),
          units_sold: new Map(),
          asp: new Map(),
          sales_mix: new Map(),
          profit_mix: new Map(),
          inventory_units: new Map(),
        },
        ca: {
          net_sales: new Map(),
          cm1_profit: new Map(),
          units_sold: new Map(),
          asp: new Map(),
          sales_mix: new Map(),
          profit_mix: new Map(),
          inventory_units: new Map(),
        },
      };

      for (const { year: responseYear, json } of responses) {
        if (!json?.success) continue;

        // ✅ For Other SKUs, read aggregate data.
        // ✅ For normal products, keep old data source.
        const responseData = isOtherSkus
          ? json.other_skus_graph_data
          : json.data;

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

      const finalData: Record<CountryKey, ProductMetricPoint[]> = {
        uk: months.map((m) => {
          const label = monthLabel(m);
          return {
            month: label,
            net_sales: valueMaps.uk.net_sales.get(label) ?? 0,
            cm1_profit: valueMaps.uk.cm1_profit.get(label) ?? 0,
            units_sold: valueMaps.uk.units_sold.get(label) ?? 0,
            asp: valueMaps.uk.asp.get(label) ?? 0,
            sales_mix: valueMaps.uk.sales_mix.get(label) ?? 0,
            profit_mix: valueMaps.uk.profit_mix.get(label) ?? 0,
            inventory_units: valueMaps.uk.inventory_units.get(label) ?? 0,
          };
        }),
        global: months.map((m) => {
          const label = monthLabel(m);
          return {
            month: label,
            net_sales: valueMaps.global.net_sales.get(label) ?? 0,
            cm1_profit: valueMaps.global.cm1_profit.get(label) ?? 0,
            units_sold: valueMaps.global.units_sold.get(label) ?? 0,
            asp: valueMaps.global.asp.get(label) ?? 0,
            sales_mix: valueMaps.global.sales_mix.get(label) ?? 0,
            profit_mix: valueMaps.global.profit_mix.get(label) ?? 0,
            inventory_units: valueMaps.global.inventory_units.get(label) ?? 0,
          };
        }),
        us: months.map((m) => {
          const label = monthLabel(m);
          return {
            month: label,
            net_sales: valueMaps.us.net_sales.get(label) ?? 0,
            cm1_profit: valueMaps.us.cm1_profit.get(label) ?? 0,
            units_sold: valueMaps.us.units_sold.get(label) ?? 0,
            asp: valueMaps.us.asp.get(label) ?? 0,
            sales_mix: valueMaps.us.sales_mix.get(label) ?? 0,
            profit_mix: valueMaps.us.profit_mix.get(label) ?? 0,
            inventory_units: valueMaps.us.inventory_units.get(label) ?? 0,
          };
        }),
        ca: months.map((m) => {
          const label = monthLabel(m);
          return {
            month: label,
            net_sales: valueMaps.ca.net_sales.get(label) ?? 0,
            cm1_profit: valueMaps.ca.cm1_profit.get(label) ?? 0,
            units_sold: valueMaps.ca.units_sold.get(label) ?? 0,
            asp: valueMaps.ca.asp.get(label) ?? 0,
            sales_mix: valueMaps.ca.sales_mix.get(label) ?? 0,
            profit_mix: valueMaps.ca.profit_mix.get(label) ?? 0,
            inventory_units: valueMaps.ca.inventory_units.get(label) ?? 0,
          };
        }),
      };

      setJourneyData(finalData);
    } catch (err: any) {
      console.error("Journey API Error:", err);
      setError(err.message || "Failed to fetch data from server");
      setJourneyData({
        uk: [],
        global: [],
        us: [],
        ca: [],
      });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchJourneyData();
  }, [productname, chartCurrency, isOtherSkus]);

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
          return (
            Number(point.inventory_units || 0) > 0 ||
            Number(point.units_sold || 0) > 0
          );
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

    if (!c) return "";

    const normalized = c.toLowerCase();

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
      const salesDatasets = activeCountries.map((country) => ({
        label: `${formatCountry(country)} Net Sales`,
        data: labels.map((label) => {
          const found = journeyData[country]?.find((d) => d.month === label);
          return found ? found.net_sales : 0;
        }),
        borderColor: getMetricColor("net_sales"),
        backgroundColor: getMetricColor("net_sales"),
        tension: 0.35,
        pointRadius: 3,
        pointHoverRadius: 5,
        pointHitRadius: 12,
        fill: false,
        borderDash: [],
        borderWidth: 2,
        yAxisID: "y",
      }));

      const cm1Datasets = activeCountries.map((country) => ({
        label: `${formatCountry(country)} CM1 Profit`,
        data: labels.map((label) => {
          const found = journeyData[country]?.find((d) => d.month === label);
          return found ? found.cm1_profit : 0;
        }),
        borderColor: getMetricColor("cm1_profit"),
        backgroundColor: getMetricColor("cm1_profit"),
        tension: 0.35,
        pointRadius: 3,
        pointHitRadius: 12,
        pointHoverRadius: 5,
        fill: false,
        borderDash: [],
        borderWidth: 2,
        yAxisID: "y",
      }));

      return {
        labels,
        datasets: [...salesDatasets, ...cm1Datasets],
      };
    }

    if (activeTab === "inventory_units") {
      const inventoryDatasets = activeCountries.map((country) => ({
        label: `${formatCountry(country)} Inventory Units`,
        data: labels.map((label) => {
          const found = journeyData[country]?.find((d) => d.month === label);
          return found ? found.inventory_units : 0;
        }),
        borderColor: getMetricColor("inventory_units"),
        backgroundColor: getMetricColor("inventory_units"),
        tension: 0.35,
        pointRadius: 3,
        pointHitRadius: 12,
        pointHoverRadius: 5,
        fill: false,
        borderDash: [],
        borderWidth: 2,
        yAxisID: "y",
      }));

      const unitDatasets = activeCountries.map((country) => ({
        label: `${formatCountry(country)} Unit Sales`,
        data: labels.map((label) => {
          const found = journeyData[country]?.find((d) => d.month === label);
          return found ? found.units_sold : 0;
        }),
        borderColor: getMetricColor("units_sold"),
        backgroundColor: getMetricColor("units_sold"),
        tension: 0.35,
        pointRadius: 3,
        pointHitRadius: 12,
        pointHoverRadius: 5,
        fill: false,
        borderDash: [],
        borderWidth: 2,
        yAxisID: "y1",
      }));

      return {
        labels,
        datasets: [...inventoryDatasets, ...unitDatasets],
      };
    }

    if (activeTab === "units_asp") {
      const unitDatasets = activeCountries.map((country) => ({
        label: `${formatCountry(country)} Units`,
        data: labels.map((label) => {
          const found = journeyData[country]?.find((d) => d.month === label);
          return found ? found.units_sold : 0;
        }),
        borderColor: getMetricColor("units_sold"),
        backgroundColor: getMetricColor("units_sold"),
        tension: 0.35,
        pointRadius: 3,
        pointHitRadius: 12,
        pointHoverRadius: 5,
        fill: false,
        borderDash: [],
        borderWidth: 2,
        yAxisID: "y",
      }));

      const aspDatasets = activeCountries.map((country) => ({
        label: `${formatCountry(country)} ASP`,
        data: labels.map((label) => {
          const found = journeyData[country]?.find((d) => d.month === label);
          return found ? found.asp : 0;
        }),
        borderColor: getMetricColor("asp"),
        backgroundColor: getMetricColor("asp"),
        tension: 0.35,
        pointRadius: 3,
        pointHitRadius: 12,
        pointHoverRadius: 5,
        fill: false,
        borderDash: [],
        borderWidth: 2,
        yAxisID: "y1",
      }));

      return {
        labels,
        datasets: [...unitDatasets, ...aspDatasets],
      };
    }

    const salesMixDatasets = activeCountries.map((country) => ({
      label: `${formatCountry(country)} Sales Mix`,
      data: labels.map((label) => {
        const found = journeyData[country]?.find((d) => d.month === label);
        return found ? found.sales_mix : 0;
      }),
      borderColor: getMetricColor("sales_mix"),
      backgroundColor: getMetricColor("sales_mix"),
      tension: 0.35,
      pointRadius: 3,
      pointHitRadius: 12,
      pointHoverRadius: 5,
      fill: false,
      borderDash: [],
      borderWidth: 2,
      yAxisID: "y",
    }));

    const profitMixDatasets = activeCountries.map((country) => ({
      label: `${formatCountry(country)} CM1 Profit Mix`,
      data: labels.map((label) => {
        const found = journeyData[country]?.find((d) => d.month === label);
        return found ? found.profit_mix : 0;
      }),
      borderColor: getMetricColor("profit_mix"),
      backgroundColor: getMetricColor("profit_mix"),
      tension: 0.35,
      pointRadius: 3,
      pointHitRadius: 12,
      pointHoverRadius: 5,
      fill: false,
      borderDash: [],
      borderWidth: 2,
      yAxisID: "y",
    }));

    return {
      labels,
      datasets: [...salesMixDatasets, ...profitMixDatasets],
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
          usePointStyle: false,
          titleFont: {
            size: 14,
            weight: "bold",
          },
          bodyFont: {
            size: 13,
            weight: "normal",
          },
          callbacks: {
            title: (items: any[]) => {
              const label = items?.[0]?.label || "";
              return label;
            },

            label: (context: any) => {
              const value = Number(context.parsed.y || 0);
              const datasetLabel = String(context.dataset.label || "");
              const lowerLabel = datasetLabel.toLowerCase();

              if (lowerLabel.includes("asp")) {
                return `${datasetLabel}: ${formatAsp(value)}`;
              }

              if (lowerLabel.includes("mix")) {
                return `${datasetLabel}: ${formatPercent(value)}`;
              }

              if (lowerLabel.includes("unit")) {
                return `${datasetLabel}: ${formatUnits(value)}`;
              }

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
            wheel: {
              enabled: false,
            },
            pinch: {
              enabled: false,
            },
            drag: {
              enabled: false,
            },
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
            font: {
              size: isSmallScreen ? 9 : 12,
            },
          },
          grid: {
            display: false,
          },
        },
        y: {
          title: {
            display: true,
            text:
              activeTab === "units_asp"
                ? "Units (in nos.)"
                : activeTab === "inventory_units"
                  ? "Inventory Units"
                  : activeTab === "mix"
                    ? "Mix (%)"
                    : `Amount (${currencySymbol})`,
          },
          min: 0,
          ticks: {
            padding: 0,
            font: {
              size: isSmallScreen ? 10 : 12,
            },
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
          grid: {
            drawOnChartArea: false,
          },
          title: {
            display: activeTab === "units_asp" || activeTab === "inventory_units",
            text:
              activeTab === "inventory_units"
                ? "Unit Sales (in nos.)"
                : `ASP (${currencySymbol})`,
          },
          ticks: {
            font: {
              size: isSmallScreen ? 10 : 12,
            },
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

  const isImprovementsPage = pathname?.includes("mprovements") || false;

  return (
    <div className="w-full">
      {loading && (
        <div className="flex flex-col items-center justify-center py-12 text-center">
          <Loader fullscreen transparent />
        </div>
      )}

      {error && (
        <div className="mb-8 rounded-2xl border border-red-200 bg-red-50 p-6">
          <div className="flex items-center">
            <div className="mr-3 text-xl text-red-600">❌</div>
            <p className="m-0 font-medium text-red-700">{error}</p>
          </div>
        </div>
      )}

      {!loading && !error && (
        <div className="flex flex-col gap-8">
          <div>
            <div className="mb-4 w-full">
              <div className="flex flex-col gap-3 2xl:flex-row 2xl:items-start 2xl:justify-between">
                <div className="min-w-0 flex-1">
                  <div className="flex flex-col items-start">
                    {/* <div className="flex items-center gap-1 flex-wrap">
                      <PageBreadcrumb
                        pageTitle="Performance Journey"
                        variant="page"
                        textSize="lg"
                      />
                    </div>

                    <p className="mt-1 text-[11px] sm:text-xs lg:text-xs text-charcoal-500">
                      Drag horizontally to navigate months.
                    </p> */}

                    <PageBreadcrumb
                      pageTitle="Performance Journey"
                      variant="page"
                      align="left"
                      textSize="xl"
                    />
                  </div>
                </div>

                <div className="w-full overflow-x-auto pb-1 2xl:w-auto 2xl:overflow-visible">
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
              className={`relative flex h-[380px] max-h-[500px] items-center justify-center rounded-md bg-white ${isDraggingChart ? "cursor-grabbing" : "cursor-grab"
                }`}
              onMouseDown={() => setIsDraggingChart(true)}
              onMouseUp={() => setIsDraggingChart(false)}
              onMouseLeave={() => setIsDraggingChart(false)}
              onTouchStart={() => setIsDraggingChart(true)}
              onTouchEnd={() => setIsDraggingChart(false)}
            >
              {chartJSData?.labels?.length ? (
                <>
                  {!isDraggingChart && allLabels.length > 12 && (
                    <div className="pointer-events-none absolute left-1/2 top-3 z-10 -translate-x-1/2 rounded-full border border-slate-200 bg-white/95 px-3 py-1 text-[11px] font-semibold text-slate-600 shadow-sm">
                      ← Drag to view more months →
                    </div>
                  )}

                  <Line data={chartJSData} options={chartOptions} />
                </>
              ) : (
                <p>No chart data available</p>
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
                    <span className="h-0 w-9 border-t-2 border border-[#7B9A6D]" />
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
                    <span className="h-0 w-9 border-t-2 border border-[#B75A5A]" />
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

export default Productinfoinpopup;