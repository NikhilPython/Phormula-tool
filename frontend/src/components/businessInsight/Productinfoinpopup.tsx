"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useRouter, useParams, usePathname } from "next/navigation";
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

type CountryKey = "uk" | "global" | "us";

type TrendTab = "sales_cm1" | "units_asp" | "mix";

interface ProductMetricPoint {
  month: string;
  net_sales: number;
  cm1_profit: number;
  units_sold: number;
  asp: number;
  sales_mix: number;
  profit_mix: number;
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
}

interface ApiResponse {
  success: boolean;
  data?: Record<string, any>;
  message?: string;
}

interface ProductinfoinpopupProps {
  productname?: string;
  countryName?: string;
  onClose?: () => void;
}

const Productinfoinpopup: React.FC<ProductinfoinpopupProps> = ({
  productname = "Menthol",
  countryName = "global",
  onClose,
}) => {
  const params = useParams();
  const pathname = usePathname();
  const router = useRouter();

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
  });

  const [journeyData, setJourneyData] = useState<Record<CountryKey, ProductMetricPoint[]>>({
    uk: [],
    global: [],
    us: [],
  });

  const authToken =
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

  const scope = (countryName || "").toLowerCase();

  useEffect(() => {
    if (scope === "uk") {
      setSelectedCountries({ uk: true, global: false, us: false });
    } else if (scope === "global") {
      setSelectedCountries({ uk: false, global: true, us: false });
    } else if (scope === "us") {
      setSelectedCountries({ uk: false, global: false, us: true });
    }
  }, [scope]);

  const pageScope = (countryName || "global").toLowerCase();
  const baseCurrency: "GBP" | "USD" = pageScope === "uk" ? "GBP" : "USD";
  const currencySymbol = baseCurrency === "GBP" ? "£" : "$";

  const getMetricColor = (metric: string) => {
    const colors: Record<string, string> = {
      net_sales: "#75BBDA",
      cm1_profit: "#7B9A6D",
      units_sold: "#FDD36F",
      asp: "#B75A5A",
      sales_mix: "#3A8EA4",
      profit_mix: "#ED9F50",
    };

    return colors[metric] || "#6b7280";
  };

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat(baseCurrency === "GBP" ? "en-GB" : "en-US", {
      style: "currency",
      currency: baseCurrency,
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
    return new Intl.NumberFormat(baseCurrency === "GBP" ? "en-GB" : "en-US", {
      style: "currency",
      currency: baseCurrency,
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);
  };

  const formatPercent = (value: number) => {
    return `${Number(value || 0).toFixed(1)}%`;
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

  const backendKeyFor = (country: CountryKey) => {
    if (country === "uk") return "uk";
    if (country === "us") return "us";
    if (country === "global") {
      return baseCurrency === "GBP" ? "global_gbp" : "global_usd";
    }
    return country;
  };

  const normalizeRows = (countryBlock: any): ApiMonthRow[] => {
    if (!countryBlock) return [];
    if (Array.isArray(countryBlock)) return countryBlock;
    if (Array.isArray(countryBlock?.Yearly)) return countryBlock.Yearly;

    const firstArray = Object.values(countryBlock).find((v) => Array.isArray(v));
    return Array.isArray(firstArray) ? (firstArray as ApiMonthRow[]) : [];
  };

  const fetchJourneyData = async () => {
    setLoading(true);
    setError("");

    try {
      const today = new Date();
      const currentYear = today.getFullYear();
      const START_YEAR = 2023;

      const countriesToRequest: CountryKey[] = ["uk", "global", "us"];
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
            home_currency: baseCurrency,
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
            } catch { }
            throw new Error(msg);
          }

          const json: ApiResponse = await response.json();
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
        }
      > = {
        uk: {
          net_sales: new Map(),
          cm1_profit: new Map(),
          units_sold: new Map(),
          asp: new Map(),
          sales_mix: new Map(),
          profit_mix: new Map(),
        },
        global: {
          net_sales: new Map(),
          cm1_profit: new Map(),
          units_sold: new Map(),
          asp: new Map(),
          sales_mix: new Map(),
          profit_mix: new Map(),
        },
        us: {
          net_sales: new Map(),
          cm1_profit: new Map(),
          units_sold: new Map(),
          asp: new Map(),
          sales_mix: new Map(),
          profit_mix: new Map(),
        },
      };

      for (const { year: responseYear, json } of responses) {
        if (!json?.success || !json?.data) continue;

        (["uk", "global", "us"] as CountryKey[]).forEach((country) => {
          const key = backendKeyFor(country);
          const rows = normalizeRows(json.data?.[key]);

          rows.forEach((row) => {
            const monthIndex = monthNameToIndex[String(row.month || "").toLowerCase()];
            if (monthIndex === undefined) return;

            const date = new Date(responseYear, monthIndex, 1);
            if (date > todayEnd) return;

            const label = monthLabel(date);

            valueMaps[country].net_sales.set(label, Number(row.net_sales ?? 0));
            valueMaps[country].cm1_profit.set(
              label,
              Number(row.cm1_profit ?? row.cm1 ?? row.profit ?? 0)
            );
            valueMaps[country].units_sold.set(
              label,
              Number(row.quantity ?? row.units_sold ?? row.units ?? 0)
            );
            valueMaps[country].asp.set(label, Number(row.asp ?? 0));
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
      });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchJourneyData();
  }, [productname, baseCurrency]);

  const visibleCountries: CountryKey[] =
    scope === "uk"
      ? ["uk"]
      : scope === "global"
        ? ["global"]
        : scope === "us"
          ? ["us"]
          : ["uk", "global", "us"];

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
          return (
            Number(point.units_sold || 0) > 0 ||
            Number(point.asp || 0) > 0
          );
        }

        if (activeTab === "mix") {
          return (
            Number(point.sales_mix || 0) > 0 ||
            Number(point.profit_mix || 0) > 0
          );
        }

        return (
          Number(point.net_sales || 0) > 0 ||
          Number(point.cm1_profit || 0) > 0
        );

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

  const chartJSData = useMemo(() => {
    const labels = allLabels;
    const activeCountries = (Object.keys(selectedCountries) as CountryKey[])
      .filter((country) => visibleCountries.includes(country))
      .filter((country) => selectedCountries[country]);

    if (activeTab === "sales_cm1") {
      const salesDatasets = activeCountries.map((country) => ({
        label: `${country.toUpperCase()} Net Sales`,
        data: labels.map((label) => {
          const found = journeyData[country]?.find((d) => d.month === label);
          return found ? found.net_sales : 0;
        }),
        borderColor: getMetricColor("net_sales"),
        backgroundColor: getMetricColor("net_sales"),
        tension: 0.35,
        pointRadius: 3,
        pointHoverRadius: 5,
        fill: false,
        borderDash: [],
        borderWidth: 2,
        yAxisID: "y",
      }));

      const cm1Datasets = activeCountries.map((country) => ({
        label: `${country.toUpperCase()} CM1 Profit`,
        data: labels.map((label) => {
          const found = journeyData[country]?.find((d) => d.month === label);
          return found ? found.cm1_profit : 0;
        }),
        borderColor: getMetricColor("cm1_profit"),
        backgroundColor: getMetricColor("cm1_profit"),
        tension: 0.35,
        pointRadius: 3,
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

    if (activeTab === "units_asp") {
      const unitDatasets = activeCountries.map((country) => ({
        label: `${country.toUpperCase()} Units`,
        data: labels.map((label) => {
          const found = journeyData[country]?.find((d) => d.month === label);
          return found ? found.units_sold : 0;
        }),
        borderColor: getMetricColor("units_sold"),
        backgroundColor: getMetricColor("units_sold"),
        tension: 0.35,
        pointRadius: 3,
        pointHoverRadius: 5,
        fill: false,
        borderDash: [],
        borderWidth: 2,
        yAxisID: "y",
      }));

      const aspDatasets = activeCountries.map((country) => ({
        label: `${country.toUpperCase()} ASP`,
        data: labels.map((label) => {
          const found = journeyData[country]?.find((d) => d.month === label);
          return found ? found.asp : 0;
        }),
        borderColor: getMetricColor("asp"),
        backgroundColor: getMetricColor("asp"),
        tension: 0.35,
        pointRadius: 3,
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
      label: `${country.toUpperCase()} Sales Mix`,
      data: labels.map((label) => {
        const found = journeyData[country]?.find((d) => d.month === label);
        return found ? found.sales_mix : 0;
      }),
      borderColor: getMetricColor("sales_mix"),
      backgroundColor: getMetricColor("sales_mix"),
      tension: 0.35,
      pointRadius: 3,
      pointHoverRadius: 5,
      fill: false,
      borderDash: [],
      borderWidth: 2,
      yAxisID: "y",
    }));

    const profitMixDatasets = activeCountries.map((country) => ({
      label: `${country.toUpperCase()} Profit Mix`,
      data: labels.map((label) => {
        const found = journeyData[country]?.find((d) => d.month === label);
        return found ? found.profit_mix : 0;
      }),
      borderColor: getMetricColor("profit_mix"),
      backgroundColor: getMetricColor("profit_mix"),
      tension: 0.35,
      pointRadius: 3,
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
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          callbacks: {
            label: (context: any) => {
              const value = context.parsed.y;
              const label = String(context.dataset.label || "").toLowerCase();

              if (label.includes("units")) {
                return `${context.dataset.label}: ${formatUnits(value)}`;
              }

              if (label.includes("asp")) {
                return `${context.dataset.label}: ${formatAsp(value)}`;
              }

              if (label.includes("mix")) {
                return `${context.dataset.label}: ${formatPercent(value)}`;
              }

              return `${context.dataset.label}: ${formatCurrency(value)}`;
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
                ? "Units"
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
              if (activeTab === "units_asp") return formatUnits(value);
              if (activeTab === "mix") return formatPercent(value);
              return formatCurrency(value);
            },
          },
        },
        y1: {
          display: activeTab === "units_asp",
          position: "right",
          min: 0,
          grid: {
            drawOnChartArea: false,
          },
          title: {
            display: activeTab === "units_asp",
            text: `ASP (${currencySymbol})`,
          },
          ticks: {
            font: {
              size: isSmallScreen ? 10 : 12,
            },
            callback: (value: number) => formatAsp(Number(value)),
          },
        },
      },
    }),
    [activeTab, allLabels.length, currencySymbol, initialMaxIndex, initialMinIndex]
  );

  const isImprovementsPage = pathname?.includes("mprovements") || false;


  return (
    <div className="w-full ">
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
              <div className="flex sm:items-center sm:justify-between gap-4">

                <div className="min-w-0 flex-1">
                  <div className="flex flex-col items-start">
                    <div className="flex items-center gap-1 flex-wrap">
                      <PageBreadcrumb
                        pageTitle="Performance Journey"
                        variant="page"
                        textSize="lg"
                      />
                    </div>

                    <p className="text-[11px] sm:text-xs lg:text-xs  text-charcoal-500">
                      Drag horizontally to navigate months.
                    </p>
                  </div>
                </div>

                <div className="flex flex-wrap items-center justify-start sm:justify-end gap-3">
                  <SegmentedToggle<TrendTab>
                    value={activeTab}
                    onChange={setActiveTab}
                    textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
                    className="w-auto"
                    options={[
                      { value: "sales_cm1", label: "Sales & CM1 Profit" },
                      { value: "units_asp", label: "Units & ASP" },
                      { value: "mix", label: "Sales Mix & Profit Mix" },
                    ]}
                  />
                </div>

              </div>
            </div>

            <div
              className={`flex h-[380px] max-h-[500px] items-center justify-center rounded-md ${isDraggingChart ? "cursor-grabbing" : "cursor-grab"
                }`}
              onMouseDown={() => setIsDraggingChart(true)}
              onMouseUp={() => setIsDraggingChart(false)}
              onMouseLeave={() => setIsDraggingChart(false)}
            >
              {chartJSData?.labels?.length ? (
                <Line data={chartJSData} options={chartOptions} />
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
                    <span className="h-0 w-9 border-t-2  border-[#B75A5A]" />
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
                    <span>Profit Mix</span>
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