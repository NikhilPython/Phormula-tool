"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import { Pie } from "react-chartjs-2";
import {
  Chart as ChartJS,
  ArcElement,
  Tooltip,
  Legend,
  ChartOptions,
  ChartData,
  TooltipItem,
} from "chart.js";
import PageBreadcrumb from "../common/PageBreadCrumb";

const shiftPieLeftPlugin = {
  id: "shiftPieLeft",
  afterLayout(chart: any, _args: any, opts: any) {
    const shift = Number(opts?.shift ?? 0);
    if (!shift || !chart?.chartArea) return;

    chart.chartArea.left -= shift;
    chart.chartArea.right -= shift;
  },
};

ChartJS.register(ArcElement, Tooltip, Legend, shiftPieLeftPlugin);

type Range = "monthly" | "quarterly" | "yearly";
type Quarter = "Q1" | "Q2" | "Q3" | "Q4";

type CircleChartProps = {
  range: Range;
  month?: string;
  year: number | string;
  selectedQuarter?: Quarter;
  countryName: string;
  homeCurrency?: string;
  onExportBase64Ready?: (base64: string | null) => void;
  disableInternalFade?: boolean;
  isPreviewMode?: boolean; // ✅ NEW
};

type Summary = {
  advertising_total: number;
  cm2_profit: number;
  total_amazon_fee: number;
  taxncredit: number;
  total_cous: number;
  otherwplatform: number;
};

type UploadHistoryResponse = {
  uploads?: unknown[];
  summary?: Summary;
};

const COLORS = ["#FDD36F", "#B75A5A", "#ED9F50", "#C49466", "#3A8EA4", "#B8C78C"];

const getCurrencySymbol = (codeOrCountry: string) => {
  switch ((codeOrCountry || "").toLowerCase()) {
    case "uk":
    case "gb":
    case "gbp":
      return "£";
    case "india":
    case "in":
    case "inr":
      return "₹";
    case "us":
    case "usa":
    case "usd":
      return "$";
    case "europe":
    case "eu":
    case "eur":
      return "€";
    default:
      return "¤";
  }
};

const toNum = (v: unknown) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

const CircleChart: React.FC<CircleChartProps> = ({
  range,
  month,
  year,
  selectedQuarter,
  countryName,
  homeCurrency,
  onExportBase64Ready,
  disableInternalFade = false,
  isPreviewMode = false,
}) => {
  const normalizedHomeCurrency = (homeCurrency || "usd").toLowerCase();
  const isGlobalPage = (countryName || "").toLowerCase() === "global";

  // ✅ IMPORTANT: explicit preview mode from parent wins
  const isDemoMode =
    isPreviewMode ||
    String(month ?? "").toUpperCase() === "NA" ||
    String(year ?? "").toUpperCase() === "NA";

  const currencySymbol = isGlobalPage
    ? getCurrencySymbol(homeCurrency || "usd")
    : getCurrencySymbol(countryName || "");

  const [uploadsData, setUploadsData] = useState<UploadHistoryResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [noDataFound, setNoDataFound] = useState(false);
  const [chartData, setChartData] =
    useState<ChartData<"pie", number[], string> | null>(null);

  const [legendTick, setLegendTick] = useState(0);
  const [isLaptop, setIsLaptop] = useState(false);
  const [isDesktop, setIsDesktop] = useState(false);

  const chartRef = useRef<any>(null);

  useEffect(() => {
    const check = () => {
      const w = window.innerWidth;
      setIsLaptop(w >= 1024 && w < 1536);
      setIsDesktop(w >= 1536);
    };
    check();
    window.addEventListener("resize", check);
    return () => window.removeEventListener("resize", check);
  }, []);

  const exportChartBase64 = () => {
    try {
      const chart = chartRef.current;
      if (!chart) return null;

      chart.update("none");
      const srcCanvas = chart.canvas as HTMLCanvasElement;

      const scale = 3;
      const out = document.createElement("canvas");
      out.width = srcCanvas.width * scale;
      out.height = srcCanvas.height * scale;

      const ctx = out.getContext("2d");
      if (!ctx) return null;

      ctx.imageSmoothingEnabled = true;
      ctx.imageSmoothingQuality = "high";

      ctx.fillStyle = "#FFFFFF";
      ctx.fillRect(0, 0, out.width, out.height);
      ctx.drawImage(srcCanvas, 0, 0, out.width, out.height);

      return out.toDataURL("image/jpeg", 0.98);
    } catch {
      return null;
    }
  };

  useEffect(() => {
    if (isDemoMode) {
      setUploadsData(null);
      setChartData(null);
      setLoading(false);
      setNoDataFound(false);
      return;
    }

    const fetchUploadHistory = async () => {
      setLoading(true);
      setNoDataFound(false);
      setChartData(null);

      try {
        const token =
          typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        const params = new URLSearchParams({
          range,
          country: countryName || "",
          year: String(year ?? ""),
        });

        if (isGlobalPage && homeCurrency) {
          params.append("homeCurrency", homeCurrency);
        }

        if (range === "monthly" && month) {
          params.append("month", month);
        } else if (range === "quarterly" && selectedQuarter) {
          params.append("quarter", selectedQuarter);
        }

        const response = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_history2?${params.toString()}`,
          {
            method: "GET",
            headers: token ? { Authorization: `Bearer ${token}` } : {},
          }
        );

        if (!response.ok) {
          console.error("Error fetching data:", await response.text());
          setUploadsData(null);
          setNoDataFound(true);
          return;
        }

        const data = (await response.json()) as UploadHistoryResponse;
        setUploadsData(data);
      } catch (error) {
        console.error("Fetch error:", error);
        setUploadsData(null);
        setNoDataFound(true);
      } finally {
        setLoading(false);
      }
    };

    fetchUploadHistory();
  }, [
    isDemoMode,
    month,
    year,
    range,
    selectedQuarter,
    countryName,
    normalizedHomeCurrency,
    homeCurrency,
    isGlobalPage,
  ]);

  useEffect(() => {
    if (isDemoMode) {
      setChartData(null);
      setNoDataFound(false);
      return;
    }

    if (!uploadsData?.summary) {
      setChartData(null);
      setNoDataFound(true);
      return;
    }

    const s = uploadsData.summary;

    const labelsRaw = [
      "COGS",
      "Amazon Fees",
      "Tax and credits",
      "Ads",
      "Others",
      "CM2 Profit",
    ];

    const valuesRaw = [
      Math.abs(s.total_cous || 0),
      Math.abs(s.total_amazon_fee || 0),
      Math.abs(s.taxncredit || 0),
      Math.abs(s.advertising_total || 0),
      Math.abs(s.otherwplatform || 0),
      Math.abs(s.cm2_profit || 0),
    ];

    const hasRenderableData = valuesRaw.some((v) => Number(v) > 0);

    if (!hasRenderableData) {
      setChartData(null);
      setNoDataFound(true);
      return;
    }

    const next: ChartData<"pie", number[], string> = {
      labels: labelsRaw,
      datasets: [
        {
          data: valuesRaw,
          backgroundColor: COLORS,
          hoverBackgroundColor: COLORS,
          borderWidth: 0,
          borderColor: "transparent",
          spacing: 0,
          hoverOffset: 4,
          offset: 0,
        },
      ],
    };

    setChartData(next);
    setNoDataFound(false);
  }, [uploadsData, isDemoMode]);

  const showEmptyState = !loading && !isDemoMode && (noDataFound || !chartData);

  const options = useMemo<ChartOptions<"pie">>(() => {
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 0 },
      radius: isLaptop ? "92%" : isDesktop ? "95%" : "100%",
      elements: {
        arc: {
          borderWidth: 0,
          hoverOffset: 0,
        },
      },
      layout: {
        padding: {
          top: isLaptop ? 0 : 10,
          bottom: isLaptop ? 0 : 10,
          left: isLaptop ? 0 : 10,
          right: 0,
        },
      },
      plugins: {
        shiftPieLeft: { shift: isLaptop ? 10 : 0 },
        legend: { display: false },
        tooltip: {
          enabled: !noDataFound,
          callbacks: {
            label: (ctx: TooltipItem<"pie">) => {
              const value = Math.abs(Number(ctx.raw ?? 0));
              const ds = ctx.chart.data.datasets?.[ctx.datasetIndex] as
                | { data: number[] }
                | undefined;

              const total = (ds?.data ?? []).reduce(
                (acc, v) => acc + Math.abs(Number(v || 0)),
                0
              );

              const pct = total ? (value / total) * 100 : 0;
              const label = ctx.label ? `${ctx.label}: ` : "";

              return `${label}${currencySymbol}${Math.round(value).toLocaleString(undefined, {
                maximumFractionDigits: 0,
              })} (${pct.toFixed(2)}%)`;
            },
          },
        },
      },
    };
  }, [isLaptop, isDesktop, currencySymbol, noDataFound]);

  const legendModel = useMemo(() => {
    if (!chartData?.labels || !chartData?.datasets?.length) return null;

    const labels = chartData.labels as string[];
    const ds = chartData.datasets[0] as any;

    const values = ((ds?.data || []) as number[]).map((v) => Math.abs(toNum(v)));
    const total = values.reduce((a, b) => a + b, 0);
    const colors = (ds?.backgroundColor as string[]) || COLORS;

    const truncate = (s: string) =>
      isLaptop && s.length > 24 ? s.slice(0, 22) + "…" : s;

    return labels.map((label, i) => {
      const value = values[i] ?? 0;
      const pct = total ? (value / total) * 100 : 0;

      return {
        label: truncate(label),
        fullLabel: label,
        value,
        pct,
        color: colors[i % colors.length],
        index: i,
      };
    });
  }, [chartData, isLaptop]);

  useEffect(() => {
    if (!onExportBase64Ready) return;

    if (!chartData || loading) {
      onExportBase64Ready(null);
      return;
    }

    const t = setTimeout(() => {
      onExportBase64Ready(exportChartBase64());
    }, 300);

    return () => clearTimeout(t);
  }, [chartData, loading, onExportBase64Ready]);

  return (
    <div
      className={[
        "relative w-full rounded-xl border border-slate-200 bg-white shadow-sm p-4 flex flex-col transition-opacity duration-300",
        disableInternalFade ? "pointer-events-none select-none opacity-45" : "opacity-100",
      ].join(" ")}
    >
      <div className="mb-1 w-fit mx-right md:mx-0">
        <PageBreadcrumb
          pageTitle="Expense Breakup"
          variant="page"
          textSize="2xl"
          align="left"
        />
      </div>

      {loading && (
        <div className="flex-1 w-full min-h-[260px] md:min-h-[287px] xl:min-h-[300px] 2xl:min-h-[360px] flex items-center justify-center">
          <p className="text-center text-sm text-gray-500">Loading chart data...</p>
        </div>
      )}

      {showEmptyState && (
        <div className="flex-1 w-full min-h-[260px] md:min-h-[287px] xl:min-h-[300px] 2xl:min-h-[360px] flex items-center justify-center">
          <p className="text-center text-sm text-gray-400">
            No data available
          </p>
        </div>
      )}

      {isDemoMode && (
        <div className="flex-1 w-full min-h-[260px] md:min-h-[287px] xl:min-h-[300px] 2xl:min-h-[360px]" />
      )}

      {!showEmptyState && !isDemoMode && chartData && legendModel && (
        <div className="flex-1 w-full min-h-[260px] md:min-h-[287px] xl:min-h-[300px] 2xl:min-h-[360px] flex items-center justify-center">
          <div className="relative w-full flex flex-col xl:flex-row gap-4 xl:gap-6 items-stretch xl:items-center">
            <div className="w-full xl:flex-1 min-w-0 h-[260px] md:h-[287px] xl:h-[300px] 2xl:h-[360px]">
              <Pie
                ref={chartRef}
                data={chartData}
                options={options}
                className="!block"
                style={{ width: "100%", height: "100%" }}
              />
            </div>

            <div
              className="w-full xl:shrink-0 xl:self-center overflow-y-auto overflow-x-hidden pr-1 flex justify-center xl:justify-start"
              style={{
                width: isDesktop ? 260 : isLaptop ? 170 : "100%",
                maxHeight: "100%",
              }}
            >
              <div className="grid w-full grid-cols-2 sm:grid-cols-3 xl:flex xl:w-auto xl:flex-col gap-x-6 gap-y-3 xl:gap-y-2">
                {legendModel.map((item) => {
                  const chart = chartRef.current;
                  const isVisible = chart
                    ? chart.getDataVisibility(item.index)
                    : true;

                  return (
                    <button
                      key={`${item.fullLabel}-${item.index}`}
                      type="button"
                      className="text-left w-full min-w-0"
                      onClick={() => {
                        const c = chartRef.current;
                        if (!c) return;
                        c.toggleDataVisibility(item.index);
                        c.update();
                        setLegendTick((t) => t + 1);
                      }}
                    >
                      <div
                        className={`flex items-start gap-2 sm:gap-3 min-w-0 ${isVisible ? "opacity-100" : "opacity-40"
                          }`}
                      >
                        <span
                          className="mt-[3px] inline-block h-2.5 w-2.5 rounded-full flex-none shrink-0"
                          style={{ backgroundColor: item.color }}
                        />

                        <div className="min-w-0 flex flex-col items-start">
                          <div
                            className={`truncate text-[10px] 2xl:text-xs ${isVisible ? "" : "line-through"
                              }`}
                            style={{ color: "#414042" }}
                            title={item.fullLabel}
                          >
                            {item.label}
                          </div>

                          <div
                            className="text-[10px] 2xl:text-xs break-words"
                            style={{ color: "#414042" }}
                          >
                            {currencySymbol}
                            {Math.round(item.value).toLocaleString(undefined, {
                              maximumFractionDigits: 0,
                            })}{" "}
                            ({item.pct.toFixed(2)}%)
                          </div>
                        </div>
                      </div>
                    </button>
                  );
                })}
              </div>
            </div>

            <span className="hidden">{legendTick}</span>
          </div>
        </div>
      )}
    </div>
  );
};

export default CircleChart;