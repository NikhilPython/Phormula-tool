"use client";

import React, { useEffect, useMemo, useState, useRef } from "react";
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

/**
 * ✅ Plugin: shift ONLY the pie/chartArea to the left
 * (Kept from your file — harmless even if legend is custom.)
 */
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
}) => {
  const normalizedHomeCurrency = (homeCurrency || "usd").toLowerCase();
  const isGlobalPage = (countryName || "").toLowerCase() === "global";

  const currencySymbol = isGlobalPage
    ? getCurrencySymbol(homeCurrency || "usd")
    : getCurrencySymbol(countryName || "");

  const [uploadsData, setUploadsData] = useState<UploadHistoryResponse | null>(
    null
  );
  const [chartData, setChartData] =
    useState<ChartData<"pie", number[], string> | null>(null);
  const [displayChartData, setDisplayChartData] =
    useState<ChartData<"pie", number[], string> | null>(null);

  const [allValuesZero, setAllValuesZero] = useState(false);

  const [isLaptop, setIsLaptop] = useState(false);
  const [isDesktop, setIsDesktop] = useState(false);

  // used only to force legend rerender after toggling visibility
  const [legendTick, setLegendTick] = useState(0);

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

  const chartRef = useRef<any>(null);

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

  const fetchUploadHistory = async () => {
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
        return;
      }

      const data = (await response.json()) as UploadHistoryResponse;
      setUploadsData(data);
    } catch (error) {
      console.error("Fetch error:", error);
    }
  };

  useEffect(() => {
    fetchUploadHistory();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [month, year, range, selectedQuarter, countryName, normalizedHomeCurrency]);

  // Build chart data from summary
  useEffect(() => {
    if (!uploadsData?.summary) {
      setChartData(null);
      return;
    }

    const s = uploadsData.summary;

    const colors = ["#FDD36F", "#B75A5A", "#ED9F50", "#C49466", "#3A8EA4", "#B8C78C"];

    const next: ChartData<"pie", number[], string> = {
      labels: ["COGS", "Amazon Fees", "Tax and credits", "Ads", "Others", "CM2 Profit"],
      datasets: [
        {
          data: [
            Math.abs(s.total_cous || 0),
            Math.abs(s.total_amazon_fee || 0),
            Math.abs(s.taxncredit || 0),
            Math.abs(s.advertising_total || 0),
            Math.abs(s.otherwplatform || 0),
            Math.abs(s.cm2_profit || 0),
          ],
          backgroundColor: colors,
          hoverBackgroundColor: colors, // ✅ SAME COLORS ON HOVER (no change)
          borderWidth: 0,
          borderColor: "transparent",
          spacing: 0,
          hoverOffset: 4,
          offset: 0,
        },
      ],
    };

    setChartData(next);
  }, [uploadsData]);

  // Use dummy values when all zero (keep your existing behavior)
  useEffect(() => {
    if (!chartData || !chartData.labels || !chartData.datasets?.[0]?.data) {
      setAllValuesZero(false);
      setDisplayChartData(null);
      return;
    }

    const vals = (chartData.datasets[0].data as number[]) || [];
    const isZero = vals.every((v) => v === 0);
    setAllValuesZero(isZero);

    const colors = ["#FDD36F", "#B75A5A", "#ED9F50", "#C49466", "#3A8EA4", "#B8C78C"];

    if (isZero) {
      const dummyValues = [25, 20, 15, 10, 18, 12];
      const dummy: ChartData<"pie", number[], string> = {
        labels: chartData.labels as string[],
        datasets: [
          {
            data: dummyValues,
            backgroundColor: colors,
            hoverBackgroundColor: colors, // ✅ SAME ON HOVER
            borderWidth: 0,
            borderColor: "transparent",
            hoverOffset: 4,
          },
        ],
      };
      setDisplayChartData(dummy);
    } else {
      setDisplayChartData(chartData);
    }
  }, [chartData]);

  // Export base64
  useEffect(() => {
    if (!displayChartData) {
      onExportBase64Ready?.(null);
      return;
    }

    const t = setTimeout(() => {
      const base64 = exportChartBase64();
      onExportBase64Ready?.(base64);
    }, 300);

    return () => clearTimeout(t);
  }, [displayChartData, onExportBase64Ready]);

  const options = useMemo<ChartOptions<"pie">>(() => {
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 0 },

      radius: isLaptop ? "91%" : "100%",

      elements: {
        arc: {
          borderWidth: 0,
          hoverOffset: 0,
        },
      },

      // keep your layout behaviour
      layout: {
        padding: {
          top: isLaptop ? 0 : 10,
          bottom: isLaptop ? 0 : 10,
          left: isLaptop ? 0 : 10,
          right: 0,
        },
      },

      plugins: {
        // keep your shift
        shiftPieLeft: { shift: isLaptop ? 10 : 0 },

        // ✅ IMPORTANT: turn off Chart.js legend
        legend: { display: false },

        tooltip: {
          enabled: !allValuesZero,
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

              return `${label}${currencySymbol}${value.toLocaleString(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              })} (${pct.toFixed(2)}%)`;
            },
          },
        },
      },
    };
  }, [isLaptop, currencySymbol, allValuesZero]);

  // Legend helpers from displayed dataset
  const legendModel = useMemo(() => {
    if (!displayChartData?.labels || !displayChartData?.datasets?.length) return null;

    const labels = displayChartData.labels as string[];
    const ds = displayChartData.datasets[0] as any;

    const values = ((ds?.data || []) as number[]).map((v) => Math.abs(toNum(v)));
    const total = values.reduce((a, b) => a + b, 0);

    const colors =
      (ds?.backgroundColor as string[]) || ["#FDD36F", "#B75A5A", "#ED9F50", "#C49466", "#3A8EA4", "#B8C78C"];

    const truncate = (s: string) => (isLaptop && s.length > 24 ? s.slice(0, 22) + "…" : s);

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
  }, [displayChartData, isLaptop, currencySymbol]);

  return (
    <div className="relative w-full rounded-xl border border-slate-200 bg-white shadow-sm p-4 flex flex-col">
      {/* Heading */}
      <div className="mb-1 w-fit mx-auto md:mx-0">
        <PageBreadcrumb
          pageTitle="Expense Breakup"
          variant="page"
          textSize="2xl"
          align="left"
        />
      </div>

      {/* Chart */}
      <div
        className={`flex-1 min-h-0 w-full ${allValuesZero ? "opacity-30" : "opacity-100"} transition-opacity duration-300`}
      >
        {displayChartData && legendModel ? (
          <div className="relative w-full h-full flex items-center gap-6">

            {/* LEFT: PIE */}
            <div className="flex-1 min-w-0 h-[260px] sm:h-[280px] md:h-[300px] 2xl:h-[360px]">
              <Pie
                // className="!block"
                ref={chartRef}
                data={displayChartData}
                options={options}
                // redraw
                className="!block"
                style={{ width: "100%", height: "100%" }}
              />
            </div>

            {/* RIGHT: CUSTOM LEGEND (like CM1 breakdown style) */}
            <div
              className="shrink-0 overflow-auto pr-1"
              style={{
                width: isDesktop ? 260 : isLaptop ? 120 : 240,
                maxHeight: "100%",
              }}
            >
              <div className="flex flex-col gap-1 2xl:gap-4">
                {legendModel.map((item) => {
                  const chart = chartRef.current;
                  const isVisible = chart ? chart.getDataVisibility(item.index) : true;

                  return (
                    <button
                      key={`${item.fullLabel}-${item.index}`}
                      type="button"
                      className="text-left"
                      onClick={() => {
                        const c = chartRef.current;
                        if (!c) return;
                        c.toggleDataVisibility(item.index);
                        c.update();
                        setLegendTick((t) => t + 1);
                      }}
                    >
                      <div
                        className={`flex items-start gap-3 ${isVisible ? "opacity-100" : "opacity-40"
                          }`}
                      >
                        <span
                          className="mt-1.5 inline-block h-2.5 w-2.5 rounded-full"
                          style={{ backgroundColor: item.color }}
                        />

                        <div className="min-w-0">
                          {/* line 1: Label */}
                          <div
                            className={`truncate ${isVisible ? "" : "line-through"
                              }`}
                            style={{
                              fontSize: isLaptop ? 10 : 12,
                              color: "#414042",
                            }}
                            title={item.fullLabel}
                          >
                            {item.label}
                          </div>

                          {/* line 2: (value) (percentage) */}
                          <div
                            className="whitespace-nowrap"
                            style={{
                              fontSize: isLaptop ? 10 : 12,
                              color: "#414042",
                            }}
                          >
                            {currencySymbol}
                            {item.value.toLocaleString(undefined, {
                              minimumFractionDigits: 2,
                              maximumFractionDigits: 2,
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

            {/* force rerender */}
            <span className="hidden">{legendTick}</span>
            {/* </div> */}
          </div>
        ) : (
          <p className="text-center text-sm text-gray-500">Loading chart data...</p>
        )}
      </div>
    </div>
  );
};

export default CircleChart;
