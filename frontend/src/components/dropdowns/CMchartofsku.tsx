"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import { Pie } from "react-chartjs-2";
import {
  Chart as ChartJS,
  ArcElement,
  Tooltip,
  Legend,
  ChartData,
  ChartOptions,
  TooltipItem,
} from "chart.js";
import PageBreadcrumb from "../common/PageBreadCrumb";

ChartJS.register(ArcElement, Tooltip, Legend);

type Range = "monthly" | "quarterly" | "yearly";
type Quarter = "Q1" | "Q2" | "Q3" | "Q4";

type CmChartOfSkuProps = {
  range: Range;
  month?: string;
  year: number | string;
  selectedQuarter?: Quarter;
  userId?: string | number; // kept for signature compatibility
  countryName: string;
  homeCurrency?: string;
  onExportBase64Ready?: (base64: string | null) => void;
};

type CompareTop5Item = {
  product: string;
  current_profit: number | string;
  previous_profit: number | string;
};

type PieChartPayload = {
  compare_top5?: CompareTop5Item[];
  labels?: string[];
  values?: Array<number | string>;
  error?: string;
};

type PieChartApiResponse =
  | PieChartPayload
  | {
    success?: boolean;
    data?: PieChartPayload;
    error?: string;
  };

type CmPieSlice = {
  name: string;
  value: number; // current
  prevValue: number; // previous
  pct: number; // share of current total
  deltaPct: number | null; // % change vs previous (null when prev==0)
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
    case "cad":
      return "CA$";
    default:
      return "¤";
  }
};

const toNum = (v: unknown) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

const DeltaUpIcon = ({ className = "" }: { className?: string }) => (
  <svg
    viewBox="0 0 24 24"
    aria-hidden="true"
    className={className}
    fill="currentColor"
  >
    <path d="M12 4l9 16H3L12 4z" />
  </svg>
);

const DeltaDownIcon = ({ className = "" }: { className?: string }) => (
  <svg
    viewBox="0 0 24 24"
    aria-hidden="true"
    className={className}
    fill="currentColor"
  >
    <path d="M12 20L3 4h18l-9 16z" />
  </svg>
);

const CMchartofsku: React.FC<CmChartOfSkuProps> = ({
  range,
  month,
  year,
  selectedQuarter,
  userId, // unused
  countryName,
  homeCurrency,
  onExportBase64Ready,
}) => {
  const normalizedHomeCurrency = (homeCurrency || "usd").toLowerCase();
  const isGlobalPage = (countryName || "").toLowerCase() === "global";

  const currencySymbol = isGlobalPage
    ? getCurrencySymbol(homeCurrency || "usd")
    : getCurrencySymbol(countryName || "");

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // if API explicitly says no data
  const [noDataFound, setNoDataFound] = useState(false);

  // rerender custom legend when toggling visibility
  const [legendTick, setLegendTick] = useState(0);

  const [isLaptop, setIsLaptop] = useState(false);
  const [isDesktop, setIsDesktop] = useState(false);

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

  const [slices, setSlices] = useState<CmPieSlice[]>([]);

  useEffect(() => {
    async function fetchData() {
      setLoading(true);
      setError(null);
      setNoDataFound(false);

      try {
        const token =
          typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        const params = new URLSearchParams({
          country: countryName || "",
          year: String(year ?? ""),
          range: range || "",
        });

        if (isGlobalPage && homeCurrency) {
          params.append("homeCurrency", homeCurrency);
        }

        if (range === "monthly" && month) {
          params.append("month", month);
        } else if (range === "quarterly" && selectedQuarter) {
          params.append("quarter", selectedQuarter);
        }

        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/pie-chart?${params.toString()}`,
          {
            method: "GET",
            headers: token ? { Authorization: `Bearer ${token}` } : {},
          }
        );

        const raw = (await res.json()) as PieChartApiResponse;

        if (!res.ok) {
          const msg =
            (raw as any)?.error ||
            (raw as any)?.data?.error ||
            "Failed to fetch data";
          throw new Error(msg);
        }

        // ✅ Support BOTH shapes:
        // 1) { compare_top5: [...] }
        // 2) { success: true, data: { compare_top5: [...] } }
        const payload: PieChartPayload = (raw as any)?.data ?? (raw as any) ?? {};

        const noDataPhrase = "no data found in any of the available tables";
        const apiErrorText = (payload.error || (raw as any)?.error || "").toLowerCase();

        if (apiErrorText.includes(noDataPhrase)) {
          setSlices([]);
          setNoDataFound(true);
          setError(null);
          return;
        }

        // Prefer compare_top5 (it has current + previous)
        const rows = Array.isArray(payload.compare_top5) ? payload.compare_top5 : [];

        let built: CmPieSlice[] = [];

        if (rows.length) {
          const currentValues = rows.map((r) => Math.abs(toNum(r.current_profit)));
          const totalCurrent = currentValues.reduce((a, b) => a + b, 0);

          built = rows.map((r) => {
            const cur = Math.abs(toNum(r.current_profit));
            const prev = Math.abs(toNum(r.previous_profit));
            const pct = totalCurrent ? (cur / totalCurrent) * 100 : 0;
            const deltaPct = prev === 0 ? null : ((cur - prev) / prev) * 100;

            return {
              name: r.product,
              value: cur,
              prevValue: prev,
              pct,
              deltaPct,
            };
          });
        } else if (Array.isArray(payload.labels) && Array.isArray(payload.values)) {
          // fallback if backend returns only labels/values
          const labels = payload.labels as string[];
          const values = (payload.values || []).map((v) => Math.abs(toNum(v)));
          const totalCurrent = values.reduce((a, b) => a + b, 0);

          built = labels.map((label, i) => {
            const cur = values[i] ?? 0;
            const pct = totalCurrent ? (cur / totalCurrent) * 100 : 0;
            return {
              name: label,
              value: cur,
              prevValue: 0,
              pct,
              deltaPct: null,
            };
          });
        }

        const isEmpty =
          built.length === 0 || built.every((s) => (s.value || 0) === 0);

        if (isEmpty) {
          setSlices([]);
          setNoDataFound(true);
          setError(null);
          return;
        }

        setSlices(built);
        setNoDataFound(false);
      } catch (e) {
        const msg = e instanceof Error ? e.message : "Unknown error";
        setError(msg);
        setSlices([]);
        setNoDataFound(true);
      } finally {
        setLoading(false);
      }
    }

    fetchData();
  }, [
    range,
    month,
    year,
    selectedQuarter,
    countryName,
    userId,
    normalizedHomeCurrency,
    homeCurrency,
    isGlobalPage,
  ]);

  const chartData = useMemo<ChartData<"pie", number[], string> | null>(() => {
    const labels = slices.map((s) => s.name);
    const values = slices.map((s) => Math.abs(toNum(s.value)));

    if (!labels.length || values.every((v) => v === 0)) return null;

    return {
      labels,
      datasets: [
        {
          data: values,
          backgroundColor: labels.map((_, i) => COLORS[i % COLORS.length]),
          borderWidth: 0,
          borderColor: "transparent",
          spacing: 0,
          hoverOffset: 4,
          offset: 0,
        },
      ],
    };
  }, [slices]);

  const options = useMemo<ChartOptions<"pie">>(() => {
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 0 },
      radius: isLaptop ? "100%" : isDesktop ? "95%" : "100%",
      layout: { padding: { top: 0, bottom: 0, left: 0, right: 0 } },
      elements: { arc: { borderWidth: 0, hoverOffset: 4 } },
      plugins: {
        legend: { display: false }, // custom legend on the right
        tooltip: {
          enabled: !noDataFound,
          callbacks: {
            label: (ctx: TooltipItem<"pie">) => {
              const i = ctx.dataIndex;
              const slice = slices?.[i];
              const val = Math.abs(toNum(ctx.raw));

              const pct = slice?.pct ?? 0;
              const delta = slice?.deltaPct;

              const deltaText =
                delta == null
                  ? "—"
                  : `${delta >= 0 ? "▲" : "▼"}${Math.abs(delta).toFixed(2)}%`;



              return `${slice?.name ?? ctx.label}: ${currencySymbol}${val.toLocaleString(
                undefined,
                { minimumFractionDigits: 2, maximumFractionDigits: 2 }
              )} (${pct.toFixed(2)}%) (${deltaText})`;
            },
          },
        },
      },
    };
  }, [currencySymbol, isLaptop, isDesktop, noDataFound, slices]);

  // Export base64 when chart is ready
  useEffect(() => {
    if (!onExportBase64Ready) return;

    if (!chartData || loading || error) {
      onExportBase64Ready(null);
      return;
    }

    const t = setTimeout(() => {
      onExportBase64Ready(exportChartBase64());
    }, 300);

    return () => clearTimeout(t);
  }, [chartData, loading, error, onExportBase64Ready]);

  return (
    <div className="relative w-full rounded-xl border border-slate-200 bg-white shadow-sm p-4 flex flex-col">
      <div className="mb-1 w-fit mx-auto md:mx-0">
        <PageBreadcrumb
          pageTitle="CM1 Profit Breakdown"
          variant="page"
          align="left"
          textSize="2xl"
        />
      </div>

      {loading && (
        <p className="text-center text-sm text-gray-500">Loading chart data...</p>
      )}

      {error && !loading && (
        <p className="text-center text-sm text-red-600">Error: {error}</p>
      )}

      {!loading && !error && !chartData && (
        <p className="text-center text-sm text-gray-500">
          No CM1 data available for this selection.
        </p>
      )}

      {chartData && (
        <div
          className={`flex-1 min-h-0 w-full ${noDataFound ? "opacity-30" : "opacity-100"
            } transition-opacity`}
        >
          <div className="relative w-full h-full flex items-center gap-6">
            {/* LEFT: PIE */}
            <div className="flex-1 min-w-0 h-[260px] sm:h-[280px] md:h-[300px] 2xl:h-[360px]">
              <Pie
                ref={chartRef}
                data={chartData}
                options={options}
                className="!block"
                style={{ width: "100%", height: "100%" }}
              />
            </div>

            {/* RIGHT: LEGEND (Product + value + pct + delta) */}
            <div
              className="shrink-0 overflow-auto pr-1"
              style={{
                width: isDesktop ? 260 : isLaptop ? 180 : 240,
                maxHeight: "100%",
              }}
            >
               <div className="flex flex-col gap-1 2xl:gap-4">
                {slices.map((slice, i) => {
                  const dot = COLORS[i % COLORS.length];
                  const chart = chartRef.current;
                  const isVisible = chart ? chart.getDataVisibility(i) : true;

                  const value = Math.abs(toNum(slice.value));
                  const pct = toNum(slice.pct);
                  const delta = slice.deltaPct;

                  const deltaText =
                    delta == null
                      ? "—"
                      : `${delta >= 0 ? "▲" : "▼"}${Math.abs(delta).toFixed(2)}%`;

                  const deltaClass =
                    delta == null
                      ? "text-[#414042]"
                      : delta >= 0
                        ? "text-green-500"
                        : "text-red-500";


                  return (
                    <button
                      key={`${slice.name}-${i}`}
                      type="button"
                      className="text-left"
                      onClick={() => {
                        const c = chartRef.current;
                        if (!c) return;
                        c.toggleDataVisibility(i);
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
                          style={{ backgroundColor: dot }}
                        />

                        <div className="min-w-0">
                          {/* line 1: Product name */}
                          <div
                            className={`truncate ${isVisible ? "" : "line-through"
                              }`}
                            style={{
                              fontSize: isLaptop ? 10 : 12,
                              color: "#414042",
                            }}
                            title={slice.name}
                          >
                            {slice.name}
                          </div>

                          {/* line 2: (value)(% share)(% change) */}
                          <div
                            className="whitespace-nowrap"
                            style={{
                              fontSize: isLaptop ? 10 : 12,
                              color: "#414042",
                            }}
                          >
                            {currencySymbol}
                            {value.toLocaleString(undefined, {
                              minimumFractionDigits: 2,
                              maximumFractionDigits: 2,
                            })}{" "}
                            ({pct.toFixed(2)}%){" "}
                            <span className={deltaClass} style={{ display: "inline-flex", alignItems: "center", gap: 1 }}>
                              (
                              {delta == null ? (
                                "—"
                              ) : delta >= 0 ? (
                                <DeltaUpIcon className="h-3 w-3" />
                              ) : (
                                <DeltaDownIcon className="h-3 w-3" />
                              )}
                              {delta == null ? "" : `${Math.abs(delta).toFixed(2)}%`}
                              )
                            </span>

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
          </div>
        </div>
      )}
    </div>
  );
};

export default CMchartofsku;
