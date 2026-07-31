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
import SegmentedToggle from "@/components/ui/SegmentedToggle";

ChartJS.register(ArcElement, Tooltip, Legend);

type Range = "monthly" | "quarterly" | "yearly";
type Quarter = "Q1" | "Q2" | "Q3" | "Q4";

type CmMetric = "net_sales" | "cm1" | "cm2";
type CmChartOfSkuProps = {
  range: Range;
  month?: string;
  year: number | string;
  selectedQuarter?: Quarter;
  userId?: string | number;
  countryName: string;
  homeCurrency?: string;
  onExportBase64Ready?: (base64: string | null) => void;
  disableInternalFade?: boolean;
  metric?: CmMetric;
  // Whether CM2 chart data exists for this period.
  // When false, the CM2 segmented toggle option is hidden.
  showCm2Toggle?: boolean;
};

type CompareTop5Item = {
  product: string;
  current_profit: number | string;
  previous_profit: number | string;
};

type CompareTop5NetSalesItem = {
  product: string;
  current_net_sales?: number | string;
  previous_net_sales?: number | string;
  net_sales_delta?: number | string;
  net_sales_delta_percentage?: number | string | null;
  current_sales_mix_percentage?: number | string;
  previous_sales_mix_percentage?: number | string;
  sales_mix_delta_percentage?: number | string;
  sales_mix_curr?: number | string;
  sales_mix_prev?: number | string;
  sales_mix_change?: number | string;
};

type ApiSkuRow = {
  product_name?: string;
  sku?: string;
  net_sales?: number | string;
  profit?: number | string;
  cm2_profit?: number | string;
};

type Cm2ProfitItem = {
  product_name: string;
  cm2_profit: number | string;
};

type PieChartPayload = {
  compare_top5?: CompareTop5Item[];
  compare_top5_net_sales?: CompareTop5NetSalesItem[];
  labels?: string[];
  values?: Array<number | string>;
  net_sales_labels?: string[];
  net_sales_values?: Array<number | string>;

  // CM1/current-vs-previous row format
  current_data?: ApiSkuRow[];
  previous_data?: ApiSkuRow[];

  // CM2 current format from /pie-chart
  cm2_profit?: Cm2ProfitItem[];

  // CM2 previous format from /pie-chart
  previous?: {
    available?: boolean;
    cm2_profit?: Cm2ProfitItem[];
    net_sales_labels?: string[];
    net_sales_values?: Array<number | string>;
  };

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
  userId,
  countryName,
  homeCurrency,
  onExportBase64Ready,
  disableInternalFade = false,
  metric = "net_sales",
  showCm2Toggle = false,
}) => {
  const normalizedHomeCurrency = (homeCurrency || "usd").toLowerCase();
  const isGlobalPage = (countryName || "").toLowerCase() === "global";

  const isDemoMode =
    String(month ?? "").toUpperCase() === "NA" ||
    String(year ?? "").toUpperCase() === "NA";

  const DEMO_SLICES: CmPieSlice[] = [
    { name: "Product A", value: 4200, prevValue: 3600, pct: 35, deltaPct: 16.67 },
    { name: "Product B", value: 3100, prevValue: 2800, pct: 25.83, deltaPct: 10.71 },
    { name: "Product C", value: 2200, prevValue: 1900, pct: 18.33, deltaPct: 15.79 },
    { name: "Product D", value: 1400, prevValue: 1200, pct: 11.67, deltaPct: 16.67 },
    { name: "Others", value: 1100, prevValue: 900, pct: 9.17, deltaPct: 22.22 },
  ];

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

  const [activeMetric, setActiveMetric] = useState<CmMetric>(
    metric === "cm2" && !showCm2Toggle ? "net_sales" : metric
  );
  const toggleOptions = useMemo(() => {
    const options: Array<{ value: CmMetric; label: string }> = [
      { value: "net_sales", label: "Net Sales" },
      { value: "cm1", label: "CM1" },
    ];

    if (showCm2Toggle) {
      options.push({ value: "cm2", label: "CM2" });
    }

    return options;
  }, [showCm2Toggle]);

  useEffect(() => {
    if (activeMetric === "cm2" && !showCm2Toggle) {
      setActiveMetric("net_sales");
    }
  }, [activeMetric, showCm2Toggle]);

  const isCm2Unavailable = activeMetric === "cm2" && !showCm2Toggle;
  const activeTitle =
    activeMetric === "net_sales" ? "Net Sales Breakdown" : "Profit Breakdown";
  const chartRef = useRef<any>(null);
  const requestIdRef = useRef(0);

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

  const getMetricValue = (row: ApiSkuRow, selectedMetric: CmMetric) => {
    if (selectedMetric === "cm2") return toNum(row.cm2_profit);
    if (selectedMetric === "net_sales") return toNum(row.net_sales);

    return toNum(row.profit);
  };

  const isTotalOrEmptyRow = (row: ApiSkuRow) => {
    const name = String(row.product_name || "").trim().toLowerCase();
    const sku = String(row.sku || "").trim().toLowerCase();

    return (
      !name ||
      name === "total" ||
      sku === "total" ||
      name === "others" ||
      sku === "others"
    );
  };

  const buildSlicesFromCurrentPreviousData = (
    currentData: ApiSkuRow[],
    previousData: ApiSkuRow[],
    selectedMetric: CmMetric
  ): CmPieSlice[] => {
    const previousMap = new Map<string, ApiSkuRow>();

    previousData
      .filter((row) => !isTotalOrEmptyRow(row))
      .forEach((row) => {
        const key = String(row.sku || row.product_name || "").trim().toLowerCase();
        if (key) previousMap.set(key, row);
      });

    const currentRows = currentData
      .filter((row) => !isTotalOrEmptyRow(row))
      .map((row) => {
        const key = String(row.sku || row.product_name || "").trim().toLowerCase();
        const previousRow = previousMap.get(key);

        const currentValue = getMetricValue(row, selectedMetric);
        const previousValue = previousRow
          ? getMetricValue(previousRow, selectedMetric)
          : 0;

        return {
          name: String(row.product_name || row.sku || "-"),
          value: currentValue,
          prevValue: previousValue,
        };
      })
      .filter((row) => Number.isFinite(row.value) && row.value !== 0)
      .sort((a, b) => Math.abs(b.value) - Math.abs(a.value));

    const top5 = currentRows.slice(0, 5);
    const remaining = currentRows.slice(5);

    const othersValue = remaining.reduce((sum, row) => sum + row.value, 0);
    const othersPrevValue = remaining.reduce((sum, row) => sum + row.prevValue, 0);

    const finalRows =
      remaining.length > 0
        ? [
          ...top5,
          {
            name: "Others",
            value: othersValue,
            prevValue: othersPrevValue,
          },
        ]
        : top5;

    const totalCurrent = finalRows.reduce(
      (sum, row) => sum + Math.abs(row.value),
      0
    );

    return finalRows.map((row) => {
      const currentAbs = Math.abs(row.value);
      const previousAbs = Math.abs(row.prevValue);

      return {
        name: row.name,
        value: currentAbs,
        prevValue: previousAbs,
        pct: totalCurrent ? (currentAbs / totalCurrent) * 100 : 0,
        deltaPct:
          previousAbs === 0
            ? null
            : ((currentAbs - previousAbs) / previousAbs) * 100,
      };
    });
  };

  const normalizeName = (value: unknown) =>
    String(value || "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, " ");

  const firstFiniteNumber = (...values: unknown[]) => {
    for (const value of values) {
      const num = Number(value);
      if (Number.isFinite(num)) return num;
    }

    return null;
  };

  const buildSlicesFromNetSalesCompare = (
    rows: CompareTop5NetSalesItem[]
  ): CmPieSlice[] => {
    const normalizedRows = rows
      .map((row) => {
        const currentValue = toNum(row.current_net_sales);
        const previousValue = toNum(row.previous_net_sales);

        return {
          name: String(row.product || "").trim(),
          value: Math.abs(currentValue),
          prevValue: Math.abs(previousValue),
          pctFromApi: firstFiniteNumber(
            row.current_sales_mix_percentage,
            row.sales_mix_curr
          ),
          deltaPctFromApi: firstFiniteNumber(row.net_sales_delta_percentage),
        };
      })
      .filter((row) => row.name && Number.isFinite(row.value) && row.value !== 0);

    const totalCurrent = normalizedRows.reduce((sum, row) => sum + row.value, 0);

    return normalizedRows.map((row) => {
      const fallbackPct = totalCurrent ? (row.value / totalCurrent) * 100 : 0;

      return {
        name: row.name,
        value: row.value,
        prevValue: row.prevValue,
        pct: row.pctFromApi == null ? fallbackPct : Math.abs(row.pctFromApi),
        deltaPct:
          row.deltaPctFromApi == null
            ? row.prevValue === 0
              ? null
              : ((row.value - row.prevValue) / row.prevValue) * 100
            : row.deltaPctFromApi,
      };
    });
  };

  const buildSlicesFromLabelsValues = (
    labels: string[] = [],
    values: Array<number | string> = [],
    previousValues: Array<number | string> = []
  ): CmPieSlice[] => {
    const numericValues = values.map((v) => Math.abs(toNum(v)));
    const totalCurrent = numericValues.reduce((a, b) => a + b, 0);

    return labels
      .map((label, i) => {
        const cur = numericValues[i] ?? 0;
        const prev = Math.abs(toNum(previousValues[i]));
        const pct = totalCurrent ? (cur / totalCurrent) * 100 : 0;

        return {
          name: label,
          value: cur,
          prevValue: prev,
          pct,
          deltaPct: prev === 0 ? null : ((cur - prev) / prev) * 100,
        };
      })
      .filter((row) => row.name && Number.isFinite(row.value) && row.value !== 0);
  };

  const buildSlicesFromCm2Profit = (
    cm2Rows: Cm2ProfitItem[],
    previousCm2Rows: Cm2ProfitItem[] = []
  ): CmPieSlice[] => {
    const previousMap = new Map<string, number>();

    previousCm2Rows.forEach((row) => {
      const key = normalizeName(row.product_name);
      if (!key) return;

      previousMap.set(key, toNum(row.cm2_profit));
    });

    const rows = cm2Rows
      .map((row) => {
        const name = String(row.product_name || "").trim();

        return {
          name,
          value: toNum(row.cm2_profit),
          prevValue: previousMap.get(normalizeName(name)) ?? 0,
        };
      })
      .filter((row) => row.name && Number.isFinite(row.value) && row.value !== 0)
      .sort((a, b) => Math.abs(b.value) - Math.abs(a.value));

    const top5 = rows.slice(0, 5);
    const remaining = rows.slice(5);

    const othersValue = remaining.reduce((sum, row) => sum + row.value, 0);
    const othersPrevValue = remaining.reduce((sum, row) => sum + row.prevValue, 0);

    const finalRows =
      remaining.length > 0
        ? [
          ...top5,
          {
            name: "Others",
            value: othersValue,
            prevValue: othersPrevValue,
          },
        ]
        : top5;

    const totalCurrent = finalRows.reduce(
      (sum, row) => sum + Math.abs(row.value),
      0
    );

    return finalRows.map((row) => {
      const currentAbs = Math.abs(row.value);
      const previousAbs = Math.abs(row.prevValue);

      return {
        name: row.name,
        value: currentAbs,
        prevValue: previousAbs,
        pct: totalCurrent ? (currentAbs / totalCurrent) * 100 : 0,
        deltaPct:
          previousAbs === 0
            ? null
            : ((currentAbs - previousAbs) / previousAbs) * 100,
      };
    });
  };

  useEffect(() => {
    const requestId = ++requestIdRef.current;
    const ac = new AbortController();

    const isLatestRequest = () => requestId === requestIdRef.current;

    if (isDemoMode) {
      setSlices(DEMO_SLICES);
      setLoading(false);
      setError(null);
      setNoDataFound(false);
      return () => ac.abort();
    }

    if (isCm2Unavailable) {
      setSlices([]);
      setLoading(false);
      setError(null);
      setNoDataFound(true);
      onExportBase64Ready?.(null);
      return () => ac.abort();
    }

    async function fetchData() {
      setLoading(true);
      setError(null);
      setNoDataFound(false);
      setSlices([]);

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
            signal: ac.signal,
          }
        );

        const raw = (await res.json()) as PieChartApiResponse;

        if (!isLatestRequest()) return;

        if (!res.ok) {
          const msg =
            (raw as any)?.error ||
            (raw as any)?.data?.error ||
            "Failed to fetch data";
          throw new Error(msg);
        }

        const payload: PieChartPayload = (raw as any)?.data ?? (raw as any) ?? {};

        const noDataPhrase = "no data found in any of the available tables";
        const apiErrorText = (payload.error || (raw as any)?.error || "").toLowerCase();

        if (apiErrorText.includes(noDataPhrase)) {
          setSlices([]);
          setNoDataFound(true);
          setError(null);
          return;
        }

        let built: CmPieSlice[] = [];

        // IMPORTANT:
        // If CM2 is selected, never fall back to CM1 data.
        if (activeMetric === "net_sales") {
          const rows = Array.isArray(payload.compare_top5_net_sales)
            ? payload.compare_top5_net_sales
            : [];

          if (rows.length) {
            built = buildSlicesFromNetSalesCompare(rows);
          } else if (
            Array.isArray(payload.net_sales_labels) &&
            Array.isArray(payload.net_sales_values)
          ) {
            built = buildSlicesFromLabelsValues(
              payload.net_sales_labels,
              payload.net_sales_values,
              payload.previous?.net_sales_values ?? []
            );
          }
        } else if (activeMetric === "cm2") {
          if (!showCm2Toggle || !Array.isArray(payload.cm2_profit)) {
            setSlices([]);
            setNoDataFound(true);
            setError(null);
            return;
          }

          built = buildSlicesFromCm2Profit(
            payload.cm2_profit,
            payload.previous?.cm2_profit ?? []
          );
        } else if (
          Array.isArray(payload.current_data) &&
          Array.isArray(payload.previous_data)
        ) {
          built = buildSlicesFromCurrentPreviousData(
            payload.current_data,
            payload.previous_data,
            "cm1"
          );
        } else {
          const rows = Array.isArray(payload.compare_top5)
            ? payload.compare_top5
            : [];

          if (rows.length) {
            const currentValues = rows.map((r) =>
              Math.abs(toNum(r.current_profit))
            );

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
            built = buildSlicesFromLabelsValues(payload.labels, payload.values);
          }
        }

        if (!isLatestRequest()) return;

        const isEmpty =
          built.length === 0 ||
          !built.some((s) => Number(s.value) > 0);

        if (isEmpty) {
          setSlices([]);
          setNoDataFound(true);
          setError(null);
          return;
        }

        setSlices(built);
        setNoDataFound(false);
      } catch (e) {
        if ((e as any)?.name === "AbortError") return;
        if (!isLatestRequest()) return;

        const msg = e instanceof Error ? e.message : "Unknown error";
        setError(msg);
        setSlices([]);
        setNoDataFound(true);
      } finally {
        if (isLatestRequest()) {
          setLoading(false);
        }
      }
    }

    fetchData();

    return () => {
      ac.abort();
    };
  }, [
    isDemoMode,
    isCm2Unavailable,
    range,
    month,
    year,
    selectedQuarter,
    countryName,
    userId,
    normalizedHomeCurrency,
    homeCurrency,
    isGlobalPage,
    activeMetric,
    showCm2Toggle,
    onExportBase64Ready,
  ]);

  const chartData = useMemo<ChartData<"pie", number[], string> | null>(() => {
    const labels = slices.map((s) => s.name);
    const values = slices.map((s) => Math.abs(toNum(s.value)));

    const hasRenderableData =
      labels.length > 0 &&
      values.some((v) => Number.isFinite(v) && v > 0);

    if (!hasRenderableData) return null;

    const bg = labels.map((_, i) => COLORS[i % COLORS.length]);

    return {
      labels,
      datasets: [
        {
          data: values,
          backgroundColor: bg,
          hoverBackgroundColor: bg, // ✅ same on hover = no color change
          borderWidth: 0,
          borderColor: "transparent",
          spacing: 0,
          hoverOffset: 4, // ✅ keep pop-out
          offset: 0,
        },
      ],
    };

  }, [slices]);

  const showEmptyState =
    !loading &&
    !isDemoMode &&
    (isCm2Unavailable || noDataFound || !chartData);

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
                  ? ""
                  : ` (${delta >= 0 ? "▲" : "▼"}${Math.abs(delta).toFixed(2)}%)`;

              return `${slice?.name ?? ctx.label}: ${currencySymbol}${Math.round(val).toLocaleString(
                undefined,
                { maximumFractionDigits: 0 }
              )} (${pct.toFixed(2)}%)${deltaText}`;
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
    <div className={[
      "relative w-full rounded-xl border border-slate-200 bg-white shadow-sm p-4 flex flex-col transition-opacity duration-300",
      disableInternalFade ? "pointer-events-none select-none opacity-45" : "opacity-100",
    ].join(" ")}>
      <div className="mb-3 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <PageBreadcrumb
          pageTitle={activeTitle}
          variant="page"
          align="left"
          textSize="2xl"
        />

        <SegmentedToggle<CmMetric>
          value={activeMetric}
          onChange={setActiveMetric}
          options={toggleOptions}
          textSizeClass="text-xs 2xl:text-sm"
          compact
        />
      </div>

      {loading && (
        <div className="flex-1 w-full min-h-[260px] md:min-h-[287px] xl:min-h-[300px] 2xl:min-h-[360px] flex items-center justify-center">
          <p className="text-center text-sm text-gray-500">Loading chart data...</p>
        </div>
      )}

      {/* {error && !loading && (
        <p className="text-center text-sm text-red-600">Error: {error}</p>
      )} */}

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

      {!showEmptyState && !isDemoMode && !isCm2Unavailable && chartData && (
        <div
          className="flex-1 w-full min-h-[260px] md:min-h-[287px] xl:min-h-[300px] 2xl:min-h-[360px]"
        >
          <div className="relative w-full flex flex-col xl:flex-row gap-4 xl:gap-6 items-stretch xl:items-center">
            {/* LEFT: PIE */}
            <div className="w-full xl:flex-1 min-w-0 h-[260px] md:h-[287px] xl:h-[300px] 2xl:h-[360px]">
              <Pie
                key={`${activeMetric}-${range}-${month ?? ""}-${selectedQuarter ?? ""}-${year}-${showCm2Toggle}`}
                ref={chartRef}
                data={chartData}
                options={options}
                className="!block"
                style={{ width: "100%", height: "100%" }}
              />
            </div>

            {/* RIGHT: LEGEND (Product + value + pct + delta) */}
            <div
              className="w-full xl:shrink-0 xl:self-center overflow-y-auto overflow-x-hidden pr-1 flex justify-center xl:justify-start"
              style={{
                width: isDesktop ? 260 : isLaptop ? 200 : "100%",
                maxHeight: "100%",
              }}
            >
              {/* ✅ w-full so columns align; items-start so every label starts same line */}
              <div className="grid w-full grid-cols-2 sm:grid-cols-3 xl:flex xl:w-auto xl:flex-col gap-x-6 gap-y-3 xl:gap-y-2">
                {slices.map((slice, i) => {
                  const dot = COLORS[i % COLORS.length];
                  const chart = chartRef.current;
                  const isVisible = chart ? chart.getDataVisibility(i) : true;

                  const value = Math.abs(toNum(slice.value));
                  const pct = toNum(slice.pct);
                  const delta = slice.deltaPct;

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
                      className="text-left w-full min-w-0"
                      onClick={() => {
                        const c = chartRef.current;
                        if (!c) return;
                        c.toggleDataVisibility(i);
                        c.update();
                        setLegendTick((t) => t + 1);
                      }}
                    >
                      {/* ✅ items-start keeps all labels aligned from the same top line */}
                      <div
                        className={`flex items-start gap-2 sm:gap-3 min-w-0 ${isVisible ? "opacity-100" : "opacity-40"
                          }`}
                      >
                        {/* ✅ fixed dot alignment (no baseline shifting) */}
                        <span
                          className="mt-[3px] inline-block h-2.5 w-2.5 rounded-full flex-none shrink-0"
                          style={{ backgroundColor: dot }}
                        />

                        <div className="min-w-0 flex flex-col items-start">
                          {/* Mobile: force 3 uniform lines */}
                          <div className="block sm:hidden w-full text-[10px]" style={{ color: "#414042" }}>
                            {/* line 1 */}
                            <div
                              className={`${isVisible ? "" : "line-through"} break-words leading-[1.2]`}
                              title={slice.name}
                            >
                              {slice.name}
                            </div>

                            {/* line 2 */}
                            <div className="leading-[1.2]">
                              {currencySymbol}
                              {Math.round(value).toLocaleString(undefined, {
                                maximumFractionDigits: 0,
                              })}
                            </div>

                            {/* line 3 */}
                            <div className="leading-[1.2]">
                              <span>({pct.toFixed(2)}%) </span>
                              {delta != null && (
                                <span
                                  className={deltaClass}
                                  style={{
                                    display: "inline-flex",
                                    alignItems: "center",
                                    gap: 1,
                                  }}
                                >
                                  (
                                  {delta >= 0 ? (
                                    <DeltaUpIcon className="h-3 w-3" />
                                  ) : (
                                    <DeltaDownIcon className="h-3 w-3" />
                                  )}
                                  {Math.abs(delta).toFixed(2)}%
                                  )
                                </span>
                              )}
                            </div>
                          </div>

                          {/* Tablet/Desktop: keep existing 2-line layout */}
                          <div className="hidden sm:flex min-w-0 flex-col items-start">
                            <div
                              className={`truncate text-[10px] 2xl:text-xs ${isVisible ? "" : "line-through"}`}
                              style={{ color: "#414042" }}
                              title={slice.name}
                            >
                              {slice.name}
                            </div>

                            <div
                              className="text-[10px] 2xl:text-xs break-words"
                              style={{ color: "#414042" }}
                            >
                              {currencySymbol}
                              {Math.round(value).toLocaleString(undefined, {
                                maximumFractionDigits: 0,
                              })}{" "}
                              ({pct.toFixed(2)}%){" "}
                              {delta != null && (
                                <span
                                  className={deltaClass}
                                  style={{
                                    display: "inline-flex",
                                    alignItems: "center",
                                    gap: 1,
                                  }}
                                >
                                  (
                                  {delta >= 0 ? (
                                    <DeltaUpIcon className="h-3 w-3" />
                                  ) : (
                                    <DeltaDownIcon className="h-3 w-3" />
                                  )}
                                  {Math.abs(delta).toFixed(2)}%
                                  )
                                </span>
                              )}
                            </div>
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
