"use client";

import React, { useMemo, useState, useEffect, useRef } from "react";
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";
import dynamic from "next/dynamic";
import type { EChartsOption, EChartsType } from "echarts";

import SegmentedToggle from "../ui/SegmentedToggle";
import PageBreadcrumb from "../common/PageBreadCrumb";
import Loader from "../loader/Loader";

const ReactECharts = dynamic(() => import("echarts-for-react"), {
  ssr: false,
  loading: () => (
    <div className="flex items-center justify-center h-[260px] w-full">
      <Loader transparent />
    </div>
  ),
});

type ChartMetric = "net_sales" | "quantity";

type DailyPoint = {
  date: string; // YYYY-MM-DD
  quantity?: number;
  net_sales?: number;
};

type DailySeries = {
  previous: DailyPoint[];
  current_mtd: DailyPoint[];
};

type PeriodInfo = {
  label: string;
  start_date: string; // YYYY-MM-DD
  end_date: string; // YYYY-MM-DD
};

type Props = {
  dailySeries: DailySeries | null;
  periods?:
  | {
    previous?: PeriodInfo;
    current_mtd?: PeriodInfo;
  }
  | null;
  loading?: boolean;
  isRefreshing?: boolean;
  error?: string | null;

  selectedStartDay?: number | null;
  selectedEndDay?: number | null;
  currencySymbol?: string;
  fullMonthMode?: boolean;
};

const monthTickLabel = (p?: PeriodInfo) => {
  const src = p?.start_date || p?.end_date || "";
  if (!src) return p?.label || "";

  const [y, m] = src.split("-");
  const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const mi = Number(m) - 1;

  if (!Number.isFinite(mi) || mi < 0 || mi > 11) return p?.label || "";
  return `${monthNames[mi]}'${y.slice(-2)}`;
};

const clampDay = (d: number) => Math.max(1, Math.min(31, d));

type TooltipSeriesParam = {
  axisValue?: string | number;
  seriesName?: string;
  data?: any;
  marker?: string;
};

type PadPoint = { value: number | null; __isPad?: boolean };

const LiveLineChart: React.FC<{
  dataPrev: DailyPoint[];
  dataCurr: DailyPoint[];
  metric: ChartMetric;
  prevLabel?: string;
  currLabel?: string;
  currencySymbol?: string;
  selectedStartDay?: number | null;
  selectedEndDay?: number | null;
  showPrevious?: boolean;
  showCurrent?: boolean;
  fullMonthMode?: boolean;
  height?: number;
}> = ({
  dataPrev,
  dataCurr,
  metric,
  prevLabel,
  currLabel,
  currencySymbol,
  selectedStartDay,
  selectedEndDay,
  showPrevious = true,
  showCurrent = true,
  fullMonthMode = false,
  height,
}) => {
    const containerRef = useRef<HTMLDivElement | null>(null);
    const echartsInstanceRef = useRef<EChartsType | null>(null);

    const getDay = (dateStr: string) => Number(dateStr?.split("-")?.[2]);

    const rangeActive = selectedStartDay != null && selectedEndDay != null;
    const s = rangeActive ? clampDay(Math.min(selectedStartDay!, selectedEndDay!)) : null;
    const e = rangeActive ? clampDay(Math.max(selectedStartDay!, selectedEndDay!)) : null;

    // ✅ responsive sizing
    const [isCompactView, setIsCompactView] = useState(false);
    useEffect(() => {
      const compute = () => setIsCompactView(typeof window !== "undefined" && window.innerWidth < 1536);
      compute();
      window.addEventListener("resize", compute);
      return () => window.removeEventListener("resize", compute);
    }, []);

    // ✅ x-axis days forced to selected range, otherwise fallback to data min/max
    // const allDays = useMemo(() => {
    //   if (rangeActive && s != null && e != null) {
    //     return Array.from({ length: e - s + 1 }, (_, i) => s + i);
    //   }

    //   const prevDays = dataPrev.map((d) => getDay(d.date)).filter(Number.isFinite);
    //   const currDays = dataCurr.map((d) => getDay(d.date)).filter(Number.isFinite);
    //   const allDaysRaw = [...prevDays, ...currDays].sort((a, b) => a - b);

    //   if (!allDaysRaw.length) return [];

    //   const minDay = allDaysRaw[0];
    //   const maxDay = allDaysRaw[allDaysRaw.length - 1];
    //   return Array.from({ length: maxDay - minDay + 1 }, (_, i) => minDay + i);
    // }, [rangeActive, s, e, dataPrev, dataCurr]);

    // ✅ x-axis should stop where current fetched MTD data stops.
    // Do not extend to previous month's full 31 days.


    // const allDays = useMemo(() => {
    //   const hasRealMetricValue = (point: DailyPoint | undefined) => {
    //     if (!point) return false;

    //     const value =
    //       metric === "quantity"
    //         ? point.quantity
    //         : point.net_sales;

    //     return value !== undefined && value !== null && Number.isFinite(Number(value));
    //   };

    //   const currDays = dataCurr
    //     .filter(hasRealMetricValue)
    //     .map((d) => getDay(d.date))
    //     .filter(Number.isFinite)
    //     .sort((a, b) => a - b);

    //   const prevDays = dataPrev
    //     .filter(hasRealMetricValue)
    //     .map((d) => getDay(d.date))
    //     .filter(Number.isFinite)
    //     .sort((a, b) => a - b);

    //   // If user selected a date range, still never show beyond fetched current data.
    //   if (rangeActive && s != null && e != null) {
    //     const maxFetchedCurrentDay = currDays.length
    //       ? currDays[currDays.length - 1]
    //       : e;

    //     const finalEndDay = Math.min(e, maxFetchedCurrentDay);

    //     if (finalEndDay < s) return [];

    //     return Array.from(
    //       { length: finalEndDay - s + 1 },
    //       (_, i) => s + i
    //     );
    //   }

    //   // Normal MTD view: use current fetched days only.
    //   // This prevents previous month from stretching x-axis to 31.
    //   if (currDays.length) {
    //     const minDay = currDays[0];
    //     const maxDay = currDays[currDays.length - 1];

    //     return Array.from(
    //       { length: maxDay - minDay + 1 },
    //       (_, i) => minDay + i
    //     );
    //   }

    //   // Fallback only if current data is unavailable.
    //   if (prevDays.length) {
    //     const minDay = prevDays[0];
    //     const maxDay = prevDays[prevDays.length - 1];

    //     return Array.from(
    //       { length: maxDay - minDay + 1 },
    //       (_, i) => minDay + i
    //     );
    //   }

    //   return [];
    // }, [rangeActive, s, e, dataPrev, dataCurr, metric]);


    const allDays = useMemo(() => {
      const hasRealMetricValue = (point: DailyPoint | undefined) => {
        if (!point) return false;

        const value = metric === "quantity" ? point.quantity : point.net_sales;

        return (
          value !== undefined &&
          value !== null &&
          Number.isFinite(Number(value))
        );
      };

      const currDays = dataCurr
        .filter(hasRealMetricValue)
        .map((d) => getDay(d.date))
        .filter(Number.isFinite)
        .sort((a, b) => a - b);

      const prevDays = dataPrev
        .filter(hasRealMetricValue)
        .map((d) => getDay(d.date))
        .filter(Number.isFinite)
        .sort((a, b) => a - b);

      if (rangeActive && s != null && e != null) {
        const maxFetchedCurrentDay = currDays.length
          ? currDays[currDays.length - 1]
          : e;

        const finalEndDay = fullMonthMode ? e : Math.min(e, maxFetchedCurrentDay);

        if (finalEndDay < s) return [];

        return Array.from({ length: finalEndDay - s + 1 }, (_, i) => s + i);
      }

      // ✅ Expanded mode:
      // show previous month full available days,
      // but current month will naturally remain blank/null after current MTD.
      if (fullMonthMode && prevDays.length) {
        const minDay = Math.min(prevDays[0], currDays[0] ?? prevDays[0]);
        const maxDay = prevDays[prevDays.length - 1];

        return Array.from({ length: maxDay - minDay + 1 }, (_, i) => minDay + i);
      }

      // ✅ Normal mode:
      // show only current MTD days, e.g. July 1-2 and June 1-2.
      if (currDays.length) {
        const minDay = currDays[0];
        const maxDay = currDays[currDays.length - 1];

        return Array.from({ length: maxDay - minDay + 1 }, (_, i) => minDay + i);
      }

      if (prevDays.length) {
        const minDay = prevDays[0];
        const maxDay = prevDays[prevDays.length - 1];

        return Array.from({ length: maxDay - minDay + 1 }, (_, i) => minDay + i);
      }

      return [];
    }, [rangeActive, s, e, dataPrev, dataCurr, metric, fullMonthMode]);

    const xAxis = useMemo(() => allDays.map(String), [allDays]);

    const pickValue = (pt: DailyPoint | undefined) =>
      metric === "quantity" ? pt?.quantity ?? null : pt?.net_sales ?? null;

    const prevData = useMemo(() => {
      return allDays.map((day) => {
        const pt = dataPrev.find((d) => getDay(d.date) === day);
        return pickValue(pt);
      });
    }, [allDays, dataPrev, metric]);

    const currData = useMemo(() => {
      return allDays.map((day) => {
        const pt = dataCurr.find((d) => getDay(d.date) === day);
        return pickValue(pt);
      });
    }, [allDays, dataCurr, metric]);

    /**
     * ✅ Critical fix:
     * If a series has only ONE non-null point (like your screenshot: Day1 has value, Day2 null),
     * ECharts renders only a dot. We duplicate that value to an adjacent day and mark it as __isPad.
     * Tooltip hides __isPad points.
     */
    const ensureTwoPoints = (vals: Array<number | null>): Array<number | PadPoint | null> => {
      const numericIdxs = vals
        .map((v, i) => (typeof v === "number" && !Number.isNaN(v) ? i : -1))
        .filter((i) => i !== -1);

      if (numericIdxs.length >= 2) return vals;

      // If there are no numeric points, keep as-is
      if (numericIdxs.length === 0) return vals;

      // Only 1 numeric point -> duplicate into a neighbor index
      const idx = numericIdxs[0];
      const v = vals[idx];

      // Need at least 2 x points to draw a segment; if only 1 day on axis, just return.
      if (vals.length < 2) return vals;

      const neighbor = idx < vals.length - 1 ? idx + 1 : idx - 1;
      const out: Array<number | PadPoint | null> = [...vals];

      // If neighbor already has a value (rare), just return
      const nVal = out[neighbor] as any;
      if (typeof nVal === "number" && !Number.isNaN(nVal)) return out;

      out[neighbor] = { value: v as number | null, __isPad: true };
      return out;
    };

    const prevSeriesData = useMemo(() => ensureTwoPoints(prevData), [prevData]);
    const currSeriesData = useMemo(() => ensureTwoPoints(currData), [currData]);

    const yAxisName =
      metric === "net_sales"
        ? currencySymbol
          ? `(${currencySymbol})`
          : "Sales"
        : "Units (in nos.)";

    const axisFontSize = isCompactView ? 10 : 12;
    const axisNameFontSize = isCompactView ? 10 : 12;
    // const legendFontSize = isCompactView ? 10 : 12;
    const tooltipFontSize = isCompactView ? 10 : 12;

    const gridTop = isCompactView ? 28 : 32;
    const gridBottom = isCompactView ? 34 : 40;
    const gridLeft = isCompactView ? 36 : 40;

    const xNameGap = isCompactView ? 20 : 25;
    const yNameGap = isCompactView ? 30 : 40;
    // const legendItemGap = isCompactView ? 12 : 20;
    // const legendItemSize = isCompactView ? 10 : 12;

    const SYMBOL_SIZE = 7;
    const SYMBOL_HOVER_SIZE = 11;

    const option: EChartsOption = useMemo(
      () => ({
        color: ["#CECBC7", "#ED9F50"],
        animation: false,
        tooltip: {
          trigger: "axis",
          textStyle: { fontSize: tooltipFontSize },
          formatter: (rawParams: unknown) => {
            const params = (Array.isArray(rawParams) ? rawParams : []) as TooltipSeriesParam[];

            const day = params?.[0]?.axisValue ?? "";

            const stripRange = (name: string) => (name || "").replace(/\s*\d+\s*[–-]\s*\d+\s*$/g, "");

            const lines = params
              // ✅ hide synthetic pad points in tooltip
              .filter((p) => !(p?.data && typeof p.data === "object" && p.data.__isPad))
              .map((p) => {
                const seriesName = stripRange(p.seriesName ?? "");

                const raw = p.data;
                const value =
                  raw == null
                    ? null
                    : typeof raw === "object" && "value" in raw
                      ? raw.value
                      : raw;

                const shownNum = typeof value === "number" ? value : null;

                const shown =
                  shownNum == null
                    ? "-"
                    : metric === "net_sales"
                      ? `${currencySymbol ?? ""}${shownNum.toFixed(2)}`
                      : `${shownNum}`;

                return `${p.marker ?? ""}${seriesName} <b>${shown}</b>`;
              });

            return [`Day ${day}`, ...lines].join("<br/>");
          },
        },

        legend: {
          show: false,
        },

        grid: { left: gridLeft, right: 16, top: gridTop, bottom: gridBottom },

        xAxis: {
          type: "category",
          data: xAxis,
          boundaryGap: false,
          nameLocation: "middle",
          nameGap: xNameGap,
          axisLine: {
            lineStyle: {
              color: "#D1D5DB",
              width: 1,
            },
          },
          axisTick: {
            lineStyle: {
              color: "#D1D5DB",
            },
          },
          axisLabel: {
            fontSize: axisFontSize,
            color: "#6B7280",
          },
          nameTextStyle: {
            fontSize: axisNameFontSize,
            color: "#6B7280",
          },
        },

        yAxis: {
          type: "value",
          name: yAxisName,
          nameLocation: "middle",
          nameGap: yNameGap,
          axisLine: {
            lineStyle: {
              color: "#D1D5DB",
              width: 1,
            },
          },
          axisTick: {
            lineStyle: {
              color: "#D1D5DB",
            },
          },
          axisLabel: {
            fontSize: axisFontSize,
            color: "#6B7280",
          },
          nameTextStyle: {
            fontSize: axisNameFontSize,
            color: "#6B7280",
          },
          splitLine: {
            lineStyle: {
              color: "#E5E7EB",
            },
          },
        },

        series: [
          ...(showPrevious
            ? [
              {
                name: prevLabel || "Previous",
                type: "line" as const,
                smooth: true,
                connectNulls: false,
                data: prevSeriesData,
                animation: false,
                lineStyle: {
                  width: 2,
                  color: "#CECBC7",
                },

                showSymbol: true,
                symbol: "circle",
                symbolSize: SYMBOL_SIZE,
                itemStyle: {
                  color: "#CECBC7",
                  borderWidth: 0,
                },

                emphasis: {
                  scale: false,
                  symbolSize: SYMBOL_HOVER_SIZE,
                  itemStyle: {
                    color: "#CECBC7",
                  },
                },
              },
            ]
            : []),

          ...(showCurrent
            ? [
              {
                name: currLabel || "Current MTD",
                type: "line" as const,
                smooth: true,
                connectNulls: false,
                data: currSeriesData,
                animation: false,
                lineStyle: {
                  width: 2,
                  color: "#ED9F50",
                },

                showSymbol: true,
                symbol: "circle",
                symbolSize: SYMBOL_SIZE,
                itemStyle: {
                  color: "#ED9F50",
                  borderWidth: 0,
                },

                emphasis: {
                  scale: false,
                  symbolSize: SYMBOL_HOVER_SIZE,
                  itemStyle: {
                    color: "#ED9F50",
                  },
                },
              },
            ]
            : []),
        ],
      }),
      [
        tooltipFontSize,
        gridLeft,
        gridTop,
        gridBottom,
        xNameGap,
        axisFontSize,
        axisNameFontSize,
        yAxisName,
        yNameGap,
        xAxis,
        prevSeriesData,
        currSeriesData,
        metric,
        currencySymbol,
        prevLabel,
        currLabel,
        showPrevious,
        showCurrent,
      ]
    );

    useEffect(() => {
      const el = containerRef.current;
      if (!el) return;

      const ro = new ResizeObserver(() => {
        try {
          echartsInstanceRef.current?.resize();
        } catch { }
      });

      ro.observe(el);
      return () => ro.disconnect();
    }, []);

    const hasDays = allDays.length > 0;

    return (
      <div ref={containerRef} className="w-full h-full min-h-0 overflow-hidden">
        {!hasDays ? (
          <p className="text-xs text-gray-500">No daily data available.</p>
        ) : (
          <ReactECharts
            option={option}
            notMerge={true}
            lazyUpdate={false}
            style={{ width: "100%", height: isCompactView ? 252 : 255 }}
            onChartReady={(instance) => {
              echartsInstanceRef.current = instance as EChartsType;
              try {
                instance.resize();
              } catch { }
            }}
          />
        )}
      </div>
    );
  };

export default function LiveBiLineChartPanel({
  dailySeries,
  periods,
  loading,
  isRefreshing,
  error,
  selectedStartDay,
  selectedEndDay,
  currencySymbol,
  fullMonthMode = false,
}: Props) {

  const CHART_METRIC_KEY = "performance-trend-chart-metric";

  const [chartMetric, setChartMetric] = useState<ChartMetric>(() => {
    if (typeof window === "undefined") return "net_sales";

    const saved = localStorage.getItem(CHART_METRIC_KEY);

    return saved === "quantity" || saved === "net_sales"
      ? saved
      : "net_sales";
  });

  const [selectedPeriods, setSelectedPeriods] = useState<Record<string, boolean>>({
    previous: true,
    current_mtd: true,
  });

  const [isExpanded, setIsExpanded] = useState(false);

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") setIsExpanded(false);
    };

    if (isExpanded) {
      window.addEventListener("keydown", onKeyDown);
    }

    return () => window.removeEventListener("keydown", onKeyDown);
  }, [isExpanded]);

  const togglePeriod = (key: "previous" | "current_mtd") => {
    const selectedCount = Object.values(selectedPeriods).filter(Boolean).length;
    const isChecked = selectedPeriods[key] !== false;

    // Keep at least one line visible
    if (isChecked && selectedCount === 1) return;

    setSelectedPeriods((prev) => ({
      ...prev,
      [key]: !isChecked,
    }));
  };

  useEffect(() => {
    if (typeof window === "undefined") return;
    localStorage.setItem(CHART_METRIC_KEY, chartMetric);
  }, [chartMetric]);

  // const prevLegend = useMemo(() => monthTickLabel(periods?.previous), [periods]);
  // const currLegend = useMemo(() => monthTickLabel(periods?.current_mtd), [periods]);

  const stripDayRange = (label: string) =>
    label.replace(/\s+\d+\s*[–-]\s*\d+$/g, "");

  const rangeSuffix = useMemo(() => {
    if (selectedStartDay == null || selectedEndDay == null) return "";

    const s = Math.min(selectedStartDay, selectedEndDay);
    const e = Math.max(selectedStartDay, selectedEndDay);

    return ` ${s}–${e}`;
  }, [selectedStartDay, selectedEndDay]);

  const prevLegend = useMemo(
    () => `${stripDayRange(periods?.previous?.label || monthTickLabel(periods?.previous))}${rangeSuffix}`,
    [periods, rangeSuffix]
  );

  const currLegend = useMemo(
    () => `${stripDayRange(periods?.current_mtd?.label || monthTickLabel(periods?.current_mtd))}${rangeSuffix}`,
    [periods, rangeSuffix]
  );

  return (
    <div className="w-full">
      <div className="flex flex-row md:items-start justify-between gap-3">
        <div className="w-full md:w-auto flex justify-start">
          <PageBreadcrumb pageTitle="Performance Trend" variant="page" textSize="2xl" />
        </div>

        <div className="w-full md:w-auto flex justify-end items-center gap-2">
          <div className="w-fit">
            <SegmentedToggle<ChartMetric>
              value={chartMetric}
              onChange={setChartMetric}
              options={[
                { value: "net_sales", label: "Net Sales" },
                { value: "quantity", label: "Units" },
              ]}
              textSizeClass="text-xs"
              className="border-[#D9D9D9E5] bg-white w-fit"
            />
          </div>

          <button
            type="button"
            onClick={() => setIsExpanded((prev) => !prev)}
            className="h-8 w-8 flex items-center justify-center rounded-md border border-[#D9D9D9E5] bg-white text-charcoal-500 hover:bg-gray-50"
            title={isExpanded ? "Collapse" : "Expand full month view"}
            aria-label={isExpanded ? "Collapse" : "Expand full month view"}
          >
            {isExpanded ? (
              <RiCollapseDiagonalFill className="text-base" />
            ) : (
              <RiExpandDiagonalFill className="text-base" />
            )}
          </button>
        </div>
      </div>

      <div
        data-no-expand
        onPointerDown={(e) => e.stopPropagation()}
        onClick={(e) => e.stopPropagation()}
        className="shrink-0 flex flex-wrap items-center justify-center gap-3 w-full mt-0"
      >
        {[
          {
            key: "previous" as const,
            label: prevLegend,
            color: "#CECBC7",
          },
          {
            key: "current_mtd" as const,
            label: currLegend,
            color: "#ED9F50",
          },
        ].map(({ key, label, color }) => {
          const isChecked = selectedPeriods[key] !== false;

          return (
            <label
              key={key}
              data-no-expand
              onClick={(e) => e.stopPropagation()}
              onMouseDown={(e) => e.stopPropagation()}
              className={[
                "shrink-0",
                "flex items-center gap-1 sm:gap-1.5",
                "font-semibold select-none whitespace-nowrap",
                "text-[10px] 2xl:text-xs leading-none",
                "text-charcoal-500",
                "cursor-pointer",
              ].join(" ")}
            >
              <span
                data-no-expand
                className="flex items-center justify-center h-3 w-3 sm:h-3.5 sm:w-3.5 rounded-sm border transition"
                style={{
                  borderColor: color,
                  backgroundColor: isChecked ? color : "white",
                }}
                onMouseDown={(e) => e.stopPropagation()}
                onClick={(e) => {
                  e.stopPropagation();
                  togglePeriod(key);
                }}
              >
                {isChecked && (
                  <svg viewBox="0 0 24 24" width="14" height="14" className="text-white">
                    <path
                      fill="currentColor"
                      d="M20.285 6.709a1 1 0 0 0-1.414-1.414L9 15.168l-3.879-3.88a1 1 0 0 0-1.414 1.415l4.586 4.586a1 1 0 0 0 1.414 0l10-10Z"
                    />
                  </svg>
                )}
              </span>

              <span>{label}</span>
            </label>
          );
        })}
      </div>

      <div className="relative min-h-0">
        {loading ? (
          <div className="flex items-center justify-center h-[260px]">
            <Loader className="bg-[transparent]" />
          </div>
        ) : error ? (
          <div className="text-sm text-red-500">{error}</div>
        ) : dailySeries ? (
          <>
            <LiveLineChart
              dataPrev={dailySeries.previous || []}
              dataCurr={dailySeries.current_mtd || []}
              metric={chartMetric}
              prevLabel={
                isExpanded
                  ? stripDayRange(periods?.previous?.label || monthTickLabel(periods?.previous))
                  : prevLegend
              }
              currLabel={
                isExpanded
                  ? stripDayRange(periods?.current_mtd?.label || monthTickLabel(periods?.current_mtd))
                  : currLegend
              }
              currencySymbol={currencySymbol}
              selectedStartDay={isExpanded ? null : selectedStartDay}
              selectedEndDay={isExpanded ? null : selectedEndDay}
              showPrevious={selectedPeriods.previous !== false}
              showCurrent={selectedPeriods.current_mtd !== false}
              fullMonthMode={isExpanded || fullMonthMode}
            // height={isExpanded ? 520 : undefined}
            />

            {isRefreshing && (
              <div className="pointer-events-none absolute right-2 top-2 rounded-full bg-white/80 px-2 py-1 text-[10px] text-gray-500 shadow-sm">
                Refreshing…
              </div>
            )}
          </>
        ) : (
          <div className="text-sm text-gray-500">No daily data available.</div>
        )}
      </div>
    </div>
  );
}