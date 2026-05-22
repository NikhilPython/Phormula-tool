"use client";

import React, { useEffect, useMemo, useState, useRef } from "react";
import dynamic from "next/dynamic";
import PageBreadcrumb from "../common/PageBreadCrumb";
import SegmentedToggle from "../ui/SegmentedToggle";
import type { TrendChartExportApi } from "@/lib/utils/exportTypes";
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

type ChartMetric = "net_sales" | "units";

type TrendBucket = Record<string, number>;
type TrendBucketOrArray = TrendBucket | number[];

type PerformanceTrendSeries = {
  label: string; // "Q4'25" OR "Dec'25" OR "2025"
  net_sales: TrendBucketOrArray;
  units: TrendBucketOrArray;
};

type PerformanceTrendPayload = {
  x: Array<string | number>;
  xType: "day" | "month" | "year" | string;
  series: PerformanceTrendSeries[];
  message?: string;
};

type PerformanceTrendChartProps = {
  range?: "monthly" | "quarterly" | "yearly" | "";
  month?: string;
  quarter?: string;
  year?: string;
  countryName?: string;
  homeCurrency?: string;
  data?: PerformanceTrendPayload | null;
  metric?: "net_sales" | "units";
  loading?: boolean;
  error?: string | null;
  selectedStartDay?: number | null;
  selectedEndDay?: number | null;
  currencySymbol?: string;
  onExportApiReady?: (api: TrendChartExportApi | null) => void;
  isExpanded?: boolean;
  onToggleExpand?: () => void;
  isPreviewMode?: boolean;
};

const MONTH_ABBR_TO_IDX: Record<string, number> = {
  jan: 0,
  feb: 1,
  mar: 2,
  apr: 3,
  may: 4,
  jun: 5,
  jul: 6,
  aug: 7,
  sep: 8,
  oct: 9,
  nov: 10,
  dec: 11,
};

const FULL_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

const clampDay = (d: number) => Math.max(1, Math.min(31, d));

const ORANGE = "#ED9F50";
const GREY = "#CECBC7";
const GREEN = "#97A95F";

/** "Q4'25" => {year:2025, q:4, key:202504} */
const parseQuarterLabel = (label: string) => {
  const m = (label || "").trim().match(/^Q([1-4])'(\d{2})$/i);
  if (!m) return null;
  const q = Number(m[1]);
  const year = 2000 + Number(m[2]);
  return { year, q, key: year * 100 + q };
};

/** Supports: "Dec'25", "Q4'25", "2025" */
const parseLabelKey = (label: string): { key: number } | null => {
  const s = (label || "").trim();

  if (/^\d{4}$/.test(s)) {
    const y = Number(s);
    return { key: y * 100 };
  }

  const q = parseQuarterLabel(s);
  if (q) return { key: q.key };

  const mm = s.match(/^([A-Za-z]{3})'(\d{2})$/);
  if (mm) {
    const mon = mm[1].toLowerCase();
    const yy = Number(mm[2]);
    const year = 2000 + yy;
    const mi = MONTH_ABBR_TO_IDX[mon];
    if (mi == null) return null;
    return { key: year * 100 + (mi + 1) };
  }

  return null;
};

const parseMonthLabel = (label: string): { year: number; monthIdx: number } | null => {
  const s = (label || "").trim();
  const mm = s.match(/^([A-Za-z]{3})'(\d{2})$/);
  if (!mm) return null;

  const mon = mm[1].toLowerCase();
  const yy = Number(mm[2]);
  const year = 2000 + yy;

  const monthIdx = MONTH_ABBR_TO_IDX[mon];
  if (monthIdx == null) return null;

  return { year, monthIdx };
};

const daysInMonth = (year: number, monthIdx: number) => new Date(year, monthIdx + 1, 0).getDate();

const isYearLabel = (label: string) => /^\d{4}$/.test((label || "").trim());

const buildRecencyColorMap = (names: string[]) => {
  const parsed = names
    .map((name) => ({ name, parsed: parseLabelKey(name) }))
    .filter((x) => x.parsed != null) as Array<{ name: string; parsed: { key: number } }>;

  if (!parsed.length) {
    const map: Record<string, string> = {};
    names.forEach((n, i) => (map[n] = [ORANGE, GREY, GREEN][i] ?? GREY));
    return map;
  }

  const mostRecent = parsed.reduce((a, b) => (b.parsed.key > a.parsed.key ? b : a));
  const mostKey = mostRecent.parsed.key;

  // ✅ YEAR MODE: only Orange (selected) + Grey (previous). No Green.
  const yearMode = parsed.every((p) => isYearLabel(p.name));
  if (yearMode) {
    const map: Record<string, string> = {};
    names.forEach((n) => (map[n] = GREY));
    map[mostRecent.name] = ORANGE;

    const prevYearKey = mostKey - 100;
    const prevYear = parsed.find((p) => p.parsed.key === prevYearKey);
    if (prevYear) map[prevYear.name] = GREY;

    return map;
  }

  // month/quarter behavior (keeps "same last year" green)
  const sameLastYearKey = mostKey - 100;
  const sameLastYear = parsed.find((p) => p.parsed.key === sameLastYearKey);

  const previous = parsed
    .filter((p) => p.parsed.key < mostKey && p.name !== sameLastYear?.name)
    .sort((a, b) => b.parsed.key - a.parsed.key)[0];

  const map: Record<string, string> = {};
  names.forEach((n) => (map[n] = GREY));

  map[mostRecent.name] = ORANGE;
  if (previous) map[previous.name] = GREY;
  if (sameLastYear) map[sameLastYear.name] = GREEN;

  return map;
};

type SeriesKind = "daily" | "monthly";

type GenericPoint = {
  x: string;
  units?: number | null;
  net_sales?: number | null;
  monthLabel?: string | null;
};

type GenericSeries = {
  name: string;
  kind: SeriesKind;
  points: GenericPoint[];
  monthLen?: number | null;
};

const sortKeysForX = (keys: string[]) => {
  const allAreMonthAbbr = keys.every((k) => MONTH_ABBR_TO_IDX[k.toLowerCase()] != null);
  if (allAreMonthAbbr) {
    return [...keys].sort(
      (a, b) => (MONTH_ABBR_TO_IDX[a.toLowerCase()] ?? 99) - (MONTH_ABBR_TO_IDX[b.toLowerCase()] ?? 99)
    );
  }

  const allAreNumbers = keys.every((k) => !isNaN(Number(k)));
  if (allAreNumbers) return [...keys].sort((a, b) => Number(a) - Number(b));

  return [...keys].sort((a, b) => a.localeCompare(b));
};

const isQuarterlyPayload = (trend: PerformanceTrendPayload) => {
  if (String(trend.xType || "").toLowerCase() !== "month") return false;
  const quarterCount = (trend.series || []).filter((s) => !!parseQuarterLabel(s.label)).length;
  return quarterCount >= 2;
};

const isNumberArray = (v: any): v is number[] => Array.isArray(v);

const getMinNumericX = (xAxis: string[]) => {
  const nums = xAxis.map((v) => Number(v)).filter((n) => !isNaN(n));
  if (!nums.length) return null;
  return Math.min(...nums);
};

const getValueForX = (
  bucketOrArr: TrendBucketOrArray | undefined,
  xKey: string,
  dayIndexBase: 0 | 1
): number | null => {
  if (bucketOrArr == null) return null;

  if (isNumberArray(bucketOrArr)) {
    const dayNum = Number(xKey);
    if (isNaN(dayNum)) return null;
    const idx = dayNum - dayIndexBase;
    if (idx < 0 || idx >= bucketOrArr.length) return null;
    const v = bucketOrArr[idx];
    return typeof v === "number" ? v : null;
  }

  const v = (bucketOrArr as TrendBucket)[xKey];
  return typeof v === "number" ? v : null;
};

const QUARTER_START_MONTH_IDX: Record<number, number> = {
  1: 0,  // Q1 starts Jan
  2: 3,  // Q2 starts Apr
  3: 6,  // Q3 starts Jul
  4: 9,  // Q4 starts Oct
};

const normalizeMonthKeyToIdx = (key: string): number | null => {
  const s = String(key || "").trim().toLowerCase();

  // Supports "May", "may", "May 2025"
  const short = s.slice(0, 3);
  if (MONTH_ABBR_TO_IDX[short] != null) return MONTH_ABBR_TO_IDX[short];

  // Supports "5" or "05"
  const asNum = Number(s);
  if (!Number.isNaN(asNum) && asNum >= 1 && asNum <= 12) {
    return asNum - 1;
  }

  return null;
};

const getQuarterRelativeMonth = (
  quarterLabel: string,
  monthKey: string
): string | null => {
  const parsedQuarter = parseQuarterLabel(quarterLabel);
  if (!parsedQuarter) return null;

  const monthIdx = normalizeMonthKeyToIdx(monthKey);
  if (monthIdx == null) return null;

  const quarterStartIdx = QUARTER_START_MONTH_IDX[parsedQuarter.q];
  const relativeMonth = monthIdx - quarterStartIdx + 1;

  if (relativeMonth < 1 || relativeMonth > 3) return null;

  return String(relativeMonth);
};


const mapBackendTrendToSeries = (trend: PerformanceTrendPayload): { xAxis: string[]; series: GenericSeries[] } => {
  const xType = String(trend.xType || "").toLowerCase();
  const kind: SeriesKind = xType === "day" ? "daily" : "monthly";
  const seriesArr = trend.series || [];

  // Quarterly alignment mode
  if (isQuarterlyPayload(trend)) {
    const outXAxis = ["1", "2", "3"];

    const outSeries: GenericSeries[] = seriesArr.map((s) => {
      const bucket = (s.net_sales || s.units || {}) as TrendBucket;
      const keys = sortKeysForX(Object.keys(bucket));

      const ns = s.net_sales as any;
      const un = s.units as any;

      const pointsByPosition = new Map<string, GenericPoint>();

      keys.forEach((k, idx) => {
        const relativePos = getQuarterRelativeMonth(s.label, k) ?? String(idx + 1);

        pointsByPosition.set(relativePos, {
          x: relativePos,
          net_sales: typeof ns?.[k] === "number" ? ns[k] : null,
          units: typeof un?.[k] === "number" ? un[k] : null,
          monthLabel: k ?? null,
        });
      });

      const points: GenericPoint[] = outXAxis.map((pos) => {
        return (
          pointsByPosition.get(pos) ?? {
            x: pos,
            net_sales: null,
            units: null,
            monthLabel: null,
          }
        );
      });

      return { name: s.label, kind, points };
    });

    return { xAxis: outXAxis, series: outSeries };
  }

  // Non-quarterly
  let xAxis: string[] = [];
  if (trend.x?.length) {
    xAxis = trend.x.map((v) => String(v));
  } else {
    const set = new Set<string>();
    for (const s of seriesArr) {
      const ns = s.net_sales as any;
      const un = s.units as any;

      if (ns && !Array.isArray(ns)) Object.keys(ns).forEach((k) => set.add(k));
      if (un && !Array.isArray(un)) Object.keys(un).forEach((k) => set.add(k));
    }
    xAxis = sortKeysForX(Array.from(set));
  }

  const minX = xType === "day" ? getMinNumericX(xAxis) : null;
  const dayIndexBase: 0 | 1 = minX === 0 ? 0 : 1;

  const outSeries: GenericSeries[] = seriesArr.map((s) => {
    const ns = s.net_sales as TrendBucketOrArray | undefined;
    const un = s.units as TrendBucketOrArray | undefined;

    const m = xType === "day" ? parseMonthLabel(s.label) : null;
    const monthLen = m ? daysInMonth(m.year, m.monthIdx) : null;

    const points: GenericPoint[] = xAxis.map((x) => ({
      x,
      net_sales: getValueForX(ns, x, dayIndexBase),
      units: getValueForX(un, x, dayIndexBase),
    }));

    return { name: s.label, kind, points, monthLen };
  });

  return { xAxis, series: outSeries };
};



const LiveLineChart: React.FC<{
  xAxisData: string[];
  series: GenericSeries[];
  metric: ChartMetric;
  currencySymbol?: string;
  selectedStartDay?: number | null;
  selectedEndDay?: number | null;
  onExportApiReady?: (api: TrendChartExportApi | null) => void;
}> = ({ xAxisData, series, metric, currencySymbol, selectedStartDay, selectedEndDay, onExportApiReady }) => {
  const echartsInstanceRef = useRef<any>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  const isDaily = series[0]?.kind === "daily";

  const isQuarterCompare = useMemo(() => {
    return (
      !isDaily &&
      xAxisData.length === 3 &&
      xAxisData.every((x) => ["1", "2", "3"].includes(String(x)))
    );
  }, [isDaily, xAxisData]);

  useEffect(() => {
    if (!onExportApiReady) return;

    const api: TrendChartExportApi = {
      title: "Performance Trend",
      getChartBase64: () => {
        try {
          const inst = echartsInstanceRef.current;
          if (!inst) return null;
          return inst.getDataURL({
            type: "png",
            pixelRatio: 2,
            backgroundColor: "#FFFFFF",
          });
        } catch {
          return null;
        }
      },
    };

    onExportApiReady(api);
    return () => onExportApiReady(null);
  }, [onExportApiReady]);

  const isNumericAxis = useMemo(
    () => xAxisData.length > 0 && xAxisData.every((x) => !isNaN(Number(x))),
    [xAxisData]
  );

  const hasZeroBasedDays = useMemo(
    () => isDaily && isNumericAxis && xAxisData.some((x) => Number(x) === 0),
    [isDaily, isNumericAxis, xAxisData]
  );

  const displayDayShift = hasZeroBasedDays ? 1 : 0;

  const maxDisplayDay = useMemo(() => {
    if (!isDaily) return null;

    const lens = series
      .map((s) => s.monthLen)
      .filter((n): n is number => typeof n === "number" && !isNaN(n));

    if (lens.length) return Math.max(...lens);

    const nums = xAxisData.map((x) => Number(x)).filter((n) => !isNaN(n));
    if (!nums.length) return null;
    return Math.max(...nums) + displayDayShift;
  }, [isDaily, series, xAxisData, displayDayShift]);

  const renderXAxis = useMemo(() => {
    if (!isDaily || maxDisplayDay == null) return xAxisData;

    return xAxisData.filter((x) => {
      const n = Number(x);
      if (isNaN(n)) return true;
      return n + displayDayShift <= maxDisplayDay;
    });
  }, [xAxisData, isDaily, maxDisplayDay, displayDayShift]);

  const rangeActive = isDaily && selectedStartDay != null && selectedEndDay != null;
  const startDay = rangeActive ? clampDay(Math.min(selectedStartDay!, selectedEndDay!)) : null;
  const endDay = rangeActive ? clampDay(Math.max(selectedStartDay!, selectedEndDay!)) : null;

  const filteredXAxis = useMemo(() => {
    if (!rangeActive || startDay == null || endDay == null) return renderXAxis;

    const keep = new Set(
      Array.from({ length: endDay - startDay + 1 }, (_, i) => {
        const uiDay = startDay + i;
        const backendDay = uiDay - displayDayShift;
        return String(backendDay);
      })
    );

    return renderXAxis.filter((x) => keep.has(String(x)));
  }, [rangeActive, startDay, endDay, renderXAxis, displayDayShift]);

  // ✅ MONTH VIEW FIX (your screenshot):
  // If month axis has only one month (e.g., ["Jan"]), expand axis to Jan..Dec
  // and fill missing months with 0 so a line drops to 0 and continues flat.
  const isMonthAbbrAxis = useMemo(() => {
    if (isDaily || isQuarterCompare) return false;
    if (filteredXAxis.length === 0) return false;
    return filteredXAxis.every((m) => MONTH_ABBR_TO_IDX[String(m).toLowerCase()] != null);
  }, [isDaily, isQuarterCompare, filteredXAxis]);

  const fullYearXAxis = useMemo(() => {
    if (!isMonthAbbrAxis) return filteredXAxis;
    // Always show Jan..Dec like your other charts
    return FULL_MONTHS;
  }, [isMonthAbbrAxis, filteredXAxis]);

  // Fallback single-point fix for non-month cases (daily / numeric / etc.)
  const PAD_X = "__single_point_pad__";
  const isPadX = (x: any) => String(x) === PAD_X;

  const effectiveXAxis = useMemo(() => {
    // Month-abbr case handled by fullYearXAxis (no pad needed)
    if (isMonthAbbrAxis) return fullYearXAxis;

    // Otherwise, if only 1 category, add a hidden pad so ECharts renders a segment/point
    if (filteredXAxis.length === 1) return [filteredXAxis[0], PAD_X];
    return filteredXAxis;
  }, [isMonthAbbrAxis, fullYearXAxis, filteredXAxis]);

  const yAxisName =
    metric === "net_sales" ? (currencySymbol ? `(${currencySymbol})` : "Sales") : "Units (in nos.)";

  const colorMap = useMemo(() => buildRecencyColorMap(series.map((s) => s.name)), [series]);

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

  const option = {
    tooltip: {
      trigger: "axis",
      textStyle: { fontSize: 12, color: "#414042" },
      formatter: (params: any) => {
        let rawX = params?.[0]?.axisValue ?? "";
        if (isPadX(rawX)) rawX = effectiveXAxis[0];

        const header = isQuarterCompare
          ? `Month ${rawX}`
          : isDaily && isNumericAxis && !isNaN(Number(rawX))
            ? `Day ${Number(rawX) + displayDayShift}`
            : String(rawX);

        const lines = (params || [])
          .filter((p: any) => !isPadX(p?.axisValue))
          .map((p: any) => {
            const valObj = p?.data;

            const value =
              valObj == null
                ? null
                : typeof valObj === "object" && "value" in valObj
                  ? valObj.value
                  : valObj;

            const monthLabel =
              typeof valObj === "object" && valObj?.monthLabel ? String(valObj.monthLabel) : null;

            const fmtNumber = (n: number) =>
              Math.round(n).toLocaleString(undefined, {
                maximumFractionDigits: 0,
              });

            const displayValue =
              value == null
                ? "-"
                : metric === "net_sales"
                  ? `${currencySymbol ?? ""}${fmtNumber(Number(value))}`
                  : `${Number(value).toLocaleString()}`;

            const suffix =
              isQuarterCompare && monthLabel ? ` <span style="color:#6B7280;">(${monthLabel})</span>` : "";

            return `
              <div style="font-size:12px; line-height:1.4; color:#44042;">
                <span style="display:inline-block;width:10px;height:10px;margin-right:6px;background:${p.color};border-radius:0;"></span>
                <span>${p.seriesName}${suffix}: </span>
                <span style="color:#414042;">${displayValue}</span>
              </div>
            `;
          });

        return `
          <div style="font-size:12px; color:#414042;">
            <div style="font-weight:600; margin-bottom:4px; color:#1414042;">
              ${header}
            </div>
            ${lines.join("")}
          </div>
        `;
      },
    },

    legend: {
      top: 10,
      left: "left",
      orient: "horizontal",
      icon: "rect",
      itemWidth: 10,
      itemHeight: 10,
      itemGap: 14,
      textStyle: {
        fontSize: 12,
        color: "#6B7280",
        padding: [0, 6, 0, 6],
      },
      data: series.map((s) => s.name),
    },

    grid: { left: 46, right: 16, top: 62, bottom: 44 },

    xAxis: {
      type: "category",
      data: effectiveXAxis,
      boundaryGap: false,
      nameLocation: "middle",
      nameGap: 25,
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
        color: "#6B7280",
        formatter: (value: string) => {
          if (isPadX(value)) return "";

          if (isQuarterCompare) return `Month ${value}`;

          if (!isDaily || !isNumericAxis) return String(value);
          const n = Number(value);
          return isNaN(n) ? String(value) : String(n + displayDayShift);
        },
      },
    },

    yAxis: {
      type: "value",
      name: yAxisName,
      nameLocation: "middle",
      nameGap: 8,
      nameTextStyle: {
        color: "#6B7280",
        padding: [0, 0, 0, 0],
      },
      axisLine: {
        lineStyle: {
          color: "#D1D5DB",
          width: 1,
        },
      },
      axisLabel: {
        margin: 2,
        color: "#6B7280",
      },
      splitLine: {
        lineStyle: {
          color: "#E5E7EB",
        },
      },
    },

    series: series.map((ser) => {
      const lineColor = colorMap[ser.name] ?? GREY;

      const realPointCount = ser.points.filter((pt) => {
        const v = metric === "units" ? pt.units : pt.net_sales;
        return typeof v === "number" && !isNaN(v);
      }).length;

      const mapByX = new Map(ser.points.map((p) => [p.x, p] as const));

      // For month-abbr full-year axis:
      // - fill missing months with 0 (not null) so the line continues flat at 0.
      // This produces exactly the behavior in your screenshot (Jan value -> Feb 0 -> flat).
      const aligned = effectiveXAxis.map((x) => {
        if (isPadX(x)) return mapByX.get(effectiveXAxis[0]);
        return mapByX.get(x);
      });

      return {
        name: ser.name,
        type: "line",
        smooth: true,

        // Keep symbols hidden normally, but show if truly only one real point AND we're not in full-year fill mode.
        // showSymbol: !isMonthAbbrAxis && realPointCount <= 1,
        // symbolSize: !isMonthAbbrAxis && realPointCount <= 1 ? 6 : 0,

        // ✅ show points (you can keep conditional logic if you want)
        showSymbol: true,
        symbol: "circle",
        // ✅ thicker/larger points (radius-like)
        // Try 8–12 depending on your preference
        symbolSize: 7,

        emphasis: {
          scale: true, // disable default scaling animation
          itemStyle: {
            color: lineColor,
          },
          symbolSize: 11, // 👈 bigger circle on hover
        },


        connectNulls: false,
        lineStyle: { color: lineColor, width: 2 },
        itemStyle: {
          color: lineColor,
          borderWidth: 0,
        },

        data: aligned.map((p, idx) => {
          const xRaw = effectiveXAxis[idx];

          // pad category: duplicate first value
          if (isPadX(xRaw)) {
            const vPad = metric === "units" ? (p?.units ?? null) : (p?.net_sales ?? null);
            if (p && (p as any).monthLabel != null) {
              return vPad == null ? null : { value: vPad, monthLabel: (p as any).monthLabel };
            }
            return vPad;
          }

          // Month-abbr axis (Jan..Dec): fill missing months with 0
          if (isMonthAbbrAxis) {
            const v = metric === "units" ? (p?.units ?? null) : (p?.net_sales ?? null);
            const vFilled = v == null ? 0 : v; // ✅ key change
            if (p && (p as any).monthLabel != null) {
              return { value: vFilled, monthLabel: (p as any).monthLabel };
            }
            return vFilled;
          }

          // Daily clipping
          const uiDay =
            isDaily && isNumericAxis && !isNaN(Number(xRaw)) ? Number(xRaw) + displayDayShift : null;

          if (isDaily && uiDay != null && ser.monthLen != null && uiDay > ser.monthLen) {
            return null;
          }

          const v = metric === "units" ? (p?.units ?? null) : (p?.net_sales ?? null);

          if (p && (p as any).monthLabel != null) {
            return v == null ? null : { value: v, monthLabel: (p as any).monthLabel };
          }

          return v;
        }),
      };
    }),
  };

  return (
    <div ref={containerRef} className="w-full h-full min-h-0 overflow-hidden">
      <ReactECharts
        option={option}
        style={{ width: "100%", height: "100%" }}
        opts={{ renderer: "canvas" }}
        onChartReady={(instance) => {
          echartsInstanceRef.current = instance;
          try {
            instance.resize();
          } catch { }
        }}
      />
    </div>
  );
};

const buildFallbackTrend = (
  range?: "monthly" | "quarterly" | "yearly" | ""
): { xAxis: string[]; series: GenericSeries[] } => {
  if (range === "monthly") {
    return {
      xAxis: ["1"],
      series: [
        {
          name: "Current",
          kind: "daily" as const,
          points: [{ x: "1", net_sales: 0, units: 0 }],
          monthLen: 1,
        },
      ],
    };
  }

  if (range === "quarterly") {
    return {
      xAxis: ["1", "2", "3"],
      series: [
        {
          name: "Current",
          kind: "monthly" as const,
          points: [
            { x: "1", net_sales: 0, units: 0, monthLabel: "M1" },
            { x: "2", net_sales: 0, units: 0, monthLabel: "M2" },
            { x: "3", net_sales: 0, units: 0, monthLabel: "M3" },
          ],
        },
      ],
    };
  }

  return {
    xAxis: FULL_MONTHS,
    series: [
      {
        name: "Current",
        kind: "monthly" as const,
        points: FULL_MONTHS.map((m) => ({
          x: m,
          net_sales: 0,
          units: 0,
        })),
      },
    ],
  };
};


export default function PerformanceTrendChart(props: PerformanceTrendChartProps) {
  const [chartMetric, setChartMetric] = useState<ChartMetric>("net_sales");

  const isPreviewMode = props.isPreviewMode ?? false;

  useEffect(() => {
    if (props.metric === "net_sales" || props.metric === "units") {
      setChartMetric(props.metric);
    }
  }, [props.metric]);

  const loading =
    props.loading === true ||
    (!isPreviewMode && props.data == null && !props.error);
    
  const error = props.error ?? null;

  const mapped = useMemo(() => {
    if (isPreviewMode) {
      return buildFallbackTrend(props.range);
    }

    if (!props.data?.series?.length) {
      return { xAxis: [], series: [] };
    }

    const mappedData = mapBackendTrendToSeries(props.data);

    const hasUsableXAxis = mappedData.xAxis.length > 0;
    const hasUsableSeries = mappedData.series.length > 0;

    if (!hasUsableXAxis || !hasUsableSeries) {
      return { xAxis: [], series: [] };
    }

    return mappedData;
  }, [props.data, props.range, isPreviewMode]);

  const hasChartStructure = useMemo(() => {
    return mapped.xAxis.length > 0 && mapped.series.length > 0;
  }, [mapped]);

  // const hasRenderableTrendData = useMemo(() => {
  //   if (isPreviewMode) return true;

  //   return mapped.series.some((ser) =>
  //     ser.points.some((pt) => {
  //       const v = chartMetric === "units" ? pt.units : pt.net_sales;
  //       return typeof v === "number" && Math.abs(v) > 0.01;
  //     })
  //   );
  // }, [mapped, chartMetric, isPreviewMode]);

  return (
    <div className="w-full h-full min-h-0 overflow-hidden flex flex-col">
      <div className="shrink-0 flex items-center justify-between gap-3 w-full">
        <PageBreadcrumb pageTitle="Performance Trend" variant="page" textSize="2xl" />
        <div
          className="flex items-center shrink-0"
          data-no-expand
          onPointerDown={(e) => e.stopPropagation()}
          onClick={(e) => e.stopPropagation()}
        >
          <div className="flex items-center justify-between gap-2">
            <SegmentedToggle<ChartMetric>
              value={chartMetric}
              onChange={setChartMetric}
              options={[
                { value: "net_sales", label: "Net Sales" },
                { value: "units", label: "Units" },
              ]}
              textSizeClass="text-xs"
              className="border-[#D9D9D9E5] bg-white"
            />

            {props.onToggleExpand && (
              <button
                type="button"
                onClick={props.onToggleExpand}
                aria-label={props.isExpanded ? "Collapse chart" : "Expand chart"}
                title={props.isExpanded ? "Collapse" : "Expand"}
                className=" hidden lg:inline-flex rounded-md border border-gray-300 bg-white text-blue-700 p-1.5 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
              >
                {props.isExpanded ? (
                  <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                ) : (
                  <RiExpandDiagonalFill size={18} className="font-extrabold" />
                )}
              </button>
            )}
          </div>
        </div>
      </div>

      <div className="mt-2 flex-1 min-h-0 overflow-hidden">
        {loading && (
          <div className="flex-1 min-h-[260px] md:min-h-[287px] xl:min-h-[300px] 2xl:min-h-[360px] flex items-center justify-center">
            <div className="text-sm text-gray-500">Loading Chart data</div>
          </div>
        )}

        {error && !isPreviewMode && (
          <div className="flex-1 min-h-[260px] md:min-h-[287px] xl:min-h-[300px] 2xl:min-h-[360px] flex items-center justify-center">
            <div className="text-sm text-red-500">{error}</div>
          </div>
        )}

        {!loading && !error && hasChartStructure && (
          <LiveLineChart
            xAxisData={mapped.xAxis}
            series={mapped.series}
            metric={chartMetric}
            currencySymbol={props.currencySymbol}
            selectedStartDay={props.selectedStartDay}
            selectedEndDay={props.selectedEndDay}
            onExportApiReady={props.onExportApiReady}
          />
        )}

        {!loading && !error && !hasChartStructure && (
          <div className="flex-1 min-h-[260px] md:min-h-[287px] xl:min-h-[300px] 2xl:min-h-[360px] flex items-center justify-center">
            <div className="text-sm text-gray-400">No data available</div>
          </div>
        )}
      </div>
    </div>
  );
}