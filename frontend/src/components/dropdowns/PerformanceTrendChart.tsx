"use client";

import React, { useEffect, useMemo, useState, useRef } from "react";
import dynamic from "next/dynamic";
import PageBreadcrumb from "../common/PageBreadCrumb";
import SegmentedToggle from "../ui/SegmentedToggle";
import type { TrendChartExportApi } from "@/lib/utils/exportTypes";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

type ChartMetric = "net_sales" | "units";

type TrendBucket = Record<string, number>;
type TrendBucketOrArray = TrendBucket | number[];

type PerformanceTrendSeries = {
  label: string; // "Q4'25" OR "Dec'25" OR "2025"
  // backend can send arrays for daily (monthly filter), or buckets for other modes
  net_sales: TrendBucketOrArray;
  units: TrendBucketOrArray;
};

type PerformanceTrendPayload = {
  x: Array<string | number>; // for your new daily payload, x = [1..31]
  xType: "day" | "month" | "year" | string;
  series: PerformanceTrendSeries[];
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

    const prevYearKey = mostKey - 100; // previous year
    const prevYear = parsed.find((p) => p.parsed.key === prevYearKey);
    if (prevYear) map[prevYear.name] = GREY; // explicitly grey (not green)

    return map;
  }

  // existing behavior for month/quarter (keeps "same last year" green)
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
  monthLen?: number | null; // used for daily compare view
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
  return quarterCount >= 2; // enough signal
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

  // Array style: day number -> index
  if (isNumberArray(bucketOrArr)) {
    const dayNum = Number(xKey);
    if (isNaN(dayNum)) return null;
    const idx = dayNum - dayIndexBase; // base=1 => day 1 -> idx 0
    if (idx < 0 || idx >= bucketOrArr.length) return null;
    const v = bucketOrArr[idx];
    return typeof v === "number" ? v : null;
  }

  // Object/bucket style
  const v = (bucketOrArr as TrendBucket)[xKey];
  return typeof v === "number" ? v : null;
};

/**
 * Mapper:
 * - Supports daily arrays + x=[1..N] (monthly filter response)
 * - Still supports old object/bucket payloads
 * - Keeps quarterly alignment logic (bucket-based)
 */
const mapBackendTrendToSeries = (trend: PerformanceTrendPayload): { xAxis: string[]; series: GenericSeries[] } => {
  const xType = String(trend.xType || "").toLowerCase();
  const kind: SeriesKind = xType === "day" ? "daily" : "monthly";
  const seriesArr = trend.series || [];

  // Quarterly alignment mode (bucket/object style) — ✅ position-based (Month 1..3)
  if (isQuarterlyPayload(trend)) {
    const outXAxis = ["1", "2", "3"]; // positions (we'll render "Month 1" in axisLabel)

    const outSeries: GenericSeries[] = seriesArr.map((s) => {
      const bucket = (s.net_sales || s.units || {}) as TrendBucket;

      // Sort the months inside this quarter for THIS series (Oct,Nov,Dec etc.)
      const keys = sortKeysForX(Object.keys(bucket));

      const points: GenericPoint[] = outXAxis.map((pos, idx) => {
        const k = keys[idx]; // month key for this series at position idx

        const ns = s.net_sales as any;
        const un = s.units as any;

        return {
          x: pos, // "1" | "2" | "3"
          net_sales: typeof ns?.[k] === "number" ? ns[k] : null,
          units: typeof un?.[k] === "number" ? un[k] : null,

          // store the real month label for tooltip
          monthLabel: k ?? null,
        } as any;
      });

      return { name: s.label, kind, points };
    });

    return { xAxis: outXAxis, series: outSeries };
  }

  // Non-quarterly
  // Prefer trend.x (daily payload)
  let xAxis: string[] = [];
  if (trend.x?.length) {
    xAxis = trend.x.map((v) => String(v));
  } else {
    // fallback union keys (old bucket format)
    const set = new Set<string>();
    for (const s of seriesArr) {
      const ns = s.net_sales as any;
      const un = s.units as any;

      if (ns && !Array.isArray(ns)) Object.keys(ns).forEach((k) => set.add(k));
      if (un && !Array.isArray(un)) Object.keys(un).forEach((k) => set.add(k));
    }
    xAxis = sortKeysForX(Array.from(set));
  }

  // For daily arrays, decide if x starts at 0 or 1
  const minX = xType === "day" ? getMinNumericX(xAxis) : null;
  const dayIndexBase: 0 | 1 = minX === 0 ? 0 : 1;

  const outSeries: GenericSeries[] = seriesArr.map((s) => {
    const ns = s.net_sales as TrendBucketOrArray | undefined;
    const un = s.units as TrendBucketOrArray | undefined;

    // monthLen based on series label like "Feb'25" (only used for daily)
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
  const chartRef = useRef<any>(null);
  const echartsInstanceRef = useRef<any>(null);
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

  // if backend ever sends 0-based days, we display +1; if it sends 1-based, shift is 0
  const hasZeroBasedDays = useMemo(
    () => isDaily && isNumericAxis && xAxisData.some((x) => Number(x) === 0),
    [isDaily, isNumericAxis, xAxisData]
  );

  const displayDayShift = hasZeroBasedDays ? 1 : 0;

  // ✅ Use MAX days across all compared months (so Jan => 31 shows fully)
  const maxDisplayDay = useMemo(() => {
    if (!isDaily) return null;

    const lens = series
      .map((s) => s.monthLen)
      .filter((n): n is number => typeof n === "number" && !isNaN(n));

    if (lens.length) return Math.max(...lens);

    // fallback: infer from axis
    const nums = xAxisData.map((x) => Number(x)).filter((n) => !isNaN(n));
    if (!nums.length) return null;
    return Math.max(...nums) + displayDayShift;
  }, [isDaily, series, xAxisData, displayDayShift]);

  // Crop axis so it never shows beyond maxDisplayDay
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

  // Range filter uses UI days 1..N; if backend is 0-based, map uiDay -> backendDay
  const filteredXAxis = useMemo(() => {
    if (!rangeActive || startDay == null || endDay == null) return renderXAxis;

    const keep = new Set(
      Array.from({ length: endDay - startDay + 1 }, (_, i) => {
        const uiDay = startDay + i;
        const backendDay = uiDay - displayDayShift; // 0-based => ui 1 -> backend 0
        return String(backendDay);
      })
    );

    return renderXAxis.filter((x) => keep.has(String(x)));
  }, [rangeActive, startDay, endDay, renderXAxis, displayDayShift]);

  const yAxisName =
    metric === "net_sales" ? (currencySymbol ? `(${currencySymbol})` : "Sales") : "Units (in nos.)";

  const colorMap = useMemo(() => buildRecencyColorMap(series.map((s) => s.name)), [series]);

  const option = {
    tooltip: {
      trigger: "axis",
      formatter: (params: any) => {
        const rawX = params?.[0]?.axisValue ?? "";

        // header
        const header = isQuarterCompare
          ? `Month ${rawX}`
          : isDaily && isNumericAxis && !isNaN(Number(rawX))
            ? `Day ${Number(rawX) + displayDayShift}`
            : String(rawX);

        const lines = (params || []).map((p: any) => {
          // IMPORTANT: because we return { value, monthLabel } for quarterly
          const valObj = p?.data;
          const value =
            valObj == null
              ? null
              : typeof valObj === "object" && "value" in valObj
                ? valObj.value
                : valObj;

          const monthLabel =
            typeof valObj === "object" && valObj?.monthLabel ? String(valObj.monthLabel) : null;

          const shown =
            value == null
              ? "-"
              : metric === "net_sales"
                ? `${currencySymbol ?? ""}${Number(value).toFixed(2)}`
                : `${Number(value)}`;

          const suffix =
            isQuarterCompare && monthLabel ? ` <span style="color:#6B7280">(${monthLabel})</span>` : "";

          return `${p.marker}${p.seriesName}${suffix} <b>${shown}</b>`;
        });

        return [header, ...lines].join("<br/>");
      },

    },
    legend: {
      top: 10,
      left: "left",
      orient: "horizontal",
      icon: "rect",
      itemWidth: 12,
      itemHeight: 12,
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
      data: filteredXAxis,
      boundaryGap: false,
      // name: isDaily ? "Days" : "Month",
      nameLocation: "middle",
      nameGap: 25,
      axisLabel: {
        formatter: (value: string) => {
          // ✅ Quarter compare mode
          if (isQuarterCompare) return `Month ${value}`;

          // existing daily behavior
          if (!isDaily || !isNumericAxis) return String(value);
          const n = Number(value);
          return isNaN(n) ? String(value) : String(n + displayDayShift);
        },
      },
    },
    yAxis: {
      type: "value",
      name: yAxisName,              // "(£)"
      nameLocation: "middle",
      nameGap: 16,                  // was 40  ✅ closer to axis
      nameTextStyle: {
        padding: [0, 0, 0, 0],       // ✅ no extra padding
      },
      axisLabel: {
        margin: 4,                  // ✅ reduces space between axis and numbers
      },
    },


    // ✅ IMPORTANT: series must be here (NOT option.data)
    series: series.map((ser) => {
      const mapByX = new Map(ser.points.map((p) => [p.x, p] as const));
      const aligned = filteredXAxis.map((x) => mapByX.get(x));
      const lineColor = colorMap[ser.name] ?? GREY;

      return {
        name: ser.name,
        type: "line",
        smooth: true,
        showSymbol: false,
        connectNulls: false,
        lineStyle: { color: lineColor, width: 2 },
        itemStyle: { color: lineColor },

        // ✅ don't plot beyond this month's length (Feb stops at 28, Jan continues to 31)
        data: aligned.map((p, idx) => {
          const xRaw = filteredXAxis[idx];
          const uiDay =
            isDaily && isNumericAxis && !isNaN(Number(xRaw))
              ? Number(xRaw) + displayDayShift
              : null;

          if (isDaily && uiDay != null && ser.monthLen != null && uiDay > ser.monthLen) {
            return null;
          }

          const v = metric === "units" ? (p?.units ?? null) : (p?.net_sales ?? null);

          // keep monthLabel for tooltip (quarter compare)
          if (p && (p as any).monthLabel != null) {
            return v == null ? null : { value: v, monthLabel: (p as any).monthLabel };
          }

          return v;

        }),
      };
    }),
  };

  // return (
  //   <div style={{ width: "100%", height: "100%" }}>
  //     <ReactECharts
  //       option={option}
  //       style={{ width: "100%", height: "100%" }}
  //       opts={{ renderer: "canvas" }}
  //       onChartReady={(instance) => {
  //         echartsInstanceRef.current = instance;
  //       }}
  //     />
  //   </div>
  // );

  const containerRef = useRef<HTMLDivElement | null>(null);
  
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


  return (
    <div ref={containerRef} style={{ width: "100%", height: "100%" }}>
      <ReactECharts
        option={option}
        style={{ width: "100%", height: "100%" }}
        opts={{ renderer: "canvas" }}
        onChartReady={(instance) => {
          echartsInstanceRef.current = instance;
          // one immediate resize helps too
          try { instance.resize(); } catch { }
        }}
      />
    </div>
  );


};

export default function PerformanceTrendChart(props: PerformanceTrendChartProps) {
  const [chartMetric, setChartMetric] = useState<ChartMetric>("net_sales");

  useEffect(() => {
    if (props.metric === "net_sales" || props.metric === "units") {
      setChartMetric(props.metric);
    }
  }, [props.metric]);

  const loading = props.loading ?? false;
  const error = props.error ?? null;

  const mapped = useMemo(() => {
    if (!props.data?.series?.length) return { xAxis: [], series: [] as GenericSeries[] };
    return mapBackendTrendToSeries(props.data);
  }, [props.data]);




  return (
    <div className="w-full h-full min-h-0 flex flex-col">

      <div className="flex flex-col md:flex-row items-center md:items-start justify-between gap-3">
        <PageBreadcrumb pageTitle="Performance Trend" variant="page" textSize="2xl" />

        <div
          className="w-full md:w-auto"
          data-no-expand
          onPointerDown={(e) => e.stopPropagation()}
          onClick={(e) => e.stopPropagation()}
        >
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
        </div>
      </div>

      {/* <div className="mt-2 h-[280px] md:h-[320px] lg:h-[320px] 2xl:h-[480px] overflow-hidden"> */}
      <div className="mt-2 flex-1 min-h-0 overflow-hidden">
        {loading && <div className="text-sm text-gray-500">Loading chart…</div>}
        {error && <div className="text-sm text-red-500">{error}</div>}

        {!loading && !error && mapped.series.length > 0 && (
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

        {!loading && !error && mapped.series.length === 0 && (
          <div className="text-sm text-gray-500">Loading...</div>
        )}
      </div>

    </div>
  );
}
