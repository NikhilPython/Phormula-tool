"use client";

import React, { useMemo, useState } from "react";
import dynamic from "next/dynamic";
import PageBreadcrumb from "../common/PageBreadCrumb";
import SegmentedToggle from "../ui/SegmentedToggle";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

type ChartMetric = "net_sales" | "quantity";

type DailyPoint = {
  date: string; // YYYY-MM-DD (kept for your future mapping)
  quantity?: number;
  net_sales?: number;
};

type DailySeries = {
  previous: DailyPoint[];
  current_mtd: DailyPoint[];
};

type PeriodInfo = {
  label: string;
  start_date: string;
  end_date: string;
};

type PerformanceTrendChartProps = {
  // ✅ later you will map these from Dropdowns page / API
  dailySeries?: DailySeries | null;
  periods?: {
    previous?: PeriodInfo;
    current_mtd?: PeriodInfo;
  } | null;

  loading?: boolean;
  error?: string | null;

  selectedStartDay?: number | null;
  selectedEndDay?: number | null;

  currencySymbol?: string;
  range?: "monthly" | "quarterly" | "yearly";
  month?: string;
  quarter?: "Q1" | "Q2" | "Q3" | "Q4";
  year?: number | string;
  countryName?: string;
  homeCurrency?: string;
};

/* =========================
   Helpers: month / quarter
========================= */

const MONTHS = [
  "january",
  "february",
  "march",
  "april",
  "may",
  "june",
  "july",
  "august",
  "september",
  "october",
  "november",
  "december",
] as const;

const monthToIndex = (m?: string) => {
  const mm = (m || "").toLowerCase();
  return MONTHS.indexOf(mm as any);
};

const monthLabelShort = (year: number, monthIdx: number) => {
  const short = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][monthIdx];
  return `${short}'${String(year).slice(-2)}`;
};

const monthIdxToNumber = (idx: number) => idx + 1;

const prevMonth = (year: number, monthIdx: number) => {
  if (monthIdx === 0) return { year: year - 1, monthIdx: 11 };
  return { year, monthIdx: monthIdx - 1 };
};

const QUARTERS: Array<"Q1" | "Q2" | "Q3" | "Q4"> = ["Q1", "Q2", "Q3", "Q4"];

const prevQuarter = (year: number, q: "Q1" | "Q2" | "Q3" | "Q4") => {
  const idx = QUARTERS.indexOf(q);
  const prevIdx = (idx - 1 + 4) % 4;
  return { year: idx === 0 ? year - 1 : year, quarter: QUARTERS[prevIdx] };
};

const quarterToMonths = (q: "Q1" | "Q2" | "Q3" | "Q4") => {
  switch (q) {
    case "Q1":
      return [0, 1, 2];
    case "Q2":
      return [3, 4, 5];
    case "Q3":
      return [6, 7, 8];
    case "Q4":
      return [9, 10, 11];
  }
};

const quarterLabelShort = (q: "Q1" | "Q2" | "Q3" | "Q4", year: number) => `${q}'${String(year).slice(-2)}`;

const clampDay = (d: number) => Math.max(1, Math.min(31, d));

/* =========================
   Dummy series types
========================= */

type SeriesKind = "daily" | "monthly";

type GenericPoint = {
  x: string; // day "1..31" OR month label "Jan'25"
  quantity?: number;
  net_sales?: number;
};

type GenericSeries = {
  name: string;
  kind: SeriesKind;
  points: GenericPoint[];
};

/* =========================
   Dummy generators
   (replace later with API mapping)
========================= */

const makeDummyDaily = (year: number, month: number, name: string): GenericSeries => {
  const mm = String(month).padStart(2, "0");
  const days = Array.from({ length: 31 }, (_, i) => i + 1);

  const points: GenericPoint[] = days.map((d) => {
    const dd = String(d).padStart(2, "0");
    // smooth-ish patterns
    const baseSales = 1200 + Math.sin(d / 4) * 180 + Math.random() * 60 + (month % 2 ? 120 : 0);
    const baseQty = 90 + Math.sin(d / 5) * 10 + Math.random() * 3 + (month % 2 ? 6 : 0);

    return {
      x: String(d),
      // date kept for future mapping (not used in dummy chart)
      net_sales: Number(baseSales.toFixed(2)),
      quantity: Math.max(0, Math.round(baseQty)),
    };
  });

  // keep a reference to date format in case you need later
  void mm; // no-op (prevents unused warning if you later remove date usage)

  return { name, kind: "daily", points };
};

const makeDummyMonthlyTotals = (year: number, name: string): GenericSeries => {
  const points: GenericPoint[] = Array.from({ length: 12 }, (_, idx) => {
    const baseSales = 7000 + Math.sin((idx + 1) / 2) * 900 + Math.random() * 200;
    const baseQty = 600 + Math.sin((idx + 1) / 2) * 60 + Math.random() * 20;

    return {
      x: monthLabelShort(year, idx),
      net_sales: Number(baseSales.toFixed(2)),
      quantity: Math.max(0, Math.round(baseQty)),
    };
  });

  return { name, kind: "monthly", points };
};

const makeDummyQuarterTotals = (year: number, q: "Q1" | "Q2" | "Q3" | "Q4", name: string): GenericSeries => {
  const months = quarterToMonths(q);

  const points: GenericPoint[] = months.map((idx) => {
    const baseSales = 7000 + Math.sin((idx + 1) / 2) * 900 + Math.random() * 200;
    const baseQty = 600 + Math.sin((idx + 1) / 2) * 60 + Math.random() * 20;

    return {
      x: monthLabelShort(year, idx),
      net_sales: Number(baseSales.toFixed(2)),
      quantity: Math.max(0, Math.round(baseQty)),
    };
  });

  return { name, kind: "monthly", points };
};

/* =========================
   Chart Component (multi-series)
========================= */

const LiveLineChart: React.FC<{
  series: GenericSeries[];
  metric: ChartMetric;
  currencySymbol?: string;
  selectedStartDay?: number | null;
  selectedEndDay?: number | null;
}> = ({ series, metric, currencySymbol, selectedStartDay, selectedEndDay }) => {
  const isDaily = series[0]?.kind === "daily";

  // Use selected day range only in daily view
  const rangeActive = isDaily && selectedStartDay != null && selectedEndDay != null;
  const s = rangeActive ? clampDay(Math.min(selectedStartDay!, selectedEndDay!)) : null;
  const e = rangeActive ? clampDay(Math.max(selectedStartDay!, selectedEndDay!)) : null;

  const xAxisAll = series[0]?.points.map((p) => p.x) ?? [];

  // Filter X axis for day range
  const xAxisData = useMemo(() => {
    if (!rangeActive || s == null || e == null) return xAxisAll;
    const keep = new Set(Array.from({ length: e - s + 1 }, (_, i) => String(s + i)));
    return xAxisAll.filter((x) => keep.has(x));
  }, [rangeActive, s, e, xAxisAll]);

  const yAxisName =
    metric === "net_sales" ? (currencySymbol ? `Sales (${currencySymbol})` : "Sales") : "Units";

  const option = {
    color: ["#CECBC7", "#ED9F50", "#97A95F"], 
    tooltip: {
      trigger: "axis",
      formatter: (params: any) => {
        const x = params?.[0]?.axisValue ?? "";
        const header = isDaily ? `Day ${x}` : x;

        const lines = (params || []).map((p: any) => {
          const val = p.data;
          const shown =
            val == null
              ? "-"
              : metric === "net_sales"
              ? `${currencySymbol ?? ""}${Number(val).toFixed(2)}`
              : `${Number(val)}`;

          return `${p.marker}${p.seriesName} <b>${shown}</b>`;
        });

        return [header, ...lines].join("<br/>");
      },
    },
  legend: {
  top: 10,
  left: "left",
  orient: "horizontal",

  icon: "rect",          // already correct
  itemWidth: 12,         // 👈 square width
  itemHeight: 12,        // 👈 square height
  itemGap: 14,           // spacing between legend items

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
      data: xAxisData,
      boundaryGap: false,
      name: isDaily ? "Days" : "Month",
      nameLocation: "middle",
      nameGap: 25,
    },
    yAxis: {
      type: "value",
      name: yAxisName,
      nameLocation: "middle",
      nameGap: 40,
    },
    series: series.map((s) => {
      // align points to xAxisData
      const mapByX = new Map(s.points.map((p) => [p.x, p] as const));
      const aligned = xAxisData.map((x) => mapByX.get(x));

      return {
        name: s.name,
        type: "line",
        smooth: true,
        showSymbol: false,
        data: aligned.map((p) =>
          metric === "quantity" ? (p?.quantity ?? null) : (p?.net_sales ?? null)
        ),
      };
    }),
  };

  return (
    <div className="w-full h-full">
      <ReactECharts option={option} style={{ width: "100%", height: "100%" }} />
    </div>
  );
};

/* =========================
   Main Component
========================= */

export default function PerformanceTrendChart(props: PerformanceTrendChartProps) {
  const [chartMetric, setChartMetric] = useState<ChartMetric>("net_sales");

  const yearNum = Number(props.year ?? new Date().getFullYear());
  const range = props.range ?? "monthly";

  // ✅ COMPARISONS:
  // Monthly: selected month + prev month + same month last year
  // Quarterly: selected quarter + prev quarter + same quarter last year
  // Yearly: selected year + previous year
  const seriesList: GenericSeries[] = useMemo(() => {
    if (range === "monthly") {
      const mIdx = monthToIndex(props.month);
      const safeIdx = mIdx >= 0 ? mIdx : 11; // fallback Dec
      const monthNum = monthIdxToNumber(safeIdx);

      const currName = monthLabelShort(yearNum, safeIdx);

      const pm = prevMonth(yearNum, safeIdx);
      const prevName = monthLabelShort(pm.year, pm.monthIdx);

      const lastYearName = monthLabelShort(yearNum - 1, safeIdx);

      return [
        makeDummyDaily(yearNum, monthNum, currName),
        makeDummyDaily(pm.year, monthIdxToNumber(pm.monthIdx), prevName),
        makeDummyDaily(yearNum - 1, monthNum, lastYearName),
      ];
    }

    if (range === "quarterly") {
      const q = props.quarter ?? "Q4";

      const currName = quarterLabelShort(q, yearNum);

      const pq = prevQuarter(yearNum, q);
      const prevName = quarterLabelShort(pq.quarter, pq.year);

      const lastYearName = quarterLabelShort(q, yearNum - 1);

      return [
        makeDummyQuarterTotals(yearNum, q, currName),
        makeDummyQuarterTotals(pq.year, pq.quarter, prevName),
        makeDummyQuarterTotals(yearNum - 1, q, lastYearName),
      ];
    }

    // yearly
    return [
      makeDummyMonthlyTotals(yearNum, String(yearNum)),
      makeDummyMonthlyTotals(yearNum - 1, String(yearNum - 1)),
    ];
  }, [range, props.month, props.quarter, yearNum]);

  const loading = props.loading ?? false;
  const error = props.error ?? null;

  return (
    <div className="w-full h-full flex flex-col">
      {/* Header */}
      <div className="flex flex-col md:flex-row items-center md:items-start justify-between gap-3">
        <PageBreadcrumb pageTitle="Performance Trend" variant="page" textSize="2xl" />

        <div className="w-full md:w-auto">
          <SegmentedToggle<ChartMetric>
            value={chartMetric}
            onChange={setChartMetric}
            options={[
              { value: "net_sales", label: "Net Sales" },
              { value: "quantity", label: "Units" },
            ]}
            textSizeClass="text-xs"
            className="border-[#D9D9D9E5] bg-white"
          />
        </div>
      </div>

      {/* Chart area (fills remaining height) */}
      <div className="flex-1 min-h-0 mt-4">
        {loading && <div className="text-sm text-gray-500">Loading chart…</div>}
        {error && <div className="text-sm text-red-500">{error}</div>}

        {!loading && !error && (
          <LiveLineChart
            series={seriesList}
            metric={chartMetric}
            currencySymbol={props.currencySymbol}
            selectedStartDay={props.selectedStartDay}
            selectedEndDay={props.selectedEndDay}
          />
        )}
      </div>
    </div>
  );
}
