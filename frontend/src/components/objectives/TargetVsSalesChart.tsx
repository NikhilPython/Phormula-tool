
"use client";

import React, { useMemo, useRef, useEffect, useState, useCallback } from "react";
import dynamic from "next/dynamic";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

type ChartMetric = "sales" | "units";

type TargetVsSalesPoint = {
  month: string;
  target_sales: number;
  monthwise_sales: number;
  target_units?: number;
  monthwise_units?: number;
};

type ApiResponse = {
  message?: string;
  data?: {
    id: number;
    month: string;
    year: number;
    country: string;
    target_sales: number;
    cashflow_total: number;
    net_sales_total: number;
    shortfall_total: number;
    created_at: string | null;
    updated_at: string | null;
    table_name: string;
    source_details?: any[];
  };
  error?: string;
};

type TargetVsSalesChartProps = {
  currencySymbol?: string;
  className?: string;
  country: string;
  token?: string;
  apiBaseUrl?: string;
  monthsToLoad?: number;
  isPreviewMode?: boolean;
  dummyData?: Array<{
    month: string;
    target: number;
    sales: number;
    target_units?: number;
    monthwise_units?: number;
  }>;
};

const ORANGE = "#ED9F50";
const GREY = "#CECBC7";

const MONTH_NAMES = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

function formatMonthLabel(month: string, year: number) {
  const shortMonth = month.substring(0, 3);
  const shortYear = String(year).slice(-2);
  return `${shortMonth}'${shortYear}`;
}

function getLastNMonths(count: number) {
  const now = new Date();
  const result: { month: string; year: number }[] = [];

  for (let i = count - 1; i >= 0; i--) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    result.push({
      month: MONTH_NAMES[d.getMonth()],
      year: d.getFullYear(),
    });
  }

  return result;
}

export default function TargetVsSalesChart({
  currencySymbol = "£",
  className = "",
  country,
  token,
  apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || "",
  monthsToLoad = 12,
  isPreviewMode = false,
  dummyData = [],
}: TargetVsSalesChartProps) {
  const [chartMetric, setChartMetric] = useState<ChartMetric>("sales");
  const [chartData, setChartData] = useState<TargetVsSalesPoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");

  const echartsInstanceRef = useRef<any>(null);
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

  const fetchTargetSummary = useCallback(
    async (month: string, year: number) => {
      const authToken =
        token || (typeof window !== "undefined" ? localStorage.getItem("token") : null);

      if (!authToken) {
        throw new Error("Authorization token not found");
      }

      const params = new URLSearchParams({
        month,
        year: String(year),
        country,
      });

      const res = await fetch(`${apiBaseUrl}/target-summary?${params.toString()}`, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${authToken}`,
          "Content-Type": "application/json",
        },
      });

      const json: ApiResponse = await res.json();

      if (!res.ok) {
        return null;
      }

      if (!json?.data) {
        return null;
      }

      return json.data;
    },
    [apiBaseUrl, country, token]
  );

  useEffect(() => {
  let ignore = false;

  const loadChartData = async () => {
    try {
      setLoading(true);
      setError("");

      if (isPreviewMode) {
        const previewSource = dummyData.length
          ? dummyData
          : [
              { month: "Jan'25", target: 12000, sales: 9800, target_units: 900, monthwise_units: 760 },
              { month: "Feb'25", target: 12000, sales: 10450, target_units: 900, monthwise_units: 810 },
              { month: "Mar'25", target: 13000, sales: 11700, target_units: 950, monthwise_units: 870 },
              { month: "Apr'25", target: 14000, sales: 12600, target_units: 980, monthwise_units: 910 },
              { month: "May'25", target: 14500, sales: 13250, target_units: 1020, monthwise_units: 940 },
              { month: "Jun'25", target: 15000, sales: 13800, target_units: 1080, monthwise_units: 990 },
              { month: "Jul'25", target: 15500, sales: 14900, target_units: 1120, monthwise_units: 1050 },
              { month: "Aug'25", target: 16000, sales: 15400, target_units: 1150, monthwise_units: 1090 },
              { month: "Sep'25", target: 16500, sales: 15150, target_units: 1180, monthwise_units: 1065 },
              { month: "Oct'25", target: 17500, sales: 16900, target_units: 1230, monthwise_units: 1140 },
              { month: "Nov'25", target: 18000, sales: 17600, target_units: 1280, monthwise_units: 1195 },
              { month: "Dec'25", target: 18500, sales: 18500, target_units: 1320, monthwise_units: 1260 },
            ];

        const mappedPreview: TargetVsSalesPoint[] = previewSource.map((item) => ({
          month: item.month,
          target_sales: Number(item.target ?? 0),
          monthwise_sales: Number(item.sales ?? 0),
          target_units: Number(item.target_units ?? 0),
          monthwise_units: Number(item.monthwise_units ?? 0),
        }));

        if (!ignore) {
          setChartData(mappedPreview);
        }
        return;
      }

      const monthList = getLastNMonths(monthsToLoad);

      const results = await Promise.all(
        monthList.map(async ({ month, year }) => {
          const data = await fetchTargetSummary(month, year);

            return {
              month,
              year,
              monthLabel: formatMonthLabel(month, year),
              apiData: data,
            };
          })
        );

      if (ignore) return;

        const availableMonths = results
          .filter(({ apiData }) => apiData) // only keep months that actually exist
          .map(({ month, year, monthLabel, apiData }) => ({
            month,
            year,
            monthLabel,
            apiData,
          }));

        let mapped: TargetVsSalesPoint[] = availableMonths.map(({ monthLabel, apiData }) => ({
          month: monthLabel,
          target_sales: Number(apiData?.target_sales ?? 0),
          monthwise_sales: Number(apiData?.net_sales_total ?? 0),
          target_units: 0,
          monthwise_units: 0,
        }));

        // If only one month is present, prepend previous month with zero values
        if (availableMonths.length === 1) {
          const { month, year } = availableMonths[0];
          const monthIndex = MONTH_NAMES.indexOf(month);

          const prevDate = new Date(year, monthIndex - 1, 1);
          const prevMonthLabel = formatMonthLabel(
            MONTH_NAMES[prevDate.getMonth()],
            prevDate.getFullYear()
          );

          mapped = [
            {
              month: prevMonthLabel,
              target_sales: 0,
              monthwise_sales: 0,
              target_units: 0,
              monthwise_units: 0,
            },
            ...mapped,
          ];
        }

        setChartData(mapped);

      setChartData(mapped);
    } catch (err: any) {
      if (!ignore) {
        setError(err?.message || "Failed to load chart data");
        setChartData([]);
      }
    } finally {
      if (!ignore) {
        setLoading(false);
      }
    }
  };

  loadChartData();

  return () => {
    ignore = true;
  };
}, [country, monthsToLoad, fetchTargetSummary, isPreviewMode, dummyData]);
  const xAxisData = useMemo(() => chartData.map((item) => item.month), [chartData]);

  const targetSeriesData = useMemo(() => {
    return chartData.map((item) =>
      chartMetric === "sales" ? item.target_sales : item.target_units ?? 0
    );
  }, [chartData, chartMetric]);

  const actualSeriesData = useMemo(() => {
    return chartData.map((item) =>
      chartMetric === "sales" ? item.monthwise_sales : item.monthwise_units ?? 0
    );
  }, [chartData, chartMetric]);

  const adjustedXAxisData = xAxisData;
  const adjustedTargetSeriesData = targetSeriesData;
  const adjustedActualSeriesData = actualSeriesData;

  const yAxisName =
    chartMetric === "sales"
      ? currencySymbol
        ? `(${currencySymbol})`
        : "Sales"
      : "Units (in nos.)";

  const legendData =
    chartMetric === "sales"
      ? ["Target Set", "Monthwise Sales"]
      : ["Target Set", "Monthwise Units"];



  const option = useMemo(
    () => ({
      animation: true,

      tooltip: {
        trigger: "axis",
        textStyle: {
          fontSize: 12,
          color: "#414042",
        },
        formatter: (params: any[]) => {
          const month = (params?.[0]?.axisValue ?? "").replace("’", "'");

          const fmtMoney = (n: number) =>
            `${currencySymbol}${n.toLocaleString(undefined, {
              minimumFractionDigits: 2,
              maximumFractionDigits: 2,
            })}`;

          const fmtUnits = (n: number) => n.toLocaleString();

          const lines = params.map((p: any) => {
            const rawValue = typeof p.data === "number" ? p.data : p?.data?.value ?? 0;
            const displayValue =
              chartMetric === "sales" ? fmtMoney(rawValue) : fmtUnits(rawValue);

            return `
              <div style="font-size:12px; line-height:1.4; color:#414042;">
                <span style="display:inline-block;width:10px;height:10px;margin-right:6px;background:${p.color};border-radius:0;"></span>
                <span>${p.seriesName}: </span>
                <span style="color:#414042;">${displayValue}</span>
              </div>
            `;
          });

          return `
            <div style="font-size:12px; color:#414042;">
              <div style="font-weight:600; margin-bottom:4px; color:#141414;">
                ${month}
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
        data: legendData,
      },

      grid: {
        left: 46,
        right: 16,
        top: 62,
        bottom: 40,
      },

      xAxis: {
        type: "category",
        data: adjustedXAxisData,
        boundaryGap: false,
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
          show: true,
          lineStyle: {
            color: "#E5E7EB",
            width: 1,
            type: "solid",
          },
        },
      },

      dataZoom: [
        {
          type: "inside",
          xAxisIndex: 0,
          filterMode: "none",
          zoomOnMouseWheel: false,
          moveOnMouseMove: true,
          moveOnMouseWheel: true,
          preventDefaultMouseMove: true,
          startValue: Math.max(0, adjustedXAxisData.length - 12),
          endValue: adjustedXAxisData.length - 1,
        },
      ],

      series: [
        {
          name: "Target Set",
          type: "line",
          smooth: 0.35,
          showSymbol: true,
          symbol: "circle",
          symbolSize: 7,
          emphasis: {
            scale: true,
            itemStyle: {
              color: ORANGE,
            },
            symbolSize: 11,
          },
          lineStyle: { color: ORANGE, width: 2 },
          itemStyle: {
            color: ORANGE,
            borderWidth: 0,
          },
          data: adjustedTargetSeriesData,
        },
        {
          name: chartMetric === "sales" ? "Monthwise Sales" : "Monthwise Units",
          type: "line",
          smooth: 0.3,
          showSymbol: true,
          symbol: "circle",
          symbolSize: 7,
          emphasis: {
            scale: true,
            itemStyle: {
              color: GREY,
            },
            symbolSize: 11,
          },
          lineStyle: { color: GREY, width: 2 },
          itemStyle: {
            color: GREY,
            borderWidth: 0,
          },
          data: adjustedActualSeriesData,
        },
      ],
    }),
    [
      adjustedActualSeriesData,
      adjustedTargetSeriesData,
      adjustedXAxisData,
      chartMetric,
      currencySymbol,
      yAxisName,
    ]
  );

  if (loading) {
    return (
      <div className={`h-full w-full flex items-center justify-center ${className}`}>
        <p className="text-sm text-gray-500">Loading chart data...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className={`h-full w-full flex items-center justify-center ${className}`}>
        <p className="text-sm text-red-500">{error}</p>
      </div>
    );
  }

  return (
    <div className={`h-full w-full overflow-hidden ${className}`}>
      <div ref={containerRef} className="h-full w-full min-h-0 overflow-hidden">
        <ReactECharts
          option={option}
          notMerge
          lazyUpdate
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
    </div>
  );
}