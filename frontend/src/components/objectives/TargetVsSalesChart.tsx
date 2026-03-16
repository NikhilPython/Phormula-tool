// "use client";

// import React, { useMemo, useRef, useEffect, useState } from "react";
// import dynamic from "next/dynamic";
// // import PageBreadcrumb from "../common/PageBreadCrumb";
// // import SegmentedToggle from "../ui/SegmentedToggle";

// const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

// type ChartMetric = "sales" | "units";

// type TargetVsSalesPoint = {
//   month: string;
//   target_sales: number;
//   monthwise_sales: number;
//   target_units?: number;
//   monthwise_units?: number;
// };

// type TargetVsSalesChartProps = {
//   currencySymbol?: string;
//   className?: string;
// };

// const ORANGE = "#ED9F50";
// const GREY = "#CECBC7";

// const dummyTargetVsSalesData: TargetVsSalesPoint[] = [
//   { month: "Jan’24", target_sales: 12000, monthwise_sales: 9800, target_units: 900, monthwise_units: 760 },
//   { month: "Feb’24", target_sales: 12000, monthwise_sales: 10450, target_units: 900, monthwise_units: 810 },
//   { month: "Mar’24", target_sales: 13000, monthwise_sales: 11700, target_units: 950, monthwise_units: 870 },
//   { month: "Apr’24", target_sales: 14000, monthwise_sales: 12600, target_units: 980, monthwise_units: 910 },
//   { month: "May’24", target_sales: 14500, monthwise_sales: 13250, target_units: 1020, monthwise_units: 940 },
//   { month: "Jun’24", target_sales: 15000, monthwise_sales: 13800, target_units: 1080, monthwise_units: 990 },
//   { month: "Jul’24", target_sales: 15500, monthwise_sales: 14900, target_units: 1120, monthwise_units: 1050 },
//   { month: "Aug’24", target_sales: 16000, monthwise_sales: 15400, target_units: 1150, monthwise_units: 1090 },
//   { month: "Sep’24", target_sales: 16500, monthwise_sales: 15150, target_units: 1180, monthwise_units: 1065 },
//   { month: "Oct’24", target_sales: 17500, monthwise_sales: 16900, target_units: 1230, monthwise_units: 1140 },
//   { month: "Nov’24", target_sales: 18000, monthwise_sales: 17600, target_units: 1280, monthwise_units: 1195 },
//   { month: "Dec’24", target_sales: 18500, monthwise_sales: 18500, target_units: 1320, monthwise_units: 1260 },

//   { month: "Jan’25", target_sales: 19000, monthwise_sales: 18100, target_units: 1350, monthwise_units: 1285 },
//   { month: "Feb’25", target_sales: 19200, monthwise_sales: 18650, target_units: 1375, monthwise_units: 1310 },
//   { month: "Mar’25", target_sales: 19800, monthwise_sales: 19150, target_units: 1410, monthwise_units: 1340 },
//   { month: "Apr’25", target_sales: 20200, monthwise_sales: 19600, target_units: 1450, monthwise_units: 1380 },
//   { month: "May’25", target_sales: 20800, monthwise_sales: 20150, target_units: 1490, monthwise_units: 1425 },
//   { month: "Jun’25", target_sales: 21400, monthwise_sales: 20700, target_units: 1535, monthwise_units: 1470 },
//   { month: "Jul’25", target_sales: 22000, monthwise_sales: 21450, target_units: 1580, monthwise_units: 1515 },
//   { month: "Aug’25", target_sales: 22500, monthwise_sales: 21900, target_units: 1610, monthwise_units: 1550 },
//   { month: "Sep’25", target_sales: 23000, monthwise_sales: 22350, target_units: 1650, monthwise_units: 1585 },
//   { month: "Oct’25", target_sales: 23600, monthwise_sales: 22900, target_units: 1700, monthwise_units: 1630 },
//   { month: "Nov’25", target_sales: 24200, monthwise_sales: 23550, target_units: 1740, monthwise_units: 1675 },
//   { month: "Dec’25", target_sales: 24800, monthwise_sales: 24100, target_units: 1790, monthwise_units: 1720 },
// ];

// export default function TargetVsSalesChart({
//   currencySymbol = "£",
//   className = "",
// }: TargetVsSalesChartProps) {
//   const [chartMetric, setChartMetric] = useState<ChartMetric>("sales");
//   const echartsInstanceRef = useRef<any>(null);
//   const containerRef = useRef<HTMLDivElement | null>(null);

//   useEffect(() => {
//     const el = containerRef.current;
//     if (!el) return;

//     const ro = new ResizeObserver(() => {
//       try {
//         echartsInstanceRef.current?.resize();
//       } catch {}
//     });

//     ro.observe(el);
//     return () => ro.disconnect();
//   }, []);

//   const xAxisData = useMemo(
//     () => dummyTargetVsSalesData.map((item) => item.month),
//     []
//   );

//   const targetSeriesData = useMemo(() => {
//     return dummyTargetVsSalesData.map((item) =>
//       chartMetric === "sales" ? item.target_sales : item.target_units ?? 0
//     );
//   }, [chartMetric]);

//   const actualSeriesData = useMemo(() => {
//     return dummyTargetVsSalesData.map((item) =>
//       chartMetric === "sales" ? item.monthwise_sales : item.monthwise_units ?? 0
//     );
//   }, [chartMetric]);

//   const yAxisName =
//     chartMetric === "sales"
//       ? currencySymbol
//         ? `(${currencySymbol})`
//         : "Sales"
//       : "Units (in nos.)";

//   const legendData =
//     chartMetric === "sales"
//       ? ["Target Set", "Monthwise Sales"]
//       : ["Target Set", "Monthwise Units"];

//   const option = useMemo(
//     () => ({
//       animation: true,

//       tooltip: {
//         trigger: "axis",
//         textStyle: {
//           fontSize: 12,
//           color: "#414042",
//         },
//         formatter: (params: any[]) => {
//           const month = params?.[0]?.axisValue ?? "";

//           const fmtMoney = (n: number) =>
//             `${currencySymbol}${n.toLocaleString(undefined, {
//               minimumFractionDigits: 2,
//               maximumFractionDigits: 2,
//             })}`;

//           const fmtUnits = (n: number) => n.toLocaleString();

//           const lines = params.map((p: any) => {
//             const rawValue = typeof p.data === "number" ? p.data : p?.data?.value ?? 0;
//             const displayValue =
//               chartMetric === "sales" ? fmtMoney(rawValue) : fmtUnits(rawValue);

//             return `
//               <div style="font-size:12px; line-height:1.4; color:#414042;">
//                 <span style="display:inline-block;width:10px;height:10px;margin-right:6px;background:${p.color};border-radius:0;"></span>
//                 <span>${p.seriesName}: </span>
//                 <span style="color:#414042;">${displayValue}</span>
//               </div>
//             `;
//           });

//           return `
//             <div style="font-size:12px; color:#414042;">
//               <div style="font-weight:600; margin-bottom:4px; color:#141414;">
//                 ${month}
//               </div>
//               ${lines.join("")}
//             </div>
//           `;
//         },
//       },

//       legend: {
//         top: 10,
//         left: "left",
//         orient: "horizontal",
//         icon: "rect",
//         itemWidth: 10,
//         itemHeight: 10,
//         itemGap: 14,
//         textStyle: {
//           fontSize: 12,
//           color: "#6B7280",
//           padding: [0, 6, 0, 6],
//         },
//         data: legendData,
//       },

//       grid: {
//         left: 46,
//         right: 16,
//         top: 62,
//         bottom: 40,
//       },

//       xAxis: {
//         type: "category",
//         data: xAxisData,
//         boundaryGap: false,
//         axisLine: {
//           lineStyle: {
//             color: "#D1D5DB",
//             width: 1,
//           },
//         },
//         axisTick: {
//           lineStyle: {
//             color: "#D1D5DB",
//           },
//         },
//         axisLabel: {
//           color: "#6B7280",
//         },
//       },

//       yAxis: {
//         type: "value",
//         name: yAxisName,
//         nameLocation: "middle",
//         nameGap: 8,
//         nameTextStyle: {
//           color: "#6B7280",
//           padding: [0, 0, 0, 0],
//         },
//         axisLine: {
//           lineStyle: {
//             color: "#D1D5DB",
//             width: 1,
//           },
//         },
//         axisLabel: {
//           margin: 2,
//           color: "#6B7280",
//         },
//         splitLine: {
//           show: true,
//           lineStyle: {
//             color: "#E5E7EB",
//             width: 1,
//             type: "solid",
//           },
//         },
//       },

//       dataZoom: [
//         {
//           type: "inside",
//           xAxisIndex: 0,
//           filterMode: "none",
//           zoomOnMouseWheel: false,
//           moveOnMouseMove: true,
//           moveOnMouseWheel: true,
//           preventDefaultMouseMove: true,
//           startValue: Math.max(0, xAxisData.length - 12),
//           endValue: xAxisData.length - 1,
//         },
//       ],

//       series: [
//         {
//           name: "Target Set",
//           type: "line",
//           smooth: true,
//           showSymbol: true,
//           symbol: "circle",
//           symbolSize: 7,
//           emphasis: {
//             scale: true,
//             itemStyle: {
//               color: ORANGE,
//             },
//             symbolSize: 11,
//           },
//           lineStyle: { color: ORANGE, width: 2 },
//           itemStyle: {
//             color: ORANGE,
//             borderWidth: 0,
//           },
//           data: targetSeriesData,
//         },
//         {
//           name: chartMetric === "sales" ? "Monthwise Sales" : "Monthwise Units",
//           type: "line",
//           smooth: true,
//           showSymbol: true,
//           symbol: "circle",
//           symbolSize: 7,
//           emphasis: {
//             scale: true,
//             itemStyle: {
//               color: GREY,
//             },
//             symbolSize: 11,
//           },
//           lineStyle: { color: GREY, width: 2 },
//           itemStyle: {
//             color: GREY,
//             borderWidth: 0,
//           },
//           data: actualSeriesData,
//         },
//       ],
//     }),
//     [actualSeriesData, chartMetric, currencySymbol, targetSeriesData, xAxisData, yAxisName]
//   );

//   return (
//     <div className={`h-full w-full overflow-hidden ${className}`}>
//       <div ref={containerRef} className="h-full w-full min-h-0 overflow-hidden">
//         <ReactECharts
//           option={option}
//           notMerge
//           lazyUpdate
//           style={{ width: "100%", height: "100%" }}
//           opts={{ renderer: "canvas" }}
//           onChartReady={(instance) => {
//             echartsInstanceRef.current = instance;
//             try {
//               instance.resize();
//             } catch {}
//           }}
//         />
//       </div>
//     </div>
//   );
// }













































"use client";

import React, { useMemo, useRef, useEffect, useState } from "react";
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

type TargetVsSalesChartProps = {
  currencySymbol?: string;
  className?: string;
  year: number;
  country: string;
  currency?: string;
  token: string;
  apiBaseUrl: string;
};

const ORANGE = "#ED9F50";
const GREY = "#CECBC7";

const monthShortMap: Record<string, string> = {
  January: "Jan",
  February: "Feb",
  March: "Mar",
  April: "Apr",
  May: "May",
  June: "Jun",
  July: "Jul",
  August: "Aug",
  September: "Sep",
  October: "Oct",
  November: "Nov",
  December: "Dec",
};

function formatMonthLabel(month: string, year: number) {
  const shortMonth = monthShortMap[month] || month.slice(0, 3);
  const shortYear = String(year).slice(-2);
  return `${shortMonth}’${shortYear}`;
}

export default function TargetVsSalesChart({
  currencySymbol = "£",
  className = "",
  year,
  country,
  currency,
  token,
  apiBaseUrl,
}: TargetVsSalesChartProps) {
  const [chartMetric, setChartMetric] = useState<ChartMetric>("sales");
  const [chartData, setChartData] = useState<TargetVsSalesPoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

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

  useEffect(() => {
    const fetchTargetSummaryHistory = async () => {
      setLoading(true);
      setError(null);

      try {
        const params = new URLSearchParams({
          year: String(year),
          country,
        });

        if (currency) {
          params.append("currency", currency);
        }

        const response = await fetch(
          `${apiBaseUrl}/target-summary-history?${params.toString()}`,
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
          }
        );

        const result = await response.json();

        if (!response.ok) {
          throw new Error(result?.error || "Failed to fetch target summary history");
        }

        const mappedData: TargetVsSalesPoint[] = (result?.data || []).map((item: any) => ({
          month: formatMonthLabel(item.month, item.year),
          target_sales: Number(item.target_sales || 0),
          monthwise_sales: Number(item.monthwise_sales || 0),

          // These are not yet returned by backend/db
          target_units: Number(item.target_units || 0),
          monthwise_units: Number(item.monthwise_units || 0),
        }));

        setChartData(mappedData);
      } catch (err: any) {
        setError(err.message || "Something went wrong");
        setChartData([]);
      } finally {
        setLoading(false);
      }
    };

    if (token && year && country) {
      fetchTargetSummaryHistory();
    }
  }, [apiBaseUrl, token, year, country, currency]);

  const xAxisData = useMemo(
    () => chartData.map((item) => item.month),
    [chartData]
  );

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
          const month = params?.[0]?.axisValue ?? "";

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
        data: xAxisData,
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
          startValue: Math.max(0, xAxisData.length - 12),
          endValue: xAxisData.length - 1,
        },
      ],
      series: [
        {
          name: "Target Set",
          type: "line",
          smooth: true,
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
          data: targetSeriesData,
        },
        {
          name: chartMetric === "sales" ? "Monthwise Sales" : "Monthwise Units",
          type: "line",
          smooth: true,
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
          data: actualSeriesData,
        },
      ],
    }),
    [actualSeriesData, chartMetric, currencySymbol, targetSeriesData, xAxisData, yAxisName]
  );

  if (loading) {
    return <div className={`h-full w-full flex items-center justify-center ${className}`}>Loading...</div>;
  }

  if (error) {
    return <div className={`h-full w-full flex items-center justify-center text-red-500 ${className}`}>{error}</div>;
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