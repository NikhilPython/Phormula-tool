// // "use client";

// // import React, { useEffect, useMemo, useRef } from "react";
// // import dynamic from "next/dynamic";

// // const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

// // type ObjectiveMoMChartProps = {
// //     title?: string;
// //     className?: string;
// // };

// // const GREEN = "#97A95F";
// // const ORANGE = "#ED9F50";

// // const yLabels = ["Conservative", "Balanced", "Aggressive"] as const;
// // type YLabel = (typeof yLabels)[number];

// // const labelToValue = (label: YLabel) => yLabels.indexOf(label);

// // const dummyObjectiveMoMData: Array<{
// //     month: string;
// //     profit: YLabel;
// //     sale: YLabel;
// // }> = [
// //         { month: "Jan’24", profit: "Balanced", sale: "Conservative" },
// //         { month: "Feb’24", profit: "Balanced", sale: "Balanced" },
// //         { month: "Mar’24", profit: "Aggressive", sale: "Balanced" },
// //         { month: "Apr’24", profit: "Aggressive", sale: "Balanced" },
// //         { month: "May’24", profit: "Balanced", sale: "Aggressive" },
// //         { month: "Jun’24", profit: "Conservative", sale: "Aggressive" },
// //         { month: "Jul’24", profit: "Balanced", sale: "Balanced" },
// //         { month: "Aug’24", profit: "Aggressive", sale: "Balanced" },
// //         { month: "Sep’24", profit: "Aggressive", sale: "Conservative" },
// //         { month: "Oct’24", profit: "Balanced", sale: "Balanced" },
// //         { month: "Nov’24", profit: "Conservative", sale: "Aggressive" },
// //         { month: "Dec’24", profit: "Balanced", sale: "Aggressive" },

// //         { month: "Jan’25", profit: "Aggressive", sale: "Balanced" },
// //         { month: "Feb’25", profit: "Balanced", sale: "Aggressive" },
// //         { month: "Mar’25", profit: "Aggressive", sale: "Balanced" },
// //         { month: "Apr’25", profit: "Balanced", sale: "Balanced" },
// //         { month: "May’25", profit: "Conservative", sale: "Aggressive" },
// //         { month: "Jun’25", profit: "Balanced", sale: "Aggressive" },
// //         { month: "Jul’25", profit: "Aggressive", sale: "Balanced" },
// //         { month: "Aug’25", profit: "Balanced", sale: "Conservative" },
// //         { month: "Sep’25", profit: "Balanced", sale: "Balanced" },
// //         { month: "Oct’25", profit: "Aggressive", sale: "Balanced" },
// //         { month: "Nov’25", profit: "Aggressive", sale: "Conservative" },
// //         { month: "Dec’25", profit: "Balanced", sale: "Aggressive" },
// //     ];

// // export default function ObjectiveMoMChart({
// //     className = "",
// // }: ObjectiveMoMChartProps) {
// //     const echartsInstanceRef = useRef<any>(null);
// //     const containerRef = useRef<HTMLDivElement | null>(null);

// //     useEffect(() => {
// //         const el = containerRef.current;
// //         if (!el) return;

// //         const ro = new ResizeObserver(() => {
// //             try {
// //                 echartsInstanceRef.current?.resize();
// //             } catch { }
// //         });

// //         ro.observe(el);
// //         return () => ro.disconnect();
// //     }, []);

// //     const xAxisData = useMemo(
// //         () => dummyObjectiveMoMData.map((item) => item.month),
// //         []
// //     );

// //     const profitSeriesData = useMemo(
// //         () =>
// //             dummyObjectiveMoMData.map((item) => ({
// //                 value: labelToValue(item.profit),
// //                 rawLabel: item.profit,
// //             })),
// //         []
// //     );

// //     const saleSeriesData = useMemo(
// //         () =>
// //             dummyObjectiveMoMData.map((item) => ({
// //                 value: labelToValue(item.sale),
// //                 rawLabel: item.sale,
// //             })),
// //         []
// //     );

// //     const option = useMemo(
// //         () => ({
// //             animation: true,

// //             tooltip: {
// //                 trigger: "axis",
// //                 textStyle: {
// //                     fontSize: 12,
// //                     color: "#414042",
// //                 },
// //                 formatter: (params: any[]) => {
// //                     const month = params?.[0]?.axisValue ?? "";

// //                     const lines = params.map((p: any) => {
// //                         const rawValue = p?.data?.rawLabel ?? "-";

// //                         return `
// //               <div style="font-size:12px; line-height:1.4; color:#414042;">
// //                 <span style="display:inline-block;width:10px;height:10px;margin-right:6px;background:${p.color};border-radius:0;"></span>
// //                 <span>${p.seriesName}: </span>
// //                 <span style="color:#414042;">${rawValue}</span>
// //               </div>
// //             `;
// //                     });

// //                     return `
// //             <div style="font-size:12px; color:#414042;">
// //               <div style="font-weight:600; margin-bottom:4px; color:#141414;">
// //                 ${month}
// //               </div>
// //               ${lines.join("")}
// //             </div>
// //           `;
// //                 },
// //             },

// //             legend: {
// //                 top: 10,
// //                 left: "left",
// //                 orient: "horizontal",
// //                 icon: "rect",
// //                 itemWidth: 10,
// //                 itemHeight: 10,
// //                 itemGap: 14,
// //                 textStyle: {
// //                     fontSize: 12,
// //                     color: "#6B7280",
// //                     padding: [0, 6, 0, 6],
// //                 },
// //                 data: ["Profit", "Sale"],
// //             },

// //             grid: {
// //                 left: 78,
// //                 right: 18,
// //                 top: 62,
// //                 bottom: 40,
// //                 containLabel: false,
// //             },

// //             xAxis: {
// //                 type: "category",
// //                 data: xAxisData,
// //                 boundaryGap: false,
// //                 axisLine: {
// //                     lineStyle: {
// //                         color: "#D1D5DB",
// //                         width: 1,
// //                     },
// //                 },
// //                 axisTick: {
// //                     lineStyle: {
// //                         color: "#D1D5DB",
// //                     },
// //                 },
// //                 axisLabel: {
// //                     color: "#6B7280",
// //                     formatter: (value: string) => value.replace("’ ", "’"),
// //                 },
// //             },

// //             yAxis: {
// //                 type: "value",
// //                 min: 0,
// //                 max: 2,
// //                 interval: 1,
// //                 name: "Growth Rate",
// //                 nameLocation: "middle",
// //                 nameGap: 55,
// //                 nameTextStyle: {
// //                     color: "#6B7280",
// //                 },
// //                 axisLine: {
// //                     lineStyle: {
// //                         color: "#D1D5DB",
// //                         width: 1,
// //                     },
// //                 },
// //                 axisLabel: {
// //                     margin: 8,
// //                     color: "#6B7280",
// //                     formatter: (value: number) => yLabels[value] ?? "",
// //                 },
// //                 splitLine: {
// //                     show: true,
// //                     lineStyle: {
// //                         color: "#E5E7EB",
// //                         width: 1,
// //                         type: "solid",
// //                     },
// //                 },
// //             },

// //             dataZoom: [
// //                 {
// //                     type: "inside",
// //                     xAxisIndex: 0,
// //                     filterMode: "none",
// //                     zoomOnMouseWheel: false,
// //                     moveOnMouseMove: true,
// //                     moveOnMouseWheel: true,
// //                     preventDefaultMouseMove: true,
// //                     startValue: Math.max(0, xAxisData.length - 12),
// //                     endValue: xAxisData.length - 1,
// //                 },
// //             ],

// //             series: [
// //                 {
// //                     name: "Profit",
// //                     type: "line",
// //                     smooth: true,
// //                     showSymbol: true,
// //                     symbol: "circle",
// //                     symbolSize: 7,
// //                     emphasis: {
// //                         scale: true,
// //                         itemStyle: {
// //                             color: GREEN,
// //                         },
// //                         symbolSize: 11,
// //                     },
// //                     lineStyle: { color: GREEN, width: 2 },
// //                     itemStyle: {
// //                         color: GREEN,
// //                         borderWidth: 0,
// //                     },
// //                     data: profitSeriesData,
// //                 },
// //                 {
// //                     name: "Sale",
// //                     type: "line",
// //                     smooth: true,
// //                     showSymbol: true,
// //                     symbol: "circle",
// //                     symbolSize: 7,
// //                     emphasis: {
// //                         scale: true,
// //                         itemStyle: {
// //                             color: ORANGE,
// //                         },
// //                         symbolSize: 11,
// //                     },
// //                     lineStyle: { color: ORANGE, width: 2 },
// //                     itemStyle: {
// //                         color: ORANGE,
// //                         borderWidth: 0,
// //                     },
// //                     data: saleSeriesData,
// //                 },
// //             ],
// //         }),
// //         [profitSeriesData, saleSeriesData, xAxisData]
// //     );

// //     return (
// //         <div className={`h-full w-full overflow-hidden ${className}`}>
// //             <div ref={containerRef} className="h-full w-full min-h-0">
// //                 <ReactECharts
// //                     option={option}
// //                     notMerge
// //                     lazyUpdate
// //                     style={{ width: "100%", height: "100%" }}
// //                     opts={{ renderer: "canvas" }}
// //                     onChartReady={(instance) => {
// //                         echartsInstanceRef.current = instance;
// //                         try {
// //                             instance.resize();
// //                         } catch { }
// //                     }}
// //                 />
// //             </div>
// //         </div>
// //     );
// // }




























// "use client";

// import React, { useEffect, useMemo, useRef, useState } from "react";
// import dynamic from "next/dynamic";

// const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

// type ObjectiveMoMChartProps = {
//   title?: string;
//   className?: string;
//   country: string;
//   token: string;
//   apiBaseUrl: string;
// };

// const GREEN = "#97A95F";
// const ORANGE = "#ED9F50";

// const yLabels = ["Conservative", "Balanced", "Aggressive"] as const;
// type YLabel = (typeof yLabels)[number];

// type ObjectiveApiItem = {
//     id: number;
//     month: string; // "2025-01"
//     growth_intent: string;
//     profit_priority: string;
//     inventory_clearance_priority: string;
//     business_context?: string;
// };

// const normalizeLabel = (value: string): YLabel => {
//     const normalized = value?.trim().toLowerCase();

//     if (normalized === "conservative") return "Conservative";
//     if (normalized === "balanced") return "Balanced";
//     if (normalized === "aggressive") return "Aggressive";

//     return "Balanced";
// };

// const labelToValue = (label: string) => {
//     const normalized = normalizeLabel(label);
//     return yLabels.indexOf(normalized);
// };

// const formatMonthLabel = (month: string) => {
//     const [year, monthNum] = month.split("-");
//     const date = new Date(Number(year), Number(monthNum) - 1, 1);

//     return date.toLocaleString("en-US", {
//         month: "short",
//         year: "2-digit",
//     }).replace(" ", "’");
// };

// export default function ObjectiveMoMChart({
//     className = "",
//     country,
//     token,
//     apiBaseUrl,
// }: ObjectiveMoMChartProps) {
//     const echartsInstanceRef = useRef<any>(null);
//     const containerRef = useRef<HTMLDivElement | null>(null);

//     const [objectiveData, setObjectiveData] = useState<ObjectiveApiItem[]>([]);
//     const [loading, setLoading] = useState(true);
//     const [error, setError] = useState<string | null>(null);

//     useEffect(() => {
//         const el = containerRef.current;
//         if (!el) return;

//         const ro = new ResizeObserver(() => {
//             try {
//                 echartsInstanceRef.current?.resize();
//             } catch { }
//         });

//         ro.observe(el);
//         return () => ro.disconnect();
//     }, []);

//     useEffect(() => {
//         const fetchObjectives = async () => {
//             if (!country || !token) return;

//             try {
//                 setLoading(true);
//                 setError(null);

//                 const res = await fetch(
//                     `${apiBaseUrl}/objective?country=${encodeURIComponent(country)}&all=true`,
//                     {
//                         method: "GET",
//                         headers: {
//                             Authorization: `Bearer ${token}`,
//                             "Content-Type": "application/json",
//                         },
//                     }
//                 );

//                 const data = await res.json();

//                 if (!res.ok) {
//                     throw new Error(data?.error || data?.message || "Failed to fetch objective data");
//                 }

//                 const objectives: ObjectiveApiItem[] = Array.isArray(data?.objectives)
//                     ? data.objectives
//                     : [];

//                 // API returns desc order, chart usually looks better in ascending order
//                 const sorted = [...objectives].sort((a, b) =>
//                     a.month.localeCompare(b.month)
//                 );

//                 setObjectiveData(sorted);
//             } catch (err: any) {
//                 setError(err?.message || "Something went wrong");
//                 setObjectiveData([]);
//             } finally {
//                 setLoading(false);
//             }
//         };

//         fetchObjectives();
//     }, [country, token]);

//     const xAxisData = useMemo(
//         () => objectiveData.map((item) => formatMonthLabel(item.month)),
//         [objectiveData]
//     );

//     const profitSeriesData = useMemo(
//         () =>
//             objectiveData.map((item) => ({
//                 value: labelToValue(item.profit_priority),
//                 rawLabel: normalizeLabel(item.profit_priority),
//             })),
//         [objectiveData]
//     );

//     const saleSeriesData = useMemo(
//         () =>
//             objectiveData.map((item) => ({
//                 // mapped from growth_intent because API has no `sale` field
//                 value: labelToValue(item.growth_intent),
//                 rawLabel: normalizeLabel(item.growth_intent),
//             })),
//         [objectiveData]
//     );

//     const option = useMemo(
//         () => ({
//             animation: true,

//             tooltip: {
//                 trigger: "axis",
//                 textStyle: {
//                     fontSize: 12,
//                     color: "#414042",
//                 },
//                 formatter: (params: any[]) => {
//                     const month = params?.[0]?.axisValue ?? "";

//                     const lines = params.map((p: any) => {
//                         const rawValue = p?.data?.rawLabel ?? "-";

//                         return `
//               <div style="font-size:12px; line-height:1.4; color:#414042;">
//                 <span style="display:inline-block;width:10px;height:10px;margin-right:6px;background:${p.color};border-radius:0;"></span>
//                 <span>${p.seriesName}: </span>
//                 <span style="color:#414042;">${rawValue}</span>
//               </div>
//             `;
//                     });

//                     return `
//             <div style="font-size:12px; color:#414042;">
//               <div style="font-weight:600; margin-bottom:4px; color:#141414;">
//                 ${month}
//               </div>
//               ${lines.join("")}
//             </div>
//           `;
//                 },
//             },

//             legend: {
//                 top: 10,
//                 left: "left",
//                 orient: "horizontal",
//                 icon: "rect",
//                 itemWidth: 10,
//                 itemHeight: 10,
//                 itemGap: 14,
//                 textStyle: {
//                     fontSize: 12,
//                     color: "#6B7280",
//                     padding: [0, 6, 0, 6],
//                 },
//                 data: ["Profit", "Sale"],
//             },

//             grid: {
//                 left: 78,
//                 right: 18,
//                 top: 62,
//                 bottom: 40,
//                 containLabel: false,
//             },

//             xAxis: {
//                 type: "category",
//                 data: xAxisData,
//                 boundaryGap: false,
//                 axisLine: {
//                     lineStyle: {
//                         color: "#D1D5DB",
//                         width: 1,
//                     },
//                 },
//                 axisTick: {
//                     lineStyle: {
//                         color: "#D1D5DB",
//                     },
//                 },
//                 axisLabel: {
//                     color: "#6B7280",
//                 },
//             },

//             yAxis: {
//                 type: "value",
//                 min: 0,
//                 max: 2,
//                 interval: 1,
//                 name: "Growth Rate",
//                 nameLocation: "middle",
//                 nameGap: 55,
//                 nameTextStyle: {
//                     color: "#6B7280",
//                 },
//                 axisLine: {
//                     lineStyle: {
//                         color: "#D1D5DB",
//                         width: 1,
//                     },
//                 },
//                 axisLabel: {
//                     margin: 8,
//                     color: "#6B7280",
//                     formatter: (value: number) => yLabels[value] ?? "",
//                 },
//                 splitLine: {
//                     show: true,
//                     lineStyle: {
//                         color: "#E5E7EB",
//                         width: 1,
//                         type: "solid",
//                     },
//                 },
//             },

//             dataZoom: [
//                 {
//                     type: "inside",
//                     xAxisIndex: 0,
//                     filterMode: "none",
//                     zoomOnMouseWheel: false,
//                     moveOnMouseMove: true,
//                     moveOnMouseWheel: true,
//                     preventDefaultMouseMove: true,
//                     startValue: Math.max(0, xAxisData.length - 12),
//                     endValue: Math.max(0, xAxisData.length - 1),
//                 },
//             ],

//             series: [
//                 {
//                     name: "Profit",
//                     type: "line",
//                     smooth: true,
//                     showSymbol: true,
//                     symbol: "circle",
//                     symbolSize: 7,
//                     emphasis: {
//                         scale: true,
//                         itemStyle: {
//                             color: GREEN,
//                         },
//                         symbolSize: 11,
//                     },
//                     lineStyle: { color: GREEN, width: 2 },
//                     itemStyle: {
//                         color: GREEN,
//                         borderWidth: 0,
//                     },
//                     data: profitSeriesData,
//                 },
//                 {
//                     name: "Sale",
//                     type: "line",
//                     smooth: true,
//                     showSymbol: true,
//                     symbol: "circle",
//                     symbolSize: 7,
//                     emphasis: {
//                         scale: true,
//                         itemStyle: {
//                             color: ORANGE,
//                         },
//                         symbolSize: 11,
//                     },
//                     lineStyle: { color: ORANGE, width: 2 },
//                     itemStyle: {
//                         color: ORANGE,
//                         borderWidth: 0,
//                     },
//                     data: saleSeriesData,
//                 },
//             ],
//         }),
//         [profitSeriesData, saleSeriesData, xAxisData]
//     );

//     if (loading) {
//         return (
//             <div className={`h-full w-full flex items-center justify-center ${className}`}>
//                 <span className="text-sm text-gray-500">Loading chart...</span>
//             </div>
//         );
//     }

//     if (error) {
//         return (
//             <div className={`h-full w-full flex items-center justify-center ${className}`}>
//                 <span className="text-sm text-red-500">{error}</span>
//             </div>
//         );
//     }

//     if (!objectiveData.length) {
//         return (
//             <div className={`h-full w-full flex items-center justify-center ${className}`}>
//                 <span className="text-sm text-gray-500">No objective data found</span>
//             </div>
//         );
//     }

//     return (
//         <div className={`h-full w-full overflow-hidden ${className}`}>
//             <div ref={containerRef} className="h-full w-full min-h-0">
//                 <ReactECharts
//                     option={option}
//                     notMerge
//                     lazyUpdate
//                     style={{ width: "100%", height: "100%" }}
//                     opts={{ renderer: "canvas" }}
//                     onChartReady={(instance) => {
//                         echartsInstanceRef.current = instance;
//                         try {
//                             instance.resize();
//                         } catch { }
//                     }}
//                 />
//             </div>
//         </div>
//     );
// }

























"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import dynamic from "next/dynamic";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

type ObjectiveMoMChartProps = {
    title?: string;
    className?: string;
    country: string;
    token?: string;
    apiBaseUrl?: string;
};

type ObjectiveApiItem = {
    id: number;
    month: string; // YYYY-MM
    growth_intent: string;
    profit_priority: string;
    inventory_clearance_priority: boolean;
    business_context?: string;
};

type ObjectiveApiResponse = {
    objectives?: ObjectiveApiItem[];
    message?: string;
    error?: string;
};

const GREEN = "#97A95F";
const ORANGE = "#ED9F50";

const yLabels = ["Conservative", "Balanced", "Aggressive"] as const;
type YLabel = (typeof yLabels)[number];

function formatMonthLabelFromApi(month: string) {
    const [year, monthNum] = month.split("-");
    const date = new Date(Number(year), Number(monthNum) - 1, 1);

    const shortMonth = date.toLocaleString("en-US", { month: "short" });
    const shortYear = String(year).slice(-2);

    return `${shortMonth}'${shortYear}`;
}

function normalizeGrowthIntent(value?: string | null): YLabel {
    const normalized = (value || "").trim().toLowerCase();

    if (normalized === "conservative") return "Conservative";
    if (normalized === "balanced") return "Balanced";
    if (normalized === "aggressive") return "Aggressive";

    return "Balanced";
}

function normalizeProfitPriority(value?: string | null): YLabel {
    const normalized = (value || "").trim().toLowerCase();

    if (normalized === "high") return "Aggressive";
    if (normalized === "protect_growth") return "Balanced";
    if (normalized === "sacrifice_short_term") return "Conservative";

    return "Balanced";
}

function labelToValue(label: YLabel) {
    return yLabels.indexOf(label);
}

export default function ObjectiveMoMChart({
    className = "",
    country,
    token,
    apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || "",
}: ObjectiveMoMChartProps) {
    const echartsInstanceRef = useRef<any>(null);
    const containerRef = useRef<HTMLDivElement | null>(null);

    const [objectiveData, setObjectiveData] = useState<ObjectiveApiItem[]>([]);

    const adjustedData =
        objectiveData.length === 1
            ? [
                objectiveData[0],
                {
                    ...objectiveData[0],
                    __isDuplicate: true,
                } as any,
            ]
            : objectiveData;

    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");

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
        let ignore = false;

        const loadChartData = async () => {
            try {
                setLoading(true);
                setError("");

                const authToken =
                    token || (typeof window !== "undefined" ? localStorage.getItem("token") : null);

                if (!authToken) {
                    throw new Error("Authorization token not found");
                }

                const params = new URLSearchParams({
                    country,
                    all: "true",
                });

                const res = await fetch(`${apiBaseUrl}/objective?${params.toString()}`, {
                    method: "GET",
                    headers: {
                        Authorization: `Bearer ${authToken}`,
                        "Content-Type": "application/json",
                    },
                });

                const json: ObjectiveApiResponse = await res.json();

                if (!res.ok) {
                    throw new Error(json?.error || json?.message || "Failed to load chart data");
                }

                const objectives = Array.isArray(json?.objectives) ? json.objectives : [];

                const sorted = [...objectives].sort((a, b) => a.month.localeCompare(b.month));

                if (!ignore) {
                    setObjectiveData(sorted);
                }
            } catch (err: any) {
                if (!ignore) {
                    setError(err?.message || "Failed to load chart data");
                    setObjectiveData([]);
                }
            } finally {
                if (!ignore) {
                    setLoading(false);
                }
            }
        };

        if (country) {
            loadChartData();
        }

        return () => {
            ignore = true;
        };
    }, [apiBaseUrl, country, token]);

    const xAxisData = useMemo(
        () =>
            adjustedData.map((item: any) =>
                item.__isDuplicate ? "" : formatMonthLabelFromApi(item.month)
            ),
        [adjustedData]
    );

    // const profitSeriesData = useMemo(
    //     () =>
    //         objectiveData.map((item) => ({
    //             value: labelToValue(normalizeProfitPriority(item.profit_priority)),
    //             rawLabel: normalizeProfitPriority(item.profit_priority),
    //         })),
    //     [objectiveData]
    // );

    const profitSeriesData = useMemo(
        () =>
            adjustedData.map((item: any) => ({
                value: labelToValue(normalizeProfitPriority(item.profit_priority)),
                rawLabel: normalizeProfitPriority(item.profit_priority),
            })),
        [adjustedData]
    );

    // const saleSeriesData = useMemo(
    //     () =>
    //         objectiveData.map((item) => ({
    //             value: labelToValue(normalizeGrowthIntent(item.growth_intent)),
    //             rawLabel: normalizeGrowthIntent(item.growth_intent),
    //         })),
    //     [objectiveData]
    // );

    const saleSeriesData = useMemo(
        () =>
            adjustedData.map((item: any) => ({
                value: labelToValue(normalizeGrowthIntent(item.growth_intent)),
                rawLabel: normalizeGrowthIntent(item.growth_intent),
            })),
        [adjustedData]
    );

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

                    const lines = params.map((p: any) => {
                        const rawValue = p?.data?.rawLabel ?? "-";

                        return `
              <div style="font-size:12px; line-height:1.4; color:#414042;">
                <span style="display:inline-block;width:10px;height:10px;margin-right:6px;background:${p.color};border-radius:0;"></span>
                <span>${p.seriesName}: </span>
                <span style="color:#414042;">${rawValue}</span>
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
                data: ["Profit", "Sale"],
            },

            grid: {
                left: 78,
                right: 18,
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
                min: 0,
                max: 2,
                interval: 1,
                name: "Growth Rate",
                nameLocation: "middle",
                nameGap: 18,
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
                    margin: 8,
                    color: "#6B7280",
                    formatter: (value: number) => yLabels[value] ?? "",
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
                    name: "Profit",
                    type: "line",
                    smooth: true,
                    showSymbol: true,
                    symbol: "circle",
                    symbolSize: 7,
                    emphasis: {
                        scale: true,
                        itemStyle: {
                            color: GREEN,
                        },
                        symbolSize: 11,
                    },
                    lineStyle: { color: GREEN, width: 2 },
                    itemStyle: {
                        color: GREEN,
                        borderWidth: 0,
                    },
                    data: profitSeriesData,
                },
                {
                    name: "Sale",
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
                    data: saleSeriesData,
                },
            ],
        }),
        [profitSeriesData, saleSeriesData, xAxisData]
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

    if (!objectiveData.length) {
        return (
            <div className={`h-full w-full flex items-center justify-center ${className}`}>
                <p className="text-sm text-gray-500">No objective data found</p>
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