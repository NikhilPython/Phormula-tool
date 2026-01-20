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
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

ChartJS.register(ArcElement, Tooltip, Legend);

type CurrencyCode = "USD" | "GBP" | "INR" | "CAD";

export type Cm1PieSlice = {
    name: string;
    value: number;
    pct: number; // already computed in DashboardPage
};

type Props = {
    title?: string;
    data: Cm1PieSlice[];
    currency: CurrencyCode;     // <-- matches your DashboardPage call
    height?: number;            // <-- matches your DashboardPage call
    noDataFound?: boolean;
    onExportBase64Ready?: (base64: string | null) => void;
};

const COLORS = ["#FDD36F", "#B75A5A", "#ED9F50", "#C49466", "#3A8EA4", "#B8C78C"];

const currencySymbolFromCode = (c: CurrencyCode) => {
    switch (c) {
        case "USD":
            return "$";
        case "GBP":
            return "£";
        case "INR":
            return "₹";
        case "CAD":
            return "CA$";
        default:
            return "¤";
    }
};

export default function Cm1ProfitBreakdownPie({
    title = "CM1 Profit Breakdown",
    data,
    currency,
    height = 280,
    noDataFound = false,
    onExportBase64Ready,
}: Props) {
    const currencySymbol = currencySymbolFromCode(currency);

    const legendPosition: "bottom" = "bottom";


    // const [isLaptop, setIsLaptop] = useState(false);
    
    const [isLaptop, setIsLaptop] = useState(false);
    const [isDesktop, setIsDesktop] = useState(false);

    useEffect(() => {
        const check = () => {
            const w = window.innerWidth;
            setIsLaptop(w >= 1024 && w < 1536);
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

    const chartData = useMemo<ChartData<"pie", number[], string> | null>(() => {
        const labels = (data || []).map((d) => d.name);
        const values = (data || []).map((d) => Math.abs(Number(d.value || 0)));

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
    }, [data]);


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


    // const options = useMemo<ChartOptions<"pie">>(() => {
    //     return {
    //         responsive: true,
    //         maintainAspectRatio: false,
    //         animation: { duration: 0 },
    //         radius: isLaptop ? "90%" : "100%",
    //         // layout: {
    //         //     padding: isLaptop ? 4 : 8,
    //         // },
    //         layout: {
    //             padding: {
    //                 top: 0,
    //                 bottom: 0,
    //                 left: 0,
    //                 right: 0,
    //             },
    //         },


    //         elements: { arc: { borderWidth: 0, hoverOffset: 4 } },
    //         plugins: {
    //             legend: {
    //                 position: "right", // ✅ move legend to right
    //                 align: "center",
    //                 labels: {
    //                     usePointStyle: true,
    //                     pointStyle: "circle",
    //                     boxWidth: isLaptop ? 8 : 10,
    //                     boxHeight: isLaptop ? 8 : 10,
    //                     padding: isLaptop ? 10 : 14,
    //                     font: {
    //                         size: isLaptop ? 10 : 12,
    //                         weight: 500,
    //                     },
    //                     color: "#414042",
    //                     generateLabels: (chart) => {
    //                         const labels = (chart.data.labels || []) as string[];
    //                         const dataset = chart.data.datasets?.[0] as any;
    //                         const values = ((dataset?.data || []) as number[]).map(v =>
    //                             Math.abs(Number(v || 0))
    //                         );
    //                         const total = values.reduce((a, b) => a + b, 0);
    //                         const bg = dataset?.backgroundColor as any[];

    //                         return labels.map((label, i) => {
    //                             const value = values[i] ?? 0;
    //                             const pct = total ? (value / total) * 100 : 0;

    //                             return {
    //                                 text: [
    //                                     label,
    //                                     `${currencySymbol}${value.toLocaleString(undefined, {
    //                                         minimumFractionDigits: 2,
    //                                         maximumFractionDigits: 2,
    //                                     })} (${pct.toFixed(2)}%)`,
    //                                 ],
    //                                 fillStyle: bg[i],
    //                                 strokeStyle: "transparent",
    //                                 hidden: !chart.getDataVisibility(i),
    //                                 index: i,
    //                                 pointStyle: "circle",
    //                             };
    //                         });
    //                     },
    //                 },
    //             },
    //         }

    //     };
    // }, [currencySymbol, noDataFound]);

    const options = useMemo<ChartOptions<"pie">>(() => {
        return {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },

            // ✅ keep laptop same, increase only on desktop
            radius: isLaptop ? "90%" : isDesktop ? "95%" : "100%",

            layout: {
                padding: { top: 0, bottom: 0, left: 0, right: 0 },
            },

            elements: { arc: { borderWidth: 0, hoverOffset: 4 } },

            plugins: {
                legend: {
                    position: "right",
                    align: "center",

                    // ✅ IMPORTANT: limit legend width so pie stays big on desktop
                    maxWidth: isDesktop ? 260 : isLaptop ? 220 : 240,

                    labels: {
                        usePointStyle: true,
                        pointStyle: "circle",
                        boxWidth: isLaptop ? 8 : 10,
                        boxHeight: isLaptop ? 8 : 10,

                        // ✅ keep laptop spacing small, allow a bit more on desktop
                        padding: isLaptop ? 10 : isDesktop ? 16 : 14,

                        font: {
                            // ✅ keep laptop same, slightly bigger on desktop only
                            size: isLaptop ? 10 : isDesktop ? 12 : 12,
                            weight: 500,
                        },

                        color: "#414042",

                        generateLabels: (chart) => {
                            const labels = (chart.data.labels || []) as string[];
                            const dataset = chart.data.datasets?.[0] as any;
                            const values = ((dataset?.data || []) as number[]).map(v =>
                                Math.abs(Number(v || 0))
                            );
                            const total = values.reduce((a, b) => a + b, 0);
                            const bg = dataset?.backgroundColor as any[];

                            return labels.map((label, i) => {
                                const value = values[i] ?? 0;
                                const pct = total ? (value / total) * 100 : 0;

                                return {
                                    // ✅ your 2-line legend format stays the same
                                    text: `${label}\n${currencySymbol}${value.toLocaleString(undefined, {
                                        minimumFractionDigits: 2,
                                        maximumFractionDigits: 2,
                                    })} (${pct.toFixed(2)}%)`,
                                    fillStyle: bg[i],
                                    strokeStyle: "transparent",
                                    hidden: !chart.getDataVisibility(i),
                                    index: i,
                                    pointStyle: "circle",
                                };
                            });
                        },
                    },
                },
            },
        };
    }, [currencySymbol, isLaptop, isDesktop]);


    useEffect(() => {
        if (!onExportBase64Ready) return;
        if (!chartData) {
            onExportBase64Ready(null);
            return;
        }
        const t = setTimeout(() => onExportBase64Ready(exportChartBase64()), 300);
        return () => clearTimeout(t);
    }, [chartData, onExportBase64Ready]);

    return (
        <div className="relative w-full h-full rounded-xl border border-slate-200 bg-[#D9D9D933] shadow-sm p-4 flex flex-col">
            <div className="mb-1">
                <div className="w-fit mx-auto md:mx-0">
                    <PageBreadcrumb pageTitle={title} variant="page" align="left" textSize="2xl" />
                </div>
            </div>

            {!chartData ? (
                <p className="text-center text-sm text-gray-500">No CM1 data available.</p>
            ) : (
                // ✅ this MUST be flex-1 so chart gets height
                <div className="flex-1 min-h-0 w-full">
                    <div className="relative w-full h-full flex flex-col justify-center items-center">

                        <Pie
                            ref={chartRef}
                            data={chartData}
                            options={options}
                            className="!block"
                            style={{ width: "100%", height: "100%" }}   // ✅ important
                        />
                    </div>
                </div>
            )}
        </div>
    );

}
