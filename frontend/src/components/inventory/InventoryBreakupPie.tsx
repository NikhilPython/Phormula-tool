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

type AnyRow = Record<string, any>;

type Slice = {
    name: string;   // product name
    value: number;  // inventory count
    pct: number;    // percent of API total
};

type Props = {
    title?: string;
    rows: AnyRow[]; // ✅ pass your API rows (can be displayRows too, but MUST include Grand Total row)
    height?: number;
    noDataFound?: boolean;
    onExportBase64Ready?: (base64: string | null) => void;
};

const COLORS = ["#FDD36F", "#B75A5A", "#ED9F50", "#C49466", "#3A8EA4", "#B8C78C"];

const toNum = (v: any) => {
    if (v === null || v === undefined || v === "") return 0;
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
};

const isGrandTotalRow = (row: AnyRow) => {
    const msku = String(row?.msku || "").trim().toUpperCase();
    const pn = String(row?.product_name || "").trim().toUpperCase();
    return (
        row?.__isTotal === true ||
        row?.is_total === true ||
        msku === "GRAND TOTAL" ||
        pn === "GRAND TOTAL"
    );
};

export default function InventoryTopProductsPie({
    title = "Inventory Breakup",
    rows,
    height = 280,
    noDataFound = false,
    onExportBase64Ready,
}: Props) {
    const [isLaptop, setIsLaptop] = useState(false);
    const [isDesktop, setIsDesktop] = useState(false);
    const [legendTick, setLegendTick] = useState(0);

    const chartRef = useRef<any>(null);

    // ✅ Choose what "inventory count" means.
    // Use "ending_total" because that's what you want breakup of (Inventory at month end -> Total)
const getInventoryCount = (r: AnyRow) =>
  toNum(r?.ending_total ?? r?.__ending_total ?? r?.EndingTotal ?? r?.endingTotal);

    const MIN_PRODUCTS = 5;

    const { displayData, apiTotal } = useMemo(() => {
        const all = rows || [];
        if (!all.length) return { displayData: [] as Slice[], apiTotal: 0 };

        const totalRow = all.find(isGrandTotalRow) || null;
        const apiTotalVal = totalRow ? getInventoryCount(totalRow) : 0;

        // Only actual products (exclude total/others rows)
        const productRows = all.filter((r) => !isGrandTotalRow(r) && String(r?.msku || "").toUpperCase() !== "OTHERS");

        const items = productRows
            .map((r) => ({
                name: String(r?.product_name || r?.msku || "Unknown"),
                value: getInventoryCount(r),
            }))
            .filter((x) => x.value > 0);

        // If API total is missing, fallback to sum of all products (last resort)
        const safeTotal =
            apiTotalVal > 0 ? apiTotalVal : items.reduce((s, d) => s + d.value, 0);

        if (!items.length || safeTotal <= 0) {
            return { displayData: [] as Slice[], apiTotal: safeTotal };
        }

        // Sort desc
        const sorted = [...items].sort((a, b) => b.value - a.value);

        // Keep top MIN_PRODUCTS
        const kept = sorted.slice(0, Math.min(MIN_PRODUCTS, sorted.length));
        const keptSum = kept.reduce((s, d) => s + d.value, 0);

        // ✅ Others must be "API Total - keptSum" (not recomputed from remaining)
        // This guarantees pie always matches your API total.
        const othersValue = Math.max(0, safeTotal - keptSum);

        const rebuiltKept: Slice[] = kept.map((d) => ({
            ...d,
            pct: (d.value / safeTotal) * 100,
        }));

        const final: Slice[] =
            othersValue > 0
                ? [
                    ...rebuiltKept,
                    { name: "Others", value: othersValue, pct: (othersValue / safeTotal) * 100 },
                ]
                : rebuiltKept;

        return { displayData: final, apiTotal: safeTotal };
    }, [rows]);

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
        const labels = (displayData || []).map((d) => d.name);
        const values = (displayData || []).map((d) => Number(d.value || 0));

        if (!labels.length || values.every((v) => v === 0)) return null;

        return {
            labels,
            datasets: [
                {
                    data: values,
                    backgroundColor: labels.map((_, i) => COLORS[i % COLORS.length]),
                    borderWidth: 0,
                    borderColor: "transparent",
                    hoverOffset: 4,
                },
            ],
        };
    }, [displayData]);

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

    const options = useMemo<ChartOptions<"pie">>(() => {
        return {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            radius: isLaptop ? "90%" : isDesktop ? "97%" : "100%",
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: (ctx: TooltipItem<"pie">) => {
                            const i = ctx.dataIndex;
                            const slice = displayData?.[i];
                            const val = Number(ctx.raw || 0);
                            return `${slice?.name ?? ctx.label}: ${val.toLocaleString()} (${(
                                slice?.pct ?? 0
                            ).toFixed(2)}%)`;
                        },
                    },
                },
            },
        };
    }, [isLaptop, isDesktop, displayData]);

    // Legend sync
    useEffect(() => {
        if (!chartData) return;
        const t = setTimeout(() => setLegendTick((x) => x + 1), 50);
        return () => clearTimeout(t);
    }, [chartData]);

    useEffect(() => {
        if (!onExportBase64Ready) return;
        if (!chartData) {
            onExportBase64Ready(null);
            return;
        }
        const t = setTimeout(() => onExportBase64Ready(exportChartBase64()), 300);
        return () => clearTimeout(t);
    }, [chartData, onExportBase64Ready]);

    const showNoData = noDataFound || !chartData || apiTotal <= 0;

    return (
        <div className="relative w-full h-full rounded-xl border border-slate-200 bg-[#D9D9D933] shadow-sm p-4 flex flex-col">
            <div className="mb-1">
                <div className="w-fit mx-auto md:mx-0">
                    <PageBreadcrumb pageTitle={title} variant="page" align="left" textSize="2xl" />
                </div>
            </div>

            {showNoData ? (
                <p className="text-center text-sm text-gray-500">No inventory data available.</p>
            ) : (
                <div className="flex-1 min-h-0 w-full" >
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

                        {/* RIGHT: LEGEND */}
                        <div
                            className="shrink-0 overflow-auto pr-1"
                            style={{
                                width: isDesktop ? 320 : isLaptop ? 280 : 300,
                                maxHeight: "100%",
                            }}
                        >
                            <div className="flex flex-col gap-1 2xl:gap-4">
                                {(displayData || []).map((slice, i) => {
                                    const dot = COLORS[i % COLORS.length];
                                    const chart = chartRef.current;
                                    const isVisible = chart ? chart.getDataVisibility(i) : true;

                                    return (
                                        <button
                                            key={`${slice.name}-${i}`}
                                            type="button"
                                            className="text-left"
                                            onClick={() => {
                                                const chart = chartRef.current;
                                                if (!chart) return;
                                                chart.toggleDataVisibility(i);
                                                chart.update();
                                                setLegendTick((t) => t + 1);
                                            }}
                                        >
                                            <div className={`flex items-start gap-3 ${isVisible ? "opacity-100" : "opacity-40"}`}>
                                                <span
                                                    className="mt-1.5 inline-block h-2.5 w-2.5 rounded-full"
                                                    style={{ backgroundColor: dot }}
                                                />

                                                <div className="min-w-0">
                                                    <div
                                                        className={`truncate ${isVisible ? "" : "line-through"}`}
                                                        style={{ fontSize: isLaptop ? 10 : 12, color: "#414042" }}
                                                        title={slice.name}
                                                    >
                                                        {slice.name}
                                                    </div>

                                                    <div
                                                        className="whitespace-nowrap"
                                                        style={{ fontSize: isLaptop ? 10 : 12, color: "#414042" }}
                                                    >
                                                        {slice.value.toLocaleString()} ({slice.pct.toFixed(2)}%)
                                                    </div>
                                                </div>
                                            </div>
                                        </button>
                                    );
                                })}
                            </div>

                            {/* ✅ Total from API (not computed from slices) */}
                            {/* <div className="mt-4 pt-3 border-t border-slate-200 text-xs text-[#414042]">
                                Total: <span className="font-semibold">{apiTotal.toLocaleString()}</span>
                            </div> */}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
