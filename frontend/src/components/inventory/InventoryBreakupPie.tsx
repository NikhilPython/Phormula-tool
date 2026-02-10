// "use client";

// import React, { useEffect, useMemo, useRef, useState } from "react";
// import { Pie } from "react-chartjs-2";
// import {
//     Chart as ChartJS,
//     ArcElement,
//     Tooltip,
//     Legend,
//     ChartData,
//     ChartOptions,
//     TooltipItem,
// } from "chart.js";
// import PageBreadcrumb from "@/components/common/PageBreadCrumb";

// ChartJS.register(ArcElement, Tooltip, Legend);

// type AnyRow = Record<string, any>;

// type Slice = {
//     name: string;   // product name
//     value: number;  // inventory count
//     pct: number;    // percent of API total
// };

// type Props = {
//     title?: string;
//     rows: AnyRow[];
//     height?: number;
//     noDataFound?: boolean;
//     onExportBase64Ready?: (base64: string | null) => void;

//     // ✅ NEW
//     exportTick?: number;

//     // optional if you still want it elsewhere
//     onExportMetricsReady?: (metrics: { name: string; value: number; pct: number }[] | null) => void;
// };

// const COLORS = ["#FDD36F", "#B75A5A", "#ED9F50", "#C49466", "#3A8EA4", "#B8C78C"];

// const toNum = (v: any) => {
//     if (v === null || v === undefined || v === "") return 0;
//     const n = Number(v);
//     return Number.isFinite(n) ? n : 0;
// };

// const isGrandTotalRow = (row: AnyRow) => {
//     const msku = String(row?.msku || "").trim().toUpperCase();
//     const pn = String(row?.product_name || "").trim().toUpperCase();
//     return (
//         row?.__isTotal === true ||
//         row?.is_total === true ||
//         msku === "GRAND TOTAL" ||
//         pn === "GRAND TOTAL"
//     );
// };

// export default function InventoryTopProductsPie({
//     title = "",
//     rows,
//     height = 280,
//     noDataFound = false,
//     onExportBase64Ready,
//     onExportMetricsReady,
//     exportTick = 0,
// }: Props) {

//     const [isLaptop, setIsLaptop] = useState(false);
//     const [isDesktop, setIsDesktop] = useState(false);
//     const [legendTick, setLegendTick] = useState(0);

//     const chartRef = useRef<any>(null);
//     const getChart = () => {
//         const refObj: any = chartRef.current;
//         return refObj?.chart ?? refObj ?? null;
//     };


//     const [pieMetrics, setPieMetrics] = useState<{ name: string; value: number; pct: number }[] | null>(null);
//     const pieMetricsRef = useRef<typeof pieMetrics>(null);

//     useEffect(() => {
//         pieMetricsRef.current = pieMetrics;
//     }, [pieMetrics]);


//     // ✅ Choose what "inventory count" means.
//     // Use "ending_total" because that's what you want breakup of (Inventory at month end -> Total)
//     const getInventoryCount = (r: AnyRow) =>
//         toNum(r?.ending_total ?? r?.__ending_total ?? r?.EndingTotal ?? r?.endingTotal);

//     const MIN_PRODUCTS = 5;

//     const { displayData, apiTotal } = useMemo(() => {
//         const all = rows || [];
//         if (!all.length) return { displayData: [] as Slice[], apiTotal: 0 };

//         const totalRow = all.find(isGrandTotalRow) || null;
//         const apiTotalVal = totalRow ? getInventoryCount(totalRow) : 0;

//         // Only actual products (exclude total/others rows)
//         const productRows = all.filter((r) => !isGrandTotalRow(r) && String(r?.msku || "").toUpperCase() !== "OTHERS");

//         const items = productRows
//             .map((r) => ({
//                 name: String(r?.product_name || r?.msku || "Unknown"),
//                 value: getInventoryCount(r),
//             }))
//             .filter((x) => x.value > 0);

//         // If API total is missing, fallback to sum of all products (last resort)
//         const safeTotal =
//             apiTotalVal > 0 ? apiTotalVal : items.reduce((s, d) => s + d.value, 0);

//         if (!items.length || safeTotal <= 0) {
//             return { displayData: [] as Slice[], apiTotal: safeTotal };
//         }

//         // Sort desc
//         const sorted = [...items].sort((a, b) => b.value - a.value);

//         // Keep top MIN_PRODUCTS
//         const kept = sorted.slice(0, Math.min(MIN_PRODUCTS, sorted.length));
//         const keptSum = kept.reduce((s, d) => s + d.value, 0);

//         // ✅ Others must be "API Total - keptSum" (not recomputed from remaining)
//         // This guarantees pie always matches your API total.
//         const othersValue = Math.max(0, safeTotal - keptSum);

//         const rebuiltKept: Slice[] = kept.map((d) => ({
//             ...d,
//             pct: (d.value / safeTotal) * 100,
//         }));

//         const final: Slice[] =
//             othersValue > 0
//                 ? [
//                     ...rebuiltKept,
//                     { name: "Others", value: othersValue, pct: (othersValue / safeTotal) * 100 },
//                 ]
//                 : rebuiltKept;

//         return { displayData: final, apiTotal: safeTotal };
//     }, [rows]);

//     useEffect(() => {
//         if (!onExportMetricsReady) return;

//         if (!displayData?.length || apiTotal <= 0) {
//             onExportMetricsReady(null);
//             return;
//         }

//         // ✅ send legend rows to page
//         onExportMetricsReady(displayData.map(d => ({
//             name: d.name,
//             value: Number(d.value || 0),
//             pct: Number(d.pct || 0), // 0..100
//         })));
//     }, [displayData, apiTotal, onExportMetricsReady]);

//     const renderSquarePieForExport = (size: number) => {
//         const src = getChart();
//         if (!src) return null;

//         const dpr = window.devicePixelRatio || 1;

//         const c = document.createElement("canvas");
//         c.width = Math.floor(size * dpr);
//         c.height = Math.floor(size * dpr);
//         c.style.width = `${size}px`;
//         c.style.height = `${size}px`;

//         const ctx = c.getContext("2d");
//         if (!ctx) return null;

//         // Build an export-only chart config (square + non-responsive)
//         const exportConfig = {
//             type: "pie" as const,
//             data: JSON.parse(JSON.stringify(src.data)), // clone
//             options: {
//                 ...(src.options || {}),
//                 responsive: false,
//                 maintainAspectRatio: true,
//                 aspectRatio: 1,
//                 animation: false,
//                 plugins: {
//                     ...(src.options?.plugins || {}),
//                     legend: { display: false },
//                 },
//             },
//         };

//         // Respect current slice visibility (legend toggles)
//         // Chart.js meta.hidden uses "null = visible, true = hidden"
//         const tmp = new ChartJS(ctx, exportConfig as any);

//         try {
//             for (let i = 0; i < (src.data?.datasets?.[0]?.data?.length ?? 0); i++) {
//                 const visible = src.getDataVisibility(i);
//                 if (!visible) tmp.toggleDataVisibility(i);
//             }
//             tmp.update("none");
//             return c;
//         } finally {
//             // We can't destroy before using canvas bitmap; return canvas, but destroy chart instance now
//             // (canvas keeps pixels)
//             tmp.destroy();
//         }
//     };

//     const exportChartBase64 = () => {
//         try {
//             const chart = getChart();
//             if (!chart) return null;

//             chart.update("none");

//             const srcCanvas: HTMLCanvasElement | undefined = chart?.canvas;

//             if (!srcCanvas) return null;

//             // ===== Output size =====
//             const outW = 1400;
//             const outH = 520;

//             // Layout controls
//             const pad = 12;

//             // ✅ pie size in image (square so it stays circular)
//             const pieBox = 500; // 🔧 change this (420..520) to resize pie in PNG

//             // ✅ spacing between pie and legend
//             const gapPieLegend = 18;

//             const legendX = pad + pieBox + gapPieLegend;
//             const legendW = outW - legendX - pad;

//             const out = document.createElement("canvas");
//             out.width = outW;
//             out.height = outH;

//             const ctx = out.getContext("2d");
//             if (!ctx) return null;

//             // background
//             ctx.fillStyle = "#ffffff";
//             ctx.fillRect(0, 0, outW, outH);

//             // ✅ NO TITLE / HEADING drawn into the image

//             // draw pie (left) — ✅ crop to square first so it stays round
//             const pieX = pad;
//             const pieY = pad;


//             if (!srcCanvas) return null;

//             // Use the real pixel size of the source canvas
//             const sw = srcCanvas.width;
//             const sh = srcCanvas.height;

//             // Take a centered square crop from the source canvas
//             const s = Math.min(sw, sh);
//             const sx = Math.floor((sw - s) / 2);
//             const sy = Math.floor((sh - s) / 2);

//             // Draw that square crop into a square box in the output
//             ctx.drawImage(
//                 srcCanvas,
//                 sx, sy, s, s,          // source crop (square)
//                 pieX, pieY, pieBox, pieBox // destination (square)
//             );

//             // legend style controls
//             const dotR = 5;
//             const gap = 8;
//             // const lineH = 16;
//             // const rowGap = 10;

//             ctx.font = "13px Arial";
//             ctx.textBaseline = "top";

//             const wrapText = (text: string, x: number, y: number, maxW: number) => {
//                 const words = String(text || "").split(" ");
//                 let line = "";
//                 let yy = y;

//                 for (const w of words) {
//                     const test = line ? `${line} ${w}` : w;
//                     if (ctx.measureText(test).width > maxW && line) {
//                         ctx.fillText(line, x, yy);
//                         line = w;
//                         yy += lineH;
//                     } else {
//                         line = test;
//                     }
//                 }
//                 if (line) ctx.fillText(line, x, yy);
//                 return yy;
//             };

//             const strike = (x: number, y: number, text: string) => {
//                 const w = ctx.measureText(text).width;
//                 const midY = y + 7;
//                 ctx.beginPath();
//                 ctx.moveTo(x, midY);
//                 ctx.lineTo(x + w, midY);
//                 ctx.strokeStyle = "#414042";
//                 ctx.lineWidth = 1;
//                 ctx.stroke();
//             };

//             // ===== compute legend total height =====
//             const lineH = 16;
//             const rowGap = 10;

//             // estimate wrapped text height (safe average)
//             const avgLinesPerItem = 2; // name + value line
//             const rowHeight = avgLinesPerItem * lineH + rowGap;

//             const legendTotalHeight = (displayData?.length || 0) * rowHeight;

//             // legend rows
//             // ✅ vertically center legend relative to pie
//             let y =
//                 pieY +
//                 Math.max(
//                     0,
//                     (pieBox - legendTotalHeight) / 2
//                 );

//             const textX = legendX + dotR * 2 + gap;
//             const maxTextW = legendW - (dotR * 2 + gap) - 10;

//             (displayData || []).forEach((slice, i) => {
//                 const dot = COLORS[i % COLORS.length];
//                 const isVisible = chart.getDataVisibility(i);

//                 ctx.save();
//                 ctx.globalAlpha = isVisible ? 1 : 0.4;

//                 // dot
//                 ctx.beginPath();
//                 ctx.fillStyle = dot;
//                 ctx.arc(legendX + dotR, y + dotR + 2, dotR, 0, Math.PI * 2);
//                 ctx.fill();

//                 // label
//                 ctx.fillStyle = "#414042";
//                 const startLabelY = y;
//                 const endY = wrapText(slice.name, textX, startLabelY, maxTextW);

//                 // value + pct
//                 const metric = `${Number(slice.value || 0).toLocaleString()} (${Number(
//                     slice.pct || 0
//                 ).toFixed(2)}%)`;

//                 ctx.fillText(metric, textX, endY + lineH);

//                 if (!isVisible) {
//                     strike(textX, startLabelY, slice.name);
//                     strike(textX, endY + lineH, metric);
//                 }

//                 ctx.restore();

//                 y = endY + lineH + lineH + rowGap;
//             });

//             return out.toDataURL("image/png");
//         } catch (e) {
//             console.error("exportChartBase64 failed:", e);
//             return null;
//         }
//     };


//     const chartData = useMemo<ChartData<"pie", number[], string> | null>(() => {
//         const labels = (displayData || []).map((d) => d.name);
//         const values = (displayData || []).map((d) => Number(d.value || 0));

//         if (!labels.length || values.every((v) => v === 0)) return null;

//         const bg = labels.map((_, i) => COLORS[i % COLORS.length]);

//         return {
//             labels,
//             datasets: [
//                 {
//                     data: values,
//                     backgroundColor: bg,
//                     hoverBackgroundColor: bg, // ✅ same color on hover (no change)
//                     borderWidth: 0,
//                     borderColor: "transparent",
//                     hoverOffset: 4, // ✅ keep pop-out
//                 },
//             ],
//         };

//     }, [displayData]);

//     useEffect(() => {
//         const check = () => {
//             const w = window.innerWidth;
//             setIsLaptop(w >= 1024 && w < 1536);
//             setIsDesktop(w >= 1536);
//         };
//         check();
//         window.addEventListener("resize", check);
//         return () => window.removeEventListener("resize", check);
//     }, []);

//     const options = useMemo<ChartOptions<"pie">>(() => {
//         return {
//             responsive: true,
//             maintainAspectRatio: false,
//             animation: { duration: 0 },
//             radius: isLaptop ? "90%" : isDesktop ? "97%" : "100%",
//             plugins: {
//                 legend: { display: false },
//                 tooltip: {
//                     callbacks: {
//                         label: (ctx: TooltipItem<"pie">) => {
//                             const i = ctx.dataIndex;
//                             const slice = displayData?.[i];
//                             const val = Number(ctx.raw || 0);
//                             return `${slice?.name ?? ctx.label}: ${val.toLocaleString()} (${(
//                                 slice?.pct ?? 0
//                             ).toFixed(2)}%)`;
//                         },
//                     },
//                 },
//             },
//         };
//     }, [isLaptop, isDesktop, displayData]);

//     // Legend sync
//     useEffect(() => {
//         if (!chartData) return;
//         const t = setTimeout(() => setLegendTick((x) => x + 1), 50);
//         return () => clearTimeout(t);
//     }, [chartData]);

//     useEffect(() => {
//         if (!onExportBase64Ready) return;

//         if (!chartData || apiTotal <= 0) {
//             onExportBase64Ready(null);
//             return;
//         }

//         let cancelled = false;

//         const run = async () => {
//             // wait for the chart to paint at final size
//             await new Promise<void>((r) => requestAnimationFrame(() => r()));
//             await new Promise<void>((r) => requestAnimationFrame(() => r()));

//             if (cancelled) return;

//             const b64 = exportChartBase64(); // ✅ composed image: pie + legend metrics text
//             onExportBase64Ready(b64);
//         };

//         run();

//         return () => {
//             cancelled = true;
//         };
//         // ✅ regenerate every time download is clicked
//     }, [exportTick]);

//     const showNoData = noDataFound || !chartData || apiTotal <= 0;

//     return (
//         <div className="relative w-full h-full rounded-xl border border-slate-200 bg-[#D9D9D933] shadow-sm p-4 flex flex-col">
//             <div className="mb-1">
//                 <div className="w-fit mx-auto md:mx-0">
//                     <PageBreadcrumb pageTitle={title} variant="page" align="left" textSize="2xl" />
//                 </div>
//             </div>

//             {showNoData ? (
//                 <p className="text-center text-sm text-gray-500">No inventory data available.</p>
//             ) : (
//                 <div className="flex-1 min-h-0 w-full" >
//                     <div className="relative w-full h-full flex items-center gap-6">
//                         {/* LEFT: PIE */}
//                         <div className="flex-1 min-w-0 h-[260px] sm:h-[280px] md:h-[300px] 2xl:h-[360px]">
//                             <Pie
//                                 ref={chartRef}
//                                 data={chartData}
//                                 options={options}
//                                 className="!block"
//                                 style={{ width: "100%", height: "100%" }}
//                             />
//                         </div>

//                         {/* RIGHT: LEGEND */}
//                         <div
//                             className="shrink-0 overflow-auto pr-1"
//                             style={{
//                                 width: isDesktop ? 320 : isLaptop ? 280 : 300,
//                                 maxHeight: "100%",
//                             }}
//                         >
//                             <div className="flex flex-col gap-1 2xl:gap-4">
//                                 {(displayData || []).map((slice, i) => {
//                                     const dot = COLORS[i % COLORS.length];
//                                     const chart = getChart();
//                                     const isVisible = chart ? chart.getDataVisibility(i) : true;
//                                     return (
//                                         <button
//                                             key={`${slice.name}-${i}`}
//                                             type="button"
//                                             className="text-left"
//                                             onClick={async () => {
//                                                 const chart = getChart();
//                                                 if (!chart) return;

//                                                 chart.toggleDataVisibility(i);
//                                                 chart.update("none");
//                                                 setLegendTick((t) => t + 1);

//                                                 if (onExportBase64Ready) {
//                                                     await new Promise<void>((r) => requestAnimationFrame(() => r()));
//                                                     await new Promise<void>((r) => requestAnimationFrame(() => r()));
//                                                     onExportBase64Ready(exportChartBase64());
//                                                 }
//                                             }}

//                                         >
//                                             <div className={`flex items-start gap-3 ${isVisible ? "opacity-100" : "opacity-40"}`}>
//                                                 <span
//                                                     className="mt-1.5 inline-block h-2.5 w-2.5 rounded-full"
//                                                     style={{ backgroundColor: dot }}
//                                                 />

//                                                 <div className="min-w-0">
//                                                     <div
//                                                         className={`truncate ${isVisible ? "" : "line-through"}`}
//                                                         style={{ fontSize: isLaptop ? 10 : 12, color: "#414042" }}
//                                                         title={slice.name}
//                                                     >
//                                                         {slice.name}
//                                                     </div>

//                                                     <div
//                                                         className="whitespace-nowrap"
//                                                         style={{ fontSize: isLaptop ? 10 : 12, color: "#414042" }}
//                                                     >
//                                                         {slice.value.toLocaleString()} ({slice.pct.toFixed(2)}%)
//                                                     </div>
//                                                 </div>
//                                             </div>
//                                         </button>
//                                     );
//                                 })}
//                             </div>
//                         </div>
//                     </div>
//                 </div>
//             )}
//         </div>
//     );
// }



























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
  name: string; // product name
  value: number; // inventory count
  pct: number; // percent of API total
};

type Props = {
  title?: string;
  rows: AnyRow[];
  height?: number;
  noDataFound?: boolean;

  onExportBase64Ready?: (base64: string | null) => void;

  // ✅ trigger export from parent (download click)
  exportTick?: number;

  // optional (kept)
  onExportMetricsReady?: (metrics: { name: string; value: number; pct: number }[] | null) => void;
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

// optional icons (same vibe as reference)
const DeltaUpIcon = ({ className = "" }: { className?: string }) => (
  <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="currentColor">
    <path d="M12 4l9 16H3L12 4z" />
  </svg>
);

const DeltaDownIcon = ({ className = "" }: { className?: string }) => (
  <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="currentColor">
    <path d="M12 20L3 4h18l-9 16z" />
  </svg>
);

export default function InventoryTopProductsPie({
  title = "",
  rows,
  height = 280,
  noDataFound = false,
  onExportBase64Ready,
  onExportMetricsReady,
  exportTick = 0,
}: Props) {
  const [isLaptop, setIsLaptop] = useState(false);
  const [isDesktop, setIsDesktop] = useState(false);

  // rerender custom legend when toggling visibility
  const [legendTick, setLegendTick] = useState(0);

  const chartRef = useRef<any>(null);

  // In react-chartjs-2 v5, ref is chart instance.
  // In some older cases it's { chart: ... }. Support both.
  const getChart = () => {
    const refObj: any = chartRef.current;
    return refObj?.chart ?? refObj ?? null;
  };

  // ✅ Choose what "inventory count" means.
  const getInventoryCount = (r: AnyRow) =>
    toNum(r?.ending_total ?? r?.__ending_total ?? r?.EndingTotal ?? r?.endingTotal);

  const MIN_PRODUCTS = 5;

  const { displayData, apiTotal } = useMemo(() => {
    const all = rows || [];
    if (!all.length) return { displayData: [] as Slice[], apiTotal: 0 };

    const totalRow = all.find(isGrandTotalRow) || null;
    const apiTotalVal = totalRow ? getInventoryCount(totalRow) : 0;

    // Only actual products (exclude total/others rows)
    const productRows = all.filter(
      (r) => !isGrandTotalRow(r) && String(r?.msku || "").toUpperCase() !== "OTHERS"
    );

    const items = productRows
      .map((r) => ({
        name: String(r?.product_name || r?.msku || "Unknown"),
        value: getInventoryCount(r),
      }))
      .filter((x) => x.value > 0);

    // If API total is missing, fallback to sum of all products (last resort)
    const safeTotal = apiTotalVal > 0 ? apiTotalVal : items.reduce((s, d) => s + d.value, 0);

    if (!items.length || safeTotal <= 0) {
      return { displayData: [] as Slice[], apiTotal: safeTotal };
    }

    // Sort desc
    const sorted = [...items].sort((a, b) => b.value - a.value);

    // Keep top MIN_PRODUCTS
    const kept = sorted.slice(0, Math.min(MIN_PRODUCTS, sorted.length));
    const keptSum = kept.reduce((s, d) => s + d.value, 0);

    // Others must be "API Total - keptSum"
    const othersValue = Math.max(0, safeTotal - keptSum);

    const rebuiltKept: Slice[] = kept.map((d) => ({
      ...d,
      pct: (d.value / safeTotal) * 100,
    }));

    const final: Slice[] =
      othersValue > 0
        ? [...rebuiltKept, { name: "Others", value: othersValue, pct: (othersValue / safeTotal) * 100 }]
        : rebuiltKept;

    return { displayData: final, apiTotal: safeTotal };
  }, [rows]);

  useEffect(() => {
    if (!onExportMetricsReady) return;

    if (!displayData?.length || apiTotal <= 0) {
      onExportMetricsReady(null);
      return;
    }

    onExportMetricsReady(
      displayData.map((d) => ({
        name: d.name,
        value: Number(d.value || 0),
        pct: Number(d.pct || 0),
      }))
    );
  }, [displayData, apiTotal, onExportMetricsReady]);

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

  const exportChartBase64 = () => {
    try {
      const chart = getChart();
      if (!chart) return null;

      chart.update("none");
      const srcCanvas = chart.canvas as HTMLCanvasElement | undefined;
      if (!srcCanvas) return null;

      // Keep your previous export layout, but it still works with the new legend UI
      const outW = 1400;
      const outH = 520;
      const pad = 12;

      const pieBox = 500;
      const gapPieLegend = 18;

      const legendX = pad + pieBox + gapPieLegend;
      const legendW = outW - legendX - pad;

      const out = document.createElement("canvas");
      out.width = outW;
      out.height = outH;

      const ctx = out.getContext("2d");
      if (!ctx) return null;

      ctx.fillStyle = "#ffffff";
      ctx.fillRect(0, 0, outW, outH);

      // draw pie (cropped square -> round)
      const pieX = pad;
      const pieY = pad;

      const sw = srcCanvas.width;
      const sh = srcCanvas.height;
      const s = Math.min(sw, sh);
      const sx = Math.floor((sw - s) / 2);
      const sy = Math.floor((sh - s) / 2);

      ctx.drawImage(srcCanvas, sx, sy, s, s, pieX, pieY, pieBox, pieBox);

      // legend paint
      const dotR = 5;
      const gap = 8;
      const lineH = 16;
      const rowGap = 10;

      ctx.font = "13px Arial";
      ctx.textBaseline = "top";

      const wrapText = (text: string, x: number, y: number, maxW: number) => {
        const words = String(text || "").split(" ");
        let line = "";
        let yy = y;

        for (const w of words) {
          const test = line ? `${line} ${w}` : w;
          if (ctx.measureText(test).width > maxW && line) {
            ctx.fillText(line, x, yy);
            line = w;
            yy += lineH;
          } else {
            line = test;
          }
        }
        if (line) ctx.fillText(line, x, yy);
        return yy;
      };

      const strike = (x: number, y: number, text: string) => {
        const w = ctx.measureText(text).width;
        const midY = y + 7;
        ctx.beginPath();
        ctx.moveTo(x, midY);
        ctx.lineTo(x + w, midY);
        ctx.strokeStyle = "#414042";
        ctx.lineWidth = 1;
        ctx.stroke();
      };

      const avgLinesPerItem = 2;
      const rowHeight = avgLinesPerItem * lineH + rowGap;
      const legendTotalHeight = (displayData?.length || 0) * rowHeight;

      let y = pieY + Math.max(0, (pieBox - legendTotalHeight) / 2);

      const textX = legendX + dotR * 2 + gap;
      const maxTextW = legendW - (dotR * 2 + gap) - 10;

      (displayData || []).forEach((slice, i) => {
        const dot = COLORS[i % COLORS.length];
        const isVisible = chart.getDataVisibility(i);

        ctx.save();
        ctx.globalAlpha = isVisible ? 1 : 0.4;

        // dot
        ctx.beginPath();
        ctx.fillStyle = dot;
        ctx.arc(legendX + dotR, y + dotR + 2, dotR, 0, Math.PI * 2);
        ctx.fill();

        // label
        ctx.fillStyle = "#414042";
        const startLabelY = y;
        const endY = wrapText(slice.name, textX, startLabelY, maxTextW);

        // metric
        const metric = `${Number(slice.value || 0).toLocaleString()} (${Number(slice.pct || 0).toFixed(2)}%)`;
        ctx.fillText(metric, textX, endY + lineH);

        if (!isVisible) {
          strike(textX, startLabelY, slice.name);
          strike(textX, endY + lineH, metric);
        }

        ctx.restore();
        y = endY + lineH + lineH + rowGap;
      });

      return out.toDataURL("image/png");
    } catch (e) {
      console.error("exportChartBase64 failed:", e);
      return null;
    }
  };

  const chartData = useMemo<ChartData<"pie", number[], string> | null>(() => {
    const labels = (displayData || []).map((d) => d.name);
    const values = (displayData || []).map((d) => Math.abs(toNum(d.value)));

    if (!labels.length || values.every((v) => v === 0)) return null;

    const bg = labels.map((_, i) => COLORS[i % COLORS.length]);

    return {
      labels,
      datasets: [
        {
          data: values,
          backgroundColor: bg,
          hoverBackgroundColor: bg, // ✅ no color change on hover
          borderWidth: 0,
          borderColor: "transparent",
          spacing: 0,
          hoverOffset: 4,
          offset: 0,
        },
      ],
    };
  }, [displayData]);

  const options = useMemo<ChartOptions<"pie">>(() => {
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 0 },
      radius: isLaptop ? "100%" : isDesktop ? "95%" : "100%",
      layout: { padding: { top: 0, bottom: 0, left: 0, right: 0 } },
      elements: { arc: { borderWidth: 0, hoverOffset: 4 } },
      plugins: {
        legend: { display: false }, // custom legend
        tooltip: {
          enabled: !noDataFound,
          callbacks: {
            label: (ctx: TooltipItem<"pie">) => {
              const i = ctx.dataIndex;
              const slice = displayData?.[i];
              const val = Math.abs(toNum(ctx.raw));
              const pct = slice?.pct ?? 0;
              return `${slice?.name ?? ctx.label}: ${val.toLocaleString()} (${pct.toFixed(2)}%)`;
            },
          },
        },
      },
    };
  }, [isLaptop, isDesktop, displayData, noDataFound]);

  // Export base64 only when parent ticks (download click)
  useEffect(() => {
    if (!onExportBase64Ready) return;

    if (!chartData || apiTotal <= 0 || noDataFound) {
      onExportBase64Ready(null);
      return;
    }

    const run = async () => {
      await new Promise<void>((r) => requestAnimationFrame(() => r()));
      await new Promise<void>((r) => requestAnimationFrame(() => r()));
      onExportBase64Ready(exportChartBase64());
    };

    run();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [exportTick]);

  // Force rerender hook (same pattern as reference)
  useEffect(() => {
    if (!chartData) return;
    const t = setTimeout(() => setLegendTick((x) => x + 1), 50);
    return () => clearTimeout(t);
  }, [chartData]);

  const showNoData = noDataFound || !chartData || apiTotal <= 0;

  return (
    <div className="relative w-full h-full rounded-xl border border-slate-200 bg-[#D9D9D933] shadow-sm p-4 flex flex-col">
      <div className="mb-1 w-fit mx-auto md:mx-0">
        <PageBreadcrumb pageTitle={title} variant="page" align="left" textSize="2xl" />
      </div>

      {showNoData ? (
        <p className="text-center text-sm text-gray-500">No inventory data available.</p>
      ) : (
        <div className="flex-1 min-h-0 w-full">
          <div className="relative w-full flex flex-col xl:flex-row gap-4 xl:gap-6 items-stretch xl:items-center">
            {/* LEFT: PIE */}
            <div className="w-full xl:flex-1 min-w-0 h-[260px] md:h-[320px] xl:h-[300px] 2xl:h-[360px]">
              <Pie
                ref={chartRef}
                data={chartData}
                options={options}
                className="!block"
                style={{ width: "100%", height: "100%" }}
              />
            </div>

            {/* RIGHT: LEGEND (match reference layout) */}
            <div
              className="w-full xl:shrink-0 xl:self-center overflow-y-auto overflow-x-hidden pr-1 flex justify-center xl:justify-start"
              style={{
                width: isDesktop ? 260 : isLaptop ? 200 : "100%",
                maxHeight: "100%",
              }}
            >
              <div className="grid grid-cols-3 md:grid-cols-3 xl:flex xl:flex-col gap-x-10 gap-y-2 xl:gap-y-1 2xl:gap-y-4 w-fit">
                {(displayData || []).map((slice, i) => {
                  const dot = COLORS[i % COLORS.length];
                  const chart = getChart();
                  const isVisible = chart ? chart.getDataVisibility(i) : true;

                  const value = Math.abs(toNum(slice.value));
                  const pct = toNum(slice.pct);

                  return (
                    <button
                      key={`${slice.name}-${i}`}
                      type="button"
                      className="text-left w-full min-w-0"
                      onClick={() => {
                        const c = getChart();
                        if (!c) return;
                        c.toggleDataVisibility(i);
                        c.update();
                        setLegendTick((t) => t + 1);
                      }}
                    >
                      <div className={`flex items-start gap-3 min-w-0 ${isVisible ? "opacity-100" : "opacity-40"}`}>
                        <span
                          className="mt-1.5 inline-block h-2.5 w-2.5 rounded-full flex-none shrink-0"
                          style={{ backgroundColor: dot }}
                        />

                        <div className="min-w-0">
                          {/* line 1: Product name */}
                          <div
                            className={`truncate text-[10px] 2xl:text-xs ${isVisible ? "" : "line-through"}`}
                            style={{ color: "#414042" }}
                            title={slice.name}
                          >
                            {slice.name}
                          </div>

                          {/* line 2: value + pct (same style as reference) */}
                          <div className="text-[10px] 2xl:text-xs break-words" style={{ color: "#414042" }}>
                            {value.toLocaleString()} ({pct.toFixed(2)}%)
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
}
