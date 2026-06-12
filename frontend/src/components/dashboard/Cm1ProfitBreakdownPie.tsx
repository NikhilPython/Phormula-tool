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
import SegmentedToggle from "@/components/ui/SegmentedToggle";

ChartJS.register(ArcElement, Tooltip, Legend);

type CurrencyCode = "USD" | "GBP" | "INR" | "CAD";

type Cm1PieSlice = {
  name: string;
  value: number; // current CM1 profit (already in display currency)
  prevValue: number; // previous CM1 profit (same currency)
  pct: number; // share of current total
  deltaPct: number | null; // % change vs previous
};

type Props = {
  title?: string;
  data: Cm1PieSlice[];
  cm2Data?: Cm1PieSlice[];
  currency: CurrencyCode;
  height?: number;
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
  cm2Data = [],
  currency,
  height = 280,
  noDataFound = false,
  onExportBase64Ready,
}: Props) {
  const currencySymbol = currencySymbolFromCode(currency);

  const [profitPieType, setProfitPieType] = useState<"cm1" | "cm2">("cm1");

  const activeData = profitPieType === "cm1" ? data : cm2Data;
  const activeTitle ="Profit Breakdown";

  const showDelta = profitPieType === "cm1";

  const [isLaptop, setIsLaptop] = useState(false);
  const [isDesktop, setIsDesktop] = useState(false);
  const [legendTick, setLegendTick] = useState(0);

  const chartRef = useRef<any>(null);

  // ✅ RULES:
  // - Compute top SKUs until cumulative profit share >= 80%
  // - Always show at least 5 SKUs
  // - If it takes >5 SKUs to reach 80%, show that many
  // - Everything else rolls into "Others"
  const MIN_SKUS = 5;
  const TARGET_SHARE = 0.8;

  // const displayData = useMemo<Cm1PieSlice[]>(() => {
  //   const arr = (data || []).filter((d) => Number(d.value || 0) !== 0);
  //   if (!arr.length) return [];

  //   // Sort by current profit magnitude (matches your chart logic)
  //   const sorted = [...arr].sort(
  //     (a, b) => Math.abs(Number(b.value || 0)) - Math.abs(Number(a.value || 0))
  //   );

  //   const total = sorted.reduce(
  //     (s, d) => s + Math.abs(Number(d.value || 0)),
  //     0
  //   );
  //   if (!total) return [];

  //   let cum = 0;
  //   let cutoff = 0;

  //   for (let i = 0; i < sorted.length; i++) {
  //     cum += Math.abs(Number(sorted[i].value || 0));
  //     cutoff = i + 1;
  //     if (cum / total >= TARGET_SHARE) break;
  //   }

  //   const keepCount = Math.min(sorted.length, Math.max(MIN_SKUS, cutoff));
  //   const kept = sorted.slice(0, keepCount);
  //   const rest = sorted.slice(keepCount);

  //   const rebuiltKept: Cm1PieSlice[] = kept.map((d) => {
  //     const v = Math.abs(Number(d.value || 0));
  //     const pv = Math.abs(Number(d.prevValue || 0));
  //     const pct = total ? (v / total) * 100 : 0;

  //     // Recompute deltaPct to remain consistent after abs/grouping
  //     const deltaPct = pv === 0 ? null : ((v - pv) / pv) * 100;

  //     return { ...d, value: v, prevValue: pv, pct, deltaPct };
  //   });

  //   if (!rest.length) return rebuiltKept;

  //   const othersValue = rest.reduce(
  //     (s, d) => s + Math.abs(Number(d.value || 0)),
  //     0
  //   );
  //   const othersPrev = rest.reduce(
  //     (s, d) => s + Math.abs(Number(d.prevValue || 0)),
  //     0
  //   );

  //   const othersPct = total ? (othersValue / total) * 100 : 0;
  //   const othersDeltaPct =
  //     othersPrev === 0 ? null : ((othersValue - othersPrev) / othersPrev) * 100;

  //   return [
  //     ...rebuiltKept,
  //     {
  //       name: "Others",
  //       value: othersValue,
  //       prevValue: othersPrev,
  //       pct: othersPct,
  //       deltaPct: othersDeltaPct,
  //     },
  //   ];
  // }, [data]);

 const displayData = useMemo<Cm1PieSlice[]>(() => {
  const isOthers = (name?: string) =>
    String(name || "").trim().toLowerCase() === "others";

  const isTotal = (name?: string) => {
    const n = String(name || "").trim().toLowerCase();
    return (
      n === "total" ||
      n === "grand total" ||
      n === "total_segment" ||
      n.includes("total")
    );
  };

  // ✅ Remove Total rows first
  const arr = (activeData || [])
    .filter((d) => !isTotal(d.name))
    .filter((d) => Number(d.value || 0) !== 0 || Number(d.prevValue || 0) !== 0);

  if (!arr.length) return [];

  const normalized = arr.map((d) => {
    const v = Math.abs(Number(d.value || 0));
    const pv = Math.abs(Number(d.prevValue || 0));
    const fallbackDelta = pv === 0 ? null : ((v - pv) / pv) * 100;

    return {
      ...d,
      name: String(d.name || "").trim() || "Others",
      value: v,
      prevValue: pv,
      pct: Number(d.pct || 0),
      deltaPct: d.deltaPct ?? fallbackDelta,
    };
  });

  // ✅ Separate existing Others, but do NOT skip grouping
  const existingOthersRows = normalized.filter((d) => isOthers(d.name));
  const skuRows = normalized.filter((d) => !isOthers(d.name));

  const sorted = [...skuRows].sort(
    (a, b) => Math.abs(Number(b.value || 0)) - Math.abs(Number(a.value || 0))
  );

  const totalAbs =
    normalized.reduce((sum, d) => sum + Math.abs(Number(d.value || 0)), 0) || 1;

  let cumulative = 0;
  let cutoff = 0;

  for (let i = 0; i < sorted.length; i++) {
    cumulative += Math.abs(Number(sorted[i].value || 0));
    cutoff = i + 1;

    if (cumulative / totalAbs >= TARGET_SHARE) break;
  }

  // ✅ Minimum 5 SKUs, or more if needed to reach 80%
  const keepCount = Math.min(sorted.length, Math.max(MIN_SKUS, cutoff));

  const kept = sorted.slice(0, keepCount);
  const rest = sorted.slice(keepCount);

  // ✅ Existing Others + all non-kept SKUs become one Others row
  const othersSource = [...rest, ...existingOthersRows];

  const rebuiltKept = kept.map((d) => ({
    ...d,
    pct: (Math.abs(Number(d.value || 0)) / totalAbs) * 100,
  }));

  if (!othersSource.length) {
    return rebuiltKept;
  }

  const othersValue = othersSource.reduce(
    (sum, d) => sum + Math.abs(Number(d.value || 0)),
    0
  );

  const othersPrev = othersSource.reduce(
    (sum, d) => sum + Math.abs(Number(d.prevValue || 0)),
    0
  );

  const othersDeltaPct =
    othersPrev === 0 ? null : ((othersValue - othersPrev) / othersPrev) * 100;

  return [
    ...rebuiltKept,
    {
      name: "Others",
      value: othersValue,
      prevValue: othersPrev,
      pct: (othersValue / totalAbs) * 100,
      deltaPct: othersDeltaPct,
    },
  ];
}, [activeData]);



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
    const values = (displayData || []).map((d) =>
      Math.round(Math.abs(Number(d.value || 0)))
    );

    if (!labels.length || values.every((v) => v === 0)) return null;

    const bg = labels.map((_, i) => COLORS[i % COLORS.length]);

    return {
      labels,
      datasets: [
        {
          data: values,
          backgroundColor: bg,
          hoverBackgroundColor: bg,
          borderWidth: 0,
          borderColor: "transparent",
          spacing: 0,
          hoverOffset: 4,
          offset: 0,
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
      radius: isLaptop ? "99%" : isDesktop ? "95%" : "100%",
      layout: { padding: { top: 0, bottom: 0, left: 0, right: 0 } },
      elements: { arc: { borderWidth: 0, hoverOffset: 4 } },

      plugins: {
        legend: { display: false }, // ✅ turn off chart legend

        tooltip: {
          callbacks: {
            label: (ctx: TooltipItem<"pie">) => {
              const i = ctx.dataIndex;
              const slice = displayData?.[i];
              const val = Math.round(Number(ctx.raw || 0));
              const delta = slice?.deltaPct;

              const deltaSymbol =
                delta == null ? "—" : delta > 0 ? "▲" : delta < 0 ? "▼" : "■";

              const deltaText =
                delta == null ? "—" : `${deltaSymbol} ${Math.abs(delta).toFixed(2)}%`;

              return `${slice?.name ?? ctx.label}: ${currencySymbol}${val.toLocaleString()} (${Math.round(
                slice?.pct ?? 0
              )}%)${showDelta ? ` (${deltaText})` : ""}`;
            },
          },
        },
      },
    };
  }, [currencySymbol, isLaptop, isDesktop, displayData, showDelta]);

  // ✅ Sync legend once chart mounts (so isVisible reads correctly)
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

  return (
    <div className="relative w-full h-full rounded-xl border border-slate-200 bg-white shadow-sm p-4 flex flex-col">
      {/* <div className="mb-1">
        <div className="w-fit mx-auto md:mx-0">
          <PageBreadcrumb
            pageTitle={title}
            variant="page"
            align="left"
            textSize="2xl"
          />
        </div>
      </div> */}
      <div className="mb-1 flex items-center justify-between gap-3">
        <div className="w-fit">
          <PageBreadcrumb
            pageTitle={activeTitle}
            variant="page"
            align="left"
            textSize="2xl"
          />
        </div>

        {cm2Data?.length > 0 && (
          <SegmentedToggle<"cm1" | "cm2">
            value={profitPieType}
            options={[
              { value: "cm1", label: "CM1" },
              { value: "cm2", label: "CM2" },
            ]}
            onChange={setProfitPieType}
            compact
            textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
          />
        )}
      </div>

      {!chartData ? (
        noDataFound ? null : (
          <p className="text-center text-sm text-gray-500">
            No CM1 data available.
          </p>
        )
      ) : (
        <div className="flex-1 min-h-0 w-full">
          <div className="relative w-full h-full flex flex-col xl:flex-row gap-4 xl:gap-6 items-stretch">
            {/* LEFT: PIE */}
            {/* <div className="w-full xl:flex-1 min-w-0 h-[260px] md:h-[320px] xl:h-[300px] 2xl:h-[460px]"> */}
            <div className="w-full xl:flex-1 min-w-0 flex-1 min-h-[260px]">
              <Pie
                ref={chartRef}
                data={chartData}
                options={options}
                className="!block"
                style={{ width: "100%", height: "100%" }}
              />
            </div>

            {/* RIGHT: LEGEND */}
            {/* <div
              className="w-full xl:shrink-0 overflow-y-auto overflow-x-hidden pr-1 flex justify-center xl:justify-start self-stretch"
              style={{
                width: isDesktop ? 260 : isLaptop ? 180 : "100%",
                maxHeight: "100%",
              }}
            >
              
              <div className="grid w-full grid-cols-2 sm:grid-cols-3 xl:flex xl:w-auto xl:flex-col gap-x-6 gap-y-3 xl:gap-y-2">
                {(displayData || []).map((slice, i) => {
                  const dot = COLORS[i % COLORS.length];
                  const chart = chartRef.current;
                  const isVisible = chart ? chart.getDataVisibility(i) : true;

                  const value = Math.abs(Number(slice.value || 0));
                  const pct = Number(slice.pct || 0);
                  const delta = slice.deltaPct;

                  const deltaSymbol =
                    delta == null ? "—" : delta > 0 ? "▲" : delta < 0 ? "▼" : "■";

                  const deltaText =
                    delta == null ? "—" : `${deltaSymbol} ${Math.abs(delta).toFixed(2)}%`;

                  const deltaClass =
                    delta == null
                      ? "text-[#414042]"
                      : delta >= 0
                        ? "text-green-500"
                        : "text-red-500";

                  return (
                    <button
                      key={`${slice.name}-${i}`}
                      type="button"
                      className="text-left w-full min-w-0"
                      onClick={() => {
                        const c = chartRef.current;
                        if (!c) return;
                        c.toggleDataVisibility(i);
                        c.update();
                        setLegendTick((t) => t + 1);
                      }}
                    >
                     
                      <div
                        className={`flex items-start gap-2 sm:gap-3 min-w-0 ${isVisible ? "opacity-100" : "opacity-40"
                          }`}
                      >
                        
                        <span
                          className="mt-[3px] inline-block h-2.5 w-2.5 rounded-full flex-none shrink-0"
                          style={{ backgroundColor: dot }}
                        />

                        <div className="min-w-0 flex flex-col items-start">
                         
                          <div
                            className={`truncate text-[10px] 2xl:text-xs ${isVisible ? "" : "line-through"
                              }`}
                            style={{ color: "#414042" }}
                            title={slice.name}
                          >
                            {slice.name}
                          </div>

                         
                          <div
                            className="text-[10px] 2xl:text-xs break-words"
                            style={{ color: "#414042" }}
                          >
                            {currencySymbol}
                            {value.toLocaleString(undefined, {
                              minimumFractionDigits: 2,
                              maximumFractionDigits: 2,
                            })}{" "}
                            ({pct.toFixed(2)}%){" "}
                            <span className={deltaClass}>({deltaText})</span>
                          </div>
                        </div>
                      </div>
                    </button>
                  );
                })}
              </div>
            </div> */}

            {/* RIGHT: LEGEND */}
            <div
              className="w-full xl:shrink-0 self-stretch flex justify-center xl:justify-start"
              style={{
                width: isDesktop ? 260 : isLaptop ? 180 : "100%",
              }}
            >
              {/* This wrapper centers vertically */}
              <div className="h-full w-full flex items-center">
                {/* This wrapper handles scrolling */}
                <div className="w-full max-h-full overflow-y-auto overflow-x-hidden pr-1">
                  <div className="grid w-full grid-cols-2 sm:grid-cols-3 xl:flex xl:w-auto xl:flex-col gap-x-6 gap-y-3 xl:gap-y-2">
                    {(displayData || []).map((slice, i) => {
                      const dot = COLORS[i % COLORS.length];
                      const chart = chartRef.current;
                      const isVisible = chart ? chart.getDataVisibility(i) : true;

                      const value = Math.round(Math.abs(Number(slice.value || 0)));
                      const pct = Math.round(Number(slice.pct || 0));
                      const delta = slice.deltaPct;

                      const deltaSymbol =
                        delta == null ? "—" : delta > 0 ? "▲" : delta < 0 ? "▼" : "■";

                      const deltaText =
                        delta == null ? "—" : `${deltaSymbol} ${Math.abs(delta).toFixed(2)}%`;

                      const deltaClass =
                        delta == null
                          ? "text-[#414042]"
                          : delta >= 0
                            ? "text-green-500"
                            : "text-red-500";

                      return (
                        <button
                          key={`${slice.name}-${i}`}
                          type="button"
                          className="text-left w-full min-w-0"
                          onClick={() => {
                            const c = chartRef.current;
                            if (!c) return;
                            c.toggleDataVisibility(i);
                            c.update();
                            setLegendTick((t) => t + 1);
                          }}
                        >
                          <div
                            className={`flex items-start gap-2 sm:gap-3 min-w-0 ${isVisible ? "opacity-100" : "opacity-40"
                              }`}
                          >
                            <span
                              className="mt-[3px] inline-block h-2.5 w-2.5 rounded-full flex-none shrink-0"
                              style={{ backgroundColor: dot }}
                            />

                            <div className="min-w-0 flex flex-col items-start">
                              <div
                                className={`truncate text-[10px] 2xl:text-xs ${isVisible ? "" : "line-through"
                                  }`}
                                style={{ color: "#414042" }}
                                title={slice.name}
                              >
                                {slice.name}
                              </div>

                              <div
                                className="text-[10px] 2xl:text-xs break-words"
                                style={{ color: "#414042" }}
                              >
                                {currencySymbol}
                                {value.toLocaleString()} ({pct}%){" "}
                                {showDelta && <span className={deltaClass}>({deltaText})</span>}
                              </div>
                            </div>
                          </div>
                        </button>
                      );
                    })}
                  </div>
                </div>
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





