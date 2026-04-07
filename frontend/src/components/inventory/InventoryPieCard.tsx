"use client";

import React, {
  useEffect,
  useMemo,
  useRef,
  useState,
  forwardRef,
  useImperativeHandle,
} from "react";
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
import "@/lib/chartSetup";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import Loader from "../loader/Loader";

ChartJS.register(ArcElement, Tooltip, Legend);

type PieDatum = { name: string; value: number };

const COLORS = ["#B75A5A", "#FDD36F", "#ED9F50", "#C49466", "#3A8EA4", "#B8C78C"];

const toNum = (v: any) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

export type InventoryPieCardHandle = {
  getExportImage: () => string | null;
};

const InventoryPieCard = forwardRef<
  InventoryPieCardHandle,
  {
    title: string;
    data: PieDatum[];
    height?: number;
    loading?: boolean;
    emptyText?: string;
  }
>(({ title, data, height = 320, loading = false, emptyText = "No data" }, ref) => {
  const [isLaptop, setIsLaptop] = useState(false);
  const [isDesktop, setIsDesktop] = useState(false);
  const [legendTick, setLegendTick] = useState(0);

  const chartRef = useRef<any>(null);
  const exportWrapRef = useRef<HTMLDivElement | null>(null);

  const getChart = () => {
    const refObj: any = chartRef.current;
    return refObj?.chart ?? refObj ?? null;
  };

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

  const total = useMemo(() => data.reduce((s, d) => s + Math.abs(toNum(d.value)), 0), [data]);

  const displayData = useMemo(() => {
    return (data || [])
      .map((d) => ({ ...d, value: Math.abs(toNum(d.value)) }))
      .filter((d) => d.value > 0);
  }, [data]);

  const chartData = useMemo<ChartData<"pie", number[], string> | null>(() => {
    const labels = displayData.map((d) => d.name);
    const values = displayData.map((d) => d.value);
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
          hoverOffset: 4,
        },
      ],
    };
  }, [displayData]);

  const options = useMemo<ChartOptions<"pie">>(() => {
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 0 },
      radius: isLaptop ? "96%" : isDesktop ? "100%" : "100%",
      layout: { padding: 0 },
      plugins: {
        legend: { display: false },
        tooltip: {
          enabled: true,
          callbacks: {
            label: (ctx: TooltipItem<"pie">) => {
              const label = String(ctx.label || "");
              const val = Math.abs(toNum(ctx.raw));
              const pct = total > 0 ? (val / total) * 100 : 0;
              return `${label}: ${val.toLocaleString(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              })} (${pct.toFixed(2)}%)`;
            },
          },
        },
      },
    };
  }, [isLaptop, isDesktop, total]);

  useImperativeHandle(ref, () => ({
    getExportImage: () => {
      const chart = getChart();
      if (!chart) return null;

      const sourceCanvas: HTMLCanvasElement | undefined =
        chart.canvas || chart.ctx?.canvas;
      if (!sourceCanvas) return null;

      const legendHeight = Math.max(180, displayData.length * 26 + 60);
      const canvas = document.createElement("canvas");
      canvas.width = 1400;
      canvas.height = 520;

      const ctx = canvas.getContext("2d");
      if (!ctx) return null;

      ctx.fillStyle = "#ffffff";
      ctx.fillRect(0, 0, canvas.width, canvas.height);

      ctx.fillStyle = "#414042";
      ctx.font = "bold 24px Arial";
      ctx.fillText(title, 32, 40);

      const chartAreaX = 20;
      const chartAreaY = 70;
      const chartAreaW = 760;
      const chartAreaH = 400;

      ctx.drawImage(sourceCanvas, chartAreaX, chartAreaY, chartAreaW, chartAreaH);

      const legendX = 840;
      let y = 110;

      displayData.forEach((slice, i) => {
        const value = Math.abs(toNum(slice.value));
        const pct = total > 0 ? (value / total) * 100 : 0;

        ctx.fillStyle = COLORS[i % COLORS.length];
        ctx.beginPath();
        ctx.arc(legendX, y, 7, 0, Math.PI * 2);
        ctx.fill();

        ctx.fillStyle = "#414042";
        ctx.font = "14px Arial";
        ctx.fillText(slice.name, legendX + 18, y + 4);

        ctx.font = "13px Arial";
        ctx.fillText(
          `${value.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })} (${pct.toFixed(2)}%)`,
          legendX + 18,
          y + 24
        );

        y += 46;
      });

      return canvas.toDataURL("image/png");
    },
  }));

  const showNoData = loading || !chartData || total <= 0;

  return (
    <div
      ref={exportWrapRef}
      className="rounded-xl border border-slate-200 bg-white shadow-sm p-4 flex flex-col"
    >
      <PageBreadcrumb variant="page" align="left" textSize="2xl" pageTitle={title} />

      {loading ? (
        // <div className="h-[260px] flex items-center justify-center text-xs text-neutral-500">
        //   Loading...
        // </div>
        <div className="flex flex-col items-center justify-center py-12 text-center">
          <Loader fullscreen transparent />
        </div>
      ) : !chartData || total <= 0 ? (
        <div className="h-[260px] flex items-center justify-center text-xs 2xl:text text-neutral-500">
          {emptyText}
        </div>
      ) : (
        <div className="mt-3 flex-1 min-h-0 w-full">
          <div className="relative w-full flex flex-col xl:flex-row gap-4 xl:gap-6 items-stretch xl:items-center">
            <div className="w-full xl:flex-1 min-w-0" style={{ height }}>
              <Pie
                ref={chartRef}
                data={chartData}
                options={options}
                className="!block"
                style={{ width: "100%", height: "100%" }}
              />
            </div>

            <div
              className="w-full xl:shrink-0 xl:self-center overflow-y-auto overflow-x-hidden pr-1 flex justify-center xl:justify-start"
              style={{
                width: isDesktop ? 200 : isLaptop ? 240 : "100%",
                maxHeight: height,
              }}
            >
              <div className="grid grid-cols-2 md:grid-cols-3 xl:flex xl:flex-col gap-x-10 gap-y-2 xl:gap-y-2 w-fit">
                {displayData.map((slice, i) => {
                  const dot = COLORS[i % COLORS.length];
                  const chart = getChart();
                  const isVisible = chart ? chart.getDataVisibility(i) : true;

                  const value = Math.abs(toNum(slice.value));
                  const pct = total > 0 ? (value / total) * 100 : 0;

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
                          <div
                            className={`truncate text-xs ${isVisible ? "" : "line-through"}`}
                            style={{ color: "#414042" }}
                            title={slice.name}
                          >
                            {slice.name}
                          </div>
                          <div className="text-xs break-words" style={{ color: "#414042" }}>
                            {value.toLocaleString(undefined, {
                              minimumFractionDigits: 2,
                              maximumFractionDigits: 2,
                            })} ({pct.toFixed(2)}%)
                          </div>
                        </div>
                      </div>
                    </button>
                  );
                })}
              </div>
            </div>

            <span className="hidden">{legendTick}</span>
          </div>
        </div>
      )}
    </div>
  );
});

InventoryPieCard.displayName = "InventoryPieCard";
export default InventoryPieCard;