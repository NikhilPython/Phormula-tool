"use client";

import React, { useMemo, useRef, useState } from "react";
import dynamic from "next/dynamic";
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
import {
  CountryKey,
  formatCountryLabel,
  getCountryColor,
  normalizeCountryKey,
} from "./productwiseHelpers";
import ExcelJS from "exceljs";
import { saveAs } from "file-saver";
import SegmentedToggle from "../ui/SegmentedToggle";
import ProductSearchDropdown from "@/components/products/ProductSearchDropdown";
import { AiButton } from "../ui/button/AiButton";
import PageBreadcrumb from "../common/PageBreadCrumb";
import { Chart as ChartJSCore } from "chart.js";
import { exportProductwiseTrendsExcel } from "@/lib/excel/exportCurrentInventoryExcel";

const Line = dynamic(() => import("react-chartjs-2").then((m) => m.Line), {
  ssr: false,
});

type TrendTab = "sales_cm1" | "units";

type ExportMeta = {
  titleLine: string;
  titleCountry: string;
  platformLabel?: string;
  periodLabel: string;
  companyName: string;
  brandName: string;
  currencyLabel?: string; // "$" / "£" / "₹"
};

type ExportCountryCard = {
  countryKey: string;
  countryLabel: string;

  totalSales: number;
  totalUnits: number;
  totalProfit: number;

  avgMonthlySales: number;
  avgSellingPrice: number;
  cm1ProfitPct: number;

  bestSalesMonth: string;
  bestSalesValue: number;

  bestUnitsMonth: string;
  bestUnitsValue: number;

  bestProfitMonth: string;
  bestProfitValue: number;
};

interface TrendChartSectionProps {
  productname: string;
  title: string;
  chartDataList: any[];
  chartOptions: any;
  nonEmptyCountriesFromApi: CountryKey[];
  selectedCountries: Record<CountryKey, boolean>;
  onToggleCountry: (country: CountryKey) => void;
  authToken: string | null;
  onProductSelect: (productName: string) => void;

  onViewBusinessInsights?: () => void;
  insightsLoading?: boolean;
  isPreviewMode?: boolean;

  // ✅ NEW props coming from ProductwisePerformance
  exportMeta?: ExportMeta;
  exportCountryCards?: ExportCountryCard[];
}

const TrendChartSection: React.FC<TrendChartSectionProps> = ({
  productname,
  title,
  chartDataList,
  chartOptions,
  nonEmptyCountriesFromApi,
  selectedCountries,
  onToggleCountry,
  authToken,
  onProductSelect,
  onViewBusinessInsights,
  isPreviewMode,

  // ✅ add these
  exportMeta,
  exportCountryCards,
}) => {
  const [activeTab, setActiveTab] = useState<TrendTab>("sales_cm1");

  const roundDatasetValues = (datasets: any[] = []) =>
    datasets.map((ds: any) => ({
      ...ds,
      data: (ds.data || []).map((value: any) => {
        const num = Number(value);
        return Number.isFinite(num) ? Math.round(num) : value;
      }),
    }));

  // ✅ AI insights loading state (toggle)
  const [insightsLoading, setInsightsLoading] = useState(false);

  // ref to the chart instance
  const chartRef = useRef<any>(null);

  // ---------- Build chart data based on active tab ----------
  const processedChartData = useMemo(() => {
    if (!chartDataList) return null;

    const styleDatasetsByLabel = (datasets: any[] = []) =>
      datasets.map((ds: any) => {
        const label = (ds.label || "").toString().toLowerCase();
        const isCm1OrProfit =
          label.includes("cm1") ||
          label.includes("cm 1") ||
          label.includes("cm-1") ||
          label.includes("profit");

        return {
          ...ds,
          fill: false,
          borderDash: isCm1OrProfit ? [6, 6] : [], // dotted for CM1/Profit
          tension: 0.35,
        };
      });

    // TAB 1: Net Sales + CM1
    if (activeTab === "sales_cm1") {
      const netSalesData = chartDataList[0]; // Net Sales
      const cm1Data = chartDataList[2]; // CM1 Profit

      if (!netSalesData) return null;

      if (!cm1Data) {
        return {
          ...netSalesData,
          datasets: roundDatasetValues(styleDatasetsByLabel(netSalesData.datasets || [])),
        };
      }

      const labels = netSalesData.labels;

      const netSalesDatasets = (netSalesData.datasets || []).map((ds: any) => {
        const label = (ds.label || "").toString().toLowerCase();
        const isCm1OrProfit =
          label.includes("cm1") ||
          label.includes("cm 1") ||
          label.includes("cm-1") ||
          label.includes("profit");

        return {
          ...ds,
          fill: false,
          borderDash: isCm1OrProfit ? [6, 6] : [],
          tension: 0.35,
        };
      });

      const cm1Datasets = (cm1Data.datasets || []).map((ds: any) => ({
        ...ds,
        fill: false,
        borderDash: [6, 6],
        tension: 0.35
      }));

      return {
        ...netSalesData,
        labels,
        datasets: roundDatasetValues([...netSalesDatasets, ...cm1Datasets]),
      };
    }

    // TAB 2: Units
    if (activeTab === "units") {
      const unitsData = chartDataList[1];
      if (!unitsData) return null;

      return {
        ...unitsData,
        datasets: styleDatasetsByLabel(unitsData.datasets || []),
      };
    }

    return null;
  }, [chartDataList, activeTab]);

  const processedChartOptions = useMemo(() => {
    return {
      ...chartOptions,
      plugins: {
        ...chartOptions?.plugins,
        tooltip: {
          ...chartOptions?.plugins?.tooltip,
          callbacks: {
            ...chartOptions?.plugins?.tooltip?.callbacks,
            label: (context: any) => {
              const rawLabel = String(context.dataset.label || "");
              const value = Math.round(Number(context.parsed.y || 0));

              const isGlobalView =
                String(exportMeta?.titleCountry || "").toLowerCase() === "global";

              const countryLabel = rawLabel
                .replace(/net sales/gi, "")
                .replace(/cm1 profit/gi, "")
                .replace(/profit/gi, "")
                .replace(/quantity/gi, "")
                .replace(/units/gi, "")
                .trim();

              const metricLabel = /profit/i.test(rawLabel)
                ? "CM1 Profit"
                : /net sales/i.test(rawLabel)
                  ? "Net Sales"
                  : /quantity|units/i.test(rawLabel)
                    ? "Units"
                    : rawLabel;

              const finalLabel = isGlobalView && countryLabel
                ? `${countryLabel} ${metricLabel}`
                : metricLabel;

              return `${finalLabel}: ${value.toLocaleString()}`;
            },
          },
        },
      },
      scales: {
        ...chartOptions?.scales,
        y: {
          ...chartOptions?.scales?.y,
          ticks: {
            ...chartOptions?.scales?.y?.ticks,
            callback: (value: any) => {
              const num = Number(value);
              return Number.isFinite(num)
                ? Math.round(num).toLocaleString()
                : value;
            },
          },
        },
      },
    };
  }, [chartOptions, exportMeta?.titleCountry]);

  const getTitleByTab = () =>
    activeTab === "sales_cm1" ? "Net Sales + CM1 Profit Trend" : "Units Trend";

  // ✅ AI button handler with local loading toggle
  const handleAiInsightsClick = async () => {
    if (!onViewBusinessInsights || insightsLoading) return;

    try {
      setInsightsLoading(true);
      await Promise.resolve(onViewBusinessInsights());
    } finally {
      setInsightsLoading(false);
    }
  };

  const buildChartDataForTab = (tab: TrendTab) => {
    if (!chartDataList) return null;

    const styleDatasetsByLabel = (datasets: any[] = []) =>
      datasets.map((ds: any) => {
        const label = (ds.label || "").toString().toLowerCase();
        const isCm1OrProfit =
          label.includes("cm1") ||
          label.includes("cm 1") ||
          label.includes("cm-1") ||
          label.includes("profit");

        return {
          ...ds,
          fill: false,
          borderDash: isCm1OrProfit ? [6, 6] : [],
          tension: 0.35,
        };
      });

    if (tab === "sales_cm1") {
      const netSalesData = chartDataList[0];
      const cm1Data = chartDataList[2];

      if (!netSalesData) return null;

      if (!cm1Data) {
        return {
          ...netSalesData,
          datasets: roundDatasetValues(styleDatasetsByLabel(netSalesData.datasets || [])),
        };
      }

      const labels = netSalesData.labels;

      const netSalesDatasets = styleDatasetsByLabel(netSalesData.datasets || []);
      const cm1Datasets = (cm1Data.datasets || []).map((ds: any) => ({
        ...ds,
        fill: false,
        borderDash: [6, 6],
        tension: 0.35,
      }));

      return {
        ...netSalesData,
        labels,
        datasets: roundDatasetValues([...netSalesDatasets, ...cm1Datasets]),
      };
    }

    if (tab === "units") {
      const unitsData = chartDataList[1];
      if (!unitsData) return null;

      return {
        ...unitsData,
        datasets: styleDatasetsByLabel(unitsData.datasets || []),
      };
    }

    return null;
  };

  const getTitleByTabLocal = (tab: TrendTab) =>
    tab === "sales_cm1" ? "Net Sales + CM1 Profit Trend" : "Units Trend";


  const renderChartToImage = async (data: any, options: any, w = 900, h = 360) => {
    const canvas = document.createElement("canvas");
    canvas.width = w;
    canvas.height = h;

    const ctx = canvas.getContext("2d");
    if (!ctx) return null;

    const chart = new ChartJSCore(ctx as any, {
      type: "line",
      data,
      options: {
        ...options,
        responsive: false,
        animation: false,
      },
    });

    chart.update();

    // ✅ IMPORTANT: put WHITE BEHIND what Chart.js already drew
    ctx.save();
    ctx.globalCompositeOperation = "destination-over";
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, w, h);
    ctx.restore();

    // ✅ Export JPEG (no transparency issues in Excel)
    const imageDataUrl = canvas.toDataURL("image/jpeg", 1.0);

    chart.destroy();
    return imageDataUrl;
  };


  const handleDownload = async () => {
    try {
      if (!chartDataList) return;

      // Build both tabs' datasets (does NOT depend on activeTab)
      const salesCm1Data = buildChartDataForTab("sales_cm1");
      const unitsData = buildChartDataForTab("units");
      if (!salesCm1Data && !unitsData) return;

      const opts = processedChartOptions;

      const EXPORT_W = 900;
      const EXPORT_H = 360;

      const [salesImg, unitsImg] = await Promise.all([
        salesCm1Data ? renderChartToImage(salesCm1Data, opts, EXPORT_W, EXPORT_H) : Promise.resolve(null),
        unitsData ? renderChartToImage(unitsData, opts, EXPORT_W, EXPORT_H) : Promise.resolve(null),
      ]);


      const labels: string[] = (processedChartData as any)?.labels || [];
      const datasets: any[] = (processedChartData as any)?.datasets || [];

      const table =
        labels.length && datasets.length
          ? {
            headers: ["Month", ...datasets.map((d) => d.label)],
            rows: labels.map((m, i) => [
              m,
              ...datasets.map((d) => d.data?.[i] ?? null),
            ]),
          }
          : undefined;

      await exportProductwiseTrendsExcel({
        filename: `${productname}-${title}.xlsx`,

        // Header block (from parent)
        titleLine: exportMeta?.titleLine || `${productname} - ${title}`,
        titleCountry: exportMeta?.titleCountry || "Global",
        platformLabel: exportMeta?.platformLabel || "Amazon",
        periodLabel: exportMeta?.periodLabel || title,
        companyName: exportMeta?.companyName || "",
        brandName: exportMeta?.brandName || "",
        currencyLabel: exportMeta?.currencyLabel || "",

        // ✅ NEW: cards data for Sheet 1 (you must support this in helper)
        countryCards: exportCountryCards || [],

        // Sheet 2 images
        salesCm1ChartBase64: salesImg,
        unitsChartBase64: unitsImg,

        chartWidth: EXPORT_W,
        chartHeight: EXPORT_H,
      });
    } catch (err) {
      console.error("Failed to export Excel with header + both charts", err);
    }
  };

  return (
    <div className="w-full rounded-xl border border-slate-200 bg-white p-4 sm:p-5 shadow-sm">
      {/* TOP SECTION */}
      <div className="flex flex-col gap-3">
        {/* ✅ MOBILE/TABLET (below md) */}
        <div className="flex flex-col gap-3 md:hidden">
          {/* Row 1: Heading + Download */}
          <div className="flex items-center justify-between gap-3">
            <div className="flex items-center gap-2">
              <PageBreadcrumb
                pageTitle="Product Name -"
                variant="page"
                textSize="2xl"
              />

              <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-semibold text-[#5EA68E]">
                {productname || "Select a product"}
              </span>
            </div>

            <DownloadIconButton onClick={handleDownload} />
          </div>

          {/* ✅ Row 2: Search dropdown (always visible in mobile/tablet) */}
          <div className="w-full">
            <ProductSearchDropdown
              authToken={authToken}
              onProductSelect={onProductSelect}
            />
          </div>

          {/* Row 3: Toggle + AI */}
          <div className="flex items-center justify-between gap-3">
            <SegmentedToggle<TrendTab>
              value={activeTab}
              onChange={setActiveTab}
              textSizeClass="text-xs sm:text-sm"
              className="w-auto"
              options={[
                { value: "sales_cm1", label: "Sales & CM1 Profit" },
                { value: "units", label: "Units" },
              ]}
            />

            {onViewBusinessInsights && (
              <AiButton
                onClick={handleAiInsightsClick}
                disabled={insightsLoading}
                loading={insightsLoading || isPreviewMode}
              >
                {insightsLoading ? "Generating..." : "AI Insights"}
              </AiButton>
            )}
          </div>
        </div>

        {/* ✅ DESKTOP (md and above) — keep as before */}
        <div className="hidden md:flex md:flex-row md:items-start md:justify-between md:gap-3">
          {/* LEFT: Title + country toggles */}
          <div className="flex-1">
            <div className="flex items-center gap-2">
              <PageBreadcrumb
                pageTitle="Product Name -"
                variant="page"
                textSize="2xl"
              />

              <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-semibold text-[#5EA68E]">
                {productname || "Select a product"}
              </span>
            </div>


          </div>

          {/* RIGHT: Search + Toggle + AI + Download */}
          <div className="flex w-full flex-col gap-2 sm:w-auto sm:flex-row sm:items-center sm:gap-3">
            <div className="w-full lg:w-72">
              <ProductSearchDropdown
                authToken={authToken}
                onProductSelect={onProductSelect}
              />
            </div>

            <div className="w-full sm:w-auto">
              <SegmentedToggle<TrendTab>
                value={activeTab}
                onChange={setActiveTab}
                textSizeClass="text-xs sm:text-sm"
                className="w-full sm:w-auto"
                options={[
                  { value: "sales_cm1", label: "Sales & CM1 Profit" },
                  { value: "units", label: "Units" },
                ]}
              />
            </div>

            {onViewBusinessInsights && (
              <AiButton
                onClick={handleAiInsightsClick}
                disabled={insightsLoading}
                loading={insightsLoading || isPreviewMode}
              >
                {insightsLoading ? "Generating..." : "AI Insights"}
              </AiButton>
            )}

            <DownloadIconButton onClick={handleDownload} />
          </div>
        </div>

        <div className="my-4 flex flex-wrap items-center justify-center gap-3">
          {nonEmptyCountriesFromApi.map((country) => {
            const normalized = normalizeCountryKey(country);
            const color = getCountryColor(normalized);
            const isChecked = selectedCountries[country as CountryKey] ?? true;
            const label = formatCountryLabel(normalized);

            return (
              <label
                key={country}
                className={[
                  "shrink-0",
                  "flex items-center gap-1 sm:gap-1.5",
                  "font-semibold select-none whitespace-nowrap",
                  "text-[9px] sm:text-[10px] md:text-[11px] lg:text-xs xl:text-sm",
                  "text-charcoal-500",
                  isChecked ? "opacity-100" : "opacity-40",
                  "cursor-pointer",
                ].join(" ")}
                onClick={() => onToggleCountry(country as CountryKey)}
              >
                <span
                  className="flex items-center justify-center h-3 w-3 sm:h-3.5 sm:w-3.5 rounded-sm border transition"
                  style={{
                    borderColor: color,
                    backgroundColor: isChecked ? color : "white",
                  }}
                >
                  {isChecked && (
                    <svg
                      viewBox="0 0 24 24"
                      width="14"
                      height="14"
                      className="text-white"
                    >
                      <path
                        fill="currentColor"
                        d="M20.285 6.709a1 1 0 0 0-1.414-1.414L9 15.168l-3.879-3.88a1 1 0 0 0-1.414 1.415l4.586 4.586a1 1 0 0 0 1.414 0l10-10Z"
                      />
                    </svg>
                  )}
                </span>

                <span className="text-charcoal-500">{label}</span>
              </label>
            );
          })}
        </div>
      </div>


      {/* CHART AREA */}
      <div className="mt-4 h-[260px] sm:h-[300px] md:h-[340px] lg:h-[380px] xl:h-[420px]">
        {processedChartData ? (
          <Line
            ref={chartRef}
            data={processedChartData as any}
            options={processedChartOptions as any}
            style={{ width: "100%", height: "100%" }}
          />
        ) : (
          <p className="flex h-full items-center justify-center">
            No chart data available.
          </p>
        )}
      </div>

      {/* BOTTOM: centered legend + download */}
      <div className="mt-4 flex items-center justify-between gap-3">
        <div className="flex-1 flex justify-center">
          {activeTab === "sales_cm1" ? (
            <div className="flex flex-wrap justify-center items-center gap-4 
                text-[10px] sm:text-xs md:text-sm lg:text-base 
                font-semibold text-charcoal-500">

              <div className="flex items-center gap-2">
                <span className="h-[2px] w-10 bg-charcoal-500" />
                <span>Net Sales</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="w-10 border-t border-dashed border-charcoal-500" />
                <span>CM1 Profit</span>
              </div>
            </div>
          ) : (
            <div className="flex items-center gap-2 
                text-[10px] sm:text-xs md:text-sm 
                text-charcoal-500">

              <span className="h-[2px] w-10 bg-gray-800" />
              <span>Units</span>
            </div>
          )}
        </div>

        {/* <div className="flex items-center gap-2">
          <DownloadIconButton onClick={handleDownload} />
        </div> */}
      </div>
    </div>
  );
};

export default TrendChartSection;
