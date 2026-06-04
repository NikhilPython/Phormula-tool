"use client";

import React, { useEffect, useMemo, useState } from "react";
import { Bar } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title as ChartTitle,
  Tooltip,
  Legend,
  PointElement,
  type Chart as ChartInstance,
  type ChartData,
  type ChartOptions,
  type TooltipItem,
} from "chart.js";
import ExcelJS from "exceljs";
import { saveAs } from "file-saver";
import Loader from "@/components/loader/Loader";
import DownloadIconButton from "../ui/button/DownloadIconButton";
import { ProfitChartExportApi } from "@/lib/utils/exportTypes";

const axisTextColor = "#6B7280";
const axisGridColor = "#E5E7EB";
const axisBorderColor = "#D1D5DB";

const hoverPopPlugin = {
  id: "hoverPopPlugin",
  afterDatasetsDraw(chart: any) {
    const active = chart.getActiveElements?.() || [];
    if (!active.length) return;

    const { datasetIndex, index } = active[0];
    const meta = chart.getDatasetMeta(datasetIndex);
    const bar = meta?.data?.[index];
    if (!bar) return;

    const ctx = chart.ctx;

    const props = bar.getProps(["x", "y", "base", "width", "height"], true);

    const x = props.x;
    const y = props.y;
    const base = props.base;
    const w = props.width;

    const popW = w * 1.18;
    const left = x - popW / 2;

    const top = Math.min(y, base);
    const height = Math.abs(base - y);

    ctx.save();

    const bg = chart.data.datasets?.[datasetIndex]?.backgroundColor;
    const fill = Array.isArray(bg) ? bg[index] : bg || "rgba(0,0,0,0.2)";

    ctx.fillStyle = fill as any;
    ctx.fillRect(left, top, popW, height);

    ctx.lineWidth = 2;
    ctx.strokeStyle = "rgba(255,255,255,0.6)";
    ctx.strokeRect(left, top, popW, height);

    ctx.restore();
  },
};

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  ChartTitle,
  Tooltip,
  Legend
);
ChartJS.register(hoverPopPlugin);

type UploadRow = {
  country: string;
  month: string;
  year: string | number;
  total_sales: number;
  total_amazon_fee: number;
  total_cous: number;

  advertising_total: number;
  advertising_total_final?: number;

  otherwplatform: number;
  taxncredit?: number;

  // CM1 Profit
  profit?: number;
  total_profit: number;

  // CM2 Profit
  cm2_profit: number;
  cm2_profit_total?: number;

  tacos?: number;
};

type BargraphProps = {
  range: "monthly" | "quarterly" | "yearly";
  selectedMonth: string;
  selectedYear: number | string;
  countryName: string;
  /** only used on global pages */
  homeCurrency?: string;

  /** ✅ NEW: parent-provided upload_history rows (already filtered for page context) */
  uploads: UploadRow[];

  /** ✅ NEW: parent-provided loading flag for uploads */
  loading?: boolean;

  /** ✅ NEW: parent-provided brand/company meta (used in Excel export header) */
  userMeta?: { company_name?: string; brand_name?: string } | null;

  onNoDataChange?: (noData: boolean) => void;
  onExportApiReady?: (api: ProfitChartExportApi) => void;
  hideDownloadButton?: boolean;
  isCollapsed?: boolean;
  isPreviewMode?: boolean;
};

const getCurrencySymbol = (codeOrCountry: string) => {
  switch ((codeOrCountry || "").toLowerCase()) {
    case "uk":
    case "gb":
    case "gbp":
      return "£";
    case "india":
    case "in":
    case "inr":
      return "₹";
    case "us":
    case "usa":
    case "usd":
      return "$";
    case "europe":
    case "eu":
    case "eur":
      return "€";
    default:
      return "¤";
  }
};

const Bargraph: React.FC<BargraphProps> = ({
  range,
  selectedMonth,
  selectedYear,
  countryName,
  homeCurrency,
  uploads,
  loading = false,
  userMeta,
  onNoDataChange,
  onExportApiReady,
  hideDownloadButton,
  isCollapsed = false,
  isPreviewMode = false,
}) => {
  const isGlobalPage = (countryName || "").toLowerCase() === "global";

  const normalizedHomeCurrency = (homeCurrency || "").toLowerCase();
  const effectiveCurrencyCode = isGlobalPage
    ? normalizedHomeCurrency || "usd"
    : countryName;
  const currencySymbol = getCurrencySymbol(effectiveCurrencyCode);

  // ✅ Use parent-provided rows
  const data = uploads || [];

  const [selectedGraphs, setSelectedGraphs] = useState({
    sales: true,
    profit: true,
    profit2: true,
    AmazonExpense: true,
    total_cous: true,
    sellingFees: true,
    advertisingCosts: true,
    Other: true,
    taxncredit: true,
  });

  const capitalizeFirstLetter = (str: string) =>
    str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
  const convertToAbbreviatedMonth = (m?: string) =>
    m ? capitalizeFirstLetter(m).slice(0, 3) : "";

  // ✅ Chart instance ref (for embedding chart image into Excel)
  const chartRef = React.useRef<ChartInstance<"bar"> | null>(null);

  const getChartBase64 = () => {
    const chart = chartRef.current;
    if (!chart) return null;
    return chart.toBase64Image("image/png", 1);
  };

  const handleCheckboxChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, checked } = e.target;
    const selectedCount = Object.values(selectedGraphs).filter(Boolean).length;
    const newSelectedCount = checked ? selectedCount + 1 : selectedCount - 1;
    if (!checked && newSelectedCount < 2) return;

    setSelectedGraphs((prev) => ({ ...prev, [name]: checked }));
  };

  const formattedMonthYear = useMemo(
    () =>
      `${convertToAbbreviatedMonth(selectedMonth)}'${String(selectedYear).slice(
        -2
      )}`,
    [selectedMonth, selectedYear]
  );

  useEffect(() => {
    onExportApiReady?.({
      getChartBase64,
      title: `Profitability - ${formattedMonthYear}`,
      currencySymbol,
    });
  }, [onExportApiReady, formattedMonthYear, currencySymbol]);

  const getExtraRows = () => {
    const formattedCountry = isGlobalPage ? "GLOBAL" : countryName?.toUpperCase();
    return [
      [`${userMeta?.brand_name || "N/A"}`],
      [`${userMeta?.company_name || "N/A"}`],
      [`Profit Breakup (SKU Level) - ${formattedMonthYear}`],
      [`Currency:  ${currencySymbol}`],
      [`Country: ${formattedCountry}`],
      [`Platform: Amazon`],
    ];
  };

  const metricMapping: Record<
    | "Net Sales"
    | "COGS"
    | "Amazon Fees"
    | "Taxes & Credits"
    | "CM1 Profit"
    | "Advertising Cost"
    | "Other"
    | "CM2 Profit",
    keyof UploadRow
  > = {
    "Net Sales": "total_sales",
    COGS: "total_cous",
    "Amazon Fees": "total_amazon_fee",
    "Taxes & Credits": "taxncredit",

    // CM1 Profit should map to profit
    "CM1 Profit": "profit",

    // Advertising Cost should map to advertising_total_final
    "Advertising Cost": "advertising_total_final",

    Other: "otherwplatform",

    // CM2 Profit should map to cm2_profit_total
    "CM2 Profit": "cm2_profit_total",
  };

  const colorMapping: Record<
    | "Net Sales"
    | "COGS"
    | "Amazon Fees"
    | "Taxes & Credits"
    | "CM1 Profit"
    | "Advertising Cost"
    | "Other"
    | "CM2 Profit",
    string
  > = {
    "Net Sales": "#75BBDA",
    COGS: "#FDD36F",
    "Amazon Fees": "#B75A5A",
    "Advertising Cost": "#C49466",
    Other: "#3A8EA4",
    "Taxes & Credits": "#ED9F50",
    "CM1 Profit": "#7B9A6D",
    "CM2 Profit": "#B8C78C",
  };

  const preferredOrder = [
    "Net Sales",
    "COGS",
    "Amazon Fees",
    "Taxes & Credits",
    "CM1 Profit",
    "Advertising Cost",
    "Other",
    "CM2 Profit",
  ] as const;

  const shortLabelMap: Record<(typeof preferredOrder)[number], string> = {
    "Net Sales": "Sales",
    COGS: "COGS",
    "Amazon Fees": "Fees",
    "Taxes & Credits": "Tax",
    "CM1 Profit": "CM1",
    "Advertising Cost": "Ads",
    Other: "Other",
    "CM2 Profit": "CM2",
  };

  const { chartData, chartOptions, exportToExcel, allValuesZero, metricsToShow, values } =
    useMemo(() => {
      if ((!data || data.length === 0) && !isPreviewMode) {
        return {
          chartData: { labels: [], datasets: [] } as ChartData<"bar">,
          chartOptions: {} as ChartOptions<"bar">,
          exportToExcel: async () => { },
          allValuesZero: true,
          metricsToShow: [] as (typeof preferredOrder)[number][],
          values: [] as number[],
        };
      }

      const selectedMonthYearKey = `${selectedMonth} ${selectedYear}`.toLowerCase();
      const monthData = data.find(
        (upload) =>
          `${upload.month} ${upload.year}`.toLowerCase() === selectedMonthYearKey
      );

      const computedMetricsToShow = (
        Object.entries(selectedGraphs)
          .filter(([, isChecked]) => isChecked)
          .map(([key]) => {
            switch (key) {
              case "sales":
                return "Net Sales";
              case "total_cous":
                return "COGS";
              case "AmazonExpense":
                return "Amazon Fees";
              case "taxncredit":
                return "Taxes & Credits";
              case "profit2":
                return "CM1 Profit";
              case "advertisingCosts":
                return "Advertising Cost";
              case "Other":
                return "Other";
              case "profit":
                return "CM2 Profit";
              default:
                return null;
            }
          })
          .filter(Boolean) as (typeof preferredOrder)[number][]
      ).sort((a, b) => preferredOrder.indexOf(a) - preferredOrder.indexOf(b));

      const fullLabels = computedMetricsToShow;

      const labels = computedMetricsToShow.map((k) =>
        isCollapsed ? shortLabelMap[k] : k
      );

      const viewportWidth = typeof window !== "undefined" ? window.innerWidth : 1200;
      const barWidthInPixels = viewportWidth * 0.05;

      let computedValues = computedMetricsToShow.map((label) => {
        if (isPreviewMode) return 0;
        if (!monthData) return 0;

        if (label === "CM1 Profit") {
          return Number(monthData.profit ?? monthData.total_profit ?? 0);
        }

        if (label === "Advertising Cost") {
          return Math.abs(
            Number(monthData.advertising_total_final ?? monthData.advertising_total ?? 0)
          );
        }

        if (label === "Other") {
          return Math.abs(Number(monthData.otherwplatform ?? 0));
        }

        if (label === "CM2 Profit") {
          return Number(monthData.cm2_profit_total ?? monthData.cm2_profit ?? 0);
        }

        const field = metricMapping[label];
        return Number(monthData?.[field] ?? 0);
      });

      const zero = computedValues.every((v) => v === 0);

      const chartData: ChartData<"bar"> = {
        labels,
        datasets: [
          {
            label: formattedMonthYear,
            data: computedValues,
            maxBarThickness: barWidthInPixels,
            backgroundColor: computedMetricsToShow.map((l) => colorMapping[l]),
            hoverBackgroundColor: computedMetricsToShow.map((l) => `${colorMapping[l]}4D`),
            hoverBorderWidth: 1,
            borderWidth: 0,
          },
        ],
      };

      const options: ChartOptions<"bar"> = {
        responsive: true,
        interaction: { mode: "index", intersect: true },
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            intersect: false,
            callbacks: {
              title: (items) => {
                const i = items[0]?.dataIndex ?? 0;
                return fullLabels[i] ?? "";
              },
              label: (context) => {
                const i = context.dataIndex;
                const value = Number(context.raw ?? 0);

                const salesIndex = fullLabels.findIndex((l) => l === "Net Sales");
                const salesValue =
                  salesIndex >= 0
                    ? Number((chartData.datasets[0].data as number[])[salesIndex] ?? 1)
                    : 1;

                const percentage = salesValue !== 0 ? (value / salesValue) * 100 : 0;
                const metricLabel = fullLabels[i] ?? "";

                const formattedValue = Math.round(Math.abs(value)).toLocaleString(undefined, {
                  maximumFractionDigits: 0,
                });

                const signedValue = `${value < 0 ? "-" : ""}${currencySymbol}${formattedValue}`;

                return `${metricLabel}: ${signedValue} (${percentage.toFixed(2)}%)`;
              },
            },
          },
        },
        scales: {
          x: {
            ticks: {
              color: axisTextColor,
              maxRotation: 0,
              minRotation: 0,
              autoSkip: false,
              padding: 8,
              callback: (_value, index) => {
                const raw = isCollapsed
                  ? String(labels[index] ?? "")
                  : String(fullLabels[index] ?? "");

                if (isCollapsed) return raw;

                const maxLineLength = 10;
                const words = raw.split(" ");
                const lines: string[] = [];
                let line = "";

                for (const w of words) {
                  const test = line ? `${line} ${w}` : w;
                  if (test.length > maxLineLength && line) {
                    lines.push(line);
                    line = w;
                  } else {
                    line = test;
                  }
                }

                if (line) lines.push(line);
                return lines;
              },
            },
            grid: {
              display: false,
              color: axisGridColor,
            },
            border: {
              color: axisBorderColor,
            },
            title: {
              display: false,
              color: axisTextColor,
            },
          },
          y: {
            ticks: {
              color: axisTextColor,
            },
            title: {
              display: true,
              text: `Amount (${currencySymbol})`,
              color: axisTextColor,
            },
            grid: {
              display: true,
              color: axisGridColor,
            },
            border: {
              color: axisBorderColor,
            },
          },
        },
      };

      const exportToExcel = async () => {
        try {
          const wb = new ExcelJS.Workbook();
          const ws = wb.addWorksheet("Sales Data");

          const extraRows = getExtraRows();
          extraRows.forEach((r) => ws.addRow(r));
          ws.addRow([""]);

          ws.addRow(["Metric", " ", `Amount (${currencySymbol})`]);

          const signs: Record<(typeof preferredOrder)[number], string> = {
            "Net Sales": "(+)",
            COGS: "(-)",
            "Amazon Fees": "(-)",
            "Taxes & Credits": "(+)",
            "CM1 Profit": "",
            "Advertising Cost": "(-)",
            Other: "(-)",
            "CM2 Profit": "",
          };

          computedValues.forEach((v, idx) => {
            const label = computedMetricsToShow[idx];
            ws.addRow([label, signs[label], Number(v.toFixed(2))]);
          });

          const totalValue = computedValues.reduce((acc, v) => acc + v, 0);
          ws.addRow(["Total", "", Number(totalValue.toFixed(2))]);

          const base64 = getChartBase64();
          if (base64) {
            const lastRowNumber = ws.lastRow?.number ?? 1;
            const imageStartRow = lastRowNumber + 2;

            const imageId = wb.addImage({ base64, extension: "png" });
            ws.addImage(imageId, {
              tl: { col: 0, row: imageStartRow - 1 },
              ext: { width: 900, height: 420 },
            });
          }

          const buffer = await wb.xlsx.writeBuffer();
          saveAs(
            new Blob([buffer], {
              type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            }),
            `Metrics-${formattedMonthYear}.xlsx`
          );
        } catch (err) {
          console.error("Excel export failed:", err);
        }
      };

      return {
        chartData,
        chartOptions: options,
        exportToExcel,
        allValuesZero: zero,
        metricsToShow: computedMetricsToShow,
        values: computedValues,
      };
    }, [
      data,
      selectedGraphs,
      selectedMonth,
      selectedYear,
      formattedMonthYear,
      currencySymbol,
      countryName,
      isGlobalPage,
      userMeta?.brand_name,
      userMeta?.company_name,
      isCollapsed,
      isPreviewMode,
    ]);

  useEffect(() => {
    onNoDataChange?.(!loading && !isPreviewMode && allValuesZero);
  }, [onNoDataChange, allValuesZero, loading, isPreviewMode]);

  const shouldShowNoData =
    !loading &&
    !isPreviewMode &&
    (
      allValuesZero ||
      !chartData.datasets.length ||
      !data.length
    );

  return (
    <div className="relative w-full h-full min-h-0">
      <div className="h-full min-h-0">
        {!hideDownloadButton && !shouldShowNoData && (
          <div className="flex justify-end mb-2">
            <DownloadIconButton onClick={exportToExcel} />
          </div>
        )}

        <div className="w-full h-full min-h-[260px]">
          {loading ? (
            <div className="flex h-full min-h-[260px] items-center justify-center">
              <Loader fullscreen transparent />
            </div>
          ) : shouldShowNoData ? (
            <div className="flex h-full min-h-[260px] items-center justify-center text-sm text-gray-400">
              No data available
            </div>
          ) : (
            <Bar
              ref={(instance) => {
                chartRef.current = (instance as any) ?? null;
              }}
              data={chartData}
              options={chartOptions}
            />
          )}
        </div>
      </div>
    </div>
  );
};

export default Bargraph;
