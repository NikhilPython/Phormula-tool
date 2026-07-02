"use client";

import React, { useEffect, useMemo, useState } from "react";
import "@/lib/chartSetup";
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  LineElement,
  PointElement,
  Title as ChartTitle,
  Tooltip,
  Legend,
  type Chart as ChartInstance,
} from "chart.js";
import ExcelJS from "exceljs";
import { saveAs } from "file-saver";
import Loader from "@/components/loader/Loader";
import { ProfitChartExportApi } from "@/lib/utils/exportTypes";
import { getMetricLabel, type MetricKey } from "@/lib/constants/metricLabels";

ChartJS.register(
  CategoryScale,
  LinearScale,
  LineElement,
  PointElement,
  ChartTitle,
  Tooltip,
  Legend
);

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

  profit?: number;
  cm2_profit: number;
  cm2_profit_total?: number;

  total_profit: number;
  total_net_credits?: number;
};

type GraphPageProps = {
  range: "monthly" | "quarterly" | "yearly";
  selectedMonth?: string;
  selectedQuarter?: "Q1" | "Q2" | "Q3" | "Q4";
  selectedYear: number | string;
  countryName: string;
  homeCurrency?: string;
  uploads: UploadRow[];
  loading: boolean;
  userMeta: { company_name?: string; brand_name?: string } | null;
  error?: string | null;
  onNoDataChange?: (noData: boolean) => void;
  onExportApiReady?: (api: ProfitChartExportApi) => void;
  hideDownloadButton?: boolean;
  isCollapsed: boolean;
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
    case "cad":
      return "C$";
    default:
      return "¤";
  }
};

const GraphPage: React.FC<GraphPageProps> = ({
  range,
  selectedMonth,
  selectedQuarter,
  selectedYear,
  countryName,
  homeCurrency,
  uploads,
  loading,
  userMeta,
  error,
  onNoDataChange,
  onExportApiReady,
  hideDownloadButton,
  isCollapsed = false,
  isPreviewMode = false,
}) => {
  const isGlobalPage = (countryName || "").toLowerCase() === "global";
  const normalizedHomeCurrency = (homeCurrency || "").trim().toLowerCase();

  const currencySymbol = isGlobalPage
    ? getCurrencySymbol(normalizedHomeCurrency || "usd")
    : getCurrencySymbol(countryName || "");

  const [allValuesZero, setAllValuesZero] = useState(false);
  const [showModal, setShowModal] = useState(false);

  const [selectedGraphs, setSelectedGraphs] = useState<Record<string, boolean>>({
    sales: true,
    total_cous: false,
    AmazonExpense: true,
    taxncredit: false,
    profit2: true,
    advertisingCosts: true,
    Other: false,
    profit: true,
  });

  // ✅ Chart instance ref (for embedding chart image into Excel)
  const chartRef = React.useRef<ChartInstance<"line"> | null>(null);

  const getChartBase64 = () => {
    const chart = chartRef.current;
    if (!chart) return null;
    return chart.toBase64Image("image/png", 1);
  };

  // const generateDummyData = (labels: string[]) => {
  //   const dummyMetrics: Record<string, number[]> = {
  //     sales: labels.map((_, i) => 15000 + Math.random() * 5000 + i * 1000),
  //     AmazonExpense: labels.map((_, i) => 3000 + Math.random() * 1000 + i * 200),
  //     total_cous: labels.map((_, i) => 8000 + Math.random() * 2000 + i * 500),
  //     advertisingCosts: labels.map((_, i) => 2000 + Math.random() * 800 + i * 150),
  //     Other: labels.map((_, i) => 1000 + Math.random() * 500 + i * 100),
  //     taxncredit: labels.map((_, i) => 500 + Math.random() * 300 + i * 50),
  //     profit: labels.map((_, i) => 1500 + Math.random() * 800 + i * 200),
  //     profit2: labels.map((_, i) => 2000 + Math.random() * 1000 + i * 250),
  //   };
  //   return dummyMetrics;
  // };

  const getQuarterLabels = (year: number | string, quarter: "Q1" | "Q2" | "Q3" | "Q4") => {
    const qMap: Record<string, string[]> = {
      Q1: ["january", "february", "march"],
      Q2: ["april", "may", "june"],
      Q3: ["july", "august", "september"],
      Q4: ["october", "november", "december"],
    };
    return qMap[quarter]?.map((m) => `${m} ${year}`) ?? [];
  };

  const capitalizeFirstLetter = (str: string) =>
    str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();

  const convertToAbbreviatedMonth = (m?: string) =>
    m ? capitalizeFirstLetter(m).slice(0, 3) : "";

  const MONTH_NAME_TO_IDX: Record<string, number> = {
    january: 0,
    february: 1,
    march: 2,
    april: 3,
    may: 4,
    june: 5,
    july: 6,
    august: 7,
    september: 8,
    october: 9,
    november: 10,
    december: 11,
  };

  const shortLabel = (key: MetricKey) => getMetricLabel(key, true);
  const fullLabel = (key: MetricKey) => getMetricLabel(key, false);

  const selectedYearNum = Number(selectedYear);
  const now = new Date();
  const currentCalendarYear = now.getFullYear();
  const currentCalendarMonthIdx = now.getMonth();

  const lastCompletedMonthIdx = useMemo(() => {
    if (range !== "yearly") return null;
    if (!selectedYearNum || selectedYearNum !== currentCalendarYear) return null;

    // Current month should not be shown in historic/yearly view.
    // Example: if today is July, show only Jan-Jun.
    return currentCalendarMonthIdx - 1;
  }, [range, selectedYearNum, currentCalendarYear, currentCalendarMonthIdx]);

  const monthlyLabels = useMemo(() => {
    if (range === "monthly" && selectedMonth && selectedYear) {
      return [`${selectedMonth} ${selectedYear}`.toLowerCase()];
    }
    if (range === "quarterly" && selectedQuarter && selectedYear) {
      return getQuarterLabels(selectedYear, selectedQuarter).map((l) => l.toLowerCase());
    }
    if (range === "yearly" && selectedYear) {
      const yearLabels = [
        `January ${selectedYear}`,
        `February ${selectedYear}`,
        `March ${selectedYear}`,
        `April ${selectedYear}`,
        `May ${selectedYear}`,
        `June ${selectedYear}`,
        `July ${selectedYear}`,
        `August ${selectedYear}`,
        `September ${selectedYear}`,
        `October ${selectedYear}`,
        `November ${selectedYear}`,
        `December ${selectedYear}`,
      ].map((l) => l.toLowerCase());

      if (lastCompletedMonthIdx != null) {
        if (lastCompletedMonthIdx < 0) return [];
        return yearLabels.slice(0, lastCompletedMonthIdx + 1);
      }

      return yearLabels;

      // Expanded view:
      // keep Jan-Dec axis.
      return yearLabels;
    }
    return [] as string[];
  }, [
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    isCollapsed,
    lastCompletedMonthIdx,
  ]);



  const processData = () => {
    if ((!uploads || uploads.length === 0) && !isPreviewMode) {
      return { labels: [] as string[], datasets: [] as any[], isAllZero: false };
    }

    const monthSums: Record<
      string,
      {
        sales: number;
        AmazonExpense: number;
        taxncredit: number;
        total_cous: number;
        advertisingCosts: number;
        Other: number;
        profit: number;
        profit2: number;
      }
    > = {};

    uploads.forEach((upload) => {
      const key = `${String(upload.month).toLowerCase()} ${upload.year}`;
      if (!monthSums[key]) {
        monthSums[key] = {
          sales: 0,
          AmazonExpense: 0,
          taxncredit: 0,
          total_cous: 0,
          advertisingCosts: 0,
          Other: 0,
          profit: 0,
          profit2: 0,
        };
      }

      monthSums[key].sales += Number(upload.total_sales || 0);
      monthSums[key].AmazonExpense += Number(upload.total_amazon_fee || 0);
      monthSums[key].total_cous += Number(upload.total_cous || 0);
      monthSums[key].advertisingCosts += Math.abs(
        Number(upload.advertising_total_final ?? upload.advertising_total ?? 0)
      );

      monthSums[key].Other += Math.abs(Number(upload.otherwplatform || 0));
      monthSums[key].taxncredit += Number(upload.taxncredit || 0);

      // CM2 Profit
      monthSums[key].profit += Number(
        upload.cm2_profit_total ?? upload.cm2_profit ?? 0
      );

      // CM1 Profit
      monthSums[key].profit2 += Number(
        upload.profit ?? upload.total_profit ?? 0
      );
    });

    const labels = monthlyLabels;

    if (labels.length > 0) {
      const allDataValues: number[] = [];
      Object.entries(selectedGraphs)
        .filter(([, checked]) => checked)
        .forEach(([metric]) => {
          const vals = labels.map((l) => monthSums[l]?.[metric as keyof (typeof monthSums)[string]] || 0);
          allDataValues.push(...vals);
        });

      const isAllZero = !allDataValues.some((v) => Math.abs(v) > 0.01);

      // let dataToUse = monthSums;

      const dataToUse = monthSums;

      // if (isAllZero) {
      //   const dummy = generateDummyData(labels);
      //   dataToUse = {} as any;
      //   labels.forEach((l, idx) => {
      //     (dataToUse as any)[l] = {
      //       sales: dummy.sales[idx],
      //       AmazonExpense: dummy.AmazonExpense[idx],
      //       taxncredit: dummy.taxncredit[idx],
      //       total_cous: dummy.total_cous[idx],
      //       advertisingCosts: dummy.advertisingCosts[idx],
      //       Other: dummy.Other[idx],
      //       profit: dummy.profit[idx],
      //       profit2: dummy.profit2[idx],
      //     };
      //   });
      // }

      const colorMap: Record<string, string> = {
        sales: "#75BBDA",
        AmazonExpense: "#B75A5A",
        taxncredit: "#ED9F50",
        total_cous: "#FDD36F",
        profit: "#B8C78C",
        advertisingCosts: "#C49466",
        Other: "#3A8EA4",
        profit2: "#7B9A6D",
      };

      const datasets = Object.entries(selectedGraphs)
        .filter(([, checked]) => checked)
        .map(([metric]) => ({
          metricKey: metric as MetricKey,
          label: getMetricLabel(metric as MetricKey, isCollapsed),
          data: labels.map((l) => {
            if (isPreviewMode) return 0;

            const [monthName] = l.split(" ");
            const monthIdx = MONTH_NAME_TO_IDX[String(monthName).toLowerCase()];

            const isCurrentOngoingYear =
              range === "yearly" &&
              selectedYearNum === currentCalendarYear &&
              lastCompletedMonthIdx != null;

            const isCurrentOrFutureMonth =
              isCurrentOngoingYear &&
              monthIdx != null &&
              monthIdx > lastCompletedMonthIdx;

            if (isCurrentOrFutureMonth) {
              return null;
            }

            return (dataToUse as any)[l]?.[metric] ?? 0;
          }),
          spanGaps: false,
          fill: false,
          borderColor: colorMap[metric] ?? "#000",
          backgroundColor: colorMap[metric] ?? "#000",
          tension: 0.35,
          cubicInterpolationMode: "monotone" as const,
          pointRadius: 3,
          pointHoverRadius: 5,
        }));

      return { labels, datasets, isAllZero };
    }

    return { labels: [], datasets: [], isAllZero: false };
  };



  const { labels: rawLabels, datasets, isAllZero } = useMemo(processData, [
    uploads,
    selectedGraphs,
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    monthlyLabels,
    isCollapsed,
    isPreviewMode,
  ]);

  useEffect(() => {
    setAllValuesZero(isAllZero);
    onNoDataChange?.(!isPreviewMode && isAllZero);
  }, [isAllZero, onNoDataChange, isPreviewMode]);

  const formattedLabels = useMemo(() => {
    return rawLabels.map((label) => {
      const [m] = label.trim().split(" ");
      return convertToAbbreviatedMonth(m);
    });
  }, [rawLabels]);

  const allDataPoints = datasets
    .flatMap((d: any) => d.data as Array<number | null>)
    .filter((v): v is number => typeof v === "number" && Number.isFinite(v));

  const minValue = allDataPoints.length ? Math.min(...allDataPoints) : 0;
  const minY = minValue < 0 ? Math.floor(minValue * 1.1) : 0;

  const periodInfo = useMemo(() => {
    if (range === "monthly" && selectedMonth) {
      return `${convertToAbbreviatedMonth(selectedMonth)}'${String(selectedYear).slice(-2)}`;
    }
    if (range === "quarterly" && selectedQuarter) {
      return `${selectedQuarter}'${String(selectedYear).slice(-2)}`;
    }
    return `Year'${String(selectedYear).slice(-2)}`;
  }, [range, selectedMonth, selectedQuarter, selectedYear]);

  const getExtraRows = () => {
    const formattedCountry = isGlobalPage ? "GLOBAL" : (countryName || "").toUpperCase();
    return [
      [`${userMeta?.brand_name || "N/A"}`],
      [`${userMeta?.company_name || "N/A"}`],
      [`Profit Breakup (SKU Level) - ${periodInfo}`],
      [`Currency:  ${currencySymbol}`],
      [`Country: ${formattedCountry}`],
      [`Platform: Amazon`],
    ];
  };

  const exportToExcel = async () => {
    try {
      const wb = new ExcelJS.Workbook();
      const ws = wb.addWorksheet("Sales Data");

      const extraRows = getExtraRows();
      extraRows.forEach((r) => ws.addRow(r));
      ws.addRow([""]);

      const monthSums: Record<string, any> = {};
      uploads.forEach((upload) => {
        const key = `${String(upload.month).toLowerCase()} ${upload.year}`;
        if (!monthSums[key]) {
          monthSums[key] = {
            sales: 0,
            AmazonExpense: 0,
            total_cous: 0,
            advertisingCosts: 0,
            Other: 0,
            net_credits: 0,
            taxncredit: 0,
            profit: 0,
            profit2: 0,
          };
        }
        monthSums[key].sales += Number(upload.total_sales || 0);
        monthSums[key].total_cous += Number(upload.total_cous || 0);
        monthSums[key].AmazonExpense += Number(upload.total_amazon_fee || 0);
        monthSums[key].taxncredit += Number(upload.taxncredit || 0);
        monthSums[key].net_credits += Number(upload.total_net_credits || 0);
        monthSums[key].profit2 += Number(
          upload.profit ?? upload.total_profit ?? 0
        );

        monthSums[key].advertisingCosts += Math.abs(
          Number(upload.advertising_total_final ?? upload.advertising_total ?? 0)
        );

        monthSums[key].Other += Number(upload.otherwplatform || 0);

        monthSums[key].profit += Number(
          upload.cm2_profit_total ?? upload.cm2_profit ?? 0
        );
      });

      const fixedOrder = [
        { key: "sales", label: "Sales", sign: "(+)" },
        { key: "total_cous", label: "COGS", sign: "(-)" },
        { key: "AmazonExpense", label: "Amazon Fees", sign: "(-)" },
        { key: "taxncredit", label: "Taxes & Credits", sign: "(+)" },
        { key: "profit2", label: "CM1 Profit", sign: "" },
        { key: "advertisingCosts", label: "Advertising Costs", sign: "(-)" },
        { key: "Other", label: "Others", sign: "(-)" },
        { key: "profit", label: "CM2 Profit", sign: "" },
      ];

      const labelsNorm = rawLabels.map((l) => {
        const [m, y] = l.split(" ");
        const mm = convertToAbbreviatedMonth(m);
        const yy = (y ?? "").slice(-2);
        return `${mm}'${yy}`;
      });

      ws.addRow(["Month", ...fixedOrder.map((i) => i.label)]);
      ws.addRow([" ", ...fixedOrder.map((i) => i.sign)]);

      rawLabels.forEach((raw, idx) => {
        const display = labelsNorm[idx];
        const key = raw.toLowerCase();
        const row = [
          display,
          ...fixedOrder.map(({ key: k }) => Number((monthSums[key]?.[k] ?? 0).toFixed(2))),
        ];
        ws.addRow(row);
      });

      const totalRow: (string | number)[] = ["Total"];
      fixedOrder.forEach(({ key }) => {
        let sum = 0;
        rawLabels.forEach((raw) => {
          const k = raw.toLowerCase();
          sum += monthSums[k]?.[key] || 0;
        });
        totalRow.push(Number(sum.toFixed(2)));
      });
      ws.addRow(totalRow);

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
      const blob = new Blob([buffer], {
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      });
      saveAs(blob, `Metrics-${periodInfo}.xlsx`);
    } catch (err) {
      console.error("Excel export failed:", err);
    }
  };

  const toggleMetric = (name: string) => {
    const selectedCount = Object.values(selectedGraphs).filter(Boolean).length;
    const isChecked = !!selectedGraphs[name];

    if (isChecked && selectedCount === 1) {
      setShowModal(true);
      return;
    }
    setSelectedGraphs((prev) => ({ ...prev, [name]: !isChecked }));
  };

  useEffect(() => {
    onExportApiReady?.({
      getChartBase64,
      title: `Profitability - ${periodInfo}`,
      currencySymbol,
    });
  }, [onExportApiReady, periodInfo, currencySymbol]);

  const shouldRenderChart = rawLabels.length > 0 && datasets.length > 0;

  return (
    <div className="w-full h-full min-h-0 overflow-hidden flex flex-col">
      {loading ? (
        <div className="flex-1 min-h-0 flex items-center justify-center">
          <Loader fullscreen transparent />
        </div>
      ) : (
        <div className="flex flex-col h-full min-h-0">
          {/* Metric toggles */}
          <div
            data-no-expand
            className={[
              "shrink-0",
              "flex flex-wrap items-center justify-center",
              "gap-4",
              "w-full",
              allValuesZero ? "opacity-30" : "opacity-100",
              "transition-opacity duration-300",
            ].join(" ")}
          >
            {[
              { name: "sales", color: "#75BBDA" },
              { name: "total_cous", color: "#FDD36F" },
              { name: "AmazonExpense", color: "#B75A5A" },
              { name: "taxncredit", color: "#ED9F50" },
              { name: "profit2", color: "#7B9A6D" },
              { name: "advertisingCosts", color: "#C49466" },
              { name: "Other", color: "#3A8EA4" },
              { name: "profit", color: "#B8C78C" },
            ].map(({ name, color }) => {
              const isChecked = !!selectedGraphs[name];

              return (
                <label
                  key={name}
                  data-no-expand
                  onClick={(e) => e.stopPropagation()}
                  onMouseDown={(e) => e.stopPropagation()}
                  className={[
                    "shrink-0",
                    "flex items-center gap-1 sm:gap-1.5",
                    "font-semibold select-none whitespace-nowrap",
                    "text-[10px] 2xl:text-xs my-1 2xl:my-3",
                    "text-charcoal-500",
                    "cursor-pointer",
                  ].join(" ")}
                >
                  <span
                    data-no-expand
                    className="flex items-center justify-center h-3 w-3 sm:h-3.5 sm:w-3.5 rounded-sm border transition"
                    style={{
                      borderColor: color,
                      backgroundColor: isChecked ? color : "white",
                      opacity: allValuesZero ? 0.6 : 1,
                    }}
                    onMouseDown={(e) => e.stopPropagation()}
                    onClick={(e) => {
                      e.stopPropagation();
                      toggleMetric(name);
                    }}
                  >
                    {isChecked && (
                      <svg viewBox="0 0 24 24" width="14" height="14" className="text-white">
                        <path
                          fill="currentColor"
                          d="M20.285 6.709a1 1 0 0 0-1.414-1.414L9 15.168l-3.879-3.88a1 1 0 0 0-1.414 1.415l4.586 4.586a1 1 0 0 0 1.414 0l10-10Z"
                        />
                      </svg>
                    )}
                  </span>

                  <span className="capitalize">{getMetricLabel(name as MetricKey, isCollapsed)}</span>
                </label>
              );
            })}
          </div>

          <div className="flex-1 min-h-0 w-full pt-4">
            {shouldRenderChart ? (
              <Line
                ref={(instance) => {
                  chartRef.current = (instance as any) ?? null;
                }}
                data={{ labels: formattedLabels, datasets }}
                options={{
                  responsive: true,
                  maintainAspectRatio: false,
                  layout: { padding: 0 },
                  interaction: {
                    intersect: false,
                    mode: "index",
                  },
                  plugins: {
                    tooltip: {
                      enabled: true,
                      mode: "index",
                      intersect: false,
                      callbacks: {
                        label: (tooltipItem: any) => {
                          const metricKey = (tooltipItem.dataset as any).metricKey as MetricKey | undefined;
                          const displayLabel = metricKey
                            ? fullLabel(metricKey)
                            : (tooltipItem.dataset.label as string) || "";
                          const value = tooltipItem.raw as number;
                          return `${displayLabel}: ${currencySymbol} ${Math.round(value).toLocaleString(undefined, {
                            maximumFractionDigits: 0,
                          })}`;
                        },
                      },
                    },
                    legend: { display: false },
                  },
                  scales: {
                    x: {
                      grid: { display: false },
                      border: { display: false },
                      ticks: {
                        color: "#6B7280",
                        minRotation: 0,
                        maxRotation: 0,
                        autoSkip: formattedLabels.length > 6,
                        maxTicksLimit: formattedLabels.length > 0 ? formattedLabels.length : 12,
                        callback: (_v, idx) => String(formattedLabels[idx] ?? ""),
                      },
                    },
                    y: {
                      title: {
                        display: true,
                        text: `(${currencySymbol})`,
                        color: "#6B7280",
                      },
                      min: minY,
                      ticks: {
                        padding: 0,
                        color: "#6B7280",
                      },
                      border: { display: false },
                      grid: {
                        color: "#E5E7EB",
                      },
                    },
                  },
                }}
                style={{ width: "100%", height: "100%" }}
              />
            ) : (
              <div className="flex h-full items-center justify-center text-gray-400 text-sm">
                No data available
              </div>
            )}
          </div>

          {/* Optional: show parent error */}
          {error && <p className="shrink-0 mt-2 text-center text-sm text-red-600">Error: {error}</p>}

          {/* Optional: keep export button if you want */}
          {!hideDownloadButton && (
            <div className="shrink-0 flex justify-end mt-2">
              <button
                type="button"
                className="px-3 py-2 rounded-md border border-gray-300 text-sm"
                onClick={exportToExcel}
              >
                Download Excel
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default GraphPage;
