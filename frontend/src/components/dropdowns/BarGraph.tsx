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
import PageBreadcrumb from "../common/PageBreadCrumb";
import Loader from "@/components/loader/Loader";
import DownloadIconButton from "../ui/button/DownloadIconButton";
import { ProfitChartExportApi } from "@/lib/utils/exportTypes";

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

    const props = bar.getProps(
      ["x", "y", "base", "width", "height"],
      true
    );

    const x = props.x;
    const y = props.y;
    const base = props.base;
    const w = props.width;

    // Pop scale
    const popW = w * 1.18;
    const left = x - popW / 2;

    const top = Math.min(y, base);
    const height = Math.abs(base - y);

    ctx.save();

    const bg = chart.data.datasets?.[datasetIndex]?.backgroundColor;
    const fill =
      Array.isArray(bg) ? bg[index] : bg || "rgba(0,0,0,0.2)";

    // Draw enlarged bar
    ctx.fillStyle = fill as any;
    ctx.fillRect(left, top, popW, height);

    // Optional clean outline (no shadow)
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

type BargraphProps = {
  range: "monthly" | "quarterly" | "yearly";
  selectedMonth: string;
  selectedYear: number | string;
  countryName: string;
  /** only used on global pages */
  homeCurrency?: string;
  onNoDataChange?: (noData: boolean) => void;
  onExportApiReady?: (api: ProfitChartExportApi) => void;   // ✅ add
  hideDownloadButton?: boolean;
  isCollapsed?: boolean;
};

type UploadRow = {
  country: string;
  month: string;
  year: string | number;
  total_sales: number;
  total_amazon_fee: number;
  total_cous: number;
  advertising_total: number;
  otherwplatform: number;
  taxncredit?: number;
  cm2_profit: number;
  total_profit: number;
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
  onNoDataChange,
  onExportApiReady,
  hideDownloadButton,
  isCollapsed = false,
}) => {
  const isGlobalPage = (countryName || "").toLowerCase() === "global";

  const normalizedHomeCurrency = (homeCurrency || "").toLowerCase();
  const effectiveCurrencyCode = isGlobalPage
    ? normalizedHomeCurrency || "usd"
    : countryName;
  const currencySymbol = getCurrencySymbol(effectiveCurrencyCode);

  const [data, setData] = useState<UploadRow[]>([]);
  const [loading, setLoading] = useState<boolean>(false);

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

  const token =
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

  const [userData, setUserData] = useState<{
    company_name?: string;
    brand_name?: string;
  } | null>(null);

  // ✅ Chart instance ref (for embedding chart image into Excel)
  const chartRef = React.useRef<ChartInstance<"bar"> | null>(null);

  const getChartBase64 = () => {
    const chart = chartRef.current;
    if (!chart) return null;
    return chart.toBase64Image("image/png", 1);
  };

  // 🔹 Fetch upload history (homeCurrency only for GLOBAL)
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);

      try {
        const token =
          typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_history`);

        // ✅ Only send homeCurrency for GLOBAL
        if (isGlobalPage && homeCurrency) {
          url.searchParams.set("homeCurrency", normalizedHomeCurrency);
        }

        const response = await fetch(url.toString(), {
          method: "GET",
          headers: token ? { Authorization: `Bearer ${token}` } : {},
        });

        const result = (await response.json()) as { uploads?: UploadRow[] };

        if (result?.uploads) {
          const rows = result.uploads;

          const isUsd = normalizedHomeCurrency === "usd";

          const filtered = rows.filter((item) => {
            const itemCountry = (item.country || "").toLowerCase();

            // ✅ GLOBAL page filtering
            if (isGlobalPage) {
              // USD can be stored as "global" OR "global_usd"
              if (isUsd) {
                return itemCountry === "global" || itemCountry === "global_usd";
              }
              // Non-USD global currencies: "global_inr", "global_cad", ...
              return itemCountry === `global_${normalizedHomeCurrency}`;
            }

            // ✅ Non-global pages: "uk", "us", etc.
            return itemCountry === countryName.toLowerCase();
          });

          setData(filtered);
        } else {
          setData([]);
        }
      } catch (error) {
        console.error("Failed to fetch upload history:", error);
        setData([]);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [countryName, homeCurrency, normalizedHomeCurrency, isGlobalPage]);

  useEffect(() => {
    const fetchUser = async () => {
      if (!token) return;
      try {
        const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/get_user_data`, {
          method: "GET",
          headers: { Authorization: `Bearer ${token}` },
        });
        if (!response.ok) return;
        const j = (await response.json()) as {
          company_name?: string;
          brand_name?: string;
        };
        setUserData(j);
      } catch {
        // ignore
      }
    };
    fetchUser();
  }, [token]);

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
      [`${userData?.brand_name || "N/A"}`],
      [`${userData?.company_name || "N/A"}`],
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
    "CM1 Profit": "total_profit",
    "Advertising Cost": "advertising_total",
    Other: "otherwplatform",
    "CM2 Profit": "cm2_profit",
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
    "COGS": "COGS",
    "Amazon Fees": "Fees",
    "Taxes & Credits": "Tax",
    "CM1 Profit": "CM1",
    "Advertising Cost": "Ads",
    "Other": "Other",
    "CM2 Profit": "CM2",
  };

  const { chartData, chartOptions, exportToExcel, allValuesZero, metricsToShow, values } =
    useMemo(() => {
      if (!data || data.length === 0) {
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

      // const labels = computedMetricsToShow as string[];

      const fullLabels = computedMetricsToShow; // full names always

      const labels = computedMetricsToShow.map((k) =>
        isCollapsed ? shortLabelMap[k] : k
      );


      const viewportWidth = typeof window !== "undefined" ? window.innerWidth : 1200;
      const barWidthInPixels = viewportWidth * 0.05;

      let computedValues = computedMetricsToShow.map((label) => {
        const field = metricMapping[label];
        return monthData ? Math.abs(Number(monthData?.[field] ?? 0)) : 0;
      });

      const zero = computedValues.every((v) => v === 0);
      if (zero) computedValues = computedMetricsToShow.map(() => Math.floor(Math.random() * 1000 + 100));

      const chartData: ChartData<"bar"> = {
        labels,
        datasets: [
          {
            label: formattedMonthYear,
            data: computedValues,
            maxBarThickness: barWidthInPixels,

            backgroundColor: computedMetricsToShow.map((l) => colorMapping[l]),

            // ✅ hover styles
            hoverBackgroundColor: computedMetricsToShow.map(
              (l) => `${colorMapping[l]}4D`
            ),

            hoverBorderWidth: 1,

            borderWidth: 0,
          },
        ],

      };

      const options: ChartOptions<"bar"> = {
        responsive: true,
        interaction: {
          mode: "index",
          intersect: true,
        },
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },

          // tooltip: {
          //   intersect: false,
          //   callbacks: {
          //     title: (tooltipItems: TooltipItem<"bar">[]) =>
          //       tooltipItems[0]?.label ?? "",
          //     label: (context: TooltipItem<"bar">) => {
          //       const value = Number(context.raw ?? 0);
          //       const salesIndex = labels.findIndex((l) => l === "Net Sales");
          //       const salesValue =
          //         salesIndex >= 0
          //           ? Number((chartData.datasets[0].data as number[])[salesIndex] ?? 1)
          //           : 1;
          //       const percentage = (value / (salesValue || 1)) * 100;
          //       const metricLabel = String(context.label ?? "");
          //       const formattedValue = Number(value.toFixed(2)).toLocaleString();
          //       return `${metricLabel}: ${currencySymbol}${formattedValue} (${percentage.toFixed(
          //         1
          //       )}%)`;
          //     },
          //   },
          // },
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

                const percentage = (value / (salesValue || 1)) * 100;
                const metricLabel = fullLabels[i] ?? "";

                const formattedValue = Number(value.toFixed(2)).toLocaleString();
                return `${metricLabel}: ${currencySymbol}${formattedValue} (${percentage.toFixed(1)}%)`;
              },
            },
          },

        },
        scales: {
          x: {
            ticks: {
              maxRotation: 0,
              minRotation: 0,
              autoSkip: false,
              padding: 8,
              callback: (_value, index) => {
                const raw = isCollapsed
                  ? String(labels[index] ?? "")           // short labels
                  : String(fullLabels[index] ?? "");      // full labels

                if (isCollapsed) return raw; // no wrap in collapsed
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
            grid: { display: false },
          },

          y: {
            title: { display: true, text: `Amount (${currencySymbol})` },
            grid: {
              display: true,
            },
          },
        },
      };

      // ✅ ExcelJS export with embedded Bar chart
      const exportToExcel = async () => {
        try {
          const wb = new ExcelJS.Workbook();
          const ws = wb.addWorksheet("Sales Data");

          // Meta rows
          const extraRows = getExtraRows();
          extraRows.forEach((r) => ws.addRow(r));
          ws.addRow([""]); // spacer

          // Table header
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

          // Embed chart image below
          const base64 = getChartBase64();
          if (base64) {
            const lastRowNumber = ws.lastRow?.number ?? 1;
            const imageStartRow = lastRowNumber + 2;

            const imageId = wb.addImage({ base64, extension: "png" });
            ws.addImage(imageId, {
              tl: { col: 0, row: imageStartRow - 1 }, // 0-based
              ext: { width: 900, height: 420 },
            });
          }

          const buffer = await wb.xlsx.writeBuffer();
          const blob = new Blob([buffer], {
            type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
          });
          saveAs(blob, `Metrics-${formattedMonthYear}.xlsx`);
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
      // used inside exportToExcel
      countryName,
      isGlobalPage,
      userData?.brand_name,
      userData?.company_name,
      isCollapsed,
    ]);

  useEffect(() => {
    onNoDataChange?.(!loading && allValuesZero);
  }, [onNoDataChange, allValuesZero, loading]);

  return (
    <div className="relative w-full h-full min-h-0">
      <div
        className={`h-full min-h-0 ${allValuesZero && !loading ? "opacity-30 pointer-events-none" : "opacity-100"
          }`}
      >
        {!hideDownloadButton && (
          <div className="flex justify-end mb-2">
            <DownloadIconButton onClick={exportToExcel} />
          </div>
        )}

        <div className="w-full h-full min-h-0">
          {loading ? (
            <div className="flex h-full items-center justify-center">
              <Loader fullscreen transparent />
            </div>
          ) : (
            chartData.datasets.length > 0 && (
              <Bar
                ref={(instance) => {
                  chartRef.current = (instance as any) ?? null;
                }}
                data={chartData}
                options={chartOptions}
              />
            )
          )}
        </div>
      </div>
    </div>
  );

};

export default Bargraph;
