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
import ModalMsg from "@/components/common/ModalMsg";
import PageBreadcrumb from "../common/PageBreadCrumb";
import Loader from "@/components/loader/Loader";
import DownloadIconButton from "../ui/button/DownloadIconButton";
import { ProfitChartExportApi } from "@/lib/utils/exportTypes";

ChartJS.register(
  CategoryScale,
  LinearScale,
  LineElement,
  PointElement,
  ChartTitle,
  Tooltip,
  Legend
);

type GraphPageProps = {
  range: "monthly" | "quarterly" | "yearly";
  selectedMonth?: string;
  selectedQuarter?: "Q1" | "Q2" | "Q3" | "Q4";
  selectedYear: number | string;
  countryName: string;
  homeCurrency?: string;
  onNoDataChange?: (noData: boolean) => void;
  onExportApiReady?: (api: ProfitChartExportApi) => void;
  hideDownloadButton?: boolean;
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
  total_net_credits?: number;
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
  onNoDataChange,
  onExportApiReady,
  hideDownloadButton,
}) => {
  const isGlobalPage = (countryName || "").toLowerCase() === "global";
  const normalizedHomeCurrency = (homeCurrency || "").trim().toLowerCase();

  // ✅ For global pages: we want to filter rows by `country` field.
  // - global (base)
  // - global_inr, global_cad, global_gbp
  const globalCountryKey = normalizedHomeCurrency
    ? `global_${normalizedHomeCurrency}`
    : "global";

  const currencySymbol = isGlobalPage
    ? getCurrencySymbol(normalizedHomeCurrency || "usd")
    : getCurrencySymbol(countryName || "");

  const [data, setData] = useState<UploadRow[]>([]);
  const [allValuesZero, setAllValuesZero] = useState(false);
  const [showModal, setShowModal] = useState(false);
  const [loading, setLoading] = useState<boolean>(true);

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

  const token =
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

  const [fetchError, setFetchError] = useState<string | null>(null);
  const [userData, setUserData] = useState<{
    company_name?: string;
    brand_name?: string;
  } | null>(null);

  // ✅ Chart instance ref (for embedding chart image into Excel)
  const chartRef = React.useRef<ChartInstance<"line"> | null>(null);

  const getChartBase64 = () => {
    const chart = chartRef.current;
    if (!chart) return null;
    // base64 data url: "data:image/png;base64,..."
    return chart.toBase64Image("image/png", 1);
  };

  const generateDummyData = (labels: string[]) => {
    const dummyMetrics: Record<string, number[]> = {
      sales: labels.map((_, i) => 15000 + Math.random() * 5000 + i * 1000),
      AmazonExpense: labels.map(
        (_, i) => 3000 + Math.random() * 1000 + i * 200
      ),
      total_cous: labels.map((_, i) => 8000 + Math.random() * 2000 + i * 500),
      advertisingCosts: labels.map(
        (_, i) => 2000 + Math.random() * 800 + i * 150
      ),
      Other: labels.map((_, i) => 1000 + Math.random() * 500 + i * 100),
      taxncredit: labels.map((_, i) => 500 + Math.random() * 300 + i * 50),
      profit: labels.map((_, i) => 1500 + Math.random() * 800 + i * 200),
      profit2: labels.map((_, i) => 2000 + Math.random() * 1000 + i * 250),
    };
    return dummyMetrics;
  };

  const getQuarterLabels = (
    year: number | string,
    quarter: "Q1" | "Q2" | "Q3" | "Q4"
  ) => {
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

  const labelMap: Record<string, string> = {
    sales: "Sales",
    total_cous: "COGS",
    taxncredit: "Taxes & Credits",
    AmazonExpense: "Amazon Fees",
    advertisingCosts: "Advertising Costs",
    Other: "Other",
    profit: "CM2 Profit",
    profit2: "CM1 Profit",
  };

  // ✅ Fetch upload history
  useEffect(() => {
    const fetchUploadHistory = async () => {
      try {
        if (!token) {
          setLoading(false);
          return;
        }
        setLoading(true);

        const url = new URL("http://127.0.0.1:5000/upload_history");

        // optional: keep sending it (backend might use it later)
        if (isGlobalPage && normalizedHomeCurrency) {
          url.searchParams.set("homeCurrency", normalizedHomeCurrency);
        }

        const resp = await fetch(url.toString(), {
          method: "GET",
          headers: { Authorization: `Bearer ${token}` },
        });

        if (!resp.ok) {
          const j = await resp.json().catch(() => ({}));
          throw new Error(j?.error || "Failed to fetch upload history");
        }

        const json = await resp.json();

        if (json?.uploads) {
          const rows = json.uploads as UploadRow[];

          const isUsd = normalizedHomeCurrency === "usd";

          const filtered = rows.filter((item) => {
            const itemCountry = (item.country || "").toLowerCase();

            if (isGlobalPage) {
              if (isUsd) {
                // ✅ USD global supports BOTH legacy + converted
                return itemCountry === "global" || itemCountry === "global_usd";
              }
              // ✅ Non-USD global currencies
              return itemCountry === `global_${normalizedHomeCurrency}`;
            }

            // ✅ Normal country pages (UK, US, etc.)
            return itemCountry === countryName.toLowerCase();
          });

          setData(filtered);
        } else {
          setData([]);
        }
      } catch (e) {
        console.error("Failed to fetch upload history:", e);
        const msg =
          e instanceof Error ? e.message : "Failed to fetch upload history";
        setFetchError(msg);
        setData([]);
      } finally {
        setLoading(false);
      }
    };

    fetchUploadHistory();
  }, [token, countryName, isGlobalPage, normalizedHomeCurrency, globalCountryKey]);

  useEffect(() => {
    const fetchUser = async () => {
      if (!token) {
        setFetchError("No token found. Please log in.");
        return;
      }
      try {
        const response = await fetch("http://127.0.0.1:5000/get_user_data", {
          method: "GET",
          headers: { Authorization: `Bearer ${token}` },
        });
        if (!response.ok) {
          const j = await response.json().catch(() => ({}));
          setFetchError(j?.error || "Something went wrong.");
          return;
        }
        const j = await response.json();
        setUserData(j);
      } catch {
        setFetchError("Error fetching user data");
      }
    };
    fetchUser();
  }, [token]);

  // ✅ FIXED: checkbox handler
  const handleCheckboxChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, checked } = e.target;
    const selectedCount = Object.values(selectedGraphs).filter(Boolean).length;

    // prevent deselecting last one
    if (!checked && selectedCount === 1) {
      setShowModal(true);
      return;
    }

    setSelectedGraphs((prev) => ({ ...prev, [name]: checked }));
  };

  const monthlyLabels = useMemo(() => {
    if (range === "monthly" && selectedMonth && selectedYear) {
      return [`${selectedMonth} ${selectedYear}`.toLowerCase()];
    }
    if (range === "quarterly" && selectedQuarter && selectedYear) {
      return getQuarterLabels(selectedYear, selectedQuarter).map((l) =>
        l.toLowerCase()
      );
    }
    if (range === "yearly" && selectedYear) {
      return [
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
    }
    return [] as string[];
  }, [range, selectedMonth, selectedQuarter, selectedYear]);

  const processData = () => {
    if (!data || data.length === 0) {
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

    data.forEach((upload) => {
      const key = `${upload.month.toLowerCase()} ${upload.year}`;
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
        Number(upload.advertising_total || 0)
      );
      monthSums[key].Other += Math.abs(Number(upload.otherwplatform || 0));
      monthSums[key].taxncredit += Number(upload.taxncredit || 0);
      monthSums[key].profit += Number(upload.cm2_profit || 0);
      monthSums[key].profit2 += Number(upload.total_profit || 0);
    });

    const labels = monthlyLabels;

    if (labels.length > 0) {
      const allDataValues: number[] = [];
      Object.entries(selectedGraphs)
        .filter(([, checked]) => checked)
        .forEach(([metric]) => {
          const vals = labels.map(
            (l) =>
              monthSums[l]?.[metric as keyof (typeof monthSums)[string]] || 0
          );
          allDataValues.push(...vals);
        });

      const isAllZero = !allDataValues.some((v) => Math.abs(v) > 0.01);

      let dataToUse = monthSums;
      if (isAllZero) {
        const dummy = generateDummyData(labels);
        dataToUse = {};
        labels.forEach((l, idx) => {
          dataToUse[l] = {
            sales: dummy.sales[idx],
            AmazonExpense: dummy.AmazonExpense[idx],
            taxncredit: dummy.taxncredit[idx],
            total_cous: dummy.total_cous[idx],
            advertisingCosts: dummy.advertisingCosts[idx],
            Other: dummy.Other[idx],
            profit: dummy.profit[idx],
            profit2: dummy.profit2[idx],
          };
        });
      }

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
          label: labelMap[metric] ?? metric,
          data: labels.map(
            (l) =>
              dataToUse[l]?.[metric as keyof (typeof dataToUse)[string]] || 0
          ),
          fill: false,
          borderColor: colorMap[metric] ?? "#000",
          backgroundColor: colorMap[metric] ?? "#000",

          // 👇 Make the line curved
          tension: 0.35,
          cubicInterpolationMode: "monotone",
          pointRadius: 3,
          pointHoverRadius: 5,
        }));

      return { labels, datasets, isAllZero };
    }

    return { labels: [], datasets: [], isAllZero: false };
  };

  const { labels: rawLabels, datasets, isAllZero } = useMemo(processData, [
    data,
    selectedGraphs,
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    monthlyLabels,
  ]);

  useEffect(() => {
    setAllValuesZero(isAllZero);
    onNoDataChange?.(isAllZero);
  }, [isAllZero, onNoDataChange]);

  const formattedLabels = useMemo(() => {
    return rawLabels.map((label) => {
      const [m, y] = label.trim().split(" ");
      const mm = convertToAbbreviatedMonth(m);
      const yy = (y ?? "").slice(-2);
      return `${mm}\u00A0'${yy}`;
    });
  }, [rawLabels]);

  const allDataPoints = datasets.flatMap((d: any) => d.data as number[]);
  const minValue = allDataPoints.length ? Math.min(...allDataPoints) : 0;
  const minY = minValue < 0 ? Math.floor(minValue * 1.1) : 0;

  const periodInfo = useMemo(() => {
    if (range === "monthly" && selectedMonth) {
      return `${convertToAbbreviatedMonth(selectedMonth)}'${String(
        selectedYear
      ).slice(-2)}`;
    }
    if (range === "quarterly" && selectedQuarter) {
      return `${selectedQuarter}'${String(selectedYear).slice(-2)}`;
    }
    return `Year'${String(selectedYear).slice(-2)}`;
  }, [range, selectedMonth, selectedQuarter, selectedYear]);

  const getExtraRows = () => {
    const formattedCountry = isGlobalPage
      ? "GLOBAL"
      : (countryName || "").toUpperCase();
    return [
      [`${userData?.brand_name || "N/A"}`],
      [`${userData?.company_name || "N/A"}`],
      [`Profit Breakup (SKU Level) - ${periodInfo}`],
      [`Currency:  ${currencySymbol}`],
      [`Country: ${formattedCountry}`],
      [`Platform: Amazon`],
    ];
  };

  // ✅ ExcelJS Export + Embed chart image (monthly/quarterly/yearly)
  const exportToExcel = async () => {
    try {
      const wb = new ExcelJS.Workbook();
      const ws = wb.addWorksheet("Sales Data");

      // ---- Top header/meta rows ----
      const extraRows = getExtraRows(); // string[][]
      extraRows.forEach((r) => ws.addRow(r));
      ws.addRow([""]); // spacer

      // ---- Build monthSums ----
      const monthSums: Record<string, any> = {};
      data.forEach((upload) => {
        const key = `${upload.month.toLowerCase()} ${upload.year}`;
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
        monthSums[key].profit2 += Number(upload.total_profit || 0);
        monthSums[key].advertisingCosts += Number(upload.advertising_total || 0);
        monthSums[key].Other += Number(upload.otherwplatform || 0);
        monthSums[key].profit += Number(upload.cm2_profit || 0);
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

      // ---- Labels in the sheet ----
      const labelsNorm = rawLabels.map((l) => {
        const [m, y] = l.split(" ");
        const mm = convertToAbbreviatedMonth(m);
        const yy = (y ?? "").slice(-2);
        return `${mm}'${yy}`;
      });

      // ---- Table header + sign row ----
      ws.addRow(["Month", ...fixedOrder.map((i) => i.label)]);
      ws.addRow([" ", ...fixedOrder.map((i) => i.sign)]);

      // ---- Rows ----
      rawLabels.forEach((raw, idx) => {
        const display = labelsNorm[idx];
        const key = raw.toLowerCase();
        const row = [
          display,
          ...fixedOrder.map(({ key: k }) =>
            Number((monthSums[key]?.[k] ?? 0).toFixed(2))
          ),
        ];
        ws.addRow(row);
      });

      // ---- Total row ----
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

      // ---- Embed chart image below table ----
      const base64 = getChartBase64();
      if (base64) {
        const lastRowNumber = ws.lastRow?.number ?? 1;
        const imageStartRow = lastRowNumber + 2;

        const imageId = wb.addImage({
          base64,
          extension: "png",
        });

        ws.addImage(imageId, {
          tl: { col: 0, row: imageStartRow - 1 }, // 0-based
          ext: { width: 900, height: 420 },
        });
      }

      // ---- Download with FileSaver ----
      const buffer = await wb.xlsx.writeBuffer();
      const blob = new Blob([buffer], {
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      });
      saveAs(blob, `Metrics-${periodInfo}.xlsx`);
    } catch (err) {
      console.error("Excel export failed:", err);
      setFetchError("Excel export failed. Please try again.");
    }
  };

  const noMetricSelected = Object.values(selectedGraphs).every((v) => v === false);

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


  return (
    // <div className="relative w-full rounded-xl border border-slate-200 bg-white shadow-sm p-4 sm:p-5">
    <div className="relative w-full">  
    {loading ? (
        <div className="flex h-[260px] md:h-[320px] items-center justify-center">
          <Loader
            src="/infinity-unscreen.gif"
            size={150}
            transparent
            roundedClass="rounded-full"
            backgroundClass="bg-transparent"
            respectReducedMotion
          />
        </div>
      ) : (
        <div className={allValuesZero ? "opacity-30 pointer-events-none" : "opacity-100"}>
          {/* <div className="mb-2 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex flex-wrap items-baseline gap-2 justify-center sm:justify-start">
              <PageBreadcrumb
                pageTitle="Profitability"
                variant="page"
                align="left"
                textSize="2xl"
              />
            </div>

            <div className="flex justify-center sm:justify-end">
              {!hideDownloadButton && <DownloadIconButton onClick={exportToExcel} />}
            </div>

          </div> */}

          {/* Metric toggles */}
          <div
            className={[
              "mt-3 sm:mt-4",
              "flex flex-wrap items-center justify-center",
              "gap-3 sm:gap-4 md:gap-5",
              "w-full mx-auto",
              allValuesZero ? "opacity-30" : "opacity-100",
              "transition-opacity duration-300",
            ].join(" ")}
          >
            {[
              { name: "sales", label: "Net Sales", color: "#75BBDA" },
              { name: "total_cous", label: "COGS", color: "#FDD36F" },
              { name: "AmazonExpense", label: "Amazon Fees", color: "#B75A5A" },
              { name: "taxncredit", label: "Taxes & Credits", color: "#ED9F50" },
              { name: "profit2", label: "CM1 Profit", color: "#7B9A6D" },
              { name: "advertisingCosts", label: "Advertising Costs", color: "#C49466" },
              { name: "Other", label: "Other", color: "#3A8EA4" },
              { name: "profit", label: "CM2 Profit", color: "#B8C78C" },
            ].map(({ name, label, color }) => {
              const isChecked = !!selectedGraphs[name];

              return (
                <label
                  key={name}
                  className={[
                    "shrink-0",
                    "flex items-center gap-1 sm:gap-1.5",
                    "font-semibold select-none whitespace-nowrap",
                    "text-[9px] sm:text[10px] md:text-[11px] lg:text-xs xl:text-sm",
                    "text-charcoal-500",
                    allValuesZero ? "cursor-not-allowed" : "cursor-pointer",
                  ].join(" ")}
                >
                  <span
                    className="flex items-center justify-center h-3 w-3 sm:h-3.5 sm:w-3.5 rounded-sm border transition"
                    style={{
                      borderColor: color,
                      backgroundColor: isChecked ? color : "white",
                      opacity: allValuesZero ? 0.6 : 1,
                    }}
                    onClick={() => !allValuesZero && toggleMetric(name)}
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
                  <span className="capitalize">{label}</span>
                </label>
              );
            })}
          </div>

          {/* Chart */}
          <div className="relative mt-2 sm:mt-3">
            <div
              className={`flex w-full items-center justify-center
                h-[320px] sm:h-[360px] md:h-[400px] lg:h-[420px]
                transition-opacity duration-300
                ${allValuesZero ? "opacity-30" : "opacity-100"}`}
            >
              {datasets.length > 0 && (
                <Line
                  // ✅ Capture chart instance for Excel export
                  ref={(instance) => {
                    chartRef.current = (instance as any) ?? null;
                  }}
                  data={{ labels: formattedLabels, datasets }}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {
                      intersect: false,
                      mode: allValuesZero ? "nearest" : "index",
                    },
                    plugins: {
                      tooltip: {
                        enabled: !allValuesZero,
                        mode: "index",
                        intersect: false,
                        callbacks: {
                          label: (tooltipItem: any) => {
                            const displayLabel = (tooltipItem.dataset.label as string) || "";
                            const value = tooltipItem.raw as number;
                            return `${displayLabel}: ${currencySymbol} ${value.toLocaleString(
                              undefined,
                              {
                                minimumFractionDigits: 2,
                                maximumFractionDigits: 2,
                              }
                            )}`;
                          },
                        },
                      },
                      legend: { display: false },
                    },
                    scales: {
                      x: {
                        title: { display: true, text: "Month" },
                        ticks: {
                          minRotation: 0,
                          maxRotation: 0,
                          autoSkip: formattedLabels.length > 6,
                          maxTicksLimit:
                            formattedLabels.length > 0 ? formattedLabels.length : 12,
                          callback: (_v, idx) => String(formattedLabels[idx] ?? ""),
                        },
                      },
                      y: {
                        title: { display: true, text: `Amount (${currencySymbol})` },
                        min: minY,
                        ticks: { padding: 0 },
                      },
                    },
                  }}
                />
              )}
            </div>

            {noMetricSelected && (
              <ModalMsg
                show={showModal}
                onClose={() => setShowModal(false)}
                message="At least one metric must be selected to display the graph."
              />
            )}
          </div>

          {fetchError && (
            <p className="mt-3 text-center text-sm text-red-600">Error: {fetchError}</p>
          )}
        </div>
      )}
    </div>
  );
};

export default GraphPage;
