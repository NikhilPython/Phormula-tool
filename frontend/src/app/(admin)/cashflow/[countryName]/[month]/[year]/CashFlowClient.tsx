"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useParams } from "next/navigation";
import dynamic from "next/dynamic";
import * as XLSX from "xlsx";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import Button from "@/components/ui/button/Button";
import PeriodFiltersTable from "@/components/filters/PeriodFiltersTable";
import Loader from "@/components/loader/Loader";
import "@/lib/chartSetup";
import {
  Chart as ChartJS,
  BarElement,
  CategoryScale,
  LinearScale,
  Tooltip,
  Legend,
  LineElement,
  PointElement,
  Title as ChartTitle,
} from "chart.js";
import CashFlowSankey from "@/components/cashflow/CashFlowSankey";

interface CashFlowPageProps {
  embedded?: boolean;
  countryNameProp?: string;
  rangeProp?: PeriodType;
  selectedMonthProp?: string;
  selectedQuarterProp?: string;
  selectedYearProp?: string | number;
}

const hoverPopPlugin = {
  id: "hoverPopPlugin",
  afterDatasetsDraw(chart: any) {
    const active = chart.getActiveElements?.() || [];
    if (!active.length) return;

    const { datasetIndex, index } = active[0];
    const meta = chart.getDatasetMeta(datasetIndex);
    const element = meta?.data?.[index];
    if (!element) return;

    // only for bar charts
    if (meta?.type !== "bar") return;

    const ctx = chart.ctx;

    const props = element.getProps(
      ["x", "y", "base", "width", "height"],
      true
    );

    const x = props.x;
    const y = props.y;
    const base = props.base;
    const w = props.width;

    // horizontal scale only
    const popW = w * 1.18;
    const left = x - popW / 2;
    const top = Math.min(y, base);
    const height = Math.abs(base - y);

    ctx.save();

    const bg = chart.data.datasets?.[datasetIndex]?.backgroundColor;
    const fill = Array.isArray(bg) ? bg[index] : bg || "rgba(0,0,0,0.2)";

    // keep same color, just widen
    ctx.fillStyle = fill as any;
    ctx.fillRect(left, top, popW, height);

    ctx.restore();
  },
};

ChartJS.register(
  BarElement,
  CategoryScale,
  LinearScale,
  Tooltip,
  Legend,
  LineElement,
  PointElement,
  ChartTitle
);

ChartJS.register(hoverPopPlugin);

const Line = dynamic(() => import("react-chartjs-2").then((m) => m.Line), {
  ssr: false,
});
const Bar = dynamic(() => import("react-chartjs-2").then((m) => m.Bar), {
  ssr: false,
});

type PeriodType = "monthly" | "quarterly" | "yearly";

type SummaryShape = {
  quantity_total: number;
  gross_sales: number;
  net_sales: number;
  amazon_fee: number;
  advertising_total: number;
  taxncredit: number;
  otherwplatform: number;
  rembursement_fee: number;
  cashflow: number;
};

type SummaryRow = {
  sno: React.ReactNode;
  category: React.ReactNode;
  sign: React.ReactNode;
  amount: React.ReactNode;
};

type CashFlowSummary = Partial<SummaryShape> & Record<string, unknown>;

type APIResponse = {
  previous_summary?: CashFlowSummary;
  summary?: CashFlowSummary;
  monthlyBreakdown?: Record<string, CashFlowSummary>;
};

type QuarterlyMonthlyData = Record<string, CashFlowSummary>;
type QuarterlyTotals = CashFlowSummary;

const getCurrencySymbol = (country?: string) => {
  switch ((country || "").toLowerCase()) {
    case "uk":
      return "£";
    case "india":
      return "₹";
    case "us":
      return "$";
    case "global":
      return "$";
    default:
      return "$";
  }
};

const capitalize = (str: string) =>
  str ? str.charAt(0).toUpperCase() + str.slice(1).toLowerCase() : "";

const formatCurrencyValue = (value: number, currencySymbol: string) => {
  const absValue = Math.round(Math.abs(Number(value || 0))).toLocaleString(undefined, {
    maximumFractionDigits: 0,
  });

  return Number(value) < 0
    ? `-${currencySymbol}${absValue}`
    : `${currencySymbol}${absValue}`;
};

// fixed columns expected
const columnsToDisplay2 = [
  "net_sales",
  "amazon_fee",
  "advertising_total",
  "taxncredit",
  "otherwplatform",
  "rembursement_fee",
  "cashflow",
] as const;

const labelMap: Record<(typeof columnsToDisplay2)[number], string> = {
  net_sales: "Net Sales",
  amazon_fee: "Amazon Fees",
  advertising_total: "Advertising Cost",
  taxncredit: "Tax and Credit",
  otherwplatform: "Other Charges",
  rembursement_fee: "Net Reimbursement",
  cashflow: "Cash Generated",
};

const colorMapping: Record<string, string> = {
  "Net Sales": "#75BBDA",
  "Amazon Fees": "#B75A5A",
  "Advertising Cost": "#C49466",
  "Other Charges": "#3A8EA4",
  "Tax and Credit": "#ED9F50",
  "CM1 Profit": "#7B9A6D",
  "Net Reimbursement": "#FDD36F",
  "Cash Generated": "#7B9A6D",
};

const mobileShortLabelMap: Record<string, string> = {
  "Net Sales": "Net Sales",
  "Amazon Fees": "Amz Fees",
  "Advertising Cost": "Ads",
  "Tax and Credit": "Tax/Crd",
  "Other Charges": "Other",
  "Net Reimbursement": "Reimb",
  "Cash Generated": "Cash",
};

const monthsList = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

const getCompletedMonthsForYear = (selectedYear: string): string[] => {
  const today = new Date();
  const currentYear = today.getFullYear();
  const selectedYearNumber = Number(selectedYear);

  // Previous years are fully historic
  if (selectedYearNumber < currentYear) {
    return monthsList;
  }

  // Current year: exclude the currently running month
  if (selectedYearNumber === currentYear) {
    return monthsList.slice(0, today.getMonth());
  }

  // Future year: no historic months
  return [];
};

const quarterMapping: Record<string, string[]> = {
  Q1: ["January", "February", "March"],
  Q2: ["April", "May", "June"],
  Q3: ["July", "August", "September"],
  Q4: ["October", "November", "December"],
};

const shortMonthMap: Record<string, string> = {
  January: "Jan",
  February: "Feb",
  March: "Mar",
  April: "Apr",
  May: "May",
  June: "Jun",
  July: "Jul",
  August: "Aug",
  September: "Sep",
  October: "Oct",
  November: "Nov",
  December: "Dec",
};

const quarterToPeriodTypeMap: Record<string, string> = {
  Q1: "quarter1",
  Q2: "quarter2",
  Q3: "quarter3",
  Q4: "quarter4",
};

const CashFlowPage: React.FC<CashFlowPageProps> = ({
  embedded = false,
  countryNameProp,
  rangeProp,
  selectedMonthProp,
  selectedQuarterProp,
  selectedYearProp,
}) => {
  const params = useParams<{
    countryName?: string;
    month?: string;
    year?: string;
  }>();

  const routeCountryName = params?.countryName || "";
  const routeMonth = params?.month ? decodeURIComponent(params.month) : "";
  const routeYear = params?.year || "";

  const countryName = embedded ? (countryNameProp || "") : routeCountryName;
  const paramMonth = embedded ? (selectedMonthProp || "") : routeMonth;
  const paramYear = embedded ? String(selectedYearProp || "") : routeYear;

  const chartRef = React.useRef<any>(null);


  const isPreviewMode =
    String(paramMonth).toLowerCase() === "na" ||
    String(paramYear).toLowerCase() === "na";

  const effectiveCountryForCurrency = isPreviewMode
    ? "global"
    : countryName;


  const currencySymbol = getCurrencySymbol(effectiveCountryForCurrency);

  const currentYear = new Date().getFullYear();
  const years = useMemo(
    () => Array.from({ length: 2 }, (_, i) => currentYear - i),
    [currentYear]
  );




  // 🔹 NEW: compute whether the route params equal the *current* month & year
  const today = new Date();
  const currentMonthName = monthsList[today.getMonth()]; // e.g. "December"
  const currentYearStr = String(today.getFullYear());

  const isParamCurrentMonthYear =
    paramMonth &&
    paramYear &&
    paramMonth.toLowerCase() === currentMonthName.toLowerCase() &&
    String(paramYear) === currentYearStr;

  // 🔹 NEW: initial values – ignore params if they are the current month+year
  const initialMonth = !isParamCurrentMonthYear && paramMonth
    ? capitalize(paramMonth)
    : "";

  const initialYear = !isParamCurrentMonthYear && paramYear
    ? String(paramYear)
    : "";



  // previous month logic
  const prevMonthDate = new Date(today.getFullYear(), today.getMonth() - 1, 1);

  const defaultMonth = monthsList[prevMonthDate.getMonth()]; // "November"
  const defaultYear = String(prevMonthDate.getFullYear());   // "2024"

  const [selectedQuarter, setSelectedQuarter] = useState<string>(
    embedded ? (selectedQuarterProp || "") : ""
  );

  const [month, setMonth] = useState<string>(
    embedded
      ? (isPreviewMode ? defaultMonth : capitalize(selectedMonthProp || ""))
      : (initialMonth || defaultMonth)
  );

  const [year, setYear] = useState<string>(
    embedded
      ? (isPreviewMode ? defaultYear : String(selectedYearProp || ""))
      : (initialYear || defaultYear)
  );

  const [periodType, setPeriodType] = useState<PeriodType>(
    embedded ? (rangeProp || "yearly") : "monthly"
  );

  const [error, setError] = useState<string>("");
  const [data, setData] = useState<APIResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [allQuarterlyData, setAllQuarterlyData] = useState<
    Record<string, QuarterlyTotals>
  >({});
  const [allYearlyData, setAllYearlyData] = useState<
    Record<string, CashFlowSummary>
  >({});
  const [quarterlyMonthlyData, setQuarterlyMonthlyData] =
    useState<QuarterlyMonthlyData>({});

  const defaultMetricState = {
    net_sales: true,
    amazon_fee: true,
    advertising_total: true,
    taxncredit: true,
    otherwplatform: true,
    rembursement_fee: true,
    cashflow: true,
  };

  useEffect(() => {
    if (!embedded) return;

    setPeriodType(rangeProp || "yearly");

    if (isPreviewMode) {
      setMonth(defaultMonth);
      setSelectedQuarter("");
      setYear(defaultYear);
      return;
    }

    setMonth(capitalize(selectedMonthProp || ""));
    setSelectedQuarter(selectedQuarterProp || "");
    setYear(String(selectedYearProp || ""));
  }, [
    embedded,
    rangeProp,
    selectedMonthProp,
    selectedQuarterProp,
    selectedYearProp,
    isPreviewMode,
    defaultMonth,
    defaultYear,
  ]);

  const DUMMY_CASHFLOW_SUMMARY: SummaryShape = {
    quantity_total: 0,
    gross_sales: 0,
    net_sales: 0,
    amazon_fee: 0,
    advertising_total: 0,
    taxncredit: 0,
    otherwplatform: 0,
    rembursement_fee: 0,
    cashflow: 0,
  };

  const EMPTY_CASHFLOW_SUMMARY: SummaryShape = {
    quantity_total: 0,
    gross_sales: 0,
    net_sales: 0,
    amazon_fee: 0,
    advertising_total: 0,
    taxncredit: 0,
    otherwplatform: 0,
    rembursement_fee: 0,
    cashflow: 0,
  };

  const [selectedGraphs, setSelectedGraphs] =
    useState<Record<string, boolean>>(defaultMetricState);

  // token (browser only)
  const token =
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

  // API helpers (using fetch)
  const fetchSpecificPeriodData = async (
    requestMonth: string | null,
    requestYear: string | null,
    requestPeriodType: PeriodType
  ): Promise<APIResponse> => {
    if (!token) {
      throw new Error("Authorization token not found. Please login.");
    }
    const searchParams = new URLSearchParams();

    if (requestMonth) searchParams.set("month", requestMonth);
    if (requestYear) searchParams.set("year", String(requestYear));

    if (countryName) {
      const country = countryName.toLowerCase();
      searchParams.set("country", country);

      // ✅ Tell backend to convert UK GBP -> USD before global total
      if (country === "global") {
        searchParams.set("homeCurrency", "usd");
      }
    }

    searchParams.set("period_type", requestPeriodType);

    const res = await fetch(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/cashflow?${searchParams.toString()}`,
      {
        method: "GET",
        headers: {
          Authorization: `Bearer ${token}`,
        },
      }
    );
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.error || `HTTP ${res.status}`);
    }
    const json = (await res.json()) as APIResponse;
    return json;
  };

  const prevMonthLabel =
    periodType === "monthly" && month && year
      ? `${monthsList[(monthsList.indexOf(month) + 11) % 12]} ${month === "January" ? Number(year) - 1 : year
      }`
      : "";

  const prevQuarterLabel =
    periodType === "quarterly" && selectedQuarter && year
      ? `${selectedQuarter === "Q1" ? "Q4" : "Q" + (Number(selectedQuarter[1]) - 1)} ${selectedQuarter === "Q1" ? Number(year) - 1 : year
      }`
      : "";

  const prevYearLabel =
    periodType === "yearly" && year ? String(Number(year) - 1) : "";
  const previousLabel =
    periodType === "monthly"
      ? prevMonthLabel
      : periodType === "quarterly"
        ? prevQuarterLabel
        : prevYearLabel;


  const fetchQuarterlyMonthlyData = async (quarter: string, y: string) => {
    const qMonths = quarterMapping[quarter] || [];
    const monthlyData: QuarterlyMonthlyData = {};
    const quarterSummary: QuarterlyTotals = {
      net_sales: 0,
      amazon_fee: 0,
      advertising_total: 0,
      taxncredit: 0,
      otherwplatform: 0,
      rembursement_fee: 0,
      cashflow: 0,
    };

    for (const mName of qMonths) {
      try {
        const result = await fetchSpecificPeriodData(
          mName.toLowerCase(),
          y,
          "monthly"
        );
        if (result && result.summary) {
          monthlyData[mName] = result.summary;
          (Object.keys(quarterSummary) as (keyof SummaryShape)[]).forEach(
            (key) => {
              quarterSummary[key] =
                (quarterSummary[key] || 0) + (result.summary?.[key] || 0);
            }
          );
        }
      } catch {
        // continue
      }
    }
    return { monthlyData, quarterSummary };
  };

  const fetchYearlyMonthlyData = async (
    targetYear: string,
    monthsToFetch: string[]
  ): Promise<Record<string, CashFlowSummary>> => {
    const yearlyData: Record<string, CashFlowSummary> = {};

    for (const mName of monthsToFetch) {
      try {
        const result = await fetchSpecificPeriodData(
          mName.toLowerCase(),
          targetYear,
          "monthly"
        );

        if (result?.summary) {
          yearlyData[mName] = {
            ...result.summary,
          };
        }
      } catch {
        // Continue fetching the remaining months
      }
    }

    return yearlyData;
  };

  const buildYearlySummaryFromMonths = (
    yearlyData: Record<string, CashFlowSummary>
  ): CashFlowSummary => {
    const summary: CashFlowSummary = {};

    Object.values(yearlyData).forEach((monthSummary) => {
      Object.entries(monthSummary || {}).forEach(([key, rawValue]) => {
        // Skip null, undefined, objects and non-numeric values
        if (rawValue === null || rawValue === undefined) return;

        const numericValue =
          typeof rawValue === "number"
            ? rawValue
            : typeof rawValue === "string" &&
              rawValue.trim() !== "" &&
              Number.isFinite(Number(rawValue.replace(/,/g, "")))
              ? Number(rawValue.replace(/,/g, ""))
              : null;

        if (numericValue === null || !Number.isFinite(numericValue)) {
          return;
        }

        summary[key] = Number(summary[key] ?? 0) + numericValue;
      });
    });

    return summary;
  };

  const fetchCashFlowData = async () => {
    if (isPreviewMode) return;
    setError("");
    setLoading(true);
    setData(null);
    setAllYearlyData({});
    setQuarterlyMonthlyData({});

    // validation
    if (periodType === "monthly" && (!month || !year)) {
      setError("Please select both month and year for monthly view.");
      setLoading(false);
      return;
    }
    if (periodType === "quarterly" && (!selectedQuarter || !year)) {
      setError("Please select both quarter and year for quarterly view.");
      setLoading(false);
      return;
    }
    if (periodType === "yearly" && !year) {
      setError("Please select year for yearly view.");
      setLoading(false);
      return;
    }

    try {
      if (periodType === "quarterly") {
        // 1️⃣ Quarterly API → Sankey + summary
        const quarterPeriodType =
          quarterToPeriodTypeMap[selectedQuarter];

        const quarterResp = await fetchSpecificPeriodData(
          null,
          year,
          quarterPeriodType as PeriodType
        );

        setData(quarterResp);

        // 2️⃣ Monthly APIs → Line chart data
        const { monthlyData } = await fetchQuarterlyMonthlyData(
          selectedQuarter,
          year
        );

        setQuarterlyMonthlyData(monthlyData);
      }
      else if (periodType === "yearly") {
        const selectedYearNumber = Number(year);
        const previousYear = String(selectedYearNumber - 1);

        // Current selected year:
        // If current year is selected, exclude the ongoing month.
        // For an old year, use all 12 months.
        const currentYearMonths = getCompletedMonthsForYear(year);

        const currentYearData = await fetchYearlyMonthlyData(
          year,
          currentYearMonths
        );

        // Use the same months for previous year comparison.
        // Example: Jan-Jun 2026 compared with Jan-Jun 2025.
        const previousYearData = await fetchYearlyMonthlyData(
          previousYear,
          currentYearMonths
        );

        const currentSummary =
          buildYearlySummaryFromMonths(currentYearData);

        const previousSummary =
          buildYearlySummaryFromMonths(previousYearData);

        // Used by the yearly line graph
        setAllYearlyData(currentYearData);

        setData({
          summary: currentSummary,
          previous_summary: previousSummary,
        });
      } else {
        const resp = await fetchSpecificPeriodData(
          month.toLowerCase(),
          year,
          "monthly"
        );
        setData(resp);
      }
    } catch (err: any) {
      const message = err?.message || "Network error or unexpected error occurred";

      if (message.toLowerCase().includes("no data")) {
        setError("");
        setData({
          previous_summary: undefined,
          summary: EMPTY_CASHFLOW_SUMMARY,
        });
      } else {
        setError(message);
      }
    } finally {
      setLoading(false);
    }
  };

  // 🔄 Auto-fetch when filters become valid
  useEffect(() => {
    if (isPreviewMode) return; // 🔥 STOP ALL API CALLS

    if (periodType === "monthly") {
      if (month && year) {
        void fetchCashFlowData();
      }
    } else if (periodType === "quarterly") {
      if (selectedQuarter && year) {
        void fetchCashFlowData();
      }
    } else if (periodType === "yearly") {
      if (year) {
        void fetchCashFlowData();
      }
    }
  }, [periodType, month, year, selectedQuarter, isPreviewMode]);

  // user data (company/brand) for export headers
  const [userData, setUserData] = useState<{
    company_name?: string;
    brand_name?: string;
  } | null>(null);

  useEffect(() => {
    if (isPreviewMode) return;
    const fetchUserData = async () => {
      if (!token) {
        setError("No token found. Please log in.");
        return;
      }
      try {
        const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/get_user_data`, {
          method: "GET",
          headers: { Authorization: `Bearer ${token}` },
        });
        if (!res.ok) {
          const err = await res.json().catch(() => ({}));
          setError(err.error || "Something went wrong.");
          return;
        }
        const json = await res.json();
        setUserData(json);
      } catch {
        setError("Error fetching user data");
      }
    };
    void fetchUserData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);


  const [viewportWidth, setViewportWidth] = useState<number>(
    typeof window !== "undefined" ? window.innerWidth : 1200
  );

  useEffect(() => {
    const handleResize = () => setViewportWidth(window.innerWidth);
    handleResize();
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  const isSmallScreen = viewportWidth < 1024;
  const isMobile = viewportWidth < 640;

  const xAxisTickFontSize = isMobile ? 9 : isSmallScreen ? 10 : 12;
  const yAxisFontSize = 12;
  const barWidthInPixels = Math.max(viewportWidth * 0.05, 40);

  const canShowResults =
    (periodType === "monthly" && !!month && !!year) ||
    (periodType === "quarterly" && !!selectedQuarter && !!year) ||
    (periodType === "yearly" && !!year);

  const effectiveData =
    isPreviewMode
      ? { summary: EMPTY_CASHFLOW_SUMMARY }
      : data?.summary
        ? data
        : canShowResults && !loading
          ? { summary: EMPTY_CASHFLOW_SUMMARY }
          : null;

  const getSafeValue = (key: keyof SummaryShape) => {
    return (effectiveData?.summary?.[key] ?? 0) as number;
  };

  const allValuesZero =
    !effectiveData?.summary ||
    Object.values(effectiveData.summary ?? {}).every((v) => !v);

  const showNoDataMessage =
    !isPreviewMode &&
    canShowResults &&
    !loading &&
    !error &&
    !!effectiveData?.summary &&
    allValuesZero;

  const getLineChartData = () => {
    let sourceMonths: string[] = [];
    const datasets: any[] = [];

    if (periodType === "quarterly" && selectedQuarter) {
      const qMonths = quarterMapping[selectedQuarter] || [];

      // Only show months that were actually fetched
      sourceMonths = qMonths.filter((monthName) => {
        return quarterlyMonthlyData[monthName];
      });
    } else if (periodType === "yearly") {
      // Only show months that were actually fetched, but keep calendar order
      sourceMonths = monthsList.filter((monthName) => {
        return allYearlyData[monthName];
      });
    }

    const labels = sourceMonths.map((m) =>
      isMobile ? shortMonthMap[m] || m : m
    );

    columnsToDisplay2.forEach((key) => {
      if (!selectedGraphs[key]) return;

      const dataSource =
        periodType === "quarterly" ? quarterlyMonthlyData : allYearlyData;

      const ds = sourceMonths.map((monthName) => {
        const md = dataSource[monthName];
        const value = Number(md?.[key] ?? 0);

        return ["amazon_fee", "advertising_total", "otherwplatform"].includes(key)
          ? Math.abs(value)
          : value;
      });

      const label = labelMap[key];

      datasets.push({
        label,
        metricKey: key,
        data: ds,
        borderColor: colorMapping[label],
        backgroundColor: colorMapping[label],
        borderWidth: 2,
        fill: false,
        tension: 0.35,
        cubicInterpolationMode: "monotone",
        pointRadius: 3,
        pointHoverRadius: 5,
      });
    });

    return { labels, datasets };
  };

  const expensePositiveKeys = [
    "amazon_fee",
    "advertising_total",
    "otherwplatform",
  ] as const;

  const getChartDisplayValue = (key: keyof SummaryShape) => {
    const value = Number(getSafeValue(key));

    return expensePositiveKeys.includes(key as any)
      ? Math.abs(value)
      : value;
  };

  const getFilteredBarChartData = () => {
    const filteredKeys = columnsToDisplay2.filter((k) => selectedGraphs[k]);

    return {
      labels: filteredKeys.map((k) => {
        const fullLabel = labelMap[k];
        return isMobile
          ? mobileShortLabelMap[fullLabel] || fullLabel
          : fullLabel;
      }),
      datasets: [
        {
          label: "Amount",
          data: filteredKeys.map((k) => getChartDisplayValue(k)),
          backgroundColor: filteredKeys.map(
            (k) => colorMapping[labelMap[k]] || "#999"
          ),
          borderColor: filteredKeys.map(
            (k) => colorMapping[labelMap[k]] || "#666"
          ),
          borderWidth: 1,
          maxBarThickness: barWidthInPixels,
        },
      ],
    };
  };

  const barChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    layout: { padding: 0 },
    plugins: {
      legend: { display: false },
      tooltip: {
        enabled: !allValuesZero,
        callbacks: {
          label: (tooltipItem: any) => {
            const label = tooltipItem.label || "";
            return `${label}: ${formatCurrencyValue(Math.abs(Number(tooltipItem.raw)), currencySymbol)}`;
          },
        },
      },
    },
    scales: {
      x: {
        grid: {
          display: false,
        },
        border: {
          display: false,
        },
        ticks: {
          color: "#6B7280",
          font: { size: xAxisTickFontSize },
          maxRotation: 0,
          minRotation: 0,
          autoSkip: false,
          callback: function (this: any, _value: unknown, index: number): string | number {
            const label = this.chart.data.labels?.[index];
            return typeof label === "string" || typeof label === "number" ? label : "";
          },
        },
        offset: true,
      },
      y: {
        beginAtZero: true,
        title: {
          display: true,
          text: `(${currencySymbol})`,
          color: "#6B7280",
          font: { size: yAxisFontSize },
        },
        ticks: {
          color: "#6B7280",
          font: { size: yAxisFontSize },
          padding: 0,
          callback: (value: any) =>
            formatCurrencyValue(Number(value), currencySymbol),
        },
        border: {
          display: false,
        },
        grid: {
          color: "#E5E7EB",
        },
      },
    },
  } as const;

  const allLineDataPoints =
    periodType === "monthly"
      ? []
      : getLineChartData().datasets.flatMap((d: any) => d.data as number[]);

  const maxLineValue = allLineDataPoints.length
    ? Math.max(...allLineDataPoints)
    : 0;

  const minLineValue = allLineDataPoints.length
    ? Math.min(...allLineDataPoints)
    : 0;

  // top hamesha clean 1000 multiple ho
  const maxLineY = Math.max(1000, Math.ceil(maxLineValue / 1000) * 1000);

  // agar negative value hai:
  // -14 => -1000
  // -1200 => -2000
  // -2600 => -3000
  const minLineY =
    minLineValue < 0
      ? Math.min(-1000, Math.floor(minLineValue / 1000) * 1000)
      : 0;

  const lineChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    layout: { padding: 0 },
    interaction: {
      intersect: false,
      mode: allValuesZero ? "nearest" : "index",
    },
    plugins: {
      legend: { display: false },
      tooltip: {
        enabled: !allValuesZero,
        mode: "index",
        intersect: false,
        callbacks: {
          label: (tooltipItem: any) => {
            const displayLabel = tooltipItem.dataset.label || "";
            return `${displayLabel}: ${formatCurrencyValue(Number(tooltipItem.raw), currencySymbol)}`;
          },
          labelColor: (context: any) => {
            const color = context.dataset.borderColor;
            return {
              borderColor: color,
              backgroundColor: color,
            };
          },
          labelTextColor: () => "#414042",
        },
      },
    },
    scales: {
      x: {
        grid: {
          display: false,
        },
        border: {
          display: false,
        },
        ticks: {
          color: "#6B7280",
          font: { size: xAxisTickFontSize },
          maxRotation: 0,
          minRotation: 0,
          autoSkip: false,
          callback: function (this: any, _value: any, index: number): string | number {
            const label = this.chart.data.labels?.[index];
            return typeof label === "string" || typeof label === "number" ? label : "";
          },
        },
      },
      y: {
        min: minLineY,
        max: maxLineY,

        title: {
          display: true,
          text: `(${currencySymbol})`,
          color: "#6B7280",
          font: { size: yAxisFontSize },
        },

        ticks: {
          stepSize: 1000,
          color: "#6B7280",
          font: { size: yAxisFontSize },
          padding: 0,
          callback: (value: any) =>
            formatCurrencyValue(Number(value), currencySymbol),
        },

        border: {
          display: false,
        },

        grid: {
          color: "#E5E7EB",
        },
      },
    },
  } as const;

  const metrics = columnsToDisplay2.map((key) => ({
    name: key,
    label: labelMap[key],
    color: colorMapping[labelMap[key]],
  }));

  // Handlers for PeriodFiltersTable
  const handleRangeChange = (v: PeriodType) => {
    setPeriodType(v);
    setData(null);
    setError("");

    // 🔥 reset graph selection on period change
    setSelectedGraphs(
      v === "monthly"
        ? {
          net_sales: true,
          amazon_fee: true,
          advertising_total: true,
          taxncredit: true,
          otherwplatform: true,
          rembursement_fee: true,
          cashflow: true,
        }
        : {
          net_sales: true,
          rembursement_fee: true,
          cashflow: true,

          amazon_fee: false,
          advertising_total: false,
          taxncredit: false,
          otherwplatform: false,
        }
    );
  };


  const handleMonthChange = (lowercaseMonth: string) => {
    setMonth(capitalize(lowercaseMonth));
    setData(null);
    setError("");
  };

  const handleQuarterChange = (q: string) => {
    setSelectedQuarter(q);
    setData(null);
    setError("");
  };

  const handleYearChange = (y: string) => {
    setYear(y);
    setData(null);
    setError("");
  };


  const toggleMetric = (name: string) => {
    if (allValuesZero) return;
    setSelectedGraphs((prev) => ({
      ...prev,
      [name]: !prev[name],
    }));
  };


  const minSelectedMetricCount = Object.values(selectedGraphs).filter(Boolean).length;

  const displayedMetrics = metrics.map((m) => ({
    ...m,
    displayLabel: periodType === "monthly" ? m.label : m.label,
  }));

  const toggleMetricSelection = (name: string) => {
    const isChecked = !!selectedGraphs[name];
    const selectedCount = Object.values(selectedGraphs).filter(Boolean).length;

    if (isChecked && selectedCount === 1) return;

    setSelectedGraphs((prev) => ({
      ...prev,
      [name]: !prev[name],
    }));
  };

  return (
    <div className="w-full pb-6 sm:pb-0">
      {!embedded && (
        <div
          className="w-full flex flex-col bg-[#F7F7F7] gap-1 sm:gap-4 border-b border-gray-200
md:sticky md:top-0 md:z-40 sm:flex-row md:items-center md:justify-between"
        >
          <div className="mb-2 flex flex-wrap items-start gap-2">
            <div>
              <div className="flex flex-wrap items-baseline gap-2 justify-start">
                <PageBreadcrumb
                  pageTitle="Cash Flow –"
                  variant="page"
                  align="left"
                  className=""
                />
                <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
                  Amazon {countryName?.toLowerCase() === "global"
                    ? "Global"
                    : countryName?.toUpperCase()}
                </span>
              </div>

              <p className="2xl:text-sm text-xs">
                Track cash generation from performance
              </p>
            </div>
          </div>

          <div className="mb-2 sm:mb-0">
            <div className="flex flex-col md:flex-row sm:items-center gap-[0.5vw]">
              <PeriodFiltersTable
                range={periodType}
                selectedMonth={month.toLowerCase()}
                selectedQuarter={selectedQuarter}
                selectedYear={year}
                yearOptions={years}
                onRangeChange={handleRangeChange}
                onMonthChange={handleMonthChange}
                onQuarterChange={handleQuarterChange}
                onYearChange={handleYearChange}
              />
            </div>
          </div>
        </div>
      )}

      {!isPreviewMode && !canShowResults && (
        <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
          <div className="flex items-center">
            <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
            <span>Choose a period to view cash flow.</span>
          </div>
        </div>
      )}

      {!isPreviewMode && loading && (
        <div className="flex flex-col items-center justify-center py-12 text-center">
          <Loader fullscreen transparent />
        </div>
      )}

      {!isPreviewMode && !!error && !showNoDataMessage && (
        <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
          <div className="flex items-center">
            <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
            <span>{error}</span>
          </div>
        </div>
      )}


      {effectiveData && (
        <div className="flex flex-col">
          <div className=" flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between" />

          {effectiveData?.summary && (
            <div className=" rounded-xl overflow-hidden">
              <CashFlowSankey
                data={effectiveData.summary}
                previous_summary={isPreviewMode ? undefined : data?.previous_summary}
                previousLabel={isPreviewMode ? undefined : previousLabel}
                periodType={periodType}
                currency={currencySymbol}
                isPreviewMode={isPreviewMode}
              />
            </div>
          )}

          <div className="mt-6 rounded-xl bg-white p-4 shadow border">
            {periodType !== "monthly" && (
              <div
                className={[
                  "flex flex-wrap items-center justify-center",
                  "gap-4",
                  "w-full",
                  allValuesZero ? "pointer-events-none" : "opacity-100",
                  "transition-opacity duration-300",
                ].join(" ")}
              >
                {displayedMetrics.map(({ name, displayLabel, color }) => {
                  const isChecked = !!selectedGraphs[name];

                  return (
                    <label
                      key={name}
                      onClick={(e) => e.stopPropagation()}
                      onMouseDown={(e) => e.stopPropagation()}
                      className={[
                        "shrink-0",
                        "flex items-center gap-1 sm:gap-1.5",
                        "font-semibold select-none whitespace-nowrap",
                        "text-[10px] 2xl:text-xs my-1 2xl:my-3",
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
                        onMouseDown={(e) => e.stopPropagation()}
                        onClick={(e) => {
                          e.stopPropagation();
                          if (!allValuesZero) toggleMetricSelection(name);
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

                      <span className="capitalize">{displayLabel}</span>
                    </label>
                  );
                })}
              </div>
            )}

            <div className="w-full pt-4 h-[320px] sm:h-[40vw] max-h-[560px]">
              {periodType === "monthly" ? (
                <Bar
                  key="cashflow-bar"
                  ref={chartRef}
                  data={getFilteredBarChartData() as any}
                  options={barChartOptions as any}
                  style={{ width: "100%", height: "100%" }}
                />
              ) : (
                <Line
                  key="cashflow-line"
                  ref={chartRef}
                  data={getLineChartData() as any}
                  options={lineChartOptions as any}
                  style={{ width: "100%", height: "100%" }}
                />
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default CashFlowPage;
