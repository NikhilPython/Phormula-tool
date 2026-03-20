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

type APIResponse = {
  previous_summary: SummaryShape | undefined;
  summary?: Partial<SummaryShape>;
  monthlyBreakdown?: Record<string, Partial<SummaryShape>>;
};

type QuarterlyMonthlyData = Record<string, Partial<SummaryShape>>;
type QuarterlyTotals = Partial<SummaryShape>;

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
  net_sales: "Sales",
  amazon_fee: "Amazon Fees",
  advertising_total: "Advertising Cost",
  taxncredit: "Tax and Credit",
  otherwplatform: "Other Charges",
  rembursement_fee: "Net Reimbursement",
  cashflow: "Cash Generated",
};

const colorMapping: Record<string, string> = {
  Sales: "#75BBDA",
  "Amazon Fees": "#B75A5A",
  "Advertising Cost": "#C49466",
  "Other Charges": "#3A8EA4",
  "Tax and Credit": "#ED9F50",
  "CM1 Profit": "#7B9A6D",
  "Net Reimbursement": "#FDD36F",
  "Cash Generated": "#7B9A6D",
};

const mobileShortLabelMap: Record<string, string> = {
  Sales: "Sales",
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
    Record<string, Partial<SummaryShape>>
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
    quantity_total: 10,
    gross_sales: 16,
    net_sales: 14,
    amazon_fee: 32,
    advertising_total: 18,
    taxncredit: 45,
    otherwplatform: 28,
    rembursement_fee: 32,
    cashflow: 82,
  };


  const effectiveData = isPreviewMode
    ? { summary: DUMMY_CASHFLOW_SUMMARY }
    : data;



  const [selectedGraphs, setSelectedGraphs] =
    useState<Record<string, boolean>>(defaultMetricState);

  // token (browser only)
  const token =
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

  const getSafeValue = (key: keyof SummaryShape) => {
    return (effectiveData?.summary?.[key] ?? 0) as number;
  };

  const allValuesZero =
    !effectiveData?.summary ||
    Object.values(effectiveData.summary).every((v) => !v);
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
    if (countryName) searchParams.set("country", countryName.toLowerCase());
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



  const fetchAllYearlyData = async () => {
    const yearlyData: Record<string, Partial<SummaryShape>> = {};
    for (const mName of monthsList) {
      try {
        const result = await fetchSpecificPeriodData(
          mName.toLowerCase(),
          year,
          "monthly"
        );
        if (result && result.summary) {
          yearlyData[mName] = result.summary;
        }
      } catch {
        // continue
      }
    }
    setAllYearlyData(yearlyData);
    return yearlyData;
  };

  const fetchCashFlowData = async () => {
    if (isPreviewMode) return;
    setError("");
    setLoading(true);
    setData(null);

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
        await fetchAllYearlyData();
        const resp = await fetchSpecificPeriodData(null, year, "yearly");
        setData(resp);
      } else {
        const resp = await fetchSpecificPeriodData(
          month.toLowerCase(),
          year,
          "monthly"
        );
        setData(resp);
      }
    } catch (err: any) {
      setError(err?.message || "Network error or unexpected error occurred");
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



  const isSmallScreen = viewportWidth < 1024; // mobile + tablet
  const isMobile = viewportWidth < 640;

  const xAxisTickFontSize = isMobile ? 9 : isSmallScreen ? 10 : 12;
  const yAxisFontSize = 12;
  const barWidthInPixels = Math.max(viewportWidth * 0.05, 40);

  const getLineChartData = () => {
    let labels: string[] = [];
    const datasets: any[] = [];

    if (
      periodType === "quarterly" &&
      selectedQuarter &&
      Object.keys(quarterlyMonthlyData).length > 0
    ) {
      const sourceMonths = quarterMapping[selectedQuarter] || [];

      labels = sourceMonths.map((m) =>
        isMobile ? shortMonthMap[m] || m : m
      );

      columnsToDisplay2.forEach((key) => {
        if (!selectedGraphs[key]) return;

        const ds = sourceMonths.map((monthName) => {
          const md = quarterlyMonthlyData[monthName];
          return Math.abs(Number(md?.[key] ?? 0));
        });

        datasets.push({
          label: labelMap[key],
          data: ds,
          borderColor: colorMapping[labelMap[key]],
          backgroundColor: `${colorMapping[labelMap[key]]}20`,
          borderWidth: 2,
          fill: false,
          tension: 0.35,
          pointRadius: 3,
          pointHoverRadius: 4,
        });
      });
    } else if (periodType === "yearly") {
      labels = monthsList.map((m) => (isMobile ? shortMonthMap[m] || m : m));

      columnsToDisplay2.forEach((key) => {
        if (!selectedGraphs[key]) return;

        const ds = monthsList.map((m) => {
          const md = allYearlyData[m];
          const val = md?.[key] ?? 0;
          return Math.abs(Number(val));
        });

        const label = labelMap[key];

        datasets.push({
          label,
          data: ds,
          borderColor: colorMapping[label],
          backgroundColor: `${colorMapping[label]}20`,
          borderWidth: 2,
          fill: false,
          tension: 0.35,
          pointRadius: 2,
          pointHoverRadius: 3,
        });
      });
    }

    return { labels, datasets };
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
          data: filteredKeys.map((k) => Math.abs(Number(getSafeValue(k)))),
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
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          label: (tooltipItem: any) =>
            `Amount: ${currencySymbol}${Number(
              tooltipItem.raw
            ).toLocaleString()}`,
        },
      },
    },
    scales: {
      x: {
        grid: {
          display: false,
          drawBorder: false,
        },
        title: {
          display: false,
        },
        ticks: {
          font: { size: xAxisTickFontSize },
          maxRotation: 0,
          minRotation: 0,
          autoSkip: false,
          callback: function (
            this: any,
            _value: unknown,
            index: number
          ): string | number {
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
          text: `Amount (${currencySymbol})`,
          font: { size: yAxisFontSize },
        },
        ticks: {
          font: { size: yAxisFontSize },
          callback: (value: any) =>
            `${currencySymbol}${Number(value).toLocaleString()}`,
        },
      },
    },
  } as const;

  const lineChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          label: (tooltipItem: any) =>
            `${tooltipItem.dataset.label}: ${currencySymbol}${Number(
              tooltipItem.raw
            ).toLocaleString()}`,
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
          drawBorder: false,
        },
        title: {
          display: false,
        },
        ticks: {
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
        beginAtZero: true,
        title: {
          display: true,
          text: `Amount (${currencySymbol})`,
          font: { size: yAxisFontSize },
        },
        ticks: {
          font: { size: yAxisFontSize },
          callback: (value: any) =>
            `${currencySymbol}${Number(value).toLocaleString()}`,
        },
      },
    },
    interaction: { mode: "index" as const, intersect: false },
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

  const canShowResults =
    (periodType === "monthly" && !!month && !!year) ||
    (periodType === "quarterly" && !!selectedQuarter && !!year) ||
    (periodType === "yearly" && !!year);


  return (
    <div className="w-full pb-6 sm:pb-0">

      {!embedded && (
        <div
          className="w-full flex flex-col bg-[#F7F7F7] gap-1 sm:gap-4 border-b border-gray-200
  md:sticky md:top-0 md:z-40 sm:flex-row md:items-center md:justify-between"
        >
          {/* LEFT: Title */}
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

          {/* RIGHT: Filters */}
          <div className="mb-2 sm:mb-0">
            <div className="flex flex-col md:flex-row sm:items-center  gap-[0.5vw]">
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

          {/* </div> */}
        </div>
      )}

      {/* Show alert until a valid period selection is made */}
      {
        !isPreviewMode && !canShowResults && (
          <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
            <div className="flex items-center">
              <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
              <span>
                Choose a period to view cash flow.
              </span>
            </div>
          </div>
        )
      }

      {/* Loading – now using Loader */}
      {
        !isPreviewMode && loading && (
          <div className="flex flex-col items-center justify-center py-12 text-center">
            <Loader fullscreen transparent />
          </div>
        )
      }

      {/* Error */}
      {
        !isPreviewMode && !!error && (
          <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
            <div className="flex items-center">
              <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
              <span>{error}</span>
            </div>
          </div>
        )
      }

      {/* Results */}
      {
        (effectiveData) && (
          <div className="flex flex-col">
            {/* Header + Download in one responsive row */}
            <div className="mb-2 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              {/* Left: title + period */}

              {/* Right: Download button */}
              {/* <div className="flex justify-center sm:justify-end">
              <DownloadIconButton onClick={downloadCombinedExcelWithImage} />
            </div> */}
            </div>

            {/* Summary Table using DataTable */}
            {effectiveData?.summary && (
              <CashFlowSankey
                data={effectiveData.summary}
                previous_summary={
                  isPreviewMode ? undefined : data?.previous_summary
                }
                previousLabel={isPreviewMode ? undefined : previousLabel}
                periodType={periodType}
                currency={currencySymbol}
              />
            )}



            {/* Chart Section */}
            <div className="mt-6 rounded-xl bg-white p-4 shadow border">
              <div className="h-[320px] sm:h-[40vw] max-h-[560px]">
                {periodType === "monthly" ? (
                  <Bar
                    ref={chartRef}
                    data={getFilteredBarChartData() as any}
                    options={barChartOptions as any}
                  />
                ) : (
                  <Line
                    ref={chartRef}
                    data={getLineChartData() as any}
                    options={lineChartOptions as any}
                  />
                )}
              </div>

            </div>
          </div>
        )
      }
    </div >
  );
};

export default CashFlowPage;
