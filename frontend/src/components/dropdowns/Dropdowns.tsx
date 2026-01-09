"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import Bargraph from "./BarGraph";
import GraphPage from "./GraphPage";
import CircleChart from "./CircleChart";
import CMchartofsku from "./CMchartofsku";
import SKUtable from "./SKUtable";
import IntegrationDashboard from "@/features/integration/IntegrationDashboard";
import PageBreadcrumb from "../common/PageBreadCrumb";
import Button from "../ui/button/Button";
import { AiOutlinePlus } from "react-icons/ai";
import { Modal } from "@/components/ui/modal";
import FileUploadForm from "@/app/(admin)/(ui-elements)/modals/FileUploadForm";
import PeriodFiltersTable from "../filters/PeriodFiltersTable";
import { FaBoxArchive, FaMoneyBillTrendUp } from "react-icons/fa6";
import { IoMdLock } from "react-icons/io";
import { MdEditDocument } from "react-icons/md";
import { TbMoneybag } from "react-icons/tb";
import { FcSalesPerformance } from "react-icons/fc";
import Loader from "@/components/loader/Loader";
import { useGetUserDataQuery } from "@/lib/api/profileApi";
import ExcelJS from "exceljs";
import { saveAs } from "file-saver";
import type { ProfitChartExportApi, SkuExportPayload } from "@/lib/utils/exportTypes";
import DownloadIconButton from "../ui/button/DownloadIconButton";
import MonthEndBusinessSummaryCard from "./MonthEndBusinessSummaryCard";
import RecommendationsCard from "./RecommendationsCard";



/* ---------------------- Types ---------------------- */
type Summary = {
  unit_sold: number;
  total_sales: number;      // (your current "Sales")
  gross_sales?: number;     // ✅ ADD THIS
  total_product_sales?: number;
  total_expense: number;
  cm2_profit: number;
  total_cous?: number;
  otherwplatform?: number;
  advertising_total?: number;
  total_amazon_fee?: number;
};



type SummaryComparisons = {
  lastMonth?: Summary;
  lastQuarter?: Summary;
  lastYear?: Summary;
};


type UploadHistoryResponse = {
  summary: Summary;
  summaryComparisons?: SummaryComparisons;
  [key: string]: unknown;
};




/* ---------------------- AI Summary Types ---------------------- */
type AiSummaryResponse = {
  summary?: string | null;
  recommendations?: string | null;
};

type AiPanelData = {
  summaryBullets: string[];
  skuInsightsBullets: string[];     // NEW
  recommendationBullets: string[];
  inventoryBullets: string[];       // NEW
  rawSummary?: string | null;
  rawRecommendations?: string | null;
};


type RangeType = "monthly" | "quarterly" | "yearly" | "";

/** Quarter union and helpers */
type Quarter = "Q1" | "Q2" | "Q3" | "Q4";
const isQuarter = (v: string): v is Quarter =>
  (["Q1", "Q2", "Q3", "Q4"] as const).includes(v as Quarter);

type DropdownsProps = {
  initialRanged: string;
  initialCountryName: string;
  initialMonth: string;
  initialYear: string;
};
type ComparisonItem = {
  label: string;
  value?: number;
  diffPct?: number | null;
};

const monthIndexMap: Record<string, number> = {
  january: 0, february: 1, march: 2, april: 3, may: 4, june: 5,
  july: 6, august: 7, september: 8, october: 9, november: 10, december: 11,
};

type FetchedPeriods = Record<string, string[]>; // { "2024": ["january","february"], ... }

const readFetchedPeriods = (): FetchedPeriods => {
  if (typeof window === "undefined") return {};
  try {
    const raw = localStorage.getItem("fetchedPeriods");
    return raw ? (JSON.parse(raw) as FetchedPeriods) : {};
  } catch {
    return {};
  }
};

const writeFetchedPeriods = (fp: FetchedPeriods) => {
  if (typeof window === "undefined") return;
  localStorage.setItem("fetchedPeriods", JSON.stringify(fp));
};

const markFetched = (year: string, month?: string) => {
  if (typeof window === "undefined") return;
  const y = String(year);
  const m = month ? month.toLowerCase() : "";

  const fp = readFetchedPeriods();
  if (!fp[y]) fp[y] = [];
  if (m && !fp[y].includes(m)) fp[y].push(m);

  // keep months sorted
  fp[y] = fp[y]
    .filter(Boolean)
    .sort((a, b) => (monthIndexMap[a] ?? 99) - (monthIndexMap[b] ?? 99));

  writeFetchedPeriods(fp);

  // keep latestFetchedPeriod updated (used by PeriodFiltersTable too)
  if (m) {
    localStorage.setItem("latestFetchedPeriod", JSON.stringify({ year: y, month: m }));
  }
};

// ✅ default year for YEARLY view:
// - If current year has ANY fetched month that is strictly before current month → show current year
// - Else show previous year
const computeDefaultYearlyYear = () => {
  const now = new Date();
  const cy = now.getFullYear();
  const cm = now.getMonth(); // 0..11, current month

  const fp = readFetchedPeriods();
  const monthsFetchedThisYear = fp[String(cy)] || [];

  const hasHistoricMonthInCurrentYear = monthsFetchedThisYear.some((m) => {
    const idx = monthIndexMap[m.toLowerCase()];
    return typeof idx === "number" && idx < cm; // strictly historic
  });

  return hasHistoricMonthInCurrentYear ? String(cy) : String(cy - 1);
};

const getPrevMonthLabel = (selectedMonth: string, selectedYear: number) => {
  const idx = monthIndexMap[selectedMonth.toLowerCase()];
  if (idx === undefined) return "Last month";

  const prev = new Date(selectedYear, idx - 1, 1);
  const mon = prev.toLocaleString("en-US", { month: "short" }); // Nov
  const yy = String(prev.getFullYear()).slice(-2); // 25
  return `${mon}'${yy}`; // Nov'25
};


/* ---------------------- Utils ---------------------- */
const getCurrencySymbol = (codeOrCountry: string) => {
  const v = (codeOrCountry || "").toLowerCase();

  switch (v) {
    // Home currency / common codes
    case "usd":
    case "us":
    case "global":
      return "$";
    case "inr":
    case "india":
      return "₹";
    case "gbp":
    case "uk":
      return "£";
    case "eur":
    case "europe":
    case "eu":
      return "€";
    case "cad":
    case "ca":
    case "canada":
      return "C$";
    default:
      return "¤";
  }
};

const getQuarterFromMonth = (m: string): Quarter | "" => {
  const month = (m ?? "").toLowerCase();
  const quarters: Record<Quarter, string[]> = {
    Q1: ["january", "february", "march"],
    Q2: ["april", "may", "june"],
    Q3: ["july", "august", "september"],
    Q4: ["october", "november", "december"],
  };
  for (const q of Object.keys(quarters) as Quarter[]) {
    if (quarters[q].includes(month)) return q;
  }
  return "";
};

const getPrevQuarterLabel = (q: Quarter, selectedYear: number) => {
  const order: Quarter[] = ["Q1", "Q2", "Q3", "Q4"];
  const idx = order.indexOf(q);
  if (idx === -1) return "Prev quarter";

  const prevIdx = (idx - 1 + 4) % 4;
  const prevQuarter = order[prevIdx];

  const prevYear = idx === 0 ? selectedYear - 1 : selectedYear; // if Q1 -> prev is Q4 of prev year
  const yy = String(prevYear).slice(-2);

  return `${prevQuarter}'${yy}`; // Q4'25
};

const getPrevYearLabel = (selectedYear: number) => {
  return String(selectedYear - 1); // 2024
};




// ---------------------- AI Summary Helpers ----------------------
const monthNameToNumber = (m: string): string => {
  const idx = monthIndexMap[(m || "").toLowerCase()];
  return typeof idx === "number" ? String(idx + 1) : "";
};

const extractBullets = (md: string | null | undefined): string[] => {
  if (!md) return [];
  return md
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.startsWith("- "))
    .map((l) => l.replace(/^-\s+/, "").trim())
    .filter(Boolean);
};

// Pull only bullets under "## SUMMARY" section if present; otherwise fallback to all bullets
// --- NEW: split markdown into sections by "## " headings
const parseMdSections = (md?: string | null): Record<string, string[]> => {
  if (!md) return {};
  const lines = md.split(/\r?\n/);

  const sections: Record<string, string[]> = {};
  let current = "ROOT";
  sections[current] = [];

  for (const raw of lines) {
    const line = raw.trim();
    if (line.toLowerCase().startsWith("## ")) {
      current = line.replace(/^##\s+/i, "").trim().toUpperCase();
      if (!sections[current]) sections[current] = [];
      continue;
    }
    sections[current].push(raw);
  }

  // convert each section to bullets
  const out: Record<string, string[]> = {};
  for (const [k, arr] of Object.entries(sections)) {
    out[k] = arr
      .map((l) => l.trim())
      .filter((l) => l.startsWith("- "))
      .map((l) => l.replace(/^-\s+/, "").trim())
      .filter(Boolean);
  }
  return out;
};

// --- REPLACE old extractSummaryBullets with this (so it can also show SKU INSIGHTS)
const extractSummaryAndSkuBullets = (md?: string | null) => {
  const sections = parseMdSections(md);
  return {
    summaryBullets: sections["SUMMARY"] ?? [],
    skuInsightsBullets: sections["SKU INSIGHTS"] ?? [],
  };
};

// --- NEW: for recommendations, keep main bullets + INVENTORY section bullets
const extractRecoAndInventoryBullets = (md?: string | null) => {
  const sections = parseMdSections(md);

  // ROOT = bullets before any "##"
  const recommendationBullets = sections["ROOT"] ?? [];
  const inventoryBullets = sections["INVENTORY"] ?? [];

  return { recommendationBullets, inventoryBullets };
};

        type AiSingleInsightCardProps = {
  loading: boolean;
  error: string | null;
  summaryBullets: string[];
  recommendationBullets: string[];
  skuInsightsBullets: string[];
  inventoryBullets: string[];
};

const Section = ({
  title,
  bullets,
}: {
  title: string;
  bullets: string[];
}) => {
  if (!bullets.length) return null;

  return (
    <div className="space-y-2">
      <h3 className="text-sm font-semibold text-charcoal-600">
        {title}
      </h3>
      <ul className="list-disc pl-4 space-y-1 text-sm text-charcoal-500">
        {bullets.map((b, i) => (
          <li key={i}>{b}</li>
        ))}
      </ul>
    </div>
  );
};

const AiSingleInsightCard: React.FC<AiSingleInsightCardProps> = ({
  loading,
  error,
  summaryBullets,
  recommendationBullets,
  skuInsightsBullets,
  inventoryBullets,
}) => {
  if (loading) {
    return (
      <div className="w-full rounded-2xl border border-slate-200 bg-white shadow-sm p-5">
        <p className="text-sm text-charcoal-400">Generating insights…</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="w-full rounded-2xl border border-red-200 bg-red-50 p-5 text-sm text-red-600">
        {error}
      </div>
    );
  }

  if (
    !summaryBullets.length &&
    !recommendationBullets.length &&
    !skuInsightsBullets.length &&
    !inventoryBullets.length
  ) {
    return null;
  }

  return (
    <div className="w-full rounded-2xl border border-slate-200 bg-white shadow-sm p-5 space-y-6">
      <Section
        title="Month-end Business Summary"
        bullets={summaryBullets}
      />

      <Section
        title="Recommendations"
        bullets={[...recommendationBullets, ...inventoryBullets]}
      />

      <Section
        title="SKU Insights"
        bullets={skuInsightsBullets}
      />

      {/* Inventory shown only if still exists separately */}
      {inventoryBullets.length > 0 && (
        <Section
          title="Inventory"
          bullets={inventoryBullets}
        />
      )}
    </div>
  );
};

/* ---------------------- Component ---------------------- */
const Dropdowns: React.FC<DropdownsProps> = ({
  initialRanged,
  initialCountryName,
  initialMonth,
  initialYear,
}) => {
  const { data: userData } = useGetUserDataQuery();

  // Normalized home currency from profile (e.g. "usd", "inr")
  const homeCurrency = (userData?.homeCurrency || "USD").toLowerCase();

  const router = useRouter();

  // params from parent
  const ranged = initialRanged;
  const countryName = initialCountryName;
  const month = initialMonth;
  const year = initialYear;

  // Global vs Country page
  const isGlobalPage = countryName.toLowerCase() === "global";

  // For child components: only pass homeCurrency when global
  const globalHomeCurrency = isGlobalPage ? homeCurrency : undefined;

  // Symbol for summary cards
  const currencySymbol = isGlobalPage
    ? getCurrencySymbol(homeCurrency) // GLOBAL → homeCurrency
    : getCurrencySymbol(countryName || ""); // Country → country currency

  const [range, setRange] = useState<RangeType>("");
  const [selectedMonth, setSelectedMonth] = useState<string>("");
  const [selectedYear, setSelectedYear] = useState<string>("");
  const [selectedQuarter, setSelectedQuarter] = useState<Quarter | "">("");
  const [uploadsData, setUploadsData] = useState<UploadHistoryResponse | null>(
    null
  );
  const [allDropdownsSelected, setAllDropdownsSelected] = useState(false);
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [loading, setLoading] = useState(false);
  const [showNoDataOverlay, setShowNoDataOverlay] = useState(false);

  // ---------------- AI Summary Panel state ----------------
  const [aiPanel, setAiPanel] = useState<AiPanelData | null>(null);
  const [aiPanelLoading, setAiPanelLoading] = useState(false);
  const [aiPanelError, setAiPanelError] = useState<string | null>(null);

  const [chartExportApi, setChartExportApi] = useState<ProfitChartExportApi | null>(null);
  const [skuExportPayload, setSkuExportPayload] = useState<SkuExportPayload | null>(null);
  const [expenseBreakdownPieBase64, setExpenseBreakdownPieBase64] = useState<string | null>(null);
  const [productWiseCm1PieBase64, setProductWiseCm1PieBase64] = useState<string | null>(null);

  const layoutRef = useRef<HTMLDivElement | null>(null);
  const [overlayBounds, setOverlayBounds] = useState<{
    left: number;
    width: number;
  }>({
    left: 0,
    width: 0,
  });

  useEffect(() => {
    setShowNoDataOverlay(false);
    setChartExportApi(null);
    setSkuExportPayload(null);
    setExpenseBreakdownPieBase64(null);
    setProductWiseCm1PieBase64(null);
  }, [range, selectedMonth, selectedQuarter, selectedYear]);



  useEffect(() => {
    if (!showNoDataOverlay) return;

    const updateBounds = () => {
      if (!layoutRef.current) return;
      const rect = layoutRef.current.getBoundingClientRect();
      setOverlayBounds({
        left: rect.left,
        width: rect.width,
      });
    };

    updateBounds();
    window.addEventListener("resize", updateBounds);
    return () => window.removeEventListener("resize", updateBounds);
  }, [showNoDataOverlay]);

  const yearOptions = useMemo(
    () => [new Date().getFullYear(), new Date().getFullYear() - 1].map(String),
    []
  );

  const zeroData: Summary = {
    unit_sold: 0,
    total_sales: 0,
    gross_sales: 0, // ✅ ADD THIS
    total_product_sales: 0,
    total_expense: 0,
    cm2_profit: 0,
    total_cous: 0,
    otherwplatform: 0,
    advertising_total: 0,
    total_amazon_fee: 0,
  };


  const displayData: Summary =
    allDropdownsSelected && uploadsData?.summary
      ? uploadsData.summary
      : zeroData;


  console.log("🔍 displayData:", displayData);

  // range: "monthly" | "quarterly" | "yearly"
  const handleRangeChange = (v: "monthly" | "quarterly" | "yearly") => {
    setRange(v);
    setSelectedMonth("");
    setSelectedQuarter("");
    setSelectedYear("");
    setUploadsData(null);
  };

  const fetchUploadHistory = async (
    rangeType: RangeType,
    monthVal: string,
    quarterVal: string, // safe for the API as plain string
    yearVal: string,
    country: string
  ) => {
    if (!rangeType || !yearVal) return;

    setLoading(true);
    try {
      const token =
        typeof window !== "undefined"
          ? localStorage.getItem("jwtToken")
          : null;

      const url = new URL("http://127.0.0.1:5000/upload_history2");
      url.searchParams.set("range", rangeType);
      url.searchParams.set("month", monthVal);
      url.searchParams.set("quarter", quarterVal);
      url.searchParams.set("year", yearVal);
      url.searchParams.set("country", country);

      // ✅ Only for GLOBAL send homeCurrency
      if (country.toLowerCase() === "global" && homeCurrency) {
        url.searchParams.set("homeCurrency", homeCurrency);
      }

      const res = await fetch(url.toString(), {
        method: "GET",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        cache: "no-store",
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        console.error(`API Error: ${err?.error ?? res.statusText}`);
        // setUploadsData(null);
        return;
      }

      const data: UploadHistoryResponse = await res.json();
      setUploadsData(data);

      // ✅ Persist fetched periods so older months/years remain selectable later
      if (data?.summary) {
        if (rangeType === "monthly" && yearVal && monthVal) {
          markFetched(yearVal, monthVal);
        }
        if (rangeType === "quarterly" && yearVal) {
          // optional: mark year as seen (no month)
          markFetched(yearVal);
        }
        if (rangeType === "yearly" && yearVal) {
          // optional: mark year as seen (no month)
          markFetched(yearVal);
        }
      }

    } catch (error) {
      console.error("Error fetching data: ", error);
      // setUploadsData(null);
    } finally {
      setLoading(false);
    }
  };


  const fetchAiSummary = async (rangeType: RangeType) => {
    // only fetch when the selection is valid for the current range
    if (!countryName || !rangeType || !selectedYear) return;

    const timeline =
      rangeType === "monthly"
        ? monthNameToNumber(selectedMonth)
        : rangeType === "quarterly"
          ? selectedQuarter
          : "ALL";

    if (rangeType === "monthly" && !timeline) return;
    if (rangeType === "quarterly" && !selectedQuarter) return;

    setAiPanelLoading(true);
    setAiPanelError(null);

    try {
      const token =
        typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

      const url = new URL("http://127.0.0.1:5000/summary");
      url.searchParams.set("country", countryName);
      url.searchParams.set("period", rangeType);
      url.searchParams.set("timeline", String(timeline));
      url.searchParams.set("year", String(selectedYear));

      const res = await fetch(url.toString(), {
        method: "GET",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        cache: "no-store",
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        setAiPanel(null);
        setAiPanelError(String(err?.error ?? res.statusText));
        return;
      }

      const data: AiSummaryResponse = await res.json();

      const { summaryBullets, skuInsightsBullets } = extractSummaryAndSkuBullets(data.summary);
      const { recommendationBullets, inventoryBullets } = extractRecoAndInventoryBullets(data.recommendations);

      setAiPanel({
        summaryBullets,
        skuInsightsBullets,
        recommendationBullets,
        inventoryBullets,
        rawSummary: data.summary ?? null,
        rawRecommendations: data.recommendations ?? null,
      });
    } catch (e: any) {
      setAiPanel(null);
      setAiPanelError(e?.message || "Failed to fetch AI summary");
    } finally {
      setAiPanelLoading(false);
    }
  };

  // month comes in as lowercase from PeriodFiltersTable ("january", etc.)
  const handleMonthChange = (v: string) => {
    setSelectedMonth(v);

    if (selectedYear) {
      fetchUploadHistory(range, v, selectedQuarter || "", selectedYear, countryName);
    } else {
      setUploadsData(null);
    }
  };

  // quarter is "Q1" | "Q2" | "Q3" | "Q4"
  const handleQuarterChange = (v: string) => {
    const q = isQuarter(v) ? v : "";
    setSelectedQuarter(q);

    if (selectedYear && q) {
      fetchUploadHistory(range, selectedMonth, q, selectedYear, countryName);
    } else {
      setUploadsData(null);
    }
  };

  const handleYearChange = (v: string) => {
    setSelectedYear(v);

    if (
      (range === "monthly" && selectedMonth) ||
      (range === "quarterly" && selectedQuarter) ||
      range === "yearly"
    ) {
      fetchUploadHistory(range, selectedMonth, selectedQuarter || "", v, countryName);
    } else {
      setUploadsData(null);
    }
  };

  // Initialize range & selections from incoming params
  // useEffect(() => {
  //   if (ranged === "QTD") {
  //     setRange("quarterly");
  //     const q = getQuarterFromMonth(month);
  //     setSelectedQuarter(q); // Quarter | ""
  //     setSelectedYear(year);
  //   } else if (ranged === "MTD") {
  //     setRange("monthly");
  //     setSelectedMonth(month);
  //     setSelectedYear(year);
  //   } else if (ranged === "YTD") {
  //     setRange("yearly");
  //     setSelectedYear(year);
  //   }
  // }, [ranged, month, year]);

  useEffect(() => {
    // ✅ Always open yearly by default
    setRange("yearly");
    setSelectedMonth("");
    setSelectedQuarter("");

    // ✅ Default yearly year based on your historic rule
    const y = computeDefaultYearlyYear();
    setSelectedYear(y);
  }, []);


  // ✅ only change when global currency changes (prevents country pages going 0)
  const fetchCurrencyKey = isGlobalPage ? homeCurrency : "country";

  useEffect(() => {
    if (!countryName) return;
    if (range === "" || !selectedYear) return;

    fetchUploadHistory(
      range,
      selectedMonth,
      selectedQuarter || "",
      selectedYear,
      countryName
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [range, selectedMonth, selectedQuarter, selectedYear, countryName, fetchCurrencyKey]);

  // Fetch AI summary/recommendations for the selected period
  useEffect(() => {
    if (!range || !selectedYear) {
      setAiPanel(null);
      return;
    }

    // align with the same dropdown validity rules
    const ready =
      (range === "monthly" && !!selectedMonth && !!selectedYear) ||
      (range === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (range === "yearly" && !!selectedYear);

    if (!ready) {
      setAiPanel(null);
      return;
    }

    fetchAiSummary(range);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [range, selectedMonth, selectedQuarter, selectedYear, countryName]);

  const cropPngBase64WithSize = async (
    base64: string,
    pad = 0,
    opts?: {
      // how close to white counts as background (0-255)
      whiteThreshold?: number;     // default 253
      // how much non-bg must exist in a row/col to keep it (0..1)
      minContentRatio?: number;    // default 0.002 (0.2%)
    }
  ): Promise<{ base64: string; w: number; h: number }> => {
    const isDataUrl = base64.startsWith("data:image/");
    const raw = base64.includes("base64,") ? base64.split("base64,")[1] : base64;

    const img = new Image();

    // ✅ Use original data URL if present (jpeg/png)
    // ✅ Otherwise assume png (your older charts send raw png base64)
    img.src = isDataUrl ? base64 : `data:image/png;base64,${raw}`;


    await new Promise<void>((res, rej) => {
      img.onload = () => res();
      img.onerror = () => rej(new Error("Failed to load image for cropping"));
    });

    const canvas = document.createElement("canvas");
    const ctx = canvas.getContext("2d");
    if (!ctx) return { base64: raw, w: img.width, h: img.height };

    canvas.width = img.width;
    canvas.height = img.height;
    ctx.drawImage(img, 0, 0);

    const { data, width, height } = ctx.getImageData(0, 0, canvas.width, canvas.height);

    const whiteThreshold = opts?.whiteThreshold ?? 253;
    const minContentRatio = opts?.minContentRatio ?? 0.002; // 0.2%

    // Background: transparent OR near-white
    const isBg = (r: number, g: number, b: number, a: number) => {
      if (a === 0) return true;
      return r >= whiteThreshold && g >= whiteThreshold && b >= whiteThreshold;
    };

    // Count non-bg pixels in a row
    const rowContentRatio = (y: number) => {
      let nonBg = 0;
      for (let x = 0; x < width; x++) {
        const i = (y * width + x) * 4;
        const r = data[i], g = data[i + 1], b = data[i + 2], a = data[i + 3];
        if (!isBg(r, g, b, a)) nonBg++;
      }
      return nonBg / width;
    };

    // Count non-bg pixels in a col
    const colContentRatio = (x: number) => {
      let nonBg = 0;
      for (let y = 0; y < height; y++) {
        const i = (y * width + x) * 4;
        const r = data[i], g = data[i + 1], b = data[i + 2], a = data[i + 3];
        if (!isBg(r, g, b, a)) nonBg++;
      }
      return nonBg / height;
    };

    // Trim top/bottom by density
    let top = 0;
    while (top < height && rowContentRatio(top) < minContentRatio) top++;

    let bottom = height - 1;
    while (bottom >= 0 && rowContentRatio(bottom) < minContentRatio) bottom--;

    // Trim left/right by density
    let left = 0;
    while (left < width && colContentRatio(left) < minContentRatio) left++;

    let right = width - 1;
    while (right >= 0 && colContentRatio(right) < minContentRatio) right--;

    // If nothing meaningful found, return original
    if (right <= left || bottom <= top) return { base64: raw, w: img.width, h: img.height };

    // Apply pad
    left = Math.max(0, left - pad);
    top = Math.max(0, top - pad);
    right = Math.min(width - 1, right + pad);
    bottom = Math.min(height - 1, bottom + pad);

    const cropW = right - left + 1;
    const cropH = bottom - top + 1;

    const out = document.createElement("canvas");
    const outCtx = out.getContext("2d");
    if (!outCtx) return { base64: raw, w: img.width, h: img.height };

    out.width = cropW;
    out.height = cropH;

    outCtx.drawImage(canvas, left, top, cropW, cropH, 0, 0, cropW, cropH);

    return {
      base64: out.toDataURL("image/png").split("base64,")[1],
      w: cropW,
      h: cropH,
    };
  };

  const toJpegBase64 = async (
    base64: string,
    quality = 0.98,
    opts?: { scale?: number; bg?: string }
  ): Promise<{ base64: string; w: number; h: number }> => {
    const raw = base64.includes("base64,") ? base64.split("base64,")[1] : base64;

    // allow passing either png or jpeg data (we always load as image/*)
    const img = new Image();
    img.src = base64.startsWith("data:image/")
      ? base64
      : `data:image/png;base64,${raw}`;

    await new Promise<void>((res, rej) => {
      img.onload = () => res();
      img.onerror = () => rej(new Error("Failed to load image for JPEG conversion"));
    });

    const scale = opts?.scale ?? 1;          // ✅ upscale to reduce Excel seams
    const bg = opts?.bg ?? "#FFFFFF";        // ✅ solid bg

    const canvas = document.createElement("canvas");
    canvas.width = Math.max(1, Math.round(img.width * scale));
    canvas.height = Math.max(1, Math.round(img.height * scale));

    const ctx = canvas.getContext("2d");
    if (!ctx) return { base64: raw, w: img.width, h: img.height };

    // ✅ high quality scaling
    ctx.imageSmoothingEnabled = true;
    ctx.imageSmoothingQuality = "high";

    // ✅ solid white background
    ctx.fillStyle = bg;
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    ctx.drawImage(img, 0, 0, canvas.width, canvas.height);

    return {
      base64: canvas.toDataURL("image/jpeg", quality).split("base64,")[1],
      w: canvas.width,
      h: canvas.height,
    };
  };


  const handleDownloadProfitabilityBundle = async () => {
    try {
      const wb = new ExcelJS.Workbook();

      const addChartBlock = async (
        ws: ExcelJS.Worksheet,
        wb: ExcelJS.Workbook,
        title: string,
        base64: string | null | undefined,
        startRow: number,
        options?: {
          width?: number;          // only WIDTH is respected; height auto
          pad?: number;            // crop pad
          bgCols?: number;         // how many columns to paint white
          gapRowsAfter?: number;   // spacing after chart block
          minBase64Len?: number;   // guard against empty export
          scale?: number;          // jpeg upscale factor (default 2)
          skipCrop?: boolean;
        }
      ): Promise<number> => {
        const targetW = options?.width ?? 520;
        const pad = options?.pad ?? 2;
        const bgCols = options?.bgCols ?? 25;
        const gapRowsAfter = options?.gapRowsAfter ?? 2;
        const minBase64Len = options?.minBase64Len ?? 5000;
        const scale = options?.scale ?? 2; // ✅ important for wedge seam reduction
        const skipCrop = options?.skipCrop ?? false;

        // ----- Title -----
        ws.getRow(startRow).getCell(1).value = title;
        ws.getRow(startRow).getCell(1).font = { bold: true, size: 14 };

        // spacer row
        ws.getRow(startRow + 1).getCell(1).value = "";

        if (!base64) {
          ws.getRow(startRow + 2).getCell(1).value = "Chart not available";
          return startRow + 6;
        }

        const raw = base64.includes("base64,") ? base64.split("base64,")[1] : base64;

        // ✅ guard: export happened too early -> blank image
        if (!raw || raw.length < minBase64Len) {
          ws.getRow(startRow + 2).getCell(1).value = "Chart not available (empty export)";
          return startRow + 6;
        }



        let imgForJpeg = base64;

        // ✅ only crop when allowed
        if (!skipCrop) {
          const cropped = await cropPngBase64WithSize(base64, pad, {
            whiteThreshold: 254,
            minContentRatio: 0.0015,
          });

          imgForJpeg = `data:image/png;base64,${cropped.base64}`;
        }

        // ✅ always convert to JPEG for Excel (no alpha seams)
        const jpeg = await toJpegBase64(imgForJpeg, 0.98, {
          scale,
          bg: "#FFFFFF",
        });



        const finalW = targetW;
        const finalH = Math.round((finalW * jpeg.h) / jpeg.w);

        // Convert pixel height to approximate row count (18px-ish per row)
        const chartRows = Math.ceil(finalH / 18) + 2;

        // ----- White background block (hide gridlines) -----
        const bgStart = startRow + 1;
        const bgEnd = bgStart + chartRows;

        for (let r = bgStart; r <= bgEnd; r++) {
          for (let c = 1; c <= bgCols; c++) {
            ws.getRow(r).getCell(c).fill = {
              type: "pattern",
              pattern: "solid",
              fgColor: { argb: "FFFFFFFF" },
            };
          }
        }

        // ✅ Add as JPEG (no alpha = no wedge gaps)
        const imageId = wb.addImage({
          base64: jpeg.base64,
          extension: "jpeg",
        });

        // ✅ Insert
        ws.addImage(imageId, {
          tl: { col: 0, row: startRow + 1 },
          ext: { width: finalW, height: finalH },
          editAs: "oneCell",
        });

        return bgEnd + gapRowsAfter;

      };


      // =========================================================
      // ✅ TAB 1: SKU Profitability  (SWAPPED → now first sheet)
      // =========================================================
      const wsSku = wb.addWorksheet("SKU Profitability");

      if (skuExportPayload) {
        const {
          brandName,
          companyName,
          currencySymbol,
          title,
          periodLabel,
          tableData,
          totals,
          countryName,
        } = skuExportPayload;

        // -------- Meta rows --------
        wsSku.addRow([brandName || "N/A"]);
        wsSku.addRow([companyName || "N/A"]);
        wsSku.addRow([`${title || "Profit Breakup (SKU Level)"} - ${periodLabel}`]);
        wsSku.addRow([`Currency: ${currencySymbol}`]);
        wsSku.addRow([`Country: ${(countryName || "").toUpperCase()}`]);
        wsSku.addRow([`Platform: Amazon`]);
        wsSku.addRow([""]);

        // -------- Columns (same order as your screenshot) --------
        const columns = [
          { key: "product_name", header: "Product Name" }, // A
          { key: "quantity", header: "Quantity Sold" }, // B
          { key: "asp", header: "ASP" }, // C
          { key: "product_sales", header: "Gross Sale" }, // D
          { key: "net_sales", header: "Net Sales" }, // E
          { key: "cost_of_unit_sold", header: "Cost of Go" }, // F
          { key: "amazon_fee", header: "Amazon Fe" }, // G
          { key: "selling_fees", header: "Selling Fee" }, // H
          { key: "fba_fees", header: "FBA fees" }, // I
          { key: "net_credits", header: "Net Credits" }, // J
          { key: "net_taxes", header: "Net Taxes" }, // K
          { key: "profit", header: "CM1 Profit" }, // L
          { key: "profit_percentage", header: "CM1 Profit (%)" }, // M
          { key: "unit_wise_profitability", header: "CM1 Profit per Unit" }, // N
        ] as const;

        // Header row
        wsSku.addRow(columns.map((c) => c.header));

        // Signage row
        wsSku.addRow([
          "",
          "",
          "",
          "",
          "(+)", // Net Sales
          "(-)", // COGS
          "(-)", // Amazon
          "(-)", // Selling
          "(-)", // FBA
          "(+)", // Net Credits
          "",
          "",
          "",
          "",
        ]);

        // Safe name fallback
        const safeName = (r: any) => {
          const name = r?.product_name;
          const sku = r?.sku;
          return name !== undefined && name !== null && String(name).trim() !== "" && String(name) !== "0"
            ? String(name)
            : sku ?? "-";
        };

        // Data rows
        // Data rows
        for (const r of tableData) {
          wsSku.addRow([
            safeName(r),
            r.quantity ?? "",
            r.asp ?? r.ASP ?? "",
            r.product_sales ?? "",
            r.net_sales ?? r.Net_Sales ?? "",
            r.cost_of_unit_sold ?? "",
            r.amazon_fee ?? "",
            r.selling_fees ?? "",
            r.fba_fees ?? "",
            r.net_credits ?? "",
            r.net_taxes ?? "",
            r.profit ?? r.Profit ?? "",
            typeof r.profit_percentage === "number" ? r.profit_percentage / 100 : "",
            r.unit_wise_profitability ?? "",
          ]);
        }

        // ✅ spacer row (blank line)
        wsSku.addRow([""]);







        // Summary rows: label in A, value in K (index 10)
        const putSummary = (label: string, value: any) => {
          const row = new Array(14).fill("");
          row[0] = label;
          row[10] = value; // Column K
          wsSku.addRow(row);
        };

        putSummary("Cost of Advertisement (-)", Math.abs(Number(totals?.advertising_total || 0)));
        if ((countryName || "").toLowerCase() === "us" || (countryName || "").toLowerCase() === "global") {
          putSummary("Shipment Charges (-)", Math.abs(Number(totals?.shipment_charges || 0)));
        }
        putSummary("Platform Fees (-)", Math.abs(Number(totals?.platform_fee || 0)));
        putSummary("CM2 Profit/Loss", Math.abs(Number(totals?.cm2_profit || 0)));
        putSummary("CM2 Margins", Number(totals?.cm2_margins || 0) / 100);
        putSummary("TACoS (Total Advertising Cost of Sale)", Number(totals?.acos || 0) / 100);
        putSummary("Net Reimbursement during the month", Math.abs(Number(totals?.rembursement_fee || 0)));
        putSummary("Reimbursement vs CM2 Margins", Number(totals?.rembursment_vs_cm2_margins || 0) / 100);
        putSummary("Reimbursement vs Sales", Number(totals?.reimbursement_vs_sales || 0) / 100);

        // Formatting
        wsSku.getColumn(2).numFmt = "#,##0"; // Quantity

        // Money columns D..L + N  => 4..12 and 14
        [4, 5, 6, 7, 8, 9, 10, 11, 12, 14].forEach((c) => {
          wsSku.getColumn(c).numFmt = "#,##0.00";
        });

        // Percent col M
        wsSku.getColumn(13).numFmt = "0.00%";
      } else {
        wsSku.addRow(["SKU data not available"]);
      }

      // =========================================================
      // ✅ TAB 2: All Graphs (SWAPPED → now second sheet)
      // =========================================================
      const wsGraphs = wb.addWorksheet("All Graphs");
      wsGraphs.views = [{ showGridLines: false }];

      let rowCursor = 1;

      rowCursor = await addChartBlock(
        wsGraphs,
        wb,
        chartExportApi?.title || "Profitability Chart",
        chartExportApi?.getChartBase64?.(),
        rowCursor,
        { width: 1000, pad: 2, bgCols: 30 }
      );

      rowCursor = await addChartBlock(wsGraphs, wb, "Expense Breakdown (Pie Chart)", expenseBreakdownPieBase64, rowCursor,
        { width: 650, bgCols: 30, scale: 2, skipCrop: true } // ✅
      );

      rowCursor = await addChartBlock(wsGraphs, wb, "Product Wise CM1 Breakdown (Pie Chart)", productWiseCm1PieBase64, rowCursor,
        { width: 650, bgCols: 30, scale: 2, skipCrop: true } // ✅
      );


      const buffer = await wb.xlsx.writeBuffer();
      saveAs(
        new Blob([buffer], {
          type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }),
        "Profitability_Bundle.xlsx"
      );
    } catch (e) {
      console.error("Combined export failed:", e);
    }
  };



  useEffect(() => {
    if (range === "monthly") {
      setAllDropdownsSelected(!!selectedMonth && !!selectedYear);
    } else if (range === "quarterly") {
      setAllDropdownsSelected(!!selectedQuarter && !!selectedYear);
    } else if (range === "yearly") {
      setAllDropdownsSelected(!!selectedYear);
    } else {
      setAllDropdownsSelected(false);
    }
  }, [range, selectedMonth, selectedQuarter, selectedYear]);

  useEffect(() => {
    if (typeof document === "undefined") return;

    const body = document.body;

    if (showNoDataOverlay) {
      body.style.overflow = "hidden"; // lock both X/Y scroll
    } else {
      body.style.overflow = ""; // restore default
    }

    return () => {
      body.style.overflow = ""; // cleanup on unmount
    };
  }, [showNoDataOverlay]);

  const goBack = () => router.push("/pnl-dashboard/QTD/global/NA/NA");

  if (month === "NA" || year === "NA") {
    return <IntegrationDashboard />;
  }

  /* 🌟 Initial fullscreen loader for this page */
  const hasAnyContent = !!uploadsData?.summary;
  const initialLoading = loading && !hasAnyContent;



  if (initialLoading) {
    return (
      <Loader
        src="/infinity-unscreen.gif"
        label="Loading financial metrics…"
        fullscreen
        size={120}
        roundedClass="rounded-none"
        backgroundClass="bg-neutral-900/60"
        respectReducedMotion
      />
    );
  }

  // 🔹 4) TITLE HELPERS FOR THE OVERLAY
  const capitalizeFirstLetter = (str: string) =>
    str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();

  const convertToAbbreviatedMonth = (m?: string) =>
    m ? capitalizeFirstLetter(m).slice(0, 3) : "";

  const getTitle = () => {
    if (range === "quarterly" && selectedQuarter) {
      return `${capitalizeFirstLetter(
        range
      )} Tracking Profitability - ${selectedQuarter}'${String(selectedYear).slice(
        -2
      )}`;
    }
    if (range === "monthly" && selectedMonth) {
      return `${capitalizeFirstLetter(
        range
      )} Tracking Profitability - ${convertToAbbreviatedMonth(
        selectedMonth
      )} ${selectedYear}`;
    }
    return `${capitalizeFirstLetter(range)} Tracking Profitability - ${selectedYear}`;
  };


const getCountryLabel = () => {
  const c = (countryName || "").toLowerCase();
  return c === "global" ? "GLOBAL" : (countryName || "").toUpperCase();
};

const getPeriodLabelShort = () => {
  const yy = String(selectedYear || "").slice(-2);

  if (range === "monthly" && selectedMonth && selectedYear) {
    return `${convertToAbbreviatedMonth(selectedMonth)}'${yy}`;
  }
  if (range === "quarterly" && selectedQuarter && selectedYear) {
    return `${selectedQuarter}'${yy}`;
  }
  if (range === "yearly" && selectedYear) {
    return String(selectedYear);
  }
  return "";
};

const getPnLTitleParts = () => {
  return {
    country: getCountryLabel(),
    period: getPeriodLabelShort(),
  };
};


  


  return (
    <div ref={layoutRef} className="space-y-6 relative">

      <div className="w-full flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        {/* LEFT: Title + Subtitle */}
        <div className="flex flex-col leading-tight w-full md:w-auto">
          <div className="flex items-baseline gap-2">
            <PageBreadcrumb
              pageTitle="Financial Metrics -"
              variant="page"
              align="left"
              textSize="2xl"
            />

            <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
              {countryName?.toLowerCase() === "global"
                ? "Global"
                : countryName?.toUpperCase()}
            </span>
          </div>

          <p className="text-xs 2xl:text-sm text-charcoal-500 mt-1">
            Track your profitability and key metrics
          </p>
        </div>

        {/* RIGHT: Filters */}
        <div className="flex w-full md:w-auto justify-start md:justify-end">
          <PeriodFiltersTable
            range={range === "" ? "yearly" : (range as "monthly" | "quarterly" | "yearly")}
            selectedMonth={selectedMonth}
            selectedQuarter={selectedQuarter || ""}
            selectedYear={selectedYear}
            yearOptions={yearOptions}
            onRangeChange={handleRangeChange}
            onMonthChange={handleMonthChange}
            onQuarterChange={handleQuarterChange}
            onYearChange={handleYearChange}
          />
        </div>
      </div>

      {/* WRAPPER: stacked layout */}
      <div className="flex flex-col gap-5 w-full">

        {/* Summary Cards */}
        {uploadsData?.summary &&
          (() => {
            const summary = displayData;
            const netSales = summary.total_sales;


            // ✅ comparisons (camelCase OR snake_case)
            const rawComparisons =
              (uploadsData as any).summaryComparisons ??
              (uploadsData as any).summary_comparisons;

            const comparisons: SummaryComparisons | undefined = rawComparisons
              ? (rawComparisons as SummaryComparisons)
              : undefined;

            const formatMoney = (val: number, opts?: { showPlus?: boolean }) => {
              const num = Number(val || 0);
              const sign = num < 0 ? "-" : opts?.showPlus && num > 0 ? "+" : "";
              const abs = Math.abs(num);

              return `${sign}${currencySymbol}${abs.toLocaleString(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              })}`;
            };


            // ✅ Cost of Ads
            const costOfAds = summary.advertising_total ?? 0;

            // ✅ "ROAS" as you defined: (Cost of Ads / Net Sales) * 100
            const getRoas = (s?: Summary) => {
              const ns = s?.total_sales ?? 0;            // net sales
              const ads = s?.advertising_total ?? 0;     // cost of ads
              return ns > 0 ? (ads / ns) * 100 : 0;
            };


            const roas = getRoas(summary);

            const formatRoas = (val: number) =>
              `${val.toLocaleString(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              })}%`;


            const renderTacosComparisons = () => {
              const yNum = Number(selectedYear);

              const label =
                range === "monthly"
                  ? selectedMonth && yNum
                    ? getPrevMonthLabel(selectedMonth, yNum)
                    : "Prev month"
                  : range === "quarterly"
                    ? selectedQuarter && yNum
                      ? getPrevQuarterLabel(selectedQuarter as Quarter, yNum)
                      : "Prev quarter"
                    : yNum
                      ? getPrevYearLabel(yNum)
                      : "Prev year";

              const prevVal =
                range === "monthly"
                  ? comparisons?.lastMonth
                    ? getRoas(comparisons.lastMonth)
                    : undefined
                  : range === "quarterly"
                    ? comparisons?.lastQuarter
                      ? getRoas(comparisons.lastQuarter)
                      : undefined
                    : comparisons?.lastYear
                      ? getRoas(comparisons.lastYear)
                      : undefined;

              const hasPrev = typeof prevVal === "number" && !isNaN(prevVal);

              const delta = hasPrev ? roas - prevVal! : null;

              const deltaColor =
                typeof delta === "number"
                  ? delta > 0
                    ? "text-red-600"        // higher TACoS = worse
                    : delta < 0
                      ? "text-emerald-600"  // lower TACoS = better
                      : "text-gray-400"
                  : "text-gray-400";

              // delta = current - prev
              const arrow =
                typeof delta === "number"
                  ? delta > 0
                    ? "▼" // ✅ TACoS increased (bad) -> show DOWN
                    : delta < 0
                      ? "▲" // ✅ TACoS decreased (good) -> show UP
                      : ""
                  : "";


              const formatDelta = (v: number) =>
                `${Math.abs(v).toLocaleString(undefined, {
                  minimumFractionDigits: 2,
                  maximumFractionDigits: 2,
                })}%`;

              return (
                <div className="mt-3 space-y-1.5">
                  <div className="flex items-end justify-between text-charcoal-500 gap-3 text-[10px] 2xl:text-xs leading-tight tabular-nums">
                    <div className="min-w-0">
                      <div className="whitespace-nowrap">
                        {label}:
                      </div>
                      <div className="whitespace-nowrap">
                        {hasPrev ? formatRoas(prevVal!) : "-"}
                      </div>
                    </div>

                    <span className={`font-bold whitespace-nowrap ${deltaColor}`}>
                      {typeof delta === "number" ? (
                        <>
                          {arrow} {formatDelta(delta)}
                        </>
                      ) : (
                        "-"
                      )}
                    </span>
                  </div>
                </div>
              );
            };



            const formatUnits = (val: number) =>
              val.toLocaleString(undefined, { maximumFractionDigits: 0 });

            const formatPercent = (val: number) =>
              `${val.toLocaleString(undefined, {
                minimumFractionDigits: 0,
                maximumFractionDigits: 2,
              })}%`;

            const getGrossSales = (s?: Summary) =>
              s?.total_product_sales ?? s?.gross_sales ?? 0;

            const isSummaryZero =
              summary.unit_sold === 0 &&
              summary.total_sales === 0 &&
              summary.total_expense === 0 &&
              summary.cm2_profit === 0;

            // const cm2Percent =
            //   summary.total_sales > 0 ? (summary.cm2_profit / summary.total_sales) * 100 : 0;

            const cm2Percent =
              netSales > 0 ? (summary.cm2_profit / netSales) * 100 : 0;

            // ---------- generic comparisons helper ----------
            const getComparisons = (metric: keyof Summary): ComparisonItem[] => {
              const current = summary[metric] ?? 0;

              const lm = comparisons?.lastMonth?.[metric];
              const lq = comparisons?.lastQuarter?.[metric];
              const ly = comparisons?.lastYear?.[metric];

              const makeItem = (label: string, prevVal?: number): ComparisonItem => {
                if (typeof prevVal !== "number") return { label, value: undefined, diffPct: null };
                const diffPct = prevVal === 0 ? null : ((current - prevVal) / prevVal) * 100;
                return { label, value: prevVal, diffPct };
              };

              const yNum = Number(selectedYear);

              if (range === "monthly") {
                const label = selectedMonth && yNum ? getPrevMonthLabel(selectedMonth, yNum) : "Prev month";
                return [makeItem(label, lm)];
              }

              if (range === "quarterly") {
                const label =
                  selectedQuarter && yNum
                    ? getPrevQuarterLabel(selectedQuarter as Quarter, yNum)
                    : "Prev quarter";
                return [makeItem(label, lq)];
              }

              if (range === "yearly") {
                const label = yNum ? getPrevYearLabel(yNum) : "Prev year";
                return [makeItem(label, ly)];
              }

              return [];
            };

            const renderComparisons = (metric: keyof Summary, formatter: (val: number) => string) => {
              const items = getComparisons(metric);
              if (!items.length) return null;

              return (
                <div className="mt-3 space-y-2">
                  {items.map((item) => {
                    const hasValue = typeof item.value === "number" && !isNaN(item.value);
                    const hasDiff = typeof item.diffPct === "number" && !isNaN(item.diffPct);

                    const diffClass = hasDiff
                      ? item.diffPct! >= 0
                        ? "text-emerald-600"
                        : "text-red-600"
                      : "text-gray-400";

                    return (
                      <div
                        key={item.label}
                        className="flex items-end text-charcoal-500 justify-between gap-3 text-[10px] 2xl:text-xs leading-tight tabular-nums"
                      >
                        <div className="min-w-0">
                          <div className=" whitespace-nowrap">
                            {item.label}:
                          </div>
                          <div className=" whitespace-nowrap">
                            {hasValue ? formatter(item.value!) : "-"}
                          </div>
                        </div>

                        <div className={`font-bold whitespace-nowrap ${diffClass}`}>
                          {hasDiff ? (
                            <>
                              {item.diffPct! >= 0 ? "▲" : "▼"}{" "}
                              {Math.abs(item.diffPct!).toFixed(2)}%
                            </>
                          ) : (
                            "-"
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              );
            };

            const pickNum = (obj: any, keys: string[]) => {
              for (const k of keys) {
                const v = obj?.[k];
                if (v === 0) return 0;
                if (typeof v === "number" && !isNaN(v)) return v;
                if (typeof v === "string" && v.trim() !== "" && !isNaN(Number(v))) return Number(v);
              }
              return 0;
            };


            // ---------- Gross Sales comparisons ----------
            const getGrossSalesComparisons = (): ComparisonItem[] => {
              const current = getGrossSales(summary);
              const yNum = Number(selectedYear);

              const prevMonth = comparisons?.lastMonth ? getGrossSales(comparisons.lastMonth) : undefined;
              const prevQuarter = comparisons?.lastQuarter ? getGrossSales(comparisons.lastQuarter) : undefined;
              const prevYear = comparisons?.lastYear ? getGrossSales(comparisons.lastYear) : undefined;

              const makeItem = (label: string, prevVal?: number): ComparisonItem => {
                if (typeof prevVal !== "number") return { label, value: undefined, diffPct: null };
                const diffPct = prevVal === 0 ? null : ((current - prevVal) / prevVal) * 100;
                return { label, value: prevVal, diffPct };
              };

              if (range === "monthly") {
                const label = selectedMonth && yNum ? getPrevMonthLabel(selectedMonth, yNum) : "Prev month";
                return [makeItem(label, prevMonth)];
              }

              if (range === "quarterly") {
                const label =
                  selectedQuarter && yNum
                    ? getPrevQuarterLabel(selectedQuarter as Quarter, yNum)
                    : "Prev quarter";
                return [makeItem(label, prevQuarter)];
              }

              if (range === "yearly") {
                const label = yNum ? getPrevYearLabel(yNum) : "Prev year";
                return [makeItem(label, prevYear)];
              }

              return [];
            };

            const renderGrossSalesComparisons = () => {
              const items = getGrossSalesComparisons();
              if (!items.length) return null;
              return (
                <div className="mt-3 space-y-2">
                  {items.map((item) => {
                    const hasValue = typeof item.value === "number" && !isNaN(item.value);
                    const hasDiff = typeof item.diffPct === "number" && !isNaN(item.diffPct);

                    const diffClass = hasDiff
                      ? item.diffPct! >= 0
                        ? "text-emerald-600"
                        : "text-red-600"
                      : "text-gray-400";

                    return (
                      <div
                        key={item.label}
                        className="flex items-end text-charcoal-500 justify-between gap-3 text-[10px] 2xl:text-xs leading-tight tabular-nums"
                      >
                        <div className="min-w-0">
                          <div className="whitespace-nowrap">
                            {item.label}:
                          </div>
                          <div className="whitespace-nowrap">
                            {hasValue ? formatMoney(item.value!) : "-"}
                          </div>
                        </div>

                        <div className={`font-bold whitespace-nowrap ${diffClass}`}>
                          {hasDiff ? (
                            <>
                              {item.diffPct! >= 0 ? "▲" : "▼"}{" "}
                              {Math.abs(item.diffPct!).toFixed(2)}%
                            </>
                          ) : (
                            "-"
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              );
            };

            // ---------- CM2% comparisons ----------
            const getCm2Percent = (s?: Summary) =>
              s && s.total_sales > 0 ? (s.cm2_profit / s.total_sales) * 100 : 0;
            const renderCm2PercentComparisons = () => {
              const yNum = Number(selectedYear);

              const label =
                range === "monthly"
                  ? selectedMonth && yNum
                    ? getPrevMonthLabel(selectedMonth, yNum)
                    : "Prev month"
                  : range === "quarterly"
                    ? selectedQuarter && yNum
                      ? getPrevQuarterLabel(selectedQuarter as Quarter, yNum)
                      : "Prev quarter"
                    : yNum
                      ? getPrevYearLabel(yNum)
                      : "Prev year";

              const prevVal =
                range === "monthly"
                  ? comparisons?.lastMonth
                    ? getCm2Percent(comparisons.lastMonth)
                    : undefined
                  : range === "quarterly"
                    ? comparisons?.lastQuarter
                      ? getCm2Percent(comparisons.lastQuarter)
                      : undefined
                    : comparisons?.lastYear
                      ? getCm2Percent(comparisons.lastYear)
                      : undefined;

              const hasPrev = typeof prevVal === "number" && !isNaN(prevVal);

              const diffPct =
                hasPrev && prevVal !== 0 ? ((cm2Percent - prevVal) / prevVal) * 100 : null;

              const diffClass =
                typeof diffPct === "number"
                  ? diffPct >= 0
                    ? "text-emerald-600"
                    : "text-red-600"
                  : "text-gray-400";

              return (
                <div className="mt-3 space-y-1.5">
                  <div className="flex items-end text-charcoal-500 justify-between gap-3 text-[10px] 2xl:text-xs leading-tight tabular-nums">
                    <div className="min-w-0">
                      <div className="whitespace-nowrap">
                        {label}:
                      </div>
                      <div className="whitespace-nowrap">
                        {hasPrev ? formatPercent(prevVal!) : "-"}
                      </div>
                    </div>

                    <span className={`font-bold whitespace-nowrap ${diffClass}`}>
                      {typeof diffPct === "number" ? (
                        <>
                          {diffPct >= 0 ? "▲" : "▼"} {Math.abs(diffPct).toFixed(1)}%
                        </>
                      ) : (
                        "-"
                      )}
                    </span>
                  </div>
                </div>
              );
            };


            return (
              <div
                className={[
                  "w-full grid gap-4",
                  "grid-cols-2 sm:grid-cols-4 2xl:grid-cols-8",
                  isSummaryZero ? "opacity-30" : "opacity-100",
                ].join(" ")}
              >
                {/* Units */}
                <div className="w-full rounded-2xl border border-[#FDD36F] bg-[#FDD36F4D] shadow-sm px-4 py-3 flex flex-col justify-between">
                  <div className="flex justify-between items-center mb-2">
                    <span className="text-[10px] 2xl:text-xs text-charcoal-500">Units</span>
                  </div>
                  <div className="text-sm 2xl:text-lg font-semibold text-charcoal-500 leading-tight tabular-nums">
                    {formatUnits(summary.unit_sold)}
                  </div>
                  {renderComparisons("unit_sold", formatUnits)}
                </div>

                <div className="w-full rounded-2xl border border-[#ED9F50] bg-[#ED9F504D] shadow-sm px-4 py-3 flex flex-col justify-between">
                  <div className="flex justify-between items-center mb-2">
                    <span className="text-[10px] 2xl:text-xs text-charcoal-500">Gross Sales</span>
                  </div>
                  <div className="text-sm 2xl:text-lg font-semibold text-charcoal-500 leading-tight tabular-nums">
                    {formatMoney(getGrossSales(summary))}
                  </div>
                  {renderGrossSalesComparisons()}
                </div>

                {/* Net Sales */}
                <div className="w-full rounded-2xl border border-[#75BBDA] bg-[#75BBDA4D] shadow-sm px-4 py-3 flex flex-col justify-between">
                  <span className="text-[10px] 2xl:text-xs text-charcoal-500">Net Sales</span>
                  <div className="text-sm 2xl:text-lg font-semibold text-charcoal-500 leading-tight tabular-nums">
                    {formatMoney(netSales)}
                  </div>
                  {renderComparisons("total_sales", formatMoney)}
                </div>


                {/* Expenses */}
                <div className="w-full rounded-2xl border border-[#B75A5A] bg-[#B75A5A4D] shadow-sm px-4 py-3 flex flex-col justify-between">
                  <div className="flex justify-between items-center mb-2">
                    <span className="text-[10px] 2xl:text-xs text-charcoal-500">Expenses</span>
                  </div>
                  <div className="text-sm 2xl:text-lg font-semibold text-charcoal-500 leading-tight tabular-nums">
                    {formatMoney(summary.total_expense)}
                  </div>
                  {renderComparisons("total_expense", formatMoney)}
                </div>

                {/* Cost of Advertisement */}
                <div className="w-full rounded-2xl border border-[#C49466] bg-[#C494664D] shadow-sm px-4 py-3 flex flex-col justify-between">
                  <div className="flex justify-between items-center mb-2">
                    <span className="text-[10px] 2xl:text-xs text-charcoal-500">Cost of Advertisement</span>
                  </div>
                  <div className="text-sm 2xl:text-lg font-semibold text-charcoal-500 leading-tight tabular-nums">
                    {formatMoney(costOfAds)}
                  </div>
                  {renderComparisons("advertising_total", formatMoney)}
                </div>

                {/* ROAS */}
                <div className="w-full rounded-2xl border border-[#3A8EA4] bg-[#3A8EA44D] shadow-sm px-4 py-3 flex flex-col justify-between">
                  <div className="flex justify-between items-center mb-2">
                    <span className="text-[10px] 2xl:text-xs text-charcoal-500">TACoS</span>
                  </div>
                  <div className="text-sm 2xl:text-lg font-semibold text-charcoal-500 leading-tight tabular-nums">
                    {formatRoas(roas)}
                  </div>

                  {renderTacosComparisons()}
                </div>


                {/* CM2 Profit */}
                <div className="w-full rounded-2xl border border-[#B8C78C] bg-[#B8C78C4D] shadow-sm px-4 py-3 flex flex-col justify-between">
                  <div className="flex justify-between items-center mb-2">
                    <span className="text-[10px] 2xl:text-xs text-charcoal-500">CM2 Profit</span>
                  </div>
                  <div className="text-sm 2xl:text-lg font-semibold text-charcoal-500 leading-tight tabular-nums">
                    {formatMoney(summary.cm2_profit)}
                  </div>
                  {renderComparisons("cm2_profit", formatMoney)}
                </div>

                {/* CM2 Profit % */}
                <div className="w-full rounded-2xl border border-[#7B9A6D] bg-[#7B9A6D4D] shadow-sm px-4 py-3 flex flex-col justify-between">
                  <div className="flex justify-between items-center mb-2">
                    <span className="text-[10px] 2xl:text-xs text-charcoal-500">CM2 Profit %</span>
                  </div>
                  <div className="text-sm 2xl:text-lg font-semibold text-charcoal-500 leading-tight tabular-nums">
                    {formatPercent(cm2Percent)}
                  </div>
                  {renderCm2PercentComparisons()}
                </div>
              </div>
            );
          })()}

      </div>


      {/* Profitability header + bundle download */}
      {allDropdownsSelected && (
        <div className="flex items-center justify-between gap-3">
          <div className="flex items-baseline gap-2">
            <PageBreadcrumb
              pageTitle="P&L - Amazon"
              variant="page"
              align="left"
              textSize="2xl"
            />

            <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
              {getPnLTitleParts().country}
            </span>

            {getPnLTitleParts().period ? (
              <>
                <span className="text-charcoal-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">-</span>
                <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
                  {getPnLTitleParts().period}
                </span>
              </>
            ) : null}
          </div>

          <DownloadIconButton
            onClick={handleDownloadProfitabilityBundle}
            disabled={
              !chartExportApi ||
              !skuExportPayload ||
              !expenseBreakdownPieBase64 ||
              !productWiseCm1PieBase64
            }
          />
        </div>
      )}


      {/* Charts & Tables */}
      {range === "monthly" && selectedMonth && selectedYear && (
        <>
          <div className="w-full rounded-2xl border border-slate-200 bg-white shadow-sm p-4 sm:p-5">
            <Bargraph
              range={range}
              selectedMonth={selectedMonth}
              selectedYear={selectedYear}
              countryName={initialCountryName}
              homeCurrency={globalHomeCurrency}
              hideDownloadButton
              onExportApiReady={setChartExportApi}
              onNoDataChange={(noData) => {
                console.log("🔥 [Monthly] Bargraph → onNoDataChange:", noData);
                setShowNoDataOverlay(noData);
              }}
            />
          </div>

{allDropdownsSelected && (
  <AiSingleInsightCard
    loading={aiPanelLoading}
    error={aiPanelError}
    summaryBullets={aiPanel?.summaryBullets ?? []}
    recommendationBullets={aiPanel?.recommendationBullets ?? []}
    skuInsightsBullets={aiPanel?.skuInsightsBullets ?? []}
    inventoryBullets={aiPanel?.inventoryBullets ?? []}
  />
)}


          <div className="flex flex-wrap justify-between gap-6 md:gap-4 mb-4">
            <div className="flex-1 min-w-[300px]">
              <CircleChart
                range={range}
                month={selectedMonth}
                year={selectedYear}
                countryName={initialCountryName}
                homeCurrency={globalHomeCurrency}
                onExportBase64Ready={setExpenseBreakdownPieBase64}
              />
            </div>
            <div className="flex-1 min-w-[300px]">
              <CMchartofsku
                range={range}
                month={selectedMonth}
                year={selectedYear}
                countryName={initialCountryName}
                homeCurrency={globalHomeCurrency}
                onExportBase64Ready={setProductWiseCm1PieBase64}
              />
            </div>
          </div>
          <SKUtable
            range={range}
            month={selectedMonth}
            year={selectedYear}
            countryName={initialCountryName}
            homeCurrency={globalHomeCurrency}
            hideDownloadButton
            onExportPayloadChange={setSkuExportPayload}
          />
        </>
      )}

      {range === "quarterly" && isQuarter(selectedQuarter) && selectedYear && (
        <>
          <div className="w-full rounded-2xl border border-slate-200 bg-white shadow-sm p-4 sm:p-5">
            <GraphPage
              range={range}
              selectedQuarter={selectedQuarter}
              selectedYear={selectedYear}
              countryName={initialCountryName}
              homeCurrency={globalHomeCurrency}
              hideDownloadButton
              onExportApiReady={setChartExportApi}
              onNoDataChange={(noData) => {
                console.log("🔥 [Quarterly] GraphPage → onNoDataChange:", noData);
                setShowNoDataOverlay(noData);
              }}
            />
          </div>

{allDropdownsSelected && (
  <AiSingleInsightCard
    loading={aiPanelLoading}
    error={aiPanelError}
    summaryBullets={aiPanel?.summaryBullets ?? []}
    recommendationBullets={aiPanel?.recommendationBullets ?? []}
    skuInsightsBullets={aiPanel?.skuInsightsBullets ?? []}
    inventoryBullets={aiPanel?.inventoryBullets ?? []}
  />
)}


          <div className="flex flex-wrap justify-between gap-6 md:gap-4">
            <div className="flex-1 min-w-[300px]">
              <CircleChart
                range={range}
                selectedQuarter={selectedQuarter}
                year={selectedYear}
                countryName={initialCountryName}
                homeCurrency={globalHomeCurrency}
                onExportBase64Ready={setExpenseBreakdownPieBase64}
              />
            </div>
            <div className="flex-1 min-w-[300px]">
              <CMchartofsku
                range={range}
                selectedQuarter={selectedQuarter}
                year={selectedYear}
                countryName={initialCountryName}
                homeCurrency={globalHomeCurrency}
                onExportBase64Ready={setProductWiseCm1PieBase64}
              />
            </div>
          </div>
          <SKUtable
            range={range}
            quarter={selectedQuarter}
            year={selectedYear}
            countryName={initialCountryName}
            homeCurrency={globalHomeCurrency}
            hideDownloadButton
            onExportPayloadChange={setSkuExportPayload}
          />
        </>
      )}

      {range === "yearly" && selectedYear && (
        <>
          <GraphPage
            range={range}
            selectedYear={selectedYear}
            countryName={initialCountryName}
            homeCurrency={globalHomeCurrency}
            hideDownloadButton
            onExportApiReady={setChartExportApi}
            onNoDataChange={(noData) => {
              console.log("🔥 [Yearly] GraphPage → onNoDataChange:", noData);
              setShowNoDataOverlay(noData);
            }}
          />

          {allDropdownsSelected && (
  <AiSingleInsightCard
    loading={aiPanelLoading}
    error={aiPanelError}
    summaryBullets={aiPanel?.summaryBullets ?? []}
    recommendationBullets={aiPanel?.recommendationBullets ?? []}
    skuInsightsBullets={aiPanel?.skuInsightsBullets ?? []}
    inventoryBullets={aiPanel?.inventoryBullets ?? []}
  />
)}

          <div className="flex flex-wrap justify-between gap-6 md:gap-4">
            <div className="flex-1 min-w-[300px]">
              <CircleChart
                range={range}
                year={selectedYear}
                countryName={initialCountryName}
                homeCurrency={globalHomeCurrency}
                onExportBase64Ready={setExpenseBreakdownPieBase64}
              />
            </div>
            <div className="flex-1 min-w-[300px]">
              <CMchartofsku
                range={range}
                year={selectedYear}
                countryName={initialCountryName}
                homeCurrency={globalHomeCurrency}
                onExportBase64Ready={setProductWiseCm1PieBase64}
              />
            </div>
          </div>
          <SKUtable
            range={range}
            year={selectedYear}
            countryName={initialCountryName}
            homeCurrency={globalHomeCurrency}
            hideDownloadButton
            onExportPayloadChange={setSkuExportPayload}
          />

        </>
      )}

      {showNoDataOverlay && (
        <div
          className="fixed inset-y-0 z-[9999] flex items-center justify-center pointer-events-none"
          style={{ left: overlayBounds.left, width: overlayBounds.width || "100%" }}
        >
          <div className="bg-white border border-[#D9D9D9] rounded-xl shadow-xl p-6 max-w-lg w-[90%] text-center pointer-events-auto">
            {/* Lock icon */}
            <div className="mb-4 flex items-center justify-center">
              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-[#D9D9D9]">
                <IoMdLock className="text-green-500 text-2xl" />
              </div>
            </div>

            <PageBreadcrumb
              pageTitle="No Data Available"
              variant="table"
              align="center"
              textSize="2xl"
            />

            <p className="text-charcoal-500 text-xs sm:text-sm leading-relaxed my-4">
              To see performance metrics, you need to upload more files for
              <span className="block mt-0.5">{getTitle()}</span>
            </p>

            {/* {countryName.toLowerCase() !== "global" && (
              <Button
                variant="primary"
                size="sm"
                onClick={() => setShowUploadModal(true)}
                className="mt-1 inline-flex items-center justify-center text-sm font-medium"
              >
                Upload MTD(s)
              </Button>
            )} */}
          </div>
        </div>
      )}

      <Modal
        isOpen={showUploadModal}
        onClose={() => setShowUploadModal(false)}
        className="max-w-3xl w-[90vw] mx-auto p-0 shadow-[6px_6px_7px_0px_#00000026] border border-[#D9D9D9]"
        showCloseButton
      >
        <div className="max-h-[85vh] overflow-y-auto">
          <FileUploadForm
            initialCountry={initialCountryName}
            onClose={() => setShowUploadModal(false)}
            onComplete={() => {
              setShowUploadModal(false);
              fetchUploadHistory(
                range,
                selectedMonth,
                selectedQuarter || "",
                selectedYear,
                initialCountryName
              );
            }}
          />
        </div>
      </Modal>
    </div>
  );
};

export default Dropdowns;
