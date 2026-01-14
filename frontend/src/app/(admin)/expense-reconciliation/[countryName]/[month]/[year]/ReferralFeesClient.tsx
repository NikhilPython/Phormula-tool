"use client";

import React, {
  useEffect,
  useMemo,
  useState,
  useRef,
  type JSX,
  useCallback,
} from "react";
import { useParams } from "next/navigation";
import MonthYearPickerTable from "@/components/filters/MonthYearPickerTable";
import { Doughnut } from "react-chartjs-2";
import { Chart as ChartJS, ArcElement, Tooltip, Legend } from "chart.js";
import { jwtDecode } from "jwt-decode";
import * as XLSX from "xlsx";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import DataTable, { ColumnDef, Row } from "@/components/ui/table/DataTable";
import Loader from "@/components/loader/Loader";
import DownloadButton from "@/components/ui/button/DownloadIconButton";
import { useHomeCurrencyContext } from "@/lib/hooks/useHomeCurrencyContext";
import { AiButton } from "@/components/ui/button/AiButton";
import PeriodFiltersTable, { type Range } from "@/components/filters/PeriodFiltersTable";
import GroupedDataTable from "@/components/ui/table/GroupedDataTable";

/* ===================== Overlap Plugin ===================== */
const overlapPlugin = {
  id: "overlapPlugin",
  afterDatasetsDraw(chart: any, _args: any, pluginOptions: any) {
    const opts = pluginOptions || {};
    const salesTotal = Math.max(0, Number(opts.salesTotal || 0));
    if (!salesTotal) return;

    const applicableValue = Math.max(0, Number(opts.applicableValue || 0));
    const overchargedValue = Math.max(0, Number(opts.overchargedValue || 0));

    const showApplicable = opts.showApplicable !== false;
    const showOvercharged = opts.showOvercharged !== false;

    if (!showApplicable && !showOvercharged) return;

    const meta = chart.getDatasetMeta(0);
    if (!meta?.data?.length) return;

    const maskArc = meta.data[0];
    const { x, y, innerRadius, outerRadius, startAngle, endAngle } = maskArc;

    const full = Math.PI * 2;
    const applAngleRaw = showApplicable ? (applicableValue / salesTotal) * full : 0;
    const overAngleRaw = showOvercharged ? (overchargedValue / salesTotal) * full : 0;

    const maskSpan = Math.max(0, endAngle - startAngle);
    const applAngle = Math.min(applAngleRaw, maskSpan);
    const overAngle = Math.min(overAngleRaw, Math.max(0, maskSpan - applAngle));

    const radius = (innerRadius + outerRadius) / 2;
    const thickness = outerRadius - innerRadius;

    const ctx = chart.ctx;
    ctx.save();
    ctx.lineWidth = thickness;
    ctx.lineCap = "butt";

    if (applAngle > 0) {
      ctx.beginPath();
      ctx.strokeStyle = "#14B8A6";
      ctx.arc(x, y, radius, startAngle, startAngle + applAngle);
      ctx.stroke();
    }

    if (overAngle > 0) {
      ctx.beginPath();
      ctx.strokeStyle = "#EF4444";
      ctx.arc(
        x,
        y,
        radius,
        startAngle + applAngle,
        startAngle + applAngle + overAngle
      );
      ctx.stroke();
    }

    ctx.restore();
  },
};

ChartJS.register(ArcElement, Tooltip, Legend, overlapPlugin);

/* ===================== ENV ===================== */
const baseURL = process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

/* ===================== Types ===================== */
type ReferralRow = Partial<{
  sku: string;
  product_name: string;
  category: string;
  asin: string;
  quantity: number | string;
  sales: number | string;
  product_sales: number | string;
  refRate: number | string;
  refFeesApplicable: number | string;
  refFeesCharged: number | string;
  overcharged: number | string;
  difference: number | string;
  errorstatus: string;
  selling_fees: number | string;
  answer: number | string;

  net_sales_total_value: number | string;
  status: string;
  total_value: number | string;
}>;

type Summary = {
  ordersUnits: number;
  totalSales: number;
  feeImpact: number;
};

type FeeSummaryRow = {
  label: string;
  units: number;
  sales: number;
  refFeesApplicable: number;
  refFeesCharged: number;
  overcharged: number;
};

type Card6Summary = {
  sales: number;
  units: number;
  productSales: number;
  totalFees: number;            // charged (sum of fee buckets)
  totalFeesApplicable: number;  // applicable (sum of fee buckets)

  refFeesApplied: number;       // charged
  refFeesApplicable: number;    // applicable

  fbaFees: number;              // charged
  fbaFeesApplicable: number;    // applicable

  platformFees: number;         // charged
  platformFeesApplicable: number; // applicable

  otherFees: number;            // charged
  otherFeesApplicable: number;  // applicable
};


type SalesStatusSummary = {
  totalSales: number;
  accurateSales: number;
  overchargedSales: number;
  underchargedSales: number;
  noRefFeeSales: number;
};

type RefFeesBreakdown = {
  totalApplicable: number; // Grand Total "answer"
  accurateApplicable: number; // Charge - Accurate "answer"
  overchargedApplicable: number; // Charge - Overcharged "answer"
  underchargedApplicable: number; // Charge - Undercharged "answer"
  noRefFeeApplicable: number; // Charge - noreferallfee "answer"
};


/* ===================== Helpers ===================== */
const fmtInteger = (n: number): string =>
  typeof n === "number"
    ? n.toLocaleString(undefined, {
      maximumFractionDigits: 0,
      minimumFractionDigits: 0,
    })
    : "-";

const toNumberSafe = (v: any): number => {
  if (v === null || v === undefined) return 0;
  if (typeof v === "number") return v;
  const num = Number(String(v).replace(/[, ]+/g, ""));
  return Number.isNaN(num) ? 0 : num;
};

const getNetSales = (r: any): number => {
  const net = toNumberSafe(r?.net_sales_total_value);
  if (net) return net;
  return toNumberSafe(r?.product_sales ?? r?.sales);
};

const currencyFromCountryName = (countryName: string) => {
  const c = (countryName || "").toLowerCase();
  if (c === "uk") return "GBP";
  if (c === "us") return "USD";
  // add more mappings later if needed
  return "USD";
};

const pctDelta = (current: number, previous: number) => {
  const prev = toNumberSafe(previous);
  if (prev === 0) return 0;
  return ((toNumberSafe(current) - prev) / prev) * 100;
};

const fmtPct = (p: number) => `${Math.abs(p).toFixed(2)}%`;


const pctOfSales = (value: number, sales: number) => {
  const s = toNumberSafe(sales);
  if (s <= 0) return 0;
  return (toNumberSafe(value) / s) * 100;
};

const fmtPctDelta = (p: number) =>
  `${Math.abs(toNumberSafe(p)).toFixed(2)}%`;

const fmtPctPlain = (p: number) => `${toNumberSafe(p).toFixed(2)}%`;



// --- Fee buckets helpers ---
const isSummaryLine = (sku: any) => {
  const s = String(sku ?? "");
  return s.startsWith("Charge -") || s === "Grand Total" || s.toLowerCase() === "total";
};

const sumByKey = (list: any[], key: string) =>
  list.reduce((acc, r) => acc + toNumberSafe(r?.[key]), 0);

// Customize these patterns to match your data keys/naming
const FBA_KEYS = ["fba_fees", "fulfillment_fees"];
const PLATFORM_KEYS = ["platform_fees", "marketplace_fees"];
const OTHER_KEYS = ["other_fees", "misc_fees"];


/* ===================== Donut Component ===================== */
type OverlapSalesDonutProps = {
  sales: number;
  applicable: number;
  charged: number;
  fmtCurrency: (n: number) => string;
};

/* ===================== Cards ===================== */

function StatCard({
  title,
  value,
  valueFmt,
  bottomLeftLabel,
  bottomLeftValue,
  bottomRightDeltaPct,

  borderColor = "#cbd5e1",
  bgColor = "#ffffff",
}: {
  title: string;
  value: number;
  valueFmt: (n: number) => string;

  bottomLeftLabel?: string;
  bottomLeftValue?: number;
  bottomRightDeltaPct?: number;

  borderColor?: string;
  bgColor?: string;
}) {
  const showBottom =
    bottomLeftLabel !== undefined ||
    bottomLeftValue !== undefined ||
    bottomRightDeltaPct !== undefined;

  const d = typeof bottomRightDeltaPct === "number" ? bottomRightDeltaPct : null;
  const isUp = (d ?? 0) > 0;
  const isDown = (d ?? 0) < 0;

  return (
    <div
      className={`rounded-2xl border shadow-sm px-4 py-3 ${showBottom ? "min-h-[110px] flex flex-col" : ""
        }`}
      style={{ borderColor, backgroundColor: bgColor }}
    >
      <p className="text-xs font-semibold text-charcoal-500">{title}</p>

      <p className="mt-1 text-base font-bold text-charcoal-500">
        {valueFmt(toNumberSafe(value))}
      </p>

      {showBottom && (
        <div className="mt-auto flex items-end justify-between gap-3 pt-2">
          <div className="text-left">
            <p className="text-[11px] sm:text-xs text-slate-600">
              {bottomLeftLabel ?? ""}
            </p>
            <p className="text-[11px] sm:text-xs font-semibold text-charcoal-500">
              {bottomLeftValue !== undefined ? valueFmt(toNumberSafe(bottomLeftValue)) : ""}
            </p>
          </div>

          {d !== null ? (
            <div
              className={`text-[11px] sm:text-xs font-bold flex items-center gap-1 ${isUp ? "text-emerald-600" : isDown ? "text-red-600" : "text-slate-500"
                }`}
            >
              <span className="text-sm leading-none">{isUp ? "▲" : isDown ? "▼" : ""}</span>
              <span>{fmtPct(d)}</span>
            </div>
          ) : (
            <div />
          )}
        </div>
      )}
    </div>
  );
}

// function SalesCard({
//   title,
//   sales,
//   units,
//   valueFmt,
//   borderColor,
//   bgColor,
// }: {
//   title: string;
//   sales: number;
//   units: number;
//   valueFmt: (n: number) => string;
//   borderColor?: string;
//   bgColor?: string;
// }) {

//   const asp =
//     toNumberSafe(units) > 0
//       ? toNumberSafe(sales) / toNumberSafe(units)
//       : 0;

//   return (
//     <div
//       className="rounded-2xl border shadow-sm px-4 py-3 flex flex-col min-h-[110px]"
//       style={{ borderColor: borderColor ?? "#cbd5e1", backgroundColor: bgColor ?? "#fff" }}
//     >
//       <p className="text-xs font-semibold text-charcoal-500">{title}</p>

//       <p className="mt-1 text-[11px] sm:text-xs font-bold text-charcoal-500">
//         {valueFmt(toNumberSafe(sales))}
//       </p>
//       <div className="mt-auto pt-2 grid grid-cols-2 gap-4">
//         {/* Units Sold */}
//         <div>
//           <p className="text-[11px] sm:text-xs text-slate-600">Units Sold</p>
//           <p className="text-[11px] sm:text-xs font-semibold text-charcoal-500">
//             {fmtInteger(Math.round(toNumberSafe(units)))}
//           </p>
//         </div>

//         {/* ASP */}
//         <div className="text-center">
//           <p className="text-[11px] sm:text-xs text-slate-600">ASP</p>
//           <p className="text-[11px] sm:text-xs font-semibold text-charcoal-500 tabular-nums">
//             {valueFmt(0).replace(/[\d.,\s]/g, "")}
//             {asp.toFixed(2)}
//           </p>

//         </div>
//       </div>

//     </div>
//   );
// }


// function SalesCard({
//   title,
//   sales,          // Net
//   productSales,   // Gross
//   units,
//   valueFmt,
//   borderColor,
//   bgColor,
// }: {
//   title: string;
//   sales: number;
//   productSales: number;
//   units: number;
//   valueFmt: (n: number) => string;
//   borderColor?: string;
//   bgColor?: string;
// }) {
//   const net = toNumberSafe(sales);
//   const gross = toNumberSafe(productSales);
//   const discount = gross - net;

//   const asp =
//     toNumberSafe(units) > 0 ? net / toNumberSafe(units) : 0;

//   const discountPct =
//     gross > 0 ? ((gross - net) / gross) * 100 : 0;

//   return (
//     <div
//       className="rounded-2xl border shadow-sm px-4 py-3 flex flex-col min-h-[110px]"
//       style={{ borderColor: borderColor ?? "#cbd5e1", backgroundColor: bgColor ?? "#fff" }}
//     >
//       <p className="text-xs font-semibold text-charcoal-500">{title}</p>

//       {/* Sales: Net / Gross / Discount */}
//       <div className="mt-1">
//         {/* <p className="text-[11px] sm:text-xs font-semibold text-slate-600">Sales</p> */}

//         <div className="grid grid-cols-3 gap-2 mt-1">

//           <div>
//             <p className="text-[10px] sm:text-[11px] text-slate-500">Gross</p>
//             <p className="text-[11px] sm:text-xs font-bold text-charcoal-500">
//               {valueFmt(gross)}
//             </p>
//           </div>
//           <div className="text-center">
//             <p className="text-[10px] sm:text-[11px] text-slate-500">Net</p>
//             <p className="text-[11px] sm:text-xs font-bold text-charcoal-500">
//               {valueFmt(net)}
//             </p>
//           </div>



//           <div className="text-right">
//             <p className="text-[10px] sm:text-[11px] text-slate-500">Disc.</p>
//             <p className="text-[11px] sm:text-xs font-bold text-charcoal-500 tabular-nums">
//               {discountPct.toFixed(2)}%
//             </p>
//           </div>

//         </div>
//       </div>

//       {/* Bottom: Units + ASP */}
//       <div className="mt-auto pt-2 grid grid-cols-2 gap-4">
//         <div className="text-center">
//           <p className="text-[11px] sm:text-xs text-slate-600">Units Sold</p>
//           <p className="text-[11px] sm:text-xs font-semibold text-charcoal-500">
//             {fmtInteger(Math.round(toNumberSafe(units)))}
//           </p>
//         </div>

//         <div className="text-center">
//           <p className="text-[11px] sm:text-xs text-slate-600">ASP</p>
//           <p className="text-[11px] sm:text-xs font-semibold text-charcoal-500 tabular-nums">
//             {valueFmt(0).replace(/[\d.,\s]/g, "")}
//             {asp.toFixed(2)}
//           </p>
//         </div>
//       </div>
//     </div>
//   );
// }


function SalesCard({
  title,
  sales,          // Net
  productSales,   // Gross
  units,
  valueFmt,
  borderColor,
  bgColor,
}: {
  title: string;
  sales: number;
  productSales: number;
  units: number;
  valueFmt: (n: number) => string;
  borderColor?: string;
  bgColor?: string;
}) {
  const net = toNumberSafe(sales);
  const gross = toNumberSafe(productSales);
  const discountPct = gross > 0 ? ((gross - net) / gross) * 100 : 0;

  const asp =
    toNumberSafe(units) > 0 ? net / toNumberSafe(units) : 0;

  const currencySymbol = valueFmt(0).replace(/[\d.,\s]/g, "");

  return (
    <div
      className="rounded-2xl border shadow-sm px-4 py-3 flex flex-col min-h-[110px]"
      style={{ borderColor: borderColor ?? "#cbd5e1", backgroundColor: bgColor ?? "#fff" }}
    >
      <p className="text-xs font-semibold text-charcoal-500">{title}</p>

      {/* 3x2 Grid */}
      <div className="mt-2 grid grid-cols-3 grid-rows-2 gap-2 text-left">
        {/* Row 1 */}
        <div>
          <p className="text-[10px] sm:text-[11px] text-slate-500">Gross</p>
          <p className="text-[11px] sm:text-xs font-bold text-charcoal-500">
            {valueFmt(gross)}
          </p>
        </div>

        <div>
          <p className="text-[10px] sm:text-[11px] text-slate-500">Net</p>
          <p className="text-[11px] sm:text-xs font-bold text-charcoal-500">
            {valueFmt(net)}
          </p>
        </div>

        <div>
          <p className="text-[10px] sm:text-[11px] text-slate-500">Disc.</p>
          <p className="text-[11px] sm:text-xs font-bold text-charcoal-500 tabular-nums">
            {discountPct.toFixed(2)}%
          </p>
        </div>

        {/* Row 2 */}
        <div>
          <p className="text-[10px] sm:text-[11px] text-slate-500">Units Sold</p>
          <p className="text-[11px] sm:text-xs font-semibold text-charcoal-500">
            {fmtInteger(Math.round(toNumberSafe(units)))}
          </p>
        </div>

        <div>
          <p className="text-[10px] sm:text-[11px] text-slate-500">ASP</p>
          <p className="text-[11px] sm:text-xs font-semibold text-charcoal-500 tabular-nums">
            {currencySymbol}
            {asp.toFixed(2)}
          </p>
        </div>

        {/* Empty cell (future use / keeps grid symmetric) */}
        <div />
      </div>
    </div>
  );
}


function FeeCard({
  title,
  sales,
  charged,
  applicable,
  fmtCurrency,
  borderColor,
  bgColor,
}: {
  title: string;
  sales: number;
  charged: number;
  applicable: number;
  fmtCurrency: (n: number) => string;
  borderColor?: string;
  bgColor?: string;
}) {
  const chargedPct = pctOfSales(charged, sales);
  const applicablePct = pctOfSales(applicable, sales);

  // delta = charged% - applicable%
  const deltaPct = chargedPct - applicablePct;

  const deltaCls =
    deltaPct > 0
      ? "text-emerald-600"
      : deltaPct < 0
        ? "text-red-600"
        : "text-slate-500";

  return (
    <div
      className="rounded-2xl border shadow-sm px-4 py-3 flex flex-col min-h-[110px]"
      style={{ borderColor: borderColor ?? "#cbd5e1", backgroundColor: bgColor ?? "#fff" }}
    >
      {/* Heading */}
      <p className="text-xs font-semibold text-charcoal-500">{title}</p>

      {/* Charged label */}
      <p className="mt-1 text-[11px] sm:text-xs text-slate-600">Charged</p>

      {/* Charged row */}
      <div className="flex items-baseline gap-3">
        <p className="text-[11px] sm:text-xs font-bold text-charcoal-500">
          {fmtCurrency(toNumberSafe(charged))}
        </p>
        <p className="text-[11px] sm:text-xs font-bold text-emerald-600 whitespace-nowrap">
          ({fmtPctPlain(chargedPct)})
        </p>
      </div>

      {/* Applicable label */}
      <p className="mt-auto pt-2 text-[11px] sm:text-xs text-slate-600">Applicable</p>

      {/* Applicable row + delta */}
      <div className="grid grid-cols-[1fr_auto] items-end gap-2">
        <div className="flex items-baseline gap-3">
          <p className="text-[11px] sm:text-xs font-semibold text-charcoal-500 whitespace-nowrap">
            {fmtCurrency(toNumberSafe(applicable))}
          </p>
          <p className="text-[11px] sm:text-xs font-bold text-emerald-600 whitespace-nowrap">
            ({fmtPctPlain(applicablePct)})
          </p>
        </div>

        <div
          className={`text-[11px] sm:text-xs font-bold whitespace-nowrap flex items-center justify-end ${deltaCls}`}
        >
          {deltaPct > 0 && <span className="text-sm leading-none">▲</span>}
          {deltaPct < 0 && <span className="text-sm leading-none">▼</span>}
          <span>{fmtPctDelta(deltaPct)}</span>
        </div>
      </div>
    </div>
  );
}


/* ===================== MAIN DASHBOARD PAGE ===================== */
export default function ReferralFeesDashboard(): JSX.Element {
  const routeParams = useParams();

  // ✅ IMPORTANT: pick platform/country from URL
  const country = ((routeParams?.countryName as string) || "global").toLowerCase();

  // ✅ Global vs country behavior
  const isGlobalPage = country === "global";

  // ✅ get home currency ONLY for global formatting + api param
  const { homeCurrency: rawHomeCurrency } = useHomeCurrencyContext(country);
  const homeCurrency = isGlobalPage ? (rawHomeCurrency || "USD").toUpperCase() : "";

  // ✅ formatting currency code (NO fallback to homeCurrency on country pages)
  const displayCurrencyCode = useMemo(() => {
    if (isGlobalPage) return homeCurrency;
    return currencyFromCountryName(country);
  }, [isGlobalPage, homeCurrency, country]);

  const [card6, setCard6] = useState<Card6Summary>({
    sales: 0,
    units: 0,
    productSales: 0,

    totalFees: 0,
    totalFeesApplicable: 0,

    refFeesApplied: 0,
    refFeesApplicable: 0,

    fbaFees: 0,
    fbaFeesApplicable: 0,

    platformFees: 0,
    platformFeesApplicable: 0,

    otherFees: 0,
    otherFeesApplicable: 0,
  });

  const fmtCurrency = useCallback(
    (n: number): string => {
      if (typeof n !== "number" || Number.isNaN(n)) return "-";
      return n.toLocaleString(undefined, {
        style: "currency",
        currency: displayCurrencyCode,
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
    },
    [displayCurrencyCode]
  );

  const fmtCurrencyRounded = useCallback(
    (n: number): string => {
      if (typeof n !== "number" || Number.isNaN(n)) return "-";
      return n.toLocaleString(undefined, {
        style: "currency",
        currency: displayCurrencyCode,
        minimumFractionDigits: 0,
        maximumFractionDigits: 0,
      });
    },
    [displayCurrencyCode]
  );




  const refDeltaPct = useMemo(
    () => pctDelta(card6.refFeesApplied, card6.refFeesApplicable),
    [card6.refFeesApplied, card6.refFeesApplicable]
  );


  const [month, setMonth] = useState<string>("");
  const [year, setYear] = useState<string>("");
  const [range, setRange] = useState<Range>("monthly");
  const [selectedQuarter, setSelectedQuarter] = useState<string>("");

  // quarter -> representative month (choose what you prefer: start month here)
  const quarterToMonth = (q: string) => {
    switch ((q || "").toUpperCase()) {
      case "Q1": return "january";
      case "Q2": return "april";
      case "Q3": return "july";
      case "Q4": return "october";
      default: return "";
    }
  };


  const [rows, setRows] = useState<ReferralRow[]>([]);
  const [skuwiseRows, setSkuwiseRows] = useState<ReferralRow[]>([]);
  const [summary, setSummary] = useState<Summary>({
    ordersUnits: 0,
    totalSales: 0,
    feeImpact: 0,
  });
  const [feeSummaryRows, setFeeSummaryRows] = useState<FeeSummaryRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [userId, setUserId] = useState<string>("unknown");
  const [allOrdersByStatus, setAllOrdersByStatus] = useState<any[]>([]);
  const [error, setError] = useState<string | null>(null);

  const [salesStatus, setSalesStatus] = useState<SalesStatusSummary>({
    totalSales: 0,
    accurateSales: 0,
    overchargedSales: 0,
    underchargedSales: 0,
    noRefFeeSales: 0,
  });

  const [refBreakdown, setRefBreakdown] = useState<RefFeesBreakdown>({
    totalApplicable: 0,
    accurateApplicable: 0,
    overchargedApplicable: 0,
    underchargedApplicable: 0,
    noRefFeeApplicable: 0,
  });

  // ✅ hydrate month/year from URL first, else localStorage.latestFetchedPeriod
  useEffect(() => {
    const mFromRoute = (routeParams?.month as string | undefined)?.toLowerCase();
    const yFromRoute = routeParams?.year as string | undefined;

    if (mFromRoute && yFromRoute) {
      setMonth(mFromRoute);
      setYear(String(yFromRoute));
      setError(null);
      return;
    }

    if (typeof window === "undefined") return;

    try {
      const raw = localStorage.getItem("latestFetchedPeriod");
      if (raw) {
        const parsed = JSON.parse(raw) as { month?: string; year?: string };
        if (parsed.month && parsed.year) {
          setMonth(String(parsed.month).toLowerCase());
          setYear(String(parsed.year));
          setError(null);
          return;
        }
      }
    } catch { }

    setError("Please select both month and year to view referral fees.");
  }, [routeParams?.month, routeParams?.year]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    if (!month || !year) return;
    localStorage.setItem("latestFetchedPeriod", JSON.stringify({ month, year }));
  }, [month, year]);

  /* ======= derive userId from JWT ======= */
  useEffect(() => {
    if (typeof window === "undefined") return;
    const token = localStorage.getItem("jwtToken");
    if (!token) return;
    try {
      const decoded: any = jwtDecode(token);
      const id = decoded?.user_id?.toString() ?? "unknown";
      setUserId(id);
    } catch {
      setUserId("unknown");
    }
  }, []);


  /* ======= fileName ======= */
  const fileName = useMemo(
    () => `user_${userId}_${country}_${month}${year}_data`.toLowerCase(),
    [userId, country, month, year]
  );

  /* ===================== API Fetch ===================== */
  const fetchReferralData = useCallback(async () => {
    if (!month || !year || !country) {
      setError("Please select both month and year to view referral fees.");
      setRows([]);
      setSkuwiseRows([]);
      setFeeSummaryRows([]);
      setAllOrdersByStatus([]);
      setSummary({ ordersUnits: 0, totalSales: 0, feeImpact: 0 });
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const token = typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

      const params = new URLSearchParams({
        country: country,
        month: month,
        year: year,
      });

      // ✅ range → send qtd/ytd flags
      if (range === "quarterly") {
        params.set("qtd", "true");       // or "1" if your backend prefers
        params.set("quarter", selectedQuarter || ""); // optional but useful
      }

      if (range === "yearly") {
        params.set("ytd", "true");       // or "1"
      }

      // ✅ ONLY GLOBAL sends homeCurrency
      if (isGlobalPage && homeCurrency) {
        params.set("homeCurrency", homeCurrency.toLowerCase());
      }

      const url = `${baseURL}/get_table_data/${fileName}?${params.toString()}`;

      const res = await fetch(url, {
        method: "GET",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });

      if (!res.ok) throw new Error(`Failed to fetch referral data (${res.status})`);

      const json: any = await res.json();

      const platformFeeTotalFromApi = toNumberSafe(json?.platform_fee_total);
      const otherTotalFromApi = toNumberSafe(json?.other_total);


      const table = json?.table ?? [];
      const arr: ReferralRow[] = Array.isArray(table) ? table : [];


      // ✅ Pick summary rows directly from backend table
      const getRowBySku = (skuLabel: string) =>
        arr.find((r) => String(r.sku ?? "").trim().toLowerCase() === skuLabel.trim().toLowerCase());

      const rAcc = getRowBySku("Charge - Accurate");
      const rUnder = getRowBySku("Charge - Undercharged");
      const rOver = getRowBySku("Charge - Overcharged");
      const rNoRef = getRowBySku("Charge - noreferallfee");
      const rGrand = getRowBySku("Grand Total");

      // ✅ Sales & Status Summary MUST use net_sales_total_value from these rows
      const grandSales = toNumberSafe(rGrand?.net_sales_total_value);

      setSalesStatus({
        totalSales: grandSales,
        accurateSales: toNumberSafe(rAcc?.net_sales_total_value),
        underchargedSales: toNumberSafe(rUnder?.net_sales_total_value),
        overchargedSales: toNumberSafe(rOver?.net_sales_total_value),
        noRefFeeSales: toNumberSafe(rNoRef?.net_sales_total_value),
      });

      setRows(arr);
      setSkuwiseRows(arr);

      const accurate = Array.isArray(json?.accurate_data) ? json.accurate_data : [];
      const overcharged = Array.isArray(json?.overcharged_data) ? json.overcharged_data : [];
      const undercharged = Array.isArray(json?.undercharged_data) ? json.undercharged_data : [];
      const noRef = Array.isArray(json?.no_ref_fee_data) ? json.no_ref_fee_data : [];

      const mergedAll = [
        { Category: "Overcharged" },
        ...overcharged.map((r: any) => ({ Category: "Overcharged", ...r })),
        {},
        { Category: "Undercharged" },
        ...undercharged.map((r: any) => ({ Category: "Undercharged", ...r })),
        {},
        { Category: "No Ref Fees" },
        ...noRef.map((r: any) => ({ Category: "No Ref Fees", ...r })),
        {},
        { Category: "Accurate" },
        ...accurate.map((r: any) => ({ Category: "Accurate", ...r })),
      ];

      setAllOrdersByStatus(mergedAll);

      const summarySource = arr.filter((r) => {
        const sku = String(r.sku ?? "");
        return sku.startsWith("Charge -") || sku === "Grand Total";
      });

      const findChargeRow = (skuLabel: string) =>
        summarySource.find((r) => String(r.sku ?? "").toLowerCase() === skuLabel.toLowerCase());

      const chAcc = findChargeRow("Charge - Accurate");
      const chOver = findChargeRow("Charge - Overcharged");
      const chUnder = findChargeRow("Charge - Undercharged");
      const chNoRef = findChargeRow("Charge - noreferallfee");
      const grand = findChargeRow("Grand Total") ?? arr.find((r) => String(r.sku ?? "").toLowerCase() === "grand total");

      // ✅ LEFT PANEL: SALES split by status (use net sales)
      const totalSalesVal = toNumberSafe(grand?.net_sales_total_value ?? 0);
      setSalesStatus({
        totalSales: totalSalesVal,
        accurateSales: toNumberSafe(chAcc?.net_sales_total_value ?? 0),
        overchargedSales: toNumberSafe(chOver?.net_sales_total_value ?? 0),
        underchargedSales: toNumberSafe(chUnder?.net_sales_total_value ?? 0),
        noRefFeeSales: toNumberSafe(chNoRef?.net_sales_total_value ?? 0),
      });

      // ✅ RIGHT PANEL: REF FEES breakdown (use "answer" i.e. applicable)
      setRefBreakdown({
        totalApplicable: toNumberSafe(grand?.answer ?? 0),
        accurateApplicable: toNumberSafe(chAcc?.answer ?? 0),
        overchargedApplicable: toNumberSafe(chOver?.answer ?? 0),     // <- 216.75
        underchargedApplicable: toNumberSafe(chUnder?.answer ?? 0),   // <- 205.22
        noRefFeeApplicable: toNumberSafe(chNoRef?.answer ?? 0),       // <- 0
      });


      const order = [
        "Charge - Accurate",
        "Charge - Undercharged",
        "Charge - Overcharged",
        "Charge - noreferallfee",
        "Grand Total",
      ];

      summarySource.sort((a, b) => {
        const aSku = String(a.sku ?? "");
        const bSku = String(b.sku ?? "");
        const ai = order.indexOf(aSku);
        const bi = order.indexOf(bSku);
        if (ai === -1 && bi === -1) return 0;
        if (ai === -1) return 1;
        if (bi === -1) return -1;
        return ai - bi;
      });

      const mappedSummary: FeeSummaryRow[] = summarySource.map((r: ReferralRow) => ({
        label: String(r.sku ?? ""),
        units: Math.round(toNumberSafe(r.quantity)),
        sales: getNetSales(r),
        refFeesApplicable: toNumberSafe(r.answer),
        refFeesCharged: toNumberSafe(r.selling_fees),
        overcharged: toNumberSafe(r.difference),
      }));

      // use only actual SKU/product lines (exclude Charge-/Grand Total lines)
      const lineItems = arr.filter((r) => {
        const skuStr = String(r.sku ?? "");
        if (skuStr === "Grand Total") return false;
        if (skuStr.startsWith("Charge -")) return false;
        return true;
      });

      // const salesTotal = lineItems.reduce((acc, r) => acc + getNetSales(r), 0);


      // const totalFees = lineItems.reduce((acc, r) => acc + toNumberSafe(r.selling_fees), 0);

      // const refFeesApplicable = lineItems.reduce((acc, r) => acc + toNumberSafe(r.answer), 0);

      // const refFeesApplied = totalFees;

      // const fbaFees = lineItems.reduce((acc, r) => {
      //   return acc + FBA_KEYS.reduce((s, k) => s + toNumberSafe((r as any)[k]), 0);
      // }, 0);

      // const platformFees = platformFeeTotalFromApi !== 0 ? platformFeeTotalFromApi : platformFeesDerived;
      // const otherFees = otherTotalFromApi !== 0 ? otherTotalFromApi : otherFeesDerived;


      // const platformFeesDerived = lineItems.reduce((acc, r) => {
      //   return acc + PLATFORM_KEYS.reduce((s, k) => s + toNumberSafe((r as any)[k]), 0);
      // }, 0);

      // const otherFeesDerived = lineItems.reduce((acc, r) => {
      //   return acc + OTHER_KEYS.reduce((s, k) => s + toNumberSafe((r as any)[k]), 0);
      // }, 0);


      const unitsSold = lineItems.reduce((acc, r) => acc + Math.round(toNumberSafe(r.quantity)), 0);

      const salesTotal = lineItems.reduce((acc, r) => acc + getNetSales(r), 0);

      const productSalesTotal = lineItems.reduce(
        (acc, r) => acc + toNumberSafe((r as any)?.product_sales),
        0
      );



      // Referral
      const refFeesApplicable = lineItems.reduce((acc, r) => acc + toNumberSafe(r.answer), 0);
      const refFeesApplied = lineItems.reduce((acc, r) => acc + toNumberSafe(r.selling_fees), 0); // charged

      const fbaFees = Math.abs(
        lineItems.reduce((acc, r) => {
          return acc + FBA_KEYS.reduce((s, k) => s + toNumberSafe((r as any)[k]), 0);
        }, 0)
      );


      // Platform (charged)
      const platformFeesDerived = lineItems.reduce((acc, r) => {
        return acc + PLATFORM_KEYS.reduce((s, k) => s + toNumberSafe((r as any)[k]), 0);
      }, 0);

      // Other (charged)
      const otherFeesDerived = lineItems.reduce((acc, r) => {
        return acc + OTHER_KEYS.reduce((s, k) => s + toNumberSafe((r as any)[k]), 0);
      }, 0);

      const platformFees = platformFeeTotalFromApi !== 0 ? platformFeeTotalFromApi : platformFeesDerived;
      const otherFees = otherTotalFromApi !== 0 ? otherTotalFromApi : otherFeesDerived;

      // ✅ APPLICABLE for non-ref fees:
      // If you don’t have “applicable” yet, use charged as placeholder.
      // Replace later if backend gives you: fba_applicable_total / platform_applicable_total / other_applicable_total
      const fbaFeesApplicable = fbaFees;
      const platformFeesApplicable = platformFees;
      const otherFeesApplicable = otherFees;

      // ✅ Total fees are aggregate of buckets (charged + applicable)
      const totalFees = refFeesApplied + fbaFees + platformFees + otherFees;
      const totalFeesApplicable = refFeesApplicable + fbaFeesApplicable + platformFeesApplicable + otherFeesApplicable;

      setCard6({
        sales: salesTotal,
        units: unitsSold,
        productSales: productSalesTotal,
        totalFees,
        totalFeesApplicable,

        refFeesApplied,
        refFeesApplicable,

        fbaFees,
        fbaFeesApplicable,

        platformFees,
        platformFeesApplicable,

        otherFees,
        otherFeesApplicable,
      });



      setFeeSummaryRows(mappedSummary);

      let totalUnits = 0;
      let totalSales = 0;
      let feeImpact = 0;

      for (const r of arr) {
        totalUnits += Math.round(toNumberSafe(r.quantity));
        totalSales += getNetSales(r);
        feeImpact += toNumberSafe(r.overcharged ?? r.difference);
      }

      setSummary({ ordersUnits: totalUnits, totalSales, feeImpact });
    } catch (e: any) {
      setError(e?.message || "Failed to load data");
      setRows([]);
      setSkuwiseRows([]);
      setFeeSummaryRows([]);
      setAllOrdersByStatus([]);
      setSummary({ ordersUnits: 0, totalSales: 0, feeImpact: 0 });
    } finally {
      setLoading(false);
    }
  }, [month, year, country, fileName, isGlobalPage, homeCurrency, range, selectedQuarter]);


  useEffect(() => {
    fetchReferralData();
  }, [fetchReferralData]);

  /* ===================== Derived ===================== */
  const totalFeeRow = useMemo<FeeSummaryRow | null>(() => {
    if (!feeSummaryRows.length) return null;
    const total =
      feeSummaryRows.find((r) => r.label && r.label.toLowerCase() === "total") ||
      feeSummaryRows[feeSummaryRows.length - 1];
    return total || null;
  }, [feeSummaryRows]);

  const cardSummary = useMemo(
    () => ({
      ordersUnits: totalFeeRow?.units ?? 0,
      totalSales: totalFeeRow?.sales ?? 0,
      feeImpact: totalFeeRow?.overcharged ?? 0,
    }),
    [totalFeeRow]
  );

  const summaryTableRows: Row[] = useMemo(() => {
    return feeSummaryRows.map((r, index) => ({
      label: r.label,
      units: r.units,
      sales: r.sales,
      refFeesApplicable: r.refFeesApplicable,
      refFeesCharged: r.refFeesCharged,
      overcharged: r.overcharged,
      _isTotal: index === feeSummaryRows.length - 1,
    })) as Row[];
  }, [feeSummaryRows]);

  // const skuTableAll: Row[] = useMemo(() => {
  //   if (!skuwiseRows.length) return [];

  //   const filtered = skuwiseRows.filter((r) => {
  //     const skuStr = String(r.sku ?? "");
  //     if (skuStr === "Grand Total") return true;
  //     return !skuStr.startsWith("Charge -");
  //   });

  //   return filtered.map((r) => {
  //     const quantity = Math.round(toNumberSafe(r.quantity));
  //     const sales = getNetSales(r);
  //     const applicable = toNumberSafe(r.answer);
  //     const charged = toNumberSafe(r.selling_fees);
  //     const overcharged = toNumberSafe(r.overcharged ?? r.difference);

  //     const skuStr = String(r.sku ?? "");
  //     const isTotal = skuStr.toLowerCase() === "grand total";

  //     return {
  //       sku: isTotal ? "" : (r.sku ?? ""),                 // ✅ blank SKU for total
  //       productName: isTotal ? "Grand Total" : (r.product_name ?? ""), // ✅ label in Product Name
  //       units: quantity,
  //       sales,
  //       applicable,
  //       charged,
  //       overcharged,
  //       _isTotal: isTotal,
  //     };

  //   });
  // }, [skuwiseRows]);

  const skuTableAll: Row[] = useMemo(() => {
    if (!skuwiseRows.length) return [];

    const filtered = skuwiseRows.filter((r) => {
      const skuStr = String(r.sku ?? "");
      if (skuStr === "Grand Total") return true;
      return !skuStr.startsWith("Charge -");
    });

    return filtered.map((r) => {
      const quantity = Math.round(toNumberSafe(r.quantity));
      const sales = getNetSales(r);

      const ref_applicable = toNumberSafe(r.answer);
      const ref_charged = toNumberSafe(r.selling_fees);
      const overcharged = toNumberSafe(r.overcharged ?? r.difference);

      // ✅ FBA from row (usually negative in your data)
      const fba_charged = Math.abs(
        FBA_KEYS.reduce((sum, k) => sum + toNumberSafe((r as any)[k]), 0)
      );
      const fba_applicable = fba_charged; // placeholder until backend gives applicable

      // ✅ Other fees from row
      const other_charged = Math.abs(
        OTHER_KEYS.reduce((sum, k) => sum + toNumberSafe((r as any)[k]), 0)
      );
      const other_applicable = other_charged; // placeholder

      // (optional) ✅ Platform fees from row
      const platform_charged = Math.abs(
        PLATFORM_KEYS.reduce((sum, k) => sum + toNumberSafe((r as any)[k]), 0)
      );

      // ✅ Total (you can include platform in total, even if not shown as a column)
      const total_charged = ref_charged + fba_charged + other_charged + platform_charged;
      const total_applicable = ref_applicable + fba_applicable + other_applicable + platform_charged;

      const skuStr = String(r.sku ?? "");
      const isTotal = skuStr.toLowerCase() === "grand total";

      return {
        sku: isTotal ? "" : (r.sku ?? ""),
        productName: isTotal ? "Grand Total" : (r.product_name ?? ""),
        units: quantity,
        sales,

        // keep your existing ones if needed elsewhere
        applicable: ref_applicable,
        charged: ref_charged,
        overcharged,

        // ✅ NEW fields for grouped table
        ref_applicable,
        ref_charged,
        fba_applicable,
        fba_charged,
        other_applicable,
        other_charged,
        total_applicable,
        total_charged,

        _isTotal: isTotal,
      };
    });
  }, [skuwiseRows]);

  const skuTableDistinctAll: Row[] = useMemo(() => {
    if (!skuTableAll.length) return [];

    const nonTotal = skuTableAll.filter((row) => !(row as any)._isTotal);
    const totalRow = skuTableAll.find((row) => (row as any)._isTotal) || null;

    // Aggregate by distinct productName (case-insensitive)
    const map = new Map<string, any>();

    for (const r of nonTotal as any[]) {
      const nameRaw = String(r.productName ?? "").trim();
      if (!nameRaw) continue;

      const key = nameRaw.toLowerCase();

      if (!map.has(key)) {
        map.set(key, {
          sno: "",
          productName: nameRaw,
          sku: "",

          units: 0,
          sales: 0,

          // referral
          ref_applicable: 0,
          ref_charged: 0,

          // for your export columns that currently use applicable/charged
          applicable: 0,
          charged: 0,

          overcharged: 0,

          _skus: new Set<string>(),
        });
      }

      const acc = map.get(key);

      acc.units += Math.round(toNumberSafe(r.units));
      acc.sales += toNumberSafe(r.sales);

      acc.ref_applicable += toNumberSafe(r.ref_applicable ?? r.applicable);
      acc.ref_charged += toNumberSafe(r.ref_charged ?? r.charged);

      // keep these for compatibility with current export headers
      acc.applicable += toNumberSafe(r.ref_applicable ?? r.applicable);
      acc.charged += toNumberSafe(r.ref_charged ?? r.charged);

      acc.overcharged += toNumberSafe(r.overcharged);

      const skuStr = String(r.sku ?? "").trim();
      if (skuStr) acc._skus.add(skuStr);
    }

    const aggregated = Array.from(map.values()).map((x) => {
      const skus = Array.from(x._skus);
      return {
        ...x,
        sku: skus.length === 1 ? skus[0] : skus.length > 1 ? "Multiple" : "",
      };
    });

    // Sort by sales desc (optional)
    aggregated.sort((a, b) => toNumberSafe(b.sales) - toNumberSafe(a.sales));

    // Add serial numbers (optional)
    let counter = 1;
    const withSno = aggregated.map((row: any) => ({ ...row, sno: counter++ }));

    // Add Grand Total at bottom (same as UI)
    if (totalRow) withSno.push({ ...(totalRow as any), sno: "" });

    return withSno as Row[];
  }, [skuTableAll]);

  const skuColumns: ColumnDef<Row>[] = [
    { key: "sku", header: "SKU" },
    { key: "productName", header: "Product Name" },
    { key: "units", header: "Units" },
    { key: "sales", header: "Net Sales", render: (_, v) => fmtCurrency(Number(v)) },
    { key: "applicable", header: "Ref Fees Applicable", render: (_, v) => fmtCurrency(Number(v)) },
    { key: "charged", header: "Ref Fees Charged", render: (_, v) => fmtCurrency(Number(v)) },
    { key: "overcharged", header: "Overcharged", render: (_, v) => <span>{fmtCurrency(Number(v))}</span> },
  ];

  const summaryColumns: ColumnDef<Row>[] = [
    { key: "label", header: "Ref. Fees" },
    { key: "units", header: "Units", render: (_, v) => fmtInteger(Number(v)) },
    { key: "sales", header: "Net Sales", render: (_, v) => fmtCurrency(Number(v)) },
    { key: "refFeesApplicable", header: "Ref Fees Applicable", render: (_, v) => fmtCurrency(Number(v)) },
    { key: "refFeesCharged", header: "Ref Fees Charged", render: (_, v) => fmtCurrency(Number(v)) },
    { key: "overcharged", header: "Overcharged", render: (_, v) => fmtCurrency(Number(v)) },
  ];

  const skuTableDisplay: Row[] = useMemo(() => {
    if (!skuTableAll.length) return [];

    const nonTotal = skuTableAll.filter((row) => !(row as any)._isTotal);
    const totalRow = skuTableAll.find((row) => (row as any)._isTotal) || null;

    // ✅ 1) Aggregate rows by DISTINCT productName
    const map = new Map<string, any>();

    for (const r of nonTotal as any[]) {
      const nameRaw = String(r.productName ?? "").trim();
      if (!nameRaw) continue;

      const key = nameRaw.toLowerCase(); // distinct productName (case-insensitive)

      if (!map.has(key)) {
        map.set(key, {
          sno: "",
          productName: nameRaw,
          sku: r.sku ? String(r.sku) : "", // will be fixed below if multiple
          units: 0,
          sales: 0,

          ref_applicable: 0,
          ref_charged: 0,

          fba_applicable: 0,
          fba_charged: 0,

          other_applicable: 0,
          other_charged: 0,

          total_applicable: 0,
          total_charged: 0,

          overcharged: 0,

          _skus: new Set<string>(),
        });
      }

      const acc = map.get(key);

      acc.units += Math.round(toNumberSafe(r.units));
      acc.sales += toNumberSafe(r.sales);

      acc.ref_applicable += toNumberSafe(r.ref_applicable);
      acc.ref_charged += toNumberSafe(r.ref_charged);

      acc.fba_applicable += toNumberSafe(r.fba_applicable);
      acc.fba_charged += toNumberSafe(r.fba_charged);

      acc.other_applicable += toNumberSafe(r.other_applicable);
      acc.other_charged += toNumberSafe(r.other_charged);

      acc.total_applicable += toNumberSafe(r.total_applicable);
      acc.total_charged += toNumberSafe(r.total_charged);

      acc.overcharged += toNumberSafe(r.overcharged);

      const skuStr = String(r.sku ?? "").trim();
      if (skuStr) acc._skus.add(skuStr);
    }

    // ✅ 2) Convert to array + decide SKU display
    const aggregated = Array.from(map.values()).map((x) => {
      const skus = Array.from(x._skus);
      return {
        ...x,
        sku: skus.length === 1 ? skus[0] : skus.length > 1 ? "Multiple" : "",
      };
    });

    if (!aggregated.length) return totalRow ? [totalRow] : [];

    // ✅ 3) Sort + Top5 + Others based on DISTINCT products
    const sorted = [...aggregated].sort(
      (a, b) => toNumberSafe(b.sales) - toNumberSafe(a.sales)
    );

    const top5 = sorted.slice(0, 5);
    const remaining = sorted.slice(5);

    let othersRow: Row | null = null;
    if (remaining.length) {
      const agg = remaining.reduce(
        (acc: any, row: any) => {
          acc.units += Math.round(toNumberSafe(row.units));
          acc.sales += toNumberSafe(row.sales);

          // ✅ YOU WERE MISSING THESE TOO
          acc.ref_applicable += toNumberSafe(row.ref_applicable);
          acc.ref_charged += toNumberSafe(row.ref_charged);

          acc.fba_applicable += toNumberSafe(row.fba_applicable);
          acc.fba_charged += toNumberSafe(row.fba_charged);

          acc.other_applicable += toNumberSafe(row.other_applicable);
          acc.other_charged += toNumberSafe(row.other_charged);

          acc.total_applicable += toNumberSafe(row.total_applicable);
          acc.total_charged += toNumberSafe(row.total_charged);

          acc.overcharged += toNumberSafe(row.overcharged);
          return acc;
        },
        {
          units: 0,
          sales: 0,

          ref_applicable: 0,
          ref_charged: 0,

          fba_applicable: 0,
          fba_charged: 0,

          other_applicable: 0,
          other_charged: 0,

          total_applicable: 0,
          total_charged: 0,

          overcharged: 0,
        }
      );


      othersRow = {
        sno: "",
        sku: "",
        productName: "Others",
        units: agg.units,
        sales: agg.sales,
        overcharged: agg.overcharged,

        ref_applicable: agg.ref_applicable,
        ref_charged: agg.ref_charged,
        fba_applicable: agg.fba_applicable,
        fba_charged: agg.fba_charged,
        other_applicable: agg.other_applicable,
        other_charged: agg.other_charged,
        total_applicable: agg.total_applicable,
        total_charged: agg.total_charged,

        _isOthers: true,
      } as Row;
    }

    // ✅ 4) Final rows + serial numbers (not for Grand Total)
    const finalRows: any[] = [...top5];
    if (othersRow) finalRows.push(othersRow);
    if (totalRow) finalRows.push(totalRow);

    let counter = 1;
    return finalRows.map((row: any) => {
      if (row._isTotal) return { ...row, sno: "" };
      return { ...row, sno: counter++ };
    });
  }, [skuTableAll]);

  const groupedSkuTableDisplay: any[] = useMemo(() => {
    return skuTableDisplay.map((row: any) => ({
      sno: row.sno,
      productName: row.productName,
      sku: row.sku,
      units: row.units,
      sales: row.sales,

      ref_applicable: row.ref_applicable,
      ref_charged: row.ref_charged,

      fba_applicable: row.fba_applicable,
      fba_charged: row.fba_charged,

      other_applicable: row.other_applicable,
      other_charged: row.other_charged,

      total_applicable: row.total_applicable,
      total_charged: row.total_charged,

      // ✅ keep total-row styling working
      _isTotal: row._isTotal,
    }));
  }, [skuTableDisplay]);


  const handleDownloadExcel = useCallback(() => {
    if (!feeSummaryRows.length && !skuTableAll.length && !rows.length) return;

    const wb = XLSX.utils.book_new();

    /* ---------------- Sheet 1: Summary ---------------- */
    const summaryData = feeSummaryRows.map((r) => {
      const units = Math.round(toNumberSafe(r.units));
      const sales = toNumberSafe(r.sales);
      const applicable = toNumberSafe(r.refFeesApplicable);
      const charged = toNumberSafe(r.refFeesCharged);
      const overcharged = toNumberSafe(r.overcharged);

      return {
        "Ref Fees": r.label,
        Units: units,
        "Net Sales": Number(sales.toFixed(2)),
        "Ref Fees Applicable": Number(applicable.toFixed(2)),
        "Ref Fees Charged": Number(charged.toFixed(2)),
        Overcharged: Number(overcharged.toFixed(2)),
      };
    });

    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(summaryData), "Summary");

    /* ---------------- Sheet 2: Overcharged Ref Fees (DISTINCT Product Names) ---------------- */
    const overData = skuTableDistinctAll.map((r: any) => {
      const units = Math.round(toNumberSafe(r.units));
      const sales = toNumberSafe(r.sales);
      const applicable = toNumberSafe(r.applicable ?? r.ref_applicable);
      const charged = toNumberSafe(r.charged ?? r.ref_charged);
      const overcharged = toNumberSafe(r.overcharged);

      return {
        SKU: String(r.sku ?? ""),
        "Product Name": String(r.productName ?? ""),
        Units: units,
        "Net Sales": Number(sales.toFixed(2)),
        "Ref Fees Applicable": Number(applicable.toFixed(2)),
        "Ref Fees Charged": Number(charged.toFixed(2)),
        Overcharged: Number(overcharged.toFixed(2)),
      };
    });

    XLSX.utils.book_append_sheet(
      wb,
      XLSX.utils.json_to_sheet(overData),
      "Overcharged Ref Fees"
    );


    /* ---------------- Sheet 3: Orders by Status (CLEANED) ---------------- */
    const cleanedOrdersByStatus = (allOrdersByStatus || []).map((r: any) => {
      // keep separators / headers as-is
      const isSeparatorRow =
        !r || Object.keys(r).length === 0 || (r.Category && Object.keys(r).length === 1);

      if (isSeparatorRow) return r;

      return {
        Category: r.Category ?? "",
        "Order ID": r.order_id ?? "",
        SKU: r.sku ?? "",
        "Product Name": r.product_name ?? "",
        Quantity: toNumberSafe(r.quantity),
        "Net Sales": getNetSales(r), // ✅ net sales
        "Referral Fees Applicable": toNumberSafe(r.answer),
        "Referral Fees Charged": toNumberSafe(r.selling_fees),
        Overcharged: toNumberSafe(r.difference ?? r.overcharged),
        Status: r.errorstatus ?? "",
      };
    });

    XLSX.utils.book_append_sheet(
      wb,
      XLSX.utils.json_to_sheet(cleanedOrdersByStatus),
      "Orders by Status"
    );

    /* ---------------- Save ---------------- */
    XLSX.writeFile(wb, `Referral-Fees-${country}-${month}-${year}.xlsx`);
  }, [
    feeSummaryRows,
    skuTableAll,
    rows,
    allOrdersByStatus,
    country,
    month,
    year,
  ]);


  const canShowContent = !loading && !error && month && year;

  const currencySymbol = useMemo(
    () => fmtCurrency(0).replace(/[\d.,\s]/g, ""),
    [fmtCurrency]
  );

  const fmtMoneyNoSymbol = useCallback((n: any) => {
    const val = toNumberSafe(n);
    return val.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }, []);


  return (
    <div className="space-y-1.5 font-sans text-charcoal-500">

      <div className="w-full flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        {/* LEFT: Title + Subtitle */}
        <div className="flex flex-col leading-tight w-full md:w-auto">
          <div className="flex items-baseline gap-2">
            <PageBreadcrumb pageTitle="Expense Reconciliation -" variant="page" align="left" textSize="2xl" className="mb-0 md:mb-2" />
            <span className="text-[#5EA68E] font-bold text-lg sm:text-2xl md:text-2xl">
              {country.toUpperCase()}
            </span>
          </div>

        </div>

        {/* RIGHT: Filters */}
        <div className="flex w-full md:w-auto justify-start md:justify-end">
          {/* <MonthYearPickerTable
            month={month}
            year={year}
            yearOptions={[new Date().getFullYear(), new Date().getFullYear() - 1]}
            onMonthChange={(v) => {
              setMonth(v);
              if (v && year) setError(null);
            }}
            onYearChange={(v) => {
              setYear(v);
              if (v && month) setError(null);
            }}
            valueMode="lower"
          /> */}

          <PeriodFiltersTable
            range={range}
            selectedMonth={month}
            selectedQuarter={selectedQuarter}
            selectedYear={year}
            yearOptions={[new Date().getFullYear(), new Date().getFullYear() - 1]}
            onRangeChange={(v) => {
              setRange(v);

              // If switching to monthly and month already exists, keep it
              if (v === "monthly") {
                // no-op; month stays your source of truth
                return;
              }

              // If switching to quarterly and a quarter is already selected, derive month
              if (v === "quarterly") {
                const m = quarterToMonth(selectedQuarter);
                if (m) setMonth(m);
                return;
              }

              // If switching to yearly, you can either:
              // A) keep month as-is (last used), or
              // B) force a month like january to keep backend happy
              if (v === "yearly") {
                if (!month) setMonth("january"); // optional
              }
            }}
            onMonthChange={(v) => {
              setMonth(v);
              if (v && year) setError(null);
            }}
            onQuarterChange={(q) => {
              setSelectedQuarter(q);

              // IMPORTANT: backend needs month/year, so derive a month from quarter
              const m = quarterToMonth(q);
              setMonth(m);

              if (m && year) setError(null);
            }}
            onYearChange={(v) => {
              setYear(String(v));
              if (v && month) setError(null);
            }}
          />
        </div>
      </div>

      {loading && (
        <div className="flex flex-col items-center justify-center py-12 text-center">
          <Loader
            src="/infinity-unscreen.gif"
            size={150}
            transparent
            roundedClass="rounded-none"
            backgroundClass="bg-transparent"
            respectReducedMotion
          />
        </div>
      )}

      {!loading && !!error && (
        <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
          <div className="flex items-center">
            <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
            <span>{error}</span>
          </div>
        </div>
      )}

      {canShowContent && (
        <>
          {/* ===================== 6 CARDS (UPDATED) ===================== */}

          <div className="grid grid-cols-1 sm:grid-cols-3 lg:grid-cols-3 xl:grid-cols-3 gap-3 mt-4">
            <SalesCard
              title="Sales"
              sales={card6.sales}
              productSales={card6.productSales}
              units={card6.units}
              valueFmt={fmtCurrencyRounded}
              borderColor="#75BBDA"
              bgColor="#75BBDA4D"
            />


            <FeeCard
              title="Total Amazon Fees"
              sales={card6.sales}
              charged={card6.totalFees}
              applicable={card6.totalFeesApplicable}
              fmtCurrency={fmtCurrencyRounded}
              borderColor="#B75A5A"
              bgColor="#B75A5A4D"
            />

            <FeeCard
              title="Referral Fees"
              sales={card6.sales}
              charged={card6.refFeesApplied}
              applicable={card6.refFeesApplicable}
              fmtCurrency={fmtCurrencyRounded}
              borderColor="#7B9A6D"
              bgColor="#7B9A6D4D"
            />

            <FeeCard
              title="FBA Fees"
              sales={card6.sales}
              charged={card6.fbaFees}
              applicable={card6.fbaFeesApplicable}
              fmtCurrency={fmtCurrencyRounded}
              borderColor="#FDD36F"
              bgColor="#FDD36F4D"
            />

            <FeeCard
              title="Platform Fees"
              sales={card6.sales}
              charged={card6.platformFees}
              applicable={card6.platformFeesApplicable}
              fmtCurrency={fmtCurrencyRounded}
              borderColor="#ED9F50"
              bgColor="#ED9F504D"
            />

            <FeeCard
              title="Other Fees"
              sales={card6.sales}
              charged={card6.otherFees}
              applicable={card6.otherFeesApplicable}
              fmtCurrency={fmtCurrencyRounded}
              borderColor="#3A8EA4"
              bgColor="#3A8EA44D"
            />
          </div>


          {/* ===================== BREAKDOWN SECTION (NEW) ===================== */}
          {(() => {
            /* =========================
               1) Pull Charge rows + Grand Total
            ========================= */
            const chargeAcc = rows.find(
              (r) => String(r.sku ?? "").toLowerCase() === "charge - accurate"
            );
            const chargeOver = rows.find(
              (r) => String(r.sku ?? "").toLowerCase() === "charge - overcharged"
            );
            const chargeUnder = rows.find(
              (r) => String(r.sku ?? "").toLowerCase() === "charge - undercharged"
            );
            const chargeNoRef = rows.find(
              (r) => String(r.sku ?? "").toLowerCase() === "charge - noreferallfee"
            );
            const grand = rows.find(
              (r) => String(r.sku ?? "").toLowerCase() === "grand total"
            );

            const fmtPctSigned = (p: number) => {
              const v = toNumberSafe(p);
              const sign = v > 0 ? "+" : v < 0 ? "-" : "";
              return `${sign}${Math.abs(v).toFixed(2)}%`;
            };


            const pctOf = (value: number, total: number) => {
              const t = Math.max(1e-9, Math.abs(toNumberSafe(total)));
              return (toNumberSafe(value) / t) * 100;
            };

            const totalSalesForPct = Math.max(1, Math.abs(toNumberSafe(grand?.net_sales_total_value)));


            /* =========================
               2) LEFT PANEL (Sales based): use net_sales_total_value from Charge lines
               Requirement: show net_sales_total_value for:
               Charge - Accurate, Charge - Undercharged, Charge - Overcharged, Charge - noreferallfee, Grand Total
            ========================= */
            // const leftList = [
            //   {
            //     label: "Total Sales",
            //     value: toNumberSafe(grand?.net_sales_total_value),
            //     color: "#F47A00",
            //   },
            //   {
            //     label: "Accurately charged",
            //     value: toNumberSafe(chargeAcc?.net_sales_total_value),
            //     color: "#14B8A6",
            //   },
            //   {
            //     label: "Over charged",
            //     value: toNumberSafe(chargeOver?.net_sales_total_value),
            //     color: "#EF4444",
            //   },
            //   {
            //     label: "Undercharged",
            //     value: toNumberSafe(chargeUnder?.net_sales_total_value),
            //     color: "#F59E0B",
            //   },
            //   {
            //     label: "No ref fee",
            //     value: toNumberSafe(chargeNoRef?.net_sales_total_value),
            //     color: "#94A3B8",
            //   },
            // ];

            const totalSalesForLeft = Math.max(1e-9, Math.abs(toNumberSafe(grand?.net_sales_total_value)));

            const leftList = [
              {
                label: "Total Sales",
                value: toNumberSafe(grand?.net_sales_total_value),
                pct: pctOf(toNumberSafe(grand?.net_sales_total_value), totalSalesForLeft),
                color: "#75BBDA",
              },
              {
                label: "Accurately charged",
                value: toNumberSafe(chargeAcc?.net_sales_total_value),
                pct: pctOf(toNumberSafe(chargeAcc?.net_sales_total_value), totalSalesForLeft),
                color: "#C49466",
              },
              {
                label: "Over charged",
                value: toNumberSafe(chargeOver?.net_sales_total_value),
                pct: pctOf(toNumberSafe(chargeOver?.net_sales_total_value), totalSalesForLeft),
                color: "#B75A5A",
              },
              {
                label: "Undercharged",
                value: toNumberSafe(chargeUnder?.net_sales_total_value),
                pct: pctOf(toNumberSafe(chargeUnder?.net_sales_total_value), totalSalesForLeft),
                color: "#FDD36F",
              },
              {
                label: "No ref fee",
                value: toNumberSafe(chargeNoRef?.net_sales_total_value),
                pct: pctOf(toNumberSafe(chargeNoRef?.net_sales_total_value), totalSalesForLeft),
                color: "#ED9F50",
              },
            ];


            /* =========================
               3) RIGHT PANEL (Ref fee breakdown): use selling_fees from Charge lines
               Also show difference in brackets next to value
            ========================= */
            const rightList = [
              {
                label: "Total Ref Fees",
                value: toNumberSafe(grand?.selling_fees),
                diff: toNumberSafe(grand?.difference),
                pct: pctOf(toNumberSafe(grand?.difference), totalSalesForPct),
                color: "#7B9A6D",
              },
              {
                label: "Accurately charged",
                value: toNumberSafe(chargeAcc?.selling_fees),
                diff: toNumberSafe(chargeAcc?.difference),
                pct: pctOf(toNumberSafe(chargeAcc?.difference), totalSalesForPct),
                color: "#C49466",
              },
              {
                label: "Over charged",
                value: toNumberSafe(chargeOver?.selling_fees),
                diff: toNumberSafe(chargeOver?.difference),
                pct: pctOf(toNumberSafe(chargeOver?.difference), totalSalesForPct),
                color: "#B75A5A",
              },
              {
                label: "Undercharged",
                value: toNumberSafe(chargeUnder?.selling_fees),
                diff: toNumberSafe(chargeUnder?.difference),
                pct: pctOf(toNumberSafe(chargeUnder?.difference), totalSalesForPct),
                color: "#FDD36F",
              },
              {
                label: "No ref fee",
                value: toNumberSafe(chargeNoRef?.selling_fees),
                diff: toNumberSafe(chargeNoRef?.difference),
                pct: pctOf(toNumberSafe(chargeNoRef?.difference), totalSalesForPct),
                color: "#ED9F50",
              },
            ];


            /* =========================
               4) Bar scaling totals
               - LEFT: scale by Grand Total sales (so bars are "out of total sales")
               - RIGHT: scale by Grand Total ref fees (so bars are "out of total ref fees")
            ========================= */
            const leftTotalForBars = Math.max(
              1,
              Math.abs(toNumberSafe(grand?.net_sales_total_value))
            );

            const rightTotalForBars = Math.max(
              1,
              Math.abs(toNumberSafe(grand?.selling_fees))
            );

            /* =========================
               5) Row renderer
               - shows bar + value (and optional diff in brackets)
            ========================= */
            const BarRow = ({
              label,
              value,
              total,
              color,
              deltaPct,
              showDelta = false,
              pctColor,
            }: {
              label: string;
              value: number;
              total: number;
              color: string;
              deltaPct?: number;
              showDelta?: boolean;
              pctColor?: string;
            }) => {

              const v = Math.abs(toNumberSafe(value));
              const t = Math.max(1, Math.abs(toNumberSafe(total)));
              const barPct = Math.min(100, (v / t) * 100);

              const deltaCls =
                typeof deltaPct === "number"
                  ? deltaPct > 0
                    ? "text-emerald-600"
                    : deltaPct < 0
                      ? "text-red-600"
                      : "text-slate-500"
                  : "text-slate-500";

              return (
                <div
                  className={`grid items-center gap-3 ${showDelta
                    ? "grid-cols-[180px_1fr_170px]"
                    : "grid-cols-[180px_1fr_120px]"
                    }`}
                >
                  {/* Label */}
                  <div className="text-sm text-slate-700">{label}</div>

                  {/* Bar */}
                  <div className="h-2 rounded-full bg-slate-100 overflow-hidden">
                    <div
                      className="h-2 rounded-full"
                      style={{ width: `${barPct}%`, backgroundColor: color }}
                    />
                  </div>

                  {/* Value (+ optional %) */}
                  {/* <div className="flex items-baseline justify-end whitespace-nowrap tabular-nums">
                    <span className="text-sm font-semibold text-slate-800 min-w-[90px] text-right">
                      {fmtNumber(value)}
                    </span>

                    {showDelta && (
                      <span className={`ml-1 text-xs font-bold min-w-[55px] text-right ${deltaCls}`}>
                        ({fmtPctSigned(deltaPct ?? 0)})
                      </span>
                    )}
                  </div> */}

                  {/* Value / Right column */}
                  <div className="flex items-baseline justify-end whitespace-nowrap tabular-nums">
                    <span className="text-sm font-semibold text-slate-800 min-w-[90px] text-right">
                      {fmtNumber(value)}
                    </span>

                    {showDelta ? (
                      <span className={`ml-1 text-xs font-bold min-w-[55px] text-right ${deltaCls}`}>
                        ({fmtPctSigned(deltaPct ?? 0)})
                      </span>
                    ) : (
                      <span
                        className={`ml-2 text-xs font-bold min-w-[55px] text-right ${deltaCls}`}
                      >
                        ({fmtPctSigned(deltaPct ?? 0)})
                      </span>
                    )}

                  </div>

                </div>
              );
            };

            const currencySymbol = fmtCurrency(0).replace(/[\d.,\s]/g, "");
            const fmtNumber = (n: number) =>
              toNumberSafe(n).toLocaleString(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              });

            /* =========================
               6) UI (wrapped in bordered div with heading)
            ========================= */
            return (
              <div className="mt-4 rounded-2xl border border-slate-200 bg-white shadow-sm p-4">
                <div className="flex flex-row items-center justify-between gap-2 flex-wrap w-full mb-2 md:mb-0">
                  <PageBreadcrumb
                    pageTitle="Referral Fee Recon"
                    variant="page"
                    align="left"
                    className="mb-0 md:mb-4 text-center"
                  />
                  <DownloadButton onClick={handleDownloadExcel} />
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                  {/* LEFT PANEL */}
                  {/* <div className="bg-slate-50 rounded-2xl border border-slate-200 p-4">
                    <div className="text-base font-semibold text-slate-800 mb-4">
                      Sales Summary <span className="text-slate-500">({currencySymbol})</span>
                    </div>


                    <div className="space-y-3">
                      {leftList.map((x) => (
                        <BarRow
                          key={x.label}
                          label={x.label}
                          value={x.value}
                          total={leftTotalForBars}
                          color={x.color}
                          showDelta={false}  
                        />
                      ))}

                    </div>
                  </div> */}

                  <div className="bg-slate-50 rounded-2xl border border-slate-200 p-4">
                    <div className="text-base font-semibold text-slate-800 mb-2">
                      Sales Summary <span className="text-slate-500">({currencySymbol})</span>
                    </div>

                    {/* LEFT headers */}
                    {/* LEFT headers */}
                    <div className="grid grid-cols-[180px_1fr_120px] items-center mb-2 text-[11px] text-slate-500 font-semibold">
                      <div /> {/* label column */}
                      <div /> {/* bar column */}

                      {/* single container for both headings (you control gap) */}
                      <div className="flex justify-end gap-4 pr-2">
                        <span>Net Sales</span>
                        <span>% of Sales</span>
                      </div>
                    </div>


                    <div className="space-y-3">
                      {leftList.map((x) => (
                        <BarRow
                          key={x.label}
                          label={x.label}
                          value={x.value}
                          total={leftTotalForBars}
                          color={x.color}
                          deltaPct={x.pct}
                          showDelta={false}
                        />
                      ))}
                    </div>
                  </div>


                  {/* RIGHT PANEL */}
                  {/* <div className="bg-slate-50 rounded-2xl border border-slate-200 p-4">
                    <div className="text-base font-semibold text-slate-800 mb-4">
                      Referral Fees Breakdown <span className="text-slate-500">(£)</span>
                    </div>


                    <div className="space-y-3">
                      {rightList.map((x) => (
                        <BarRow
                          key={x.label}
                          label={x.label}
                          value={x.value}
                          total={rightTotalForBars}
                          color={x.color}
                          deltaPct={x.pct}
                          showDelta={true}
                        />
                      ))}


                    </div>
                  </div> */}

                  <div className="bg-slate-50 rounded-2xl border border-slate-200 p-4">
                    <div className="text-base font-semibold text-slate-800 mb-2">
                      Referral Fees Breakdown <span className="text-slate-500">(£)</span>
                    </div>

                    {/* RIGHT headers */}
                    {/* Header row */}
                    <div className="grid grid-cols-[180px_1fr_170px] items-center mb-2 text-[11px] font-semibold text-slate-500">
                      {/* Empty label column */}
                      <div />

                      {/* Empty bar column */}
                      <div />

                      {/* Single container for both headings */}
                      <div className="flex justify-end gap-4 pr-2">
                        <span>Fees Charged</span>
                        <span>Delta</span>
                      </div>
                    </div>

                    <div className="space-y-3">
                      {rightList.map((x) => (
                        <BarRow
                          key={x.label}
                          label={x.label}
                          value={x.value}
                          total={rightTotalForBars}
                          color={x.color}
                          deltaPct={x.pct}
                          showDelta={true}
                        />
                      ))}
                    </div>
                  </div>

                </div>
              </div>
            );
          })()}



          <div className="bg-white rounded-xl border border-slate-200 shadow-sm px-2 md:px-4 pb-2 md:pb-4 w-full overflow-x-auto mt-4">
            <div className="flex flex-col md:flex-row items-center justify-between gap-2 flex-wrap w-full mb-2 md:mb-0">
              <PageBreadcrumb
                pageTitle="Product-wise breakdown"
                variant="page"
                align="left"
                className="mt-4 mb-0 md:mb-4 text-center"
              />
              {/* <DownloadButton onClick={handleDownloadExcel} /> */}
            </div>

            {/* <AiButton /> */}

            <div className="[&_table]:w-full ">
              <GroupedDataTable
                data={groupedSkuTableDisplay}
                rowClassName={(row) =>
                  (row as any)._isTotal ? "bg-[#EFEFEF] font-semibold" : ""
                }
                baseColumns={[
                  { key: "sno", header: "S.No.", width: "60px" },
                  {
                    key: "productName", header: "Product Name", width: "180px", headerClassName: "text-left",
                    cellClassName: "text-left"
                  },
                  { key: "sku", header: "SKU", width: "110px" },
                  { key: "units", header: "Units", width: "70px" },
                  {
                    key: "sales",
                    header: `Net Sales (${currencySymbol})`,
                    width: "110px",
                    render: (_, v) => fmtMoneyNoSymbol(v),
                  },
                ]}
                groupedColumns={[
                  {
                    groupHeader: "Referral Fees",
                    columns: [
                      {
                        key: "ref_applicable",
                        header: `Applicable (${currencySymbol})`,
                        width: "110px",
                        render: (_, v) => fmtMoneyNoSymbol(v),
                      },
                      {
                        key: "ref_charged",
                        header: `Charged (${currencySymbol})`,
                        width: "110px",
                        render: (_, v) => fmtMoneyNoSymbol(v),
                      },
                    ],
                  },
                  {
                    groupHeader: "FBA Fees",
                    columns: [
                      {
                        key: "fba_applicable",
                        header: `Applicable (${currencySymbol})`,
                        width: "110px",
                        render: (_, v) => fmtMoneyNoSymbol(v),
                      },
                      {
                        key: "fba_charged",
                        header: `Charged (${currencySymbol})`,
                        width: "110px",
                        render: (_, v) => fmtMoneyNoSymbol(v),
                      },
                    ],
                  },
                  {
                    groupHeader: "Other Fees",
                    columns: [
                      {
                        key: "other_applicable",
                        header: `Applicable (${currencySymbol})`,
                        width: "110px",
                        render: (_, v) => fmtMoneyNoSymbol(v),
                      },
                      {
                        key: "other_charged",
                        header: `Charged (${currencySymbol})`,
                        width: "110px",
                        render: (_, v) => fmtMoneyNoSymbol(v),
                      },
                    ],
                  },
                  {
                    groupHeader: "Total Fees",
                    columns: [
                      {
                        key: "total_applicable",
                        header: `Applicable (${currencySymbol})`,
                        width: "110px",
                        render: (_, v) => fmtMoneyNoSymbol(v),
                      },
                      {
                        key: "total_charged",
                        header: `Charged (${currencySymbol})`,
                        width: "110px",
                        render: (_, v) => fmtMoneyNoSymbol(v),
                      },
                    ],
                  },
                ]}
              />

            </div>
          </div>
        </>
      )}
    </div>
  );
}
