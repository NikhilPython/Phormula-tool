'use client';

import React, { use, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import PageBreadcrumb from '@/components/common/PageBreadCrumb';
import DownloadIconButton from '@/components/ui/button/DownloadIconButton';
import Modalmsg from '@/components/ui/modal/Modalmsg';
import SkuMultiuseCountryUpload from '@/components/ui/modal/SkuMultiCountryUpload';
import PeriodFiltersTable, { Range } from '@/components/filters/PeriodFiltersTable';
import GroupedCollapsibleTable, {
  ColGroup,
  LeafCol,
} from '@/components/ui/table/GroupedCollapsibleTable'; // adjust path if needed
import InventoryBreakupPie from '@/components/inventory/InventoryBreakupPie';
import InventoryTopProductsPie from '@/components/inventory/InventoryBreakupPie';
import { exportInventoryReconExcel } from "@/lib/excel/exportCurrentInventoryExcel";
import { useGetUserDataQuery } from '@/lib/api/profileApi';
import {
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
} from "recharts";
import DataTable, { ColumnDef as DataTableColumnDef } from "@/components/ui/table/DataTable";
import InventoryPieCard, { InventoryPieCardHandle } from "@/components/inventory/InventoryPieCard";
import SegmentedToggle from "@/components/ui/SegmentedToggle";
/* ================= TYPES ================= */
interface Params {
  params: Promise<{
    countryName: string;
    month: string;
    year: string;
  }>;
}

type AnyRow = Record<string, any>;

type LedgerDBReadParams =
  | { range: 'monthly'; month: string; year: string; country?: string }
  | { range: 'quarterly'; quarter: string; year: string; country?: string }
  | { range: 'yearly'; year: string; country?: string };

const months = [
  'january',
  'february',
  'march',
  'april',
  'may',
  'june',
  'july',
  'august',
  'september',
  'october',
  'november',
  'december',
];

const monthNameToNumber = (m: string) => {
  const idx = months.indexOf((m || '').toLowerCase());
  return idx === -1 ? null : idx + 1; // 1..12
};

const quarterToNumber = (q: string) => {
  const v = (q || '').toUpperCase().trim();
  const n = Number(v.replace('Q', ''));
  return [1, 2, 3, 4].includes(n) ? n : null;
};

const buildQuery = (obj: Record<string, string | number | undefined | null>) => {
  const sp = new URLSearchParams();
  Object.entries(obj).forEach(([k, v]) => {
    if (v === undefined || v === null || v === '') return;
    sp.set(k, String(v));
  });
  return sp.toString();
};

const titleCase = (s: string) =>
  (s || '')
    .replaceAll('-', ' ')
    .replaceAll('_', ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());

const isNumericLike = (v: any) => {
  if (v === null || v === undefined) return false;
  if (typeof v === 'number') return Number.isFinite(v);
  if (typeof v === 'string' && v.trim() !== '') return !Number.isNaN(Number(v));
  return false;
};

const formatCell = (v: any) => {
  if (v === null || v === undefined || v === '') return '-';
  if (typeof v === 'boolean') return v ? 'Yes' : 'No';
  if (isNumericLike(v)) {
    const n = Math.abs(Number(v)); // ✅ force positive
    return Number.isInteger(n)
      ? n.toLocaleString()
      : n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }

  return String(v);
};

const toNum = (v: any) => {
  if (v === null || v === undefined || v === '') return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

const sum = (row: AnyRow, keys: string[]) => keys.reduce((acc, k) => acc + toNum(row?.[k]), 0);

const seedKey = (country: string, year: string) => `ledgerSeeded:${country}:${year}`;

type CurrencyRateRow = {
  user_currency: string;
  country: string;
  selected_currency: string;
  conversion_rate: number;
  month: string;
  year: number;
};

const currencyCodeByCountry = (country: string) => {
  const c = (country || "").toLowerCase();
  if (c === "uk") return "GBP";
  if (c === "us") return "USD";
  if (c === "ca") return "CAD";
  if (c === "in" || c === "india") return "INR";
  return "USD";
};

const currencySymbolByCode = (code: string) => {
  const c = (code || "").toUpperCase();
  if (c === "USD") return "$";
  if (c === "GBP") return "£";
  if (c === "EUR") return "€";
  if (c === "CAD") return "C$";
  if (c === "INR") return "₹";
  return c;
};

const normalizeCurrencyCode = (value: string) => {
  return (value || "").trim().toUpperCase();
};
const isTotalRow = (row: AnyRow) => {
  const msku = String(row?.msku || "").trim().toUpperCase();
  const pn = String(row?.product_name || "").trim().toUpperCase();
  return (
    msku === "TOTAL" ||
    pn === "TOTAL" ||
    msku === "GRAND TOTAL" ||
    pn === "GRAND TOTAL" ||
    row?.is_total === true ||
    row?.__isTotal === true
  );
};


const sumRowForKeys = (rowsToSum: AnyRow[], keys: string[], base: AnyRow = {}) => {
  const out: AnyRow = { ...base };
  keys.forEach((k) => {
    out[k] = rowsToSum.reduce((acc, r) => acc + toNum(r?.[k]), 0);
  });
  return out;
};

const normalizeMonth = (m: string) => {
  const mm = (m || "").toLowerCase().trim();
  const map: Record<string, string> = {
    jan: "january",
    feb: "february",
    mar: "march",
    apr: "april",
    may: "may",
    jun: "june",
    jul: "july",
    aug: "august",
    sep: "september",
    sept: "september",
    oct: "october",
    nov: "november",
    dec: "december",
  };

  // if already full month, return
  if (months.includes(mm)) return mm;

  // handle short month
  if (map[mm]) return map[mm];

  // handle numeric month "1" / "01"
  const n = Number(mm);
  if (Number.isFinite(n) && n >= 1 && n <= 12) return months[n - 1];

  return mm;
};

const normalizeYear = (y: string) => {
  const yy = (y || "").trim();
  // "26" => "2026"
  if (/^\d{2}$/.test(yy)) return `20${yy}`;
  return yy;
};

const DUMMY_ROWS: AnyRow[] = [
  {
    id: "dummy-1",
    product_name: "Sample Product A",
    msku: "SKU-001",
    sellable_sum_first: 120,
    expired_sum_first: 5,
    beginning_total: 125,
    sum_receipts: 40,
    transit_total: 40,
    sum_disposed: 2,
    sum_damaged: 1,
    sum_lost: 0,
    sum_found: 0,
    sold_total: 60,
    ending_total: 104,
    difference_total: 0,
  },
  {
    id: "__TOTAL__",
    product_name: "GRAND TOTAL",
    msku: "GRAND TOTAL",
    sellable_sum_first: 120,
    expired_sum_first: 5,
    beginning_total: 125,
    sum_receipts: 40,
    transit_total: 40,
    sum_disposed: 2,
    sum_damaged: 1,
    sum_lost: 0,
    sum_found: 0,
    sold_total: 60,
    ending_total: 104,
    difference_total: 0,
    __isTotal: true,
  },
];


const monthToShort = (m: string) => {
  const mm = (m || "").toLowerCase().trim();
  const map: Record<string, string> = {
    january: "Jan", february: "Feb", march: "Mar", april: "Apr",
    may: "May", june: "Jun", july: "Jul", august: "Aug",
    september: "Sep", october: "Oct", november: "Nov", december: "Dec",
  };
  return map[mm] || "Mon";
};

const formatPeriodLabel = (monthName: string, year4: string) => {
  const yy = String(year4 || "").slice(-2);
  return `${monthToShort(monthName)}'${yy}`;
};


const formatQuarterLabel = (q: string, year4: string) => {
  const yy = String(year4 || "").slice(-2);
  const qq = String(q || "").toUpperCase().trim(); // "Q1"..."Q4"
  return `${qq}'${yy}`;
};

const buildReconFilename = (range: Range, opts: {
  month: string;
  quarter: string;
  year: string;
}) => {
  if (range === "monthly") {
    return `Inventory Reconciliation ${formatPeriodLabel(opts.month, opts.year)}`;
  }
  if (range === "quarterly") {
    return `Inventory Reconciliation ${formatQuarterLabel(opts.quarter, opts.year)}`;
  }
  // yearly
  return `Inventory Reconciliation ${opts.year}`;
};

export default function InventoryReconciliationPage({ params }: Params) {

  const { data: userData } = useGetUserDataQuery();

  const companyName =
    (userData as any)?.companyName ||
    (userData as any)?.company_name ||
    (userData as any)?.company ||
    "";

  const brandName =
    (userData as any)?.brandName ||
    (userData as any)?.brand_name ||
    (userData as any)?.brand ||
    "";

  const { countryName: countryNameRaw, month: monthRaw, year: yearRaw } = use(params);
  const [anyExpanded, setAnyExpanded] = useState(false);

  const anyExpandedRef = useRef(false);

  const handleAnyGroupExpandedChange = useCallback((v: boolean) => {
    anyExpandedRef.current = v;   // updates immediately
    setAnyExpanded(v);            // keeps UI state in sync
  }, []);

  const countryName = decodeURIComponent(countryNameRaw ?? '').toLowerCase();

  const breakupPieRef = useRef<InventoryPieCardHandle | null>(null);
  const ageingPieRef = useRef<InventoryPieCardHandle | null>(null);

  const monthParam = normalizeMonth(decodeURIComponent(monthRaw ?? ""));
  const yearParam = normalizeYear(decodeURIComponent(yearRaw ?? ""));
  const [pieBase64, setPieBase64] = useState<string | null>(null);
  const pieBase64Ref = useRef<string | null>(null);
  const [pieMetrics, setPieMetrics] = useState<
    { name: string; value: number; pct: number }[] | null
  >(null);
  const pieMetricsRef = useRef<typeof pieMetrics>(null);

  const userHomeCurrencyCode = normalizeCurrencyCode(
    (userData as any)?.homeCurrency || "USD"
  );

  const pageCurrencyCode = normalizeCurrencyCode(
    currencyCodeByCountry(countryName)
  );

  const displayCurrencyCode =
    countryName === "global" ? userHomeCurrencyCode : pageCurrencyCode;

  useEffect(() => {
    pieBase64Ref.current = pieBase64;
  }, [pieBase64]);
  useEffect(() => {
    pieMetricsRef.current = pieMetrics;
  }, [pieMetrics]);

  /* ================= FILTER STATE ================= */
  const now = new Date();
  const prevDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const defaultMonth = months[prevDate.getMonth()];
  const defaultYear = String(prevDate.getFullYear());
  const currentMonth = months[now.getMonth()];
  const currentYear = String(now.getFullYear());

  const resolvedMonth =
    monthParam === currentMonth && yearParam === currentYear
      ? defaultMonth
      : monthParam || defaultMonth;

  const resolvedYear =
    monthParam === currentMonth && yearParam === currentYear
      ? defaultYear
      : yearParam || defaultYear;

  type InventoryViewTab = "charts" | "table" | "extra";

  const [range, setRange] = useState<Range>("monthly");
  const [selectedMonth, setSelectedMonth] = useState<string>(resolvedMonth);
  const [selectedQuarter, setSelectedQuarter] = useState<string>("Q1");
  const [selectedYear, setSelectedYear] = useState<string>(resolvedYear);
  const [exportTick, setExportTick] = useState(0);
  const [lostCompRows, setLostCompRows] = useState<AnyRow[]>([]);
  const [lostCompLoading, setLostCompLoading] = useState(false);
  const [lostCompLoaded, setLostCompLoaded] = useState(false);

  const isNA =
    monthParam?.toLowerCase() === "na" ||
    yearParam?.toLowerCase() === "na";

  const hasValidPeriod = !isNA;

  // ✅ IMPORTANT: sync state with URL params AFTER state exists
  useEffect(() => {
    if (isNA) {
      setSelectedMonth("");
      setSelectedYear("");
      return;
    }

    if (monthParam === currentMonth && yearParam === currentYear) {
      setSelectedMonth(defaultMonth);
      setSelectedYear(defaultYear);
      return;
    }

    setSelectedMonth(monthParam || defaultMonth);
    setSelectedYear(yearParam || defaultYear);
  }, [
    monthParam,
    yearParam,
    isNA,
    currentMonth,
    currentYear,
    defaultMonth,
    defaultYear,
  ]);

  const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL;

  const AMAZON_LEDGER_SEED = `${API_BASE}/amazon_api/inventory/ledger-summary`;

  const LEDGER_DB_STORE_MONTH = `${API_BASE}/amazon_api/inventory/ledger-summary/db/store-month`;
  const LEDGER_DB_STORE_QUARTER = `${API_BASE}/amazon_api/inventory/ledger-summary/db/store-quarter`;
  const LEDGER_DB_STORE_YEAR = `${API_BASE}/amazon_api/inventory/ledger-summary/db/store-year`;

  /* ================= UI STATE ================= */
  const [pageLoading, setPageLoading] = useState(true);
  const [seeding, setSeeding] = useState(false);
  const [fetching, setFetching] = useState(false);
  const [selectedRow, setSelectedRow] = useState<AnyRow | null>(null);
  const [showModal, setShowModal] = useState(false);
  const [modalMessage, setModalMessage] = useState('');

  const [showMultiuseCountry, setShowMultiuseCountry] = useState(false);
  const [activeView, setActiveView] = useState<InventoryViewTab>("charts");
  const [currencyRates, setCurrencyRates] = useState<CurrencyRateRow[]>([]);
  const [ratesLoading, setRatesLoading] = useState(false);
  const yearOptions = useMemo(() => {
    const MIN_YEAR = 2024;
    const y = new Date().getFullYear();
    return Array.from({ length: y - MIN_YEAR + 1 }, (_, i) => String(MIN_YEAR + i));
  }, []);

  /* ================= DATA STATE ================= */
  const [rows, setRows] = useState<AnyRow[]>([]);
  const [meta, setMeta] = useState<{ mode?: string; start_date?: string; end_date?: string; count?: number } | null>(
    null
  );
  const marketplaceId =
    (typeof window !== "undefined" && localStorage.getItem("marketplace_id")) ||
    "A1F83G8C2ARO7P"; // fallback

  const [breakupPie, setBreakupPie] = useState<PieDatum[]>([]);
  const [ageingPie, setAgeingPie] = useState<PieDatum[]>([]);
  const [pieLoading, setPieLoading] = useState(false);

  useEffect(() => {
    if (pageLoading) return;
    if (!hasValidPeriod) {
      setBreakupPie([]);
      setAgeingPie([]);
      return;
    }

    let cancelled = false;

    const run = async () => {
      setPieLoading(true);
      try {
        await Promise.all([fetchInventoryBreakup(), fetchInventoryAgeing()]);
      } catch (e) {
        console.error(e);
        if (!cancelled) {
          setBreakupPie([]);
          setAgeingPie([]);
        }
      } finally {
        if (!cancelled) setPieLoading(false);
      }
    };

    void run();

    return () => {
      cancelled = true;
    };
  }, [range, selectedMonth, selectedQuarter, selectedYear, marketplaceId, pageLoading, hasValidPeriod]);


  async function fetchInventoryLostCompensation() {
    if (!hasValidPeriod) {
      setLostCompRows([]);
      return;
    }

    setLostCompLoading(true);
    try {
      const mode =
        range === "monthly" ? "month" : range === "quarterly" ? "quarter" : "year";

      const q: Record<string, any> = {
        country: countryName,
        year: selectedYear,
        mode,
      };

      if (mode === "month") {
        q.month = selectedMonth;
      }

      if (mode === "quarter") {
        q.quarter = quarterToNumber(selectedQuarter);
      }

      const url = `${API_BASE}/api/inventory_lost_compensation?${buildQuery(q)}`;
      const res = await fetch(url, { headers: authHeaders() });
      const json = await res.json();

      if (!res.ok || json?.success === false) {
        throw new Error(json?.error || "Failed to fetch inventory lost compensation");
      }

      setLostCompRows(Array.isArray(json?.data) ? json.data : []);
    } catch (e: any) {
      console.error(e);
      setLostCompRows([]);
      setModalMessage(e?.message || "Failed to fetch inventory lost compensation");
      setShowModal(true);
    } finally {
      setLostCompLoading(false);
    }
  }



  useEffect(() => {
    if (pageLoading) return;
    if (activeView !== "extra") return;

    void fetchInventoryLostCompensation();
  }, [activeView, range, selectedMonth, selectedQuarter, selectedYear, countryName, pageLoading]);


  const rateMap = useMemo(() => {
    const map = new Map<string, number>();

    for (const r of currencyRates) {
      const key = [
        (r.user_currency || "").toLowerCase(),
        (r.selected_currency || "").toLowerCase(),
        (r.country || "").toLowerCase(),
        (r.month || "").toLowerCase(),
        Number(r.year),
      ].join("|");

      map.set(key, Number(r.conversion_rate));
    }

    return map;
  }, [currencyRates]);

  const getFxDb = useCallback(
    (
      from: string,
      to: string,
      country: string,
      month: string,
      year: number
    ) => {
      const f = (from || "").toLowerCase();
      const t = (to || "").toLowerCase();
      const c = (country || "").toLowerCase();
      const m = (month || "").toLowerCase();
      const y = Number(year);

      if (!f || !t) return 1;
      if (f === t) return 1;

      const directKey = `${f}|${t}|${c}|${m}|${y}`;
      const inverseKey = `${t}|${f}|${c}|${m}|${y}`;

      const direct = rateMap.get(directKey);
      if (direct != null) return direct;

      const inverse = rateMap.get(inverseKey);
      if (inverse != null && inverse !== 0) return 1 / inverse;

      return 1;
    },
    [rateMap]
  );

  const lostCompTableData = useMemo<Record<string, React.ReactNode>[]>(() => {
    return (lostCompRows || []).map((row, idx) => {
      const sourceCurrency = normalizeCurrencyCode(
        row?.currency || pageCurrencyCode
      );

      const targetCurrency =
        countryName === "global" ? userHomeCurrencyCode : pageCurrencyCode;

      const fx =
        countryName === "global"
          ? getFxDb(
            sourceCurrency,
            targetCurrency,
            countryName,
            selectedMonth,
            Number(selectedYear)
          )
          : 1;

      const convertedPrice = toNum(row?.price) * fx;
      const convertedCompensationValue = toNum(row?.compensation_value) * fx;
      const convertedCompReimbursement =
        toNum(row?.compensation_reimbursement_amount) * fx;
      const convertedSettlementAmount =
        toNum(row?.settlement_loss_event_amount) * fx;
      const convertedLossValue = toNum(row?.loss_value) * fx;
      const convertedNetValue = toNum(row?.net_value) * fx;

      const productName = String(row?.product_name || "").trim().toUpperCase();
      const msku = String(row?.msku || "").trim().toUpperCase();

      const isTotal =
        productName === "TOTAL" ||
        productName === "GRAND TOTAL" ||
        msku === "TOTAL" ||
        msku === "GRAND TOTAL" ||
        row?.__isTotal === true;

      const isOthers =
        productName === "OTHERS" ||
        msku === "OTHERS" ||
        row?.__isOthers === true;

      return {
        __sno: isTotal || isOthers ? "" : idx + 1,
        __isTotal: isTotal,
        __isOthers: isOthers,

        product_name: isTotal
          ? "Total"
          : isOthers
            ? "Others"
            : formatCell(row?.product_name),

        msku: isTotal ? "-" : isOthers ? "-" : formatCell(row?.msku),

        price: formatCell(convertedPrice),
        lost_units: formatCell(row?.lost_units),
        damaged_units: formatCell(row?.damaged_units),
        total_lost_units: formatCell(row?.total_lost_units),
        compensation_units: formatCell(row?.compensation_units),
        compensation_value: formatCell(convertedCompensationValue),
        compensation_reimbursement_amount: formatCell(convertedCompReimbursement),
        settlement_loss_event_units: formatCell(row?.settlement_loss_event_units),
        settlement_loss_event_amount: formatCell(convertedSettlementAmount),
        loss_value: formatCell(convertedLossValue),
        net_units: formatCell(row?.net_units),
        net_value: formatCell(convertedNetValue),
      };
    });
  }, [
    lostCompRows,
    countryName,
    pageCurrencyCode,
    userHomeCurrencyCode,
    getFxDb,
    selectedMonth,
    selectedYear,
  ]);

  const lostCompRowClassName = (row: Record<string, React.ReactNode>) => {
    if ((row as any).__isTotal) {
      return "bg-[#D9D9D9] font-semibold";
    }
    return "";
  };
  useEffect(() => {
    if (pageLoading) return;
    if (!hasValidPeriod) return;

    const fetchRates = async () => {
      try {
        setRatesLoading(true);

        const res = await fetch(`${API_BASE}/currency-rates`, {
          headers: authHeaders(),
        });

        if (!res.ok) {
          const errText = await res.text();
          throw new Error(`Failed to fetch rates: ${res.status} ${errText}`);
        }

        const json = await res.json();
        setCurrencyRates(Array.isArray(json) ? json : []);
      } catch (e) {
        console.error("Failed to fetch currency rates:", e);
        setCurrencyRates([]);
      } finally {
        setRatesLoading(false);
      }
    };

    void fetchRates();
  }, [API_BASE, pageLoading, hasValidPeriod]);



  const lostCompTableColumns = useMemo<
    DataTableColumnDef<Record<string, React.ReactNode>>[]
  >(() => {
    return [
      { key: "__sno", header: "S. No.", width: "70px", cellClassName: "text-center" },
      { key: "product_name", header: "Product Name", width: "220px", cellClassName: "text-left" },
      { key: "msku", header: "SKU", width: "120px", cellClassName: "text-center" },
      // { key: "asin", header: "ASIN", width: "140px", cellClassName: "text-center" },
      // { key: "product_barcode", header: "Product Barcode", width: "160px", cellClassName: "text-center" },
      {
        key: "price",
        header: `Price (${currencySymbolByCode(displayCurrencyCode)})`,
        width: "120px",
        cellClassName: "text-center",
      },
      // { key: "currency", header: "Currency", width: "90px", cellClassName: "text-center" },
      { key: "lost_units", header: "Lost Units", width: "100px", cellClassName: "text-center" },
      { key: "damaged_units", header: "Damaged Units", width: "120px", cellClassName: "text-center" },
      { key: "total_lost_units", header: "Total Lost Units", width: "130px", cellClassName: "text-center" },
      { key: "compensation_units", header: "Compensation Units", width: "150px", cellClassName: "text-center", headerClassName: "whitespace-normal break-words text-center", },
      { key: "compensation_value", header: "Compensation Value", width: "150px", cellClassName: "text-center", headerClassName: "whitespace-normal break-words text-center", },
      {
        key: "compensation_reimbursement_amount",
        header: "Compensation Reimbursement Amount",
        width: "220px",
        cellClassName: "text-center",
        headerClassName: "whitespace-normal break-words text-center",
      },
      {
        key: "settlement_loss_event_units",
        header: "Settlement Loss Event Units",
        width: "190px",
        cellClassName: "text-center",
        headerClassName: "whitespace-normal break-words text-center",
      },
      {
        key: "settlement_loss_event_amount",
        header: "Settlement Loss Event Amount",
        width: "200px",
        cellClassName: "text-center",
        headerClassName: "whitespace-normal break-words text-center",
      },
      { key: "loss_value", header: "Loss Value", width: "120px", cellClassName: "text-center" },
      { key: "net_units", header: "Net Units", width: "100px", cellClassName: "text-center" },
      { key: "net_value", header: "Net Value", width: "120px", cellClassName: "text-center" },
    ];
  }, [displayCurrencyCode]);



  /* ================= AUTH ================= */
  const authHeaders = () => {
    const token = localStorage.getItem('jwtToken');
    if (!token) throw new Error('Missing jwtToken');
    return { Authorization: `Bearer ${token}` };
  };

  /* ================= 1) SEED ONCE ================= */
  async function seedAmazonLedgerOnce(year: string) {
    // if already seeded for this (country+year), skip
    const k = seedKey(countryName, year);
    if (localStorage.getItem(k) === '1') return;

    setSeeding(true);
    try {
      const url = `${AMAZON_LEDGER_SEED}?${buildQuery({
        year,
        store_in_db: 'true',
        // optional:
        // keep_first_last: 'false'
      })}`;

      const res = await fetch(url, { headers: authHeaders() });
      const json = await res.json();

      if (!res.ok || json?.success === false) {
        throw new Error(json?.error || 'Failed to seed Amazon ledger into DB');
      }

      // mark seeded so next page load doesn't hit Amazon again
      localStorage.setItem(k, '1');

      // store latestFetchedPeriod (optional)
      try {
        localStorage.setItem(
          'latestFetchedPeriod',
          JSON.stringify({
            month: selectedMonth,
            year: String(year),
          })
        );
      } catch { }

      return json;
    } finally {
      setSeeding(false);
    }
  }

  const INVENTORY_BREAKUP_API = `${API_BASE}/api/inventory_breakup`;
  const INVENTORY_AGEING_API = `${API_BASE}/api/inventory_ageing`;

  async function fetchInventoryBreakup() {
    const mode =
      range === "monthly" ? "month" : range === "quarterly" ? "quarter" : "year";

    const q: Record<string, any> = {
      mode,
      year: selectedYear,
      marketplace_id: marketplaceId,
    };

    if (mode === "month") q.month = titleCase(selectedMonth);
    if (mode === "quarter") q.quarter = String(selectedQuarter || "Q1").toLowerCase();

    const url = `${INVENTORY_BREAKUP_API}?${buildQuery(q)}`;
    const res = await fetch(url, { headers: authHeaders() });
    const json = await res.json();

    if (!res.ok || json?.success === false) {
      throw new Error(json?.error || "Failed to fetch inventory breakup");
    }

    const t = json?.totals || {};

    // ✅ show ALL keys from totals (even if 0 you can keep or drop)
    const mapped = [
      { name: "Sellable", value: toInt(t.sellable) },
      { name: "Expired", value: toInt(t.expired) },
      { name: "Defective", value: toInt(t.defective) },
      { name: "Customer Damaged", value: toInt(t.customer_damaged) },
      { name: "Warehouse Damaged", value: toInt(t.warehouse_damaged) },
      { name: "Distributor Damaged", value: toInt(t.distributor_damaged) },
    ];

    // If you DON'T want 0 slices in chart, keep this filter:
    setBreakupPie(mapped.filter((d) => d.value > 0));

    // If you DO want to show zeros in legend too, use:
    // setBreakupPie(mapped);
  }

  async function fetchInventoryAgeing() {
    const url = `${INVENTORY_AGEING_API}?${buildQuery({
      marketplace_id: marketplaceId,
    })}`;

    const res = await fetch(url, { headers: authHeaders() });
    const json = await res.json();

    if (!res.ok || json?.success === false) {
      throw new Error(json?.error || "Failed to fetch inventory ageing");
    }

    const t = json?.totals || {};
    setAgeingPie([
      { name: "0-90", value: toInt(t.age_0_90) },
      { name: "91-180", value: toInt(t.age_91_180) },
      { name: "181-270", value: toInt(t.age_181_270) },
      { name: "271-365", value: toInt(t.age_271_365) },
      { name: "365+", value: toInt(t.age_365_plus) },
    ]);
  }

  /* ================= 2) FETCH FROM DB (store-month/quarter/year) ================= */

  const viewOptions = useMemo(
    () => [
      { value: "charts" as const, label: "Inventory Ageing Split" },
      { value: "table" as const, label: "Recon Table" },
      { value: "extra" as const, label: "Inventory Cost for Ageing" },
    ],
    []
  );


  async function fetchLedgerSummaryDB(params: LedgerDBReadParams) {
    const { range, year, country } = params;

    const q: Record<string, any> = { year };
    if (country) q.country = country;

    let endpoint = LEDGER_DB_STORE_YEAR;

    if (range === 'monthly') {
      const mm = monthNameToNumber(params.month);
      if (!mm) throw new Error('Invalid month selected');
      q.month = mm;
      endpoint = LEDGER_DB_STORE_MONTH;
    }

    if (range === 'quarterly') {
      const qq = quarterToNumber(params.quarter);
      if (!qq) throw new Error('Invalid quarter selected');
      q.quarter = qq;
      endpoint = LEDGER_DB_STORE_QUARTER;
    }

    const url = `${endpoint}?${buildQuery(q)}`;

    const res = await fetch(url, { headers: authHeaders() });
    const json = await res.json();

    if (!res.ok || json?.success === false) {
      throw new Error(json?.error || 'Failed to fetch ledger summary from DB');
    }

    const items = Array.isArray(json?.items) ? json.items : [];

    return {
      items,
      meta: {
        mode: json?.mode,
        start_date: json?.start_date,
        end_date: json?.end_date,
        count: json?.count,
      },
    };
  }

  /* ================= MAIN FLOW ================= */

  const debounceRef = useRef<number | null>(null);
  const initializedRef = useRef(false);

  type PieDatum = { name: string; value: number };



  const toInt = (v: any) => {
    const n = Number(v);
    return Number.isFinite(n) ? Math.trunc(n) : 0;
  };


  const runDBFetchForFilters = async () => {
    if (!hasValidPeriod) {
      setRows([]);     // real rows clear
      setMeta(null);
      return;
    }
    setFetching(true);
    try {
      const country = countryName;

      let payload: LedgerDBReadParams;

      if (range === 'monthly') {
        payload = { range: 'monthly', month: selectedMonth, year: selectedYear, country };
      } else if (range === 'quarterly') {
        payload = { range: 'quarterly', quarter: selectedQuarter, year: selectedYear, country };
      } else {
        payload = { range: 'yearly', year: selectedYear, country };
      }

      const { items, meta } = await fetchLedgerSummaryDB(payload);
      setRows(items);
      setMeta(meta);
    } catch (e: any) {
      console.error(e);
      setRows([]);
      setMeta(null);
      setModalMessage(e?.message || 'Failed to load DB summary');
      setShowModal(true);
    } finally {
      setFetching(false);
    }
  };

  // initial mount
  useEffect(() => {
    setPageLoading(false);
  }, []);

  // Seed once per year (only when year changes or first time) + fetch DB
  useEffect(() => {
    if (pageLoading) return;

    // 🚨 NA/NA case → NO seed, NO API
    if (!hasValidPeriod) {
      setRows([]);
      setMeta(null);
      return;
    }

    const doSeedThenFetch = async () => {
      try {
        await seedAmazonLedgerOnce(selectedYear);
      } catch (e: any) {
        console.error(e);
        setModalMessage(e?.message || "Seed failed");
        setShowModal(true);
      } finally {
        await runDBFetchForFilters();
      }
    };

    if (!initializedRef.current) {
      initializedRef.current = true;
      void doSeedThenFetch();
      return;
    }

    void doSeedThenFetch();
  }, [selectedYear, countryName, pageLoading]);



  // When filters change (range/month/quarter/year), DO NOT seed again. Only DB fetch.
  useEffect(() => {
    if (pageLoading) return;
    if (!initializedRef.current) return;

    if (debounceRef.current) window.clearTimeout(debounceRef.current);
    debounceRef.current = window.setTimeout(() => {
      void runDBFetchForFilters();
    }, 350);

    return () => {
      if (debounceRef.current) window.clearTimeout(debounceRef.current);
    };
    // ✅ include selectedYear so year changes also re-fetch via debounce if needed
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [range, selectedMonth, selectedQuarter, selectedYear]);

  /* ================= TABLE CONFIG (CUSTOM ORDER + GROUPS) ================= */
  const keyOrder = useMemo(() => {
    const first = rows?.[0];
    if (!first) return [];
    return Object.keys(first);
  }, [rows]);



  const displayRows = useMemo(() => {
    if (!rows || rows.length === 0) return [];

    // Separate out any backend GRAND TOTAL row (if present)
    const grandTotalRow = rows.find(isTotalRow) || null;

    // Only actual products (exclude total rows)
    const dataRows = rows.filter((r) => !isTotalRow(r));

    // ✅ Sort products by "Inventory at month end" (Total) DESC
    // Using ending_total since that is the month-end total shown in your group.
    const sortedDataRows = [...dataRows].sort((a, b) => {
      return toNum(b?.ending_total) - toNum(a?.ending_total);
    });

    // Take first 9 data rows
    const top = sortedDataRows.slice(0, 9);
    const remaining = sortedDataRows.slice(9);

    // Keys we should sum for numeric totals
    const keys = Array.from(
      new Set(
        dataRows.flatMap((r) =>
          Object.keys(r || {}).filter((k) => isNumericLike(r?.[k]))
        )
      )
    );

    const out: AnyRow[] = [...top];

    // Add OTHERS if there are remaining rows
    if (remaining.length > 0) {
      const others = sumRowForKeys(remaining, keys, {
        id: '__OTHERS__',
        msku: 'OTHERS',
        product_name: `OTHERS`,
        __isOthers: true,
      });
      out.push(others);
    }

    // Add TOTAL at end (prefer backend grand total)
    const total =
      grandTotalRow
        ? { ...grandTotalRow, id: '__TOTAL__', __isTotal: true }
        : sumRowForKeys(dataRows, keys, {
          id: '__TOTAL__',
          msku: 'GRAND TOTAL',
          product_name: 'GRAND TOTAL',
          __isTotal: true,
        });

    out.push(total);

    return out;
  }, [rows]);


  const totalRow = useMemo(() => {
    const r = displayRows?.find((x) => x?.__isTotal === true) || null;
    return r;
  }, [displayRows]);

  const pieSourceRow = selectedRow ?? totalRow;

  const effectiveRows = useMemo(() => {
    if (!hasValidPeriod) return DUMMY_ROWS;
    return displayRows;
  }, [hasValidPeriod, displayRows]);


  // Sign row (stable sets)
  const SIGN_PLUS = useMemo(
    () =>
      new Set([
        "sum_disposed",
        "sum_damaged",
        "sum_unknown_events",
        "sum_other_events",
        "sum_vendor_returns",
        "sum_lost",
        // if you want Total to show + too, include "__other_items_total"
        // "__other_items_total",
      ]),
    []
  );

  const SIGN_MINUS = useMemo(
    () =>
      new Set([
        "sum_found", // as per your screenshot (-)
      ]),
    []
  );

  const getSignForCol = useCallback(
    (colKey: string) => {
      if (SIGN_PLUS.has(colKey)) return { text: "(+)", className: "text-green-700" };
      if (SIGN_MINUS.has(colKey)) return { text: "(-)", className: "text-[#ff5c5c]" };
      return null;
    },
    [SIGN_PLUS, SIGN_MINUS]
  );


  // 1) Left columns: S.No + Product Name
  const leftCols: LeafCol<AnyRow>[] = [
    { key: '__sno', label: 'S. No.', width: 70, align: 'center' },
    { key: 'product_name', label: 'Product Name', width: 120, align: 'left' },
    { key: 'msku', label: 'SKU', width: 110, align: 'center' },
  ];


  // 2) Group: Inventory at the beginning of the month
  const groups: ColGroup<AnyRow>[] = useMemo(
    () => [
      // =======================
      // Group 1: Beginning (you already have)
      // =======================
      {
        id: 'beginning',
        label: 'Inventory at the beginning of the month',
        headerClassName: 'min-w-[120px]',
        collapsedCols: [
          { key: '__beginning_total', label: 'Total', width: 140, align: 'center' }
        ]
        ,
        expandedCols: [
          { key: 'sellable_sum_first', label: 'Sellable', width: 110, align: 'center' },
          { key: '__beginning_damaged_total', label: 'Damaged', width: 110, align: 'center' },
          { key: 'expired_sum_first', label: 'Expired', width: 110, align: 'center' },
          // ⚠️ If you later add an opening transit field, replace this
          { key: 'sum_in_transit_between_warehouses', label: 'Transit (Between WH)', width: 110, align: 'center' },
          { key: 'beginning_total', label: 'Total', width: 110, align: 'center' },
        ],
      },

      // =======================
      // Group 2: Units in transit
      // =======================
      {
        id: 'units_in_transit',
        label: 'Units in transit',
        headerClassName: 'min-w-[120px]',
        collapsedCols: [
          { key: '__transit_total', label: 'Total', width: 100, align: 'center' },
        ],
        expandedCols: [
          // map these to your actual keys:
          // "in transit" -> transit_total (based on your sample)
          { key: 'transit_total', label: 'In Transit', width: 110, align: 'center' },

          // "delivered" -> I’m assuming receipts represent delivered to FC.
          // If you have a better field, replace sum_receipts with it.
          { key: 'sum_receipts', label: 'Delivered', width: 110, align: 'center' },
          { key: '__transit_total', label: 'Total', width: 110, align: 'center' },
        ],
      },

      // =======================
      // Group 3: Other Items
      // =======================
      {
        id: 'other_items',
        label: 'Other Items',
        headerClassName: 'min-w-[120px]',
        collapsedCols: [
          {
            key: '__other_items_total',
            label: 'Total',
            width: 90, align: 'center'
          },
        ],
        expandedCols: [
          { key: 'sum_disposed', label: 'Units Disposed', width: 110, align: 'center' },
          { key: 'sum_damaged', label: 'Damaged', width: 110, align: 'center' },
          { key: 'sum_unknown_events', label: 'Unknown Event', width: 110, align: 'center' },
          { key: 'sum_other_events', label: 'Other Events', width: 110, align: 'center' },
          { key: 'sum_vendor_returns', label: 'Vendor Return', width: 110, align: 'center' },
          { key: 'sum_lost', label: 'Lost', width: 110, align: 'center' },
          { key: 'sum_found', label: 'Found', width: 110, align: 'center' },
          { key: '__other_items_total', label: 'Total', width: 110, align: 'center' },
        ],
      },
      // =======================
      // Group 4: Units Sold
      // =======================
      {
        id: 'units_sold',
        label: 'Units Sold',
        headerClassName: 'min-w-[120px]',
        collapsedCols: [
          { key: '__units_sold_net', label: 'Net Units', width: 90, align: 'center' },
        ],
        expandedCols: [
          // Map based on your data keys
          { key: '__units_sold_gross', label: 'Gross Sales', width: 110, align: 'center' },
          { key: '__units_sold_returns', label: 'Return', width: 110, align: 'center' },
          { key: '__units_sold_net', label: 'Net Units', width: 110, align: 'center' },
        ],
      },
      {
        id: 'open_orders',
        label: 'Open orders',
        headerClassName: 'min-w-[120px]',
        collapsedCols: [
          {
            key: '__open_orders_total',
            label: 'Total',
            width: 110, align: 'center'
          },
        ],
        expandedCols: [
          {
            key: '__open_orders_beginning',
            label: 'Beginning',
            width: 110, align: 'center'
          },
          {
            key: '__open_orders_end',
            label: 'End',
            width: 110, align: 'center'
          },
          {
            key: '__open_orders_total',
            label: 'Total',
            width: 110, align: 'center'
          },
        ],
      },

      // =======================
      // Group 6: Inventory at month end
      // =======================
      {
        id: 'ending',
        label: 'Inventory at month end',
        headerClassName: 'min-w-[120px]',
        collapsedCols: [
          { key: '__ending_total', label: 'Total', width: 110, align: 'center' },
        ],
        expandedCols: [
          { key: 'sellable_sum_last', label: 'Sellable', width: 110, align: 'center' },
          { key: '__ending_damaged_lost_total', label: 'Damaged/Lost', width: 110, align: 'center' },
          { key: 'expired_sum_last', label: 'Expired', width: 110, align: 'center' },

          {
            key: '__ending_transit_placeholder',
            label: 'Transit (Between WH)',
            width: 110, align: 'center'
          },


          { key: 'ending_total', label: 'Total', width: 110, align: 'center' },
        ],
      },


    ],
    []
  );

  // 3) Everything else goes to singleCols for now (optional)
  const usedKeys = useMemo(
    () =>
      new Set<string>([
        // left
        '__sno',
        'msku',
        'product_name',

        // group 1 beginning
        '__beginning_total',
        '__beginning_damaged_total',
        'sellable_sum_first',
        'expired_sum_first',
        'sum_in_transit_between_warehouses',
        'beginning_total',
        'warehouse_damaged_sum_first',
        'customer_damaged_sum_first',
        'distributor_damaged_sum_first',
        'defective_sum_first',

        // group 2 transit
        '__transit_total',
        'transit_total',
        'sum_receipts',

        // group 3 other items
        '__other_items_total',
        'sum_disposed',
        'sum_damaged',
        'sum_unknown_events',
        'sum_other_events',
        'sum_vendor_returns',
        'sum_lost',
        'sum_found',
        // Units sold computed
        '__units_sold_gross',
        '__units_sold_returns',
        '__units_sold_net',
        '__open_orders_beginning',
        '__open_orders_end',
        '__open_orders_total',

        // Ending inventory computed + raw
        '__ending_total',
        '__ending_damaged_lost_total',
        'sellable_sum_last',
        'expired_sum_last',
        'ending_total',

        // Difference
        '__difference_total',
        'difference_total',

      ]),
    []
  );

  const singleCols: LeafCol<AnyRow>[] = useMemo(
    () => [
      {
        key: "inventory_coverage_ratio",
        label: "Inventory Coverage Ratio",
        width: 140,
        align: "center",
      },
      {
        key: "difference_total",
        label: "Difference",
        width: 90,
        align: "center",
      },
    ],
    []
  );




  const leftKeys = useMemo(() => {
    const preferred = ['msku', 'sku', 'product_name', 'asin', 'fnsku', 'disposition'];
    const present = preferred.filter((k) => keyOrder.includes(k));
    if (present.length > 0) return present.slice(0, 3);
    return keyOrder.slice(0, Math.min(3, keyOrder.length));
  }, [keyOrder]);



  const restKeys = useMemo(() => keyOrder.filter((k) => !leftKeys.includes(k)), [keyOrder, leftKeys]);

  const { numericKeys, otherKeys } = useMemo(() => {
    const nums: string[] = [];
    const oth: string[] = [];

    restKeys.forEach((k) => {
      const sample = rows.slice(0, 5).map((r) => r?.[k]);
      const isNum = sample.some((v) => isNumericLike(v));
      if (isNum) nums.push(k);
      else oth.push(k);
    });

    return { numericKeys: nums, otherKeys: oth };
  }, [restKeys, rows]);



  const getRowClassName = (row: AnyRow) => {
    const msku = String(row?.msku || '').trim().toUpperCase();
    const isGrand = isTotalRow(row) || msku === 'TOTAL' || row?.__isTotal === true;
    const isOthers = msku === 'OTHERS' || row?.__isOthers === true;

    if (isGrand) return 'bg-[#D9D9D9] font-semibold';
    if (isOthers) return '';
    return '';
  };



  const getValue = (row: AnyRow, colKey: string, exportIndex?: number) => {
    const isSpecialRow =
      row?.__isTotal === true ||
      row?.__isOthers === true ||
      isTotalRow(row);

    if (colKey === 'msku' && isSpecialRow) {
      return '-';
    }

    const pn = String(row?.product_name || '').trim().toUpperCase();

    if (colKey === 'product_name') {
      if (pn === 'OTHERS') return 'Others';
      if (pn === 'TOTAL' || pn === 'GRAND TOTAL') return 'Total';
    }

    if (colKey === "__sno") {
      // leave blank for total rows
      if (isSpecialRow) return "";

      // ✅ for export: use provided index (1-based)
      if (typeof exportIndex === "number") return String(exportIndex + 1);

      // ✅ for UI: fallback to your existing logic
      const idx = displayRows.findIndex(
        (r) => (r?.id ?? r?.msku) === (row?.id ?? row?.msku)
      );
      return idx >= 0 ? String(idx + 1) : "";
    }


    // Beginning inventory -> Transit (Between WH) is not available yet
    if (colKey === 'sum_in_transit_between_warehouses') {
      return '-';
    }

    if (
      colKey === '__open_orders_beginning' ||
      colKey === '__open_orders_end' ||
      colKey === '__open_orders_total'
    ) {
      return '-';
    }

    if (colKey === '__ending_transit_placeholder') {
      return '-';
    }


    // Beginning group computed fields
    if (colKey === '__beginning_damaged_total') {
      return formatCell(
        toNum(row?.warehouse_damaged_sum_first) +
        toNum(row?.customer_damaged_sum_first) +
        toNum(row?.distributor_damaged_sum_first) +
        toNum(row?.defective_sum_first)
      );
    }
    if (colKey === '__beginning_total') {
      return formatCell(row?.beginning_total);
    }

    // =======================
    // Units in Transit (DIRECT DB MAPPING)
    // =======================

    // In Transit → not available
    if (colKey === 'transit_total' && false) {
      // safeguard, never hit
      return '-';
    }

    // Explicit placeholder for In Transit column
    if (colKey === 'transit_total_in_transit_placeholder') {
      return '-';
    }

    // In Transit column (your expandedCols uses `transit_total` for label "In Transit")
    // We override it to "-"
    if (colKey === 'transit_total') {
      return '-';
    }

    // Delivered → DB value
    if (colKey === 'sum_receipts') {
      return formatCell(row?.sum_receipts);
    }

    // Total → DB value
    if (colKey === '__transit_total') {
      return formatCell(row?.transit_total);
    }


    // Other items total
    if (colKey === '__other_items_total') {
      const total = toNum(row?.other_total)

      return formatCell(total);
    }

    // =======================
    // Units Sold (DIRECT DB MAPPING)
    // =======================

    // Gross Sales → sum_customer_shipments
    if (colKey === '__units_sold_gross') {
      return formatCell(Math.abs(toNum(row?.sum_customer_shipments)));
    }

    // Returns → sum_customer_returns
    if (colKey === '__units_sold_returns') {
      return formatCell(Math.abs(toNum(row?.sum_customer_returns)));
    }

    // Net Units → sold_total
    if (colKey === '__units_sold_net') {
      return formatCell(Math.abs(toNum(row?.sold_total)));
    }

    // =======================
    // Ending inventory computed
    // =======================
    if (colKey === '__ending_total') {
      return formatCell(row?.ending_total);
    }

    // =======================
    // Inventory at month end -> Damaged / Lost
    // =======================
    if (colKey === '__ending_damaged_lost_total') {
      const total =
        toNum(row?.defective_sum_last) +
        toNum(row?.warehouse_damaged_sum_last) +
        toNum(row?.customer_damaged_sum_last) +
        toNum(row?.distributor_damaged_sum_last);

      return formatCell(total);
    }


    // =======================
    // Difference
    // =======================
    if (colKey === '__difference_total') {
      return formatCell(row?.difference_total);
    }

    if (colKey === "inventory_coverage_ratio") {
      const n = Math.abs(toNum(row?.inventory_coverage_ratio)); // ✅ force positive
      if (!n) return "-";
      return n.toFixed(1); // ✅ no "x"
    }


    return formatCell(row?.[colKey]);
  };

  async function svgToPngDataUrl(svgNode: SVGElement, width = 900, height = 600) {
    const serializer = new XMLSerializer();
    const svgString = serializer.serializeToString(svgNode);

    const svgBlob = new Blob([svgString], { type: "image/svg+xml;charset=utf-8" });
    const url = URL.createObjectURL(svgBlob);

    try {
      const img = new Image();
      img.decoding = "async";
      img.crossOrigin = "anonymous";

      await new Promise<void>((resolve, reject) => {
        img.onload = () => resolve();
        img.onerror = () => reject(new Error("SVG load failed"));
        img.src = url;
      });

      const canvas = document.createElement("canvas");
      canvas.width = width;
      canvas.height = height;

      const ctx = canvas.getContext("2d");
      if (!ctx) throw new Error("No canvas context");

      ctx.fillStyle = "#ffffff";
      ctx.fillRect(0, 0, width, height);
      ctx.drawImage(img, 0, 0, width, height);

      return canvas.toDataURL("image/png");
    } finally {
      URL.revokeObjectURL(url);
    }
  }

  const getVisibleExportCols = useCallback(() => {
    const cols: { key: string; header: string }[] = [];
    const expandedNow = anyExpandedRef.current; // ✅ always correct at click time

    for (const c of leftCols) cols.push({ key: c.key, header: String(c.label) });

    for (const g of groups) {
      const leafCols = expandedNow ? g.expandedCols : g.collapsedCols;

      for (const c of leafCols) {
        cols.push({
          key: c.key,
          header: `${String(g.label)} - ${String(c.label)}`,
        });
      }
    }

    for (const c of singleCols) cols.push({ key: c.key, header: String(c.label) });

    return cols;
  }, [leftCols, groups, singleCols]);


  // 2) Build export rows using your SAME getValue() so Excel matches what UI displays
  const buildExportRows = useCallback(() => {
    const visibleCols = getVisibleExportCols();

    return (displayRows || []).map((row) => {
      const out: Record<string, any> = {};

      // insertion order here becomes Excel column order
      for (const col of visibleCols) {
        out[col.header] = getValue(row, col.key);
      }

      return out;
    });
  }, [displayRows, getVisibleExportCols, getValue]);

  // ✅ Build headers/columns EXACTLY like the UI (leftCols -> groups -> singleCols)
  const getExportColumns = useCallback(() => {
    const cols: { key: string; header: string }[] = [];

    // If table is expanded, export expandedCols; else export collapsedCols
    const expandedNow = anyExpandedRef.current;

    // Left columns
    for (const c of leftCols) {
      cols.push({ key: c.key, header: String(c.label) });
    }

    // Grouped columns
    for (const g of groups) {
      const leafCols = expandedNow ? g.expandedCols : g.collapsedCols;

      for (const c of leafCols) {
        // Important: Prefix group label so headers like "Total" don't collide
        cols.push({
          key: c.key,
          header: `${String(g.label)} - ${String(c.label)}`,
        });
      }
    }

    // Single columns
    for (const c of singleCols) {
      cols.push({ key: c.key, header: String(c.label) });
    }

    return cols;
  }, [leftCols, groups, singleCols]);

  // const pieRows = useMemo(() => {
  //   return effectiveRows.filter(
  //     (r) =>
  //       !r?.__isTotal &&
  //       String(r?.product_name || "").toUpperCase() !== "GRAND TOTAL" &&
  //       String(r?.product_name || "").toUpperCase() !== "TOTAL" &&
  //       String(r?.product_name || "").toUpperCase() !== "OTHERS"
  //   );
  // }, [effectiveRows]);

  const pieRows = useMemo(() => {
    const dataOnly = (rows || []).filter((r) => !isTotalRow(r));
    return [...dataOnly].sort((a, b) => toNum(b?.ending_total) - toNum(a?.ending_total));
  }, [rows]);


  // ✅ Build export rows using getValue() so Excel matches the UI cells
  const buildExcelRows = useCallback(() => {
    const cols = getExportColumns();

    return (displayRows || []).map((row) => {
      const out: Record<string, any> = {};
      for (const col of cols) {
        out[col.header] = getValue(row, col.key);
      }
      return out;
    });
  }, [displayRows, getExportColumns, getValue]);


  const getExpandedExportCols = useCallback(() => {
    const cols: { key: string; header: string }[] = [];

    // left columns
    for (const c of leftCols) cols.push({ key: c.key, header: String(c.label) });

    // always expanded leaf cols
    for (const g of groups) {
      for (const c of g.expandedCols) {
        cols.push({
          key: c.key,
          header: `${String(g.label)} - ${String(c.label)}`,
        });
      }
    }

    // single cols
    for (const c of singleCols) cols.push({ key: c.key, header: String(c.label) });

    return cols;
  }, [leftCols, groups, singleCols]);

  const buildReconExportDataRows = useCallback(() => {
    const cols = getExpandedExportCols();

    const grandTotalRow = rows.find(isTotalRow) || null;
    const dataOnly = rows.filter((r) => !isTotalRow(r));
    const all = grandTotalRow ? [...dataOnly, grandTotalRow] : dataOnly;

    return all.map((row, i) => {
      const out: Record<string, any> = {};
      for (const col of cols) {
        out[col.header] = getValue(row, col.key, i); // ✅ pass index
      }
      return out;
    });
  }, [rows, getExpandedExportCols, getValue]);

  const handleDownloadXLSX = async () => {
    const dataRows = buildReconExportDataRows();
    if (!dataRows.length) return;

    const periodLabel =
      range === "monthly"
        ? formatPeriodLabel(selectedMonth, selectedYear)
        : range === "quarterly"
          ? formatQuarterLabel(selectedQuarter, selectedYear)
          : selectedYear;

    const breakupChartBase64 = breakupPieRef.current?.getExportImage() || null;
    const ageingChartBase64 = ageingPieRef.current?.getExportImage() || null;

    const filenameBase = buildReconFilename(range, {
      month: selectedMonth,
      quarter: selectedQuarter,
      year: selectedYear,
    });

    await exportInventoryReconExcel({
      filename: `${filenameBase}.xlsx`,
      titleLine: `Amazon ${countryName?.toUpperCase()} - Inventory Recon - ${periodLabel}`,
      countryName,
      titleCountry: countryName?.toUpperCase(),
      platformLabel: "Amazon",
      periodLabel,
      companyName,
      brandName,
      dataRows,
      breakupChartBase64,
      ageingChartBase64,
    });
  };

  if (pageLoading) {
    return (
      <div className="w-full px-4 py-6">
        <div className="animate-pulse text-sm text-neutral-500">Loading...</div>
      </div>
    );
  }

  return (
    <div className="w-full">

      <div className="sticky top-0 z-40 w-full border-b border-gray-200 bg-[#F7F7F7] ">
        <div className="flex flex-col gap-4">
          {/* Row 1: title left, filters right */}
          <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
            {/* Left title block */}
            <div className="flex flex-col">
              <PageBreadcrumb
                variant="page"
                align="left"
                textSize="2xl"
                pageTitle={
                  <div className="flex flex-wrap items-baseline gap-2">
                    <span className="text-[#414042] font-bold">
                      Inventory Reconciliation -
                    </span>
                    <span className="text-[#60a68e] font-bold">
                      {countryName?.toUpperCase()}
                    </span>
                  </div>
                }
              />

              {/* optional subtitle spacing like second screenshot */}
              {/* <p className="mt-1 text-sm text-neutral-600">
          Add your subtitle here if needed
        </p> */}
            </div>

            {/* Right filters */}
            <div className="flex flex-wrap items-center gap-3 lg:justify-end">
              <PeriodFiltersTable
                range={range}
                selectedMonth={selectedMonth}
                selectedQuarter={selectedQuarter}
                selectedYear={selectedYear}
                yearOptions={yearOptions}
                onRangeChange={(v) => setRange(v)}
                onMonthChange={(v) => {
                  const raw = String(v ?? "").trim();
                  const parts = raw.split(/\s+/).filter(Boolean);
                  const monthPart = parts[0] ?? raw;
                  const yearPart = parts.find((p) => /^\d{4}$/.test(p));

                  setSelectedMonth(normalizeMonth(monthPart));
                  if (yearPart) setSelectedYear(normalizeYear(yearPart));
                }}
                onQuarterChange={(v) => setSelectedQuarter(String(v).toUpperCase())}
                onYearChange={(v) => setSelectedYear(String(v))}
                allowedRanges={["monthly", "quarterly", "yearly"]}
              />

              <DownloadIconButton onClick={handleDownloadXLSX} size="md" />
            </div>
          </div>

          {/* Row 2: toggle only */}
          <div className="flex items-center">
            <SegmentedToggle
              value={activeView}
              options={viewOptions}
              onChange={(val) => setActiveView(val as InventoryViewTab)}
              compact
              textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
            />
          </div>
        </div>
      </div>

      {/* ✅ Pie charts row */}
      {activeView === "charts" && (
        <div className="mt-5 grid grid-cols-1 gap-4 lg:grid-cols-2">
          <InventoryPieCard
            ref={breakupPieRef}
            title="Inventory Breakup"
            data={breakupPie}
            loading={pieLoading}
            height={320}
          />

          <InventoryPieCard
            ref={ageingPieRef}
            title="Inventory Ageing"
            data={ageingPie}
            loading={pieLoading}
            height={320}
          />
        </div>
      )}

      {/* Table */}
      {activeView === "table" && (
        <div
          className={[
            "mt-5 w-full rounded-lg border border-gray-200 bg-white",
            "overflow-x-auto",
            "[-webkit-overflow-scrolling:touch]",
          ].join(" ")}
        >
          {effectiveRows.length === 0 ? (
            <div className="p-6 text-sm text-neutral-600">No rows returned.</div>
          ) : (
            <GroupedCollapsibleTable
              rows={effectiveRows}
              getRowKey={(r, idx) => r?.id ?? r?.msku ?? idx}
              leftCols={leftCols}
              groups={groups}
              singleCols={singleCols}
              getValue={getValue}
              getRowClassName={getRowClassName}
              onAnyGroupExpandedChange={handleAnyGroupExpandedChange}
              tableClassName={
                anyExpanded
                  ? "min-w-[900px] w-full table-auto border-collapse bg-white text-[#414042] text-xs 2xl:text-sm"
                  : "w-full table-fixed border-collapse bg-white text-[#414042] text-xs 2xl:text-sm"
              }
              headerRow1ClassName="bg-[#5EA68E] text-[#f8edcf]"
              headerRow2ClassName="bg-[#5EA68E] text-[#f8edcf]"
              showSignRowInBody
              getSignForCol={getSignForCol}
            />
          )}
        </div>
      )}

      {activeView === "extra" && (
        <div className="mt-5">
          <DataTable<Record<string, React.ReactNode>>
            columns={lostCompTableColumns}
            data={lostCompTableData}
            loading={lostCompLoading || ratesLoading}
            paginate={false}
            stickyHeader
            scrollY={false}
            maxHeight="auto"
            emptyMessage="No rows returned."
            tableClassName="text-xs 2xl:text-sm"
            rowClassName={lostCompRowClassName}
          />
        </div>
      )}

      {/* Multi-country modal (kept) */}
      {showMultiuseCountry && (
        <div
          onClick={() => setShowMultiuseCountry(false)}
          className="fixed inset-0 z-[1000] flex items-center justify-center bg-black/50 p-4"
        >
          <div
            onClick={(e) => e.stopPropagation()}
            className="relative flex h-[30vh] w-full max-w-lg flex-col items-center justify-center overflow-y-auto rounded-lg bg-white p-4 shadow-lg"
          >
            <button
              onClick={() => setShowMultiuseCountry(false)}
              className="absolute right-3 top-2 text-2xl leading-none text-neutral-600 hover:text-neutral-900"
              aria-label="Close"
              type="button"
            >
              &times;
            </button>

            <SkuMultiuseCountryUpload
              onClose={function (): void {
                throw new Error('Function not implemented.');
              }}
              onComplete={function (): void {
                throw new Error('Function not implemented.');
              }}
            />
          </div>
        </div>
      )}

      <Modalmsg
        show={showModal}
        message={modalMessage}
        onClose={() => setShowModal(false)}
        onCancel={() => setShowModal(false)}
      />
    </div>
  );
}
