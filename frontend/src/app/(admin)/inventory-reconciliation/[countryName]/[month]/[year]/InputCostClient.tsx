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

// localStorage keys
const seedKey = (country: string, year: string) => `ledgerSeeded:${country}:${year}`;

// const isTotalRow = (row: AnyRow) => {
//   const msku = String(row?.msku || '').trim().toUpperCase();
//   const pn = String(row?.product_name || '').trim().toUpperCase();
//   return (
//     msku === 'TOTAL' ||
//     pn === 'TOTAL' ||
//     row?.is_total === true ||
//     row?.__isTotal === true
//   );
// };

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


export default function InventoryReconciliationPage({ params }: Params) {

  // ✅ profile data (API)
  const { data: userData } = useGetUserDataQuery();

  // ✅ safe derived strings
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

  const monthParam = normalizeMonth(decodeURIComponent(monthRaw ?? ""));
  const yearParam = normalizeYear(decodeURIComponent(yearRaw ?? ""));
  const [pieBase64, setPieBase64] = useState<string | null>(null);
  const pieBase64Ref = useRef<string | null>(null);
  const [pieMetrics, setPieMetrics] = useState<
    { name: string; value: number; pct: number }[] | null
  >(null);
  const pieMetricsRef = useRef<typeof pieMetrics>(null);

  useEffect(() => {
    pieBase64Ref.current = pieBase64;
  }, [pieBase64]);
  useEffect(() => {
    pieMetricsRef.current = pieMetrics;
  }, [pieMetrics]);

  /* ================= FILTER STATE ================= */
  const now = new Date();
  const currentMonth = months[now.getMonth()];
  const currentYear = String(now.getFullYear());

  const [range, setRange] = useState<Range>("monthly");
  const [selectedMonth, setSelectedMonth] = useState<string>(monthParam || currentMonth);
  const [selectedQuarter, setSelectedQuarter] = useState<string>("Q1");
  const [selectedYear, setSelectedYear] = useState<string>(yearParam || currentYear);
const [exportTick, setExportTick] = useState(0);

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

  if (monthParam) setSelectedMonth(monthParam);
  if (yearParam) setSelectedYear(yearParam);
}, [monthParam, yearParam, isNA]);




  const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL;

  // ✅ 1) Amazon → DB seed route (run ONCE only per year)
  const AMAZON_LEDGER_SEED = `${API_BASE}/amazon_api/inventory/ledger-summary`;

  // ✅ 2) DB aggregation routes (HIT ON FILTER CHANGE)
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

  /* ================= 2) FETCH FROM DB (store-month/quarter/year) ================= */
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
    const dataRows = rows.filter((r) => !isTotalRow(r));

    // Take first 9 data rows
    const top = dataRows.slice(0, 9);
    const remaining = dataRows.slice(9);

    // Keys we should sum for numeric totals (sum all numeric-like keys seen in data)
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

    // Add TOTAL at end
    // Prefer backend grand total if available, else compute from all dataRows
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
        key: 'difference_total',
        label: 'Difference',
        width: 90, align: 'center'
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

  const pieRows = useMemo(() => {
  return effectiveRows.filter(
    (r) =>
      !r?.__isTotal &&
      String(r?.product_name || "").toUpperCase() !== "GRAND TOTAL" &&
      String(r?.product_name || "").toUpperCase() !== "TOTAL" &&
      String(r?.product_name || "").toUpperCase() !== "OTHERS"
  );
}, [effectiveRows]);

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


  // const handleDownloadXLSX = async () => {
  //   const dataRows = buildReconExportDataRows();
  //   if (!dataRows.length) return;

  //   const periodLabel = formatPeriodLabel(selectedMonth, selectedYear); // ✅ Dec'25

  //   exportInventoryReconExcel({
  //     filename: "Inventory_Reconciliation.xlsx",
  //     titleLine: `Amazon ${countryName?.toUpperCase()} - Inventory Recon - ${periodLabel}`,
  //     countryName,
  //     titleCountry: countryName?.toUpperCase(),
  //     platformLabel: "Amazon",
  //     periodLabel,         // ✅ Dec'25
  //     companyName,
  //     brandName,
  //     dataRows,
  //   });
  // };

 const handleDownloadXLSX = async () => {
  const dataRows = buildReconExportDataRows();
  if (!dataRows.length) return;

  const periodLabel = formatPeriodLabel(selectedMonth, selectedYear);

  // ✅ force pie to regenerate a fresh composed image (pie + legend metrics)
  setExportTick((t) => t + 1);

  // wait 2 frames so chart canvas + composed export canvas are up-to-date
  await new Promise<void>((r) => requestAnimationFrame(() => r()));
  await new Promise<void>((r) => requestAnimationFrame(() => r()));

  if (!pieBase64Ref.current) {
    setModalMessage("Pie export image not ready. Please click Download again.");
    setShowModal(true);
    return;
  }

  exportInventoryReconExcel({
    filename: "Inventory_Reconciliation.xlsx",
    titleLine: `Amazon ${countryName?.toUpperCase()} - Inventory Recon - ${periodLabel}`,
    countryName,
    titleCountry: countryName?.toUpperCase(),
    platformLabel: "Amazon",
    periodLabel,
    companyName,
    brandName,
    dataRows,

    // ✅ only pass composed PNG (includes legend metrics inside the image)
    chartBase64: pieBase64Ref.current,

    // ❌ do NOT pass metrics for Excel cell text (keep tab2 clean)
    // chartMetrics: pieMetricsRef.current,
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
      <div className="w-full">
        <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
          <PageBreadcrumb
            variant="page"
            align="left"
            textSize="2xl"
            pageTitle={
              <div className="flex flex-wrap items-baseline gap-2">
                <span className="text-[#414042] font-bold">Inventory Reconciliation -</span>
                <span className="text-[#60a68e] font-bold">{countryName?.toUpperCase()}</span>
              </div>
            }
          />

          <div className="flex w-full flex-wrap items-center gap-3 md:w-auto md:justify-end">
            <PeriodFiltersTable
              range={range}
              selectedMonth={selectedMonth}
              selectedQuarter={selectedQuarter}
              selectedYear={selectedYear}
              yearOptions={yearOptions}
              onRangeChange={(v) => setRange(v)}
              onMonthChange={(v) => {
                const raw = String(v ?? "").trim();
                // PeriodFiltersTable may emit values like "Jan" or "Jan 2026".
                const parts = raw.split(/\s+/).filter(Boolean);
                const monthPart = parts[0] ?? raw;
                const yearPart = parts.find((p) => /^\d{4}$/.test(p));

                setSelectedMonth(normalizeMonth(monthPart));
                if (yearPart) setSelectedYear(normalizeYear(yearPart));
              }}
              onQuarterChange={(v) => setSelectedQuarter(String(v).toUpperCase())}
              onYearChange={(v) => setSelectedYear(String(v))}
              allowedRanges={['monthly', 'quarterly', 'yearly']}
            />

            <DownloadIconButton onClick={handleDownloadXLSX} size="md" />
          </div>

        </div>
      </div>

      {/* Table */}
      <div
        className={[
          "mt-5 w-full rounded-lg border border-gray-200 bg-white",
          anyExpanded ? "overflow-x-auto" : "overflow-x-hidden",
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

      <div className="mt-4" id="inventory-pie-export">
        {/* <InventoryTopProductsPie
          key={`${countryName}-${selectedYear}-${selectedMonth}-${range}-${selectedQuarter}`}
          rows={displayRows}
          title="Inventory Breakup"
        /> */}

        <InventoryTopProductsPie
  key={`${countryName}-${selectedYear}-${selectedMonth}-${range}-${selectedQuarter}`}
  rows={pieRows}
  title="Inventory Breakup"
  exportTick={exportTick} // ✅ NEW
  onExportBase64Ready={(b64) => setPieBase64(b64)}
  // onExportMetricsReady={(m) => setPieMetrics(m)} // optional: you can remove if not needed anywhere else
/>

      </div>


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
