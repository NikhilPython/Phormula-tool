'use client';

import React, { use, useEffect, useState } from 'react';
import * as XLSX from 'xlsx';
import '@/app/(admin)/pnlforecast/[countryName]/[month]/[year]/Styles.css';
import Modalmsg from '@/components/ui/modal/Modalmsg';
import SkuMultiuseCountryUpload from '@/components/ui/modal/SkuMultiCountryUpload';
import { IoDownload } from "react-icons/io5";
import { FaCaretLeft, FaCaretRight } from "react-icons/fa";
import MonthYearPickerTable from '@/components/filters/MonthYearPickerTable';
import DataTable, { ColumnDef } from "@/components/ui/table/DataTable";
import MonthYearPickerWithCurrent from "@/components/filters/MonthYearPickerWithCurrent";
import { ReactNode } from "react";
import PageBreadcrumb from '@/components/common/PageBreadCrumb';
import DownloadIconButton from '@/components/ui/button/DownloadIconButton';






/* ================= TYPES ================= */
interface Params {
  params: Promise<{
    countryName: string;
    month: string;
    year: string;
  }>;
}

interface SkuRow {
  s_no: number;
  product_name: string;
  sku_uk?: string;
  sku_us?: string;
  sku_canada?: string;
  asin?: string;
  product_barcode?: string;
  price?: number;
  currency?: string;
  [key: string]: any;
}

export type InventoryTableRow = {
 s_no: number | string;
  product_name: string;
  sku: string;
  asin: string;
  barcode: string;
  price: string;
  margin: ReactNode;
  available: string;
  age_0_90: string;
  age_91_180: string;
  age_180: string;
  storage: string;
  ledger: number;
  __isTotal?: boolean;
};

/* ================= UTILS ================= */
const getCurrencySymbol = (country: string | undefined): string => {
  switch (country) {
    case 'GBP': return '£';
    case 'INR': return '₹';
    case 'USD': return '$';
    case 'CAD': return '$';
    case 'EUR': return '€';
    default: return '$';
  }
};

function getCurrencyForCountry(country: string): string {
  switch (country.toLowerCase()) {
    case 'uk': return 'GBP';
    case 'us': return 'USD';
    case 'canada': return 'CAD';
    case 'eu': return 'EUR';
    default: return 'USD';
  }
}

const allMonths = [
  'january', 'february', 'march', 'april', 'may', 'june',
  'july', 'august', 'september', 'october', 'november', 'december'
];



/* ================= COMPONENT ================= */
export default function InputCostPage({ params }: Params) {
  const { countryName: countryNameRaw, month: monthRaw, year: yearRaw } = use(params);
  const countryName = decodeURIComponent(countryNameRaw ?? '').toLowerCase();
  const monthParam = decodeURIComponent(monthRaw ?? '');
  const yearParam = decodeURIComponent(yearRaw ?? '');

  /* ===== EXISTING STATE ===== */
  const [skuData, setSkuData] = useState<SkuRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isEditing, setIsEditing] = useState(false);
  const [editedPrices, setEditedPrices] = useState<Record<string, number>>({});
  const [showModal, setShowModal] = useState(false);
  const [modalMessage, setModalMessage] = useState('');
  const [visibleColumns, setVisibleColumns] = useState<string[]>([]);
  const [aspData, setAspData] = useState<Record<string, number>>({});
  const [showMultiuseCountry, setShowMultiuseCountry] = useState(false);
  const [currencyRates, setCurrencyRates] = useState<CurrencyRateRow[]>([]);

  /* ===== NEW AGEING STATE ===== */
  const [ageingMap, setAgeingMap] = useState<Record<string, any>>({});
  const [showAgeBreakup, setShowAgeBreakup] = useState(false);
  const [showSkuExpand, setShowSkuExpand] = useState(false);
  const [ledgerMode, setLedgerMode] = useState(false);
  const [ledgerMap, setLedgerMap] = useState<Record<string, number>>({});
  const now = new Date();

  const [month, setMonth] = useState(
    allMonths[now.getMonth()] // e.g. "september"
  );

  const [year, setYear] = useState(
    String(now.getFullYear())
  );

  const [ledgerLoading, setLedgerLoading] = useState(false);





  // ✅ Helper: match ageing row by SKU (most reliable for uploaded SKU table)
  const getAgeingForRow = (row: SkuRow) => {
    const key = String(row.product_name || '').trim().toLowerCase();
    return ageingMap[key] || {};
  };

  const currentMonthIndex = new Date().getMonth(); // 0-based


  const filteredMonths = allMonths.filter((_, i) => i !== currentMonthIndex);


  /* ================= COLUMN LOGIC ================= */
  const isColumnEmpty = (data: SkuRow[], columnName: string) =>
    data.every(r => !r[columnName]);

  const isCurrentMonth =
    month === allMonths[new Date().getMonth()] &&
    year === String(new Date().getFullYear());

  const fetchLedgerSnapshot = async () => {
    if (!month || !year) return;

    if (isCurrentMonth) {
      cancelLedgerView();
      return;
    }

    setLedgerLoading(true); // 🔥 start loader

    try {
      const monthMap: Record<string, number> = {
        january: 1,
        february: 2,
        march: 3,
        april: 4,
        may: 5,
        june: 6,
        july: 7,
        august: 8,
        september: 9,
        october: 10,
        november: 11,
        december: 12,
      };

      const m = monthMap[month.toLowerCase()];
      if (!m) {
        setLedgerLoading(false);
        return;
      }

      // 🔥 Last date of selected month
      const lastDate = new Date(Date.UTC(Number(year), m, 0))
        .toISOString()
        .split('T')[0];

      const token = localStorage.getItem('jwtToken');
      if (!token) {
        setLedgerLoading(false);
        return;
      }

      const res = await fetch(
        `http://127.0.0.1:5000/amazon_api/inventory/ledger-summary` +
        `?start_date=${lastDate}&end_date=${lastDate}&store_in_db=true`,
        {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        }
      );

      if (!res.ok) {
        throw new Error('Failed to fetch ledger snapshot');
      }

      const json = await res.json();
      const items = Array.isArray(json?.items) ? json.items : [];

      // 🔥 SKU → Ending Balance map
      const map: Record<string, number> = {};

      items.forEach((it: any) => {
        if (
          it.disposition === 'SELLABLE' &&
          it.msku
        ) {
          const sku = it.msku.trim().toUpperCase();

          map[sku] =
            (map[sku] || 0) +
            Number(it.ending_warehouse_balance || 0);
        }
      });

      setLedgerMap(map);
      setLedgerMode(true); // 🔥 switch to ledger view
    } catch (error) {
      console.error('Ledger snapshot error:', error);
      alert('Failed to load ledger report. Please try again.');
    } finally {
      setLedgerLoading(false); // 🔥 stop loader
    }
  };


  const cancelLedgerView = () => {
    const now = new Date();

    setMonth(allMonths[now.getMonth()]);
    setYear(String(now.getFullYear()));

    setLedgerMode(false);
    setLedgerMap({});
  };






  const getVisibleColumns = (data: SkuRow[]) => {
    const base = ['s_no', 'product_name'];
    const skuCol = `sku_${countryName}`;
    const cols = [...base];
    if (!isColumnEmpty(data, skuCol)) cols.push(skuCol);
    cols.push('asin', 'product_barcode', 'price', `gross_margin_${countryName}`);
    return cols;
  };

  const getColumnDisplayName = (col: string) => {
    if (col === 's_no') return 'Sno.';
    if (col === 'product_name') return 'Product Name';
    if (col === 'price') return 'Landing Cost';
    if (col.startsWith('sku_')) return `SKU (${col.replace('sku_', '').toUpperCase()})`;
    if (col.startsWith('gross_margin_')) return `Gross Margin (%)`;
    return col.toUpperCase();
  };

  /* ================= FINANCE LOGIC (UNCHANGED) ================= */
  // const getCurrencyRate = (currency: string | undefined, country: string) =>
  //   currencyRates[`${currency}_${country}`] ?? 1;

  const getAspForProduct = (product: string) =>
    aspData[product] ?? null;

  type CurrencyRateRow = {
    user_currency: string;
    country: string;
    conversion_rate: number;
    selected_currency?: string;
    month?: string;
    year?: number;
  };

  const normalizeCountry = (c?: string) => {
    const v = (c || "").trim().toLowerCase();
    if (v === "uk" || v === "united kingdom" || v === "gb") return "uk";
    if (v === "us" || v === "usa" || v === "united states") return "us";
    if (v === "ca" || v === "can" || v === "canada") return "canada"; // your DB sometimes has "ca" & "canada"
    if (v === "india" || v === "in") return "india";
    return v;
  };

  const normalizeCurrency = (c?: string) => (c || "").trim().toLowerCase();

  const getCurrencyRate = (
    fromCurrency: string | undefined,
    targetCountry: string,
    currencyRates: CurrencyRateRow[]
  ) => {
    const from = normalizeCurrency(fromCurrency);
    const toCountry = normalizeCountry(targetCountry);

    // Try exact country match first
    let row = currencyRates.find(
      r => normalizeCurrency(r.user_currency) === from &&
        normalizeCountry(r.country) === toCountry
    );

    // Fallback: because your DB has BOTH "ca" and "canada"
    if (!row && toCountry === "canada") {
      row = currencyRates.find(
        r => normalizeCurrency(r.user_currency) === from &&
          (normalizeCountry(r.country) === "ca" || normalizeCountry(r.country) === "canada")
      );
    }

    return row?.conversion_rate ?? 1;
  };

  const calculateGrossMargin = (
    price: number | undefined,
    currency: string | undefined,
    targetCountry: string,
    productName: string,
    currencyRates: CurrencyRateRow[]
  ) => {
    const asp = getAspForProduct(productName);
    if (!price || asp == null || asp === 0) return "N/A";

    const rate = getCurrencyRate(currency, targetCountry, currencyRates);
    const converted = price * rate;

    return (((asp - converted) / asp) * 100).toFixed(2);
  };


  /* ================= DATA FETCHERS ================= */

  // SKU
  const fetchSkuData = async () => {
    const token = localStorage.getItem('jwtToken');
    if (!token) return;

    const res = await fetch('http://127.0.0.1:5000/skuprice', {
      headers: { Authorization: `Bearer ${token}` },
    });

    const data = await res.json();
    setSkuData(data);
    setVisibleColumns(getVisibleColumns(data));
  };

  // Currency
  const fetchCurrencyRates = async () => {
    const token = localStorage.getItem('jwtToken');
    if (!token) return;

    const res = await fetch('http://127.0.0.1:5000/currency-rates', {
      headers: { Authorization: `Bearer ${token}` },
    });

    const rates = await res.json();

    setCurrencyRates(Array.isArray(rates) ? rates : []);
  };


  // ASP
  const fetchAspData = async () => {
    const token = localStorage.getItem('jwtToken');
    if (!token) return;

    const res = await fetch(
      `http://127.0.0.1:5000/asp-data?country=${countryName}&month=${monthParam}&year=${yearParam}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );

    const data = await res.json();
    const map: Record<string, number> = {};
    data.forEach((d: any) => map[d.product_name] = d.asp);
    setAspData(map);
  };

  // 🔥 AGEING INVENTORY (NEW)
  // 🔥 AGEING INVENTORY (map by SKU)
  const fetchAgeingInventory = async () => {
    const token = localStorage.getItem('jwtToken');
    if (!token) return;

    const res = await fetch(
      'http://127.0.0.1:5000/amazon_api/inventory/aged/columns?latest=1',
      { headers: { Authorization: `Bearer ${token}` } }
    );

    const json = await res.json();
    const rows = Array.isArray(json?.data) ? json.data : [];

    const map: Record<string, any> = {};
    rows.forEach((r: any) => {
      const key = String(r['product-name'] || '').trim().toLowerCase();
      if (key) map[key] = r;
    });

    setAgeingMap(map);
  };




  /* ================= EFFECT ================= */
  useEffect(() => {
    Promise.all([
      fetchSkuData(),
      fetchCurrencyRates(),
      fetchAspData(),
      fetchAgeingInventory(),
    ])
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [countryName, monthParam, yearParam]);

  const saveChanges = async () => {
    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) {
      alert('Authorization token is missing');
      return;
    }
    if (Object.keys(editedPrices).length === 0) {
      alert('No changes to save.');
      return;
    }
    try {
      const response = await fetch('http://127.0.0.1:5000/updatePrices', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ prices: editedPrices }),
      });
      if (response.ok) {
        const result = await response.json();
        setModalMessage('Prices updated successfully');
        setShowModal(true);
        setIsEditing(false);
        setEditedPrices({});
        if (result.data) {
          const sortedData: SkuRow[] = result.data.sort((a: SkuRow, b: SkuRow) => (a.s_no ?? 0) - (b.s_no ?? 0));
          setSkuData(sortedData);
          const columns = getVisibleColumns(sortedData);
          setVisibleColumns(columns);
        } else {
          window.location.reload();
        }
      } else {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to update prices');
      }
    } catch (e: any) {
      alert(`Error: ${e.message}`);
      console.error('Update prices error:', e);
    }
  };

  const renderGrossMarginCell = (row: SkuRow, column: string) => {
    const targetCountry = column.replace('gross_margin_', '');
    const currentPrice =
      editedPrices[row.product_name] !== undefined ? editedPrices[row.product_name] : row.price;
    const currency = row.currency;
    const grossMargin = calculateGrossMargin(
      currentPrice,
      currency,
      targetCountry,
      row.product_name,
      currencyRates
    );

    if (grossMargin === 'N/A') return <span className="gross-margin-na">N/A</span>;
    const marginValue = parseFloat(grossMargin);
    const className = marginValue >= 0 ? 'gross-margin-positive' : 'gross-margin-negative';
    return <span className={className}>{grossMargin}%</span>;
  };


  const handleDownloadXLSX = () => {
    if (!skuData || skuData.length === 0) {
      alert('No data available to download.');
      return;
    }

    const dataToExport = skuData.map((row) => {
      const newRow = { ...row } as SkuRow;
      if (editedPrices[row.product_name] !== undefined) {
        newRow.price = editedPrices[row.product_name];
      }
      return newRow;
    });

    const exportData = dataToExport.map((row) => {
      const filtered: Record<string, any> = {};
      visibleColumns.forEach((col) => {
        if (col.startsWith('gross_margin_')) {
          const c = col.replace('gross_margin_', '');
          const gm = calculateGrossMargin(
            editedPrices[row.product_name] !== undefined ? editedPrices[row.product_name] : row.price,
            row.currency,
            c,
            row.product_name,
            currencyRates
          );
          filtered[String(getColumnDisplayName(col))] = gm !== 'N/A' ? `${gm}%` : 'N/A';
        } else if (col === 'price') {
          const priceValue =
            editedPrices[row.product_name] !== undefined ? editedPrices[row.product_name] : row.price;
          const symbol = getCurrencySymbol(row.currency);
          filtered[String(getColumnDisplayName(col))] = `${symbol}${priceValue}`;
        } else {
          filtered[String(getColumnDisplayName(col))] = (row as any)[col];
        }
      });
      return filtered;
    });

    const worksheet = XLSX.utils.json_to_sheet(exportData);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, 'SKU Price Data');

    const fileName = `SKU_Price_Data_${countryName?.toUpperCase() || 'EXPORT'}.xlsx`;
    XLSX.writeFile(workbook, fileName);
  };


  if (loading) return <div>Loading...</div>;
  if (error) return <div>Error: {error}</div>;

  const totalAvailableUnits = skuData.reduce((sum, row) => {
    const ageing = getAgeingForRow(row);
    return sum + Number(ageing.available || 0);
  }, 0);

  const totalStorageCost = skuData.reduce((sum, row) => {
    const ageing = getAgeingForRow(row);
    return sum + Number(ageing['estimated-storage-cost-next-month'] || 0);
  }, 0);

  const totals = skuData.reduce(
    (acc, row) => {
      const ageing = getAgeingForRow(row);
      const sku = String(row[`sku_${countryName}`] || '').toUpperCase();

      acc.available += Number(ageing.available || 0);

      acc.age180 +=
        Number(ageing['inv-age-181-to-270-days'] || 0) +
        Number(ageing['inv-age-271-to-365-days'] || 0) +
        Number(ageing['inv-age-365-plus-days'] || 0);

      acc.age0_90 += Number(ageing['inv-age-0-to-90-days'] || 0);
      acc.age91_180 += Number(ageing['inv-age-91-to-180-days'] || 0);

      acc.storageCost += Number(ageing['estimated-storage-cost-next-month'] || 0);

      acc.endingBalance += Number(ledgerMap[sku] || 0);

      return acc;
    },
    {
      available: 0,
      age180: 0,
      age0_90: 0,
      age91_180: 0,
      storageCost: 0,
      endingBalance: 0,
    }
  );

  const formatNumber = (v: any) => {
    const n = Number(v);
    return Number.isFinite(n) ? n.toLocaleString() : '-';
  };

  const tableRows: InventoryTableRow[] = skuData.map((row) => {
    const ageing = getAgeingForRow(row);
    const sku = String(row[`sku_${countryName}`] || "").toUpperCase();

    return {
      s_no: row.s_no,
      product_name: row.product_name,
      sku: row[`sku_${countryName}`] ?? "-",
      asin: row.asin ?? "-",
      barcode: row.product_barcode ?? "-",
      price: `${getCurrencySymbol(row.currency)} ${row.price}`,
      margin: renderGrossMarginCell(row, `gross_margin_${countryName}`),
      available: formatNumber(ageing.available),
      age_0_90: formatNumber(ageing["inv-age-0-to-90-days"]),
      age_91_180: formatNumber(ageing["inv-age-91-to-180-days"]),
      age_180: formatNumber(
        Number(ageing["inv-age-181-to-270-days"] || 0) +
        Number(ageing["inv-age-271-to-365-days"] || 0)
      ),
      storage: formatNumber(
        Number(ageing["estimated-storage-cost-next-month"] || 0).toFixed(2)
      ),
      ledger: ledgerMap[sku] ?? 0,
    };
  });

  tableRows.push({
    s_no: "",
    product_name: "TOTAL",
    sku: "",
    asin: "",
    barcode: "",
    price: "",
    margin: <></>,
    available: formatNumber(totals.available),
    age_0_90: formatNumber(totals.age0_90),
    age_91_180: formatNumber(totals.age91_180),
    age_180: formatNumber(totals.age180),
    storage: formatNumber(totals.storageCost.toFixed(2)),
    ledger: totals.endingBalance,
    __isTotal: true,
  });

  const columns: ColumnDef<any>[] = [
    { key: "s_no", header: "Sno.", width: "60px" },
    {
      key: "product_name",
      header: "Product Name",
      cellClassName: "text-left whitespace-normal",
      width: "200px",
    },

    {
      key: "sku",
      header: (
        <div
          onClick={() => setShowSkuExpand(p => !p)}
          className="relative cursor-pointer flex items-center justify-center gap-2"
        >
          <FaCaretLeft />
          <span>SKU</span>
          <FaCaretRight />
        </div>
      ),
    },

    ...(showSkuExpand
      ? [
        { key: "asin", header: "ASIN" },
        { key: "barcode", header: "Product Barcode" },
      ]
      : []),

    { key: "price", header: "Landing Cost" },
    { key: "margin", header: "Gross Margin (%)" },

    ...(!ledgerMode
      ? [
        { key: "available", header: "Total Inventory" },

        ...(showAgeBreakup
          ? [
            { key: "age_0_90", header: "0–90" },
            { key: "age_91_180", header: "91–180" },
          ]
          : []),

        {
          key: "age_180",
          header: (
            <div
              onClick={() => setShowAgeBreakup(p => !p)}
              className="relative cursor-pointer flex items-center justify-center"
            >
              {/* LEFT ICON */}
              <span className="absolute left-2">
                {showAgeBreakup ? <FaCaretRight /> : <FaCaretLeft />}
              </span>

              {/* TEXT */}
              <span className="px-6">
                {showAgeBreakup ? "180+" : "Aged Inventory"}
              </span>

              {/* RIGHT ICON */}
              <span className="absolute right-2">
                {showAgeBreakup ? <FaCaretLeft /> : <FaCaretRight />}
              </span>
            </div>
          ),
        },

        { key: "storage", header: "Est. Storage Cost" },
      ]
      : [
        { key: "ledger", header: "Ending Warehouse Balance" },
      ]),
  ];




  /* ================= RENDER ================= */
  return (
    <div>
      <style>{`
  div {
    font-family: 'Lato', sans-serif;
  }

  .table-wrapper {
    width: 100%;
    max-width: 100%;
    max-height: 80vh;
    overflow-x: auto;
    overflow-y: auto;
    margin-top: 20px;
    scrollbar-width: thin;
    scrollbar-color: #5EA68E #f8edcf;
    -webkit-overflow-scrolling: touch;
  }

  .tablec {
    width: 100%;
    border-collapse: collapse;
    min-width: 1100px;
  }

  .tablec th,
  .tablec td {
    border: 1px solid #414042;
    padding: 8px;
    text-align: center;
    font-size: clamp(12px, 0.729vw, 16px) !important;
    width: 120px;
  }

  .theadc th {
    background-color: #5EA68E;
    color: #f8edcf;
    font-weight: bold;
  }

  

  .left-align {
    text-align: left !important;
    width: 150px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .serial-no-col {
    max-width: 38px;
    width: 38px !important;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .gross-margin-positive {
    color: #5EA68E;
    font-weight: bold;
  }

  .gross-margin-negative {
    color: #FF5C5C;
    font-weight: bold;
  }

  .gross-margin-na {
    color: #6c757d;
    font-style: italic;
  }

  /* 🔥 Expandable headers (SKU & 180+) */
  .expandable {
    background-color: #4f9b84;
    cursor: pointer;
  }

  .fetch-button { padding: 9px 16px; font-size: 0.9rem; border: none; border-radius: 6px; cursor: pointer; transition: background-color 0.2s ease; box-shadow: 0 3px 6px rgba(0,0,0,0.15); white-space: nowrap; font-family: 'Lato', sans-serif; background-color: #2c3e50; color: #f8edcf; font-weight: bold; }
        .fetch-button:hover:not(:disabled) { background-color: #1f2a36; }
        .fetch-button:disabled { background-color: #6b7280; cursor: not-allowed; opacity: 0.8; }
`}</style>
      {/* <h2 className='text-2xl text-[#414042] font-bold '>
        Inventory Summary - <span style={{ color: '#60a68e' }}> {countryName?.toUpperCase()}</span>
      </h2>
      <div className='flex justify-between w-full items-center '>
        <div>

          <div className="flex gap-4 items-center my-4">
            <MonthYearPickerWithCurrent
              month={month}
              year={year}
              onMonthChange={setMonth}
              onYearChange={setYear}
              valueMode="lower"
            />

            {!ledgerMode && (
              <div className="button-wrapper">
                <button
                  className="fetch-button"
                  onClick={fetchLedgerSnapshot}
                  disabled={ledgerLoading}
                >
                  {ledgerLoading ? 'Loading...' : 'Get Report'}
                </button>
              </div>

            )}

            {ledgerMode && (
              <button className="fetch-button " onClick={cancelLedgerView}>
                Cancel
              </button>
            )}
          </div>
        </div>
        <div className='flex justify-end items-end '>
          <button
            onClick={handleDownloadXLSX}
            className="bg-white border border-[#8B8585] px-1 rounded-sm py-1"
            style={{
              boxShadow: "0px 4px 4px 0px #00000040",
            }}
          >
            <IoDownload size={27} />
          </button>
        </div>
      </div> */}

<div className="w-full">
  {/* Top row: Breadcrumb (left) + Month/Year picker (right) */}
  <div className="flex items-start justify-between w-full">
    <PageBreadcrumb
      variant="page"
      align="left"
      textSize="2xl"
      pageTitle={
        <>
          <span className="text-[#414042] font-bold">Inventory Reconciliation - </span>
          <span className="text-[#60a68e] font-bold">
            {countryName?.toUpperCase()}
          </span>
        </>
      }
    />

    <div className="flex flex-col items-end mb-2">
      {/* Picker on the top right */}
      <MonthYearPickerWithCurrent
        month={month}
        year={year}
        onMonthChange={setMonth}
        onYearChange={setYear}
        valueMode="lower"
      />

      {/* Buttons below picker, right aligned */}
      <div className="flex items-center gap-3 mt-3">
        {!ledgerMode && (
          <button
            className="fetch-button"
            onClick={fetchLedgerSnapshot}
            disabled={ledgerLoading}
          >
            {ledgerLoading ? "Loading..." : "Get Report"}
          </button>
        )}

        {ledgerMode && (
          <button className="fetch-button" onClick={cancelLedgerView}>
            Cancel
          </button>
        )}

        {/* <button
          onClick={handleDownloadXLSX}
          className="bg-white border border-[#8B8585] px-1 py-1 rounded-sm shadow-[0px_4px_4px_0px_#00000040]"
        >
          <IoDownload size={27} />
        </button> */}
         <DownloadIconButton onClick={handleDownloadXLSX} size='md'/>
      </div>
    </div>
  </div>
</div>


      {/* ================= TABLE ================= */}
      <DataTable
        columns={columns}
        data={tableRows}
        loading={loading || ledgerLoading}
        paginate
        pageSize={20}
        maxHeight="70vh"
        zebra
        stickyHeader
        rowClassName={(row) =>
          (row as any).__isTotal
            ? "bg-[#D9D9D9] font-semibold"
            : ""
        }
      />






      {showMultiuseCountry && (
        <div
          onClick={() => setShowMultiuseCountry(false)}
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            backgroundColor: 'rgba(0,0,0,0.5)',
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            zIndex: 1000,
          }}
        >
          <div
            onClick={(e) => e.stopPropagation()}
            style={{
              backgroundColor: 'white',
              borderRadius: '8px',
              width: '30vw',
              height: '30vh',
              overflowY: 'auto',
              position: 'relative',
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'space-between',
              alignItems: 'center',
            }}
          >
            <button
              onClick={() => setShowMultiuseCountry(false)}
              style={{
                position: 'absolute',
                top: '10px',
                right: '10px',
                border: 'none',
                background: 'transparent',
                fontSize: '1.5rem',
                cursor: 'pointer',
              }}
            >
              &times;
            </button>

            <div
              style={{
                flex: 1,
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'center',
                alignItems: 'center',
                position: 'relative',
                zIndex: 2000, // 🔥 high z-index
              }}
            >
              <SkuMultiuseCountryUpload onClose={function (): void {
                throw new Error('Function not implemented.');
              }} onComplete={function (): void {
                throw new Error('Function not implemented.');
              }} />
            </div>
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
