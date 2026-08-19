

'use client';

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useParams, useRouter } from 'next/navigation';
import * as XLSX from 'xlsx';
import UploadLocalInvModal from '@/components/Modal/UploadLocalInvModal';
import MonthYearPickerTable from '@/components/filters/MonthYearPickerTable';
import DataTable, { ColumnDef } from '@/components/ui/table/DataTable';
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
import PageBreadcrumb from '@/components/common/PageBreadCrumb';
import { useGetUserDataQuery } from '@/lib/api/profileApi';
import { exportPurchaseOrderExcel } from '@/lib/excel/exportCurrentInventoryExcel';
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";

interface Row {
  [key: string]: any;
}

type PurchaseOrderPageProps = {
  embedded?: boolean;
  countryNameProp?: string;
  selectedMonthProp?: string;
  selectedYearProp?: string;
  showAllRowsProp?: boolean;
  onShowAllRowsChange?: React.Dispatch<React.SetStateAction<boolean>>;
  onProductNameClick?: (productName: string) => void;
};

const MONTHS = [
  'January',
  'February',
  'March',
  'April',
  'May',
  'June',
  'July',
  'August',
  'September',
  'October',
  'November',
  'December',
] as const;

const YEARS = Array.from({ length: 2 }, (_, i) => new Date().getFullYear() - i);

function capitalize(value: string) {
  return value ? value.charAt(0).toUpperCase() + value.slice(1).toLowerCase() : '';
}

function parseWorkbookToRows(buffer: ArrayBuffer): Row[] {
  const workbook = XLSX.read(new Uint8Array(buffer), { type: 'array' });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  return XLSX.utils.sheet_to_json<Row>(sheet);
}

function monthToLowerName(value: string) {
  return value ? value.trim().toLowerCase() : '';
}

function isCurrentMonthYear(selectedMonth: string, selectedYear: string) {
  const now = new Date();
  const currentMonth = MONTHS[now.getMonth()].toLowerCase();
  const currentYear = String(now.getFullYear());

  return (
    monthToLowerName(selectedMonth) === currentMonth &&
    String(selectedYear) === currentYear
  );
}

async function fetchInventoryForecastFile(
  token: string,
  country: string,
  month: string,
  year: string
) {
  return fetch(
    `${process.env.NEXT_PUBLIC_API_BASE_URL}/api/forecast?country=${encodeURIComponent(
      country.toLowerCase()
    )}&month=${encodeURIComponent(monthToLowerName(month))}&year=${encodeURIComponent(year)}`,
    {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
  );
}

async function ensureInventoryForecastReady(
  token: string,
  country: string,
  month: string,
  year: string
) {
  const res = await fetchInventoryForecastFile(token, country, month, year);

  if (!res.ok) {
    const errJson = await res.json().catch(() => ({}));
    throw new Error(errJson?.error || errJson?.message || 'Inventory forecast not found');
  }

  return res;
}


function toNumber(value: unknown): number {
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0
  const num = Number(String(value ?? '').replace(/,/g, '').trim())
  return Number.isFinite(num) ? num : 0
}

const TOTAL_ROW_LABELS = new Set(['total', 'grand total']);
const PLACEHOLDER_SKU_LABELS = new Set(['', 'nan', 'none', 'null', 'undefined']);

function normalizeCellText(value: unknown): string {
  return String(value ?? '').trim().toLowerCase();
}

function isTotalProductRow(row: Row): boolean {
  return TOTAL_ROW_LABELS.has(normalizeCellText(row['Product Name']));
}

function isPlaceholderSkuRow(row: Row): boolean {
  const hasSku =
    Object.prototype.hasOwnProperty.call(row, 'SKU') ||
    Object.prototype.hasOwnProperty.call(row, 'sku');

  if (!hasSku) return false;

  const skuValue = Object.prototype.hasOwnProperty.call(row, 'SKU')
    ? row.SKU
    : row.sku;

  return PLACEHOLDER_SKU_LABELS.has(normalizeCellText(skuValue));
}

function isPurchaseOrderProductRow(row: Row): boolean {
  return !isTotalProductRow(row) && !isPlaceholderSkuRow(row);
}

function buildOthersPoRow(rows: Row[], displayedColumns: string[]): Row {
  const othersRow: Row = {
    'Product Name': 'Others',
    'S. No.': '',
  }

  displayedColumns.forEach((col) => {
    if (col === 'S. No.' || col === 'Product Name') return

    // keep rate-like fields blank in Others row
    if (col === 'Cost per Unit (in INR)') {
      othersRow[col] = ''
      return
    }

    othersRow[col] = rows.reduce((sum, row) => sum + toNumber(row[col]), 0)
  })

  return othersRow
}

function buildPoTotalRow(
  rows: Row[],
  displayedColumns: string[],
  normalCountryDispatchKey: string
): Row {
  const totalRow: Row = {
    'Product Name': 'Total',
    'S. No.': '',
  };

  displayedColumns.forEach((col) => {
    if (col === 'S. No.' || col === 'Product Name') return;

    if (col === 'Cost per Unit (in INR)') {
      totalRow[col] = '';
      return;
    }

    if (col === 'Dispatch') {
      const dispatchTotal = rows.reduce(
        (sum, row) =>
          sum + toNumber(row.Dispatch ?? row[normalCountryDispatchKey]),
        0
      );

      totalRow[col] = dispatchTotal;
      totalRow[normalCountryDispatchKey] = dispatchTotal;
      return;
    }

    totalRow[col] = rows.reduce((sum, row) => sum + toNumber(row[col]), 0);
  });

  return totalRow;
}


export default function PurchaseOrderPage({
  embedded = false,
  countryNameProp,
  selectedMonthProp,
  selectedYearProp,
  showAllRowsProp,
  onShowAllRowsChange,
  onProductNameClick,
}: PurchaseOrderPageProps) {
  const params = useParams() as {
    countryName?: string;
    month?: string;
    year?: string;
  };

  const router = useRouter();
  const { data: userData } = useGetUserDataQuery();

  const companyName = String(
    (userData as any)?.companyName ||
    (userData as any)?.company_name ||
    (userData as any)?.company ||
    ""
  ).trim();

  const brandName = String(
    (userData as any)?.brandName ||
    (userData as any)?.brand_name ||
    (userData as any)?.brand ||
    ""
  ).trim();

  const countryName = (countryNameProp ?? params?.countryName ?? '').toString();
  const urlMonth = (selectedMonthProp ?? params?.month ?? '').toString();
  const urlYear = (selectedYearProp ?? params?.year ?? '').toString();

  const [month, setMonth] = useState<string>('');
  const [year, setYear] = useState<string>('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [skuData, setSkuData] = useState<Row[]>([]);
  const [showModal, setShowModal] = useState(false);
  const [noData, setNoData] = useState(false);
  const [localShowAllPoRows, setLocalShowAllPoRows] = useState(false);

  const showAllPoRows =
    typeof showAllRowsProp === 'boolean'
      ? showAllRowsProp
      : localShowAllPoRows;

  const setShowAllPoRows =
    onShowAllRowsChange ?? setLocalShowAllPoRows;

  const isGlobalRoute = useMemo(
    () => countryName.toLowerCase() === 'global',
    [countryName]
  );

  const hasColumnValue = useCallback(
    (columnName: string) => {
      return skuData.some(
        (row) => isPurchaseOrderProductRow(row) && toNumber(row[columnName]) > 0
      );
    },
    [skuData]
  );

  const displayedColumns = useMemo(() => {
    if (isGlobalRoute) {
      const dispatchColumns = [
        hasColumnValue('Dispatches UK') ? 'Dispatches UK' : null,
        hasColumnValue('Dispatches Canada') ? 'Dispatches Canada' : null,
        hasColumnValue('Dispatches Amazon US') ? 'Dispatches Amazon US' : null,
      ].filter(Boolean) as string[];

      return [
        'S. No.',
        'Product Name',
        ...dispatchColumns,
        'Total Dispatches',
        'Current Inventory - Local Warehouse',
        'In Transit Units',
        'PO to be raised',
        'Cost per Unit (in INR)',
        'PO Cost (in INR)',
      ];
    }

    return [
      'S. No.',
      'Product Name',
      'Dispatch',
      'Current Inventory - Local Warehouse',
      'In Transit Units',
      'PO to be raised',
      'Cost per Unit (in INR)',
      'PO Cost (in INR)',
    ];
  }, [isGlobalRoute, hasColumnValue]);

  const signRowMap = useMemo<Record<string, string>>(
    () => ({
      Dispatch: '(+)',
      'Dispatches UK': '(+)',
      'Dispatches Canada': '(+)',
      'Dispatches Amazon US': '(+)',
      'Current Inventory - Local Warehouse': '(-)',
      'In Transit Units': '(-)',
    }),
    []
  );

  useEffect(() => {
    const normalizedMonth = capitalize(urlMonth);
    const normalizedYear = urlYear;

    if (MONTHS.includes(normalizedMonth as (typeof MONTHS)[number])) {
      setMonth(normalizedMonth);
    }

    if (normalizedYear && YEARS.includes(parseInt(normalizedYear, 10))) {
      setYear(normalizedYear);
    }
  }, [urlMonth, urlYear]);

  const getTokenOrThrow = useCallback(() => {
    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    if (!token) {
      throw new Error('Authorization token is missing');
    }

    return token;
  }, []);

  const fetchGeneratedPOFile = useCallback(
    async (selectedMonth: string, selectedYear: string) => {
      const token = getTokenOrThrow();

      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/getDispatchfile2?country=${countryName}&month=${selectedMonth.toLowerCase()}&year=${selectedYear}`,
        {
          headers: { Authorization: `Bearer ${token}` },
        }
      );

      return res;
    },
    [countryName, getTokenOrThrow]
  );

  const generatePurchaseOrder = useCallback(
    async (selectedMonth: string, selectedYear: string) => {
      const token = getTokenOrThrow();

      const formData = new FormData();
      formData.append('month', monthToLowerName(selectedMonth));
      formData.append('year', selectedYear);
      formData.append('country', countryName.toLowerCase());

      const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/purchase_order`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: 'application/json',
        },
        body: formData,
      });

      const json = await res.json().catch(() => ({}));

      if (!res.ok) {
        throw new Error(json?.error || 'Failed to generate PO');
      }

      return json;
    },
    [countryName, getTokenOrThrow]
  );

  const fetchDispatchFile = useCallback(
    async (monthArg?: string, yearArg?: string) => {
      const selectedMonth = monthArg ?? month;
      const selectedYear = yearArg ?? year;

      if (!selectedMonth || !selectedYear) {
        setError('Please select both month and year.');
        setNoData(false);
        return;
      }

      setLoading(true);
      setError('');
      setNoData(false);
      setSkuData([]);

      try {
        const token = getTokenOrThrow();

        let res = await fetchGeneratedPOFile(selectedMonth, selectedYear);

        if (!res.ok && res.status === 404) {
          const shouldGenerateForCurrentMonth = isCurrentMonthYear(
            selectedMonth,
            selectedYear
          );

          if (!shouldGenerateForCurrentMonth) {
            setNoData(true);
            return;
          }

          // Only for current month: ensure forecast exists, then generate PO
          await ensureInventoryForecastReady(
            token,
            countryName,
            selectedMonth,
            selectedYear
          );

          await generatePurchaseOrder(selectedMonth, selectedYear);

          res = await fetchGeneratedPOFile(selectedMonth, selectedYear);
        }

        if (!res.ok) {
          const errJson = await res.json().catch(() => ({}));

          if (
            errJson?.error === 'Generated file not found' ||
            errJson?.error === 'Purchase order file not found'
          ) {
            setNoData(true);
            return;
          }

          throw new Error(errJson?.error || 'Failed to fetch PO file');
        }

        const blob = await res.blob();
        const buffer = await blob.arrayBuffer();
        const rows = parseWorkbookToRows(buffer);

        if (!rows.length) {
          setNoData(true);
          return;
        }

        setSkuData(rows);
        setNoData(false);
      } catch (err: any) {
        const msg = err?.message || 'Unknown error';

        if (
          msg.toLowerCase().includes('inventory forecast not found') ||
          msg.toLowerCase().includes('purchase order file not found') ||
          msg.toLowerCase().includes('generated file not found')
        ) {
          setError('');
          setNoData(true);
          return;
        }

        setError(msg);
        setNoData(false);
      } finally {
        setLoading(false);
      }
    },
    [fetchGeneratedPOFile, generatePurchaseOrder, month, year, getTokenOrThrow, countryName]
  );

  const fetchGlobalDispatchFile = useCallback(
    async (monthArg?: string, yearArg?: string) => {
      const selectedMonth = monthArg ?? month;
      const selectedYear = yearArg ?? year;

      if (!selectedMonth || !selectedYear) {
        setError('Please select both month and year.');
        setNoData(false);
        return;
      }

      setLoading(true);
      setError('');
      setNoData(false);
      setSkuData([]);

      try {
        const token = getTokenOrThrow();

        let res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/getGlobalDispatchfile?month=${selectedMonth.toLowerCase()}&year=${selectedYear}`,
          {
            headers: { Authorization: `Bearer ${token}` },
          }
        );

        if (res.ok) {
          const blob = await res.blob();
          const buffer = await blob.arrayBuffer();
          const rows = parseWorkbookToRows(buffer);

          if (!rows.length) {
            setNoData(true);
            return;
          }

          setSkuData(rows);
          setNoData(false);
          return;
        }

        if (res.status === 404) {
          const shouldGenerateForCurrentMonth = isCurrentMonthYear(
            selectedMonth,
            selectedYear
          );

          if (!shouldGenerateForCurrentMonth) {
            setNoData(true);
            return;
          }
        }

        if (res.status !== 404) {
          const errJson = await res.json().catch(() => ({}));
          throw new Error(errJson?.error || 'Failed to fetch global PO file');
        }

        res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/global_purchase_order?month=${selectedMonth}&year=${selectedYear}`,
          {
            headers: { Authorization: `Bearer ${token}` },
          }
        );

        if (!res.ok) {
          const errJson = await res.json().catch(() => ({}));
          throw new Error(errJson?.error || 'Failed to generate global PO');
        }

        const result = await res.json();

        if (!result?.data) {
          setNoData(true);
          return;
        }

        const binary = atob(result.data);
        const buffer = new ArrayBuffer(binary.length);
        const view = new Uint8Array(buffer);

        for (let i = 0; i < binary.length; i += 1) {
          view[i] = binary.charCodeAt(i);
        }

        const rows = parseWorkbookToRows(buffer);

        if (!rows.length) {
          setNoData(true);
          return;
        }

        setSkuData(rows);
        setNoData(false);
      } catch (err: any) {
        setError(err?.message || 'Unknown error');
        setNoData(false);
      } finally {
        setLoading(false);
      }
    },
    [getTokenOrThrow, month, year]
  );

  useEffect(() => {
    if (!month || !year) return;
    void (isGlobalRoute
      ? fetchGlobalDispatchFile(month, year)
      : fetchDispatchFile(month, year));
  }, [month, year, isGlobalRoute, fetchDispatchFile, fetchGlobalDispatchFile]);


  const handleRedirectToForecast = () => {
    router.push(`/inventory-forecast/${countryName}/${month}/${year}`);
  };

  const handleUploadSuccessNavigate = (nextMonth: string, nextYear: string) => {
    setShowModal(false);
    setMonth(nextMonth);
    setYear(nextYear);

    if (isGlobalRoute) {
      void fetchGlobalDispatchFile(nextMonth, nextYear);
    } else {
      void fetchDispatchFile(nextMonth, nextYear);
    }
  };

  const normalCountryDispatchKey = useMemo(() => {
    const country = countryName.toLowerCase();

    if (country === 'us') return 'Dispatches Amazon US';
    if (country === 'uk') return 'Dispatches UK';
    if (country === 'canada') return 'Dispatches Canada';

    return `Dispatches ${countryName.toUpperCase()}`;
  }, [countryName]);

  const handleExportToExcel = useCallback(() => {
    if (!skuData.length || noData || loading) return;

    const sourceRows = skuData.filter(isPurchaseOrderProductRow);
    if (!sourceRows.length) return;

    const sortedRows = sourceRows
      .sort((a, b) => {
        const valA = isGlobalRoute
          ? toNumber(a["Total Dispatches"])
          : toNumber(a[normalCountryDispatchKey]);

        const valB = isGlobalRoute
          ? toNumber(b["Total Dispatches"])
          : toNumber(b[normalCountryDispatchKey]);

        return valB - valA;
      });

    // Excel export: all product rows, no Others grouping
    const rowsForExport: Row[] = [
      ...sortedRows,
      buildPoTotalRow(sourceRows, displayedColumns, normalCountryDispatchKey),
    ];

    const exportRows = rowsForExport.map((row, index) => {
      const isTotalRow =
        isTotalProductRow(row);

      const formatted: Record<string, any> = {};

      displayedColumns.forEach((col) => {
        if (col === "S. No.") {
          formatted[col] = isTotalRow ? "" : index + 1;
          return;
        }

        if (col === "Dispatch") {
          formatted[col] = row.Dispatch ?? row[normalCountryDispatchKey] ?? 0;
          return;
        }

        if (isTotalRow && col === "Cost per Unit (in INR)") {
          formatted[col] = "";
          return;
        }

        formatted[col] = row[col] ?? "";
      });

      return formatted;
    });

    const titleCountry =
      countryName?.toLowerCase() === "global"
        ? "Global"
        : countryName?.toUpperCase();

    void exportPurchaseOrderExcel({
      filename: isGlobalRoute
        ? `Global_PO_Report_${month}_${year}.xlsx`
        : `PO_Report_${countryName}_${month}_${year}.xlsx`,
      titleLine: `Amazon ${titleCountry} - PO Report - ${month} ${year}`,
      titleCountry,
      platformLabel: "Phormula",
      periodLabel: `${month} ${year}`,
      companyName,
      brandName,
      sheetName: isGlobalRoute ? "Global PO Data" : "PO Data",
      dataRows: exportRows,
    });
  }, [
    skuData,
    noData,
    loading,
    displayedColumns,
    isGlobalRoute,
    normalCountryDispatchKey,
    month,
    year,
    countryName,
    companyName,
    brandName,
  ]);

  const tableData = useMemo(() => {
    if (!skuData.length) return []

    const signRow: Row = {}
    displayedColumns.forEach((col) => {
      signRow[col] = signRowMap[col] || ''
    })
    signRow.__isSignRow = true

    const sourceRows = skuData.filter(isPurchaseOrderProductRow)

    if (!sourceRows.length) return []

    const sortedRows = [...sourceRows]
      .sort((a, b) => {
        const valA = isGlobalRoute
          ? toNumber(a['Total Dispatches'])
          : toNumber(a[normalCountryDispatchKey]);

        const valB = isGlobalRoute
          ? toNumber(b['Total Dispatches'])
          : toNumber(b[normalCountryDispatchKey]);

        return valB - valA;
      })

    let rowsForDisplay: Row[] = []

    if (showAllPoRows || sortedRows.length <= 9) {
      rowsForDisplay = [...sortedRows]
    } else {
      const firstNine = sortedRows.slice(0, 9)
      const remainingRows = sortedRows.slice(9)
      const othersRow = buildOthersPoRow(remainingRows, displayedColumns)

      rowsForDisplay = [...firstNine, othersRow]
    }

    rowsForDisplay.push(
      buildPoTotalRow(sourceRows, displayedColumns, normalCountryDispatchKey)
    )

    const formattedRows = rowsForDisplay.map((row, index) => {
      const output: Row = {}

      displayedColumns.forEach((col) => {
        // let value = col === 'S. No.' ? row[col] ?? index + 1 : row[col]
        let value =
          col === 'S. No.'
            ? row[col] ?? index + 1
            : col === 'Dispatch'
              ? row.Dispatch ?? row[normalCountryDispatchKey] ?? 0
              : row[col]


        const isTotalRow =
          String(row['Product Name'] ?? '').trim().toLowerCase() === 'total'

        if (col === 'S. No.') {
          if (isTotalRow) {
            value = ''
          } else {
            value = index + 1
          }
        }

        if (typeof value === 'number') {
          value = value.toLocaleString('en-IN', {
            minimumFractionDigits: 0,
            maximumFractionDigits: 2,
          })
        }

        output[col] = value ?? ''
      })

      output.__isOthersRow =
        String(row['Product Name'] ?? '').trim().toLowerCase() === 'others'
      output.__isTotalRow =
        String(row['Product Name'] ?? '').trim().toLowerCase() === 'total'

      return output
    })

    return [signRow, ...formattedRows]
  }, [
    skuData,
    displayedColumns,
    signRowMap,
    showAllPoRows,
    isGlobalRoute,
    normalCountryDispatchKey,
  ])

  const getDispatchHeaderLabel = (col: string) => {
    if (col === 'Dispatches Amazon US') return 'Dispatch US';

    if (col.startsWith('Dispatches ')) {
      return col.replace('Dispatches ', 'Dispatch ');
    }

    return col;
  };

  const tableColumns = useMemo<ColumnDef<Row>[]>(
    () =>
      displayedColumns.map((col) => ({
        key: col,
        header: getDispatchHeaderLabel(col),
        width: col === 'S. No.' ? '60px' : col === 'Product Name' ? '180px' : '140px',
        cellClassName:
          col === 'Product Name'
            ? 'text-left'
            : col === 'S. No.'
              ? 'text-center'
              : '',
        headerClassName: col === 'S. No.'
          ? 'text-center whitespace-normal break-words'
          : 'whitespace-normal break-words text-center',
        render: (row, value) => {
          const text = String(value ?? '');

          if (row.__isSignRow) {
            if (text === '(+)') return <span className="text-green-600 font-medium">{text}</span>;
            if (text === '(-)') return <span className="text-red-500 font-medium">{text}</span>;
            return '';
          }

          if (col === 'Product Name') {
            const normalized = text.trim().toLowerCase();

            // Make "Others" green, but not clickable
            if (row.__isOthersRow) {
              return (
                <span className="text-green-500">
                  {text}
                </span>
              );
            }

            const isClickable =
              !!text &&
              !row.__isTotalRow &&
              !row.__isOthersRow &&
              !['total', 'others', 'other skus', '-'].includes(normalized);

            if (isClickable) {
              return (
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    onProductNameClick?.(text);
                  }}
                  className="cursor-zoom-in text-left text-green-500"
                >
                  {text}
                </button>
              );
            }
          }

          return text;
        },
      })),
    [displayedColumns, onProductNameClick]
  );

  const getTableRowClassName = useCallback((row: Row) => {
    if (row.__isSignRow) return 'bg-white'
    if (row.__isTotalRow) return 'bg-[#EFEFEF] font-semibold'
    if (row.__isOthersRow) return showAllPoRows ? 'bg-white' : 'bg-white cursor-pointer'
    return 'bg-white'
  }, [showAllPoRows])

  useEffect(() => {
    if (!embedded || typeof window === 'undefined') return;

    const handleRefresh = (event: Event) => {
      const customEvent = event as CustomEvent<{ month?: string; year?: string }>;
      const nextMonth = customEvent.detail?.month;
      const nextYear = customEvent.detail?.year;

      if (!nextMonth || !nextYear) return;

      setMonth(nextMonth);
      setYear(nextYear);

      if (isGlobalRoute) {
        void fetchGlobalDispatchFile(nextMonth, nextYear);
      } else {
        void fetchDispatchFile(nextMonth, nextYear);
      }
    };

    const handleDownload = () => {
      handleExportToExcel();
    };

    window.addEventListener('po-report-refresh', handleRefresh as EventListener);
    window.addEventListener('po-report-download', handleDownload);

    return () => {
      window.removeEventListener('po-report-refresh', handleRefresh as EventListener);
      window.removeEventListener('po-report-download', handleDownload);
    };
  }, [
    embedded,
    isGlobalRoute,
    fetchDispatchFile,
    fetchGlobalDispatchFile,
    handleExportToExcel,
  ]);

  return (
    <>
      <style jsx>{`
        .styled-button:active {
          box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
          transform: translateY(1px);
        }
        .table-wrapper {
          width: 100%;
          max-width: 100%;
          max-height: 80vh;
          overflow-x: auto;
          overflow-y: auto;
          margin-top: 20px;
          scrollbar-width: thin;
          scrollbar-color: #5ea68e #f8edcf;
          -webkit-overflow-scrolling: touch;
        }
        .tablec {
          width: 100%;
          border-collapse: collapse;
          min-width: 900px;
        }
        .tablec td,
        .tablec th {
          border: 1px solid #414042;
          padding: 8px;
          text-align: center;
          font-size: clamp(12px, 0.729vw, 16px) !important;
        }
        .theadc th {
          background-color: #5ea68e;
          color: #f8edcf;
          font-weight: bold;
        }
        .tablec tbody tr:nth-child(even) {
          background-color: #5ea68e33;
        }
        .tablec tbody tr:nth-child(odd) {
          background-color: #ffffff;
        }
        .inline-dropdowns {
          display: flex;
          flex-wrap: wrap;
          gap: 12px;
          align-items: center;
          justify-content: flex-end;
          margin-bottom: 24px;
        }
        @media (max-width: 768px) {
          .inline-dropdowns {
            width: 100%;
            justify-content: flex-start;
          }
        }
        .dropdown-select {
          width: 100%;
        }
        .dropdown-table {
          border-collapse: collapse;
          border-radius: 0.5vw;
          width: auto;
          min-width: 80px;
          max-width: 100px;
        }
        .dropdown-header {
          background: #fff;
          color: #5ea68e;
          border: 0.05vw solid #414042;
        }
        .dropdown-cell {
          padding: 1vh 0.9vw;
          border: 0.05vw solid #414042;
          text-align: center;
          font-size: clamp(12px, 0.729vw, 16px);
        }
        .dropdown-select {
          font-size: clamp(12px, 0.729vw, 16px);
          text-align: center;
          min-width: 60px;
        }
        .dropdown-select:focus {
          outline: none;
          box-shadow: none;
        }
        .fetch-button {
          font-size: clamp(12px, 0.729vw, 16px);
          background: #2c3e50;
          color: #f8edcf;
          font-weight: bold;
          border: none;
          border-radius: 5px;
          cursor: pointer;
          padding: 9px 18px;
        }
        .fetch-button:hover {
          background: #1f2a36;
        }
        .alert-container {
          display: flex;
          align-items: center;
          background: #f2f2f2;
          border-top: 4px solid #ff5c5c;
          padding: 12px 16px;
          border-radius: 6px;
          width: 50%;
          justify-content: space-between;
          box-sizing: border-box;
          margin-top: 20px;
        }
        .alert-message {
          display: flex;
          align-items: center;
          color: #414042;
          font-size: 14px;
        }
        .alert-icon {
          color: #ff5c5c;
          font-size: 18px;
          margin-right: 10px;
        }
        .alert-button {
          background: none;
          border: none;
          color: #414042;
          font-weight: 600;
          cursor: pointer;
          font-size: 14px;
          text-decoration: underline;
          display: inline-flex;
          align-items: center;
          gap: 5px;
          padding: 0;
          white-space: nowrap;
        }
      `}</style>

      {!embedded && (
        <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
          <div className="flex flex-wrap items-baseline gap-2 justify-start">
            <PageBreadcrumb pageTitle="PO Report - " variant="page" align="left" className="" />
            <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
              Amazon {countryName?.toLowerCase() === 'global' ? 'Global' : countryName?.toUpperCase()}
            </span>
          </div>

          <div className={`inline-dropdowns flex sm:flex-row flex-col ${embedded ? 'w-full justify-end' : ''}`}>
            <MonthYearPickerTable
              month={month}
              year={year}
              yearOptions={[new Date().getFullYear(), new Date().getFullYear() - 1]}
              onMonthChange={(value) => setMonth(value)}
              onYearChange={(value) => setYear(value)}
              valueMode="lower"
            />

            {skuData.filter(isPurchaseOrderProductRow).length > 9 && (
                <button
                  type="button"
                  onClick={() => setShowAllPoRows((prev) => !prev)}
                  title={showAllPoRows ? "Collapse rows" : "Expand all rows"}
                  aria-label={showAllPoRows ? "Collapse rows" : "Expand all rows"}
                  disabled={loading || noData}
                  className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md disabled:cursor-not-allowed disabled:opacity-50 lg:h-9 lg:w-9"
                >
                  {showAllPoRows ? (
                    <RiCollapseDiagonalFill className="h-4 w-4 font-extrabold lg:h-[18px] lg:w-[18px]" />
                  ) : (
                    <RiExpandDiagonalFill className="h-4 w-4 font-extrabold lg:h-[18px] lg:w-[18px]" />
                  )}
                </button>
              )}

            <DownloadIconButton onClick={handleExportToExcel} size="md" />
          </div>
        </div>
      )}

      {loading ? (
        <div style={{ padding: '48px', textAlign: 'center' }} />
      ) : error ? (
        <div className="alert-container">
          <div className="alert-message">
            <i className="fa-solid fa-circle-exclamation alert-icon" />
            <span>{error}</span>
          </div>
          <button className="alert-button" onClick={handleRedirectToForecast}>
            Run Now <i className="fa-solid fa-chevron-right" />
          </button>
        </div>
      ) : (
        <DataTable<Row>
          columns={tableColumns}
          data={tableData}
          paginate={false}
          pageSize={10}
          stickyHeader
          scrollY={false}
          maxHeight="none"
          emptyMessage="No Data Available for selected period"
          rowClassName={getTableRowClassName}
          onRowClick={(row) => {
            if (!showAllPoRows && row.__isOthersRow) {
              setShowAllPoRows(true);
            }
          }}
          isTotalRow={(row) => !!row.__isTotalRow}
          bodyMaxHeight={
            showAllPoRows &&
              tableData.filter((row) => !row.__isTotalRow).length > 16
              ? 40 * 16
              : undefined
          }
          tableClassName="text-xs 2xl:text-sm [&_th]:whitespace-normal [&_th]:break-words [&_th]:text-center"
        />
      )}

      {showModal && (
        <UploadLocalInvModal
          countryName={countryName}
          onClose={() => setShowModal(false)}
          onSuccessNavigate={(nextMonth: string, nextYear: string) =>
            handleUploadSuccessNavigate(nextMonth, nextYear)
          }
        />
      )}
    </>
  );
}
