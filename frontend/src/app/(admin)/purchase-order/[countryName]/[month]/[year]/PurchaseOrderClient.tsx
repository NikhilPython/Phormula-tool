

'use client';

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useParams, useRouter } from 'next/navigation';
import * as XLSX from 'xlsx';
import UploadLocalInvModal from '@/components/Modal/UploadLocalInvModal';
import MonthYearPickerTable from '@/components/filters/MonthYearPickerTable';
import DataTable, { ColumnDef } from '@/components/ui/table/DataTable';
import DownloadIconButton from '@/components/ui/button/DownloadButton';
import PageBreadcrumb from '@/components/common/PageBreadCrumb';

interface Row {
  [key: string]: any;
}

type PurchaseOrderPageProps = {
  embedded?: boolean;
  countryNameProp?: string;
  selectedMonthProp?: string;
  selectedYearProp?: string;
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

export default function PurchaseOrderPage({
  embedded = false,
  countryNameProp,
  selectedMonthProp,
  selectedYearProp,
}: PurchaseOrderPageProps) {
  const params = useParams() as {
    countryName?: string;
    month?: string;
    year?: string;
  };

  const router = useRouter();

  const countryName = (countryNameProp ?? params?.countryName ?? '').toString();
  const urlMonth = (selectedMonthProp ?? params?.month ?? '').toString();
  const urlYear = (selectedYearProp ?? params?.year ?? '').toString();

  const [month, setMonth] = useState<string>('');
  const [year, setYear] = useState<string>('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [skuData, setSkuData] = useState<Row[]>([]);
  const [showModal, setShowModal] = useState(false);

  const isGlobalRoute = useMemo(
    () => countryName.toLowerCase() === 'global',
    [countryName]
  );

  const displayedColumns = useMemo(
    () =>
      isGlobalRoute
        ? [
            'Sno.',
            'Product Name',
            'Dispatches UK',
            'Dispatches Canada',
            'Dispatches Amazon US',
            'Total Dispatches',
            'Current Inventory - Local Warehouse',
            'PO Already Raised',
            'PO to be raised',
            'Cost per Unit (in INR)',
            'PO Cost (in INR)',
          ]
        : [
            'Sno.',
            'Product Name',
            'Dispatches UK',
            'Total Dispatches',
            'Current Inventory - Local Warehouse',
            'PO Already Raised',
            'PO to be raised',
            'Cost per Unit (in INR)',
            'PO Cost (in INR)',
          ],
    [isGlobalRoute]
  );

  const signRowMap = useMemo<Record<string, string>>(
    () => ({
      'Dispatches UK': '(+)',
      'Dispatches Canada': '(+)',
      'Dispatches Amazon US': '(+)',
      'Current Inventory - Local Warehouse': '(-)',
      'PO Already Raised': '(-)',
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
      formData.append('month', capitalize(selectedMonth));
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
        return;
      }

      setLoading(true);
      setError('');

      try {
        let res = await fetchGeneratedPOFile(selectedMonth, selectedYear);

        if (!res.ok && res.status === 404) {
          await generatePurchaseOrder(selectedMonth, selectedYear);
          res = await fetchGeneratedPOFile(selectedMonth, selectedYear);
        }

        if (!res.ok) {
          const errJson = await res.json().catch(() => ({}));

          if (errJson?.error === 'Generated file not found') {
            setError('Please upload your local house inventory file to see PO.');
            return;
          }

          throw new Error(errJson?.error || 'Failed to fetch PO file');
        }

        const blob = await res.blob();
        const buffer = await blob.arrayBuffer();
        setSkuData(parseWorkbookToRows(buffer));
      } catch (err: any) {
        setError(err?.message || 'Unknown error');
      } finally {
        setLoading(false);
      }
    },
    [fetchGeneratedPOFile, generatePurchaseOrder, month, year]
  );

  const fetchGlobalDispatchFile = useCallback(
    async (monthArg?: string, yearArg?: string) => {
      const selectedMonth = monthArg ?? month;
      const selectedYear = yearArg ?? year;

      if (!selectedMonth || !selectedYear) {
        setError('Please select both month and year.');
        return;
      }

      setLoading(true);
      setError('');

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
          setSkuData(parseWorkbookToRows(buffer));
          return;
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
          throw new Error('Global PO data was empty');
        }

        const binary = atob(result.data);
        const buffer = new ArrayBuffer(binary.length);
        const view = new Uint8Array(buffer);

        for (let i = 0; i < binary.length; i += 1) {
          view[i] = binary.charCodeAt(i);
        }

        setSkuData(parseWorkbookToRows(buffer));
      } catch (err: any) {
        setError(err?.message || 'Unknown error');
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

  const handleExportToExcel = useCallback(() => {
    const worksheetData = skuData.map((row, index) => {
      const formatted: Record<string, any> = { 'Sno.': index + 1 };

      displayedColumns.forEach((col) => {
        if (col === 'Sno.') return;

        let value = row[col];
        if (typeof value === 'number') {
          value = value.toLocaleString('en-IN', {
            minimumFractionDigits: 0,
            maximumFractionDigits: 2,
          });
        }

        formatted[col] = value;
      });

      return formatted;
    });

    const worksheet = XLSX.utils.json_to_sheet(worksheetData);
    const workbook = XLSX.utils.book_new();

    XLSX.utils.book_append_sheet(
      workbook,
      worksheet,
      isGlobalRoute ? 'Global PO Data' : 'PO Data'
    );

    const fileName = isGlobalRoute
      ? `Global_PO_Report_${month}_${year}.xlsx`
      : `PO_Report_${countryName}_${month}_${year}.xlsx`;

    XLSX.writeFile(workbook, fileName);
  }, [skuData, displayedColumns, isGlobalRoute, month, year, countryName]);

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

  const tableData = useMemo(() => {
    if (!skuData.length) return [];

    const signRow: Row = {};
    displayedColumns.forEach((col) => {
      signRow[col] = signRowMap[col] || '';
    });
    signRow.__isSignRow = true;

    const formattedRows = skuData.map((row, index) => {
      const output: Row = {};

      displayedColumns.forEach((col) => {
        let value = col === 'Sno.' ? row[col] ?? index + 1 : row[col];

        if (typeof value === 'number') {
          value = value.toLocaleString('en-IN', {
            minimumFractionDigits: 0,
            maximumFractionDigits: 2,
          });
        }

        output[col] = value ?? '';
      });

      const isTotalRow =
        String(row['Product Name'] ?? '').trim().toLowerCase() === 'total';

      output.__isTotalRow = isTotalRow;
      return output;
    });

    return [signRow, ...formattedRows];
  }, [skuData, displayedColumns, signRowMap]);

  const tableColumns = useMemo<ColumnDef<Row>[]>(
    () =>
      displayedColumns.map((col) => ({
        key: col,
        header: col,
        width: col === 'Sno.' ? '60px' : col === 'Product Name' ? '220px' : '140px',
        cellClassName:
          col === 'Product Name'
            ? 'text-left'
            : col === 'Sno.'
            ? 'text-center'
            : '',
        headerClassName: col === 'Sno.' ? 'text-center' : '',
        render: (row, value) => {
          const text = String(value ?? '');

          if (row.__isSignRow) {
            if (text === '(+)') {
              return <span className="text-green-600 font-medium">{text}</span>;
            }
            if (text === '(-)') {
              return <span className="text-red-500 font-medium">{text}</span>;
            }
            return '';
          }

          return text;
        },
      })),
    [displayedColumns]
  );

  const getTableRowClassName = useCallback((row: Row) => {
    if (row.__isSignRow) return 'bg-white';
    if (row.__isTotalRow) return 'bg-[#D9D9D9] font-semibold';
    return 'bg-white';
  }, []);

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

            <DownloadIconButton onClick={handleExportToExcel} size="md" />

            <div className="flex sm:flex-row flex-col gap-4">
              <button
                className="fetch-button"
                onClick={() =>
                  isGlobalRoute
                    ? void fetchGlobalDispatchFile(month, year)
                    : void fetchDispatchFile(month, year)
                }
              >
                {isGlobalRoute ? 'Get Global Report' : 'Get Report'}
              </button>
            </div>
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
        <div>
          {skuData.length > 0 ? (
            <DataTable<Row>
              columns={tableColumns}
              data={tableData}
              paginate={false}
              pageSize={10}
              stickyHeader
              scrollY
              maxHeight="90vh"
              emptyMessage={`Select Month and Year to see ${isGlobalRoute ? 'Global PO' : 'PO'}!`}
              rowClassName={getTableRowClassName}
              tableClassName="text-xs 2xl:text-sm"
            />
          ) : (
            <p>Select Month and Year to see {isGlobalRoute ? 'Global PO' : 'PO'}!</p>
          )}
        </div>
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