'use client';

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useParams, useRouter } from 'next/navigation';
import * as XLSX from 'xlsx';
import UploadLocalInvModal from '@/components/Modal/UploadLocalInvModal';
import MonthYearPickerTable from '@/components/filters/MonthYearPickerTable';
import DataTable, { ColumnDef } from '@/components/ui/table/DataTable';
import { IoDownload } from 'react-icons/io5';
import DownloadIconButton from '@/components/ui/button/DownloadButton';
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

// ---------- Types ----------
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
  'January','February','March','April','May','June',
  'July','August','September','October','November','December',
] as const;

const YEARS = Array.from({ length: 2 }, (_, i) => new Date().getFullYear() - i);

function capitalize(s: string) {
  return s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : s;
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

  const countryName = (countryNameProp ?? params?.countryName ?? '').toString();
  const urlMonth = (selectedMonthProp ?? params?.month ?? '').toString();
  const urlYear = (selectedYearProp ?? params?.year ?? '').toString();

  const router = useRouter();

  const [month, setMonth] = useState<string>('');
  const [year, setYear] = useState<string>('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [skuData, setSkuData] = useState<Row[]>([]);
  const [autoFetched, setAutoFetched] = useState(false);
  const [showModal, setShowModal] = useState(false);

  const isGlobalRoute = useMemo(
    () => countryName.toLowerCase() === 'global',
    [countryName],
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
  [isGlobalRoute],
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

  // Pre-fill from URL
  useEffect(() => {
    const m = capitalize(urlMonth);
    const y = urlYear;
    if (MONTHS.includes(m as (typeof MONTHS)[number])) setMonth(m);
    if (y && YEARS.includes(parseInt(y))) setYear(y);
  }, [urlMonth, urlYear]);

  useEffect(() => {
    if (month && year && !autoFetched) {
      (isGlobalRoute ? fetchGlobalDispatchFile : fetchDispatchFile)();
      setAutoFetched(true);
    }
  }, [month, year, isGlobalRoute, autoFetched]);

  const fetchDispatchFile = useCallback(async () => {
    if (!month || !year) {
      setError('Please select both month and year.');
      return;
    }
    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) {
      setError('Authorization token is missing');
      return;
    }
    setLoading(true);
    setError('');
    try {
      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/getDispatchfile2?country=${countryName}&month=${month.toLowerCase()}&year=${year}`,
        { headers: { Authorization: `Bearer ${token}` } },
      );
      if (!res.ok) {
        const errJson = await res.json().catch(() => ({}));
        if (errJson?.error === 'Generated file not found') {
          setError('Please upload your local house inventory file to see PO.');
          return;
        }
        throw new Error(errJson?.error || 'Failed to fetch PO file');
      }
      const blob = await res.blob();
      const data = await blob.arrayBuffer();
      const wb = XLSX.read(new Uint8Array(data), { type: 'array' });
      const sheet = wb.Sheets[wb.SheetNames[0]];
      const json = XLSX.utils.sheet_to_json<Row>(sheet);
      setSkuData(json);
    } catch (e: any) {
      setError(e?.message || 'Unknown error');
    } finally {
      setLoading(false);
    }
  }, [countryName, month, year]);

  const fetchGlobalDispatchFile = useCallback(async () => {
    if (!month || !year) {
      setError('Please select both month and year.');
      return;
    }
    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) {
      setError('Authorization token is missing');
      return;
    }
    setLoading(true);
    setError('');
    try {
      let res = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/getGlobalDispatchfile?month=${month.toLowerCase()}&year=${year}`,
        { headers: { Authorization: `Bearer ${token}` } },
      );
      if (!res.ok) {
        if (res.status === 404) {
          // generate then display
          res = await fetch(
            `${process.env.NEXT_PUBLIC_API_BASE_URL}/global_purchase_order?month=${month}&year=${year}`,
            { headers: { Authorization: `Bearer ${token}` } },
          );
          if (!res.ok) {
            const ej = await res.json().catch(() => ({}));
            throw new Error(ej?.error || 'Failed to generate global PO');
          }
          const result = await res.json();
          if (result?.data) {
            const bin = atob(result.data);
            const buf = new ArrayBuffer(bin.length);
            const view = new Uint8Array(buf);
            for (let i = 0; i < bin.length; i++) view[i] = bin.charCodeAt(i);
            const wb = XLSX.read(buf, { type: 'array' });
            const sheet = wb.Sheets[wb.SheetNames[0]];
            const json = XLSX.utils.sheet_to_json<Row>(sheet);
            setSkuData(json);
          }
        } else {
          const ej = await res.json().catch(() => ({}));
          throw new Error(ej?.error || 'Failed to fetch global PO file');
        }
      } else {
        const blob = await res.blob();
        const data = await blob.arrayBuffer();
        const wb = XLSX.read(new Uint8Array(data), { type: 'array' });
        const sheet = wb.Sheets[wb.SheetNames[0]];
        const json = XLSX.utils.sheet_to_json<Row>(sheet);
        setSkuData(json);
      }
    } catch (e: any) {
      setError(e?.message || 'Unknown error');
    } finally {
      setLoading(false);
    }
  }, [month, year]);

  const handleExportToExcel = () => {
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
    const ws = XLSX.utils.json_to_sheet(worksheetData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(
      wb,
      ws,
      isGlobalRoute ? 'Global PO Data' : 'PO Data',
    );
    const fileName = isGlobalRoute
      ? `Global_PO_Report_${month}_${year}.xlsx`
      : `PO_Report_${countryName}_${month}_${year}.xlsx`;
    XLSX.writeFile(wb, fileName);
  };

  const handleRedirectToForecast = () => {
    router.push(`/inventoryForecast/${countryName}/${month}/${year}`);
  };

  const openUploadModal = () => setShowModal(true);
  const closeUploadModal = () => setShowModal(false);

  // When upload succeeds from modal
  const handleUploadSuccessNavigate = (m: string, y: string) => {
    setShowModal(false);
    setMonth(m);
    setYear(y);
    setAutoFetched(false);
    setTimeout(
      () => (isGlobalRoute ? fetchGlobalDispatchFile() : fetchDispatchFile()),
      0,
    );
  };


  const tableData = useMemo(() => {
  if (!skuData.length) return [];

  const signRow: Row = {};
  displayedColumns.forEach((col) => {
    signRow[col] = signRowMap[col] || '';
  });
  signRow.__isSignRow = true;

  const formattedRows = skuData.map((row, index) => {
    const out: Row = {};

    displayedColumns.forEach((col) => {
      let value =
        col === 'Sno.'
          ? row[col] ?? index + 1
          : row[col];

      if (typeof value === 'number') {
        value = value.toLocaleString('en-IN', {
          minimumFractionDigits: 0,
          maximumFractionDigits: 2,
        });
      }

      out[col] = value ?? '';
    });

    const isTotalRow =
      String(row['Product Name'] ?? '').trim().toLowerCase() === 'total';

    out.__isTotalRow = isTotalRow;
    return out;
  });

  return [signRow, ...formattedRows];
}, [skuData, displayedColumns, signRowMap]);


const tableColumns = useMemo<ColumnDef<Row>[]>(
  () =>
    displayedColumns.map((col) => ({
      key: col,
      header: col,
      width:
        col === 'Sno.'
          ? '60px'
          : col === 'Product Name'
          ? '220px'
          : '140px',
      cellClassName:
        col === 'Product Name'
          ? 'text-left'
          : col === 'Sno.'
          ? 'text-center'
          : '',
      headerClassName:
        col === 'Sno.'
          ? 'text-center'
          : '',
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

const getTableRowClassName = useCallback((row: Row, rowIndex: number) => {
  if (row.__isSignRow) return 'bg-white';
  if (row.__isTotalRow) return 'bg-[#D9D9D9] font-semibold';
  return rowIndex % 2 === 0 ? 'bg-white' : 'bg-white';
}, []);

  return (
    <>
       <style jsx>{`
        .styled-button:active {
          box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
          transform: translateY(1px);
        }
       .table-wrapper {
          width: 100%; max-width: 100%; max-height: 80vh; overflow-x: auto; overflow-y: auto; margin-top: 20px;
          scrollbar-width: thin; scrollbar-color: #5EA68E #f8edcf; -webkit-overflow-scrolling: touch;
        }
        .tablec { width: 100%; border-collapse: collapse; min-width: 900px; }
        .tablec td, .tablec th { border: 1px solid #414042; padding: 8px; text-align: center;  font-size: clamp(12px, 0.729vw, 16px) !important; }
       .theadc th {
background-color: #5EA68E;
color: #f8edcf;
font-weight: bold;
}
        .tablec tbody tr:nth-child(even) { background-color: #5EA68E33; }
        .tablec tbody tr:nth-child(odd) { background-color: #ffffff; }
        .inline-dropdowns {
         
          gap: 15px;
          align-items: center;
         
          margin-bottom: 30px;
        }
        @media (max-width: 768px) {
          .styled-button {
            width: 100%;
            padding: 12px;
          }
          .dropdown-select {
            width: 100%;
          }
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
      <div className='flex items-center justify-between items-start'>
         <div className="flex flex-wrap items-baseline gap-2 justify-start">
              <PageBreadcrumb
                pageTitle="PO Report - "
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
{/* <h2 className='text-2xl font-bold text-[#414042] mb-6'>
        {isGlobalRoute ? (
          <>
            <span style={{ color: '#414042' }}>PO Report for </span>
            <span style={{ color: '#60a68e' }}>Global</span>
          </>
        ) : (
          <span style={{ color: '#414042' }}>
            PO Report for{' '}
            <span style={{ color: '#60a68e' }}>{countryName.toUpperCase()}</span>
          </span>
        )}
      </h2> */}
<div className="inline-dropdowns flex sm:flex-row flex-col">
        <MonthYearPickerTable
                        month={month}
                        year={year}
                        yearOptions={[
                          new Date().getFullYear(),
                          new Date().getFullYear() - 1,
                        ]}
                        onMonthChange={(v) => setMonth(v)}
                        onYearChange={(v) => setYear(v)}
                        valueMode="lower"
                      />
                     
                                        <DownloadIconButton onClick={handleExportToExcel} size="md" />

        <div className="flex sm:flex-row flex-col gap-4">
          <button
            className="fetch-button"
            onClick={isGlobalRoute ? fetchGlobalDispatchFile : fetchDispatchFile}
          >
            {isGlobalRoute ? 'Get Global Report' : 'Get Report'}
          </button>
          {/* {!isGlobalRoute && (
            <button className="fetch-button" onClick={openUploadModal}>
              Upload Local Warehouse Inventory File&nbsp;
              <i className="fa-solid fa-plus" />
            </button>
          )} */}
        </div>
      </div>
      </div>
      
      

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
        <div className="">
          {skuData.length > 0 ? (
            <>
              <div className="">
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
</div>
               
            </>
          ) : (
            <p>
              Select Month and Year to see {isGlobalRoute ? 'Global PO' : 'PO'}!
            </p>
          )}
        </div>
      )}

      {showModal && (
        <UploadLocalInvModal
          countryName={countryName}
          onClose={closeUploadModal}
          onSuccessNavigate={(m: string, y: string) => handleUploadSuccessNavigate(m, y)}
        />
      )}

   
    </>
  );
}