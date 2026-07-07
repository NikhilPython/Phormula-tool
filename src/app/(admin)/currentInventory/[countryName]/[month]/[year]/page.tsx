'use client';

import React, { use, useEffect, useMemo, useState } from 'react';
import * as XLSX from 'xlsx';
import { useRouter } from 'next/navigation';
import { Modal } from '@/components/ui/modal';
import FileUploadForm from '@/app/(admin)/(ui-elements)/modals/FileUploadForm';
import '@/app/(admin)/pnlforecast/[countryName]/[month]/[year]/Styles.css';
import { IoDownload } from 'react-icons/io5';
import MonthYearPickerTable from '@/components/filters/MonthYearPickerTable';

interface PageParams {
  params: Promise<{
    countryName: string;
    month?: string;
    year?: string;
  }>;
}

type Row = Record<string, string | number>;

function monthNameToNumber(m: string): number | null {
  const months = [
    'january','february','march','april','may','june',
    'july','august','september','october','november','december',
  ];
  const idx = months.indexOf((m || '').toLowerCase());
  return idx >= 0 ? idx + 1 : null;
}

function pad2(n: number) {
  return String(n).padStart(2, '0');
}

function getLastDayOfMonth(year: number, month1to12: number) {
  // JS Date: month is 0-based; setting day=0 gives last day of previous month
  const d = new Date(Date.UTC(year, month1to12, 0)); // month1to12 as "next month index"
  return d.getUTCDate();
}

export default function CurrentInventoryPage({ params }: PageParams) {
  const { countryName: countryNameRaw, month: routeMonthRaw, year: routeYearRaw } = use(params);
  const countryName = decodeURIComponent(countryNameRaw ?? '').toLowerCase();
  const routeMonth = routeMonthRaw ? decodeURIComponent(routeMonthRaw) : '';
  const routeYear = routeYearRaw ? decodeURIComponent(routeYearRaw) : '';

  const router = useRouter();

  const [month, setMonth] = useState<string>(routeMonth || '');
  const [year, setYear] = useState<string>(routeYear || '');
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>('');
  const [skuData, setSkuData] = useState<Row[]>([]);
  const [infoMessage, setInfoMessage] = useState<string>('');
  const [showUpload, setShowUpload] = useState<boolean>(false);

  const monthNumber = useMemo(() => monthNameToNumber(month), [month]);

  // ✅ last-date snapshot (start_date=end_date=last day of selected month)
  const lastDateISO = useMemo(() => {
    if (!monthNumber || !year) return '';
    const y = Number(year);
    if (!Number.isFinite(y)) return '';
    const lastDay = getLastDayOfMonth(y, monthNumber);
    return `${y}-${pad2(monthNumber)}-${pad2(lastDay)}`; // e.g. 2025-11-30
  }, [monthNumber, year]);

  const displayedColumns: string[] = [
    'Sno.',
    ...(countryName !== 'global' ? ['SKU'] : []),
    'Product Name',
    'Column A',
  ];

  // ✅ mapping: Column A/G/V as per DB columns you’re saving
  // A = starting_warehouse_balance
  // G = customer_returns
  // V = unknown_events
  const toDisplayRow = (item: any, idx: number): Row => {
    const sku = String(item?.msku ?? item?.MSKU ?? item?.sku ?? item?.SKU ?? '').trim();
    const productName = String(item?.product_name ?? item?.product_name ?? item?.product_name ?? '').trim();

    return {
      'Sno.': idx + 1,
      ...(countryName !== 'global' ? { SKU: sku } : {}),
      'Product Name': productName,
      '': Number(item?.ending_warehouse_balance ?? 0),
    };
  };

  const fetchLedgerSnapshot = async () => {
    if (!month || !year) {
      setError('Please select both month and year.');
      return;
    }
    if (!monthNumber) {
      setError('Invalid month selected.');
      return;
    }
    if (!lastDateISO) {
      setError('Could not compute last date for this month.');
      return;
    }

    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) {
      setError('Authorization token is missing');
      return;
    }

    setLoading(true);
    setError('');
    setInfoMessage('');

    try {
      const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://127.0.0.1:5000';

      // ✅ send start_date=end_date=last day so start_date=1 never happens
      const url =
        `${baseUrl}/amazon_api/inventory/ledger-summary` +
        `?start_date=${encodeURIComponent(lastDateISO)}` +
        `&end_date=${encodeURIComponent(lastDateISO)}` +
        `&store_in_db=true`;

      const response = await fetch(url, {
        method: 'GET',
        headers: { Authorization: `Bearer ${token}` },
      });

      const responseData = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(responseData?.error || 'Failed to fetch Ledger Summary');

      const items = responseData?.items || [];
      if (!Array.isArray(items) || items.length === 0) {
        setSkuData([]);
        setInfoMessage(responseData?.empty_message || 'No data found for selected month snapshot.');
        return;
      }

      setSkuData(items.map((it: any, i: number) => toDisplayRow(it, i)));

      // ✅ show snapshot date (backend should show same start/end now)
      if (responseData?.start_date && responseData?.end_date) {
        setInfoMessage(`Snapshot Date: ${responseData.start_date}`);
      } else {
        setInfoMessage(`Snapshot Date: ${lastDateISO}`);
      }
    } catch (err: any) {
      setError(err?.message || 'Unknown error');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (month && year) fetchLedgerSnapshot();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [month, year, lastDateISO]);

  const exportToExcel = (data: Row[], countryName: string, month: string, year: string) => {
    const fileName = `LedgerSummary-${countryName}${month}${year}.xlsx`;
    if (!data || data.length === 0) return;

    const worksheet = XLSX.utils.json_to_sheet(data);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, 'Sheet1');
    XLSX.writeFile(workbook, fileName);
  };

  return (
    <>
      <style>{`
        .tablec tbody tr:last-child { background-color: #ccc !important; color: #414042; text-align: center; font-weight: bold; }
        .tablec td:first-child, .tablec th:first-child { text-align: center; width: 19px; }
        .tablec thead th { background-color: #5EA68E !important; color: #f8edcf !important; font-weight: bold !important; text-align: center !important; font-size: clamp(12px, 0.729vw, 16px) !important; }
        .tablec tbody tr:nth-child(even) { background-color: #5EA68E33; }
        .tablec tbody tr:nth-child(odd) { background-color: #ffffff; }
        .fetch-button { padding: 9px 16px; font-size: 0.9rem; border: none; border-radius: 6px; cursor: pointer; transition: background-color 0.2s ease; box-shadow: 0 3px 6px rgba(0,0,0,0.15); white-space: nowrap; font-family: 'Lato', sans-serif; background-color: #2c3e50; color: #f8edcf; font-weight: bold; }
        .fetch-button:hover:not(:disabled) { background-color: #1f2a36; }
        .fetch-button:disabled { background-color: #6b7280; cursor: not-allowed; opacity: 0.8; }
        .alert-container { display: flex; align-items: center; background-color: #f2f2f2; border-top: 4px solid #ff5c5c; padding: 12px 16px; border-radius: 6px; font-family: 'Lato', sans-serif; width: 30%; justify-content: space-between; box-sizing: border-box; margin-top: 20px; }
        .alert-message { display: flex; align-items: center; color: #414042; font-size: 14px; }
        .alert-icon { color: #ff5c5c; font-size: 18px; margin-right: 10px; }
        .alert-button { background: none; border: none; color: #414042; font-weight: 600; cursor: pointer; font-size: 14px; text-decoration: underline; display: inline-flex; align-items: center; gap: 5px; padding: 0; white-space: nowrap; }
      `}</style>

      <h2 style={{ color: '#414042' }} className="text-2xl font-bold text-[#414042] mb-6">
        Inventory Ledger Summary for <span style={{ color: '#60a68e' }}>{countryName.toUpperCase()}</span>
      </h2>

      {infoMessage ? <p style={{ color: '#2c3e50', marginTop: 8 }}>{infoMessage}</p> : null}

      <div className="flex gap-5 items-center">
        <MonthYearPickerTable
          month={month}
          year={year}
          yearOptions={[new Date().getFullYear(), new Date().getFullYear() - 1]}
          onMonthChange={(v) => setMonth(v)}
          onYearChange={(v) => setYear(v)}
          valueMode="lower"
        />
        <div className="button-wrapper">
          <button className="fetch-button" onClick={fetchLedgerSnapshot} disabled={loading}>
            {loading ? 'Loading...' : 'Get Report'}
          </button>
        </div>
      </div>

      {loading ? (
        <div style={{ padding: '48px', textAlign: 'center' }}>
          <style>{`@keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }`}</style>
        </div>
      ) : error ? (
        <>
          <div className="alert-container">
            <div className="alert-message">
              <i className="fa-solid fa-circle-exclamation alert-icon"></i>
              <span>{error}</span>
            </div>
            <button className="alert-button" onClick={() => setShowUpload(true)}>
              Upload Now <i className="fa-solid fa-chevron-right"></i>
            </button>
          </div>

          <Modal
            isOpen={showUpload}
            onClose={() => setShowUpload(false)}
            showCloseButton
            className="max-w-4xl w-full mx-auto p-0"
          >
            <FileUploadForm initialCountry={''} onClose={function (): void {
                throw new Error('Function not implemented.');
              } } onComplete={function (): void {
                throw new Error('Function not implemented.');
              } } />
          </Modal>
        </>
      ) : (
        <>
          {skuData.length > 0 ? (
            <>
              <div className="overflow-x-auto pt-10">
                <table className="tablec">
                  <thead className="bg-[#5EA68E] !text-[#f8edcf]">
                    <tr>
                      {displayedColumns.map((col) => (
                        <th key={col}>{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {skuData.map((row, index) => (
                      <tr key={index}>
                        {displayedColumns.map((col) => (
                          <td key={col}>
                            {col === 'Sno.'
                              ? index + 1
                              : typeof row[col] === 'number'
                              ? (row[col] as number).toLocaleString()
                              : String(row[col] ?? '')}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="flex justify-end">
                <button
                  onClick={() => exportToExcel(skuData, countryName, month, year)}
                  className="bg-white border border-[#8B8585] px-1 rounded-sm py-1 mt-2"
                  style={{ boxShadow: '0px 4px 4px 0px #00000040' }}
                >
                  <IoDownload size={27} />
                </button>
              </div>
            </>
          ) : (
            <p>Select Month and Year to see Ledger Summary!</p>
          )}
        </>
      )}
    </>
  );
}
