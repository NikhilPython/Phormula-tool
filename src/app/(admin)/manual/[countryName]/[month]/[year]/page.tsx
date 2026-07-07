'use client';

import React, { useEffect, useMemo, useState } from 'react';
import { useParams } from 'next/navigation';

import {
  useManualPreviewMutation,
  useManualSubmitMutation,
} from '@/lib/api/inventoryapi';

import InventoryManualResult from '@/components/inventory/InventoryManualResult';
import { Modal } from '@/components/ui/modal';
import FileUploadForm from '@/app/(admin)/(ui-elements)/modals/FileUploadForm';

// ---------------- Types ----------------
type ManualRow = {
  sku: string;
  productName: string;
  lastMonthSales: number;
  peakLast3: number;
  lastMonthGrowth: number;
  userInputGrowth: number;
};

export default function ManualPage() {
  const params = useParams() as {
    countryName?: string;
    month?: string;
    year?: string;
  };

  const countryName = (params.countryName || '').toLowerCase();

  // URL se month / year, nahi mile to current
  const today = useMemo(() => new Date(), []);
  const effectiveMonth =
    params.month ||
    today.toLocaleString('en-US', { month: 'long' }).toLowerCase();
  const effectiveYear = params.year || String(today.getFullYear());

  const [manualPreview, { isLoading: loadingPreview }] =
    useManualPreviewMutation();
  const [manualSubmit, { isLoading: loadingSubmit }] =
    useManualSubmitMutation();

  const [tableData, setTableData] = useState<ManualRow[]>([]);
  const [rows, setRows] = useState<any[]>([]);
  const [showResult, setShowResult] = useState(false);

  const [error, setError] = useState<string>('');
  const [missingMonths, setMissingMonths] = useState<string[]>([]);
  const [notice, setNotice] = useState<string>('');

  const [showUpload, setShowUpload] = useState(false);

  // ---------------- Preview Load (WITHOUT unwrap) ----------------
 
useEffect(() => {
  let cancelled = false;

  async function loadPreview() {
    setError('');
    setMissingMonths([]);
    setNotice('');
    setTableData([]);

    const result: any = await manualPreview({
      country: countryName,
      month: effectiveMonth,
      year: effectiveYear,
    });

    if (cancelled) return;

    // ❌ Error case
    if ('error' in result) {
      const rawErr = result.error;

      // Removed debug logs to avoid noise from empty {} errors

      const errData =
        rawErr?.data ||
        rawErr?.error?.data ||
        (typeof rawErr?.error === 'string'
          ? { message: rawErr.error }
          : undefined);

      // Backend wala missing_sales_months
      if (errData?.error === 'missing_sales_months') {
        const monthsRaw: string[] =
          errData.months || errData.needed || [];

        const formatted = monthsRaw.map((m) => {
          const match = m.match(/^([A-Za-z]+)(\d{4})$/);
          if (match) return `${match[1]} ${match[2]}`;
          return m;
        });

        setError(
          errData.detail ||
            'Please upload at least 4 months files to see for the next two months.'
        );
        setMissingMonths(formatted);
        setTableData([]); // table clear
        return;
      }

      // Other detailed error
      if (errData?.message || errData?.detail) {
        setError(errData.message || errData.detail);
        setTableData([]);
        return;
      }

      // Network / generic
      if (rawErr?.status === 'FETCH_ERROR') {
        setError(
          'Network error while loading preview. Please check your connection and try again.'
        );
      } else if (rawErr?.message) {
        setError(rawErr.message);
      } else {
        setError('Failed to load preview. Please try again.');
      }
      setTableData([]);
      return;
    }

    // ✅ Success case (unchanged)
    const res = result.data ?? result;
    const previewRows: any[] =
      res?.rows || res?.items || res?.data || [];

    if (!Array.isArray(previewRows) || previewRows.length === 0) {
      setNotice(
        'No preview data – you can still input Growth % and Submit.'
      );
      setTableData([]);
      return;
    }

    const mapped: ManualRow[] = previewRows.map((r: any, idx: number) => ({
      sku: String(r.sku || `sku_${idx}`),
      productName:
        r.name ||
        r.product_name ||
        r.sku ||
        `Item ${idx + 1}`,
      lastMonthSales: Number(
        r.last_month_units ??
          r.last_month_sales ??
          r.last_month_qty ??
          0
      ),
      peakLast3: Number(
        r.peak_last3 ??
          r['Peak Sale (last 3 mo)'] ??
          0
      ),
      lastMonthGrowth: Number(
        r.last_month_growth_pct ??
          r.last_month_growth ??
          0
      ),
      userInputGrowth: Number(r.growth_pct ?? 0),
    }));

    setTableData(mapped);
    setNotice('Preview loaded');
  }

  loadPreview();
  return () => {
    cancelled = true;
  };
}, [countryName, effectiveMonth, effectiveYear, manualPreview]);
  // ---------------- Submit Handler ----------------
  const buildGrowthMap = () =>
    tableData.reduce((acc: Record<string, number>, row) => {
      acc[row.sku] = Number(row.userInputGrowth) || 0;
      return acc;
    }, {});

  const onSubmit = async () => {
    setError('');
    setMissingMonths([]);

    const payload = {
      country: countryName,
      month: effectiveMonth,
      year: effectiveYear,
      growth: buildGrowthMap(),
    };

    const result: any = await manualSubmit(payload);

    if ('error' in result) {
      const rawErr = result.error;

      const errData =
        rawErr?.data ||
        rawErr?.error?.data ||
        (typeof rawErr?.error === 'string'
          ? { message: rawErr.error }
          : undefined);

      if (errData?.error === 'missing_sales_months') {
        const monthsRaw: string[] =
          errData.months || errData.needed || [];

        const formatted = monthsRaw.map((m) => {
          const match = m.match(/^([A-Za-z]+)(\d{4})$/);
          if (match) return `${match[1]} ${match[2]}`;
          return m;
        });

        setError(
          errData.detail ||
            'Please upload at least 4 months files to see for the next two months.'
        );
        setMissingMonths(formatted);
        return;
      }

      setError(
        errData?.message ||
          errData?.detail ||
          rawErr?.message ||
          'Submit failed'
      );
      return;
    }

    const res = result.data ?? result;
    const finalRows =
      res?.rows ??
      res?.table ??
      res?.forecast ??
      res?.data?.rows ??
      res?.data?.table ??
      res?.data?.forecast ??
      (Array.isArray(res) ? res : []);

    const safeRows = Array.isArray(finalRows) ? finalRows : [];
    sessionStorage.setItem(
      'manualForecastRows',
      JSON.stringify(safeRows)
    );
    setRows(safeRows);
    setShowResult(true);
  };

  const totalLastMonthSales = tableData.reduce(
    (sum, row) => sum + Number(row.lastMonthSales || 0),
    0
  );

  // ---------------- Result View ----------------
  if (showResult) {
    return (
      <div className="sm:p-8 p-4 bg-gray-50 font-lato ">
        <InventoryManualResult
          inlineData={rows}
          inlineCountry={countryName}
          inlineMonth={effectiveMonth}
          inlineYear={effectiveYear}
        />
      </div>
    );
  }

  // ---------------- Main Render ----------------
  return (
    <div className="p-3">
      <style>{`
        .alert-container {
          display: flex; align-items: center; background-color: #f2f2f2;
          border-top: 4px solid #ff5c5c; padding: 12px 16px; border-radius: 6px;
          font-family: 'Lato', sans-serif; width: 50%; justify-content: space-between;
          box-sizing: border-box; margin-top: 20px;
        }
        .alert-message { display: flex; align-items: center; color: #414042; font-size: 12px; }
        .alert-icon { color: #ff5c5c; font-size: 18px; margin-right: 10px; }
        .alert-button {
          background: none; border: none; color: #414042; font-weight: 600; cursor: pointer;
          font-size: 14px; text-decoration: underline; display: inline-flex; align-items: center; gap: 5px;
          padding: 0; white-space: nowrap;
        }
      `}</style>

      {/* Error strip + Upload button */}
      {error && (
        <>
        <h3 className="text-2xl font-bold text-[#414042]">
       Inventory Forecast </h3>
         {missingMonths.length > 0 && (
            <span style={{ fontSize: '12px' }}>
              The following Monthly files are needed to upload:&nbsp;
              <strong style={{ color: '#60a68e' }}>
                {missingMonths.join(', ')}
              </strong>
            </span>
          )}
          <div className="alert-container">
            <div className="alert-message">
              <i className="fa-solid fa-circle-exclamation alert-icon"></i>
              <span>Please upload at least 4 months files to see for the next two months.</span>
            </div>
            <button
              className="alert-button"
              onClick={() => setShowUpload(true)}
            >
              Upload Now <i className="fa-solid fa-chevron-right"></i>
            </button>
          </div>
         
        </>
      )}

    

      {/* {notice && !error && (
        <p className="text-xs text-gray-600 mb-2">{notice}</p>
      )} */}

      {/* 👉 Table sirf tab dikhana jab error nahi hai & missingMonths nahi hain */}
      {!error && missingMonths.length === 0 && (
<>
          <h1 className="text-2xl font-bold text-[#414042] mb-6 mt-4">
        Forecasted Data - {' '}
        <span className="text-[#5EA68E]">
          {countryName.toUpperCase() || 'COUNTRY'} ({effectiveMonth.toLocaleUpperCase()}{' '}
          {effectiveYear})
        </span>                  
      </h1>                  

        <div className="overflow-x-auto  bg-white mb-6">
          <table className="min-w-[780px] sm:min-w-full border border-[#414042] font-lato">
            <thead className="bg-[#5EA68E] text-[#F8EDCE]">
              <tr>
                <th className="py-3 xl:px-4 text-center font-medium border border-[#414042] text-sm ">
                  S. No.
                </th>
                <th className="py-3 xl:px-4 text-center font-medium border border-[#414042] text-sm">
                  Product Name
                </th>
                <th className="py-3 xl:px-4 text-center font-medium border border-[#414042] text-sm">
                  SKU
                </th>
                <th className="py-3 xl:px-4 text-center font-medium border border-[#414042] text-sm">
                  Last Month Sales (Units)
                </th>
                <th className="py-3 xl:px-4 text-center font-medium border border-[#414042] text-sm">
                  Peak Sale (last 3 mo)
                </th>
                <th className="py-3 xl:px-4 text-center font-medium border border-[#414042] text-sm">
                  Last Month Growth (%)
                </th>
                <th className="py-3 xl:px-4 text-center font-medium border border-[#414042] text-sm">
                  Growth (%)
                </th>
              </tr>
            </thead>

            <tbody>
              {tableData.map((row, i) => (
                <tr
                  key={row.sku}
                  className={i % 2 === 0 ? 'bg-white' : 'bg-white'}
                >
                  <td className="py-1 xl:px-4 px-2 text-center border border-[#414042] text-sm">
                    {i + 1}
                  </td>
                  <td className="py-1 xl:px-4 px-2 text-gray-800 border border-[#414042] text-left text-sm">
                    {row.productName}
                  </td>
                  <td className="py-1 xl:px-4 px-2 text-center border border-[#414042] text-sm">
                    {row.sku}
                  </td>
                  <td className="py-1 xl:px-4 px-2 text-center border border-[#414042] text-sm">
                    {row.lastMonthSales}
                  </td>
                  <td className="py-1 xl:px-4 px-2 text-center border border-[#414042] text-sm">
                    {row.peakLast3}
                  </td>
                  <td className="py-1 xl:px-4 px-2 text-center border border-[#414042] text-sm ">
                    {row.lastMonthGrowth}%
                  </td>
                  <td className="py-1 xl:px-4 px-2 text-center border border-[#414042] ">
                    <div className="flex justify-center items-center gap-1 ">
                      <input
                        type="number"
                        className="w-20  rounded text-center py-1  text-sm font-lato bg-[#D9D9D966]"
                        value={row.userInputGrowth}
                        onChange={(e) => {
                          const copy = [...tableData];
                          copy[i] = {
                            ...copy[i],
                            userInputGrowth:
                              Number(e.target.value) || 0,
                          };
                          setTableData(copy);
                        }}
                      />
                      <span className="text-gray-600">%</span>
                    </div>
                  </td>
                </tr>
              ))}

              {/* Total row */}
              <tr className="bg-gray-100 font-semibold text-gray-800">
                <td
                  colSpan={3}
                  className="py-2 px-4 text-left border border-[#414042]"
                >
                  Total
                </td>
                <td className="py-2 px-4 text-center border border-[#414042]">
                  {totalLastMonthSales}
                </td>
                <td
                  colSpan={3}
                  className="py-2 px-4 border border-[#414042]"
                ></td>
              </tr>

              {!loadingPreview && tableData.length === 0 && (
                <tr>
                  <td
                    colSpan={7}
                    className="py-4 px-4 text-center text-sm text-gray-500"
                  >
                    No data to display. Please check your uploads or
                    adjust the selected month/year.
                  </td>
                </tr>
              )}

              {loadingPreview && (
                <tr>
                  <td
                    colSpan={7}
                    className="py-4 px-4 text-center text-sm text-gray-500"
                  >
                    Loading preview...
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

         <div className="flex justify-end items-center mt-5">
        <button
          onClick={onSubmit}
          disabled={
            loadingSubmit ||
            tableData.length === 0 ||
            missingMonths.length > 0
          }
          className={`bg-[#37455F] text-sm text-[#F8EDCE] font-bold w-[200px] py-2 rounded-lg shadow ${
            loadingSubmit ||
            tableData.length === 0 ||
            missingMonths.length > 0
              ? 'opacity-70 cursor-not-allowed'
              : ''
          }`}
        >
          {loadingSubmit ? 'Submitting…' : 'Submit'}
        </button>
      </div>
        </>
      )}

     

      {/* Upload modal */}
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
    </div>
  );
}
