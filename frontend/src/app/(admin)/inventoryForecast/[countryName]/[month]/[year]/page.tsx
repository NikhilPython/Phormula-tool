'use client';

import React, { useEffect, useMemo, useState } from 'react';
import { useParams, useRouter } from 'next/navigation';
import * as XLSX from 'xlsx';

// ⬇️ apne project ke paths ke hisaab se adjust kar lo
import DisplayInventoryForecast from '@/components/inventory/DisplayInventoryForecast';
import Loading from '@/components/inventory/Loading';
import { Modal } from '@/components/ui/modal';
import FileUploadForm from '@/app/(admin)/(ui-elements)/modals/FileUploadForm';

// ---------------- Types ----------------
type UploadItem = {
  filename?: string;
  month?: string | number;
  year?: string | number;
  country?: string;
  [k: string]: any;
};

type UploadHistoryRes = { uploads: UploadItem[] };

type ForecastRow = Record<string, any>;

// --------------------------------------

export default function InventoryForecastPage() {
  const router = useRouter();
  const params = useParams() as {
    countryName?: string;
    month?: string;
    year?: string;
  };

  const countryName = (params.countryName ?? '').toLowerCase();
  const today = new Date();
  const currentMonthIndex = today.getMonth();
  const thisYear = today.getFullYear();
  const previousMonthIndex = currentMonthIndex === 0 ? 11 : currentMonthIndex - 1;

  const monthNames = ['january','February','March','April','May','June','July','August','September','October','November','December'] as const;;


  // URL month/year normalize (fallback to prev month / current year)
  const urlMonth = (params.month ?? '').toLowerCase().trim();
  const apiMonth = useMemo(() => {
    if (!urlMonth) return monthNames[previousMonthIndex];
    // allow numeric / short / long
    const mnum = urlMonth.match(/\b(1[0-2]|0?[1-9])\b/);
    if (mnum) return monthNames[parseInt(mnum[0], 10) - 1];
    const short = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'];
    const si = short.indexOf(urlMonth.slice(0,3));
    if (si !== -1) return monthNames[si];
    const li = monthNames.indexOf(urlMonth as any);
    return li !== -1 ? monthNames[li] : urlMonth;
  }, [urlMonth]);

  const apiYear = /^\d{4}$/.test(params.year ?? '') ? (params.year as string) : String(thisYear);

  // -------------- State --------------
  const [uploads, setUploads] = useState<UploadItem[]>([]);
  const [filteredUploads, setFilteredUploads] = useState<UploadItem[]>([]);
  const [missingMonths, setMissingMonths] = useState<string[]>([]);
  const [excelData, setExcelData] = useState<ForecastRow[] | null>(null);
  const [lastFetched, setLastFetched] = useState<Date | null>(null);

  const [loading, setLoading] = useState<boolean>(true);
  const [isServerError, setIsServerError] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [showUpload, setShowUpload] = useState(false);

  // -------------- Effects --------------
  useEffect(() => {
    void fetchUploadHistory();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [countryName]);

  // -------------- Helpers --------------
  const tokenOrFail = () => {
    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) throw new Error('Authorization token is missing');
    return token;
  };

  const getLatestUploadDate = (list: UploadItem[]) => {
    if (list.length === 0) return 0;
    return list.reduce((latest, u) => {
      const y = Number(u.year);
      const m = typeof u.month === 'number' ? u.month : Number(u.month);
      const ts = new Date(`${y}-${String(m).padStart(2, '0')}-01`).getTime();
      return ts > latest ? ts : latest;
    }, 0);
  };

  async function fetchUploadHistory() {
    setLoading(true);
    setError(null);
    try {
      const token = tokenOrFail();

      const res = await fetch('http://127.0.0.1:5000/upload_history', {
        method: 'GET',
        headers: { Authorization: `Bearer ${token}` },
      });

      if (!res.ok) {
        let msg = 'An error occurred';
        try {
          const j = (await res.json()) as any;
          msg = j?.error || j?.message || msg;
        } catch {}
        throw new Error(msg);
      }

      const data = (await res.json()) as UploadHistoryRes;
      const filtered = countryName
        ? data.uploads.filter(
            (u) => (u.country ?? '').toString().toLowerCase() === countryName
          )
        : data.uploads;

      setUploads(data.uploads);
      setFilteredUploads(filtered);

      // compute last 5 months needed
      const months = filtered.map((u) => {
        const m = new Date(`${u.month} 1, ${u.year}`).getMonth();
        return `${u.year}-${String(m + 1).padStart(2, '0')}`;
      });
      const unique = new Set(months);

      const currentDate = new Date();
      const curM = currentDate.getMonth();
      const curY = currentDate.getFullYear();
      const prev5: string[] = [];
      for (let i = 1; i <= 5; i++) {
        let m = curM - i;
        let y = curY;
        if (m < 0) { m += 12; y -= 1; }
        prev5.push(`${y}-${String(m + 1).padStart(2, '0')}`);
      }
      const missing = prev5.filter((m) => !unique.has(m));
      if (missing.length) {
        const formatted = missing.map((s) => {
          const [y, mon] = s.split('-').map(Number);
          const d = new Date(y, mon - 1, 1);
          return d.toLocaleString('default', { month: 'long', year: 'numeric' });
        });
        setMissingMonths(formatted);
      }

      // need at least 5 months to proceed
      if (filtered.length < 5) {
        setIsServerError(true);
        setLoading(false);
        return;
      }

      checkLocalCacheAndFetch(filtered);
    } catch (e: any) {
      setError(e?.message ?? 'Failed to fetch upload history');
      setLoading(false);
    }
  }

  function checkLocalCacheAndFetch(filtered: UploadItem[]) {
    try {
      const cachedData = localStorage.getItem(`forecast-${countryName}`);
      const cachedTime = localStorage.getItem(`forecast-time-${countryName}`);
      const latestUploadTs = getLatestUploadDate(filtered);

      if (cachedData && cachedTime) {
        const ts = parseInt(cachedTime, 10);
        if (latestUploadTs > ts) {
          void fetchForecastData();
        } else {
          setExcelData(JSON.parse(cachedData));
          setLastFetched(new Date(ts));
          setLoading(false);
        }
      } else {
        void fetchForecastData();
      }
    } catch {
      void fetchForecastData();
    }
  }

  async function fetchForecastData() {
    try {
      const token = tokenOrFail();

      const endpoint =
        countryName === 'global'
          ? `http://127.0.0.1:5000/forecast_global?month=${encodeURIComponent(apiMonth)}&year=${encodeURIComponent(apiYear)}`
          : `http://127.0.0.1:5000/api/forecast?country=${encodeURIComponent(countryName)}&month=${encodeURIComponent(apiMonth)}&year=${encodeURIComponent(apiYear)}`;

      const res = await fetch(endpoint, {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
      });

      const ctype = res.headers.get('Content-Type') || '';

      // Non-OK → show server msg + zero_months if present
      if (!res.ok) {
        let serverMsg = '';
        let zeroMonths: string[] = [];
        try {
          const errJson = (await res.json()) as any;
          serverMsg = errJson?.error || errJson?.message || errJson?.warning || '';
          zeroMonths = Array.isArray(errJson?.zero_months) ? errJson.zero_months : [];
        } catch {}
        setError(serverMsg || `Server error (${res.status})`);
        if (zeroMonths.length) {
          const formatted = zeroMonths.map((s) => {
            const [y, m] = s.split('-').map(Number);
            const d = new Date(y, m - 1, 1);
            return d.toLocaleString('default', { month: 'long', year: 'numeric' });
          });
          setMissingMonths(formatted);
        }
        setIsServerError(res.status >= 500);
        setLoading(false);
        return;
      }

      // Excel → parse
      if (
        ctype.includes('spreadsheetml') ||
        ctype.includes('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
      ) {
        const blob = await res.blob();
        const buffer = await blob.arrayBuffer();
        const workbook = XLSX.read(buffer, { type: 'array' });
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const jsonRows = XLSX.utils.sheet_to_json<ForecastRow>(sheet);

        setExcelData(jsonRows);
        localStorage.setItem(`forecast-${countryName}`, JSON.stringify(jsonRows));
        localStorage.setItem(`forecast-time-${countryName}`, Date.now().toString());
        setLoading(false);
        return;
      }

      // JSON (fallbacks / messages)
      const data = (await res.json()) as any;
      if (Array.isArray(data?.forecast)) {
        setExcelData(data.forecast);
        localStorage.setItem(`forecast-${countryName}`, JSON.stringify(data.forecast));
        localStorage.setItem(`forecast-time-${countryName}`, Date.now().toString());
        setLoading(false);
        return;
      }

      setError(data?.warning || data?.message || 'Forecast generated, but no file was returned.');
      setIsServerError(false);
      setLoading(false);
    } catch (e: any) {
      setError(e?.message || 'Failed to fetch forecast');
      setIsServerError(false);
      setLoading(false);
    }
  }

  // Columns detection (DisplayInventoryForecast bhi apna detect karta hai — ye optional)
  // const displayedColumns = useMemo(() => {
  //   if (!excelData || excelData.length === 0) return [] as string[];
  //   const all = Object.keys(excelData[0] ?? {});
  //   const fixed = [
  //     'Product Name', 'SKU Type', 'Last Month Sales(Units)',
  //     'Projected Sales Total', 'Inventory at Month End',
  //     'Dispatch', 'Current Inventory + Dispatch'
  //   ];

  //   const monthLike = all.filter((col) =>
  //     /^[A-Z][a-z]{2}'\d{2}$/.test(col) ||          // Sep'25
  //     /^[A-Z][a-z]+ ?\d{4}$/.test(col) ||           // October 2025
  //     (/^[A-Z][a-z]+$/.test(col) && !fixed.includes(col)) // October
  //   ).filter((col) => !fixed.includes(col));

  //   return [...fixed.filter((f) => all.includes(f)), ...monthLike];
  // }, [excelData]);

  // -------------- Render --------------
  return (
  
  <div className="">
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
      .forecast-heading { font-size: 18px; color: #414042; background-color: white;
        border-radius: 7px; font-weight: bold; }
      .country-name { color: #414042; }
    `}</style>

    {loading ? (
      // 1) sabse pehle loading
      <Loading />
    ) : missingMonths.length > 0 ? (
      // 2) agar missing months hain → ye sabse high priority
      <div>
         <h3 className="text-2xl font-bold text-[#414042]">
       Inventory Forecast </h3>
         <span style={{ fontSize: '12px' }}>
          The following Monthly files are needed to upload:&nbsp;
          <strong style={{ color: '#60a68e' }}>
            {missingMonths.join(', ')}
          </strong>
        </span>
        <div className="alert-container">
          <div className="alert-message">
            <i className="fa-solid fa-circle-exclamation alert-icon"></i>
            <span>Please upload at least 4 months&apos; files to see for the next two months.</span>
          </div>
          <button
            className="alert-button"
            onClick={() => setShowUpload(true)}
          >
            Upload Now <i className="fa-solid fa-chevron-right"></i>
          </button>
        </div>

       
      </div>
    ) : error ? (
      // 3) agar error hai, lekin missing months nahi (server error type)
      <div className="alert-container">
        <div className="alert-message">
          <i className="fa-solid fa-circle-exclamation alert-icon"></i>
          <span>{error}</span>
        </div>
        <button
          className="alert-button"
          onClick={() => setShowUpload(true)}
        >
          Upload Now <i className="fa-solid fa-chevron-right"></i>
        </button>
      </div>
    ) : (
      // 4) normal case → data hai to table/forecast show karo
      <DisplayInventoryForecast
        countryName={countryName}
        month={apiMonth}
        year={apiYear}
        data={excelData ?? []}
      />
    )}

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