'use client';

import React, { useEffect, useMemo, useState } from 'react';
import { useParams, useRouter } from 'next/navigation';
import * as XLSX from 'xlsx';
import { CheckCircle } from 'lucide-react';
import { API_BASE } from '@/config/env';

import DisplayInventoryForecast from '@/components/inventory/DisplayInventoryForecast';
import Loading from '@/components/inventory/Loading';
import { Modal } from '@/components/ui/modal';
import FileUploadForm from '@/app/(admin)/(ui-elements)/modals/FileUploadForm';
import InventoryFlowTabs, { InventoryFlowTab } from '@/components/inventory/InventoryFlowTabs';
import DispatchPage from '@/app/(admin)/dispatch/[countryName]/[month]/[year]/DispatchClient';
import PurchaseOrderPage from '@/app/(admin)/purchase-order/[countryName]/[month]/[year]/PurchaseOrderClient';
import PageBreadcrumb from '@/components/common/PageBreadCrumb';
import MonthYearPickerTable from '@/components/filters/MonthYearPickerTable'
import DownloadIconButton from '@/components/ui/button/DownloadButton';
import { IoMdLock } from "react-icons/io";

type UploadItem = {
  filename?: string;
  month?: string | number;
  year?: string | number;
  country?: string;
  [k: string]: any;
};

type UploadHistoryRes = { uploads: UploadItem[] };
type ForecastRow = Record<string, any>;

const HASH_TO_TAB: Record<string, InventoryFlowTab> = {
  'inventory-forecast': 'inventory',
  'dispatch': 'dispatch',
  'purchase-order': 'purchaseOrder',
};

const TAB_TO_HASH: Record<InventoryFlowTab, string> = {
  inventory: 'inventory-forecast',
  dispatch: 'dispatch',
  purchaseOrder: 'purchase-order',
};

const DUMMY_INVENTORY_FORECAST = [
  {
    sku: 'SKU-DEMO-1',
    'Product Name': 'Demo Product A',
    "Oct'25 Sold": 120,
    "Nov'25 Sold": 140,
    "Dec'25 Sold": 160,
    "Jan'26": 180,
    "Feb'26": 200,
    "Mar'26": 220,
  },
  {
    sku: 'SKU-DEMO-2',
    'Product Name': 'Demo Product B',
    "Oct'25 Sold": 220,
    "Nov'25 Sold": 250,
    "Dec'25 Sold": 280,
    "Jan'26": 300,
    "Feb'26": 330,
    "Mar'26": 360,
  },
  {
    sku: 'Total',
    'Product Name': 'Total',
    "Oct'25 Sold": 340,
    "Nov'25 Sold": 390,
    "Dec'25 Sold": 440,
    "Jan'26": 480,
    "Feb'26": 530,
    "Mar'26": 580,
  },
];

export default function InventoryFlowPage() {
  const params = useParams() as {
    countryName?: string;
    month?: string;
    year?: string;
  };

  const countryName = (params?.countryName ?? '').toLowerCase();

  const today = new Date();
  const currentMonthIndex = today.getMonth();
  const thisYear = today.getFullYear();
  const previousMonthIndex = currentMonthIndex === 0 ? 11 : currentMonthIndex - 1;

  const monthNames = [
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
  ] as const;

  const urlMonth = (params.month ?? '').toLowerCase().trim();

  const effectiveMonth: string = useMemo(() => {
    if (!urlMonth) return monthNames[previousMonthIndex];

    const mnum = urlMonth.match(/\b(1[0-2]|0?[1-9])\b/);
    if (mnum) return monthNames[parseInt(mnum[0], 10) - 1];

    const short = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
    const si = short.indexOf(urlMonth.slice(0, 3));
    if (si !== -1) return monthNames[si];

    const li = monthNames.indexOf(urlMonth as any);
    return li !== -1 ? monthNames[li] : monthNames[previousMonthIndex];
  }, [urlMonth, previousMonthIndex]);

  const effectiveYear =
    /^\d{4}$/.test(params.year ?? '') ? String(params.year) : String(thisYear);

  const isDemoMode =
    params.month?.toUpperCase() === 'NA' &&
    params.year?.toUpperCase() === 'NA';

  const [activeTab, setActiveTab] = useState<InventoryFlowTab>('inventory');
  const [forecastEnabled, setForecastEnabled] = useState(false);

  const [profileCompleted, setProfileCompleted] = useState(false);
  const [isPopupOpen, setIsPopupOpen] = useState(false);

  const [country, setCountry] = useState<'uk' | 'us'>(() =>
    countryName === 'us' ? 'us' : 'uk'
  );
  const [transitTime, setTransitTime] = useState('');
  const [stockUnit, setStockUnit] = useState('');

  const MARKETPLACE_BY_COUNTRY: Record<'uk' | 'us', string> = {
    us: 'ATVPDKIKX0DER',
    uk: 'A1F83G8C2ARO7P',
  };

  const marketplace = MARKETPLACE_BY_COUNTRY[country];

  const [errors, setErrors] = useState<{ [key: string]: string }>({});
  const [apiError, setApiError] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);

  const [uploads, setUploads] = useState<UploadItem[]>([]);
  const [filteredUploads, setFilteredUploads] = useState<UploadItem[]>([]);
  const [missingMonths, setMissingMonths] = useState<string[]>([]);
  const [excelData, setExcelData] = useState<ForecastRow[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showUpload, setShowUpload] = useState(false);
  const [poTriggered, setPoTriggered] = useState(false);
  const [sharedMonth, setSharedMonth] = useState<string>(effectiveMonth);
  const [sharedYear, setSharedYear] = useState<string>(effectiveYear);
  const router = useRouter();

  const countryProfileKeyBase = countryName || 'global';

  useEffect(() => {
    if (typeof window === 'undefined') return;

    const applyHash = (rawHash?: string) => {
      const hash = (rawHash ?? window.location.hash).replace('#', '');
      if (!hash) {
        setActiveTab('inventory');
        return;
      }

      const tab = HASH_TO_TAB[hash];
      if (tab) setActiveTab(tab);
    };

    applyHash(window.location.hash);

    const onHashChange = () => applyHash(window.location.hash);

    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, []);

  const handleTabChange = (tab: InventoryFlowTab) => {
    setActiveTab(tab);
    const hash = TAB_TO_HASH[tab];
    if (typeof window !== 'undefined') {
      window.history.pushState(null, '', `#${hash}`);
    }
  };

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const key = `countryProfileCompleted-${countryProfileKeyBase}`;
    const hasProfile = localStorage.getItem(key) === 'true';
    setProfileCompleted(hasProfile);
  }, [countryProfileKeyBase]);

  useEffect(() => {
    if (!profileCompleted) return;
    const savedMethod = typeof window !== 'undefined'
      ? localStorage.getItem(`forecastMethod-${countryName}-${effectiveMonth}-${effectiveYear}`)
      : null;

    if (savedMethod === 'automation') {
      setForecastEnabled(true);
    }
  }, [profileCompleted, countryName, effectiveMonth, effectiveYear]);

  const tokenOrFail = () => {
    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) throw new Error('Authorization token is missing');
    return token;
  };

  useEffect(() => {
    setSharedMonth(effectiveMonth);
    setSharedYear(effectiveYear);
  }, [effectiveMonth, effectiveYear]);

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

      const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_history`, {
        method: 'GET',
        headers: { Authorization: `Bearer ${token}` },
      });

      if (!res.ok) {
        let msg = 'An error occurred';
        try {
          const j = await res.json();
          msg = j?.error || j?.message || msg;
        } catch { }
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
        if (m < 0) {
          m += 12;
          y -= 1;
        }
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
      } else {
        setMissingMonths([]);
      }

      if (filtered.length < 5) {
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
          ? `${process.env.NEXT_PUBLIC_API_BASE_URL}/forecast_global?month=${encodeURIComponent(
            effectiveMonth
          )}&year=${encodeURIComponent(effectiveYear)}`
          : `${process.env.NEXT_PUBLIC_API_BASE_URL}/api/forecast?country=${encodeURIComponent(
            countryName
          )}&month=${encodeURIComponent(effectiveMonth)}&year=${encodeURIComponent(effectiveYear)}`;

      const res = await fetch(endpoint, {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
      });

      const ctype = res.headers.get('Content-Type') || '';

      if (!res.ok) {
        let serverMsg = '';
        let zeroMonths: string[] = [];
        try {
          const errJson = await res.json();
          serverMsg = errJson?.error || errJson?.message || errJson?.warning || '';
          zeroMonths = Array.isArray(errJson?.zero_months) ? errJson.zero_months : [];
        } catch { }

        setError(serverMsg || `Server error (${res.status})`);

        if (zeroMonths.length) {
          const formatted = zeroMonths.map((s) => {
            const [y, m] = s.split('-').map(Number);
            const d = new Date(y, m - 1, 1);
            return d.toLocaleString('default', { month: 'long', year: 'numeric' });
          });
          setMissingMonths(formatted);
        }

        setLoading(false);
        return;
      }

      if (
        ctype.includes('spreadsheetml') ||
        ctype.includes('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
      ) {
        const blob = await res.blob();
        const buffer = await blob.arrayBuffer();
        const workbook = XLSX.read(buffer, { type: 'array' });
        const sheet = workbook.Sheets[workbook.SheetNames[0]];

        const rows = XLSX.utils.sheet_to_json<any[]>(sheet, {
          header: 1,
          defval: '',
        });

        if (!rows || rows.length < 7) {
          setError('Forecast file format is invalid.');
          setLoading(false);
          return;
        }

        const headerRowIndex = 6;
        const rawHeaders = (rows[headerRowIndex] || []).map((h) =>
          String(h ?? '').trim().replace(/\s+Sold$/i, '')
        );

        const dataRows = rows.slice(headerRowIndex + 1);

        const jsonRows: ForecastRow[] = dataRows
          .filter((row) => Array.isArray(row) && row.some((cell) => String(cell ?? '').trim() !== ''))
          .map((row) => {
            const obj: ForecastRow = {};
            rawHeaders.forEach((header, idx) => {
              if (header) obj[header] = row[idx] ?? '';
            });
            return obj;
          });

        setExcelData(jsonRows);
        localStorage.setItem(`forecast-${countryName}`, JSON.stringify(jsonRows));
        localStorage.setItem(`forecast-time-${countryName}`, Date.now().toString());
        setLoading(false);
        return;
      }

      const data = await res.json();

      if (Array.isArray(data?.forecast)) {
        setExcelData(data.forecast);
        localStorage.setItem(`forecast-${countryName}`, JSON.stringify(data.forecast));
        localStorage.setItem(`forecast-time-${countryName}`, Date.now().toString());
        setLoading(false);
        return;
      }

      setError(data?.warning || data?.message || 'Forecast generated, but no file was returned.');
      setLoading(false);
    } catch (e: any) {
      setError(e?.message || 'Failed to fetch forecast');
      setLoading(false);
    }
  }

  useEffect(() => {
    if (!forecastEnabled) return;

    if (isDemoMode) {
      setExcelData(DUMMY_INVENTORY_FORECAST);
      setLoading(false);
      setError(null);
      return;
    }

    void fetchUploadHistory();
  }, [forecastEnabled, countryName, effectiveMonth, effectiveYear, isDemoMode]);

  const PreviewLockedSection = ({
    enabled,
    children,
    title,
    description,
    buttonText,
    onAction,
  }: {
    enabled: boolean;
    children: React.ReactNode;
    title?: string;
    description?: string;
    buttonText?: string;
    onAction?: () => void;
  }) => {
    return (
      <div className="relative w-full">
        <div
          className={
            enabled
              ? "pointer-events-none select-none opacity-45 transition-all duration-300"
              : "opacity-100 transition-all duration-300"
          }
        >
          {children}
        </div>

        {enabled && (
          <>
            <div className="absolute inset-0 z-10 rounded-xl bg-white/45" />

            <div className="absolute inset-0 z-20 pointer-events-none">
              <div className="sticky top-[18vh] sm:top-[20vh] lg:top-[22vh] 2xl:top-[24vh] flex justify-center px-4">
                <div className="pointer-events-auto w-full max-w-md rounded-2xl bg-white shadow-2xl p-6 text-center">
                  <div className="mb-4 flex justify-center">
                    <div className="flex h-16 w-16 items-center justify-center rounded-full  bg-[#37455F]">
                      <IoMdLock className="text-3xl text-[#F8EDCE]" />
                    </div>
                  </div>

                  <h3 className="text-lg font-semibold text-[#414042]">
                    {title}
                  </h3>

                  <p className="mt-2 text-sm text-gray-600 leading-6">
                    {description}
                  </p>

                  <button
                    onClick={onAction}
                    className="mt-4 rounded-md bg-[#37455F] px-4 py-2 text-sm text-[#F8EDCE] hover:opacity-90 transition"
                  >
                    {buttonText}
                  </button>

                  <p className="mt-3 text-xs text-gray-500">
                    Demo data is shown for preview only.
                  </p>
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    );
  };



  const triggerPurchaseOrderApi = async (country: string, month: string, year: string) => {
    const jwtToken =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    if (!jwtToken) throw new Error('Missing jwt token');

    const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
    if (!baseUrl) throw new Error('Missing NEXT_PUBLIC_API_BASE_URL');

    const safeMonth = month.charAt(0).toUpperCase() + month.slice(1).toLowerCase();

    const formData = new FormData();
    formData.append('month', safeMonth);
    formData.append('year', year);
    formData.append('country', country.toLowerCase());

    const res = await fetch(`${baseUrl}/purchase_order`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${jwtToken}`,
        Accept: 'application/json',
      },
      body: formData,
    });

    const json = await res.json().catch(() => ({}));

    if (!res.ok) {
      throw new Error(json?.error || 'Purchase order API failed');
    }

    return json;
  };

  useEffect(() => {
    if (activeTab === 'purchaseOrder' && !poTriggered) {
      setPoTriggered(true);
      triggerPurchaseOrderApi(countryName, effectiveMonth, effectiveYear).catch((err) => {
        console.error('PO API error:', err);
      });
    }
  }, [activeTab, poTriggered, countryName, effectiveMonth, effectiveYear]);

  const handleAutomation = () => {
    if (!profileCompleted) {
      setIsPopupOpen(true);
      return;
    }

    localStorage.setItem(
      `forecastMethod-${countryName}-${effectiveMonth}-${effectiveYear}`,
      'automation'
    );

    setForecastEnabled(true);
  };

  const handleDispatchAccess = () => {
    if (!profileCompleted) {
      setActiveTab('dispatch');
      setIsPopupOpen(true);
      return;
    }
    setActiveTab('dispatch');
  };

  const handlePurchaseOrderAccess = () => {
    if (!profileCompleted) {
      setActiveTab('purchaseOrder');
      setIsPopupOpen(true);
      return;
    }
    setActiveTab('purchaseOrder');
  };

  const handleSaveProfile = async (e: React.FormEvent) => {
    e.preventDefault();
    setApiError('');
    setErrors({});

    const validationErrors: { [key: string]: string } = {};

    if (!country) {
      validationErrors.country = 'Country is required';
    }

    if (!transitTime) {
      validationErrors.transit_time = 'Transit time is required';
    } else if (!Number.isInteger(Number(transitTime)) || Number(transitTime) <= 0) {
      validationErrors.transit_time = 'Transit time must be a positive integer';
    }

    if (stockUnit === '') {
      validationErrors.stock_unit = 'Stock unit is required';
    } else if (!Number.isInteger(Number(stockUnit)) || Number(stockUnit) < 0) {
      validationErrors.stock_unit = 'Stock unit must be a non-negative integer';
    }

    if (Object.keys(validationErrors).length > 0) {
      setErrors(validationErrors);
      return;
    }

    try {
      setIsSubmitting(true);

      const token = localStorage.getItem('jwtToken');
      if (!token) {
        setApiError('User not authenticated. Token missing.');
        return;
      }

      const res = await fetch(`${API_BASE}/country-profile`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({
          country,
          marketplace,
          transit_time: Number(transitTime),
          stock_unit: Number(stockUnit),
        }),
      });

      const data = await res.json().catch(() => null);

      if (!res.ok) {
        if (data?.errors) {
          setErrors((prev) => ({ ...prev, ...data.errors }));
          setApiError('Please fix the highlighted fields.');
        } else if (data?.error) {
          setApiError(data.error);
        } else {
          setApiError('Failed to save country profile.');
        }
        return;
      }

      const key = `countryProfileCompleted-${countryProfileKeyBase}`;
      localStorage.setItem(key, 'true');
      setProfileCompleted(true);
      setIsPopupOpen(false);

      localStorage.setItem(
        `forecastMethod-${countryName}-${effectiveMonth}-${effectiveYear}`,
        'automation'
      );

      setForecastEnabled(true);
    } catch (err) {
      console.error(err);
      setApiError('Something went wrong while saving country profile.');
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleConnectAmazonPreview = () => {
    const connectCountry = countryName === "global" ? "uk" : countryName;
    router.push(`/integration-dashboard/${connectCountry}/NA/NA`);
  };



  return (
    <>
      <style jsx>{`
     .fetch-button {
          font-family: 'Lato', sans-serif;
          font-size: clamp(12px, 0.729vw, 16px) !important;
          background-color: #2c3e50;
          color: #f8edcf;
          font-weight: bold;
          border: none;
          border-radius: 5px;
          cursor: pointer;
          text-align: center;
          padding: 10px 18px;
          transition: background-color 0.2s ease;
          box-shadow: 0 3px 6px rgba(0, 0, 0, 0.15);
          white-space: nowrap;
        }

        .fetch-button:hover:not(:disabled) {
          background-color: #1f2a36;
        }

        .fetch-button:disabled {
          background-color: #6b7280;
          cursor: not-allowed;
          opacity: 0.8;
        }

        .styled-button {
          font-family: 'Lato', sans-serif;
          font-size: clamp(12px, 0.729vw, 16px) !important;
          background-color: #2c3e50;
          color: #f8edcf;
          font-weight: bold;
          border: none;
          border-radius: 5px;
          cursor: pointer;
          text-align: center;
          padding: 9px 18px;
          margin-left: auto;
        }
     `}</style>
      <div className="font-lato">
        <div className="flex flex-col justify-start">
          <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between mb-4">
            <div className="flex items-baseline gap-2">
              <PageBreadcrumb
                pageTitle={
                  activeTab === 'dispatch'
                    ? 'Dispatch Report -'
                    : activeTab === 'purchaseOrder'
                      ? 'PO Report -'
                      : 'Inventory Forecast -'
                }
                variant="page"
                align="left"
                textSize="2xl"
              />
              <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
                Amazon{" "}
                {countryName?.toLowerCase() === "global" ? "Global" : countryName?.toUpperCase()}
              </span>
            </div>

            {(activeTab === 'dispatch' || activeTab === 'purchaseOrder') && (
              <div className="flex flex-wrap items-center gap-3 justify-start md:justify-end">
                <MonthYearPickerTable
                  month={sharedMonth}
                  year={sharedYear}
                  yearOptions={[new Date().getFullYear(), new Date().getFullYear() - 1]}
                  onMonthChange={(v) => setSharedMonth(v)}
                  onYearChange={(v) => setSharedYear(v)}
                  valueMode="lower"
                />

                {activeTab === 'dispatch' ? (
                  <>
                    <button
                      className="fetch-button"
                      onClick={() => {
                        window.dispatchEvent(
                          new CustomEvent('dispatch-report-refresh', {
                            detail: { month: sharedMonth, year: sharedYear },
                          })
                        );
                      }}
                    >
                      Get Report
                    </button>

                    <DownloadIconButton
                      size="md"
                      onClick={() => {
                        window.dispatchEvent(new CustomEvent('dispatch-report-download'));
                      }}
                    />
                  </>
                ) : (
                  <>
                    <button
                      className="fetch-button"
                      onClick={() => {
                        window.dispatchEvent(
                          new CustomEvent('po-report-refresh', {
                            detail: { month: sharedMonth, year: sharedYear },
                          })
                        );
                      }}
                    >
                      {countryName?.toLowerCase() === 'global' ? 'Get Global Report' : 'Get Report'}
                    </button>

                    <DownloadIconButton
                      size="md"
                      onClick={() => {
                        window.dispatchEvent(new CustomEvent('po-report-download'));
                      }}
                    />
                  </>
                )}
              </div>
            )}
          </div>

          <div className="mb-6">
            <InventoryFlowTabs value={activeTab} onChange={handleTabChange} />
          </div>

          <PreviewLockedSection
            enabled={isDemoMode}
            title="Preview mode"
            description="You're not seeing your real data yet.Connect your Amazon account now to unlock complete visibility into your business performance."
            buttonText="Connect Amazon"
            onAction={handleConnectAmazonPreview}
          >
            {isPopupOpen && (
              <div className="fixed inset-0 z-[99999] flex items-center justify-center bg-black/40">
                <div className="bg-white rounded-2xl shadow-lg w-full max-w-md p-6">
                  <h2 className="text-xl font-semibold text-charcoal-500 mb-1 text-center">
                    Country Profile
                  </h2>
                  <p className="text-sm text-gray-600 mb-4 text-center">
                    Please fill these details once to continue.
                  </p>

                  <form onSubmit={handleSaveProfile} className="space-y-4">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Country <span className="text-red-500">*</span>
                      </label>
                      <select
                        value={country}
                        onChange={(e) => setCountry(e.target.value as 'uk' | 'us')}
                        className="w-full border rounded-lg px-3 py-2 text-sm"
                      >
                        <option value="uk">UK</option>
                        <option value="us">US</option>
                      </select>
                      {errors.country && <p className="text-xs text-red-500 mt-1">{errors.country}</p>}
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Marketplace
                      </label>
                      <input
                        type="text"
                        value={marketplace}
                        disabled
                        className="w-full border rounded-lg px-3 py-2 text-sm bg-gray-100 text-gray-500"
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Transit Time (months) <span className="text-red-500">*</span>
                      </label>
                      <input
                        type="number"
                        min={1}
                        value={transitTime}
                        onChange={(e) => setTransitTime(e.target.value)}
                        className="w-full border rounded-lg px-3 py-2 text-sm"
                        placeholder="e.g. 7"
                      />
                      {errors.transit_time && (
                        <p className="text-xs text-red-500 mt-1">{errors.transit_time}</p>
                      )}
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Stock Unit <span className="text-red-500">*</span>
                      </label>
                      <input
                        type="number"
                        min={0}
                        value={stockUnit}
                        onChange={(e) => setStockUnit(e.target.value)}
                        className="w-full border rounded-lg px-3 py-2 text-sm"
                        placeholder="e.g. 2"
                      />
                      {errors.stock_unit && (
                        <p className="text-xs text-red-500 mt-1">{errors.stock_unit}</p>
                      )}
                    </div>

                    {apiError && (
                      <p className="text-xs text-red-500 mt-1 text-center">{apiError}</p>
                    )}

                    <button
                      type="submit"
                      disabled={isSubmitting}
                      className="mt-2 w-full bg-green-500 text-yellow-200 py-2.5 rounded-lg font-bold"
                    >
                      {isSubmitting ? 'Saving...' : 'Save & Continue'}
                    </button>
                  </form>
                </div>
              </div>
            )}

            <div className="bg-white p-5 sm:p-6 rounded-xl shadow-sm border border-gray-100 ">
              {activeTab === 'inventory' && (
                <div className="w-full ">


                  {!forecastEnabled ? (
                    <div className='max-w-xl'>
                      <div className="flex justify-start items-start gap-2  ">
                        <div>
                          <h3 className="text-base sm:text-lg font-semibold text-charcoal-500">
                            Automation
                          </h3>
                          <p className="text-xs sm:text-sm text-green-500 mt-1">
                            Setup in 30 seconds
                          </p>
                        </div>
                      </div>

                      <ul className="space-y-2 text-[#414042] text-sm sm:text-sm md:text-base mt-4">
                        <li className="flex items-center gap-2">
                          <CheckCircle size={16} className="text-green-500 shrink-0" />
                          <span>AI-powered prediction</span>
                        </li>
                        <li className="flex items-center gap-2">
                          <CheckCircle size={16} className="text-green-500 shrink-0" />
                          <span>Historical data analysis</span>
                        </li>
                        <li className="flex items-center gap-2">
                          <CheckCircle size={16} className="text-green-500 shrink-0" />
                          <span>Always up-to-date insights</span>
                        </li>
                        <li className="flex items-center gap-2">
                          <CheckCircle size={16} className="text-green-500 shrink-0" />
                          <span>No manual work required</span>
                        </li>
                      </ul>

                      <div className="flex justify-start mt-5">
                        <button
                          onClick={handleAutomation}
                          className="bg-green-500 text-yellow-200 px-6 py-2 rounded-lg font-semibold text-sm sm:text-base hover:bg-[#4e937b] transition"
                        >
                          Enable Automation
                        </button>
                      </div>
                    </div>
                  ) : loading ? (
                    <Loading />
                  ) : missingMonths.length > 0 ? (
                    <div className="max-w-3xl">
                      <p className="text-sm sm:text-base mb-4 text-[#414042]">
                        The following monthly files are needed to upload:{' '}
                        <strong className="text-[#60a68e]">{missingMonths.join(', ')}</strong>
                      </p>

                      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 rounded-md border-t-4 border-red-400 bg-gray-100 p-4">
                        <div className="text-sm text-[#414042]">
                          Please fetch at least 4 months&apos; files to see the next two months.
                        </div>
                      </div>
                    </div>
                  ) : error ? (
                    <div className="max-w-3xl">
                      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 rounded-md border-t-4 border-red-400 bg-gray-100 p-4">
                        <div className="text-sm text-[#414042]">{error}</div>
                      </div>
                    </div>
                  ) : (
                    <DisplayInventoryForecast
                      countryName={countryName}
                      month={effectiveMonth}
                      year={effectiveYear}
                      data={excelData ?? []}
                      isDemoMode={isDemoMode}
                    />
                  )}
                </div>
              )}

              {activeTab === 'dispatch' && (
                <>

                  {!profileCompleted ? (
                    <div className="rounded-xl border border-slate-200 bg-slate-50 p-5 text-center">
                      <p className="mt-2 text-sm text-slate-600">
                        Complete inventory setup first, then continue to dispatch planning.
                      </p>
                      <button
                        onClick={handleDispatchAccess}
                        className="mt-5 w-full bg-green-500 text-yellow-200 py-2.5 rounded-lg font-bold"
                      >
                        Continue to Dispatch
                      </button>
                    </div>
                  ) : (
                    <DispatchPage
                      embedded
                      countryNameProp={countryName}
                      selectedMonthProp={sharedMonth}
                      selectedYearProp={sharedYear}
                    />
                  )}
                </>
              )}

              {activeTab === 'purchaseOrder' && (
                <>

                  {!profileCompleted ? (
                    <div className="rounded-xl border border-slate-200 bg-slate-50 p-5 text-center">
                      <p className="mt-2 text-sm text-slate-600">
                        Complete inventory setup first, then continue to purchase order planning.
                      </p>
                      <button
                        onClick={handlePurchaseOrderAccess}
                        className="mt-5 w-full bg-green-500 text-yellow-200 py-2.5 rounded-lg font-bold"
                      >
                        Continue to Purchase Order
                      </button>
                    </div>
                  ) : (
                    <PurchaseOrderPage
                      embedded
                      countryNameProp={countryName}
                      selectedMonthProp={sharedMonth}
                      selectedYearProp={sharedYear}
                    />
                  )}
                </>
              )}
            </div>
          </PreviewLockedSection>
        </div>
      </div>
    </>

  );
}