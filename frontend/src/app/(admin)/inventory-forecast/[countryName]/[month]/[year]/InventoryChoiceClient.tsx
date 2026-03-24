'use client';

import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useParams, useRouter } from 'next/navigation';
import * as XLSX from 'xlsx';

import DisplayInventoryForecast from '@/components/inventory/DisplayInventoryForecast';
import Loading from '@/components/inventory/Loading';
import { Modal } from '@/components/ui/modal';
import FileUploadForm from '@/app/(admin)/(ui-elements)/modals/FileUploadForm';
import InventoryFlowTabs, { InventoryFlowTab } from '@/components/inventory/InventoryFlowTabs';
import DispatchPage from '@/app/(admin)/dispatch/[countryName]/[month]/[year]/DispatchClient';
import PurchaseOrderPage from '@/app/(admin)/purchase-order/[countryName]/[month]/[year]/PurchaseOrderClient';
import PageBreadcrumb from '@/components/common/PageBreadCrumb';
import MonthYearPickerTable from '@/components/filters/MonthYearPickerTable';
import DownloadIconButton from '@/components/ui/button/DownloadButton';
import { IoMdLock } from 'react-icons/io';

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
  dispatch: 'dispatch',
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

  const router = useRouter();
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
  const [uploads, setUploads] = useState<UploadItem[]>([]);
  const [filteredUploads, setFilteredUploads] = useState<UploadItem[]>([]);
  const [missingMonths, setMissingMonths] = useState<string[]>([]);
  const [excelData, setExcelData] = useState<ForecastRow[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showUpload, setShowUpload] = useState(false);
  const [sharedMonth, setSharedMonth] = useState<string>(effectiveMonth);
  const [sharedYear, setSharedYear] = useState<string>(effectiveYear);
  const [poTriggered, setPoTriggered] = useState(false);

  const uploadHistoryInFlightRef = useRef<string | null>(null);
  const forecastInFlightRef = useRef<string | null>(null);
  const latestForecastRequestRef = useRef<string | null>(null);

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
    setSharedMonth(effectiveMonth);
    setSharedYear(effectiveYear);
  }, [effectiveMonth, effectiveYear]);

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
    if (!countryName || !effectiveMonth || !effectiveYear) return;

    const requestKey = `${countryName}-${effectiveMonth}-${effectiveYear}`;

    if (uploadHistoryInFlightRef.current === requestKey) return;
    uploadHistoryInFlightRef.current = requestKey;

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
        setLoading(false);
        return;
      }

      setMissingMonths([]);

      if (filtered.length < 5) {
        setLoading(false);
        return;
      }

      await fetchForecastData();
    } catch (e: any) {
      setError(e?.message ?? 'Failed to fetch upload history');
      setLoading(false);
    } finally {
      if (uploadHistoryInFlightRef.current === requestKey) {
        uploadHistoryInFlightRef.current = null;
      }
    }
  }

  async function fetchForecastData() {
    if (!countryName || !effectiveMonth || !effectiveYear) return;

    const requestKey = `${countryName}-${effectiveMonth}-${effectiveYear}`;

    if (forecastInFlightRef.current === requestKey) return;

    forecastInFlightRef.current = requestKey;
    latestForecastRequestRef.current = requestKey;

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

      if (latestForecastRequestRef.current !== requestKey) return;

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
        setError(null);
        setLoading(false);
        return;
      }

      const data = await res.json();

      if (Array.isArray(data?.forecast)) {
        setExcelData(data.forecast);
        setError(null);
        setLoading(false);
        return;
      }

      setError(data?.warning || data?.message || 'Forecast generated, but no file was returned.');
      setLoading(false);
    } catch (e: any) {
      setError(e?.message || 'Failed to fetch forecast');
      setLoading(false);
    } finally {
      if (forecastInFlightRef.current === requestKey) {
        forecastInFlightRef.current = null;
      }
    }
  }

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
    if (activeTab === 'purchaseOrder' && !poTriggered && !isDemoMode) {
      setPoTriggered(true);
      triggerPurchaseOrderApi(countryName, effectiveMonth, effectiveYear).catch((err) => {
        console.error('PO API error:', err);
      });
    }
  }, [activeTab, poTriggered, countryName, effectiveMonth, effectiveYear, isDemoMode]);

  useEffect(() => {
    if (isDemoMode) {
      setExcelData(DUMMY_INVENTORY_FORECAST);
      setLoading(false);
      setError(null);
      return;
    }

    if (!countryName || !effectiveMonth || !effectiveYear) return;

    void fetchUploadHistory();
  }, [countryName, effectiveMonth, effectiveYear, isDemoMode]);

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
              ? 'pointer-events-none select-none opacity-45 transition-all duration-300'
              : 'opacity-100 transition-all duration-300'
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
                    <div className="flex h-16 w-16 items-center justify-center rounded-full bg-[#37455F]">
                      <IoMdLock className="text-3xl text-[#F8EDCE]" />
                    </div>
                  </div>

                  <h3 className="text-lg font-semibold text-[#414042]">{title}</h3>
                  <p className="mt-2 text-sm text-gray-600 leading-6">{description}</p>

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

  const handleConnectAmazonPreview = () => {
    const connectCountry = countryName === 'global' ? 'uk' : countryName;
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

        .alert-container {
          display: flex;
          align-items: center;
          background-color: #f2f2f2;
          border-top: 4px solid #ff5c5c;
          padding: 12px 16px;
          border-radius: 6px;
          font-family: 'Lato', sans-serif;
          width: 100%;
          justify-content: space-between;
          box-sizing: border-box;
          margin-top: 20px;
          gap: 12px;
          flex-wrap: wrap;
        }

        .alert-message {
          display: flex;
          align-items: center;
          color: #414042;
          font-size: 12px;
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
                Amazon {countryName?.toLowerCase() === 'global' ? 'Global' : countryName?.toUpperCase()}
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
            <div className="bg-white p-5 sm:p-6 rounded-xl shadow-sm border border-gray-100">
              {loading ? (
                <Loading />
              ) : activeTab === 'inventory' ? (
                missingMonths.length > 0 ? (
                  <div id="inventory-forecast" className="scroll-mt-[80px]">
                    <p className="text-sm sm:text-base mb-4 text-[#414042]">
                      The following monthly files are needed to upload:{' '}
                      <strong className="text-[#60a68e]">{missingMonths.join(', ')}</strong>
                    </p>

                    <div className="alert-container">
                      <div className="alert-message">
                        <span>Please upload at least 4 months&apos; files to see the next two months.</span>
                      </div>
                      <button
                        className="alert-button"
                        onClick={() => setShowUpload(true)}
                      >
                        Upload Now
                      </button>
                    </div>
                  </div>
                ) : error ? (
                  <div id="inventory-forecast" className="scroll-mt-[80px]">
                    <div className="alert-container">
                      <div className="alert-message">
                        <span>{error}</span>
                      </div>
                      <button
                        className="alert-button"
                        onClick={() => setShowUpload(true)}
                      >
                        Upload Now
                      </button>
                    </div>
                  </div>
                ) : (
                  <div id="inventory-forecast" className="scroll-mt-[80px]">
                    <DisplayInventoryForecast
                      countryName={countryName}
                      month={effectiveMonth}
                      year={effectiveYear}
                      data={excelData ?? []}
                      isDemoMode={isDemoMode}
                    />
                  </div>
                )
              ) : activeTab === 'dispatch' ? (
                <div id="dispatch" className="scroll-mt-[80px]">
                  <DispatchPage
                    embedded
                    countryNameProp={countryName}
                    selectedMonthProp={sharedMonth}
                    selectedYearProp={sharedYear}
                  />
                </div>
              ) : (
                <div id="purchase-order" className="scroll-mt-[80px]">
                  <PurchaseOrderPage
                    embedded
                    countryNameProp={countryName}
                    selectedMonthProp={sharedMonth}
                    selectedYearProp={sharedYear}
                  />
                </div>
              )}
            </div>
          </PreviewLockedSection>
        </div>

        <Modal
          isOpen={showUpload}
          onClose={() => setShowUpload(false)}
          showCloseButton
          className="max-w-4xl w-full mx-auto p-0"
        >
          <FileUploadForm
            initialCountry={countryName}
            onClose={() => setShowUpload(false)}
            onComplete={() => {
              setShowUpload(false);
              void fetchUploadHistory();
            }}
          />
        </Modal>
      </div>
    </>
  );
}