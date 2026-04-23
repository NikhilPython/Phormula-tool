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
import IntegrationsModal from '@/features/integration/IntegrationsModal';

type UploadItem = {
  filename?: string;
  month?: string | number;
  year?: string | number;
  country?: string;
  [key: string]: any;
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

const INVENTORY_FORECAST_MIN_DATA_MESSAGE =
  "At least 6 months data to be fetched in order to view inventory forecast";

const DUMMY_INVENTORY_FORECAST = [
  {
    sku: 'SKU-DEMO-1',
    'Product Name': 'Demo Product A',
    "Oct'25 Sold": 220,
    "Nov'25 Sold": 250,
    "Dec'25 Sold": 280,
    "Jan'26": 300,
    "Feb'26": 330,
    "Mar'26": 360,
  },
    {
    sku: 'SKU-DEMO-5',
    'Product Name': 'Demo Product E',
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
    sku: 'SKU-DEMO-3',
    'Product Name': 'Demo Product C',
    "Oct'25 Sold": 220,
    "Nov'25 Sold": 250,
    "Dec'25 Sold": 280,
    "Jan'26": 300,
    "Feb'26": 330,
    "Mar'26": 360,
  },
  {
    sku: 'SKU-DEMO-4',
    'Product Name': 'Demo Product D',
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

const DUMMY_DISPATCH_DATA = [
  {
    sNo: 1,
    productName: 'Demo Product A',
    sku: 'DSP-DEMO-1',
    inventoryAtMonthEnd: 0,
    coverageRatioBeforeDispatch: 0,
    dispatch: 0,
    currentInventoryPlusDispatch: 0,
  },
  {
    sNo: 2,
    productName: 'Demo Product B',
    sku: 'DSP-DEMO-1',
    inventoryAtMonthEnd: 0,
    coverageRatioBeforeDispatch: 0,
    dispatch: 0,
    currentInventoryPlusDispatch: 0,
  },
  {
    sNo: 3,
    productName: 'Demo Product C',
    sku: 'DSP-DEMO-3',
    inventoryAtMonthEnd: 0,
    coverageRatioBeforeDispatch: 0,
    dispatch: 0,
    currentInventoryPlusDispatch: 0,
  },
  {
    sNo: 4,
    productName: 'Demo Product D',
    sku: 'DSP-DEMO-4',
    inventoryAtMonthEnd: 0,
    coverageRatioBeforeDispatch: 0,
    dispatch: 0,
    currentInventoryPlusDispatch: 0,
  },
  {
    sNo: 5,
    productName: 'Demo Product E',
    sku: 'DSP-DEMO-5',
    inventoryAtMonthEnd: 0,
    coverageRatioBeforeDispatch: 0,
    dispatch: 0,
    currentInventoryPlusDispatch: 0,
  },
];

const DUMMY_PO_DATA = [
  {
    sNo: 1,
    productName: 'Demo Product A',
    dispatchesUK: 0,
    totalDispatches: 0,
    currentInventory: 0,
    inTransitUnits: 0,
    poToBeRaised: 0,
    costPerUnit: 0,
    poCost: 0,
  },
  {
    sNo: 2,
    productName: 'Demo Product B',
    dispatchesUK: 0,
    totalDispatches: 0,
    currentInventory: 0,
    inTransitUnits: 0,
    poToBeRaised: 0,
    costPerUnit: 0,
    poCost: 0,
  },
  {
    sNo: 3,
    productName: 'Demo Product C',
    dispatchesUK: 0,
    totalDispatches: 0,
    currentInventory: 0,
    inTransitUnits: 0,
    poToBeRaised: 0,
    costPerUnit: 0,
    poCost: 0,
  },
  {
    sNo: 4,
    productName: 'Demo Product D',
    dispatchesUK: 0,
    totalDispatches: 0,
    currentInventory: 0,
    inTransitUnits: 0,
    poToBeRaised: 0,
    costPerUnit: 0,
    poCost: 0,
  },
  {
    sNo: 5,
    productName: 'Demo Product E',
    dispatchesUK: 0,
    totalDispatches: 0,
    currentInventory: 0,
    inTransitUnits: 0,
    poToBeRaised: 0,
    costPerUnit: 0,
    poCost: 0,
  },
];

const INVENTORY_REQUIREMENT_MESSAGE =
  "This section requires you to fetch at least 6 months of data";

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
  const effectiveYear =
    /^\d{4}$/.test(params.year ?? '') ? String(params.year) : String(thisYear);

  const effectiveMonth: string = useMemo(() => {
    if (!urlMonth) return monthNames[currentMonthIndex];

    const numericMonthMatch = urlMonth.match(/\b(1[0-2]|0?[1-9])\b/);
    if (numericMonthMatch) {
      return monthNames[parseInt(numericMonthMatch[0], 10) - 1];
    }

    const shortMonths = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
    const shortMonthIndex = shortMonths.indexOf(urlMonth.slice(0, 3));
    if (shortMonthIndex !== -1) {
      return monthNames[shortMonthIndex];
    }

    const fullMonthIndex = monthNames.indexOf(urlMonth as any);
    return fullMonthIndex !== -1 ? monthNames[fullMonthIndex] : monthNames[currentMonthIndex];
  }, [urlMonth, currentMonthIndex]);

  const isDemoMode =
    params.month?.toUpperCase() === 'NA' &&
    params.year?.toUpperCase() === 'NA';

  const [activeTab, setActiveTab] = useState<InventoryFlowTab>('inventory');
  const [, setUploads] = useState<UploadItem[]>([]);
  const [filteredUploads, setFilteredUploads] = useState<UploadItem[]>([]);
  const [missingMonths, setMissingMonths] = useState<string[]>([]);
  const [excelData, setExcelData] = useState<ForecastRow[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showUpload, setShowUpload] = useState(false);
  const [showIntegrationModal, setShowIntegrationModal] = useState(false);
  const [sharedMonth, setSharedMonth] = useState<string>(effectiveMonth);
  const [sharedYear, setSharedYear] = useState<string>(effectiveYear);
  const [showInventoryRequirementPopup, setShowInventoryRequirementPopup] = useState(false);
  const uploadHistoryInFlightRef = useRef<string | null>(null);
  const forecastInFlightRef = useRef<string | null>(null);
  const latestForecastRequestRef = useRef<string | null>(null);
  const lastPoTriggerRef = useRef<string | null>(null);

  useEffect(() => {
    if (typeof window === 'undefined') return;

    const applyHash = (rawHash?: string) => {
      const hash = (rawHash ?? window.location.hash).replace('#', '');

      if (!hash) {
        setActiveTab('inventory');
        return;
      }

      const tab = HASH_TO_TAB[hash];
      if (tab) {
        setActiveTab(tab);
      }
    };

    const onHashChange = () => applyHash(window.location.hash);

    const onPageHashNavigate = (event: Event) => {
      const customEvent = event as CustomEvent<{ hash?: string }>;
      applyHash(customEvent.detail?.hash ? `#${customEvent.detail.hash}` : window.location.hash);
    };

    applyHash(window.location.hash);

    window.addEventListener('hashchange', onHashChange);
    window.addEventListener('page-hash-navigate', onPageHashNavigate as EventListener);

    return () => {
      window.removeEventListener('hashchange', onHashChange);
      window.removeEventListener('page-hash-navigate', onPageHashNavigate as EventListener);
    };
  }, []);

  const handleTabChange = (tab: InventoryFlowTab) => {
    setActiveTab(tab);
    const hash = TAB_TO_HASH[tab];

    if (typeof window !== 'undefined') {
      window.location.hash = hash;
    }
  };

  useEffect(() => {
    setSharedMonth(effectiveMonth);
    setSharedYear(effectiveYear);
  }, [effectiveMonth, effectiveYear]);

  const tokenOrFail = () => {
    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    if (!token) {
      throw new Error('Authorization token is missing');
    }

    return token;
  };

  const normalizeMonthForApi = (month: string) => {
    const raw = String(month || '').trim().toLowerCase();

    const shortMonths = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
    const fullMonths = [
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

    const fullIndex = fullMonths.indexOf(raw);
    if (fullIndex !== -1) return fullMonths[fullIndex];

    const shortIndex = shortMonths.indexOf(raw.slice(0, 3));
    if (shortIndex !== -1) return fullMonths[shortIndex];

    const numeric = parseInt(raw, 10);
    if (!Number.isNaN(numeric) && numeric >= 1 && numeric <= 12) {
      return fullMonths[numeric - 1];
    }

    return raw;
  };

  const ensureForecastExists = async (country: string, month: string, year: string) => {
    const token = tokenOrFail();
    const normalizedMonth = normalizeMonthForApi(month);

    const res = await fetch(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/api/forecast?country=${encodeURIComponent(
        country.toLowerCase()
      )}&month=${encodeURIComponent(normalizedMonth)}&year=${encodeURIComponent(year)}`,
      {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
        },
      }
    );

    if (!res.ok) {
      const json = await res.json().catch(() => ({}));
      throw new Error(json?.error || json?.message || 'Inventory forecast not found');
    }

    return res;
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
        let message = 'An error occurred';

        try {
          const json = await res.json();
          message = json?.error || json?.message || message;
        } catch { }

        throw new Error(message);
      }

      const data = (await res.json()) as UploadHistoryRes;

      const filtered = countryName
        ? data.uploads.filter(
          (upload) => (upload.country ?? '').toString().toLowerCase() === countryName
        )
        : data.uploads;

      setUploads(data.uploads);
      setFilteredUploads(filtered);

      const months = filtered.map((upload) => {
        const monthIndex = new Date(`${upload.month} 1, ${upload.year}`).getMonth();
        return `${upload.year}-${String(monthIndex + 1).padStart(2, '0')}`;
      });

      const uniqueMonths = new Set(months);

      const currentDate = new Date();
      const currentMonth = currentDate.getMonth();
      const currentYear = currentDate.getFullYear();

      const previousFiveMonths: string[] = [];

      for (let i = 1; i <= 5; i += 1) {
        let month = currentMonth - i;
        let year = currentYear;

        if (month < 0) {
          month += 12;
          year -= 1;
        }

        previousFiveMonths.push(`${year}-${String(month + 1).padStart(2, '0')}`);
      }

      const missing = previousFiveMonths.filter((value) => !uniqueMonths.has(value));

      if (missing.length > 0) {
        const formattedMissing = missing.map((value) => {
          const [year, month] = value.split('-').map(Number);
          const date = new Date(year, month - 1, 1);
          return date.toLocaleString('default', { month: 'long', year: 'numeric' });
        });

        setMissingMonths(formattedMissing);
        setExcelData([]);
        setError(INVENTORY_REQUIREMENT_MESSAGE);
        setShowInventoryRequirementPopup(true);
        setLoading(false);
        return;
      }

      setMissingMonths([]);

      if (filtered.length < 6) {
        setExcelData([]);
        setError(INVENTORY_REQUIREMENT_MESSAGE);
        setShowInventoryRequirementPopup(true);
        setLoading(false);
        return;
      }

      await fetchForecastData();
    } catch (err: any) {
      setError(err?.message ?? 'Failed to fetch upload history');
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
          )}&month=${encodeURIComponent(effectiveMonth)}&year=${encodeURIComponent(
            effectiveYear
          )}`;

      const res = await fetch(endpoint, {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
      });

      if (latestForecastRequestRef.current !== requestKey) return;

      const contentType = res.headers.get('Content-Type') || '';

      if (!res.ok) {
        let serverMessage = '';

        try {
          const errJson = await res.json();
          serverMessage = errJson?.error || errJson?.message || errJson?.warning || '';
        } catch { }

        setExcelData([]);
        setError(serverMessage || `Server error (${res.status})`);
        setLoading(false);
        return;
      }

      if (
        contentType.includes('spreadsheetml') ||
        contentType.includes('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
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
          setExcelData([]);
          setError('Forecast file format is invalid.');
          setLoading(false);
          return;
        }

        const headerRowIndex = 6;
        const rawHeaders = (rows[headerRowIndex] || []).map((header) =>
          String(header ?? '').trim().replace(/\s+Sold$/i, '')
        );

        const dataRows = rows.slice(headerRowIndex + 1);

        const jsonRows: ForecastRow[] = dataRows
          .filter(
            (row) => Array.isArray(row) && row.some((cell) => String(cell ?? '').trim() !== '')
          )
          .map((row) => {
            const obj: ForecastRow = {};
            rawHeaders.forEach((header, index) => {
              if (header) obj[header] = row[index] ?? '';
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
      setExcelData([]);
      setError(data?.warning || data?.message || 'Forecast generated, but no file was returned.');
      setLoading(false);
    } catch (err: any) {
      setExcelData([]);
      setError(err?.message || 'Failed to fetch forecast');
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

    if (!jwtToken) {
      throw new Error('Missing jwt token');
    }

    const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
    if (!baseUrl) {
      throw new Error('Missing NEXT_PUBLIC_API_BASE_URL');
    }

    const normalizedMonth = normalizeMonthForApi(month).toLowerCase();

    const formData = new FormData();
    formData.append('month', normalizedMonth);
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
      throw new Error(json?.error || json?.message || 'Purchase order API failed');
    }

    return json;
  };

  useEffect(() => {
    if (activeTab !== 'purchaseOrder' || isDemoMode) return;
    if (!countryName || !sharedMonth || !sharedYear) return;

    const normalizedMonth = normalizeMonthForApi(sharedMonth);
    const key = `${countryName}-${normalizedMonth}-${sharedYear}`;
    if (lastPoTriggerRef.current === key) return;

    lastPoTriggerRef.current = key;

    const run = async () => {
      try {
        await ensureForecastExists(countryName, normalizedMonth, sharedYear);
        await triggerPurchaseOrderApi(countryName, normalizedMonth, sharedYear);
      } catch (err: any) {
        console.error('PO API error:', err);
      }
    };

    void run();
  }, [activeTab, countryName, sharedMonth, sharedYear, isDemoMode]);

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

  useEffect(() => {
    const hasValidData =
      Array.isArray(excelData) &&
      excelData.length > 0 &&
      missingMonths.length === 0 &&
      !error;

    if (hasValidData) {
      setShowInventoryRequirementPopup(false);
    } else {
      setShowInventoryRequirementPopup(true);
    }
  }, [excelData, missingMonths, error]);

  const DemoDispatchPreview = () => {
    return (
      <div className="rounded-xl border border-slate-200 bg-white shadow-sm overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-green-500 text-[#F8EDCE]">
              <tr>
                <th className="px-4 py-3 text-center">S. No.</th>
                <th className="px-4 py-3 text-center">Product Name</th>
                <th className="px-4 py-3 text-center">SKU</th>
                <th className="px-4 py-3 text-center">Inventory At Month End</th>
                <th className="px-4 py-3 text-center">Coverage Ratio Before Dispatch</th>
                <th className="px-4 py-3 text-center">Dispatch</th>
                <th className="px-4 py-3 text-center">Current Inventory + Dispatch</th>
              </tr>
            </thead>

            <tbody>
              {DUMMY_DISPATCH_DATA.map((row, idx) => (
                <tr key={idx} className="border-b border-slate-100">
                  <td className="px-4 py-3 text-center">{row.sNo}</td>
                  <td className="px-4 py-3 text-center">{row.productName}</td>
                  <td className="px-4 py-3 text-center">{row.sku}</td>
                  <td className="px-4 py-3 text-center">{row.inventoryAtMonthEnd}</td>
                  <td className="px-4 py-3 text-center">{row.coverageRatioBeforeDispatch}</td>
                  <td className="px-4 py-3 text-center">{row.dispatch}</td>
                  <td className="px-4 py-3 text-center">{row.currentInventoryPlusDispatch}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  const DemoPurchaseOrderPreview = () => {
    return (
      <div className="rounded-xl border border-slate-200 bg-white shadow-sm overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-green-500 text-[#F8EDCE]">
              <tr>
                <th className="px-4 py-3 text-center">S. No.</th>
                <th className="px-4 py-3 text-center">Product Name</th>
                <th className="px-4 py-3 text-center">Dispatches UK</th>
                <th className="px-4 py-3 text-center">Total Dispatches</th>
                <th className="px-4 py-3 text-center">Current Inventory - Local Warehouse</th>
                <th className="px-4 py-3 text-center">In Transit Units</th>
                <th className="px-4 py-3 text-center">PO To Be Raised</th>
                <th className="px-4 py-3 text-center">Cost Per Unit (in INR)</th>
                <th className="px-4 py-3 text-center">PO Cost (in INR)</th>
              </tr>
            </thead>
            <tbody>
              {DUMMY_PO_DATA.map((row, idx) => (
                <tr key={idx} className="border-b border-slate-100">
                  <td className="px-4 py-3 text-center">{row.sNo}</td>
                  <td className="px-4 py-3 text-center">{row.productName}</td>
                  <td className="px-4 py-3 text-center">{row.dispatchesUK}</td>
                  <td className="px-4 py-3 text-center">{row.totalDispatches}</td>
                  <td className="px-4 py-3 text-center">{row.currentInventory}</td>
                  <td className="px-4 py-3 text-center">{row.inTransitUnits}</td>
                  <td className="px-4 py-3 text-center">{row.poToBeRaised}</td>
                  <td className="px-4 py-3 text-center">{row.costPerUnit}</td>
                  <td className="px-4 py-3 text-center">{row.poCost}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

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
              ? "pointer-events-none select-none opacity-45  transition-all duration-300"
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

                  {/* <p className="mt-3 text-xs text-gray-500">
                    Demo data is shown for preview only.
                  </p> */}
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
    router.push(`/profile/${connectCountry}/NA/NA`);
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
                    ? 'Dispatch Report - Amazon'
                    : activeTab === 'purchaseOrder'
                      ? 'PO Report - Amazon'
                      : 'Inventory Forecast - Amazon'
                }
                variant="page"
                align="left"
                textSize="2xl"
              />
              <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
                {countryName?.toLowerCase() === 'global' ? 'Global' : countryName?.toUpperCase()}
              </span>
            </div>

            {(activeTab === 'dispatch' || activeTab === 'purchaseOrder') && (
              <div className="flex flex-wrap items-center gap-3 justify-start md:justify-end">
                <MonthYearPickerTable
                  month={sharedMonth}
                  year={sharedYear}
                  yearOptions={[new Date().getFullYear(), new Date().getFullYear() - 1]}
                  onMonthChange={(value) => setSharedMonth(value)}
                  onYearChange={(value) => setSharedYear(value)}
                  valueMode="lower"
                />

                {activeTab === 'dispatch' ? (
                  <>
                    {/* <button
                      className="fetch-button"
                      disabled={isDemoMode}
                      onClick={() => {
                        window.dispatchEvent(
                          new CustomEvent('dispatch-report-refresh', {
                            detail: { month: sharedMonth, year: sharedYear },
                          })
                        );
                      }}
                    >
                      Get Report
                    </button> */}

                    <DownloadIconButton
                      size="md"
                      onClick={() => {
                        window.dispatchEvent(new CustomEvent('dispatch-report-download'));
                      }}
                      disabled={isDemoMode}
                    />
                  </>
                ) : (
                  <>
                    {/* <button
                      className="fetch-button"
                      disabled={isDemoMode}
                      onClick={() => {
                        window.dispatchEvent(
                          new CustomEvent('po-report-refresh', {
                            detail: { month: sharedMonth, year: sharedYear },
                          })
                        );
                      }}
                    >
                      {countryName?.toLowerCase() === 'global' ? 'Get Global Report' : 'Get Report'}
                    </button> */}

                    <DownloadIconButton
                      size="md"
                      onClick={() => {
                        window.dispatchEvent(new CustomEvent('po-report-download'));
                      }}
                      disabled={isDemoMode}
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
            title="Preview Mode"
            description="To view your real business data and analytics, please complete your profile and connect your Amazon account. This will unlock your performance dashboard and insights."
            buttonText="Complete Setup"
            onAction={handleConnectAmazonPreview}
          >
            <div>
              {loading ? (
                <Loading />
              ) : activeTab === 'inventory' ? (
                <div id="inventory-forecast" className="scroll-mt-[80px] relative">
                  <DisplayInventoryForecast
                    countryName={countryName}
                    month={effectiveMonth}
                    year={effectiveYear}
                    data={excelData ?? []}
                    isDemoMode={isDemoMode}
                  />

                  {(missingMonths.length > 0 || !excelData || excelData.length === 0) && (
                    <div className="fixed inset-0 z-[100] pointer-events-none">
                      <div className="absolute inset-0 bg-white/45" />

                      <div className="absolute inset-0 flex items-center justify-center px-4 py-8">
                        <div className="pointer-events-auto w-full max-w-md rounded-2xl bg-white shadow-2xl border border-slate-200 p-6 text-center">
                          <div className="mx-auto mb-4 flex h-14 w-14 items-center justify-center rounded-full bg-amber-50 border border-amber-200">
                            <svg
                              width="24"
                              height="24"
                              viewBox="0 0 24 24"
                              fill="none"
                              className="text-amber-600"
                            >
                              <path
                                d="M12 9V13M12 16H12.01M10.29 3.86L1.82 18A2 2 0 0 0 3.55 21H20.45A2 2 0 0 0 22.18 18L13.71 3.86A2 2 0 0 0 10.29 3.86Z"
                                stroke="currentColor"
                                strokeWidth="2"
                                strokeLinecap="round"
                                strokeLinejoin="round"
                              />
                            </svg>
                          </div>

                          <h3 className="text-lg font-semibold text-[#414042]">
                            Data required
                          </h3>

                          <p className="mt-2 text-sm leading-6 text-gray-600">
                            This section requires you to fetch at least 6 months of data.
                          </p>

                          <div className="mt-5 flex items-center justify-center gap-3">

                            <button
                              onClick={() => {
                                setShowIntegrationModal(true);
                              }}
                              className="rounded-md bg-[#37455F] px-4 py-2 text-sm text-[#F8EDCE] hover:opacity-90 transition"
                            >
                              Fetch Data
                            </button>
                          </div>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              ) : activeTab === 'dispatch' ? (
                <div id="dispatch" className="scroll-mt-[80px]">
                  {isDemoMode ? (
                    <DemoDispatchPreview />
                  ) : (
                    <DispatchPage
                      embedded
                      countryNameProp={countryName}
                      selectedMonthProp={sharedMonth}
                      selectedYearProp={sharedYear}
                    />
                  )}
                </div>
              ) : (
                <div id="purchase-order" className="scroll-mt-[80px]">
                  {isDemoMode ? (
                    <DemoPurchaseOrderPreview />
                  ) : (
                    <PurchaseOrderPage
                      embedded
                      countryNameProp={countryName}
                      selectedMonthProp={sharedMonth}
                      selectedYearProp={sharedYear}
                    />
                  )}
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
        <IntegrationsModal
          open={showIntegrationModal}
          onClose={() => setShowIntegrationModal(false)}
        />
      </div>
    </>
  );
}