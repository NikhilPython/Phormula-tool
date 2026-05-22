'use client';

import React, { useState, useEffect, useRef } from 'react';
import * as XLSX from 'xlsx';
import { useParams, useRouter } from 'next/navigation';
import './Styles.css';
import PnlForecastChart from '@/components/pnlforecast/PnlForecastChart';
import GroupedCollapsibleTables, {
  ColGroup,
  LeafCol,
} from '@/components/ui/table/GroupedCollapsibleTables';
import { exportPnLForecastExcel } from '@/lib/excel/exportCurrentInventoryExcel';
import DownloadIconButton from '@/components/ui/button/DownloadIconButton';
import PageBreadcrumb from '@/components/common/PageBreadCrumb';
import { useGetUserDataQuery } from '@/lib/api/profileApi';
import { IoMdLock } from 'react-icons/io';
import Loader from '@/components/loader/Loader';

type RowData = {
  sku?: string;
  product_name?: string;
  value?: number;
  [key: string]: any;
};

type ChartDataItem = {
  month: string;
  SALES?: number;
  COGS?: number;
  'AMAZON EXPENSE'?: number;
  'ADVERTISING COSTS'?: number;
  'CM1 PROFIT'?: number;
  'CM2 PROFIT'?: number;
  isForecast?: boolean;
  isHistorical?: boolean;
};

type SelectedGraphs = Record<string, boolean>;

const getCurrencySymbol = (country: string): string => {
  switch ((country || '').toLowerCase()) {
    case 'uk':
      return '£';
    case 'india':
      return '₹';
    case 'us':
      return '$';
    case 'europe':
    case 'eu':
      return '€';
    case 'global':
      return '$';
    default:
      return '¤';
  }
};

const formatNumber = (val: any): string => {
  if (val === null || val === undefined || val === '' || isNaN(Number(val))) {
    return 'N/A';
  }

  return Math.abs(Math.round(Number(val))).toLocaleString(undefined, {
    maximumFractionDigits: 0,
  });
};

const roundNumber = (val: any): number => {
  if (val === null || val === undefined || val === '' || isNaN(Number(val))) {
    return 0;
  }

  return Math.round(Number(val));
};

const formatPercent = (val: any): string => {
  if (val === null || val === undefined || val === '' || isNaN(Number(val))) return '';
  return `${Number(val).toFixed(2)}%`;
};

const formatCellValue = (key: string, value: any): string => {
  if (value === null || value === undefined || value === '') return '';

  const unitsKeys = [
    'forecast_sum',
    'forecast_1st',
    'forecast_2nd',
    'forecast_3rd',
  ];

  if (unitsKeys.includes(key)) return formatNumber(value);

  const percentKeys = [
    'profit_percentage_sum',
    'profit_percentage_1st',
    'profit_percentage_2nd',
    'profit_percentage_3rd',
  ];

  if (percentKeys.includes(key)) return formatPercent(value);

  const formattedKeys = [
    'Total_Sales_sum',
    'Total_Sales_1st',
    'Total_Sales_2nd',
    'Total_Sales_3rd',
    'profit_sum',
    'profit_1st',
    'profit_2nd',
    'profit_3rd',
  ];

  if (formattedKeys.includes(key)) return formatNumber(value);

  return typeof value === 'number' || !isNaN(Number(value))
    ? formatNumber(value)
    : value;
};

const formatMonthYear = (monthName: string, yearVal: number | string) => {
  const date = new Date(`${monthName} 1, ${yearVal}`);
  return date.toLocaleString('en-US', { month: 'short' }) + `'` + String(yearVal).slice(-2);
};

const addMonths = (monthName: string, yearVal: number, offset: number) => {
  const date = new Date(`${monthName} 1, ${yearVal}`);
  date.setMonth(date.getMonth() + offset);
  return {
    month: date.toLocaleString('default', { month: 'long' }),
    year: date.getFullYear(),
  };
};

const toNumber = (value: any): number => {
  if (value === null || value === undefined || value === '') return 0;
  const num = Number(String(value).replace(/,/g, '').trim());
  return Number.isFinite(num) ? num : 0;
};

const isPnlTotalRow = (row: RowData) =>
  String(row?.product_name || '').trim().toLowerCase() === 'total' ||
  String(row?.sku || '').trim().toLowerCase() === 'total';

const PNL_PERCENT_KEYS = [
  'profit_percentage_1st',
  'profit_percentage_2nd',
  'profit_percentage_3rd',
  'profit_percentage_sum',
];

const recomputePnlPercentages = (row: RowData): RowData => {
  const next = { ...row };

  const percentagePairs = [
    ['Total_Sales_1st', 'profit_1st', 'profit_percentage_1st'],
    ['Total_Sales_2nd', 'profit_2nd', 'profit_percentage_2nd'],
    ['Total_Sales_3rd', 'profit_3rd', 'profit_percentage_3rd'],
    ['Total_Sales_sum', 'profit_sum', 'profit_percentage_sum'],
  ];

  percentagePairs.forEach(([salesKey, profitKey, percentageKey]) => {
    const sales = toNumber(next[salesKey]);
    const profit = toNumber(next[profitKey]);

    next[percentageKey] = sales !== 0 ? (profit / sales) * 100 : 0;
  });

  return next;
};

const buildAggregatePnlRow = (
  rows: RowData[],
  productName: string
): RowData => {
  const nonSummableKeys = new Set([
    'sku',
    'product_name',
    'sr_no',
    ...PNL_PERCENT_KEYS,
  ]);

  const numericKeys = Array.from(
    new Set(
      rows.flatMap((row) =>
        Object.keys(row || {}).filter((key) => {
          if (nonSummableKeys.has(key)) return false;

          const value = row[key];

          return (
            value !== null &&
            value !== undefined &&
            value !== '' &&
            !isNaN(Number(String(value).replace(/,/g, '').trim()))
          );
        })
      )
    )
  );

  const aggregateRow: RowData = {
    product_name: productName,
    sku: productName === 'Total' ? 'Total' : '',
  };

  numericKeys.forEach((key) => {
    aggregateRow[key] = rows.reduce((sum, row) => sum + toNumber(row[key]), 0);
  });

  return recomputePnlPercentages(aggregateRow);
};

const buildOthersPnlRow = (rows: RowData[]): RowData => {
  return buildAggregatePnlRow(rows, 'Others');
};

const Pnlforecast: React.FC = () => {
  const router = useRouter();
  const params = useParams();
  const countryName = (params?.countryName as string) || '';
  const urlMonth = (params?.month as string) || '';
  const urlYear = (params?.year as string) || '';

  const isDemoMode =
    urlMonth?.toUpperCase() === 'NA' &&
    urlYear?.toUpperCase() === 'NA';

  const { data: userData } = useGetUserDataQuery();

  const companyName =
    (userData as any)?.companyName ||
    (userData as any)?.company_name ||
    (userData as any)?.company ||
    '';

  const brandName =
    (userData as any)?.brandName ||
    (userData as any)?.brand_name ||
    (userData as any)?.brand ||
    '';

  const effectiveCountry = isDemoMode ? 'global' : countryName;
  const currencySymbol = getCurrencySymbol(effectiveCountry);

  const selectedMonth = isDemoMode ? 'March' : urlMonth;
  const selectedYear = isDemoMode ? '2026' : urlYear;

  const month = selectedMonth;
  const year = selectedYear;

  const currentMonth = month;
  const currentYear = Number(year);

  const nextMonthObj = addMonths(currentMonth, currentYear, 1);
  const nextToNextMonthObj = addMonths(currentMonth, currentYear, 2);

  const nextMonth = nextMonthObj.month;
  const nextMonthYear = nextMonthObj.year;
  const nextToNextMonth = nextToNextMonthObj.month;
  const nextToNextMonthYear = nextToNextMonthObj.year;

  const [data, setData] = useState<RowData[] | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [chartData, setChartData] = useState<ChartDataItem[]>([]);

  const [showCm1, setshowCm1] = useState<boolean>(false);
  const [LosSalesUnits, setLosSalesUnits] = useState<boolean>(false);
  const [noDataAvailable, setNoDataAvailable] = useState<boolean>(false);

  const DUMMY_PNL_ROWS: RowData[] = [
    {
      sku: "SKU-DEMO-1",
      product_name: "Demo Product A",
      units_1st: 0,
      units_2nd: 0,
      units_3rd: 0,
      units_sum: 0,
      Total_Sales_1st: 0,
      profit_1st: 0,
      profit_percentage_1st: 0,
      Total_Sales_2nd: 0,
      profit_2nd: 0,
      profit_percentage_2nd: 0,
      Total_Sales_3rd: 0,
      profit_3rd: 0,
      profit_percentage_3rd: 0,
      Total_Sales_sum: 0,
      profit_sum: 0,
    },
    {
      sku: "SKU-DEMO-2",
      product_name: "Demo Product B",
      units_1st: 0,
      units_2nd: 0,
      units_3rd: 0,
      units_sum: 0,
      Total_Sales_1st: 0,
      profit_1st: 0,
      profit_percentage_1st: 0,
      Total_Sales_2nd: 0,
      profit_2nd: 0,
      profit_percentage_2nd: 0,
      Total_Sales_3rd: 0,
      profit_3rd: 0,
      profit_percentage_3rd: 0,
      Total_Sales_sum: 0,
      profit_sum: 0,
    },
    {
      sku: "SKU-DEMO-3",
      product_name: "Demo Product C",
      units_1st: 0,
      units_2nd: 0,
      units_3rd: 0,
      units_sum: 0,
      Total_Sales_1st: 0,
      profit_1st: 0,
      profit_percentage_1st: 0,
      Total_Sales_2nd: 0,
      profit_2nd: 0,
      profit_percentage_2nd: 0,
      Total_Sales_3rd: 0,
      profit_3rd: 0,
      profit_percentage_3rd: 0,
      Total_Sales_sum: 0,
      profit_sum: 0,
    },
    {
      sku: "SKU-DEMO-4",
      product_name: "Demo Product D",
      units_1st: 0,
      units_2nd: 0,
      units_3rd: 0,
      units_sum: 0,
      Total_Sales_1st: 0,
      profit_1st: 0,
      profit_percentage_1st: 0,
      Total_Sales_2nd: 0,
      profit_2nd: 0,
      profit_percentage_2nd: 0,
      Total_Sales_3rd: 0,
      profit_3rd: 0,
      profit_percentage_3rd: 0,
      Total_Sales_sum: 0,
      profit_sum: 0,
    },
    {
      sku: "SKU-DEMO-5",
      product_name: "Demo Product E",
      units_1st: 0,
      units_2nd: 0,
      units_3rd: 0,
      units_sum: 0,
      Total_Sales_1st: 0,
      profit_1st: 0,
      profit_percentage_1st: 0,
      Total_Sales_2nd: 0,
      profit_2nd: 0,
      profit_percentage_2nd: 0,
      Total_Sales_3rd: 0,
      profit_3rd: 0,
      profit_percentage_3rd: 0,
      Total_Sales_sum: 0,
      profit_sum: 0,
    },
    {
      sku: "Total",
      product_name: "Total",
      units_1st: 0,
      units_2nd: 0,
      units_3rd: 0,
      units_sum: 0,
      Total_Sales_1st: 0,
      profit_1st: 0,
      profit_percentage_1st: 0,
      Total_Sales_2nd: 0,
      profit_2nd: 0,
      profit_percentage_2nd: 0,
      Total_Sales_3rd: 0,
      profit_3rd: 0,
      profit_percentage_3rd: 0,
      Total_Sales_sum: 0,
      profit_sum: 0,
      profit_percentage_sum: 0,
    },
  ];

  const DUMMY_PNL_CHART: ChartDataItem[] = [
    {
      month: 'Dec 25',
      SALES: 0,
      'ADVERTISING COSTS': 0,
      'CM1 PROFIT': 0,
      'CM2 PROFIT': 0,
      isHistorical: true,
    },
    {
      month: 'Jan 26',
      SALES: 0,
      'ADVERTISING COSTS': 0,
      'CM1 PROFIT': 0,
      'CM2 PROFIT': 0,
      isForecast: true,
    },
    {
      month: 'Feb 26',
      SALES: 0,
      'ADVERTISING COSTS': 0,
      'CM1 PROFIT': 0,
      'CM2 PROFIT': 0,
      isForecast: true,
    },
    {
      month: 'Mar 26',
      SALES: 0,
      'ADVERTISING COSTS': 0,
      'CM1 PROFIT': 0,
      'CM2 PROFIT': 0,
      isForecast: true,
    },
  ];

  const chartRef = useRef<any>(null);

  const [selectedColumns, setSelectedColumns] = useState<string[]>(() => [
    'sku',
    'product_name',
    LosSalesUnits && 'forecast_1st',
    'Total_Sales_1st',
    'profit_1st',
    showCm1 && 'profit_percentage_1st',
    LosSalesUnits && 'forecast_2nd',
    'Total_Sales_2nd',
    'profit_2nd',
    showCm1 && 'profit_percentage_2nd',
    LosSalesUnits && 'forecast_3rd',
    'Total_Sales_3rd',
    'profit_3rd',
    showCm1 && 'profit_percentage_3rd',
    LosSalesUnits && 'forecast_sum',
    'Total_Sales_sum',
    'profit_sum',
    showCm1 && 'profit_percentage_sum',
  ].filter(Boolean) as string[]);

  useEffect(() => {
    setSelectedColumns([
      'sku',
      'product_name',
      LosSalesUnits && 'forecast_1st',
      'Total_Sales_1st',
      'profit_1st',
      showCm1 && 'profit_percentage_1st',
      LosSalesUnits && 'forecast_2nd',
      'Total_Sales_2nd',
      'profit_2nd',
      showCm1 && 'profit_percentage_2nd',
      LosSalesUnits && 'forecast_3rd',
      'Total_Sales_3rd',
      'profit_3rd',
      showCm1 && 'profit_percentage_3rd',
      LosSalesUnits && 'forecast_sum',
      'Total_Sales_sum',
      'profit_sum',
      showCm1 && 'profit_percentage_sum',
    ].filter(Boolean) as string[]);
  }, [LosSalesUnits, showCm1]);

  const getChartPngWithWhiteBg = (): string | null => {
    const chartInstance = chartRef.current;
    if (!chartInstance) return null;

    const sourceCanvas = chartInstance.canvas || chartInstance.ctx?.canvas;
    if (!sourceCanvas) return null;

    const exportCanvas = document.createElement('canvas');
    exportCanvas.width = sourceCanvas.width;
    exportCanvas.height = sourceCanvas.height;

    const ctx = exportCanvas.getContext('2d');
    if (!ctx) return null;

    ctx.fillStyle = '#ffffff';
    ctx.fillRect(0, 0, exportCanvas.width, exportCanvas.height);
    ctx.drawImage(sourceCanvas, 0, 0);

    return exportCanvas.toDataURL('image/png');
  };

  const [selectedGraphs, setSelectedGraphs] = useState<SelectedGraphs>({
    SALES: true,
    COGS: true,
    'AMAZON EXPENSE': true,
    'ADVERTISING COSTS': true,
    'CM1 PROFIT': true,
    'CM2 PROFIT': true,
  });

  const handleCheckboxChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, checked } = e.target;
    setSelectedGraphs((prev) => ({ ...prev, [name]: checked }));
  };

  const fetchPreviousMonthsData = async (): Promise<ChartDataItem[]> => {
    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) return [];

    try {
      const previousMonths: ChartDataItem[] = [];
      const base = new Date(`${month} 1, ${year}`);
      base.setMonth(base.getMonth() - 6);

      for (let i = 0; i < 6; i++) {
        const monthName = base.toLocaleString('default', { month: 'long' });
        const yearValue = base.getFullYear();

        try {
          const response = await fetch(
            `${process.env.NEXT_PUBLIC_API_BASE_URL}/api/Pnlforecast/previous_months?month=${monthName}&year=${yearValue}&country=${countryName}&period_type=monthly`,
            {
              method: 'GET',
              headers: {
                Authorization: `Bearer ${token}`,
                'Content-Type': 'application/json',
              },
            }
          );

          if (response.ok) {
            const result = await response.json();
            if (result.data && result.totals) {
              previousMonths.push({
                month: `${monthName} ${yearValue}`,
                SALES: roundNumber(result.totals.net_sales_total),
                COGS: roundNumber(result.totals.cost_of_unit_sold_total),
                'AMAZON EXPENSE': roundNumber(result.totals.amazon_fee_total),
                'CM1 PROFIT': roundNumber(result.totals.profit_total),
                'ADVERTISING COSTS': roundNumber(result.totals.advertising_total),
                'CM2 PROFIT': roundNumber(result.totals.cm2_profit_total),
                isHistorical: true,
              });
            }
          }
        } catch { }

        base.setMonth(base.getMonth() + 1);
      }

      return previousMonths;
    } catch {
      return [];
    }
  };

  const prepareChartData = (forecastData: RowData[], previousData: ChartDataItem[] = []): ChartDataItem[] => {
    if (!forecastData || !Array.isArray(forecastData) || forecastData.length === 0) {
      return [];
    }

    const totalRow = forecastData.find(
      (row) =>
        String(row?.sku || '').trim().toLowerCase() === 'total' ||
        String(row?.product_name || '').trim().toLowerCase() === 'total'
    );

    if (!totalRow) {
      return [];
    }

    const cogs3 = (totalRow.Total_Sales_3rd || 0) - (totalRow.profit_3rd || 0);

    const getMetricValue = (sku: string, defaultValue: number = 0): number => {
      const row = forecastData.find((r) => r.sku === sku);
      return row ? (Number(row.value) || defaultValue) : defaultValue;
    };

    const forecastChartData: ChartDataItem[] = [
      {
        month: `${currentMonth} ${currentYear}`,
        SALES: roundNumber(totalRow.Total_Sales_1st),
        'ADVERTISING COSTS': roundNumber(getMetricValue('advertising_total1')),
        'CM1 PROFIT': roundNumber(totalRow.profit_1st),
        'CM2 PROFIT': roundNumber(getMetricValue('cm2profit1')),
        isForecast: false,
      },
      {
        month: `${nextMonth} ${nextMonthYear}`,
        SALES: roundNumber(totalRow.Total_Sales_2nd),
        'ADVERTISING COSTS': roundNumber(getMetricValue('advertising_total2')),
        'CM1 PROFIT': roundNumber(totalRow.profit_2nd),
        'CM2 PROFIT': roundNumber(getMetricValue('cm2profit2')),
        isForecast: true,
      },
      {
        month: `${nextToNextMonth} ${nextToNextMonthYear}`,
        SALES: roundNumber(totalRow.Total_Sales_3rd),
        COGS: roundNumber(Math.abs(cogs3)),
        'AMAZON EXPENSE': roundNumber(Math.abs(getMetricValue('Platform_Fees3'))),
        'ADVERTISING COSTS': roundNumber(getMetricValue('advertising_total3')),
        'CM1 PROFIT': roundNumber(totalRow.profit_3rd),
        'CM2 PROFIT': roundNumber(getMetricValue('cm2profit3')),
        isForecast: true,
      },
    ];

    return [...previousData, ...forecastChartData];
  };

  const parseExcelResponse = async (response: Response): Promise<RowData[]> => {
    const blob = await response.blob();
    const buffer = await blob.arrayBuffer();
    const workbook = XLSX.read(new Uint8Array(buffer), { type: 'array' });
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    return XLSX.utils.sheet_to_json<RowData>(sheet);
  };

  const generateCountryPnlIfMissing = async (country: 'uk' | 'us', token: string) => {
    const res = await fetch(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/api/Pnlforecast?country=${country}&month=${month}&year=${year}`,
      {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
      }
    );

    return res.ok;
  };

  useEffect(() => {
    if (isDemoMode) {
      setData(DUMMY_PNL_ROWS);
      setChartData(DUMMY_PNL_CHART);
      setLoading(false);
      setError(null);
      return;
    }

    const fetchForecastData = async () => {
      const token =
        typeof window !== 'undefined'
          ? localStorage.getItem('jwtToken')
          : null;

      if (!token) {
        setError('Authorization token is missing');
        setLoading(false);
        return;
      }

      try {
        setNoDataAvailable(false);
        setError(null);

        const previousData = await fetchPreviousMonthsData();

        if (countryName.toLowerCase() === 'global') {
          let response = await fetch(
            `${process.env.NEXT_PUBLIC_API_BASE_URL}/api/Pnlforecast/global?month=${month}&year=${year}`,
            {
              method: 'GET',
              headers: {
                Authorization: `Bearer ${token}`,
                'Content-Type': 'application/json',
              },
            }
          );

          if (!response.ok && response.status === 404) {
            await generateCountryPnlIfMissing('uk', token);
            await generateCountryPnlIfMissing('us', token);

            response = await fetch(
              `${process.env.NEXT_PUBLIC_API_BASE_URL}/api/Pnlforecast/global?month=${month}&year=${year}`,
              {
                method: 'GET',
                headers: {
                  Authorization: `Bearer ${token}`,
                  'Content-Type': 'application/json',
                },
              }
            );
          }

          if (!response.ok) {
            setData([]);
            setChartData([]);
            setNoDataAvailable(true);
            setLoading(false);
            return;
          }

          const contentType = response.headers.get('Content-Type') || '';

          if (
            contentType.startsWith(
              'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
          ) {
            const jsonData = await parseExcelResponse(response);

            if (!jsonData || jsonData.length === 0) {
              setData([]);
              setChartData([]);
              setNoDataAvailable(true);
            } else {
              const preparedChartData = prepareChartData(jsonData, previousData);
              setData(jsonData);
              setChartData(preparedChartData);
              setNoDataAvailable(preparedChartData.length === 0 && jsonData.length === 0);
            }

            setLoading(false);
            return;
          }

          if (contentType.includes('application/json')) {
            const json = (await response.json()) as RowData[];

            if (Array.isArray(json) && json.length > 0) {
              const preparedChartData = prepareChartData(json, previousData);
              setData(json);
              setChartData(preparedChartData);
              setNoDataAvailable(preparedChartData.length === 0 && json.length === 0);
            } else {
              setData([]);
              setChartData([]);
              setNoDataAvailable(true);
            }
          }

          setLoading(false);
          return;
        }

        const endpoint = `${process.env.NEXT_PUBLIC_API_BASE_URL}/api/Pnlforecast?country=${countryName}&month=${month}&year=${year}`;

        const response = await fetch(endpoint, {
          method: 'GET',
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json',
          },
        });

        if (!response.ok) {
          setData([]);
          setChartData([]);
          setNoDataAvailable(true);
          setLoading(false);
          return;
        }

        const contentType = response.headers.get('Content-Type') || '';

        if (
          contentType.startsWith(
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
          )
        ) {
          const jsonData = await parseExcelResponse(response);

          if (!jsonData || jsonData.length === 0) {
            setData([]);
            setChartData([]);
            setNoDataAvailable(true);
          } else {
            const preparedChartData = prepareChartData(jsonData, previousData);
            setData(jsonData);
            setChartData(preparedChartData);
            setNoDataAvailable(preparedChartData.length === 0 && jsonData.length === 0);
          }

          setLoading(false);
          return;
        }

        if (contentType.includes('application/json')) {
          const json = (await response.json()) as RowData[];

          if (Array.isArray(json) && json.length > 0) {
            const preparedChartData = prepareChartData(json, previousData);
            setData(json);
            setChartData(preparedChartData);
            setNoDataAvailable(preparedChartData.length === 0 && json.length === 0);
          } else {
            setData([]);
            setChartData([]);
            setNoDataAvailable(true);
          }
        }
      } catch (err: any) {
        setData([]);
        setChartData([]);
        setNoDataAvailable(true);
        setError(null);
      } finally {
        setLoading(false);
      }
    };

    fetchForecastData();
  }, [countryName, month, year, isDemoMode]);

  useEffect(() => {
    if (!data || data.length === 0 || isDemoMode || noDataAvailable) return;
    uploadTableToBackend();
  }, [data, isDemoMode, noDataAvailable]);

  const uploadTableToBackend = async () => {
    const table = document.querySelector('.tablec') as HTMLTableElement | null;
    if (!table) return;

    const workbook = XLSX.utils.book_new();
    const worksheet = XLSX.utils.table_to_sheet(table, { raw: true });
    XLSX.utils.book_append_sheet(workbook, worksheet, 'P&L Forecast');
    const excelBuffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
    const excelBlob = new Blob([excelBuffer], { type: 'application/octet-stream' });

    const formData = new FormData();
    formData.append('file', excelBlob, 'PNL_Forecast.xlsx');
    formData.append('month', month);
    formData.append('year', year);
    formData.append('country', countryName);

    try {
      await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/api/save_pnl_forecast`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${localStorage.getItem('jwtToken')}` },
        body: formData,
      });
    } catch { }
  };



  const handleDownload = async () => {
    const dataUrl = getChartPngWithWhiteBg();

    await exportPnLForecastExcel({
      filename: "PNL_Forecast_With_Chart.xlsx",
      titleLine: `P&L Forecast - ${effectiveCountry.toUpperCase()} (${formatMonthYear(currentMonth, currentYear)} to ${formatMonthYear(nextToNextMonth, nextToNextMonthYear)})`,
      titleCountry: effectiveCountry.toUpperCase(),
      platformLabel: "Phormula",
      periodLabel: `${formatMonthYear(currentMonth, currentYear)} to ${formatMonthYear(nextToNextMonth, nextToNextMonthYear)}`,
      companyName,
      brandName,

      currencyLabel: currencySymbol,
      month1Label: `P&L Forecast ${formatMonthYear(currentMonth, currentYear)}`,
      month2Label: `P&L Forecast ${formatMonthYear(nextMonth, nextMonthYear)}`,
      month3Label: `P&L Forecast ${formatMonthYear(nextToNextMonth, nextToNextMonthYear)}`,
      totalLabel: "P&L Forecast for 3 months",

      productRows: excelProductRows || [],
      summaryRows: summaryAsRows || [],
      chartImageBase64: dataUrl,
    });
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
    router.push(`/profile/${countryName}/NA/NA`);
  };

  const monthGroup = (
    id: string,
    label: string,
    u: string,
    s: string,
    p: string,
    pp: string
  ): ColGroup<RowData> => ({
    id,
    label,
    collapsedCols: [
      { key: s, label: `Sales (${currencySymbol})`, align: 'center' },
      { key: p, label: `CM1 (${currencySymbol})`, align: 'center' },
    ],
    expandedCols: [
      { key: u, label: 'Units', align: 'center' },
      { key: s, label: `Sales (${currencySymbol})`, align: 'center' },
      { key: p, label: `CM1 (${currencySymbol})`, align: 'center' },
      { key: pp, label: 'CM1 %', align: 'center' },
    ],
  });

  const leftCols: LeafCol<RowData>[] = [
    {
      key: 'sr_no',
      label: 'S. No.',
      align: 'center',
      thClassName: 'th-center',
      tdClassName: 'td-center',
    },
    {
      key: 'product_name',
      label: 'Product Name',
      align: 'left',
      thClassName: 'th-left',
    },
    {
      key: 'sku',
      label: 'SKU',
      align: 'center',
    },
  ];

  const groups: ColGroup<RowData>[] = [
    monthGroup(
      'm1',
      `P&L Forecast ${formatMonthYear(currentMonth, currentYear)}`,
      'forecast_1st',
      'Total_Sales_1st',
      'profit_1st',
      'profit_percentage_1st'
    ),
    monthGroup(
      'm2',
      `P&L Forecast ${formatMonthYear(nextMonth, nextMonthYear)}`,
      'forecast_2nd',
      'Total_Sales_2nd',
      'profit_2nd',
      'profit_percentage_2nd'
    ),
    monthGroup(
      'm3',
      `P&L Forecast ${formatMonthYear(nextToNextMonth, nextToNextMonthYear)}`,
      'forecast_3rd',
      'Total_Sales_3rd',
      'profit_3rd',
      'profit_percentage_3rd'
    ),
    monthGroup(
      'sum',
      'P&L Forecast for 3 months',
      'forecast_sum',
      'Total_Sales_sum',
      'profit_sum',
      'profit_percentage_sum'
    ),
  ];

  const DUMMY_SUMMARY_ROWS = [
    { label: "Cost of Advertisement", m1: 0, m2: 0, m3: 0, sum: 0 },
    { label: "Platform Fees", m1: 0, m2: 0, m3: 0, sum: 0 },
    { label: "Other Expenses", m1: 0, m2: 0, m3: 0, sum: 0 },
    { label: "CM2 Profit/Loss", m1: 0, m2: 0, m3: 0, sum: 0 },
    { label: "Net Reimbursement (Projected)", m1: 0, m2: 0, m3: 0, sum: 0 },
    { label: "Reimbursement vs CM2 Margins", m1: 0, m2: 0, m3: 0, sum: 0 },
  ];

  const summaryRows = isDemoMode
    ? DUMMY_SUMMARY_ROWS
    : [
      {
        label: 'Cost of Advertisement',
        m1: data?.find(r => r.sku === 'advertising_total1')?.value,
        m2: data?.find(r => r.sku === 'advertising_total2')?.value,
        m3: data?.find(r => r.sku === 'advertising_total3')?.value,
        sum: data?.find(r => r.sku === 'advertising_total')?.value,
      },
      {
        label: 'Platform Fees',
        m1: data?.find(r => r.sku === 'Platform_Fees1')?.value,
        m2: data?.find(r => r.sku === 'Platform_Fees2')?.value,
        m3: data?.find(r => r.sku === 'Platform_Fees3')?.value,
        sum: data?.find(r => r.sku === 'platform_fees_total')?.value,
      },
      {
        label: 'Other Expenses',
        m1:
          (data?.find(r => r.sku === 'Platform_Fees1')?.value || 0) +
          (data?.find(r => r.sku === 'advertising_total1')?.value || 0),
        m2:
          (data?.find(r => r.sku === 'Platform_Fees2')?.value || 0) +
          (data?.find(r => r.sku === 'advertising_total2')?.value || 0),
        m3:
          (data?.find(r => r.sku === 'Platform_Fees3')?.value || 0) +
          (data?.find(r => r.sku === 'advertising_total3')?.value || 0),
        sum:
          (data?.find(r => r.sku === 'platform_fees_total')?.value || 0) +
          (data?.find(r => r.sku === 'advertising_total')?.value || 0),
      },
      {
        label: 'CM2 Profit/Loss',
        m1: data?.find(r => r.sku === 'cm2profit1')?.value,
        m2: data?.find(r => r.sku === 'cm2profit2')?.value,
        m3: data?.find(r => r.sku === 'cm2profit3')?.value,
        sum: data?.find(r => r.sku === 'cm2profit_total')?.value,
      },
      {
        label: 'Net Reimbursement (Projected)',
        m1: data?.find(r => r.sku === 'NetReimbursement1')?.value,
        m2: data?.find(r => r.sku === 'NetReimbursement2')?.value,
        m3: data?.find(r => r.sku === 'NetReimbursement3')?.value,
        sum: data?.find(r => r.sku === 'NetReimbursement_total')?.value,
      },
      {
        label: 'Reimbursement vs CM2 Margins',
        m1: data?.find(r => r.sku === 'ReimbursementvsCM2Margins1')?.value,
        m2: data?.find(r => r.sku === 'ReimbursementvsCM2Margins2')?.value,
        m3: data?.find(r => r.sku === 'ReimbursementvsCM2Margins3')?.value,
        sum: data?.find(r => r.sku === 'ReimbursementvsCM2Margins_total')?.value,
      },
    ];

  const productRows = data?.filter(
    (row) =>
      row.sku &&
      ![
        'acos1',
        'acos2',
        'acos3',
        'Platform_Fees1',
        'Platform_Fees2',
        'Platform_Fees3',
        'advertising_total1',
        'advertising_total2',
        'advertising_total3',
        'cm2profit1',
        'cm2profit2',
        'cm2profit3',
        'NetReimbursement1',
        'NetReimbursement2',
        'NetReimbursement3',
        'ReimbursementvsCM2Margins1',
        'ReimbursementvsCM2Margins2',
        'ReimbursementvsCM2Margins3',
        'Reimbursementvssales1',
        'Reimbursementvssales2',
        'Reimbursementvssales3',
        'cm2margin1',
        'cm2margin2',
        'cm2margin3',
        'platform_fees_total',
        'advertising_total',
        'cm2profit_total',
        'cm2margin_total',
        'acos_total',
        'NetReimbursement_total',
        'ReimbursementvsCM2Margins_total',
        'Reimbursementvssales_total',
      ].includes(row.sku)
  );

  const summaryAsRows: RowData[] = summaryRows.map(r => ({
    product_name: r.label,
    sku: '',
    forecast_1st: '',
    Total_Sales_1st: r.m1 ?? '',
    profit_1st: '',
    profit_percentage_1st: '',
    forecast_2nd: '',
    Total_Sales_2nd: r.m2 ?? '',
    profit_2nd: '',
    profit_percentage_2nd: '',
    forecast_3rd: '',
    Total_Sales_3rd: r.m3 ?? '',
    profit_3rd: '',
    profit_percentage_3rd: '',
    forecast_sum: '',
    Total_Sales_sum: r.sum ?? '',
    profit_sum: '',
    profit_percentage_sum: '',
  }));

  const normalizedProductRows = React.useMemo(() => {
    const rows = productRows || [];

    const nonTotalRows = rows
      .filter((row) => !isPnlTotalRow(row))
      .map((row) => recomputePnlPercentages(row));

    const totalRow =
      nonTotalRows.length > 0
        ? buildAggregatePnlRow(nonTotalRows, 'Total')
        : null;

    return totalRow
      ? [...nonTotalRows, { ...totalRow, sku: '' }]
      : nonTotalRows;
  }, [productRows]);

  const excelProductRows = React.useMemo(() => {
    const rows = normalizedProductRows || [];

    const totalRow = rows.find((row) => isPnlTotalRow(row)) || null;

    const nonTotalRows = rows.filter((row) => !isPnlTotalRow(row));

    return totalRow ? [...nonTotalRows, totalRow] : nonTotalRows;
  }, [normalizedProductRows]);

  const displayProductRows = React.useMemo(() => {
    const rows = normalizedProductRows || [];

    const totalRow =
      rows.find((row) => isPnlTotalRow(row)) || null;

    const nonTotalRows = rows.filter((row) => !isPnlTotalRow(row));

    if (nonTotalRows.length <= 9) {
      return totalRow ? [...nonTotalRows, totalRow] : nonTotalRows;
    }

    const firstNine = nonTotalRows.slice(0, 9);
    const remainingRows = nonTotalRows.slice(9);
    const othersRow = buildOthersPnlRow(remainingRows);

    return totalRow
      ? [...firstNine, othersRow, totalRow]
      : [...firstNine, othersRow];
  }, [normalizedProductRows]);

  const tableRows = [
    ...displayProductRows,
    ...summaryAsRows,
  ];

  return (
    <div className="flex flex-col gap-6">
      <div className="flex justify-between items-center gap-4">
        <div className="flex items-center justify-between w-full gap-3">
          <div className="flex flex-col leading-tight">
            <div className="flex items-baseline gap-2">
              {/* <PageBreadcrumb
                pageTitle="P&L Forecast - Amazon"
                variant="page"
                align="left"
                textSize="2xl"
              /> */}
              <PageBreadcrumb
                variant="page"
                align="left"
                textSize="2xl"
                pageTitle={
                  <div className="flex flex-wrap items-baseline gap-2">
                    <span className="text-[#414042] font-bold">
                      P&L Forecast - Amazon
                    </span>
                    <span className="text-green-500 font-bold">
                      {countryName?.toUpperCase()}
                    </span>
                  </div>
                }
              />
            </div>
            <p className="text-xs 2xl:text-sm text-charcoal-500 mt-1">
              Historical Data vs Forecasted Trends
            </p>
          </div>

          <div className="flex items-center gap-3">
            <DownloadIconButton
              onClick={handleDownload}
              disabled={isDemoMode}
            />
          </div>
        </div>
      </div>

      {loading && (
        <div className="flex flex-col items-center justify-center py-12 text-center">
          <Loader fullscreen transparent />
        </div>
      )}

      {error && !isDemoMode && (
        <div className="rounded-xl border border-red-200 bg-red-50 shadow-sm p-4 text-sm text-red-600">
          {error}
        </div>
      )}
      <PreviewLockedSection
        enabled={isDemoMode}
        title="Preview Mode"
        description="To view your real business data and analytics, please complete your profile and connect your Amazon account. This will unlock your performance dashboard and insights."
        buttonText="Complete Setup"
        onAction={handleConnectAmazonPreview}
      >
        <div className="rounded-xl border border-slate-200 bg-white shadow-sm p-3">
          <div className="flex flex-col gap-4">
            <div className="mt-2">
              {noDataAvailable ? (
                <div className="flex min-h-[320px] items-center justify-center rounded-lg text-sm text-slate-500">
                  No data available
                </div>
              ) : (
                <PnlForecastChart
                  ref={chartRef}
                  chartData={chartData}
                  currencySymbol={currencySymbol}
                  selectedGraphs={selectedGraphs}
                  handleCheckboxChange={handleCheckboxChange}
                />
              )}
            </div>
          </div>
        </div>

        <div className="rounded-xl border border-slate-200 bg-white shadow-sm p-3 mt-2 2xl:mt-4">
          <PageBreadcrumb
            pageTitle="Detailed P&L Forecast Data"
            variant="page"
            align="left"
            textSize="2xl"
          />

          <div className="mt-4 w-full overflow-x-auto">
            <div className="rounded-xl border border-gray-300 overflow-auto min-w-[1100px]">
              <div className="w-full text-xs 2xl:text-sm text-[#414042]">
                <GroupedCollapsibleTables<RowData>
                  rows={noDataAvailable ? [] : tableRows}
                  getRowKey={(r, idx) =>
                    r.sku && r.sku !== "" ? r.sku : `row-${idx}`
                  }
                  leftCols={leftCols}
                  groups={groups}
                  singleCols={[]}
                  getValue={(row, key) => {
                    if (key === "sr_no") {
                      const isTotal =
                        String(row.product_name || '').trim().toLowerCase() === 'total';

                      const isSummary = summaryRows.some(
                        (s) => s.label === row.product_name
                      );

                      if (isTotal || isSummary) return "";

                      const productIndex = displayProductRows.findIndex((r) => r === row);
                      return productIndex >= 0 ? productIndex + 1 : "";
                    }

                    return formatCellValue(key, row[key]);
                  }}
                  getRowClassName={(row) => {
                    if (row.product_name === "Total") {
                      return "bg-[#EFEFEF] font-semibold";
                    }
                    if (summaryRows.some(s => s.label === row.product_name)) {
                      return "bg-white";
                    }
                    return "bg-white";
                  }}
                />

                {noDataAvailable && (
                  <div className="w-full text-center py-6 text-sm text-gray-500 font-medium">
                    No Data Available for selected period
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </PreviewLockedSection>
    </div>
  );
};

export default Pnlforecast;