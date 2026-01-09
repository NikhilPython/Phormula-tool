'use client';

import React, { useState, useEffect } from 'react';
import * as XLSX from 'xlsx';
import { useParams, useRouter } from 'next/navigation';
import './Styles.css';
import PnlForecastChart from '@/components/pnlforecast/PnlForecastChart';
import { ChevronLeft, ChevronRight } from "lucide-react";
import GroupedCollapsibleTable, {
  ColGroup,
  LeafCol,
} from "@/components/ui/table/GroupedCollapsibleTable";
import { IoDownload } from "react-icons/io5";
import ExcelJS from "exceljs";
import { saveAs } from "file-saver";
import { useRef } from "react";



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
  if (val === null || val === undefined || val === '' || isNaN(Number(val))) return 'N/A';
  return Math.abs(Number(val)).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
};

const getPreviousMonthYear = (month: string, year: string) => {
  const date = new Date(`${month} 1, ${year}`);
  date.setMonth(date.getMonth() - 1);

  return {
    month: date.toLocaleString("default", { month: "long" }),
    year: date.getFullYear().toString(),
  };
};


const formatPercent = (val: any): string => {
  if (val === null || val === undefined || val === '' || isNaN(Number(val))) return '';
  return `${Number(val).toFixed(2)}%`;
};

const formatCellValue = (key: string, value: any): string => {
  if (value === null || value === undefined || value === '') return '';

  const rawKeys = ['forecast_sum', 'forecast_1st', 'forecast_2nd', 'forecast_3rd'];
  if (rawKeys.includes(key)) return value;

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

  return typeof value === 'number' || !isNaN(Number(value)) ? formatNumber(value) : value;
};

const Pnlforecast: React.FC = () => {
  const router = useRouter();
  const params = useParams();
  const countryName = (params?.countryName as string) || '';
  const urlMonth = (params?.month as string) || '';
const urlYear = (params?.year as string) || '';
  const currencySymbol = getCurrencySymbol(countryName);
  const { month, year } = getPreviousMonthYear(urlMonth, urlYear);

  const [data, setData] = useState<RowData[] | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [chartData, setChartData] = useState<ChartDataItem[]>([]);
  const [showTacosSection, setShowTacosSection] = useState<boolean>(false);
  const [showCm1, setshowCm1] = useState<boolean>(false);
  const [showamazonfee, setshowamazonfee] = useState<boolean>(false);
  const [LosSalesUnits, setLosSalesUnits] = useState<boolean>(false);
  const [isExpanded, setIsExpanded] = useState<boolean>(true);

  const toggleTacosSection = () => setShowTacosSection((p) => !p);
  const handleshowCm1click = () => setshowCm1((p) => !p);
  const handleAmazonFeeClick = () => setshowamazonfee((p) => !p);
  const handleLosSalesUnitsclick = () => setLosSalesUnits((p) => !p);

  const chartRef = useRef<any>(null);

  const [selectedColumns, setSelectedColumns] = useState<string[]>(() => ([
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
  ].filter(Boolean) as string[]));

  useEffect(() => {
    setSelectedColumns(([
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
    ].filter(Boolean) as string[]));
  }, [LosSalesUnits, showCm1]);

  const getChartPngWithWhiteBg = (): string | null => {
  const chartInstance = chartRef.current;
  if (!chartInstance) return null;

  const sourceCanvas =
    chartInstance.canvas || chartInstance.ctx?.canvas;
  if (!sourceCanvas) return null;

  const exportCanvas = document.createElement("canvas");
  exportCanvas.width = sourceCanvas.width;
  exportCanvas.height = sourceCanvas.height;

  const ctx = exportCanvas.getContext("2d");
  if (!ctx) return null;

  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, exportCanvas.width, exportCanvas.height);
  ctx.drawImage(sourceCanvas, 0, 0);

  return exportCanvas.toDataURL("image/png");
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

  const currentDate = new Date();
  const currentMonth = currentDate.toLocaleString('default', { month: 'long' });
  const currentYear = currentDate.getFullYear();

  const nextMonthDate = new Date(currentDate);
  nextMonthDate.setDate(1);
  nextMonthDate.setMonth(currentDate.getMonth() + 1);
  const nextMonth = nextMonthDate.toLocaleString('default', { month: 'long' });
  const nextMonthYear = nextMonthDate.getFullYear();

  const nextToNextMonthDate = new Date(currentDate);
  nextToNextMonthDate.setDate(1);
  nextToNextMonthDate.setMonth(currentDate.getMonth() + 2);
  const nextToNextMonth = nextToNextMonthDate.toLocaleString('default', { month: 'long' });
  const nextToNextMonthYear = nextToNextMonthDate.getFullYear();

  const formatMonthYear = (monthName: string, yearVal: number) => {
    const date = new Date(`${monthName} 1, ${yearVal}`);
    return date.toLocaleString('en-US', { month: 'short' }) + `'` + String(yearVal).slice(-2);
  };

  const fetchPreviousMonthsData = async (): Promise<ChartDataItem[]> => {
    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) return [];
    try {
      const previousMonths: ChartDataItem[] = [];
      const now = new Date();
      let date = new Date(now.getFullYear(), now.getMonth(), 1);
      date.setMonth(date.getMonth() - 6);
      for (let i = 0; i < 6; i++) {
        const monthName = date.toLocaleString('default', { month: 'long' });
        const yearValue = date.getFullYear();
        try {
          const response = await fetch(
            `http://127.0.0.1:5000/api/Pnlforecast/previous_months?month=${monthName}&year=${yearValue}&country=${countryName}&period_type=monthly`,
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
                SALES: result.totals.net_sales_total || 0,
                COGS: result.totals.cost_of_unit_sold_total || 0,
                'AMAZON EXPENSE': result.totals.amazon_fee_total || 0,
                'CM1 PROFIT': result.totals.profit_total || 0,
                'ADVERTISING COSTS': result.totals.advertising_total || 0,
                'CM2 PROFIT': result.totals.cm2_profit_total || 0,
                isHistorical: true,
              });
            }
          }
        } catch {}
        date.setMonth(date.getMonth() + 1);
      }
      return previousMonths;
    } catch {
      return [];
    }
  };

  const prepareChartData = (forecastData: RowData[], previousData: ChartDataItem[] = []): ChartDataItem[] => {
    if (!forecastData || !Array.isArray(forecastData)) return previousData;
    const totalRow = forecastData.find((row) => row.sku === 'Total');
    if (!totalRow) return previousData;

    const cogs3 = (totalRow.Total_Sales_3rd || 0) - (totalRow.profit_3rd || 0);

    const getMetricValue = (sku: string, defaultValue: number = 0): number => {
      const row = forecastData.find((r) => r.sku === sku);
      return row ? (Number(row.value) || defaultValue) : defaultValue;
    };

    const forecastChartData: ChartDataItem[] = [
      {
        month: `${currentMonth} ${currentYear}`,
        SALES: totalRow.Total_Sales_1st || 0,
        'ADVERTISING COSTS': getMetricValue('advertising_total1'),
        'CM1 PROFIT': totalRow.profit_1st || 0,
        'CM2 PROFIT': getMetricValue('cm2profit1'),
        isForecast: false,
      },
      {
        month: `${nextMonth} ${nextMonthYear}`,
        SALES: totalRow.Total_Sales_2nd || 0,
        'ADVERTISING COSTS': getMetricValue('advertising_total2'),
        'CM1 PROFIT': totalRow.profit_2nd || 0,
        'CM2 PROFIT': getMetricValue('cm2profit2'),
        isForecast: true,
      },
      {
        month: `${nextToNextMonth} ${nextToNextMonthYear}`,
        SALES: totalRow.Total_Sales_3rd || 0,
        COGS: Math.abs(cogs3),
        'AMAZON EXPENSE': Math.abs(getMetricValue('Platform_Fees3')),
        'ADVERTISING COSTS': getMetricValue('advertising_total3'),
        'CM1 PROFIT': totalRow.profit_3rd || 0,
        'CM2 PROFIT': getMetricValue('cm2profit3'),
        isForecast: true,
      },
    ];
    return [...previousData, ...forecastChartData];
  };

  useEffect(() => {
    const fetchForecastData = async () => {
      const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
      if (!token) {
        setError('Authorization token is missing');
        setLoading(false);
        return;
      }
      try {
        const previousData = await fetchPreviousMonthsData();
        const endpoint =
          countryName.toLowerCase() === 'global'
            ? `http://127.0.0.1:5000/api/Pnlforecast/global?month=${month}&year=${year}`
            : `http://127.0.0.1:5000/api/Pnlforecast?country=${countryName}&month=${month}&year=${year}`;

        const response = await fetch(endpoint, {
          method: 'GET',
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json',
          },
        });

        if (!response.ok) {
          if (response.status === 404) {
            setError('You need to load Inventory forecast first to load PnL forecast');
          } else {
            setError(`Error fetching data: ${response.statusText}`);
          }
          setLoading(false);
          return;
        }

        const contentType = response.headers.get('Content-Type') || '';
        if (contentType.startsWith('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')) {
          const blob = await response.blob();
          const reader = new FileReader();
          reader.onload = async (e: ProgressEvent<FileReader>) => {
            if (!e.target) return;
            const arr = new Uint8Array(e.target.result as ArrayBuffer);
            const workbook = XLSX.read(arr, { type: 'array' });
            const sheetName = workbook.SheetNames[0];
            const sheet = workbook.Sheets[sheetName];
            const jsonData = XLSX.utils.sheet_to_json<RowData>(sheet);
            if (jsonData.length === 0) {
              setError('Empty table found in the Excel file at the specified location');
            } else {
              setData(jsonData);
              setChartData(prepareChartData(jsonData, previousData));
            }
          };
          reader.readAsArrayBuffer(blob);
          return;
        }

        if (contentType.includes('application/json')) {
          const json = (await response.json()) as RowData[];
          if (json && Array.isArray(json)) {
            setData(json);
            setChartData(prepareChartData(json, previousData));

            // Auto-save JSON as Excel to backend
            try {
              const worksheet = XLSX.utils.json_to_sheet(json);
              const workbook = XLSX.utils.book_new();
              XLSX.utils.book_append_sheet(workbook, worksheet, 'PNL Forecast');
              const excelBuffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
              const excelBlob = new Blob([excelBuffer], { type: 'application/octet-stream' });
              const formData = new FormData();
              formData.append('file', excelBlob, 'PNL_Forecast.xlsx');
              await fetch('http://127.0.0.1:5000/api/save_pnl_forecast', {
                method: 'POST',
                headers: { Authorization: `Bearer ${localStorage.getItem('jwtToken')}` },
                body: formData,
              });
            } catch {}
          } else {
            throw new Error('Invalid JSON format');
          }
        } else {
          throw new Error('Expected JSON or Excel response');
        }
      } catch (err: any) {
        setError(err?.message || 'An error occurred while fetching the data');
      } finally {
        setLoading(false);
      }
    };

    fetchForecastData();
  }, [countryName, month, year]);

  useEffect(() => {
    if (data && data.length > 0) {
      uploadTableToBackend();
    }
  }, [data]);

  const formatValue = (sku: string): string => {
    const row = data?.find((r) => r.sku === sku);
    return formatNumber(row?.value || 0);
  };

  const getTotal = (platformFees: string, advertising: string): string => {
    const pfRow = data?.find((row) => row.sku === platformFees);
    const adRow = data?.find((row) => row.sku === advertising);
    const total = (pfRow?.value || 0) + (adRow?.value || 0);
    return formatNumber(total);
  };

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
      await fetch('http://127.0.0.1:5000/api/save_pnl_forecast', {
        method: 'POST',
        headers: { Authorization: `Bearer ${localStorage.getItem('jwtToken')}` },
        body: formData,
      });
    } catch {}
  };

 const exportTableToExcel = async () => {
  const workbook = new ExcelJS.Workbook();

  /* =====================
     SHEET 1: P&L TABLE
     ===================== */
  const tableSheet = workbook.addWorksheet("P&L Forecast");

  tableSheet.addRow([
    "Product Name",
    "SKU",
    "Sales M1",
    "CM1 M1",
    "Sales M2",
    "CM1 M2",
    "Sales M3",
    "CM1 M3",
    "Sales Total",
    "CM1 Total",
  ]).font = { bold: true };

  const rows = [
    ...(productRows || []),
    ...summaryAsRows,
  ];

  rows.forEach(r => {
    tableSheet.addRow([
      r.product_name,
      r.sku,
      r.Total_Sales_1st,
      r.profit_1st,
      r.Total_Sales_2nd,
      r.profit_2nd,
      r.Total_Sales_3rd,
      r.profit_3rd,
      r.Total_Sales_sum,
      r.profit_sum,
    ]);
  });

  tableSheet.columns.forEach(col => col.width = 18);

  /* =====================
     SHEET 2: CHART IMAGE
     ===================== */
  const chartSheet = workbook.addWorksheet("P&L Chart");

  const dataUrl = getChartPngWithWhiteBg();
  if (dataUrl) {
    const base64 = dataUrl.split(",")[1];
const binary = atob(base64);
const buffer = new ArrayBuffer(binary.length);
const view = new Uint8Array(buffer);

for (let i = 0; i < binary.length; i++) {
  view[i] = binary.charCodeAt(i);
}
    const imageId = workbook.addImage({
      buffer,
      extension: "png",
    });

    chartSheet.addImage(imageId, "A1:J25");
  }

  /* =====================
     DOWNLOAD
     ===================== */
  const buf = await workbook.xlsx.writeBuffer();
  saveAs(
    new Blob([buf], {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }),
    "PNL_Forecast_With_Chart.xlsx"
  );
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
    { key: s, label: `Sales (${currencySymbol})`, align: "center" },
    { key: p, label: `CM1 (${currencySymbol})`, align: "center" },
  ],
  expandedCols: [
    { key: u, label: "Units", align: "center" },
    { key: s, label: `Sales (${currencySymbol})`, align: "center" },
    { key: p, label: `CM1 (${currencySymbol})`, align: "center" },
    { key: pp, label: "CM1 %", align: "center" },
  ],
});



const leftCols: LeafCol<RowData>[] = [
  {
    key: "sr_no",
    label: "S. No.",
    align: "center",
    thClassName: "th-center",
    tdClassName: "td-center",
  },
  {
    key: "product_name",
    label: "Product Name",
    align: "left",
    thClassName: "th-left",
  },
  {
    key: "sku",
    label: "SKU",
    align: "center",
  },
];


const groups: ColGroup<RowData>[] = [
  monthGroup(
    "m1",
    `P&L Forecast for ${formatMonthYear(currentMonth, currentYear)}`,
    "forecast_1st",
    "Total_Sales_1st",
    "profit_1st",
    "profit_percentage_1st"
  ),
  monthGroup(
    "m2",
    `P&L Forecast for ${formatMonthYear(nextMonth, nextMonthYear)}`,
    "forecast_2nd",
    "Total_Sales_2nd",
    "profit_2nd",
    "profit_percentage_2nd"
  ),
  monthGroup(
    "m3",
    `P&L Forecast for ${formatMonthYear(nextToNextMonth, nextToNextMonthYear)}`,
    "forecast_3rd",
    "Total_Sales_3rd",
    "profit_3rd",
    "profit_percentage_3rd"
  ),
  monthGroup(
    "sum",
    "P&L Forecast for 3 months",
    "forecast_sum",
    "Total_Sales_sum",
    "profit_sum",
    "profit_percentage_sum"
  ),
];

const summaryRows = [
  {
    label: "Cost of Advertisement",
    m1: data?.find(r => r.sku === "advertising_total1")?.value,
    m2: data?.find(r => r.sku === "advertising_total2")?.value,
    m3: data?.find(r => r.sku === "advertising_total3")?.value,
    sum: data?.find(r => r.sku === "advertising_total")?.value,
  },
  {
    label: "Platform Fees",
    m1: data?.find(r => r.sku === "Platform_Fees1")?.value,
    m2: data?.find(r => r.sku === "Platform_Fees2")?.value,
    m3: data?.find(r => r.sku === "Platform_Fees3")?.value,
    sum: data?.find(r => r.sku === "platform_fees_total")?.value,
  },
  {
    label: "Other Expenses",
    m1:
      (data?.find(r => r.sku === "Platform_Fees1")?.value || 0) +
      (data?.find(r => r.sku === "advertising_total1")?.value || 0),
    m2:
      (data?.find(r => r.sku === "Platform_Fees2")?.value || 0) +
      (data?.find(r => r.sku === "advertising_total2")?.value || 0),
    m3:
      (data?.find(r => r.sku === "Platform_Fees3")?.value || 0) +
      (data?.find(r => r.sku === "advertising_total3")?.value || 0),
    sum:
      (data?.find(r => r.sku === "platform_fees_total")?.value || 0) +
      (data?.find(r => r.sku === "advertising_total")?.value || 0),
  },
  {
    label: "CM2 Profit/Loss",
    m1: data?.find(r => r.sku === "cm2profit1")?.value,
    m2: data?.find(r => r.sku === "cm2profit2")?.value,
    m3: data?.find(r => r.sku === "cm2profit3")?.value,
    sum: data?.find(r => r.sku === "cm2profit_total")?.value,
  },
  {
    label: "Net Reimbursement (Projected)",
    m1: data?.find(r => r.sku === "NetReimbursement1")?.value,
    m2: data?.find(r => r.sku === "NetReimbursement2")?.value,
    m3: data?.find(r => r.sku === "NetReimbursement3")?.value,
    sum: data?.find(r => r.sku === "NetReimbursement_total")?.value,
  },
  {
    label: "Reimbursement vs CM2 Margins",
    m1: data?.find(r => r.sku === "ReimbursementvsCM2Margins1")?.value,
    m2: data?.find(r => r.sku === "ReimbursementvsCM2Margins2")?.value,
    m3: data?.find(r => r.sku === "ReimbursementvsCM2Margins3")?.value,
    sum: data?.find(r => r.sku === "ReimbursementvsCM2Margins_total")?.value,
  },
];


const productRows = data?.filter(
  (row) =>
    row.sku &&
    ![
      "acos1",
      "acos2",
      "acos3",
      "Platform_Fees1",
      "Platform_Fees2",
      "Platform_Fees3",
      "advertising_total1",
      "advertising_total2",
      "advertising_total3",
      "cm2profit1",
      "cm2profit2",
      "cm2profit3",
      "NetReimbursement1",
      "NetReimbursement2",
      "NetReimbursement3",
      "ReimbursementvsCM2Margins1",
      "ReimbursementvsCM2Margins2",
      "ReimbursementvsCM2Margins3",
      "Reimbursementvssales1",
      "Reimbursementvssales2",
      "Reimbursementvssales3",
      "cm2margin1",
      "cm2margin2",
      "cm2margin3",
      "platform_fees_total",
      "advertising_total",
      "cm2profit_total",
      "cm2margin_total",
      "acos_total",
      "NetReimbursement_total",
      "ReimbursementvsCM2Margins_total",
      "Reimbursementvssales_total",
    ].includes(row.sku)
);

const summaryAsRows: RowData[] = summaryRows.map(r => ({
  product_name: r.label,
  sku: "",

  // Month 1
  forecast_1st: "",
  Total_Sales_1st: r.m1 ?? "",
  profit_1st: "",
  profit_percentage_1st: "",

  // Month 2
  forecast_2nd: "",
  Total_Sales_2nd: r.m2 ?? "",
  profit_2nd: "",
  profit_percentage_2nd: "",

  // Month 3
  forecast_3rd: "",
  Total_Sales_3rd: r.m3 ?? "",
  profit_3rd: "",
  profit_percentage_3rd: "",

  // Sum
  forecast_sum: "",
  Total_Sales_sum: r.sum ?? "",
  profit_sum: "",
  profit_percentage_sum: "",
}));

const normalizedProductRows = productRows?.map(r => ({
  ...r,
  sku: r.sku === "Total" ? "" : r.sku,
}));






  return (
    <div className='flex flex-col gap-8'>
      <div className='flex justify-between'>
<h2 style={{ marginBottom: 10, color: '#414042' }} className='2xl:text-2xl text-lg text-[#414042] font-bold'>
        P &amp; L Forecast -{' '}
        <span style={{ color: '#60a68e' }}>
        {countryName.toUpperCase()}  ({formatMonthYear(currentMonth, currentYear)} to {formatMonthYear(nextToNextMonth, nextToNextMonthYear)} )
        </span>
      </h2>
     <button
              onClick={() => exportTableToExcel()}
            className="bg-white border border-[#8B8585] px-1  rounded-sm"
                                        style={{
                             boxShadow: "0px 4px 4px 0px #00000040",  
                           }}
                                     >
                                     <IoDownload size={27} />
            </button>
      </div>
      

      {loading && <div className="loading">Loading...</div>}
      {error && (
        <div className="error">
          {error}
        </div>
      )}

      {data && chartData.length > 0 && (
        <div className='border border-[#414042] rounded-sm'>
          <PnlForecastChart
  ref={chartRef}
  chartData={chartData}
  currencySymbol={currencySymbol}
  selectedGraphs={selectedGraphs}
  handleCheckboxChange={handleCheckboxChange}
/>
        </div>
        
      )}
{data && (
  <div>
    <div className="overflow-x-auto rounded-sm">
    <GroupedCollapsibleTable<RowData>
  rows={[
    ...(normalizedProductRows || []),
    ...summaryAsRows,
  ]}
  getRowKey={(r, idx) =>
    r.sku && r.sku !== "" ? r.sku : `summary-${idx}-${r.product_name}`
  }
  leftCols={leftCols}
  groups={groups}
  singleCols={[]}
 getValue={(row, key, rowIndex) => {
  if (key === "sr_no") {
    // ❌ No serial number for Total & Summary rows
    if (
      row.product_name === "Total" ||
      summaryRows.some(s => s.label === row.product_name)
    ) {
      return "";
    }

    // ✅ Count ONLY product rows
    const productIndex = normalizedProductRows?.findIndex(
      r => r === row
    );

    return productIndex !== undefined && productIndex >= 0
      ? productIndex + 1
      : "";
  }

  return formatCellValue(key, row[key]);
}}

  getRowClassName={(row) => {
    if (row.product_name === "Total") {
      return "bg-[#D9D9D9]/90 font-bold ";
    }
    if (summaryRows.some(s => s.label === row.product_name)) {
      return "bg-[#ffffff]";
    }
    return "";
  }}
/>

    </div>
    
    <br />
  </div>
)}
    </div>
  );
};

export default Pnlforecast;
