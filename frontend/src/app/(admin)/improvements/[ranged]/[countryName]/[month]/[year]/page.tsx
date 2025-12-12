'use client'

import React, { useState, useEffect, useMemo } from 'react';
import axios from 'axios';
import { useParams } from 'next/navigation'; // Next.js uses next/navigation for app router
import * as XLSX from 'xlsx';
import { FaThumbsUp, FaThumbsDown } from 'react-icons/fa';
import Productinfoinpopup from '@/components/businessInsight/Productinfoinpopup';
import Drawer from '@mui/material/Drawer';
import IconButton from '@mui/material/IconButton';
import { IoDownload } from "react-icons/io5";
import { BsStars } from "react-icons/bs";
import { FaArrowUp, FaArrowDown } from 'react-icons/fa';
import * as echarts from 'echarts';
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  LineElement,
  PointElement,
  Tooltip,
  Legend,
} from "chart.js";

ChartJS.register(CategoryScale, LinearScale, LineElement, PointElement, Tooltip, Legend);



// =========================
// Types/Interfaces
// =========================
interface MonthOption {
  value: string;
  label: string;
}

interface GrowthCategory {
  category: string;
  value: number;
}

interface SkuItem {
  product_name: string;
  sku?: string;
  'Sales Mix (Month2)'?: number;
  quantity?: number;
  asp?: number;
  net_sales?: number;
  sales_mix?: number;
  unit_wise_profitability?: number;
  profit?: number;

  // ✅ new raw fields from backend for Excel:
  quantity_month1?: number;
  quantity_month2?: number;
  asp_month1?: number;
  asp_month2?: number;
  net_sales_month1?: number;
  net_sales_month2?: number;
  product_sales_month1?: number;
  product_sales_month2?: number;
  sales_mix_month1?: number;
  sales_mix_month2?: number;
   profit_percentage_month1?: number;
  profit_percentage_month2?: number;
  unit_wise_profitability_month1?: number;
  unit_wise_profitability_month2?: number;
  profit_month1?: number;
  profit_month2?: number;

  [key: string]: any; // For growth fields like 'Unit Growth'
}

interface CategorizedGrowth {
  top_80_skus: SkuItem[];
  new_or_reviving_skus: SkuItem[];
  other_skus: SkuItem[];

  // ✅ add
  all_skus?: SkuItem[];

  top_80_total?: SkuItem | null;
  new_or_reviving_total?: SkuItem | null;
  other_total?: SkuItem | null;
  all_skus_total?: SkuItem | null;
}



interface SkuInsight {
  product_name: string;
  insight: string;  [key: string]: any;
}

interface ApiResponse {
  comparison_range?: { month2_label: string };
  categorized_growth?: CategorizedGrowth;
  insights?: Record<string, SkuInsight>;
  reimbursement_totals?: { month1: number; month2: number };

  // ✅ add
  advertising_totals?: { month1: number; month2: number };
  expense_totals?: { month1: number; month2: number };
}


// =========================
// Config
// =========================
const API_BASE = 'http://127.0.0.1:5000';

type TabKey = 'top_80_skus' | 'new_or_reviving_skus' | 'other_skus' | 'all_skus';

// Persist keys
const STORAGE_KEY = 'bi_insight_data';        // compare inputs + results
const INSIGHTS_KEY = 'bi_sku_insights';       // AI insights (optional persist)

// Axios instance with JWT
const api = axios.create({ baseURL: API_BASE });
api.interceptors.request.use((cfg) => {
  const t = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
  if (t) cfg.headers.Authorization = `Bearer ${t}`;
  return cfg;
});

const MonthsforBI: React.FC = () => {
  const params = useParams();
  const countryName = params?.countryName as string | undefined;

  const hexToRgba = (hex: string, a: number) => {
  const h = hex.replace("#", "");
  const full = h.length === 3 ? h.split("").map(c => c + c).join("") : h;
  const n = parseInt(full, 16);
  const r = (n >> 16) & 255;
  const g = (n >> 8) & 255;
  const b = n & 255;
  return `rgba(${r}, ${g}, ${b}, ${a})`;
};

  // Month/year form
  const [month1, setMonth1] = useState<string>('');
  const [year1, setYear1] = useState<string>('');
  const [month2, setMonth2] = useState<string>('');
  const [year2, setYear2] = useState<string>('');

  // Data + UI state
 const [categorizedGrowth, setCategorizedGrowth] = useState<CategorizedGrowth>({
  top_80_skus: [],
  new_or_reviving_skus: [],
  other_skus: [],
  all_skus: [],              // ✅ add
  top_80_total: null,
  new_or_reviving_total: null,
  other_total: null,
  all_skus_total: null,
});


const [activeTab, setActiveTab] = useState<TabKey>('all_skus');
  const [month2Label, setMonth2Label] = useState<string>('');
  const [error, setError] = useState<string | null>(null);

  // Insights + modal
  const [loadingInsight, setLoadingInsight] = useState<boolean>(false);
  const [skuInsights, setSkuInsights] = useState<Record<string, SkuInsight>>({});
  const [selectedSku, setSelectedSku] = useState<string | null>(null);
  const [modalOpen, setModalOpen] = useState<boolean>(false);

  // Feedback (Summary)
  const [fbType, setFbType] = useState<'like' | 'dislike' | null>(null);
  const [fbText, setFbText] = useState<string>('');
  const [fbSubmitting, setFbSubmitting] = useState<boolean>(false);
  const [fbSuccess, setFbSuccess] = useState<boolean>(false);
  const [autoCompared, setAutoCompared] = useState(false);
const [expandAllSkusOthers, setExpandAllSkusOthers] = useState(true);
const [reimbursementTotals, setReimbursementTotals] = useState<{month1:number; month2:number} | null>(null);
const [advertisingTotals, setAdvertisingTotals] = useState<{month1:number; month2:number} | null>(null);
const [expenseTotals, setExpenseTotals] = useState<{month1:number; month2:number} | null>(null);

  // ✅ NEW: available periods from backend (['YYYY-MM'])
  const [availablePeriods, setAvailablePeriods] = useState<string[]>([]);
  const [showTotalsModal, setShowTotalsModal] = useState(false);

const [selectedTotals, setSelectedTotals] = useState<Record<string, boolean>>({
  netSales: true,
  cm1Profit: true,
  otherExpense: true,
  advertising: true,
  reimbursement: true,
});

const toggleTotalsMetric = (key: string) => {
  const selectedCount = Object.values(selectedTotals).filter(Boolean).length;
  const isChecked = !!selectedTotals[key];

  // prevent turning off the last metric (same logic as GraphPage) :contentReference[oaicite:2]{index=2}
  if (isChecked && selectedCount === 1) {
    setShowTotalsModal(true);
    return;
  }

  setSelectedTotals((prev) => ({ ...prev, [key]: !isChecked }));
};


  const chartRef = React.useRef<HTMLDivElement | null>(null);
const chartInstanceRef = React.useRef<echarts.EChartsType | null>(null);

const profitChartRef = React.useRef<HTMLDivElement | null>(null);
const profitChartInstanceRef = React.useRef<echarts.EChartsType | null>(null);

const unitsChartRef = React.useRef<HTMLDivElement | null>(null);
const unitsChartInstanceRef = React.useRef<echarts.EChartsType | null>(null);

const aspChartRef = React.useRef<HTMLDivElement | null>(null);
const aspChartInstanceRef = React.useRef<echarts.EChartsType | null>(null);

const totalsChartRef = React.useRef<HTMLDivElement | null>(null);
const totalsChartInstanceRef = React.useRef<echarts.EChartsType | null>(null);


const periodToDate = (p: string) => {
  // p = "YYYY-MM"
  const [yy, mm] = p.split('-').map(Number);
  return new Date(yy, (mm || 1) - 1, 1).getTime();
};

const pickDefaultComparePeriods = (periods: string[]) => {
  if (!periods?.length) return null;

  const now = new Date();
  const cy = now.getFullYear();
  const cm = pad2(now.getMonth() + 1);
  const currentKey = `${cy}-${cm}`;

  // exclude current month (even if available)
  const filtered = periods.filter((p) => p !== currentKey);

  // sort descending (latest first)
  const sorted = [...filtered].sort((a, b) => periodToDate(b) - periodToDate(a));

  if (sorted.length < 2) return null;

  return { newer: sorted[0], older: sorted[1] }; // month2=newer, month1=older
};

const sumField = (rows: any[], key: string) =>
  (rows || []).reduce((a, r) => a + Number(r?.[key] ?? 0), 0);

const buildCompareSeries = (
  metricKeyBase: 'net_sales' | 'profit' | 'quantity' | 'rembursement_fee' | 'asp'
) => {
  const m1Label = `${getAbbr(month1)}'${String(year1).slice(2)}`;
  const m2Label = `${getAbbr(month2)}'${String(year2).slice(2)}`;
  const x = [`${m1Label}`, '', `${m2Label}`];

  const top80Rows = categorizedGrowth.top_80_skus || [];
  const newRevRows = categorizedGrowth.new_or_reviving_skus || [];
  const otherRows = categorizedGrowth.other_skus || [];

  const sumField = (rows: any[], key: string) =>
    (rows || []).reduce((a, r) => a + Number(r?.[key] ?? 0), 0);

  const safeDiv = (a: number, b: number) => (b ? a / b : 0);

  // ✅ SPECIAL CASE: ASP should be weighted = total net_sales / total qty
  if (metricKeyBase === 'asp') {
    const top80_ns_m1 = sumField(top80Rows, 'net_sales_month1');
    const top80_ns_m2 = sumField(top80Rows, 'net_sales_month2');
    const top80_q_m1  = sumField(top80Rows, 'quantity_month1');
    const top80_q_m2  = sumField(top80Rows, 'quantity_month2');

    const newRev_ns_m1 = sumField(newRevRows, 'net_sales_month1');
    const newRev_ns_m2 = sumField(newRevRows, 'net_sales_month2');
    const newRev_q_m1  = sumField(newRevRows, 'quantity_month1');
    const newRev_q_m2  = sumField(newRevRows, 'quantity_month2');

    const other_ns_m1 = sumField(otherRows, 'net_sales_month1');
    const other_ns_m2 = sumField(otherRows, 'net_sales_month2');
    const other_q_m1  = sumField(otherRows, 'quantity_month1');
    const other_q_m2  = sumField(otherRows, 'quantity_month2');

    const top80_m1 = safeDiv(top80_ns_m1, top80_q_m1);
    const top80_m2 = safeDiv(top80_ns_m2, top80_q_m2);

    const newRev_m1 = safeDiv(newRev_ns_m1, newRev_q_m1);
    const newRev_m2 = safeDiv(newRev_ns_m2, newRev_q_m2);

    const other_m1 = safeDiv(other_ns_m1, other_q_m1);
    const other_m2 = safeDiv(other_ns_m2, other_q_m2);

    return { x, values: { top80_m1, top80_m2, newRev_m1, newRev_m2, other_m1, other_m2 } };
  }

  // ✅ DEFAULT: SUM metrics
  const m1Key = `${metricKeyBase}_month1`;
  const m2Key = `${metricKeyBase}_month2`;

  const top80_m1 = sumField(top80Rows, m1Key);
  const top80_m2 = sumField(top80Rows, m2Key);

  const newRev_m1 = sumField(newRevRows, m1Key);
  const newRev_m2 = sumField(newRevRows, m2Key);

  const other_m1 = sumField(otherRows, m1Key);
  const other_m2 = sumField(otherRows, m2Key);

  return { x, values: { top80_m1, top80_m2, newRev_m1, newRev_m2, other_m1, other_m2 } };
};

const getAllRows = () => ([
  ...(categorizedGrowth.top_80_skus || []),
  ...(categorizedGrowth.new_or_reviving_skus || []),
  ...(categorizedGrowth.other_skus || []),
]);

const totalOf = (key: string) =>
  getAllRows().reduce((a, r) => a + Number(r?.[key] ?? 0), 0);




useEffect(() => {
  const el = chartRef.current;
  if (!el) return;

  const raf = requestAnimationFrame(() => {
    if (chartInstanceRef.current && chartInstanceRef.current.getDom() !== el) {
      chartInstanceRef.current.dispose();
      chartInstanceRef.current = null;
    }
    if (!chartInstanceRef.current) {
      chartInstanceRef.current = echarts.init(el);
    }

    const { x, values } = buildCompareSeries('net_sales');
    const { top80_m1, top80_m2, newRev_m1, newRev_m2, other_m1, other_m2 } = values;
    const currency = getCurrencySymbol();

    const hasAny =
      top80_m1 || top80_m2 || newRev_m1 || newRev_m2 || other_m1 || other_m2 

    if (!hasAny) {
      chartInstanceRef.current?.clear();
      chartInstanceRef.current?.setOption({ title: { text: 'No data' } });
      chartInstanceRef.current?.resize();
      return;
    }

    const option: echarts.EChartsOption = {


tooltip: {
  trigger: 'axis',
  formatter: (params: any) => {
    // params: array of series points
    const axisLabel = params?.[0]?.axisValueLabel ?? '';
    const lines: string[] = [];
    lines.push(`<div style="font-weight:700;margin-bottom:6px;">Net Sales  ${axisLabel}</div>`);

    // reverse order: Top 80, Other, New/Reviving
    const map = new Map(params.map((p: any) => [p.seriesName, p]));
    const ordered = SERIES_ORDER.map(s => map.get(s.name)).filter(Boolean);

    ordered.forEach((p: any) => {
      lines.push(
        `<div style="display:flex;align-items:center;gap:8px;margin:2px 0;">
          <span style="display:inline-block;width:10px;height:10px;border-radius:999px;background:${p.color};"></span>
          <span style="flex:1;">${p.seriesName}</span>
          <span style="font-weight:700;">${currency}${fmtNum(p.data)}</span>
        </div>`
      );
    });

    return `<div style="min-width:180px;">${lines.join('')}</div>`;
  },
},


legend: { show: false },

      grid: { left: 50, right: 20, top: 40, bottom: 35 },
        color: ['#87AD12', '#AB64B5', '#F47A00',], // Net Sales palette
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: x,
axisLabel: {
  interval: 0,
  margin: 20,
  align: 'center',
  formatter: (v: string, idx: number) => {
    if (v === '') return '';
    if (idx === 0) return `{m1|${v}}`; // Month1
    if (idx === 2) return `{m2|${v}}`; // Month2
    return v;
  },
  rich: {
    // Month1 ko center-left lane ke liye RIGHT align + left padding
    m1: {
      align: 'right',
      padding: [0, 0, 0, 80], // <-- increase/decrease (60-120)
    },
    // Month2 ko center-right lane ke liye LEFT align + right padding
    m2: {
      align: 'left',
      padding: [0, 80, 0, 0], // <-- increase/decrease (60-120)
    },
  },
},


        splitLine: { show: true, lineStyle: { type: 'dashed', opacity: 0.35 } },
      },
    yAxis: {
  type: 'value',
  name: `Amount (${currency})`,
  nameLocation: 'middle',
  nameGap: 45,
  axisLabel: {
    formatter: (v: number) => `${Math.round(v).toLocaleString()}`
  }
},

      
    series: [
  {
    name: 'New/Reviving',
    type: 'line',
    stack: 'Total',
    symbol: 'none',
   areaStyle: {
  color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
    { offset: 0.0, color: hexToRgba("#87AD12", 0.12) },
    { offset: 0.49, color: hexToRgba("#87AD12", 0.12) },
    { offset: 0.51, color: hexToRgba("#87AD12", 0.28) },
    { offset: 1.0, color: hexToRgba("#87AD12", 0.28) },
  ]),
},
    data: [newRev_m1, newRev_m1, newRev_m2],
  },
  {
    name: 'Other SKUs',
    type: 'line',
    stack: 'Total',
    symbol: 'none',
    areaStyle: {
  color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
    { offset: 0.0, color: hexToRgba("#AB64B5", 0.12) },
    { offset: 0.49, color: hexToRgba("#AB64B5", 0.12) },
    { offset: 0.51, color: hexToRgba("#AB64B5", 0.28) },
    { offset: 1.0, color: hexToRgba("#AB64B5", 0.28) },
  ]),
},
    data: [other_m1, other_m1, other_m2],
  },
  {
    name: 'Top 80%',
    type: 'line',
    stack: 'Total',
    symbol: 'none',
    areaStyle: {
  color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
    { offset: 0.0, color: hexToRgba("#F47A00", 0.12) },
    { offset: 0.49, color: hexToRgba("#F47A00", 0.12) },
    { offset: 0.51, color: hexToRgba("#F47A00", 0.28) },
    { offset: 1.0, color: hexToRgba("#F47A00", 0.28) },
  ]),
},
    data: [top80_m1, top80_m1, top80_m2],
     markLine: {
      symbol: "none",
      silent: true,
      data: [{ xAxis: "" }], // center category
      lineStyle: { color: "#111827", width: 1, opacity: 0.35 },
      label: { show: false },
    },
  },
 
],



    };

    chartInstanceRef.current!.setOption(option, true);
    chartInstanceRef.current!.resize();

    const onResize = () => chartInstanceRef.current?.resize();
    window.addEventListener('resize', onResize);
    const ro = new ResizeObserver(() => chartInstanceRef.current?.resize());
    ro.observe(el);

    return () => {
      window.removeEventListener('resize', onResize);
      ro.disconnect();
    };
  });

  return () => cancelAnimationFrame(raf);
}, [
  month1, year1, month2, year2,
  categorizedGrowth.top_80_skus,
  categorizedGrowth.new_or_reviving_skus,
  categorizedGrowth.other_skus,
]);

useEffect(() => {
  const el = profitChartRef.current;
  if (!el) return;

  const raf = requestAnimationFrame(() => {
    if (profitChartInstanceRef.current && profitChartInstanceRef.current.getDom() !== el) {
      profitChartInstanceRef.current.dispose();
      profitChartInstanceRef.current = null;
    }
    if (!profitChartInstanceRef.current) {
      profitChartInstanceRef.current = echarts.init(el);
    }

    const { x, values } = buildCompareSeries('profit');
    const { top80_m1, top80_m2, newRev_m1, newRev_m2, other_m1, other_m2 } = values;
    const currency = getCurrencySymbol();

    const hasAny =
      top80_m1 || top80_m2 || newRev_m1 || newRev_m2 || other_m1 || other_m2 ;

    if (!hasAny) {
      profitChartInstanceRef.current?.clear();
      profitChartInstanceRef.current?.setOption({ title: { text: 'No data' } });
      profitChartInstanceRef.current?.resize();
      return;
    }

    const option: echarts.EChartsOption = {
tooltip: {
  trigger: 'axis',
  formatter: (params: any) => {
    const axisLabel = params?.[0]?.axisValueLabel ?? '';
    const lines: string[] = [];
    lines.push(`<div style="font-weight:700;margin-bottom:6px;">CM1 Profit ${axisLabel}</div>`);

    const map = new Map(params.map((p: any) => [p.seriesName, p]));
    const ordered = SERIES_ORDER.map(s => map.get(s.name)).filter(Boolean);

    ordered.forEach((p: any) => {
      lines.push(
        `<div style="display:flex;align-items:center;gap:8px;margin:2px 0;">
          <span style="display:inline-block;width:10px;height:10px;border-radius:999px;background:${p.color};"></span>
          <span style="flex:1;">${p.seriesName}</span>
          <span style="font-weight:700;">${currency}${fmtNum(p.data)}</span>
        </div>`
      );
    });

    return `<div style="min-width:180px;">${lines.join('')}</div>`;
  },
},

legend: { show: false },

      grid: { left: 50, right: 20, top: 40, bottom: 35 },
       color: ['#87AD12', '#AB64B5', '#F47A00',], // Net Sales palette
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: x,
axisLabel: {
  interval: 0,
  margin: 20,
  align: 'center',
  formatter: (v: string, idx: number) => {
    if (v === '') return '';
    if (idx === 0) return `{m1|${v}}`; // Month1
    if (idx === 2) return `{m2|${v}}`; // Month2
    return v;
  },
  rich: {
    // Month1 ko center-left lane ke liye RIGHT align + left padding
    m1: {
      align: 'right',
      padding: [0, 0, 0, 80], // <-- increase/decrease (60-120)
    },
    // Month2 ko center-right lane ke liye LEFT align + right padding
    m2: {
      align: 'left',
      padding: [0, 80, 0, 0], // <-- increase/decrease (60-120)
    },
  },
},


        splitLine: { show: true, lineStyle: { type: 'dashed', opacity: 0.35 } },
      },
yAxis: {
  type: 'value',
  name: `Amount (${currency})`,
  nameLocation: 'middle',
  nameGap: 45,
  axisLabel: {
    formatter: (v: number) => `${Math.round(v).toLocaleString()}`
  }
},

   series: [
  {
    name: 'New/Reviving',
    type: 'line',
    stack: 'Total',
    symbol: 'none',
areaStyle: {
  color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
    { offset: 0.0, color: hexToRgba("#87AD12", 0.12) },
    { offset: 0.49, color: hexToRgba("#87AD12", 0.12) },
    { offset: 0.51, color: hexToRgba("#87AD12", 0.28) },
    { offset: 1.0, color: hexToRgba("#87AD12", 0.28) },
  ]),
},
    data: [newRev_m1, newRev_m1, newRev_m2],
  },
  {
    name: 'Other SKUs',
    type: 'line',
    stack: 'Total',
    symbol: 'none',
    areaStyle: {
  color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
    { offset: 0.0, color: hexToRgba("#AB64B5", 0.12) },
    { offset: 0.49, color: hexToRgba("#AB64B5", 0.12) },
    { offset: 0.51, color: hexToRgba("#AB64B5", 0.28) },
    { offset: 1.0, color: hexToRgba("#AB64B5", 0.28) },
  ]),
},
    data: [other_m1, other_m1, other_m2],

  },
  {
    name: 'Top 80%',
    type: 'line',
    stack: 'Total',
    symbol: 'none',
    areaStyle: {
  color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
    { offset: 0.0, color: hexToRgba("#F47A00", 0.12) },
    { offset: 0.49, color: hexToRgba("#F47A00", 0.12) },
    { offset: 0.51, color: hexToRgba("#F47A00", 0.28) },
    { offset: 1.0, color: hexToRgba("#F47A00", 0.28) },
  ]),
},

    data: [top80_m1, top80_m1, top80_m2],
     markLine: {
      symbol: "none",
      silent: true,
      data: [{ xAxis: "" }], // center category
      lineStyle: { color: "#111827", width: 1, opacity: 0.35 },
      label: { show: false },
    },
  },
],




    };

    profitChartInstanceRef.current!.setOption(option, true);
    profitChartInstanceRef.current!.resize();

    const onResize = () => profitChartInstanceRef.current?.resize();
    window.addEventListener('resize', onResize);
    const ro = new ResizeObserver(() => profitChartInstanceRef.current?.resize());
    ro.observe(el);

    return () => {
      window.removeEventListener('resize', onResize);
      ro.disconnect();
    };
  });

  return () => cancelAnimationFrame(raf);
}, [
  month1, year1, month2, year2,
  categorizedGrowth.top_80_skus,
  categorizedGrowth.new_or_reviving_skus,
  categorizedGrowth.other_skus,
]);

useEffect(() => {
  const el = unitsChartRef.current;
  if (!el) return;

  const raf = requestAnimationFrame(() => {
    if (unitsChartInstanceRef.current && unitsChartInstanceRef.current.getDom() !== el) {
      unitsChartInstanceRef.current.dispose();
      unitsChartInstanceRef.current = null;
    }
    if (!unitsChartInstanceRef.current) {
      unitsChartInstanceRef.current = echarts.init(el);
    }

    const { x, values } = buildCompareSeries('quantity');
    const { top80_m1, top80_m2, newRev_m1, newRev_m2, other_m1, other_m2 } = values;

    const hasAny = top80_m1 || top80_m2 || newRev_m1 || newRev_m2 || other_m1 || other_m2;
    if (!hasAny) {
      unitsChartInstanceRef.current?.clear();
      unitsChartInstanceRef.current?.setOption({ title: { text: 'No data' } });
      unitsChartInstanceRef.current?.resize();
      return;
    }

    const option: echarts.EChartsOption = {
      tooltip: {
        trigger: 'axis',
        formatter: (params: any) => {
          const axisLabel = params?.[0]?.axisValueLabel ?? '';
          const lines: string[] = [];
          lines.push(`<div style="font-weight:700;margin-bottom:6px;">Units  ${axisLabel}</div>`);

          const map = new Map(params.map((p: any) => [p.seriesName, p]));
          const ordered = SERIES_ORDER.map(s => map.get(s.name)).filter(Boolean);

          ordered.forEach((p: any) => {
            lines.push(
              `<div style="display:flex;align-items:center;gap:8px;margin:2px 0;">
                <span style="display:inline-block;width:10px;height:10px;border-radius:999px;background:${p.color};"></span>
                <span style="flex:1;">${p.seriesName}</span>
                <span style="font-weight:700;">${fmtNum(p.data)}</span>
              </div>`
            );
          });

          return `<div style="min-width:180px;">${lines.join('')}</div>`;
        },
      },

      legend: { show: false },
      grid: { left: 50, right: 20, top: 40, bottom: 35 },
      color: ['#87AD12', '#AB64B5', '#F47A00'],
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: x,
        axisLabel: {
          interval: 0,
          margin: 20,
          align: 'center',
          formatter: (v: string, idx: number) => {
            if (v === '') return '';
            if (idx === 0) return `{m1|${v}}`;
            if (idx === 2) return `{m2|${v}}`;
            return v;
          },
          rich: {
            m1: { align: 'right', padding: [0, 0, 0, 80] },
            m2: { align: 'left', padding: [0, 80, 0, 0] },
          },
        },
        splitLine: { show: true, lineStyle: { type: 'dashed', opacity: 0.35 } },
      },
    yAxis: {
  type: 'value',
  name: 'Nos.',
  nameLocation: 'middle',
  nameGap: 45,
  axisLabel: {
    formatter: (v: number) => `${Math.round(v).toLocaleString()}`
  }
}

,

      series: [
        {
          name: 'New/Reviving',
          type: 'line',
          stack: 'Total',
          symbol: 'none',
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
              { offset: 0.0, color: hexToRgba("#87AD12", 0.12) },
              { offset: 0.49, color: hexToRgba("#87AD12", 0.12) },
              { offset: 0.51, color: hexToRgba("#87AD12", 0.28) },
              { offset: 1.0, color: hexToRgba("#87AD12", 0.28) },
            ]),
          },
          data: [newRev_m1, newRev_m1, newRev_m2],
        },
        {
          name: 'Other SKUs',
          type: 'line',
          stack: 'Total',
          symbol: 'none',
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
              { offset: 0.0, color: hexToRgba("#AB64B5", 0.12) },
              { offset: 0.49, color: hexToRgba("#AB64B5", 0.12) },
              { offset: 0.51, color: hexToRgba("#AB64B5", 0.28) },
              { offset: 1.0, color: hexToRgba("#AB64B5", 0.28) },
            ]),
          },
          data: [other_m1, other_m1, other_m2],
        },
        {
          name: 'Top 80%',
          type: 'line',
          stack: 'Total',
          symbol: 'none',
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
              { offset: 0.0, color: hexToRgba("#F47A00", 0.12) },
              { offset: 0.49, color: hexToRgba("#F47A00", 0.12) },
              { offset: 0.51, color: hexToRgba("#F47A00", 0.28) },
              { offset: 1.0, color: hexToRgba("#F47A00", 0.28) },
            ]),
          },
          data: [top80_m1, top80_m1, top80_m2],
          markLine: {
            symbol: "none",
            silent: true,
            data: [{ xAxis: "" }],
            lineStyle: { color: "#111827", width: 1, opacity: 0.35 },
            label: { show: false },
          },
        },
      ],
    };

    unitsChartInstanceRef.current!.setOption(option, true);
    unitsChartInstanceRef.current!.resize();

    const onResize = () => unitsChartInstanceRef.current?.resize();
    window.addEventListener('resize', onResize);
    const ro = new ResizeObserver(() => unitsChartInstanceRef.current?.resize());
    ro.observe(el);

    return () => {
      window.removeEventListener('resize', onResize);
      ro.disconnect();
    };
  });

  return () => cancelAnimationFrame(raf);
}, [
  month1, year1, month2, year2,
  categorizedGrowth.top_80_skus,
  categorizedGrowth.new_or_reviving_skus,
  categorizedGrowth.other_skus,
]);

// --- Reimbursement Fees chart ---
useEffect(() => {
  const el = aspChartRef.current;
  if (!el) return;

  const raf = requestAnimationFrame(() => {
    if (aspChartInstanceRef.current && aspChartInstanceRef.current.getDom() !== el) {
      aspChartInstanceRef.current.dispose();
      aspChartInstanceRef.current = null;
    }
    if (!aspChartInstanceRef.current) {
      aspChartInstanceRef.current = echarts.init(el);
    }

    const { x, values } = buildCompareSeries('asp');
    const { top80_m1, top80_m2, newRev_m1, newRev_m2, other_m1, other_m2 } = values;
     const currency = getCurrencySymbol();

    const hasAny =
      top80_m1 || top80_m2 || newRev_m1 || newRev_m2 || other_m1 || other_m2;

    if (!hasAny) {
      aspChartInstanceRef.current?.clear();
      aspChartInstanceRef.current?.setOption({ title: { text: 'No data' } });
      aspChartInstanceRef.current?.resize();
      return;
    }

    const option: echarts.EChartsOption = {
      tooltip: {
        trigger: 'axis',
        formatter: (params: any) => {
          const axisLabel = params?.[0]?.axisValueLabel ?? '';
          const lines: string[] = [];
          lines.push(`<div style="font-weight:700;margin-bottom:6px;">ASP  ${axisLabel}</div>`);

          const map = new Map(params.map((p: any) => [p.seriesName, p]));
          const ordered = SERIES_ORDER.map(s => map.get(s.name)).filter(Boolean);

          ordered.forEach((p: any) => {
            lines.push(
              `<div style="display:flex;align-items:center;gap:8px;margin:2px 0;">
                <span style="display:inline-block;width:10px;height:10px;border-radius:999px;background:${p.color};"></span>
                <span style="flex:1;">${p.seriesName}</span>
                <span style="font-weight:700;">${currency}${Number(p.data ?? 0).toFixed(2)}</span>
              </div>`
            );
          });

          return `<div style="min-width:180px;">${lines.join('')}</div>`;
        },
      },
      legend: { show: false },
      grid: { left: 50, right: 20, top: 40, bottom: 35 },
      color: ['#87AD12', '#AB64B5', '#F47A00'],
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: x,
        axisLabel: {
          interval: 0,
          margin: 20,
          align: 'center',
          formatter: (v: string, idx: number) => {
            if (v === '') return '';
            if (idx === 0) return `{m1|${v}}`;
            if (idx === 2) return `{m2|${v}}`;
            return v;
          },
          rich: {
            m1: { align: 'right', padding: [0, 0, 0, 80] },
            m2: { align: 'left', padding: [0, 80, 0, 0] },
          },
        },
        splitLine: { show: true, lineStyle: { type: 'dashed', opacity: 0.35 } },
      },
      yAxis: {
       type: 'value',
  name: `Amount (${currency})`,
  nameLocation: 'middle',
  nameGap: 45,
        axisLabel: {formatter: (value: number) => {
      if (!value) return '0';
      return Number.isInteger(value)
        ? value.toString()
        : value.toFixed(0);
    } },
      },
      series: [
        {
          name: 'New/Reviving',
          type: 'line',
          stack: 'Total',
          symbol: 'none',
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
              { offset: 0.0, color: hexToRgba("#87AD12", 0.12) },
              { offset: 0.49, color: hexToRgba("#87AD12", 0.12) },
              { offset: 0.51, color: hexToRgba("#87AD12", 0.28) },
              { offset: 1.0, color: hexToRgba("#87AD12", 0.28) },
            ]),
          },
          data: [newRev_m1, newRev_m1, newRev_m2],
        },
        {
          name: 'Other SKUs',
          type: 'line',
          stack: 'Total',
          symbol: 'none',
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
              { offset: 0.0, color: hexToRgba("#AB64B5", 0.12) },
              { offset: 0.49, color: hexToRgba("#AB64B5", 0.12) },
              { offset: 0.51, color: hexToRgba("#AB64B5", 0.28) },
              { offset: 1.0, color: hexToRgba("#AB64B5", 0.28) },
            ]),
          },
          data: [other_m1, other_m1, other_m2],
        },
        {
          name: 'Top 80%',
          type: 'line',
          stack: 'Total',
          symbol: 'none',
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
              { offset: 0.0, color: hexToRgba("#F47A00", 0.12) },
              { offset: 0.49, color: hexToRgba("#F47A00", 0.12) },
              { offset: 0.51, color: hexToRgba("#F47A00", 0.28) },
              { offset: 1.0, color: hexToRgba("#F47A00", 0.28) },
            ]),
          },
          data: [top80_m1, top80_m1, top80_m2],
          markLine: {
            symbol: "none",
            silent: true,
            data: [{ xAxis: "" }],
            lineStyle: { color: "#111827", width: 1, opacity: 0.35 },
            label: { show: false },
          },
        },
      ],
    };

    aspChartInstanceRef.current!.setOption(option, true);
    aspChartInstanceRef.current!.resize();

    const onResize = () => aspChartInstanceRef.current?.resize();
    window.addEventListener('resize', onResize);
    const ro = new ResizeObserver(() => aspChartInstanceRef.current?.resize());
    ro.observe(el);

    return () => {
      window.removeEventListener('resize', onResize);
      ro.disconnect();
    };
  });

  return () => cancelAnimationFrame(raf);
}, [
  month1, year1, month2, year2,
  categorizedGrowth.top_80_skus,
  categorizedGrowth.new_or_reviving_skus,
  categorizedGrowth.other_skus,
]);

useEffect(() => {
  const el = totalsChartRef.current;
  if (!el) return;

  const raf = requestAnimationFrame(() => {
    if (totalsChartInstanceRef.current && totalsChartInstanceRef.current.getDom() !== el) {
      totalsChartInstanceRef.current.dispose();
      totalsChartInstanceRef.current = null;
    }
    if (!totalsChartInstanceRef.current) totalsChartInstanceRef.current = echarts.init(el);

    const m1Label = `${getAbbr(month1)}'${String(year1).slice(2)}`;
    const m2Label = `${getAbbr(month2)}'${String(year2).slice(2)}`;
    const x = [m1Label, '', m2Label];
    const currency = getCurrencySymbol();

    const netSales_m1 = totalOf('net_sales_month1');
    const netSales_m2 = totalOf('net_sales_month2');

    const profit_m1 = totalOf('profit_month1');
    const profit_m2 = totalOf('profit_month2');

    const otherExp_m1 = expenseTotals?.month1 ?? 0;
    const otherExp_m2 = expenseTotals?.month2 ?? 0;

    const adv_m1 = advertisingTotals?.month1 ?? 0;
    const adv_m2 = advertisingTotals?.month2 ?? 0;

    const reimb_m1 = reimbursementTotals?.month1 ?? 0;
    const reimb_m2 = reimbursementTotals?.month2 ?? 0;

    const hasAny =
      netSales_m1 || netSales_m2 || profit_m1 || profit_m2 ||
      otherExp_m1 || otherExp_m2 || adv_m1 || adv_m2 || reimb_m1 || reimb_m2;

    if (!hasAny) {
      totalsChartInstanceRef.current?.clear();
      totalsChartInstanceRef.current?.setOption({ title: { text: 'No data' } });
      totalsChartInstanceRef.current?.resize();
      return;
    }

    const option: echarts.EChartsOption = {
      tooltip: {
        trigger: 'axis',
        formatter: (params: any) => {
          const axisLabel = params?.[0]?.axisValueLabel ?? '';
          const lines: string[] = [];
          lines.push(`<div style="font-weight:700;margin-bottom:6px;">Totals ${axisLabel}</div>`);
          (params || []).forEach((p: any) => {
            lines.push(
              `<div style="display:flex;align-items:center;gap:8px;margin:2px 0;">
                <span style="display:inline-block;width:10px;height:10px;border-radius:999px;background:${p.color};"></span>
                <span style="flex:1;">${p.seriesName}</span>
                <span style="font-weight:700;">${currency}${fmtNum(p.data)}</span>
              </div>`
            );
          });
          return `<div style="min-width:210px;">${lines.join('')}</div>`;
        },
      },
      legend: { top: 8 },
      grid: { left: 55, right: 20, top: 55, bottom: 35 },
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: x,
        splitLine: { show: true, lineStyle: { type: 'dashed', opacity: 0.35 } },
      },
      yAxis: {
        type: 'value',
        axisLabel: { formatter: (v: any) => Math.round(Number(v)).toLocaleString() },
      },
      series: [
        { name: 'Net Sales', type: 'line', symbol: 'none', data: [netSales_m1, netSales_m1, netSales_m2] },
        { name: 'CM1 Profit', type: 'line', symbol: 'none', data: [profit_m1, profit_m1, profit_m2] },
        { name: 'Other Expense', type: 'line', symbol: 'none', data: [otherExp_m1, otherExp_m1, otherExp_m2] },
        { name: 'Advertising Total', type: 'line', symbol: 'none', data: [adv_m1, adv_m1, adv_m2] },
        { name: 'Reimbursement Fee', type: 'line', symbol: 'none', data: [reimb_m1, reimb_m1, reimb_m2] },
      ],
    };

    totalsChartInstanceRef.current!.setOption(option, true);
    totalsChartInstanceRef.current!.resize();

    const onResize = () => totalsChartInstanceRef.current?.resize();
    window.addEventListener('resize', onResize);
    const ro = new ResizeObserver(() => totalsChartInstanceRef.current?.resize());
    ro.observe(el);

    return () => {
      window.removeEventListener('resize', onResize);
      ro.disconnect();
    };
  });

  return () => cancelAnimationFrame(raf);
}, [
  month1, year1, month2, year2,
  categorizedGrowth.top_80_skus,
  categorizedGrowth.new_or_reviving_skus,
  categorizedGrowth.other_skus,
  advertisingTotals,
  expenseTotals,
  reimbursementTotals,
]);








  // Helpers
  const months: MonthOption[] = [
    { value: '01', label: 'January' },   { value: '02', label: 'February' },
    { value: '03', label: 'March' },     { value: '04', label: 'April' },
    { value: '05', label: 'May' },       { value: '06', label: 'June' },
    { value: '07', label: 'July' },      { value: '08', label: 'August' },
    { value: '09', label: 'September' }, { value: '10', label: 'October' },
    { value: '11', label: 'November' },  { value: '12', label: 'December' },
  ];
  const currentYear = new Date().getFullYear();
  const years = Array.from({ length: 2 }, (_, i) => String(currentYear - i));
  const pad2 = (m: string | number) => String(m).padStart(2, '0');
  const getAbbr = (m: string | number) => months.find(x => x.value === pad2(m))?.label.slice(0, 3) || '';
  const y1 = String(year1 || '');
  const y2 = String(year2 || '');

  const isGlobalData = () => (countryName || '').toLowerCase() === 'global';
  const getTabLabel = (key: TabKey): string =>
  key === 'top_80_skus' ? 'Top 80% SKUs'
  : key === 'new_or_reviving_skus' ? 'New/Reviving SKUs'
  : key === 'other_skus' ? 'Other SKUs'
  : 'All SKUs';

  const getTabNumberForFeedback = (key: keyof CategorizedGrowth): number =>
    key === 'top_80_skus' ? 1 : key === 'new_or_reviving_skus' ? 2 : 3;

  // ✅ NEW helper: check if (year, month) allowed by backend
  const isPeriodAvailable = (year: string, month: string) => {
    if (!year || !month) return false;
    if (!availablePeriods.length) return true; // if API failed, don't block UI
    const key = `${year}-${month}`;
    return availablePeriods.includes(key);
  };

  // =========================
  // Persistence: helpers
  // =========================
  const saveCompareToStorage = (payload: any) => {
    if (typeof window === 'undefined') return;
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
    } catch (e) {
      console.warn('Failed to save BI compare state:', e);
    }
  };

  const loadCompareFromStorage = (): any => {
    if (typeof window === 'undefined') return null;
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch (e) {
      console.warn('Failed to load BI compare state:', e);
      return null;
    }
  };

  const saveInsightsToStorage = (insights: Record<string, SkuInsight>) => {
    if (typeof window === 'undefined') return;
    try {
      localStorage.setItem(INSIGHTS_KEY, JSON.stringify(insights || {}));
    } catch (e) {
      console.warn('Failed to save insights:', e);
    }
  };

  const loadInsightsFromStorage = (): Record<string, SkuInsight> => {
    if (typeof window === 'undefined') return {};
    try {
      const raw = localStorage.getItem(INSIGHTS_KEY);
      return raw ? JSON.parse(raw) : {};
    } catch (e) {
      console.warn('Failed to load insights:', e);
      return {};
    }
  };

  const normalizeCategorizedGrowth = (raw?: any): CategorizedGrowth => {
  const mapRow = (row: any): SkuItem => {
    const clone: any = { ...row };

    // sales mix key mapping (if needed)
    if (row['Sales Mix (Current)'] != null) clone['Sales Mix (Month2)'] = row['Sales Mix (Current)'];

    // growth field mapping (if your backend uses (%) keys)
    const fieldMap: Record<string, string> = {
      'Unit Growth (%)': 'Unit Growth',
      'ASP Growth (%)': 'ASP Growth',
      'Gross Sales Growth (%)': 'Gross Sales Growth',
      'Net Sales Growth (%)': 'Net Sales Growth',
      'Sales Mix Change (%)': 'Sales Mix Change',
      'Profit Per Unit (%)': 'Profit Per Unit',
      'CM1 Profit Impact (%)': 'CM1 Profit Impact',
    };
    Object.entries(fieldMap).forEach(([bk, fk]) => {
      if (row[bk] != null) clone[fk] = row[bk];
    });

    // prev/curr → month1/month2 (aapke months compare page me bhi same keys use ho rahe)
   clone.quantity_month1 = row.quantity_month1 ?? row.quantity_prev ?? null;
clone.quantity_month2 = row.quantity_month2 ?? row.quantity_curr ?? null;

clone.asp_month1 = row.asp_month1 ?? row.asp_prev ?? null;
clone.asp_month2 = row.asp_month2 ?? row.asp_curr ?? null;

clone.net_sales_month1 = row.net_sales_month1 ?? row.net_sales_prev ?? null;
clone.net_sales_month2 = row.net_sales_month2 ?? row.net_sales_curr ?? null;

clone.product_sales_month1 = row.product_sales_month1 ?? row.product_sales_prev ?? null;
clone.product_sales_month2 = row.product_sales_month2 ?? row.product_sales_curr ?? null;


clone.sales_mix_month1 = row.sales_mix_month1 ?? row.sales_mix_prev ?? null;
clone.sales_mix_month2 = row.sales_mix_month2 ?? row.sales_mix_curr ?? row['Sales Mix (Current)'] ?? null;

clone.profit_percentage_month1 = row.profit_percentage_month1 ?? row.profit_percentage_prev ?? null;
clone.profit_percentage_month2 = row.profit_percentage_month2 ?? row.profit_percentage_curr ?? null;


clone.unit_wise_profitability_month1 =
  row.unit_wise_profitability_month1 ?? row.unit_wise_profitability_prev ?? null;
clone.unit_wise_profitability_month2 =
  row.unit_wise_profitability_month2 ?? row.unit_wise_profitability_curr ?? null;

clone.profit_month1 = row.profit_month1 ?? row.profit_prev ?? null;
clone.profit_month2 = row.profit_month2 ?? row.profit_curr ?? null;

clone.rembursement_fee_month1 = row.rembursement_fee_month1 ?? row.rembursement_fee_prev ?? null;
clone.rembursement_fee_month2 = row.rembursement_fee_month2 ?? row.rembursement_fee_curr ?? null;


    return clone;
  };

  const empty: CategorizedGrowth = {
    top_80_skus: [],
    new_or_reviving_skus: [],
    other_skus: [],
    top_80_total: null,
    new_or_reviving_total: null,
    other_total: null,
    all_skus_total: null,
  };

  if (!raw) return empty;

  return {
    top_80_skus: (raw.top_80_skus || []).map(mapRow),
    new_or_reviving_skus: (raw.new_or_reviving_skus || []).map(mapRow),
    other_skus: (raw.other_skus || []).map(mapRow),

    all_skus: (raw.all_skus || [
    ...(raw.top_80_skus || []),
    ...(raw.new_or_reviving_skus || []),
    ...(raw.other_skus || []),
  ]).map(mapRow),

  top_80_total: raw.top_80_total ? mapRow(raw.top_80_total) : null,
  new_or_reviving_total: raw.new_or_reviving_total ? mapRow(raw.new_or_reviving_total) : null,
  other_total: raw.other_total ? mapRow(raw.other_total) : null,
  all_skus_total: raw.all_skus_total ? mapRow(raw.all_skus_total) : null,
  };
};


  // =========================
  // Load persisted state on mount
  // =========================
  useEffect(() => {
    const saved = loadCompareFromStorage();
    if (saved) {
      setMonth1(saved.month1 || '');
      setYear1(saved.year1 || '');
      setMonth2(saved.month2 || '');
      setYear2(saved.year2 || '');
      setCategorizedGrowth(saved.categorizedGrowth || { top_80_skus: [], new_or_reviving_skus: [], other_skus: [] });
      setMonth2Label(saved.month2Label || '');
      setActiveTab(saved.activeTab || 'all_skus');
    }
    const cachedInsights = loadInsightsFromStorage();
    if (cachedInsights && Object.keys(cachedInsights).length) {
      setSkuInsights(cachedInsights);
    }
  }, []);

  // Also persist activeTab changes (lightweight)
  useEffect(() => {
    const saved = loadCompareFromStorage();
    if (saved) {
      saveCompareToStorage({ ...saved, activeTab });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab]);

  // ✅ NEW: fetch available periods from backend
  useEffect(() => {
    if (!countryName) return;
    const fetchAvailable = async () => {
      try {
        const res = await api.get<{ periods: string[] }>('/MonthsforBI/available-periods', {
          params: { countryName },
        });
        setAvailablePeriods(res.data?.periods || []);
      } catch (err: any) {
        console.error('Failed to load available periods:', err?.response?.data || err.message);
      }
    };
    fetchAvailable();
  }, [countryName]);

  // ✅ NEW: if year change ke baad combination invalid ho jaye to month reset
  useEffect(() => {
    if (year1 && month1 && !isPeriodAvailable(year1, month1)) {
      setMonth1('');
    }
  }, [year1, availablePeriods]);

  useEffect(() => {
    if (year2 && month2 && !isPeriodAvailable(year2, month2)) {
      setMonth2('');
    }
  }, [year2, availablePeriods]);

  useEffect(() => {
  if (!availablePeriods?.length) return;

  // If user already has a valid selection (or storage restored it), don't override
  const hasValid =
    month1 && year1 && month2 && year2 &&
    isPeriodAvailable(year1, month1) &&
    isPeriodAvailable(year2, month2);

  if (hasValid) return;

  const def = pickDefaultComparePeriods(availablePeriods);
  if (!def) return;

  const [y2, m2] = def.newer.split('-'); // newer => Month2
  const [y1, m1] = def.older.split('-'); // older => Month1

  setYear1(y1);
  setMonth1(m1);
  setYear2(y2);
  setMonth2(m2);

  setAutoCompared(false);

  // optional: auto compare on load
  // handleSubmit(); // (If you want auto fetch immediately)
}, [availablePeriods]);


  // =====================
  // Fetch compare result
  // =====================
  const handleSubmit = async (e?: React.FormEvent) => {
    e?.preventDefault?.();
    setError(null);
    setCategorizedGrowth({ top_80_skus: [], new_or_reviving_skus: [], other_skus: [] });
    
    setMonth2Label('');
    setSkuInsights({});
    setModalOpen(false);
    // Clear previous persisted insights if any (fresh compare)
    saveInsightsToStorage({});

    // ✅ NEW: basic + availability validation
    if (!month1 || !year1 || !month2 || !year2) {
      setError('Please select both months and years.');
      return;
    }
    if (!isPeriodAvailable(year1, month1) || !isPeriodAvailable(year2, month2)) {
      setError('Selected month ka data available nahi hai. Sirf highlighted months select karein.');
      return;
    }

    try {
     const res = await api.get<ApiResponse>('/MonthsforBI', {
  params: { month1, year1, month2, year2, countryName },
});

const newMonth2Label = res.data?.comparison_range?.month2_label || '';

// ✅ normalize here
const raw = res.data?.categorized_growth;
const newCategorized = normalizeCategorizedGrowth(raw);

setMonth2Label(newMonth2Label);
setCategorizedGrowth(newCategorized);
setReimbursementTotals(res.data?.reimbursement_totals ?? null);
setAdvertisingTotals(res.data?.advertising_totals ?? null);
setExpenseTotals(res.data?.expense_totals ?? null);

// Save to localStorage (persist compare)
saveCompareToStorage({
  month1, year1, month2, year2,
  categorizedGrowth: newCategorized,
  month2Label: newMonth2Label,
  activeTab,
  countryName
});

    } catch (err: any) {
      console.error('MonthsforBI error:', err?.response?.data || err.message);
      setError(err?.response?.data?.error || 'An error occurred');
    }
  };

  useEffect(() => {
  // auto compare only after defaults are set AND only once
  if (autoCompared) return;

  if (!month1 || !year1 || !month2 || !year2) return;

  // make sure chosen periods are valid
  if (!isPeriodAvailable(year1, month1) || !isPeriodAvailable(year2, month2)) return;

  // run compare automatically
  handleSubmit();
  setAutoCompared(true);
  // eslint-disable-next-line react-hooks/exhaustive-deps
}, [month1, year1, month2, year2, availablePeriods, autoCompared]);


  // =====================
  // AI insights generate
  // =====================
  const analyzeSkus = async () => {
    setLoadingInsight(true);
    try {
      const allSkus: SkuItem[] = [
        ...categorizedGrowth.top_80_skus,
        ...categorizedGrowth.new_or_reviving_skus,
        ...categorizedGrowth.other_skus,
      ];
      const res = await api.post<{ insights: Record<string, SkuInsight> }>('/analyze_skus', {
        month1, year1, month2, year2,
        country: countryName,
        skus: allSkus,
      });
      const insights = res.data?.insights || {};
      setSkuInsights(insights);

      // persist insights so returning to route still shows "View Insights" etc.
      saveInsightsToStorage(insights);
    } catch (err: any) {
      console.error('analyze_skus error:', err?.response?.data || err.message);
    } finally {
      setLoadingInsight(false);
    }
  };

  // =====================
  // Insight lookups
  // =====================
  const getInsightByProductName = (productName: string): [string, SkuInsight] | null => {
    if (!productName) return null;
    const needle = productName.toLowerCase().trim();

    // Prefer exact match
    let entry = Object.entries(skuInsights).find(
      ([, d]) => d.product_name?.toLowerCase().trim() === needle
    );
    // For GLOBAL, allow partial fallback
    if (!entry && isGlobalData()) {
      entry = Object.entries(skuInsights).find(([, d]) => {
        const n = d.product_name?.toLowerCase().trim();
        return n && (n.includes(needle) || needle.includes(n));
      });
    }
    return entry ? entry as [string, SkuInsight] : null; // [key, value]
  };

  const getInsightForItem = (item: SkuItem): [string, SkuInsight] | null => {
    if (isGlobalData()) return getInsightByProductName(item.product_name);
    if (item.sku && skuInsights[item.sku]) return [item.sku, skuInsights[item.sku]];
    return getInsightByProductName(item.product_name);
  };

  const getCurrencySymbol = () => {
  // Global => $, UK => £, baaki default $
  const c = (countryName || '').toLowerCase();
  if (c === 'uk') return '£';
  if (c === 'global') return '$';
  return '$';
};

const fmtNum = (v: any) => Math.round(Number(v || 0)).toLocaleString();

const SERIES_ORDER = [
  { name: 'Top 80%', color: '#F47A00' },
  { name: 'Other SKUs', color: '#AB64B5' },
  { name: 'New/Reviving', color: '#87AD12' },
];


  // =====================
  // Export to Excel
  // =====================
const exportToExcel = (rows: SkuItem[], filename = 'export.xlsx') => {
  // ✅ IMPORTANT: backend fields are tied to month1(old) / month2(new). Keep fixed mapping.
  const isM2New = true;

  const newMonth = isM2New ? month2 : month1;
  const newYear = isM2New ? year2 : year1;
  const oldMonth = isM2New ? month1 : month2;
  const oldYear = isM2New ? year1 : year2;

  const newAbbr = `${getAbbr(newMonth)}'${String(newYear).slice(2)}`;
  const oldAbbr = `${getAbbr(oldMonth)}'${String(oldYear).slice(2)}`;

  // 1) remove any existing total rows coming from API/data
  const cleanRows = (rows || []).filter((r) => {
    const name = String(r?.product_name || '').toLowerCase().trim();
    return name !== 'total' && !name.includes('total (top 80') && name !== 'total (top 80%)';
  });

  const num = (v: any) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  };

  const round2 = (v: any) => {
    const n = Number(v);
    return Number.isFinite(n) ? Number(n.toFixed(2)) : null;
  };

  // helper to pick month1/month2 value based on which is new/old
  const pickNew = (row: any, keyMonth1: string, keyMonth2: string) =>
    isM2New ? row?.[keyMonth2] : row?.[keyMonth1];

  const pickOld = (row: any, keyMonth1: string, keyMonth2: string) =>
    isM2New ? row?.[keyMonth1] : row?.[keyMonth2];

  const pct = (oldV: number, newV: number) => (oldV ? ((newV - oldV) / oldV) * 100 : null);

  // ✅ EXACT column order (NEW month first, then OLD month)
  const headerOrder = [
    'SKU',
    'Product',

    `Qty ${newAbbr}`,
    `Qty ${oldAbbr}`,
    'Change in Qty (%age)',

    `Gross Sales ${newAbbr}`,
    `Gross Sales ${oldAbbr}`,
    'Change in Gross Sales (%age)',

    `Net Sales ${newAbbr}`,
    `Net Sales ${oldAbbr}`,
    'Change in Net Sales (%age)',

    `ASP ${newAbbr}`,
    `ASP ${oldAbbr}`,
    'Change in ASP (%age)',

    `Sales Mix ${newAbbr}`,
    `Sales Mix ${oldAbbr}`,
    'Change in Sales Mix (%age)',

    `CM1 Profit ${newAbbr}`,
    `CM1 Profit ${oldAbbr}`,
    'Change in CM1 Profit',

    `CM1 Profit %age(${newAbbr})`,
    `CM1 Profit %age(${oldAbbr})`,

    `CM1 Unit Profit ${newAbbr}`,
    `CM1 Unit Profit ${oldAbbr}`,
    'Change in Unit Profit (%age)',
  ];

  /**
   * ✅ Percent formatting:
   * formats columns whose header contains "%" OR starts with "Sales Mix "
   * for rows below the provided header row until it hits a blank separator row.
   */
  const addPercentToPercentColumns = (ws: XLSX.WorkSheet, headerRowIndexes: number[] = [0]) => {
    const ref = ws["!ref"];
    if (!ref) return;

    const range = XLSX.utils.decode_range(ref);
    const isSalesMixHeader = (h: string) => h.trim().toLowerCase().startsWith("sales mix ");

    for (const headerRow of headerRowIndexes) {
      if (headerRow < range.s.r || headerRow > range.e.r) continue;

      for (let C = range.s.c; C <= range.e.c; C++) {
        const headerCell = ws[XLSX.utils.encode_cell({ r: headerRow, c: C })];
        const header = String(headerCell?.v ?? "");

        const shouldFormatAsPercent = header.includes("%") || isSalesMixHeader(header);
        if (!shouldFormatAsPercent) continue;

        for (let R = headerRow + 1; R <= range.e.r; R++) {
          // stop at blank separator row
          const a0 = ws[XLSX.utils.encode_cell({ r: R, c: 0 })];
          const b0 = ws[XLSX.utils.encode_cell({ r: R, c: 1 })];
          const rowLooksBlank =
            (!a0 || a0.v == null || a0.v === "") &&
            (!b0 || b0.v == null || b0.v === "");
          if (rowLooksBlank) break;

          const addr = XLSX.utils.encode_cell({ r: R, c: C });
          const cell = ws[addr];
          if (!cell || cell.v == null || cell.v === "") continue;

          const n = Number(cell.v);
          if (!Number.isFinite(n)) continue;

          ws[addr] = { ...cell, t: "n", v: n, z: '0.00"%"' };
        }
      }
    }
  };

  // -------------------------
  // Shared: formatter for a section (adds a Total row at bottom)
  // -------------------------
  const formatRowsWithTotals = (inputRows: SkuItem[]) => {
    const clean = (inputRows || []).filter((r) => {
      const name = String(r?.product_name || '').toLowerCase().trim();
      return name !== 'total' && !name.includes('total (top 80') && name !== 'total (top 80%)';
    });

    const formatted = clean.map((row) => {
      const unitGrowth = row['Unit Growth'] as GrowthCategory | undefined;
      const aspGrowth = row['ASP Growth'] as GrowthCategory | undefined;
      const grossSalesGrowth = row['Gross Sales Growth'] as GrowthCategory | undefined;
      const netSalesGrowth = row['Net Sales Growth'] as GrowthCategory | undefined;
      const mixGrowth = row['Sales Mix Change'] as GrowthCategory | undefined;
      const unitProfitGrowth = row['Profit Per Unit'] as GrowthCategory | undefined;

      const qtyOld = pickOld(row, 'quantity_month1', 'quantity_month2');
      const qtyNew = pickNew(row, 'quantity_month1', 'quantity_month2');

      const gsOld = pickOld(row, 'product_sales_month1', 'product_sales_month2');
      const gsNew = pickNew(row, 'product_sales_month1', 'product_sales_month2');

      const nsOld = pickOld(row, 'net_sales_month1', 'net_sales_month2');
      const nsNew = pickNew(row, 'net_sales_month1', 'net_sales_month2');

      const aspOld = pickOld(row, 'asp_month1', 'asp_month2');
      const aspNew = pickNew(row, 'asp_month1', 'asp_month2');

      const mixOld = pickOld(row, 'sales_mix_month1', 'sales_mix_month2');
      const mixNew = pickNew(row, 'sales_mix_month1', 'sales_mix_month2') ?? row?.['Sales Mix (Month2)'];

      const cm1Old = pickOld(row, 'profit_month1', 'profit_month2');
      const cm1New = pickNew(row, 'profit_month1', 'profit_month2');

      const cm1PctOld = pickOld(row, 'profit_percentage_month1', 'profit_percentage_month2');
      const cm1PctNew = pickNew(row, 'profit_percentage_month1', 'profit_percentage_month2');

      const upOld = pickOld(row, 'unit_wise_profitability_month1', 'unit_wise_profitability_month2');
      const upNew = pickNew(row, 'unit_wise_profitability_month1', 'unit_wise_profitability_month2');

      return {
        SKU: row.sku || '',
        Product: row.product_name || '',

        [`Qty ${newAbbr}`]: qtyNew ?? null,
        [`Qty ${oldAbbr}`]: qtyOld ?? null,
        'Change in Qty (%age)': unitGrowth?.value ?? null,

        [`Gross Sales ${newAbbr}`]: gsNew ?? null,
        [`Gross Sales ${oldAbbr}`]: gsOld ?? null,
        'Change in Gross Sales (%age)': grossSalesGrowth?.value ?? null,

        [`Net Sales ${newAbbr}`]: nsNew ?? null,
        [`Net Sales ${oldAbbr}`]: nsOld ?? null,
        'Change in Net Sales (%age)': netSalesGrowth?.value ?? null,

        [`ASP ${newAbbr}`]: round2(aspNew ?? null),
        [`ASP ${oldAbbr}`]: round2(aspOld ?? null),
        'Change in ASP (%age)': aspGrowth?.value ?? null,

        [`Sales Mix ${newAbbr}`]: mixNew ?? null,
        [`Sales Mix ${oldAbbr}`]: mixOld ?? null,
        'Change in Sales Mix (%age)': mixGrowth?.value ?? null,

        [`CM1 Profit ${newAbbr}`]: cm1New ?? null,
        [`CM1 Profit ${oldAbbr}`]: cm1Old ?? null,
        'Change in CM1 Profit': cm1New != null && cm1Old != null ? Number(cm1New) - Number(cm1Old) : null,

        [`CM1 Profit %age(${newAbbr})`]: cm1PctNew ?? null,
        [`CM1 Profit %age(${oldAbbr})`]: cm1PctOld ?? null,

        [`Unit Profit ${newAbbr}`]: upNew ?? null,
        [`Unit Profit ${oldAbbr}`]: upOld ?? null,
        'Change in Unit Profit (%age)': unitProfitGrowth?.value ?? null,
      };
    });

    // totals
    const totals = clean.reduce(
      (acc, r) => {
        acc.qtyOld += num(pickOld(r, 'quantity_month1', 'quantity_month2'));
        acc.qtyNew += num(pickNew(r, 'quantity_month1', 'quantity_month2'));

        acc.gsOld += num(pickOld(r, 'product_sales_month1', 'product_sales_month2'));
        acc.gsNew += num(pickNew(r, 'product_sales_month1', 'product_sales_month2'));

        acc.nsOld += num(pickOld(r, 'net_sales_month1', 'net_sales_month2'));
        acc.nsNew += num(pickNew(r, 'net_sales_month1', 'net_sales_month2'));

        acc.mixOld += num(pickOld(r, 'sales_mix_month1', 'sales_mix_month2'));
        acc.mixNew += num(pickNew(r, 'sales_mix_month1', 'sales_mix_month2') ?? r?.['Sales Mix (Month2)']);

        acc.cm1Old += num(pickOld(r, 'profit_month1', 'profit_month2'));
        acc.cm1New += num(pickNew(r, 'profit_month1', 'profit_month2'));

        acc.upOld += num(pickOld(r, 'unit_wise_profitability_month1', 'unit_wise_profitability_month2'));
        acc.upNew += num(pickNew(r, 'unit_wise_profitability_month1', 'unit_wise_profitability_month2'));

        return acc;
      },
      { qtyOld: 0, qtyNew: 0, gsOld: 0, gsNew: 0, nsOld: 0, nsNew: 0, mixOld: 0, mixNew: 0, cm1Old: 0, cm1New: 0, upOld: 0, upNew: 0 }
    );

    const safeDiv = (a: number, b: number) => (b ? a / b : null);
    const totalAspOld = round2(safeDiv(totals.nsOld, totals.qtyOld));
    const totalAspNew = round2(safeDiv(totals.nsNew, totals.qtyNew));

    const profitPct = (profit: number, sales: number) => (sales ? (profit / sales) * 100 : null);
    const totalCm1PctOld = profitPct(totals.cm1Old, totals.nsOld);
    const totalCm1PctNew = profitPct(totals.cm1New, totals.nsNew);

    const totalSalesMixOld = round2(totals.mixOld);
    const totalSalesMixNew = round2(totals.mixNew);
    const totalSalesMixChange =
      totalSalesMixOld != null && totalSalesMixNew != null ? pct(totalSalesMixOld, totalSalesMixNew) : null;

    formatted.push({
      SKU: '',
      Product: 'Total',

      [`Qty ${newAbbr}`]: totals.qtyNew,
      [`Qty ${oldAbbr}`]: totals.qtyOld,
      'Change in Qty (%age)': pct(totals.qtyOld, totals.qtyNew),

      [`Gross Sales ${newAbbr}`]: totals.gsNew,
      [`Gross Sales ${oldAbbr}`]: totals.gsOld,
      'Change in Gross Sales (%age)': pct(totals.gsOld, totals.gsNew),

      [`Net Sales ${newAbbr}`]: totals.nsNew,
      [`Net Sales ${oldAbbr}`]: totals.nsOld,
      'Change in Net Sales (%age)': pct(totals.nsOld, totals.nsNew),

      [`ASP ${newAbbr}`]: totalAspNew,
      [`ASP ${oldAbbr}`]: totalAspOld,
      'Change in ASP (%age)': totalAspOld != null && totalAspNew != null ? pct(totalAspOld, totalAspNew) : null,

      [`Sales Mix ${newAbbr}`]: totalSalesMixNew,
      [`Sales Mix ${oldAbbr}`]: totalSalesMixOld,
      'Change in Sales Mix (%age)': totalSalesMixChange,

      [`CM1 Profit ${newAbbr}`]: totals.cm1New,
      [`CM1 Profit ${oldAbbr}`]: totals.cm1Old,
      'Change in CM1 Profit': totals.cm1New - totals.cm1Old,

      [`CM1 Profit %age(${newAbbr})`]: totalCm1PctNew,
      [`CM1 Profit %age(${oldAbbr})`]: totalCm1PctOld,

      [`Unit Profit ${newAbbr}`]: totals.upNew,
      [`Unit Profit ${oldAbbr}`]: totals.upOld,
      'Change in Unit Profit (%age)': pct(totals.upOld, totals.upNew),
    });

    return formatted;
  };

console.log(Object.keys(categorizedGrowth.top_80_skus?.[0] || {}));


  // -------------------------
  // Sheet 1: All SKUs (Growth Comparison)
  // -------------------------
  const formattedAll = formatRowsWithTotals(cleanRows);

  const ws1 = XLSX.utils.json_to_sheet(formattedAll, { header: headerOrder });
  XLSX.utils.sheet_add_aoa(ws1, [headerOrder], { origin: 'A1' });
  addPercentToPercentColumns(ws1, [0]);
  ws1['!freeze'] = { xSplit: 0, ySplit: 1 };

  // -------------------------
  // Sheet 2: SKU Split (3 sections + ✅ only Grand Total row)
  // -------------------------
  const splitHeader = [...headerOrder];
  const aoa: any[][] = [];

  const makeSectionAoA = (sectionTitle: string, sectionRows: SkuItem[]) => {
    const formatted = formatRowsWithTotals(sectionRows);

    const body = formatted.map((obj) => {
      const rowArr: any[] = [];
      for (const h of headerOrder) rowArr.push((obj as any)[h] ?? null);
      return rowArr;
    });

    const titleRow = [sectionTitle];
    while (titleRow.length < splitHeader.length) titleRow.push('');

    return { titleRow, headerRow: splitHeader, body };
  };

  const top80 = categorizedGrowth.top_80_skus || [];
  const newRev = categorizedGrowth.new_or_reviving_skus || [];
  const other = categorizedGrowth.other_skus || [];

  const pushSection = (title: string, sectionRows: SkuItem[]) => {
    const { titleRow, headerRow, body } = makeSectionAoA(title, sectionRows);
    aoa.push(titleRow);
    aoa.push(headerRow);
    aoa.push(...body);
    aoa.push([]); // blank row gap
  };

  pushSection('Top 80% SKUs', top80);
  pushSection('New/Reviving SKUs', newRev);
  pushSection('Other SKUs', other);

  // ✅ Append Grand Total title + header + ONLY the grand total row (not full table)
  {
    const grandTitleRow = ['Grand Total'];
    while (grandTitleRow.length < splitHeader.length) grandTitleRow.push('');

    // formatRowsWithTotals adds many rows + a Total at the end.
    // We only want the final "Total" row.
    const grandFormatted = formatRowsWithTotals([...top80, ...newRev, ...other]);
    const grandTotalObj = grandFormatted[grandFormatted.length - 1]; // the "Total" row

    const grandTotalRow = headerOrder.map((h) => (grandTotalObj as any)?.[h] ?? null);

    aoa.push(grandTitleRow);
    aoa.push(splitHeader);
    aoa.push(grandTotalRow);
  }

  const ws2 = XLSX.utils.aoa_to_sheet(aoa);
  ws2['!freeze'] = { xSplit: 0, ySplit: 2 };

  // ✅ apply percent formatting for EVERY repeated header row (3 sections + grand total)
  // In SKU Split, each table header row is the row where col A is "SKU".
  const findHeaderRows = (ws: XLSX.WorkSheet) => {
    const ref = ws["!ref"];
    if (!ref) return [0];
    const range = XLSX.utils.decode_range(ref);

    const rows: number[] = [];
    for (let R = range.s.r; R <= range.e.r; R++) {
      const a = ws[XLSX.utils.encode_cell({ r: R, c: 0 })];
      if (String(a?.v ?? "").trim() === "SKU") rows.push(R);
    }
    return rows.length ? rows : [0];
  };

  const ws2HeaderRows = findHeaderRows(ws2);
  addPercentToPercentColumns(ws2, ws2HeaderRows);

  // -------------------------
  // Build workbook with 2 sheets
  // -------------------------
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws1, 'All SKUs');
  XLSX.utils.book_append_sheet(wb, ws2, 'SKU Split');

  XLSX.writeFile(wb, filename);
};

const allRows = useMemo(() => ([
  ...(categorizedGrowth.top_80_skus || []),
  ...(categorizedGrowth.new_or_reviving_skus || []),
  ...(categorizedGrowth.other_skus || []),
]), [categorizedGrowth]);

const sumKey = (k: string) =>
  allRows.reduce((a, r: any) => a + Number(r?.[k] ?? 0), 0);

const m1Label = `${getAbbr(month1)}'${String(year1).slice(2)}`;
const m2Label = `${getAbbr(month2)}'${String(year2).slice(2)}`;
const currency = getCurrencySymbol(); // same helper you already have

const totalsLine = useMemo(() => {
  const netSales_m1 = sumKey("net_sales_month1");
  const netSales_m2 = sumKey("net_sales_month2");

  const profit_m1 = sumKey("profit_month1");
  const profit_m2 = sumKey("profit_month2");

  const otherExp_m1 = expenseTotals?.month1 ?? 0;
  const otherExp_m2 = expenseTotals?.month2 ?? 0;

  const adv_m1 = advertisingTotals?.month1 ?? 0;
  const adv_m2 = advertisingTotals?.month2 ?? 0;

  const reimb_m1 = reimbursementTotals?.month1 ?? 0;
  const reimb_m2 = reimbursementTotals?.month2 ?? 0;

  // ✅ YAHAN ADD KARO (return se just pehle)
  const ds = [
    { key: "netSales", label: "Net Sales", data: [netSales_m1, netSales_m2], color: "#2CA9E0" },
    { key: "cm1Profit", label: "CM1 Profit", data: [profit_m1, profit_m2], color: "#5EA49B" },
    { key: "otherExpense", label: "Other Expense", data: [otherExp_m1, otherExp_m2], color: "#00627D" },
    { key: "advertising", label: "Advertising Total", data: [adv_m1, adv_m2], color: "#F47A00" },
    { key: "reimbursement", label: "Reimbursement", data: [reimb_m1, reimb_m2], color: "#AB64B5" },
  ];

  const datasets = ds
    .filter((d) => selectedTotals[d.key])
    .map((d) => ({
      label: d.label,
      data: d.data,
      fill: false,
      borderColor: d.color,
      backgroundColor: d.color,
      tension: 0.1,
    }));

  // ✅ RETURN ME OLD datasets array replace karke yeh use karo
  return {
    data: {
      labels: [m1Label, m2Label],
      datasets, // ✅
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: "index" as const },
      plugins: {
        legend: { display: false },
       tooltip: {
    mode: "index" as const,
    intersect: false,

    backgroundColor: "#ffffff",      // ✅ white bg
    titleColor: "#111827",           // ✅ dark text
    bodyColor: "#111827",
    borderColor: "#e5e7eb",          // ✅ light border
    borderWidth: 1,
    cornerRadius: 8,
    padding: 10,

    callbacks: {
      label: (tooltipItem: any) => {
        const label = tooltipItem.dataset.label || "";
        const value = tooltipItem.raw as number;
        return `${label}: ${currency}${Number(value ?? 0).toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })}`;
      },
    },
  },
      },
      scales: {
        x: { title: { display: true, text: "Month" } },
        y: { title: { display: true, text: `Amount (${currency})` } },
      },
    },
  };
}, [
  m1Label, m2Label, currency,
  expenseTotals, advertisingTotals, reimbursementTotals,
  selectedTotals, // ✅ IMPORTANT dependency
  allRows,        // (or categorizedGrowth deps)
]);



  // =====================
  // Save feedback (Summary)
  // =====================
  const submitSummaryFeedback = async () => {
    try {
      if (!selectedSku) return;
      if (!fbType) {
        setError('Please choose 👍 or 👎 before submitting.');
        return;
      }

      setFbSubmitting(true);
      setError(null);

      // Current insight
      const insightData = skuInsights[selectedSku as keyof typeof skuInsights] || getInsightByProductName(selectedSku as string)?.[1];
      const productName = insightData?.product_name || selectedSku;

      // ⬇️ send FULL text; no front-end truncation
      const fullInsightText = (insightData?.insight || '');

      // Find row index for active tab (optional)
      const currentRows = categorizedGrowth[activeTab] || [];
      const rowIndex = Math.max(
        currentRows.findIndex((r) =>
          (r.sku && r.sku === selectedSku) ||
          (r.product_name &&
            r.product_name.toLowerCase().trim() === String(productName).toLowerCase().trim())
        ),
        -1
      );

      const payload = {
        country: countryName,
        rowIndex: rowIndex === -1 ? 0 : rowIndex,
        tab: getTabNumberForFeedback(activeTab),
        type: fbType,             // 'like' | 'dislike'
        text: fbText || '',
        productData: {
          product_name: productName,
          // backend prefers combined_text; also send raw as fallback
          combined_text: fullInsightText,
          raw_ai_response: fullInsightText,
        },
      };

      await api.post('/row-feedback', payload, {
        headers: { 'Content-Type': 'application/json' },
      });

      setFbSuccess(true);
      setTimeout(() => setFbSuccess(false), 2500);
      setFbText('');
      setFbType(null);
    } catch (err: any) {
      console.error('row-feedback error:', err?.response?.data || err.message);
      setError(
        err?.response?.data?.error ||
          err?.response?.data?.message ||
          'Failed to submit feedback. Please try again.'
      );
    } finally {
      setFbSubmitting(false);
    }
  };

  // =====================
  // Insight renderer (headings + bullets)
  // =====================

  
  useEffect(() => {
  // whenever compare/month changes -> categorizedGrowth updates -> reset Others view
  setExpandAllSkusOthers(false);
}, [categorizedGrowth]);




const highlightInsightText = (text: string) => {
  const greenWords = [
    'profit',
    'profits',
    'increase',
    'growth',
    'improvement',
    'gain',
    'gains',
    'up',
    'higher',
  ];

  const redWords = [
    'loss',
    'losses',
    'decrease',
    'decline',
    'drop',
    'down',
    'lower',
  ];

  const regex = new RegExp(
    `\\b(${[...greenWords, ...redWords].join('|')})\\b`,
    'gi'
  );

  const parts = text.split(regex);

  return parts.map((part, idx) => {
    const lower = part.toLowerCase();

    if (greenWords.includes(lower)) {
      return (
        <span key={idx} style={{ color: '#16a34a', fontWeight: 600 }}>
          {part}
        </span>
      );
    }

    if (redWords.includes(lower)) {
      return (
        <span key={idx} style={{ color: '#dc2626', fontWeight: 600 }}>
          {part}
        </span>
      );
    }

    return <span key={idx}>{part}</span>;
  });
};



  const renderFormattedInsight = (raw: string) => {
  if (!raw) return null;

  const lines = raw
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);

  const SECTION_ORDER = [
    'Details',
    'Observations',
    'Improvements',
    'Unit Growth',
    'ASP',
    'Sales',
    'Profit',
    'Unit Profitability',
    'Summary',
  ];

  const LIST_SECTIONS = new Set([
    'Observations',
    'Improvements',
    'Unit Growth',
    'ASP',
    'Sales',
    'Profit',
    'Unit Profitability',
  ]);

  const headingOf = (line: string): string | null => {
    const m =
      line.match(/^details\s+for/i) ? ['Details'] :
      line.match(/^(observations)\s*:?\s*$/i) ? ['Observations'] :
      line.match(/^(improvements)\s*:?\s*$/i) ? ['Improvements'] :
      line.match(/^(unit\s+growth)\s*:?\s*$/i) ? ['Unit Growth'] :
      line.match(/^(asp)\s*:?\s*$/i) ? ['ASP'] :
      line.match(/^(sales)\s*:?\s*$/i) ? ['Sales'] :
      line.match(/^(profit)\s*:?\s*$/i) ? ['Profit'] :
      line.match(/^(unit\s+profitability)\s*:?\s*$/i) ? ['Unit Profitability'] :
      line.match(/^(summary)\s*:?\s*$/i) ? ['Summary'] :
      null;
    return m ? m[0] : null;
  };

  const sections: Record<string, string[]> = {};
  let current: string | null = null;

  for (const line of lines) {
    const hd = headingOf(line);
    if (hd) {
      current = hd;
      if (!sections[current]) sections[current] = [];
      if (current === 'Details') sections[current].push(line);
      continue;
    }
    if (!current) current = 'Details';
    if (!sections[current]) sections[current] = [];
    const isLabel = !!line.match(
      /^(observations|improvements|unit\s+growth|asp|sales|profit|unit\s+profitability|summary)\s*:?\s*$/i
    );
    if (isLabel) continue;
    sections[current].push(line);
  }

  const clean = (s: string) =>
    s.replace(/^[•\-\u2013\u2014]\s+/, '').replace(/^\d+\.\s+/, '');

  return SECTION_ORDER.filter((sec) => sections[sec]?.length).map(
    (sec, idx) => {
      const content = sections[sec];
      const isList = LIST_SECTIONS.has(sec);

      return (
        <div
          key={idx}
          className="insight-section"
          style={{ marginBottom: 12 }}
        >
          {(isList || sec === 'Summary') && (
            <strong
              className="insight-section-title"
              style={{ display: 'block', marginBottom: 6 }}
            >
              {sec}
            </strong>
          )}

         {isList ? (
  <ul className="insight-list list-disc">
    {content.map((line, i) => {
      const trimmed = clean(line);

      // 🔹 Detect ANY subheading
      const isSubHeading =
        /^[A-Za-z][A-Za-z\s\/]+:?$/i.test(trimmed) &&  // text only
        !trimmed.match(/\d|%|,/) &&                    // no numbers/percent
        trimmed.split(/\s+/).length <= 5;              // short phrase

      if (isSubHeading) {
        const label = trimmed.replace(/:$/, '').trim();
        return (
          <li
            key={i}
            style={{
              listStyle: 'none',
              marginTop: 10,
              marginBottom: 4,
            }}
          >
            <span
              style={{
                fontWeight: 700,
                fontSize: 14,
                color: '#374151',
                borderLeft: '3px solid #60a68e',
                paddingLeft: 8,
              }}
            >
              {label}
            </span>
          </li>
        );
      }

      // 🔹 Normal bullet points
      return (
        <li
          key={i}
          className="insight-list-item"
          style={{
            marginBottom: 4,
            lineHeight: 1.6,
            fontSize: 13,
          }}
        >
          {highlightInsightText(trimmed)}
        </li>
      );
    })}
  </ul>
) : (
  <div className="insight-paragraphs list-disc">
    {content.map((line, i) => (
      <p
        key={i}
        className="insight-paragraph"
        style={{
          margin: '4px 0',
          lineHeight: 1.6,
          fontSize: 13,
        }}
      >
        {highlightInsightText(line)}
      </p>
    ))}
  </div>
)}


          {sec === 'Summary' && (
            <div className="feedback-container" style={{ marginTop: 10 }}>
              <div
                className="feedback-buttons"
                style={{
                  display: 'flex',
                  justifyContent: 'center',
                  gap: 12,
                }}
              >
                <button
                  type="button"
                  className="feedback-button"
                  onClick={() => setFbType('like')}
                  title="Like"
                  style={{
                    background: 'none',
                    border: 'none',
                    cursor: 'pointer',
                    opacity: fbType === 'like' ? 1 : 0.6,
                  }}
                >
                  <FaThumbsUp size={18} />
                </button>
                <button
                  type="button"
                  className="feedback-button"
                  onClick={() => setFbType('dislike')}
                  title="Dislike"
                  style={{
                    background: 'none',
                    border: 'none',
                    cursor: 'pointer',
                    opacity: fbType === 'dislike' ? 1 : 0.6,
                  }}
                >
                  <FaThumbsDown size={18} />
                </button>
              </div>

              <div
                className="comment-box"
                style={{
                  marginTop: 10,
                  backgroundColor: '#f1f1f1',
                  padding: '10px 12px',
                  borderRadius: 8,
                  display: 'flex',
                  gap: 10,
                  alignItems: 'center',
                }}
              >
                <input
                  type="text"
                  placeholder="Add a Comment......"
                  className="comment-input"
                  value={fbText}
                  onChange={(e) => setFbText(e.target.value)}
                  style={{
                    flex: 1,
                    border: 'none',
                    outline: 'none',
                    background: 'transparent',
                  }}
                />
                <button
                  type="button"
                  onClick={submitSummaryFeedback}
                  disabled={fbSubmitting}
                  className="styled-button"
                  style={{ whiteSpace: 'nowrap' }}
                >
                  {fbSubmitting ? 'Submitting...' : 'Submit'}
                </button>
              </div>

              {fbSuccess && (
                <div
                  style={{
                    color: '#2e7d32',
                    fontWeight: 600,
                    marginTop: 6,
                  }}
                >
                  Feedback submitted!
                </div>
              )}
            </div>
          )}
        </div>
      );
    }
  );
};

  const getAllSkusForExport = (): SkuItem[] => {
  return [
    ...(categorizedGrowth.top_80_skus || []),
    ...(categorizedGrowth.new_or_reviving_skus || []),
    ...(categorizedGrowth.other_skus || []),
  ];
};

  const fullCurrent = categorizedGrowth[activeTab] || [];

const currentTabData = React.useMemo(() => {
  // ✅ full dataset for active tab (your real source)
  const fullCurrent = (categorizedGrowth?.[activeTab] || []) as SkuItem[];

  // If not All SKUs, return directly
  if (activeTab !== 'all_skus') return fullCurrent;

  // Sort by Sales Mix (Month2) desc (fallback -Infinity)
  const sorted = [...fullCurrent].sort((a, b) => {
    const am = Number(a?.['Sales Mix (Month2)'] ?? -Infinity);
    const bm = Number(b?.['Sales Mix (Month2)'] ?? -Infinity);
    return bm - am;
  });

  // If expanded, show all SKUs (no "Others" row)
  if (expandAllSkusOthers) {
    return sorted;
  }

  // Otherwise show top 5 + Others aggregation
  const top5 = sorted.slice(0, 5);
  const rest = sorted.slice(5);

  if (!rest.length) return top5;

  const sumNum = (arr: any[], key: string) =>
    arr.reduce((acc, r) => acc + (Number(r?.[key]) || 0), 0);

  // For GrowthCategory fields: compute weighted average by Net Sales (month2)
  const wAvgGrowth = (arr: any[], field: string) => {
    const wKey = 'net_sales_month2';
    const totalW = arr.reduce((s, r) => s + (Number(r?.[wKey]) || 0), 0);
    if (!totalW) return null;

    const val = arr.reduce((s, r) => {
      const g = r?.[field];
      const v = Number(g?.value ?? 0);
      const w = Number(r?.[wKey] ?? 0);
      return s + v * w;
    }, 0) / totalW;

    // category only for coloring; keep "Low Growth" so it stays neutral
    return { category: 'Low Growth', value: val } as GrowthCategory;
  };

  const others: any = {
    product_name: 'Others',

    // ✅ key MUST match UI
    'Sales Mix (Month2)': sumNum(rest, 'Sales Mix (Month2)'),

    // ✅ fields used by table mapping
    'Unit Growth': wAvgGrowth(rest, 'Unit Growth'),
    'ASP Growth': wAvgGrowth(rest, 'ASP Growth'),
    'Net Sales Growth': wAvgGrowth(rest, 'Net Sales Growth'),
    'Sales Mix Change': wAvgGrowth(rest, 'Sales Mix Change'),
    'CM1 Profit Impact': wAvgGrowth(rest, 'CM1 Profit Impact'),
    'Profit Per Unit': wAvgGrowth(rest, 'Profit Per Unit'),
  };

  return [...top5, others];
}, [categorizedGrowth, activeTab, expandAllSkusOthers]);



 const totalRow =
  activeTab === "top_80_skus"
    ? categorizedGrowth.top_80_total
    : activeTab === "new_or_reviving_skus"
    ? categorizedGrowth.new_or_reviving_total
    : activeTab === "other_skus"
    ? categorizedGrowth.other_total
    : categorizedGrowth.all_skus_total; // ✅ all_skus


    const formatCountryLabel = (country: string) => {
  const lower = country.toLowerCase();
  if (lower === "global") return "Global"; // special case
  return country.toUpperCase(); // UK, US, etc.
};

const currentYearStr = String(new Date().getFullYear());
const currentMonthStr = pad2(new Date().getMonth() + 1); // "01".."12"
const currentPeriodKey = `${currentYearStr}-${currentMonthStr}`;

const isCurrentPeriodAvailable = availablePeriods.includes(currentPeriodKey);

const isLockedCurrent = (year: string, month: string) => {
  if (!year || !month) return false;
  // Lock ONLY if backend has current month AND the option is current month
  return isCurrentPeriodAvailable && `${year}-${month}` === currentPeriodKey;
};


  return (
    <>
     <style>{`
  div{ font-family: 'Lato', sans-serif; }
  select{ outline: none; }
 
  .total-row td{ background-color:#ccc; font-weight:bold; }
  .styled-button, .compare-button{
    padding:8px 16px; font-size:.9rem; border:none; border-radius:6px; cursor:pointer;
    transition:background-color .2s ease; box-shadow:0 3px 6px rgba(0,0,0,.15);
    background-color:#2c3e50; color:#f8edcf; font-weight:bold;
  }
  .styled-button:hover, .compare-button:hover{ background-color:#1f2a36; }
  .month-form{ max-width:100%; margin:15px 0; border:1px solid #e4e7ec ; padding:10px; background:#fff; }
  .month-tag{ font-size:12px; font-weight:bold; color:#414042; position:absolute; top:-25px; }
  .highlight{ color:#60a68e; }
  .subtitle{ margin-top:0; color:#414042; font-size:14px; }

  .month-row{
    display:flex;
    align-items:center;
    margin-top:20px;
    gap:10px;
  }
  .year-dropdown{
    margin-right:10px;
    padding:6px;
    font-size:14px;
    border-radius:4px;
    border:1px solid #ccc;
  }
  .month-slider{
    margin-top:30px;
    display:flex;
    flex-grow:1;
    justify-content:space-between;
    padding:0 10px;
    position:relative;
    border-top:2px solid #ccc;
  }
  .month-dot{
    display:flex;
    flex-direction:column;
    align-items:center;
    cursor:pointer;
    position:relative;
    top:-6px;
  }
  .month-dot .dot{
    width:12px;
    height:12px;
    background:#ccc;
    border-radius:50%;
    margin-bottom:4px;
  }
  .month-dot.selected .dot{ background:#5EA68E; }

  /* ✅ NEW: disabled months styling */
  .month-dot.disabled{
    opacity:0.3;
    cursor:not-allowed;
  }
  .month-dot.disabled .dot{
    background:#eee;
  }

  .month-label{
    font-size:12px;
    color:#414042;
    white-space:nowrap;
  }
  .month-label-short{ display:none; }   /* default: desktop pe short hidden */
  .month-label-full{ display:inline; }

  @keyframes pulseRing {
  0%   { transform: scale(1);   box-shadow: 0 0 0 0 rgba(94,166,142,.45); }
  70%  { transform: scale(1.05); box-shadow: 0 0 0 10px rgba(94,166,142,0); }
  100% { transform: scale(1);   box-shadow: 0 0 0 0 rgba(94,166,142,0); }
}

.month-slider.needs-pick .month-dot:not(.disabled):not(.selected) .dot {
  animation: pulseRing 1.3s infinite;
}

.month-help {
  font-size: 12px;
  color: #5EA68E;
  font-weight: 700;
  margin-top: 6px;
}

  /* ======= Responsive changes (under lg) ======= */
  @media (max-width: 1023.98px){
    .month-row{
      flex-direction:column;
      align-items:stretch;
    }
    .year-dropdown{
      width:100%;
      margin-right:0;
    }
    .month-slider{
      margin-top:20px;
      padding:0 4px;
    }
    .month-label{
      font-size:10px;
    }
    .month-label-full{ display:none; }   /* mobile: sirf 3 letter show */
    .month-label-short{ display:inline; }

    .month-tag{
      top:-20px;
      font-size:11px;
    }
  }

  .compare-button-container{ margin-top:20px; text-align:right; }
  .theadc{ background:#5EA68E; color:#f8edcf; }
  .tablec{ width:100%; border-collapse:collapse; }
  .tablec td, .tablec th{ border:1px solid #414042; padding:10px 8px; text-align:center; }
  .insight-section-title{ font-size:15px; color:#414042; }
  .insight-list{ margin: 6px 0 10px 20px; padding:0; }
  .insight-list-item{ line-height:1.6; }
  .insight-paragraphs p{ margin:4px 0; line-height:1.6; }

  .sku-zero-wrap{
  position: relative;
  display: inline-flex;
  align-items: center;
  gap: 6px;
}
.sku-zero{
  color: #dc2626;
  font-weight: 700;
}
.sku-zero-tooltip{
  position: absolute;
  left: 0;
  top: 100%;
  transform: translateY(6px);
  background: #111827;
  color: #fff;
  padding: 6px 8px;
  font-size: 12px;
  border-radius: 6px;
  white-space: nowrap;
  opacity: 0;
  pointer-events: none;
  transition: opacity .15s ease;
  z-index: 50;
}
.sku-zero-wrap:hover .sku-zero-tooltip{
  opacity: 1;
}
`}</style>
<div className='w-full'>
      {/* Month selectors */}
      <h2 className="text-2xl font-bold text-[#414042] mb-2">
          Business Insights - AI Analyst&nbsp;- 
          <span className="text-[#5EA68E] pl-1">
              {countryName && formatCountryLabel(countryName)}<span className="text-[#5EA68E] px-2">
           {/* {month1 && year1 && month2 && year2
  ? (() => {
      const monthToIndex = (m: string) => {
        const s = String(m).trim().toLowerCase();

        // numeric month: "1".."12" or 1..12
        const n = Number(s);
        if (!Number.isNaN(n) && n >= 1 && n <= 12) return n - 1;

        // normalize to 3-letter
        const abbr = s.slice(0, 3);

        const map: Record<string, number> = {
          jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
          jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11,
        };

        return map[abbr] ?? -1; // -1 means unknown
      };

      const m1 = monthToIndex(month1);
      const m2 = monthToIndex(month2);

      const y1n = Number(year1);
      const y2n = Number(year2);

      // If month parsing failed, fallback to original order (safe)
      if (m1 === -1 || m2 === -1 || Number.isNaN(y1n) || Number.isNaN(y2n)) {
        const y1s = String(year1);
        const y2s = String(year2);
        return `(${getAbbr(month1)}'${y1s.slice(2)} vs ${getAbbr(month2)}'${y2s.slice(2)})`;
      }

      const d1 = new Date(y1n, m1, 1).getTime();
      const d2 = new Date(y2n, m2, 1).getTime();

      // NEW should come first
      const monthNewerFirst = d2 >= d1;

      const newMonth = monthNewerFirst ? month2 : month1;
      const newYear = monthNewerFirst ? year2 : year1;
      const oldMonth = monthNewerFirst ? month1 : month2;
      const oldYear = monthNewerFirst ? year1 : year2;

      const ny = String(newYear);
      const oy = String(oldYear);

      return `(${getAbbr(newMonth)}'${ny.slice(2)} vs ${getAbbr(oldMonth)}'${oy.slice(2)})`;
    })()
  : ''} */}

</span>
          </span>
        </h2>
        <p><i className="">Select the year and month for both periods to compare growth metrics.</i></p>
      <form onSubmit={handleSubmit} className="month-form ">
        {/* Row 1 */}
        <div className="month-row">
          <select value={year1} onChange={(e)=>setYear1(e.target.value)} className="year-dropdown">
            <option value="">Year 1</option>
            {years.map(y => <option key={y} value={y}>{y}</option>)}
          </select>
       <div className={`month-slider ${year1 && !month1 ? 'needs-pick' : ''}`}>
            {months.map(m => {
const disabled =
  !year1 ||
  !isPeriodAvailable(year1, m.value) ||
  isLockedCurrent(year1, m.value);
              const selected = month1 === m.value;
              return (
                <div
                  key={m.value}
                  className={`month-dot ${selected ? 'selected':''} ${disabled ? 'disabled' : ''}`}
                  onClick={() => {
                    if (disabled) return;
                    setMonth1(m.value);
                  }}
                >
                  {selected && !disabled && <div className="month-tag text-nowrap"></div>}
                  <span className="dot"></span>
                  <div className="month-label">
                    <span className="month-label-full">{m.label}</span>
                    <span className="month-label-short">{m.label.slice(0, 3)}</span>
                  </div>
                </div>
              );
            })}
          </div>
          {year1 && !month1 && (
  <div className="month-help">Tap any highlighted month to select Month 1</div>
)}
        </div>
        {/* Row 2 */}
        <div className="month-row">
          <select value={year2} onChange={(e)=>setYear2(e.target.value)} className="year-dropdown">
            <option value="">Year 2</option>
            {years.map(y => <option key={y} value={y}>{y}</option>)}
          </select>
<div className={`month-slider ${year2 && !month2 ? 'needs-pick' : ''}`}>
            {months.map(m => {
            const disabled =
  !year2 ||
  !isPeriodAvailable(year2, m.value) ||
  isLockedCurrent(year2, m.value);
              const selected = month2 === m.value;
              return (
                <div
                  key={m.value}
                  className={`month-dot ${selected ? 'selected':''} ${disabled ? 'disabled' : ''}`}
                  onClick={() => {
                    if (disabled) return;
                    setMonth2(m.value);
                  }}
                >
                  {selected && !disabled && <div className="month-tag text-nowrap"></div>}
                  <span className="dot"></span>
                  <div className="month-label">
                    <span className="month-label-full">{m.label}</span>
                    <span className="month-label-short">{m.label.slice(0, 3)}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </form>
       

      <div className="compare-button-container">
        <button type="submit" onClick={handleSubmit} className="compare-button">Compare</button>
      </div>

      {error && <p style={{ color: 'red' }}>{error}</p>}

     <div className="mt-4 mb-3 rounded-xl border border-gray-200  p-3 w-full bg-[#D9D9D933]">
  <div className="text-2xl font-bold text-[#414042]">Profitability</div>

 {/* Center labels like GraphPage */}
<div className="mt-3 sm:mt-4 flex flex-wrap items-center justify-center gap-3 sm:gap-4 md:gap-5 w-full mx-auto transition-opacity duration-300">
  {[
    { key: "netSales", label: "Net Sales", color: "#2CA9E0" },
    { key: "cm1Profit", label: "CM1 Profit", color: "#5EA49B" },
    { key: "otherExpense", label: "Other Expense", color: "#00627D" },
    { key: "advertising", label: "Advertising Total", color: "#F47A00" },
    { key: "reimbursement", label: "Reimbursement", color: "#AB64B5" },
  ].map(({ key, label, color }) => {
    const isChecked = !!selectedTotals[key];

    return (
      <label
        key={key}
        className={[
          "shrink-0 flex items-center gap-1 sm:gap-1.5",
          "font-semibold select-none whitespace-nowrap",
          "text-[9px] sm:text-[10px] md:text-[11px] lg:text-xs xl:text-sm",
          "text-charcoal-500",
          isChecked ? "opacity-100" : "opacity-40",
          "cursor-pointer",
        ].join(" ")}
      >
        <span
          className="flex items-center justify-center h-3 w-3 sm:h-3.5 sm:w-3.5 rounded-sm border transition"
          style={{
            borderColor: color,
            backgroundColor: isChecked ? color : "white",
          }}
          onClick={() => toggleTotalsMetric(key)}
        >
          {isChecked && (
            <svg viewBox="0 0 24 24" width="14" height="14" className="text-white">
              <path
                fill="currentColor"
                d="M20.285 6.709a1 1 0 0 0-1.414-1.414L9 15.168l-3.879-3.88a1 1 0 0 0-1.414 1.415l4.586 4.586a1 1 0 0 0 1.414 0l10-10Z"
              />
            </svg>
          )}
        </span>

        <span className="capitalize">{label}</span>
      </label>
    );
  })}
</div>

{/* chart */}
<div className=" h-[320px] sm:h-[360px] md:h-[400px] lg:h-[420px] w-full mt-2 sm:mt-3">
  <Line data={totalsLine.data as any} options={totalsLine.options as any} />
</div>

</div>


{/* ✅ ONE BOX */}
<div className="mt-4 mb-3 rounded-xl border border-gray-200 bg-white p-3">
 
  <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
    <div>
      <div className="text-2xl font-bold text-[#414042]">Units Sold</div>
      <div ref={unitsChartRef} className="h-[320px] w-full" />
    </div>

    <div>
      <div className="text-2xl font-bold text-[#414042]">Net Sales</div>
      <div ref={chartRef} className="h-[320px] w-full" />
    </div>
  

  {/* Row 2: Profit */}
  <div className="mt-3">
    <div className="text-2xl font-bold text-[#414042]">CM1 Profit</div>
    <div ref={profitChartRef} className="h-[320px] w-full" />
  </div>

  <div className="mt-3">
  <div className="text-2xl font-bold text-[#414042]">Average Selling Price</div>
  <div ref={aspChartRef} className="h-[320px] w-full" />
</div>
</div>

  {/* Shared legend bottom center */}
  <div className="mt-3 flex flex-wrap justify-center gap-4 text-[14px] font-semibold text-[#414042]">
    <span className="inline-flex items-center gap-2">
      <span className="inline-block h-[10px] w-[10px] rounded-full bg-[#F47A00]" />
      Top 80%
    </span>

    <span className="inline-flex items-center gap-2">
      <span className="inline-block h-[10px] w-[10px] rounded-full bg-[#AB64B5]" />
      Other SKUs
    </span>

    <span className="inline-flex items-center gap-2">
      <span className="inline-block h-[10px] w-[10px] rounded-full bg-[#87AD12]" />
      New/Reviving
    </span>
  </div>
</div>



      {/* Table + actions */}
{(['all_skus','top_80_skus','new_or_reviving_skus','other_skus'] as TabKey[]).some(
  (k) => (categorizedGrowth[k] || []).length > 0
) && (
        <div className='border border-gray-200 rounded-xl p-4 mt-6 w-full bg-white'>
          <div className='flex xl:flex-row flex-col lg:justify-between justify-start xl:items-center items-start '>
            <div className='flex xl:flex-row flex-col lg:justify-between justify-start xl:items-center items-start w-full'>
<h2 className="xl:text-2xl text-xl font-bold text-[#414042] mb-4">Performance-based SKU split</h2>
            <div className='flex justify-center gap-3'>

  <div
              style={{
               
                border: '1px solid #D9D9D9E5',
                borderRadius: 8,
                display: 'inline-flex',
                overflow: 'hidden'
              }}
              className='p-1'
            >
{(['all_skus','top_80_skus','new_or_reviving_skus','other_skus'] as TabKey[]).map(key => (
                <button
                  key={key}
onClick={() => setActiveTab(key)}
                  className="text-sm font-normal"
                  style={{
                    padding: '3px 12px',
                    backgroundColor: activeTab === key ? '#5EA68E80' : '#ffffff',
                    color: '#414042',
                    border: 'none',
                    borderRadius: 5,
                    fontWeight: activeTab === key ? 600 : 400
                  }}
                >
                  {getTabLabel(key as keyof CategorizedGrowth)}
                </button>
              ))}
            </div>
            
            <div className='flex gap-3'>
 <button
              onClick={analyzeSkus}
disabled={!['top_80_skus','new_or_reviving_skus','other_skus'].some(
  (k) => (categorizedGrowth[k as keyof CategorizedGrowth] as SkuItem[])?.length > 0
)}
              className="bg-custom-effect text-[#F8EDCE] rounded-sm xl:px-4 px-3 text-nowrap flex items-center justify-end  disabled:opacity-50 disabled:cursor-not-allowed"
               style={{
                 boxShadow: "0px 4px 4px 0px #00000040",
               }}
             >
               <BsStars 
                 style={{ 
                   fontSize: "12px", 
                   color: "#F8EDCE" 
                 }} 
               />
               {loadingInsight ? "Generating..." : "AI Insights"}
            </button>
            <button
              onClick={() => {
                const file = `AllSKUs-${getAbbr(month1)}'${String(year1).slice(2)}vs${getAbbr(month2)}'${String(year2).slice(2)}.xlsx`;
const allRows = getAllSkusForExport();
exportToExcel(allRows, file);
              }}
             className="bg-white border border-[#8B8585] px-1 rounded-sm"
                            style={{
                 boxShadow: "0px 4px 4px 0px #00000040",  
               }}
                         >
                         <IoDownload size={27} color='[#414042]' />
            </button>

            </div>
           
            </div>
            </div>
            
          
          </div>

          <div className="overflow-x-auto pt-4 ">
<table className="tablec w-full border-collapse md:text-sm text-xs 2xl:min-w-full xl:min-w-[1000px]">
  <thead className="theadc">
    <tr>
      <th>S.No.</th>
      <th className="text-left" style={{ textAlign: 'left' }}>Product Name</th>
      <th>Sales Mix ({month2Label || 'Month 2'})</th>

      {activeTab !== 'new_or_reviving_skus' && <th>Sales Mix Change (%)</th>}

      <th>Unit Growth (%)</th>
      <th>ASP Growth (%)</th>
      <th>Net Sales Growth (%)</th>

      <th>CM1 Profit Impact (%)</th>
      <th>CM1 Profit Per Unit (%)</th>

      {Object.keys(skuInsights).length > 0 && <th>AI Insight</th>}
    </tr>
  </thead>

  <tbody>
    {(() => {
      const isNewRev = activeTab === 'new_or_reviving_skus';
      const hasSkus = (currentTabData?.length ?? 0) > 0;
      const showInsight = Object.keys(skuInsights).length > 0;

      // ✅ Case 1: New/Reviving AND no SKUs => show 1 dummy row with ---
      if (isNewRev && !hasSkus) {
        return (
          <tr>
            <td className="border border-[#414042] px-2 py-2.5 text-center text-[#414042]">1</td>
            <td className="border border-[#414042] px-2 py-2.5 text-left text-[#414042]" style={{ textAlign: 'left' }}>
              ---
            </td>

            {/* Sales Mix (Month2) */}
            <td className="border border-[#414042] px-2 py-2.5 text-center">---</td>

            {/* ✅ Sales Mix Change column is NOT shown for new_or_reviving_skus */}

            {/* Growth columns */}
            <td className="border border-[#414042] px-2 py-2.5 text-center">---</td>
            <td className="border border-[#414042] px-2 py-2.5 text-center">---</td>
            <td className="border border-[#414042] px-2 py-2.5 text-center">---</td>
            <td className="border border-[#414042] px-2 py-2.5 text-center">---</td>
            <td className="border border-[#414042] px-2 py-2.5 text-center">---</td>

            {showInsight && (
              <td className="border border-[#414042] px-2 text-nowrap py-2.5 text-center">---</td>
            )}
          </tr>
        );
      }

      // ✅ Case 2 (default): render SKU rows as-is (your existing code)
      return currentTabData?.map((item, idx) => (
        <tr key={idx} className="">
          <td className="border border-[#414042] px-2 py-2.5 text-center text-[#414042]">
            {idx + 1}
          </td>

          <td
            className="border border-[#414042] px-2 py-2.5 text-left text-[#414042]"
            style={{ textAlign: 'left' }}
          >
            {String(item.product_name).trim() === '0' ? (
              <span className="sku-zero-wrap">
                <span className="sku-zero">{item.sku || 'N/A'}</span>
                <span className="sku-zero-tooltip">Product name is 0; SKU is used.</span>
              </span>
            ) : (
              item.product_name
            )}
          </td>

          <td className="border border-[#414042] px-2 py-2.5 text-center">
            {item['Sales Mix (Month2)'] != null
              ? `${Number(item['Sales Mix (Month2)']).toFixed(2)}%`
              : 'N/A'}
          </td>

          {[
            ...(activeTab !== 'new_or_reviving_skus' ? [{ field: 'Sales Mix Change' }] : []),
            { field: 'Unit Growth' },
            { field: 'ASP Growth' },
            { field: 'Net Sales Growth' },
            { field: 'CM1 Profit Impact' },
            { field: 'Profit Per Unit' },
          ].map(({ field }) => {
            const growth = item[field] as GrowthCategory | undefined;

            if (!growth || growth.value == null) {
              return (
                <td key={field} className="border border-[#414042] px-2 py-2.5 text-center">
                  N/A
                </td>
              );
            }

            const val = Number(growth.value);
            const sign = val >= 0 ? '+' : '';
            const text = `${sign}${val.toFixed(2)}%`;

            if (growth.category === 'High Growth') {
              return (
                <td
                  key={field}
                  className="border border-[#414042] px-2 py-2.5 text-center"
                  style={{ fontWeight: 600 }}
                >
                  <span className="inline-flex items-center justify-center gap-2 font-semibold text-[#5EA68E]">
  <span className="w-4 flex justify-center">
    <FaArrowUp size={12} />
  </span>

  <span className="tabular-nums inline-block w-[10px] text-right">
    {text}
  </span>
</span>

                </td>
              );
            }

            if (growth.category === 'Negative Growth') {
              return (
                <td
                  key={field}
                  className="border border-[#414042] px-2 py-2.5 text-center"
                  style={{ fontWeight: 600 }}
                >
                 <span className="inline-flex items-center justify-center gap-2 font-semibold text-[#FF5C5C]">
  <span className="w-4 flex justify-center">
    <FaArrowDown size={12} />
  </span>

  <span className="tabular-nums inline-block w-[10px] text-right">
    {text}
  </span>
</span>

                </td>
              );
            }

            // Low Growth / No Growth / No Data etc.
            return (
              <td
                key={field}
                className="border border-[#414042] px-2 py-2.5 text-center text-[#414042]"
                style={{ fontWeight: 600, color: '#414042' }}
              >
                <span className="inline-flex items-center justify-center gap-2 font-semibold text-[#414042]">
  <span className="w-4 flex justify-center">
    {val > 0 ? <FaArrowUp size={12} /> : val < 0 ? <FaArrowDown size={12} /> : null}
  </span>

  <span className="tabular-nums inline-block w-[10px] text-right">
    {text}
  </span>
</span>

              </td>
            );
          })}

          {Object.keys(skuInsights).length > 0 && (
            <td className="border border-[#414042] px-2 text-nowrap py-2.5 text-center">
              {(() => {
                // ✅ Only for All SKUs tab + Others row => show expand button
                if (
                  activeTab === 'all_skus' &&
                  String(item?.product_name ?? '').toLowerCase().trim() === 'others'
                ) {
                  return (
                    <button
                      className="font-semibold underline text-[#414042]"
                      style={{ margin: 0 }}
                      onClick={() => setExpandAllSkusOthers(true)}
                    >
                      Expand SKUs
                    </button>
                  );
                }

                // Normal insight behavior
                const entry = getInsightForItem(item);

                if (entry) {
                  return (
                    <button
                      className="font-semibold underline text-[#414042]"
                      style={{ margin: 0 }}
                      onClick={() => {
                        setSelectedSku(entry[0]);
                        setModalOpen(true);
                        setFbType(null);
                        setFbText('');
                        setFbSuccess(false);
                      }}
                    >
                      View Insights
                    </button>
                  );
                }

                return (
                  <em style={{ color: '#888' }}>
                    --
                    <br />
                  </em>
                );
              })()}
            </td>
          )}
        </tr>
      ));
    })()}
  </tbody>

 <tfoot>
  <tr className="bg-[#D9D9D9E5]">
    <td className="border border-[#414042] px-2 py-2.5 text-center"></td>

    <td className="border border-[#414042] px-2 py-2.5 text-left font-bold " style={{ textAlign: 'left' }}>
      <strong>Total</strong>
    </td>

    {/* Sales Mix */}
    <td className="border border-[#414042] px-2 py-2.5 text-center font-bold">
      {(() => {
        const rows = (categorizedGrowth[activeTab] || []) as any[];
        const sum = rows.reduce(
          (s, r) => s + Number(r?.['Sales Mix (Month2)'] ?? 0),
          0
        );
        const rounded = Number(sum.toFixed(2));
        const fixed = Math.abs(rounded - 100) < 0.05 ? 100 : rounded;
        return `${fixed.toFixed(2)}%`;
      })()}
    </td>

    {(() => {
      const rows = (categorizedGrowth[activeTab] || []) as any[];

      const sum = (k: string) =>
        rows.reduce((s, r) => s + Number(r?.[k] ?? 0), 0);

      const pct = (m1: number, m2: number) => {
        if (m1 === 0) return 0;
        return ((m2 - m1) / m1) * 100;
      };

      const cells = [
        ...(activeTab !== 'new_or_reviving_skus'
          ? [0]
          : []),
        pct(sum('quantity_month1'), sum('quantity_month2')),
        pct(sum('asp_month1'), sum('asp_month2')),
        pct(sum('net_sales_month1'), sum('net_sales_month2')),
        pct(sum('unit_wise_profitability_month1'), sum('unit_wise_profitability_month2')),
        pct(sum('profit_month1'), sum('profit_month2')),
      ];

      return cells.map((v, i) => {
        const val = Number(v);
        const sign = val >= 0 ? '+' : '';
        const text = `${sign}${val.toFixed(2)}%`;

        // ✅ Total row classification:
        // High Growth: val >= 5
        // Negative Growth: val < 0
        // Low Growth: 0 <= val < 5 (or any other neutral)
        if (val >= 5) {
          return (
            <td
              key={i}
              className="border border-[#414042] px-2 py-2.5 text-center font-bold"
              style={{ fontWeight: 600 }}
            >
              <span className="inline-flex items-center justify-center gap-2 font-semibold text-[#5EA68E]">
                <span className="w-4 flex justify-center">
                  <FaArrowUp size={12} />
                </span>
                <span className="tabular-nums inline-block w-[10px] text-right">
                  {text}
                </span>
              </span>
            </td>
          );
        }

        if (val < 0) {
          return (
            <td
              key={i}
              className="border border-[#414042] px-2 py-2.5 text-center font-bold"
              style={{ fontWeight: 600 }}
            >
              <span className="inline-flex items-center justify-center gap-2 font-semibold text-[#FF5C5C]">
                <span className="w-4 flex justify-center">
                  <FaArrowDown size={12} />
                </span>
                <span className="tabular-nums inline-block w-[10px] text-right">
                  {text}
                </span>
              </span>
            </td>
          );
        }

        return (
          <td
            key={i}
            className="border border-[#414042] px-2 py-2.5 text-center font-bold"
            style={{ fontWeight: 600, color: '#414042' }}
          >
            <span className="inline-flex items-center justify-center gap-2 font-semibold text-[#414042]">
              <span className="w-4 flex justify-center">
                {val > 0 ? <FaArrowUp size={12} /> : val < 0 ? <FaArrowDown size={12} /> : null}
              </span>
              <span className="tabular-nums inline-block w-[10px] text-right">
                {text}
              </span>
            </span>
          </td>
        );
      });
    })()}

    {Object.keys(skuInsights).length > 0 && (
      <td className="border border-[#414042] px-2 py-2.5 text-center font-bold"></td>
    )}
  </tr>
</tfoot>

</table>
            < div className='flex justify-center mt-2'>
 <div
  style={{
    display: 'flex',
    alignItems: 'center',
    gap: 14,
    flexWrap: 'wrap',
    fontSize: 14 ,
    color: '#414042',
    marginTop: 6,
  }}
>
  <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, color: '#5EA68E', fontWeight: 700 }}>
      <FaArrowUp size={12} /> High growth
    </span>
  </span>

  <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, color: '#FF5C5C', fontWeight: 700 }}>
      <FaArrowDown size={12} /> Negative growth
    </span>
  </span>
 
  <span
  style={{
    display: 'inline-flex',
    alignItems: 'center',
    gap: 6,
    fontWeight: 700,
    whiteSpace: 'nowrap',
  }}
>
  <span style={{ color: '#414042', display: 'inline-flex', alignItems: 'center', gap: 4 }}>
    <FaArrowUp size={12} /> + / <FaArrowDown size={12} /> -
  </span>
  Low growth
</span>
</div>
            </div>
             
          </div>
        </div>
      )}
</div>



      {/* Insight Modal */}
{(() => {
  if (!modalOpen || !selectedSku) return null;

  const insightData =
    skuInsights[selectedSku as keyof typeof skuInsights] ||
    getInsightByProductName(selectedSku as string)?.[1];

  if (!insightData) return null;

  return (
    <Drawer
      anchor="right"
      open={modalOpen}
      onClose={() => setModalOpen(false)}
      PaperProps={{
        sx: {
          width: { xs: '100vw', sm: '80vw', md: '60vw', lg: '50vw' },
          maxWidth: 900,
          padding: 2,
        },
      }}
    >
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          gap: 12,
          height: '100%',
        }}
      >
        {/* Header */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            marginBottom: 8,
          }}
        >
          <h2 style={{ margin: 0, fontSize: 18 }}>
            AI Insight for{' '}
            <span style={{ color: '#60a68e' }}>
              {insightData.product_name || selectedSku}
            </span>
          </h2>

          <IconButton
            size="small"
            onClick={() => setModalOpen(false)}
            aria-label="Close"
          >
           x
          </IconButton>
        </div>

        {/* Chart */}
        <div style={{ border: '1px solid #e5e7eb', borderRadius: 8, padding: 8 }}>
          <Productinfoinpopup productname={insightData.product_name} />
        </div>

        {/* Insights text with bullets & colors */}
        <div
          style={{
            flex: 1,
            overflowY: 'auto',
            marginTop: 8,
            paddingRight: 4,
          }}
        >
          {renderFormattedInsight(insightData.insight)}
        </div>
      </div>
    </Drawer>
  );
})()}
    </>
  );
};

export default MonthsforBI;



