'use client';

import React, { useEffect, useMemo, useRef, useState } from 'react';
import { saveAs } from 'file-saver';
import { Line } from 'react-chartjs-2';
import ExcelJS from 'exceljs';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { IoDownload } from "react-icons/io5";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend);

/* ---------------------- Types ---------------------- */
type YM = { y: number; m: number };

export interface DisplayInventoryForecastProps {
  countryName: string;
  month: string;
  year: string;
  data: Array<Record<string, any>>;
  isDemoMode?: boolean;
}

/* -------------------- Constants -------------------- */
const MONTH_ABBR = [
  'Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec',
] as const;

const FULL_MONTHS = [
  'January','February','March','April','May','June',
  'July','August','September','October','November','December',
] as const;

/* -------------------- Utilities -------------------- */
function parseMonthHeaderToDate(col?: string | null): YM | null {
  if (!col) return null;

  // e.g., Oct'25
  let m = col.match(/^([A-Z][a-z]{2})'\s?(\d{2})$/);
  if (m) {
    const mi = MONTH_ABBR.indexOf(m[1] as (typeof MONTH_ABBR)[number]);
    const y = 2000 + parseInt(m[2], 10);
    if (mi >= 0) return { y, m: mi };
  }

  // e.g., October 2025
  m = col.match(/^([A-Z][a-z]+)\s+(\d{4})$/);
  if (m) {
    const mi = FULL_MONTHS.indexOf(m[1] as (typeof FULL_MONTHS)[number]);
    const y = parseInt(m[2], 10);
    if (mi >= 0) return { y, m: mi };
  }

  // e.g., Oct 2025
  m = col.match(/^([A-Z][a-z]{2})\s+(\d{4})$/);
  if (m) {
    const mi = MONTH_ABBR.indexOf(m[1] as (typeof MONTH_ABBR)[number]);
    const y = parseInt(m[2], 10);
    if (mi >= 0) return { y, m: mi };
  }

  return null;
}

const monthShortLabel = (col: string) => {
  const p = parseMonthHeaderToDate(col);
  return p ? MONTH_ABBR[p.m] : col;
};

const compareYM = (a: YM, b: YM) => (a.y !== b.y ? a.y - b.y : a.m - b.m);

/* -------------------- Component -------------------- */
const DisplayInventoryForecast: React.FC<DisplayInventoryForecastProps> = ({
  countryName,
  month,
  year,
  data,
  isDemoMode = false,
}) => {
  const [monthRange, setMonthRange] = useState<string | null>(null);
  const chartRef = useRef<any>(null);

  const demoMode = Boolean(isDemoMode);

  const forecastData = useMemo(() => (Array.isArray(data) ? data : []), [data]);

  // Collect all keys
  const allKeys = useMemo<string[]>(() => {
    const s = new Set<string>();
    forecastData.forEach((r) => Object.keys(r || {}).forEach((k) => s.add(k)))
    return Array.from(s);
  }, [forecastData]);

  const monthWithYearLabel = (col: string) => {
  const p = parseMonthHeaderToDate(col);
  if (!p) return col;
  return `${MONTH_ABBR[p.m]}'${String(p.y).slice(-2)}`;
};

  // Detect "* Sold" month columns, sort oldest->newest
  const soldColsSorted = useMemo(() => {
    const items: Array<{ key: string; ym: YM }> = [];
    for (const k of allKeys) {
      if (!/\sSold$/i.test(k)) continue;
      const core = k.replace(/\s+Sold$/i, '');
      const parsed = parseMonthHeaderToDate(core);
      if (parsed) items.push({ key: k, ym: parsed });
    }
    items.sort((a, b) => compareYM(a.ym, b.ym));
    return items;
  }, [allKeys]);

  // The max sold month (to pick forecasts after this)
  const maxSoldYM = useMemo<YM | null>(() => {
    if (!soldColsSorted.length) return null;
    return soldColsSorted[soldColsSorted.length - 1].ym;
  }, [soldColsSorted]);

  // Last 3 sold months, oldest -> newest
  const last3SoldOldestFirst = useMemo<string[]>(() => {
    return soldColsSorted.slice(-3).map((x) => x.key);
  }, [soldColsSorted]);

  // Labels for header 2nd row
const soldLabels = useMemo(
  () => last3SoldOldestFirst.map((k) =>
    monthWithYearLabel(k.replace(/\s+Sold$/i, ''))
  ),
  [last3SoldOldestFirst]
);


  // Forecast month columns (no "Sold"), sorted oldest->newest
  const forecastMonthColsSorted = useMemo(() => {
    const arr: Array<{ key: string; ym: YM }> = [];
    for (const k of allKeys) {
      if (/\sSold$/i.test(k)) continue;
      const parsed = parseMonthHeaderToDate(k);
      if (parsed) arr.push({ key: k, ym: parsed });
    }
    arr.sort((a, b) => compareYM(a.ym, b.ym));
    return arr;
  }, [allKeys]);

  // Next 3 forecast months **after** the max sold month (chronological)
  const forecast3 = useMemo<string[]>(() => {
    if (!forecastMonthColsSorted.length) return [];
    let after = forecastMonthColsSorted;
    if (maxSoldYM) {
      after = forecastMonthColsSorted.filter((x) => compareYM(x.ym, maxSoldYM) > 0);
    }
    const chosen = (after.length >= 3 ? after.slice(0, 3) : forecastMonthColsSorted.slice(0, 3)).map(
      (x) => x.key
    );
    return chosen;
  }, [forecastMonthColsSorted, maxSoldYM]);

  const forecastLabels = useMemo(
  () => forecast3.map((k) => monthWithYearLabel(k)),
  [forecast3]
);


  // Build table rows
  const tableRows = useMemo(
    () =>
      forecastData
        .filter((r) => r && r.sku && r.sku !== 'Total')
        .map((r, idx) => ({
          sNo: idx + 1,
          product: r['Product Name'] ?? '',
          sku: r['sku'] ?? '',
          sold1: r[last3SoldOldestFirst[0]] ?? '',
          sold2: r[last3SoldOldestFirst[1]] ?? '',
          sold3: r[last3SoldOldestFirst[2]] ?? '',
          f1: r[forecast3[0]] ?? '',
          f2: r[forecast3[1]] ?? '',
          f3: r[forecast3[2]] ?? '',
        })),
    [forecastData, last3SoldOldestFirst, forecast3]
  );

  // Totals row in same order
  const totalsRow = useMemo(() => {
    const sumCol = (key: string) => {
      if (!key) return 0;
      let sum = 0;
      for (const r of forecastData) {
        if (!r || r.sku === 'Total') continue;
        const n = Number(r[key]);
        if (Number.isFinite(n)) sum += n;
      }
      return Math.round(sum);
    };
    return {
      label: 'Total',
      sold1: sumCol(last3SoldOldestFirst[0] || ''),
      sold2: sumCol(last3SoldOldestFirst[1] || ''),
      sold3: sumCol(last3SoldOldestFirst[2] || ''),
      f1: sumCol(forecast3[0] || ''),
      f2: sumCol(forecast3[1] || ''),
      f3: sumCol(forecast3[2] || ''),
    };
  }, [forecastData, last3SoldOldestFirst, forecast3]);

  // ===== Chart: Top 5 SKUs + Total =====

  // Labels aligned with: [sold1, sold2, sold3, f1, f2, f3]
  const chartLabels = useMemo(
    () => [
      ...soldLabels,
      ...forecastLabels,
    ],
    [soldLabels, forecastLabels]
  );

  const valuesForRow = (r: Record<string, any>) => [
    Number(r[last3SoldOldestFirst[0]]) || 0,
    Number(r[last3SoldOldestFirst[1]]) || 0,
    Number(r[last3SoldOldestFirst[2]]) || 0,
    Number(r[forecast3[0]]) || 0,
    Number(r[forecast3[1]]) || 0,
    Number(r[forecast3[2]]) || 0,
  ];

  const top5Rows = useMemo(() => {
    const rows = forecastData
      .filter((r) => r && r.sku && r.sku !== 'Total')
      .map((r) => {
        const vals = valuesForRow(r);
        const total = vals.reduce((a, b) => a + b, 0);
        return { row: r, vals, total };
      })
      .sort((a, b) => b.total - a.total)
      .slice(0, 5);
    return rows;
  }, [forecastData, last3SoldOldestFirst, forecast3]);

  // Grand total aligned with chartLabels
  const grandTotalSeries = useMemo(
    () => [
      totalsRow.sold1 || 0,
      totalsRow.sold2 || 0,
      totalsRow.sold3 || 0,
      totalsRow.f1 || 0,
      totalsRow.f2 || 0,
      totalsRow.f3 || 0,
    ],
    [totalsRow]
  );

  const palette = [  "#FDD36F",
          "#5EA49B",
          "#ED9F50",
          "#00627D",
          "#87AD12", '#C49466'];
  const forecastStartIndex = 3;

  const datasets = useMemo(() => {
    const skuDatasets = top5Rows.map((t, i) => ({
      label: (t.row['sku'] as string) || (t.row['Product Name'] as string) || `SKU ${i + 1}`,
      data: t.vals,
      borderColor: palette[i % palette.length],
      backgroundColor: palette[i % palette.length],
      borderWidth: 2,
      tension: 0.3,
      fill: false,
      segment: {
        borderDash: (ctx: any) => {
          const idx = ctx?.p0DataIndex ?? 0;
          return idx >= forecastStartIndex ? [6, 6] : undefined;
        },
      },
    }));

    const totalDs = {
      label: 'Total',
      data: grandTotalSeries,
      borderColor: '#C49466',
      backgroundColor: '#C49466',
      borderWidth: 3,
      tension: 0.3,
      fill: false,
      segment: {
        borderDash: (ctx: any) => {
          const idx = ctx?.p0DataIndex ?? 0;
          return idx >= forecastStartIndex ? [6, 6] : undefined;
        },
      },
    };

    return [...skuDatasets, totalDs];
  }, [top5Rows, grandTotalSeries]);

  const chartData = useMemo(
    () => ({
      labels: chartLabels,
      datasets,
    }),
    [chartLabels, datasets]
  );

const chartOptions = useMemo(
  () => ({
    responsive: true,
    maintainAspectRatio: false,
    layout: {
      padding: { top: 0, bottom: 24 },
    },
    plugins: {
      legend: {
        position: 'top' as const,
        align: 'center' as const,
        labels: {
          padding: 20,
          boxWidth: 14,
          boxHeight: 14,
          font: {
            size: 12,
          },
        },
      },
      tooltip: {
        callbacks: {
          label: (ctx: any) => {
            const val = ctx.parsed?.y ?? 0;
            return `${ctx.dataset.label}: ${Number(val).toLocaleString()}`;
          },
        },
      },
    },
    scales: {
      x: {
        title: {
          display: true,
          text: 'Months',
          font: { size: 12 }, // ✅ mobile label size
        },
        ticks: {
          font: {
            size: window.innerWidth < 768 ? 12 : 14, // ✅ mobile ticks 12px
          },
        },
      },
      y: {
        title: {
          display: true,
          text: 'Units',
          font: { size: 12 }, // ✅ mobile label size
        },
        ticks: {
          font: {
            size: window.innerWidth < 768 ? 12 : 14, // ✅ mobile ticks 12px
          },
        },
        beginAtZero: true,
      },
    },
  }),
  [countryName]
);


 const forecastPlugin = {
  id: 'forecastBackground',
  beforeDraw(chart: any) {
    const { ctx, chartArea, data, scales } = chart;
    const scaleX = scales?.x;
    if (!scaleX || !data?.labels?.length) return;

    const idx = forecastStartIndex;
    if (idx >= data.labels.length) return;

    // 🔥 EXACT forecast start boundary
    const startX = scaleX.getPixelForTick(idx);

    ctx.save();
    ctx.fillStyle = 'rgba(217,217,217,0.35)';
    ctx.fillRect(
      startX,
      chartArea.top,
      chartArea.right - startX,
      chartArea.bottom - chartArea.top
    );
    ctx.restore();
  },
};


  // Optional month range
  useEffect(() => {
    if (!Array.isArray(data) || data.length === 0) return; // guard

    const fetchMonthRange = async () => {
      try {
        const token = localStorage.getItem('jwtToken');
        if (!token) return;

        const resp = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/api/forecast_monthrange?country=${encodeURIComponent(
            countryName.toLowerCase()
          )}`,
          { method: 'GET', headers: { Authorization: `Bearer ${token}` } }
        );

        if (!resp.ok) {
          console.warn('monthrange failed', resp.status);
          return;
        }
        const j = (await resp.json()) as { month_range?: string };
        setMonthRange(j.month_range ?? null);
      } catch (e) {
        console.warn('monthrange error', e);
      }
    };

    fetchMonthRange();
  }, [countryName, data]);

  // Helper: convert data URL -> ArrayBuffer for exceljs
  const base64DataUrlToArrayBuffer = (dataUrl: string): ArrayBuffer => {
    const base64 = dataUrl.split(',')[1];
    const binary = atob(base64);
    const len = binary.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes.buffer;
  };

  // Helper: get PNG with **white background** ONLY for Excel
  const getChartPngWithWhiteBg = (): string | null => {
    const chartInstance = chartRef.current as any;
    if (!chartInstance) return null;

    const sourceCanvas: HTMLCanvasElement | undefined =
      chartInstance.canvas || chartInstance.ctx?.canvas;
    if (!sourceCanvas) return null;

    const exportCanvas = document.createElement('canvas');
    exportCanvas.width = sourceCanvas.width;
    exportCanvas.height = sourceCanvas.height;
    const ctx = exportCanvas.getContext('2d');
    if (!ctx) return null;

    // White background
    ctx.fillStyle = '#ffffff';
    ctx.fillRect(0, 0, exportCanvas.width, exportCanvas.height);

    // Draw original chart canvas on top
    ctx.drawImage(sourceCanvas, 0, 0);

    return exportCanvas.toDataURL('image/png');
  };

  const handleDownload = async () => {
    // ===== 1. Prepare table data (same as table) =====
    const header1 = ['S.No', 'Product Name', 'SKU', 'Last 3 Months', '', '', 'Forecasted Months', '', ''];
    const header2 = [
      '', '', '',
      soldLabels[0] || '',
      soldLabels[1] || '',
      soldLabels[2] || '',
      forecastLabels[0] || '',
      forecastLabels[1] || '',
      forecastLabels[2] || '',
    ];

    const rows = tableRows.map((r) => [
      r.sNo,
      r.product,
      r.sku,
      r.sold1,
      r.sold2,
      r.sold3,
      r.f1,
      r.f2,
      r.f3,
    ]);

    const totalsExcelRow = [
      '', 'Total', '',
      totalsRow.sold1,
      totalsRow.sold2,
      totalsRow.sold3,
      totalsRow.f1,
      totalsRow.f2,
      totalsRow.f3,
    ];

    const tableData = [header1, header2, ...rows, totalsExcelRow];

    // ===== 2. Create workbook & sheet =====
    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Forecast (View)');

    // ===== 3. Add chart image (with white background) at TOP =====
    const dataUrl = getChartPngWithWhiteBg();
    if (dataUrl) {
      const buffer = base64DataUrlToArrayBuffer(dataUrl);
      const imageId = workbook.addImage({
        buffer,
        extension: 'png',
      });

      // Chart on top: A1 to I18
      sheet.addImage(imageId, 'A1:I18');
    }

    // ===== 4. Add table BELOW chart =====
    const tableStartRow = 20; // little gap after chart

    tableData.forEach((row, idx) => {
      sheet.getRow(tableStartRow + idx).values = row;
    });

    // Merge cells for group headers (offset by tableStartRow)
    sheet.mergeCells(tableStartRow, 4, tableStartRow, 6); // D? - F? "Last 3 Months"
    sheet.mergeCells(tableStartRow, 7, tableStartRow, 9); // G? - I? "Forecasted Months"
    sheet.mergeCells(tableStartRow, 1, tableStartRow + 1, 1); // S.No
    sheet.mergeCells(tableStartRow, 2, tableStartRow + 1, 2); // Product Name
    sheet.mergeCells(tableStartRow, 3, tableStartRow + 1, 3); // SKU

    // Header styling
    const headerRow1 = sheet.getRow(tableStartRow);
    const headerRow2 = sheet.getRow(tableStartRow + 1);
    [headerRow1, headerRow2].forEach((row) => {
      row.eachCell((cell) => {
        cell.alignment = { vertical: 'middle', horizontal: 'center' };
        cell.font = { bold: true };
        cell.border = {
          top: { style: 'thin' },
          bottom: { style: 'thin' },
          left: { style: 'thin' },
          right: { style: 'thin' },
        };
      });
    });

    // Body borders
    for (let r = tableStartRow + 2; r < tableStartRow + tableData.length; r++) {
      const row = sheet.getRow(r);
      row.eachCell((cell) => {
        cell.border = {
          top: { style: 'thin' },
          bottom: { style: 'thin' },
          left: { style: 'thin' },
          right: { style: 'thin' },
        };
      });
    }

    // Auto column width
   sheet.columns.forEach((col) => {
  if (!col) return; // safety

  let maxLength = 10;

  if (col.eachCell) {
    col.eachCell((cell) => {
      const v = cell.value as string | number | null;
      if (v != null) {
        const len = String(v).length;
        if (len > maxLength) maxLength = len;
      }
    });
  }

  col.width = maxLength + 2;
});

    // ===== 5. Download .xlsx (chart + table together) =====
    const xlsxBuffer = await workbook.xlsx.writeBuffer();
    const blob = new Blob(
      [xlsxBuffer],
      { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }
    );
    saveAs(
      blob,
      `Inventory_Forecast_View_${countryName}_${month}_${year}.xlsx`
    );
  };

  if (!forecastData.length) return <p style={{ color: 'gray' }}>No data available.</p>;

  return (
    <div>
      <h3 className="2xl:text-2xl text-[18px] font-bold text-[#414042]">
        Forecasted Data -{' '}
        {monthRange && (
          <span className="text-[#5EA68E]">
            {countryName.toUpperCase()} <strong>({monthRange})</strong>
          </span>
        )}
      </h3>

      {/* Chart: Top 5 SKUs + Total */}
      <div className=" p-4 border border-[#000000] rounded-lg mt-5">
        
        <div className="flex flex-col gap-3 md:flex-row md:justify-between md:items-center">
  {/* Left: Title + subtitle */}
  <div className="text-left">
    <h2 className="text-base sm:text-lg md:text-2xl text-[#414042] font-semibold leading-snug">
      Top 5 SKUs Inventory Trend
    </h2>
    <p className="text-[11px] sm:text-xs md:text-sm leading-snug">
      Historical data vs forecasted trends
    </p>
  </div>

  {/* Right: legend + actions */}
  <div className="flex flex-col sm:flex-row gap-4 md:items-center">
    {/* Legend */}
    <div className="flex flex-wrap gap-4 text-[11px] sm:text-xs md:text-xs">
      <div className="flex items-center gap-2">
        <span className="inline-block w-8 border-b-2 border-black" />
        <span className="whitespace-nowrap">Last 3 months (Actual)</span>
      </div>
      <div className="flex items-center gap-2">
        <span className="inline-block w-8 border-b-2 border-black border-dashed" />
        <span className="whitespace-nowrap">Next 3 months (Forecast)</span>
      </div>
    </div>

    {/* Actions */}
    <div className="flex items-center justify-end gap-3">
      <button
        onClick={handleDownload}
        disabled={demoMode}
        className={`bg-white border border-[#8B8585] px-1 rounded-sm ${
          demoMode ? 'opacity-50 cursor-not-allowed' : ''
        }`}
      >
        <IoDownload size={24} className="md:w-[27px] md:h-[27px]" />
      </button>

      <button
        onClick={handleDownload}
        className="bg-[#37455F] text-xs sm:text-sm text-[#F8EDCE] font-bold px-4 sm:px-6 py-2 rounded-lg shadow-[0px_4px_4px_0px_#00000040]"
      >
        Raise PO
      </button>
    </div>
  </div>
</div>

       <div className='w-full h-[550px]'>
 <Line ref={chartRef} data={chartData} options={chartOptions} plugins={[forecastPlugin]} />
     
       </div>
       
<h2 className='2xl:text-2xl text-[18px] font-bold text-[#414042]'>Detailed Forecast Data (All SKUs)</h2>
      {/* Table with two-row header and totals row */}
   <div className="overflow-x-auto mt-6">
  <table className="min-w-full 2xl:text-sm text-xs border border-[#e1e5ea] rounded-lg text-[#414042]">
    <thead>
  <tr className="font-normal">
   <th rowSpan={2} className="p-3 border border-[#e1e5ea] bg-[#5EA68E] text-[#F8EDCE] font-semibold text-center align-middle">
  S.No
</th>
<th rowSpan={2} className="p-3 border border-[#e1e5ea] bg-[#5EA68E] text-[#F8EDCE]  font-semibold text-left align-middle">
  Product Name
</th>
<th rowSpan={2} className="p-3 border border-[#e1e5ea] bg-[#5EA68E] text-[#F8EDCE] font-semibold text-center align-middle">
  SKU
</th>


    {/* Last 3 Months */}
    <th className="p-3 border border-[#e1e5ea] bg-[#5EA68E] text-[#F8EDCE] font-semibold" colSpan={3}>
      Last 3 Months
    </th>

    {/* Forecasted Months */}
    <th className="p-3 border border-[#e1e5ea] bg-[#5EA68E] text-[#F8EDCE] font-semibold" colSpan={3}>
      Forecasted Months
    </th>
  </tr>

  <tr>
   
    {/* Dynamic month labels */}
    <th className="p-2 border border-[#e1e5ea] bg-[#D9D9D9]">{soldLabels[0] || ''}</th>
    <th className="p-2 border border-[#e1e5ea] bg-[#D9D9D9]">{soldLabels[1] || ''}</th>
    <th className="p-2 border border-[#e1e5ea] bg-[#D9D9D9]">{soldLabels[2] || ''}</th>
    <th className="p-2 border border-[#e1e5ea] bg-[#D9D9D9]">{forecastLabels[0] || ''}</th>
    <th className="p-2 border border-[#e1e5ea] bg-[#D9D9D9]">{forecastLabels[1] || ''}</th>
    <th className="p-2 border border-[#e1e5ea] bg-[#D9D9D9]">{forecastLabels[2] || ''}</th>
  </tr>
</thead>

    <tbody>
      {tableRows.map((row, i) => (
        <tr key={i} className="text-center border-t border-[#e1e5ea] bg-white">
          <td className="p-2 border border-[#e1e5ea]">{row.sNo}</td>
          <td className="p-2 border border-[#e1e5ea] text-left">{row.product}</td>
          <td className="p-2 border border-[#e1e5ea] ">{row.sku}</td>
          <td className="p-2 border border-[#e1e5ea]">{row.sold1}</td>
          <td className="p-2 border border-[#e1e5ea]">{row.sold2}</td>
          <td className="p-2 border border-[#e1e5ea]">{row.sold3}</td>
          <td className="p-2 border border-[#e1e5ea]">{row.f1}</td>
          <td className="p-2 border border-[#e1e5ea]">{row.f2}</td>
          <td className="p-2 border border-[#e1e5ea]">{row.f3}</td>
        </tr>
      ))}
      <tr className="text-center border-t border-[#e1e5ea] bg-[#D9D9D9] font-semibold">
        <td className="p-2 border border-[#e1e5ea]"></td>
        <td className="p-2 border border-[#e1e5ea] text-left">Total</td>
        <td className="p-2 border border-[#e1e5ea]"></td>
        <td className="p-2 border border-[#e1e5ea]">{totalsRow.sold1}</td>
        <td className="p-2 border border-[#e1e5ea]">{totalsRow.sold2}</td>
        <td className="p-2 border border-[#e1e5ea]">{totalsRow.sold3}</td>
        <td className="p-2 border border-[#e1e5ea]">{totalsRow.f1}</td>
        <td className="p-2 border border-[#e1e5ea]">{totalsRow.f2}</td>
        <td className="p-2 border border-[#e1e5ea]">{totalsRow.f3}</td>
      </tr>
    </tbody>
  </table>
</div>
 </div>

      
    </div>
  );
};

export default DisplayInventoryForecast;
