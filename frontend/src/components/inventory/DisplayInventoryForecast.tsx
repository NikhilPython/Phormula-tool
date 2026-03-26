'use client';

import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Line } from 'react-chartjs-2';
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
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
import PageBreadcrumb from "../common/PageBreadCrumb";
import { exportInventoryForecastViewExcel } from "@/lib/excel/exportCurrentInventoryExcel";
import { useGetUserDataQuery } from '@/lib/api/profileApi';
import "@/lib/chartSetup";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend);

type YM = { y: number; m: number };

export interface DisplayInventoryForecastProps {
  countryName: string;
  month: string;
  year: string;
  data: Array<Record<string, any>>;
  isDemoMode?: boolean;
  platformLabel?: string;
}

const MONTH_ABBR = [
  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
] as const;

const FULL_MONTHS = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
] as const;

function parseMonthHeaderToDate(col?: string | null): YM | null {
  if (!col) return null;

  let m = col.match(/^([A-Z][a-z]{2})'\s?(\d{2})$/);
  if (m) {
    const mi = MONTH_ABBR.indexOf(m[1] as (typeof MONTH_ABBR)[number]);
    const y = 2000 + parseInt(m[2], 10);
    if (mi >= 0) return { y, m: mi };
  }

  m = col.match(/^([A-Z][a-z]+)\s+(\d{4})$/);
  if (m) {
    const mi = FULL_MONTHS.indexOf(m[1] as (typeof FULL_MONTHS)[number]);
    const y = parseInt(m[2], 10);
    if (mi >= 0) return { y, m: mi };
  }

  m = col.match(/^([A-Z][a-z]{2})\s+(\d{4})$/);
  if (m) {
    const mi = MONTH_ABBR.indexOf(m[1] as (typeof MONTH_ABBR)[number]);
    const y = parseInt(m[2], 10);
    if (mi >= 0) return { y, m: mi };
  }

  return null;
}

const compareYM = (a: YM, b: YM) => (a.y !== b.y ? a.y - b.y : a.m - b.m);

const DisplayInventoryForecast: React.FC<DisplayInventoryForecastProps> = ({
  countryName,
  month,
  year,
  data,
  isDemoMode = false,
  platformLabel = "Amazon",
}) => {
  const { data: userData } = useGetUserDataQuery();

  const companyName =
    (userData as any)?.companyName ||
    (userData as any)?.company_name ||
    (userData as any)?.company ||
    "";

  const brandName =
    (userData as any)?.brandName ||
    (userData as any)?.brand_name ||
    (userData as any)?.brand ||
    "";

  const [monthRange, setMonthRange] = useState<string | null>(null);
  const [selectedSeries, setSelectedSeries] = useState<Record<string, boolean>>({
    top1: true,
    top2: true,
    top3: true,
    top4: true,
    top5: true,
    // total: true,
  });
  const [showToggleModal, setShowToggleModal] = useState(false);
  const chartRef = useRef<any>(null);
  const demoMode = Boolean(isDemoMode);
  const forecastData = useMemo(() => (Array.isArray(data) ? data : []), [data]);

  const allKeys = useMemo<string[]>(() => {
    const s = new Set<string>();
    forecastData.forEach((r) => Object.keys(r || {}).forEach((k) => s.add(k)));
    return Array.from(s);
  }, [forecastData]);

  const monthWithYearLabel = (col: string) => {
    const p = parseMonthHeaderToDate(col);
    if (!p) return col;
    return `${MONTH_ABBR[p.m]}'${String(p.y).slice(-2)}`;
  };

  // FIXED:
  // Selected month remains in actuals.
  // Forecast starts from next month.
  const selectedForecastStart = useMemo<YM | null>(() => {
    const fullMonthIndex = FULL_MONTHS.findIndex(
      (m) => m.toLowerCase() === String(month).toLowerCase()
    );

    if (fullMonthIndex === -1) return null;

    const yr = parseInt(String(year), 10);
    if (!Number.isFinite(yr)) return null;

    const nextMonth = fullMonthIndex + 1;

    if (nextMonth <= 11) {
      return { y: yr, m: nextMonth };
    }

    return { y: yr + 1, m: 0 };
  }, [month, year]);

  const monthColsSorted = useMemo(() => {
    const arr: Array<{ key: string; ym: YM }> = [];

    for (const k of allKeys) {
      const normalizedKey = String(k).replace(/\s+Sold$/i, '').trim();
      const parsed = parseMonthHeaderToDate(normalizedKey);
      if (parsed) {
        arr.push({ key: k, ym: parsed });
      }
    }

    arr.sort((a, b) => compareYM(a.ym, b.ym));
    return arr;
  }, [allKeys]);

  const last3SoldOldestFirst = useMemo<string[]>(() => {
    if (!monthColsSorted.length) return [];

    if (!selectedForecastStart) {
      return monthColsSorted.slice(0, 3).map((x) => x.key);
    }

    const actuals = monthColsSorted.filter(
      (x) => compareYM(x.ym, selectedForecastStart) < 0
    );

    return actuals.slice(-3).map((x) => x.key);
  }, [monthColsSorted, selectedForecastStart]);

  const forecast3 = useMemo<string[]>(() => {
    if (!monthColsSorted.length) return [];

    if (!selectedForecastStart) {
      return monthColsSorted.slice(3, 6).map((x) => x.key);
    }

    const forecasts = monthColsSorted.filter(
      (x) => compareYM(x.ym, selectedForecastStart) >= 0
    );

    return forecasts.slice(0, 3).map((x) => x.key);
  }, [monthColsSorted, selectedForecastStart]);

  const soldLabels = useMemo(
    () =>
      last3SoldOldestFirst.map((k) =>
        monthWithYearLabel(String(k).replace(/\s+Sold$/i, '').trim())
      ),
    [last3SoldOldestFirst]
  );

  const forecastLabels = useMemo(
    () =>
      forecast3.map((k) =>
        monthWithYearLabel(String(k).replace(/\s+Sold$/i, '').trim())
      ),
    [forecast3]
  );

  const tableRows = useMemo(() => {
    const baseRows = forecastData
      .filter((r) => r && r.sku && r.sku !== 'Total')
      .map((r) => ({
        product: r['Product Name'] ?? '',
        sku: r['sku'] ?? '',
        sold1: Number(r[last3SoldOldestFirst[0]]) || 0,
        sold2: Number(r[last3SoldOldestFirst[1]]) || 0,
        sold3: Number(r[last3SoldOldestFirst[2]]) || 0,
        f1: Number(r[forecast3[0]]) || 0,
        f2: Number(r[forecast3[1]]) || 0,
        f3: Number(r[forecast3[2]]) || 0,
      }));

    const first9 = baseRows.slice(0, 9);
    const remaining = baseRows.slice(9);

    const rows = [...first9];

    if (remaining.length > 0) {
      const othersRow = remaining.reduce(
        (acc, r) => ({
          product: 'Others',
          sku: '-',
          sold1: acc.sold1 + r.sold1,
          sold2: acc.sold2 + r.sold2,
          sold3: acc.sold3 + r.sold3,
          f1: acc.f1 + r.f1,
          f2: acc.f2 + r.f2,
          f3: acc.f3 + r.f3,
        }),
        {
          product: 'Others',
          sku: '-',
          sold1: 0,
          sold2: 0,
          sold3: 0,
          f1: 0,
          f2: 0,
          f3: 0,
        }
      );

      rows.push(othersRow);
    }

    return rows.map((r, idx) => ({
      sNo: idx + 1,
      ...r,
    }));
  }, [forecastData, last3SoldOldestFirst, forecast3]);

  const totalsRow = useMemo(() => {
    const sumCol = (key: string) => {
      if (!key) return 0;
      let total = 0;
      for (const r of forecastData) {
        if (!r || r.sku === 'Total') continue;
        const n = Number(r[key]);
        if (Number.isFinite(n)) total += n;
      }
      return Math.round(total);
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

  const chartLabels = useMemo(() => [...soldLabels, ...forecastLabels], [soldLabels, forecastLabels]);

  const valuesForRow = (r: Record<string, any>) => [
    Number(r[last3SoldOldestFirst[0]]) || 0,
    Number(r[last3SoldOldestFirst[1]]) || 0,
    Number(r[last3SoldOldestFirst[2]]) || 0,
    Number(r[forecast3[0]]) || 0,
    Number(r[forecast3[1]]) || 0,
    Number(r[forecast3[2]]) || 0,
  ];

  const top5Rows = useMemo(() => {
    return forecastData
      .filter((r) => r && r.sku && r.sku !== 'Total')
      .map((r) => {
        const vals = valuesForRow(r);
        return { row: r, vals, total: vals.reduce((a, b) => a + b, 0) };
      })
      .sort((a, b) => b.total - a.total)
      .slice(0, 5);
  }, [forecastData, last3SoldOldestFirst, forecast3]);

  const grandTotalSeries = useMemo(
    () => [totalsRow.sold1 || 0, totalsRow.sold2 || 0, totalsRow.sold3 || 0, totalsRow.f1 || 0, totalsRow.f2 || 0, totalsRow.f3 || 0],
    [totalsRow]
  );

  const palette = ["#FDD36F", "#5EA49B", "#ED9F50", "#00627D", "#87AD12", "#C49466"];

  // FIXED:
  // dynamic forecast start index
  const forecastStartIndex = soldLabels.length;

  const datasets = useMemo(() => {
    return top5Rows
      .map((t, i) => ({
        key: `top${i + 1}`,
        label:
          (t.row["Product Name"] as string) ||
          (t.row["sku"] as string) ||
          `Product ${i + 1}`,
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
      }))
      .filter((ds) => selectedSeries[ds.key] !== false);
  }, [top5Rows, selectedSeries, forecastStartIndex]);

  const chartData = useMemo(() => ({ labels: chartLabels, datasets }), [chartLabels, datasets]);

  const chartOptions = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      layout: { padding: { top: 0, bottom: 24 } },
      plugins: {
        legend: { display: false },
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
          title: { display: false, text: 'Months', font: { size: 12 } },
          ticks: { font: { size: 12 } },
        },
        y: {
          title: { display: true, text: 'Units', font: { size: 12 } },
          ticks: { font: { size: 12 } },
          beginAtZero: true,
        },
      },
    }),
    []
  );

  const forecastPlugin = {
    id: 'forecastBackground',
    beforeDraw(chart: any) {
      const { ctx, chartArea, data, scales } = chart;
      const scaleX = scales?.x;
      if (!scaleX || !data?.labels?.length) return;

      const idx = forecastStartIndex;
      if (idx >= data.labels.length) return;

      const startX = scaleX.getPixelForTick(idx);

      ctx.save();
      ctx.fillStyle = 'rgba(217,217,217,0.35)';
      ctx.fillRect(startX, chartArea.top, chartArea.right - startX, chartArea.bottom - startX + startX - chartArea.top);
      ctx.restore();
    },
  };

  const toggleSeries = (name: string) => {
    const selectedCount = Object.values(selectedSeries).filter(Boolean).length;
    const isChecked = !!selectedSeries[name];

    if (isChecked && selectedCount === 1) {
      setShowToggleModal(true);
      return;
    }

    setSelectedSeries((prev) => ({
      ...prev,
      [name]: !isChecked,
    }));
  };

  useEffect(() => {
    if (showToggleModal) {
      alert("At least one series must remain selected.");
      setShowToggleModal(false);
    }
  }, [showToggleModal]);

  useEffect(() => {
    if (!Array.isArray(data) || data.length === 0) return;

    const fetchMonthRange = async () => {
      try {
        const token = localStorage.getItem('jwtToken');
        if (!token) return;

        const resp = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/api/forecast_monthrange?country=${encodeURIComponent(countryName.toLowerCase())}`,
          { method: 'GET', headers: { Authorization: `Bearer ${token}` } }
        );

        if (!resp.ok) return;
        const j = (await resp.json()) as { month_range?: string };
        setMonthRange(j.month_range ?? null);
      } catch {
        // ignore
      }
    };

    fetchMonthRange();
  }, [countryName, data]);

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

    ctx.fillStyle = '#ffffff';
    ctx.fillRect(0, 0, exportCanvas.width, exportCanvas.height);
    ctx.drawImage(sourceCanvas, 0, 0);

    return exportCanvas.toDataURL('image/png');
  };

  const exportPeriodLabel = useMemo(() => {
    const fullMonthIndex = FULL_MONTHS.findIndex(
      (m) => m.toLowerCase() === String(month).toLowerCase()
    );

    if (fullMonthIndex >= 0) {
      return `${MONTH_ABBR[fullMonthIndex]}'${String(year).slice(-2)}`;
    }

    return monthRange || `${month} ${year}`;
  }, [month, year, monthRange]);

  const handleDownload = async () => {
    const dataUrl = getChartPngWithWhiteBg();

    await exportInventoryForecastViewExcel({
      filename: `Inventory_Forecast_View_${countryName}_${month}_${year}.xlsx`,
      countryName,
      month,
      year,
      soldLabels,
      forecastLabels,
      tableRows,
      totalsRow,
      chartImageBase64: dataUrl,
      titleLine: `Amazon ${countryName.toUpperCase()} - Inventory Forecast - ${exportPeriodLabel}`,
      titleCountry: countryName.toUpperCase(),
      platformLabel,
      periodLabel: exportPeriodLabel,
      companyName,
      brandName,
    });
  };

  if (!forecastData.length) return <p style={{ color: 'gray' }}>No data available.</p>;

  return (
    <div>
      <div className="flex flex-col gap-6 mt-5 ">
        <div className="rounded-xl border border-slate-200 bg-white shadow-sm p-3">
          <div className="flex flex-col gap-4">
            <div className="flex items-baseline gap-2">
              <PageBreadcrumb
                pageTitle={
                  <>
                    Forecasted Data -{" "}
                    {monthRange && (
                      <span className="text-green-500">
                        {countryName.toUpperCase()} ({monthRange})
                      </span>
                    )}
                  </>
                }
                variant="page"
                align="left"
                textSize="2xl"
              />
            </div>
            <div className="flex items-center justify-between w-full gap-3">
              <div className="flex flex-col leading-tight">
                <div className="flex items-baseline gap-2">
                  <PageBreadcrumb
                    pageTitle="Top 5 SKUs Inventory Trend"
                    variant="page"
                    align="left"
                    textSize="2xl"
                  />
                </div>
                <p className="text-xs 2xl:text-sm text-charcoal-500 mt-1">
                  Historical data vs forecasted trends
                </p>
              </div>

              <div className="flex items-center gap-3">
                <DownloadIconButton
                  onClick={handleDownload}
                  disabled={demoMode}
                />
              </div>
            </div>
          </div>

          <div className="shrink-0 mt-4 md:mt-2 flex flex-wrap items-center justify-center gap-4 w-full transition-opacity duration-300">
            {top5Rows.map((t, i) => ({
              name: `top${i + 1}`,
              label:
                (t.row["Product Name"] as string) ||
                (t.row["sku"] as string) ||
                `Product ${i + 1}`,
              color: palette[i % palette.length],
            })).map(({ name, label, color }) => {
              const isChecked = !!selectedSeries[name];

              return (
                <label
                  key={name}
                  className="shrink-0 flex items-center gap-1 sm:gap-1.5 font-semibold select-none whitespace-nowrap text-[10px] 2xl:text-xs my-1 2xl:my-3 text-charcoal-500 cursor-pointer"
                >
                  <span
                    className="flex items-center justify-center h-3 w-3 sm:h-3.5 sm:w-3.5 rounded-sm border transition"
                    style={{
                      borderColor: color,
                      backgroundColor: isChecked ? color : "white",
                    }}
                    onClick={(e) => {
                      e.stopPropagation();
                      toggleSeries(name);
                    }}
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

          <div className='w-full h-[550px]'>
            <Line ref={chartRef} data={chartData} options={chartOptions} plugins={[forecastPlugin]} />
          </div>

          <div className="flex justify-center mt-4">
            <div className="flex flex-wrap justify-center items-center gap-6 text-xs ">
              <div className="flex items-center gap-2 text-charcoal-500">
                <span className="inline-block w-8 border-b-2 border-charcoal-500" />
                <span className="whitespace-nowrap">Last 3 months (Actual)</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="inline-block w-8 border-b-2 border-charcoal-500 border-dashed" />
                <span className="whitespace-nowrap">Next 3 months (Forecast)</span>
              </div>
            </div>
          </div>
        </div>

        <div className="rounded-xl border border-slate-200 bg-white shadow-sm p-3">
          <PageBreadcrumb
            pageTitle="Detailed Forecast Data (All SKUs)"
            variant="page"
            align="left"
            textSize="2xl"
          />

          <div className="mt-4 w-full overflow-x-auto">
            <div className="rounded-xl border border-gray-300 overflow-hidden min-w-[900px]">
              <table className="w-full 2xl:text-sm text-xs text-[#414042]">
                <thead>
                  <tr className="font-normal">
                    <th
                      rowSpan={2}
                      className="p-3 border border-gray-300 bg-[#5EA68E] text-[#F8EDCE] font-semibold text-center align-middle"
                    >
                      S.No
                    </th>
                    <th
                      rowSpan={2}
                      className="p-3 border border-gray-300 bg-[#5EA68E] text-[#F8EDCE] font-semibold text-left align-middle"
                    >
                      Product Name
                    </th>
                    <th
                      rowSpan={2}
                      className="p-3 border border-gray-300 bg-[#5EA68E] text-[#F8EDCE] font-semibold text-center align-middle"
                    >
                      SKU
                    </th>

                    <th
                      className="p-3 border border-gray-300 bg-[#5EA68E] text-[#F8EDCE] font-semibold"
                      colSpan={3}
                    >
                      Last 3 Months
                    </th>

                    <th
                      className="p-3 border border-gray-300 bg-[#5EA68E] text-[#F8EDCE] font-semibold"
                      colSpan={3}
                    >
                      Forecasted Months
                    </th>
                  </tr>

                  <tr>
                    <th className="p-2 border border-gray-300 bg-[#5EA68E] text-[#f8edcf]">
                      {soldLabels[0] || ''}
                    </th>
                    <th className="p-2 border border-gray-300 bg-[#5EA68E] text-[#f8edcf]">
                      {soldLabels[1] || ''}
                    </th>
                    <th className="p-2 border border-gray-300 bg-[#5EA68E] text-[#f8edcf]">
                      {soldLabels[2] || ''}
                    </th>
                    <th className="p-2 border border-gray-300 bg-[#5EA68E] text-[#f8edcf]">
                      {forecastLabels[0] || ''}
                    </th>
                    <th className="p-2 border border-gray-300 bg-[#5EA68E] text-[#f8edcf]">
                      {forecastLabels[1] || ''}
                    </th>
                    <th className="p-2 border border-gray-300 bg-[#5EA68E] text-[#f8edcf]">
                      {forecastLabels[2] || ''}
                    </th>
                  </tr>
                </thead>

                <tbody>
                  {tableRows.map((row, i) => (
                    <tr key={i} className="text-center border-t border-gray-300 bg-white">
                      <td className="p-2 border border-gray-300">{row.sNo}</td>
                      <td className="p-2 border border-gray-300 text-left">{row.product}</td>
                      <td className="p-2 border border-gray-300">{row.sku}</td>
                      <td className="p-2 border border-gray-300">{row.sold1}</td>
                      <td className="p-2 border border-gray-300">{row.sold2}</td>
                      <td className="p-2 border border-gray-300">{row.sold3}</td>
                      <td className="p-2 border border-gray-300">{row.f1}</td>
                      <td className="p-2 border border-gray-300">{row.f2}</td>
                      <td className="p-2 border border-gray-300">{row.f3}</td>
                    </tr>
                  ))}

                  <tr className="text-center border-t border-gray-300 bg-[#EFEFEF] font-semibold">
                    <td className="p-2 border border-gray-300"></td>
                    <td className="p-2 border border-gray-300 text-left">Total</td>
                    <td className="p-2 border border-gray-300"></td>
                    <td className="p-2 border border-gray-300">{totalsRow.sold1}</td>
                    <td className="p-2 border border-gray-300">{totalsRow.sold2}</td>
                    <td className="p-2 border border-gray-300">{totalsRow.sold3}</td>
                    <td className="p-2 border border-gray-300">{totalsRow.f1}</td>
                    <td className="p-2 border border-gray-300">{totalsRow.f2}</td>
                    <td className="p-2 border border-gray-300">{totalsRow.f3}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DisplayInventoryForecast;