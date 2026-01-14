"use client";

import React, { useEffect, useMemo, useState } from "react";
import { Line, Bar } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title as ChartTitle,
  Tooltip,
  Legend,
} from "chart.js";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ChartTitle,
  Tooltip,
  Legend
);

// ----- Types -----
const MONTHS = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
] as const;

type MonthName = typeof MONTHS[number];
type RangeOption = "monthly" | "quarterly" | "yearly";

interface DataRow {
  month: MonthName | string;
  net_sales?: number | string;
  total_orders?: number | string;
  total_tax?: number | string;
  // passthrough for any other backend fields
  [key: string]: unknown;
}

interface GraphProps {
  range: RangeOption;
  selectedYear: string;
  selectedQuarter?: string; // e.g. "Q1" | "Q2" | "Q3" | "Q4"
  selectedMonth?: string;   // e.g. "january"
}

const metrics = [
  { name: "net_sales", label: "Sales", color: "#2CA9E0" },
  { name: "total_orders", label: "Orders", color: "#800080" },
  { name: "total_tax", label: "Tax", color: "#FF5C5C" },
] as const;

const QUARTERS: Record<string, MonthName[]> = {
  Q1: ["January", "February", "March"],
  Q2: ["April", "May", "June"],
  Q3: ["July", "August", "September"],
  Q4: ["October", "November", "December"],
};

const capFirst = (s = "") =>
  s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : "";

const makeTitle = (
  range: RangeOption,
  year?: string,
  quarter?: string,
  month?: string
) => {
  if (range === "quarterly") return `Tracking Profitability - ${quarter ?? ""} ${year ?? ""}`.trim();
  if (range === "monthly")
    return `Tracking Profitability - ${capFirst(month)} ${year ?? ""}`.trim();
  return `Tracking Profitability - ${year ?? ""}`.trim();
};

export default function GraphPageforShopify({
  range,
  selectedYear,
  selectedQuarter,
  selectedMonth,
}: GraphProps) {
  const [rawRows, setRawRows] = useState<DataRow[]>([]);
  const [selectedGraphs, setSelectedGraphs] = useState<Record<string, boolean>>(
    () => {
      const init: Record<string, boolean> = {};
      metrics.forEach((m) => (init[m.name] = true));
      return init;
    }
  );
  const [allValuesZero, setAllValuesZero] = useState(false);

  // Fetch data
  useEffect(() => {
    if (!selectedYear) return;
    const token = typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
    const params = new URLSearchParams({ year: selectedYear });
    if (range === "quarterly" && selectedQuarter) params.set("quarter", selectedQuarter);

    (async () => {
      try {
        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_historyofshopify?${params.toString()}`,
          {
            headers: token ? { Authorization: `Bearer ${token}` } : {},
          }
        );
        const json = await res.json();
        const rows = Array.isArray(json?.data) ? (json.data as DataRow[]) : [];
        setRawRows(rows);
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error("Failed to fetch graph data", err);
        setRawRows([]);
      }
    })();
  }, [range, selectedYear, selectedQuarter]);

  // Sanitize rows for the selected period
  const graphRows = useMemo(() => {
    let rows = [...rawRows];
    rows.sort((a, b) => MONTHS.indexOf(a.month as MonthName) - MONTHS.indexOf(b.month as MonthName));

    if (range === "quarterly" && selectedQuarter) {
      rows = rows.filter((r) => (QUARTERS[selectedQuarter] || []).includes(r.month as MonthName));
    } else if (range === "monthly" && selectedMonth) {
      rows = rows.filter((r) => r.month === capFirst(selectedMonth));
    }

    const allZero =
      rows.length > 0 &&
      rows.every((row) => metrics.every((m) => !row[m.name] || Number(row[m.name]) === 0));
    setAllValuesZero(allZero);

    return rows;
  }, [rawRows, range, selectedQuarter, selectedMonth]);

  const toggleMetric = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, checked } = e.target;
    setSelectedGraphs((prev) => ({ ...prev, [name]: checked }));
  };

  // Line dataset (quarterly/yearly)
  const lineData = useMemo(
    () => ({
      labels: graphRows.map((r) => r.month),
      datasets: metrics
        .filter((m) => selectedGraphs[m.name])
        .map((m) => ({
          label: m.label,
          data: graphRows.map((r) => Number(r[m.name] || 0)),
          borderColor: m.color,
          backgroundColor: m.color,
          fill: false,
          tension: 0.3,
          pointRadius: 3,
        })),
    }),
    [graphRows, selectedGraphs]
  );

  // Monthly bar dataset (single month -> one bar per metric)
  const monthlyBarData = useMemo(() => {
    const row = graphRows[0] || ({} as DataRow);
    const mset = metrics.filter((m) => selectedGraphs[m.name]);
    return {
      labels: mset.map((m) => m.label.toUpperCase()),
      datasets: [
        {
          label: `${capFirst(selectedMonth)} ${selectedYear}`.trim(),
          data: mset.map((m) => Number(row[m.name] || 0)),
          backgroundColor: mset.map((m) => m.color),
          borderColor: mset.map((m) => m.color),
          borderWidth: 1,
        },
      ],
    };
  }, [graphRows, selectedGraphs, selectedMonth, selectedYear]);

  const commonOpts = {
    responsive: true,
    plugins: {
      legend: { display: false },
      title: { display: false },
      tooltip: {
        callbacks: {
          label: (ctx: any) =>
            `${ctx.dataset.label || ctx.label}: ${Number(ctx.parsed.y || 0).toLocaleString()}`,
        },
      },
    },
    scales: {
      x: { title: { display: true, text: range === "monthly" ? "Metric" : "Month" } },
      y: {
        beginAtZero: true,
        title: { display: true, text: "Amount" },
        ticks: { callback: (v: string | number) => Number(v).toLocaleString() },
      },
    },
  } as const;

  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold text-neutral-800">
        {makeTitle(range, selectedYear, selectedQuarter, selectedMonth)}
      </h2>

      {/* Metric toggles */}
      <div
        className={[
          "flex flex-wrap items-center justify-between gap-2 md:gap-3",
          "w-full md:w-4/5 mx-auto",
          allValuesZero ? "opacity-30" : "opacity-100",
        ].join(" ")}
      >
        {metrics.map(({ name, label, color }) => (
          <label
            key={name}
            className="flex items-center gap-2 text-sm font-semibold underline decoration-2"
            style={{ color }}
          >
            <input
              type="checkbox"
              name={name}
              checked={!!selectedGraphs[name]}
              onChange={toggleMetric}
              className="h-4 w-4 rounded border-neutral-300 text-black focus:ring-0"
            />
            {label.toUpperCase()}
          </label>
        ))}
      </div>

      {/* Chart */}
      <div className="rounded-xl border border-neutral-200 p-3 md:p-4 bg-white shadow-sm">
        {range === "monthly" ? (
          <Bar data={monthlyBarData} options={commonOpts} />
        ) : graphRows.length > 0 ? (
          <Line data={lineData} options={commonOpts} />
        ) : (
          <p className="text-sm text-neutral-600">No data available for selected period.</p>
        )}
      </div>
    </div>
  );
}
