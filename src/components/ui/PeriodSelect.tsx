// components/ui/PeriodSelect.tsx
"use client";

import React from "react";
import CustomTable from "@/components/ui/CustomTable";

const monthOptions = [
  "january","february","march","april","may","june",
  "july","august","september","october","november","december",
];

export default function PeriodSelect({
  range,
  setRange,
  selectedMonth,
  setSelectedMonth,
  selectedQuarter,
  setSelectedQuarter,
  selectedYear,
  setSelectedYear,
  yearOptions,
}: {
  range: string;
  setRange: (v: string) => void;
  selectedMonth: string;
  setSelectedMonth: (v: string) => void;
  selectedQuarter: string;
  setSelectedQuarter: (v: string) => void;
  selectedYear: string;
  setSelectedYear: (v: string) => void;
  yearOptions: string[];
}) {
  const headers = [
    "Period",
    range === "monthly" || range === "quarterly" ? "Range" : null,
    "Year",
  ].filter(Boolean) as string[];

  const RangeCell = (
    <select
      className="bg-white text-sm md:text-base focus:outline-none"
      value={range}
      onChange={(e) => {
        setRange(e.target.value);
        setSelectedMonth("");
        setSelectedQuarter("");
        setSelectedYear("");
      }}
    >
      <option value="monthly">Monthly</option>
      <option value="quarterly">Quarterly</option>
      <option value="yearly">Yearly</option>
    </select>
  );

  const RangePicker =
    range === "monthly" ? (
      <select
        className="bg-white text-sm md:text-base focus:outline-none"
        value={selectedMonth}
        onChange={(e) => setSelectedMonth(e.target.value)}
      >
        <option value="">Select</option>
        {monthOptions.map((m) => (
          <option key={m} value={m}>
            {m[0].toUpperCase() + m.slice(1)}
          </option>
        ))}
      </select>
    ) : range === "quarterly" ? (
      <select
        className="bg-white text-sm md:text-base focus:outline-none"
        value={selectedQuarter}
        onChange={(e) => setSelectedQuarter(e.target.value)}
      >
        <option value="">Select</option>
        <option value="Q1">Q1</option>
        <option value="Q2">Q2</option>
        <option value="Q3">Q3</option>
        <option value="Q4">Q4</option>
      </select>
    ) : null;

  const YearPicker = (
    <select
      className="bg-white text-sm md:text-base focus:outline-none"
      value={selectedYear}
      onChange={(e) => setSelectedYear(e.target.value)}
    >
      <option value="">Select</option>
      {yearOptions.map((y) => (
        <option key={y} value={y}>
          {y}
        </option>
      ))}
    </select>
  );

  const row =
    range === "yearly" ? [RangeCell, YearPicker] : [RangeCell, RangePicker, YearPicker];

  return <CustomTable headers={headers} rows={[row]} />;
}
