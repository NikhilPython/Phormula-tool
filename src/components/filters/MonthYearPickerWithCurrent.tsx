"use client";

import React from "react";
import { FaAngleDown } from "react-icons/fa";

type ValueMode = "lower" | "preserve";

interface Props {
  month: string;
  year: string | number;
  onMonthChange: (value: string) => void;
  onYearChange: (value: string) => void;
  valueMode?: ValueMode;
  className?: string;
}

const MONTHS = [
  "January","February","March","April","May","June",
  "July","August","September","October","November","December",
];

const MIN_YEAR = 2024;

const MonthYearPickerWithCurrent: React.FC<Props> = ({
  month,
  year,
  onMonthChange,
  onYearChange,
  valueMode = "lower",
  className = "",
}) => {
  const now = new Date();
  const currentYear = now.getFullYear();

  const yearList = Array.from(
    { length: currentYear - MIN_YEAR + 1 },
    (_, i) => MIN_YEAR + i
  );

  const emitMonth = (raw: string) => {
    if (!raw) return;
    onMonthChange(valueMode === "lower" ? raw.toLowerCase() : raw);
  };

  const wrapCls =
    "relative inline-flex items-center rounded-lg border border-gray-300 bg-white px-3 py-1.5 text-xs 2xl:text-sm shadow-sm";
  const selectCls =
    "appearance-none bg-transparent px-2 py-1 pr-6 text-center text-xs 2xl:text-sm text-[#414042] focus:outline-none cursor-pointer leading-tight";

  return (
    <div className={`flex items-center gap-2 sm:gap-3 ${className}`}>
      {/* MONTH */}
      <div className={wrapCls}>
        <select
          value={month ? MONTHS.find(m => m.toLowerCase() === month)?.toString() : ""}
          onChange={(e) => emitMonth(e.target.value)}
          className={selectCls}
        >
          <option value="">Month</option>
          {MONTHS.map((m) => (
            <option key={m} value={m}>
              {m}
            </option>
          ))}
        </select>

        <span className="pointer-events-none absolute inset-y-0 right-5 flex items-center text-[10px] text-[#414042]">
          <FaAngleDown />
        </span>
      </div>

      {/* YEAR */}
      <div className={wrapCls}>
        <select
          value={year ? String(year) : ""}
          onChange={(e) => onYearChange(e.target.value)}
          className={selectCls}
        >
          <option value="">Year</option>
          {yearList.map((y) => (
            <option key={y} value={y}>
              {y}
            </option>
          ))}
        </select>

        <span className="pointer-events-none absolute inset-y-0 right-5 flex items-center text-[10px] text-[#414042]">
          <FaAngleDown />
        </span>
      </div>
    </div>
  );
};

export default MonthYearPickerWithCurrent;
