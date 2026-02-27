

"use client";

import React from "react";
import { FaAngleDown } from "react-icons/fa";

export type Range = "monthly" | "quarterly" | "yearly";

interface Props {
  range: "monthly" | "quarterly" | "yearly" | undefined;
  selectedMonth: string;
  selectedQuarter: string;
  selectedYear: string | number;
  yearOptions: (string | number)[]; // kept for compatibility
  onRangeChange: (v: Range) => void;
  onMonthChange: (v: string) => void;
  onQuarterChange: (v: string) => void;
  onYearChange: (v: string) => void;
  allowedRanges?: Range[];
}

const ALL_RANGES: Range[] = ["monthly", "quarterly", "yearly"];
type LatestPeriod = { month?: string; year?: string };

const months = [
  "january",
  "february",
  "march",
  "april",
  "may",
  "june",
  "july",
  "august",
  "september",
  "october",
  "november",
  "december",
];

const cap = (s: string) => (s ? s[0].toUpperCase() + s.slice(1) : "");

const monthToQuarter = (m?: string) => {
  if (!m) return "";
  const idx = months.indexOf(m.toLowerCase());
  if (idx === -1) return "";
  return `Q${Math.floor(idx / 3) + 1}`;
};

const MIN_YEAR = 2024;

const PeriodFiltersTable: React.FC<Props> = (props) => {
  const {
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    onRangeChange,
    onMonthChange,
    onQuarterChange,
    onYearChange,
    allowedRanges = ALL_RANGES,
  } = props;

  const safeRange: Range | "" =
    range && allowedRanges.includes(range as Range) ? (range as Range) : "";

  // Current date (client)
  const now = new Date();
  const currentYear = now.getFullYear();
  const currentMonthIndex = now.getMonth(); // 0..11

  // Year list: 2024 → current year
  const yearList = Array.from(
    { length: currentYear - MIN_YEAR + 1 },
    (_, i) => MIN_YEAR + i
  );

  const selectedYearNum = Number(selectedYear);

  const getLatestPeriod = (): LatestPeriod | null => {
    if (typeof window === "undefined") return null;
    try {
      const raw = localStorage.getItem("latestFetchedPeriod");
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (!parsed.month || !parsed.year) return null;
      return { month: String(parsed.month).toLowerCase(), year: String(parsed.year) };
    } catch {
      return null;
    }
  };

  const handleMonthChange = (m: string) => {
    const mIdx = months.indexOf(m.toLowerCase());

    if (selectedYearNum === currentYear && mIdx >= currentMonthIndex) {
      // Invalid for current year → switch to previous year
      onYearChange(String(currentYear - 1));
    }

    onMonthChange(m);
  };

  const handleQuarterChange = (q: string) => {
    const qNum = Number(q.replace("Q", ""));
    const quarterStartMonth = (qNum - 1) * 3;

    if (
      selectedYearNum === currentYear &&
      quarterStartMonth >= currentMonthIndex
    ) {
      // Invalid quarter for current year → switch to previous year
      onYearChange(String(currentYear - 1));
    }

    onQuarterChange(q);
  };



  /* =========================
     Helpers: historic month normalization + allowed month list
     ========================= */

  const monthIndex = (m?: string) => (m ? months.indexOf(m.toLowerCase()) : -1);

  // Returns latest HISTORIC monthly period (month strictly before current month if same year)
  const getLatestHistoricMonthly = (y: number, m: string) => {
    let year = y;
    let idx = monthIndex(m);
    if (idx === -1) return null;

    // clamp year bounds
    year = Math.min(Math.max(year, MIN_YEAR), currentYear);

    if (year === currentYear) {
      // only months strictly before current month
      if (idx >= currentMonthIndex) idx = currentMonthIndex - 1;

      // If it's January (currentMonthIndex=0) -> no historic month in current year.
      if (idx < 0) {
        year = currentYear - 1;
        if (year < MIN_YEAR) return null;
        idx = 11; // december
      }
    }

    return { year: String(year), month: months[idx] };
  };

  // For dropdown: show only historic months in current year, all months in past years
  const getAllowedMonthsForYear = (y: number) => {
    if (!y || Number.isNaN(y)) return months;
    if (y === currentYear) return months.slice(0, currentMonthIndex);
    if (y < MIN_YEAR) return [];
    if (y > currentYear) return [];
    return months;
  };

  /* =========================
     Range change (seed from latestFetchedPeriod but normalized to historic)
     ========================= */

  const handleRangeChange = (nextRange: Range) => {
    onRangeChange(nextRange);

    const latest = getLatestPeriod();
    if (!latest?.month || !latest?.year) return;

    const y = Number(latest.year);
    if (Number.isNaN(y)) return;

    const normalized = getLatestHistoricMonthly(y, latest.month);
    if (!normalized) return;

    onYearChange(normalized.year);

    if (nextRange === "monthly") {
      onMonthChange(normalized.month);
    }

    if (nextRange === "quarterly") {
      const q = monthToQuarter(normalized.month);
      if (q) onQuarterChange(q);
    }
  };

  // Seed once from latestFetchedPeriod (but normalized to historic)
  const initializedRef = React.useRef(false);

  React.useEffect(() => {
    if (initializedRef.current) return;
    initializedRef.current = true;

    const latest = getLatestPeriod();
    if (!latest?.month || !latest?.year) return;

    const y = Number(latest.year);
    if (Number.isNaN(y)) return;

    const normalized = getLatestHistoricMonthly(y, latest.month);
    if (!normalized) return;

    // ✅ Force default year to latest HISTORIC (only once)
    if (String(selectedYear) !== normalized.year) {
      onYearChange(normalized.year);
    }

    // ✅ Optionally seed month/quarter once if empty
    if (safeRange === "monthly") {
      if (!selectedMonth) onMonthChange(normalized.month);
    }

    if (safeRange === "quarterly") {
      if (!selectedQuarter || selectedQuarter === "Range") {
        const q = monthToQuarter(normalized.month);
        if (q) onQuarterChange(q);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /* =========================
     Month/Quarter disable rules (kept for safety)
     ========================= */

  // In currentYear: disable current month and future months
  const isMonthDisabled = (m: string) => {
    const mIdx = months.indexOf(m);
    if (mIdx === -1) return false;
    if (selectedYearNum === currentYear) return mIdx >= currentMonthIndex;
    return false;
  };

  // In currentYear: disable quarters whose START month is current month or later
  const isQuarterDisabled = (q: string) => {
    if (selectedYearNum !== currentYear) return false;
    const qNum = Number(q.replace("Q", ""));
    const quarterStartMonth = (qNum - 1) * 3; // Q1=0, Q2=3...
    return quarterStartMonth >= currentMonthIndex;
  };

  /* =========================
     Year disable rules
     ========================= */

  const monthAllowedInYear = (y: number, m: string) => {
    const mIdx = months.indexOf(m.toLowerCase());
    if (mIdx === -1) return true;
    if (y === currentYear) return mIdx < currentMonthIndex;
    return true;
  };

  const quarterAllowedInYear = (y: number, q: string) => {
    const qNum = Number(q.replace("Q", ""));
    if (![1, 2, 3, 4].includes(qNum)) return true;

    if (y === currentYear) {
      const quarterStartMonth = (qNum - 1) * 3;
      return quarterStartMonth < currentMonthIndex;
    }
    return true;
  };

  const isYearDisabled = (y: number) => {
    if (y < MIN_YEAR || y > currentYear) return true;

    if (safeRange === "monthly" && selectedMonth) {
      return !monthAllowedInYear(y, selectedMonth);
    }

    if (safeRange === "quarterly" && selectedQuarter) {
      return !quarterAllowedInYear(y, selectedQuarter);
    }

    return false;
  };

  /* -------- UI classes -------- */
  const wrapCls =
    "relative inline-flex items-center rounded-md sm:rounded-lg border border-gray-300 bg-white " +
    "px-2 py-1 2xl:px-3 2xl:py-1.5 " +
    "text-[10px] sm:text-xs lg:text-sm 2xl:text-sm shadow-sm";

  const selectCls =
    "appearance-none bg-transparent text-center text-[#414042] focus:outline-none cursor-pointer " +
    "px-1 py-0.5 pr-5 lg:px-1.5 lg:py-1 lg:pr-7 xl:px-2 xl:py-1 xl:pr-6 " +
    "text-[10px] sm:text-xs lg:text-sm 2xl:text-sm";


  return (
    <div className="flex items-center gap-1.5 sm:gap-3">
      {/* Period */}
      <div className={wrapCls}>
        <select
          value={safeRange}
          onChange={(e) => handleRangeChange(e.target.value as Range)}
          className={selectCls}
        >
          <option value="" disabled>
            Period
          </option>
          {allowedRanges.includes("monthly") && (
            <option value="monthly">Monthly</option>
          )}
          {allowedRanges.includes("quarterly") && (
            <option value="quarterly">Quarterly</option>
          )}
          {allowedRanges.includes("yearly") && (
            <option value="yearly">Yearly</option>
          )}
        </select>
        <span className="pointer-events-none absolute inset-y-0 right-3 sm:right-5 flex items-center text-[9px] sm:text-[10px]">
          <FaAngleDown />
        </span>
      </div>

      {/* Month / Quarter */}
      {(safeRange === "monthly" || safeRange === "quarterly") && (
        <div className={wrapCls}>
          <select
            value={safeRange === "monthly" ? selectedMonth : selectedQuarter}
            onChange={(e) =>
              safeRange === "monthly"
                ? handleMonthChange(e.target.value)
                : handleQuarterChange(e.target.value)
            }
            className={selectCls}
          >
            <option value="">Range</option>

            {safeRange === "monthly" &&
              months.map((m) => (
                <option key={m} value={m}>
                  {cap(m)}
                </option>
              ))}

            {safeRange === "quarterly" &&
              ["Q1", "Q2", "Q3", "Q4"].map((q) => (
                <option key={q} value={q}>
                  {q}
                </option>
              ))}
          </select>

          <span className="pointer-events-none absolute inset-y-0 right-3 sm:right-5 flex items-center text-[9px] sm:text-[10px]">
            <FaAngleDown />
          </span>
        </div>
      )}

      {/* Year */}
      <div className={wrapCls}>
        <select
          value={selectedYear ? String(selectedYear) : ""}
          onChange={(e) => onYearChange(e.target.value)}
          className={selectCls}
        >
          <option value="">Year</option>
          {yearList.map((y) => (
            <option key={y} value={y} disabled={isYearDisabled(y)}>
              {y}
            </option>
          ))}
        </select>

        <span className="pointer-events-none absolute inset-y-0 right-3 sm:right-5 flex items-center text-[9px] sm:text-[10px]">
          <FaAngleDown />
        </span>
      </div>
    </div>
  );
};

export default PeriodFiltersTable;
