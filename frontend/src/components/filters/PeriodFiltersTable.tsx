

"use client";

import React from "react";
import { FaAngleDown } from "react-icons/fa";

export type Range = "monthly" | "quarterly" | "yearly";
export type RangeValue = Range | "";
export type AvailablePeriods = Record<string, string[]>;

interface Props {
  range: RangeValue | undefined;
  selectedMonth: string;
  selectedQuarter: string;
  selectedYear: string | number;
  yearOptions: (string | number)[];
  onRangeChange: (v: Range) => void;
  onMonthChange: (v: string) => void;
  onQuarterChange: (v: string) => void;
  onYearChange: (v: string) => void;
  allowedRanges?: Range[];
  availablePeriods?: AvailablePeriods | null;
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

const quarterToMonths: Record<string, string[]> = {
  Q1: ["january", "february", "march"],
  Q2: ["april", "may", "june"],
  Q3: ["july", "august", "september"],
  Q4: ["october", "november", "december"],
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
    availablePeriods,
  } = props;

  const safeRange: Range | "" =
    range && allowedRanges.includes(range)
      ? range
      : allowedRanges.includes("yearly")
        ? "yearly"
        : allowedRanges[0] ?? "";

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
  const hasAvailabilityRules =
    availablePeriods !== undefined && availablePeriods !== null;

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

  const getFetchedMonthsForYear = (y: number) => {
    if (!hasAvailabilityRules) return months;

    return Array.from(
      new Set(
        (availablePeriods?.[String(y)] ?? [])
          .map((m) => String(m || "").toLowerCase())
          .filter((m) => months.includes(m))
      )
    );
  };

  const fetchedMonthExistsInYear = (y: number, m: string) => {
    if (!hasAvailabilityRules) return true;
    return getFetchedMonthsForYear(y).includes(m.toLowerCase());
  };

  const fetchedQuarterExistsInYear = (y: number, q: string) => {
    if (!hasAvailabilityRules) return true;

    const quarterMonths = quarterToMonths[q] ?? [];
    if (!quarterMonths.length) return true;

    return quarterMonths.some((m) => fetchedMonthExistsInYear(y, m));
  };

  const yearHasFetchedMonths = (y: number) => {
    if (!hasAvailabilityRules) return true;

    const historicMonths = getAllowedMonthsForYear(y);
    if (!historicMonths.length) return false;

    return historicMonths.some((m) => fetchedMonthExistsInYear(y, m));
  };

  const getLatestAvailablePeriod = (): LatestPeriod | null => {
    if (!hasAvailabilityRules) return null;

    const candidates = Object.entries(availablePeriods ?? {}).flatMap(
      ([year, yearMonths]) => {
        const y = Number(year);
        if (!Number.isFinite(y)) return [];

        return yearMonths
          .map((month) => ({
            year: String(y),
            month: String(month || "").toLowerCase(),
          }))
          .filter(({ month }) => months.includes(month))
          .filter(({ month }) => getAllowedMonthsForYear(y).includes(month));
      }
    );

    if (!candidates.length) return null;

    return candidates.sort((a, b) => {
      const yDiff = Number(b.year) - Number(a.year);
      if (yDiff !== 0) return yDiff;
      return monthIndex(b.month) - monthIndex(a.month);
    })[0];
  };

  const hasSelectableMonthlyPeriod = () => {
    if (!hasAvailabilityRules) return true;
    return yearList.some((y) =>
      getAllowedMonthsForYear(y).some((m) => fetchedMonthExistsInYear(y, m))
    );
  };

  const hasSelectableQuarterlyPeriod = () => {
    if (!hasAvailabilityRules) return true;
    return yearList.some((y) =>
      ["Q1", "Q2", "Q3", "Q4"].some(
        (q) => quarterAllowedInYear(y, q) && fetchedQuarterExistsInYear(y, q)
      )
    );
  };

  const hasSelectableYearlyPeriod = () => {
    if (!hasAvailabilityRules) return true;
    return yearList.some((y) => yearHasFetchedMonths(y));
  };

  const isRangeDisabled = (r: Range) => {
    if (!allowedRanges.includes(r)) return true;

    if (r === "monthly") return !hasSelectableMonthlyPeriod();
    if (r === "quarterly") return !hasSelectableQuarterlyPeriod();
    if (r === "yearly") return !hasSelectableYearlyPeriod();

    return false;
  };

  /* =========================
     Range change (seed from latestFetchedPeriod but normalized to historic)
     ========================= */

  const handleRangeChange = (nextRange: Range) => {
    onRangeChange(nextRange);

    // Yearly does not need month/quarter seeding.
    // Keep the current selected year.
    if (nextRange === "yearly") return;

    const latest = getLatestAvailablePeriod() ?? getLatestPeriod();
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
  // const initializedRef = React.useRef(false);

  // React.useEffect(() => {
  //   if (initializedRef.current) return;
  //   initializedRef.current = true;

  //   const latest = getLatestPeriod();
  //   if (!latest?.month || !latest?.year) return;

  //   const y = Number(latest.year);
  //   if (Number.isNaN(y)) return;

  //   const normalized = getLatestHistoricMonthly(y, latest.month);
  //   if (!normalized) return;

  //   // ✅ Force default year to latest HISTORIC (only once)
  //   if (String(selectedYear) !== normalized.year) {
  //     onYearChange(normalized.year);
  //   }

  //   // ✅ Optionally seed month/quarter once if empty
  //   if (safeRange === "monthly") {
  //     if (!selectedMonth) onMonthChange(normalized.month);
  //   }

  //   if (safeRange === "quarterly") {
  //     if (!selectedQuarter || selectedQuarter === "Range") {
  //       const q = monthToQuarter(normalized.month);
  //       if (q) onQuarterChange(q);
  //     }
  //   }
  //   // eslint-disable-next-line react-hooks/exhaustive-deps
  // }, []);

  /* =========================
     Month/Quarter disable rules (kept for safety)
     ========================= */

  // In currentYear: disable current month and future months
  const isMonthDisabled = (m: string) => {
    const mIdx = months.indexOf(m);
    if (mIdx === -1) return false;
    if (selectedYearNum === currentYear && mIdx >= currentMonthIndex) {
      return true;
    }
    if (Number.isNaN(selectedYearNum)) return false;
    return !fetchedMonthExistsInYear(selectedYearNum, m);
  };

  // In currentYear: disable quarters whose START month is current month or later
  const isQuarterDisabled = (q: string) => {
    const qNum = Number(q.replace("Q", ""));
    const quarterStartMonth = (qNum - 1) * 3; // Q1=0, Q2=3...
    if (selectedYearNum === currentYear && quarterStartMonth >= currentMonthIndex) {
      return true;
    }
    if (Number.isNaN(selectedYearNum)) return false;
    return !fetchedQuarterExistsInYear(selectedYearNum, q);
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
      return (
        !monthAllowedInYear(y, selectedMonth) ||
        !fetchedMonthExistsInYear(y, selectedMonth)
      );
    }

    if (safeRange === "quarterly" && selectedQuarter) {
      return (
        !quarterAllowedInYear(y, selectedQuarter) ||
        !fetchedQuarterExistsInYear(y, selectedQuarter)
      );
    }

    if (safeRange === "monthly") {
      return !getAllowedMonthsForYear(y).some((m) =>
        fetchedMonthExistsInYear(y, m)
      );
    }

    if (safeRange === "quarterly") {
      return !["Q1", "Q2", "Q3", "Q4"].some(
        (q) => quarterAllowedInYear(y, q) && fetchedQuarterExistsInYear(y, q)
      );
    }

    if (safeRange === "yearly") {
      return !yearHasFetchedMonths(y);
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
    "px-1 py-0.5 pr-5 sm:px-1.5 sm:py-1 sm:pr-7 2xl:px-2 2xl:py-1 2xl:pr-6 " +
    "text-[10px] sm:text-xs 2xl:text-sm";


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
            <option value="monthly" disabled={isRangeDisabled("monthly")}>
              Monthly
            </option>
          )}
          {allowedRanges.includes("quarterly") && (
            <option value="quarterly" disabled={isRangeDisabled("quarterly")}>
              Quarterly
            </option>
          )}
          {allowedRanges.includes("yearly") && (
            <option value="yearly" disabled={isRangeDisabled("yearly")}>
              Yearly
            </option>
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
                <option key={m} value={m} disabled={isMonthDisabled(m)}>
                  {cap(m)}
                </option>
              ))}

            {safeRange === "quarterly" &&
              ["Q1", "Q2", "Q3", "Q4"].map((q) => (
                <option key={q} value={q} disabled={isQuarterDisabled(q)}>
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
