// "use client";

// import React from "react";
// import { Range } from "./productwiseHelpers";
// import QuarterlyLast12Filters from "../filters/QuarterlyLast12Filters";

// interface FiltersAndSearchRowProps {
//   range: Range | undefined;
//   selectedMonth: string;
//   selectedQuarter: string;
//   selectedYear: number | "";
//   years: number[];
//   allowedRanges?: Range[];
//   onRangeChange: (range: Range) => void;
//   onMonthChange: (month: string) => void;
//   onQuarterChange: (quarter: string) => void;
//   onYearChange: (year: string) => void;
// }

// const FiltersAndSearchRow: React.FC<FiltersAndSearchRowProps> = ({
//   range,
//   selectedMonth,
//   selectedQuarter,
//   selectedYear,
//   years,
//   allowedRanges = ["quarterly", "yearly"],
//   onRangeChange,
//   onMonthChange,
//   onQuarterChange,
//   onYearChange,
// }) => {
//   return (
//     <div className="flex items-center gap-3">

//       <QuarterlyLast12Filters
//         range={range}
//         selectedQuarter={selectedQuarter}   // already "Q1" | "Q2" | ...
//         selectedYear={selectedYear}
//         yearOptions={years}
//         onRangeChange={onRangeChange}
//         onQuarterChange={onQuarterChange}   // pass "Q1"/"Q2"/"Q3"/"Q4" straight up
//         onYearChange={onYearChange}
//       />

//     </div>
//   );
// };

// export default FiltersAndSearchRow;





"use client";

import React, { useEffect, useMemo } from "react";
import {
  Range,
  PeriodRange,
  toPeriodRange,
  toPeriodRanges,
} from "./productwiseHelpers";
import PeriodFiltersTable from "../filters/PeriodFiltersTable";

interface FiltersAndSearchRowProps {
  range: Range | undefined;
  selectedMonth: string;
  selectedQuarter: string;
  selectedYear: number | "";
  years: number[];
  allowedRanges?: Range[];
  onRangeChange: (range: Range) => void;
  onMonthChange: (month: string) => void;
  onQuarterChange: (quarter: string) => void;
  onYearChange: (year: string) => void;
}

const FiltersAndSearchRow: React.FC<FiltersAndSearchRowProps> = ({
  range,
  selectedMonth,
  selectedQuarter,
  selectedYear,
  years,
  allowedRanges = ["quarterly", "yearly"],
  onRangeChange,
  onMonthChange,
  onQuarterChange,
  onYearChange,
}) => {
  // ✅ Map productwise Range -> PeriodRange (drops "lifetime")
  const safeRange: PeriodRange = toPeriodRange(range) ?? "yearly";

  // ✅ Filter allowed ranges to what PeriodFiltersTable supports
  const safeAllowedRanges: PeriodRange[] = useMemo(() => {
    const filtered = toPeriodRanges(allowedRanges);
    return filtered.length ? filtered : (["quarterly", "yearly"] as PeriodRange[]);
  }, [allowedRanges]);

  // ✅ Default to YEARLY if range is missing OR unsupported (e.g. "lifetime")
  useEffect(() => {
    if (!toPeriodRange(range)) {
      onRangeChange("yearly");
    }
  }, [range, onRangeChange]);

  // ✅ When in yearly mode, ensure a default year is selected (latest available)
  useEffect(() => {
    if (safeRange !== "yearly") return;

    if (selectedYear === "" && years?.length) {
      const latestYear = Math.max(...years);
      onYearChange(String(latestYear));
    }
  }, [safeRange, selectedYear, years, onYearChange]);

  return (
    <div className="flex items-center gap-3">
      <PeriodFiltersTable
        range={safeRange}
        selectedMonth={selectedMonth}
        selectedQuarter={selectedQuarter}
        selectedYear={selectedYear}
        yearOptions={years}
        allowedRanges={safeAllowedRanges}
        onRangeChange={(r) => onRangeChange(r)} // r is PeriodRange (subset of Range)
        onMonthChange={onMonthChange}
        onQuarterChange={onQuarterChange}
        onYearChange={onYearChange}
      />
    </div>
  );
};

export default FiltersAndSearchRow;
