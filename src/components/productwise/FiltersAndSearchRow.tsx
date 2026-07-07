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
  const safeRange: PeriodRange = toPeriodRange(range) ?? "yearly";

  const safeAllowedRanges: PeriodRange[] = useMemo(() => {
    const filtered = toPeriodRanges(allowedRanges);
    return filtered.length ? filtered : (["quarterly", "yearly"] as PeriodRange[]);
  }, [allowedRanges]);

  useEffect(() => {
    if (!toPeriodRange(range)) {
      onRangeChange("yearly");
    }
  }, [range, onRangeChange]);

  useEffect(() => {
    if (safeRange !== "yearly") return;

    if (selectedYear === "" && years?.length) {
      const latestYear = Math.max(...years);
      onYearChange(String(latestYear));
    }
  }, [safeRange, selectedYear, years, onYearChange]);

  return (
    // <div className="flex items-center gap-3">
    <div className="mb-2 sm:mb-0">
      <div className="flex flex-col md:flex-row sm:items-center  gap-[0.5vw]">
        <PeriodFiltersTable
          range={safeRange}
          selectedMonth={selectedMonth}
          selectedQuarter={selectedQuarter}
          selectedYear={selectedYear}
          yearOptions={years}
          allowedRanges={safeAllowedRanges}
          onRangeChange={(r) => onRangeChange(r)}
          onMonthChange={onMonthChange}
          onQuarterChange={onQuarterChange}
          onYearChange={onYearChange}
        />
      </div>
    </div>
  );
};

export default FiltersAndSearchRow;