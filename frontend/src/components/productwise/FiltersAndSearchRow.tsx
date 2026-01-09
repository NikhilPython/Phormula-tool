"use client";

import React from "react";
import { Range } from "./productwiseHelpers";
import QuarterlyLast12Filters from "../filters/QuarterlyLast12Filters";

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
  return (
    <div className="flex items-center gap-3">

      <QuarterlyLast12Filters
        range={range}
        selectedQuarter={selectedQuarter}   // already "Q1" | "Q2" | ...
        selectedYear={selectedYear}
        yearOptions={years}
        onRangeChange={onRangeChange}
        onQuarterChange={onQuarterChange}   // pass "Q1"/"Q2"/"Q3"/"Q4" straight up
        onYearChange={onYearChange}
      />

    </div>
  );
};

export default FiltersAndSearchRow;




