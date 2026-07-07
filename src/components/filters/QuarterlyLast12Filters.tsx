
"use client";

import React from "react";
import { FaAngleDown } from "react-icons/fa";
import { HiOutlineCalculator } from "react-icons/hi";

export type Range = "monthly" | "quarterly" | "yearly" | "lifetime";

interface Props {
  range: Range | undefined;              // "quarterly" | "yearly" | "lifetime"
  selectedQuarter: string;               // "Q1", "Q2", ...
  selectedYear: string | number;
  yearOptions: (string | number)[];
  onRangeChange: (v: Range) => void;
  onQuarterChange: (v: string) => void;  // "Q1" | "Q2" | ...
  onYearChange: (v: string) => void;
}

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

const monthToQuarter = (m?: string) => {
  if (!m) return "";
  const idx = months.indexOf(m.toLowerCase());
  if (idx === -1) return "";
  return `Q${Math.floor(idx / 3) + 1}`;
};

const QuarterlyLast12Filters: React.FC<Props> = ({
  range,
  selectedQuarter,
  selectedYear,
  yearOptions,
  onRangeChange,
  onQuarterChange,
  onYearChange,
}) => {
  const safeRange: Range =
    range === "quarterly" || range === "yearly" || range === "lifetime"
      ? range
      : "quarterly";

  const getLatestPeriod = (): LatestPeriod | null => {
    if (typeof window === "undefined") return null;
    try {
      const raw = localStorage.getItem("latestFetchedPeriod");
      if (!raw) return null;
      const parsed = JSON.parse(raw) as LatestPeriod;
      if (!parsed.month || !parsed.year) return null;
      return {
        month: parsed.month.toLowerCase(),
        year: String(parsed.year),
      };
    } catch {
      return null;
    }
  };

  const handleRangeChange = (nextRange: Range) => {
    onRangeChange(nextRange);

    // still seed year/quarter from latest period when range changes
    const latest = getLatestPeriod();
    if (!latest) return;

    const { month, year } = latest;

    if (year) {
      onYearChange(String(year));
    }

    if (nextRange === "quarterly" && month) {
      const q = monthToQuarter(month);
      if (q) onQuarterChange(q);
    }
  };

  // 🔹 Seed from latestFetchedPeriod only once – but now we ALWAYS
  // override initial parent values so data + dropdown are in sync.
  const initializedRef = React.useRef(false);
  React.useEffect(() => {
    if (initializedRef.current) return;
    initializedRef.current = true;

    const latest = getLatestPeriod();
    if (!latest) return;

    const { month, year } = latest;

    // 👉 Always push the latest year/quarter up to the parent on first mount
    if (year) {
      onYearChange(String(year));
    }

    if (safeRange === "quarterly" && month) {
      const q = monthToQuarter(month);
      if (q) {
        onQuarterChange(q);
      }
    }
  }, [safeRange, onYearChange, onQuarterChange]);

  // 🔹 Build "all 4 quarters for the current year"
  const lastFourQuarters = React.useMemo(() => {
    const pickBaseYear = () => {
      if (selectedYear !== "" && selectedYear !== undefined) {
        const n = Number(selectedYear);
        if (!Number.isNaN(n)) return n;
      }

      const latest = getLatestPeriod();
      if (latest?.year) {
        const n = Number(latest.year);
        if (!Number.isNaN(n)) return n;
      }

      return new Date().getFullYear();
    };

    const y = pickBaseYear();
    const shortYear = String(y).slice(-2);

    return [1, 2, 3, 4].map((q) => ({
      year: y,
      quarter: q,
      label: `Q${q} '${shortYear}`,
      value: `${y}-Q${q}`,
    }));
  }, [selectedYear]);

  // 🔹 Map current selection to "YYYY-Qx"
  const currentQuarterValue = React.useMemo(() => {
    const y = selectedYear ? String(selectedYear) : "";
    const q = selectedQuarter || "";
    return y && q ? `${y}-${q}` : "";
  }, [selectedYear, selectedQuarter]);

  const wrapCls =
    "relative inline-flex items-center rounded-lg border border-gray-300 bg-white px-3 py-1.5 text-xs 2xl:text-sm shadow-sm";
  const selectCls =
    "appearance-none bg-transparent px-2 py-1 pr-6 text-center text-xs 2xl:text-sm text-[#414042] focus:outline-none cursor-pointer leading-tight";

  return (
    <div className="flex items-center gap-2 sm:gap-3">
      {/* 1️⃣ Range selector: Quarterly / Last 12 Months / Lifetime */}
      <div className={wrapCls}>
        <select
          value={safeRange}
          onChange={(e) => handleRangeChange(e.target.value as Range)}
          className={selectCls}
        >
          <option value="quarterly">Quarterly</option>
          <option value="yearly">Last 12 Months</option>
          <option value="lifetime">Lifetime</option>
        </select>
        <span className="pointer-events-none absolute inset-y-0 right-5 flex items-center text-[10px] text-[#414042]">
          <FaAngleDown />
        </span>
      </div>

      {/* 2️⃣ Quarter picker – only when Quarterly is selected */}
      {safeRange === "quarterly" && (
        <div className={wrapCls}>
          {/* <span className="mr-2 flex items-center text-base text-[#414042]/70">
            <HiOutlineCalculator />
          </span> */}

          <select
            value={currentQuarterValue}
            onChange={(e) => {
              const value = e.target.value; // "YYYY-Qx"
              const [yStr, qStr] = value.split("-");
              if (yStr) onYearChange(yStr);
              if (qStr) onQuarterChange(qStr); // "Q1", "Q2", ...
            }}
            className={selectCls}
          >
            {lastFourQuarters.map((q) => (
              <option key={q.value} value={q.value}>
                {q.label}
              </option>
            ))}
          </select>

          <span className="pointer-events-none absolute inset-y-0 right-5 flex items-center text-[10px] text-[#414042]">

            <FaAngleDown />
          </span>
        </div>
      )}
    </div>
  );
};

export default QuarterlyLast12Filters;
