// "use client";

// import React from "react";
// import { FaAngleDown } from "react-icons/fa";
// import { HiOutlineCalculator } from "react-icons/hi";

// export type Range = "monthly" | "quarterly" | "yearly";

// interface Props {
//   range: Range | undefined;              // we will only use "quarterly" | "yearly"
//   selectedQuarter: string;               // "Q1", "Q2", ...
//   selectedYear: string | number;
//   yearOptions: (string | number)[];
//   onRangeChange: (v: Range) => void;     // "quarterly" or "yearly"
//   onQuarterChange: (v: string) => void;  // "Q1" | "Q2" | ...
//   onYearChange: (v: string) => void;
// }

// type LatestPeriod = { month?: string; year?: string };

// const months = [
//   "january",
//   "february",
//   "march",
//   "april",
//   "may",
//   "june",
//   "july",
//   "august",
//   "september",
//   "october",
//   "november",
//   "december",
// ];

// const monthToQuarter = (m?: string) => {
//   if (!m) return "";
//   const idx = months.indexOf(m.toLowerCase());
//   if (idx === -1) return "";
//   return `Q${Math.floor(idx / 3) + 1}`;
// };

// const QuarterlyLast12Filters: React.FC<Props> = ({
//   range,
//   selectedQuarter,
//   selectedYear,
//   yearOptions,
//   onRangeChange,
//   onQuarterChange,
//   onYearChange,
// }) => {
//   // we only care about "quarterly" and "yearly" here
//   const safeRange: Range =
//     range === "quarterly" || range === "yearly" ? range : "quarterly";

//   const getLatestPeriod = (): LatestPeriod | null => {
//     if (typeof window === "undefined") return null;
//     try {
//       const raw = localStorage.getItem("latestFetchedPeriod");
//       if (!raw) return null;
//       const parsed = JSON.parse(raw) as LatestPeriod;
//       if (!parsed.month || !parsed.year) return null;
//       return {
//         month: parsed.month.toLowerCase(),
//         year: String(parsed.year),
//       };
//     } catch {
//       return null;
//     }
//   };

//   const handleRangeChange = (nextRange: Range) => {
//     // we only expect "quarterly" or "yearly" coming from the UI
//     onRangeChange(nextRange);

//     const latest = getLatestPeriod();
//     if (!latest) return;

//     const { month, year } = latest;

//     if (year) {
//       onYearChange(String(year));
//     }

//     if (nextRange === "quarterly" && month) {
//       const q = monthToQuarter(month);
//       if (q) onQuarterChange(q);
//     }
//     // "yearly" (Last 12 Months) → only year is relevant, already set
//   };

//   // On first mount, seed from latestFetchedPeriod if nothing selected
//   const initializedRef = React.useRef(false);
//   React.useEffect(() => {
//     if (initializedRef.current) return;
//     initializedRef.current = true;

//     const latest = getLatestPeriod();
//     if (!latest) return;

//     const { month, year } = latest;

//     const hasYear = selectedYear !== "" && selectedYear !== undefined;
//     const hasQuarter = !!selectedQuarter && selectedQuarter !== "Range";

//     if (!hasYear && year) {
//       onYearChange(String(year));
//     }

//     if (safeRange === "quarterly" && month && !hasQuarter) {
//       const q = monthToQuarter(month);
//       if (q) onQuarterChange(q);
//     }
//   }, [safeRange, selectedQuarter, selectedYear, onYearChange, onQuarterChange]);

//   return (
//     <div className="flex items-center gap-2">
//       {/* 1️⃣ Primary filter: Quarterly / Last 12 Months */}
//       <div className="relative inline-flex items-center rounded-md border border-[#414042] bg-white px-3 py-1.5 text-xs sm:text-sm">
//         <select
//           value={safeRange}
//           onChange={(e) => handleRangeChange(e.target.value as Range)}
//           className="appearance-none bg-transparent pr-6 text-xs sm:text-sm focus:outline-none cursor-pointer"
//         >
//           <option value="quarterly">Quarterly</option>
//           <option value="yearly">Last 12 Months</option>
//         </select>
//         <span className="pointer-events-none absolute inset-y-0 right-2 flex items-center text-xs text-[#414042]">
//           <FaAngleDown />
//         </span>
//       </div>

//       {/* 2️⃣ Select Range (only for Quarterly) */}
//       {safeRange === "quarterly" && (
//         <div className="relative inline-flex items-center rounded-md border border-[#414042] bg-white px-3 py-1.5 text-xs sm:text-sm">
//           <span className="mr-2 flex items-center text-base text-[#414042]">
//             <HiOutlineCalculator />
//           </span>
//           <select
//             value={selectedQuarter || ""}
//             onChange={(e) => onQuarterChange(e.target.value)}
//             className="appearance-none bg-transparent pr-6 text-xs sm:text-sm focus:outline-none cursor-pointer"
//           >
//             <option value="">Select Range</option>
//             {["Q1", "Q2", "Q3", "Q4"].map((q) => (
//               <option key={q} value={q}>
//                 {q}
//               </option>
//             ))}
//           </select>
//           <span className="pointer-events-none absolute inset-y-0 right-2 flex items-center text-xs text-[#414042]">
//             <FaAngleDown />
//           </span>
//         </div>
//       )}

//       {/* 3️⃣ Year selector (always visible) */}
//       <div className="relative inline-flex items-center rounded-md border border-[#414042] bg-white px-3 py-1.5 text-xs sm:text-sm">
//         <select
//           value={selectedYear ? String(selectedYear) : ""}
//           onChange={(e) => onYearChange(e.target.value)}
//           className="appearance-none bg-transparent pr-6 text-xs sm:text-sm focus:outline-none cursor-pointer"
//         >
//           <option value="">Year</option>
//           {yearOptions.map((y) => (
//             <option key={y} value={y}>
//               {y}
//             </option>
//           ))}
//         </select>
//         <span className="pointer-events-none absolute inset-y-0 right-2 flex items-center text-xs text-[#414042]">
//           <FaAngleDown />
//         </span>
//       </div>
//     </div>
//   );
// };

// export default QuarterlyLast12Filters;









// "use client";

// import React from "react";
// import { FaAngleDown } from "react-icons/fa";
// import { HiOutlineCalculator } from "react-icons/hi";

// export type Range = "monthly" | "quarterly" | "yearly";

// interface Props {
//   range: Range | undefined;              // "quarterly" | "yearly"
//   selectedQuarter: string;               // "Q1", "Q2", ...
//   selectedYear: string | number;
//   yearOptions: (string | number)[];
//   onRangeChange: (v: Range) => void;     // "quarterly" or "yearly"
//   onQuarterChange: (v: string) => void;  // "Q1" | "Q2" | ...
//   onYearChange: (v: string) => void;
// }

// type LatestPeriod = { month?: string; year?: string };

// const months = [
//   "january",
//   "february",
//   "march",
//   "april",
//   "may",
//   "june",
//   "july",
//   "august",
//   "september",
//   "october",
//   "november",
//   "december",
// ];

// const monthToQuarter = (m?: string) => {
//   if (!m) return "";
//   const idx = months.indexOf(m.toLowerCase());
//   if (idx === -1) return "";
//   return `Q${Math.floor(idx / 3) + 1}`;
// };

// const QuarterlyLast12Filters: React.FC<Props> = ({
//   range,
//   selectedQuarter,
//   selectedYear,
//   yearOptions,
//   onRangeChange,
//   onQuarterChange,
//   onYearChange,
// }) => {
//   const safeRange: Range =
//     range === "quarterly" || range === "yearly" ? range : "quarterly";

//   const getLatestPeriod = (): LatestPeriod | null => {
//     if (typeof window === "undefined") return null;
//     try {
//       const raw = localStorage.getItem("latestFetchedPeriod");
//       if (!raw) return null;
//       const parsed = JSON.parse(raw) as LatestPeriod;
//       if (!parsed.month || !parsed.year) return null;
//       return {
//         month: parsed.month.toLowerCase(),
//         year: String(parsed.year),
//       };
//     } catch {
//       return null;
//     }
//   };

//   const handleRangeChange = (nextRange: Range) => {
//     onRangeChange(nextRange);

//     const latest = getLatestPeriod();
//     if (!latest) return;

//     const { month, year } = latest;

//     if (year) {
//       onYearChange(String(year));
//     }

//     if (nextRange === "quarterly" && month) {
//       const q = monthToQuarter(month);
//       if (q) onQuarterChange(q);
//     }
//   };

//   // On first mount, seed from latestFetchedPeriod if parent hasn't
//   const initializedRef = React.useRef(false);

//   React.useEffect(() => {
//     if (initializedRef.current) return;
//     initializedRef.current = true;

//     const latest = getLatestPeriod();
//     if (!latest) return;

//     const { month, year } = latest;

//     // 1️⃣ Set YEAR from latestFetchedPeriod if parent doesn't have one yet
//     const hasYear = selectedYear !== "" && selectedYear !== undefined;
//     if (!hasYear && year) {
//       onYearChange(String(year));
//     }

//     // 2️⃣ Set QUARTER from latestFetchedPeriod if we are in quarterly mode
//     //    and parent doesn't already have a meaningful quarter
//     if (safeRange === "quarterly" && month) {
//       const q = monthToQuarter(month); // e.g. "Q1", "Q2"...
//       const hasQuarter =
//         !!selectedQuarter && selectedQuarter !== "" && selectedQuarter !== "Range";

//       if (q && !hasQuarter) {
//         onQuarterChange(q); // parent will get "Q1" etc and can strip the "Q"
//       }
//     }
//   }, [
//     safeRange,
//     selectedQuarter,
//     selectedYear,
//     onYearChange,
//     onQuarterChange,
//   ]);

//   // shared dropdown wrapper styles
//   const wrapCls =
//     "relative inline-flex items-center rounded-lg border border-gray-300 bg-white px-3 py-1.5 text-xs sm:text-sm shadow-sm";

//   const selectCls =
//     "appearance-none bg-transparent pr-5 text-xs sm:text-sm text-[#414042] focus:outline-none cursor-pointer leading-tight";

//   return (
//     <div className="flex items-center gap-2 sm:gap-3">
//       {/* 1️⃣ Primary filter: Quarterly / Last 12 Months */}
//       <div className={wrapCls}>
//         <select
//           value={safeRange}
//           onChange={(e) => handleRangeChange(e.target.value as Range)}
//           className={selectCls}
//         >
//           <option value="quarterly">Quarterly</option>
//           <option value="yearly">Last 12 Months</option>
//         </select>
//         <span className="pointer-events-none absolute inset-y-0 right-2 flex items-center text-[10px] text-[#414042]">
//           <FaAngleDown />
//         </span>
//       </div>

//       {/* 2️⃣ Select Range (only for Quarterly) */}
//       {safeRange === "quarterly" && (
//         <div className={wrapCls}>
//           <span className="mr-2 flex items-center text-base text-[#414042]/70">
//             <HiOutlineCalculator />
//           </span>
//           <select
//             value={selectedQuarter || ""}
//             onChange={(e) => onQuarterChange(e.target.value)}
//             className={selectCls}
//           >
//             <option value="">Select Range</option>
//             {["Q1", "Q2", "Q3", "Q4"].map((q) => (
//               <option key={q} value={q}>
//                 {q}
//               </option>
//             ))}
//           </select>
//           <span className="pointer-events-none absolute inset-y-0 right-2 flex items-center text-[10px] text-[#414042]">
//             <FaAngleDown />
//           </span>
//         </div>
//       )}

//       {/* 3️⃣ Year selector */}
//       <div className={wrapCls}>
//         <select
//           value={selectedYear ? String(selectedYear) : ""}
//           onChange={(e) => onYearChange(e.target.value)}
//           className={selectCls}
//         >
//           <option value="">Year</option>
//           {yearOptions.map((y) => (
//             <option key={y} value={y}>
//               {y}
//             </option>
//           ))}
//         </select>
//         <span className="pointer-events-none absolute inset-y-0 right-2 flex items-center text-[10px] text-[#414042]">
//           <FaAngleDown />
//         </span>
//       </div>
//     </div>
//   );
// };

// export default QuarterlyLast12Filters;










































"use client";

import React from "react";
import { FaAngleDown } from "react-icons/fa";
import { HiOutlineCalculator } from "react-icons/hi";

export type Range = "monthly" | "quarterly" | "yearly" | "lifetime";

interface Props {
  range: Range | undefined;              // "quarterly" | "yearly" | "lifetime"
  selectedQuarter: string;               // "Q1", "Q2", ...
  selectedYear: string | number;
  yearOptions: (string | number)[];      // unused now, but kept for prop compatibility
  onRangeChange: (v: Range) => void;
  onQuarterChange: (v: string) => void;  // "Q1" | "Q2" | ...
  onYearChange: (v: string) => void;     // still used internally when quarter changes
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
    // "yearly" and "lifetime" will just use the year; no special UI here for now
  };

  // 🔹 Seed from latestFetchedPeriod only once, if parent hasn't already
  const initializedRef = React.useRef(false);
  React.useEffect(() => {
    if (initializedRef.current) return;
    initializedRef.current = true;

    const latest = getLatestPeriod();
    if (!latest) return;

    const { month, year } = latest;

    const hasYear = selectedYear !== "" && selectedYear !== undefined;
    if (!hasYear && year) {
      onYearChange(String(year));
    }

    if (safeRange === "quarterly" && month) {
      const q = monthToQuarter(month);
      const hasQuarter =
        !!selectedQuarter && selectedQuarter !== "" && selectedQuarter !== "Range";
      if (q && !hasQuarter) {
        onQuarterChange(q);
      }
    }
  }, [
    safeRange,
    selectedQuarter,
    selectedYear,
    onYearChange,
    onQuarterChange,
  ]);

  // 🔹 Build "all 4 quarters for the current year"
  const lastFourQuarters = React.useMemo(() => {
    // Decide which year to use for the quarter list
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

    // Always Q1–Q4 of that year
    return [1, 2, 3, 4].map((q) => ({
      year: y,
      quarter: q,
      label: `Q${q} '${shortYear}`,
      value: `${y}-Q${q}`,
    }));
  }, [selectedYear]);


  // 🔹 map current selection to the value format "YYYY-Qx"
  const currentQuarterValue = React.useMemo(() => {
    const y = selectedYear ? String(selectedYear) : "";
    const q = selectedQuarter || "";
    return y && q ? `${y}-${q}` : "";
  }, [selectedYear, selectedQuarter]);

  const wrapCls =
    "relative inline-flex items-center rounded-lg border border-gray-300 bg-white px-3 py-1.5 text-xs sm:text-sm shadow-sm";
  const selectCls =
    "appearance-none bg-transparent pr-5 text-xs sm:text-sm text-[#414042] focus:outline-none cursor-pointer leading-tight";

  return (
    <div className="flex items-center gap-2 sm:gap-3">
      {/* 1️⃣ Primary filter: Quarterly / Last 12 Months / Lifetime */}
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
        <span className="pointer-events-none absolute inset-y-0 right-2 flex items-center text-[10px] text-[#414042]">
          <FaAngleDown />
        </span>
      </div>

      {/* 2️⃣ Select Range (last 4 quarters) – only when Quarterly is selected */}
      {safeRange === "quarterly" && (
        <div className={wrapCls}>
          <span className="mr-2 flex items-center text-base text-[#414042]/70">
            <HiOutlineCalculator />
          </span>

          <select
            value={currentQuarterValue}
            onChange={(e) => {
              const value = e.target.value; // "YYYY-Qx"
              const [yStr, qStr] = value.split("-");
              if (yStr) onYearChange(yStr);
              if (qStr) onQuarterChange(qStr); // e.g. "Q1"
            }}
            className={selectCls}
          >
            {lastFourQuarters.map((q) => (
              <option key={q.value} value={q.value}>
                {q.label}
              </option>
            ))}
          </select>

          <span className="pointer-events-none absolute inset-y-0 right-2 flex items-center text-[10px] text-[#414042]">
            <FaAngleDown />
          </span>
        </div>
      )}

      {/* 3️⃣ Year dropdown has been removed as requested */}
    </div>
  );
};

export default QuarterlyLast12Filters;
