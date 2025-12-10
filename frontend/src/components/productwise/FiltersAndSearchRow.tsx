// // // components/productwise/FiltersAndSearchRow.tsx
// // "use client";

// // import React from "react";
// // import PeriodFiltersTable from "@/components/filters/PeriodFiltersTable";
// // import ProductSearchDropdown from "@/components/products/ProductSearchDropdown";
// // import { Range } from "./productwiseHelpers";

// // interface FiltersAndSearchRowProps {
// //   range: Range | undefined;
// //   selectedMonth: string;
// //   selectedQuarter: string;
// //   selectedYear: number | "";
// //   years: number[];
// //   allowedRanges?: Range[];
// //   authToken: string | null;
// //   onRangeChange: (range: Range) => void;
// //   onMonthChange: (month: string) => void;
// //   onQuarterChange: (quarter: string) => void; // expects "Q1", "Q2", ...
// //   onYearChange: (year: string) => void; // raw string from select
// //   onProductSelect: (productName: string) => void;
// // }

// // const FiltersAndSearchRow: React.FC<FiltersAndSearchRowProps> = ({
// //   range,
// //   selectedMonth,
// //   selectedQuarter,
// //   selectedYear,
// //   years,
// //   allowedRanges = ["quarterly", "yearly"],
// //   authToken,
// //   onRangeChange,
// //   onMonthChange,
// //   onQuarterChange,
// //   onYearChange,
// //   onProductSelect,
// // }) => {
// //   return (
// //     <div className="mb-5 flex flex-col md:flex-row items-center justify-between gap-4">
// //       <PeriodFiltersTable
// //         range={range}
// //         selectedMonth={selectedMonth}
// //         selectedQuarter={`Q${selectedQuarter}`}
// //         selectedYear={selectedYear === "" ? "" : selectedYear}
// //         yearOptions={years}
// //         onRangeChange={onRangeChange}
// //         onMonthChange={onMonthChange}
// //         onQuarterChange={onQuarterChange}
// //         onYearChange={onYearChange}
// //         allowedRanges={allowedRanges}
// //       />

// //       <ProductSearchDropdown
// //         authToken={authToken}
// //         onProductSelect={onProductSelect}
// //       />
// //     </div>
// //   );
// // };

// // export default FiltersAndSearchRow;






// // components/productwise/FiltersAndSearchRow.tsx
// "use client";

// import React from "react";
// import PeriodFiltersTable from "@/components/filters/PeriodFiltersTable";
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
//   onQuarterChange: (quarter: string) => void; // expects "Q1", "Q2", ...
//   onYearChange: (year: string) => void; // raw string from select
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
//     <div className="mb-5 flex flex-col md:flex-row items-center justify-between gap-4">
//       {/* <PeriodFiltersTable
//         range={range}
//         selectedMonth={selectedMonth}
//         selectedQuarter={`Q${selectedQuarter}`}
//         selectedYear={selectedYear === "" ? "" : selectedYear}
//         yearOptions={years}
//         onRangeChange={onRangeChange}
//         onMonthChange={onMonthChange}
//         onQuarterChange={onQuarterChange}
//         onYearChange={onYearChange}
//         allowedRanges={allowedRanges}
//       /> */}

//       <QuarterlyLast12Filters
//         range={range}
//         selectedQuarter={`Q${selectedQuarter}`}
//         selectedYear={selectedYear === "" ? "" : selectedYear}
//         yearOptions={years}
//         onRangeChange={onRangeChange}
//         onQuarterChange={onQuarterChange}
//         onYearChange={onYearChange}
//       />


//     </div>
//   );
// };

// export default FiltersAndSearchRow;











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




