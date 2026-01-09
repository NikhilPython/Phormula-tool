// // components/productwise/CountryCard.tsx
// "use client";

// import React from "react";
// import {
//   formatCurrencyByCountry,
//   formatMonthYear,
//   getCountryColor,
// } from "./productwiseHelpers";

// interface CountryCardProps {
//   country: string;
//   stats: any;
//   selectedYear: number | "";
// }

// const CountryCard: React.FC<CountryCardProps> = ({
//   country,
//   stats,
//   selectedYear,
// }) => (
//   <div className="rounded-lg border border-charcoal-500 bg-white p-4 sm:p-5 shadow-sm transition-shadow hover:shadow-md">
//     <div className="mb-4 flex items-center justify-between gap-2">
//       <h4 className="m-0 font-extrabold text-green-500 text-[clamp(14px,1.2vw,20px)] flex items-center gap-2">
//         <span
//           className="inline-block h-3 w-3 sm:h-3.5 sm:w-3.5 rounded-full"
//           style={{ backgroundColor: getCountryColor(country) }}
//         />
//         <span className="text-charcoal-500 text-[clamp(14px,1.1vw,18px)]">
//           {country.toUpperCase()}
//         </span>
//       </h4>
//     </div>

//     <div className="flex flex-col gap-4">
//       {/* Stats grid */}
//       <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
//         <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//           <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-charcoal-500">
//             Net Sales
//           </p>
//           <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//             {formatCurrencyByCountry(country, stats.totalSales)}
//           </p>
//         </div>

//         <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//           <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//             Units
//           </p>
//           <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//             {stats.totalUnits.toLocaleString()}
//           </p>
//         </div>

//         <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//           <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//             CM1 Profit
//           </p>
//           <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//             {formatCurrencyByCountry(country, stats.totalProfit)}
//           </p>
//         </div>

//         <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//           <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//             Avg. Monthly Sales
//           </p>
//           <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//             {formatCurrencyByCountry(country, stats.avgSales)}
//           </p>
//         </div>

//         <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//           <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//             Avg. Selling Price
//           </p>
//           <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//             {formatCurrencyByCountry(country, stats.avgSellingPrice)}
//           </p>
//         </div>

//         <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//           <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//             CM1 Profit (%)
//           </p>
//           <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//             {stats.gross_margin_avg.toFixed(2)}%
//           </p>
//         </div>
//       </div>

//       <p className="m-0 text-[clamp(13px,1vw,16px)] font-bold">
//         Best Performance Month
//       </p>

//       <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
//         {/* Sales */}
//         <div
//           className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3"
//           style={{
//             borderTopWidth: 4,
//             borderTopColor: getCountryColor(country),
//           }}
//         >
//           <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//             Sales
//           </p>
//           <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//             {formatMonthYear(stats.maxSalesMonth.month, selectedYear || "")}
//           </p>
//           <p className="text-[clamp(12px,0.95vw,16px)]">
//             {formatCurrencyByCountry(country, stats.maxSalesMonth.net_sales)}
//           </p>
//         </div>

//         {/* Units */}
//         <div
//           className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3"
//           style={{
//             borderTopWidth: 4,
//             borderTopColor: getCountryColor(country),
//           }}
//         >
//           <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//             Units
//           </p>
//           <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//             {formatMonthYear(stats.maxUnitsMonth.month, selectedYear || "")}
//           </p>
//           <p className="text-[clamp(12px,0.95vw,16px)]">
//             {stats.maxUnitsMonth.quantity.toLocaleString()}
//           </p>
//         </div>

//         {/* CM1 Profit */}
//         <div
//           className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3"
//           style={{
//             borderTopWidth: 4,
//             borderTopColor: getCountryColor(country),
//           }}
//         >
//           <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//             CM1 Profit
//           </p>
//           <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//             {formatMonthYear(stats.maxSalesMonth.month, selectedYear || "")}
//           </p>
//           <p className="text-[clamp(12px,0.95vw,16px)]">
//             {formatCurrencyByCountry(country, stats.maxSalesMonth.profit)}
//           </p>
//         </div>
//       </div>
//     </div>
//   </div>
// );

// export default CountryCard;

































// // components/productwise/CountryCard.tsx
// "use client";

// import React from "react";
// import {
//   CountryKey,
//   formatCurrencyByCountry,
//   formatMonthYear,
//   getCountryColor,
// } from "./productwiseHelpers";

// interface CountryCardProps {
//   country: string;
//   stats: any;
//   selectedYear: number | "";
//   homeCurrency: "USD" | "GBP" | "INR" | "CAD";
// }

// const CountryCard: React.FC<CountryCardProps> = ({
//   country,
//   stats,
//   selectedYear,
//   homeCurrency, // kept for props compatibility, not used for conversion
// }) => {
//   const countryKey = country.toLowerCase() as CountryKey;

//   // Use the raw country key for formatting (this gives ₹ for global_inr, £ for uk, etc.)
//   const formatAmount = (value: number) => {
//     return formatCurrencyByCountry(countryKey, value);
//   };

//   // For color + label, normalize "global_*" variants to "GLOBAL"
//   const colorKey: CountryKey = countryKey.startsWith("global")
//     ? "global"
//     : countryKey;

//   const displayLabel = countryKey.startsWith("global")
//     ? "GLOBAL"
//     : country.toUpperCase();

//   return (
//     <div className="rounded-lg border border-charcoal-500 bg-white p-4 sm:p-5 shadow-sm transition-shadow hover:shadow-md">
//       <div className="mb-4 flex items-center justify-between gap-2">
//         <h4 className="m-0 font-extrabold text-green-500 text-[clamp(14px,1.2vw,20px)] flex items-center gap-2">
//           <span
//             className="inline-block h-3 w-3 sm:h-3.5 sm:w-3.5 rounded-full"
//             style={{ backgroundColor: getCountryColor(colorKey) }}
//           />
//           <span className="text-charcoal-500 text-[clamp(14px,1.1vw,18px)]">
//             {displayLabel}
//           </span>
//         </h4>
//       </div>

//       <div className="flex flex-col gap-4">
//         {/* Stats grid */}
//         <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
//           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-charcoal-500">
//               Net Sales
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatAmount(stats.totalSales)}
//             </p>
//           </div>

//           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               Units
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {stats.totalUnits.toLocaleString()}
//             </p>
//           </div>

//           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               CM1 Profit
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatAmount(stats.totalProfit)}
//             </p>
//           </div>

//           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               Avg. Monthly Sales
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatAmount(stats.avgSales)}
//             </p>
//           </div>

//           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               Avg. Selling Price
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatAmount(Number(stats.avgSellingPrice.toFixed(2)))}
//             </p>

//           </div>

//           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               CM1 Profit (%)
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {stats.gross_margin_avg.toFixed(2)}%
//             </p>
//           </div>
//         </div>

//         <p className="m-0 text-[clamp(13px,1vw,16px)] font-bold">
//           Best Performing Month
//         </p>

//         <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
//           {/* Sales */}
//           <div
//             className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3"
//             style={{
//               borderTopWidth: 4,
//               borderTopColor: getCountryColor(colorKey),
//             }}
//           >
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               Sales
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatMonthYear(stats.maxSalesMonth.month, selectedYear || "")}
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)]">
//               {formatAmount(stats.maxSalesMonth.net_sales)}
//             </p>
//           </div>

//           {/* Units */}
//           <div
//             className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3"
//             style={{
//               borderTopWidth: 4,
//               borderTopColor: getCountryColor(colorKey),
//             }}
//           >
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               Units
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatMonthYear(stats.maxUnitsMonth.month, selectedYear || "")}
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)]">
//               {stats.maxUnitsMonth.quantity.toLocaleString()}
//             </p>
//           </div>

//           {/* CM1 Profit */}
//           <div
//             className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3"
//             style={{
//               borderTopWidth: 4,
//               borderTopColor: getCountryColor(colorKey),
//             }}
//           >
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               CM1 Profit
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatMonthYear(stats.maxSalesMonth.month, selectedYear || "")}
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)]">
//               {formatAmount(stats.maxSalesMonth.profit)}
//             </p>
//           </div>
//         </div>
//       </div>
//     </div>
//   );
// };

// export default CountryCard;



























// "use client";

// import React from "react";
// import {
//   CountryKey,
//   formatMonthYear,
//   getCountryColor,
//   normalizeCountryKey
// } from "./productwiseHelpers";
// import type { HomeCurrency } from "@/components/dashboard/useFx";

// interface CountryCardProps {
//   country: string;
//   stats: any;
//   selectedYear: number | "";
//   homeCurrency: "USD" | "GBP" | "INR" | "CAD";
//   activeCountry: string;
// }

// // const inferCurrencyFromKey = (key: CountryKey): "USD" | "GBP" | "INR" | "CAD" => {
// //   switch (key) {
// //     case "uk":
// //     case "global_gbp":
// //       return "GBP";
// //     case "global_inr":
// //       return "INR";
// //     case "global_cad":
// //       return "CAD";
// //     case "global":
// //     case "us":
// //     default:
// //       return "USD";
// //   }
// // };

// const CountryCard: React.FC<CountryCardProps> = ({
//   country,
//   stats,
//   selectedYear,
//   homeCurrency,
//   activeCountry,
// }) => {

//   const formatAmount = (value: number) => {
//     if (value == null) return "-";

//     const currency = homeCurrency; // "USD" | "GBP" | "INR" | "CAD"

//     const locale =
//       currency === "GBP"
//         ? "en-GB"
//         : currency === "INR"
//           ? "en-IN"
//           : currency === "CAD"
//             ? "en-CA"
//             : "en-US";

//     return new Intl.NumberFormat(locale, {
//       style: "currency",
//       currency,
//       maximumFractionDigits: 0,
//     }).format(value);
//   };

//   const formatAmountWith2Decimals = (value: number) => {
//     if (value == null) return "-";

//     const currency = homeCurrency;

//     const locale =
//       currency === "GBP"
//         ? "en-GB"
//         : currency === "INR"
//           ? "en-IN"
//           : currency === "CAD"
//             ? "en-CA"
//             : "en-US";

//     return new Intl.NumberFormat(locale, {
//       style: "currency",
//       currency,
//       minimumFractionDigits: 2,
//       maximumFractionDigits: 2,
//     }).format(value);
//   };


//   // For color + label, normalize "global_*" variants to "GLOBAL"
//   // const colorKey: CountryKey = countryKey.startsWith("global")
//   //   ? "global"
//   //   : countryKey;

//   // const displayLabel = countryKey.startsWith("global")
//   //   ? "GLOBAL"
//   //   : country.toUpperCase();


//   const backendKey = country.toLowerCase();          // e.g. "uk_usd"
//   const normalized = normalizeCountryKey(backendKey); // -> "uk"

//   // Card title label (GLOBAL / UK / US / CA)
//   const displayLabel =
//     normalized === "global"
//       ? "GLOBAL"
//       : normalized.toUpperCase();

//   // Card circle color
//   const colorKey = normalized;

//   return (
//     <div className="rounded-lg border border-charcoal-500 bg-white p-4 sm:p-5 shadow-sm transition-shadow hover:shadow-md">
//       <div className="mb-4 flex items-center justify-between gap-2">
//         <h4 className="m-0 font-extrabold text-green-500 text-[clamp(14px,1.2vw,20px)] flex items-center gap-2">
//           <span
//             className="inline-block h-3 w-3 sm:h-3.5 sm:w-3.5 rounded-full"
//             style={{ backgroundColor: getCountryColor(colorKey) }}
//           />
//           <span className="text-charcoal-500 text-[clamp(14px,1.1vw,18px)]">
//             {displayLabel}
//           </span>
//         </h4>

//       </div>

//       <div className="flex flex-col gap-4">
//         {/* Stats grid */}
//         <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
//           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-charcoal-500">
//               Net Sales
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatAmount(stats.totalSales)}
//             </p>
//           </div>

//           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               Units
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {stats.totalUnits.toLocaleString()}
//             </p>
//           </div>

//           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               CM1 Profit
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatAmount(stats.totalProfit)}
//             </p>
//           </div>

//           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               Avg. Monthly Sales
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatAmount(stats.avgSales)}
//             </p>
//           </div>

//           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               Avg. Selling Price
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatAmountWith2Decimals(stats.avgSellingPrice)}
//             </p>

//           </div>

//           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               CM1 Profit (%)
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {stats.gross_margin_avg.toFixed(2)}%
//             </p>
//           </div>
//         </div>

//         <p className="m-0 text-[clamp(13px,1vw,16px)] font-bold">
//           Best Performing Month
//         </p>

//         <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
//           {/* Sales */}
//           <div
//             className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3"
//             style={{
//               borderTopWidth: 4,
//               borderTopColor: getCountryColor(colorKey),
//             }}
//           >
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               Sales
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatMonthYear(stats.maxSalesMonth.month, selectedYear || "")}
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatAmount(stats.maxSalesMonth.net_sales)}
//             </p>
//           </div>

//           {/* Units */}
//           <div
//             className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3"
//             style={{
//               borderTopWidth: 4,
//               borderTopColor: getCountryColor(colorKey),
//             }}
//           >
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               Units
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatMonthYear(stats.maxUnitsMonth.month, selectedYear || "")}
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {stats.maxUnitsMonth.quantity.toLocaleString()}
//             </p>
//           </div>

//           {/* CM1 Profit */}
//           <div
//             className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3"
//             style={{
//               borderTopWidth: 4,
//               borderTopColor: getCountryColor(colorKey),
//             }}
//           >
//             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
//               CM1 Profit
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatMonthYear(stats.maxSalesMonth.month, selectedYear || "")}
//             </p>
//             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
//               {formatAmount(stats.maxSalesMonth.profit)}
//             </p>
//           </div>
//         </div>
//       </div>
//     </div>
//   );
// };

// export default CountryCard;







"use client";

import React, { useMemo } from "react";
import {
  CountryKey,
  formatMonthYear,
  getCountryColor,
  normalizeCountryKey,
} from "./productwiseHelpers";

interface CountryCardProps {
  country: string;
  stats: any;
  selectedYear: number | "";
  homeCurrency: "USD" | "GBP" | "INR" | "CAD";
  activeCountry: string;
}

const CountryCard: React.FC<CountryCardProps> = ({
  country,
  stats,
  selectedYear,
  homeCurrency,
  activeCountry,
}) => {
  const formatAmount = (value: number) => {
    if (value == null) return "-";

    const currency = homeCurrency;
    const locale =
      currency === "GBP"
        ? "en-GB"
        : currency === "INR"
          ? "en-IN"
          : currency === "CAD"
            ? "en-CA"
            : "en-US";

    return new Intl.NumberFormat(locale, {
      style: "currency",
      currency,
      maximumFractionDigits: 0,
    }).format(value);
  };

  const formatAmountWith2Decimals = (value: number) => {
    if (value == null) return "-";

    const currency = homeCurrency;
    const locale =
      currency === "GBP"
        ? "en-GB"
        : currency === "INR"
          ? "en-IN"
          : currency === "CAD"
            ? "en-CA"
            : "en-US";

    return new Intl.NumberFormat(locale, {
      style: "currency",
      currency,
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
  };

  const toNum = (v: any) => {
    const n = typeof v === "number" ? v : Number(v);
    return Number.isFinite(n) ? n : 0;
  };

  const avgMonthlyUnitsValue = useMemo(() => {
    return toNum(
      stats?.avgMonthlyUnits ??
      stats?.avg_monthly_units ??
      stats?.avgMonthlyUnit ??
      stats?.avg_monthly_unit ??
      stats?.avgUnits ??
      stats?.avg_units
    );
  }, [stats]);


  const backendKey = country.toLowerCase();
  const normalized = normalizeCountryKey(backendKey); // "global" | "uk" | "us" | "ca"
  const displayLabel = normalized === "global" ? "GLOBAL" : normalized.toUpperCase();
  const colorKey = normalized;

  // ✅ Pastel tile colors (match your screenshot vibe)
  type TileKey = "netSales" | "units" | "cm1Profit" | "avgMonthlySales" | "avgMonthlyUnits" | "cm1ProfitPct";

  const getTileStyle = (k: TileKey) => {
    switch (k) {
      case "netSales":
        return "bg-[#75BBDA4D] border-[#75BBDA]"; 
      case "units":
        return "bg-[#FDD36F4D] border-[#FDD36F]"; 
      case "cm1Profit":
        return "bg-[#7B9A6D4D] border-[#7B9A6D]"; 
      case "avgMonthlySales":
        return "bg-[#ED9F504D] border-[#ED9F50]"; 
      case "avgMonthlyUnits":
        return "bg-[#C494664D] border-[#C49466]"; 
      case "cm1ProfitPct":
        return "bg-[#B8C78C4D] border-[#B8C78C]"; // light peach
      default:
        return "bg-gray-200/40 border-gray-300";
    }
  };

  const getBestPerfBg = () => {
    switch (colorKey) {
      case "global":
        return "bg-[#F5F4F4]"; // soft orange
      case "uk":
        return "bg-[#F5F4F4]"; // soft purple
      case "us":
        return "bg-[#F5F4F4]"; // soft green
      case "ca":
        return "bg-[#F5F4F4]"; // soft blue
      default:
        return "bg-[#F5F4F4]";
    }
  };

  const StatTile = ({
    label,
    value,
    tileKey,
  }: {
    label: string;
    value: React.ReactNode;
    tileKey: TileKey;
  }) => (
    <div className={`rounded-md border p-2 sm:p-3 ${getTileStyle(tileKey)}`}>
      <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-medium text-charcoal-500">
        {label}
      </p>
      <p className="text-[clamp(12px,0.95vw,16px)] font-semibold text-[#414042]">
        {value}
      </p>
    </div>
  );



  return (
    <div className=" rounded-xl border border-slate-200 bg-white p-4 sm:p-5 shadow-sm transition-shadow hover:shadow-md">
      <div className="mb-4 flex items-center justify-between gap-2">
        <h4 className="m-0 font-extrabold text-green-500 text-[clamp(14px,1.2vw,20px)] flex items-center gap-2">
          <span
            className="inline-block h-3 w-3 sm:h-3.5 sm:w-3.5 rounded-full"
            style={{ backgroundColor: getCountryColor(colorKey) }}
          />
          <span className="text-charcoal-500 text-[clamp(14px,1.1vw,18px)]">
            {displayLabel}
          </span>
        </h4>
      </div>

      <div className="flex flex-col gap-4">
        {/* ✅ Stats grid with pastel tiles */}
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
          <StatTile label="Net Sales" tileKey="netSales" value={formatAmount(stats.totalSales)} />
          <StatTile label="Units" tileKey="units" value={(stats.totalUnits ?? 0).toLocaleString()} />
          <StatTile label="CM1 Profit" tileKey="cm1Profit" value={formatAmount(stats.totalProfit)} />

          <StatTile
            label="Avg. Monthly Sales"
            tileKey="avgMonthlySales"
            value={formatAmount(stats.avgSales)}
          />
          <StatTile
  label="Avg Selling Price"
  tileKey="avgMonthlyUnits" // keep same styling key so nothing else changes
  value={formatAmountWith2Decimals(stats.avgSellingPrice ?? stats.avg_selling_price ?? stats.asp ?? stats.ASP ?? 0)}
/>

          <StatTile
            label="CM1 Profit %"
            tileKey="cm1ProfitPct"
            value={`${Number(stats.gross_margin_avg ?? 0).toFixed(2)}%`}
          />
        </div>

        <p className="m-0 text-[clamp(13px,1vw,16px)] font-bold">Best Performance</p>

        {/* Best Performance stays white with country-colored top border */}
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
          <div
            className="rounded-lg p-2 sm:p-3"
            style={{
              backgroundColor: "#F5F4F4",
              border: "1px solid #41404233",
              borderTopWidth: 4,
              borderTopColor: getCountryColor(colorKey),
            }}
          >

            <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
              Sales
            </p>
            <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
              {formatMonthYear(stats.maxSalesMonth.month, selectedYear || "")}
            </p>
            <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
              {formatAmount(stats.maxSalesMonth.net_sales)}
            </p>
          </div>

          <div
            className={`rounded-lg border border-gray-300 p-2 sm:p-3 ${getBestPerfBg()}`}
            style={{
              borderTopWidth: 4,
              borderTopColor: getCountryColor(colorKey),
            }}
          >

            <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
              Units
            </p>
            <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
              {formatMonthYear(stats.maxUnitsMonth.month, selectedYear || "")}
            </p>
            <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
              {stats.maxUnitsMonth.quantity.toLocaleString()}
            </p>
          </div>

          <div
            className={`rounded-lg border border-gray-300 p-2 sm:p-3 ${getBestPerfBg()}`}
            style={{
              borderTopWidth: 4,
              borderTopColor: getCountryColor(colorKey),
            }}
          >

            <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
              Profit
            </p>
            <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
              {formatMonthYear(stats.maxSalesMonth.month, selectedYear || "")}
            </p>
            <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
              {formatAmount(stats.maxSalesMonth.profit)}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default CountryCard;
