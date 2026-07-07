// // "use client";

// // import React, { useEffect, useMemo, useState } from "react";
// // import { useRouter } from "next/navigation";

// // // Replace Alertdashboard with IntegrationDashboard
// // import IntegrationDashboard from "@/features/integration/IntegrationDashboard";

// // // Adjust these import paths to where your components live
// // import SKUtable from "@/components/dashboard/SKUtable";
// // import GraphPage from "@/components/dashboard/GraphPage";
// // import Bargraph from "@/components/dashboard/Bargraph";
// // import CircleChart from "@/components/dashboard/CircleChart";
// // import CMchartofsku from "@/components/dashboard/CMchartofsku";

// // /** ---------- Types ---------- */
// // type Summary = {
// //   unit_sold: number;
// //   total_sales: number;
// //   total_expense: number;
// //   cm2_profit: number;
// // };

// // type UploadsData = {
// //   summary?: Summary;
// // };

// // type Props = {
// //   initialRanged: string;      // "MTD" | "QTD"
// //   initialCountryName: string; // "uk" | "us" | "global"
// //   initialMonth: string;       // "september" | "NA"
// //   initialYear: string;        // "2025" | "NA"
// // };

// // /** ---------- Helpers ---------- */
// // const zeroSummary: Summary = {
// //   unit_sold: 0,
// //   total_sales: 0,
// //   total_expense: 0,
// //   cm2_profit: 0,
// // };

// // const monthOptions = [
// //   "january","february","march","april","may","june",
// //   "july","august","september","october","november","december",
// // ];

// // const quartersMap: Record<string, string[]> = {
// //   Q1: ["january", "february", "march"],
// //   Q2: ["april", "may", "june"],
// //   Q3: ["july", "august", "september"],
// //   Q4: ["october", "november", "december"],
// // };

// // const getQuarterFromMonth = (m: string) => {
// //   const month = (m || "").toLowerCase();
// //   for (const q of Object.keys(quartersMap)) {
// //     if (quartersMap[q].includes(month)) return q;
// //   }
// //   return "";
// // };

// // const getCurrencySymbol = (country: string) => {
// //   switch ((country || "").toLowerCase()) {
// //     case "uk":
// //       return "£";
// //     case "india":
// //       return "₹";
// //     case "us":
// //       return "$";
// //     case "europe":
// //     case "eu":
// //       return "€";
// //     case "global":
// //       return "$";
// //     default:
// //       return "¤";
// //   }
// // };

// // /** ---------- Internal UI parts ---------- */
// // function SummaryStrip({
// //   summary,
// //   currencySymbol,
// // }: {
// //   summary: Summary;
// //   currencySymbol: string;
// // }) {
// //   const cm2Pct =
// //     summary.total_sales > 0
// //       ? `${((summary.cm2_profit / summary.total_sales) * 100).toFixed(2)}%`
// //       : "0%";

// //   const faded =
// //     summary.unit_sold === 0 &&
// //     summary.total_sales === 0 &&
// //     summary.total_expense === 0 &&
// //     summary.cm2_profit === 0;

// //   const cell =
// //     "px-4 py-2 text-center border border-neutral-700 text-sm md:text-base";
// //   const th =
// //     "px-4 py-2 text-center border border-neutral-700 font-semibold text-emerald-600";

// //   return (
// //     <div
// //       className={`rounded-lg overflow-hidden border border-neutral-700 ${
// //         faded ? "opacity-40" : ""
// //       }`}
// //     >
// //       <table className="w-full border-collapse">
// //         <thead>
// //           <tr className="bg-white">
// //             <th className={th}>Units</th>
// //             <th className={th}>Sales</th>
// //             <th className={th}>Expense</th>
// //             <th className={th}>CM2 Profit</th>
// //             <th className={th}>CM2 Profit (%)</th>
// //           </tr>
// //         </thead>
// //         <tbody>
// //           <tr className="bg-white">
// //             <td className={cell}>{summary.unit_sold}</td>
// //             <td className={cell}>
// //               {currencySymbol}{" "}
// //               {summary.total_sales.toLocaleString(undefined, {
// //                 minimumFractionDigits: 2,
// //                 maximumFractionDigits: 2,
// //               })}
// //             </td>
// //             <td className={cell}>
// //               {currencySymbol}{" "}
// //               {summary.total_expense.toLocaleString(undefined, {
// //                 minimumFractionDigits: 2,
// //                 maximumFractionDigits: 2,
// //               })}
// //             </td>
// //             <td className={cell}>
// //               {currencySymbol}{" "}
// //               {summary.cm2_profit.toLocaleString(undefined, {
// //                 minimumFractionDigits: 2,
// //                 maximumFractionDigits: 2,
// //               })}
// //             </td>
// //             <td className={cell}>{cm2Pct}</td>
// //           </tr>
// //         </tbody>
// //       </table>
// //     </div>
// //   );
// // }

// // function PeriodSelect({
// //   range,
// //   setRange,
// //   selectedMonth,
// //   setSelectedMonth,
// //   selectedQuarter,
// //   setSelectedQuarter,
// //   selectedYear,
// //   setSelectedYear,
// //   yearOptions,
// // }: {
// //   range: string;
// //   setRange: (v: string) => void;
// //   selectedMonth: string;
// //   setSelectedMonth: (v: string) => void;
// //   selectedQuarter: string;
// //   setSelectedQuarter: (v: string) => void;
// //   selectedYear: string;
// //   setSelectedYear: (v: string) => void;
// //   yearOptions: string[];
// // }) {
// //   const cell = "px-3 py-2 text-center border border-neutral-700";
// //   const th =
// //     "px-3 py-2 text-center border border-neutral-700 text-emerald-600 font-semibold";
// //   const select = "bg-white text-sm md:text-base focus:outline-none";

// //   return (
// //     <div className="rounded-lg overflow-hidden border border-neutral-700">
// //       <table className="w-full border-collapse min-w-[260px]">
// //         <thead className="bg-white">
// //           <tr>
// //             <th className={th}>Period</th>
// //             {(range === "monthly" || range === "quarterly") && (
// //               <th className={th}>Range</th>
// //             )}
// //             <th className={th}>Year</th>
// //           </tr>
// //         </thead>
// //         <tbody>
// //           <tr className="bg-white">
// //             <td className={cell}>
// //               <select
// //                 className={select}
// //                 value={range}
// //                 onChange={(e) => {
// //                   setRange(e.target.value);
// //                   setSelectedMonth("");
// //                   setSelectedQuarter("");
// //                   setSelectedYear("");
// //                 }}
// //               >
// //                 <option value="monthly">Monthly</option>
// //                 <option value="quarterly">Quarterly</option>
// //                 <option value="yearly">Yearly</option>
// //               </select>
// //             </td>

// //             {range === "monthly" && (
// //               <>
// //                 <td className={cell}>
// //                   <select
// //                     className={select}
// //                     value={selectedMonth}
// //                     onChange={(e) => setSelectedMonth(e.target.value)}
// //                   >
// //                     <option value="">Select</option>
// //                     {monthOptions.map((m) => (
// //                       <option key={m} value={m}>
// //                         {m[0].toUpperCase() + m.slice(1)}
// //                       </option>
// //                     ))}
// //                   </select>
// //                 </td>
// //                 <td className={cell}>
// //                   <select
// //                     className={select}
// //                     value={selectedYear}
// //                     onChange={(e) => setSelectedYear(e.target.value)}
// //                   >
// //                     <option value="">Select</option>
// //                     {yearOptions.map((y) => (
// //                       <option key={y} value={y}>
// //                         {y}
// //                       </option>
// //                     ))}
// //                   </select>
// //                 </td>
// //               </>
// //             )}

// //             {range === "quarterly" && (
// //               <>
// //                 <td className={cell}>
// //                   <select
// //                     className={select}
// //                     value={selectedQuarter}
// //                     onChange={(e) => setSelectedQuarter(e.target.value)}
// //                   >
// //                     <option value="">Select</option>
// //                     <option value="Q1">Q1</option>
// //                     <option value="Q2">Q2</option>
// //                     <option value="Q3">Q3</option>
// //                     <option value="Q4">Q4</option>
// //                   </select>
// //                 </td>
// //                 <td className={cell}>
// //                   <select
// //                     className={select}
// //                     value={selectedYear}
// //                     onChange={(e) => setSelectedYear(e.target.value)}
// //                   >
// //                     <option value="">Select</option>
// //                     {yearOptions.map((y) => (
// //                       <option key={y} value={y}>
// //                         {y}
// //                       </option>
// //                     ))}
// //                   </select>
// //                 </td>
// //               </>
// //             )}

// //             {range === "yearly" && (
// //               <td className={cell} colSpan={2}>
// //                 <select
// //                   className={select}
// //                   value={selectedYear}
// //                   onChange={(e) => setSelectedYear(e.target.value)}
// //                 >
// //                   <option value="">Select</option>
// //                   {yearOptions.map((y) => (
// //                     <option key={y} value={y}>
// //                       {y}
// //                     </option>
// //                   ))}
// //                 </select>
// //               </td>
// //             )}
// //           </tr>
// //         </tbody>
// //       </table>
// //     </div>
// //   );
// // }

// // /** ---------- Main Dashboard ---------- */
// // export default function CountryDashboard({
// //   initialRanged,
// //   initialCountryName,
// //   initialMonth,
// //   initialYear,
// // }: Props) {
// //   const router = useRouter();

// //   const currencySymbol = useMemo(
// //     () => getCurrencySymbol(initialCountryName),
// //     [initialCountryName]
// //   );

// //   // UI state (selectors)
// //   const [range, setRange] = useState<string>("monthly");
// //   const [selectedMonth, setSelectedMonth] = useState<string>("");
// //   const [selectedQuarter, setSelectedQuarter] = useState<string>("");
// //   const [selectedYear, setSelectedYear] = useState<string>("");

// //   // Data state
// //   const [uploadsData, setUploadsData] = useState<UploadsData | null>(null);

// //   const yearOptions = useMemo(
// //     () =>
// //       Array.from({ length: 2 }, (_, i) =>
// //         String(new Date().getFullYear() - i)
// //       ),
// //     []
// //   );

// //   /** Sync local selectors whenever URL params change */
// //   useEffect(() => {
// //     const rangedUpper = (initialRanged || "").toUpperCase();
// //     if (rangedUpper === "QTD") {
// //       setRange("quarterly");
// //       setSelectedQuarter(getQuarterFromMonth(initialMonth));
// //       setSelectedYear(initialYear);
// //       setSelectedMonth("");
// //     } else {
// //       // default (& MTD)
// //       setRange("monthly");
// //       setSelectedMonth(initialMonth);
// //       setSelectedYear(initialYear);
// //       setSelectedQuarter("");
// //     }
// //   }, [initialRanged, initialMonth, initialYear]);

// //   /** Fetch upload history based on current selectors */
// //   const fetchUploadHistory = async (
// //     rangeType: string,
// //     m: string,
// //     q: string,
// //     y: string,
// //     ctry: string
// //   ) => {
// //     if (!rangeType) return;
// //     try {
// //       const token =
// //         typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

// //       const url = new URL("http://127.0.0.1:5000/upload_history2");
// //       url.searchParams.set("range", rangeType);
// //       url.searchParams.set("month", m || "");
// //       url.searchParams.set("quarter", q || "");
// //       url.searchParams.set("year", y || "");
// //       url.searchParams.set("country", ctry || "");

// //       const resp = await fetch(url.toString(), {
// //         method: "GET",
// //         headers: token ? { Authorization: `Bearer ${token}` } : {},
// //       });

// //       if (!resp.ok) {
// //         setUploadsData(null);
// //         return;
// //       }

// //       const data = (await resp.json()) as UploadsData;
// //       setUploadsData(data);
// //     } catch (e) {
// //       console.error("Error fetching upload history:", e);
// //       setUploadsData(null);
// //     }
// //   };

// //   /** Refetch whenever selectors (or country) change */
// //   useEffect(() => {
// //     fetchUploadHistory(
// //       range,
// //       selectedMonth,
// //       selectedQuarter,
// //       selectedYear,
// //       initialCountryName
// //     );
// //     // eslint-disable-next-line react-hooks/exhaustive-deps
// //   }, [range, selectedMonth, selectedQuarter, selectedYear, initialCountryName]);

// //   /** If month/year are NA, show IntegrationDashboard (instead of Alertdashboard) */
// //   if (initialMonth === "NA" || initialYear === "NA") {
// //     return <IntegrationDashboard />;
// //   }

// //   /** Nav helpers */
// //   const goBack = () => router.push("/country/QTD/global/NA/NA");
// //   const goUpload = () =>
// //     initialCountryName !== "global" && router.push(`/Upload/${initialCountryName}`);

// //   /** Visibility flags */
// //   const showMonthly = range === "monthly" && !!selectedMonth && !!selectedYear;
// //   const showQuarterly = range === "quarterly" && !!selectedQuarter && !!selectedYear;
// //   const showYearly = range === "yearly" && !!selectedYear;

// //   return (
// //     <div className="px-3 md:px-5 lg:px-8 py-4">
// //       {/* Back */}
// //       <div className="mb-3">
// //         <button
// //           onClick={goBack}
// //           className="inline-flex items-center gap-2 bg-slate-800 text-[#f8edcf] font-semibold rounded-md px-4 py-2 hover:bg-slate-700"
// //         >
// //           <i className="fa-solid fa-arrow-left" />
// //           Back
// //         </button>
// //       </div>

// //       {/* Title */}
// //       <h2 className="text-xl md:text-2xl font-semibold text-slate-800">
// //         Financial Metrics –{" "}
// //         <span className="text-emerald-600">
// //           {initialCountryName?.toLowerCase() === "global"
// //             ? "GLOBAL"
// //             : initialCountryName?.toUpperCase()}
// //         </span>
// //       </h2>

// //       {/* Top controls + summary + upload */}
// //       <div className="mt-4 flex flex-col lg:flex-row gap-4">
// //         <div className="w-full lg:max-w-md">
// //           <PeriodSelect
// //             range={range}
// //             setRange={setRange}
// //             selectedMonth={selectedMonth}
// //             setSelectedMonth={setSelectedMonth}
// //             selectedQuarter={selectedQuarter}
// //             setSelectedQuarter={setSelectedQuarter}
// //             selectedYear={selectedYear}
// //             setSelectedYear={setSelectedYear}
// //             yearOptions={yearOptions}
// //           />
// //         </div>

// //         <div className="flex-1">
// //           <SummaryStrip
// //             summary={uploadsData?.summary ?? zeroSummary}
// //             currencySymbol={currencySymbol}
// //           />
// //         </div>

// //         {initialCountryName !== "global" && (
// //           <div className="flex items-start">
// //             <button
// //               onClick={goUpload}
// //               className="inline-flex items-center gap-2 bg-slate-800 text-[#f8edcf] font-semibold rounded-md px-4 py-2 hover:bg-slate-700 whitespace-nowrap"
// //             >
// //               Upload MTD <i className="fa-solid fa-plus" />
// //             </button>
// //           </div>
// //         )}
// //       </div>

// //       {/* Visualizations */}
// //       <div className="mt-6 space-y-6">
// //         {/* Monthly */}
// //         {showMonthly && (
// //           <>
// //             <Bargraph
// //               range={range}
// //               selectedMonth={selectedMonth}
// //               selectedYear={selectedYear}
// //             />

// //             <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
// //               <div className="w-full">
// //                 <CircleChart
// //                   range={range}
// //                   month={selectedMonth}
// //                   year={selectedYear}
// //                 />
// //               </div>
// //               <div className="w-full">
// //                 <CMchartofsku
// //                   range={range}
// //                   month={selectedMonth}
// //                   year={selectedYear}
// //                 />
// //               </div>
// //             </div>

// //             <SKUtable range={range} month={selectedMonth} year={selectedYear} />
// //           </>
// //         )}

// //         {/* Quarterly */}
// //         {showQuarterly && (
// //           <>
// //             <GraphPage
// //               range={range}
// //               selectedQuarter={selectedQuarter}
// //               selectedYear={selectedYear}
// //             />

// //             <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
// //               <div className="w-full">
// //                 <CircleChart
// //                   range={range}
// //                   selectedQuarter={selectedQuarter}
// //                   year={selectedYear}
// //                 />
// //               </div>
// //               <div className="w-full">
// //                 <CMchartofsku
// //                   range={range}
// //                   selectedQuarter={selectedQuarter}
// //                   year={selectedYear}
// //                 />
// //               </div>
// //             </div>

// //             <SKUtable range={range} quarter={selectedQuarter} year={selectedYear} />
// //           </>
// //         )}

// //         {/* Yearly */}
// //         {showYearly && (
// //           <>
// //             <GraphPage range={range} selectedYear={selectedYear} />

// //             <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
// //               <div className="w-full">
// //                 <CircleChart range={range} year={selectedYear} />
// //               </div>
// //               <div className="w-full">
// //                 <CMchartofsku range={range} year={selectedYear} />
// //               </div>
// //             </div>

// //             <SKUtable range={range} year={selectedYear} />
// //           </>
// //         )}
// //       </div>
// //     </div>
// //   );
// // }















// "use client";

// import React, { useEffect, useMemo, useState } from "react";
// import { useRouter } from "next/navigation";
// import dynamic from "next/dynamic";
// import IntegrationDashboard from "@/features/integration/IntegrationDashboard";
// import DataTable, { DataRow } from "@/components/dashboard/DataTable";

// // ApexCharts (client-only)
// const ReactApexChart = dynamic(() => import("react-apexcharts"), { ssr: false });

// /** ---------- API endpoints (adjust as needed) ---------- */
// const HISTORY_ENDPOINT = "http://127.0.0.1:5000/upload_history2";
// /** 👉 This should return an array of row objects OR { rows: [...] } */
// const TABLE_ENDPOINT = "http://127.0.0.1:5000/upload_table";

// /** ---------- Types ---------- */
// type Summary = {
//   unit_sold: number;
//   total_sales: number;
//   total_expense: number;
//   cm2_profit: number;
// };

// type UploadsData = {
//   summary?: Summary;
// };

// type Props = {
//   initialRanged: string;      // "MTD" | "QTD"
//   initialCountryName: string; // "uk" | "us" | "global"
//   initialMonth: string;       // "september" | "NA"
//   initialYear: string;        // "2025" | "NA"
// };

// /** ---------- Helpers ---------- */
// const zeroSummary: Summary = {
//   unit_sold: 0,
//   total_sales: 0,
//   total_expense: 0,
//   cm2_profit: 0,
// };

// const monthOptions = [
//   "january","february","march","april","may","june",
//   "july","august","september","october","november","december",
// ];

// const quartersMap: Record<string, string[]> = {
//   Q1: ["january", "february", "march"],
//   Q2: ["april", "may", "june"],
//   Q3: ["july", "august", "september"],
//   Q4: ["october", "november", "december"],
// };

// const getQuarterFromMonth = (m: string) => {
//   const month = (m || "").toLowerCase();
//   for (const q of Object.keys(quartersMap)) {
//     if (quartersMap[q].includes(month)) return q;
//   }
//   return "";
// };

// const getCurrencySymbol = (country: string) => {
//   switch ((country || "").toLowerCase()) {
//     case "uk":
//       return "£";
//     case "india":
//       return "₹";
//     case "us":
//       return "$";
//     case "europe":
//     case "eu":
//       return "€";
//     case "global":
//       return "$";
//     default:
//       return "¤";
//   }
// };

// /** ---------- Internal UI parts ---------- */
// function SummaryStrip({
//   summary,
//   currencySymbol,
// }: {
//   summary: Summary;
//   currencySymbol: string;
// }) {
//   const cm2Pct =
//     summary.total_sales > 0
//       ? `${((summary.cm2_profit / summary.total_sales) * 100).toFixed(2)}%`
//       : "0%";

//   const faded =
//     summary.unit_sold === 0 &&
//     summary.total_sales === 0 &&
//     summary.total_expense === 0 &&
//     summary.cm2_profit === 0;

//   const cell =
//     "px-4 py-2 text-center border border-neutral-700 text-sm md:text-base";
//   const th =
//     "px-4 py-2 text-center border border-neutral-700 font-semibold text-emerald-600";

//   return (
//     <div
//       className={`rounded-lg overflow-hidden border border-neutral-700 ${
//         faded ? "opacity-40" : ""
//       }`}
//     >
//       <table className="w-full border-collapse">
//         <thead>
//           <tr className="bg-white">
//             <th className={th}>Units</th>
//             <th className={th}>Sales</th>
//             <th className={th}>Expense</th>
//             <th className={th}>CM2 Profit</th>
//             <th className={th}>CM2 Profit (%)</th>
//           </tr>
//         </thead>
//         <tbody>
//           <tr className="bg-white">
//             <td className={cell}>{summary.unit_sold}</td>
//             <td className={cell}>
//               {currencySymbol}{" "}
//               {Number(summary.total_sales).toLocaleString(undefined, {
//                 minimumFractionDigits: 2,
//                 maximumFractionDigits: 2,
//               })}
//             </td>
//             <td className={cell}>
//               {currencySymbol}{" "}
//               {Number(summary.total_expense).toLocaleString(undefined, {
//                 minimumFractionDigits: 2,
//                 maximumFractionDigits: 2,
//               })}
//             </td>
//             <td className={cell}>
//               {currencySymbol}{" "}
//               {Number(summary.cm2_profit).toLocaleString(undefined, {
//                 minimumFractionDigits: 2,
//                 maximumFractionDigits: 2,
//               })}
//             </td>
//             <td className={cell}>{cm2Pct}</td>
//           </tr>
//         </tbody>
//       </table>
//     </div>
//   );
// }

// function PeriodSelect({
//   range,
//   setRange,
//   selectedMonth,
//   setSelectedMonth,
//   selectedQuarter,
//   setSelectedQuarter,
//   selectedYear,
//   setSelectedYear,
//   yearOptions,
// }: {
//   range: string;
//   setRange: (v: string) => void;
//   selectedMonth: string;
//   setSelectedMonth: (v: string) => void;
//   selectedQuarter: string;
//   setSelectedQuarter: (v: string) => void;
//   selectedYear: string;
//   setSelectedYear: (v: string) => void;
//   yearOptions: string[];
// }) {
//   const cell = "px-3 py-2 text-center border border-neutral-700";
//   const th =
//     "px-3 py-2 text-center border border-neutral-700 text-emerald-600 font-semibold";
//   const select = "bg-white text-sm md:text-base focus:outline-none";

//   return (
//     <div className="rounded-lg overflow-hidden border border-neutral-700">
//       <table className="w-full border-collapse min-w-[260px]">
//         <thead className="bg-white">
//           <tr>
//             <th className={th}>Period</th>
//             {(range === "monthly" || range === "quarterly") && (
//               <th className={th}>Range</th>
//             )}
//             <th className={th}>Year</th>
//           </tr>
//         </thead>
//         <tbody>
//           <tr className="bg-white">
//             <td className={cell}>
//               <select
//                 className={select}
//                 value={range}
//                 onChange={(e) => {
//                   setRange(e.target.value);
//                   setSelectedMonth("");
//                   setSelectedQuarter("");
//                   setSelectedYear("");
//                 }}
//               >
//                 <option value="monthly">Monthly</option>
//                 <option value="quarterly">Quarterly</option>
//                 <option value="yearly">Yearly</option>
//               </select>
//             </td>

//             {range === "monthly" && (
//               <>
//                 <td className={cell}>
//                   <select
//                     className={select}
//                     value={selectedMonth}
//                     onChange={(e) => setSelectedMonth(e.target.value)}
//                   >
//                     <option value="">Select</option>
//                     {monthOptions.map((m) => (
//                       <option key={m} value={m}>
//                         {m[0].toUpperCase() + m.slice(1)}
//                       </option>
//                     ))}
//                   </select>
//                 </td>
//                 <td className={cell}>
//                   <select
//                     className={select}
//                     value={selectedYear}
//                     onChange={(e) => setSelectedYear(e.target.value)}
//                   >
//                     <option value="">Select</option>
//                     {yearOptions.map((y) => (
//                       <option key={y} value={y}>
//                         {y}
//                       </option>
//                     ))}
//                   </select>
//                 </td>
//               </>
//             )}

//             {range === "quarterly" && (
//               <>
//                 <td className={cell}>
//                   <select
//                     className={select}
//                     value={selectedQuarter}
//                     onChange={(e) => setSelectedQuarter(e.target.value)}
//                   >
//                     <option value="">Select</option>
//                     <option value="Q1">Q1</option>
//                     <option value="Q2">Q2</option>
//                     <option value="Q3">Q3</option>
//                     <option value="Q4">Q4</option>
//                   </select>
//                 </td>
//                 <td className={cell}>
//                   <select
//                     className={select}
//                     value={selectedYear}
//                     onChange={(e) => setSelectedYear(e.target.value)}
//                   >
//                     <option value="">Select</option>
//                     {yearOptions.map((y) => (
//                       <option key={y} value={y}>
//                         {y}
//                       </option>
//                     ))}
//                   </select>
//                 </td>
//               </>
//             )}

//             {range === "yearly" && (
//               <td className={cell} colSpan={2}>
//                 <select
//                   className={select}
//                   value={selectedYear}
//                   onChange={(e) => setSelectedYear(e.target.value)}
//                 >
//                   <option value="">Select</option>
//                   {yearOptions.map((y) => (
//                     <option key={y} value={y}>
//                       {y}
//                     </option>
//                   ))}
//                 </select>
//               </td>
//             )}
//           </tr>
//         </tbody>
//       </table>
//     </div>
//   );
// }

// /** ---------- Main Dashboard (ApexCharts + DataTable) ---------- */
// export default function CountryDashboard({
//   initialRanged,
//   initialCountryName,
//   initialMonth,
//   initialYear,
// }: Props) {
//   const router = useRouter();

//   const currencySymbol = useMemo(
//     () => getCurrencySymbol(initialCountryName),
//     [initialCountryName]
//   );

//   // UI state (selectors)
//   const [range, setRange] = useState<string>("monthly");
//   const [selectedMonth, setSelectedMonth] = useState<string>("");
//   const [selectedQuarter, setSelectedQuarter] = useState<string>("");
//   const [selectedYear, setSelectedYear] = useState<string>("");

//   // Data state
//   const [summaryData, setSummaryData] = useState<UploadsData | null>(null);
//   const [tableRows, setTableRows] = useState<DataRow[]>([]);
//   const [tableLoading, setTableLoading] = useState<boolean>(false);

//   const yearOptions = useMemo(
//     () =>
//       Array.from({ length: 2 }, (_, i) =>
//         String(new Date().getFullYear() - i)
//       ),
//     []
//   );

//   /** Sync local selectors whenever URL params change */
//   useEffect(() => {
//     const rangedUpper = (initialRanged || "").toUpperCase();
//     if (rangedUpper === "QTD") {
//       setRange("quarterly");
//       setSelectedQuarter(getQuarterFromMonth(initialMonth));
//       setSelectedYear(initialYear);
//       setSelectedMonth("");
//     } else {
//       // default (& MTD)
//       setRange("monthly");
//       setSelectedMonth(initialMonth);
//       setSelectedYear(initialYear);
//       setSelectedQuarter("");
//     }
//   }, [initialRanged, initialMonth, initialYear]);

//   /** Fetch summary */
//   const fetchSummary = async (
//     rangeType: string,
//     m: string,
//     q: string,
//     y: string,
//     ctry: string
//   ) => {
//     if (!rangeType) return;
//     try {
//       const token =
//         typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

//       const url = new URL(HISTORY_ENDPOINT);
//       url.searchParams.set("range", rangeType);
//       url.searchParams.set("month", m || "");
//       url.searchParams.set("quarter", q || "");
//       url.searchParams.set("year", y || "");
//       url.searchParams.set("country", ctry || "");

//       const resp = await fetch(url.toString(), {
//         method: "GET",
//         headers: token ? { Authorization: `Bearer ${token}` } : {},
//       });

//       if (!resp.ok) {
//         setSummaryData(null);
//         return;
//       }

//       const data = (await resp.json()) as UploadsData;
//       setSummaryData(data);
//     } catch (e) {
//       console.error("Error fetching summary:", e);
//       setSummaryData(null);
//     }
//   };

//   /** Fetch table */
//   const fetchTable = async (
//     rangeType: string,
//     m: string,
//     q: string,
//     y: string,
//     ctry: string
//   ) => {
//     if (!rangeType) return;
//     setTableLoading(true);
//     try {
//       const token =
//         typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

//       const url = new URL(TABLE_ENDPOINT);
//       url.searchParams.set("range", rangeType);
//       url.searchParams.set("month", m || "");
//       url.searchParams.set("quarter", q || "");
//       url.searchParams.set("year", y || "");
//       url.searchParams.set("country", ctry || "");

//       const resp = await fetch(url.toString(), {
//         method: "GET",
//         headers: token ? { Authorization: `Bearer ${token}` } : {},
//       });

//       if (!resp.ok) {
//         setTableRows([]);
//         setTableLoading(false);
//         return;
//       }

//       // Accept either an array or an object wrapper
//       const data = await resp.json();
//       const rows: DataRow[] = Array.isArray(data)
//         ? data
//         : data?.rows || data?.data || [];
//       setTableRows(rows);
//     } catch (e) {
//       console.error("Error fetching table:", e);
//       setTableRows([]);
//     } finally {
//       setTableLoading(false);
//     }
//   };

//   /** Refetch both whenever selectors (or country) change */
//   useEffect(() => {
//     fetchSummary(
//       range,
//       selectedMonth,
//       selectedQuarter,
//       selectedYear,
//       initialCountryName
//     );
//     fetchTable(
//       range,
//       selectedMonth,
//       selectedQuarter,
//       selectedYear,
//       initialCountryName
//     );
//     // eslint-disable-next-line react-hooks/exhaustive-deps
//   }, [range, selectedMonth, selectedQuarter, selectedYear, initialCountryName]);

//   /** If month/year are NA, show IntegrationDashboard */
//   if (initialMonth === "NA" || initialYear === "NA") {
//     return <IntegrationDashboard />;
//   }

//   /** Nav helpers */
//   const goBack = () => router.push("/country/QTD/global/NA/NA");
//   const goUpload =
//     initialCountryName !== "global"
//       ? () => router.push(`/Upload/${initialCountryName}`)
//       : undefined;

//   /** Visibility flags */
//   const showMonthly = range === "monthly" && !!selectedMonth && !!selectedYear;
//   const showQuarterly =
//     range === "quarterly" && !!selectedQuarter && !!selectedYear;
//   const showYearly = range === "yearly" && !!selectedYear;

//   /** ---------- Chart data from summary ---------- */
//   const summary = summaryData?.summary ?? zeroSummary;

//   const barSeries = useMemo(
//     () => [
//       { name: "Sales", data: [Number(summary.total_sales || 0)] },
//       { name: "Expense", data: [Number(summary.total_expense || 0)] },
//       { name: "Profit", data: [Number(summary.cm2_profit || 0)] },
//     ],
//     [summary]
//   );

//   const barOptions: ApexCharts.ApexOptions = useMemo(
//     () => ({
//       chart: { type: "bar", toolbar: { show: false } },
//       plotOptions: { bar: { columnWidth: "40%", borderRadius: 6 } },
//       dataLabels: { enabled: false },
//       xaxis: {
//         categories: ["Totals"],
//         axisBorder: { show: false },
//         axisTicks: { show: false },
//       },
//       yaxis: {
//         labels: {
//           formatter: (val) =>
//             `${currencySymbol} ${Number(val).toLocaleString(undefined, {
//               maximumFractionDigits: 0,
//             })}`,
//         },
//       },
//       tooltip: {
//         y: {
//           formatter: (val: number) =>
//             `${currencySymbol} ${Number(val).toLocaleString(undefined, {
//               minimumFractionDigits: 2,
//               maximumFractionDigits: 2,
//             })}`,
//         },
//       },
//       legend: { position: "top" },
//       colors: ["#60a5fa", "#f97316", "#10b981"], // blue / orange / emerald
//       grid: { strokeDashArray: 4 },
//     }),
//     [currencySymbol]
//   );

//   const donutSeries = useMemo(
//     () => [
//       Number(summary.total_sales || 0),
//       Number(summary.total_expense || 0),
//       Number(summary.cm2_profit || 0),
//     ],
//     [summary]
//   );

//   const donutOptions: ApexCharts.ApexOptions = useMemo(
//     () => ({
//       chart: { type: "donut" },
//       labels: ["Sales", "Expense", "Profit"],
//       legend: { position: "bottom" },
//       colors: ["#60a5fa", "#f97316", "#10b981"],
//       tooltip: {
//         y: {
//           formatter: (val: number) =>
//             `${currencySymbol} ${Number(val).toLocaleString()}`,
//         },
//       },
//       dataLabels: {
//         formatter: (val: number, opts: any) => {
//           const raw = opts.w.config.series[opts.seriesIndex] || 0;
//           return `${currencySymbol} ${Number(raw).toLocaleString()}`;
//         },
//       },
//       plotOptions: {
//         pie: {
//           donut: {
//             size: "60%",
//             labels: {
//               show: true,
//               total: {
//                 show: true,
//                 label: "Total",
//                 formatter: (w: any) => {
//                   const s = w?.globals?.seriesTotals || [0, 0, 0];
//                   const total = s.reduce((a: number, b: number) => a + b, 0);
//                   return `${currencySymbol} ${Number(total).toLocaleString()}`;
//                 },
//               },
//             },
//           },
//         },
//       },
//     }),
//     [currencySymbol]
//   );

//   return (
//     <div className="px-3 md:px-5 lg:px-8 py-4">
//       {/* Back */}
//       <div className="mb-3">
//         <button
//           onClick={goBack}
//           className="inline-flex items-center gap-2 bg-slate-800 text-[#f8edcf] font-semibold rounded-md px-4 py-2 hover:bg-slate-700"
//         >
//           <i className="fa-solid fa-arrow-left" />
//           Back
//         </button>
//       </div>

//       {/* Title */}
//       <h2 className="text-xl md:text-2xl font-semibold text-slate-800">
//         Financial Metrics –{" "}
//         <span className="text-emerald-600">
//           {initialCountryName?.toLowerCase() === "global"
//             ? "GLOBAL"
//             : initialCountryName?.toUpperCase()}
//         </span>
//       </h2>

//       {/* Top controls + summary + upload */}
//       <div className="mt-4 flex flex-col lg:flex-row gap-4">
//         <div className="w-full lg:max-w-md">
//           <PeriodSelect
//             range={range}
//             setRange={setRange}
//             selectedMonth={selectedMonth}
//             setSelectedMonth={setSelectedMonth}
//             selectedQuarter={selectedQuarter}
//             setSelectedQuarter={setSelectedQuarter}
//             selectedYear={selectedYear}
//             setSelectedYear={setSelectedYear}
//             yearOptions={yearOptions}
//           />
//         </div>

//         <div className="flex-1">
//           <SummaryStrip summary={summary} currencySymbol={currencySymbol} />
//         </div>

//         {goUpload && (
//           <div className="flex items-start">
//             <button
//               onClick={goUpload}
//               className="inline-flex items-center gap-2 bg-slate-800 text-[#f8edcf] font-semibold rounded-md px-4 py-2 hover:bg-slate-700 whitespace-nowrap"
//             >
//               Upload MTD <i className="fa-solid fa-plus" />
//             </button>
//           </div>
//         )}
//       </div>

//       {/* Visualizations */}
//       {(showMonthly || showQuarterly || showYearly) && (
//         <div className="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-6">
//           {/* Bar */}
//           <div className="rounded-xl border border-neutral-200 bg-white p-4 shadow-sm">
//             <h3 className="text-sm font-semibold text-slate-700 mb-3">
//               Sales vs Expense vs Profit
//             </h3>
//             <div className="h-[320px]">
//               <ReactApexChart
//                 options={barOptions}
//                 series={barSeries}
//                 type="bar"
//                 height="100%"
//               />
//             </div>
//           </div>

//           {/* Donut */}
//           <div className="rounded-xl border border-neutral-200 bg-white p-4 shadow-sm">
//             <h3 className="text-sm font-semibold text-slate-700 mb-3">
//               Composition
//             </h3>
//             <div className="h-[320px]">
//               <ReactApexChart
//                 options={donutOptions}
//                 series={donutSeries}
//                 type="donut"
//                 height="100%"
//               />
//             </div>
//           </div>
//         </div>
//       )}

//       {/* Table */}
//       <div className="mt-6">
//         <DataTable
//           title="Details"
//           rows={tableRows}
//           loading={tableLoading}
//           prettifyNumbers
//           exportFileName={`details_${initialCountryName}_${
//             selectedMonth || selectedQuarter || selectedYear || "all"
//           }`}
//         />
//       </div>
//     </div>
//   );
// }
























// // // features/country/CountryDashboard.tsx
// // "use client";

// // import React, { useEffect, useMemo, useState } from "react";
// // import { useRouter } from "next/navigation";
// // import dynamic from "next/dynamic";

// // import IntegrationDashboard from "@/features/integration/IntegrationDashboard";
// // import SummaryStrip, { Summary } from "@/components/ui/SummaryStrip";
// // import PeriodSelect from "@/components/ui/PeriodSelect";

// // import ApexBargraph from "@/components/charts/ApexBargraph";
// // import ApexDonut from "@/components/charts/ApexDonut";
// // import ApexCMChartOfSKU from "@/components/charts/ApexCMChartOfSKU";
// // import ApexGraphPage from "@/components/charts/ApexGraphPage";
// // import SKUtable from "@/components/tables/SKUtable";

// // import { API, authedFetchJson, buildUrl, rangeToApi, titleCase } from "@/lib/api/api";

// // const ReactApexChart = dynamic(() => import("react-apexcharts"), { ssr: false });

// // type UploadsData = { summary?: Summary };

// // type Props = {
// //   initialRanged: string;      // "MTD" | "QTD"
// //   initialCountryName: string; // "uk" | "us" | "global"
// //   initialMonth: string;       // "september" | "NA"
// //   initialYear: string;        // "2025" | "NA"
// // };

// // const getCurrencySymbol = (country: string) => {
// //   switch ((country || "").toLowerCase()) {
// //     case "uk": return "£";
// //     case "india": return "₹";
// //     case "us": return "$";
// //     case "europe":
// //     case "eu": return "€";
// //     case "global": return "$";
// //     default: return "¤";
// //   }
// // };

// // const quartersMap: Record<string, string[]> = {
// //   Q1: ["january", "february", "march"],
// //   Q2: ["april", "may", "june"],
// //   Q3: ["july", "august", "september"],
// //   Q4: ["october", "november", "december"],
// // };
// // const getQuarterFromMonth = (m: string) => {
// //   const month = (m || "").toLowerCase();
// //   for (const q of Object.keys(quartersMap)) if (quartersMap[q].includes(month)) return q;
// //   return "";
// // };

// // const zeroSummary: Summary = {
// //   unit_sold: 0,
// //   total_sales: 0,
// //   total_expense: 0,
// //   cm2_profit: 0,
// // };

// // export default function CountryDashboard({
// //   initialRanged,
// //   initialCountryName,
// //   initialMonth,
// //   initialYear,
// // }: Props) {
// //   const router = useRouter();
// //   const currencySymbol = useMemo(() => getCurrencySymbol(initialCountryName), [initialCountryName]);

// //   // selectors
// //   const [range, setRange] = useState<string>("monthly");
// //   const [selectedMonth, setSelectedMonth] = useState<string>("");
// //   const [selectedQuarter, setSelectedQuarter] = useState<string>("");
// //   const [selectedYear, setSelectedYear] = useState<string>("");

// //   // data
// //   const [summaryData, setSummaryData] = useState<UploadsData | null>(null);

// //   const yearOptions = useMemo(
// //     () => Array.from({ length: 2 }, (_, i) => String(new Date().getFullYear() - i)),
// //     []
// //   );

// //   // initialize from props
// //   useEffect(() => {
// //     const rangedUpper = (initialRanged || "").toUpperCase();
// //     if (rangedUpper === "QTD") {
// //       setRange("quarterly");
// //       setSelectedQuarter(getQuarterFromMonth(initialMonth));
// //       setSelectedYear(initialYear);
// //       setSelectedMonth("");
// //     } else {
// //       setRange("monthly");
// //       setSelectedMonth(initialMonth);
// //       setSelectedYear(initialYear);
// //       setSelectedQuarter("");
// //     }
// //   }, [initialRanged, initialMonth, initialYear]);

// //   const selectionsReady =
// //     (range === "monthly" && !!selectedMonth && !!selectedYear) ||
// //     (range === "quarterly" && !!selectedQuarter && !!selectedYear) ||
// //     (range === "yearly" && !!selectedYear);

// //   // summary fetch
// //   useEffect(() => {
// //     if (!selectionsReady) return;
// //     (async () => {
// //       try {
// //         const url = buildUrl(API.history, {
// //           range: rangeToApi(range),
// //           month: titleCase(selectedMonth),
// //           quarter: selectedQuarter,
// //           year: selectedYear,
// //           country: initialCountryName.toLowerCase(),
// //         });
// //         const json = await authedFetchJson<UploadsData>(url);
// //         setSummaryData(json);
// //       } catch {
// //         setSummaryData(null);
// //       }
// //     })();
// //   }, [selectionsReady, range, selectedMonth, selectedQuarter, selectedYear, initialCountryName]);

// //   if (initialMonth === "NA" || initialYear === "NA") {
// //     return <IntegrationDashboard />;
// //   }

// //   const monthApi = titleCase(selectedMonth);
// //   const rangeApi = rangeToApi(range) as "MTD" | "QTD" | "YTD";

// //   const summary = summaryData?.summary ?? zeroSummary;

// //   // Totals (bar/donut) from summary to match your classic page fallback:
// //   const barSeries = [
// //     { name: "Sales", data: [Number(summary.total_sales || 0)] },
// //     { name: "Expense", data: [Number(summary.total_expense || 0)] },
// //     { name: "Profit", data: [Number(summary.cm2_profit || 0)] },
// //   ];
// //   const barOptions: ApexCharts.ApexOptions = {
// //     chart: { type: "bar", toolbar: { show: false } },
// //     plotOptions: { bar: { columnWidth: "40%", borderRadius: 6 } },
// //     dataLabels: { enabled: false },
// //     xaxis: { categories: ["Totals"], axisBorder: { show: false }, axisTicks: { show: false } },
// //     yaxis: { labels: { formatter: (v) => `${currencySymbol} ${Number(v).toLocaleString()}` } },
// //     tooltip: { y: { formatter: (v: number) => `${currencySymbol} ${v.toLocaleString()}` } },
// //     legend: { position: "top" },
// //     grid: { strokeDashArray: 4 },
// //   };

// //   const donutSeries = [
// //     Number(summary.total_sales || 0),
// //     Number(summary.total_expense || 0),
// //     Number(summary.cm2_profit || 0),
// //   ];
// //   const donutOptions: ApexCharts.ApexOptions = {
// //     chart: { type: "donut" },
// //     labels: ["Sales", "Expense", "Profit"],
// //     legend: { position: "bottom" },
// //     dataLabels: {
// //       formatter: (_val: number, opts: any) => {
// //         const raw = opts.w.config.series[opts.seriesIndex] || 0;
// //         return `${currencySymbol} ${Number(raw).toLocaleString()}`;
// //       },
// //     },
// //     plotOptions: {
// //       pie: { donut: { size: "60%", labels: { show: true, total: { show: true } } } },
// //     },
// //   };

// //   const goBack = () => router.push("/country/QTD/global/NA/NA");

// //   const showMonthly = range === "monthly" && !!selectedMonth && !!selectedYear;
// //   const showQuarterly = range === "quarterly" && !!selectedQuarter && !!selectedYear;
// //   const showYearly = range === "yearly" && !!selectedYear;

// //   return (
// //     <div className="px-3 md:px-5 lg:px-8 py-4">
// //       <div className="mb-3">
// //         <button
// //           onClick={goBack}
// //           className="inline-flex items-center gap-2 bg-slate-800 text-[#f8edcf] font-semibold rounded-md px-4 py-2 hover:bg-slate-700"
// //         >
// //           <i className="fa-solid fa-arrow-left" />
// //           Back
// //         </button>
// //       </div>

// //       <h2 className="text-xl md:text-2xl font-semibold text-slate-800">
// //         Financial Metrics –{" "}
// //         <span className="text-emerald-600">
// //           {initialCountryName?.toLowerCase() === "global" ? "GLOBAL" : initialCountryName?.toUpperCase()}
// //         </span>
// //       </h2>

// //       {/* Controls + summary + upload */}
// //       <div className="mt-4 flex flex-col lg:flex-row gap-4">
// //         <div className="w-full lg:max-w-md">
// //           <PeriodSelect
// //             range={range}
// //             setRange={setRange}
// //             selectedMonth={selectedMonth}
// //             setSelectedMonth={setSelectedMonth}
// //             selectedQuarter={selectedQuarter}
// //             setSelectedQuarter={setSelectedQuarter}
// //             selectedYear={selectedYear}
// //             setSelectedYear={setSelectedYear}
// //             yearOptions={yearOptions}
// //           />
// //         </div>

// //         <div className="flex-1">
// //           <SummaryStrip summary={summary} currencySymbol={currencySymbol} />
// //         </div>

// //         {initialCountryName !== "global" && (
// //           <div className="flex items-start">
// //             <button
// //               onClick={() => router.push(`/Upload/${initialCountryName}`)}
// //               className="inline-flex items-center gap-2 bg-slate-800 text-[#f8edcf] font-semibold rounded-md px-4 py-2 hover:bg-slate-700 whitespace-nowrap"
// //             >
// //               Upload MTD <i className="fa-solid fa-plus" />
// //             </button>
// //           </div>
// //         )}
// //       </div>

// //       {/* Visuals (mirror your working page) */}
// //       {(showMonthly || showQuarterly || showYearly) && (
// //         <div className="mt-6 space-y-6">
// //           {/* Totals (fallback) */}
// //           <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
// //             <div className="rounded-xl border border-neutral-200 bg-white p-4 shadow-sm">
// //               <h3 className="text-sm font-semibold text-slate-700 mb-3">Sales vs Expense vs Profit</h3>
// //               <div className="h-[320px]">
// //                 <ReactApexChart options={barOptions} series={barSeries} type="bar" height="100%" />
// //               </div>
// //             </div>

// //             <div className="rounded-xl border border-neutral-200 bg-white p-4 shadow-sm">
// //               <h3 className="text-sm font-semibold text-slate-700 mb-3">Composition</h3>
// //               <div className="h-[320px]">
// //                 <ReactApexChart options={donutOptions} series={donutSeries} type="donut" height="100%" />
// //               </div>
// //             </div>
// //           </div>

// //           {/* Trend */}
// //           <ApexGraphPage
// //             rangeApi={rangeApi}
// //             monthApi={monthApi}
// //             quarter={selectedQuarter}
// //             year={selectedYear}
// //             country={initialCountryName}
// //           />

// //           {/* Donut + CM2 by SKU (remote) */}
// //           <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
// //             <ApexDonut
// //               rangeApi={rangeApi}
// //               monthApi={monthApi}
// //               quarter={selectedQuarter}
// //               year={selectedYear}
// //               country={initialCountryName}
// //             />
// //             <ApexCMChartOfSKU
// //               rangeApi={rangeApi}
// //               monthApi={monthApi}
// //               quarter={selectedQuarter}
// //               year={selectedYear}
// //               country={initialCountryName}
// //             />
// //           </div>
// //         </div>
// //       )}

// //       {/* Tables */}
// //       <div className="mt-6 space-y-6">
// //         {/* Overview table — optional: reuse your existing DataTable if you want */}
// //         {/* <OverviewTable ... /> */}

// //         {/* SKU Table */}
// //         <SKUtable
// //           rangeApi={rangeApi}
// //           monthApi={monthApi}
// //           quarter={selectedQuarter}
// //           year={selectedYear}
// //           country={initialCountryName}
// //         />
// //       </div>
// //     </div>
// //   );
// // }














