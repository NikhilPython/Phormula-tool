// "use client";

// import React, { useEffect, useMemo, useState } from "react";
// import { useRouter, useParams, usePathname } from "next/navigation";
// import { Line } from "react-chartjs-2";
// import {
//   Chart as ChartJS,
//   CategoryScale,
//   LinearScale,
//   PointElement,
//   LineElement,
//   Title,
//   Tooltip,
//   Legend,
//   Filler,
// } from "chart.js";
// import zoomPlugin from "chartjs-plugin-zoom";
// import Loader from "@/components/loader/Loader";
// import SegmentedToggle from "../ui/SegmentedToggle";

// ChartJS.register(
//   CategoryScale,
//   LinearScale,
//   PointElement,
//   LineElement,
//   Title,
//   Tooltip,
//   Legend,
//   Filler,
//   zoomPlugin
// );

// type CountryKey = "uk" | "global" | "us";
// type TrendTab = "sales_cm1" | "units";

// interface ProductMetricPoint {
//   month: string; // "Jan'24"
//   net_sales: number;
//   cm1_profit: number;
//   units_sold: number;
// }

// interface ApiMonthRow {
//   month: string;
//   net_sales?: number;
//   cm1_profit?: number;
//   cm1?: number;
//   profit?: number;
//   quantity?: number;
//   units_sold?: number;
//   units?: number;
// }

// interface ApiResponse {
//   success: boolean;
//   data?: Record<string, any>;
//   message?: string;
// }

// interface ProductinfoinpopupProps {
//   productname?: string;
//   countryName?: string;
//   onClose?: () => void;
// }

// const Productinfoinpopup: React.FC<ProductinfoinpopupProps> = ({
//   productname = "Menthol",
//   countryName = "global",
//   onClose,
// }) => {
//   const params = useParams();
//   const pathname = usePathname();
//   const router = useRouter();

//   const { month, year } = params as {
//     month?: string;
//     year?: string;
//   };

//   const [loading, setLoading] = useState<boolean>(false);
//   const [error, setError] = useState<string>("");
//   const [activeTab, setActiveTab] = useState<TrendTab>("sales_cm1");

//   const [selectedCountries, setSelectedCountries] = useState<Record<CountryKey, boolean>>({
//     uk: true,
//     global: true,
//     us: false,
//   });

//   const [journeyData, setJourneyData] = useState<Record<CountryKey, ProductMetricPoint[]>>({
//     uk: [],
//     global: [],
//     us: [],
//   });

//   const authToken =
//     typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

//   const scope = (countryName || "").toLowerCase();

//   useEffect(() => {
//     if (scope === "uk") {
//       setSelectedCountries({ uk: true, global: false, us: false });
//     } else if (scope === "global") {
//       setSelectedCountries({ uk: false, global: true, us: false });
//     } else if (scope === "us") {
//       setSelectedCountries({ uk: false, global: false, us: true });
//     }
//   }, [scope]);

//   const pageScope = (countryName || "global").toLowerCase();
//   const baseCurrency: "GBP" | "USD" = pageScope === "uk" ? "GBP" : "USD";
//   const currencySymbol = baseCurrency === "GBP" ? "£" : "$";

//   const handleCountryChange = (country: CountryKey) => {
//     setSelectedCountries((prev) => ({
//       ...prev,
//       [country]: !prev[country],
//     }));
//   };

//   const getCountryColor = (country: CountryKey) => {
//     const colors: Record<CountryKey, string> = {
//       uk: "#AB64B5",
//       us: "#87AD12",
//       global: "#F47A00",
//     };
//     return colors[country];
//   };

//   const formatCurrency = (value: number) => {
//     return new Intl.NumberFormat(baseCurrency === "GBP" ? "en-GB" : "en-US", {
//       style: "currency",
//       currency: baseCurrency,
//       minimumFractionDigits: 0,
//       maximumFractionDigits: 0,
//     }).format(value);
//   };

//   const formatUnits = (value: number) => {
//     return new Intl.NumberFormat("en-US", {
//       maximumFractionDigits: 0,
//     }).format(value);
//   };

//   const monthShort = (d: Date) =>
//     d.toLocaleString("en-US", { month: "short" });

//   const monthLabel = (d: Date) =>
//     `${monthShort(d)}'${String(d.getFullYear()).slice(-2)}`;

//   const monthNameToIndex: Record<string, number> = {
//     january: 0,
//     february: 1,
//     march: 2,
//     april: 3,
//     may: 4,
//     june: 5,
//     july: 6,
//     august: 7,
//     september: 8,
//     october: 9,
//     november: 10,
//     december: 11,
//   };

//   const backendKeyFor = (country: CountryKey) => {
//     if (country === "uk") return "uk";
//     if (country === "us") return "us";
//     if (country === "global") {
//       return baseCurrency === "GBP" ? "global_gbp" : "global_usd";
//     }
//     return country;
//   };

//   const normalizeRows = (countryBlock: any): ApiMonthRow[] => {
//     if (!countryBlock) return [];

//     if (Array.isArray(countryBlock)) return countryBlock;

//     if (Array.isArray(countryBlock?.Yearly)) return countryBlock.Yearly;

//     const firstArray = Object.values(countryBlock).find((v) => Array.isArray(v));
//     return Array.isArray(firstArray) ? (firstArray as ApiMonthRow[]) : [];
//   };

//   const fetchJourneyData = async () => {
//     setLoading(true);
//     setError("");

//     try {
//       const today = new Date();
//       const currentYear = today.getFullYear();
//       const START_YEAR = 2023;

//       const countriesToRequest: CountryKey[] = ["uk", "global", "us"];
//       const yearsToFetch = Array.from(
//         { length: currentYear - START_YEAR + 1 },
//         (_, i) => START_YEAR + i
//       );

//       const responses = await Promise.all(
//         yearsToFetch.map(async (yr) => {
//           const requestPayload = {
//             product_name: productname,
//             time_range: "Yearly",
//             year: yr,
//             quarter: null,
//             countries: countriesToRequest,
//             home_currency: baseCurrency,
//           };

//           const response = await fetch(
//             `${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductwisePerformance`,
//             {
//               method: "POST",
//               headers: {
//                 "Content-Type": "application/json",
//                 Authorization: `Bearer ${authToken}`,
//               },
//               body: JSON.stringify(requestPayload),
//             }
//           );

//           if (!response.ok) {
//             let msg = `HTTP error! status: ${response.status}`;
//             try {
//               const errJson = await response.json();
//               msg = errJson.error || errJson.message || msg;
//             } catch { }
//             throw new Error(msg);
//           }

//           const json: ApiResponse = await response.json();
//           return { year: yr, json };
//         })
//       );

//       const todayEnd = new Date(today.getFullYear(), today.getMonth() - 1, 1);
//       const startDate = new Date(START_YEAR, 0, 1);

//       const valueMaps: Record<
//         CountryKey,
//         {
//           net_sales: Map<string, number>;
//           cm1_profit: Map<string, number>;
//           units_sold: Map<string, number>;
//         }
//       > = {
//         uk: {
//           net_sales: new Map(),
//           cm1_profit: new Map(),
//           units_sold: new Map(),
//         },
//         global: {
//           net_sales: new Map(),
//           cm1_profit: new Map(),
//           units_sold: new Map(),
//         },
//         us: {
//           net_sales: new Map(),
//           cm1_profit: new Map(),
//           units_sold: new Map(),
//         },
//       };

//       for (const { year: responseYear, json } of responses) {
//         if (!json?.success || !json?.data) continue;

//         (["uk", "global", "us"] as CountryKey[]).forEach((country) => {
//           const key = backendKeyFor(country);
//           const rows = normalizeRows(json.data?.[key]);

//           rows.forEach((row) => {
//             const monthIndex = monthNameToIndex[String(row.month || "").toLowerCase()];
//             if (monthIndex === undefined) return;

//             const date = new Date(responseYear, monthIndex, 1);
//             if (date > todayEnd) return;

//             const label = monthLabel(date);

//             valueMaps[country].net_sales.set(
//               label,
//               Number(row.net_sales ?? 0)
//             );

//             valueMaps[country].cm1_profit.set(
//               label,
//               Number(row.cm1_profit ?? row.cm1 ?? row.profit ?? 0)
//             );

//             valueMaps[country].units_sold.set(
//               label,
//               Number(row.quantity ?? row.units_sold ?? row.units ?? 0)
//             );
//           });
//         });
//       }

//       const months: Date[] = [];
//       const cursor = new Date(startDate);

//       while (cursor <= todayEnd) {
//         months.push(new Date(cursor));
//         cursor.setMonth(cursor.getMonth() + 1);
//       }

//       const finalData: Record<CountryKey, ProductMetricPoint[]> = {
//         uk: months.map((m) => {
//           const label = monthLabel(m);
//           return {
//             month: label,
//             net_sales: valueMaps.uk.net_sales.get(label) ?? 0,
//             cm1_profit: valueMaps.uk.cm1_profit.get(label) ?? 0,
//             units_sold: valueMaps.uk.units_sold.get(label) ?? 0,
//           };
//         }),
//         global: months.map((m) => {
//           const label = monthLabel(m);
//           return {
//             month: label,
//             net_sales: valueMaps.global.net_sales.get(label) ?? 0,
//             cm1_profit: valueMaps.global.cm1_profit.get(label) ?? 0,
//             units_sold: valueMaps.global.units_sold.get(label) ?? 0,
//           };
//         }),
//         us: months.map((m) => {
//           const label = monthLabel(m);
//           return {
//             month: label,
//             net_sales: valueMaps.us.net_sales.get(label) ?? 0,
//             cm1_profit: valueMaps.us.cm1_profit.get(label) ?? 0,
//             units_sold: valueMaps.us.units_sold.get(label) ?? 0,
//           };
//         }),
//       };

//       setJourneyData(finalData);
//     } catch (err: any) {
//       console.error("Journey API Error:", err);
//       setError(err.message || "Failed to fetch data from server");
//       setJourneyData({
//         uk: [],
//         global: [],
//         us: [],
//       });
//     } finally {
//       setLoading(false);
//     }
//   };

//   useEffect(() => {
//     fetchJourneyData();
//   }, [productname, baseCurrency]);

//   const visibleCountries: CountryKey[] =
//     scope === "uk"
//       ? ["uk"]
//       : scope === "global"
//         ? ["global"]
//         : scope === "us"
//           ? ["us"]
//           : ["uk", "global", "us"];

//   const trimmedJourneyData = useMemo(() => {
//     const activeCountries = (Object.keys(selectedCountries) as CountryKey[])
//       .filter((country) => visibleCountries.includes(country))
//       .filter((country) => selectedCountries[country]);

//     if (!activeCountries.length) {
//       return { labels: [], startIndex: 0 };
//     }

//     const baseSeries = journeyData[activeCountries[0]] || [];

//     if (!baseSeries.length) {
//       return { labels: [], startIndex: 0 };
//     }

//     const hasValueAtIndex = (idx: number) => {
//       return activeCountries.some((country) => {
//         const point = journeyData[country]?.[idx];
//         if (!point) return false;

//         if (activeTab === "units") {
//           return Number(point.units_sold || 0) > 0;
//         }

//         return (
//           Number(point.net_sales || 0) > 0 ||
//           Number(point.cm1_profit || 0) > 0
//         );
//       });
//     };

//     const firstMeaningfulIndex = baseSeries.findIndex((_, idx) => hasValueAtIndex(idx));
//     const startIndex = firstMeaningfulIndex === -1 ? 0 : firstMeaningfulIndex;

//     return {
//       labels: baseSeries.slice(startIndex).map((d) => d.month),
//       startIndex,
//     };
//   }, [journeyData, selectedCountries, visibleCountries, activeTab]);

//   const allLabels = trimmedJourneyData.labels;

//   const chartJSData = useMemo(() => {
//     const labels = allLabels;

//     if (activeTab === "sales_cm1") {
//       const salesDatasets = (Object.keys(selectedCountries) as CountryKey[])
//         .filter((country) => visibleCountries.includes(country))
//         .filter((country) => selectedCountries[country])
//         .map((country) => ({
//           label: `${country.toUpperCase()} Net Sales`,
//           data: labels.map((label) => {
//             const found = journeyData[country]?.find((d) => d.month === label);
//             return found ? found.net_sales : 0;
//           }),
//           borderColor: getCountryColor(country),
//           backgroundColor: getCountryColor(country),
//           tension: 0.35,
//           pointRadius: 3,
//           pointHoverRadius: 5,
//           fill: false,
//           borderDash: [],
//           borderWidth: 2,
//         }));

//       const cm1Datasets = (Object.keys(selectedCountries) as CountryKey[])
//         .filter((country) => visibleCountries.includes(country))
//         .filter((country) => selectedCountries[country])
//         .map((country) => ({
//           label: `${country.toUpperCase()} CM1 Profit`,
//           data: labels.map((label) => {
//             const found = journeyData[country]?.find((d) => d.month === label);
//             return found ? found.cm1_profit : 0;
//           }),
//           borderColor: getCountryColor(country),
//           backgroundColor: getCountryColor(country),
//           tension: 0.35,
//           pointRadius: 3,
//           pointHoverRadius: 5,
//           fill: false,
//           borderDash: [6, 6],
//           borderWidth: 2.5,
//         }));

//       return {
//         labels,
//         datasets: [...salesDatasets, ...cm1Datasets],
//       };
//     }

//     const unitDatasets = (Object.keys(selectedCountries) as CountryKey[])
//       .filter((country) => visibleCountries.includes(country))
//       .filter((country) => selectedCountries[country])
//       .map((country) => ({
//         label: `${country.toUpperCase()} Units`,
//         data: labels.map((label) => {
//           const found = journeyData[country]?.find((d) => d.month === label);
//           return found ? found.units_sold : 0;
//         }),
//         borderColor: getCountryColor(country),
//         backgroundColor: getCountryColor(country),
//         tension: 0.35,
//         pointRadius: 3,
//         pointHoverRadius: 5,
//         fill: false,
//         borderDash: [],
//         borderWidth: 2,
//       }));

//     return {
//       labels,
//       datasets: unitDatasets,
//     };
//   }, [activeTab, allLabels, journeyData, selectedCountries, visibleCountries]);

//   const initialMinIndex = Math.max(0, allLabels.length - 12);
//   const initialMaxIndex = Math.max(0, allLabels.length - 1);

//   const chartOptions: any = useMemo(
//     () => ({
//       responsive: true,
//       maintainAspectRatio: false,
//       animation: false,
//       plugins: {
//         legend: {
//           display: false,
//         },
//         tooltip: {
//           callbacks: {
//             label: (context: any) => {
//               const value = context.parsed.y;
//               const label = String(context.dataset.label || "").toLowerCase();

//               if (label.includes("unit")) {
//                 return `${context.dataset.label}: ${formatUnits(value)}`;
//               }

//               return `${context.dataset.label}: ${formatCurrency(value)}`;
//             },
//           },
//         },
//         zoom: {
//           limits: {
//             x: {
//               min: 0,
//               max: Math.max(0, allLabels.length - 1),
//               minRange: Math.min(12, Math.max(1, allLabels.length)),
//             },
//           },
//           pan: {
//             enabled: true,
//             mode: "x",
//           },
//           zoom: {
//             wheel: {
//               enabled: false,
//             },
//             pinch: {
//               enabled: false,
//             },
//             drag: {
//               enabled: false,
//             },
//             mode: "x",
//           },
//         },
//       },
//       scales: {
//         x: {
//           min: initialMinIndex,
//           max: initialMaxIndex,
//           title: {
//             display: false,
//             text: "Month",
//           },
//           ticks: {
//             maxRotation: 0,
//             minRotation: 0,
//             autoSkip: false,
//           },
//           grid: {
//             display: false,
//           },
//         },
//         y: {
//           title: {
//             display: true,
//             text: activeTab === "units" ? "Units (in nos.)" : `Amount (${currencySymbol})`,
//           },
//           min: 0,
//           ticks: {
//             padding: 0,
//             callback: (value: number) =>
//               activeTab === "units" ? formatUnits(value) : formatCurrency(value),
//           },
//         },
//       },
//     }),
//     [activeTab, allLabels.length, currencySymbol, initialMaxIndex, initialMinIndex]
//   );

//   const isImprovementsPage = pathname?.includes("mprovements") || false;

//   return (
//     <>
//       <style>{`
//         .net-sales-wrapper {
//           width: 100%;
//           margin-bottom: 15px;
//         }

//         .net-sales-content {
//           display: flex;
//           justify-content: space-between;
//           align-items: flex-start;
//           flex-wrap: wrap;
//           gap: 16px;
//         }

//         .net-sales-left {
//           flex: 1 1 auto;
//         }

//         .net-sales-header {
//           display: flex;
//           flex-direction: column;
//           align-items: flex-start;
//         }

//         .net-sales-title {
//           margin: 0;
//           font-size: 18px;
//           font-family: 'Lato', sans-serif;
//           color: #414042;
//           background-color: white;
//           border-radius: 7px;
//           font-weight: bold;
//           padding: 0;
//         }

//         .highlighted {
//           color: #5ea68e;
//           font-weight: 500;
//         }

//         .net-sales-right {
//           display: flex;
//           align-items: center;
//           justify-content: flex-end;
//           flex-wrap: wrap;
//           gap: 12px;
//         }

//         .country-toggle-group {
//           display: flex;
//           flex-wrap: wrap;
//           gap: 16px;
//           justify-content: flex-end;
//         }

//         .country-toggle {
//           display: flex;
//           align-items: center;
//           gap: 6px;
//           font-size: 0.9vw;
//           color: #111827;
//           cursor: pointer;
//           font-weight: 600;
//           padding: 5px 10px;
//           border-radius: 16px;
//           transition: all 0.2s ease-in-out;
//           user-select: none;
//           --country-color: #ccc;
//         }

//         .country-toggle input[type="checkbox"] {
//           appearance: none;
//           width: 13px;
//           height: 13px;
//           margin: 0;
//           border: none;
//           border-radius: 2px;
//           background-color: var(--country-color);
//           display: grid;
//           place-content: center;
//           cursor: pointer;
//           transition: background-color 0.2s;
//           position: relative;
//         }

//         .country-toggle input[type="checkbox"]::before {
//           content: "✔";
//           font-size: 0.5vw;
//           color: white;
//           transform: scale(0);
//           transition: transform 0.1s ease-in-out;
//         }

//         .country-toggle input[type="checkbox"]:checked::before {
//           transform: scale(1);
//         }

//         .country-label {
//           color: var(--country-color);
//           text-decoration: underline;
//           text-decoration-thickness: 1.2px;
//           text-underline-offset: 2px;
//           font-size: 0.9vw;
//           font-weight: 600;
//         }

//         .styled-button {
//           padding: 8px 16px;
//           font-size: .9rem;
//           border: none;
//           border-radius: 6px;
//           cursor: pointer;
//           transition: background-color .2s ease;
//           box-shadow: 0 3px 6px rgba(0,0,0,.15);
//           background-color: #2c3e50;
//           color: #f8edcf;
//           font-weight: bold;
//         }

//         .styled-button:hover {
//           background-color: #1f2a36;
//         }

//         .tab-switch {
//           display: inline-flex;
//           border: 1px solid #d1d5db;
//           border-radius: 8px;
//           overflow: hidden;
//           background: #fff;
//         }

//         .tab-btn {
//           border: none;
//           background: transparent;
//           padding: 8px 14px;
//           cursor: pointer;
//           font-size: 13px;
//           font-weight: 600;
//           color: #374151;
//         }

//         .tab-btn.active {
//           background: #2c3e50;
//           color: #fff;
//         }

//         .bottom-legend {
//           margin-top: 12px;
//           display: flex;
//           justify-content: center;
//           align-items: center;
//           gap: 20px;
//           flex-wrap: wrap;
//           color: #374151;
//           font-size: 13px;
//           font-weight: 600;
//         }

//         .legend-item {
//           display: flex;
//           align-items: center;
//           gap: 8px;
//         }

//         .legend-solid {
//           width: 36px;
//           height: 0;
//           border-top: 2px solid #374151;
//         }

//         .legend-dashed {
//           width: 36px;
//           height: 0;
//           border-top: 2px dashed #374151;
//         }
//       `}</style>

//       <div className="modal-backdrop" onClick={onClose}>
//         <div className="modal-content" onClick={(e) => e.stopPropagation()}>
//           {loading && (
//             <div className="flex flex-col items-center justify-center py-12 text-center">
//               <Loader fullscreen transparent />
//             </div>
//           )}

//           {error && (
//             <div
//               style={{
//                 backgroundColor: "#fef2f2",
//                 border: "1px solid #fecaca",
//                 borderRadius: "16px",
//                 padding: "24px",
//                 marginBottom: "32px",
//               }}
//             >
//               <div style={{ display: "flex", alignItems: "center" }}>
//                 <div style={{ color: "#dc2626", fontSize: "1.25rem", marginRight: "12px" }}>
//                   ❌
//                 </div>
//                 <p style={{ color: "#b91c1c", fontWeight: "500", margin: 0 }}>{error}</p>
//               </div>
//             </div>
//           )}

//           {!loading && !error && (
//             <div style={{ display: "flex", flexDirection: "column", gap: "32px" }}>
//               <div>
//                 <div className="net-sales-wrapper">
//                   <div className="net-sales-content">
//                     <div className="net-sales-left">
//                       <div className="net-sales-header">
//                         <h3 className="net-sales-title">
//                           Product Journey -{" "}
//                           <b className="highlighted">
//                             {productname} (last completed months only)
//                           </b>
//                         </h3>
//                         <p className="text-xs text-gray-500 mt-1">
//                           Drag horizontally on the graph to explore older or newer months.
//                           Viewport shows 12 months at a time.
//                         </p>
//                       </div>
//                     </div>

//                     <div className="net-sales-right">
//                       <SegmentedToggle<TrendTab>
//                         value={activeTab}
//                         onChange={setActiveTab}
//                         textSizeClass="text-xs sm:text-sm"
//                         className="w-auto"
//                         options={[
//                           { value: "sales_cm1", label: "Sales & CM1 Profit" },
//                           { value: "units", label: "Units" },
//                         ]}
//                       />


//                     </div>
//                   </div>
//                 </div>

//                 <div
//                   style={{
//                     height: "380px",
//                     maxHeight: "500px",
//                     display: "flex",
//                     justifyContent: "center",
//                     alignItems: "center",
//                   }}
//                 >
//                   {chartJSData?.labels?.length ? (
//                     <Line data={chartJSData} options={chartOptions} />
//                   ) : (
//                     <p>No chart data available</p>
//                   )}
//                 </div>

//                 <div className="bottom-legend">
//                   {activeTab === "sales_cm1" ? (
//                     <>
//                       <div className="legend-item">
//                         <span className="legend-solid" />
//                         <span>Net Sales</span>
//                       </div>
//                       <div className="legend-item">
//                         <span className="legend-dashed" />
//                         <span>CM1 Profit</span>
//                       </div>
//                     </>
//                   ) : (
//                     <div className="legend-item">
//                       <span className="legend-solid" />
//                       <span>Units</span>
//                     </div>
//                   )}
//                 </div>

//                 {!isImprovementsPage && (
//                   <button
//                     className="styled-button"
//                     onClick={() =>
//                       router.push(`/skuwiseprofit/${productname}/${countryName}/${month}/${year}`)
//                     }
//                   >
//                     Check Full Performance{" "}
//                     <i className="fa-solid fa-arrow-up-right-from-square"></i>
//                   </button>
//                 )}
//               </div>
//             </div>
//           )}
//         </div>
//       </div>
//     </>
//   );
// };

// export default Productinfoinpopup;




















"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useRouter, useParams, usePathname } from "next/navigation";
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from "chart.js";
import zoomPlugin from "chartjs-plugin-zoom";
import Loader from "@/components/loader/Loader";
import SegmentedToggle from "../ui/SegmentedToggle";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
  zoomPlugin
);

type CountryKey = "uk" | "global" | "us";
type TrendTab = "sales_cm1" | "units";

interface ProductMetricPoint {
  month: string;
  net_sales: number;
  cm1_profit: number;
  units_sold: number;
}

interface ApiMonthRow {
  month: string;
  net_sales?: number;
  cm1_profit?: number;
  cm1?: number;
  profit?: number;
  quantity?: number;
  units_sold?: number;
  units?: number;
}

interface ApiResponse {
  success: boolean;
  data?: Record<string, any>;
  message?: string;
}

interface ProductinfoinpopupProps {
  productname?: string;
  countryName?: string;
  onClose?: () => void;
}

const Productinfoinpopup: React.FC<ProductinfoinpopupProps> = ({
  productname = "Menthol",
  countryName = "global",
  onClose,
}) => {
  const params = useParams();
  const pathname = usePathname();
  const router = useRouter();

  const { month, year } = params as {
    month?: string;
    year?: string;
  };

  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>("");
  const [activeTab, setActiveTab] = useState<TrendTab>("sales_cm1");

  const [selectedCountries, setSelectedCountries] = useState<Record<CountryKey, boolean>>({
    uk: true,
    global: true,
    us: false,
  });

  const [journeyData, setJourneyData] = useState<Record<CountryKey, ProductMetricPoint[]>>({
    uk: [],
    global: [],
    us: [],
  });

  const authToken =
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

  const scope = (countryName || "").toLowerCase();

  useEffect(() => {
    if (scope === "uk") {
      setSelectedCountries({ uk: true, global: false, us: false });
    } else if (scope === "global") {
      setSelectedCountries({ uk: false, global: true, us: false });
    } else if (scope === "us") {
      setSelectedCountries({ uk: false, global: false, us: true });
    }
  }, [scope]);

  const pageScope = (countryName || "global").toLowerCase();
  const baseCurrency: "GBP" | "USD" = pageScope === "uk" ? "GBP" : "USD";
  const currencySymbol = baseCurrency === "GBP" ? "£" : "$";

  const getCountryColor = (country: CountryKey) => {
    const colors: Record<CountryKey, string> = {
      uk: "#AB64B5",
      us: "#87AD12",
      global: "#F47A00",
    };
    return colors[country];
  };

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat(baseCurrency === "GBP" ? "en-GB" : "en-US", {
      style: "currency",
      currency: baseCurrency,
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);
  };

  const formatUnits = (value: number) => {
    return new Intl.NumberFormat("en-US", {
      maximumFractionDigits: 0,
    }).format(value);
  };

  const monthShort = (d: Date) =>
    d.toLocaleString("en-US", { month: "short" });

  const monthLabel = (d: Date) =>
    `${monthShort(d)}'${String(d.getFullYear()).slice(-2)}`;

  const monthNameToIndex: Record<string, number> = {
    january: 0,
    february: 1,
    march: 2,
    april: 3,
    may: 4,
    june: 5,
    july: 6,
    august: 7,
    september: 8,
    october: 9,
    november: 10,
    december: 11,
  };

  const backendKeyFor = (country: CountryKey) => {
    if (country === "uk") return "uk";
    if (country === "us") return "us";
    if (country === "global") {
      return baseCurrency === "GBP" ? "global_gbp" : "global_usd";
    }
    return country;
  };

  const normalizeRows = (countryBlock: any): ApiMonthRow[] => {
    if (!countryBlock) return [];
    if (Array.isArray(countryBlock)) return countryBlock;
    if (Array.isArray(countryBlock?.Yearly)) return countryBlock.Yearly;

    const firstArray = Object.values(countryBlock).find((v) => Array.isArray(v));
    return Array.isArray(firstArray) ? (firstArray as ApiMonthRow[]) : [];
  };

  const fetchJourneyData = async () => {
    setLoading(true);
    setError("");

    try {
      const today = new Date();
      const currentYear = today.getFullYear();
      const START_YEAR = 2023;

      const countriesToRequest: CountryKey[] = ["uk", "global", "us"];
      const yearsToFetch = Array.from(
        { length: currentYear - START_YEAR + 1 },
        (_, i) => START_YEAR + i
      );

      const responses = await Promise.all(
        yearsToFetch.map(async (yr) => {
          const requestPayload = {
            product_name: productname,
            time_range: "Yearly",
            year: yr,
            quarter: null,
            countries: countriesToRequest,
            home_currency: baseCurrency,
          };

          const response = await fetch(
            `${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductwisePerformance`,
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${authToken}`,
              },
              body: JSON.stringify(requestPayload),
            }
          );

          if (!response.ok) {
            let msg = `HTTP error! status: ${response.status}`;
            try {
              const errJson = await response.json();
              msg = errJson.error || errJson.message || msg;
            } catch { }
            throw new Error(msg);
          }

          const json: ApiResponse = await response.json();
          return { year: yr, json };
        })
      );

      const todayEnd = new Date(today.getFullYear(), today.getMonth() - 1, 1);
      const startDate = new Date(START_YEAR, 0, 1);

      const valueMaps: Record<
        CountryKey,
        {
          net_sales: Map<string, number>;
          cm1_profit: Map<string, number>;
          units_sold: Map<string, number>;
        }
      > = {
        uk: {
          net_sales: new Map(),
          cm1_profit: new Map(),
          units_sold: new Map(),
        },
        global: {
          net_sales: new Map(),
          cm1_profit: new Map(),
          units_sold: new Map(),
        },
        us: {
          net_sales: new Map(),
          cm1_profit: new Map(),
          units_sold: new Map(),
        },
      };

      for (const { year: responseYear, json } of responses) {
        if (!json?.success || !json?.data) continue;

        (["uk", "global", "us"] as CountryKey[]).forEach((country) => {
          const key = backendKeyFor(country);
          const rows = normalizeRows(json.data?.[key]);

          rows.forEach((row) => {
            const monthIndex = monthNameToIndex[String(row.month || "").toLowerCase()];
            if (monthIndex === undefined) return;

            const date = new Date(responseYear, monthIndex, 1);
            if (date > todayEnd) return;

            const label = monthLabel(date);

            valueMaps[country].net_sales.set(label, Number(row.net_sales ?? 0));
            valueMaps[country].cm1_profit.set(
              label,
              Number(row.cm1_profit ?? row.cm1 ?? row.profit ?? 0)
            );
            valueMaps[country].units_sold.set(
              label,
              Number(row.quantity ?? row.units_sold ?? row.units ?? 0)
            );
          });
        });
      }

      const months: Date[] = [];
      const cursor = new Date(startDate);

      while (cursor <= todayEnd) {
        months.push(new Date(cursor));
        cursor.setMonth(cursor.getMonth() + 1);
      }

      const finalData: Record<CountryKey, ProductMetricPoint[]> = {
        uk: months.map((m) => {
          const label = monthLabel(m);
          return {
            month: label,
            net_sales: valueMaps.uk.net_sales.get(label) ?? 0,
            cm1_profit: valueMaps.uk.cm1_profit.get(label) ?? 0,
            units_sold: valueMaps.uk.units_sold.get(label) ?? 0,
          };
        }),
        global: months.map((m) => {
          const label = monthLabel(m);
          return {
            month: label,
            net_sales: valueMaps.global.net_sales.get(label) ?? 0,
            cm1_profit: valueMaps.global.cm1_profit.get(label) ?? 0,
            units_sold: valueMaps.global.units_sold.get(label) ?? 0,
          };
        }),
        us: months.map((m) => {
          const label = monthLabel(m);
          return {
            month: label,
            net_sales: valueMaps.us.net_sales.get(label) ?? 0,
            cm1_profit: valueMaps.us.cm1_profit.get(label) ?? 0,
            units_sold: valueMaps.us.units_sold.get(label) ?? 0,
          };
        }),
      };

      setJourneyData(finalData);
    } catch (err: any) {
      console.error("Journey API Error:", err);
      setError(err.message || "Failed to fetch data from server");
      setJourneyData({
        uk: [],
        global: [],
        us: [],
      });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchJourneyData();
  }, [productname, baseCurrency]);

  const visibleCountries: CountryKey[] =
    scope === "uk"
      ? ["uk"]
      : scope === "global"
        ? ["global"]
        : scope === "us"
          ? ["us"]
          : ["uk", "global", "us"];

  const trimmedJourneyData = useMemo(() => {
    const activeCountries = (Object.keys(selectedCountries) as CountryKey[])
      .filter((country) => visibleCountries.includes(country))
      .filter((country) => selectedCountries[country]);

    if (!activeCountries.length) {
      return { labels: [], startIndex: 0 };
    }

    const baseSeries = journeyData[activeCountries[0]] || [];

    if (!baseSeries.length) {
      return { labels: [], startIndex: 0 };
    }

    const hasValueAtIndex = (idx: number) => {
      return activeCountries.some((country) => {
        const point = journeyData[country]?.[idx];
        if (!point) return false;

        if (activeTab === "units") {
          return Number(point.units_sold || 0) > 0;
        }

        return (
          Number(point.net_sales || 0) > 0 ||
          Number(point.cm1_profit || 0) > 0
        );
      });
    };

    const firstMeaningfulIndex = baseSeries.findIndex((_, idx) => hasValueAtIndex(idx));
    const startIndex = firstMeaningfulIndex === -1 ? 0 : firstMeaningfulIndex;

    return {
      labels: baseSeries.slice(startIndex).map((d) => d.month),
      startIndex,
    };
  }, [journeyData, selectedCountries, visibleCountries, activeTab]);

  const allLabels = trimmedJourneyData.labels;

  const chartJSData = useMemo(() => {
    const labels = allLabels;

    if (activeTab === "sales_cm1") {
      const salesDatasets = (Object.keys(selectedCountries) as CountryKey[])
        .filter((country) => visibleCountries.includes(country))
        .filter((country) => selectedCountries[country])
        .map((country) => ({
          label: `${country.toUpperCase()} Net Sales`,
          data: labels.map((label) => {
            const found = journeyData[country]?.find((d) => d.month === label);
            return found ? found.net_sales : 0;
          }),
          borderColor: getCountryColor(country),
          backgroundColor: getCountryColor(country),
          tension: 0.35,
          pointRadius: 3,
          pointHoverRadius: 5,
          fill: false,
          borderDash: [],
          borderWidth: 2,
        }));

      const cm1Datasets = (Object.keys(selectedCountries) as CountryKey[])
        .filter((country) => visibleCountries.includes(country))
        .filter((country) => selectedCountries[country])
        .map((country) => ({
          label: `${country.toUpperCase()} CM1 Profit`,
          data: labels.map((label) => {
            const found = journeyData[country]?.find((d) => d.month === label);
            return found ? found.cm1_profit : 0;
          }),
          borderColor: getCountryColor(country),
          backgroundColor: getCountryColor(country),
          tension: 0.35,
          pointRadius: 3,
          pointHoverRadius: 5,
          fill: false,
          borderDash: [6, 6],
          borderWidth: 2.5,
        }));

      return {
        labels,
        datasets: [...salesDatasets, ...cm1Datasets],
      };
    }

    const unitDatasets = (Object.keys(selectedCountries) as CountryKey[])
      .filter((country) => visibleCountries.includes(country))
      .filter((country) => selectedCountries[country])
      .map((country) => ({
        label: `${country.toUpperCase()} Units`,
        data: labels.map((label) => {
          const found = journeyData[country]?.find((d) => d.month === label);
          return found ? found.units_sold : 0;
        }),
        borderColor: getCountryColor(country),
        backgroundColor: getCountryColor(country),
        tension: 0.35,
        pointRadius: 3,
        pointHoverRadius: 5,
        fill: false,
        borderDash: [],
        borderWidth: 2,
      }));

    return {
      labels,
      datasets: unitDatasets,
    };
  }, [activeTab, allLabels, journeyData, selectedCountries, visibleCountries]);

  const initialMinIndex = Math.max(0, allLabels.length - 12);
  const initialMaxIndex = Math.max(0, allLabels.length - 1);

  const chartOptions: any = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          callbacks: {
            label: (context: any) => {
              const value = context.parsed.y;
              const label = String(context.dataset.label || "").toLowerCase();

              if (label.includes("unit")) {
                return `${context.dataset.label}: ${formatUnits(value)}`;
              }

              return `${context.dataset.label}: ${formatCurrency(value)}`;
            },
          },
        },
        zoom: {
          limits: {
            x: {
              min: 0,
              max: Math.max(0, allLabels.length - 1),
              minRange: Math.min(12, Math.max(1, allLabels.length)),
            },
          },
          pan: {
            enabled: true,
            mode: "x",
          },
          zoom: {
            wheel: {
              enabled: false,
            },
            pinch: {
              enabled: false,
            },
            drag: {
              enabled: false,
            },
            mode: "x",
          },
        },
      },
      scales: {
        x: {
          min: initialMinIndex,
          max: initialMaxIndex,
          title: {
            display: false,
            text: "Month",
          },
          ticks: {
            maxRotation: 0,
            minRotation: 0,
            autoSkip: false,
          },
          grid: {
            display: false,
          },
        },
        y: {
          title: {
            display: true,
            text: activeTab === "units" ? "Units (in nos.)" : `Amount (${currencySymbol})`,
          },
          min: 0,
          ticks: {
            padding: 0,
            callback: (value: number) =>
              activeTab === "units" ? formatUnits(value) : formatCurrency(value),
          },
        },
      },
    }),
    [activeTab, allLabels.length, currencySymbol, initialMaxIndex, initialMinIndex]
  );

  const isImprovementsPage = pathname?.includes("mprovements") || false;


  return (
    <div className="w-full rounded-2xl border border-gray-200 bg-white p-3 shadow-sm">
      {loading && (
        <div className="flex flex-col items-center justify-center py-12 text-center">
          <Loader fullscreen transparent />
        </div>
      )}

      {error && (
        <div className="mb-8 rounded-2xl border border-red-200 bg-red-50 p-6">
          <div className="flex items-center">
            <div className="mr-3 text-xl text-red-600">❌</div>
            <p className="m-0 font-medium text-red-700">{error}</p>
          </div>
        </div>
      )}

      {!loading && !error && (
        <div className="flex flex-col gap-8">
          <div>
            <div className="mb-4 w-full">
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div className="min-w-0 flex-1">
                  <div className="flex flex-col items-start">
                    <h3 className="m-0 rounded-md bg-white p-0 font-['Lato',sans-serif] text-[18px] font-bold text-[#414042]">
                      Product Journey -{" "}
                      <b className="font-medium text-[#5ea68e]">
                        {productname} (last completed months only)
                      </b>
                    </h3>
                    <p className="mt-1 text-xs text-gray-500">
                      Drag horizontally on the graph to explore older or newer months.
                      Viewport shows 12 months at a time.
                    </p>
                  </div>
                </div>

                <div className="flex flex-wrap items-center justify-end gap-3">
                  <SegmentedToggle<TrendTab>
                    value={activeTab}
                    onChange={setActiveTab}
                    textSizeClass="text-xs sm:text-sm"
                    className="w-auto"
                    options={[
                      { value: "sales_cm1", label: "Sales & CM1 Profit" },
                      { value: "units", label: "Units" },
                    ]}
                  />
                </div>
              </div>
            </div>

            <div className="flex h-[380px] max-h-[500px] items-center justify-center">
              {chartJSData?.labels?.length ? (
                <Line data={chartJSData} options={chartOptions} />
              ) : (
                <p>No chart data available</p>
              )}
            </div>

            <div className="mt-3 flex flex-wrap items-center justify-center gap-5 text-[13px] font-semibold text-gray-700">
              {activeTab === "sales_cm1" ? (
                <>
                  <div className="flex items-center gap-2">
                    <span className="h-0 w-9 border-t-2 border-gray-700" />
                    <span>Net Sales</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="h-0 w-9 border-t-2 border-dashed border-gray-700" />
                    <span>CM1 Profit</span>
                  </div>
                </>
              ) : (
                <div className="flex items-center gap-2">
                  <span className="h-0 w-9 border-t-2 border-gray-700" />
                  <span>Units</span>
                </div>
              )}
            </div>

            {/* {!isImprovementsPage && (
              <button
                className="mt-4 cursor-pointer rounded-md bg-[#2c3e50] px-4 py-2 text-[0.9rem] font-bold text-[#f8edcf] shadow-md transition-colors duration-200 hover:bg-[#1f2a36]"
                onClick={() =>
                  router.push(`/skuwiseprofit/${productname}/${countryName}/${month}/${year}`)
                }
              >
                Check Full Performance{" "}
                <i className="fa-solid fa-arrow-up-right-from-square"></i>
              </button>
            )} */}
          </div>
        </div>
      )}
    </div>

  );
};

export default Productinfoinpopup;