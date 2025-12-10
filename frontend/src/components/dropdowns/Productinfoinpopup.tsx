// "use client";

// import React, { useEffect, useMemo, useState } from "react";
// import { Line } from "react-chartjs-2";
// import {
//   Chart as ChartJS,
//   CategoryScale,
//   LinearScale,
//   PointElement,
//   LineElement,
//   Title as ChartTitle,
//   Tooltip,
//   Legend,
//   Filler,
// } from "chart.js";
// import { useRouter, usePathname } from "next/navigation";

// ChartJS.register(
//   CategoryScale,
//   LinearScale,
//   PointElement,
//   LineElement,
//   ChartTitle,
//   Tooltip,
//   Legend,
//   Filler
// );

// type ProductInfoInPopupProps = {
//   productname?: string; // default "Menthol"
//   onClose: () => void;
//   countryName: string;
//   month: string;
//   year: string | number;
// };

// type ApiResponse = {
//   success?: boolean;
//   data?: Record<
//     string, // country key (e.g., "uk", "us", "global")
//     { month: string; net_sales: number }[]
//   >;
//   message?: string;
// };

// const getCurrencySymbol = (country: string) => {
//   switch (country.toLowerCase()) {
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

// const Productinfoinpopup: React.FC<ProductInfoInPopupProps> = ({
//   productname = "Menthol",
//   onClose,
//   countryName,
//   month,
//   year,
// }) => {
//   const router = useRouter();
//   const pathname = usePathname();
//   const isImprovementsPage = useMemo(
//     () => pathname?.toLowerCase().includes("mprovements") ?? false,
//     [pathname]
//   );

//   const [data, setData] = useState<ApiResponse | null>(null);
//   const [loading, setLoading] = useState(false);
//   const [error, setError] = useState("");

//   const authToken =
//     typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

//   const currencySymbol = countryName ? getCurrencySymbol(countryName) : "¤";

//   // Search (no visible UI here, retained for future use)
//   const [searchQuery, setSearchQuery] = useState("");
//   const [searchResults, setSearchResults] = useState<any[]>([]);
//   const [showSearchResults, setShowSearchResults] = useState(false);
//   const [searchLoading, setSearchLoading] = useState(false);

//   // Filters
//   const [timeRange, setTimeRange] = useState<"Yearly" | "Quarterly">("Yearly");
//   const selectedYear =
//     typeof year === "string" ? parseInt(year) || new Date().getFullYear() : year;
//   const [selectedQuarter, setSelectedQuarter] = useState<"1" | "2" | "3" | "4">("1");
//   const [selectedCountries, setSelectedCountries] = useState<{
//     uk: boolean;
//     us: boolean;
//     global: boolean;
//   }>({
//     uk: true,
//     us: true,
//     global: true,
//   });

//   const years = Array.from({ length: 5 }, (_, i) => new Date().getFullYear() - i);
//   const quarters = [
//     { value: "1", label: "Q1" },
//     { value: "2", label: "Q2" },
//     { value: "3", label: "Q3" },
//     { value: "4", label: "Q4" },
//   ];

//   const handleCountryChange = (country: "uk" | "us" | "global") => {
//     setSelectedCountries((prev) => ({ ...prev, [country]: !prev[country] }));
//   };

//   const searchProducts = async (query: string) => {
//     if (!query.trim()) {
//       setSearchResults([]);
//       setShowSearchResults(false);
//       return;
//     }
//     setSearchLoading(true);
//     try {
//       const response = await fetch(
//         `http://localhost:5000/Product_search?query=${encodeURIComponent(query)}`,
//         {
//           method: "GET",
//           headers: {
//             Authorization: `Bearer ${authToken}`,
//             "Content-Type": "application/json",
//           },
//         }
//       );
//       if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
//       const d = await response.json();
//       setSearchResults(d.products || []);
//       setShowSearchResults(true);
//     } catch (err) {
//       console.error("Search error:", err);
//       setSearchResults([]);
//       setShowSearchResults(false);
//     } finally {
//       setSearchLoading(false);
//     }
//   };

//   useEffect(() => {
//     const id = setTimeout(() => searchProducts(searchQuery), 300);
//     return () => clearTimeout(id);
//     // eslint-disable-next-line react-hooks/exhaustive-deps
//   }, [searchQuery]);

//   const fetchProductData = async () => {
//     setLoading(true);
//     setError("");

//     try {
//       const countries = Object.keys(selectedCountries).filter(
//         (c) => (selectedCountries as any)[c]
//       );

//       const payload = {
//         product_name: productname,
//         time_range: timeRange,
//         year: selectedYear,
//         quarter: timeRange === "Quarterly" ? selectedQuarter : null,
//         countries,
//       };

//       const response = await fetch(`http://localhost:5000/ProductwisePerformance`, {
//         method: "POST",
//         headers: {
//           "Content-Type": "application/json",
//           Authorization: `Bearer ${authToken}`,
//         },
//         body: JSON.stringify(payload),
//       });

//       if (!response.ok) {
//         const err = await response.json().catch(() => ({}));
//         throw new Error(err?.error || `HTTP error! status: ${response.status}`);
//       }

//       const resp = (await response.json()) as ApiResponse;
//       if (resp?.success) {
//         setData(resp);
//       } else {
//         throw new Error(resp?.message || "API returned unsuccessful response");
//       }
//     } catch (err: any) {
//       console.error("API Error:", err);
//       setError(err?.message || "Failed to fetch data from server");
//     } finally {
//       setLoading(false);
//     }
//   };

//   useEffect(() => {
//     fetchProductData();
//     // eslint-disable-next-line react-hooks/exhaustive-deps
//   }, [productname, year]);

//   const getCountryColor = (country: string) => {
//     const colors: Record<string, string> = {
//       uk: "#AB64B5",
//       us: "#87AD12",
//       global: "#F47A00",
//     };
//     return colors[country] || "#ff7c7c";
//   };

//   const prepareChartData = () => {
//     if (!data || !data.data) return [] as any[];

//     const allMonths = new Set<string>();
//     Object.values(data.data).forEach((countryData) => {
//       countryData.forEach((m) => allMonths.add(m.month));
//     });

//     const sortedMonths = Array.from(allMonths).sort((a, b) => {
//       const order = [
//         "January",
//         "February",
//         "March",
//         "April",
//         "May",
//         "June",
//         "July",
//         "August",
//         "September",
//         "October",
//         "November",
//         "December",
//       ];
//       return order.indexOf(a) - order.indexOf(b);
//     });

//     return sortedMonths.map((m) => {
//       const point: Record<string, number | string> = { month: m };
//       Object.entries(data.data!).forEach(([country, countryData]) => {
//         const found = countryData.find((d) => d.month === m);
//         point[country] = found ? found.net_sales : 0;
//       });
//       return point;
//     });
//   };

//   const formatCurrency = (value: number) =>
//     new Intl.NumberFormat("en-US", {
//       style: "currency",
//       currency: "USD",
//       minimumFractionDigits: 0,
//       maximumFractionDigits: 0,
//     }).format(value);

//   const chartJSData = useMemo(() => {
//     const raw = prepareChartData();
//     if (!raw || raw.length === 0) return null;

//     const labels = raw.map((r: any) => r.month);
//     const datasets = Object.keys(selectedCountries)
//       .filter((c) => (selectedCountries as any)[c])
//       .map((country) => ({
//         label: country.toUpperCase(),
//         data: raw.map((r: any) => (r[country] as number) || 0),
//         borderColor: getCountryColor(country),
//         backgroundColor: getCountryColor(country),
//         tension: 0.1,
//         pointRadius: 3,
//         fill: false,
//       }));

//     return { labels, datasets };
//   }, [data, selectedCountries]);

//   const chartOptions = {
//     responsive: true,
//     plugins: {
//       legend: { display: false },
//       tooltip: {
//         callbacks: {
//           label: (ctx: any) => {
//             const value = ctx.parsed.y;
//             return `${ctx.dataset.label}: ${formatCurrency(value)}`;
//           },
//         },
//       },
//     },
//     scales: {
//       x: { title: { display: true, text: "Month" } },
//       y: {
//         title: { display: true, text: `Amount (${currencySymbol})` },
//         min: 0,
//         ticks: { padding: 0 },
//       },
//     },
//   } as const;

//   const titleSuffix =
//     timeRange === "Yearly" ? `YTD ${selectedYear}` : `Q${selectedQuarter}'${selectedYear}`;

//   return (
//     <div
//       className="fixed inset-0 z-[1000] bg-black/50 flex items-center justify-center"
//       onClick={onClose}
//     >
//       <div
//         className="bg-white rounded-xl p-4 sm:p-5 md:p-6 w-[95vw] max-w-[1000px] max-h-[85vh] overflow-auto"
//         onClick={(e) => e.stopPropagation()}
//       >
//         {/* Loading */}
//         {loading && (
//           <div className="py-12 text-center">
//             <video
//               src="/infinity2.webm"
//               autoPlay
//               loop
//               muted
//               playsInline
//               className="w-[150px] h-auto mx-auto"
//             />
//           </div>
//         )}

//         {/* Error */}
//         {error && (
//           <div className="mb-6 rounded-2xl border border-red-200 bg-red-50 p-6">
//             <div className="flex items-center">
//               <div className="text-red-600 text-xl mr-3">❌</div>
//               <p className="text-red-700 font-medium m-0">{error}</p>
//             </div>
//           </div>
//         )}

//         {/* Results */}
//         {data && !loading && (
//           <div className="flex flex-col gap-6">
//             {/* Header & toggles */}
//             <div className="flex flex-col gap-3">
//               <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-2">
//                 <h3 className="text-[18px] font-bold text-[#414042]">
//                   Net Sales Trend –{" "}
//                   <span className="text-[#5EA68E] font-extrabold">
//                     {productname} ({titleSuffix})
//                   </span>
//                 </h3>

//                 <div className="flex flex-wrap items-center gap-2">
//                   {Object.entries(selectedCountries).map(([country, isSelected]) => {
//                     const color = getCountryColor(country);
//                     return (
//                       <label
//                         key={country}
//                         className={[
//                           "cursor-pointer select-none inline-flex items-center gap-2",
//                           "rounded-md border px-3 py-1.5 text-sm font-semibold transition-colors",
//                           isSelected ? "text-white" : "text-gray-700",
//                         ].join(" ")}
//                         style={{
//                           backgroundColor: isSelected ? color : "transparent",
//                           borderColor: color,
//                         }}
//                       >
//                         <input
//                           type="checkbox"
//                           className="sr-only"
//                           checked={isSelected}
//                           onChange={() => handleCountryChange(country as "uk" | "us" | "global")}
//                         />
//                         <span>{country.toUpperCase()}</span>
//                       </label>
//                     );
//                   })}
//                 </div>
//               </div>
//             </div>

//             {/* Chart */}
//             <div className="w-full flex items-center justify-center">
//               <div className="w-full h-[48vh] sm:h-[50vh] md:h-[52vh]">
//                 {chartJSData ? (
//                   <Line data={chartJSData} options={chartOptions} />
//                 ) : (
//                   <p className="text-center text-sm text-gray-500">No chart data available</p>
//                 )}
//               </div>
//             </div>

//             {/* CTA */}
//             {!isImprovementsPage && (
//               <div className="flex justify-end">
//                 <button
//                   className="inline-flex items-center gap-2 rounded-md bg-slate-800 px-4 py-2 font-semibold text-amber-100 hover:bg-slate-700 focus:outline-none focus:ring-2 focus:ring-slate-400"
//                   onClick={() =>
//                     router.push(
//                       `/productwiseperformance/${encodeURIComponent(
//                         productname
//                       )}/${encodeURIComponent(countryName)}/${encodeURIComponent(
//                         month
//                       )}/${encodeURIComponent(String(year))}`
//                     )
//                   }
//                 >
//                   Check Full Performance
//                   <i className="fa-solid fa-arrow-up-right-from-square" />
//                 </button>
//               </div>
//             )}
//           </div>
//         )}
//       </div>
//     </div>
//   );
// };

// export default Productinfoinpopup;



























"use client";

import React, { useEffect, useMemo, useState } from "react";
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title as ChartTitle,
  Tooltip,
  Legend,
  Filler,
} from "chart.js";
import { useRouter, usePathname } from "next/navigation";

// 🔹 NEW: use connected platforms
import { useConnectedPlatforms } from "@/lib/utils/useConnectedPlatforms";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  ChartTitle,
  Tooltip,
  Legend,
  Filler
);

type ProductInfoInPopupProps = {
  productname?: string;
  onClose: () => void;
  countryName: string;
  month: string;
  year: string | number;
};

type ApiResponse = {
  success?: boolean;
  data?: Record<string, { month: string; net_sales: number }[]>;
  message?: string;
};

const getCurrencySymbol = (country: string) => {
  switch (country.toLowerCase()) {
    case "uk":
      return "£";
    case "india":
      return "₹";
    case "us":
      return "$";
    case "europe":
    case "eu":
      return "€";
    case "global":
      return "$";
    default:
      return "¤";
  }
};

const getCountryColor = (country: string) => {
  const colors: Record<string, string> = {
    uk: "#AB64B5",
    us: "#87AD12",
    ca: "#0EA5E9",
    global: "#F47A00",
  };
  return colors[country] || "#FF7C7C";
};

const formatCountryLabel = (country: string) => {
  const c = country.toLowerCase();
  if (c === "uk") return "UK";
  if (c === "us") return "US";
  if (c === "ca") return "CA";
  if (c === "global") return "GLOBAL";
  return country.toUpperCase();
};

const Productinfoinpopup: React.FC<ProductInfoInPopupProps> = ({
  productname = "Menthol",
  onClose,
  countryName,
  month,
  year,
}) => {
  const router = useRouter();
  const pathname = usePathname();
  const isImprovementsPage = useMemo(
    () => pathname?.toLowerCase().includes("mprovements") ?? false,
    [pathname]
  );

  const [data, setData] = useState<ApiResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const authToken =
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

  const currencySymbol = countryName ? getCurrencySymbol(countryName) : "¤";

  // time range is still fixed to Yearly in this modal
  const [timeRange] = useState<"Yearly" | "Quarterly">("Yearly");
  const selectedYear =
    typeof year === "string" ? parseInt(year) || new Date().getFullYear() : year;
  const [selectedQuarter] = useState<"1" | "2" | "3" | "4">("1");

  // 🔹 use connected platforms → which Amazon countries are actually connected?
  const connectedPlatforms = useConnectedPlatforms();
  const connectedCountries = useMemo(() => {
    const arr: string[] = [];
    if (connectedPlatforms.amazonUk) arr.push("uk");
    if (connectedPlatforms.amazonUs) arr.push("us");
    if (connectedPlatforms.amazonCa) arr.push("ca");
    return arr;
  }, [connectedPlatforms]);

  // Which countries appear as toggles / lines
  const [countryOrder, setCountryOrder] = useState<string[]>([]);
  const [selectedCountries, setSelectedCountries] = useState<
    Record<string, boolean>
  >({});

  const handleCountryChange = (country: string) => {
    setSelectedCountries((prev) => ({
      ...prev,
      [country]: !prev[country],
    }));
  };

  // ========= FETCH DATA (only for connected countries) =========
  const fetchProductData = async (connected: string[]) => {
    setLoading(true);
    setError("");

    try {
      // Build countries list for API:
      // - all connected country codes (uk/us/ca)
      // - plus "global" for aggregate snapshot
      const countriesForApi: string[] = [...connected];
      if (!countriesForApi.includes("global")) {
        countriesForApi.unshift("global");
      }
      if (countriesForApi.length === 0) {
        countriesForApi.push("global");
      }

      const payload = {
        product_name: productname,
        time_range: timeRange,
        year: selectedYear,
        quarter: timeRange === "Quarterly" ? selectedQuarter : null,
        countries: countriesForApi,
      };

      const response = await fetch(`http://localhost:5000/ProductwisePerformance`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${authToken}`,
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err?.error || `HTTP error! status: ${response.status}`);
      }

      const resp = (await response.json()) as ApiResponse;
      if (!resp?.success || !resp.data) {
        throw new Error(resp?.message || "API returned unsuccessful response");
      }

      setData(resp);

      // 🔹 build visible country list based on:
      //    1) what API actually returns
      //    2) which platforms are connected
      const apiCountries = Object.keys(resp.data);
      const connectedSet = new Set(connected.map((c) => c.toLowerCase()));

      const nonGlobal = apiCountries.filter(
        (c) =>
          c.toLowerCase() !== "global" &&
          (connectedSet.size === 0 || connectedSet.has(c.toLowerCase()))
      );

      const ordered =
        nonGlobal.length > 0
          ? ["global", ...nonGlobal]
          : apiCountries; // fallback if only GLOBAL is present

      setCountryOrder(ordered);

      const initialSelected: Record<string, boolean> = {};
      ordered.forEach((c) => {
        initialSelected[c] = true;
      });
      setSelectedCountries(initialSelected);
    } catch (err: any) {
      console.error("API Error:", err);
      setError(err?.message || "Failed to fetch data from server");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchProductData(connectedCountries);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [productname, year, connectedCountries]);

  // ========= BUILD MONTH DATA + GLOBAL AGGREGATE =========
  const prepareChartData = () => {
    if (!data || !data.data) return [] as any[];

    const apiCountries = Object.keys(data.data);
    const connectedSet = new Set(connectedCountries.map((c) => c.toLowerCase()));

    const nonGlobal = apiCountries.filter(
      (c) =>
        c.toLowerCase() !== "global" &&
        (connectedSet.size === 0 || connectedSet.has(c.toLowerCase()))
    );

    // Months from all non-global connected countries (or from global if none)
    const allMonths = new Set<string>();
    const baseCountries =
      nonGlobal.length > 0 ? nonGlobal : apiCountries;

    baseCountries.forEach((country) => {
      (data.data![country] || []).forEach((entry) => {
        allMonths.add(entry.month);
      });
    });

    const sortedMonths = Array.from(allMonths).sort((a, b) => {
      const order = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
      ];
      return order.indexOf(a) - order.indexOf(b);
    });

    const points: Record<string, any>[] = sortedMonths.map((m) => {
      const point: Record<string, number | string> = { month: m };

      // Per-country values
      baseCountries.forEach((country) => {
        const countryData = data.data![country] || [];
        const found = countryData.find((d) => d.month === m);
        point[country] = found ? Number(found.net_sales) : 0;
      });

      // 🔹 GLOBAL = sum of all connected non-global countries for that month
      if (nonGlobal.length > 0) {
        const sum = nonGlobal.reduce(
          (total, country) => total + (point[country] as number || 0),
          0
        );
        point["global"] = sum;
      } else if (apiCountries.includes("global")) {
        // Fallback: only GLOBAL came back from API
        const globalData = data.data!["global"] || [];
        const found = globalData.find((d) => d.month === m);
        point["global"] = found ? Number(found.net_sales) : 0;
      }

      return point;
    });

    return points;
  };

  const formatCurrency = (value: number) =>
    new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);

  const chartJSData = useMemo(() => {
    const raw = prepareChartData();
    if (!raw || raw.length === 0) return null;

    const labels = raw.map((r: any) => r.month);

    const datasets = countryOrder
      .filter((country) => selectedCountries[country])
      .map((country) => ({
        label: formatCountryLabel(country),
        data: raw.map((r: any) => (r[country] as number) || 0),
        borderColor: getCountryColor(country),
        backgroundColor: getCountryColor(country),
        tension: 0.1,
        pointRadius: 3,
        fill: false,
      }));

    if (!datasets.length) return null;
    return { labels, datasets };
  }, [data, selectedCountries, countryOrder, connectedCountries]);

const chartOptions = {
  responsive: true,
  maintainAspectRatio: false, // 👈 important

  plugins: {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (ctx: any) => {
          const value = ctx.parsed.y;
          return `${ctx.dataset.label}: ${formatCurrency(value)}`;
        },
      },
    },
  },
  scales: {
    x: { title: { display: true, text: "Month" } },
    y: {
      title: { display: true, text: `Amount (${currencySymbol})` },
      min: 0,
      ticks: { padding: 0 },
    },
  },
} as const;


  const titleSuffix =
    timeRange === "Yearly" ? `YTD ${selectedYear}` : `Q${selectedQuarter}'${selectedYear}`;

  // ========= RENDER =========
  return (
    <div
      className="fixed inset-0 z-[9999] bg-black/50 flex items-center justify-center"
      onClick={onClose}
    >
      <div
        className="bg-white rounded-xl p-4 sm:p-5 md:p-6 w-[95vw] max-w-[1000px] max-h-[85vh] overflow-auto"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Loading */}
        {loading && (
          <div className="py-12 text-center">
            <video
              src="/infinity2.webm"
              autoPlay
              loop
              muted
              playsInline
              className="w-[150px] h-auto mx-auto"
            />
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="mb-6 rounded-2xl border border-red-200 bg-red-50 p-6">
            <div className="flex items-center">
              <div className="text-red-600 text-xl mr-3">❌</div>
              <p className="text-red-700 font-medium m-0">{error}</p>
            </div>
          </div>
        )}

        {/* Content */}
        {data && !loading && (
          <div className="flex flex-col gap-5">
            {/* Header + dynamic country toggles */}
            <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
              <div className="flex-1">
                <h3 className="m-0 text-xl font-bold text-[#414042]">
                  Net Sales Trend –{" "}
                  <span className="text-[#5EA68E] font-extrabold capitalize">
                    {productname} ({titleSuffix})
                  </span>
                </h3>
                <p className="mt-1 text-xs sm:text-sm text-gray-500">
                  Yearly performance comparison across regions
                </p>

                <div className="mt-3 flex flex-wrap items-center gap-3">
                  {countryOrder.map((country) => {
                    const color = getCountryColor(country);
                    const isChecked = selectedCountries[country] ?? true;
                    const label = formatCountryLabel(country);

                    return (
                      <label
                        key={country}
                        className={[
                          "shrink-0",
                          "flex items-center gap-1 sm:gap-1.5",
                          "font-semibold select-none whitespace-nowrap",
                          "text-[9px] sm:text-[10px] md:text-[11px] lg:text-xs xl:text-sm]",
                          "text-charcoal-500",
                          isChecked ? "opacity-100" : "opacity-40",
                          "cursor-pointer",
                        ].join(" ")}
                        onClick={() => handleCountryChange(country)}
                      >
                        <span
                          className="flex items-center justify-center h-3 w-3 sm:h-3.5 sm:w-3.5 rounded-sm border transition"
                          style={{
                            borderColor: color,
                            backgroundColor: isChecked ? color : "white",
                          }}
                        >
                          {isChecked && (
                            <svg
                              viewBox="0 0 24 24"
                              width="14"
                              height="14"
                              className="text-white"
                            >
                              <path
                                fill="currentColor"
                                d="M20.285 6.709a1 1 0 0 0-1.414-1.414L9 15.168l-3.879-3.88a1 1 0 0 0-1.414 1.415l4.586 4.586a1 1 0 0 0 1.414 0l10-10Z"
                              />
                            </svg>
                          )}
                        </span>
                        <span className="text-charcoal-500">{label}</span>
                      </label>
                    );
                  })}
                </div>
              </div>
            </div>

    {/* Chart */}
<div className="mt-4 flex w-full justify-center">
  <div
    className="
      relative 
      w-full 
      max-w-screen-lg      
      h-[40vh]             
      sm:h-[45vh]
      md:h-[50vh]
    "
  >
    {chartJSData ? (
      <Line
        data={chartJSData}
        options={chartOptions}
        className="!w-full !h-full"  // force canvas to fill container
      />
    ) : (
      <p className="flex h-full items-center justify-center text-sm text-gray-500">
        No chart data available.
      </p>
    )}
  </div>
</div>


            {/* CTA */}
            {/* CTA */}
            {!isImprovementsPage && (
              <div className="flex justify-end">
                <button
                  className="inline-flex items-center gap-2 rounded-md bg-slate-800 px-4 py-2 font-semibold text-amber-100 hover:bg-slate-700 focus:outline-none focus:ring-2 focus:ring-slate-400"
                  onClick={() => {
                    // 🔹 Read last fetched month/year from localStorage
                    let lastFetchedMonth = month;
                    let lastFetchedYear = String(year);

                    if (typeof window !== "undefined") {
                      const raw = window.localStorage.getItem("latestFetchedPeriod");
                      if (raw) {
                        try {
                          const parsed = JSON.parse(raw) as {
                            month?: string;
                            year?: string | number;
                          };

                          if (parsed.month) {
                            // store as slug just like AmazonFinancialDashboard does (e.g. "september")
                            lastFetchedMonth = String(parsed.month).toLowerCase();
                          }
                          if (parsed.year) {
                            lastFetchedYear = String(parsed.year);
                          }
                        } catch {
                          // ignore parse errors and just fall back to current month/year
                        }
                      }
                    }

                    // 🔹 Now push with extra params for last fetched period
                    router.push(
                      `/productwiseperformance/${encodeURIComponent(
                        productname
                      )}/${encodeURIComponent(countryName)}/${encodeURIComponent(
                        month
                      )}/${encodeURIComponent(
                        lastFetchedMonth
                      )}/${encodeURIComponent(lastFetchedYear)}`
                    );
                  }}
                >
                  Check Full Performance
                  <i className="fa-solid fa-arrow-up-right-from-square" />
                </button>
              </div>
            )}

          </div>
        )}
      </div>
    </div>
  );
};

export default Productinfoinpopup;
