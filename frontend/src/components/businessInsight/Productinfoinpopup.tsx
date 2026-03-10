// // // Productinfoinpopup.tsx
// // import React, { useEffect, useState } from 'react';
// // import { useRouter, useParams, usePathname } from 'next/navigation'; // Next.js uses next/navigation
// // import { Line } from "react-chartjs-2";
// // import {
// //   Chart as ChartJS,
// //   CategoryScale,
// //   LinearScale,
// //   PointElement,
// //   LineElement,
// //   Title,
// //   Tooltip,
// //   Legend,
// //   Filler,
// // } from "chart.js";
// // import Loader from '@/components/loader/Loader';

// // ChartJS.register(
// //   CategoryScale,
// //   LinearScale,
// //   PointElement,
// //   LineElement,
// //   Title,
// //   Tooltip,
// //   Legend,
// //   Filler
// // );

// // interface ProductDataPoint {
// //   month: string;
// //   net_sales: number;
// // }

// // interface CountryData {
// //   [country: string]: ProductDataPoint[];
// // }

// // interface ApiResponse {
// //   success: boolean;
// //   data?: {
// //     [country: string]: CountryData;
// //   };
// //   message?: string;
// // }

// // interface ProductinfoinpopupProps {
// //   productname?: string;
// //   countryName?: string;
// //   onClose?: () => void;
// // }

// // const Productinfoinpopup: React.FC<ProductinfoinpopupProps> = ({
// //   productname = "Menthol",
// //   countryName = "global",
// //   onClose
// // }) => {
// //   const params = useParams();
// //   const pathname = usePathname();
// //   const router = useRouter();
// //  const { month, quarter, year } = params as {
// //   month?: string;
// //   quarter?: string;
// //   year?: string;
// // };
// //   const [data, setData] = useState<ApiResponse | null>(null);
// //   const [loading, setLoading] = useState<boolean>(false);
// //   const [error, setError] = useState<string>('');
// //   const authToken = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
// //   const [searchQuery, setSearchQuery] = useState<string>('');
// //   const [searchResults, setSearchResults] = useState<any[]>([]);
// //   const [showSearchResults, setShowSearchResults] = useState<boolean>(false);
// //   const [searchLoading, setSearchLoading] = useState<boolean>(false);

// //   // State for controls
// //   const [timeRange, setTimeRange] = useState<'Yearly' | 'Quarterly'>('Yearly');
// //   const selectedYear = parseInt(year as string) || new Date().getFullYear(); 
// //   const [selectedQuarter, setSelectedQuarter] = useState<string>('1');
// //   const [selectedCountries, setSelectedCountries] = useState<Record<string, boolean>>({
// //     uk: true,
// //     global: true
// //   });

// //   useEffect(() => {
// //   const scope = (countryName || "").toLowerCase();

// //   if (scope === "uk") {
// //     setSelectedCountries({ uk: true, global: false });
// //   } else if (scope === "global") {
// //     setSelectedCountries({ uk: false, global: true });
// //   }
// // }, [countryName]);



// //   // Generate years (e.g., last 5 years)
// //   const years = Array.from({ length: 5 }, (_, i) => new Date().getFullYear() - i);
// //   const quarters = [
// //     { value: '1', label: 'Q1' },
// //     { value: '2', label: 'Q2' },
// //     { value: '3', label: 'Q3' },
// //     { value: '4', label: 'Q4' }
// //   ];

// //   const handleCountryChange = (country: string) => {
// //     setSelectedCountries(prev => ({
// //       ...prev,
// //       [country]: !prev[country]
// //     }));
// //   };

// //   // Search products function
// //   const searchProducts = async (query: string) => {
// //     if (!query.trim()) {
// //       setSearchResults([]);
// //       setShowSearchResults(false);
// //       return;
// //     }

// //     setSearchLoading(true);
// //     try {
// //       const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/Product_search?query=${encodeURIComponent(query)}`, {
// //         method: 'GET',
// //         headers: {
// //           'Authorization': `Bearer ${authToken}`,
// //           'Content-Type': 'application/json'
// //         }
// //       });

// //       if (!response.ok) {
// //         throw new Error(`HTTP error! status: ${response.status}`);
// //       }

// //       const data = await response.json();
// //       setSearchResults(data.products || []);
// //       setShowSearchResults(true);
// //     } catch (err: any) {
// //       console.error('Search error:', err);
// //       setSearchResults([]);
// //       setShowSearchResults(false);
// //     } finally {
// //       setSearchLoading(false);
// //     }
// //   };

// //   // Handle search input change with debounce
// //   useEffect(() => {
// //     const timeoutId = setTimeout(() => {
// //       searchProducts(searchQuery);
// //     }, 300);

// //     return () => clearTimeout(timeoutId);
// //   }, [searchQuery]);

// //   // Currency for chart: follow the same behavior as TrendChartSection/ProductwisePerformance.
// //   // If the page scope is UK, show GBP; otherwise default to USD.
// // const pageScope = (countryName || "global").toLowerCase();

// //   const baseCurrency: "GBP" | "USD" = pageScope === "uk" ? "GBP" : "USD";

// //   const currencySymbol = baseCurrency === "GBP" ? "£" : "$";

// //   // Lowercase currency for backend keys (uk_gbp / uk_usd)
// //   const baseCurrencyLower = baseCurrency.toLowerCase() as "gbp" | "usd";

// //   // Map UI country keys (uk/global/us) to backend keys based on currency (e.g., uk_gbp vs uk_usd).
// //   // If a key is already suffixed, keep it as-is.
// // //  const backendKeyFor = (country: string) => {
// // //   const c = country.toLowerCase();
// // //   if (c.includes("_")) return c;

// // //   // UK data source generally GBP
// // //   if (c === "uk") return "uk_gbp";

// // //   // global should follow page currency
// // //   if (c === "global") return baseCurrencyLower === "gbp" ? "global_gbp" : "global_usd";

// // //   return `${c}_${baseCurrencyLower}`;
// // // };

// // const backendKeyFor = (country: string) => {
// //   const c = country.toLowerCase();

// //   // ✅ API already returns uk & us directly
// //   if (c === "uk") return "uk";
// //   if (c === "us") return "us";

// //   // Global is currency-specific
// //   if (c === "global") {
// //     return baseCurrency === "GBP" ? "global_gbp" : "global_usd";
// //   }

// //   return c;
// // };


// //   const fetchProductData = async () => {
// //     setLoading(true);
// //     setError('');

// //     try {
// //       // Get selected countries as array
// // const countries = Object.keys(selectedCountries).filter(c => selectedCountries[c]);

// // const requestPayload = {
// //   product_name: productname,
// //   time_range: timeRange,
// //   year: selectedYear,
// //   quarter: timeRange === "Quarterly" ? selectedQuarter : null,
// //   countries,               // ✅ "uk", "global" direct
// //   home_currency: baseCurrency,  // ✅ backend ko bata do kis currency me chahiye
// // };

// //       console.log('Sending request:', requestPayload);

// //       // Make API call to backend
// //       const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductwisePerformance`, {
// //         method: 'POST',
// //         headers: {
// //           'Content-Type': 'application/json',
// //           'Authorization': `Bearer ${authToken}`
// //         },
// //         body: JSON.stringify(requestPayload)
// //       });

// //       if (!response.ok) {
// //         const errorData = await response.json();
// //         throw new Error(errorData.error || `HTTP error! status: ${response.status}`);
// //       }

// //       const responseData: ApiResponse = await response.json();
// //       console.log('Received response:', responseData);

// //       if (responseData.success) {
// //         setData(responseData);
// //         console.log('Received response (pretty):', JSON.stringify(responseData, null, 2));
// //         console.log('Success:', responseData.success);
// //         console.log('Data:', responseData.data);  // if such a field exists
// //         console.log('Message:', responseData.message); // or any other known field

// //       } else {
// //         throw new Error('API returned unsuccessful response');
// //       }
// //     } catch (err: any) {
// //       console.error('API Error:', err);
// //       setError(err.message || 'Failed to fetch data from server');
// //     } finally {
// //       setLoading(false);
// //     }
// //   };

// //   const getQuarterMonths = (quarter: string) => {
// //     const quarterMap: Record<string, string[]> = {
// //       '1': ['January', 'February', 'March'],
// //       '2': ['April', 'May', 'June'],
// //       '3': ['July', 'August', 'September'],
// //       '4': ['October', 'November', 'December']
// //     };
// //     return quarterMap[quarter] || [];
// //   };

// //   useEffect(() => {
// //   fetchProductData();
// // }, [productname, year, timeRange, selectedQuarter, baseCurrency]);


// // const prepareChartData = () => {
// //   if (!data || !data.data) return [];

// //   const monthOrder = [
// //     'January','February','March','April','May','June',
// //     'July','August','September','October','November','December'
// //   ];

// //   const allMonths = new Set<string>();

// //   // 1) Collect months from ANY arrays under each country
// //   Object.values(data.data as any).forEach((countryBlock: any) => {
// //     if (!countryBlock) return;

// //     // Normalize to: list of arrays of rows
// //     const arrays: any[][] = [];

// //     if (Array.isArray(countryBlock)) {
// //       arrays.push(countryBlock);
// //     } else {
// //       // countryBlock is an object like { Yearly: [...], Quarterly: [...] }
// //       Object.values(countryBlock).forEach((maybeArr: any) => {
// //         if (Array.isArray(maybeArr)) arrays.push(maybeArr);
// //       });
// //     }

// //     arrays.forEach(rows => {
// //       rows.forEach((m: any) => {
// //         if (m?.month) allMonths.add(String(m.month));
// //       });
// //     });
// //   });

// //   const labels = Array.from(allMonths).sort(
// //     (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
// //   );

// //   if (!labels.length) return []; 

// //   const getMetric = (country: string, month: string) => {
// //   const backendKey = backendKeyFor(country);
// //   const countryBlock: any = (data.data as any)[backendKey];
// //   if (!countryBlock) return 0;

// //   let rows: any[] = [];

// //   if (Array.isArray(countryBlock)) {
// //     rows = countryBlock;
// //   } else {
// //     const preferred = countryBlock?.[timeRange];
// //     if (Array.isArray(preferred)) {
// //       rows = preferred;
// //     } else {
// //       const firstArr = Object.values(countryBlock).find(
// //         (v: any) => Array.isArray(v)
// //       ) as any[] | undefined;
// //       if (firstArr) rows = firstArr;
// //     }
// //   }

// //   const found = rows.find((m: any) => String(m.month) === String(month));
// //   return found ? Number(found.net_sales || 0) : 0;
// // };


// //   return labels.map((month) => {
// // const ukRaw = getMetric("uk", month);
// // const usRaw = getMetric("us", month);
// // const rawGlobal = getMetric("global", month);

// // // 🔑 PAGE SCOPE CHECK
// // const pageScope = (countryName || "global").toLowerCase();

// // // UK page par GLOBAL ko forcefully band
// // let globalShown = rawGlobal;

// // if (pageScope === "uk") {
// //   globalShown = 0;
// // }

// // const point: Record<string, any> = { month };

// // if (selectedCountries.uk) point.uk = ukRaw;
// // if (selectedCountries.us) point.us = usRaw;

// // // ❗ UK page par GLOBAL bilkul mat add karo
// // if (selectedCountries.global && pageScope !== "uk") {
// //   point.global = globalShown;
// // }

// // return point;
// //   });
// // };



// //   const getCountryColor = (country: string) => {
// //     const colors: Record<string, string> = {
// //       uk: '#AB64B5',
// //       us: '#87AD12',
// //       global: '#F47A00'
// //     };
// //     return colors[country] || '#ff7c7c';
// //   };

// //   const formatCurrency = (value: number) => {
// //     return new Intl.NumberFormat(baseCurrency === "GBP" ? "en-GB" : "en-US", {
// //       style: "currency",
// //       currency: baseCurrency,
// //       minimumFractionDigits: 0,
// //       maximumFractionDigits: 0,
// //     }).format(value);
// //   };


// //   const buildChartJSData = () => {
// //     const raw = prepareChartData();
// //     if (!raw || raw.length === 0) return null;

// //     const monthShort = (m: string) => {
// //   const map: Record<string, string> = {
// //     January: "Jan",
// //     February: "Feb",
// //     March: "Mar",
// //     April: "Apr",
// //     May: "May",
// //     June: "Jun",
// //     July: "Jul",
// //     August: "Aug",
// //     September: "Sep",
// //     October: "Oct",
// //     November: "Nov",
// //     December: "Dec",
// //   };
// //   return map[m] || m?.slice(0, 3);
// // };

// //     const labels = raw.map(item => monthShort(item.month));
// //   const datasets = Object.keys(selectedCountries)
// //   .filter(country => selectedCountries[country])
// // .filter((country) => {
// //   const key = backendKeyFor(country);                 // ✅ uk -> uk_gbp
// //   const block = (data?.data as any)?.[key];
// //   if (!block) return false;

// //   if (Array.isArray(block)) return block.length > 0;

// //   return Object.values(block).some(
// //     (v: any) => Array.isArray(v) && v.length > 0
// //   );
// // })
// //       .map(country => ({
// //         label: country.toUpperCase(),
// //         data: raw.map(item => item[country] || 0),
// //         borderColor: getCountryColor(country),
// //         backgroundColor: getCountryColor(country),
        
// // tension: 0.35,
// //         pointRadius: 3,
// //         // pointHoverRadius: 5,
// //         fill: false,
// //          borderDash: country === "global" ? [6, 4] : undefined,
// //   borderWidth: country === "global" ? 2.5 : 2,
// //       } as const));

// //     return { labels, datasets };
// //   };

// //   const chartJSData = buildChartJSData();

// //   const chartOptions = {
// //     responsive: true,
// //     plugins: {
// //       legend: {
// //         display: false, // as per your config
// //       },
// //       tooltip: {
// //         callbacks: {
// //           label: (context: any) => {
// //             const value = context.parsed.y;
// //             return `${context.dataset.label}: ${formatCurrency(value)}`;
// //           }
// //         }
// //       }
// //     },
// //     scales: {
// //      x: {
// //   title: { display: true, text: "Month" },
// //   ticks: {
// //     maxRotation: 0,
// //     minRotation: 0,
// //     autoSkip: true,
// //   },
// // },
// //       y: {
// //         title: {
// //           display: true,
// //           text: `Amount (${currencySymbol})`
// //         },
// //         min: 0,
// //         ticks: {
// //           padding: 0
// //         }
// //       }
// //     }
// //   };

// //   const isImprovementsPage = pathname?.includes("mprovements") || false;

// //   const scope = (countryName || "").toLowerCase();

// // const visibleCountries =
// //   scope === "uk"
// //     ? ["uk"]              // UK page: only UK option visible
// //     : scope === "global"
// //     ? ["global"]          // Global page: only Global option visible
// //     : Object.keys(selectedCountries);

// //   return (
// //     <>
// //       <style>{`
// // .net-sales-wrapper {
// //   width: 100%;
// //   margin-bottom: 15px;
// // }

// // .net-sales-content {
// //   display: flex;
// //   justify-content: space-between; /* Left and right sections */
// //   align-items: flex-start;
// //   flex-wrap: wrap;
// //   gap: 16px;
// // }

// // .net-sales-left {
// //   flex: 1 1 auto;
// // }

// // .net-sales-header {
// //   display: flex;
// //   flex-direction: column;
// //   align-items: flex-start;
// // }

// // .net-sales-title {
// //   margin: 0;
// //   font-size: 18px;
// //   font-family: 'Lato', sans-serif;
// //   color: #414042;
// //   background-color: white;
// //   border-radius: 7px;
// //   font-weight: bold;
// //   padding:0px;
// // }

// // .net-sales-subtitle {
// //   font-size: 0.875rem;
// //   color: #6b7280;
// //   margin: 0;
// //   font-style: italic;
// // }

// // .highlighted {
// //   color: #5ea68e;
// //   font-weight: 500;
// // }

// // .net-sales-right {
// //   display: flex;
// //   align-items: center;
// //   justify-content: flex-end;
// // }

// // .country-toggle-group {
// //   display: flex;
// //   flex-wrap: wrap;
// //   gap: 16px;
// //   justify-content: flex-end;
// // }


// // .highlighted {
// //   color: #5ea68e;
// //   font-weight: bold;
// // }


// // .country-toggle-group {
// //   display: flex;
// //   flex-wrap: wrap;
// //   gap: 16px;
// // }

// //   .label{
// //     font-weight: bold;
// //     }

// // .country-toggle {
// //   display: flex;
// //   align-items: center;
// //   gap: 6px;
// //   font-size: 0.9vw;
// //   color: #111827;
// //   cursor: pointer;
// //   font-weight: 600;
// //   padding: 5px 10px;
// //   border-radius: 16px;
// //   transition: all 0.2s ease-in-out;
// //   user-select: none;
// //   --country-color: #ccc; /* fallback */
// // }

// // .country-toggle input[type="checkbox"] {
// //   appearance: none;
// //   width: 13px;
// //   height: 13px;
// //   margin: 0;
// //   border: none;
// //   border-radius: 2px;
// //   background-color: var(--country-color);
// //   display: grid;
// //   place-content: center;
// //   cursor: pointer;
// //   transition: background-color 0.2s;
// //   position: relative;
// // }

// // .country-toggle input[type="checkbox"]::before {
// //   content: "✔";
// // font-size: 0.5vw;
// //   color: white;
// //   transform: scale(0);
// //   transition: transform 0.1s ease-in-out;
// // }

// // .country-toggle input[type="checkbox"]:checked {
// //   background-color: var(--country-color);
// // }

// // .country-toggle input[type="checkbox"]:checked::before {
// //   transform: scale(1);
// //   //  background-color: var(--country-color);
// // }

// // .country-label {
// //   color: var(--country-color);
// //   text-decoration: underline;
// //   text-decoration-thickness: 1.2px; /* Slightly thicker */
// //   text-underline-offset: 2px;
// //     font-size: 0.9vw;
// //     font-weight: 600;
// // }
// // .country-toggle .dot {
// //   width: 1vw;
// //   height: 1vw;
// //   border-radius: 50%;
// //   display: inline-block;
// // }

// // .country-toggle.active {
// // }

// // .country-toggle:hover {
// // }

// // /* Responsive */
// // @media (max-width: 640px) {
// //   .net-sales-content {
// //     flex-direction: column;
// //     align-items: flex-start;
// //   }

// //   .country-toggle-group {
// //     justify-content: flex-start;
// //   }
// // }




// // /* On small screens (phones), stack vertically */
// // @media (max-width: 600px) {
// //   .performance-header {
// //     flex-direction: column;
// //     align-items: flex-start;
// //   }

// //   .performance-title {
// //     width: 100%;
// //   }
// // }


// // .loading-spinner {
// //   position: absolute;
// //   right: 12px;
// //   top: 50%;
// //   transform: translateY(-50%);
// //   width: 16px;
// //   height: 16px;
// //   border: 2px solid #e5e7eb;
// //   border-top: 2px solid #3b82f6;
// //   border-radius: 50%;
// //   animation: spin 1s linear infinite;
// // }

// // @keyframes spin {
// //   to {
// //     transform: translateY(-50%) rotate(360deg);
// //   }
// // }


// // .item-name {
// //   font-weight: 600;
// //   color: #1f2937;
// // }


// // /* Wrapper ensures full left alignment and spacing */
// // .filter-wrapper {
// //   display: flex;
// //   align-items: flex-end;
// //   gap: 16px;
// //   margin-bottom: 24px;
// //   flex-wrap: wrap;
// //  align-items: center;  /* Vertical alignment */ 
// // }

// // /* Filter container styled as a table-like block */
// // .filter-container {
// //   display: flex;
// //   background: white;
// //   border: 1px solid #414042;
// //   // border-radius: 6px;
// //   box-shadow: 0 2px 5px rgba(0,0,0,0.1);
// //   overflow: hidden;
// //   max-width: 640px;
// //   flex-wrap: wrap;
// // }

// // /* Dropdown section styles */
// // .dropdown-group {
// // text-align: center;
// //   flex: 1;
// //   min-width: 100px;
// //   border-right: 1px solid #414042;
// // }

// // .dropdown-group:last-child {
// //   border-right: none;
// // }


// //   option{
// //   text-align: center;
// //   }


// // .dropdown-select:hover {
// //   background-color: #f9fafb;
// // }


// // /* Mobile responsiveness */
// // @media (max-width: 600px) {
// //   .filter-wrapper {
// //     flex-direction: column;
// //     align-items: stretch;
// //   }

// //   .filter-container {
// //     flex-direction: column;
// //     width: 100%;
// //   }

// //   .dropdown-group {
// //     border-right: none;
// //     border-bottom: 1px solid #414042;
// //   }

// //   .dropdown-group:last-child {
// //     border-bottom: none;
// //   }

// //   .fetch-button {
// //     width: 100%;
// //   }
// // }

// //         .loading-spinner {
// //           display: inline-block;
// //           width: 12px;
// //           height: 12px;
// //           border: 2px solid rgba(255,255,255,0.3);
// //           border-top: 2px solid white;
// //           border-radius: 50%;
// //           animation: spin 1s linear infinite;
// //           margin-left: 8px;
// //         }

// //         @keyframes spin {
// //           0% { transform: rotate(0deg); }
// //           100% { transform: rotate(360deg); }
// //         }

// //         @media (max-width: 768px) {
// //           .filter-container {
// //             flex-direction: column;
// //           }

// //           .dropdown-group {
// //             border-right: none;
// //             border-bottom: 1px solid #e0e0e0;
// //           }

// //           .dropdown-group:last-child {
// //             border-bottom: none;
// //           }

// //           .fetch-button {
// //             border-radius: 0 0 7px 7px;
// //             padding: 16px 24px;
// //           }
// //         }

// // h2 {
// //   font-size: 18px;
// //   font-family: 'Lato', sans-serif;
// //   color: #414042;
// //   background-color: white;
// //   border-radius: 7px;
// //   font-weight: bold;
// // }
// //     `}</style>

// //       <div className="modal-backdrop" onClick={onClose}>
// //         <div className="modal-content" onClick={(e) => e.stopPropagation()}>


// //           {/* Loading State */}
// //           {loading && (
// //            <div className="flex flex-col items-center justify-center py-12 text-center">
// //                                  <Loader fullscreen transparent />
// //                       </div>
// //           )}

// //           {/* Error State */}
// //           {error && (
// //             <div style={{
// //               backgroundColor: '#fef2f2',
// //               border: '1px solid #fecaca',
// //               borderRadius: '16px',
// //               padding: '24px',
// //               marginBottom: '32px'
// //             }}>
// //               <div style={{ display: 'flex', alignItems: 'center' }}>
// //                 <div style={{ color: '#dc2626', fontSize: '1.25rem', marginRight: '12px' }}>❌</div>
// //                 <p style={{ color: '#b91c1c', fontWeight: '500', margin: 0 }}>
// //                   {error}
// //                 </p>
// //               </div>
// //             </div>
// //           )}

// //           {/* Results */}
// //           {data && !loading && (
// //             <div style={{ display: 'flex', flexDirection: 'column', gap: '32px' }}>


// //               {/* Chart */}
// //               <div>

// //                 <div className="net-sales-wrapper">
// //                   <div className="net-sales-content">
// //                     <div className="net-sales-left">
// //                       <div className="net-sales-header">
// //                         <h3 className="net-sales-title">Net Sales Trend -
// //                           <b className="highlighted">  {productname} {" "}(
// //                     {timeRange === "Yearly"
// //                       ? `YTD ${selectedYear}`
// //                       : `Q${selectedQuarter}'${selectedYear}`})
// //                   </b></h3>                       
// //                       </div>
// //                     </div>

// //                     <div className="net-sales-right">
// //                       <div className="country-toggle-group">
                        
// // {Object.entries(selectedCountries)
// //   .filter(([country]) => visibleCountries.includes(country))
// //   .map(([country, isSelected]) => {
// //     const color = getCountryColor(country);
// //     return (
// //       <label
// //         key={country}
// //         className={`country-toggle ${isSelected ? "active" : ""}`}
// //         style={{ ['--country-color' as string]: color }}
// //       >
// //         <input
// //           type="checkbox"
// //           checked={isSelected}
// //           onChange={() => handleCountryChange(country)}
// //         />
// //         <span className="country-label">{country.toUpperCase()}</span>
// //       </label>
// //     );
// //   })}

// //                       </div>
// //                     </div>
// //                   </div>
// //                 </div>
// //                 <div style={{
// //                   height: 'auto', maxHeight: '500px', display: 'flex',
// //                   justifyContent: 'center', alignItems: 'center'
// //                 }}>

// //                   {chartJSData ? (
// //                     <Line data={chartJSData} options={chartOptions} />
// //                   ) : (
// //                     <p>No chart data available</p>
// //                   )}

// //                 </div>
// //                 {!isImprovementsPage && (
// //                   <button className="styled-button"
// //                   onClick={() =>router.push(`/skuwiseprofit/${productname}/${countryName}/${month}/${year}`)}>
// //                   Check Full Performance{" "}
// //                   <i className="fa-solid fa-arrow-up-right-from-square"></i>
// //                 </button>
// //                 )}
// //               </div>


// //             </div>
// //           )}
// //         </div>
// //       </div>


// //     </>
// //   );
// // };

// // export default Productinfoinpopup;



















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

// interface ProductDataPoint {
//   month: string; // "Jan'24"
//   net_sales: number;
// }

// interface ProductinfoinpopupProps {
//   productname?: string;
//   countryName?: string;
//   onClose?: () => void;
// }

// type CountryKey = "uk" | "global" | "us";

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

//   const [timeRange, setTimeRange] = useState<"Journey">("Journey");

//   const [selectedCountries, setSelectedCountries] = useState<Record<CountryKey, boolean>>({
//     uk: true,
//     global: true,
//     us: false,
//   });

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

//   const monthShort = (d: Date) =>
//     d.toLocaleString("en-US", { month: "short" });

//   const monthLabel = (d: Date) => `${monthShort(d)}'${String(d.getFullYear()).slice(-2)}`;

//   /**
//    * Build dummy journey data:
//    * - starts from a chosen historical month
//    * - ends at previous completed month
//    * - excludes current ongoing month
//    */
//   const buildDummyJourneyData = () => {
//     const today = new Date();

//     // latest completed month
//     const end = new Date(today.getFullYear(), today.getMonth() - 1, 1);

//     // starting month of product journey (dummy)
//     const start = new Date(2023, 0, 1); // Jan 2023

//     const months: Date[] = [];
//     const cursor = new Date(start);

//     while (cursor <= end) {
//       months.push(new Date(cursor));
//       cursor.setMonth(cursor.getMonth() + 1);
//     }

//     const makeSeries = (
//       base: number,
//       volatility: number,
//       trend: number
//     ): ProductDataPoint[] => {
//       return months.map((m, i) => {
//         const seasonal =
//           Math.sin(i / 2.5) * volatility +
//           Math.cos(i / 4.2) * (volatility * 0.45);
//         const noise = (Math.sin(i * 1.7) + 1) * volatility * 0.18;
//         const value = Math.max(0, Math.round(base + i * trend + seasonal + noise));

//         return {
//           month: monthLabel(m),
//           net_sales: value,
//         };
//       });
//     };

//     return {
//       uk: makeSeries(12000, 1800, 240),
//       global: makeSeries(26000, 2600, 420),
//       us: makeSeries(10000, 1500, 210),
//     };
//   };

//   const dummyData = useMemo(() => buildDummyJourneyData(), []);

//   const visibleCountries: CountryKey[] =
//     scope === "uk"
//       ? ["uk"]
//       : scope === "global"
//         ? ["global"]
//         : scope === "us"
//           ? ["us"]
//           : ["uk", "global", "us"];

//   const allLabels = useMemo(() => {
//     const firstSelected =
//       (Object.keys(selectedCountries) as CountryKey[]).find((k) => selectedCountries[k]) || "global";
//     return dummyData[firstSelected]?.map((d) => d.month) || [];
//   }, [dummyData, selectedCountries]);

//   const chartJSData = useMemo(() => {
//     const labels = allLabels;

//     const datasets = (Object.keys(selectedCountries) as CountryKey[])
//       .filter((country) => visibleCountries.includes(country))
//       .filter((country) => selectedCountries[country])
//       .map((country) => ({
//         label: country.toUpperCase(),
//         data: dummyData[country].map((d) => d.net_sales),
//         borderColor: getCountryColor(country),
//         backgroundColor: getCountryColor(country),
//         tension: 0.35,
//         pointRadius: 3,
//         pointHoverRadius: 5,
//         fill: false,
//         borderDash: country === "global" ? [6, 4] : undefined,
//         borderWidth: country === "global" ? 2.5 : 2,
//       }));

//     return { labels, datasets };
//   }, [allLabels, dummyData, selectedCountries, visibleCountries]);

//   // show exactly 12 months initially
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
//             display: true,
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
//             text: `Amount (${currencySymbol})`,
//           },
//           min: 0,
//           ticks: {
//             padding: 0,
//             callback: (value: number) => formatCurrency(value),
//           },
//         },
//       },
//     }),
//     [allLabels.length, currencySymbol, initialMaxIndex, initialMinIndex]
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
//                 <div style={{ color: "#dc2626", fontSize: "1.25rem", marginRight: "12px" }}>❌</div>
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
//                           <b className="highlighted">{productname} (last completed months only)</b>
//                         </h3>
//                         <p className="text-xs text-gray-500 mt-1">
//                           Drag horizontally on the graph to explore older or newer months. Viewport shows 12 months at a time.
//                         </p>
//                       </div>
//                     </div>

//                     <div className="net-sales-right">
//                       <div className="country-toggle-group">
//                         {(Object.keys(selectedCountries) as CountryKey[])
//                           .filter((country) => visibleCountries.includes(country))
//                           .map((country) => {
//                             const color = getCountryColor(country);
//                             const isSelected = selectedCountries[country];

//                             return (
//                               <label
//                                 key={country}
//                                 className={`country-toggle ${isSelected ? "active" : ""}`}
//                                 style={{ ["--country-color" as string]: color }}
//                               >
//                                 <input
//                                   type="checkbox"
//                                   checked={isSelected}
//                                   onChange={() => handleCountryChange(country)}
//                                 />
//                                 <span className="country-label">{country.toUpperCase()}</span>
//                               </label>
//                             );
//                           })}
//                       </div>
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

interface ProductDataPoint {
  month: string; // "Jan'24"
  net_sales: number;
}

interface ApiMonthRow {
  month: string; // January, February...
  net_sales: number;
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

type CountryKey = "uk" | "global" | "us";

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

  const [selectedCountries, setSelectedCountries] = useState<Record<CountryKey, boolean>>({
    uk: true,
    global: true,
    us: false,
  });

  const [journeyData, setJourneyData] = useState<Record<CountryKey, ProductDataPoint[]>>({
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

  const handleCountryChange = (country: CountryKey) => {
    setSelectedCountries((prev) => ({
      ...prev,
      [country]: !prev[country],
    }));
  };

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

      // change this if your product history starts earlier
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
            } catch {
              // ignore json parse failure
            }
            throw new Error(msg);
          }

          const json: ApiResponse = await response.json();
          return { year: yr, json };
        })
      );

      const todayEnd = new Date(today.getFullYear(), today.getMonth() - 1, 1);
      const startDate = new Date(START_YEAR, 0, 1);

      const valueMaps: Record<CountryKey, Map<string, number>> = {
        uk: new Map(),
        global: new Map(),
        us: new Map(),
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

            // do not include current ongoing month / future
            if (date > todayEnd) return;

            const label = monthLabel(date);
            valueMaps[country].set(label, Number(row.net_sales || 0));
          });
        });
      }

      // build full month range and fill missing months with 0
      const months: Date[] = [];
      const cursor = new Date(startDate);

      while (cursor <= todayEnd) {
        months.push(new Date(cursor));
        cursor.setMonth(cursor.getMonth() + 1);
      }

      const finalData: Record<CountryKey, ProductDataPoint[]> = {
        uk: months.map((m) => {
          const label = monthLabel(m);
          return {
            month: label,
            net_sales: valueMaps.uk.get(label) ?? 0,
          };
        }),
        global: months.map((m) => {
          const label = monthLabel(m);
          return {
            month: label,
            net_sales: valueMaps.global.get(label) ?? 0,
          };
        }),
        us: months.map((m) => {
          const label = monthLabel(m);
          return {
            month: label,
            net_sales: valueMaps.us.get(label) ?? 0,
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

  const allLabels = useMemo(() => {
    const firstSelected =
      (Object.keys(selectedCountries) as CountryKey[]).find(
        (k) => selectedCountries[k]
      ) || "global";

    return journeyData[firstSelected]?.map((d) => d.month) || [];
  }, [journeyData, selectedCountries]);

  const chartJSData = useMemo(() => {
    const labels = allLabels;

    const datasets = (Object.keys(selectedCountries) as CountryKey[])
      .filter((country) => visibleCountries.includes(country))
      .filter((country) => selectedCountries[country])
      .map((country) => ({
        label: country.toUpperCase(),
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
        borderDash: country === "global" ? [6, 4] : undefined,
        borderWidth: country === "global" ? 2.5 : 2,
      }));

    return { labels, datasets };
  }, [allLabels, journeyData, selectedCountries, visibleCountries]);

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
            display: true,
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
            text: `Amount (${currencySymbol})`,
          },
          min: 0,
          ticks: {
            padding: 0,
            callback: (value: number) => formatCurrency(value),
          },
        },
      },
    }),
    [allLabels.length, currencySymbol, initialMaxIndex, initialMinIndex]
  );

  const isImprovementsPage = pathname?.includes("mprovements") || false;

  return (
    <>
      <style>{`
        .net-sales-wrapper {
          width: 100%;
          margin-bottom: 15px;
        }

        .net-sales-content {
          display: flex;
          justify-content: space-between;
          align-items: flex-start;
          flex-wrap: wrap;
          gap: 16px;
        }

        .net-sales-left {
          flex: 1 1 auto;
        }

        .net-sales-header {
          display: flex;
          flex-direction: column;
          align-items: flex-start;
        }

        .net-sales-title {
          margin: 0;
          font-size: 18px;
          font-family: 'Lato', sans-serif;
          color: #414042;
          background-color: white;
          border-radius: 7px;
          font-weight: bold;
          padding: 0;
        }

        .highlighted {
          color: #5ea68e;
          font-weight: 500;
        }

        .net-sales-right {
          display: flex;
          align-items: center;
          justify-content: flex-end;
        }

        .country-toggle-group {
          display: flex;
          flex-wrap: wrap;
          gap: 16px;
          justify-content: flex-end;
        }

        .country-toggle {
          display: flex;
          align-items: center;
          gap: 6px;
          font-size: 0.9vw;
          color: #111827;
          cursor: pointer;
          font-weight: 600;
          padding: 5px 10px;
          border-radius: 16px;
          transition: all 0.2s ease-in-out;
          user-select: none;
          --country-color: #ccc;
        }

        .country-toggle input[type="checkbox"] {
          appearance: none;
          width: 13px;
          height: 13px;
          margin: 0;
          border: none;
          border-radius: 2px;
          background-color: var(--country-color);
          display: grid;
          place-content: center;
          cursor: pointer;
          transition: background-color 0.2s;
          position: relative;
        }

        .country-toggle input[type="checkbox"]::before {
          content: "✔";
          font-size: 0.5vw;
          color: white;
          transform: scale(0);
          transition: transform 0.1s ease-in-out;
        }

        .country-toggle input[type="checkbox"]:checked::before {
          transform: scale(1);
        }

        .country-label {
          color: var(--country-color);
          text-decoration: underline;
          text-decoration-thickness: 1.2px;
          text-underline-offset: 2px;
          font-size: 0.9vw;
          font-weight: 600;
        }

        .styled-button {
          padding: 8px 16px;
          font-size: .9rem;
          border: none;
          border-radius: 6px;
          cursor: pointer;
          transition: background-color .2s ease;
          box-shadow: 0 3px 6px rgba(0,0,0,.15);
          background-color: #2c3e50;
          color: #f8edcf;
          font-weight: bold;
        }

        .styled-button:hover {
          background-color: #1f2a36;
        }
      `}</style>

      <div className="modal-backdrop" onClick={onClose}>
        <div className="modal-content" onClick={(e) => e.stopPropagation()}>
          {loading && (
            <div className="flex flex-col items-center justify-center py-12 text-center">
              <Loader fullscreen transparent />
            </div>
          )}

          {error && (
            <div
              style={{
                backgroundColor: "#fef2f2",
                border: "1px solid #fecaca",
                borderRadius: "16px",
                padding: "24px",
                marginBottom: "32px",
              }}
            >
              <div style={{ display: "flex", alignItems: "center" }}>
                <div style={{ color: "#dc2626", fontSize: "1.25rem", marginRight: "12px" }}>
                  ❌
                </div>
                <p style={{ color: "#b91c1c", fontWeight: "500", margin: 0 }}>{error}</p>
              </div>
            </div>
          )}

          {!loading && !error && (
            <div style={{ display: "flex", flexDirection: "column", gap: "32px" }}>
              <div>
                <div className="net-sales-wrapper">
                  <div className="net-sales-content">
                    <div className="net-sales-left">
                      <div className="net-sales-header">
                        <h3 className="net-sales-title">
                          Product Journey -{" "}
                          <b className="highlighted">
                            {productname} (last completed months only)
                          </b>
                        </h3>
                        <p className="text-xs text-gray-500 mt-1">
                          Drag horizontally on the graph to explore older or newer months.
                          Viewport shows 12 months at a time.
                        </p>
                      </div>
                    </div>

                    <div className="net-sales-right">
                      <div className="country-toggle-group">
                        {(Object.keys(selectedCountries) as CountryKey[])
                          .filter((country) => visibleCountries.includes(country))
                          .map((country) => {
                            const color = getCountryColor(country);
                            const isSelected = selectedCountries[country];

                            return (
                              <label
                                key={country}
                                className={`country-toggle ${isSelected ? "active" : ""}`}
                                style={{ ["--country-color" as string]: color }}
                              >
                                <input
                                  type="checkbox"
                                  checked={isSelected}
                                  onChange={() => handleCountryChange(country)}
                                />
                                <span className="country-label">{country.toUpperCase()}</span>
                              </label>
                            );
                          })}
                      </div>
                    </div>
                  </div>
                </div>

                <div
                  style={{
                    height: "380px",
                    maxHeight: "500px",
                    display: "flex",
                    justifyContent: "center",
                    alignItems: "center",
                  }}
                >
                  {chartJSData?.labels?.length ? (
                    <Line data={chartJSData} options={chartOptions} />
                  ) : (
                    <p>No chart data available</p>
                  )}
                </div>

                {!isImprovementsPage && (
                  <button
                    className="styled-button"
                    onClick={() =>
                      router.push(`/skuwiseprofit/${productname}/${countryName}/${month}/${year}`)
                    }
                  >
                    Check Full Performance{" "}
                    <i className="fa-solid fa-arrow-up-right-from-square"></i>
                  </button>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
};

export default Productinfoinpopup;