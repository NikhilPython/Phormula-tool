// // // // // // "use client";

// // // // // // import React, { useEffect, useMemo, useState } from "react";
// // // // // // import { useParams, useRouter } from "next/navigation";
// // // // // // import dynamic from "next/dynamic";
// // // // // // import "@/lib/chartSetup";
// // // // // // import {
// // // // // //   Chart as ChartJS,
// // // // // //   CategoryScale,
// // // // // //   LinearScale,
// // // // // //   PointElement,
// // // // // //   LineElement,
// // // // // //   Title as ChartTitle,
// // // // // //   Tooltip,
// // // // // //   Legend,
// // // // // //   Filler,
// // // // // // } from "chart.js";
// // // // // // import PageBreadcrumb from "@/components/common/PageBreadCrumb";
// // // // // // import PeriodFiltersTable from "@/components/filters/PeriodFiltersTable";
// // // // // // import ProductSearchDropdown from "@/components/products/ProductSearchDropdown";
// // // // // // import Loader from "@/components/loader/Loader";
// // // // // // import DownloadIconButton from "@/components/ui/button/DownloadIconButton";

// // // // // // const Line = dynamic(() => import("react-chartjs-2").then((m) => m.Line), {
// // // // // //   ssr: false,
// // // // // // });

// // // // // // ChartJS.register(
// // // // // //   CategoryScale,
// // // // // //   LinearScale,
// // // // // //   PointElement,
// // // // // //   LineElement,
// // // // // //   ChartTitle,
// // // // // //   Tooltip,
// // // // // //   Legend,
// // // // // //   Filler
// // // // // // );

// // // // // // type CountryKey = "uk" | "us" | "global" | string;
// // // // // // type Range = "monthly" | "quarterly" | "yearly";

// // // // // // type MonthDatum = {
// // // // // //   month: string;
// // // // // //   net_sales: number;
// // // // // //   quantity: number;
// // // // // //   profit: number;
// // // // // // };

// // // // // // type APIResponse = {
// // // // // //   success: boolean;
// // // // // //   message?: string;
// // // // // //   data: Record<CountryKey, MonthDatum[]>;
// // // // // // };

// // // // // // interface ProductwisePerformanceProps {
// // // // // //   productname?: string;
// // // // // // }

// // // // // // // ----------------------
// // // // // // // Slug helper functions
// // // // // // // ----------------------
// // // // // // const toSlug = (name: string) =>
// // // // // //   name
// // // // // //     .trim()
// // // // // //     .toLowerCase()
// // // // // //     .replace(/\s*\+\s*/g, " plus ")
// // // // // //     .replace(/\s+/g, "-");

// // // // // // const fromSlug = (slug: string) =>
// // // // // //   slug
// // // // // //     .replace(/-/g, " ")
// // // // // //     .replace(/\bplus\b/gi, "+")
// // // // // //     .replace(/\s+/g, " ")
// // // // // //     .trim();

// // // // // // const normalizeProductSlug = (slug?: string) => {
// // // // // //   if (!slug) return undefined;

// // // // // //   try {
// // // // // //     const decoded = decodeURIComponent(slug);
// // // // // //     if (!decoded.trim()) return undefined;
// // // // // //     return fromSlug(decoded);
// // // // // //   } catch {
// // // // // //     if (!slug.trim()) return undefined;
// // // // // //     return fromSlug(slug);
// // // // // //   }
// // // // // // };

// // // // // // const formatCountryLabel = (country: string) => {
// // // // // //   const lower = country.toLowerCase();
// // // // // //   if (lower === "global") return "Global"; // special case
// // // // // //   return country.toUpperCase(); // UK, US, etc.
// // // // // // };


// // // // // // // ----------------------
// // // // // // // Currency helpers
// // // // // // // ----------------------
// // // // // // const GBP_TO_USD_RATE = 1.27;

// // // // // // const formatUSD = (value: number) =>
// // // // // //   new Intl.NumberFormat("en-US", {
// // // // // //     style: "currency",
// // // // // //     currency: "USD",
// // // // // //     minimumFractionDigits: 0,
// // // // // //     maximumFractionDigits: 0,
// // // // // //   }).format(value);

// // // // // // const ProductwisePerformance: React.FC<ProductwisePerformanceProps> = ({
// // // // // //   productname: propProductName,
// // // // // // }) => {
// // // // // //   const params = useParams();
// // // // // //   const router = useRouter();

// // // // // //   const rawSlug = params?.productname as string | undefined;
// // // // // //   const urlProductName = normalizeProductSlug(rawSlug);

// // // // // //   const countryName = (params?.countryName as string) || undefined;
// // // // // //   const monthParam = (params?.month as string) || undefined;
// // // // // //   const yearParam = (params?.year as string) || undefined;

// // // // // //   const productname = propProductName || urlProductName || "";

// // // // // //   const [data, setData] = useState<APIResponse | null>(null);
// // // // // //   const [loading, setLoading] = useState(false);
// // // // // //   const [error, setError] = useState<string>("");

// // // // // //   const authToken =
// // // // // //     typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

// // // // // //   const getCurrencySymbol = (country?: string) => {
// // // // // //     if (!country) return "¤";
// // // // // //     switch (country.toLowerCase()) {
// // // // // //       case "uk":
// // // // // //         return "£";
// // // // // //       case "india":
// // // // // //         return "₹";
// // // // // //       case "us":
// // // // // //         return "$";
// // // // // //       case "europe":
// // // // // //       case "eu":
// // // // // //         return "€";
// // // // // //       case "global":
// // // // // //         return "$";
// // // // // //       default:
// // // // // //         return "¤";
// // // // // //     }
// // // // // //   };

// // // // // //   const currencySymbol = countryName ? getCurrencySymbol(countryName) : "¤";

// // // // // //   // -------------------------
// // // // // //   // Controls State
// // // // // //   // -------------------------
// // // // // //   const [range, setRange] = useState<Range>();

// // // // // //   // const initialYear = useMemo(() => new Date().getFullYear(), []);
// // // // // //   // const [selectedYear, setSelectedYear] = useState<number>(
// // // // // //   //   yearParam ? Number(yearParam) : initialYear
// // // // // //   // );

// // // // // //   // allow "no year selected" by default
// // // // // //   const [selectedYear, setSelectedYear] = useState<number | "">("");


// // // // // //   const [selectedMonth, setSelectedMonth] = useState<string>(monthParam || "");
// // // // // //   const [selectedQuarter, setSelectedQuarter] = useState("1");

// // // // // //   // UI toggle state for chart lines only
// // // // // //   const [selectedCountries, setSelectedCountries] = useState<
// // // // // //     Record<CountryKey, boolean>
// // // // // //   >({
// // // // // //     uk: true,
// // // // // //     us: true,
// // // // // //     global: true,
// // // // // //   });

// // // // // //   const years = useMemo(
// // // // // //     () => Array.from({ length: 2 }, (_, i) => new Date().getFullYear() - i),
// // // // // //     []
// // // // // //   );

// // // // // //   // allow toggling any country, including global (chart only)
// // // // // //   const handleCountryChange = (country: CountryKey) => {
// // // // // //     setSelectedCountries((prev) => ({
// // // // // //       ...prev,
// // // // // //       [country]: !(prev[country] ?? true),
// // // // // //     }));
// // // // // //   };

// // // // // //   // -------------------------
// // // // // //   // Product select handler
// // // // // //   // -------------------------
// // // // // //   const handleProductSelect = (productName: string) => {
// // // // // //     const base = "/productwiseperformance";
// // // // // //     const slug = toSlug(productName);

// // // // // //     const to = `${base}/${slug}/${countryName ?? ""}/${selectedMonth ?? ""}/${selectedYear ?? ""}`;
// // // // // //     router.push(to);
// // // // // //   };

// // // // // //   // -------------------------
// // // // // //   // Conditions for showing data / calling API
// // // // // //   // -------------------------
// // // // // //   const isProductSelected = !!productname;

// // // // // //   const hasYear = selectedYear !== "" && selectedYear !== undefined;

// // // // // //   const isPeriodComplete =
// // // // // //     (range === "yearly" && hasYear) ||
// // // // // //     (range === "quarterly" && hasYear && !!selectedQuarter) ||
// // // // // //     (range === "monthly" && hasYear && !!selectedMonth);

// // // // // //   const canShowResults = isProductSelected && isPeriodComplete;

// // // // // //   // -------------------------
// // // // // //   // Fetch Product Data
// // // // // //   // -------------------------
// // // // // //   const fetchProductData = async () => {
// // // // // //     if (!canShowResults) return;

// // // // // //     setLoading(true);
// // // // // //     setError("");
// // // // // //     try {
// // // // // //       // toggles should NOT affect backend; ask for all
// // // // // //       const countries: CountryKey[] = ["global", "uk", "us"];

// // // // // //       const backendTimeRange =
// // // // // //         range === "yearly"
// // // // // //           ? "Yearly"
// // // // // //           : range === "quarterly"
// // // // // //             ? "Quarterly"
// // // // // //             : "Monthly";

// // // // // //       const payload: any = {
// // // // // //         product_name: productname,
// // // // // //         time_range: backendTimeRange,
// // // // // //         year: selectedYear,
// // // // // //         countries,
// // // // // //       };

// // // // // //       if (range === "quarterly") {
// // // // // //         payload.quarter = selectedQuarter;
// // // // // //       }

// // // // // //       if (range === "monthly") {
// // // // // //         payload.month = selectedMonth;
// // // // // //       }

// // // // // //       const res = await fetch("http://localhost:5000/ProductwisePerformance", {
// // // // // //         method: "POST",
// // // // // //         headers: {
// // // // // //           "Content-Type": "application/json",
// // // // // //           Authorization: `Bearer ${authToken ?? ""}`,
// // // // // //         },
// // // // // //         body: JSON.stringify(payload),
// // // // // //       });

// // // // // //       const json: APIResponse | any = await res.json().catch(() => null);

// // // // // //       if (!res.ok || !json?.success) {
// // // // // //         const errMsg =
// // // // // //           (json && (json.error || json.message)) ||
// // // // // //           `HTTP error! status: ${res.status}`;
// // // // // //         throw new Error(errMsg);
// // // // // //       }

// // // // // //       setData(json as APIResponse);
// // // // // //     } catch (e: any) {
// // // // // //       console.error("API Error:", e);
// // // // // //       setError(e?.message || "Failed to fetch data from server");
// // // // // //     } finally {
// // // // // //       setLoading(false);
// // // // // //     }
// // // // // //   };

// // // // // //   useEffect(() => {
// // // // // //     if (!canShowResults) return;
// // // // // //     fetchProductData();
// // // // // //     // eslint-disable-next-line react-hooks/exhaustive-deps
// // // // // //   }, [productname, selectedYear, range, selectedQuarter, selectedMonth, canShowResults]);

// // // // // //   // -------------------------
// // // // // //   // Helpers for Chart Data
// // // // // //   // -------------------------
// // // // // //   const monthOrder = [
// // // // // //     "January",
// // // // // //     "February",
// // // // // //     "March",
// // // // // //     "April",
// // // // // //     "May",
// // // // // //     "June",
// // // // // //     "July",
// // // // // //     "August",
// // // // // //     "September",
// // // // // //     "October",
// // // // // //     "November",
// // // // // //     "December",
// // // // // //   ];

// // // // // //   const getCurrencyByCountry = (country: string, value: number) => {
// // // // // //     const lower = country.toLowerCase();

// // // // // //     if (lower === "uk") {
// // // // // //       return new Intl.NumberFormat("en-GB", {
// // // // // //         style: "currency",
// // // // // //         currency: "GBP",
// // // // // //         minimumFractionDigits: 0,
// // // // // //         maximumFractionDigits: 0,
// // // // // //       }).format(value);
// // // // // //     }

// // // // // //     return new Intl.NumberFormat("en-US", {
// // // // // //       style: "currency",
// // // // // //       currency: "USD",
// // // // // //       minimumFractionDigits: 0,
// // // // // //       maximumFractionDigits: 0,
// // // // // //     }).format(value);
// // // // // //   };

// // // // // //   const prepareProfitData = () => {
// // // // // //     if (!data?.data) return [] as any[];

// // // // // //     const allMonths = new Set<string>();
// // // // // //     Object.values(data.data).forEach((countryData) => {
// // // // // //       countryData.forEach((m) => allMonths.add(m.month));
// // // // // //     });

// // // // // //     const sortedMonths = Array.from(allMonths).sort(
// // // // // //       (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
// // // // // //     );

// // // // // //     const profitData: any[] = [];
// // // // // //     sortedMonths.forEach((m) => {
// // // // // //       const point: Record<string, any> = { month: m };
// // // // // //       Object.entries(data.data).forEach(([country, cd]) => {
// // // // // //         const md = cd.find((d) => d.month === m);
// // // // // //         point[country] = md ? md.profit : 0;
// // // // // //       });
// // // // // //       profitData.push(point);
// // // // // //     });

// // // // // //     return profitData;
// // // // // //   };

// // // // // //   const prepareChartData = () => {
// // // // // //     if (!data?.data)
// // // // // //       return { netSalesData: [] as any[], quantityData: [] as any[] };

// // // // // //     const allMonths = new Set<string>();
// // // // // //     Object.values(data.data).forEach((countryData) => {
// // // // // //       countryData.forEach((m) => allMonths.add(m.month));
// // // // // //     });

// // // // // //     const sortedMonths = Array.from(allMonths).sort(
// // // // // //       (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
// // // // // //     );

// // // // // //     const netSalesData: any[] = [];
// // // // // //     const quantityData: any[] = [];

// // // // // //     sortedMonths.forEach((m) => {
// // // // // //       const netSalesPoint: Record<string, any> = { month: m };
// // // // // //       const quantityPoint: Record<string, any> = { month: m };

// // // // // //       Object.entries(data.data).forEach(([country, cd]) => {
// // // // // //         const md = cd.find((d) => d.month === m);
// // // // // //         netSalesPoint[country] = md ? md.net_sales : 0;
// // // // // //         quantityPoint[country] = md ? md.quantity : 0;
// // // // // //       });

// // // // // //       netSalesData.push(netSalesPoint);
// // // // // //       quantityData.push(quantityPoint);
// // // // // //     });

// // // // // //     return { netSalesData, quantityData };
// // // // // //   };

// // // // // //   const getCountryColor = (country: CountryKey) => {
// // // // // //     const colors: Record<string, string> = {
// // // // // //       uk: "#AB64B5",
// // // // // //       us: "#87AD12",
// // // // // //       global: "#F47A00",
// // // // // //     };
// // // // // //     return colors[country] || "#ff7c7c";
// // // // // //   };

// // // // // //   const formatCurrencyByCountry = (country: string, value: number) => {
// // // // // //     return getCurrencyByCountry(country, value);
// // // // // //   };

// // // // // //   // Countries that actually have some non-zero data (excluding purely empty/zero series),
// // // // // //   // excluding global which we treat separately.
// // // // // //   const nonEmptyCountriesFromApi = useMemo(() => {
// // // // // //     if (!data?.data) return [] as CountryKey[];

// // // // // //     return (Object.entries(data.data) as [CountryKey, MonthDatum[]][])
// // // // // //       .filter(([country, rows]) => {
// // // // // //         if (country.toLowerCase() === "global") return false;
// // // // // //         return rows.some(
// // // // // //           (m) =>
// // // // // //             m.net_sales !== 0 ||
// // // // // //             m.quantity !== 0 ||
// // // // // //             m.profit !== 0
// // // // // //         );
// // // // // //       })
// // // // // //       .map(([country]) => country);
// // // // // //   }, [data]);

// // // // // //   // -------------------------
// // // // // //   // Chart datasets (GLOBAL uses UK→USD + US, with fallback to backend global)
// // // // // //   // -------------------------
// // // // // //   const buildChartJSData = () => {
// // // // // //     if (!data?.data) return [null, null, null];

// // // // // //     const allMonths = new Set<string>();
// // // // // //     Object.values(data.data).forEach((countryData) => {
// // // // // //       countryData.forEach((m) => allMonths.add(m.month));
// // // // // //     });

// // // // // //     const labels = Array.from(allMonths).sort(
// // // // // //       (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
// // // // // //     );

// // // // // //     const getMetric = (
// // // // // //       country: CountryKey,
// // // // // //       month: string,
// // // // // //       metric: keyof MonthDatum
// // // // // //     ) => {
// // // // // //       const arr = data.data[country];
// // // // // //       if (!arr) return 0;
// // // // // //       const found = arr.find((m) => m.month === month);
// // // // // //       return found ? (found[metric] as number) : 0;
// // // // // //     };

// // // // // //     const makeDataset = (
// // // // // //       country: string,
// // // // // //       metric: keyof MonthDatum,
// // // // // //       labelSuffix: string
// // // // // //     ) => {
// // // // // //       const lower = country.toLowerCase();

// // // // // //       const dataSeries = labels.map((month) => {
// // // // // //         const ukVal = getMetric("uk", month, metric);
// // // // // //         const usVal = getMetric("us", month, metric);
// // // // // //         const rawGlobal = getMetric("global", month, metric);
// // // // // //         const isMoney = metric === "net_sales" || metric === "profit";

// // // // // //         if (lower === "global") {
// // // // // //           if (metric === "quantity") {
// // // // // //             const sumUnits = ukVal + usVal;
// // // // // //             return sumUnits !== 0 ? sumUnits : rawGlobal;
// // // // // //           }
// // // // // //           const sumMoney = ukVal * GBP_TO_USD_RATE + usVal;
// // // // // //           if (sumMoney !== 0) return sumMoney;
// // // // // //           return rawGlobal;
// // // // // //         }

// // // // // //         if (lower === "uk") {
// // // // // //           if (metric === "quantity") return ukVal;
// // // // // //           return ukVal * GBP_TO_USD_RATE;
// // // // // //         }

// // // // // //         if (lower === "us") {
// // // // // //           return usVal;
// // // // // //         }

// // // // // //         const raw = getMetric(country as CountryKey, month, metric);
// // // // // //         if (!isMoney) return raw;
// // // // // //         return raw;
// // // // // //       });

// // // // // //       const isGlobal = lower === "global";

// // // // // //       return {
// // // // // //         label: `${formatCountryLabel(country)} ${labelSuffix}`,
// // // // // //         data: dataSeries,
// // // // // //         borderColor: getCountryColor(country),
// // // // // //         backgroundColor: getCountryColor(country),
// // // // // //         tension: 0.1,
// // // // // //         pointRadius: 3,
// // // // // //         fill: false,
// // // // // //         borderDash: isGlobal ? [6, 4] : [],
// // // // // //         borderWidth: 2,
// // // // // //         order: isGlobal ? 99 : 0,
// // // // // //       };
// // // // // //     };

// // // // // //     const metrics: { metric: keyof MonthDatum; suffix: string }[] = [
// // // // // //       { metric: "net_sales", suffix: "Net Sales" },
// // // // // //       { metric: "quantity", suffix: "Quantity" },
// // // // // //       { metric: "profit", suffix: "Profit" },
// // // // // //     ];

// // // // // //     const charts = metrics.map(({ metric, suffix }) => {
// // // // // //       // visible countries for chart: obey toggles, including global
// // // // // //       const visibleCountries: CountryKey[] = [];

// // // // // //       if (selectedCountries["global"] ?? true) {
// // // // // //         visibleCountries.push("global");
// // // // // //       }

// // // // // //       visibleCountries.push(
// // // // // //         ...nonEmptyCountriesFromApi.filter(
// // // // // //           (c) => selectedCountries[c] ?? true
// // // // // //         )
// // // // // //       );

// // // // // //       const datasets = visibleCountries.map((country) =>
// // // // // //         makeDataset(country, metric, suffix)
// // // // // //       );

// // // // // //       return {
// // // // // //         labels,
// // // // // //         datasets,
// // // // // //       };
// // // // // //     });

// // // // // //     return charts;
// // // // // //   };

// // // // // //   const chartDataList = buildChartJSData();

// // // // // //   const chartOptions = {
// // // // // //     responsive: true,
// // // // // //     plugins: {
// // // // // //       legend: { display: false },
// // // // // //       tooltip: {
// // // // // //         callbacks: {
// // // // // //           label: (context: any) => {
// // // // // //             const value = context.parsed.y as number;
// // // // // //             const datasetLabel = context.dataset.label as string;
// // // // // //             const metricPart = (
// // // // // //               datasetLabel
// // // // // //                 .split(" ")
// // // // // //                 .slice(1)
// // // // // //                 .join(" ") || ""
// // // // // //             ).toLowerCase();

// // // // // //             if (
// // // // // //               metricPart.includes("quantity") ||
// // // // // //               metricPart.includes("units")
// // // // // //             ) {
// // // // // //               return `${datasetLabel}: ${value.toLocaleString()}`;
// // // // // //             }

// // // // // //             return `${datasetLabel}: ${formatUSD(value)}`;
// // // // // //           },
// // // // // //         },
// // // // // //       },
// // // // // //     },
// // // // // //     scales: {
// // // // // //       x: { title: { display: true, text: "Month" } },
// // // // // //       y: {
// // // // // //         title: { display: true, text: "Amount ($)" },
// // // // // //         min: 0,
// // // // // //         ticks: {
// // // // // //           padding: 0,
// // // // // //         },
// // // // // //       },
// // // // // //     },
// // // // // //   } as const;

// // // // // //   const [currentIndex, setCurrentIndex] = useState(0);
// // // // // //   const handlePrev = () =>
// // // // // //     setCurrentIndex((i) => (i === 0 ? chartDataList.length - 1 : i - 1));
// // // // // //   const handleNext = () =>
// // // // // //     setCurrentIndex((i) => (i === chartDataList.length - 1 ? 0 : i + 1));

// // // // // //   // const yearShort = selectedYear.toString().slice(-2);

// // // // // //   const yearShort =
// // // // // //     selectedYear === "" ? "" : selectedYear.toString().slice(-2);

// // // // // //   const cap = (s: string) => (s ? s[0].toUpperCase() + s.slice(1) : "");

// // // // // //   const getTitle = () => {
// // // // // //     if (range === "yearly") return `Year'${yearShort}`;
// // // // // //     if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
// // // // // //     return selectedMonth ? `${cap(selectedMonth)}'${yearShort}` : `Year'${yearShort}`;
// // // // // //   };

// // // // // //   const getHeadingPeriod = () => {
// // // // // //     // For the heading: Yearly -> YTD'25, others as-is
// // // // // //     if (range === "yearly") return `YTD'${yearShort}`;
// // // // // //     if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
// // // // // //     if (range === "monthly" && selectedMonth) {
// // // // // //       return `${cap(selectedMonth)}'${yearShort}`;
// // // // // //     }
// // // // // //     return "";
// // // // // //   };


// // // // // //   // -------------------------
// // // // // //   // Cards (GLOBAL uses same conversion + fallback as chart)
// // // // // //   // -------------------------
// // // // // //   const cards = useMemo(() => {
// // // // // //     if (!data?.data) return [] as { country: string; stats: any }[];

// // // // // //     const allMonths = new Set<string>();
// // // // // //     Object.values(data.data).forEach((countryData) => {
// // // // // //       countryData.forEach((m) => allMonths.add(m.month));
// // // // // //     });

// // // // // //     const labels = Array.from(allMonths).sort(
// // // // // //       (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
// // // // // //     );

// // // // // //     return Object.entries(data.data).map(([country, countryData]) => {
// // // // // //       let processedData = countryData;

// // // // // //       if (country.toLowerCase() === "global") {
// // // // // //         processedData = labels.map((month) => {
// // // // // //           const uk = data.data.uk?.find((m) => m.month === month);
// // // // // //           const us = data.data.us?.find((m) => m.month === month);
// // // // // //           const g = data.data.global?.find((m) => m.month === month);

// // // // // //           const ukNetUSD = (uk?.net_sales || 0) * GBP_TO_USD_RATE;
// // // // // //           const usNet = us?.net_sales || 0;
// // // // // //           const ukProfitUSD = (uk?.profit || 0) * GBP_TO_USD_RATE;
// // // // // //           const usProfit = us?.profit || 0;

// // // // // //           const sumNetSales = ukNetUSD + usNet;
// // // // // //           const sumProfit = ukProfitUSD + usProfit;
// // // // // //           const sumUnits = (uk?.quantity || 0) + (us?.quantity || 0);

// // // // // //           return {
// // // // // //             month,
// // // // // //             net_sales: sumNetSales !== 0 ? sumNetSales : g?.net_sales || 0,
// // // // // //             profit: sumProfit !== 0 ? sumProfit : g?.profit || 0,
// // // // // //             quantity: sumUnits !== 0 ? sumUnits : g?.quantity || 0,
// // // // // //           };
// // // // // //         });
// // // // // //       }

// // // // // //       const totalSales = processedData.reduce((s, m) => s + m.net_sales, 0);
// // // // // //       const totalProfit = processedData.reduce((s, m) => s + m.profit, 0);
// // // // // //       const totalUnits = processedData.reduce((s, m) => s + m.quantity, 0);

// // // // // //       const gross_margin_avg =
// // // // // //         totalSales > 0 ? (totalProfit / totalSales) * 100 : 0;

// // // // // //       const monthsWithSales = processedData.filter((m) => m.net_sales > 0);

// // // // // //       const avgSales =
// // // // // //         monthsWithSales.length > 0 ? totalSales / monthsWithSales.length : 0;

// // // // // //       const avgSellingPrice = totalUnits > 0 ? totalSales / totalUnits : 0;

// // // // // //       const avgMonthlyProfit =
// // // // // //         processedData.length > 0 ? totalProfit / processedData.length : 0;

// // // // // //       const maxSalesMonth = processedData.reduce((max, m) =>
// // // // // //         m.net_sales > max.net_sales ? m : max
// // // // // //       );

// // // // // //       const maxUnitsMonth = processedData.reduce((max, m) =>
// // // // // //         m.quantity > max.quantity ? m : max
// // // // // //       );

// // // // // //       return {
// // // // // //         country,
// // // // // //         stats: {
// // // // // //           totalSales,
// // // // // //           totalProfit,
// // // // // //           totalUnits,
// // // // // //           gross_margin_avg,
// // // // // //           avgSales,
// // // // // //           avgSellingPrice,
// // // // // //           avgMonthlyProfit,
// // // // // //           maxSalesMonth,
// // // // // //           maxUnitsMonth,
// // // // // //         },
// // // // // //       };
// // // // // //     });
// // // // // //   }, [data, monthOrder]);

// // // // // //   const formatMonthYear = (monthName: string, year: number | string) => {
// // // // // //     const MONTH_ABBRS = [
// // // // // //       "Jan",
// // // // // //       "Feb",
// // // // // //       "Mar",
// // // // // //       "Apr",
// // // // // //       "May",
// // // // // //       "Jun",
// // // // // //       "Jul",
// // // // // //       "Aug",
// // // // // //       "Sep",
// // // // // //       "Oct",
// // // // // //       "Nov",
// // // // // //       "Dec",
// // // // // //     ];

// // // // // //     if (!monthName) return "";

// // // // // //     const fullNames = [
// // // // // //       "january",
// // // // // //       "february",
// // // // // //       "march",
// // // // // //       "april",
// // // // // //       "may",
// // // // // //       "june",
// // // // // //       "july",
// // // // // //       "august",
// // // // // //       "september",
// // // // // //       "october",
// // // // // //       "november",
// // // // // //       "december",
// // // // // //     ];

// // // // // //     const idx = fullNames.findIndex(
// // // // // //       (full) =>
// // // // // //         monthName.toLowerCase().startsWith(full.slice(0, 3)) ||
// // // // // //         monthName.toLowerCase() === full
// // // // // //     );

// // // // // //     const abbr = idx >= 0 ? MONTH_ABBRS[idx] : monthName.slice(0, 3) || monthName;
// // // // // //     const y = String(year);
// // // // // //     const shortYear = y.slice(-2);
// // // // // //     return `${abbr}'${shortYear}`;
// // // // // //   };

// // // // // //   const CountryCard: React.FC<{ country: string; stats: any }> = ({
// // // // // //     country,
// // // // // //     stats,
// // // // // //   }) => (
// // // // // //     <div className="rounded-lg border border-[#414042] bg-white p-4 sm:p-5 shadow-sm transition-shadow hover:shadow-md">
// // // // // //       <div className="mb-4 flex items-center justify-between gap-2">
// // // // // //         <h4 className="m-0 font-extrabold text-[#5EA68E] text-[clamp(14px,1.2vw,20px)] flex items-center gap-2">
// // // // // //           <span
// // // // // //             className="inline-block h-3 w-3 sm:h-3.5 sm:w-3.5 rounded-full"
// // // // // //             style={{ backgroundColor: getCountryColor(country) }}
// // // // // //           />
// // // // // //           <span className="text-[#414042] text-[clamp(14px,1.1vw,18px)]">
// // // // // //             {country.toUpperCase()}
// // // // // //           </span>
// // // // // //         </h4>
// // // // // //       </div>

// // // // // //       <div className="flex flex-col gap-4">
// // // // // //         {/* Stats grid */}
// // // // // //         <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">

// // // // // //           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
// // // // // //             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
// // // // // //               Net Sales
// // // // // //             </p>
// // // // // //             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
// // // // // //               {formatCurrencyByCountry(country, stats.totalSales)}
// // // // // //             </p>
// // // // // //           </div>
// // // // // //           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
// // // // // //             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
// // // // // //               Units
// // // // // //             </p>
// // // // // //             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
// // // // // //               {stats.totalUnits.toLocaleString()}
// // // // // //             </p>
// // // // // //           </div>
// // // // // //           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
// // // // // //             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
// // // // // //               CM1 Profit
// // // // // //             </p>
// // // // // //             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
// // // // // //               {formatCurrencyByCountry(country, stats.totalProfit)}
// // // // // //             </p>
// // // // // //           </div>

// // // // // //           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
// // // // // //             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
// // // // // //               Avg. Monthly Sales
// // // // // //             </p>
// // // // // //             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
// // // // // //               {formatCurrencyByCountry(country, stats.avgSales)}
// // // // // //             </p>
// // // // // //           </div>
// // // // // //           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
// // // // // //             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
// // // // // //               Avg. Selling Price
// // // // // //             </p>
// // // // // //             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
// // // // // //               {formatCurrencyByCountry(country, stats.avgSellingPrice)}
// // // // // //             </p>
// // // // // //           </div>
// // // // // //           <div className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3">
// // // // // //             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
// // // // // //               CM1 Profit (%)
// // // // // //             </p>
// // // // // //             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
// // // // // //               {stats.gross_margin_avg.toFixed(2)}%
// // // // // //             </p>
// // // // // //           </div>
// // // // // //         </div>

// // // // // //         <p className="m-0 text-[clamp(13px,1vw,16px)] font-bold">
// // // // // //           Best Performance Month
// // // // // //         </p>

// // // // // //         <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
// // // // // //           {/* Sales */}
// // // // // //           <div
// // // // // //             className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3"
// // // // // //             style={{
// // // // // //               borderTopWidth: 4,
// // // // // //               borderTopColor: getCountryColor(country),
// // // // // //             }}
// // // // // //           >
// // // // // //             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
// // // // // //               Sales
// // // // // //             </p>
// // // // // //             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
// // // // // //               {formatMonthYear(stats.maxSalesMonth.month, selectedYear)}
// // // // // //             </p>
// // // // // //             <p className="text-[clamp(12px,0.95vw,16px)]">
// // // // // //               {formatCurrencyByCountry(country, stats.maxSalesMonth.net_sales)}
// // // // // //             </p>
// // // // // //           </div>

// // // // // //           {/* Units */}
// // // // // //           <div
// // // // // //             className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3"
// // // // // //             style={{
// // // // // //               borderTopWidth: 4,
// // // // // //               borderTopColor: getCountryColor(country),
// // // // // //             }}
// // // // // //           >
// // // // // //             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
// // // // // //               Units
// // // // // //             </p>
// // // // // //             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
// // // // // //               {formatMonthYear(stats.maxUnitsMonth.month, selectedYear)}
// // // // // //             </p>
// // // // // //             <p className="text-[clamp(12px,0.95vw,16px)]">
// // // // // //               {stats.maxUnitsMonth.quantity.toLocaleString()}
// // // // // //             </p>
// // // // // //           </div>

// // // // // //           {/* CM1 Profit */}
// // // // // //           <div
// // // // // //             className="rounded-lg border border-gray-300 bg-gray-200/40 p-2 sm:p-3"
// // // // // //             style={{
// // // // // //               borderTopWidth: 4,
// // // // // //               borderTopColor: getCountryColor(country),
// // // // // //             }}
// // // // // //           >
// // // // // //             <p className="mb-1 text-[clamp(11px,0.85vw,13px)] font-semibold text-[#414042]">
// // // // // //               CM1 Profit
// // // // // //             </p>
// // // // // //             <p className="text-[clamp(12px,0.95vw,16px)] font-semibold">
// // // // // //               {formatMonthYear(stats.maxSalesMonth.month, selectedYear)}
// // // // // //             </p>
// // // // // //             <p className="text-[clamp(12px,0.95vw,16px)]">
// // // // // //               {formatCurrencyByCountry(country, stats.maxSalesMonth.profit)}
// // // // // //             </p>
// // // // // //           </div>
// // // // // //         </div>
// // // // // //       </div>
// // // // // //     </div>
// // // // // //   );

// // // // // //   // Global card first, then others
// // // // // //   const orderedCards = useMemo(() => {
// // // // // //     if (!cards.length) return [];
// // // // // //     const global = cards.filter(
// // // // // //       (c) => c.country.toLowerCase() === "global"
// // // // // //     );
// // // // // //     const others = cards.filter(
// // // // // //       (c) => c.country.toLowerCase() !== "global"
// // // // // //     );
// // // // // //     return [...global, ...others];
// // // // // //   }, [cards]);

// // // // // //   return (
// // // // // //     <div className="w-full">

// // // // // //       {/* Header */}
// // // // // //       {/* <div className="mb-4">
// // // // // //         <PageBreadcrumb
// // // // // //           pageTitle="Performance Analysis"
// // // // // //           variant="page"
// // // // // //           align="left"
// // // // // //           textSize="2xl"
// // // // // //         />
// // // // // //       </div> */}

// // // // // //       {/* Header */}
// // // // // //       <div className="mb-4">
// // // // // //         <h2
// // // // // //           className="
// // // // // //         flex flex-wrap items-baseline gap-x-1 gap-y-1
// // // // // //         text-[15px] sm:text-lg md:text-xl lg:text-2xl
// // // // // //         font-semibold text-[#414042]
// // // // // //       "
// // // // // //         >
// // // // // //           <PageBreadcrumb
// // // // // //             pageTitle="Performance Analysis"
// // // // // //             variant="page"
// // // // // //             align="left"
// // // // // //             textSize="2xl"
// // // // // //           />

// // // // // //           {canShowResults && (
// // // // // //             <>
// // // // // //               <span className="text-gray-400">-</span>
// // // // // //               <span
// // // // // //                 className="
// // // // // //               text-lg sm:text-2xl md:text-2xl
// // // // // //               font-bold text-green-500
// // // // // //             "
// // // // // //               >
// // // // // //                 {/* UK: Classic (YTD'25) */}
// // // // // //                 {countryName && formatCountryLabel(countryName)}
// // // // // //                 {productname && `: ${productname}`}{" "}
// // // // // //                 {`(${getHeadingPeriod()})`}
// // // // // //               </span>
// // // // // //             </>
// // // // // //           )}
// // // // // //         </h2>
// // // // // //       </div>


// // // // // //       {/* Search + Filters in SAME ROW */}
// // // // // //       <div className="mb-5 flex flex-col md:flex-row items-center justify-between gap-4">
// // // // // //         {/* Period Filters */}
// // // // // //         <PeriodFiltersTable
// // // // // //           range={range}
// // // // // //           selectedMonth={selectedMonth}
// // // // // //           selectedQuarter={`Q${selectedQuarter}`}
// // // // // //           selectedYear={selectedYear === "" ? "" : selectedYear} // pass empty when none
// // // // // //           yearOptions={years}
// // // // // //           onRangeChange={(v: Range) => setRange(v)}
// // // // // //           onMonthChange={(val) => setSelectedMonth(val)}
// // // // // //           onQuarterChange={(val) => {
// // // // // //             const num = val.replace("Q", "");
// // // // // //             setSelectedQuarter(num || "1");
// // // // // //           }}
// // // // // //           onYearChange={(val) => {
// // // // // //             // val is string from <select>
// // // // // //             if (!val) {
// // // // // //               setSelectedYear("");
// // // // // //             } else {
// // // // // //               setSelectedYear(Number(val));
// // // // // //             }
// // // // // //           }}
// // // // // //           allowedRanges={["quarterly", "yearly"]}
// // // // // //         />



// // // // // //         {/* Search Bar */}
// // // // // //         <ProductSearchDropdown
// // // // // //           authToken={authToken}
// // // // // //           onProductSelect={handleProductSelect}
// // // // // //         />
// // // // // //       </div>


// // // // // //       {/* Alert Box - Only show when no product or period is selected */}
// // // // // //       {!canShowResults && (
// // // // // //         <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
// // // // // //           <div className="flex items-center">
// // // // // //             <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
// // // // // //             <span>
// // // // // //               Search a product and choose the period to view SKU-wise performance.
// // // // // //             </span>
// // // // // //           </div>
// // // // // //         </div>
// // // // // //       )}

// // // // // //       {/* Loading */}
// // // // // //       {canShowResults && loading && (
// // // // // //         <div className="flex flex-col items-center justify-center py-12 text-center">
// // // // // //           <Loader
// // // // // //             src="/infinity-unscreen.gif"
// // // // // //             size={150}
// // // // // //             transparent
// // // // // //             roundedClass="rounded-none"
// // // // // //             backgroundClass="bg-transparent"
// // // // // //             respectReducedMotion
// // // // // //           />
// // // // // //         </div>
// // // // // //       )}

// // // // // //       {/* Error */}
// // // // // //       {canShowResults && !!error && (
// // // // // //         <div className="mb-8 rounded-2xl border border-red-200 bg-red-50 p-6">
// // // // // //           <div className="flex items-center gap-3 text-red-700">
// // // // // //             <span className="text-xl">❌</span>
// // // // // //             <p className="m-0 font-medium">{error}</p>
// // // // // //           </div>
// // // // // //         </div>
// // // // // //       )}

// // // // // //       {canShowResults && data && !loading && (
// // // // // //         <div className="flex flex-col">
// // // // // //           <div className="w-full rounded-md border border-charcoal-500 bg-[#D9D9D933] p-4 sm:p-5 shadow-sm">
// // // // // //             <div className="flex items-start justify-between gap-4">
// // // // // //               <div className="flex-1">
// // // // // //                 <h3 className="m-0 text-xl font-bold text-[#414042]">
// // // // // //                   {currentIndex === 0
// // // // // //                     ? "Net Sales Trend"
// // // // // //                     : currentIndex === 1
// // // // // //                       ? "Units Trend"
// // // // // //                       : "CM1 Profit Trend"}{" "}
// // // // // //                   -{" "}
// // // // // //                   <b className="text-green-500 capitalize">
// // // // // //                     {productname} ({getTitle()})
// // // // // //                   </b>
// // // // // //                 </h3>

// // // // // //                 <p className="mt-1 text-xs sm:text-sm text-gray-500">
// // // // // //                   Year-over-year performance comparison across regions.
// // // // // //                 </p>

// // // // // //                 <div className="my-4 flex flex-wrap items-center gap-3">
// // // // // //                   {["global", ...nonEmptyCountriesFromApi].map((country) => {
// // // // // //                     const color = getCountryColor(country);
// // // // // //                     const isChecked = selectedCountries[country] ?? true;
// // // // // //                     const label = formatCountryLabel(country);

// // // // // //                     return (
// // // // // //                       <label
// // // // // //                         key={country}
// // // // // //                         className={[
// // // // // //                           "shrink-0",
// // // // // //                           "flex items-center gap-1 sm:gap-1.5",
// // // // // //                           "font-semibold select-none whitespace-nowrap",
// // // // // //                           "text-[9px] sm:text-[10px] md:text-[11px] lg:text-xs xl:text-sm",
// // // // // //                           "text-charcoal-500",
// // // // // //                           isChecked ? "opacity-100" : "opacity-40",
// // // // // //                           "cursor-pointer",
// // // // // //                         ].join(" ")}
// // // // // //                         onClick={() => handleCountryChange(country as CountryKey)}
// // // // // //                       >
// // // // // //                         <span
// // // // // //                           className="
// // // // // //           flex items-center justify-center
// // // // // //           h-3 w-3 sm:h-3.5 sm:w-3.5
// // // // // //           rounded-sm border transition
// // // // // //         "
// // // // // //                           style={{
// // // // // //                             borderColor: color,
// // // // // //                             backgroundColor: isChecked ? color : "white",
// // // // // //                           }}
// // // // // //                         >
// // // // // //                           {isChecked && (
// // // // // //                             <svg
// // // // // //                               viewBox="0 0 24 24"
// // // // // //                               width="14"
// // // // // //                               height="14"
// // // // // //                               className="text-white"
// // // // // //                             >
// // // // // //                               <path
// // // // // //                                 fill="currentColor"
// // // // // //                                 d="M20.285 6.709a1 1 0 0 0-1.414-1.414L9 15.168l-3.879-3.88a1 1 0 0 0-1.414 1.415l4.586 4.586a1 1 0 0 0 1.414 0l10-10Z"
// // // // // //                               />
// // // // // //                             </svg>
// // // // // //                           )}
// // // // // //                         </span>

// // // // // //                         <span className="text-charcoal-500">
// // // // // //                           {label}
// // // // // //                         </span>
// // // // // //                       </label>
// // // // // //                     );
// // // // // //                   })}

// // // // // //                 </div>
// // // // // //               </div>

// // // // // //               {/* Download icon in top-right */}
// // // // // //               <div className="shrink-0">
// // // // // //                 <DownloadIconButton />
// // // // // //                 {/* if your component needs props like onClick, pass them here */}
// // // // // //               </div>
// // // // // //             </div>

// // // // // //             {/* Chart */}
// // // // // //             <div className="flex h-[40vw] min-h-[260px] items-center justify-between">
// // // // // //               {chartDataList ? (
// // // // // //                 <>
// // // // // //                   <button
// // // // // //                     className="ml-1 flex h-7 w-7 items-center justify-center rounded-full bg-[#2c3e50] text-[#f8edcf] shadow transition active:scale-95"
// // // // // //                     onClick={handlePrev}
// // // // // //                     aria-label="Previous chart"
// // // // // //                   >
// // // // // //                     <svg
// // // // // //                       xmlns="http://www.w3.org/2000/svg"
// // // // // //                       viewBox="0 0 24 24"
// // // // // //                       fill="currentColor"
// // // // // //                       className="h-4 w-4"
// // // // // //                     >
// // // // // //                       <path
// // // // // //                         fillRule="evenodd"
// // // // // //                         d="M15.78 4.22a.75.75 0 010 1.06L9.06 12l6.72 6.72a.75.75 0 11-1.06 1.06l-7.25-7.25a.75.75 0 010-1.06l7.25-7.25a.75.75 0 011.06 0z"
// // // // // //                         clipRule="evenodd"
// // // // // //                       />
// // // // // //                     </svg>
// // // // // //                   </button>

// // // // // //                   {chartDataList[currentIndex] ? (
// // // // // //                     <div className="mx-2 w-full">
// // // // // //                       <Line
// // // // // //                         data={chartDataList[currentIndex] as any}
// // // // // //                         options={chartOptions as any}
// // // // // //                       />
// // // // // //                     </div>
// // // // // //                   ) : (
// // // // // //                     <p className="mx-auto">No chart data available.</p>
// // // // // //                   )}

// // // // // //                   <button
// // // // // //                     className="mr-2 flex h-7 w-7 items-center justify-center rounded-full bg-[#2c3e50] text-[#f8edcf] shadow transition active:scale-95"
// // // // // //                     onClick={handleNext}
// // // // // //                     aria-label="Next chart"
// // // // // //                   >
// // // // // //                     <svg
// // // // // //                       xmlns="http://www.w3.org/2000/svg"
// // // // // //                       viewBox="0 0 24 24"
// // // // // //                       fill="currentColor"
// // // // // //                       className="h-4 w-4"
// // // // // //                     >
// // // // // //                       <path
// // // // // //                         fillRule="evenodd"
// // // // // //                         d="M8.22 19.78a.75.75 0 010-1.06L14.94 12 8.22 5.28a.75.75 0 111.06-1.06l7.25 7.25a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0z"
// // // // // //                         clipRule="evenodd"
// // // // // //                       />
// // // // // //                     </svg>
// // // // // //                   </button>
// // // // // //                 </>
// // // // // //               ) : (
// // // // // //                 <p>No chart data available</p>
// // // // // //               )}
// // // // // //             </div>

// // // // // //             {/* Dots */}
// // // // // //             <div className="mt-3 flex items-center justify-center gap-2">
// // // // // //               {[0, 1, 2].map((idx) => (
// // // // // //                 <span
// // // // // //                   key={idx}
// // // // // //                   className={`h-2 w-2 rounded-full border ${currentIndex === idx
// // // // // //                     ? "border-gray-300 bg-gray-300"
// // // // // //                     : "border-[#414042] bg-white"
// // // // // //                     }`}
// // // // // //                 />
// // // // // //               ))}
// // // // // //             </div>
// // // // // //           </div>

// // // // // //           {/* Summary Cards – unchanged, just below the chart card */}
// // // // // //           <div className="mt-8">
// // // // // //             <div className="grid gap-5 grid-cols-1 md:grid-cols-[repeat(auto-fit,minmax(260px,1fr))]">
// // // // // //               {orderedCards.map((card) => {
// // // // // //                 const key = card.country.toLowerCase();
// // // // // //                 const isGlobal = key === "global";

// // // // // //                 if (
// // // // // //                   !isGlobal &&
// // // // // //                   card.stats.totalSales === 0 &&
// // // // // //                   card.stats.totalUnits === 0 &&
// // // // // //                   card.stats.totalProfit === 0
// // // // // //                 ) {
// // // // // //                   return null;
// // // // // //                 }

// // // // // //                 return (
// // // // // //                   <CountryCard
// // // // // //                     key={key}
// // // // // //                     country={card.country}
// // // // // //                     stats={card.stats}
// // // // // //                   />
// // // // // //                 );
// // // // // //               })}
// // // // // //             </div>
// // // // // //           </div>
// // // // // //         </div>
// // // // // //       )}

// // // // // //     </div>
// // // // // //   );
// // // // // // };

// // // // // // export default ProductwisePerformance;










































// // // // // // components/productwise/ProductwisePerformance.tsx
// // // // // "use client";

// // // // // import React, { useEffect, useMemo, useState } from "react";
// // // // // import { useParams, useRouter } from "next/navigation";
// // // // // import "@/lib/chartSetup";
// // // // // import {
// // // // //   Chart as ChartJS,
// // // // //   CategoryScale,
// // // // //   LinearScale,
// // // // //   PointElement,
// // // // //   LineElement,
// // // // //   Title as ChartTitle,
// // // // //   Tooltip,
// // // // //   Legend,
// // // // //   Filler,
// // // // // } from "chart.js";

// // // // // import Loader from "@/components/loader/Loader";


// // // // // import {
// // // // //   APIResponse,
// // // // //   CountryKey,
// // // // //   GBP_TO_USD_RATE,
// // // // //   MonthDatum,
// // // // //   Range,
// // // // //   formatCountryLabel,
// // // // //   formatCurrencyByCountry,
// // // // //   getCountryColor,
// // // // //   monthOrder,
// // // // // } from "@/components/productwise/productwiseHelpers";
// // // // // import CountryCard from "@/components/productwise/CountryCard";
// // // // // import TrendChartSection from "@/components/productwise/TrendChartSection";
// // // // // import ProductwiseHeader from "@/components/productwise/ProductwiseHeader";
// // // // // import FiltersAndSearchRow from "@/components/productwise/FiltersAndSearchRow";
// // // // // import { useFx } from "@/components/dashboard/useFx";


// // // // // ChartJS.register(
// // // // //   CategoryScale,
// // // // //   LinearScale,
// // // // //   PointElement,
// // // // //   LineElement,
// // // // //   ChartTitle,
// // // // //   Tooltip,
// // // // //   Legend,
// // // // //   Filler
// // // // // );

// // // // // interface ProductwisePerformanceProps {
// // // // //   productname?: string;
// // // // // }

// // // // // // -------- Slug helpers --------
// // // // // const toSlug = (name: string) =>
// // // // //   name
// // // // //     .trim()
// // // // //     .toLowerCase()
// // // // //     .replace(/\s*\+\s*/g, " plus ")
// // // // //     .replace(/\s+/g, "-");

// // // // // const fromSlug = (slug: string) =>
// // // // //   slug
// // // // //     .replace(/-/g, " ")
// // // // //     .replace(/\bplus\b/gi, "+")
// // // // //     .replace(/\s+/g, " ")
// // // // //     .trim();

// // // // // const normalizeProductSlug = (slug?: string) => {
// // // // //   if (!slug) return undefined;

// // // // //   try {
// // // // //     const decoded = decodeURIComponent(slug);
// // // // //     if (!decoded.trim()) return undefined;
// // // // //     return fromSlug(decoded);
// // // // //   } catch {
// // // // //     if (!slug.trim()) return undefined;
// // // // //     return fromSlug(slug);
// // // // //   }
// // // // // };

// // // // // const ProductwisePerformance: React.FC<ProductwisePerformanceProps> = ({
// // // // //   productname: propProductName,
// // // // // }) => {
// // // // //   const {
// // // // //     homeCurrency,
// // // // //     setHomeCurrency,
// // // // //     convertToHomeCurrency,
// // // // //     formatHomeAmount,
// // // // //   } = useFx();

// // // // //   const params = useParams();
// // // // //   const router = useRouter();

// // // // //   const rawSlug = params?.productname as string | undefined;
// // // // //   const urlProductName = normalizeProductSlug(rawSlug);

// // // // //   const countryName = (params?.countryName as string) || undefined;
// // // // //   const monthParam = (params?.month as string) || undefined;
// // // // //   const yearParam = (params?.year as string) || undefined;

// // // // //   const productname = propProductName || urlProductName || "";

// // // // //   const [data, setData] = useState<APIResponse | null>(null);
// // // // //   const [loading, setLoading] = useState(false);
// // // // //   const [error, setError] = useState<string>("");

// // // // //   const authToken =
// // // // //     typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

// // // // //   // controls
// // // // //   const [range, setRange] = useState<Range>();
// // // // //   // const [selectedYear, setSelectedYear] = useState<number | "">("");
// // // // //   // const [selectedMonth, setSelectedMonth] = useState<string>(monthParam || "");
// // // // //   const [selectedQuarter, setSelectedQuarter] = useState("1");

// // // // //   const [selectedMonth, setSelectedMonth] = useState<string>(() => {
// // // // //     if (monthParam) return monthParam;          // URL wins

// // // // //     if (typeof window !== "undefined") {
// // // // //       const raw = localStorage.getItem("latestFetchedPeriod");
// // // // //       if (raw) {
// // // // //         try {
// // // // //           const parsed = JSON.parse(raw) as { month?: string; year?: string };
// // // // //           if (parsed.month) return parsed.month;  // e.g. "november"
// // // // //         } catch { /* ignore */ }
// // // // //       }
// // // // //     }

// // // // //     return ""; // fallback
// // // // //   });

// // // // //   const [selectedYear, setSelectedYear] = useState<number | "">(() => {
// // // // //     if (yearParam) return Number(yearParam);

// // // // //     if (typeof window !== "undefined") {
// // // // //       const raw = localStorage.getItem("latestFetchedPeriod");
// // // // //       if (raw) {
// // // // //         try {
// // // // //           const parsed = JSON.parse(raw) as { month?: string; year?: string };
// // // // //           if (parsed.year) return Number(parsed.year);  // e.g. 2025
// // // // //         } catch { /* ignore */ }
// // // // //       }
// // // // //     }

// // // // //     return ""; // fallback
// // // // //   });


// // // // //   const [selectedCountries, setSelectedCountries] = useState<
// // // // //     Record<CountryKey, boolean>
// // // // //   >({
// // // // //     uk: true,
// // // // //     us: true,
// // // // //     global: true,
// // // // //   });

// // // // //   const years = useMemo(
// // // // //     () => Array.from({ length: 2 }, (_, i) => new Date().getFullYear() - i),
// // // // //     []
// // // // //   );

// // // // //   const isProductSelected = !!productname;
// // // // //   const hasYear = selectedYear !== "" && selectedYear !== undefined;

// // // // //   const isPeriodComplete =
// // // // //     (range === "yearly" && hasYear) ||
// // // // //     (range === "quarterly" && hasYear && !!selectedQuarter) ||
// // // // //     (range === "monthly" && hasYear && !!selectedMonth);

// // // // //   const canShowResults = isProductSelected && isPeriodComplete;

// // // // //   useEffect(() => {
// // // // //     const scope = (countryName || "global").toLowerCase();

// // // // //     if (scope === "uk") {
// // // // //       setHomeCurrency("GBP");
// // // // //     } else {
// // // // //       // Global or anything else → default to USD
// // // // //       setHomeCurrency("USD");
// // // // //     }
// // // // //   }, [countryName, setHomeCurrency]);


// // // // //   // country toggle
// // // // //   const handleCountryChange = (country: CountryKey) => {
// // // // //     setSelectedCountries((prev) => ({
// // // // //       ...prev,
// // // // //       [country]: !(prev[country] ?? true),
// // // // //     }));
// // // // //   };

// // // // //   // product select handler
// // // // //   const handleProductSelect = (selectedProductName: string) => {
// // // // //     const base = "/productwiseperformance";
// // // // //     const slug = toSlug(selectedProductName);

// // // // //     const to = `${base}/${slug}/${countryName ?? ""}/${selectedMonth ?? ""}/${selectedYear ?? ""}`;
// // // // //     router.push(to);
// // // // //   };

// // // // //   // fetch data
// // // // //   const fetchProductData = async () => {
// // // // //     if (!canShowResults) return;

// // // // //     setLoading(true);
// // // // //     setError("");

// // // // //     try {
// // // // //       const countries: CountryKey[] = ["global", "uk", "us"];

// // // // //       const backendTimeRange =
// // // // //         range === "yearly"
// // // // //           ? "Yearly"
// // // // //           : range === "quarterly"
// // // // //             ? "Quarterly"
// // // // //             : "Monthly";

// // // // //       const payload: any = {
// // // // //         product_name: productname,
// // // // //         time_range: backendTimeRange,
// // // // //         year: selectedYear,
// // // // //         countries,
// // // // //       };

// // // // //       if (range === "quarterly") {
// // // // //         payload.quarter = selectedQuarter;
// // // // //       }

// // // // //       if (range === "monthly") {
// // // // //         payload.month = selectedMonth;
// // // // //       }

// // // // //       const res = await fetch("http://localhost:5000/ProductwisePerformance", {
// // // // //         method: "POST",
// // // // //         headers: {
// // // // //           "Content-Type": "application/json",
// // // // //           Authorization: `Bearer ${authToken ?? ""}`,
// // // // //         },
// // // // //         body: JSON.stringify(payload),
// // // // //       });

// // // // //       const json: APIResponse | any = await res.json().catch(() => null);

// // // // //       if (!res.ok || !json?.success) {
// // // // //         const errMsg =
// // // // //           (json && (json.error || json.message)) ||
// // // // //           `HTTP error! status: ${res.status}`;
// // // // //         throw new Error(errMsg);
// // // // //       }

// // // // //       setData(json as APIResponse);
// // // // //     } catch (e: any) {
// // // // //       console.error("API Error:", e);
// // // // //       setError(e?.message || "Failed to fetch data from server");
// // // // //     } finally {
// // // // //       setLoading(false);
// // // // //     }
// // // // //   };

// // // // //   useEffect(() => {
// // // // //     if (!canShowResults) return;
// // // // //     fetchProductData();
// // // // //     // eslint-disable-next-line react-hooks/exhaustive-deps
// // // // //   }, [productname, selectedYear, range, selectedQuarter, selectedMonth, canShowResults]);

// // // // //   // countries with non-zero data (excluding global)
// // // // //   const nonEmptyCountriesFromApi = useMemo(() => {
// // // // //     if (!data?.data) return [] as CountryKey[];

// // // // //     return (Object.entries(data.data) as [CountryKey, MonthDatum[]][])
// // // // //       .filter(([country, rows]) => {
// // // // //         if (country.toLowerCase() === "global") return false;
// // // // //         return rows.some(
// // // // //           (m) =>
// // // // //             m.net_sales !== 0 ||
// // // // //             m.quantity !== 0 ||
// // // // //             m.profit !== 0
// // // // //         );
// // // // //       })
// // // // //       .map(([country]) => country);
// // // // //   }, [data]);

// // // // //   // charts builder (net sales, quantity, profit)
// // // // //   const buildChartJSData = () => {
// // // // //     if (!data?.data) return [null, null, null];

// // // // //     const allMonths = new Set<string>();
// // // // //     Object.values(data.data).forEach((countryData) => {
// // // // //       countryData.forEach((m) => allMonths.add(m.month));
// // // // //     });

// // // // //     const labels = Array.from(allMonths).sort(
// // // // //       (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
// // // // //     );

// // // // //     const getMetric = (
// // // // //       country: CountryKey,
// // // // //       month: string,
// // // // //       metric: keyof MonthDatum
// // // // //     ) => {
// // // // //       const arr = data.data[country];
// // // // //       if (!arr) return 0;
// // // // //       const found = arr.find((m) => m.month === month);
// // // // //       return found ? (found[metric] as number) : 0;
// // // // //     };

// // // // //     const makeDataset = (
// // // // //       country: string,
// // // // //       metric: keyof MonthDatum,
// // // // //       labelSuffix: string
// // // // //     ) => {
// // // // //       const lower = country.toLowerCase();

// // // // //       const dataSeries = labels.map((month) => {
// // // // //         const ukVal = getMetric("uk", month, metric);
// // // // //         const usVal = getMetric("us", month, metric);
// // // // //         const rawGlobal = getMetric("global", month, metric);
// // // // //         const isMoney = metric === "net_sales" || metric === "profit";

// // // // //         if (lower === "global") {
// // // // //           if (metric === "quantity") {
// // // // //             const sumUnits = ukVal + usVal;
// // // // //             return sumUnits !== 0 ? sumUnits : rawGlobal;
// // // // //           }
// // // // //           const sumMoney = ukVal * GBP_TO_USD_RATE + usVal;
// // // // //           if (sumMoney !== 0) return sumMoney;
// // // // //           return rawGlobal;
// // // // //         }

// // // // //         if (lower === "uk") {
// // // // //           if (metric === "quantity") return ukVal;
// // // // //           return ukVal * GBP_TO_USD_RATE;
// // // // //         }

// // // // //         if (lower === "us") {
// // // // //           return usVal;
// // // // //         }

// // // // //         const raw = getMetric(country as CountryKey, month, metric);
// // // // //         if (!isMoney) return raw;
// // // // //         return raw;
// // // // //       });

// // // // //       const isGlobal = lower === "global";

// // // // //       return {
// // // // //         label: `${formatCountryLabel(country)} ${labelSuffix}`,
// // // // //         data: dataSeries,
// // // // //         borderColor: getCountryColor(country),
// // // // //         backgroundColor: getCountryColor(country),
// // // // //         tension: 0.1,
// // // // //         pointRadius: 3,
// // // // //         fill: false,
// // // // //         borderDash: isGlobal ? [6, 4] : [],
// // // // //         borderWidth: 2,
// // // // //         order: isGlobal ? 99 : 0,
// // // // //       };
// // // // //     };

// // // // //     const metrics: { metric: keyof MonthDatum; suffix: string }[] = [
// // // // //       { metric: "net_sales", suffix: "Net Sales" },
// // // // //       { metric: "quantity", suffix: "Quantity" },
// // // // //       { metric: "profit", suffix: "Profit" },
// // // // //     ];

// // // // //     const charts = metrics.map(({ metric, suffix }) => {
// // // // //       const visibleCountries: CountryKey[] = [];

// // // // //       if (selectedCountries["global"] ?? true) {
// // // // //         visibleCountries.push("global");
// // // // //       }

// // // // //       visibleCountries.push(
// // // // //         ...nonEmptyCountriesFromApi.filter(
// // // // //           (c) => selectedCountries[c] ?? true
// // // // //         )
// // // // //       );

// // // // //       const datasets = visibleCountries.map((country) =>
// // // // //         makeDataset(country, metric, suffix)
// // // // //       );

// // // // //       return {
// // // // //         labels,
// // // // //         datasets,
// // // // //       };
// // // // //     });

// // // // //     return charts;
// // // // //   };

// // // // //   const chartDataList = buildChartJSData();

// // // // //   const chartOptions = {
// // // // //     responsive: true,
// // // // //     maintainAspectRatio: false,      // 👈 IMPORTANT
// // // // //     plugins: {
// // // // //       legend: { display: false },
// // // // //       tooltip: {
// // // // //         callbacks: {
// // // // //           label: (context: any) => {
// // // // //             const value = context.parsed.y as number;
// // // // //             const datasetLabel = context.dataset.label as string;
// // // // //             const metricPart = (
// // // // //               datasetLabel
// // // // //                 .split(" ")
// // // // //                 .slice(1)
// // // // //                 .join(" ") || ""
// // // // //             ).toLowerCase();

// // // // //             if (
// // // // //               metricPart.includes("quantity") ||
// // // // //               metricPart.includes("units")
// // // // //             ) {
// // // // //               return `${datasetLabel}: ${value.toLocaleString()}`;
// // // // //             }

// // // // //             return `${datasetLabel}: ${formatCurrencyByCountry("us", value)}`;
// // // // //           },
// // // // //         },
// // // // //       },
// // // // //     },
// // // // //     scales: {
// // // // //       x: { title: { display: true, text: "Month" } },
// // // // //       y: {
// // // // //         title: { display: true, text: "Amount ($)" },
// // // // //         min: 0,
// // // // //         ticks: {
// // // // //           padding: 0,
// // // // //         },
// // // // //       },
// // // // //     },
// // // // //   } as const;


// // // // //   const [currentIndex, setCurrentIndex] = useState(0);
// // // // //   const handlePrev = () =>
// // // // //     setCurrentIndex((i) => (i === 0 ? chartDataList.length - 1 : i - 1));
// // // // //   const handleNext = () =>
// // // // //     setCurrentIndex((i) => (i === chartDataList.length - 1 ? 0 : i + 1));

// // // // //   const yearShort =
// // // // //     selectedYear === "" ? "" : selectedYear.toString().slice(-2);

// // // // //   const cap = (s: string) => (s ? s[0].toUpperCase() + s.slice(1) : "");

// // // // //   const getTitle = () => {
// // // // //     if (range === "yearly") return `Year'${yearShort}`;
// // // // //     if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
// // // // //     return selectedMonth ? `${cap(selectedMonth)}'${yearShort}` : `Year'${yearShort}`;
// // // // //   };

// // // // //   const getHeadingPeriod = () => {
// // // // //     if (range === "yearly") return `YTD'${yearShort}`;
// // // // //     if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
// // // // //     if (range === "monthly" && selectedMonth) {
// // // // //       return `${cap(selectedMonth)}'${yearShort}`;
// // // // //     }
// // // // //     return "";
// // // // //   };

// // // // //   // summary cards data
// // // // //   // const cards = useMemo(() => {
// // // // //   //   if (!data?.data) return [] as { country: string; stats: any }[];

// // // // //   //   const allMonths = new Set<string>();
// // // // //   //   Object.values(data.data).forEach((countryData) => {
// // // // //   //     countryData.forEach((m) => allMonths.add(m.month));
// // // // //   //   });

// // // // //   //   const labels = Array.from(allMonths).sort(
// // // // //   //     (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
// // // // //   //   );

// // // // //   //   return Object.entries(data.data).map(([country, countryData]) => {
// // // // //   //     let processedData = countryData;

// // // // //   //     if (country.toLowerCase() === "global") {
// // // // //   //       processedData = labels.map((month) => {
// // // // //   //         const uk = data.data.uk?.find((m) => m.month === month);
// // // // //   //         const us = data.data.us?.find((m) => m.month === month);
// // // // //   //         const g = data.data.global?.find((m) => m.month === month);

// // // // //   //         const ukNetUSD = (uk?.net_sales || 0) * GBP_TO_USD_RATE;
// // // // //   //         const usNet = us?.net_sales || 0;
// // // // //   //         const ukProfitUSD = (uk?.profit || 0) * GBP_TO_USD_RATE;
// // // // //   //         const usProfit = us?.profit || 0;

// // // // //   //         const sumNetSales = ukNetUSD + usNet;
// // // // //   //         const sumProfit = ukProfitUSD + usProfit;
// // // // //   //         const sumUnits = (uk?.quantity || 0) + (us?.quantity || 0);

// // // // //   //         return {
// // // // //   //           month,
// // // // //   //           net_sales: sumNetSales !== 0 ? sumNetSales : g?.net_sales || 0,
// // // // //   //           profit: sumProfit !== 0 ? sumProfit : g?.profit || 0,
// // // // //   //           quantity: sumUnits !== 0 ? sumUnits : g?.quantity || 0,
// // // // //   //         };
// // // // //   //       });
// // // // //   //     }

// // // // //   //     const totalSales = processedData.reduce((s, m) => s + m.net_sales, 0);
// // // // //   //     const totalProfit = processedData.reduce((s, m) => s + m.profit, 0);
// // // // //   //     const totalUnits = processedData.reduce((s, m) => s + m.quantity, 0);

// // // // //   //     const gross_margin_avg =
// // // // //   //       totalSales > 0 ? (totalProfit / totalSales) * 100 : 0;

// // // // //   //     const monthsWithSales = processedData.filter((m) => m.net_sales > 0);

// // // // //   //     const avgSales =
// // // // //   //       monthsWithSales.length > 0 ? totalSales / monthsWithSales.length : 0;

// // // // //   //     const avgSellingPrice = totalUnits > 0 ? totalSales / totalUnits : 0;

// // // // //   //     const avgMonthlyProfit =
// // // // //   //       processedData.length > 0 ? totalProfit / processedData.length : 0;

// // // // //   //     const maxSalesMonth = processedData.reduce((max, m) =>
// // // // //   //       m.net_sales > max.net_sales ? m : max
// // // // //   //     );

// // // // //   //     const maxUnitsMonth = processedData.reduce((max, m) =>
// // // // //   //       m.quantity > max.quantity ? m : max
// // // // //   //     );

// // // // //   //     return {
// // // // //   //       country,
// // // // //   //       stats: {
// // // // //   //         totalSales,
// // // // //   //         totalProfit,
// // // // //   //         totalUnits,
// // // // //   //         gross_margin_avg,
// // // // //   //         avgSales,
// // // // //   //         avgSellingPrice,
// // // // //   //         avgMonthlyProfit,
// // // // //   //         maxSalesMonth,
// // // // //   //         maxUnitsMonth,
// // // // //   //       },
// // // // //   //     };
// // // // //   //   });
// // // // //   // }, [data]);

// // // // //   const cards = useMemo(() => {
// // // // //     if (!data?.data) return [] as { country: string; stats: any }[];

// // // // //     const selectedScope = (countryName || "global").toLowerCase();

// // // // //     const allMonths = new Set<string>();
// // // // //     Object.values(data.data).forEach((countryData) => {
// // // // //       countryData.forEach((m) => allMonths.add(m.month));
// // // // //     });

// // // // //     const labels = Array.from(allMonths).sort(
// // // // //       (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
// // // // //     );

// // // // //     return Object.entries(data.data).map(([country, countryData]) => {
// // // // //       const key = country.toLowerCase();
// // // // //       let processedData = countryData as MonthDatum[];

// // // // //       // ---------- GLOBAL CARD LOGIC ----------
// // // // //       if (key === "global") {
// // // // //         // UK scope → Global should mirror UK in GBP
// // // // //         if (homeCurrency === "GBP" && selectedScope === "uk") {
// // // // //           processedData = data.data.uk ? [...data.data.uk] : [];
// // // // //         } else {
// // // // //           // Global scope → aggregate all countries into homeCurrency (USD now)
// // // // //           processedData = labels.map((month) => {
// // // // //             const uk = data.data.uk?.find((m) => m.month === month);
// // // // //             const us = data.data.us?.find((m) => m.month === month);
// // // // //             const g = data.data.global?.find((m) => m.month === month);

// // // // //             // Convert UK & US to homeCurrency (USD)
// // // // //             const ukNetHome = uk
// // // // //               ? convertToHomeCurrency(uk.net_sales, "GBP")
// // // // //               : 0;
// // // // //             const usNetHome = us
// // // // //               ? convertToHomeCurrency(us.net_sales, "USD")
// // // // //               : 0;

// // // // //             const ukProfitHome = uk
// // // // //               ? convertToHomeCurrency(uk.profit, "GBP")
// // // // //               : 0;
// // // // //             const usProfitHome = us
// // // // //               ? convertToHomeCurrency(us.profit, "USD")
// // // // //               : 0;

// // // // //             const sumNetSales = ukNetHome + usNetHome;
// // // // //             const sumProfit = ukProfitHome + usProfitHome;
// // // // //             const sumUnits = (uk?.quantity || 0) + (us?.quantity || 0);

// // // // //             // Fallback to backend "global" row (assumed USD) if sums are zero
// // // // //             const fallbackNet = g
// // // // //               ? convertToHomeCurrency(g.net_sales, "USD")
// // // // //               : 0;
// // // // //             const fallbackProfit = g
// // // // //               ? convertToHomeCurrency(g.profit, "USD")
// // // // //               : 0;
// // // // //             const fallbackUnits = g?.quantity || 0;

// // // // //             return {
// // // // //               month,
// // // // //               net_sales: sumNetSales !== 0 ? sumNetSales : fallbackNet,
// // // // //               profit: sumProfit !== 0 ? sumProfit : fallbackProfit,
// // // // //               quantity: sumUnits !== 0 ? sumUnits : fallbackUnits,
// // // // //             };
// // // // //           });
// // // // //         }
// // // // //       } else {
// // // // //         // ---------- NON-GLOBAL CARDS ----------
// // // // //         // When in Global scope, convert each country into homeCurrency (USD).
// // // // //         // Otherwise, keep original currency (GBP for UK, USD for US).
// // // // //         const fromCurrency = key === "uk" ? "GBP" : ("USD" as const);
// // // // //         const shouldConvert =
// // // // //           homeCurrency === "USD" && selectedScope === "global";

// // // // //         processedData = shouldConvert
// // // // //           ? countryData.map((m) => ({
// // // // //             ...m,
// // // // //             net_sales: convertToHomeCurrency(m.net_sales, fromCurrency),
// // // // //             profit: convertToHomeCurrency(m.profit, fromCurrency),
// // // // //           }))
// // // // //           : countryData;
// // // // //       }

// // // // //       // ---------- Totals / averages based on processedData ----------
// // // // //       const totalSales = processedData.reduce((s, m) => s + m.net_sales, 0);
// // // // //       const totalProfit = processedData.reduce((s, m) => s + m.profit, 0);
// // // // //       const totalUnits = processedData.reduce((s, m) => s + m.quantity, 0);

// // // // //       const gross_margin_avg =
// // // // //         totalSales > 0 ? (totalProfit / totalSales) * 100 : 0;

// // // // //       const monthsWithSales = processedData.filter((m) => m.net_sales > 0);

// // // // //       const avgSales =
// // // // //         monthsWithSales.length > 0 ? totalSales / monthsWithSales.length : 0;

// // // // //       const avgSellingPrice = totalUnits > 0 ? totalSales / totalUnits : 0;

// // // // //       const avgMonthlyProfit =
// // // // //         processedData.length > 0 ? totalProfit / processedData.length : 0;

// // // // //       const maxSalesMonth = processedData.reduce((max, m) =>
// // // // //         m.net_sales > max.net_sales ? m : max
// // // // //       );

// // // // //       const maxUnitsMonth = processedData.reduce((max, m) =>
// // // // //         m.quantity > max.quantity ? m : max
// // // // //       );

// // // // //       return {
// // // // //         country,
// // // // //         stats: {
// // // // //           totalSales,
// // // // //           totalProfit,
// // // // //           totalUnits,
// // // // //           gross_margin_avg,
// // // // //           avgSales,
// // // // //           avgSellingPrice,
// // // // //           avgMonthlyProfit,
// // // // //           maxSalesMonth,
// // // // //           maxUnitsMonth,
// // // // //         },
// // // // //       };
// // // // //     });
// // // // //   }, [data, countryName, homeCurrency, convertToHomeCurrency]);

// // // // //   const orderedCards = useMemo(() => {
// // // // //     if (!cards.length) return [];
// // // // //     const global = cards.filter(
// // // // //       (c) => c.country.toLowerCase() === "global"
// // // // //     );
// // // // //     const others = cards.filter(
// // // // //       (c) => c.country.toLowerCase() !== "global"
// // // // //     );
// // // // //     return [...global, ...others];
// // // // //   }, [cards]);

// // // // //   return (
// // // // //     <div className="w-full">
// // // // //       <ProductwiseHeader
// // // // //         canShowResults={canShowResults}
// // // // //         countryName={countryName}
// // // // //         productname={productname}
// // // // //         headingPeriod={getHeadingPeriod()}
// // // // //       />

// // // // //       <FiltersAndSearchRow
// // // // //         range={range}
// // // // //         selectedMonth={selectedMonth}
// // // // //         selectedQuarter={selectedQuarter}
// // // // //         selectedYear={selectedYear}
// // // // //         years={years}
// // // // //         authToken={authToken}
// // // // //         onRangeChange={(v) => setRange(v)}
// // // // //         onMonthChange={(val) => setSelectedMonth(val)}
// // // // //         onQuarterChange={(val) => {
// // // // //           const num = val.replace("Q", "");
// // // // //           setSelectedQuarter(num || "1");
// // // // //         }}
// // // // //         onYearChange={(val) => {
// // // // //           if (!val) {
// // // // //             setSelectedYear("");
// // // // //           } else {
// // // // //             setSelectedYear(Number(val));
// // // // //           }
// // // // //         }}
// // // // //         onProductSelect={handleProductSelect}
// // // // //       />

// // // // //       {!canShowResults && (
// // // // //         <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
// // // // //           <div className="flex items-center">
// // // // //             <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
// // // // //             <span>
// // // // //               Search a product and choose the period to view SKU-wise performance.
// // // // //             </span>
// // // // //           </div>
// // // // //         </div>
// // // // //       )}

// // // // //       {canShowResults && loading && (
// // // // //         <div className="flex flex-col items-center justify-center py-12 text-center">
// // // // //           <Loader
// // // // //             src="/infinity-unscreen.gif"
// // // // //             size={150}
// // // // //             transparent
// // // // //             roundedClass="rounded-none"
// // // // //             backgroundClass="bg-transparent"
// // // // //             respectReducedMotion
// // // // //           />
// // // // //         </div>
// // // // //       )}

// // // // //       {canShowResults && !!error && (
// // // // //         <div className="mb-8 rounded-2xl border border-red-200 bg-red-50 p-6">
// // // // //           <div className="flex items-center gap-3 text-red-700">
// // // // //             <span className="text-xl">❌</span>
// // // // //             <p className="m-0 font-medium">{error}</p>
// // // // //           </div>
// // // // //         </div>
// // // // //       )}

// // // // //       {canShowResults && data && !loading && (
// // // // //         <div className="flex flex-col">
// // // // //           <TrendChartSection
// // // // //             productname={productname}
// // // // //             title={getTitle()}
// // // // //             chartDataList={chartDataList}
// // // // //             chartOptions={chartOptions}
// // // // //             currentIndex={currentIndex}
// // // // //             onPrev={handlePrev}
// // // // //             onNext={handleNext}
// // // // //             nonEmptyCountriesFromApi={nonEmptyCountriesFromApi}
// // // // //             selectedCountries={selectedCountries}
// // // // //             onToggleCountry={handleCountryChange}
// // // // //           />

// // // // //           <div className="mt-8">
// // // // //             <div className="grid gap-5 grid-cols-1 md:grid-cols-[repeat(auto-fit,minmax(260px,1fr))]">
// // // // //               {orderedCards.map((card) => {
// // // // //                 const key = card.country.toLowerCase();
// // // // //                 const isGlobal = key === "global";

// // // // //                 if (
// // // // //                   !isGlobal &&
// // // // //                   card.stats.totalSales === 0 &&
// // // // //                   card.stats.totalUnits === 0 &&
// // // // //                   card.stats.totalProfit === 0
// // // // //                 ) {
// // // // //                   return null;
// // // // //                 }

// // // // //                 return (
// // // // //                   // <CountryCard
// // // // //                   //   key={key}
// // // // //                   //   country={card.country}
// // // // //                   //   stats={card.stats}
// // // // //                   //   selectedYear={selectedYear}
// // // // //                   // />
// // // // //                   <CountryCard
// // // // //                     key={key}
// // // // //                     country={card.country}
// // // // //                     stats={card.stats}
// // // // //                     selectedYear={selectedYear}
// // // // //                     homeCurrency={homeCurrency}
// // // // //                   />

// // // // //                 );
// // // // //               })}
// // // // //             </div>
// // // // //           </div>
// // // // //         </div>
// // // // //       )}
// // // // //     </div>
// // // // //   );
// // // // // };

// // // // // export default ProductwisePerformance;
























// // // // // components/productwise/ProductwisePerformance.tsx
// // // // "use client";

// // // // import React, { useEffect, useMemo, useState } from "react";
// // // // import { useParams, useRouter } from "next/navigation";
// // // // import "@/lib/chartSetup";
// // // // import {
// // // //   Chart as ChartJS,
// // // //   CategoryScale,
// // // //   LinearScale,
// // // //   PointElement,
// // // //   LineElement,
// // // //   Title as ChartTitle,
// // // //   Tooltip,
// // // //   Legend,
// // // //   Filler,
// // // // } from "chart.js";

// // // // import Loader from "@/components/loader/Loader";

// // // // import {
// // // //   APIResponse,
// // // //   CountryKey,
// // // //   GBP_TO_USD_RATE, // still used for charts only
// // // //   MonthDatum,
// // // //   Range,
// // // //   formatCountryLabel,
// // // //   formatCurrencyByCountry,
// // // //   getCountryColor,
// // // //   monthOrder,
// // // // } from "@/components/productwise/productwiseHelpers";
// // // // import CountryCard from "@/components/productwise/CountryCard";
// // // // import TrendChartSection from "@/components/productwise/TrendChartSection";
// // // // import ProductwiseHeader from "@/components/productwise/ProductwiseHeader";
// // // // import FiltersAndSearchRow from "@/components/productwise/FiltersAndSearchRow";
// // // // import { useFx } from "@/components/dashboard/useFx";

// // // // ChartJS.register(
// // // //   CategoryScale,
// // // //   LinearScale,
// // // //   PointElement,
// // // //   LineElement,
// // // //   ChartTitle,
// // // //   Tooltip,
// // // //   Legend,
// // // //   Filler
// // // // );

// // // // interface ProductwisePerformanceProps {
// // // //   productname?: string;
// // // // }

// // // // /* ---------- Slug helpers ---------- */
// // // // const toSlug = (name: string) =>
// // // //   name
// // // //     .trim()
// // // //     .toLowerCase()
// // // //     .replace(/\s*\+\s*/g, " plus ")
// // // //     .replace(/\s+/g, "-");

// // // // const fromSlug = (slug: string) =>
// // // //   slug
// // // //     .replace(/-/g, " ")
// // // //     .replace(/\bplus\b/gi, "+")
// // // //     .replace(/\s+/g, " ")
// // // //     .trim();

// // // // const normalizeProductSlug = (slug?: string) => {
// // // //   if (!slug) return undefined;

// // // //   try {
// // // //     const decoded = decodeURIComponent(slug);
// // // //     if (!decoded.trim()) return undefined;
// // // //     return fromSlug(decoded);
// // // //   } catch {
// // // //     if (!slug.trim()) return undefined;
// // // //     return fromSlug(slug);
// // // //   }
// // // // };

// // // // const months = [
// // // //   "january",
// // // //   "february",
// // // //   "march",
// // // //   "april",
// // // //   "may",
// // // //   "june",
// // // //   "july",
// // // //   "august",
// // // //   "september",
// // // //   "october",
// // // //   "november",
// // // //   "december",
// // // // ];


// // // // const ProductwisePerformance: React.FC<ProductwisePerformanceProps> = ({
// // // //   productname: propProductName,
// // // // }) => {
// // // //   const {
// // // //     homeCurrency,
// // // //     setHomeCurrency,
// // // //     convertToHomeCurrency,
// // // //     formatHomeAmount,
// // // //   } = useFx();

// // // //   const params = useParams();
// // // //   const router = useRouter();

// // // //   const rawSlug = params?.productname as string | undefined;
// // // //   const urlProductName = normalizeProductSlug(rawSlug);

// // // //   const countryName = (params?.countryName as string) || undefined;
// // // //   const monthParam = (params?.month as string) || undefined;
// // // //   const yearParam = (params?.year as string) || undefined;

// // // //   const productname = propProductName || urlProductName || "";

// // // //   const [data, setData] = useState<APIResponse | null>(null);
// // // //   const [loading, setLoading] = useState(false);
// // // //   const [error, setError] = useState<string>("");

// // // //   const authToken =
// // // //     typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;


// // // //   const [range, setRange] = useState<Range>(() => {

// // // //     return "monthly";
// // // //   });



// // // //   const [selectedMonth, setSelectedMonth] = useState<string>(() => {
// // // //     if (monthParam) return monthParam.toLowerCase(); // URL wins

// // // //     if (typeof window !== "undefined") {
// // // //       const raw = localStorage.getItem("latestFetchedPeriod");
// // // //       if (raw) {
// // // //         try {
// // // //           const parsed = JSON.parse(raw) as { month?: string; year?: string };
// // // //           if (parsed.month) return parsed.month.toLowerCase(); // e.g. "november"
// // // //         } catch {
// // // //           /* ignore */
// // // //         }
// // // //       }
// // // //     }

// // // //     return "";
// // // //   });


// // // //   const [selectedQuarter, setSelectedQuarter] = useState("1");
// // // //   useEffect(() => {
// // // //     if (!selectedMonth) return;
// // // //     const idx = months.indexOf(selectedMonth.toLowerCase());
// // // //     if (idx >= 0) {
// // // //       const q = Math.floor(idx / 3) + 1; // 0–2->Q1, 3–5->Q2, etc.
// // // //       setSelectedQuarter(String(q));
// // // //     }
// // // //   }, [selectedMonth]);


// // // //   const [selectedYear, setSelectedYear] = useState<number | "">(() => {
// // // //     if (yearParam) return Number(yearParam);

// // // //     if (typeof window !== "undefined") {
// // // //       const raw = localStorage.getItem("latestFetchedPeriod");
// // // //       if (raw) {
// // // //         try {
// // // //           const parsed = JSON.parse(raw) as { month?: string; year?: string };
// // // //           if (parsed.year) return Number(parsed.year); // e.g. 2025
// // // //         } catch {
// // // //           /* ignore */
// // // //         }
// // // //       }
// // // //     }

// // // //     return "";
// // // //   });


// // // //   const [selectedCountries, setSelectedCountries] = useState<
// // // //     Record<CountryKey, boolean>
// // // //   >({
// // // //     uk: true,
// // // //     us: true,
// // // //     global: true,
// // // //   });

// // // //   const years = useMemo(
// // // //     () => Array.from({ length: 2 }, (_, i) => new Date().getFullYear() - i),
// // // //     []
// // // //   );

// // // //   const isProductSelected = !!productname;
// // // //   const hasYear = selectedYear !== "" && selectedYear !== undefined;

// // // //   const isPeriodComplete =
// // // //     (range === "yearly" && hasYear) ||
// // // //     (range === "quarterly" && hasYear && !!selectedQuarter) ||
// // // //     (range === "monthly" && hasYear && !!selectedMonth);

// // // //   const canShowResults = isProductSelected && isPeriodComplete;

// // // //   /* ---------- Tie homeCurrency to sidebar region ---------- */
// // // //   useEffect(() => {
// // // //     const scope = (countryName || "global").toLowerCase();

// // // //     if (scope === "uk") {
// // // //       setHomeCurrency("GBP");
// // // //     } else {
// // // //       // Global or anything else → default to USD
// // // //       setHomeCurrency("USD");
// // // //     }
// // // //   }, [countryName, setHomeCurrency]);

// // // //   /* ---------- Country toggle ---------- */
// // // //   const handleCountryChange = (country: CountryKey) => {
// // // //     setSelectedCountries((prev) => ({
// // // //       ...prev,
// // // //       [country]: !(prev[country] ?? true),
// // // //     }));
// // // //   };

// // // //   /* ---------- Product select handler ---------- */
// // // // const handleProductSelect = (selectedProductName: string) => {
// // // //   const base = "/productwiseperformance";
// // // //   const slug = toSlug(selectedProductName);

// // // //   let month = selectedMonth;
// // // //   let year = selectedYear;

// // // //   // Fallback: if filters are empty but we have latestFetchedPeriod in LS
// // // //   if ((!month || !year) && typeof window !== "undefined") {
// // // //     try {
// // // //       const raw = localStorage.getItem("latestFetchedPeriod");
// // // //       if (raw) {
// // // //         const parsed = JSON.parse(raw) as { month?: string; year?: string };
// // // //         if (!month && parsed.month) month = parsed.month.toLowerCase();
// // // //         if (!year && parsed.year) year = Number(parsed.year);
// // // //       }
// // // //     } catch {
// // // //       // ignore
// // // //     }
// // // //   }

// // // //   const to = `${base}/${slug}/${countryName ?? ""}/${month || ""}/${year || ""}`;
// // // //   router.push(to);
// // // // };


// // // //   /* ---------- Fetch data ---------- */
// // // //   const fetchProductData = async () => {
// // // //     if (!canShowResults) return;

// // // //     setLoading(true);
// // // //     setError("");

// // // //     try {
// // // //       const countries: CountryKey[] = ["global", "uk", "us"];

// // // //       const backendTimeRange =
// // // //         range === "yearly"
// // // //           ? "Yearly"
// // // //           : range === "quarterly"
// // // //             ? "Quarterly"
// // // //             : "Monthly";

// // // //       const payload: any = {
// // // //         product_name: productname,
// // // //         time_range: backendTimeRange,
// // // //         year: selectedYear,
// // // //         countries,
// // // //       };

// // // //       if (range === "quarterly") {
// // // //         payload.quarter = selectedQuarter;
// // // //       }

// // // //       if (range === "monthly") {
// // // //         payload.month = selectedMonth;
// // // //       }

// // // //       const res = await fetch("http://localhost:5000/ProductwisePerformance", {
// // // //         method: "POST",
// // // //         headers: {
// // // //           "Content-Type": "application/json",
// // // //           Authorization: `Bearer ${authToken ?? ""}`,
// // // //         },
// // // //         body: JSON.stringify(payload),
// // // //       });

// // // //       const json: APIResponse | any = await res.json().catch(() => null);

// // // //       if (!res.ok || !json?.success) {
// // // //         const errMsg =
// // // //           (json && (json.error || json.message)) ||
// // // //           `HTTP error! status: ${res.status}`;
// // // //         throw new Error(errMsg);
// // // //       }

// // // //       setData(json as APIResponse);
// // // //     } catch (e: any) {
// // // //       console.error("API Error:", e);
// // // //       setError(e?.message || "Failed to fetch data from server");
// // // //     } finally {
// // // //       setLoading(false);
// // // //     }
// // // //   };

// // // //   useEffect(() => {
// // // //     if (!canShowResults) return;
// // // //     fetchProductData();
// // // //     // eslint-disable-next-line react-hooks/exhaustive-deps
// // // //   }, [
// // // //     productname,
// // // //     selectedYear,
// // // //     range,
// // // //     selectedQuarter,
// // // //     selectedMonth,
// // // //     canShowResults,
// // // //   ]);

// // // //   /* ---------- Non-empty countries (excluding global) ---------- */
// // // //   const nonEmptyCountriesFromApi = useMemo(() => {
// // // //     if (!data?.data) return [] as CountryKey[];

// // // //     return (Object.entries(data.data) as [CountryKey, any][])
// // // //       .filter(([country, countryObj]) => {
// // // //         if (country.toLowerCase() === "global") return false;
// // // //         const rows = Array.isArray(countryObj?.monthly)
// // // //           ? (countryObj.monthly as MonthDatum[])
// // // //           : [];
// // // //         return rows.some(
// // // //           (m) =>
// // // //             m.net_sales !== 0 ||
// // // //             m.quantity !== 0 ||
// // // //             m.profit !== 0
// // // //         );
// // // //       })
// // // //       .map(([country]) => country);
// // // //   }, [data]);

// // // //   /* ---------- Charts (Net Sales, Quantity, Profit) ---------- */
// // // //   const buildChartJSData = () => {
// // // //     if (!data?.data) return [null, null, null];

// // // //     const allMonths = new Set<string>();
// // // //     Object.values(data.data).forEach((countryObj: any) => {
// // // //       const monthly = Array.isArray(countryObj?.monthly)
// // // //         ? (countryObj.monthly as MonthDatum[])
// // // //         : [];
// // // //       monthly.forEach((m) => allMonths.add(m.month));
// // // //     });

// // // //     const labels = Array.from(allMonths).sort(
// // // //       (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
// // // //     );

// // // //     const getMetric = (
// // // //       country: CountryKey,
// // // //       month: string,
// // // //       metric: keyof MonthDatum
// // // //     ) => {
// // // //       const countryObj = (data.data as any)[country];
// // // //       const monthly: MonthDatum[] = Array.isArray(countryObj?.monthly)
// // // //         ? countryObj.monthly
// // // //         : [];
// // // //       const found = monthly.find((m) => m.month === month);
// // // //       return found ? (found[metric] as number) : 0;
// // // //     };

// // // //     const makeDataset = (
// // // //       country: string,
// // // //       metric: keyof MonthDatum,
// // // //       labelSuffix: string
// // // //     ) => {
// // // //       const lower = country.toLowerCase();

// // // //       const dataSeries = labels.map((month) => {
// // // //         const ukVal = getMetric("uk", month, metric);
// // // //         const usVal = getMetric("us", month, metric); // may be 0 if no US data
// // // //         const rawGlobal = getMetric("global", month, metric);
// // // //         const isMoney = metric === "net_sales" || metric === "profit";

// // // //         if (lower === "global") {
// // // //           if (metric === "quantity") {
// // // //             const sumUnits = ukVal + usVal;
// // // //             return sumUnits !== 0 ? sumUnits : rawGlobal;
// // // //           }
// // // //           const sumMoney = ukVal * GBP_TO_USD_RATE + usVal;
// // // //           if (sumMoney !== 0) return sumMoney;
// // // //           return rawGlobal;
// // // //         }

// // // //         if (lower === "uk") {
// // // //           if (metric === "quantity") return ukVal;
// // // //           return ukVal * GBP_TO_USD_RATE;
// // // //         }

// // // //         if (lower === "us") {
// // // //           return usVal;
// // // //         }

// // // //         const raw = getMetric(country as CountryKey, month, metric);
// // // //         if (!isMoney) return raw;
// // // //         return raw;
// // // //       });

// // // //       const isGlobal = lower === "global";

// // // //       return {
// // // //         label: `${formatCountryLabel(country)} ${labelSuffix}`,
// // // //         data: dataSeries,
// // // //         borderColor: getCountryColor(country),
// // // //         backgroundColor: getCountryColor(country),
// // // //         tension: 0.1,
// // // //         pointRadius: 3,
// // // //         fill: false,
// // // //         borderDash: isGlobal ? [6, 4] : [],
// // // //         borderWidth: 2,
// // // //         order: isGlobal ? 99 : 0,
// // // //       };
// // // //     };

// // // //     const metrics: { metric: keyof MonthDatum; suffix: string }[] = [
// // // //       { metric: "net_sales", suffix: "Net Sales" },
// // // //       { metric: "quantity", suffix: "Quantity" },
// // // //       { metric: "profit", suffix: "Profit" },
// // // //     ];

// // // //     const charts = metrics.map(({ metric, suffix }) => {
// // // //       const visibleCountries: CountryKey[] = [];

// // // //       if (selectedCountries["global"] ?? true) {
// // // //         visibleCountries.push("global");
// // // //       }

// // // //       visibleCountries.push(
// // // //         ...nonEmptyCountriesFromApi.filter(
// // // //           (c) => selectedCountries[c] ?? true
// // // //         )
// // // //       );

// // // //       const datasets = visibleCountries.map((country) =>
// // // //         makeDataset(country, metric, suffix)
// // // //       );

// // // //       return {
// // // //         labels,
// // // //         datasets,
// // // //       };
// // // //     });

// // // //     return charts;
// // // //   };

// // // //   const chartDataList = buildChartJSData();

// // // //   const chartOptions = {
// // // //     responsive: true,
// // // //     maintainAspectRatio: false,
// // // //     plugins: {
// // // //       legend: { display: false },
// // // //       tooltip: {
// // // //         callbacks: {
// // // //           label: (context: any) => {
// // // //             const value = context.parsed.y as number;
// // // //             const datasetLabel = context.dataset.label as string;
// // // //             const metricPart = (
// // // //               datasetLabel
// // // //                 .split(" ")
// // // //                 .slice(1)
// // // //                 .join(" ") || ""
// // // //             ).toLowerCase();

// // // //             if (
// // // //               metricPart.includes("quantity") ||
// // // //               metricPart.includes("units")
// // // //             ) {
// // // //               return `${datasetLabel}: ${value.toLocaleString()}`;
// // // //             }

// // // //             return `${datasetLabel}: ${formatCurrencyByCountry("us", value)}`;
// // // //           },
// // // //         },
// // // //       },
// // // //     },
// // // //     scales: {
// // // //       x: { title: { display: true, text: "Month" } },
// // // //       y: {
// // // //         title: { display: true, text: "Amount ($)" },
// // // //         min: 0,
// // // //         ticks: {
// // // //           padding: 0,
// // // //         },
// // // //       },
// // // //     },
// // // //   } as const;

// // // //   /* ---------- Chart carousel ---------- */
// // // //   const [currentIndex, setCurrentIndex] = useState(0);
// // // //   const handlePrev = () =>
// // // //     setCurrentIndex((i) => (i === 0 ? chartDataList.length - 1 : i - 1));
// // // //   const handleNext = () =>
// // // //     setCurrentIndex((i) => (i === chartDataList.length - 1 ? 0 : i + 1));

// // // //   const yearShort =
// // // //     selectedYear === "" ? "" : selectedYear.toString().slice(-2);

// // // //   const cap = (s: string) => (s ? s[0].toUpperCase() + s.slice(1) : "");

// // // //   const getTitle = () => {
// // // //     if (range === "yearly") return `Year'${yearShort}`;
// // // //     if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
// // // //     return selectedMonth
// // // //       ? `${cap(selectedMonth)}'${yearShort}`
// // // //       : `Year'${yearShort}`;
// // // //   };

// // // //   const getHeadingPeriod = () => {
// // // //     if (range === "yearly") return `YTD'${yearShort}`;
// // // //     if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
// // // //     if (range === "monthly" && selectedMonth) {
// // // //       return `${cap(selectedMonth)}'${yearShort}`;
// // // //     }
// // // //     return "";
// // // //   };

// // // //   /* ---------- Summary cards data (with FX + Global logic) ---------- */
// // // //   const cards = useMemo(() => {
// // // //     if (!data?.data) return [] as { country: string; stats: any }[];

// // // //     const selectedScope = (countryName || "global").toLowerCase();

// // // //     // Collect all months across all countries
// // // //     const allMonths = new Set<string>();
// // // //     Object.values(data.data).forEach((countryObj: any) => {
// // // //       const monthly = Array.isArray(countryObj?.monthly)
// // // //         ? (countryObj.monthly as MonthDatum[])
// // // //         : [];
// // // //       monthly.forEach((m) => allMonths.add(m.month));
// // // //     });

// // // //     const labels = Array.from(allMonths).sort(
// // // //       (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
// // // //     );

// // // //     // Helper to get monthly rows for a given country code
// // // //     const getMonthly = (code: string): MonthDatum[] => {
// // // //       const obj = (data.data as any)[code];
// // // //       if (!obj || !Array.isArray(obj.monthly)) return [];
// // // //       return obj.monthly;
// // // //     };

// // // //     const ukMonthly = getMonthly("uk");
// // // //     const usMonthly = getMonthly("us");
// // // //     const globalMonthlyBackend = getMonthly("global");

// // // //     return Object.entries(data.data).map(([country, countryObjRaw]) => {
// // // //       const key = country.toLowerCase();
// // // //       const monthlyRaw: MonthDatum[] = Array.isArray(
// // // //         (countryObjRaw as any)?.monthly
// // // //       )
// // // //         ? (countryObjRaw as any).monthly
// // // //         : [];

// // // //       let processedData: MonthDatum[] = monthlyRaw;

// // // //       // ---------- GLOBAL CARD LOGIC ----------
// // // //       if (key === "global") {
// // // //         // UK scope → Global should mirror UK in GBP
// // // //         if (homeCurrency === "GBP" && selectedScope === "uk") {
// // // //           processedData = ukMonthly ? [...ukMonthly] : [];
// // // //         } else {
// // // //           // Global scope → aggregate all countries into homeCurrency (USD now)
// // // //           processedData = labels.map((month) => {
// // // //             const uk = ukMonthly.find((m) => m.month === month);
// // // //             const us = usMonthly.find((m) => m.month === month);
// // // //             const g = globalMonthlyBackend.find((m) => m.month === month);

// // // //             const ukNetHome = uk
// // // //               ? convertToHomeCurrency(uk.net_sales, "GBP")
// // // //               : 0;
// // // //             const usNetHome = us
// // // //               ? convertToHomeCurrency(us.net_sales, "USD")
// // // //               : 0;

// // // //             const ukProfitHome = uk
// // // //               ? convertToHomeCurrency(uk.profit, "GBP")
// // // //               : 0;
// // // //             const usProfitHome = us
// // // //               ? convertToHomeCurrency(us.profit, "USD")
// // // //               : 0;

// // // //             const sumNetSales = ukNetHome + usNetHome;
// // // //             const sumProfit = ukProfitHome + usProfitHome;
// // // //             const sumUnits = (uk?.quantity || 0) + (us?.quantity || 0);

// // // //             const fallbackNet = g
// // // //               ? convertToHomeCurrency(g.net_sales, "USD")
// // // //               : 0;
// // // //             const fallbackProfit = g
// // // //               ? convertToHomeCurrency(g.profit, "USD")
// // // //               : 0;
// // // //             const fallbackUnits = g?.quantity || 0;

// // // //             return {
// // // //               month,
// // // //               net_sales: sumNetSales !== 0 ? sumNetSales : fallbackNet,
// // // //               profit: sumProfit !== 0 ? sumProfit : fallbackProfit,
// // // //               quantity: sumUnits !== 0 ? sumUnits : fallbackUnits,
// // // //             };
// // // //           });
// // // //         }
// // // //       } else {
// // // //         // ---------- NON-GLOBAL CARDS ----------
// // // //         // When in Global scope, convert each country into homeCurrency (USD).
// // // //         // Otherwise, keep original currency (GBP for UK, USD for US).
// // // //         const fromCurrency = key === "uk" ? "GBP" : ("USD" as const);
// // // //         const shouldConvert =
// // // //           homeCurrency === "USD" && selectedScope === "global";

// // // //         processedData = shouldConvert
// // // //           ? monthlyRaw.map((m) => ({
// // // //             ...m,
// // // //             net_sales: convertToHomeCurrency(m.net_sales, fromCurrency),
// // // //             profit: convertToHomeCurrency(m.profit, fromCurrency),
// // // //           }))
// // // //           : monthlyRaw;
// // // //       }

// // // //       // ---------- Totals / averages based on processedData ----------
// // // //       const totalSales = processedData.reduce((s, m) => s + m.net_sales, 0);
// // // //       const totalProfit = processedData.reduce((s, m) => s + m.profit, 0);
// // // //       const totalUnits = processedData.reduce((s, m) => s + m.quantity, 0);

// // // //       const gross_margin_avg =
// // // //         totalSales > 0 ? (totalProfit / totalSales) * 100 : 0;

// // // //       const monthsWithSales = processedData.filter((m) => m.net_sales > 0);

// // // //       const avgSales =
// // // //         monthsWithSales.length > 0 ? totalSales / monthsWithSales.length : 0;

// // // //       const avgSellingPrice = totalUnits > 0 ? totalSales / totalUnits : 0;

// // // //       const avgMonthlyProfit =
// // // //         processedData.length > 0 ? totalProfit / processedData.length : 0;

// // // //       const maxSalesMonth =
// // // //         processedData.length > 0
// // // //           ? processedData.reduce((max, m) =>
// // // //             m.net_sales > max.net_sales ? m : max
// // // //           )
// // // //           : {
// // // //             month: "",
// // // //             net_sales: 0,
// // // //             quantity: 0,
// // // //             profit: 0,
// // // //           };

// // // //       const maxUnitsMonth =
// // // //         processedData.length > 0
// // // //           ? processedData.reduce((max, m) =>
// // // //             m.quantity > max.quantity ? m : max
// // // //           )
// // // //           : {
// // // //             month: "",
// // // //             net_sales: 0,
// // // //             quantity: 0,
// // // //             profit: 0,
// // // //           };

// // // //       return {
// // // //         country,
// // // //         stats: {
// // // //           totalSales,
// // // //           totalProfit,
// // // //           totalUnits,
// // // //           gross_margin_avg,
// // // //           avgSales,
// // // //           avgSellingPrice,
// // // //           avgMonthlyProfit,
// // // //           maxSalesMonth,
// // // //           maxUnitsMonth,
// // // //         },
// // // //       };
// // // //     });
// // // //   }, [data, countryName, homeCurrency, convertToHomeCurrency]);

// // // //   const orderedCards = useMemo(() => {
// // // //     if (!cards.length) return [];
// // // //     const global = cards.filter(
// // // //       (c) => c.country.toLowerCase() === "global"
// // // //     );
// // // //     const others = cards.filter(
// // // //       (c) => c.country.toLowerCase() !== "global"
// // // //     );
// // // //     return [...global, ...others];
// // // //   }, [cards]);

// // // //   /* ---------- Render ---------- */
// // // //   return (
// // // //     <div className="w-full">
// // // //       <ProductwiseHeader
// // // //         canShowResults={canShowResults}
// // // //         countryName={countryName}
// // // //         productname={productname}
// // // //         headingPeriod={getHeadingPeriod()}
// // // //       />

// // // //       <FiltersAndSearchRow
// // // //         range={range}
// // // //         selectedMonth={selectedMonth}
// // // //         selectedQuarter={selectedQuarter}
// // // //         selectedYear={selectedYear}
// // // //         years={years}
// // // //         authToken={authToken}
// // // //         onRangeChange={(v) => setRange(v)}
// // // //         onMonthChange={(val) => setSelectedMonth(val)}
// // // //         onQuarterChange={(val) => {
// // // //           const num = val.replace("Q", "");
// // // //           setSelectedQuarter(num || "1");
// // // //         }}
// // // //         onYearChange={(val) => {
// // // //           if (!val) {
// // // //             setSelectedYear("");
// // // //           } else {
// // // //             setSelectedYear(Number(val));
// // // //           }
// // // //         }}
// // // //         onProductSelect={handleProductSelect}
// // // //       />

// // // //       {!canShowResults && (
// // // //         <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
// // // //           <div className="flex items-center">
// // // //             <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
// // // //             <span>
// // // //               Search a product and choose the period to view SKU-wise
// // // //               performance.
// // // //             </span>
// // // //           </div>
// // // //         </div>
// // // //       )}

// // // //       {canShowResults && loading && (
// // // //         <div className="flex flex-col items-center justify-center py-12 textcenter">
// // // //           <Loader
// // // //             src="/infinity-unscreen.gif"
// // // //             size={150}
// // // //             transparent
// // // //             roundedClass="rounded-none"
// // // //             backgroundClass="bg-transparent"
// // // //             respectReducedMotion
// // // //           />
// // // //         </div>
// // // //       )}

// // // //       {canShowResults && !!error && (
// // // //         <div className="mb-8 rounded-2xl border border-red-200 bg-red-50 p-6">
// // // //           <div className="flex items-center gap-3 text-red-700">
// // // //             <span className="text-xl">❌</span>
// // // //             <p className="m-0 font-medium">{error}</p>
// // // //           </div>
// // // //         </div>
// // // //       )}

// // // //       {canShowResults && data && !loading && (
// // // //         <div className="flex flex-col">
// // // //           <TrendChartSection
// // // //             productname={productname}
// // // //             title={getTitle()}
// // // //             chartDataList={chartDataList}
// // // //             chartOptions={chartOptions}
// // // //             currentIndex={currentIndex}
// // // //             onPrev={handlePrev}
// // // //             onNext={handleNext}
// // // //             nonEmptyCountriesFromApi={nonEmptyCountriesFromApi}
// // // //             selectedCountries={selectedCountries}
// // // //             onToggleCountry={handleCountryChange}
// // // //           />

// // // //           <div className="mt-8">
// // // //             <div className="grid gap-5 grid-cols-1 md:grid-cols-[repeat(auto-fit,minmax(260px,1fr))]">
// // // //               {orderedCards.map((card) => {
// // // //                 const key = card.country.toLowerCase();
// // // //                 const isGlobal = key === "global";

// // // //                 if (
// // // //                   !isGlobal &&
// // // //                   card.stats.totalSales === 0 &&
// // // //                   card.stats.totalUnits === 0 &&
// // // //                   card.stats.totalProfit === 0
// // // //                 ) {
// // // //                   return null;
// // // //                 }

// // // //                 return (
// // // //                   <CountryCard
// // // //                     key={key}
// // // //                     country={card.country}
// // // //                     stats={card.stats}
// // // //                     selectedYear={selectedYear}
// // // //                     homeCurrency={homeCurrency}
// // // //                   />
// // // //                 );
// // // //               })}
// // // //             </div>
// // // //           </div>
// // // //         </div>
// // // //       )}
// // // //     </div>
// // // //   );
// // // // };

// // // // export default ProductwisePerformance;









































// // // // components/productwise/ProductwisePerformance.tsx
// // // "use client";

// // // import React, { useEffect, useMemo, useState } from "react";
// // // import { useParams, useRouter } from "next/navigation";
// // // import "@/lib/chartSetup";
// // // import {
// // //   Chart as ChartJS,
// // //   CategoryScale,
// // //   LinearScale,
// // //   PointElement,
// // //   LineElement,
// // //   Title as ChartTitle,
// // //   Tooltip,
// // //   Legend,
// // //   Filler,
// // // } from "chart.js";

// // // import Loader from "@/components/loader/Loader";

// // // import {
// // //   APIResponse,
// // //   CountryKey,
// // //   GBP_TO_USD_RATE, // still used for charts only
// // //   MonthDatum,
// // //   Range,
// // //   formatCountryLabel,
// // //   formatCurrencyByCountry,
// // //   getCountryColor,
// // //   monthOrder,
// // // } from "@/components/productwise/productwiseHelpers";
// // // import CountryCard from "@/components/productwise/CountryCard";
// // // import TrendChartSection from "@/components/productwise/TrendChartSection";
// // // import ProductwiseHeader from "@/components/productwise/ProductwiseHeader";
// // // import FiltersAndSearchRow from "@/components/productwise/FiltersAndSearchRow";
// // // import { useFx } from "@/components/dashboard/useFx";

// // // ChartJS.register(
// // //   CategoryScale,
// // //   LinearScale,
// // //   PointElement,
// // //   LineElement,
// // //   ChartTitle,
// // //   Tooltip,
// // //   Legend,
// // //   Filler
// // // );

// // // interface ProductwisePerformanceProps {
// // //   productname?: string;
// // // }

// // // /* ---------- Slug helpers ---------- */
// // // const toSlug = (name: string) =>
// // //   name
// // //     .trim()
// // //     .toLowerCase()
// // //     .replace(/\s*\+\s*/g, " plus ")
// // //     .replace(/\s+/g, "-");

// // // const fromSlug = (slug: string) =>
// // //   slug
// // //     .replace(/-/g, " ")
// // //     .replace(/\bplus\b/gi, "+")
// // //     .replace(/\s+/g, " ")
// // //     .trim();

// // // const normalizeProductSlug = (slug?: string) => {
// // //   if (!slug) return undefined;

// // //   try {
// // //     const decoded = decodeURIComponent(slug);
// // //     if (!decoded.trim()) return undefined;
// // //     return fromSlug(decoded);
// // //   } catch {
// // //     if (!slug.trim()) return undefined;
// // //     return fromSlug(slug);
// // //   }
// // // };

// // // const months = [
// // //   "january",
// // //   "february",
// // //   "march",
// // //   "april",
// // //   "may",
// // //   "june",
// // //   "july",
// // //   "august",
// // //   "september",
// // //   "october",
// // //   "november",
// // //   "december",
// // // ];

// // // const ProductwisePerformance: React.FC<ProductwisePerformanceProps> = ({
// // //   productname: propProductName,
// // // }) => {
// // //   const {
// // //     homeCurrency,
// // //     setHomeCurrency,
// // //     convertToHomeCurrency,
// // //   } = useFx();

// // //   const params = useParams();
// // //   const router = useRouter();

// // //   const rawSlug = params?.productname as string | undefined;
// // //   const urlProductName = normalizeProductSlug(rawSlug);

// // //   const countryName = (params?.countryName as string) || undefined;
// // //   const monthParam = (params?.month as string) || undefined;
// // //   const yearParam = (params?.year as string) || undefined;

// // //   const productname = propProductName || urlProductName || "";

// // //   const [data, setData] = useState<APIResponse | null>(null);
// // //   const [loading, setLoading] = useState(false);
// // //   const [error, setError] = useState<string>("");

// // //   const authToken =
// // //     typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

// // //   const [range, setRange] = useState<Range>(() => "monthly");

// // //   const [selectedMonth, setSelectedMonth] = useState<string>(() => {
// // //     if (monthParam) return monthParam.toLowerCase(); // URL wins

// // //     if (typeof window !== "undefined") {
// // //       const raw = localStorage.getItem("latestFetchedPeriod");
// // //       if (raw) {
// // //         try {
// // //           const parsed = JSON.parse(raw) as { month?: string; year?: string };
// // //           if (parsed.month) return parsed.month.toLowerCase();
// // //         } catch {
// // //           /* ignore */
// // //         }
// // //       }
// // //     }

// // //     return "";
// // //   });

// // //   const [selectedQuarter, setSelectedQuarter] = useState("1");

// // //   useEffect(() => {
// // //     if (!selectedMonth) return;
// // //     const idx = months.indexOf(selectedMonth.toLowerCase());
// // //     if (idx >= 0) {
// // //       const q = Math.floor(idx / 3) + 1;
// // //       setSelectedQuarter(String(q));
// // //     }
// // //   }, [selectedMonth]);

// // //   const [selectedYear, setSelectedYear] = useState<number | "">(() => {
// // //     if (yearParam) return Number(yearParam);

// // //     if (typeof window !== "undefined") {
// // //       const raw = localStorage.getItem("latestFetchedPeriod");
// // //       if (raw) {
// // //         try {
// // //           const parsed = JSON.parse(raw) as { month?: string; year?: string };
// // //           if (parsed.year) return Number(parsed.year);
// // //         } catch {
// // //           /* ignore */
// // //         }
// // //       }
// // //     }

// // //     return "";
// // //   });

// // //   const [selectedCountries, setSelectedCountries] = useState<
// // //     Record<CountryKey, boolean>
// // //   >({
// // //     uk: true,
// // //     us: true,
// // //     global: true,
// // //   });

// // //   const years = useMemo(
// // //     () => Array.from({ length: 2 }, (_, i) => new Date().getFullYear() - i),
// // //     []
// // //   );

// // //   const isProductSelected = !!productname;
// // //   const hasYear = selectedYear !== "" && selectedYear !== undefined;

// // //   const isPeriodComplete =
// // //     (range === "yearly" && hasYear) ||
// // //     (range === "quarterly" && hasYear && !!selectedQuarter) ||
// // //     (range === "monthly" && hasYear && !!selectedMonth);

// // //   const canShowResults = isProductSelected && isPeriodComplete;

// // //   /* ---------- Tie homeCurrency to sidebar region ---------- */
// // //   useEffect(() => {
// // //     const scope = (countryName || "global").toLowerCase();

// // //     if (scope === "uk") {
// // //       setHomeCurrency("GBP");
// // //     } else {
// // //       setHomeCurrency("USD");
// // //     }
// // //   }, [countryName, setHomeCurrency]);

// // //   /* ---------- Country toggle ---------- */
// // //   const handleCountryChange = (country: CountryKey) => {
// // //     setSelectedCountries((prev) => ({
// // //       ...prev,
// // //       [country]: !(prev[country] ?? true),
// // //     }));
// // //   };

// // //   /* ---------- Product select handler (used by search in chart) ---------- */
// // //   const handleProductSelect = (selectedProductName: string) => {
// // //     const base = "/productwiseperformance";
// // //     const slug = toSlug(selectedProductName);

// // //     let month = selectedMonth;
// // //     let year = selectedYear;

// // //     if ((!month || !year) && typeof window !== "undefined") {
// // //       try {
// // //         const raw = localStorage.getItem("latestFetchedPeriod");
// // //         if (raw) {
// // //           const parsed = JSON.parse(raw) as { month?: string; year?: string };
// // //           if (!month && parsed.month) month = parsed.month.toLowerCase();
// // //           if (!year && parsed.year) year = Number(parsed.year);
// // //         }
// // //       } catch {
// // //         // ignore
// // //       }
// // //     }

// // //     const to = `${base}/${slug}/${countryName ?? ""}/${month || ""}/${year || ""}`;
// // //     router.push(to);
// // //   };

// // //   /* ---------- Fetch data ---------- */
// // //   const fetchProductData = async () => {
// // //     if (!canShowResults) return;

// // //     setLoading(true);
// // //     setError("");

// // //     try {
// // //       const countries: CountryKey[] = ["global", "uk", "us"];

// // //       const backendTimeRange =
// // //         range === "yearly"
// // //           ? "Yearly"
// // //           : range === "quarterly"
// // //             ? "Quarterly"
// // //             : "Monthly";

// // //       const payload: any = {
// // //         product_name: productname,
// // //         time_range: backendTimeRange,
// // //         year: selectedYear,
// // //         countries,
// // //       };

// // //       if (range === "quarterly") {
// // //         payload.quarter = selectedQuarter;
// // //       }

// // //       if (range === "monthly") {
// // //         payload.month = selectedMonth;
// // //       }

// // //       const res = await fetch("http://localhost:5000/ProductwisePerformance", {
// // //         method: "POST",
// // //         headers: {
// // //           "Content-Type": "application/json",
// // //           Authorization: `Bearer ${authToken ?? ""}`,
// // //         },
// // //         body: JSON.stringify(payload),
// // //       });

// // //       const json: APIResponse | any = await res.json().catch(() => null);

// // //       if (!res.ok || !json?.success) {
// // //         const errMsg =
// // //           (json && (json.error || json.message)) ||
// // //           `HTTP error! status: ${res.status}`;
// // //         throw new Error(errMsg);
// // //       }

// // //       setData(json as APIResponse);
// // //     } catch (e: any) {
// // //       console.error("API Error:", e);
// // //       setError(e?.message || "Failed to fetch data from server");
// // //     } finally {
// // //       setLoading(false);
// // //     }
// // //   };

// // //   useEffect(() => {
// // //     if (!canShowResults) return;
// // //     fetchProductData();
// // //     // eslint-disable-next-line react-hooks/exhaustive-deps
// // //   }, [productname, selectedYear, range, selectedQuarter, selectedMonth, canShowResults]);

// // //   /* ---------- Non-empty countries (excluding global) ---------- */
// // //   const nonEmptyCountriesFromApi = useMemo(() => {
// // //     if (!data?.data) return [] as CountryKey[];

// // //     return (Object.entries(data.data) as [CountryKey, any][])
// // //       .filter(([country, countryObj]) => {
// // //         if (country.toLowerCase() === "global") return false;
// // //         const rows = Array.isArray(countryObj?.monthly)
// // //           ? (countryObj.monthly as MonthDatum[])
// // //           : [];
// // //         return rows.some(
// // //           (m) =>
// // //             m.net_sales !== 0 ||
// // //             m.quantity !== 0 ||
// // //             m.profit !== 0
// // //         );
// // //       })
// // //       .map(([country]) => country);
// // //   }, [data]);

// // //   /* ---------- Charts (Net Sales, Quantity, Profit) ---------- */
// // //   const buildChartJSData = () => {
// // //     if (!data?.data) return [null, null, null];

// // //     const allMonths = new Set<string>();
// // //     Object.values(data.data).forEach((countryObj: any) => {
// // //       const monthly = Array.isArray(countryObj?.monthly)
// // //         ? (countryObj.monthly as MonthDatum[])
// // //         : [];
// // //       monthly.forEach((m) => allMonths.add(m.month));
// // //     });

// // //     const labels = Array.from(allMonths).sort(
// // //       (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
// // //     );

// // //     const getMetric = (
// // //       country: CountryKey,
// // //       month: string,
// // //       metric: keyof MonthDatum
// // //     ) => {
// // //       const countryObj = (data.data as any)[country];
// // //       const monthly: MonthDatum[] = Array.isArray(countryObj?.monthly)
// // //         ? countryObj.monthly
// // //         : [];
// // //       const found = monthly.find((m) => m.month === month);
// // //       return found ? (found[metric] as number) : 0;
// // //     };

// // //     const makeDataset = (
// // //       country: string,
// // //       metric: keyof MonthDatum,
// // //       labelSuffix: string
// // //     ) => {
// // //       const lower = country.toLowerCase();

// // //       const dataSeries = labels.map((month) => {
// // //         const ukVal = getMetric("uk", month, metric);
// // //         const usVal = getMetric("us", month, metric);
// // //         const rawGlobal = getMetric("global", month, metric);
// // //         const isMoney = metric === "net_sales" || metric === "profit";

// // //         if (lower === "global") {
// // //           if (metric === "quantity") {
// // //             const sumUnits = ukVal + usVal;
// // //             return sumUnits !== 0 ? sumUnits : rawGlobal;
// // //           }
// // //           const sumMoney = ukVal * GBP_TO_USD_RATE + usVal;
// // //           if (sumMoney !== 0) return sumMoney;
// // //           return rawGlobal;
// // //         }

// // //         if (lower === "uk") {
// // //           if (metric === "quantity") return ukVal;
// // //           return ukVal * GBP_TO_USD_RATE;
// // //         }

// // //         if (lower === "us") {
// // //           return usVal;
// // //         }

// // //         const raw = getMetric(country as CountryKey, month, metric);
// // //         if (!isMoney) return raw;
// // //         return raw;
// // //       });

// // //       const isGlobal = lower === "global";

// // //       return {
// // //         label: `${formatCountryLabel(country)} ${labelSuffix}`,
// // //         data: dataSeries,
// // //         borderColor: getCountryColor(country),
// // //         backgroundColor: getCountryColor(country),
// // //         tension: 0.1,
// // //         pointRadius: 3,
// // //         fill: false,
// // //         borderDash: isGlobal ? [6, 4] : [],
// // //         borderWidth: 2,
// // //         order: isGlobal ? 99 : 0,
// // //       };
// // //     };

// // //     const metrics: { metric: keyof MonthDatum; suffix: string }[] = [
// // //       { metric: "net_sales", suffix: "Net Sales" },
// // //       { metric: "quantity", suffix: "Quantity" },
// // //       { metric: "profit", suffix: "Profit" },
// // //     ];

// // //     const charts = metrics.map(({ metric, suffix }) => {
// // //       const visibleCountries: CountryKey[] = [];

// // //       if (selectedCountries["global"] ?? true) {
// // //         visibleCountries.push("global");
// // //       }

// // //       visibleCountries.push(
// // //         ...nonEmptyCountriesFromApi.filter(
// // //           (c) => selectedCountries[c] ?? true
// // //         )
// // //       );

// // //       const datasets = visibleCountries.map((country) =>
// // //         makeDataset(country, metric, suffix)
// // //       );

// // //       return {
// // //         labels,
// // //         datasets,
// // //       };
// // //     });

// // //     return charts;
// // //   };

// // //   const chartDataList = buildChartJSData();

// // //   const chartOptions = {
// // //     responsive: true,
// // //     maintainAspectRatio: false,
// // //     plugins: {
// // //       legend: { display: false },
// // //       tooltip: {
// // //         callbacks: {
// // //           label: (context: any) => {
// // //             const value = context.parsed.y as number;
// // //             const datasetLabel = context.dataset.label as string;
// // //             const metricPart = (
// // //               datasetLabel
// // //                 .split(" ")
// // //                 .slice(1)
// // //                 .join(" ") || ""
// // //             ).toLowerCase();

// // //             if (
// // //               metricPart.includes("quantity") ||
// // //               metricPart.includes("units")
// // //             ) {
// // //               return `${datasetLabel}: ${value.toLocaleString()}`;
// // //             }

// // //             return `${datasetLabel}: ${formatCurrencyByCountry("us", value)}`;
// // //           },
// // //         },
// // //       },
// // //     },
// // //     scales: {
// // //       x: { title: { display: true, text: "Month" } },
// // //       y: {
// // //         title: { display: true, text: "Amount ($)" },
// // //         min: 0,
// // //         ticks: {
// // //           padding: 0,
// // //         },
// // //       },
// // //     },
// // //   } as const;

// // //   const yearShort =
// // //     selectedYear === "" ? "" : selectedYear.toString().slice(-2);

// // //   const cap = (s: string) => (s ? s[0].toUpperCase() + s.slice(1) : "");

// // //   const getTitle = () => {
// // //     if (range === "yearly") return `Year'${yearShort}`;
// // //     if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
// // //     return selectedMonth
// // //       ? `${cap(selectedMonth)}'${yearShort}`
// // //       : `Year'${yearShort}`;
// // //   };

// // //   const getHeadingPeriod = () => {
// // //     if (range === "yearly") return `YTD'${yearShort}`;
// // //     if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
// // //     if (range === "monthly" && selectedMonth) {
// // //       return `${cap(selectedMonth)}'${yearShort}`;
// // //     }
// // //     return "";
// // //   };

// // //   /* ---------- Summary cards (same as your latest version) ---------- */
// // //   const cards = useMemo(() => {
// // //     if (!data?.data) return [] as { country: string; stats: any }[];

// // //     const selectedScope = (countryName || "global").toLowerCase();

// // //     const allMonths = new Set<string>();
// // //     Object.values(data.data).forEach((countryObj: any) => {
// // //       const monthly = Array.isArray(countryObj?.monthly)
// // //         ? (countryObj.monthly as MonthDatum[])
// // //         : [];
// // //       monthly.forEach((m) => allMonths.add(m.month));
// // //     });

// // //     const labels = Array.from(allMonths).sort(
// // //       (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
// // //     );

// // //     const getMonthly = (code: string): MonthDatum[] => {
// // //       const obj = (data.data as any)[code];
// // //       if (!obj || !Array.isArray(obj.monthly)) return [];
// // //       return obj.monthly;
// // //     };

// // //     const ukMonthly = getMonthly("uk");
// // //     const usMonthly = getMonthly("us");
// // //     const globalMonthlyBackend = getMonthly("global");

// // //     return Object.entries(data.data).map(([country, countryObjRaw]) => {
// // //       const key = country.toLowerCase();
// // //       const monthlyRaw: MonthDatum[] = Array.isArray(
// // //         (countryObjRaw as any)?.monthly
// // //       )
// // //         ? (countryObjRaw as any).monthly
// // //         : [];

// // //       let processedData: MonthDatum[] = monthlyRaw;

// // //       if (key === "global") {
// // //         if (homeCurrency === "GBP" && selectedScope === "uk") {
// // //           processedData = ukMonthly ? [...ukMonthly] : [];
// // //         } else {
// // //           processedData = labels.map((month) => {
// // //             const uk = ukMonthly.find((m) => m.month === month);
// // //             const us = usMonthly.find((m) => m.month === month);
// // //             const g = globalMonthlyBackend.find((m) => m.month === month);

// // //             const ukNetHome = uk
// // //               ? convertToHomeCurrency(uk.net_sales, "GBP")
// // //               : 0;
// // //             const usNetHome = us
// // //               ? convertToHomeCurrency(us.net_sales, "USD")
// // //               : 0;

// // //             const ukProfitHome = uk
// // //               ? convertToHomeCurrency(uk.profit, "GBP")
// // //               : 0;
// // //             const usProfitHome = us
// // //               ? convertToHomeCurrency(us.profit, "USD")
// // //               : 0;

// // //             const sumNetSales = ukNetHome + usNetHome;
// // //             const sumProfit = ukProfitHome + usProfitHome;
// // //             const sumUnits = (uk?.quantity || 0) + (us?.quantity || 0);

// // //             const fallbackNet = g
// // //               ? convertToHomeCurrency(g.net_sales, "USD")
// // //               : 0;
// // //             const fallbackProfit = g
// // //               ? convertToHomeCurrency(g.profit, "USD")
// // //               : 0;
// // //             const fallbackUnits = g?.quantity || 0;

// // //             return {
// // //               month,
// // //               net_sales: sumNetSales !== 0 ? sumNetSales : fallbackNet,
// // //               profit: sumProfit !== 0 ? sumProfit : fallbackProfit,
// // //               quantity: sumUnits !== 0 ? sumUnits : fallbackUnits,
// // //             };
// // //           });
// // //         }
// // //       } else {
// // //         const fromCurrency = key === "uk" ? "GBP" : ("USD" as const);
// // //         const shouldConvert =
// // //           homeCurrency === "USD" && selectedScope === "global";

// // //         processedData = shouldConvert
// // //           ? monthlyRaw.map((m) => ({
// // //             ...m,
// // //             net_sales: convertToHomeCurrency(m.net_sales, fromCurrency),
// // //             profit: convertToHomeCurrency(m.profit, fromCurrency),
// // //           }))
// // //           : monthlyRaw;
// // //       }

// // //       const totalSales = processedData.reduce((s, m) => s + m.net_sales, 0);
// // //       const totalProfit = processedData.reduce((s, m) => s + m.profit, 0);
// // //       const totalUnits = processedData.reduce((s, m) => s + m.quantity, 0);

// // //       const gross_margin_avg =
// // //         totalSales > 0 ? (totalProfit / totalSales) * 100 : 0;

// // //       const monthsWithSales = processedData.filter((m) => m.net_sales > 0);

// // //       const avgSales =
// // //         monthsWithSales.length > 0 ? totalSales / monthsWithSales.length : 0;

// // //       const avgSellingPrice = totalUnits > 0 ? totalSales / totalUnits : 0;

// // //       const avgMonthlyProfit =
// // //         processedData.length > 0 ? totalProfit / processedData.length : 0;

// // //       const defaultMonth = {
// // //         month: "",
// // //         net_sales: 0,
// // //         quantity: 0,
// // //         profit: 0,
// // //       };

// // //       const maxSalesMonth =
// // //         processedData.length > 0
// // //           ? processedData.reduce((max, m) =>
// // //             m.net_sales > max.net_sales ? m : max
// // //           )
// // //           : defaultMonth;

// // //       const maxUnitsMonth =
// // //         processedData.length > 0
// // //           ? processedData.reduce((max, m) =>
// // //             m.quantity > max.quantity ? m : max
// // //           )
// // //           : defaultMonth;

// // //       return {
// // //         country,
// // //         stats: {
// // //           totalSales,
// // //           totalProfit,
// // //           totalUnits,
// // //           gross_margin_avg,
// // //           avgSales,
// // //           avgSellingPrice,
// // //           avgMonthlyProfit,
// // //           maxSalesMonth,
// // //           maxUnitsMonth,
// // //         },
// // //       };
// // //     });
// // //   }, [data, countryName, homeCurrency, convertToHomeCurrency]);

// // //   const orderedCards = useMemo(() => {
// // //     if (!cards.length) return [];
// // //     const global = cards.filter(
// // //       (c) => c.country.toLowerCase() === "global"
// // //     );
// // //     const others = cards.filter(
// // //       (c) => c.country.toLowerCase() !== "global"
// // //     );
// // //     return [...global, ...others];
// // //   }, [cards]);

// // //   /* ---------- Render ---------- */
// // //   return (
// // //     <div className="w-full">
// // //       <ProductwiseHeader
// // //         canShowResults={canShowResults}
// // //         countryName={countryName}
// // //         productname={productname}
// // //         headingPeriod={getHeadingPeriod()}
// // //       />


// // //       <FiltersAndSearchRow
// // //         range={range}
// // //         selectedMonth={selectedMonth}
// // //         selectedQuarter={selectedQuarter}
// // //         selectedYear={selectedYear}
// // //         years={years}
// // //         onRangeChange={(v) => setRange(v)}
// // //         onMonthChange={(val) => setSelectedMonth(val)}
// // //         onQuarterChange={(val) => {
// // //           const num = val.replace("Q", "");
// // //           setSelectedQuarter(num || "1");
// // //         }}
// // //         onYearChange={(val) => {
// // //           if (!val) {
// // //             setSelectedYear("");
// // //           } else {
// // //             setSelectedYear(Number(val));
// // //           }
// // //         }}
// // //       />


// // //       {!canShowResults && (
// // //         <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
// // //           <div className="flex items-center">
// // //             <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
// // //             <span>
// // //               Search a product and choose the period to view SKU-wise
// // //               performance.
// // //             </span>
// // //           </div>
// // //         </div>
// // //       )}

// // //       {canShowResults && loading && (
// // //         <div className="flex flex-col items-center justify-center py-12 text-center">
// // //           <Loader
// // //             src="/infinity-unscreen.gif"
// // //             size={150}
// // //             transparent
// // //             roundedClass="rounded-none"
// // //             backgroundClass="bg-transparent"
// // //             respectReducedMotion
// // //           />
// // //         </div>
// // //       )}

// // //       {canShowResults && !!error && (
// // //         <div className="mb-8 rounded-2xl border border-red-200 bg-red-50 p-6">
// // //           <div className="flex items-center gap-3 text-red-700">
// // //             <span className="text-xl">❌</span>
// // //             <p className="m-0 font-medium">{error}</p>
// // //           </div>
// // //         </div>
// // //       )}

// // //       {canShowResults && data && !loading && (
// // //         <div className="flex flex-col">
// // //           <TrendChartSection
// // //             productname={productname}
// // //             title={getTitle()}
// // //             chartDataList={chartDataList}
// // //             chartOptions={chartOptions}
// // //             nonEmptyCountriesFromApi={nonEmptyCountriesFromApi}
// // //             selectedCountries={selectedCountries}
// // //             onToggleCountry={handleCountryChange}
// // //             authToken={authToken}
// // //             onProductSelect={handleProductSelect}
// // //           />

// // //           <div className="mt-8">
// // //             <div className="grid gap-5 grid-cols-1 md:grid-cols-[repeat(auto-fit,minmax(260px,1fr))]">
// // //               {orderedCards.map((card) => {
// // //                 const key = card.country.toLowerCase();
// // //                 const isGlobal = key === "global";

// // //                 if (
// // //                   !isGlobal &&
// // //                   card.stats.totalSales === 0 &&
// // //                   card.stats.totalUnits === 0 &&
// // //                   card.stats.totalProfit === 0
// // //                 ) {
// // //                   return null;
// // //                 }

// // //                 return (
// // //                   <CountryCard
// // //                     key={key}
// // //                     country={card.country}
// // //                     stats={card.stats}
// // //                     selectedYear={selectedYear}
// // //                     homeCurrency={homeCurrency}
// // //                   />
// // //                 );
// // //               })}
// // //             </div>
// // //           </div>
// // //         </div>
// // //       )}
// // //     </div>
// // //   );
// // // };

// // // export default ProductwisePerformance;









































// // // components/productwise/ProductwisePerformance.tsx
// // "use client";

// // import React, { useEffect, useMemo, useState } from "react";
// // import { useParams, useRouter } from "next/navigation";
// // import "@/lib/chartSetup";
// // import {
// //   Chart as ChartJS,
// //   CategoryScale,
// //   LinearScale,
// //   PointElement,
// //   LineElement,
// //   Title as ChartTitle,
// //   Tooltip,
// //   Legend,
// //   Filler,
// // } from "chart.js";

// // import Loader from "@/components/loader/Loader";

// // import {
// //   APIResponse,
// //   CountryKey,
// //   GBP_TO_USD_RATE,
// //   MonthDatum,
// //   Range,
// //   formatCountryLabel,
// //   formatCurrencyByCountry,
// //   getCountryColor,
// //   monthOrder,
// // } from "@/components/productwise/productwiseHelpers";
// // import CountryCard from "@/components/productwise/CountryCard";
// // import TrendChartSection from "@/components/productwise/TrendChartSection";
// // import ProductwiseHeader from "@/components/productwise/ProductwiseHeader";
// // import FiltersAndSearchRow from "@/components/productwise/FiltersAndSearchRow";
// // import { useFx } from "@/components/dashboard/useFx";

// // ChartJS.register(
// //   CategoryScale,
// //   LinearScale,
// //   PointElement,
// //   LineElement,
// //   ChartTitle,
// //   Tooltip,
// //   Legend,
// //   Filler
// // );

// // interface ProductwisePerformanceProps {
// //   productname?: string;
// // }

// // /* ---------- Slug helpers ---------- */
// // const toSlug = (name: string) =>
// //   name
// //     .trim()
// //     .toLowerCase()
// //     .replace(/\s*\+\s*/g, " plus ")
// //     .replace(/\s+/g, "-");

// // const fromSlug = (slug: string) =>
// //   slug
// //     .replace(/-/g, " ")
// //     .replace(/\bplus\b/gi, "+")
// //     .replace(/\s+/g, " ")
// //     .trim();

// // const normalizeProductSlug = (slug?: string) => {
// //   if (!slug) return undefined;

// //   try {
// //     const decoded = decodeURIComponent(slug);
// //     if (!decoded.trim()) return undefined;
// //     return fromSlug(decoded);
// //   } catch {
// //     if (!slug.trim()) return undefined;
// //     return fromSlug(slug);
// //   }
// // };

// // const months = [
// //   "january",
// //   "february",
// //   "march",
// //   "april",
// //   "may",
// //   "june",
// //   "july",
// //   "august",
// //   "september",
// //   "october",
// //   "november",
// //   "december",
// // ];

// // const cap = (s: string) => (s ? s[0].toUpperCase() + s.slice(1) : "");

// // // helper: latest fetched month from localStorage (for Last 12 Months logic)
// // const getLatestFetchedMonth = (): string | null => {
// //   if (typeof window === "undefined") return null;
// //   try {
// //     const raw = localStorage.getItem("latestFetchedPeriod");
// //     if (!raw) return null;
// //     const parsed = JSON.parse(raw) as { month?: string; year?: string };
// //     if (!parsed.month) return null;
// //     return parsed.month.toLowerCase();
// //   } catch {
// //     return null;
// //   }
// // };

// // const ProductwisePerformance: React.FC<ProductwisePerformanceProps> = ({
// //   productname: propProductName,
// // }) => {
// //   const {
// //     homeCurrency,
// //     setHomeCurrency,
// //     convertToHomeCurrency,
// //   } = useFx();

// //   const params = useParams();
// //   const router = useRouter();

// //   const rawSlug = params?.productname as string | undefined;
// //   const urlProductName = normalizeProductSlug(rawSlug);

// //   const countryName = (params?.countryName as string) || undefined;
// //   const monthParam = (params?.month as string) || undefined;
// //   const yearParam = (params?.year as string) || undefined;

// //   const productname = propProductName || urlProductName || "";

// //   const [data, setData] = useState<APIResponse | null>(null);
// //   const [loading, setLoading] = useState(false);
// //   const [error, setError] = useState<string>("");

// //   const authToken =
// //     typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

// //   // ---------- controls ----------
// //   // With new UI we only expose Quarterly + Last 12 Months, so default to quarterly
// //   const [range, setRange] = useState<Range>("quarterly");

// //   const [selectedMonth, setSelectedMonth] = useState<string>(() => {
// //     if (monthParam) return monthParam.toLowerCase();

// //     if (typeof window !== "undefined") {
// //       const raw = localStorage.getItem("latestFetchedPeriod");
// //       if (raw) {
// //         try {
// //           const parsed = JSON.parse(raw) as { month?: string; year?: string };
// //           if (parsed.month) return parsed.month.toLowerCase();
// //         } catch {
// //           /* ignore */
// //         }
// //       }
// //     }

// //     return "";
// //   });

// //   const [selectedQuarter, setSelectedQuarter] = useState("1");
// //   useEffect(() => {
// //     if (!selectedMonth) return;
// //     const idx = months.indexOf(selectedMonth.toLowerCase());
// //     if (idx >= 0) {
// //       const q = Math.floor(idx / 3) + 1;
// //       setSelectedQuarter(String(q));
// //     }
// //   }, [selectedMonth]);

// //   const [selectedYear, setSelectedYear] = useState<number | "">(() => {
// //     if (yearParam) return Number(yearParam);

// //     if (typeof window !== "undefined") {
// //       const raw = localStorage.getItem("latestFetchedPeriod");
// //       if (raw) {
// //         try {
// //           const parsed = JSON.parse(raw) as { month?: string; year?: string };
// //           if (parsed.year) return Number(parsed.year);
// //         } catch {
// //           /* ignore */
// //         }
// //       }
// //     }

// //     return "";
// //   });

// //   const [selectedCountries, setSelectedCountries] = useState<
// //     Record<CountryKey, boolean>
// //   >({
// //     uk: true,
// //     us: true,
// //     global: true,
// //   });

// //   const years = useMemo(
// //     () => Array.from({ length: 2 }, (_, i) => new Date().getFullYear() - i),
// //     []
// //   );

// //   const isProductSelected = !!productname;
// //   const hasYear = selectedYear !== "" && selectedYear !== undefined;

// //   const isPeriodComplete =
// //     (range === "yearly" && hasYear) ||
// //     (range === "quarterly" && hasYear && !!selectedQuarter) ||
// //     (range === "monthly" && hasYear && !!selectedMonth);

// //   const canShowResults = isProductSelected && isPeriodComplete;

// //   /* ---------- tie homeCurrency to sidebar region ---------- */
// //   useEffect(() => {
// //     const scope = (countryName || "global").toLowerCase();
// //     if (scope === "uk") {
// //       setHomeCurrency("GBP");
// //     } else {
// //       setHomeCurrency("USD");
// //     }
// //   }, [countryName, setHomeCurrency]);

// //   /* ---------- country toggle ---------- */
// //   const handleCountryChange = (country: CountryKey) => {
// //     setSelectedCountries((prev) => ({
// //       ...prev,
// //       [country]: !(prev[country] ?? true),
// //     }));
// //   };

// //   /* ---------- product select handler (from search in chart header) ---------- */
// //   const handleProductSelect = (selectedProductName: string) => {
// //     const base = "/productwiseperformance";
// //     const slug = toSlug(selectedProductName);

// //     let month = selectedMonth;
// //     let year = selectedYear;

// //     if ((!month || !year) && typeof window !== "undefined") {
// //       try {
// //         const raw = localStorage.getItem("latestFetchedPeriod");
// //         if (raw) {
// //           const parsed = JSON.parse(raw) as { month?: string; year?: string };
// //           if (!month && parsed.month) month = parsed.month.toLowerCase();
// //           if (!year && parsed.year) year = Number(parsed.year);
// //         }
// //       } catch {
// //         // ignore
// //       }
// //     }

// //     const to = `${base}/${slug}/${countryName ?? ""}/${month || ""}/${year || ""
// //       }`;
// //     router.push(to);
// //   };

// //   /* ---------- fetch data ---------- */
// //   const fetchProductData = async () => {
// //     if (!canShowResults) return;

// //     setLoading(true);
// //     setError("");

// //     try {
// //       const countries: CountryKey[] = ["global", "uk", "us"];

// //       const backendTimeRange =
// //         range === "yearly"
// //           ? "Yearly"
// //           : range === "quarterly"
// //             ? "Quarterly"
// //             : "Monthly";

// //       const payload: any = {
// //         product_name: productname,
// //         time_range: backendTimeRange,
// //         year: selectedYear,
// //         countries,
// //       };

// //       if (range === "quarterly") {
// //         payload.quarter = selectedQuarter;
// //       }

// //       if (range === "monthly") {
// //         payload.month = selectedMonth;
// //       }

// //       const res = await fetch("http://localhost:5000/ProductwisePerformance", {
// //         method: "POST",
// //         headers: {
// //           "Content-Type": "application/json",
// //           Authorization: `Bearer ${authToken ?? ""}`,
// //         },
// //         body: JSON.stringify(payload),
// //       });

// //       const json: APIResponse | any = await res.json().catch(() => null);

// //       if (!res.ok || !json?.success) {
// //         const errMsg =
// //           (json && (json.error || json.message)) ||
// //           `HTTP error! status: ${res.status}`;
// //         throw new Error(errMsg);
// //       }

// //       setData(json as APIResponse);
// //     } catch (e: any) {
// //       console.error("API Error:", e);
// //       setError(e?.message || "Failed to fetch data from server");
// //     } finally {
// //       setLoading(false);
// //     }
// //   };

// //   useEffect(() => {
// //     if (!canShowResults) return;
// //     fetchProductData();
// //     // eslint-disable-next-line react-hooks/exhaustive-deps
// //   }, [
// //     productname,
// //     selectedYear,
// //     range,
// //     selectedQuarter,
// //     selectedMonth,
// //     canShowResults,
// //   ]);

// //   /* ---------- non-empty countries (excluding global) ---------- */
// //   const nonEmptyCountriesFromApi = useMemo(() => {
// //     if (!data?.data) return [] as CountryKey[];

// //     return (Object.entries(data.data) as [CountryKey, any][])
// //       .filter(([country, countryObj]) => {
// //         if (country.toLowerCase() === "global") return false;
// //        const rows = Array.isArray(countryObj)
// //   ? countryObj
// //   : [];

// //         return rows.some(
// //           (m) => m.net_sales !== 0 || m.quantity !== 0 || m.profit !== 0
// //         );
// //       })
// //       .map(([country]) => country);
// //   }, [data]);

// //   /* ---------- chart data (Net Sales, Quantity, Profit) ---------- */

// //   const chartDataList = useMemo(() => {
// //     if (!data?.data) return [null, null, null];

// //     const scope = (countryName || "global").toLowerCase();
// //     const isUKScope = scope === "uk";

// //     const allMonths = new Set<string>();
// //     Object.values(data.data).forEach((countryObj: any) => {
// //       const monthly = Array.isArray(countryObj)
// //   ? countryObj
// //   : [];

// //       monthly.forEach((m) => allMonths.add(m.month));
// //     });

// //     const labels = Array.from(allMonths).sort(
// //       (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
// //     );

// //     const getMetric = (
// //       country: CountryKey,
// //       month: string,
// //       metric: keyof MonthDatum
// //     ) => {
// //       const countryObj = (data.data as any)[country];
// //      const monthly = Array.isArray(countryObj)
// //   ? countryObj
// //   : [];

// //       const found = monthly.find((m) => m.month === month);
// //       return found ? (found[metric] as number) : 0;
// //     };

// //     const makeDataset = (
// //       country: string,
// //       metric: keyof MonthDatum,
// //       labelSuffix: string
// //     ) => {
// //       const lower = country.toLowerCase();

// //       const dataSeries = labels.map((month) => {
// //         const ukVal = getMetric("uk", month, metric);
// //         const usVal = getMetric("us", month, metric);
// //         const rawGlobal = getMetric("global", month, metric);
// //         const isMoney = metric === "net_sales" || metric === "profit";

// //         // ---------- UK SCOPE: everything in GBP ----------
// //         if (isUKScope) {
// //           // Global should mirror UK, in GBP
// //           if (lower === "global" || lower === "uk") {
// //             return ukVal; // already GBP from DB
// //           }
// //           // For safety, any other country just returns its raw value
// //           return getMetric(country as CountryKey, month, metric);
// //         }

// //         // ---------- GLOBAL SCOPE: everything in USD ----------
// //         if (lower === "global") {
// //           if (metric === "quantity") {
// //             const sumUnits = ukVal + usVal;
// //             return sumUnits !== 0 ? sumUnits : rawGlobal;
// //           }
// //           // aggregate UK (GBP) + US (USD) into USD
// //           const sumMoney = ukVal * GBP_TO_USD_RATE + usVal;
// //           return sumMoney !== 0 ? sumMoney : rawGlobal;
// //         }

// //         if (lower === "uk") {
// //           if (metric === "quantity") return ukVal;
// //           return ukVal * GBP_TO_USD_RATE; // GBP → USD
// //         }

// //         if (lower === "us") {
// //           return usVal;
// //         }

// //         const raw = getMetric(country as CountryKey, month, metric);
// //         if (!isMoney) return raw;
// //         return raw;
// //       });

// //       const isGlobalSeries = lower === "global";

// //       return {
// //         label: `${formatCountryLabel(country)} ${labelSuffix}`,
// //         data: dataSeries,
// //         borderColor: getCountryColor(country),
// //         backgroundColor: getCountryColor(country),
// //         tension: 0.1,
// //         pointRadius: 3,
// //         fill: false,
// //         borderDash: isGlobalSeries ? [6, 4] : [],
// //         borderWidth: 2,
// //         order: isGlobalSeries ? 99 : 0,
// //       };
// //     };

// //     const metrics: { metric: keyof MonthDatum; suffix: string }[] = [
// //       { metric: "net_sales", suffix: "Net Sales" },
// //       { metric: "quantity", suffix: "Quantity" },
// //       { metric: "profit", suffix: "Profit" },
// //     ];

// //     return metrics.map(({ metric, suffix }) => {
// //       const visibleCountries: CountryKey[] = [];

// //       if (selectedCountries["global"] ?? true) {
// //         visibleCountries.push("global");
// //       }

// //       visibleCountries.push(
// //         ...nonEmptyCountriesFromApi.filter((c) => selectedCountries[c] ?? true)
// //       );

// //       const datasets = visibleCountries.map((country) =>
// //         makeDataset(country, metric, suffix)
// //       );

// //       return { labels, datasets };
// //     });
// //   }, [data, countryName, nonEmptyCountriesFromApi, selectedCountries]);


// //   // just above chartOptions, reuse this helper
// //   const scope = (countryName || "global").toLowerCase();
// //   const isUKScope = scope === "uk";
// //   const yAxisLabel = isUKScope ? "Amount (£)" : "Amount ($)";
// //   const tooltipCountry = isUKScope ? "uk" : "us";


// //   /* ---------- chart options (currency-aware) ---------- */
// // const chartOptions = useMemo(
// //   () => ({
// //     responsive: true,
// //     maintainAspectRatio: false,
// //     plugins: {
// //       legend: { display: false },
// //       tooltip: {
// //         callbacks: {
// //           label: (context: any) => {
// //             const value = context.parsed.y as number;
// //             const datasetLabel = context.dataset.label as string;
// //             const metricPart = (
// //               datasetLabel
// //                 .split(" ")
// //                 .slice(1)
// //                 .join(" ") || ""
// //             ).toLowerCase();

// //             if (
// //               metricPart.includes("quantity") ||
// //               metricPart.includes("units")
// //             ) {
// //               return `${datasetLabel}: ${value.toLocaleString()}`;
// //             }

// //             // ⬇️ Format in GBP on UK scope, USD otherwise
// //             return `${datasetLabel}: ${formatCurrencyByCountry(
// //               tooltipCountry,
// //               value
// //             )}`;
// //           },
// //         },
// //       },
// //     },
// //     scales: {
// //       x: { title: { display: true, text: "Month" } },
// //       y: {
// //         title: { display: true, text: yAxisLabel },
// //         min: 0,
// //         ticks: { padding: 0 },
// //       },
// //     },
// //   }),
// //   [tooltipCountry, yAxisLabel]
// // );


// //   const yearShort =
// //     selectedYear === "" ? "" : selectedYear.toString().slice(-2);

// //   const getTitle = () => {
// //     if (range === "yearly") return `Last 12 Months'${yearShort}`;
// //     if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
// //     return selectedMonth
// //       ? `${cap(selectedMonth)}'${yearShort}`
// //       : `Year'${yearShort}`;
// //   };

// //   const getHeadingPeriod = () => {
// //     if (range === "yearly") return `Last 12 Months'${yearShort}`;
// //     if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
// //     if (range === "monthly" && selectedMonth) {
// //       return `${cap(selectedMonth)}'${yearShort}`;
// //     }
// //     return "";
// //   };

// //   /* ---------- summary cards (same as before, using FX + scope) ---------- */
// //   const cards = useMemo(() => {
// //     if (!data?.data) return [] as { country: string; stats: any }[];

// //     const selectedScope = (countryName || "global").toLowerCase();

// //     // collect all months
// //     const allMonths = new Set<string>();
// //     Object.values(data.data).forEach((countryObj: any) => {
// //      const monthly = Array.isArray(countryObj)
// //   ? countryObj
// //   : [];

// //       monthly.forEach((m) => allMonths.add(m.month));
// //     });

// //     const labels = Array.from(allMonths).sort(
// //       (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
// //     );

// //     const getMonthly = (code: string) => {
// //   const obj = data.data[code];
// //   return Array.isArray(obj) ? obj : [];
// // };


// //     const ukMonthly = getMonthly("uk");
// //     const usMonthly = getMonthly("us");
// //     const globalMonthlyBackend = getMonthly("global");

// //     return Object.entries(data.data).map(([country, countryObjRaw]) => {
// //       const key = country.toLowerCase();
// //       const monthlyRaw: MonthDatum[] = Array.isArray(
// //         (countryObjRaw as any)?.monthly
// //       )
// //         ? (countryObjRaw as any).monthly
// //         : [];

// //       let processedData: MonthDatum[] = monthlyRaw;

// //       if (key === "global") {
// //         if (homeCurrency === "GBP" && selectedScope === "uk") {
// //           processedData = ukMonthly ? [...ukMonthly] : [];
// //         } else {
// //           processedData = labels.map((month) => {
// //             const uk = ukMonthly.find((m) => m.month === month);
// //             const us = usMonthly.find((m) => m.month === month);
// //             const g = globalMonthlyBackend.find((m) => m.month === month);

// //             const ukNetHome = uk
// //               ? convertToHomeCurrency(uk.net_sales, "GBP")
// //               : 0;
// //             const usNetHome = us
// //               ? convertToHomeCurrency(us.net_sales, "USD")
// //               : 0;

// //             const ukProfitHome = uk
// //               ? convertToHomeCurrency(uk.profit, "GBP")
// //               : 0;
// //             const usProfitHome = us
// //               ? convertToHomeCurrency(us.profit, "USD")
// //               : 0;

// //             const sumNetSales = ukNetHome + usNetHome;
// //             const sumProfit = ukProfitHome + usProfitHome;
// //             const sumUnits = (uk?.quantity || 0) + (us?.quantity || 0);

// //             const fallbackNet = g
// //               ? convertToHomeCurrency(g.net_sales, "USD")
// //               : 0;
// //             const fallbackProfit = g
// //               ? convertToHomeCurrency(g.profit, "USD")
// //               : 0;
// //             const fallbackUnits = g?.quantity || 0;

// //             return {
// //               month,
// //               net_sales: sumNetSales !== 0 ? sumNetSales : fallbackNet,
// //               profit: sumProfit !== 0 ? sumProfit : fallbackProfit,
// //               quantity: sumUnits !== 0 ? sumUnits : fallbackUnits,
// //             };
// //           });
// //         }
// //       } else {
// //         const fromCurrency = key === "uk" ? "GBP" : ("USD" as const);
// //         const shouldConvert =
// //           homeCurrency === "USD" && selectedScope === "global";

// //         processedData = shouldConvert
// //           ? monthlyRaw.map((m) => ({
// //             ...m,
// //             net_sales: convertToHomeCurrency(m.net_sales, fromCurrency),
// //             profit: convertToHomeCurrency(m.profit, fromCurrency),
// //           }))
// //           : monthlyRaw;
// //       }

// //       const totalSales = processedData.reduce((s, m) => s + m.net_sales, 0);
// //       const totalProfit = processedData.reduce((s, m) => s + m.profit, 0);
// //       const totalUnits = processedData.reduce((s, m) => s + m.quantity, 0);

// //       const gross_margin_avg =
// //         totalSales > 0 ? (totalProfit / totalSales) * 100 : 0;

// //       const monthsWithSales = processedData.filter((m) => m.net_sales > 0);

// //       const avgSales =
// //         monthsWithSales.length > 0 ? totalSales / monthsWithSales.length : 0;

// //       const avgSellingPrice = totalUnits > 0 ? totalSales / totalUnits : 0;

// //       const avgMonthlyProfit =
// //         processedData.length > 0 ? totalProfit / processedData.length : 0;

// //       const maxSalesMonth =
// //         processedData.length > 0
// //           ? processedData.reduce((max, m) =>
// //             m.net_sales > max.net_sales ? m : max
// //           )
// //           : {
// //             month: "",
// //             net_sales: 0,
// //             quantity: 0,
// //             profit: 0,
// //           };

// //       const maxUnitsMonth =
// //         processedData.length > 0
// //           ? processedData.reduce((max, m) =>
// //             m.quantity > max.quantity ? m : max
// //           )
// //           : {
// //             month: "",
// //             net_sales: 0,
// //             quantity: 0,
// //             profit: 0,
// //           };

// //       return {
// //         country,
// //         stats: {
// //           totalSales,
// //           totalProfit,
// //           totalUnits,
// //           gross_margin_avg,
// //           avgSales,
// //           avgSellingPrice,
// //           avgMonthlyProfit,
// //           maxSalesMonth,
// //           maxUnitsMonth,
// //         },
// //       };
// //     });
// //   }, [data, countryName, homeCurrency, convertToHomeCurrency]);

// //   const orderedCards = useMemo(() => {
// //     if (!cards.length) return [];
// //     const global = cards.filter((c) => c.country.toLowerCase() === "global");
// //     const others = cards.filter((c) => c.country.toLowerCase() !== "global");
// //     return [...global, ...others];
// //   }, [cards]);

// //   /* ---------- render ---------- */
// //   return (
// //     <div className="w-full">
// //       <ProductwiseHeader
// //         canShowResults={canShowResults}
// //         countryName={countryName}
// //         productname={productname}
// //         headingPeriod={getHeadingPeriod()}
// //       />

// //       <FiltersAndSearchRow
// //         range={range}
// //         selectedMonth={selectedMonth}
// //         selectedQuarter={selectedQuarter}
// //         selectedYear={selectedYear}
// //         years={years}
// //         onRangeChange={setRange}
// //         onMonthChange={setSelectedMonth}
// //         onQuarterChange={(val) => {
// //           const num = val.replace("Q", "");
// //           setSelectedQuarter(num || "1");
// //         }}
// //         onYearChange={(val) => {
// //           if (!val) {
// //             setSelectedYear("");
// //           } else {
// //             setSelectedYear(Number(val));
// //           }
// //         }}
// //       />

// //       {!canShowResults && (
// //         <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
// //           <div className="flex items-center">
// //             <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
// //             <span>
// //               Search a product and choose the period to view SKU-wise
// //               performance.
// //             </span>
// //           </div>
// //         </div>
// //       )}

// //       {canShowResults && loading && (
// //         <div className="flex flex-col items-center justify-center py-12 textcenter">
// //           <Loader
// //             src="/infinity-unscreen.gif"
// //             size={150}
// //             transparent
// //             roundedClass="rounded-none"
// //             backgroundClass="bg-transparent"
// //             respectReducedMotion
// //           />
// //         </div>
// //       )}

// //       {canShowResults && !!error && (
// //         <div className="mb-8 rounded-2xl border border-red-200 bg-red-50 p-6">
// //           <div className="flex items-center gap-3 text-red-700">
// //             <span className="text-xl">❌</span>
// //             <p className="m-0 font-medium">{error}</p>
// //           </div>
// //         </div>
// //       )}

// //       {canShowResults && data && !loading && (
// //         <div className="flex flex-col">
// //           <TrendChartSection
// //             productname={productname}
// //             title={getTitle()}
// //             chartDataList={chartDataList}
// //             chartOptions={chartOptions}
// //             nonEmptyCountriesFromApi={nonEmptyCountriesFromApi}
// //             selectedCountries={selectedCountries}
// //             onToggleCountry={handleCountryChange}
// //             authToken={authToken}
// //             onProductSelect={handleProductSelect}
// //           />

// //           <div className="mt-8">
// //             <div className="grid gap-5 grid-cols-1 md:grid-cols-[repeat(auto-fit,minmax(260px,1fr))]">
// //               {orderedCards.map((card) => {
// //                 const key = card.country.toLowerCase();
// //                 const isGlobal = key === "global";

// //                 if (
// //                   !isGlobal &&
// //                   card.stats.totalSales === 0 &&
// //                   card.stats.totalUnits === 0 &&
// //                   card.stats.totalProfit === 0
// //                 ) {
// //                   return null;
// //                 }

// //                 return (
// //                   <CountryCard
// //                     key={key}
// //                     country={card.country}
// //                     stats={card.stats}
// //                     selectedYear={selectedYear}
// //                     homeCurrency={homeCurrency}
// //                   />
// //                 );
// //               })}
// //             </div>
// //           </div>
// //         </div>
// //       )}
// //     </div>
// //   );
// // };

// // export default ProductwisePerformance;









































// // components/productwise/ProductwisePerformance.tsx
// "use client";

// import React, { useEffect, useMemo, useState } from "react";
// import { useParams, useRouter } from "next/navigation";
// import "@/lib/chartSetup";
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

// import Loader from "@/components/loader/Loader";

// import {
//   APIResponse,
//   CountryKey,
//   MonthDatum,
//   Range,
//   GBP_TO_USD_RATE,
//   formatCountryLabel,
//   formatCurrencyByCountry,
//   getCountryColor,
//   monthOrder,
// } from "@/components/productwise/productwiseHelpers";
// import CountryCard from "@/components/productwise/CountryCard";
// import TrendChartSection from "@/components/productwise/TrendChartSection";
// import ProductwiseHeader from "@/components/productwise/ProductwiseHeader";
// import FiltersAndSearchRow from "@/components/productwise/FiltersAndSearchRow";
// import { useFx } from "@/components/dashboard/useFx";
// import { useGetUserDataQuery } from "@/lib/api/profileApi";

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

// interface ProductwisePerformanceProps {
//   productname?: string;
// }

// /* ---------- Slug helpers ---------- */
// const toSlug = (name: string) =>
//   name
//     .trim()
//     .toLowerCase()
//     .replace(/\s*\+\s*/g, " plus ")
//     .replace(/\s+/g, "-");

// const fromSlug = (slug: string) =>
//   slug
//     .replace(/-/g, " ")
//     .replace(/\bplus\b/gi, "+")
//     .replace(/\s+/g, " ")
//     .trim();

// const normalizeProductSlug = (slug?: string) => {
//   if (!slug) return undefined;

//   try {
//     const decoded = decodeURIComponent(slug);
//     if (!decoded.trim()) return undefined;
//     return fromSlug(decoded);
//   } catch {
//     if (!slug.trim()) return undefined;
//     return fromSlug(slug);
//   }
// };

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

// const cap = (s: string) => (s ? s[0].toUpperCase() + s.slice(1) : "");

// // helper: latest fetched month from localStorage (for Last 12 Months logic)
// const getLatestFetchedMonth = (): string | null => {
//   if (typeof window === "undefined") return null;
//   try {
//     const raw = localStorage.getItem("latestFetchedPeriod");
//     if (!raw) return null;
//     const parsed = JSON.parse(raw) as { month?: string; year?: string };
//     return parsed.month ? parsed.month.toLowerCase() : null; // e.g. "november"
//   } catch {
//     return null;
//   }
// };


// const ProductwisePerformance: React.FC<ProductwisePerformanceProps> = ({
//   productname: propProductName,
// }) => {
//   const { homeCurrency, setHomeCurrency, convertToHomeCurrency } = useFx();
//   const { data: userData } = useGetUserDataQuery(); // ⬅️ profile

//   const params = useParams();
//   const router = useRouter();

//   const rawSlug = params?.productname as string | undefined;
//   const urlProductName = normalizeProductSlug(rawSlug);

//   const countryName = (params?.countryName as string) || undefined;
//   const monthParam = (params?.month as string) || undefined;
//   const yearParam = (params?.year as string) || undefined;

//   const productname = propProductName || urlProductName || "";

//   const [data, setData] = useState<APIResponse | null>(null);
//   const [loading, setLoading] = useState(false);
//   const [error, setError] = useState<string>("");

//   const authToken =
//     typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

//   // ---------- derive homeCurrency from profile ----------
//   const profileHomeCurrency = (userData?.homeCurrency || "USD").toUpperCase() as
//     | "USD"
//     | "GBP";

//   // push profile currency into FX context
//   useEffect(() => {
//     if (profileHomeCurrency && profileHomeCurrency !== homeCurrency) {
//       setHomeCurrency(profileHomeCurrency);
//     }
//   }, [profileHomeCurrency, homeCurrency, setHomeCurrency]);

//   // which "global" key to use from backend
//   const globalKey: CountryKey = profileHomeCurrency === "GBP" ? "global_gbp" : "global";

//   // ---------- controls ----------
//   const [range, setRange] = useState<Range>("quarterly");

//   const [selectedMonth, setSelectedMonth] = useState<string>(() => {
//     if (monthParam) return monthParam.toLowerCase();

//     if (typeof window !== "undefined") {
//       const raw = localStorage.getItem("latestFetchedPeriod");
//       if (raw) {
//         try {
//           const parsed = JSON.parse(raw) as { month?: string; year?: string };
//           if (parsed.month) return parsed.month.toLowerCase();
//         } catch {
//           /* ignore */
//         }
//       }
//     }

//     return "";
//   });

//   const [selectedQuarter, setSelectedQuarter] = useState("1");
//   useEffect(() => {
//     if (!selectedMonth) return;
//     const idx = months.indexOf(selectedMonth.toLowerCase());
//     if (idx >= 0) {
//       const q = Math.floor(idx / 3) + 1;
//       setSelectedQuarter(String(q));
//     }
//   }, [selectedMonth]);

//   const [selectedYear, setSelectedYear] = useState<number | "">(() => {
//     if (yearParam) return Number(yearParam);

//     if (typeof window !== "undefined") {
//       const raw = localStorage.getItem("latestFetchedPeriod");
//       if (raw) {
//         try {
//           const parsed = JSON.parse(raw) as { month?: string; year?: string };
//           if (parsed.year) return Number(parsed.year);
//         } catch {
//           /* ignore */
//         }
//       }
//     }

//     return "";
//   });

//   const [selectedCountries, setSelectedCountries] = useState<
//     Record<CountryKey, boolean>
//   >({
//     uk: true,
//     us: true,
//     global: true,
//     global_gbp: true,
//   });

//   const years = useMemo(
//     () => Array.from({ length: 2 }, (_, i) => new Date().getFullYear() - i),
//     []
//   );

//   const isProductSelected = !!productname;
//   const hasYear = selectedYear !== "" && selectedYear !== undefined;

//   const isPeriodComplete =
//     (range === "yearly" && hasYear) ||
//     (range === "quarterly" && hasYear && !!selectedQuarter) ||
//     (range === "monthly" && hasYear && !!selectedMonth);

//   const canShowResults = isProductSelected && isPeriodComplete;

//   /* ---------- country toggle ---------- */
//   const handleCountryChange = (country: CountryKey) => {
//     setSelectedCountries((prev) => ({
//       ...prev,
//       [country]: !(prev[country] ?? true),
//     }));
//   };

//   /* ---------- product select handler ---------- */
//   const handleProductSelect = (selectedProductName: string) => {
//     const base = "/productwiseperformance";
//     const slug = toSlug(selectedProductName);

//     let month = selectedMonth;
//     let year = selectedYear;

//     if ((!month || !year) && typeof window !== "undefined") {
//       try {
//         const raw = localStorage.getItem("latestFetchedPeriod");
//         if (raw) {
//           const parsed = JSON.parse(raw) as { month?: string; year?: string };
//           if (!month && parsed.month) month = parsed.month.toLowerCase();
//           if (!year && parsed.year) year = Number(parsed.year);
//         }
//       } catch {
//         // ignore
//       }
//     }

//     const to = `${base}/${slug}/${countryName ?? ""}/${month || ""}/${year || ""}`;
//     router.push(to);
//   };

//   /* ---------- fetch data ---------- */
//   const fetchProductData = async () => {
//     if (!canShowResults) return;

//     setLoading(true);
//     setError("");

//     try {
//       const countries: string[] = [globalKey, "uk", "us"];

//       const backendTimeRange =
//         range === "yearly"
//           ? "Yearly"
//           : range === "quarterly"
//           ? "Quarterly"
//           : "Monthly";

//       const payload: any = {
//         product_name: productname,
//         time_range: backendTimeRange,
//         year: selectedYear,
//         countries,
//         home_currency: profileHomeCurrency, // ⬅️ tell backend what we want
//       };

//       if (range === "quarterly") {
//         payload.quarter = selectedQuarter;
//       }

//       if (range === "monthly") {
//         payload.month = selectedMonth;
//       }

//       const res = await fetch("http://localhost:5000/ProductwisePerformance", {
//         method: "POST",
//         headers: {
//           "Content-Type": "application/json",
//           Authorization: `Bearer ${authToken ?? ""}`,
//         },
//         body: JSON.stringify(payload),
//       });

//       const json: APIResponse | any = await res.json().catch(() => null);

//       if (!res.ok || !json?.success) {
//         const errMsg =
//           (json && (json.error || json.message)) ||
//           `HTTP error! status: ${res.status}`;
//         throw new Error(errMsg);
//       }

//       setData(json as APIResponse);
//     } catch (e: any) {
//       console.error("API Error:", e);
//       setError(e?.message || "Failed to fetch data from server");
//     } finally {
//       setLoading(false);
//     }
//   };

//   useEffect(() => {
//     if (!canShowResults) return;
//     fetchProductData();
//     // eslint-disable-next-line react-hooks/exhaustive-deps
//   }, [
//     productname,
//     selectedYear,
//     range,
//     selectedQuarter,
//     selectedMonth,
//     canShowResults,
//     globalKey,
//     profileHomeCurrency,
//   ]);

//   /* ---------- non-empty countries (excluding globalKey) ---------- */
//   const nonEmptyCountriesFromApi = useMemo(() => {
//     if (!data?.data) return [] as CountryKey[];

//     return (Object.entries(data.data) as [CountryKey, any][])
//       .filter(([country, countryArray]) => {
//         if (country.toLowerCase() === globalKey.toLowerCase()) return false;
//         const rows: MonthDatum[] = Array.isArray(countryArray)
//           ? (countryArray as MonthDatum[])
//           : [];
//         return rows.some(
//           (m) => m.net_sales !== 0 || m.quantity !== 0 || m.profit !== 0
//         );
//       })
//       .map(([country]) => country);
//   }, [data, globalKey]);

//   /* ---------- chart data ---------- */
//  // keep your existing imports, including monthOrder and getLatestFetchedMonth helper

// const chartDataList = useMemo(() => {
//   if (!data?.data) return [null, null, null];

//   const scope = (countryName || "global").toLowerCase();
//   const isUKScope = scope === "uk";

//   // 1) collect all months we have from the API
//   const allMonthsSet = new Set<string>();
//   Object.values(data.data).forEach((countryObj: any) => {
//     const monthly = Array.isArray(countryObj?.monthly)
//       ? (countryObj.monthly as MonthDatum[])
//       : [];
//     monthly.forEach((m) => allMonthsSet.add(m.month));
//   });

//   let labels = Array.from(allMonthsSet).sort(
//     (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
//   );

//   // 2) If "yearly" = Last 12 Months → build a rolling 12-month window
//   if (range === "yearly") {
//     // prefer latestFetchedPeriod.month from localStorage
//     const latestFromStorage = getLatestFetchedMonth(); // returns lowercase e.g. "november"

//     let latestIdx = -1;
//     if (latestFromStorage) {
//       latestIdx = monthOrder.findIndex(
//         (m) => m.toLowerCase() === latestFromStorage
//       );
//     }

//     // fallback: last non-zero month from API
//     if (latestIdx === -1 && labels.length > 0) {
//       // look from the end for any month where any country has non-zero sales
//       for (let i = monthOrder.length - 1; i >= 0; i--) {
//         const monthName = monthOrder[i];
//         const hasData = Object.values(data.data).some((countryObj: any) => {
//           const monthly = Array.isArray(countryObj?.monthly)
//             ? (countryObj.monthly as MonthDatum[])
//             : [];
//           const row = monthly.find((m) => m.month === monthName);
//           return row && (row.net_sales !== 0 || row.profit !== 0 || row.quantity !== 0);
//         });
//         if (hasData) {
//           latestIdx = i;
//           break;
//         }
//       }
//       // worst-case: still -1 → just use the last label we have
//       if (latestIdx === -1) {
//         const lastLabel = labels[labels.length - 1];
//         latestIdx = monthOrder.indexOf(lastLabel);
//       }
//     }

//     if (latestIdx >= 0) {
//       const last12: string[] = [];
//       const startIdx = (latestIdx - 11 + monthOrder.length) % monthOrder.length;
//       for (let i = 0; i < 12; i++) {
//         const idx = (startIdx + i) % monthOrder.length;
//         last12.push(monthOrder[idx]);
//       }
//       labels = last12;
//     }
//   }

//   const getMetric = (
//     country: CountryKey,
//     month: string,
//     metric: keyof MonthDatum
//   ) => {
//     const countryObj = (data.data as any)[country];
//     const monthly: MonthDatum[] = Array.isArray(countryObj?.monthly)
//       ? countryObj.monthly
//       : [];
//     const found = monthly.find((m) => m.month === month);
//     return found ? (found[metric] as number) : 0;
//   };

//   const makeDataset = (
//     country: string,
//     metric: keyof MonthDatum,
//     labelSuffix: string
//   ) => {
//     const lower = country.toLowerCase();

//     const dataSeries = labels.map((month) => {
//       const ukVal = getMetric("uk", month, metric);
//       const usVal = getMetric("us", month, metric);
//       const rawGlobal = getMetric("global", month, metric);
//       const isMoney = metric === "net_sales" || metric === "profit";

//       // UK scope → everything in GBP (global mirrors UK)
//       if (isUKScope) {
//         if (lower === "global" || lower === "uk") {
//           return ukVal;
//         }
//         return getMetric(country as CountryKey, month, metric);
//       }

//       // Global scope → everything in USD (UK converted)
//       if (lower === "global") {
//         if (metric === "quantity") {
//           const sumUnits = ukVal + usVal;
//           return sumUnits !== 0 ? sumUnits : rawGlobal;
//         }
//         const sumMoney = ukVal * GBP_TO_USD_RATE + usVal;
//         return sumMoney !== 0 ? sumMoney : rawGlobal;
//       }

//       if (lower === "uk") {
//         if (metric === "quantity") return ukVal;
//         return ukVal * GBP_TO_USD_RATE;
//       }

//       if (lower === "us") {
//         return usVal;
//       }

//       const raw = getMetric(country as CountryKey, month, metric);
//       if (!isMoney) return raw;
//       return raw;
//     });

//     const isGlobalSeries = lower === "global";

//     return {
//       label: `${formatCountryLabel(country)} ${labelSuffix}`,
//       data: dataSeries,
//       borderColor: getCountryColor(country),
//       backgroundColor: getCountryColor(country),
//       tension: 0.1,
//       pointRadius: 3,
//       fill: false,
//       borderDash: isGlobalSeries ? [6, 4] : [],
//       borderWidth: 2,
//       order: isGlobalSeries ? 99 : 0,
//     };
//   };

//   const metrics: { metric: keyof MonthDatum; suffix: string }[] = [
//     { metric: "net_sales", suffix: "Net Sales" },
//     { metric: "quantity", suffix: "Quantity" },
//     { metric: "profit", suffix: "Profit" },
//   ];

//   return metrics.map(({ metric, suffix }) => {
//     const visibleCountries: CountryKey[] = [];

//     if (selectedCountries["global"] ?? true) {
//       visibleCountries.push("global");
//     }

//     visibleCountries.push(
//       ...nonEmptyCountriesFromApi.filter((c) => selectedCountries[c] ?? true)
//     );

//     const datasets = visibleCountries.map((country) =>
//       makeDataset(country, metric, suffix)
//     );

//     return { labels, datasets };
//   });
// }, [data, countryName, nonEmptyCountriesFromApi, selectedCountries, range]);


//   /* ---------- chart options (currency-aware) ---------- */
//   const yAxisLabel =
//     profileHomeCurrency === "GBP" ? "Amount (£)" : "Amount ($)";
//   const tooltipCountry =
//     profileHomeCurrency === "GBP" ? "uk" : "us"; // just to reuse your formatter

//   const chartOptions = useMemo(
//     () => ({
//       responsive: true,
//       maintainAspectRatio: false,
//       plugins: {
//         legend: { display: false },
//         tooltip: {
//           callbacks: {
//             label: (context: any) => {
//               const value = context.parsed.y as number;
//               const datasetLabel = context.dataset.label as string;
//               const metricPart = (
//                 datasetLabel
//                   .split(" ")
//                   .slice(1)
//                   .join(" ") || ""
//               ).toLowerCase();

//               if (
//                 metricPart.includes("quantity") ||
//                 metricPart.includes("units")
//               ) {
//                 return `${datasetLabel}: ${value.toLocaleString()}`;
//               }

//               return `${datasetLabel}: ${formatCurrencyByCountry(
//                 tooltipCountry,
//                 value
//               )}`;
//             },
//           },
//         },
//       },
//       scales: {
//         x: { title: { display: true, text: "Month" } },
//         y: {
//           title: { display: true, text: yAxisLabel },
//           min: 0,
//           ticks: { padding: 0 },
//         },
//       },
//     }),
//     [tooltipCountry, yAxisLabel]
//   );

//   const yearShort =
//     selectedYear === "" ? "" : selectedYear.toString().slice(-2);

//   const getTitle = () => {
//     if (range === "yearly") return `Last 12 Months'${yearShort}`;
//     if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
//     return selectedMonth
//       ? `${cap(selectedMonth)}'${yearShort}`
//       : `Year'${yearShort}`;
//   };

//   const getHeadingPeriod = () => {
//     if (range === "yearly") return `Last 12 Months'${yearShort}`;
//     if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
//     if (range === "monthly" && selectedMonth) {
//       return `${cap(selectedMonth)}'${yearShort}`;
//     }
//     return "";
//   };

//   /* ---------- summary cards ---------- */
//   const cards = useMemo(() => {
//     if (!data?.data) return [] as { country: string; stats: any }[];

//     const selectedScope = (countryName || "global").toLowerCase();

//     const allMonths = new Set<string>();
//     Object.values(data.data).forEach((countryArray: any) => {
//       const monthly = Array.isArray(countryArray)
//         ? (countryArray as MonthDatum[])
//         : [];
//       monthly.forEach((m) => allMonths.add(m.month));
//     });

//     const labels = Array.from(allMonths).sort(
//       (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
//     );

//     const getMonthly = (code: string): MonthDatum[] => {
//       const arr = (data.data as any)[code];
//       return Array.isArray(arr) ? (arr as MonthDatum[]) : [];
//     };

//     const ukMonthly = getMonthly("uk");
//     const usMonthly = getMonthly("us");
//     const globalMonthlyBackend = getMonthly(globalKey);

//     return Object.entries(data.data).map(([country, rawArray]) => {
//       const key = country.toLowerCase();
//       const monthlyRaw: MonthDatum[] = Array.isArray(rawArray)
//         ? (rawArray as MonthDatum[])
//         : [];

//       let processedData: MonthDatum[] = monthlyRaw;

//       // treat global/global_gbp specially
//       if (key === globalKey.toLowerCase()) {
//         // when viewing "uk" scope and GBP homeCurrency, we might want global to mirror UK;
//         // but since you now have global_gbp from backend, we can trust that table
//         processedData = globalMonthlyBackend;
//       } else {
//         const fromCurrency =
//           key === "uk" ? ("GBP" as const) : ("USD" as const);
//         const shouldConvert =
//           profileHomeCurrency === "USD" && selectedScope === "global";

//         processedData = shouldConvert
//           ? monthlyRaw.map((m) => ({
//               ...m,
//               net_sales: convertToHomeCurrency(m.net_sales, fromCurrency),
//               profit: convertToHomeCurrency(m.profit, fromCurrency),
//             }))
//           : monthlyRaw;
//       }

//       const totalSales = processedData.reduce((s, m) => s + m.net_sales, 0);
//       const totalProfit = processedData.reduce((s, m) => s + m.profit, 0);
//       const totalUnits = processedData.reduce((s, m) => s + m.quantity, 0);

//       const gross_margin_avg =
//         totalSales > 0 ? (totalProfit / totalSales) * 100 : 0;

//       const monthsWithSales = processedData.filter((m) => m.net_sales > 0);

//       const avgSales =
//         monthsWithSales.length > 0 ? totalSales / monthsWithSales.length : 0;

//       const avgSellingPrice = totalUnits > 0 ? totalSales / totalUnits : 0;

//       const avgMonthlyProfit =
//         processedData.length > 0 ? totalProfit / processedData.length : 0;

//       const maxSalesMonth =
//         processedData.length > 0
//           ? processedData.reduce((max, m) =>
//               m.net_sales > max.net_sales ? m : max
//             )
//           : { month: "", net_sales: 0, quantity: 0, profit: 0 };

//       const maxUnitsMonth =
//         processedData.length > 0
//           ? processedData.reduce((max, m) =>
//               m.quantity > max.quantity ? m : max
//             )
//           : { month: "", net_sales: 0, quantity: 0, profit: 0 };

//       return {
//         country: key === "global_gbp" ? "global" : country,
//         stats: {
//           totalSales,
//           totalProfit,
//           totalUnits,
//           gross_margin_avg,
//           avgSales,
//           avgSellingPrice,
//           avgMonthlyProfit,
//           maxSalesMonth,
//           maxUnitsMonth,
//         },
//       };
//     });
//   }, [data, countryName, convertToHomeCurrency, globalKey, profileHomeCurrency]);

//   const orderedCards = useMemo(() => {
//     if (!cards.length) return [];
//     const globalCard = cards.filter(
//       (c) => c.country.toLowerCase() === "global"
//     );
//     const others = cards.filter(
//       (c) => c.country.toLowerCase() !== "global"
//     );
//     return [...globalCard, ...others];
//   }, [cards]);

//   /* ---------- render ---------- */
//   return (
//     <div className="w-full">
//       {/* <ProductwiseHeader
//         canShowResults={canShowResults}
//         countryName={countryName}
//         productname={productname}
//         headingPeriod={getHeadingPeriod()}
//       />

//       <FiltersAndSearchRow
//         range={range}
//         selectedMonth={selectedMonth}
//         selectedQuarter={selectedQuarter}
//         selectedYear={selectedYear}
//         years={years}
//         onRangeChange={setRange}
//         onMonthChange={setSelectedMonth}
//         onQuarterChange={(val) => {
//           const num = val.replace("Q", "");
//           setSelectedQuarter(num || "1");
//         }}
//         onYearChange={(val) => {
//           if (!val) {
//             setSelectedYear("");
//           } else {
//             setSelectedYear(Number(val));
//           }
//         }}
//       /> */}


//       {/* Header + filters row (like the screenshot) */}
//       <div className="mb-6 flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
//         <ProductwiseHeader
//           canShowResults={canShowResults}
//           countryName={countryName}
//           productname={productname}
//           headingPeriod={getHeadingPeriod()}
//         />

//         <FiltersAndSearchRow
//           range={range}
//           selectedMonth={selectedMonth}
//           selectedQuarter={selectedQuarter}
//           selectedYear={selectedYear}
//           years={years}
//           onRangeChange={setRange}
//           onMonthChange={setSelectedMonth}
//           onQuarterChange={(val) => {
//             const num = val.replace("Q", "");
//             setSelectedQuarter(num || "1");
//           }}
//           onYearChange={(val) => {
//             if (!val) {
//               setSelectedYear("");
//             } else {
//               setSelectedYear(Number(val));
//             }
//           }}
//         />
//       </div>


//       {!canShowResults && (
//         <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
//           <div className="flex items-center">
//             <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
//             <span>
//               Search a product and choose the period to view SKU-wise
//               performance.
//             </span>
//           </div>
//         </div>
//       )}

//       {canShowResults && loading && (
//         <div className="flex flex-col items-center justify-center py-12 textcenter">
//           <Loader
//             src="/infinity-unscreen.gif"
//             size={150}
//             transparent
//             roundedClass="rounded-none"
//             backgroundClass="bg-transparent"
//             respectReducedMotion
//           />
//         </div>
//       )}

//       {canShowResults && !!error && (
//         <div className="mb-8 rounded-2xl border border-red-200 bg-red-50 p-6">
//           <div className="flex items-center gap-3 text-red-700">
//             <span className="text-xl">❌</span>
//             <p className="m-0 font-medium">{error}</p>
//           </div>
//         </div>
//       )}

//       {canShowResults && data && !loading && (
//         <div className="flex flex-col">
//           <TrendChartSection
//             productname={productname}
//             title={getTitle()}
//             chartDataList={chartDataList}
//             chartOptions={chartOptions}
//             nonEmptyCountriesFromApi={nonEmptyCountriesFromApi}
//             selectedCountries={selectedCountries}
//             onToggleCountry={handleCountryChange}
//             authToken={authToken}
//             onProductSelect={handleProductSelect}
//           />

//           <div className="mt-8">
//             <div className="grid gap-5 grid-cols-1 md:grid-cols-[repeat(auto-fit,minmax(260px,1fr))]">
//               {orderedCards.map((card) => {
//                 const key = card.country.toLowerCase();
//                 const isGlobal = key === "global";

//                 if (
//                   !isGlobal &&
//                   card.stats.totalSales === 0 &&
//                   card.stats.totalUnits === 0 &&
//                   card.stats.totalProfit === 0
//                 ) {
//                   return null;
//                 }

//                 return (
//                   <CountryCard
//                     key={key}
//                     country={card.country}
//                     stats={card.stats}
//                     selectedYear={selectedYear}
//                     homeCurrency={homeCurrency}
//                   />
//                 );
//               })}
//             </div>
//           </div>
//         </div>
//       )}
//     </div>
//   );
// };

// export default ProductwisePerformance;

























// components/productwise/ProductwisePerformance.tsx
"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import "@/lib/chartSetup";
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

import Loader from "@/components/loader/Loader";

// REMOVE GBP_TO_USD_RATE and formatCurrencyByCountry here
import {
  APIResponse,
  CountryKey,
  MonthDatum,
  Range,
  formatCountryLabel,
  getCountryColor,
  monthOrder,
} from "@/components/productwise/productwiseHelpers";
import { useFx, HomeCurrency, FromCurrency } from "@/components/dashboard/useFx";
import CountryCard from "@/components/productwise/CountryCard";
import TrendChartSection from "@/components/productwise/TrendChartSection";
import ProductwiseHeader from "@/components/productwise/ProductwiseHeader";
import FiltersAndSearchRow from "@/components/productwise/FiltersAndSearchRow";
import { useGetUserDataQuery } from "@/lib/api/profileApi";

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

interface ProductwisePerformanceProps {
  productname?: string;
}

/* ---------- Slug helpers ---------- */
const toSlug = (name: string) =>
  name
    .trim()
    .toLowerCase()
    .replace(/\s*\+\s*/g, " plus ")
    .replace(/\s+/g, "-");

const fromSlug = (slug: string) =>
  slug
    .replace(/-/g, " ")
    .replace(/\bplus\b/gi, "+")
    .replace(/\s+/g, " ")
    .trim();

const normalizeProductSlug = (slug?: string) => {
  if (!slug) return undefined;

  try {
    const decoded = decodeURIComponent(slug);
    if (!decoded.trim()) return undefined;
    return fromSlug(decoded);
  } catch {
    if (!slug.trim()) return undefined;
    return fromSlug(slug);
  }
};

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

const cap = (s: string) => (s ? s[0].toUpperCase() + s.slice(1) : "");

/** latest fetched month from localStorage (for Last 12 Months) */
const getLatestFetchedMonth = (): string | null => {
  if (typeof window === "undefined") return null;
  try {
    const raw = localStorage.getItem("latestFetchedPeriod");
    if (!raw) return null;
    const parsed = JSON.parse(raw) as { month?: string; year?: string };
    return parsed.month ? parsed.month.toLowerCase() : null; // e.g. "november"
  } catch {
    return null;
  }
};

const ProductwisePerformance: React.FC<ProductwisePerformanceProps> = ({
  productname: propProductName,
}) => {
  const { homeCurrency, setHomeCurrency, convertToHomeCurrency, formatHomeAmount } =
    useFx();
  const { data: userData } = useGetUserDataQuery();


  const params = useParams();
  const router = useRouter();

  const rawSlug = params?.productname as string | undefined;
  const urlProductName = normalizeProductSlug(rawSlug);

  const countryName = (params?.countryName as string) || undefined;
  const monthParam = (params?.month as string) || undefined;
  const yearParam = (params?.year as string) || undefined;

  const productname = propProductName || urlProductName || "";

  const [data, setData] = useState<APIResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");

  const authToken =
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

  // ---------- derive homeCurrency from profile ----------
  const profileHomeCurrency = (userData?.homeCurrency || "USD").toUpperCase() as HomeCurrency;

  useEffect(() => {
    if (profileHomeCurrency && profileHomeCurrency !== homeCurrency) {
      setHomeCurrency(profileHomeCurrency);
    }
  }, [profileHomeCurrency, homeCurrency, setHomeCurrency]);

  const globalKey: CountryKey =
    profileHomeCurrency === "GBP"
      ? "global_gbp"
      : profileHomeCurrency === "INR"
        ? "global_inr"
        : profileHomeCurrency === "CAD"
          ? "global_cad"
          : "global"; // USD

  // ---------- controls ----------
  const [range, setRange] = useState<Range>("quarterly");

  const [selectedMonth, setSelectedMonth] = useState<string>(() => {
    if (monthParam) return monthParam.toLowerCase();

    if (typeof window !== "undefined") {
      const raw = localStorage.getItem("latestFetchedPeriod");
      if (raw) {
        try {
          const parsed = JSON.parse(raw) as { month?: string; year?: string };
          if (parsed.month) return parsed.month.toLowerCase();
        } catch {
          /* ignore */
        }
      }
    }

    return "";
  });

  const [selectedQuarter, setSelectedQuarter] = useState<string>("Q1");

  useEffect(() => {
    if (!selectedMonth) return;
    const idx = months.indexOf(selectedMonth.toLowerCase());
    if (idx >= 0) {
      const q = Math.floor(idx / 3) + 1;
      setSelectedQuarter(`Q${q}`);   // october -> "Q4"
    }
  }, [selectedMonth]);



  const [selectedYear, setSelectedYear] = useState<number | "">(() => {
    if (yearParam) return Number(yearParam);

    if (typeof window !== "undefined") {
      const raw = localStorage.getItem("latestFetchedPeriod");
      if (raw) {
        try {
          const parsed = JSON.parse(raw) as { month?: string; year?: string };
          if (parsed.year) return Number(parsed.year);
        } catch {
          /* ignore */
        }
      }
    }

    return "";
  });

  const [selectedCountries, setSelectedCountries] = useState<
    Record<CountryKey, boolean>
  >({
    uk: true,
    us: true,
    global: true,
    global_gbp: true,
    global_inr: true,
    global_cad: true,
  });

  const years = useMemo(
    () => Array.from({ length: 2 }, (_, i) => new Date().getFullYear() - i),
    []
  );

  const isProductSelected = !!productname;
  const hasYear = selectedYear !== "" && selectedYear !== undefined;

  const isPeriodComplete =
    (range === "yearly" && hasYear) ||
    (range === "quarterly" && hasYear && !!selectedQuarter) ||
    (range === "monthly" && hasYear && !!selectedMonth);

  const canShowResults = isProductSelected && isPeriodComplete;

  /* ---------- country toggle ---------- */
  const handleCountryChange = (country: CountryKey) => {
    setSelectedCountries((prev) => ({
      ...prev,
      [country]: !(prev[country] ?? true),
    }));
  };

  /* ---------- product select handler ---------- */
  const handleProductSelect = (selectedProductName: string) => {
    const base = "/productwiseperformance";
    const slug = toSlug(selectedProductName);

    let month = selectedMonth;
    let year = selectedYear;

    if ((!month || !year) && typeof window !== "undefined") {
      try {
        const raw = localStorage.getItem("latestFetchedPeriod");
        if (raw) {
          const parsed = JSON.parse(raw) as { month?: string; year?: string };
          if (!month && parsed.month) month = parsed.month.toLowerCase();
          if (!year && parsed.year) year = Number(parsed.year);
        }
      } catch {
        // ignore
      }
    }

    const to = `${base}/${slug}/${countryName ?? ""}/${month || ""}/${year || ""}`;
    router.push(to);
  };

  /* ---------- fetch data ---------- */
  const fetchProductData = async () => {
    if (!canShowResults) return;

    setLoading(true);
    setError("");

    try {
      const countries: string[] = [globalKey, "uk", "us"];

      const backendTimeRange =
        range === "yearly"
          ? "Yearly"
          : range === "quarterly"
            ? "Quarterly"
            : "Monthly";

      const payload: any = {
        product_name: productname,
        time_range: backendTimeRange,
        year: selectedYear,
        countries,
        home_currency: profileHomeCurrency,
      };

      if (range === "quarterly") {
        // selectedQuarter is like "Q1", "Q2", "Q3", "Q4"
        payload.quarter = selectedQuarter.replace("Q", ""); // "Q4" -> "4"
      }


      if (range === "monthly") {
        payload.month = selectedMonth;
      }

      const res = await fetch("http://localhost:5000/ProductwisePerformance", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${authToken ?? ""}`,
        },
        body: JSON.stringify(payload),
      });

      const json: APIResponse | any = await res.json().catch(() => null);

      if (!res.ok || !json?.success) {
        const errMsg =
          (json && (json.error || json.message)) ||
          `HTTP error! status: ${res.status}`;
        throw new Error(errMsg);
      }

      setData(json as APIResponse);
    } catch (e: any) {
      console.error("API Error:", e);
      setError(e?.message || "Failed to fetch data from server");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!canShowResults) return;
    fetchProductData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    productname,
    selectedYear,
    range,
    selectedQuarter,
    selectedMonth,
    canShowResults,
    globalKey,
    profileHomeCurrency,
  ]);

  /* ---------- non-empty countries (excluding globalKey) ---------- */
  const nonEmptyCountriesFromApi = useMemo(() => {
    if (!data?.data) return [] as CountryKey[];

    return (Object.entries(data.data) as [CountryKey, any][])
      .filter(([country, countryArray]) => {
        if (country.toLowerCase() === globalKey.toLowerCase()) return false;
        const rows: MonthDatum[] = Array.isArray(countryArray)
          ? (countryArray as MonthDatum[])
          : [];
        return rows.some(
          (m) => m.net_sales !== 0 || m.quantity !== 0 || m.profit !== 0
        );
      })
      .map(([country]) => country);
  }, [data, globalKey]);

  // helper: sort "January", "january", etc. by calendar month Jan–Dec
  const sortByCalendarMonth = (a: string, b: string) => {
    const idxA = months.indexOf(a.toLowerCase());
    const idxB = months.indexOf(b.toLowerCase());

    // If both not found, keep alphabetical
    if (idxA === -1 && idxB === -1) return a.localeCompare(b);
    // Unknown months go to the end
    if (idxA === -1) return 1;
    if (idxB === -1) return -1;

    return idxA - idxB; // Jan (0) → Dec (11)
  };


  /* ---------- chart data (Last 12 Months + FX) ---------- */
  const chartDataList = useMemo(() => {
    if (!data?.data) return [null, null, null];

    // 1) Collect all months we have from the API
    const allMonthsSet = new Set<string>();
    Object.values(data.data).forEach((countryArray: any) => {
      const monthly = Array.isArray(countryArray)
        ? (countryArray as MonthDatum[])
        : [];
      monthly.forEach((m) => allMonthsSet.add(m.month));
    });

    const labels = Array.from(allMonthsSet).sort(sortByCalendarMonth);

    const getMetric = (
      country: CountryKey,
      month: string,
      metric: keyof MonthDatum
    ) => {
      const countryArr = (data.data as any)[country];
      const monthly: MonthDatum[] = Array.isArray(countryArr)
        ? countryArr
        : [];
      const found = monthly.find((m) => m.month === month);
      if (!found) return 0;

      const raw = found[metric] as number;

      // Quantities: just raw
      if (metric === "quantity") {
        return raw;
      }

      // Money metrics: convert if needed
      const isGlobalSeries =
        country.toLowerCase() === globalKey.toLowerCase();

      if (isGlobalSeries) {
        // Backend global_* already in home currency
        return raw;
      }

      // UK / US series: convert from source currency → homeCurrency
      let fromCurrency: FromCurrency = "USD";
      if (country === "uk") fromCurrency = "GBP";
      if (country === "us") fromCurrency = "USD";

      return convertToHomeCurrency(raw, fromCurrency);
    };

    const makeDataset = (
      country: CountryKey,
      metric: keyof MonthDatum,
      labelSuffix: string
    ) => {
      const dataSeries = labels.map((month) =>
        getMetric(country, month, metric)
      );

      const isGlobalSeries =
        country.toLowerCase() === globalKey.toLowerCase();

      // For label, show "Global" instead of "global_inr" etc.
      const displayCountry =
        country.toLowerCase().startsWith("global") ? "global" : country;

      return {
        label: `${formatCountryLabel(displayCountry)} ${labelSuffix}`,
        data: dataSeries,
        borderColor: getCountryColor(displayCountry),
        backgroundColor: getCountryColor(displayCountry),
        tension: 0.1,
        pointRadius: 3,
        fill: false,
        borderDash: isGlobalSeries ? [6, 4] : [],
        borderWidth: 2,
        order: isGlobalSeries ? 99 : 0,
      };
    };

    const metrics: { metric: keyof MonthDatum; suffix: string }[] = [
      { metric: "net_sales", suffix: "Net Sales" },
      { metric: "quantity", suffix: "Quantity" },
      { metric: "profit", suffix: "Profit" },
    ];

    return metrics.map(({ metric, suffix }) => {
      const visibleCountries: CountryKey[] = [];

      // Include globalKey if toggled on
      if (selectedCountries[globalKey] ?? true) {
        visibleCountries.push(globalKey);
      }

      // Include non-empty real countries (UK, US)
      visibleCountries.push(
        ...nonEmptyCountriesFromApi.filter(
          (c) => selectedCountries[c] ?? true
        )
      );

      const datasets = visibleCountries.map((country) =>
        makeDataset(country, metric, suffix)
      );

      return { labels, datasets };
    });
  }, [
    data,
    globalKey,
    nonEmptyCountriesFromApi,
    selectedCountries,
    convertToHomeCurrency,
  ]);


  /* ---------- chart options (currency-aware) ---------- */
  const yAxisLabel = (() => {
    switch (homeCurrency) {
      case "GBP":
        return "Amount (£)";
      case "INR":
        return "Amount (₹)";
      case "CAD":
        return "Amount (CA$)";
      default:
        return "Amount ($)";
    }
  })();


  const chartOptions = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (context: any) => {
              const value = context.parsed.y as number;
              const datasetLabel = context.dataset.label as string;
              const metricPart = (
                datasetLabel
                  .split(" ")
                  .slice(1)
                  .join(" ") || ""
              ).toLowerCase();

              if (
                metricPart.includes("quantity") ||
                metricPart.includes("units")
              ) {
                return `${datasetLabel}: ${value.toLocaleString()}`;
              }

              // 💰 always format in home currency now
              return `${datasetLabel}: ${formatHomeAmount(value)}`;
            },
          },
        },
      },
      scales: {
        x: { title: { display: true, text: "Month" } },
        y: {
          title: { display: true, text: yAxisLabel },
          min: 0,
          ticks: { padding: 0 },
        },
      },
    }),
    [formatHomeAmount, yAxisLabel]
  );


  const yearShort =
    selectedYear === "" ? "" : selectedYear.toString().slice(-2);

  // const getTitle = () => {
  //   if (range === "yearly") return `Last 12 Months'${yearShort}`;
  //   if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
  //   return selectedMonth
  //     ? `${cap(selectedMonth)}'${yearShort}`
  //     : `Year'${yearShort}`;
  // };

  const getTitle = () => {
    if (range === "yearly") return `Last 12 Months'${yearShort}`;
    if (range === "quarterly") return `${selectedQuarter}'${yearShort}`; // selectedQuarter is "Q4"
    return selectedMonth
      ? `${cap(selectedMonth)}'${yearShort}`
      : `Year'${yearShort}`;
  };



  // const getHeadingPeriod = () => {
  //   if (range === "yearly") return `Last 12 Months'${yearShort}`;
  //   if (range === "quarterly") return `Q${selectedQuarter}'${yearShort}`;
  //   if (range === "monthly" && selectedMonth) {
  //     return `${cap(selectedMonth)}'${yearShort}`;
  //   }
  //   return "";
  // };

  const getHeadingPeriod = () => {
    if (range === "yearly") return `Last 12 Months'${yearShort}`;
    if (range === "quarterly") return `${selectedQuarter}'${yearShort}`;
    if (range === "monthly" && selectedMonth) {
      return `${cap(selectedMonth)}'${yearShort}`;
    }
    return "";
  };


  /* ---------- summary cards ---------- */
  const cards = useMemo(() => {
    if (!data?.data) return [] as { country: string; stats: any }[];

    return Object.entries(data.data).map(([country, rawArray]) => {
      const key = country.toLowerCase();
      const monthly: MonthDatum[] = Array.isArray(rawArray)
        ? (rawArray as MonthDatum[])
        : [];

      const totalSales = monthly.reduce((s, m) => s + m.net_sales, 0);
      const totalProfit = monthly.reduce((s, m) => s + m.profit, 0);
      const totalUnits = monthly.reduce((s, m) => s + m.quantity, 0);

      const gross_margin_avg =
        totalSales > 0 ? (totalProfit / totalSales) * 100 : 0;

      const monthsWithSales = monthly.filter((m) => m.net_sales > 0);
      const avgSales =
        monthsWithSales.length > 0 ? totalSales / monthsWithSales.length : 0;

      const avgSellingPrice = totalUnits > 0 ? totalSales / totalUnits : 0;

      const avgMonthlyProfit =
        monthly.length > 0 ? totalProfit / monthly.length : 0;

      const maxSalesMonth =
        monthly.length > 0
          ? monthly.reduce((max, m) =>
            m.net_sales > max.net_sales ? m : max
          )
          : { month: "", net_sales: 0, quantity: 0, profit: 0 };

      const maxUnitsMonth =
        monthly.length > 0
          ? monthly.reduce((max, m) =>
            m.quantity > max.quantity ? m : max
          )
          : { month: "", net_sales: 0, quantity: 0, profit: 0 };

      return {
        // keep the REAL key: "global", "global_inr", "uk", "us"
        country: key,
        stats: {
          totalSales,
          totalProfit,
          totalUnits,
          gross_margin_avg,
          avgSales,
          avgSellingPrice,
          avgMonthlyProfit,
          maxSalesMonth,
          maxUnitsMonth,
        },
      };
    });
  }, [data]);



  const orderedCards = useMemo(() => {
    if (!cards.length) return [];
    const globals = cards.filter((c) => c.country.startsWith("global"));
    const others = cards.filter((c) => !c.country.startsWith("global"));
    return [...globals, ...others];
  }, [cards]);

  /* ---------- render ---------- */
  return (
    <div className="w-full">
      {/* Header + filters row */}
      <div className="mb-6 flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <ProductwiseHeader
          canShowResults={canShowResults}
          countryName={countryName}
          productname={productname}
          headingPeriod={getHeadingPeriod()}
        />

        {/* <FiltersAndSearchRow
          range={range}
          selectedMonth={selectedMonth}
          selectedQuarter={selectedQuarter}
          selectedYear={selectedYear}
          years={years}
          onRangeChange={setRange}
          onMonthChange={setSelectedMonth}
          onQuarterChange={(val) => {
            const num = val.replace("Q", "");
            setSelectedQuarter(num || "1");
          }}
          onYearChange={(val) => {
            if (!val) {
              setSelectedYear("");
            } else {
              setSelectedYear(Number(val));
            }
          }}
        /> */}

        <FiltersAndSearchRow
          range={range}
          selectedMonth={selectedMonth}
          selectedQuarter={selectedQuarter}   // "Q1" | "Q2" | ...
          selectedYear={selectedYear}
          years={years}
          onRangeChange={setRange}
          onMonthChange={setSelectedMonth}
          onQuarterChange={(val) => {
            // val is "Q1", "Q2", etc.
            setSelectedQuarter(val || "Q1");
          }}
          onYearChange={(val) => {
            if (!val) {
              setSelectedYear("");
            } else {
              setSelectedYear(Number(val));
            }
          }}
        />



      </div>

      {!canShowResults && (
        <div className="mt-5 box-border flex w-full items-center justify-between rounded-md border-t-4 border-[#ff5c5c] bg-[#f2f2f2] px-4 py-3 text-sm text-[#414042] lg:max-w-fit">
          <div className="flex items-center">
            <i className="fa-solid fa-circle-exclamation mr-2 text-lg text-[#ff5c5c]" />
            <span>
              Search a product and choose the period to view SKU-wise
              performance.
            </span>
          </div>
        </div>
      )}

      {canShowResults && loading && (
        <div className="flex flex-col items-center justify-center py-12 textcenter">
          <Loader
            src="/infinity-unscreen.gif"
            size={150}
            transparent
            roundedClass="rounded-none"
            backgroundClass="bg-transparent"
            respectReducedMotion
          />
        </div>
      )}

      {canShowResults && !!error && (
        <div className="mb-8 rounded-2xl border border-red-200 bg-red-50 p-6">
          <div className="flex items-center gap-3 text-red-700">
            <span className="text-xl">❌</span>
            <p className="m-0 font-medium">{error}</p>
          </div>
        </div>
      )}

      {canShowResults && data && !loading && (
        <div className="flex flex-col">
          <TrendChartSection
            productname={productname}
            title={getTitle()}
            chartDataList={chartDataList}
            chartOptions={chartOptions}
            nonEmptyCountriesFromApi={nonEmptyCountriesFromApi}
            selectedCountries={selectedCountries}
            onToggleCountry={handleCountryChange}
            authToken={authToken}
            onProductSelect={handleProductSelect}
          />

          <div className="mt-8">
            <div className="grid gap-5 grid-cols-1 md:grid-cols-[repeat(auto-fit,minmax(260px,1fr))]">
              {orderedCards.map((card) => {
                const key = card.country.toLowerCase();
                const isGlobal = key.startsWith("global");

                if (
                  !isGlobal &&
                  card.stats.totalSales === 0 &&
                  card.stats.totalUnits === 0 &&
                  card.stats.totalProfit === 0
                ) {
                  return null;
                }

                return (
                  <CountryCard
                    key={key}
                    country={card.country}
                    stats={card.stats}
                    selectedYear={selectedYear}
                    homeCurrency={homeCurrency}
                  />
                );
              })}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default ProductwisePerformance;
