// "use client";

// import React, { useEffect, useMemo, useState } from "react";
// import { Pie } from "react-chartjs-2";
// import {
//   Chart as ChartJS,
//   ArcElement,
//   Tooltip,
//   Legend,
//   ChartOptions,
//   ChartData,
//   TooltipItem,
// } from "chart.js";
// import PageBreadcrumb from "../common/PageBreadCrumb";

// ChartJS.register(ArcElement, Tooltip, Legend);

// type Range = "monthly" | "quarterly" | "yearly";
// type Quarter = "Q1" | "Q2" | "Q3" | "Q4";

// type CircleChartProps = {
//   range: Range;
//   month?: string;
//   year: number | string;
//   selectedQuarter?: Quarter;
//   /** Supply from parent (Next.js route params) */
//   countryName: string;
// };

// type Summary = {
//   advertising_total: number;
//   cm2_profit: number;
//   total_amazon_fee: number;
//   taxncredit: number;
//   total_cous: number;
//   otherwplatform: number;
// };

// type UploadHistoryResponse = {
//   uploads?: unknown[];
//   summary?: Summary;
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

// const capitalizeFirstLetter = (str: string) =>
//   str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();

// const convertToAbbreviatedMonth = (m?: string) =>
//   m ? capitalizeFirstLetter(m).slice(0, 3) : "";

// const CircleChart: React.FC<CircleChartProps> = ({
//   range,
//   month,
//   year,
//   selectedQuarter,
//   countryName,
// }) => {
//   const currencySymbol = getCurrencySymbol(countryName || "");
//   const [uploadsData, setUploadsData] = useState<UploadHistoryResponse | null>(null);
//   const [chartData, setChartData] = useState<ChartData<"pie", number[], string> | null>(null);
//   const [displayChartData, setDisplayChartData] =
//     useState<ChartData<"pie", number[], string> | null>(null);
//   const [allValuesZero, setAllValuesZero] = useState(false);

//   // Legend position responsive handling (TS-safe)
//   const [legendPosition, setLegendPosition] = useState<"top" | "left" | "bottom" | "right">(
//     typeof window !== "undefined" && window.innerWidth < 768 ? "bottom" : "right"
//   );

//   useEffect(() => {
//     const onResize = () => setLegendPosition(window.innerWidth < 768 ? "bottom" : "right");
//     if (typeof window !== "undefined") {
//       window.addEventListener("resize", onResize);
//       return () => window.removeEventListener("resize", onResize);
//     }
//   }, []);

//   const fetchUploadHistory = async () => {
//     try {
//       const token = typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

//       const params = new URLSearchParams({
//         range,
//         country: countryName || "",
//         year: String(year ?? ""),
//       });

//       if (range === "monthly" && month) {
//         params.append("month", month);
//       } else if (range === "quarterly" && selectedQuarter) {
//         params.append("quarter", selectedQuarter);
//       }

//       const response = await fetch(
//         `http://127.0.0.1:5000/upload_history2?${params.toString()}`,
//         {
//           method: "GET",
//           headers: token ? { Authorization: `Bearer ${token}` } : {},
//         }
//       );

//       if (!response.ok) {
//         // eslint-disable-next-line no-console
//         console.error("Error fetching data:", await response.text());
//         return;
//       }

//       const data = (await response.json()) as UploadHistoryResponse;
//       setUploadsData(data);
//     } catch (error) {
//       // eslint-disable-next-line no-console
//       console.error("Fetch error:", error);
//     }
//   };

//   useEffect(() => {
//     fetchUploadHistory();
//     // eslint-disable-next-line react-hooks/exhaustive-deps
//   }, [month, year, range, selectedQuarter, countryName]);

//   // Build chart data from summary
//   useEffect(() => {
//     if (!uploadsData?.summary) {
//       setChartData(null);
//       return;
//     }
//     const s = uploadsData.summary;

//     const labels = [
//       "COGS",
//       "Amazon Fees",
//       "Taxes & Credits",
//       "Advertisement Cost",
//       "Other Expense",
//       "CM2 Profit",
//     ];

//     const values = [
//       Math.abs(s.total_cous || 0),
//       Math.abs(s.total_amazon_fee || 0),
//       Math.abs(s.taxncredit || 0),
//       Math.abs(s.advertising_total || 0),
//       Math.abs(s.otherwplatform || 0),
//       Math.abs(s.cm2_profit || 0),
//     ];

//     const next: ChartData<"pie", number[], string> = {
//       labels,
//       datasets: [
//         {
//           data: values,
//           backgroundColor: ["#AB64B5", "#ff5c5c", "#154B9B", "#F47A00", "#00627D", "#87AD12"],
//           borderWidth: 1,
//         },
//       ],
//     };
//     setChartData(next);
//   }, [uploadsData]);

//   // Fallback dummy data when all zeros
//   useEffect(() => {
//     if (!chartData || !chartData.labels || !chartData.datasets?.[0]?.data) {
//       setAllValuesZero(false);
//       setDisplayChartData(null);
//       return;
//     }

//     const vals = (chartData.datasets[0].data as number[]) || [];
//     const isZero = vals.every((v) => v === 0);
//     setAllValuesZero(isZero);

//     if (isZero) {
//       const dummyValues = [25, 20, 15, 10, 18, 12]; // 6 values to match 6 labels
//       const dummy: ChartData<"pie", number[], string> = {
//         labels: chartData.labels as string[],
//         datasets: [
//           {
//             data: dummyValues,
//             backgroundColor: ["#AB64B5", "#ff5c5c", "#154B9B", "#F47A00", "#00627D", "#87AD12"],
//             borderWidth: 1,
//           },
//         ],
//       };
//       setDisplayChartData(dummy);
//     } else {
//       setDisplayChartData(chartData);
//     }
//   }, [chartData]);

//   const titleNode = useMemo(() => {
//     const y = String(year);
//     if (range === "quarterly") {
//       return (
//         <div className="flex gap-2">
//           <PageBreadcrumb pageTitle=" Expense Breakdown - " variant="page" align="left" textSize="2xl" />
//           <span className="text-[#5EA68E] font-bold text-lg sm:text-2xl md:text-2xl">
//             {countryName?.toLowerCase() === "global"
//               ? "GLOBAL"
//               : countryName?.toUpperCase()}
//           </span>
//         </div>
//       );
//     }
//     if (range === "monthly") {
//       return (
//         <div className="flex gap-2">
//           <PageBreadcrumb pageTitle=" Expense Breakdown - " variant="page" align="left" textSize="2xl" />
//           <span className="text-[#5EA68E] font-bold text-lg sm:text-2xl md:text-2xl">
//             {countryName?.toLowerCase() === "global"
//               ? "GLOBAL"
//               : countryName?.toUpperCase()}
//           </span>
//         </div>
//       );
//     }
//     return (

//       <div className="flex gap-2">
//         <PageBreadcrumb pageTitle="Expense Breakdown - " variant="page" align="left" textSize="2xl" />
//         <span className="text-[#5EA68E] font-bold text-lg sm:text-2xl md:text-2xl">
//           Year&apos;{y.slice(-2)}
//         </span>
//       </div>
//     );
//   }, [range, month, year, selectedQuarter]);

//   const options: ChartOptions<"pie"> = {
//     responsive: true,
//     // maintainAspectRatio: true,
//     plugins: {
//       legend: {
//         position: legendPosition,
//         align: "center",
//         labels: {
//           usePointStyle: true,
//         },
//       },
//       tooltip: {
        
//         callbacks: {
//           label: (ctx: TooltipItem<"pie">) => {
//             const value = Math.abs(Number(ctx.raw ?? 0));
//             const dataset = ctx.chart.data.datasets?.[ctx.datasetIndex] as
//               | { data: number[] }
//               | undefined;
//             const total = (dataset?.data ?? []).reduce(
//               (acc, v) => acc + Math.abs(Number(v || 0)),
//               0
//             );
//             const pct = total ? (value / total) * 100 : 0;
//             const label = ctx.label ? `${ctx.label}: ` : "";
//             return `${label}${currencySymbol}${value.toLocaleString(undefined, {
//               minimumFractionDigits: 2,
//               maximumFractionDigits: 2,
//             })} (${pct.toFixed(2)}%)`;
//           },
//         },
//       },
//     },
//     layout: {
//       padding: 0,
//     },
//     animation: {
//       duration: 900,
//     },
//     maintainAspectRatio: false,
//   };

//   return (
//     <div className="relative w-full">

//       <div className="mb-4">
//         {/* responsive wrapper JUST for this heading */}
//         <div className="w-fit mx-auto md:mx-0">
//           <PageBreadcrumb
//             pageTitle={`Expense Breakdown – <span class='text-[#5EA68E] font-bold'>
//         ${range === "yearly"
//                 ? `Year'${String(year).slice(-2)}`
//                 : countryName?.toLowerCase() === "global"
//                   ? "GLOBAL"
//                   : countryName?.toUpperCase()
//               }
//       </span>`}
//             variant="page"
//             textSize="2xl"
//             align="left"
//           />
//         </div>
//       </div>


//       <div className="w-full">
//         {displayChartData && displayChartData.labels && displayChartData.datasets?.length ? (
//           <div
//             className={[
//               "mx-auto",
//               "w-full",
//               "max-w-[260px] sm:max-w-[320px] md:max-w-[420px] lg:max-w-[520px]",
//               "relative",
//             ].join(" ")}
//           >

//             <div className="relative h-[260px] sm:h-[300px] md:h-[340px] lg:h-[380px]">
//               <Pie data={displayChartData} options={options} />
//             </div>

//           </div>

//         ) : (
//           <p className="text-center text-sm text-gray-500">Loading chart data...</p>
//         )}
//       </div>
//     </div>
//   );
// };

// export default CircleChart;






























"use client";

import React, { useEffect, useMemo, useState } from "react";
import { Pie } from "react-chartjs-2";
import {
  Chart as ChartJS,
  ArcElement,
  Tooltip,
  Legend,
  ChartOptions,
  ChartData,
  TooltipItem,
} from "chart.js";
import PageBreadcrumb from "../common/PageBreadCrumb";

ChartJS.register(ArcElement, Tooltip, Legend);

type Range = "monthly" | "quarterly" | "yearly";
type Quarter = "Q1" | "Q2" | "Q3" | "Q4";

type CircleChartProps = {
  range: Range;
  month?: string;
  year: number | string;
  selectedQuarter?: Quarter;
  /** Supply from parent (Next.js route params) */
  countryName: string;
};

type Summary = {
  advertising_total: number;
  cm2_profit: number;
  total_amazon_fee: number;
  taxncredit: number;
  total_cous: number;
  otherwplatform: number;
};

type UploadHistoryResponse = {
  uploads?: unknown[];
  summary?: Summary;
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

const capitalizeFirstLetter = (str: string) =>
  str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();

const convertToAbbreviatedMonth = (m?: string) =>
  m ? capitalizeFirstLetter(m).slice(0, 3) : "";

const CircleChart: React.FC<CircleChartProps> = ({
  range,
  month,
  year,
  selectedQuarter,
  countryName,
}) => {
  const currencySymbol = getCurrencySymbol(countryName || "");
  const [uploadsData, setUploadsData] =
    useState<UploadHistoryResponse | null>(null);
  const [chartData, setChartData] =
    useState<ChartData<"pie", number[], string> | null>(null);
  const [displayChartData, setDisplayChartData] =
    useState<ChartData<"pie", number[], string> | null>(null);
  const [allValuesZero, setAllValuesZero] = useState(false);

  // Legend position responsive handling (TS-safe)
  const [legendPosition, setLegendPosition] = useState<
    "top" | "left" | "bottom" | "right"
  >(
    typeof window !== "undefined" && window.innerWidth < 768
      ? "bottom"
      : "right"
  );

  useEffect(() => {
    const onResize = () =>
      setLegendPosition(window.innerWidth < 768 ? "bottom" : "right");
    if (typeof window !== "undefined") {
      window.addEventListener("resize", onResize);
      return () => window.removeEventListener("resize", onResize);
    }
  }, []);

  const fetchUploadHistory = async () => {
    try {
      const token =
        typeof window !== "undefined"
          ? localStorage.getItem("jwtToken")
          : null;

      const params = new URLSearchParams({
        range,
        country: countryName || "",
        year: String(year ?? ""),
      });

      if (range === "monthly" && month) {
        params.append("month", month);
      } else if (range === "quarterly" && selectedQuarter) {
        params.append("quarter", selectedQuarter);
      }

      const response = await fetch(
        `http://127.0.0.1:5000/upload_history2?${params.toString()}`,
        {
          method: "GET",
          headers: token ? { Authorization: `Bearer ${token}` } : {},
        }
      );

      if (!response.ok) {
        console.error("Error fetching data:", await response.text());
        return;
      }

      const data = (await response.json()) as UploadHistoryResponse;
      setUploadsData(data);
    } catch (error) {
      console.error("Fetch error:", error);
    }
  };

  useEffect(() => {
    fetchUploadHistory();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [month, year, range, selectedQuarter, countryName]);

  // Build chart data from summary
  useEffect(() => {
    if (!uploadsData?.summary) {
      setChartData(null);
      return;
    }
    const s = uploadsData.summary;

    const labels = [
      "COGS",
      "Amazon Fees",
      "Taxes & Credits",
      "Advertisement Cost",
      "Other Expense",
      "CM2 Profit",
    ];

    const values = [
      Math.abs(s.total_cous || 0),
      Math.abs(s.total_amazon_fee || 0),
      Math.abs(s.taxncredit || 0),
      Math.abs(s.advertising_total || 0),
      Math.abs(s.otherwplatform || 0),
      Math.abs(s.cm2_profit || 0),
    ];

    const next: ChartData<"pie", number[], string> = {
      labels,
      datasets: [
        {
          data: values,
          backgroundColor: [
            "#AB64B5",
            "#ff5c5c",
            "#FFBE26",
            "#F47A00",
            "#00627D",
            "#5EA49B",
          ],
          borderWidth: 1,
        },
      ],
    };
    setChartData(next);
  }, [uploadsData]);

  // Fallback dummy data when all zeros
  useEffect(() => {
    if (!chartData || !chartData.labels || !chartData.datasets?.[0]?.data) {
      setAllValuesZero(false);
      setDisplayChartData(null);
      return;
    }

    const vals = (chartData.datasets[0].data as number[]) || [];
    const isZero = vals.every((v) => v === 0);
    setAllValuesZero(isZero);

    if (isZero) {
      const dummyValues = [25, 20, 15, 10, 18, 12]; // 6 values to match 6 labels
      const dummy: ChartData<"pie", number[], string> = {
        labels: chartData.labels as string[],
        datasets: [
          {
            data: dummyValues,
            backgroundColor: [
              "#AB64B5",
              "#ff5c5c",
              "#FFBE26",
              "#F47A00",
              "#00627D",
              "#5EA49B",
            ],
            borderWidth: 1,
          },
        ],
      };
      setDisplayChartData(dummy);
    } else {
      setDisplayChartData(chartData);
    }
  }, [chartData]);

  const titleNode = useMemo(() => {
    const y = String(year);
    if (range === "quarterly" || range === "monthly") {
      return (
        <div className="flex gap-2">
          <PageBreadcrumb
            pageTitle="Expense Breakdown -"
            variant="page"
            align="left"
            textSize="2xl"
          />
          <span className="text-[#5EA68E] font-bold text-lg sm:text-2xl md:text-2xl">
            {countryName?.toLowerCase() === "global"
              ? "GLOBAL"
              : countryName?.toUpperCase()}
          </span>
        </div>
      );
    }
    return (
      <div className="flex gap-2">
        <PageBreadcrumb
          pageTitle="Expense Breakdown - "
          variant="page"
          align="left"
          textSize="2xl"
        />
        <span className="text-[#5EA68E] font-bold text-lg sm:text-2xl md:text-2xl">
          Year&apos;{y.slice(-2)}
        </span>
      </div>
    );
  }, [range, month, year, selectedQuarter, countryName]);

  const options: ChartOptions<"pie"> = {
    responsive: true,
    plugins: {
      legend: {
        position: legendPosition,
        align: "center",
        labels: {
          usePointStyle: true,
        },
      },
      tooltip: {
        callbacks: {
          label: (ctx: TooltipItem<"pie">) => {
            const value = Math.abs(Number(ctx.raw ?? 0));
            const dataset = ctx.chart.data.datasets?.[ctx.datasetIndex] as
              | { data: number[] }
              | undefined;
            const total = (dataset?.data ?? []).reduce(
              (acc, v) => acc + Math.abs(Number(v || 0)),
              0
            );
            const pct = total ? (value / total) * 100 : 0;
            const label = ctx.label ? `${ctx.label}: ` : "";
            return `${label}${currencySymbol}${value.toLocaleString(
              undefined,
              {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              }
            )} (${pct.toFixed(2)}%)`;
          },
        },
      },
    },
    layout: {
      padding: 0,
    },
    animation: {
      duration: 900,
    },
    maintainAspectRatio: false,
  };

  return (
    <div className="relative w-full rounded-xl border border-slate-200 bg-white shadow-sm p-4 sm:p-5">
      {/* Heading */}
      <div className="mb-4">
        <div className="w-fit mx-auto md:mx-0">
          <PageBreadcrumb
            pageTitle={`Expense Breakdown – <span class='text-[#5EA68E] font-bold'>
        ${
          range === "yearly"
            ? `Year'${String(year).slice(-2)}`
            : countryName?.toLowerCase() === "global"
            ? "GLOBAL"
            : countryName?.toUpperCase()
        }
      </span>`}
            variant="page"
            textSize="2xl"
            align="left"
          />
        </div>
      </div>

      {/* Chart */}
      <div
        className={[
          "w-full",
          allValuesZero ? "opacity-30" : "opacity-100",
          "transition-opacity duration-300",
        ].join(" ")}
      >
        {displayChartData &&
        displayChartData.labels &&
        displayChartData.datasets?.length ? (
          <div
            className={[
              "mx-auto",
              "w-full",
              "max-w-[260px] sm:max-w-[320px] md:max-w-[420px] lg:max-w-[520px]",
              "relative",
            ].join(" ")}
          >
            <div className="relative h-[260px] sm:h-[300px] md:h-[340px] lg:h-[380px]">
              <Pie data={displayChartData} options={options} />
            </div>
          </div>
        ) : (
          <p className="text-center text-sm text-gray-500">
            Loading chart data...
          </p>
        )}
      </div>
    </div>
  );
};

export default CircleChart;
