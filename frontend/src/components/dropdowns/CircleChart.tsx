"use client";

import React, { useEffect, useMemo, useState, useRef } from "react";
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
  countryName: string;
  homeCurrency?: string;
  onExportBase64Ready?: (base64: string | null) => void;
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

const getCurrencySymbol = (codeOrCountry: string) => {
  switch (codeOrCountry.toLowerCase()) {
    case "uk":
    case "gb":
    case "gbp":
      return "£";
    case "india":
    case "in":
    case "inr":
      return "₹";
    case "us":
    case "usa":
    case "usd":
      return "$";
    case "europe":
    case "eu":
    case "eur":
      return "€";
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
  homeCurrency,
  onExportBase64Ready,
}) => {
  const normalizedHomeCurrency = (homeCurrency || "usd").toLowerCase();
  const isGlobalPage = countryName.toLowerCase() === "global";

  const currencySymbol = isGlobalPage
    ? getCurrencySymbol(homeCurrency || "usd") // GLOBAL → home currency
    : getCurrencySymbol(countryName || "");    // Country → country currency

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

  const [isLaptop, setIsLaptop] = useState(false);

  useEffect(() => {
    const check = () => {
      const w = window.innerWidth;
      // adjust range if your “laptop” definition differs
      setIsLaptop(w >= 1024 && w < 1536); // Tailwind lg..xl
    };
    check();
    window.addEventListener("resize", check);
    return () => window.removeEventListener("resize", check);
  }, []);


  const chartRef = useRef<any>(null);

  const exportChartBase64 = () => {
    try {
      const chart = chartRef.current;
      if (!chart) return null;

      // ✅ ensure fully rendered frame
      chart.update("none");

      const srcCanvas = chart.canvas as HTMLCanvasElement;

      // ✅ upscale helps a LOT for Excel
      const scale = 3;

      const out = document.createElement("canvas");
      out.width = srcCanvas.width * scale;
      out.height = srcCanvas.height * scale;

      const ctx = out.getContext("2d");
      if (!ctx) return null;

      ctx.imageSmoothingEnabled = true;
      ctx.imageSmoothingQuality = "high";

      // ✅ force solid background (removes alpha seams)
      ctx.fillStyle = "#FFFFFF";
      ctx.fillRect(0, 0, out.width, out.height);

      ctx.drawImage(srcCanvas, 0, 0, out.width, out.height);

      // ✅ export JPEG (no alpha)
      return out.toDataURL("image/jpeg", 0.98);
    } catch {
      return null;
    }
  };



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

      if (countryName.toLowerCase() === "global" && homeCurrency) {
        params.append("homeCurrency", homeCurrency);
      }


      if (range === "monthly" && month) {
        params.append("month", month);
      } else if (range === "quarterly" && selectedQuarter) {
        params.append("quarter", selectedQuarter);
      }

      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_history2?${params.toString()}`,
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
  }, [month, year, range, selectedQuarter, countryName, normalizedHomeCurrency]);

  // Build chart data from summary
  // useEffect(() => {
  //   if (!uploadsData?.summary) {
  //     setChartData(null);
  //     return;
  //   }
  //   const s = uploadsData.summary;

  //   const labels = [
  //     "COGS",
  //     "Amazon Fees",
  //     "Taxes & Credits",
  //     "Advertisement Cost",
  //     "Other Expense",
  //     "CM2 Profit",
  //   ];

  //   const values = [
  //     Math.abs(s.total_cous || 0),
  //     Math.abs(s.total_amazon_fee || 0),
  //     Math.abs(s.taxncredit || 0),
  //     Math.abs(s.advertising_total || 0),
  //     Math.abs(s.otherwplatform || 0),
  //     Math.abs(s.cm2_profit || 0),
  //   ];

  //   const colors = ["#FDD36F", "#B75A5A", "#ED9F50", "#C49466", "#3A8EA4", "#B8C78C"];

  //   const next: ChartData<"pie", number[], string> = {
  //     labels,
  //     datasets: [
  //       {
  //         data: values,
  //         backgroundColor: colors,

  //         // ✅ IMPORTANT: no borders (borders are causing that white wedge)
  //         borderWidth: 0,
  //         borderColor: "transparent",

  //         spacing: 0,
  //         hoverOffset: 0,
  //         offset: 0,
  //       },
  //     ],
  //   };


  //   setChartData(next);
  // }, [uploadsData]);

  useEffect(() => {
    if (!uploadsData?.summary) {
      setChartData(null);
      return;
    }

    const s = uploadsData.summary;

    const next: ChartData<"pie", number[], string> = {
      labels: ["COGS", "Amazon Fees", "Tax and credits", "Ads", "Others", "CM2 Profit"],
      datasets: [
        {
          data: [
            Math.abs(s.total_cous || 0),
            Math.abs(s.total_amazon_fee || 0),
            Math.abs(s.taxncredit || 0),
            Math.abs(s.advertising_total || 0),
            Math.abs(s.otherwplatform || 0),
            Math.abs(s.cm2_profit || 0),
          ],
          backgroundColor: ["#FDD36F", "#B75A5A", "#ED9F50", "#C49466", "#3A8EA4", "#B8C78C"],
          borderWidth: 0,
          borderColor: "transparent",
          spacing: 0,
          hoverOffset: 0,
          offset: 0,
        },
      ],
    };

    setChartData(next);
  }, [uploadsData]);


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
              "#FDD36F", "#B75A5A", "#ED9F50", "#C49466", "#3A8EA4", "#B8C78C"
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

  useEffect(() => {
    if (!displayChartData) {
      onExportBase64Ready?.(null);
      return;
    }

    // wait a tick so canvas is painted
    const t = setTimeout(() => {
      const base64 = exportChartBase64();
      onExportBase64Ready?.(base64);
    }, 300);

    return () => clearTimeout(t);
  }, [displayChartData, onExportBase64Ready]);

  const isSmallScreen = legendPosition === "bottom"; // since you already switch at <768

  const options = useMemo<ChartOptions<"pie">>(() => ({
    responsive: true,
    elements: { arc: { borderWidth: 0 } },
    plugins: {
      legend: {
        position: legendPosition,
        align: "center",
        labels: {
          usePointStyle: true,

          // ✅ legend text color
          color: "#DC2626", // test with red first

          // ✅ 12 desktop, 10 mobile
          font: {
            size: isSmallScreen ? 10 : 12,
          },

          generateLabels: (chart) => {
            const data = chart.data;
            const labels = (data.labels || []) as string[];

            const dataset = data.datasets?.[0] as any;
            const rawValues = ((dataset?.data || []) as number[]).map((v) =>
              Math.abs(Number(v || 0))
            );
            const total = rawValues.reduce((a, b) => a + b, 0);
            const bg = dataset?.backgroundColor as any[];

            return labels.map((label, i) => {
              const value = rawValues[i] ?? 0;
              const pct = total ? (value / total) * 100 : 0;

              return {
                text: `${label} (${pct.toFixed(2)}%)`,
                fillStyle: Array.isArray(bg) ? bg[i] : bg,
                strokeStyle: "transparent",
                lineWidth: 0,
                hidden: !chart.getDataVisibility(i),
                index: i,
                pointStyle: "circle",
              };
            });
          },
        },
      },
      tooltip: { /* keep your tooltip */ },
    },
    layout: { padding: isLaptop ? 0 : 10 },
    animation: { duration: 0 },
    maintainAspectRatio: false,
  }), [legendPosition, isLaptop, currencySymbol]);

  return (
    <div className="relative w-full rounded-xl border border-slate-200 bg-white shadow-sm p-4">
      {/* Heading */}
      <div className="2xl:mb-4">
        <div className="w-fit mx-auto md:mx-0">
          <PageBreadcrumb
            pageTitle={`Expense Breakup`}
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
              "max-w-[360px] sm:max-w-[460px] md:max-w-[600px] 2xl:max-w-[720px]",
              "relative",
            ].join(" ")}
          >
            <div
              className={[
                "relative",
                "h-[240px] sm:h-[280px] md:h-[280px] 2xl:h-[360px]",
                "flex justify-center", // center horizontally
                isLaptop ? "items-center px-4 py-1" : "items-center", // ✅ laptop: less top, more bottom, add side padding
              ].join(" ")}
            >
              <Pie className="!block" ref={chartRef} data={displayChartData} options={options} redraw/>
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
