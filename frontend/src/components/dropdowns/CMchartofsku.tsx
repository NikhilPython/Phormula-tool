"use client";

import React, { useEffect, useMemo, useState, useRef } from "react";
import { Pie } from "react-chartjs-2";
import {
  Chart as ChartJS,
  ArcElement,
  Tooltip,
  Legend,
  ChartData,
  ChartOptions,
  TooltipItem,
} from "chart.js";
import PageBreadcrumb from "../common/PageBreadCrumb";

ChartJS.register(ArcElement, Tooltip, Legend);

type Range = "monthly" | "quarterly" | "yearly";
type Quarter = "Q1" | "Q2" | "Q3" | "Q4";

type CmChartOfSkuProps = {
  range: Range;
  month?: string;
  year: number | string;
  selectedQuarter?: Quarter;
  userId?: string | number;
  countryName: string;
  homeCurrency?: string;
  onExportBase64Ready?: (base64: string | null) => void;
};


type PieApiSuccess = {
  success: true;
  data: { labels: string[]; values: number[] };
};

type PieApiError = {
  success?: false;
  error?: string;
};

type PieApiResponse = PieApiSuccess | PieApiError;

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


const CMchartofsku: React.FC<CmChartOfSkuProps> = ({
  range,
  month,
  year,
  selectedQuarter,
  userId, // currently unused
  countryName,
  homeCurrency,
  onExportBase64Ready
}) => {
  const normalizedHomeCurrency = (homeCurrency || "usd").toLowerCase();
  const isGlobalPage = countryName.toLowerCase() === "global";

  const currencySymbol = isGlobalPage
    ? getCurrencySymbol(homeCurrency || "usd")
    : getCurrencySymbol(countryName || "");

  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [chartData, setChartData] =
    useState<ChartData<"pie", number[], string> | null>(null);
  const [noDataFound, setNoDataFound] = useState<boolean>(false);

  // Responsive legend position (TS-safe)
  const [legendPosition, setLegendPosition] = useState<
    "top" | "left" | "bottom" | "right"
  >(
    typeof window !== "undefined" && window.innerWidth < 768
      ? "bottom"
      : "right"
  );

  const chartRef = useRef<any>(null);

  const exportChartBase64 = () => {
    try {
      const chart = chartRef.current;
      if (!chart) return null;

      // ✅ freeze final frame
      chart.update("none");

      const srcCanvas = chart.canvas as HTMLCanvasElement;

      const scale = 3; // ✅ helps Excel render cleanly
      const out = document.createElement("canvas");
      out.width = srcCanvas.width * scale;
      out.height = srcCanvas.height * scale;

      const ctx = out.getContext("2d");
      if (!ctx) return null;

      ctx.imageSmoothingEnabled = true;
      ctx.imageSmoothingQuality = "high";

      // ✅ remove alpha (fixes wedge seams in Excel)
      ctx.fillStyle = "#FFFFFF";
      ctx.fillRect(0, 0, out.width, out.height);

      ctx.drawImage(srcCanvas, 0, 0, out.width, out.height);

      // ✅ export JPEG
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

  const getDummyData = (): ChartData<"pie", number[], string> => ({
    labels: ["Product A", "Product B", "Product C", "Product D", "Product E"],
    datasets: [
      {
        data: [25, 30, 20, 15, 10],
        backgroundColor: [
          "#FDD36F", "#B75A5A", "#ED9F50", "#C49466", "#3A8EA4", "#B8C78C"
        ],
        borderWidth: 1,
      },
    ],
  });

  useEffect(() => {
    async function fetchData() {
      setLoading(true);
      setError(null);
      setNoDataFound(false);

      try {
        const token =
          typeof window !== "undefined"
            ? localStorage.getItem("jwtToken")
            : null;
        const params = new URLSearchParams({
          country: countryName || "",
          year: String(year ?? ""),
          range: range || "",
        });
        if (countryName.toLowerCase() === "global" && homeCurrency) {
          params.append("homeCurrency", homeCurrency);
        }


        if (range === "monthly" && month) {
          params.append("month", month);
        } else if (range === "quarterly" && selectedQuarter) {
          params.append("quarter", selectedQuarter);
        }

        const res = await fetch(
          `http://127.0.0.1:5000/pie-chart?${params.toString()}`,
          {
            method: "GET",
            headers: token ? { Authorization: `Bearer ${token}` } : {},
          }
        );

        if (!res.ok) {
          let serverError = "";
          try {
            const errBody: PieApiError = await res.json();
            serverError = errBody.error || "Failed to fetch data";
          } catch {
            serverError = "Failed to fetch data";
          }
          throw new Error(serverError);
        }

        const json: PieApiResponse = await res.json();

        const noDataPhrase = "no data found in any of the available tables";
        const textError = (json as PieApiError).error?.toLowerCase() || "";

        if ("success" in json && json.success && json.data) {
          const labels = json.data.labels || [];
          const values = (json.data.values || []).map((v) =>
            Math.abs(Number(v || 0))
          );

          const isEmpty =
            labels.length === 0 || values.length === 0 || values.every((v) => v === 0);

          if (isEmpty) {
            setNoDataFound(true);
            setChartData(getDummyData());
            setLoading(false);
            return;
          }

          const next: ChartData<"pie", number[], string> = {
            labels,
            datasets: [
              {
                data: values,
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
          setNoDataFound(false);
        } else if (textError.includes(noDataPhrase)) {
          setNoDataFound(true);
          setChartData(getDummyData());
          setError(null);
        } else {
          setNoDataFound(true);
          setChartData(getDummyData());
          setError(
            (json as PieApiError).error || "Failed to fetch valid chart data"
          );
        }
      } catch (e) {
        const msg = e instanceof Error ? e.message : "Unknown error";
        if (msg.toLowerCase().includes("no data found in any of the available tables")) {
          setNoDataFound(true);
          setChartData(getDummyData());
          setError(null);
        } else {
          setError(msg);
        }
      } finally {
        setLoading(false);
      }
    }

    fetchData();
  }, [
    range,
    month,
    year,
    selectedQuarter,
    countryName,
    userId,
    normalizedHomeCurrency, // 👈 NEW
  ]);

  const capitalizeFirstLetter = (str: string) =>
    str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
  const convertToAbbreviatedMonth = (m?: string) =>
    m ? capitalizeFirstLetter(m).slice(0, 3) : "";

  const titleNode = useMemo(() => {
    const y = String(year);
    if (range === "quarterly") {
      return (
        <div className="flex flex-wrap items-baseline gap-2 justify-center sm:justify-start">
          <PageBreadcrumb
            pageTitle="CM1 Breakup"
            variant="page"
            align="left"
            textSize="2xl"
          />
          {/* <span className="text-[#5EA68E] font-bold text-lg sm:text-2xl md:text-2xl">
            {countryName?.toLowerCase() === "global"
              ? "GLOBAL"
              : countryName?.toUpperCase()}
          </span> */}
        </div>
      );
    }
    if (range === "monthly") {
      return (
        <div className="flex gap-2">
          <PageBreadcrumb
            pageTitle="CM1 Breakup"
            variant="page"
            align="left"
            textSize="2xl"
          />
          {/* <span className="text-[#5EA68E] text-2xl">
            {convertToAbbreviatedMonth(month)}&apos;{y.slice(-2)}
          </span> */}
        </div>
      );
    }
    return (
      <div className="flex gap-2">
        <PageBreadcrumb
          pageTitle="CM1 Breakup"
          variant="page"
          align="left"
          textSize="2xl"
        />
        {/* <span className="text-[#5EA68E] font-bold text-lg sm:text-2xl md:text-2xl">
          Year&apos;{y.slice(-2)}
        </span> */}
      </div>
    );
  }, [range, month, year, selectedQuarter, countryName]);

const options: ChartOptions<"pie"> = {
  responsive: true,
  elements: {
    arc: {
      borderWidth: 0,
    },
  },
  plugins: {
    legend: {
      position: legendPosition,
      align: "center",
      labels: {
        usePointStyle: true,

        // ✅ Change legend label text color here
        color: "#414042", // <-- change to whatever you want (e.g. "#FF0000")

        // ✅ Show ALL labels + percentage in legend
        generateLabels: (chart) => {
          const data = chart.data;
          const labels = (data.labels || []) as string[];

          const dataset = data.datasets?.[0] as any;
          const values = ((dataset?.data || []) as number[]).map((v) =>
            Math.abs(Number(v || 0))
          );

          const total = values.reduce((a, b) => a + b, 0);

          const bg = dataset?.backgroundColor as any[]; // array of colors

          return labels.map((label, i) => {
            const value = values[i] ?? 0;
            const pct = total ? (value / total) * 100 : 0;

            return {
              text: `${label} (${pct.toFixed(2)}%)`,
              fillStyle: Array.isArray(bg) ? bg[i] : bg,
              strokeStyle: "transparent",
              lineWidth: 0,
              hidden: !chart.getDataVisibility(i),
              index: i, // ✅ important for toggling slice
              pointStyle: "circle",
            };
          });
        },
      },
    },
    tooltip: {
      enabled: !noDataFound,
      callbacks: {
        label: (ctx: TooltipItem<"pie">) => {
          const value = Math.abs(Number(ctx.raw ?? 0));
          const ds = ctx.chart.data.datasets?.[ctx.datasetIndex] as
            | { data: number[] }
            | undefined;

          const total = (ds?.data ?? []).reduce(
            (acc, v) => acc + Math.abs(Number(v || 0)),
            0
          );

          const pct = total ? (value / total) * 100 : 0;
          const label = ctx.label ? `${ctx.label}: ` : "";

          return `${label}${currencySymbol}${value.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })} (${pct.toFixed(2)}%)`;
        },
      },
    },
  },
  layout: {
    padding: { top: 18, bottom: 18, left: 18, right: 52 },
  },
  animation: { duration: 0 },
  maintainAspectRatio: false,
};


  useEffect(() => {
    if (!chartData || loading || error) {
      onExportBase64Ready?.(null);
      return;
    }

    const t = setTimeout(() => {
      const base64 = exportChartBase64();
      onExportBase64Ready?.(base64);
    }, 300);

    return () => clearTimeout(t);
  }, [chartData, loading, error, onExportBase64Ready]);


  return (
    <div className="relative w-full rounded-xl border border-slate-200 bg-white shadow-sm p-4 sm:p-5">
      {/* Heading */}
      <div className="mb-4">
        <div className="w-fit mx-auto md:mx-0">
          <PageBreadcrumb
            pageTitle={`CM1 Breakup`}
            variant="page"
            align="left"
            textSize="2xl"
          />
        </div>
      </div>

      {/* Chart */}
      <div
        className={[
          "w-full",
          noDataFound ? "opacity-30" : "opacity-100",
          "transition-opacity duration-300",
        ].join(" ")}
      >
        {loading && (
          <p className="text-center text-sm text-gray-500">
            Loading chart data...
          </p>
        )}
        {error && (
          <p className="text-center text-sm text-red-600">Error: {error}</p>
        )}

        {chartData && !loading && !error && (
          <div
            className={[
              "mx-auto",
              "w-full",
              "max-w-[260px] sm:max-w-[320px] md:max-w-[420px] lg:max-w-[520px]",
              "relative",
            ].join(" ")}
          >
            <div className="relative h-[260px] sm:h-[300px] md:h-[340px] lg:h-[380px]">
              {/* <Pie data={chartData} options={options} /> */}
              <Pie ref={chartRef} data={chartData} options={options} />
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default CMchartofsku;
