"use client";

import React, { useMemo } from "react";
import dynamic from "next/dynamic";
import { ApexOptions } from "apexcharts";
import PageBreadcrumb from "../PageBreadCrumb";

const ReactApexChart = dynamic(() => import("react-apexcharts"), {
  ssr: false,
});

export type AgeingTrendItem = {
  label: string;
  value: number;
};

export type AgeingTrendBucketOption = {
  label: string;
  value: string;
  color: string;
};

type AgeingTrendChartProps = {
  title?: string;
  subtitle?: string;
  selectedBucket: string;
  data: AgeingTrendItem[];
  lineColor: string;
  showChange?: boolean;

  bucketOptions?: AgeingTrendBucketOption[];
  onBucketChange?: (bucketValue: string) => void;
};

const AgeingTrendChart: React.FC<AgeingTrendChartProps> = ({
  title = "Ageing Trend Over Time",
  subtitle = "Track how old inventory is increasing or decreasing",
  selectedBucket,
  data,
  lineColor,
  showChange = true,
  bucketOptions = [],
  onBucketChange,
}) => {
  const currentMonthShort = new Date().toLocaleString("default", {
    month: "short",
  });

  const getShortMonth = (label: string) => {
    const monthPart = label.split(" ")[0];

    const date = new Date(`${monthPart} 1, 2000`);

    if (Number.isNaN(date.getTime())) {
      return monthPart;
    }

    return date.toLocaleString("default", {
      month: "short",
    });
  };

  const chartData = useMemo(() => {
    return data.filter((item) => {
      const itemMonth = getShortMonth(item.label).toLowerCase();

      return itemMonth !== currentMonthShort.toLowerCase();
    });
  }, [data, currentMonthShort]);

  const firstValue = chartData[0]?.value ?? 0;
  const lastValue = chartData[chartData.length - 1]?.value ?? 0;

  const changePercent =
    firstValue > 0 ? ((lastValue - firstValue) / firstValue) * 100 : 0;

  const categories = useMemo(
    () => chartData.map((item) => getShortMonth(item.label)),
    [chartData]
  );

  const series = useMemo(
    () => [
      {
        name: selectedBucket,
        data: chartData.map((item) => Number(item.value || 0)),
      },
    ],
    [chartData, selectedBucket]
  );

  const options: ApexOptions = useMemo(
    () => ({
      legend: {
        show: false,
        position: "top",
        horizontalAlign: "left",
      },
      colors: [lineColor || "#465FFF"],
      chart: {
        fontFamily: "Outfit, sans-serif",
        height: 310,
        type: "area",
        toolbar: {
          show: false,
        },
        zoom: {
          enabled: false,
        },
      },
      stroke: {
        curve: "smooth",
        width: 2,
      },
      fill: {
        type: "gradient",
        gradient: {
          opacityFrom: 0.35,
          opacityTo: 0,
        },
      },
      markers: {
        size: 0,
        strokeColors: "#fff",
        strokeWidth: 2,
        hover: {
          size: 6,
        },
      },
      grid: {
        borderColor: "#E5E7EB",
        xaxis: {
          lines: {
            show: false,
          },
        },
        yaxis: {
          lines: {
            show: true,
          },
        },
      },
      dataLabels: {
        enabled: false,
      },
      tooltip: {
        enabled: true,
        y: {
          formatter: (value) => `${Number(value).toLocaleString()} units`,
          title: {
            formatter: () => selectedBucket,
          },
        },
      },
      xaxis: {
        type: "category",
        categories,
        axisBorder: {
          show: false,
        },
        axisTicks: {
          show: false,
        },
        tooltip: {
          enabled: false,
        },
        labels: {
          style: {
            fontSize: "12px",
            colors: "#6B7280",
          },
        },
      },
      yaxis: {
        labels: {
          style: {
            fontSize: "12px",
            colors: ["#6B7280"],
          },
          formatter: (value) => {
            return Number(value).toLocaleString();
          },
        },
        title: {
          text: "",
          style: {
            fontSize: "0px",
          },
        },
      },
    }),
    [categories, lineColor, selectedBucket]
  );

  return (
    <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="mb-4 flex flex-col justify-between gap-3 sm:flex-row sm:items-center">
        <PageBreadcrumb
          pageTitle={title}
          variant="page"
          align="left"
          textSize="2xl"
        />

        <div className="flex items-center gap-2 text-sm sm:justify-end">
          <span className="whitespace-nowrap">Ageing Bucket</span>

          <select
            value={selectedBucket}
            onChange={(e) => onBucketChange?.(e.target.value)}
            className="rounded-md border border-slate-300 bg-white px-4 py-1.5 text-sm font-semibold outline-none focus:border-[#5EA68E] focus:ring-2 focus:ring-[#5EA68E]/20"
          >
            {bucketOptions.length > 0 ? (
              bucketOptions.map((bucket) => (
                <option key={bucket.value} value={bucket.value}>
                  {bucket.label}
                </option>
              ))
            ) : (
              <option value={selectedBucket}>{selectedBucket}</option>
            )}
          </select>
        </div>
      </div>

      <div className="max-w-full overflow-x-auto custom-scrollbar">
        <div id="ageingTrendChart" className="min-w-[650px]">
          <ReactApexChart
            options={options}
            series={series}
            type="area"
            height={310}
          />
        </div>
      </div>
    </div>
  );
};

export default AgeingTrendChart;