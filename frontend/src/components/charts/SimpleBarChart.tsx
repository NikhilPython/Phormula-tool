"use client";

import React, { useRef, useEffect, useState } from "react";
import "@/lib/chartSetup";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Tooltip,
  Legend,
  Title as ChartTitle,
  type ChartOptions,
} from "chart.js";
import { Bar } from "react-chartjs-2";

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend, ChartTitle);

type SimpleBarChartProps = {
  labels: string[];
  values: number[];
  prevValues?: number[];
  colors?: string[];
  prevColors?: string[];
  currentLabel?: string;
  prevLabel?: string;
  xTitle?: string;
  yTitle?: string;
};

const SimpleBarChart: React.FC<SimpleBarChartProps> = ({
  labels,
  values,
  prevValues = [],
  colors = [],
  prevColors = [],
  currentLabel = "MTD",
  prevLabel = "Last month till date",
  xTitle,
  yTitle,
}) => {
  const chartRef = useRef<any>(null);
  const [isSmallScreen, setIsSmallScreen] = useState(false);

  useEffect(() => {
    const mq = window.matchMedia("(max-width: 1280px)") as MediaQueryList & {
      addListener?: (callback: (e: MediaQueryListEvent | MediaQueryList) => void) => void;
      removeListener?: (callback: (e: MediaQueryListEvent | MediaQueryList) => void) => void;
    }; // laptop & below
    const onChange = (e: MediaQueryListEvent | MediaQueryList) => {
      setIsSmallScreen(e.matches);
    };

    setIsSmallScreen(mq.matches);

    // Safari fallback support
    if ("addEventListener" in mq) mq.addEventListener("change", onChange as any);
    else (mq as MediaQueryList & { addListener: (callback: (e: MediaQueryListEvent | MediaQueryList) => void) => void }).addListener?.(onChange as any);

    return () => {
      if ("removeEventListener" in mq) mq.removeEventListener("change", onChange as any);
      else (mq as MediaQueryList & { removeListener: (callback: (e: MediaQueryListEvent | MediaQueryList) => void) => void }).removeListener?.(onChange as any);
    };
  }, []);


  const hasPrev = Array.isArray(prevValues) && prevValues.length === labels.length;

  const currentColors = colors.length ? colors : "#75BBDA";
  const previousColors = prevColors.length === labels.length ? prevColors : "#D9D9D9";

  const data = {
    labels,
    datasets: [
      {
        label: currentLabel,
        data: values,
        backgroundColor: currentColors,
        borderRadius: 4,
        borderWidth: 0,
        barPercentage: 0.9,
        categoryPercentage: 0.7,
      },
      ...(hasPrev
        ? [
          {
            label: prevLabel,
            data: prevValues,
            backgroundColor: previousColors,
            borderRadius: 4,
            borderWidth: 0,
            barPercentage: 0.9,
            categoryPercentage: 0.7,
          },
        ]
        : []),
    ],
  };

  const shortLabel = (s: string) => {
    const map: Record<string, string> = {
      "Net Sales": "Net Sales",
      "COGS": "COGS",
      "Marketplace Fees": "Mkt. Fees",
      "Tax & Credits": "Taxes",
      "Advertisements": "Ads",
      "CM1 Profit": "CM1 Profit",
      "CM2 Profit": "CM2 Profit",
      "Other Charges": "Others",
      "Others": "Others",
    };
    return map[s] ?? s;
  };


  const options: ChartOptions<"bar"> = {
    responsive: true,
    maintainAspectRatio: false,

    // extra space so labels don’t get clipped
    layout: {
      padding: { bottom: 18 },
    },

    plugins: {
      legend: {
        display: hasPrev,
        position: "top",
      },
      tooltip: {
        callbacks: {
          label: (context) => {
            const v = Number(context.raw ?? 0);
            const dsLabel = context.dataset.label || "";
            const prefix = yTitle ? `${dsLabel} - ${yTitle}` : dsLabel || "Value";
            return `${prefix}: ${v.toLocaleString()}`;
          },
        },
      },
      title: {
        display: false,
      },
    },

    scales: {
      x: {
        grid: { display: false, drawOnChartArea: false, drawTicks: false },
        title: { display: Boolean(xTitle), text: xTitle },

      ticks: {
  autoSkip: false,        // IMPORTANT: show all labels
  maxRotation: 0,
  minRotation: 0,
  padding: 8,
  callback: (_value, index) => {
    const label = labels[index] ?? "";
    const finalLabel = isSmallScreen ? shortLabel(label) : label;

    // wrap into 2 lines if still long (only if needed)
    if (isSmallScreen && finalLabel.length > 8) {
      const parts = finalLabel.split(" ");
      if (parts.length >= 2) return [parts[0], parts.slice(1).join(" ")];
      return [finalLabel.slice(0, 8), finalLabel.slice(8)];
    }

    return finalLabel;
  },
},


      },
      y: {
        beginAtZero: true,
        grid: { display: true, drawOnChartArea: true, drawTicks: false },
        title: { display: Boolean(yTitle), text: yTitle },
      },
    },
  };

  return (
    <div className="relative w-full h-full">
      <Bar
        key={isSmallScreen ? "small" : "large"}
        ref={chartRef}
        data={data}
        options={options}
      />

    </div>
  );
};

export default SimpleBarChart;
