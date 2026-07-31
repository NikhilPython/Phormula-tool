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
  type Chart,
  type ChartOptions,
  type TooltipModel,
} from "chart.js";
import { Bar } from "react-chartjs-2";

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Tooltip,
  Legend,
  ChartTitle,
);

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
  showPrev?: boolean;
  showDeltaInTooltip?: boolean;
};

const LONG_LABELS_QUERY = "(min-width: 1700px)";
const LOWER_IS_BETTER_DELTA_LABELS = new Set([
  "marketplace fees",
  "advertisements",
]);

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
  showPrev = false,
  showDeltaInTooltip = false,
}) => {
  const chartRef = useRef<Chart<"bar"> | null>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const tooltipKeyRef = useRef("");
  const [showLongLabels, setShowLongLabels] = useState(false);

  useEffect(() => {
    const mq = window.matchMedia(LONG_LABELS_QUERY) as MediaQueryList & {
      addListener?: (
        callback: (e: MediaQueryListEvent | MediaQueryList) => void
      ) => void;
      removeListener?: (
        callback: (e: MediaQueryListEvent | MediaQueryList) => void
      ) => void;
    };

    const onChange = (e: MediaQueryListEvent | MediaQueryList) => {
      setShowLongLabels(e.matches);
    };

    setShowLongLabels(mq.matches);

    if ("addEventListener" in mq) {
      mq.addEventListener("change", onChange as any);
    } else {
      (
        mq as MediaQueryList & {
          addListener: (
            callback: (e: MediaQueryListEvent | MediaQueryList) => void
          ) => void;
        }
      ).addListener?.(onChange as any);
    }

    return () => {
      if ("removeEventListener" in mq) {
        mq.removeEventListener("change", onChange as any);
      } else {
        (
          mq as MediaQueryList & {
            removeListener: (
              callback: (e: MediaQueryListEvent | MediaQueryList) => void
            ) => void;
          }
        ).removeListener?.(onChange as any);
      }
    };
  }, []);

  const hasPrev =
    showPrev &&
    Array.isArray(prevValues) &&
    prevValues.length === labels.length;
  const hasTooltipDelta =
    showDeltaInTooltip &&
    Array.isArray(prevValues) &&
    prevValues.length === labels.length;
  const currentDatasetIndex = hasPrev ? 1 : 0;

  const currentColors = colors.length ? colors : "#75BBDA";
  const previousColors =
    prevColors.length === labels.length ? prevColors : "#D9D9D9";

 const data = {
  labels,
  datasets: [
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
    {
      label: currentLabel,
      data: values,
      backgroundColor: currentColors,
      borderRadius: 4,
      borderWidth: 0,
      barPercentage: 0.9,
      categoryPercentage: 0.7,
    },
  ],
};

  const shortLabel = (s: string) => {
    const map: Record<string, string> = {
      "Net Sales": "Net Sales",
      COGS: "COGS",
      "Marketplace Fees": "Mkt. Fees",
      "Tax & Credits": "Tax",
      Advertisements: "Ads",
      "CM1 Profit": "CM1 Profit",
      "CM2 Profit": "CM2 Profit",
      "Other Charges": "Other",
      Others: "Other",
    };
    return map[s] ?? s;
  };

  const formatNumber = (value: unknown) => {
    return Number(value ?? 0).toLocaleString();
  };

  const getDeltaPercent = (current: number, previous: number) => {
    if (!previous || !Number.isFinite(previous)) return null;

    return ((current - previous) / previous) * 100;
  };

  const formatDeltaPercent = (deltaPercent: number) => {
    const symbol = deltaPercent >= 0 ? "▲" : "▼";

    return `${symbol} ${Math.abs(deltaPercent).toFixed(2)}%`;
  };

  const getDeltaColor = (deltaPercent: number, label: string) => {
    const lowerIsBetter = LOWER_IS_BETTER_DELTA_LABELS.has(
      label.trim().toLowerCase()
    );

    if (lowerIsBetter) {
      return deltaPercent > 0 ? "#FF5C5C" : "#5EA68E";
    }

    return deltaPercent >= 0 ? "#5EA68E" : "#FF5C5C";
  };

  const updateExternalTooltip = (context: {
    chart: Chart<"bar">;
    tooltip: TooltipModel<"bar">;
  }) => {
    const tooltipEl = tooltipRef.current;
    const { chart, tooltip } = context;

    if (!tooltipEl) return;

    if (!tooltip || tooltip.opacity === 0) {
      tooltipEl.style.opacity = "0";
      return;
    }

    const tooltipKey = [
      tooltip.title?.[0] ?? "",
      ...(tooltip.dataPoints?.map(
        (point) => `${point.datasetIndex}:${point.dataIndex}:${point.raw}`
      ) ?? []),
    ].join("|");

    if (tooltipKeyRef.current !== tooltipKey) {
      tooltipKeyRef.current = tooltipKey;
      tooltipEl.replaceChildren();

      const title = tooltip.title?.[0];
      if (title) {
        const titleEl = document.createElement("div");
        titleEl.textContent = title;
        titleEl.style.fontWeight = "600";
        titleEl.style.marginBottom = "4px";
        tooltipEl.appendChild(titleEl);
      }

      const rowsEl = document.createElement("div");
      rowsEl.style.display = "grid";
      rowsEl.style.gap = "2px";

      tooltip.dataPoints?.forEach((point, pointIndex) => {
        const rowEl = document.createElement("div");
        rowEl.style.display = "flex";
        rowEl.style.alignItems = "center";
        rowEl.style.whiteSpace = "nowrap";

        const labelColor = tooltip.labelColors?.[pointIndex];
        const swatchEl = document.createElement("span");
        swatchEl.style.width = "10px";
        swatchEl.style.height = "10px";
        swatchEl.style.marginRight = "4px";
        swatchEl.style.borderRadius = "1px";
        swatchEl.style.borderWidth = "2px";
        swatchEl.style.borderStyle = "solid";
        swatchEl.style.borderColor =
          String(labelColor?.borderColor || labelColor?.backgroundColor || "#75BBDA");
        swatchEl.style.backgroundColor =
          String(labelColor?.backgroundColor || "#75BBDA");

        const textEl = document.createElement("span");
        const datasetLabel = point.dataset?.label || "";
        const prefix = yTitle
          ? `${datasetLabel} - ${yTitle}`
          : datasetLabel || "Value";
        textEl.textContent = `${prefix}: ${formatNumber(point.raw)}`;

        rowEl.appendChild(swatchEl);
        rowEl.appendChild(textEl);

        if (hasTooltipDelta && point.datasetIndex === currentDatasetIndex) {
          const currentValue = Number(point.raw ?? 0);
          const previousValue = Number(prevValues?.[point.dataIndex] ?? 0);
          const deltaPercent = getDeltaPercent(currentValue, previousValue);
          const label = labels[point.dataIndex] ?? "";

          if (deltaPercent === null) {
            rowsEl.appendChild(rowEl);
            return;
          }

          const deltaEl = document.createElement("span");
          deltaEl.textContent = ` ${formatDeltaPercent(deltaPercent)}`;
          deltaEl.style.color = getDeltaColor(deltaPercent, label);
          deltaEl.style.fontWeight = "600";
          deltaEl.style.marginLeft = "4px";
          rowEl.appendChild(deltaEl);
        }

        rowsEl.appendChild(rowEl);
      });

      tooltipEl.appendChild(rowsEl);
    }

    tooltipEl.style.opacity = "1";

    const tooltipWidth = tooltipEl.offsetWidth;
    const tooltipHeight = tooltipEl.offsetHeight;
    const horizontalPadding = 4;
    const left = Math.min(
      Math.max(tooltip.caretX, tooltipWidth / 2 + horizontalPadding),
      chart.width - tooltipWidth / 2 - horizontalPadding
    );
    const topAbove = tooltip.caretY - tooltipHeight - 8;
    const top =
      topAbove > horizontalPadding
        ? topAbove
        : Math.min(
            tooltip.caretY + 8,
            chart.height - tooltipHeight - horizontalPadding
          );

    tooltipEl.style.transform = `translate3d(${left}px, ${Math.max(
      top,
      horizontalPadding
    )}px, 0) translateX(-50%)`;
  };

  const options: ChartOptions<"bar"> = {
    interaction: {
      mode: "index",
      intersect: false,
      axis: "x",
    },

    hover: {
      mode: "index",
      intersect: false,
    },

    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },

    layout: {
      padding: {
        top: 5,
        bottom: 10,
      },
    },

   plugins: {
  legend: {
  display: false,
},
  tooltip: {
    enabled: !hasTooltipDelta,
    mode: "index",
    intersect: false,
    external: hasTooltipDelta ? updateExternalTooltip : undefined,
    callbacks: {
      label: (context) => {
        const v = Number(context.raw ?? 0);
        const dsLabel = context.dataset.label || "";
        const prefix = yTitle
          ? `${dsLabel} - ${yTitle}`
          : dsLabel || "Value";

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
        grid: {
          display: false,
          drawOnChartArea: false,
          drawTicks: false,
        },
        title: {
          display: Boolean(xTitle),
          text: xTitle,
        },
        ticks: {
          autoSkip: false,
          maxRotation: 0,
          minRotation: 0,
          padding: 16,
          callback: (_value, index) => {
            const label = labels[index] ?? "";

            if (!showLongLabels) {
              const finalLabel = shortLabel(label);

              if (finalLabel.length > 8) {
                const parts = finalLabel.split(" ");
                if (parts.length >= 2) {
                  return [parts[0], parts.slice(1).join(" ")];
                }
                return [finalLabel.slice(0, 8), finalLabel.slice(8)];
              }

              return finalLabel;
            }

            const parts = label.trim().split(/\s+/).filter(Boolean);

            if (parts.length >= 2) {
              const mid = Math.ceil(parts.length / 2);
              return [
                parts.slice(0, mid).join(" "),
                parts.slice(mid).join(" "),
              ];
            }

            return label;
          },
        },
      },
      y: {
        beginAtZero: true,
        grid: {
          display: true,
          drawOnChartArea: true,
          drawTicks: false,
        },
        title: {
          display: Boolean(yTitle),
          text: yTitle,
        },
        ticks: {
          callback: (value) => {
            return Math.round(Number(value)).toLocaleString();
          },
        },
      },
    },
  };

  return (
    <div className="relative w-full h-full">
      <Bar
        key={showLongLabels ? "long-labels" : "short-labels"}
        ref={chartRef}
        data={data}
        options={options}
      />
      <div
        ref={tooltipRef}
        className="pointer-events-none absolute z-20 rounded-md border border-gray-200 bg-white px-2 py-1 text-xs text-[#414042] opacity-0"
        style={{
          boxShadow: "0 1px 2px rgba(15, 23, 42, 0.06)",
          left: 0,
          top: 0,
          transition: "transform 140ms ease-out, opacity 80ms ease",
          willChange: "transform, opacity",
        }}
      />
    </div>
  );
};

export default SimpleBarChart;
