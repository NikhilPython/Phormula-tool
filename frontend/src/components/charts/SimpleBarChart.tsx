// "use client";

// import React, { useRef, useEffect, useState } from "react";
// import "@/lib/chartSetup";
// import {
//   Chart as ChartJS,
//   CategoryScale,
//   LinearScale,
//   BarElement,
//   Tooltip,
//   Legend,
//   Title as ChartTitle,
//   type ChartOptions,
// } from "chart.js";
// import { Bar } from "react-chartjs-2";

// const hoverPopPlugin = {
//   id: "hoverPopPlugin",
//   afterDatasetsDraw(chart: any) {
//     const active = chart.getActiveElements?.() || [];
//     if (!active.length) return;

//     const { datasetIndex, index } = active[0];
//     const meta = chart.getDatasetMeta(datasetIndex);
//     const bar = meta?.data?.[index];
//     if (!bar) return;

//     const ctx = chart.ctx;

//     const props = bar.getProps(["x", "y", "base", "width"], true);
//     const x = props.x;
//     const y = props.y;
//     const base = props.base;
//     const w = props.width;

//     // pop width
//     const popW = w * 1.18;
//     const left = x - popW / 2;

//     const top = Math.min(y, base);
//     const height = Math.abs(base - y);

//     ctx.save();

//     // pick the correct color for the hovered dataset + bar index
//     const bg = chart.data.datasets?.[datasetIndex]?.backgroundColor;
//     const fill =
//       Array.isArray(bg) ? bg[index] : bg || "rgba(0,0,0,0.2)";

//     ctx.fillStyle = fill as any;
//     ctx.fillRect(left, top, popW, height);

//     // clean outline (no shadow)
//     ctx.lineWidth = 2;
//     ctx.strokeStyle = "rgba(255,255,255,0.6)";
//     ctx.strokeRect(left, top, popW, height);

//     ctx.restore();
//   },
// };

// ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend, ChartTitle, hoverPopPlugin);

// type SimpleBarChartProps = {
//   labels: string[];
//   values: number[];
//   prevValues?: number[];
//   colors?: string[];
//   prevColors?: string[];
//   currentLabel?: string;
//   prevLabel?: string;
//   xTitle?: string;
//   yTitle?: string;
//   showPrev?: boolean;
// };

// const SimpleBarChart: React.FC<SimpleBarChartProps> = ({
//   labels,
//   values,
//   prevValues = [],
//   colors = [],
//   prevColors = [],
//   currentLabel = "MTD",
//   prevLabel = "Last month till date",
//   xTitle,
//   yTitle,
//   showPrev = false,
// }) => {
//   const chartRef = useRef<any>(null);
//   const [isSmallScreen, setIsSmallScreen] = useState(false);

//   useEffect(() => {
//     const mq = window.matchMedia("(max-width: 1280px)") as MediaQueryList & {
//       addListener?: (callback: (e: MediaQueryListEvent | MediaQueryList) => void) => void;
//       removeListener?: (callback: (e: MediaQueryListEvent | MediaQueryList) => void) => void;
//     }; // laptop & below
//     const onChange = (e: MediaQueryListEvent | MediaQueryList) => {
//       setIsSmallScreen(e.matches);
//     };

//     setIsSmallScreen(mq.matches);

//     // Safari fallback support
//     if ("addEventListener" in mq) mq.addEventListener("change", onChange as any);
//     else (mq as MediaQueryList & { addListener: (callback: (e: MediaQueryListEvent | MediaQueryList) => void) => void }).addListener?.(onChange as any);

//     return () => {
//       if ("removeEventListener" in mq) mq.removeEventListener("change", onChange as any);
//       else (mq as MediaQueryList & { removeListener: (callback: (e: MediaQueryListEvent | MediaQueryList) => void) => void }).removeListener?.(onChange as any);
//     };
//   }, []);


//   const hasPrev =
//     showPrev &&
//     Array.isArray(prevValues) &&
//     prevValues.length === labels.length;


//   const currentColors = colors.length ? colors : "#75BBDA";
//   const previousColors = prevColors.length === labels.length ? prevColors : "#D9D9D9";

//   const data = {
//     labels,
//     datasets: [
//       {
//         label: currentLabel,
//         data: values,
//         backgroundColor: currentColors,
//         borderRadius: 4,
//         borderWidth: 0,
//         barPercentage: 0.9,
//         categoryPercentage: 0.7,
//       },
//       ...(hasPrev
//         ? [
//           {
//             label: prevLabel,
//             data: prevValues,
//             backgroundColor: previousColors,
//             borderRadius: 4,
//             borderWidth: 0,
//             barPercentage: 0.9,
//             categoryPercentage: 0.7,
//           },
//         ]
//         : []),
//     ],
//   };

//   const shortLabel = (s: string) => {
//     const map: Record<string, string> = {
//       "Net Sales": "Net Sales",
//       "COGS": "COGS",
//       "Marketplace Fees": "Mkt. Fees",
//       "Tax & Credits": "Tax",
//       "Advertisements": "Ads",
//       "CM1 Profit": "CM1 Profit",
//       "CM2 Profit": "CM2 Profit",
//       "Other Charges": "Other",
//       "Others": "Other",
//     };
//     return map[s] ?? s;
//   };


//   const options: ChartOptions<"bar"> = {
//     interaction: {
//       mode: "nearest",
//       intersect: true,
//     },
//     responsive: true,
//     maintainAspectRatio: false,

//     // extra space so labels don’t get clipped
//     layout: {
//       padding: { bottom: 18 },
//     },

//     plugins: {
//       legend: {
//         display: hasPrev,
//         position: "top",
//       },
//       tooltip: {
//         callbacks: {
//           label: (context) => {
//             const v = Number(context.raw ?? 0);
//             const dsLabel = context.dataset.label || "";
//             const prefix = yTitle ? `${dsLabel} - ${yTitle}` : dsLabel || "Value";
//             return `${prefix}: ${v.toLocaleString()}`;
//           },
//         },
//       },
//       title: {
//         display: false,
//       },
//     },

//     scales: {
//       x: {
//         grid: { display: false, drawOnChartArea: false, drawTicks: false },
//         title: { display: Boolean(xTitle), text: xTitle },

//         ticks: {
//           autoSkip: false,        // IMPORTANT: show all labels
//           maxRotation: 0,
//           minRotation: 0,
//           padding: 8,
//           // callback: (_value, index) => {
//           //   const label = labels[index] ?? "";
//           //   const finalLabel = isSmallScreen ? shortLabel(label) : label;

//           //   // wrap into 2 lines if still long (only if needed)
//           //   if (isSmallScreen && finalLabel.length > 8) {
//           //     const parts = finalLabel.split(" ");
//           //     if (parts.length >= 2) return [parts[0], parts.slice(1).join(" ")];
//           //     return [finalLabel.slice(0, 8), finalLabel.slice(8)];
//           //   }

//           //   return finalLabel;
//           // },
//           // callback: (_value, index) => {
//           //   const label = labels[index] ?? "";

//           //   // ✅ keep small screens unchanged
//           //   if (isSmallScreen) {
//           //     const finalLabel = shortLabel(label);

//           //     // wrap into 2 lines if still long (only if needed)
//           //     if (finalLabel.length > 8) {
//           //       const parts = finalLabel.split(" ");
//           //       if (parts.length >= 2) return [parts[0], parts.slice(1).join(" ")];
//           //       return [finalLabel.slice(0, 8), finalLabel.slice(8)];
//           //     }

//           //     return finalLabel;
//           //   }

//           //   // ✅ large screens: always 2 lines (best-effort split)
//           //   const parts = label.trim().split(/\s+/);

//           //   if (parts.length >= 2) {
//           //     const mid = Math.ceil(parts.length / 2);
//           //     return [parts.slice(0, mid).join(" "), parts.slice(mid).join(" ")];
//           //   }

//           //   // Single long word: split into two halves
//           //   const mid = Math.ceil(label.length / 2);
//           //   return [label.slice(0, mid), label.slice(mid)];
//           // },
//           callback: (_value, index) => {
//             const label = labels[index] ?? "";

//             // ✅ keep small screens unchanged (your existing behavior)
//             if (isSmallScreen) {
//               const finalLabel = shortLabel(label);

//               if (finalLabel.length > 8) {
//                 const parts = finalLabel.split(" ");
//                 if (parts.length >= 2) return [parts[0], parts.slice(1).join(" ")];
//                 return [finalLabel.slice(0, 8), finalLabel.slice(8)];
//               }

//               return finalLabel;
//             }

//             // ✅ large screens: split ONLY when there are 2+ words
//             const parts = label.trim().split(/\s+/).filter(Boolean);

//             if (parts.length >= 2) {
//               const mid = Math.ceil(parts.length / 2);
//               return [parts.slice(0, mid).join(" "), parts.slice(mid).join(" ")];
//             }

//             // single word => do NOT split
//             return label;
//           },

//         },


//       },
//       y: {
//         beginAtZero: true,
//         grid: { display: true, drawOnChartArea: true, drawTicks: false },
//         title: { display: Boolean(yTitle), text: yTitle },
//       },
//     },
//   };

//   return (
//     <div className="relative w-full h-full">
//       <Bar
//         key={isSmallScreen ? "small" : "large"}
//         ref={chartRef}
//         data={data}
//         options={options}
//       />

//     </div>
//   );
// };

// export default SimpleBarChart;

















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

const hoverPopPlugin = {
  id: "hoverPopPlugin",
  afterDatasetsDraw(chart: any) {
    const active = chart.getActiveElements?.() || [];
    if (!active.length) return;

    const { datasetIndex, index } = active[0];
    const meta = chart.getDatasetMeta(datasetIndex);
    const bar = meta?.data?.[index];
    if (!bar) return;

    const ctx = chart.ctx;

    const props = bar.getProps(["x", "y", "base", "width"], true);
    const x = props.x;
    const y = props.y;
    const base = props.base;
    const w = props.width;

    const popW = w * 1.18;
    const left = x - popW / 2;

    const top = Math.min(y, base);
    const height = Math.abs(base - y);

    ctx.save();

    const bg = chart.data.datasets?.[datasetIndex]?.backgroundColor;
    const fill = Array.isArray(bg) ? bg[index] : bg || "rgba(0,0,0,0.2)";

    ctx.fillStyle = fill as any;
    ctx.fillRect(left, top, popW, height);

    ctx.lineWidth = 2;
    ctx.strokeStyle = "rgba(255,255,255,0.6)";
    ctx.strokeRect(left, top, popW, height);

    ctx.restore();
  },
};

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Tooltip,
  Legend,
  ChartTitle,
  hoverPopPlugin
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
  showPrev = false,
}) => {
  const chartRef = useRef<any>(null);
  const [isSmallScreen, setIsSmallScreen] = useState(false);

  useEffect(() => {
    const mq = window.matchMedia("(max-width: 1280px)") as MediaQueryList & {
      addListener?: (
        callback: (e: MediaQueryListEvent | MediaQueryList) => void
      ) => void;
      removeListener?: (
        callback: (e: MediaQueryListEvent | MediaQueryList) => void
      ) => void;
    };

    const onChange = (e: MediaQueryListEvent | MediaQueryList) => {
      setIsSmallScreen(e.matches);
    };

    setIsSmallScreen(mq.matches);

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

  const currentColors = colors.length ? colors : "#75BBDA";
  const previousColors =
    prevColors.length === labels.length ? prevColors : "#D9D9D9";

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

  const options: ChartOptions<"bar"> = {
    interaction: {
      mode: "nearest",
      intersect: true,
    },
    responsive: true,
    maintainAspectRatio: false,

    layout: {
      padding: {
        top: 20,
        bottom: 30,
      },
    },

    plugins: {
      legend: {
        display: hasPrev,
        position: "top",
        labels: {
          padding: 18,
        },
      },
      tooltip: {
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

            if (isSmallScreen) {
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
              return [parts.slice(0, mid).join(" "), parts.slice(mid).join(" ")];
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