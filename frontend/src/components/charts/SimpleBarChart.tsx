"use client";

import React, { useRef } from "react";
import "@/lib/chartSetup";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Tooltip,
  Legend,
  Title as ChartTitle,
} from "chart.js";
import { Bar } from "react-chartjs-2";

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend, ChartTitle);

type SimpleBarChartProps = {
  labels: string[];
  values: number[];
  prevValues?: number[];
  colors?: string[];
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
  currentLabel = "MTD",
  prevLabel = "Last month till date",
  xTitle,
  yTitle,
}) => {
  const chartRef = useRef<any>(null);

  const hasPrev = Array.isArray(prevValues) && prevValues.length === labels.length;

  // ✅ color helpers
  const currentColors = colors.length ? colors : "#75BBDA";
  const prevColors = "#D9D9D9"; // grey for last month bars

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
              backgroundColor: prevColors,
              borderRadius: 4,
              borderWidth: 0,
              barPercentage: 0.9,
              categoryPercentage: 0.7,
            },
          ]
        : []),
    ],
  };

  const options: any = {
    responsive: true,
    maintainAspectRatio: false,

    plugins: {
      legend: {
        display: hasPrev, // ✅ show legend only when comparison exists
        position: "top",
      },
      tooltip: {
        callbacks: {
          label: (context: any) => {
            const v = Number(context.raw ?? 0);
            const dsLabel = context.dataset?.label || "";
            // yTitle usually like: "Amount ($)" — so keep that as prefix
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
        stacked: false,
        title: {
          display: Boolean(xTitle),
          text: xTitle,
        },
        ticks: {
          maxRotation: 0,
        },
      },
      y: {
        stacked: false,
        beginAtZero: true,
        title: {
          display: Boolean(yTitle),
          text: yTitle,
        },
      },
    },
  };

  return (
    <div className="relative w-full h-full">
      <Bar ref={chartRef} data={data} options={options} />
    </div>
  );
};

export default SimpleBarChart;







// "use client";

// import React, { useRef, useMemo } from "react";
// import "@/lib/chartSetup";
// import {
//   Chart as ChartJS,
//   CategoryScale,
//   LinearScale,
//   BarElement,
//   Tooltip,
//   Legend,
//   Title as ChartTitle,
// } from "chart.js";
// import { Bar } from "react-chartjs-2";
// import pattern from "patternomaly";

// ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend, ChartTitle);

// type SimpleBarChartProps = {
//   labels: string[];
//   values: number[];
//   prevValues?: number[];
//   colors?: string[]; // you can pass a single color OR array
//   currentLabel?: string;
//   prevLabel?: string;
//   xTitle?: string;
//   yTitle?: string;
// };

// const SimpleBarChart: React.FC<SimpleBarChartProps> = ({
//   labels,
//   values,
//   prevValues = [],
//   colors = [],
//   currentLabel = "MTD",
//   prevLabel = "Last month till date",
//   xTitle,
//   yTitle,
// }) => {
//   const chartRef = useRef<any>(null);

//   const hasPrev = Array.isArray(prevValues) && prevValues.length === labels.length;

//   const baseColors: string[] = useMemo(() => {
//     if (Array.isArray(colors) && colors.length === labels.length) return colors;
//     if (Array.isArray(colors) && colors.length === 1) return Array(labels.length).fill(colors[0]);
//     if (typeof (colors as any) === "string") return Array(labels.length).fill(colors as any);
//     return Array(labels.length).fill("#75BBDA"); // fallback
//   }, [colors, labels.length]);

//   const prevPatternColors = useMemo(() => {
//     // pick any pattern type: 'diagonal', 'zigzag', 'dot', 'dash', 'cross', etc.
//     return baseColors.map((c) => pattern.draw("dash", c));
//   }, [baseColors]);

//   const data = {
//     labels,
//     datasets: [
//       {
//         label: currentLabel,
//         data: values,
//         backgroundColor: baseColors,
//         borderRadius: 4,
//         borderWidth: 0,
//         barPercentage: 0.9,
//         categoryPercentage: 0.7,
//       },
//       ...(hasPrev
//         ? [
//             {
//               label: prevLabel,
//               data: prevValues,
//               backgroundColor: prevPatternColors, // ✅ same color, patterned
//               borderRadius: 4,
//               borderWidth: 0,
//               barPercentage: 0.9,
//               categoryPercentage: 0.7,
//             },
//           ]
//         : []),
//     ],
//   };

//   const options: any = {
//     responsive: true,
//     maintainAspectRatio: false,
//     plugins: {
//       legend: { display: hasPrev, position: "top" },
//       tooltip: {
//         callbacks: {
//           label: (context: any) => {
//             const v = Number(context.raw ?? 0);
//             const dsLabel = context.dataset?.label || "";
//             const prefix = yTitle ? `${dsLabel} - ${yTitle}` : dsLabel || "Value";
//             return `${prefix}: ${v.toLocaleString()}`;
//           },
//         },
//       },
//       title: { display: false },
//     },
//     scales: {
//       x: {
//         stacked: false,
//         title: { display: Boolean(xTitle), text: xTitle },
//         ticks: { maxRotation: 0 },
//       },
//       y: {
//         stacked: false,
//         beginAtZero: true,
//         title: { display: Boolean(yTitle), text: yTitle },
//       },
//     },
//   };

//   return (
//     <div className="relative w-full h-full">
//       <Bar ref={chartRef} data={data} options={options} />
//     </div>
//   );
// };

// export default SimpleBarChart;
