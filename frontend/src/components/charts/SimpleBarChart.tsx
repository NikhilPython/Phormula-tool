// "use client";

// import React, { useRef } from "react";
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

// ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend, ChartTitle);

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
// };


// const SimpleBarChart: React.FC<SimpleBarChartProps> = ({
//   labels,
//   values,
//   prevValues = [],
//   colors = [],
//   prevColors = [], // ✅ add this
//   currentLabel = "MTD",
//   prevLabel = "Last month till date",
//   xTitle,
//   yTitle,
// }) => {
//   const chartRef = useRef<any>(null);


//   const hasPrev = Array.isArray(prevValues) && prevValues.length === labels.length;
//   const currentColors = colors.length ? colors : "#75BBDA";


//   // ✅ per-category prev colors
//   const previousColors =
//     prevColors.length === labels.length ? prevColors : "#D9D9D9";


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

//   const options: any = {
//     responsive: true,
//     maintainAspectRatio: false,

//     plugins: {
//       legend: {
//         display: hasPrev, // ✅ show legend only when comparison exists
//         position: "top",
//       },
//       tooltip: {
//         callbacks: {
//           label: (context: any) => {
//             const v = Number(context.raw ?? 0);
//             const dsLabel = context.dataset?.label || "";
//             // yTitle usually like: "Amount ($)" — so keep that as prefix
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
//         stacked: false,
//         grid: {
//           display: false, 
//           drawBorder: false,
//         },
//         title: {
//           display: Boolean(xTitle),
//           text: xTitle,
//         },
//         ticks: {
//           maxRotation: 0,
//         },
//       },
//       y: {
//         stacked: false,
//         beginAtZero: true,
//         grid: {
//           display: true, // ✅ remove horizontal grid lines
//           drawBorder: false,
//         },
//         title: {
//           display: Boolean(yTitle),
//           text: yTitle,
//         },
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
          autoSkip: false, // ✅ show all labels
          maxRotation: 45,
          minRotation: 0,
          padding: 8,

          // ✅ TS-safe: no `this`, no getLabelForValue
          callback: (_value, index): string | string[] => {
            const text = labels[index] ?? "";
            // Wrap onto multiple lines (optional)
            return text.split(" ");
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
      <Bar ref={chartRef} data={data} options={options} />
    </div>
  );
};

export default SimpleBarChart;
