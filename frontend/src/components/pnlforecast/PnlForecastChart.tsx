// // components/PnlForecastChart.tsx
// 'use client';

// import React, { forwardRef, useEffect, useState } from 'react';
// import { Line } from 'react-chartjs-2';
// import {
//   Chart as ChartJS,
//   CategoryScale,
//   LinearScale,
//   PointElement,
//   LineElement,
//   Title,
//   Tooltip,
//   Legend,
//   ChartOptions,
// } from 'chart.js';

// ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend);

// interface ChartDataItem {
//   month: string;
//   SALES?: number;
//   COGS?: number;
//   'AMAZON EXPENSE'?: number;
//   'ADVERTISING COSTS'?: number;
//   'CM1 PROFIT'?: number;
//   'CM2 PROFIT'?: number;
//   isForecast?: boolean;
//   isHistorical?: boolean;
// }

// interface SelectedGraphs {
//   [key: string]: boolean;
// }

// interface PnlForecastChartProps {
//   chartData: ChartDataItem[];
//   currencySymbol: string;
//   selectedGraphs: SelectedGraphs;
//   handleCheckboxChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
// }

// const PnlForecastChart = forwardRef<any, PnlForecastChartProps>(({ chartData, currencySymbol, selectedGraphs, handleCheckboxChange }, ref) => {

//   // ✅ NEW: Forecast/Current+Forecast region should start from first NON-historical month (e.g. Dec)
//   const forecastStartIndex = chartData.findIndex(d => !d.isHistorical);

//   // Responsive font sizes (mobile vs desktop)
//   const [isMobile, setIsMobile] = useState(false);
//   const [is2xlUp, setIs2xlUp] = useState(false);

// useEffect(() => {
//   const onResize = () => setIs2xlUp(window.innerWidth >= 1536);
//   onResize();
//   window.addEventListener("resize", onResize);
//   return () => window.removeEventListener("resize", onResize);
// }, []);

// const tickFontSize = is2xlUp ? 16 : 14;

//   useEffect(() => {
//     const handleResize = () => setIsMobile(window.innerWidth < 1536);
//     handleResize();
//     window.addEventListener('resize', handleResize);
//     return () => window.removeEventListener('resize', handleResize);
//   }, []);

//   const axisFontSize = isMobile ? 12 : 16;

//   const shortMonth = (m: string) => {
//   const map: Record<string, string> = {
//     january: "Jan",
//     february: "Feb",
//     march: "Mar",
//     april: "Apr",
//     may: "May",
//     june: "Jun",
//     july: "Jul",
//     august: "Aug",
//     september: "Sep",
//     october: "Oct",
//     november: "Nov",
//     december: "Dec",
//   };

//   const key = (m || "").trim().toLowerCase();
//   return map[key] || (m ? m.slice(0, 3) : "");
// };

// const labels = chartData.map((item) => {
//   let suffix = "";
//   if (item.isForecast) suffix = " ";
//   else if (item.isHistorical) suffix = "";
//   else suffix = " ";
//   return `${shortMonth(item.month)}${suffix}`;
// });

//   type DataKey = keyof ChartDataItem;

//   const datasetDefs: { key: DataKey; label: string; borderColor: string; backgroundColor: string }[] = [
//     {
//       key: 'SALES',
//       label: 'Sales',
//       borderColor: '#75BBDA',
//       backgroundColor: '#75BBDA',
//     },
//     // {
//     //   key: 'COGS',
//     //   label: 'COGS',
//     //   borderColor: '#AB64B5',
//     //   backgroundColor: '#AB64B5',
//     // },
//     {
//       key: 'CM1 PROFIT',
//       label: 'CM1 Profit',
//       borderColor: '#7B9A6d',
//       backgroundColor: '#7B9A6d',
//     },
//     {
//       key: 'ADVERTISING COSTS',
//       label: 'Advertising Costs',
//       borderColor: '#C49466',
//       backgroundColor: '#C49466',
//     },
//     {
//       key: 'CM2 PROFIT',
//       label: 'CM2 Profit',
//       borderColor: '#B8C78C',
//       backgroundColor: '#B8C78C',
//     },
//   ];

//   const datasets = datasetDefs
//     .filter(dataset => selectedGraphs[dataset.key])
//     .map(dataset => {
//       const values = chartData.map(d => d[dataset.key]);

//       return {
//         label: dataset.label,
//         data: values.map(v => Math.abs(Number(v || 0))),
//         borderColor: dataset.borderColor,
//         backgroundColor: dataset.backgroundColor,
//         borderWidth: 2,
//         tension: 0.3,
//         fill: false,
//         pointRadius: 3,
//         segment: {
//           // ✅ FIX: use p0DataIndex so Nov→Dec segment stays solid.
//           // Dotted line starts visually from Dec→Jan when forecastStartIndex is Dec.
//           borderDash: (ctx: any) => {
//             if (forecastStartIndex === -1) return undefined;
//             return ctx.p0DataIndex >= forecastStartIndex ? [5, 5] : undefined;
//           }
//         }
//       };
//     });

//   const data = {
//     labels,
//     datasets
//   };

//   const options: ChartOptions<'line'> = {
//     responsive: true,
//   maintainAspectRatio: false,
//     plugins: {
//       legend: {
//         display: false,
//       },
//       tooltip: {
//         callbacks: {
//           label: function (context: any) {
//             const value = context.parsed.y;
//             return `${context.dataset.label}: ${currencySymbol}${value.toFixed(2)}`;
//           },
//           title: function (context: any) {
//             const index = context[0].dataIndex;
//             const label = chartData[index].month;
//             const isForecast = chartData[index].isForecast;
//             const isHistorical = chartData[index].isHistorical;

//             let note = '(Forecast)';
//             if (isForecast) note = '(Forecast)';
//             else if (isHistorical) note = '';

//             return `${label} ${note}`;
//           }
//         }
//       },
//       title: {
//         display: true,
//       }
//     },
//     scales: {
//   x: {
//     title: {
//       display: false,
//       text: "Months",
//       font: { size: axisFontSize }, // title as-is
//     },
//     ticks: {
//       font: { size: tickFontSize }, // ✅ labels/ticks
//     },
//   },
//   y: {
//     title: {
//       display: true,
//       text: `Amount (${currencySymbol})`,
//       font: { size: axisFontSize }, // title as-is
//     },
//     ticks: {
//       font: { size: tickFontSize }, // ✅ labels/ticks
//       callback: function (tickValue: string | number) {
//         return typeof tickValue === "number"
//           ? tickValue.toLocaleString()
//           : tickValue;
//       },
//     },
//   },
// }
//   };

//   // ✅ FIX: remove old forecastTransitionIndex logic and use forecastStartIndex for background too
//   const forecastBackgroundPlugin = {
//     id: 'forecastBackground',
//     beforeDraw: (chart: any) => {
//       const { ctx, chartArea: { top, bottom }, scales: { x } } = chart;

//       if (forecastStartIndex === -1) return;

//       const startPixel = x.getPixelForTick(forecastStartIndex);

//       ctx.save();
//       ctx.fillStyle = 'rgba(217, 217, 217, 0.5)';
//       ctx.fillRect(startPixel, top, chart.chartArea.right - startPixel, bottom - top);
//       ctx.restore();
//     }
//   };

//   return (
//    <div className="chart-container" >
//       <style>{`
// /* =========================
//    TOPBAR LAYOUT
//    ========================= */
// .topbar {
//   display: flex;
//   justify-content: space-between;
//   align-items: center;
//   gap: 16px;
// }

// /* =========================
//    FORECAST LEGEND
//    ========================= */
// .forecast-legend {
//   display: flex;
//   justify-content: center;
//   align-items: center;
//   gap: 24px;
//   padding: 16px 20px;
//   border-radius: 12px;
//   font-family: 'Lato', sans-serif;
//   position: relative;
//   overflow: hidden;
// }

// .forecast-legend::before {
//   content: '';
//   position: absolute;
//   top: 0;
//   left: 0;
//   width: 4px;
//   height: 100%;
//   border-radius: 0 4px 4px 0;
// }

// .forecast-legend-item {
//   display: flex;
//   align-items: center;
//   gap: 12px;
//   font-weight: 500;
//   color: #414042;

//   /* ✅ below 2xl default */
//   font-size: 14px;
// }

// .forecast-legend-item:hover {
//   color: #414042;
//   transform: translateY(-1px);
// }

// .solid-line {
//   width: 40px;
//   height: 3px;
//   background-color: #414042;
//   border-radius: 2px;
// }

// .dotted-line {
//   width: 40px;
//   height: 3px;
//   background-image: repeating-linear-gradient(
//     90deg,
//     #414042,
//     #414042 4px,
//     transparent 4px,
//     transparent 8px
//   );
//   border-radius: 2px;
//   position: relative;
// }

// .dotted-line::after {
//   content: '';
//   position: absolute;
//   top: 50%;
//   right: -2px;
//   transform: translateY(-50%);
//   width: 6px;
//   height: 6px;
//   background: #414042;
//   border-radius: 50%;
//   box-shadow: 0 0 0 2px white, 0 1px 3px #414042;
// }

// /* =========================
//    CHECKBOX GROUP (NO DUPLICATES)
//    ========================= */
// .checkbox-group {
//   display: flex;
//   flex-wrap: wrap;
//   justify-content: flex-end;
//   align-items: center;
//   width: auto;
//   gap: 10px;
// }

// .checkbox-group label {
//   display: flex;
//   align-items: center;
//   gap: 10px;
//   white-space: nowrap;
//   font-weight: 600;
//   color: #414042;

//   /* ✅ below 2xl default */
//   font-size: 14px;
// }

// /* checkbox base */
// .checkbox-group input[type="checkbox"] {
//   appearance: none;
//   width: 14px;     /* ✅ below 2xl */
//   height: 14px;
//   position: relative;
//   cursor: pointer;
//   border-radius: 3px;
// }

// /* checkmark base */
// .checkbox-group input[type="checkbox"]:checked::before {
//   content: '✓';
//   font-size: 11px; /* ✅ below 2xl */
//   font-weight: 900;
//   color: white;
//   position: absolute;
//   left: 2.5px;
//   top: -2px;
// }

// /* per-series checkbox colors */
// .checkbox-label.sales { color: #414042; }
// .checkbox-label.sales input[type="checkbox"] { background-color: #75BBDA; }

// .checkbox-label.ad { color: #414042; }
// .checkbox-label.ad input[type="checkbox"] { background-color: #C49466; }

// .checkbox-label.cm1 { color: #414042; }
// .checkbox-label.cm1 input[type="checkbox"] { background-color: #7B9A6d; }

// .checkbox-label.cm2 { color: #414042; }
// .checkbox-label.cm2 input[type="checkbox"] { background-color: #B8C78C; }

// /* =========================
//    ✅ 2XL UP (>=1536px)
//    ========================= */
// @media (min-width: 1536px) {
//   .checkbox-group label {
//     font-size: 16px;
//   }

//   .forecast-legend-item {
//     font-size: 16px;
//   }

//   .checkbox-group input[type="checkbox"] {
//     width: 16px;
//     height: 16px;
//   }

//   .checkbox-group input[type="checkbox"]:checked::before {
//     font-size: 12px;
//     left: 3px;
//     top: -2px;
//   }
// }

// /* =========================
//    ✅ MOBILE (<=768px)
//    ========================= */
// @media (max-width: 768px) {
//   .topbar {
//     flex-direction: column;
//     align-items: flex-start;
//   }

//   .forecast-legend {
//     flex-direction: column;
//     align-items: flex-start;
//     gap: 10px;
//     padding: 12px 0;
//   }

//   .forecast-legend-item {
//     font-size: 12px;
//   }

//   .checkbox-group {
//     width: 100%;
//     justify-content: flex-start;
//     gap: 12px;
//   }

//   .checkbox-group label {
//     font-size: 12px;
//     gap: 10px;
//   }

//   .checkbox-group input[type="checkbox"] {
//     width: 18px;
//     height: 18px;
//     border-radius: 4px;
//   }

//   .checkbox-group input[type="checkbox"]:checked::before {
//     font-size: 14px;
//     left: 4px;
//     top: -3px;
//   }

//   .solid-line,
//   .dotted-line {
//     width: 32px;
//     height: 2px;
//   }
// }
// `}</style>

// <br/>
// <div className="topbar ">
//    <div className="forecast-legend">
//         <div className="forecast-legend-item">
//           <div className="solid-line"></div>
//           <span>Historical & Current Data</span>
//         </div>
//         <div className="forecast-legend-item">
//           <div className="dotted-line"></div>
//           <span>Forecast Data</span>
//         </div>
//       </div>
//        <div className="checkbox-group">
//         {[
//           { name: "SALES", label: "Sales", colorClass: "sales" },
//           // { name: "COGS", label: "COGS", colorClass: "cogs" },
//           { name: "CM1 PROFIT", label: "CM1 Profit", colorClass: "cm1" },
//           { name: "ADVERTISING COSTS", label: "Advertising Costs", colorClass: "ad" },      
//           { name: "CM2 PROFIT", label: "CM2 Profit", colorClass: "cm2" },
//         ].map(({ name, label, colorClass }, idx) => (
//           <label key={idx} className={`checkbox-label ${colorClass}`}>
//             <input
//               type="checkbox"
//               name={name}
//               checked={selectedGraphs[name]}
//               onChange={handleCheckboxChange}
//             />
//            {label}
//           </label>
//         ))}
//       </div>
// </div>


//      <div
//   style={{
//     height: '450px', // 👈 FIXED HEIGHT
//     width: '100%',
//     display: 'flex',
//     justifyContent: 'center',
//     alignItems: 'center',
//   }}
// >
//   <Line
//     ref={ref}
//     data={data}
//     options={options}
//     plugins={[forecastBackgroundPlugin]}
//   />
// </div>

//     </div>
//   );
// });

// PnlForecastChart.displayName = 'PnlForecastChart';

// export default PnlForecastChart;











'use client';

import React, { forwardRef } from 'react';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  ChartOptions,
} from 'chart.js';
import "@/lib/chartSetup";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

interface ChartDataItem {
  month: string;
  SALES?: number;
  COGS?: number;
  'AMAZON EXPENSE'?: number;
  'ADVERTISING COSTS'?: number;
  'CM1 PROFIT'?: number;
  'CM2 PROFIT'?: number;
  isForecast?: boolean;
  isHistorical?: boolean;
}

interface SelectedGraphs {
  [key: string]: boolean;
}

interface PnlForecastChartProps {
  chartData: ChartDataItem[];
  currencySymbol: string;
  selectedGraphs: SelectedGraphs;
  handleCheckboxChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
}

const PnlForecastChart = forwardRef<any, PnlForecastChartProps>(
  ({ chartData, currencySymbol, selectedGraphs, handleCheckboxChange }, ref) => {
    const forecastStartIndex = chartData.findIndex((d) => !d.isHistorical);

    const shortMonth = (m: string) => {
      const raw = (m || '').trim();

      const firstWord = raw.split(' ')[0];
      const yearPart = raw.split(' ')[1];

      const monthMap: Record<string, string> = {
        january: 'Jan',
        february: 'Feb',
        march: 'Mar',
        april: 'Apr',
        may: 'May',
        june: 'Jun',
        july: 'Jul',
        august: 'Aug',
        september: 'Sep',
        october: 'Oct',
        november: 'Nov',
        december: 'Dec',
      };

      const monthShort = monthMap[firstWord.toLowerCase()] || firstWord.slice(0, 3);

      if (yearPart) {
        return `${monthShort}`;
      }

      return monthShort;
    };

    const formatMonthYear = (m: string) => {
      if (!m) return "";

      const parts = m.split(" ");
      const month = parts[0];
      const year = parts[1];

      const monthMap: Record<string, string> = {
        January: "Jan",
        February: "Feb",
        March: "Mar",
        April: "Apr",
        May: "May",
        June: "Jun",
        July: "Jul",
        August: "Aug",
        September: "Sep",
        October: "Oct",
        November: "Nov",
        December: "Dec",
      };

      const shortMonth = monthMap[month] || month.slice(0, 3);

      return `${shortMonth} '${year?.slice(-2)}`;
    };

    const labels = chartData.map((item) => formatMonthYear(item.month));

    type DataKey = keyof ChartDataItem;

    const datasetDefs: {
      key: DataKey;
      label: string;
      borderColor: string;
      backgroundColor: string;
    }[] = [
        {
          key: 'SALES',
          label: 'Sales',
          borderColor: '#75BBDA',
          backgroundColor: '#75BBDA',
        },
        {
          key: 'CM1 PROFIT',
          label: 'CM1 Profit',
          borderColor: '#7B9A6D',
          backgroundColor: '#7B9A6D',
        },
        {
          key: 'ADVERTISING COSTS',
          label: 'Advertising Costs',
          borderColor: '#C49466',
          backgroundColor: '#C49466',
        },
        {
          key: 'CM2 PROFIT',
          label: 'CM2 Profit',
          borderColor: '#B8C78C',
          backgroundColor: '#B8C78C',
        },
      ];

    const datasets = datasetDefs
      .filter((dataset) => selectedGraphs[dataset.key])
      .map((dataset) => ({
        label: dataset.label,
        data: chartData.map((d) => Math.abs(Number(d[dataset.key] || 0))),
        borderColor: dataset.borderColor,
        backgroundColor: dataset.backgroundColor,
        borderWidth: 2,
        tension: 0.3,
        fill: false,
        pointRadius: 4,
        pointHoverRadius: 5,
        segment: {
          borderDash: (ctx: any) => {
            if (forecastStartIndex === -1) return undefined;
            return ctx.p0DataIndex >= forecastStartIndex ? [6, 6] : undefined;
          },
        },
      }));

    const data = {
      labels,
      datasets,
    };

    const options: ChartOptions<'line'> = {
      responsive: true,
      maintainAspectRatio: false,
      layout: {
        padding: {
          top: 10,
          bottom: 10,
        },
      },
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          callbacks: {
            label: function (context: any) {
              const value = Math.round(Number(context.parsed.y ?? 0));

              return `${context.dataset.label}: ${currencySymbol}${value.toLocaleString(undefined, {
                maximumFractionDigits: 0,
              })}`;
            },
            title: function (context: any) {
              const index = context[0].dataIndex;
              const label = chartData[index].month;
              const isForecast = chartData[index].isForecast;
              return isForecast ? `${label} (Forecast)` : label;
            },
          },
        },
      },
      scales: {
        x: {
          title: {
            display: false,
          },
          ticks: {
            font: {
              size: 12,
            },
          },
          border: {
            display: false,
          },
        },
        y: {
          beginAtZero: true,
          title: {
            display: true,
            text: `Amount (${currencySymbol})`,
            font: {
              size: 12,
            },
          },
          ticks: {
            font: {
              size: 12,
            },
            callback: function (tickValue: string | number) {
              const value = Math.round(Number(tickValue || 0));

              return value.toLocaleString(undefined, {
                maximumFractionDigits: 0,
              });
            },
          },
          border: {
            display: false,
          },
        },
      },
    };

    const forecastBackgroundPlugin = {
      id: 'forecastBackground',
      beforeDraw: (chart: any) => {
        const { ctx, chartArea, scales } = chart;
        const x = scales?.x;

        if (!x || forecastStartIndex === -1) return;

        const startPixel = x.getPixelForTick(forecastStartIndex);

        ctx.save();
        ctx.fillStyle = 'rgba(217, 217, 217, 0.35)';
        ctx.fillRect(
          startPixel,
          chartArea.top,
          chartArea.right - startPixel,
          chartArea.bottom - chartArea.top
        );
        ctx.restore();
      },
    };

    const metricOptions = [
      { name: 'SALES', label: 'Sales', color: '#75BBDA' },
      { name: 'CM1 PROFIT', label: 'CM1 Profit', color: '#7B9A6D' },
      { name: 'ADVERTISING COSTS', label: 'Advertising Costs', color: '#C49466' },
      { name: 'CM2 PROFIT', label: 'CM2 Profit', color: '#B8C78C' },
    ];

    return (
      <div className="flex flex-col gap-4">
        <div className="flex flex-wrap items-center justify-center gap-4 w-full transition-opacity duration-300">
          {metricOptions.map(({ name, label, color }) => {
            const isChecked = !!selectedGraphs[name];

            return (
              <label
                key={name}
                className="shrink-0 flex items-center gap-1.5 font-semibold select-none whitespace-nowrap text-[10px] 2xl:text-xs my-1 2xl:my-3 text-charcoal-500 cursor-pointer"
              >
                <span
                  className="flex items-center justify-center h-3 w-3 sm:h-3.5 sm:w-3.5 rounded-sm border transition"
                  style={{
                    borderColor: color,
                    backgroundColor: isChecked ? color : 'white',
                  }}
                >
                  <input
                    type="checkbox"
                    name={name}
                    checked={isChecked}
                    onChange={handleCheckboxChange}
                    className="sr-only"
                  />
                  {isChecked && (
                    <svg viewBox="0 0 24 24" width="14" height="14" className="text-white">
                      <path
                        fill="currentColor"
                        d="M20.285 6.709a1 1 0 0 0-1.414-1.414L9 15.168l-3.879-3.88a1 1 0 0 0-1.414 1.415l4.586 4.586a1 1 0 0 0 1.414 0l10-10Z"
                      />
                    </svg>
                  )}
                </span>

                <span>{label}</span>
              </label>
            );
          })}
        </div>

        <div className="w-full h-[550px]">
          <Line
            ref={ref}
            data={data}
            options={options}
            plugins={[forecastBackgroundPlugin]}
          />
        </div>

        <div className="flex justify-center mt-2">
          <div className="flex flex-wrap justify-center items-center gap-6 text-xs">
            <div className="flex items-center gap-2 text-charcoal-500">
              <span className="inline-block w-8 border-b-2 border-charcoal-500" />
              <span className="whitespace-nowrap">Historical &amp; Current Data</span>
            </div>
            <div className="flex items-center gap-2 text-charcoal-500">
              <span className="inline-block w-8 border-b-2 border-charcoal-500 border-dashed" />
              <span className="whitespace-nowrap">Forecast Data</span>
            </div>
          </div>
        </div>
      </div>
    );
  }
);

PnlForecastChart.displayName = 'PnlForecastChart';

export default PnlForecastChart;