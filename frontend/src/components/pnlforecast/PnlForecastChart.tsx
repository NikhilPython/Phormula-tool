// components/PnlForecastChart.tsx
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

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend);

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

const PnlForecastChart = forwardRef<any, PnlForecastChartProps>(({ chartData, currencySymbol, selectedGraphs, handleCheckboxChange }, ref) => {

  // ✅ NEW: Forecast/Current+Forecast region should start from first NON-historical month (e.g. Dec)
  const forecastStartIndex = chartData.findIndex(d => !d.isHistorical);

  const labels = chartData.map(item => {
    let suffix = '';
    if (item.isForecast) suffix = ' ';
    else if (item.isHistorical) suffix = '';
    else suffix = ' '; // Current & Forecast (as your original logic)
    return `${item.month}${suffix}`;
  });

  type DataKey = keyof ChartDataItem;

  const datasetDefs: { key: DataKey; label: string; borderColor: string; backgroundColor: string }[] = [
    {
      key: 'SALES',
      label: 'Sales',
      borderColor: '#75BBDA',
      backgroundColor: '#75BBDA',
    },
    // {
    //   key: 'COGS',
    //   label: 'COGS',
    //   borderColor: '#AB64B5',
    //   backgroundColor: '#AB64B5',
    // },
    {
      key: 'CM1 PROFIT',
      label: 'CM1 Profit',
      borderColor: '#7B9A6d',
      backgroundColor: '#7B9A6d',
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
    .filter(dataset => selectedGraphs[dataset.key])
    .map(dataset => {
      const values = chartData.map(d => d[dataset.key]);

      return {
        label: dataset.label,
        data: values.map(v => Math.abs(Number(v || 0))),
        borderColor: dataset.borderColor,
        backgroundColor: dataset.backgroundColor,
        borderWidth: 2,
        tension: 0.3,
        fill: false,
        pointRadius: 3,
        segment: {
          // ✅ FIX: use p0DataIndex so Nov→Dec segment stays solid.
          // Dotted line starts visually from Dec→Jan when forecastStartIndex is Dec.
          borderDash: (ctx: any) => {
            if (forecastStartIndex === -1) return undefined;
            return ctx.p0DataIndex >= forecastStartIndex ? [5, 5] : undefined;
          }
        }
      };
    });

  const data = {
    labels,
    datasets
  };

  const options: ChartOptions<'line'> = {
    responsive: true,
  maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
      tooltip: {
        callbacks: {
          label: function (context: any) {
            const value = context.parsed.y;
            return `${context.dataset.label}: ${currencySymbol}${value.toFixed(2)}`;
          },
          title: function (context: any) {
            const index = context[0].dataIndex;
            const label = chartData[index].month;
            const isForecast = chartData[index].isForecast;
            const isHistorical = chartData[index].isHistorical;

            let note = '(Forecast)';
            if (isForecast) note = '(Forecast)';
            else if (isHistorical) note = '';

            return `${label} ${note}`;
          }
        }
      },
      title: {
        display: true,
      }
    },
    scales: {
      x: {
        title: {
          display: true,
          text: 'Months'
        }
      },
      y: {
        title: {
          display: true,
          text: `Amount (${currencySymbol})`
        },
        ticks: {
          callback: function (tickValue: string | number) {
            return typeof tickValue === 'number' ? tickValue.toLocaleString() : tickValue;
          }
        }
      }
    }
  };

  // ✅ FIX: remove old forecastTransitionIndex logic and use forecastStartIndex for background too
  const forecastBackgroundPlugin = {
    id: 'forecastBackground',
    beforeDraw: (chart: any) => {
      const { ctx, chartArea: { top, bottom }, scales: { x } } = chart;

      if (forecastStartIndex === -1) return;

      const startPixel = x.getPixelForTick(forecastStartIndex);

      ctx.save();
      ctx.fillStyle = 'rgba(217, 217, 217, 0.5)';
      ctx.fillRect(startPixel, top, chart.chartArea.right - startPixel, bottom - top);
      ctx.restore();
    }
  };

  return (
   <div className="chart-container" >
       <style>{`
.checkbox-group {
        display: flex;
        flex-wrap: wrap;
        justify-content: space-between;
        align-items: center;
        width: 30%;
        gap: 0.4vh;
      }

 .checkbox-group label {
    font-size: 0.7vw; /* Smaller font */
    font-weight: 600;
      text-decoration-thickness: 1.2px;
    display: flex;
    align-items: center;
    gap: 0.2vw; /* Tighter spacing */
    white-space: nowrap;
  }

  input[type="checkbox"] {
    appearance: none;
    width: 0.7vw; /* Smaller box */
    height: 0.7vw;
    background-color: #ff5c5c;
    position: relative;
    cursor: pointer;
    border-radius: 2px;
  }

  input[type="checkbox"]:checked::before {
    content: '✓';
    font-size: 0.6vw; /* Smaller checkmark */
    font-weight: bold;
    color: white;
    position: absolute;
    left: 0.1vw;
    top: -0.05vw;
  }


.checkbox-label.sales {
  color: #414042;
}
.checkbox-label.sales input[type="checkbox"] {
  background-color: #75BBDA;
}

// .checkbox-label.cogs {
//    color: #414042;
// }


.checkbox-label.ad {
  color: #414042;
}
.checkbox-label.ad input[type="checkbox"] {
  background-color: #C49466;
}

.checkbox-label.cm1 {
  color: #414042;
}
.checkbox-label.cm1 input[type="checkbox"] {
  background-color: #7B9A6d;
}

.checkbox-label.cm2 {
   color: #414042;
}
.checkbox-label.cm2 input[type="checkbox"] {
  background-color: #B8C78C;
}

/* Add this CSS to your stylesheet */

/* Forecast Legend Styles */
.forecast-legend {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 24px;
  padding: 16px 20px;
  // background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
  // border: 1px solid #dee2e6;
  border-radius: 12px;
  // box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
 font-family: 'Lato', sans-serif;
  position: relative;
  overflow: hidden;
}

.forecast-legend::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  width: 4px;
  height: 100%;
  // background: linear-gradient(to bottom, #007bff, #0056b3);
  border-radius: 0 4px 4px 0;
}

.forecast-legend-item {
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 14px;
  font-weight: 500;
  color: #414042;
  transition: all 0.2s ease;
}

.forecast-legend-item:hover {
  color: #414042;
  transform: translateY(-1px);
}

/* Solid line for historical data */
.solid-line {
  width: 40px;
  height: 3px;
  background-color: #414042;
  border-radius: 2px;
}

/* Dotted line for forecast data */
.dotted-line {
  width: 40px;
  height: 3px;
  background-image: repeating-linear-gradient(
    90deg,
    #414042,
    #414042 4px,
    transparent 4px,
    transparent 8px
  );
  border-radius: 2px;
  position: relative;
}

.dotted-line::after {
  content: '';
  position: absolute;
  top: 50%;
  right: -2px;
  transform: translateY(-50%);
  width: 6px;
  height: 6px;
  background: #414042;
  border-radius: 50%;
  box-shadow: 0 0 0 2px white, 0 1px 3px #414042;
}

/* Animations */
// @keyframes dotted-pulse {
//   0%, 100% { opacity: 0.7; }
//   50% { opacity: 1; }
// }

// @keyframes forecast-blink {
//   0%, 100% { transform: translateY(-50%) scale(1); opacity: 0.8; }
//   50% { transform: translateY(-50%) scale(1.1); opacity: 1; }
// }

/* Responsive design for smaller screens */
@media (max-width: 768px) {
  .forecast-legend {
    flex-direction: column;
    align-items: flex-start;
    gap: 16px;
    padding: 16px;
  }
  
  .forecast-legend-item {
    font-size: 13px;
  }
  
  .solid-line,
  .dotted-line {
    width: 32px;
    height: 2px;
  }
}



      `}</style>

<br/>
<div className='flex justify-between items-center mx-5 '>
   <div className="forecast-legend">
        <div className="forecast-legend-item">
          <div className="solid-line"></div>
          <span>Historical & Current Data</span>
        </div>
        <div className="forecast-legend-item">
          <div className="dotted-line"></div>
          <span>Forecast Data</span>
        </div>
      </div>
       <div className="checkbox-group">
        {[
          { name: "SALES", label: "Sales", colorClass: "sales" },
          // { name: "COGS", label: "COGS", colorClass: "cogs" },
          { name: "CM1 PROFIT", label: "CM1 Profit", colorClass: "cm1" },
          { name: "ADVERTISING COSTS", label: "Advertising Costs", colorClass: "ad" },      
          { name: "CM2 PROFIT", label: "CM2 Profit", colorClass: "cm2" },
        ].map(({ name, label, colorClass }, idx) => (
          <label key={idx} className={`checkbox-label ${colorClass}`}>
            <input
              type="checkbox"
              name={name}
              checked={selectedGraphs[name]}
              onChange={handleCheckboxChange}
            />
           {label}
          </label>
        ))}
      </div>
</div>
     

     <div
  style={{
    height: '450px', // 👈 FIXED HEIGHT
    width: '100%',
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
  }}
>
  <Line
    ref={ref}
    data={data}
    options={options}
    plugins={[forecastBackgroundPlugin]}
  />
</div>

    </div>
  );
})

export default PnlForecastChart;
