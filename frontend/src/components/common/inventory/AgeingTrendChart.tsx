import React from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

export type AgeingTrendItem = {
  label: string;
  value: number;
};

type AgeingTrendChartProps = {
  title?: string;
  subtitle?: string;
  selectedBucket: string;
  data: AgeingTrendItem[];
  lineColor: string;
  showChange?: boolean;
};

const AgeingTrendChart: React.FC<AgeingTrendChartProps> = ({
  title = "Ageing Trend Over Time",
  subtitle = "Track how old inventory is increasing or decreasing",
  selectedBucket,
  data,
  lineColor,
  showChange = true,
}) => {
  const firstValue = data[0]?.value ?? 0;
  const lastValue = data[data.length - 1]?.value ?? 0;

  const changePercent =
    firstValue > 0 ? ((lastValue - firstValue) / firstValue) * 100 : 0;

  const gradientId = `ageingTrendFill-${selectedBucket.replace(/\W/g, "")}`;

  return (
    <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="mb-4 flex flex-col justify-between gap-3 sm:flex-row sm:items-start">
        <div>
          <h3 className="text-lg font-extrabold uppercase text-slate-900">
            {title}
          </h3>
          <p className="mt-1 text-sm text-slate-900">{subtitle}</p>
        </div>

        {showChange && (
          <div className="rounded-lg border border-slate-200 bg-white px-4 py-2 text-center">
            <span className="block text-xs text-slate-700">Change</span>
            <strong
              className={`text-lg ${
                changePercent >= 0 ? "text-red-600" : "text-green-600"
              }`}
            >
              {changePercent >= 0 ? "+" : ""}
              {changePercent.toFixed(1)}%
            </strong>
          </div>
        )}
      </div>

      <div className="mb-3 flex items-center gap-2 text-sm">
        <span>Ageing Bucket</span>
        <strong className="rounded-md border border-slate-300 bg-white px-4 py-1.5">
          {selectedBucket}
        </strong>
      </div>

      <ResponsiveContainer width="100%" height={280}>
        <AreaChart data={data}>
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={lineColor} stopOpacity={0.25} />
              <stop offset="95%" stopColor={lineColor} stopOpacity={0} />
            </linearGradient>
          </defs>

          <CartesianGrid strokeDasharray="3 3" vertical={false} />

          <XAxis dataKey="label" />
          <YAxis />

          <Tooltip formatter={(value) => [`${value} units`, selectedBucket]} />

          <Area
            type="monotone"
            dataKey="value"
            stroke={lineColor}
            fill={`url(#${gradientId})`}
            strokeWidth={2}
          />

          <Line
            type="monotone"
            dataKey="value"
            stroke={lineColor}
            strokeWidth={2}
            dot={{ r: 4 }}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
};

export default AgeingTrendChart;