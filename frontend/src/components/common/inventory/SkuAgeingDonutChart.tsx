"use client";

import * as React from "react";
import { Label, Pie, PieChart } from "recharts";

import {
    ChartContainer,
    ChartTooltip,
    ChartTooltipContent,
    type ChartConfig,
} from "@/components/ui/chart";
import PageBreadcrumb from "../PageBreadCrumb";

export type DonutChartItem = {
    bucket: string;
    units: number;
    color: string;
};

type SkuAgeingDonutChartProps = {
    title?: string;
    subtitle?: string;
    sku?: string;
    data: DonutChartItem[];
    totalUnits?: number;
};

const SkuAgeingDonutChart: React.FC<SkuAgeingDonutChartProps> = ({
    title = "Ageing Donut Chart",
    subtitle = "",
    data,
    totalUnits,
}) => {
    const calculatedTotal = React.useMemo(() => {
        return data.reduce((sum, item) => sum + item.units, 0);
    }, [data]);

    const finalTotal = totalUnits ?? calculatedTotal;

    const chartData = React.useMemo(() => {
        return data.map((item, index) => ({
            ...item,
            fill: item.color,
            chartKey: `bucket_${index}`,
            percentage: finalTotal ? (item.units / finalTotal) * 100 : 0,
        }));
    }, [data, finalTotal]);

    const chartConfig = React.useMemo(() => {
        const config: ChartConfig = {
            units: {
                label: "Units",
            },
        };

        chartData.forEach((item) => {
            config[item.chartKey] = {
                label: item.bucket,
                color: item.color,
            };
        });

        return config;
    }, [chartData]);

    return (
       <div className="flex h-full w-full min-w-0 flex-col rounded-xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5 min-[1700px]:min-h-[360px] min-[1700px]:p-4">
            <PageBreadcrumb
                pageTitle={title}
                variant="page"
                align="left"
                textSize="2xl"
            />

            <div className="mt-4 flex flex-1 flex-col gap-5 min-[1700px]:flex-row min-[1700px]:items-center min-[1700px]:gap-4">
                <div className="flex min-w-0 justify-center min-[1700px]:w-[42%] min-[1700px]:shrink-0">
                    <ChartContainer
                        config={chartConfig}
                       className="mx-auto aspect-square h-[230px] max-h-[230px] w-full max-w-[230px] sm:h-[270px] sm:max-h-[270px] sm:max-w-[270px] xl:h-[300px] xl:max-h-[300px] xl:max-w-[300px] min-[1700px]:h-[220px] min-[1700px]:max-h-[220px] min-[1700px]:max-w-[220px]"
                    >
                        <PieChart>
                            <ChartTooltip
                                cursor={false}
                                content={
                                    <ChartTooltipContent
                                        hideLabel
                                        formatter={(value, _name, item) => {
                                            const payload = item.payload as DonutChartItem & {
                                                percentage: number;
                                            };

                                            const displayValue = Array.isArray(value)
                                                ? value[0]
                                                : value;

                                            return (
                                                <div className="flex min-w-[185px] items-center justify-between gap-4 text-xs">
                                                    <div className="flex items-center gap-2">
                                                        <span
                                                            className="h-2.5 w-2.5 rounded-full"
                                                            style={{ backgroundColor: payload.color }}
                                                        />
                                                        <span className="font-medium text-slate-800">
                                                            {payload.bucket}
                                                        </span>
                                                    </div>

                                                    <span className="font-semibold text-slate-900">
                                                        {Number(displayValue ?? 0).toLocaleString()} (
                                                        {payload.percentage.toFixed(1)}%)
                                                    </span>
                                                </div>
                                            );
                                        }}
                                    />
                                }
                            />

                            <Pie
                                data={chartData}
                                dataKey="units"
                                nameKey="bucket"
                                innerRadius="58%"
                                outerRadius="82%"
                                paddingAngle={1.5}
                                strokeWidth={4}
                                isAnimationActive={false}
                            >
                                <Label
                                    content={({ viewBox }) => {
                                        if (viewBox && "cx" in viewBox && "cy" in viewBox) {
                                            return (
                                                <text
                                                    x={viewBox.cx}
                                                    y={viewBox.cy}
                                                    textAnchor="middle"
                                                    dominantBaseline="middle"
                                                >
                                                    <tspan
                                                        x={viewBox.cx}
                                                        y={(viewBox.cy || 0) - 6}
                                                        className="fill-muted-foreground text-xs font-medium sm:text-sm"
                                                    >
                                                        Total Units
                                                    </tspan>

                                                    <tspan
                                                        x={viewBox.cx}
                                                        y={(viewBox.cy || 0) + 22}
                                                        className="fill-foreground text-xl font-bold sm:text-2xl"
                                                    >
                                                        {finalTotal.toLocaleString()}
                                                    </tspan>
                                                </text>
                                            );
                                        }

                                        return null;
                                    }}
                                />
                            </Pie>
                        </PieChart>
                    </ChartContainer>
                </div>

                <div className="w-full min-w-0 rounded-lg min-[1700px]:flex-1">
                    <table className="w-full table-fixed border-separate border-spacing-0 text-xs">
                        <thead>
                            <tr className="text-slate-600">
                                <th className="w-[48%] px-3 py-2 text-left font-semibold">
                                    Ageing Bucket
                                </th>
                                <th className="w-[24%] px-3 py-2 text-center font-semibold">
                                    Units
                                </th>
                                <th className="w-[28%] px-3 py-2 text-center font-semibold">
                                    % of Total
                                </th>
                            </tr>
                        </thead>

                        <tbody>
                            {chartData.map((item) => (
                                <tr
                                    key={item.bucket}
                                    className="border-t border-slate-200 transition-colors hover:bg-white"
                                >
                                    <td className="border-t border-slate-200 px-3 py-2 text-slate-800">
                                        <span
                                            className="mr-2 inline-block h-2.5 w-2.5 rounded-full"
                                            style={{ backgroundColor: item.color }}
                                        />
                                        {item.bucket}
                                    </td>

                                    <td className="border-t border-slate-200 px-3 py-2 text-center text-slate-700">
                                        {item.units.toLocaleString()}
                                    </td>

                                    <td className="border-t border-slate-200 px-3 py-2 text-center text-slate-700">
                                        {item.percentage.toFixed(1)}%
                                    </td>
                                </tr>
                            ))}

                            <tr className="font-bold text-slate-900">
                                <td className="border-t border-slate-300 px-3 py-2">
                                    Total
                                </td>
                                <td className="border-t border-slate-300 px-3 py-2 text-center">
                                    {finalTotal.toLocaleString()}
                                </td>
                                <td className="border-t border-slate-300 px-3 py-2 text-center">
                                    100%
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
};

export default React.memo(SkuAgeingDonutChart);