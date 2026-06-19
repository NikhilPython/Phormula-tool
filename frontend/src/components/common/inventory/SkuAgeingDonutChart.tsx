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
        <div className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
            {/* <div className="mb-4">
                <h3 className="text-lg font-extrabold uppercase text-slate-900">
                    {title}
                </h3>
            </div> */}
            <PageBreadcrumb pageTitle="Ageing Donut Chart" variant="page" align="left" textSize="2xl" />

            <div className="grid grid-cols-1 items-center gap-6 lg:grid-cols-[1.1fr_0.9fr]">
                <div className="flex justify-center">
                    <ChartContainer
                        config={chartConfig}
                        className="mx-auto aspect-square h-[330px] max-h-[330px] w-full max-w-[330px]"
                    >
                        <PieChart>
                            <ChartTooltip
                                cursor={false}
                                content={
                                    <ChartTooltipContent
                                        hideLabel
                                        formatter={(
                                            value: number | string | undefined,
                                            _name: string,
                                            item: any
                                        ) => {
                                            const payload = item.payload as DonutChartItem & {
                                                percentage: number;
                                            };

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
                                                        {Number(value ?? 0).toLocaleString()} (
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
                                innerRadius={82}
                                outerRadius={125}
                                paddingAngle={1.5}
                                strokeWidth={4}
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
                                                        className="fill-muted-foreground text-sm font-medium"
                                                    >
                                                        Total Units
                                                    </tspan>

                                                    <tspan
                                                        x={viewBox.cx}
                                                        y={(viewBox.cy || 0) + 22}
                                                        className="fill-foreground text-2xl font-bold"
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

                <div className="overflow-hidden rounded-lg p-2">
                    <table className="w-full border-separate border-spacing-0 text-xs">
                        <thead>
                            <tr className="text-slate-600">
                                <th className="px-3 py-2 text-left font-semibold">
                                    Ageing Bucket
                                </th>
                                <th className="px-3 py-2 text-center font-semibold">Units</th>
                                <th className="px-3 py-2 text-center font-semibold">
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
                                <td className="border-t border-slate-300 px-3 py-2">Total</td>
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

export default SkuAgeingDonutChart;