"use client";

import React, { useEffect, useMemo, useRef } from "react";
import dynamic from "next/dynamic";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

type ObjectiveMoMChartProps = {
    title?: string;
    className?: string;
};

const GREEN = "#97A95F";
const ORANGE = "#ED9F50";

const yLabels = ["Conservative", "Balanced", "Aggressive"] as const;
type YLabel = (typeof yLabels)[number];

const labelToValue = (label: YLabel) => yLabels.indexOf(label);

const dummyObjectiveMoMData: Array<{
    month: string;
    profit: YLabel;
    sale: YLabel;
}> = [
        { month: "Jan’24", profit: "Balanced", sale: "Conservative" },
        { month: "Feb’24", profit: "Balanced", sale: "Balanced" },
        { month: "Mar’24", profit: "Aggressive", sale: "Balanced" },
        { month: "Apr’24", profit: "Aggressive", sale: "Balanced" },
        { month: "May’24", profit: "Balanced", sale: "Aggressive" },
        { month: "Jun’24", profit: "Conservative", sale: "Aggressive" },
        { month: "Jul’24", profit: "Balanced", sale: "Balanced" },
        { month: "Aug’24", profit: "Aggressive", sale: "Balanced" },
        { month: "Sep’24", profit: "Aggressive", sale: "Conservative" },
        { month: "Oct’24", profit: "Balanced", sale: "Balanced" },
        { month: "Nov’24", profit: "Conservative", sale: "Aggressive" },
        { month: "Dec’24", profit: "Balanced", sale: "Aggressive" },

        { month: "Jan’25", profit: "Aggressive", sale: "Balanced" },
        { month: "Feb’25", profit: "Balanced", sale: "Aggressive" },
        { month: "Mar’25", profit: "Aggressive", sale: "Balanced" },
        { month: "Apr’25", profit: "Balanced", sale: "Balanced" },
        { month: "May’25", profit: "Conservative", sale: "Aggressive" },
        { month: "Jun’25", profit: "Balanced", sale: "Aggressive" },
        { month: "Jul’25", profit: "Aggressive", sale: "Balanced" },
        { month: "Aug’25", profit: "Balanced", sale: "Conservative" },
        { month: "Sep’25", profit: "Balanced", sale: "Balanced" },
        { month: "Oct’25", profit: "Aggressive", sale: "Balanced" },
        { month: "Nov’25", profit: "Aggressive", sale: "Conservative" },
        { month: "Dec’25", profit: "Balanced", sale: "Aggressive" },
    ];

export default function ObjectiveMoMChart({
    className = "",
}: ObjectiveMoMChartProps) {
    const echartsInstanceRef = useRef<any>(null);
    const containerRef = useRef<HTMLDivElement | null>(null);

    useEffect(() => {
        const el = containerRef.current;
        if (!el) return;

        const ro = new ResizeObserver(() => {
            try {
                echartsInstanceRef.current?.resize();
            } catch { }
        });

        ro.observe(el);
        return () => ro.disconnect();
    }, []);

    const xAxisData = useMemo(
        () => dummyObjectiveMoMData.map((item) => item.month),
        []
    );

    const profitSeriesData = useMemo(
        () =>
            dummyObjectiveMoMData.map((item) => ({
                value: labelToValue(item.profit),
                rawLabel: item.profit,
            })),
        []
    );

    const saleSeriesData = useMemo(
        () =>
            dummyObjectiveMoMData.map((item) => ({
                value: labelToValue(item.sale),
                rawLabel: item.sale,
            })),
        []
    );

    const option = useMemo(
        () => ({
            animation: true,

            tooltip: {
                trigger: "axis",
                textStyle: {
                    fontSize: 12,
                    color: "#414042",
                },
                formatter: (params: any[]) => {
                    const month = params?.[0]?.axisValue ?? "";

                    const lines = params.map((p: any) => {
                        const rawValue = p?.data?.rawLabel ?? "-";

                        return `
              <div style="font-size:12px; line-height:1.4; color:#414042;">
                <span style="display:inline-block;width:10px;height:10px;margin-right:6px;background:${p.color};border-radius:0;"></span>
                <span>${p.seriesName}: </span>
                <span style="color:#414042;">${rawValue}</span>
              </div>
            `;
                    });

                    return `
            <div style="font-size:12px; color:#414042;">
              <div style="font-weight:600; margin-bottom:4px; color:#141414;">
                ${month}
              </div>
              ${lines.join("")}
            </div>
          `;
                },
            },

            legend: {
                top: 10,
                left: "left",
                orient: "horizontal",
                icon: "rect",
                itemWidth: 10,
                itemHeight: 10,
                itemGap: 14,
                textStyle: {
                    fontSize: 12,
                    color: "#6B7280",
                    padding: [0, 6, 0, 6],
                },
                data: ["Profit", "Sale"],
            },

            grid: {
                left: 78,
                right: 18,
                top: 62,
                bottom: 40,
                containLabel: false,
            },

            xAxis: {
                type: "category",
                data: xAxisData,
                boundaryGap: false,
                axisLine: {
                    lineStyle: {
                        color: "#D1D5DB",
                        width: 1,
                    },
                },
                axisTick: {
                    lineStyle: {
                        color: "#D1D5DB",
                    },
                },
                axisLabel: {
                    color: "#6B7280",
                    formatter: (value: string) => value.replace("’ ", "’"),
                },
            },

            yAxis: {
                type: "value",
                min: 0,
                max: 2,
                interval: 1,
                name: "Growth Rate",
                nameLocation: "middle",
                nameGap: 55,
                nameTextStyle: {
                    color: "#6B7280",
                },
                axisLine: {
                    lineStyle: {
                        color: "#D1D5DB",
                        width: 1,
                    },
                },
                axisLabel: {
                    margin: 8,
                    color: "#6B7280",
                    formatter: (value: number) => yLabels[value] ?? "",
                },
                splitLine: {
                    show: true,
                    lineStyle: {
                        color: "#E5E7EB",
                        width: 1,
                        type: "solid",
                    },
                },
            },

            dataZoom: [
                {
                    type: "inside",
                    xAxisIndex: 0,
                    filterMode: "none",
                    zoomOnMouseWheel: false,
                    moveOnMouseMove: true,
                    moveOnMouseWheel: true,
                    preventDefaultMouseMove: true,
                    startValue: Math.max(0, xAxisData.length - 12),
                    endValue: xAxisData.length - 1,
                },
            ],

            series: [
                {
                    name: "Profit",
                    type: "line",
                    smooth: true,
                    showSymbol: true,
                    symbol: "circle",
                    symbolSize: 7,
                    emphasis: {
                        scale: true,
                        itemStyle: {
                            color: GREEN,
                        },
                        symbolSize: 11,
                    },
                    lineStyle: { color: GREEN, width: 2 },
                    itemStyle: {
                        color: GREEN,
                        borderWidth: 0,
                    },
                    data: profitSeriesData,
                },
                {
                    name: "Sale",
                    type: "line",
                    smooth: true,
                    showSymbol: true,
                    symbol: "circle",
                    symbolSize: 7,
                    emphasis: {
                        scale: true,
                        itemStyle: {
                            color: ORANGE,
                        },
                        symbolSize: 11,
                    },
                    lineStyle: { color: ORANGE, width: 2 },
                    itemStyle: {
                        color: ORANGE,
                        borderWidth: 0,
                    },
                    data: saleSeriesData,
                },
            ],
        }),
        [profitSeriesData, saleSeriesData, xAxisData]
    );

    return (
        <div className={`h-full w-full overflow-hidden ${className}`}>
            <div ref={containerRef} className="h-full w-full min-h-0">
                <ReactECharts
                    option={option}
                    notMerge
                    lazyUpdate
                    style={{ width: "100%", height: "100%" }}
                    opts={{ renderer: "canvas" }}
                    onChartReady={(instance) => {
                        echartsInstanceRef.current = instance;
                        try {
                            instance.resize();
                        } catch { }
                    }}
                />
            </div>
        </div>
    );
}