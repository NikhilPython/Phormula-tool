// components/charts/InventoryAgeStackedBarChart.tsx

"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
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

ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    Tooltip,
    Legend,
    ChartTitle
);

type InventoryAgeChartRow = {
    sku: string;
    productName: string;
    age0to90: number;
    age91to180: number;
    age181to270: number;
    age271to365: number;
    age365plus: number;
};

type InventoryAgeStackedBarChartProps = {
    data: InventoryAgeChartRow[];
};

const InventoryAgeStackedBarChart: React.FC<InventoryAgeStackedBarChartProps> = ({
    data,
}) => {
    const chartRef = useRef<any>(null);
    const [isSmallScreen, setIsSmallScreen] = useState(false);

    useEffect(() => {
        const mq = window.matchMedia("(max-width: 1280px)");

        const onChange = (e: MediaQueryListEvent | MediaQueryList) => {
            setIsSmallScreen(e.matches);
        };

        setIsSmallScreen(mq.matches);

        if ("addEventListener" in mq) {
            mq.addEventListener("change", onChange as any);
        } else {
            (mq as any).addListener?.(onChange);
        }

        return () => {
            if ("removeEventListener" in mq) {
                mq.removeEventListener("change", onChange as any);
            } else {
                (mq as any).removeListener?.(onChange);
            }
        };
    }, []);

    const labels = useMemo(
        () => data.map((row) => row.productName || row.sku),
        [data]
    );

    const chartData = useMemo(
        () => ({
            labels,
            datasets: [
                {
                    label: "0-90",
                    data: data.map((row) => row.age0to90),
                    backgroundColor: "#7B9A6D",
                    borderRadius: 4,
                    borderWidth: 0,
                    stack: "inventoryAge",
                },
                {
                    label: "91-180",
                    data: data.map((row) => row.age91to180),
                    backgroundColor: "#B8C78C",
                    borderRadius: 4,
                    borderWidth: 0,
                    stack: "inventoryAge",
                },
                {
                    label: "181-270",
                    data: data.map((row) => row.age181to270),
                    backgroundColor: "#FDD36F",
                    borderRadius: 4,
                    borderWidth: 0,
                    stack: "inventoryAge",
                },
                {
                    label: "271-365",
                    data: data.map((row) => row.age271to365),
                    backgroundColor: "#ED9F50",
                    borderRadius: 4,
                    borderWidth: 0,
                    stack: "inventoryAge",
                },
                {
                    label: "365+",
                    data: data.map((row) => row.age365plus),
                    backgroundColor: "#B75A5A",
                    borderRadius: 4,
                    borderWidth: 0,
                    stack: "inventoryAge",
                },
            ],
        }),
        [data, labels]
    );

    const options: ChartOptions<"bar"> = {
        responsive: true,
        maintainAspectRatio: false,

        interaction: {
            mode: "nearest",
            intersect: true,
        },

        layout: {
            padding: {
                top: 5,
                bottom: 10,
            },
        },

        plugins: {
            legend: {
                display: true,
                position: "top",
                labels: {
                    padding: 18,
                    boxWidth: 12,
                    boxHeight: 12,
                },
            },
            tooltip: {
                callbacks: {
                    label: (context) => {
                        const value = Number(context.raw ?? 0);
                        return `${context.dataset.label}: ${value.toLocaleString("en-IN")} units`;
                    },
                    footer: (items) => {
                        const index = items?.[0]?.dataIndex ?? 0;
                        const row = data[index];

                        if (!row) return "";

                        const total =
                            row.age0to90 +
                            row.age91to180 +
                            row.age181to270 +
                            row.age271to365 +
                            row.age365plus;

                        return `Total: ${total.toLocaleString("en-IN")} units`;
                    },
                },
            },
            title: {
                display: false,
            },
        },

        scales: {
            x: {
                stacked: true,
                grid: {
                    display: false,
                    drawOnChartArea: false,
                    drawTicks: false,
                },
                ticks: {
                    autoSkip: false,
                    maxRotation: isSmallScreen ? 45 : 0,
                    minRotation: isSmallScreen ? 45 : 0,
                    padding: 16,
                    callback: (_value, index) => {
                        const label = labels[index] ?? "";

                        if (label.length > 14) {
                            const words = label.split(" ");

                            if (words.length >= 2) {
                                const mid = Math.ceil(words.length / 2);
                                return [
                                    words.slice(0, mid).join(" "),
                                    words.slice(mid).join(" "),
                                ];
                            }

                            return `${label.slice(0, 14)}…`;
                        }

                        return label;
                    },
                },
            },
            y: {
                stacked: true,
                beginAtZero: true,
                grid: {
                    display: true,
                    drawOnChartArea: true,
                    drawTicks: false,
                },
                title: {
                    display: true,
                    text: "Inventory units",
                },
                ticks: {
                    callback: (value) => {
                        return Math.round(Number(value)).toLocaleString("en-IN");
                    },
                },
            },
        },
    };

    return (
        <div className="relative h-full w-full">
            <Bar
                key={isSmallScreen ? "small" : "large"}
                ref={chartRef}
                data={chartData}
                options={options}
            />
        </div>
    );
};

export default InventoryAgeStackedBarChart;