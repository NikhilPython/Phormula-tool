"use client";

import React, { useMemo, useRef, useEffect } from "react";
import dynamic from "next/dynamic";
import PageBreadcrumb from "../PageBreadCrumb";

const ReactECharts = dynamic(() => import("echarts-for-react"), {
  ssr: false,
});

export type AgeingTrendItem = {
  label: string;
  value: number;
};

export type AgeingTrendBucketOption = {
  label: string;
  value: string;
  color: string;
};

export type AgeingTrendAllSeriesItem = {
  bucketValue: string;
  bucketLabel: string;
  color: string;
  data: AgeingTrendItem[];
};

type AgeingTrendChartProps = {
  title?: string;
  subtitle?: string;
  allSeriesData?: AgeingTrendAllSeriesItem[];
};

const getShortMonth = (label: string) => {
  const monthPart = label.split(" ")[0];
  const date = new Date(`${monthPart} 1, 2000`);

  if (Number.isNaN(date.getTime())) {
    return monthPart;
  }

  return date.toLocaleString("default", {
    month: "short",
  });
};

const AgeingTrendChart: React.FC<AgeingTrendChartProps> = ({
  title = "Ageing Trend Over Time",
  allSeriesData = [],
}) => {
  const echartsInstanceRef = useRef<any>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  const currentMonthShort = new Date().toLocaleString("default", {
    month: "short",
  });

  const filterCurrentMonth = (items: AgeingTrendItem[]) => {
    return items.filter((item) => {
      const itemMonth = getShortMonth(item.label).toLowerCase();
      return itemMonth !== currentMonthShort.toLowerCase();
    });
  };

  const allChartSeriesData = useMemo(() => {
    return allSeriesData.map((bucket) => ({
      ...bucket,
      data: filterCurrentMonth(bucket.data),
    }));
  }, [allSeriesData, currentMonthShort]);

  const categories = useMemo(() => {
    if (allChartSeriesData.length > 0) {
      return allChartSeriesData[0].data.map((item) => getShortMonth(item.label));
    }

    return [];
  }, [allChartSeriesData]);

  const chartSeries = useMemo(() => {
    return allChartSeriesData.map((bucket) => ({
      name: bucket.bucketLabel,
      color: bucket.color,
      data: bucket.data.map((item) => Number(item.value || 0)),
    }));
  }, [allChartSeriesData]);

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

  const option = useMemo(
    () => ({
      tooltip: {
        trigger: "axis",
        textStyle: {
          fontSize: 12,
          color: "#414042",
        },
        formatter: (params: any) => {
          const header = params?.[0]?.axisValue ?? "";

          const lines = (params || []).map((p: any) => {
            const value =
              p?.data == null ? "-" : Number(p.data).toLocaleString();

            return `
              <div style="font-size:12px; line-height:1.4; color:#414042;">
                <span style="display:inline-block;width:10px;height:10px;margin-right:6px;background:${p.color};border-radius:0;"></span>
                <span>${p.seriesName}: </span>
                <span style="color:#414042;">${value} units</span>
              </div>
            `;
          });

          return `
            <div style="font-size:12px; color:#414042;">
              <div style="font-weight:600; margin-bottom:4px; color:#414042;">
                ${header}
              </div>
              ${lines.join("")}
            </div>
          `;
        },
      },

      legend: {
        show: true,
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
        data: chartSeries.map((series) => series.name),
      },

      grid: {
        left: 46,
        right: 16,
        top: 62,
        bottom: 44,
      },

      xAxis: {
        type: "category",
        data: categories,
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
          fontSize: 12,
        },
      },

      yAxis: {
        type: "value",
        axisLine: {
          lineStyle: {
            color: "#D1D5DB",
            width: 1,
          },
        },
        axisLabel: {
          margin: 2,
          color: "#6B7280",
          fontSize: 12,
          formatter: (value: number) => Number(value).toLocaleString(),
        },
        splitLine: {
          lineStyle: {
            color: "#E5E7EB",
          },
        },
      },

      series: chartSeries.map((series) => ({
        name: series.name,
        type: "line",
        smooth: true,
        showSymbol: true,
        symbol: "circle",
        symbolSize: 7,
        emphasis: {
          scale: true,
          itemStyle: {
            color: series.color,
          },
          symbolSize: 11,
        },
        lineStyle: {
          color: series.color,
          width: 2,
        },
        itemStyle: {
          color: series.color,
          borderWidth: 0,
        },
        areaStyle: {
          opacity: 0.08,
          color: series.color,
        },
        data: series.data,
      })),
    }),
    [categories, chartSeries]
  );

  return (
    <div className="w-full h-full min-h-0 overflow-hidden flex flex-col rounded-xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5">
      <div className="shrink-0 flex items-center gap-3 w-full">
        <PageBreadcrumb
          pageTitle={title}
          variant="page"
          align="left"
          textSize="2xl"
        />

        {/* <div
          className="flex items-center shrink-0"
          data-no-expand
          onPointerDown={(e) => e.stopPropagation()}
          onClick={(e) => e.stopPropagation()}
        >
          <div className="flex items-center gap-2 text-sm">
            <span className="whitespace-nowrap font-medium text-slate-700">
              Ageing Bucket
            </span>

            <select
              value={selectedBucket}
              onChange={(e) => onBucketChange?.(e.target.value)}
              className="h-10 min-w-[200px] rounded-md border border-slate-300 bg-white px-4 text-sm font-semibold outline-none focus:border-[#5EA68E] focus:ring-2 focus:ring-[#5EA68E]/20"
            >
              <option value="all">All</option>

              {bucketOptions.length > 0 ? (
                bucketOptions.map((bucket) => (
                  <option key={bucket.value} value={bucket.value}>
                    {bucket.label}
                  </option>
                ))
              ) : (
                <option value={selectedBucket}>{selectedBucket}</option>
              )}
            </select>
          </div>
        </div> */}
      </div>

      <div className="mt-2 flex-1 min-h-[260px] md:min-h-[287px] xl:min-h-[300px] 2xl:min-h-[360px] overflow-hidden">
        <div ref={containerRef} className="w-full h-full min-h-0 overflow-hidden">
          <ReactECharts
            option={option}
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
    </div>
  );
};

export default AgeingTrendChart;