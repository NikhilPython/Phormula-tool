"use client";

import React, { useMemo, useRef, useEffect, useState } from "react";
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
  title = "FBA Inventory Ageing Trend Over Time",
  allSeriesData = [],
}) => {
  const echartsInstanceRef = useRef<any>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  const [selectedBuckets, setSelectedBuckets] = useState<Record<string, boolean>>({});

  useEffect(() => {
    setSelectedBuckets((prev) => {
      const next: Record<string, boolean> = {};

      allSeriesData.forEach((bucket) => {
        next[bucket.bucketValue] = prev[bucket.bucketValue] ?? true;
      });

      return next;
    });
  }, [allSeriesData]);

  const toggleBucket = (bucketValue: string) => {
    const selectedCount = Object.values(selectedBuckets).filter(Boolean).length;
    const isChecked = !!selectedBuckets[bucketValue];

    if (isChecked && selectedCount === 1) return;

    setSelectedBuckets((prev) => ({
      ...prev,
      [bucketValue]: !isChecked,
    }));
  };

  // const currentMonthShort = new Date().toLocaleString("default", {
  //   month: "short",
  // });

  // const filterCurrentMonth = (items: AgeingTrendItem[]) => {
  //   return items.filter((item) => {
  //     const itemMonth = getShortMonth(item.label).toLowerCase();
  //     return itemMonth !== currentMonthShort.toLowerCase();
  //   });
  // };

  // const allChartSeriesData = useMemo(() => {
  //   return allSeriesData.map((bucket) => ({
  //     ...bucket,
  //     data: filterCurrentMonth(bucket.data),
  //   }));
  // }, [allSeriesData, currentMonthShort]);

  const allChartSeriesData = useMemo(() => {
    return allSeriesData.map((bucket) => ({
      ...bucket,
      data: bucket.data || [],
    }));
  }, [allSeriesData]);

  // const categories = useMemo(() => {
  //   if (allChartSeriesData.length > 0) {
  //     return allChartSeriesData[0].data.map((item) => getShortMonth(item.label));
  //   }

  //   return [];
  // }, [allChartSeriesData]);
  const monthOrder = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];

  const categories = useMemo(() => {
    const monthSet = new Set<string>();

    allChartSeriesData.forEach((bucket) => {
      bucket.data.forEach((item) => {
        monthSet.add(getShortMonth(item.label));
      });
    });

    return Array.from(monthSet).sort(
      (a, b) => monthOrder.indexOf(a) - monthOrder.indexOf(b)
    );
  }, [allChartSeriesData]);

  // const chartSeries = useMemo(() => {
  //   return allChartSeriesData
  //     .filter((bucket) => selectedBuckets[bucket.bucketValue] !== false)
  //     .map((bucket) => ({
  //       name: bucket.bucketLabel,
  //       color: bucket.color,
  //       data: bucket.data.map((item) => Number(item.value || 0)),
  //     }));
  // }, [allChartSeriesData, selectedBuckets]);

  const chartSeries = useMemo(() => {
    return allChartSeriesData
      .filter((bucket) => selectedBuckets[bucket.bucketValue] !== false)
      .map((bucket) => {
        const valueByMonth = new Map<string, number>();

        bucket.data.forEach((item) => {
          valueByMonth.set(
            getShortMonth(item.label),
            Number(item.value || 0)
          );
        });

        return {
          name: bucket.bucketLabel,
          color: bucket.color,
          data: categories.map((month) => valueByMonth.get(month) ?? 0),
        };
      });
  }, [allChartSeriesData, selectedBuckets, categories]);

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
        show: false,
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
        left: 40,
        right: 12,
        top: 48,
        bottom: 32,
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
    <div className="w-full h-full min-h-0 overflow-hidden flex flex-col rounded-xl border border-slate-200 bg-white p-3 shadow-sm sm:p-4 xl:p-5">
      <div className="shrink-0 flex items-center gap-3 w-full">
        <PageBreadcrumb
          pageTitle={title}
          variant="page"
          align="left"
          textSize="2xl"
        />
      </div>

      <div
        data-no-expand
        className="shrink-0 flex flex-wrap items-center justify-center gap-4 w-full mt-2"
      >
        {allChartSeriesData.map((bucket) => {
          const isChecked = selectedBuckets[bucket.bucketValue] !== false;

          return (
            <button
              key={bucket.bucketValue}
              type="button"
              data-no-expand
              onMouseDown={(e) => e.stopPropagation()}
              onClick={(e) => {
                e.stopPropagation();
                toggleBucket(bucket.bucketValue);
              }}
              className={[
                "shrink-0",
                "flex items-center gap-1 sm:gap-1.5",
                "font-semibold select-none whitespace-nowrap",
                "text-[10px] 2xl:text-xs my-1 2xl:my-3",
                "text-charcoal-500",
                "cursor-pointer",
              ].join(" ")}
            >
              <span
                data-no-expand
                className="flex items-center justify-center h-3 w-3 sm:h-3.5 sm:w-3.5 rounded-sm border transition"
                style={{
                  borderColor: bucket.color,
                  backgroundColor: isChecked ? bucket.color : "white",
                }}
              >
                {isChecked && (
                  <svg
                    viewBox="0 0 24 24"
                    width="12"
                    height="12"
                    className="text-white"
                  >
                    <path
                      fill="currentColor"
                      d="M20.285 6.709a1 1 0 0 0-1.414-1.414L9 15.168l-3.879-3.88a1 1 0 0 0-1.414 1.415l4.586 4.586a1 1 0 0 0 1.414 0l10-10Z"
                    />
                  </svg>
                )}
              </span>

              <span>{bucket.bucketLabel}</span>
            </button>
          );
        })}
      </div>

      <div className="mt-1 flex-1 min-h-[220px] md:min-h-[240px] lg:min-h-[260px] xl:min-h-[280px] 2xl:min-h-[340px] overflow-hidden">
        <div ref={containerRef} className="w-full h-full min-h-0 overflow-hidden">
          <ReactECharts
            option={option}
            notMerge={true}
            lazyUpdate={false}
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