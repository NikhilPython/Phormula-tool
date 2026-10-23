"use client";

import React, { useEffect, useMemo, useState } from "react";
import {
  ArrowRight,
  Bot,
  Boxes,
  ChevronLeft,
  ChevronRight,
  CircleDollarSign,
  Sparkles,
  TrendingDown,
  TrendingUp,
  TriangleAlert,
} from "lucide-react";
import { AnimatePresence, motion } from "framer-motion";
import PageBreadcrumb from "../common/PageBreadCrumb";
import SummaryMetricCard from "./SummaryMetricCard";
import DataTable, {
  type ColumnDef,
  type Row as DataTableRow,
} from "@/components/ui/table/DataTable";

export type SummaryDestination =
  | "graphs"
  | "skuBreakdown"
  | "cashFlow"
  | "skuwiseProfit"
  | "businessSummary"
  | "inventoryInsights";

type DeltaMetric = {
  label?: string;
  value: number;
  delta?: number;
  previousValue?: number;
  perUnit?: number;
  previousPerUnit?: number;
  percentage?: number;
  previousPercentage?: number;
  loading?: boolean;
};

type SkuTableMetric = {
  productName: string;
  sku: string;
  units: number;
  netSales: number;
  netSalesIncrease?: number;
  secondaryValue: number;
  secondaryPercentage?: number;
  previousNetSales?: number;
  netSalesDeltaPercentage?: number;
  cm2ProfitDeltaPercentage?: number;
};

export type PnlSummaryOverviewData = {
  periodLabel: string;
  comparisonLabel: string;
  currencySymbol: string;
  financial: {
    netSales: DeltaMetric;
    cm2Profit: DeltaMetric;
    tacos: DeltaMetric;
    units: DeltaMetric;
    marketplaceFees: DeltaMetric;
    cm2Margin: number;
    target?: DeltaMetric;
  };
  ai: {
    summary: string;
    recommendation: string;
    loading?: boolean;
  };
  pnl: {
    profitMetric: "CM1" | "CM2";
    heroSkus: SkuTableMetric[];
    leastPerformingSkus: SkuTableMetric[];
    topNetSalesGrowthSkus: SkuTableMetric[];
  };
  cashFlow: {
    cashGenerated: number;
    previousCashGenerated?: number;
    delta?: number;
    loading?: boolean;
  };
  inventory: {
    totalUnits: number;
    totalSkus?: number;
    currentFbaPercentage?: number;
    currentAwdPercentage?: number;
    inventoryCoverageRatio?: number;
    ageingInventoryUnits?: number;
    healthySkus: number;
    healthyUnits: number;
    highAlertSkus: number;
    highAlertUnits: number;
    returnRate?: number;
    returnRateSkus?: number;
    inTransitUnits?: number;
    inTransitSkus?: number;
    seaDispatchUnits?: number;
    airDispatchUnits?: number;
    seaDispatchSkus?: number;
    airDispatchSkus?: number;
    storageCostLabel?: string;
    estimatedStorageCost: string;
    estimatedStorageCostDeltaValue?: string;
    estimatedStorageCostDeltaPercentage?: number | null;
    loading?: boolean;
    unavailable?: boolean;
  };
};

type Props = {
  data: PnlSummaryOverviewData;
  onNavigate: (tab: SummaryDestination) => void;
  hideSkuColumns?: boolean;
};

const formatWholeNumber = (value: number) => {
  const roundedValue = Math.round(Number(value || 0));

  return new Intl.NumberFormat("en", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(Object.is(roundedValue, -0) ? 0 : roundedValue);
};

const formatMoney = (value: number, currencySymbol: string) => {
  const roundedValue = Math.round(Number(value || 0));
  const sign = roundedValue < 0 ? "-" : "";
  return `${sign}${currencySymbol}${formatWholeNumber(Math.abs(roundedValue))}`;
};

const formatPercentageNumber = (value: number) =>
  Number(value || 0).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });

const formatPercent = (value: number) => `${formatPercentageNumber(value)}%`;


const formatMoneyWithPerUnit = (
  value: number,
  perUnit: number | undefined,
  currencySymbol: string
): React.ReactNode => (
  <span className="inline-flex items-baseline gap-1 whitespace-nowrap">
    <span className="text-base 2xl:text-lg font-bold text-charcoal-500">
      {formatMoney(value, currencySymbol)}
    </span>

    {typeof perUnit === "number" && Number.isFinite(perUnit) && (
      <span className="text-[10px] 2xl:text-xs font-normal text-charcoal-500">
        ({formatMoney(perUnit, currencySymbol)}/unit)
      </span>
    )}
  </span>
);

const formatMoneyWithPercentage = (
  value: number,
  percentage: number | undefined,
  currencySymbol: string
): React.ReactNode => (
  <span className="inline-flex items-baseline gap-1 whitespace-nowrap">
    <span className="text-base 2xl:text-lg font-bold text-charcoal-500">
      {formatMoney(value, currencySymbol)}
    </span>

    {typeof percentage === "number" && Number.isFinite(percentage) && (
      <span className="text-[10px] 2xl:text-xs font-normal text-charcoal-500">
        ({formatPercent(percentage)})
      </span>
    )}
  </span>
);

const formatMoneyWithPerUnitText = (
  value: number,
  perUnit: number | undefined,
  currencySymbol: string
) =>
  `${formatMoney(value, currencySymbol)}${typeof perUnit === "number" && Number.isFinite(perUnit)
    ? ` (${formatMoney(perUnit, currencySymbol)}/unit)`
    : ""
  }`;

const formatMoneyWithPercentageText = (
  value: number,
  percentage: number | undefined,
  currencySymbol: string
) =>
  `${formatMoney(value, currencySymbol)}${typeof percentage === "number" && Number.isFinite(percentage)
    ? ` (${formatPercent(percentage)})`
    : ""
  }`;


const roundFormattedValue = (value: string) =>
  value.replace(/-?\d[\d,]*(?:\.\d+)?/, (match) => {
    const numericValue = Number(match.replace(/,/g, ""));
    return Number.isFinite(numericValue) ? formatWholeNumber(numericValue) : match;
  });

const SkeletonLine = ({ className = "" }: { className?: string }) => (
  <div className={`h-3 animate-pulse rounded-full bg-slate-200 ${className}`} />
);

const StorageCostDelta = ({
  deltaValue,
  deltaPercentage,
}: {
  deltaValue?: string;
  deltaPercentage?: number | null;
}) => {
  if (typeof deltaPercentage !== "number") return null;

  const deltaSymbol = deltaPercentage <= 0 ? "▼" : "▲";

  return (
    <span
      className={`inline-flex items-center gap-1 text-xs font-semibold ${deltaPercentage <= 0 ? "text-emerald-600" : "text-red-600"
        }`}
      title={
        deltaValue
          ? `Change vs previous month: ${roundFormattedValue(deltaValue)}`
          : "Change vs previous month"
      }
    >
      <span aria-hidden="true">{deltaSymbol}</span>
      {formatPercentageNumber(Math.abs(deltaPercentage))}%
    </span>
  );
};

const MetricTile = ({
  label,
  value,
  detail,
  trailing,
  accent = "border-[#5EA68E] ",
  compact = false,
}: {
  label: string;
  value: React.ReactNode;
  detail?: React.ReactNode;
  trailing?: React.ReactNode;
  accent?: string;
  compact?: boolean;
}) => (
  <div
    className={`relative flex h-full flex-col justify-center overflow-hidden rounded-xl border border-t-4 bg-white shadow-sm ${compact
      ? "min-h-[72px] p-2.5 sm:p-3 lg:min-h-0 2xl:p-3"
      : "min-h-[108px] p-3.5 sm:p-4 2xl:min-h-[132px] 2xl:p-5"
      } ${accent}`}
  >
    <p className="text-xs font-medium capitalize tracking-wide text-charcoal-500">
      {label}
    </p>

    <div className={`${compact ? "mt-1" : "mt-2"} flex items-center justify-between gap-3`}>
      <div className={`min-w-0 font-bold text-charcoal-500 ${compact
        ? "text-lg 2xl:text-xl"
        : "text-lg sm:text-xl 2xl:text-2xl"
        }`}>
        {value}
      </div>
      {trailing ? (
        <div className="shrink-0 text-[10px] 2xl:text-xs">
          {trailing}
        </div>
      ) : null}
    </div>

    {detail ? (
      <div className="mt-1.5 text-[10px] leading-relaxed text-charcoal-500 2xl:mt-2 2xl:text-xs">
        {detail}
      </div>
    ) : null}
  </div>
);

const InventoryInsightsGrid = ({
  inventory,
  loading = false,
}: {
  inventory: PnlSummaryOverviewData["inventory"];
  loading?: boolean;
}) => {
  const loadingValue = <SkeletonLine className="w-20" />;
  const blankValue = <span aria-hidden="true">&nbsp;</span>;
  const value = (content: React.ReactNode) => loading ? loadingValue : content;
  const optionalUnits = (units?: number) =>
    typeof units === "number" && Number.isFinite(units)
      ? `${formatWholeNumber(units)} units`
      : "-";
  const optionalPercentage = (percentage?: number) =>
    typeof percentage === "number" && Number.isFinite(percentage)
      ? formatPercent(percentage)
      : "-";

  return (
    <div className="grid w-full grid-cols-2 gap-2 lg:h-full lg:grid-cols-4 lg:grid-rows-3 2xl:gap-2.5">
      <MetricTile
        label="Total inventory"
        value={value(`${formatWholeNumber(inventory.totalUnits)} units`)}
        trailing={
          loading ? null : (
            <span className="whitespace-nowrap text-xs font-semibold text-slate-500">
              {typeof inventory.currentFbaPercentage === "number" &&
              Number.isFinite(inventory.currentFbaPercentage)
                ? formatPercent(inventory.currentFbaPercentage)
                : "-"}{" "}
              FBA ·{" "}
              {typeof inventory.currentAwdPercentage === "number" &&
              Number.isFinite(inventory.currentAwdPercentage)
                ? formatPercent(inventory.currentAwdPercentage)
                : "-"}{" "}
              AWD
            </span>
          )
        }
        accent="border-[#75BBDA]"
        compact
      />

      <MetricTile
        label="Inventory coverage ratio"
        value={value(
          typeof inventory.inventoryCoverageRatio === "number" &&
            Number.isFinite(inventory.inventoryCoverageRatio)
            ? formatPercentageNumber(inventory.inventoryCoverageRatio)
            : "-"
        )}
        trailing={
          loading ? null : (
            <span className="text-xs font-semibold text-slate-500">
              in months
            </span>
          )
        }
        accent="border-[#ED9F50]"
        compact
      />

      <MetricTile
        label={inventory.storageCostLabel ?? "Estimated storage cost"}
        value={value(roundFormattedValue(inventory.estimatedStorageCost))}
        trailing={
          loading ? null : (
            <StorageCostDelta
              deltaValue={inventory.estimatedStorageCostDeltaValue}
              deltaPercentage={inventory.estimatedStorageCostDeltaPercentage}
            />
          )
        }
        accent="border-[#C49466]"
        compact
      />

      <MetricTile
        label=""
        value={blankValue}
        accent="border-slate-200"
        compact
      />

      <MetricTile
        label="Ageing inventory"
        value={value(optionalUnits(inventory.ageingInventoryUnits))}
        trailing={
          loading ? null : (
            <span className="text-xs font-semibold text-slate-500">
              180+ days
            </span>
          )
        }
        accent="border-[#B8C78C]"
        compact
      />


      <MetricTile
        label="Healthy SKUs"
        value={value(`${formatWholeNumber(inventory.healthyUnits)} units`)}
        trailing={
          loading ? null : (
            <span className="text-xs font-semibold text-slate-500">
              {formatWholeNumber(inventory.healthySkus)} SKUs
            </span>
          )
        }
        accent="border-[#7B9A6D]"
        compact
      />

      <MetricTile
        label="Return rate"
        value={value(optionalPercentage(inventory.returnRate))}
        trailing={
          loading ? null : (
            <span className="text-xs font-semibold text-slate-500">
              {typeof inventory.returnRateSkus === "number"
                ? `${formatWholeNumber(inventory.returnRateSkus)} SKUs`
                : "-"}
            </span>
          )
        }
        accent="border-[#3A8EA4]"
        compact
      />


      <MetricTile
        label="Purchase order"
        value={blankValue}
        accent="border-slate-200"
        compact
      />


      <MetricTile
        label="In transit units"
        value={value(optionalUnits(inventory.inTransitUnits))}
        trailing={
          loading ? null : (
            <span className="text-xs font-semibold text-slate-500">
              {typeof inventory.inTransitSkus === "number"
                ? `${formatWholeNumber(inventory.inTransitSkus)} SKUs`
                : "-"}
            </span>
          )
        }
        accent="border-[#FDD36F]"
        compact
      />


      <MetricTile
        label="Dispatch (SEA)"
        value={value(optionalUnits(inventory.seaDispatchUnits))}
        trailing={
          loading ? null : (
            <span className="text-xs font-semibold text-slate-500">
              {typeof inventory.seaDispatchSkus === "number"
                ? `${formatWholeNumber(inventory.seaDispatchSkus)} SKUs`
                : "-"}
            </span>
          )
        }
        accent="border-[#75BBDA]"
        compact
      />

      <MetricTile
        label="Dispatch (AIR)"
        value={value(optionalUnits(inventory.airDispatchUnits))}
        trailing={
          loading ? null : (
            <span className="text-xs font-semibold text-slate-500">
              {typeof inventory.airDispatchSkus === "number"
                ? `${formatWholeNumber(inventory.airDispatchSkus)} SKUs`
                : "-"}
            </span>
          )
        }
        accent="border-[#ED9F50]"
        compact
      />

      <MetricTile
        label="High alert (Less than 2 months of coverage)"
        value={value(`${formatWholeNumber(inventory.highAlertUnits)} units left`)}
        trailing={
          loading ? null : (
            <span className="text-xs font-semibold text-slate-500">
              {formatWholeNumber(inventory.highAlertSkus)} SKUs
            </span>
          )
        }
        accent="border-[#B85C5C]"
        compact
      />


    </div>
  );
};

const SkuPerformanceTable = ({
  rows,
  metricHeading,
  currencySymbol,
  emptyMessage = "No SKU performance data is available for this period.",
  variant = "profit",
  hideSkuColumn = false,
}: {
  rows: SkuTableMetric[];
  metricHeading: string;
  currencySymbol: string;
  emptyMessage?: string;
  variant?: "profit" | "netSalesGrowth";
  hideSkuColumn?: boolean;
}) => {
  type SkuPerformanceTableRow = DataTableRow &
    SkuTableMetric & {
      rank: number;
    };

  const tableRows: SkuPerformanceTableRow[] = rows.map((row, index) => ({
    ...row,
    rank: index + 1,
  }));
  const isNetSalesGrowthTable = variant === "netSalesGrowth";
  const renderProfitMetric = (row: SkuPerformanceTableRow) => (
    <div className="inline-flex items-center justify-center gap-2 whitespace-nowrap">
      <span className="inline-flex items-baseline gap-1 text-charcoal-500">
        <span>{formatMoney(row.secondaryValue, currencySymbol)}</span>
        {typeof row.secondaryPercentage === "number" &&
          Number.isFinite(row.secondaryPercentage) ? (
          <span className="text-[9px] font-normal text-slate-500 2xl:text-[11px]">
            ({formatPercent(row.secondaryPercentage)})
          </span>
        ) : null}
      </span>
      {typeof row.cm2ProfitDeltaPercentage === "number" &&
        Number.isFinite(row.cm2ProfitDeltaPercentage) ? (
        <span
          className={`shrink-0 text-xs font-semibold ${row.cm2ProfitDeltaPercentage >= 0
            ? "text-[#5EA68E]"
            : "text-[#FF5C5C]"
            }`}
          title="Backend-provided CM2 profit change vs previous period"
        >
          {row.cm2ProfitDeltaPercentage >= 0 ? "▲" : "▼"}{" "}
          {formatPercentageNumber(Math.abs(row.cm2ProfitDeltaPercentage))}%
        </span>
      ) : null}
    </div>
  );

  const columns: ColumnDef<SkuPerformanceTableRow>[] = [
    {
      key: "rank",
      header: "S.No.",
      width: isNetSalesGrowthTable ? "6%" : "7%",
      render: (row) => row.rank,
    },
    {
      key: "productName",
      header: "Product Name",
      width: isNetSalesGrowthTable
        ? hideSkuColumn ? "39%" : "24%"
        : hideSkuColumn ? "46%" : "28%",
      cellClassName: "!text-left",
      render: (row) => {
        const hasDelta =
          typeof row.netSalesDeltaPercentage === "number" &&
          Number.isFinite(row.netSalesDeltaPercentage);
        const delta = Number(row.netSalesDeltaPercentage);
        const isPositive = delta >= 0;

        return (
          <div className="flex w-full min-w-0 items-end justify-between gap-2">
            <span
              className="min-w-0 flex-1 truncate font-normal text-green-500"
              title={row.productName}
            >
              {row.productName || "Unnamed product"}
            </span>
            {hasDelta ? (
              <span
                className={`shrink-0 whitespace-nowrap text-xs font-semibold ${isPositive ? "text-[#5EA68E]" : "text-[#FF5C5C]"
                  }`}
                title={
                  typeof row.previousNetSales === "number"
                    ? `Previous Net Sales: ${formatWholeNumber(
                      Math.abs(row.previousNetSales)
                    )}`
                    : "Net sales change vs previous period"
                }
              >
                {isPositive ? "▲" : "▼"} {formatPercentageNumber(Math.abs(delta))}%
              </span>
            ) : null}
          </div>
        );
      },
    },
    ...(hideSkuColumn
      ? []
      : [
        {
          key: "sku",
          header: "SKU",
          width: isNetSalesGrowthTable ? "15%" : "18%",
          cellClassName: "!text-left",
          render: (row: SkuPerformanceTableRow) => row.sku || "-",
        },
      ]),
    {
      key: "units",
      header: "Units",
      width: isNetSalesGrowthTable ? "10%" : "12%",
      render: (row) => formatWholeNumber(row.units),
    },
    {
      key: "netSales",
      header: "Net sales",
      width: isNetSalesGrowthTable ? "15%" : "17.5%",
      render: (row) => formatMoney(row.netSales, currencySymbol),
    },
    ...(isNetSalesGrowthTable
      ? [
        {
          key: "netSalesIncrease",
          header: "Net sales increase",
          width: "15%",
          render: (row: SkuPerformanceTableRow) => {
            const increase = Number(row.netSalesIncrease);
            const hasIncrease = Number.isFinite(increase);

            return hasIncrease ? (
              <span className={increase < 0 ? "text-[#FF5C5C]" : "text-charcoal-500"}>
                {formatMoney(increase, currencySymbol)}
              </span>
            ) : (
              "-"
            );
          },
        },
        {
          key: "secondaryValue",
          header: metricHeading,
          width: "15%",
          render: renderProfitMetric,
        },
        // {
        //   key: "netSalesDeltaPercentage",
        //   header: "Net Sales Growth (%)",
        //   width: "15%",
        //   render: (row: SkuPerformanceTableRow) => {
        //     const growth = Number(row.netSalesDeltaPercentage);
        //     const hasGrowth = Number.isFinite(growth);
        //     const isPositive = growth >= 0;

        //     return hasGrowth ? (
        //       <span
        //         className={`whitespace-nowrap text-xs font-semibold ${
        //           isPositive ? "text-[#5EA68E]" : "text-[#FF5C5C]"
        //         }`}
        //       >
        //         {isPositive ? "▲" : "▼"} {formatPercentageNumber(Math.abs(growth))}%
        //       </span>
        //     ) : (
        //       "-"
        //     );
        //   },
        // },
      ]
      : [
        {
          key: "secondaryValue",
          header: metricHeading,
          width: "17.5%",
          render: renderProfitMetric,
        },
      ]),
  ];

  return (
    <DataTable<SkuPerformanceTableRow>
      columns={columns}
      data={tableRows}
      paginate={false}
      scrollY={false}
      stickyHeader={false}
      emptyMessage={emptyMessage}
      className="!overflow-hidden !border-gray-300 !shadow-none"
      rowClassName={(_row, index) =>
        index % 2 === 0 ? "bg-white" : "bg-gray-50"
      }
      tableClassName="w-full table-fixed !text-[12px] !text-[#414042] lg:!text-[12px] min-[1700px]:!text-[14px] [&_th]:!border-gray-300 [&_th]:!bg-[#5EA68E] [&_th]:!px-3 [&_th]:!py-3 [&_th]:!text-[#f8edcf] [&_th]:whitespace-normal [&_th]:leading-tight [&_td]:!border-gray-200 [&_td]:!px-3 [&_td]:!py-3"
    />
  );
};


const formatPeriodLabel = (label: string) => {
  // Monthly: "August 2026" -> "Aug'26"
  const monthMatch = label.match(
    /^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})$/i
  );

  if (monthMatch) {
    const month = monthMatch[1].slice(0, 3);
    const year = monthMatch[2].slice(-2);

    return `${month}'${year}`;
  }

  // Quarterly: "Q3 2026" -> "Q3'26"
  const quarterMatch = label.match(/^Q([1-4])\s+(\d{4})$/i);

  if (quarterMatch) {
    return `Q${quarterMatch[1]}'${quarterMatch[2].slice(-2)}`;
  }

  return label;
};

export default function PnlSummaryOverview({
  data,
  onNavigate,
  hideSkuColumns = false,
}: Props) {
  const [activeSlide, setActiveSlide] = useState(0);
  const { currencySymbol, financial } = data;

  const slides = useMemo(
    () => [
      {
        key: "ai",
        label: "AI Insights",
        eyebrow: "Executive intelligence",
        title: "Your business story for this period",
        description: "The most important context and recommended next move.",
        icon: Bot,
        tone: "violet",
        destination: "businessSummary" as SummaryDestination,
      },
      {
        key: "finance",
        label: "Finance Dashboard",
        eyebrow: "Financial performance",
        title: "Profitability at a glance",
        description: "Track sales, profitability, expenses, and cash flow for the selected period.",
        icon: CircleDollarSign,
        tone: "sky",
        destination: "graphs" as SummaryDestination,
      },
      {
        key: "heroSkus",
        label: "Hero SKUs",
        eyebrow: "Leading products",
        title: "Your hero SKUs",
        description: `The products generating the strongest ${data.pnl.profitMetric} profit.`,
        icon: TrendingUp,
        tone: "emerald",
        destination: "skuBreakdown" as SummaryDestination,
      },
      {
        key: "leastSkus",
        label: "Least Performing SKUs",
        eyebrow: "Products to review",
        title: "Least performing SKUs",
        description: `The products with the weakest ${data.pnl.profitMetric} contribution.`,
        icon: TrendingDown,
        tone: "rose",
        destination: "skuBreakdown" as SummaryDestination,
      },
      {
        key: "netSalesGrowth",
        label: "Top Net Sales Growth",
        eyebrow: "Period-over-period growth",
        title: "SKUs gaining the most sales",
        description: "The strongest increases in net sales versus the past period.",
        icon: TrendingUp,
        tone: "sky",
        destination: "skuBreakdown" as SummaryDestination,
      },
      {
        key: "inventory",
        label: "Inventory Insights",
        eyebrow: "Inventory health",
        title: "Stock health and immediate risk",
        description: "The current balance of healthy and actionable inventory.",
        icon: Boxes,
        tone: "rose",
        destination: "inventoryInsights" as SummaryDestination,
      },
    ],
    [data.pnl.profitMetric]
  );

  useEffect(() => {
    setActiveSlide(0);
  }, [data.periodLabel]);

  const showSlide = (index: number) => {
    setActiveSlide((index + slides.length) % slides.length);
  };

  const slide = slides[activeSlide];
  const SlideIcon = slide.icon;

  const toneClasses: Record<string, { icon: string; badge: string; glow: string }> = {
    violet: { icon: "bg-violet-100 text-violet-600", badge: "border-violet-200 bg-violet-50 text-violet-700", glow: "bg-violet-200/40" },
    sky: { icon: "bg-sky-100 text-sky-600", badge: "border-sky-200 bg-sky-50 text-sky-700", glow: "bg-sky-200/40" },
    emerald: { icon: "bg-emerald-100 text-emerald-600", badge: "border-emerald-200 bg-emerald-50 text-emerald-700", glow: "bg-emerald-200/40" },
    cyan: { icon: "bg-cyan-100 text-cyan-600", badge: "border-cyan-200 bg-cyan-50 text-cyan-700", glow: "bg-cyan-200/40" },
    amber: { icon: "bg-amber-100 text-amber-600", badge: "border-amber-200 bg-amber-50 text-amber-700", glow: "bg-amber-200/40" },
    rose: { icon: "bg-rose-100 text-rose-600", badge: "border-rose-200 bg-rose-50 text-rose-700", glow: "bg-rose-200/40" },
  };

  const tone = toneClasses[slide.tone];
  const isFinanceSlide = slide.key === "finance";
  const isInventorySlide = slide.key === "inventory";
  const isSkuTableSlide =
    slide.key === "heroSkus" ||
    slide.key === "leastSkus" ||
    slide.key === "netSalesGrowth";
  const usesCompactHeader = isFinanceSlide || isSkuTableSlide;

  const renderSlideBody = () => {
    if (slide.key === "ai") {
      return data.ai.loading ? (
        <div className="mx-auto w-full max-w-3xl space-y-4 rounded-xl border border-slate-200 bg-white p-5">
          <SkeletonLine /><SkeletonLine className="w-5/6" /><SkeletonLine className="w-2/3" />
        </div>
      ) : (
        <div className="mx-auto grid w-full max-w-5xl grid-cols-1 gap-3 lg:grid-cols-2 2xl:max-w-none 2xl:gap-4">
          <div className="rounded-xl border border-violet-200 bg-white p-3.5 shadow-sm sm:p-4 2xl:min-h-[132px] 2xl:p-5">
            <div className="flex items-center gap-2 text-xs font-semibold text-violet-700"><Sparkles size={15} /> Period summary</div>
            <p className="mt-2 line-clamp-3 text-sm leading-5 text-charcoal-500">{data.ai.summary || "AI commentary is not available for this period yet."}</p>
          </div>
          <div className="rounded-xl border border-emerald-200 bg-white p-3.5 shadow-sm sm:p-4 2xl:min-h-[132px] 2xl:p-5">
            <div className="flex items-center gap-2 text-xs font-semibold text-emerald-700"><ArrowRight size={15} /> Recommended next move</div>
            <p className="mt-2 line-clamp-3 text-sm leading-5 text-charcoal-500">{data.ai.recommendation || "Review the detailed dashboard to identify the next priority."}</p>
          </div>
        </div>
      );
    }

    if (slide.key === "finance") {
      const buildComparisonRow = (
        previousValue: number | undefined,
        valueText: string,
        delta?: number,
        inverse = false
      ) => {
        const hasPrevious =
          typeof previousValue === "number" && Number.isFinite(previousValue);
        const hasDelta = typeof delta === "number" && Number.isFinite(delta);
        const positive = inverse ? Number(delta) <= 0 : Number(delta) >= 0;
        const deltaSymbol = Number(delta) >= 0 ? "▲" : "▼";

        return [
          {
            label: formatPeriodLabel(data.comparisonLabel),
            valueText: hasPrevious ? valueText : "-",
            deltaText: hasDelta
              ? (
                <span className="inline-flex items-center gap-1">
                  <span aria-hidden="true">{deltaSymbol}</span>
                  {formatPercentageNumber(Math.abs(Number(delta)))}%
                </span>
              )
              : "-",
            deltaClassName: !hasDelta
              ? "text-gray-400"
              : positive
                ? "text-emerald-600"
                : "text-rose-600",
          },
        ];
      };

      return (
        <div className="grid h-full min-h-0 w-full grid-cols-2 grid-rows-3 gap-3 lg:grid-cols-3 lg:grid-rows-2 2xl:gap-4">
          <SummaryMetricCard
            title="Units"
            value={formatWholeNumber(financial.units.value)}
            comparisons={buildComparisonRow(
              financial.units.previousValue,
              formatWholeNumber(financial.units.previousValue ?? 0),
              financial.units.delta
            )}
            className="h-full border border-[#FDD36F] border-t-4 border-t-[#FDD36F] bg-white"
          />

          <SummaryMetricCard
            title="Net Sales"
            value={formatMoneyWithPerUnit(
              financial.netSales.value,
              financial.netSales.perUnit,
              currencySymbol
            )}
            comparisons={buildComparisonRow(
              financial.netSales.previousValue,
              formatMoneyWithPerUnitText(
                financial.netSales.previousValue ?? 0,
                financial.netSales.previousPerUnit,
                currencySymbol
              ),
              financial.netSales.delta
            )}
            className="border border-[#75BBDA] border-t-4 border-t-[#75BBDA] bg-white"
          />



          <SummaryMetricCard
            // title={financial.marketplaceFees.label ?? "Marketplace fees"}
            title="Marketplace Fees"
            value={formatMoneyWithPerUnit(
              financial.marketplaceFees.value,
              financial.marketplaceFees.perUnit,
              currencySymbol
            )}
            comparisons={buildComparisonRow(
              financial.marketplaceFees.previousValue,
              formatMoneyWithPerUnitText(
                financial.marketplaceFees.previousValue ?? 0,
                financial.marketplaceFees.previousPerUnit,
                currencySymbol
              ),
              financial.marketplaceFees.delta,
              true
            )}
            className="border border-[#B75A5A] border-t-4 border-t-[#B75A5A] bg-white"
          />

          <SummaryMetricCard
            title="TACoS"
            value={formatPercent(financial.tacos.value)}
            comparisons={buildComparisonRow(
              financial.tacos.previousValue,
              formatPercent(financial.tacos.previousValue ?? 0),
              financial.tacos.delta,
              true
            )}
            className="border border-[#3A8EA4] border-t-4 border-t-[#3A8EA4] bg-white"
          />

          <SummaryMetricCard
            title="CM2 Profit"
            value={formatMoneyWithPercentage(
              financial.cm2Profit.value,
              financial.cm2Profit.percentage,
              currencySymbol
            )}
            comparisons={buildComparisonRow(
              financial.cm2Profit.previousValue,
              formatMoneyWithPercentageText(
                financial.cm2Profit.previousValue ?? 0,
                financial.cm2Profit.previousPercentage,
                currencySymbol
              ),
              financial.cm2Profit.delta
            )}
            className="border border-[#B8C78C] border-t-4 border-t-[#B8C78C] bg-white"
          />

          {financial.target ? (
            <SummaryMetricCard
              title="Target"
              value={
                financial.target.loading ? (
                  <SkeletonLine className="w-24" />
                ) : (
                  formatMoney(financial.target.value, currencySymbol)
                )
              }
              comparisons={buildComparisonRow(
                financial.target.loading
                  ? undefined
                  : financial.target.previousValue,
                formatMoney(
                  financial.target.previousValue ?? 0,
                  currencySymbol
                ),
                financial.target.delta
              )}
              className="border border-[#7B9A6D] border-t-4 border-t-[#7B9A6D] bg-white"
            />
          ) : (
            <SummaryMetricCard
              title="Cash generated"
              value={
                data.cashFlow.loading ? (
                  <SkeletonLine className="w-24" />
                ) : (
                  formatMoney(data.cashFlow.cashGenerated, currencySymbol)
                )
              }
              comparisons={buildComparisonRow(
                data.cashFlow.loading
                  ? undefined
                  : data.cashFlow.previousCashGenerated,
                formatMoney(
                  data.cashFlow.previousCashGenerated ?? 0,
                  currencySymbol
                ),
                data.cashFlow.delta
              )}
              className="border border-[#7B9A6D] border-t-4 border-t-[#7B9A6D] bg-white"
            />
          )}
        </div>
      );
    }

    if (slide.key === "heroSkus") {
      return (
        <SkuPerformanceTable
          rows={data.pnl.heroSkus}
          metricHeading={`${data.pnl.profitMetric} profit`}
          currencySymbol={currencySymbol}
          hideSkuColumn={hideSkuColumns}
        />
      );
    }

    if (slide.key === "leastSkus") {
      return (
        <SkuPerformanceTable
          rows={data.pnl.leastPerformingSkus}
          metricHeading={`${data.pnl.profitMetric} profit`}
          currencySymbol={currencySymbol}
          hideSkuColumn={hideSkuColumns}
        />
      );
    }

    if (slide.key === "netSalesGrowth") {
      return (
        <SkuPerformanceTable
          rows={data.pnl.topNetSalesGrowthSkus}
          metricHeading={`${data.pnl.profitMetric} profit`}
          currencySymbol={currencySymbol}
          emptyMessage="No SKUs with positive net sales growth are available for this period."
          variant="netSalesGrowth"
          hideSkuColumn={hideSkuColumns}
        />
      );
    }

    if (data.inventory.loading) {
      return <InventoryInsightsGrid inventory={data.inventory} loading />;
    }

    if (data.inventory.unavailable) {
      return (
        <div className="mx-auto flex w-full max-w-2xl items-center gap-3 rounded-xl border border-amber-200 bg-amber-50 p-5 text-sm text-amber-800">
          <TriangleAlert size={20} /> Inventory data is not available for this selection.
        </div>
      );
    }

    return <InventoryInsightsGrid inventory={data.inventory} />;
  };

  return (
    <section id="summary" className="flex h-[calc(100dvh-260px)] min-h-[440px] max-h-[620px] flex-col overflow-hidden rounded-2xl border border-[#CFE8DF] bg-white shadow-sm 2xl:h-[560px] 2xl:min-h-[560px] 2xl:max-h-[560px]">
      <div className="flex min-h-[42px] items-center justify-between gap-3 border-b border-slate-100 px-4 py-2 sm:px-6 2xl:min-h-[48px] 2xl:px-8">
        <div className="flex min-w-0 items-center gap-2">
          <PageBreadcrumb
            pageTitle={
              <>
                Performance overview · {formatPeriodLabel(data.periodLabel)}
              </>
            }
            textSize="sm"
            variant="table"
            align="left"
          />
        </div>

        <div className="flex shrink-0 items-center gap-1" aria-label="Manual slide controls">
          <button
            type="button"
            onClick={() => showSlide(activeSlide - 1)}
            aria-label="Show previous overview slide"
            title="Previous slide"
            className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-slate-200 bg-white text-slate-600 transition hover:border-[#5EA68E] hover:text-[#3f806d] focus:outline-none focus-visible:ring-2 focus-visible:ring-[#5EA68E] focus-visible:ring-offset-1"
          >
            <ChevronLeft size={16} />
          </button>
          <button
            type="button"
            onClick={() => showSlide(activeSlide + 1)}
            aria-label="Show next overview slide"
            title="Next slide"
            className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-slate-200 bg-white text-slate-600 transition hover:border-[#5EA68E] hover:text-[#3f806d] focus:outline-none focus-visible:ring-2 focus-visible:ring-[#5EA68E] focus-visible:ring-offset-1"
          >
            <ChevronRight size={16} />
          </button>
        </div>
      </div>

      <nav
        aria-label="Summary sections"
        className="grid grid-cols-6 gap-1.5 px-4 pt-2 sm:px-6 2xl:gap-2.5 2xl:px-8 2xl:pt-3"
      >
        {slides.map((item, index) => (
          <button
            key={item.key}
            type="button"
            onClick={() => showSlide(index)}
            aria-label={`Show ${item.label} summary`}
            aria-current={index === activeSlide ? "step" : undefined}
            title={`Show ${item.label} summary`}
            className="group min-w-0 rounded-md px-0.5 pb-1 focus:outline-none focus-visible:ring-2 focus-visible:ring-[#5EA68E] focus-visible:ring-offset-1"
          >
            <div className="h-1 overflow-hidden rounded-full bg-slate-100">
              {index === activeSlide ? (
                <div className="h-full w-full bg-[#5EA68E] transition-colors" />
              ) : (
                <div className="h-full w-0 bg-[#5EA68E] group-hover:w-full group-hover:bg-[#5EA68E]/35" />
              )}
            </div>
            <p className={`mt-1 hidden truncate text-center text-[9px] font-medium transition-colors md:block 2xl:text-[11px] ${index === activeSlide ? "text-[#3f806d]" : "text-slate-400 group-hover:text-slate-600"}`}>{item.label}</p>
          </button>
        ))}
      </nav>

      <div className="relative flex min-h-0 flex-1 overflow-hidden px-4 py-2 sm:px-6 sm:py-3 2xl:px-8 2xl:pb-5 2xl:pt-3">
        <AnimatePresence mode="wait">
          <motion.div
            key={slide.key}
            initial={{ opacity: 0, y: 14 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            transition={{ duration: 0.45, ease: "easeOut" }}
            className="relative flex h-full w-full flex-col items-stretch justify-start overflow-hidden rounded-2xl border border-slate-100 bg-gradient-to-br from-slate-50/60 via-white to-[#f2f8f6] px-4 py-3 sm:px-6 sm:py-4 2xl:px-8"

          >
            {/* <div className={`pointer-events-none absolute -right-16 -top-16 h-52 w-52 rounded-full blur-3xl ${tone.glow}`} /> */}

            {/* Compact horizontal slide header */}
            <div className="relative z-10 mx-auto mb-3 flex w-full max-w-[1480px] items-center gap-3 border-b border-slate-200/70 pb-3">

              {/* Icon */}
              {/* <div
                className={`flex h-9 w-9 shrink-0 items-center justify-center rounded-lg ${tone.icon}`}
              >
                <SlideIcon size={18} strokeWidth={2.2} />
              </div> */}

              {/* Title and description */}
              <div className="min-w-0 flex-1">
                <h2 className="text-base font-semibold leading-6 text-charcoal-500 sm:text-lg">
                  {slide.title}
                </h2>

                <p className="mt-0.5 text-[11px] leading-4 text-slate-500 2xl:text-xs">
                  {slide.description}
                </p>
              </div>

              {/* Section label */}
              {/* <span
                className={`hidden shrink-0 rounded-md border px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wide sm:inline-flex ${tone.badge}`}
              >
                {slide.eyebrow}
              </span> */}
            </div>

            <div
              className={`relative z-10 w-full min-h-0 2xl:mx-auto 2xl:max-w-[1480px] ${isFinanceSlide || isInventorySlide ? "flex-1" : ""
                }`}
            >
              {renderSlideBody()}
            </div>
          </motion.div>
        </AnimatePresence>
      </div>

      <div className="flex shrink-0 justify-center border-t border-slate-100 bg-white px-4 py-3">
        <button
          type="button"
          onClick={() => onNavigate(slide.destination)}
          className="group inline-flex items-center justify-center gap-3 rounded-xl bg-[#37455F] px-6 py-2.5 text-sm font-semibold text-[#F8EDCE] shadow-[0_6px_14px_rgba(55,69,95,0.16)] transition-all duration-200 hover:-translate-y-0.5 hover:bg-[#2F3B52] focus:outline-none focus:ring-2 focus:ring-[#5EA68E] focus:ring-offset-2"
        >
          Explore detailed dashboard
          <ArrowRight size={17} className="transition-transform duration-200 group-hover:translate-x-0.5" />
        </button>
      </div>
    </section>
  );
}
