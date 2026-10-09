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

export type SummaryDestination =
  | "graphs"
  | "skuBreakdown"
  | "cashFlow"
  | "skuwiseProfit"
  | "businessSummary"
  | "inventoryInsights";

type DeltaMetric = {
  value: number;
  delta?: number;
  previousValue?: number;
  perUnit?: number;
  previousPerUnit?: number;
  percentage?: number;
  previousPercentage?: number;
};

type SkuTableMetric = {
  productName: string;
  sku: string;
  netSales: number;
  secondaryValue: number;
  netSalesDeltaPercentage?: number;
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
  };
  ai: {
    summary: string;
    recommendation: string;
    loading?: boolean;
  };
  pnl: {
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
    healthySkus: number;
    healthyUnits: number;
    highAlertSkus: number;
    highAlertUnits: number;
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


const roundFormattedValue = (value: string) =>
  value.replace(/-?\d[\d,]*(?:\.\d+)?/, (match) => {
    const numericValue = Number(match.replace(/,/g, ""));
    return Number.isFinite(numericValue) ? formatWholeNumber(numericValue) : match;
  });

const Delta = ({ value, inverse = false }: { value?: number; inverse?: boolean }) => {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return <span className="text-slate-400">No comparison</span>;
  }

  const positive = inverse ? value <= 0 : value >= 0;
  const Icon = value >= 0 ? TrendingUp : TrendingDown;

  return (
    <span className={`inline-flex items-center gap-1 font-semibold ${positive ? "text-emerald-600" : "text-rose-600"}`}>
      <Icon size={13} strokeWidth={2.5} />
      {formatPercentageNumber(Math.abs(value))}%
    </span>
  );
};

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

  const DeltaIcon = deltaPercentage <= 0 ? TrendingDown : TrendingUp;

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
      <DeltaIcon size={13} strokeWidth={2.5} />
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
    className={`relative flex h-full flex-col justify-center overflow-hidden rounded-xl border border-t-4 bg-white p-3.5 shadow-sm sm:p-4 ${compact
      ? "min-h-[88px] 2xl:min-h-[88px] 2xl:p-3.5"
      : "min-h-[108px] 2xl:min-h-[132px] 2xl:p-5"
      } ${accent}`}
  >
    <p className="text-[10px] font-medium uppercase tracking-wide text-slate-500 2xl:text-[11px]">
      {label}
    </p>

    <div className="mt-2 flex items-center justify-between gap-3">
      <div className="min-w-0 text-lg font-bold text-charcoal-500 sm:text-xl 2xl:text-2xl">
        {value}
      </div>
      {trailing ? (
        <div className="shrink-0 text-[10px] 2xl:text-xs">
          {trailing}
        </div>
      ) : null}
    </div>

    {detail ? (
      <div className="mt-1.5 text-[10px] leading-relaxed text-slate-500 2xl:mt-2 2xl:text-xs">
        {detail}
      </div>
    ) : null}
  </div>
);

const SkuPerformanceTable = ({
  rows,
  productHeading,
  metricHeading,
  metricType,
  currencySymbol,
  tone,
  emptyMessage = "No SKU performance data is available for this period.",
}: {
  rows: SkuTableMetric[];
  productHeading: string;
  metricHeading: string;
  metricType: "money" | "percentage";
  currencySymbol: string;
  tone: "positive" | "negative";
  emptyMessage?: string;
}) => {
  const badgeClass =
    tone === "positive"
      ? "bg-emerald-50 text-emerald-700"
      : "bg-rose-50 text-rose-700";

  return (
    <div className="mx-auto w-full max-w-6xl overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm 2xl:max-w-[1320px]">
      <div className="grid grid-cols-4 items-center gap-2 border-b border-slate-200 bg-slate-50 px-4 py-1.5 text-[10px] font-semibold uppercase tracking-wide text-slate-500 md:grid-cols-[minmax(0,1.5fr)_minmax(90px,0.65fr)_minmax(90px,0.65fr)_minmax(90px,0.65fr)] 2xl:gap-3 2xl:px-5 2xl:text-xs">
        <span>{productHeading}</span>
        <span className="text-right">Net sales</span>
        <span className="text-right">{metricHeading}</span>
        <span className="text-right">Net Sales Growth (%)</span>
      </div>

      {rows.length ? (
        rows.map((row, index) => (
          <div
            key={`${row.sku}-${row.productName}-${index}`}
            className="grid grid-cols-4 items-center gap-2 border-b border-slate-100 px-4 py-1 last:border-b-0 md:grid-cols-[minmax(0,1.5fr)_minmax(90px,0.65fr)_minmax(90px,0.65fr)_minmax(90px,0.65fr)] 2xl:gap-3 2xl:px-5"
          >
            <div className="flex min-w-0 items-center gap-2.5">
              <span className={`flex h-6 w-6 shrink-0 items-center justify-center rounded-md text-[10px] font-bold 2xl:h-7 2xl:w-7 2xl:text-xs ${badgeClass}`}>
                {index + 1}
              </span>
              <div className="min-w-0">
                <p className="truncate text-xs font-semibold text-charcoal-500 2xl:text-sm" title={row.productName}>
                  {row.productName || "Unnamed product"}
                </p>
                {row.sku ? (
                  <p className="truncate text-[9px] leading-3 text-slate-400 2xl:text-[11px]" title={row.sku}>
                    {row.sku}
                  </p>
                ) : null}
              </div>
            </div>
            <span className="text-right text-xs font-semibold text-charcoal-500 2xl:text-sm">
              {formatMoney(row.netSales, currencySymbol)}
            </span>
            <span
              className={`text-right text-xs font-semibold 2xl:text-sm ${metricType === "percentage"
                ? "inline-flex items-center justify-end gap-1 text-emerald-600"
                : row.secondaryValue < 0
                  ? "text-rose-600"
                  : "text-charcoal-500"
                }`}
            >
              {metricType === "percentage" ? (
                <>
                  <TrendingUp size={13} strokeWidth={2.5} />
                  {formatPercent(row.secondaryValue)}
                </>
              ) : (
                formatMoney(row.secondaryValue, currencySymbol)
              )}
            </span>
            <span className="flex justify-end text-right text-[10px] 2xl:text-xs">
              <Delta value={row.netSalesDeltaPercentage} />
            </span>
          </div>
        ))
      ) : (
        <div className="px-4 py-8 text-center text-sm text-slate-500">
          {emptyMessage}
        </div>
      )}
    </div>
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

export default function PnlSummaryOverview({ data, onNavigate }: Props) {
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
        description: "The headline measures driving this period's result.",
        icon: CircleDollarSign,
        tone: "sky",
        destination: "graphs" as SummaryDestination,
      },
      {
        key: "heroSkus",
        label: "Hero SKUs",
        eyebrow: "Leading products",
        title: "Your hero SKUs",
        description: "The products generating the strongest CM1 profit.",
        icon: TrendingUp,
        tone: "emerald",
        destination: "skuBreakdown" as SummaryDestination,
      },
      {
        key: "leastSkus",
        label: "Least Performing SKUs",
        eyebrow: "Products to review",
        title: "Least performing SKUs",
        description: "The products with the weakest CM1 contribution.",
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
    []
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

      const financeValue = (
        value: React.ReactNode,
        delta?: number,
        inverse = false
      ) => (
        <div className="flex w-full items-baseline justify-between gap-2">
          <div className="min-w-0">
            {value}
          </div>

          <div className="shrink-0 whitespace-nowrap text-[9px] leading-none 2xl:text-[10px]">
            <Delta value={delta} inverse={inverse} />
          </div>
        </div>
      );


      return (
        <div className="grid w-full grid-cols-2 gap-2 lg:grid-cols-6 2xl:gap-3">
          <SummaryMetricCard
            title="Units sold"
            value={financeValue(
              formatWholeNumber(financial.units.value),
              financial.units.delta
            )}
            className="border border-[#FDD36F] border-t-4 border-t-[#FDD36F] bg-white"
          />

          <SummaryMetricCard
            title="Net sales"
            value={financeValue(
              formatMoneyWithPerUnit(
                financial.netSales.value,
                financial.netSales.perUnit,
                currencySymbol
              ),
              financial.netSales.delta
            )}
            className="border border-[#75BBDA] border-t-4 border-t-[#75BBDA] bg-white"
          />



          <SummaryMetricCard
            title="Marketplace fees"
            value={financeValue(
              formatMoneyWithPerUnit(
                financial.marketplaceFees.value,
                financial.marketplaceFees.perUnit,
                currencySymbol
              ),
              financial.marketplaceFees.delta,
              true
            )}
            className="border border-[#B75A5A] border-t-4 border-t-[#B75A5A] bg-white"
          />

          <SummaryMetricCard
            title="TACoS"
            value={financeValue(
              formatPercent(financial.tacos.value),
              financial.tacos.delta,
              true
            )}
            className="border border-[#3A8EA4] border-t-4 border-t-[#3A8EA4] bg-white"
          />

          <SummaryMetricCard
            title="CM2 profit"
            value={financeValue(
              formatMoneyWithPercentage(
                financial.cm2Profit.value,
                financial.cm2Profit.percentage,
                currencySymbol
              ),
              financial.cm2Profit.delta
            )}
            className="border border-[#B8C78C] border-t-4 border-t-[#B8C78C] bg-white"
          />

          <SummaryMetricCard
            title="Cash generated"
            value={financeValue(
              data.cashFlow.loading ? (
                <SkeletonLine className="w-24" />
              ) : (
                formatMoney(data.cashFlow.cashGenerated, currencySymbol)
              ),
              data.cashFlow.delta
            )}
            className="border border-[#7B9A6D] border-t-4 border-t-[#7B9A6D] bg-white"
          />
        </div>
      );
    }

    if (slide.key === "heroSkus") {
      return (
        <SkuPerformanceTable
          rows={data.pnl.heroSkus}
          productHeading="Leading products"
          metricHeading="CM1 profit"
          metricType="money"
          currencySymbol={currencySymbol}
          tone="positive"
        />
      );
    }

    if (slide.key === "leastSkus") {
      return (
        <SkuPerformanceTable
          rows={data.pnl.leastPerformingSkus}
          productHeading="Least performing SKUs"
          metricHeading="CM1 profit"
          metricType="money"
          currencySymbol={currencySymbol}
          tone="negative"
        />
      );
    }

    if (slide.key === "netSalesGrowth") {
      return (
        <SkuPerformanceTable
          rows={data.pnl.topNetSalesGrowthSkus}
          productHeading="Fastest-growing SKUs"
          metricHeading="Net sales increase"
          metricType="money"
          currencySymbol={currencySymbol}
          tone="positive"
          emptyMessage="No SKUs with positive net sales growth are available for this period."
        />
      );
    }

    if (data.inventory.loading) {
      return (
        <div className="grid w-full grid-cols-2 gap-3 lg:grid-cols-4 2xl:gap-4">
          <MetricTile
            label="Total inventory"
            value={`${formatWholeNumber(data.inventory.totalUnits)} units`}
            detail="Inventory included in this view"
            accent="border-sky-400"
          />

          <MetricTile
            label="Healthy"
            value={`${formatWholeNumber(data.inventory.healthySkus)} SKUs`}
            trailing={
              <span className="text-xs font-semibold text-slate-500">
                {formatWholeNumber(data.inventory.healthyUnits)} units
              </span>
            }
            detail="Within a healthy stock range"
            accent="border-emerald-400"
          />

          <MetricTile
            label="High alert"
            value={`${formatWholeNumber(data.inventory.highAlertSkus)} SKUs`}
            trailing={
              <span className="text-xs font-semibold text-slate-500">
                {formatWholeNumber(data.inventory.highAlertUnits)} units
              </span>
            }
            detail="Need near-term attention"
            accent="border-rose-400"
          />

          <MetricTile
            label="Estimate Storage Cost"
            value={roundFormattedValue(data.inventory.estimatedStorageCost)}
            trailing={
              <StorageCostDelta
                deltaValue={data.inventory.estimatedStorageCostDeltaValue}
                deltaPercentage={data.inventory.estimatedStorageCostDeltaPercentage}
              />
            }
            detail="Monthly storage estimate"
            accent="border-amber-400"
          />
        </div>
      );
    }

    if (data.inventory.unavailable) {
      return (
        <div className="mx-auto flex w-full max-w-2xl items-center gap-3 rounded-xl border border-amber-200 bg-amber-50 p-5 text-sm text-amber-800">
          <TriangleAlert size={20} /> Inventory data is not available for this selection.
        </div>
      );
    }

    return (
      <div className="grid w-full grid-cols-2 gap-3 lg:grid-cols-4 2xl:gap-4">
        <MetricTile
          label="Total inventory"
          value={`${formatWholeNumber(data.inventory.totalUnits)} units`}
          detail="Inventory included in this view"
          accent="border-[#75BBDA]"
        />

        <MetricTile
          label="Healthy"
          value={`${formatWholeNumber(data.inventory.healthySkus)} SKUs`}
          trailing={
            <span className="text-xs font-semibold text-slate-500">
              {formatWholeNumber(data.inventory.healthyUnits)} units
            </span>
          }
          detail="Within a healthy stock range"
          accent="border-[#7B9A6D]"
        />

        <MetricTile
          label="High alert"
          value={`${formatWholeNumber(data.inventory.highAlertSkus)} SKUs`}
          trailing={
            <span className="text-xs font-semibold text-slate-500">
              {formatWholeNumber(data.inventory.highAlertUnits)} units
            </span>
          }
          detail="Need near-term attention"
          accent="border-[#B75A5A]"
        />

        <MetricTile
          label="Estimate Storage Cost"
          value={roundFormattedValue(data.inventory.estimatedStorageCost)}
          trailing={
            <StorageCostDelta
              deltaValue={data.inventory.estimatedStorageCostDeltaValue}
              deltaPercentage={data.inventory.estimatedStorageCostDeltaPercentage}
            />
          }
          detail="Monthly storage estimate"
          accent="border-[#EDA153]"
        />
      </div>
    );
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
            className={`relative flex h-full w-full flex-col items-center overflow-hidden rounded-2xl border border-slate-100 bg-gradient-to-br from-slate-50/60 via-white to-[#f2f8f6] ${slide.key === "ai"
              ? "justify-start p-4 sm:p-4 2xl:px-8 2xl:py-6"
              : isSkuTableSlide
                ? "justify-center px-4 py-2 sm:px-6 sm:py-2 2xl:px-8 2xl:py-2"
                : isFinanceSlide
                  ? "justify-center px-4 py-3 sm:px-6 sm:py-3 2xl:px-8 2xl:py-3"
                  : "justify-center p-4 sm:p-6 2xl:px-8 2xl:py-6"
              }`}
          >
            {/* <div className={`pointer-events-none absolute -right-16 -top-16 h-52 w-52 rounded-full blur-3xl ${tone.glow}`} /> */}
            <div className={`relative z-10 flex max-w-3xl flex-col items-center text-center ${slide.key === "ai" ? "mb-2" : isSkuTableSlide ? "mb-1.5" : usesCompactHeader ? "mb-2.5" : "mb-4"}`}>
              <div
                className={`flex items-center justify-center rounded-xl ${isSkuTableSlide
                  ? "h-8 w-8 2xl:h-8 2xl:w-8"
                  : usesCompactHeader
                    ? "h-8 w-8 2xl:h-9 2xl:w-9"
                    : "h-10 w-10 2xl:h-12 2xl:w-12"
                  } ${tone.icon}`}
              >
                <SlideIcon size={20} />
              </div>

              <span
                className={`${isSkuTableSlide ? "mt-1 py-0.5" : usesCompactHeader ? "mt-1.5 py-0.5" : "mt-2 py-1"} rounded-full border px-2.5 text-[9px] font-semibold uppercase tracking-[0.12em] ${tone.badge}`}
              >
                {slide.eyebrow}
              </span>
              <h2 className={`${slide.key === "ai" ? "mt-1.5" : usesCompactHeader ? "mt-1" : "mt-2"} text-xl font-bold leading-tight text-charcoal-500 ${usesCompactHeader ? "sm:text-xl 2xl:text-2xl" : "sm:text-2xl 2xl:text-[28px]"}`}>{slide.title}</h2>
              <p className={`${isSkuTableSlide ? "mt-0.5 2xl:mt-0 2xl:text-xs 2xl:leading-4" : usesCompactHeader ? "mt-0.5 2xl:mt-1 2xl:text-xs" : "mt-1 2xl:mt-2 2xl:text-sm"} text-[10px] text-slate-500 sm:text-xs 2xl:max-w-sm 2xl:leading-5`}>{slide.description}</p>
            </div>
            <div className="relative z-10 w-full 2xl:mx-auto 2xl:max-w-[1480px]">{renderSlideBody()}</div>
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
