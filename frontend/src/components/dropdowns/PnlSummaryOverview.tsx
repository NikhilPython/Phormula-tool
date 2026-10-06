"use client";

import React, { useEffect, useMemo, useState } from "react";
import {
  ArrowRight,
  Bot,
  Boxes,
  ChevronLeft,
  ChevronRight,
  CircleDollarSign,
  PackageSearch,
  Sparkles,
  TrendingDown,
  TrendingUp,
  TriangleAlert,
  WalletCards,
} from "lucide-react";
import { AnimatePresence, motion } from "framer-motion";
import PageBreadcrumb from "../common/PageBreadCrumb";

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
};

export type PnlSummaryOverviewData = {
  periodLabel: string;
  currencySymbol: string;
  financial: {
    netSales: DeltaMetric;
    cm2Profit: DeltaMetric;
    tacos: DeltaMetric;
    units: DeltaMetric;
    cm2Margin: number;
  };
  ai: {
    summary: string;
    recommendation: string;
    loading?: boolean;
  };
  pnl: {
    topProduct: string;
    topProfit: number;
    bottomProduct: string;
    bottomProfit: number;
    productCount: number;
  };
  cashFlow: {
    cashPosition: number;
    targetSales: number;
    shortfall: number;
    loading?: boolean;
  };
  sku: {
    topProduct: string;
    netSales: number;
    units: number;
    productCount: number;
  };
  inventory: {
    totalUnits: number;
    healthySkus: number;
    highAlertSkus: number;
    liquidateSkus: number;
    loading?: boolean;
    unavailable?: boolean;
  };
};

type Props = {
  data: PnlSummaryOverviewData;
  onNavigate: (tab: SummaryDestination) => void;
};

const compactNumber = (value: number, maximumFractionDigits = 1) =>
  new Intl.NumberFormat("en", {
    notation: Math.abs(value) >= 10_000 ? "compact" : "standard",
    maximumFractionDigits,
  }).format(Number(value || 0));

const formatMoney = (value: number, currencySymbol: string) => {
  const sign = value < 0 ? "-" : "";
  return `${sign}${currencySymbol}${compactNumber(Math.abs(value))}`;
};

const formatPercent = (value: number) =>
  `${Number(value || 0).toLocaleString(undefined, {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  })}%`;

const Delta = ({ value, inverse = false }: { value?: number; inverse?: boolean }) => {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return <span className="text-slate-400">No comparison</span>;
  }

  const positive = inverse ? value <= 0 : value >= 0;
  const Icon = value >= 0 ? TrendingUp : TrendingDown;

  return (
    <span className={`inline-flex items-center gap-1 font-semibold ${positive ? "text-emerald-600" : "text-rose-600"}`}>
      <Icon size={13} strokeWidth={2.5} />
      {Math.abs(value).toFixed(1)}%
    </span>
  );
};

const SkeletonLine = ({ className = "" }: { className?: string }) => (
  <div className={`h-3 animate-pulse rounded-full bg-slate-200 ${className}`} />
);

const MetricTile = ({
  label,
  value,
  detail,
  trailing,
  accent = "border-[#5EA68E] ",
}: {
  label: string;
  value: React.ReactNode;
  detail?: React.ReactNode;
  trailing?: React.ReactNode;
  accent?: string;
}) => (
  <div
    className={`relative flex h-full min-h-[108px] flex-col justify-center overflow-hidden rounded-xl border border-t-4 bg-white p-3.5 shadow-sm sm:p-4 2xl:min-h-[132px] 2xl:p-5 ${accent}`}
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
        key: "pnl",
        label: "P&L Breakdown",
        eyebrow: "Product profitability",
        title: "Where profit was made and lost",
        description: "The strongest and weakest product contribution.",
        icon: TrendingUp,
        tone: "emerald",
        destination: "skuBreakdown" as SummaryDestination,
      },
      {
        key: "cash",
        label: "Cash Flow",
        eyebrow: "Cash and targets",
        title: "Cash position versus plan",
        description: "A compact view of liquidity, targets, and shortfall.",
        icon: WalletCards,
        tone: "cyan",
        destination: "cashFlow" as SummaryDestination,
      },
      {
        key: "sku",
        label: "SKU Journey",
        eyebrow: "SKU performance",
        title: "Your leading product journey",
        description: "The product creating the most sales momentum.",
        icon: PackageSearch,
        tone: "amber",
        destination: "skuwiseProfit" as SummaryDestination,
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
      return (
        <div className="grid w-full grid-cols-2 gap-3 lg:grid-cols-4 2xl:gap-4">
          <MetricTile
            label="Units sold"
            value={compactNumber(financial.units.value, 0)}
            trailing={<Delta value={financial.units.delta} />}
            accent="border-[#FDD36F]"
          />

          <MetricTile
            label="Net sales"
            value={formatMoney(financial.netSales.value, currencySymbol)}
            trailing={<Delta value={financial.netSales.delta} />}
            accent="border-[#75BBDA]"
          />

          <MetricTile
            label="CM2 profit"
            value={
              <div className="flex items-end gap-1">
                <span>{formatMoney(financial.cm2Profit.value, currencySymbol)}</span>

                <span className="text-[10px] font-medium text-slate-500 sm:text-xs mb-0.5">
                  {formatPercent(financial.cm2Margin)}
                </span>
              </div>
            }
            trailing={<Delta value={financial.cm2Profit.delta} />}
            accent="border-[#B8C78C]"
          />

          <MetricTile
            label="TACoS"
            value={formatPercent(financial.tacos.value)}
            trailing={<Delta value={financial.tacos.delta} inverse />}
            accent="border-[#3A8EA4]"
          />


        </div>
      );
    }

    if (slide.key === "pnl") {
      return (
        <div className="grid w-full grid-cols-1 gap-3 sm:grid-cols-3 2xl:gap-4">
          <MetricTile
            label="Top profit contributor"
            value={formatMoney(data.pnl.topProfit, currencySymbol)}
            detail={data.pnl.topProduct || "No product ranking available"}
            accent="border-green-500"
          />

          <MetricTile
            label="Weakest contributor"
            value={formatMoney(data.pnl.bottomProfit, currencySymbol)}
            detail={data.pnl.bottomProduct || "No product ranking available"}
            accent="border-[#B75A5A]"
          />

          <MetricTile
            label="Products reviewed"
            value={compactNumber(data.pnl.productCount, 0)}
            detail="Ranked by period profit contribution"
            accent="border-[#EDA052]"
          />
        </div>
      );
    }

    if (slide.key === "cash") {
      return data.cashFlow.loading ? (
        <div className="w-full space-y-3 rounded-xl border border-slate-200 bg-white p-5">
          <SkeletonLine />
          <SkeletonLine className="w-3/4" />
        </div>
      ) : (
        <div className="grid w-full grid-cols-1 gap-3 sm:grid-cols-3 2xl:gap-4">
          <MetricTile
            label="Cash position"
            value={formatMoney(data.cashFlow.cashPosition, currencySymbol)}
            detail={
              data.cashFlow.cashPosition >= 0
                ? "Positive cash position"
                : "Negative cash position"
            }
            accent="border-[#B8C78C]"
          />

          <MetricTile
            label="Sales target"
            value={formatMoney(data.cashFlow.targetSales, currencySymbol)}
            detail={`Selected ${data.periodLabel.toLowerCase()} target`}
            accent="border-[#75BBDA]"
          />

          <MetricTile
            label="Target shortfall"
            value={formatMoney(data.cashFlow.shortfall, currencySymbol)}
            detail={
              data.cashFlow.shortfall > 0
                ? "Remaining to reach target"
                : "Target achieved"
            }
            accent={
              data.cashFlow.shortfall > 0
                ? "border-[#B75A5A]"
                : "border-green-500"
            }
          />
        </div>
      );
    }

    if (slide.key === "sku") {
      return (
        <div className="mx-auto grid w-full max-w-5xl grid-cols-1 gap-3 sm:grid-cols-3 2xl:max-w-none 2xl:gap-4">
          <MetricTile
            label="Leading product"
            value={
              <span className="line-clamp-1 text-base sm:text-lg">
                {data.sku.topProduct || "No product available"}
              </span>
            }
            detail="Highest net-sales product"
            accent="border-[#FDD36F]"
          />

          <MetricTile
            label="Product net sales"
            value={formatMoney(data.sku.netSales, currencySymbol)}
            detail={`${compactNumber(data.sku.units, 0)} units sold`}
            accent="border-[#75BBDA]"
          />

          <MetricTile
            label="SKUs reviewed"
            value={compactNumber(data.sku.productCount, 0)}
            detail="Included in the selected period"
            accent="border-[#3A8EA4]"
          />
        </div>
      );
    }

    if (data.inventory.loading) {
      return (
        <div className="grid w-full grid-cols-2 gap-3 lg:grid-cols-4 2xl:gap-4">
          <MetricTile
            label="Total inventory"
            value={`${compactNumber(data.inventory.totalUnits, 0)} units`}
            detail="Inventory included in this view"
            accent="border-sky-400"
          />

          <MetricTile
            label="Healthy"
            value={`${compactNumber(data.inventory.healthySkus, 0)} SKUs`}
            detail="Within a healthy stock range"
            accent="border-emerald-400"
          />

          <MetricTile
            label="High alert"
            value={`${compactNumber(data.inventory.highAlertSkus, 0)} SKUs`}
            detail="Need near-term attention"
            accent="border-rose-400"
          />

          <MetricTile
            label="Liquidate"
            value={`${compactNumber(data.inventory.liquidateSkus, 0)} SKUs`}
            detail="Aged inventory to action"
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
          value={`${compactNumber(data.inventory.totalUnits, 0)} units`}
          detail="Inventory included in this view"
          accent="border-[#75BBDA]"
        />

        <MetricTile
          label="Healthy"
          value={`${compactNumber(data.inventory.healthySkus, 0)} SKUs`}
          detail="Within a healthy stock range"
          accent="border-[#7B9A6D]"
        />

        <MetricTile
          label="High alert"
          value={`${compactNumber(data.inventory.highAlertSkus, 0)} SKUs`}
          detail="Need near-term attention"
          accent="border-[#B75A5A]"
        />

        <MetricTile
          label="Liquidate"
          value={`${compactNumber(data.inventory.liquidateSkus, 0)} SKUs`}
          detail="Aged inventory to action"
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
            className={`relative flex h-full w-full flex-col items-center overflow-hidden rounded-2xl border border-slate-100 bg-gradient-to-br from-slate-50/60 via-white to-[#f2f8f6] p-4 2xl:px-8 2xl:py-6 ${slide.key === "ai" ? "justify-start sm:p-4" : "justify-center sm:p-6"}`}
          >
            {/* <div className={`pointer-events-none absolute -right-16 -top-16 h-52 w-52 rounded-full blur-3xl ${tone.glow}`} /> */}
            <div className={`relative z-10 flex max-w-3xl flex-col items-center text-center ${slide.key === "ai" ? "mb-2" : "mb-4"}`}>
              <div
                className={`flex h-10 w-10 items-center justify-center rounded-xl 2xl:h-12 2xl:w-12 ${tone.icon}`}
              >
                <SlideIcon size={20} />
              </div>

              <span
                className={`mt-2 rounded-full border px-2.5 py-1 text-[9px] font-semibold uppercase tracking-[0.12em] ${tone.badge}`}
              >
                {slide.eyebrow}
              </span>
              <h2 className={`${slide.key === "ai" ? "mt-1.5" : "mt-2"} text-xl font-bold leading-tight text-charcoal-500 sm:text-2xl 2xl:text-[28px]`}>{slide.title}</h2>
              <p className="mt-1 text-[10px] text-slate-500 sm:text-xs 2xl:mt-2 2xl:max-w-sm 2xl:text-sm 2xl:leading-5">{slide.description}</p>
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
