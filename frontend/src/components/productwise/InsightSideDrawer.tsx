"use client";

import React from "react";
import { AnimatePresence, motion } from "framer-motion";
import PageBreadcrumb from "../common/PageBreadCrumb";

export interface BestPerfItem {
  month: string;
  value: number;
}

export interface BestPerformance {
  sales?: BestPerfItem;
  units?: BestPerfItem;
  profit?: BestPerfItem;
}

export interface SkuInsight {
  product_name: string;
  insight: string;
  inventory_recommendation?: string;
  objective?: Record<string, any> | null;
  recommendation?: string;
  best_performance?: BestPerformance;
  product_journey?: string[];
  [key: string]: any;
}

export interface InsightSideDrawerProps {
  open: boolean;
  selectedSku: string | null;
  skuInsights: Record<string, SkuInsight>;
  getInsightByProductName?: (productName: string) => [string, SkuInsight] | null;
  onClose: () => void;

  enableFeedback?: boolean;
  fbType?: "like" | "dislike" | null;
  setFbType?: (v: "like" | "dislike" | null) => void;
  fbText?: string;
  setFbText?: (v: string) => void;
  fbSubmitting?: boolean;
  fbSuccess?: boolean;
  onSubmitFeedback?: () => void;

  countryName?: string;
  drawerPeriodText?: string;
  periodBadge?: string;

  selectedYear?: number | "";
  homeCurrency?: string;
}

const metricColors = [
  "border border-[#FDD36F] border-t-[#FDD36F]",
  "border border-[#75BBDA] border-t-[#75BBDA]",
  "border border-[#B75A5A] border-t-[#B75A5A]",
];

const InsightSideDrawer: React.FC<InsightSideDrawerProps> = ({
  open,
  selectedSku,
  skuInsights,
  getInsightByProductName,
  onClose,
  drawerPeriodText,
  periodBadge,
  selectedYear,
  homeCurrency,
}) => {
  if (!open || !selectedSku) return null;

  const insightData =
    skuInsights[selectedSku as keyof typeof skuInsights] ||
    getInsightByProductName?.(selectedSku)?.[1];

  if (!insightData) return null;

  const formatPerfMonth = (month?: string) => {
    if (!month) return "-";

    const fullNames = [
      "january",
      "february",
      "march",
      "april",
      "may",
      "june",
      "july",
      "august",
      "september",
      "october",
      "november",
      "december",
    ];

    const abbrs = [
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

    const lower = month.toLowerCase();
    let idx = fullNames.indexOf(lower);

    if (idx === -1) {
      idx = fullNames.findIndex((m) => lower.startsWith(m.slice(0, 3)));
    }

    const shortMonth = idx >= 0 ? abbrs[idx] : month;
    const shortYear =
      selectedYear !== undefined && selectedYear !== ""
        ? String(selectedYear).slice(-2)
        : "";

    return shortYear ? `${shortMonth}'${shortYear}` : shortMonth;
  };

  const getCurrencySymbol = (code?: string) => {
    const c = (code || "").toUpperCase();

    if (c === "USD") return "$";
    if (c === "GBP") return "£";
    if (c === "EUR") return "€";
    if (c === "CAD") return "C$";
    if (c === "INR") return "₹";

    return "";
  };

  const formatPerfValue = (label: string, value?: number) => {
    if (typeof value !== "number") return "-";

    const lower = label.toLowerCase();

    if (lower.includes("unit")) {
      return value.toLocaleString();
    }

    return `${getCurrencySymbol(homeCurrency)}${value.toLocaleString()}`;
  };

  const journeyBullets = Array.isArray(insightData.product_journey)
    ? insightData.product_journey
    : [];

  const bestPerformanceCards = [
    {
      label: "Sales",
      data: insightData.best_performance?.sales,
    },
    {
      label: "Units",
      data: insightData.best_performance?.units,
    },
    {
      label: "Profit",
      data: insightData.best_performance?.profit,
    },
  ];

  const objectiveCards = [
    {
      label: "Primary Focus",
      value: insightData.objective?.growth_intent || "balanced",
    },
    {
      label: "Profit Strategy",
      value:
        insightData.objective?.profit_priority?.replaceAll("_", " ") ||
        "protect growth",
    },
    {
      label: "Inventory Dilution",
      value: insightData.objective?.inventory_clearance_priority ? "Yes" : "No",
    },
  ];

  return (
    <AnimatePresence>
      {open && (
        <>
          <motion.div
            className="fixed inset-0 z-[999999] h-full bg-black/40"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
          />

          <motion.aside
            className="fixed right-0 top-0 z-[1000000] h-screen w-[95vw] sm:w-[75vw] md:w-[60vw] lg:w-[50vw] min-[1700px]:w-[50vw] bg-white shadow-2xl"
            initial={{ x: 520 }}
            animate={{ x: 0 }}
            exit={{ x: 520 }}
            transition={{ type: "tween", duration: 0.25 }}
          >
            <div className="flex h-full flex-col gap-4">
              {/* Header - same style as second drawer */}
              <div className="shrink-0 border-b border-slate-200 p-3 flex items-center justify-between gap-3">
                <div>
                  <div className="flex items-center gap-1 flex-wrap">
                    <PageBreadcrumb
                      pageTitle="AI Insight - "
                      variant="page"
                      textSize="2xl"
                    />

                    <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
                      {insightData.product_name || selectedSku}
                    </span>
                  </div>

                  {(drawerPeriodText || periodBadge) && (
                    <div className="mt-1">
                      {drawerPeriodText ? (
                        <span className="text-xs sm:text-sm 2xl:text-sm font-semibold text-[#5EA68E]">
                          {drawerPeriodText}
                        </span>
                      ) : (
                        <span className="inline-flex rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[10px] 2xl:text-xs text-slate-700">
                          {periodBadge}
                        </span>
                      )}
                    </div>
                  )}
                </div>

                <button
                  onClick={onClose}
                  className="rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
                >
                  ✕
                </button>
              </div>

              {/* Content - same spacing/text system as second drawer */}
              <div className="flex-1 overflow-y-auto space-y-6 px-3">
                {/* Best Performance */}
                {insightData.best_performance && (
                  <div>
                    <div className="text-xs sm:text-sm 2xl:text-lg font-semibold text-charcoal-700 mb-2">
                      Best Performance
                    </div>

                    <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                      {bestPerformanceCards.map((card, index) => (
                        <div
                          key={card.label}
                          className={`rounded-lg border border-t-4 ${
                            metricColors[index % metricColors.length]
                          } px-3 py-2`}
                        >
                          <div className="text-[10px] 2xl:text-xs text-charcoal-400">
                            {card.label}
                          </div>

                          <div className="mt-1 text-[10px] 2xl:text-xs text-charcoal-400">
                            {formatPerfMonth(card.data?.month)}
                          </div>

                          <div className="mt-1 text-sm 2xl:text-lg font-bold text-[#414042]">
                            {formatPerfValue(card.label, card.data?.value)}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Objectives */}
                {insightData.objective && (
                  <div>
                    <div className="text-xs sm:text-sm 2xl:text-lg font-semibold text-charcoal-700 mb-2">
                      Objectives
                    </div>

                    <div className="grid grid-cols-1 sm:grid-cols-3 gap-5">
                      {objectiveCards.map((card) => (
                        <div
                          key={card.label}
                          className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2"
                        >
                          <div className="2xl:text-sm text-xs text-[#7A7A7A]">
                            {card.label}
                          </div>

                          <div className="mt-1 2xl:text-sm text-xs font-semibold text-[#0F172A] capitalize">
                            {card.value}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Recommendations */}
                <div>
                  <div className="mb-2 text-xs font-semibold text-charcoal-500 sm:text-sm 2xl:text-lg">
                    Recommendations
                  </div>

                  {!!insightData.recommendation ? (
                    <div>
                      <div className="text-xs 2xl:text-sm font-semibold text-charcoal-500">
                        Action
                      </div>

                      <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
                        <li>{insightData.recommendation}</li>
                      </ul>
                    </div>
                  ) : (
                    <div className="text-xs 2xl:text-sm text-charcoal-500">
                      —
                    </div>
                  )}

                  {!!insightData.inventory_recommendation && (
                    <div className="mt-2">
                      <div className="text-xs 2xl:text-sm font-semibold text-charcoal-500">
                        Inventory
                      </div>

                      <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
                        <li>{insightData.inventory_recommendation}</li>
                      </ul>
                    </div>
                  )}
                </div>

                {/* Product Journey */}
                <div className="pb-4">
                  <div className="flex items-center gap-1 flex-wrap">
                    <PageBreadcrumb
                      pageTitle="Product Journey"
                      variant="page"
                      textSize="lg"
                    />
                  </div>

                  {journeyBullets.length > 0 ? (
                    <ol className="list-decimal pl-3 space-y-1 text-xs text-charcoal-500 2xl:text-sm marker:font-semibold marker:text-charcoal-400">
                      {journeyBullets.map((item, index) => (
                        <li key={index}>{item}</li>
                      ))}
                    </ol>
                  ) : (
                    <div className="text-xs 2xl:text-sm text-charcoal-500">
                      —
                    </div>
                  )}
                </div>
              </div>
            </div>
          </motion.aside>
        </>
      )}
    </AnimatePresence>
  );
};

export default InsightSideDrawer;