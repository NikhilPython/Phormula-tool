'use client';

import React from 'react';
import { AnimatePresence, motion } from "framer-motion";
import Productinfoinpopup from '@/components/businessInsight/Productinfoinpopup';
import PageBreadcrumb from '../common/PageBreadCrumb';

// --- types same as yours ---
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

  // ✅ add explicit (optional but recommended)
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
  fbType?: 'like' | 'dislike' | null;
  setFbType?: (v: 'like' | 'dislike' | null) => void;
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

const InsightSideDrawer: React.FC<InsightSideDrawerProps> = ({
  open,
  selectedSku,
  skuInsights,
  getInsightByProductName,
  onClose,
  countryName,
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
    if (lower.includes("unit")) return value.toLocaleString();

    return `${getCurrencySymbol(homeCurrency)}${value.toLocaleString()}`;
  };

  const journeyBullets =
    Array.isArray(insightData.product_journey) ? insightData.product_journey : [];

  return (
    <AnimatePresence>
      {open && (
        <>
          {/* overlay */}
          <motion.div
            className="fixed h-full inset-0 z-[999999] bg-black/40"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
          />

          {/* drawer */}
          <motion.aside
            className="fixed right-0 top-0 z-[1000000] h-screen w-[95vw] max-w-[720px] bg-white shadow-2xl flex flex-col"
            initial={{ x: 520 }}
            animate={{ x: 0 }}
            exit={{ x: 520 }}
            transition={{ type: "tween", duration: 0.25 }}
          >
            {/* header */}
            <div className="shrink-0 border-b border-slate-200 p-4 flex items-start justify-between gap-3">
              <div>
                <div className="flex items-center gap-2">
                  <div className="text-sm text-slate-500">AI Insight</div>

                  {drawerPeriodText ? (
                    <span className="text-[#5EA68E] font-semibold text-sm">
                      {drawerPeriodText}
                    </span>
                  ) : periodBadge ? (
                    <span className="text-[11px] px-2 py-0.5 rounded-full border border-slate-200 bg-slate-50 text-slate-700">
                      {periodBadge}
                    </span>
                  ) : null}
                </div>

                <div className="text-lg font-semibold text-slate-900">
                  {insightData.product_name || selectedSku}
                </div>
              </div>

              <button
                onClick={onClose}
                className="rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
              >
                ✕
              </button>
            </div>

            {/* content */}
            <div className="flex-1 overflow-y-auto p-4 space-y-5">
              {/* 1) Best performance */}
              {insightData.best_performance && (
                <div className="space-y-2">
                  <div className="text-sm font-semibold text-slate-800">Best Performance</div>

                  <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
                    {[
                      { label: "Sales", data: insightData.best_performance.sales },
                      { label: "Units", data: insightData.best_performance.units },
                      { label: "Profit", data: insightData.best_performance.profit },
                    ].map((b, i) => (
                      <div key={i} className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                        <div className="text-xs text-slate-500">{b.label}</div>
                        <div className="text-xs text-slate-500">
                          {formatPerfMonth(b.data?.month)}
                        </div>
                        <div className="text-sm font-bold text-slate-900 mt-1">
                          {formatPerfValue(b.label, b.data?.value)}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* 2) Objective strip */}
              {insightData.objective && (
                <div className="space-y-2">
                  <div className="text-sm font-semibold text-slate-800">Objectives</div>

                  <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
                    <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                      <div className="text-xs text-slate-500">Primary Focus</div>
                      <div className="text-sm font-bold text-slate-800 mt-1">
                        {insightData.objective?.growth_intent || "balanced"}
                      </div>
                    </div>

                    <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                      <div className="text-xs text-slate-500">Profit Strategy</div>
                      <div className="text-sm font-bold text-slate-800 mt-1">
                        {(insightData.objective?.profit_priority?.replaceAll("_", " ") || "protect growth")}
                      </div>
                    </div>

                    <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                      <div className="text-xs text-slate-500">Inventory Dilution</div>
                      <div className="text-sm font-bold text-slate-800 mt-1">
                        {insightData.objective?.inventory_clearance_priority ? "Yes" : "No"}
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {/* 3) Recommendations */}
              <div className="space-y-2">
                <div className="text-sm font-semibold text-slate-800">Recommendations</div>

                {!!insightData.recommendation ? (
                  <div>
                    <div className="text-xs font-semibold text-blue-900 mb-1">💡 Action</div>
                    <ul className="list-disc pl-5 space-y-1 text-xs text-slate-700">
                      <li>{insightData.recommendation}</li>
                    </ul>
                  </div>
                ) : (
                  <div className="text-xs text-slate-500">No recommendation available.</div>
                )}

                {!!insightData.inventory_recommendation && (
                  <div>
                    <div className="text-xs font-semibold text-amber-900 mb-1">📦 Inventory</div>
                    <ul className="list-disc pl-5 space-y-1 text-xs text-slate-700">
                      <li>{insightData.inventory_recommendation}</li>
                    </ul>
                  </div>
                )}
              </div>


              {/* 5) Product Journey ✅ FIXED */}
              {journeyBullets.length > 0 && (
                <div className="space-y-2">
                  <div className="text-sm font-semibold text-slate-800">Product Journey</div>

                  <ol className="list-decimal list-outside space-y-2 pl-5 text-xs text-slate-700 marker:text-slate-400 marker:font-semibold">
                    {journeyBullets.map((j: string, i: number) => (
                      <li key={i}>
                        <span>{j}</span>
                      </li>
                    ))}
                  </ol>
                </div>
              )}
            </div>
          </motion.aside>
        </>
      )}
    </AnimatePresence>
  );
};

export default InsightSideDrawer;