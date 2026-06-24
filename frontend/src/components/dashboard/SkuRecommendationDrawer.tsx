"use client";

import React, { useEffect, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import Productinfoinpopup from "@/components/businessInsight/Productinfoinpopup";
import PageBreadcrumb from "../common/PageBreadCrumb";

type CurrencyCode = "USD" | "GBP" | "INR" | "CAD";

type ObjectiveContext = {
  growth_intent?: string;
  profit_priority?: string;
  inventory_clearance_priority?: boolean;
} | null;

type MetricItem = {
  label: string;
  value: string;
  color?: string;
};

type BestPerformanceMetric = {
  month?: string;
  year?: string | number;

  units?: number;
  net_sales?: number;
  asp?: number;
  cm1_profit?: number;
  unit_wise_profitability?: number;
};

type ProductBestPerformanceData = {
  units?: BestPerformanceMetric;
  net_sales?: BestPerformanceMetric;
  asp?: BestPerformanceMetric;
  cm1_profit?: BestPerformanceMetric;
  unit_wise_profitability?: BestPerformanceMetric;
};

type SelectedRec = {
  productName: string;
  metrics: MetricItem[];
  journeyPoints: string[];
  recommendationPoints: string[];
  advertisingPoints?: string[];
  inventoryPoints?: string[];
  showChart?: boolean;

  // ✅ aggregate SKU group support
  isOtherSkus?: boolean;
  otherSkuProductNames?: string[];
} | null;

const ObjectiveCards = ({
  objective,
  className = "",
}: {
  objective?: ObjectiveContext;
  className?: string;
}) => {
  const growth = objective?.growth_intent?.replaceAll("_", " ") || "Not Defined";
  const profit = objective?.profit_priority?.replaceAll("_", " ") || "Not Defined";
  const inv = objective?.inventory_clearance_priority ? "Yes" : "No";

  const Card = ({ label, value }: { label: string; value: string }) => (
    <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
      <div className="2xl:text-sm text-xs text-[#7A7A7A]">{label}</div>
      <div className="mt-1 2xl:text-sm text-xs font-semibold text-[#0F172A] capitalize">
        {value}
      </div>
    </div>
  );

  return (
    <div className={`grid grid-cols-1 sm:grid-cols-3 gap-5 ${className}`}>
      <Card label="Primary Focus" value={growth} />
      <Card label="Profit Strategy" value={profit} />
      <Card label="Inventory Dilution" value={inv} />
    </div>
  );
};

type Props = {
  open: boolean;
  onClose: () => void;
  selectedRec: SelectedRec;
  objectiveContext?: ObjectiveContext;
  countryName: string;
  sourceCountryName?: string;
  displayCurrency?: CurrencyCode;
  formattedMonthYear?: string; // ✅ add this
};

const metricColors = [
  "border border-[#FDD36F] border-t-[#FDD36F]", // Units
  "border border-[#75BBDA] border-t-[#75BBDA]", // Net Sales
  "border border-[#B75A5A] border-t-[#B75A5A]", // ASP
  "border border-[#C49466] border-t-[#C49466]", // Ads
  "border border-[#7B9A6D] border-t-[#7B9A6D]", // CM2 Profit
  "border border-[#C49466] border-t-[#C49466]", // CM2 Profit Per Unit
  "border border-[#7B9A6D] border-t-[#7B9A6D]", // CM1 Profit
  "border border-[#C49466] border-t-[#C49466]", // CM1 Profit Per Unit
];

const metricOrder = [
  "units",
  "net sales",
  "asp",
  "ads",
  "cm2 profit",
  "cm2 profit per unit",
  "cm1 profit",
  "cm1 profit per unit",
];


const toNum = (v: any) => {
  if (v === null || v === undefined) return 0;
  if (typeof v === "number") return Number.isFinite(v) ? v : 0;

  const n = Number(String(v).replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : 0;
};

const currencyCodeToSymbol = (code?: CurrencyCode | string) => {
  const c = String(code || "").toUpperCase();

  if (c === "USD") return "$";
  if (c === "GBP") return "£";
  if (c === "CAD") return "C$";
  if (c === "INR") return "₹";

  return "";
};

const formatMoneyNoDecimal = (value: any, currency?: CurrencyCode | string) => {
  const symbol = currencyCodeToSymbol(currency);
  const n = Math.round(toNum(value));

  return `${symbol}${n.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  })}`;
};

const formatMoneyTwoDecimal = (value: any, currency?: CurrencyCode | string) => {
  const symbol = currencyCodeToSymbol(currency);
  const n = toNum(value);

  return `${symbol}${n.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
};

const formatUnitsNoDecimal = (value: any) => {
  return Math.round(toNum(value)).toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
};

const formatBestPerformancePeriod = (
  month?: string,
  year?: string | number
) => {
  if (!month) return "-";

  const monthMap: Record<string, string> = {
    january: "Jan",
    february: "Feb",
    march: "Mar",
    april: "Apr",
    may: "May",
    june: "Jun",
    july: "Jul",
    august: "Aug",
    september: "Sep",
    october: "Oct",
    november: "Nov",
    december: "Dec",
  };

  const shortMonth =
    monthMap[String(month).toLowerCase()] || String(month).slice(0, 3);

  const shortYear = year ? String(year).slice(-2) : "";

  return shortYear ? `${shortMonth}'${shortYear}` : shortMonth;
};

const normalizeSkuGroupName = (name: string) =>
  String(name || "")
    .trim()
    .toLowerCase()
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/sku's/g, "skus")
    .replace(/product's/g, "products");

const isSkuGroupCardName = (name: string) => {
  const value = normalizeSkuGroupName(name);

  return (
    value === "others" ||
    value === "other" ||
    value === "other sku" ||
    value === "other skus" ||
    value === "other product" ||
    value === "other products" ||
    value === "remaining" ||
    value === "remaining sku" ||
    value === "remaining skus" ||
    value === "remaining product" ||
    value === "remaining products" ||
    value === "rest sku" ||
    value === "rest skus" ||
    value === "rest product" ||
    value === "rest products" ||
    value === "leftover sku" ||
    value === "leftover skus" ||
    value === "leftover product" ||
    value === "leftover products" ||
    value === "all other skus" ||
    value === "all remaining skus"
  );
};

export default function SkuRecommendationDrawer({
  open,
  onClose,
  selectedRec,
  objectiveContext,
  countryName,
  sourceCountryName,
  displayCurrency,
  formattedMonthYear, // ✅ add this
}: Props) {
  const [bestPerformanceLoading, setBestPerformanceLoading] = useState(false);
  const [bestPerformanceError, setBestPerformanceError] = useState<string | null>(null);
  const [bestPerformanceData, setBestPerformanceData] =
    useState<ProductBestPerformanceData | null>(null);
  const sortedMetrics = [...(selectedRec?.metrics || [])].sort((a, b) => {
    const aIndex = metricOrder.indexOf(a.label.toLowerCase());
    const bIndex = metricOrder.indexOf(b.label.toLowerCase());

    const safeAIndex = aIndex === -1 ? Number.MAX_SAFE_INTEGER : aIndex;
    const safeBIndex = bIndex === -1 ? Number.MAX_SAFE_INTEGER : bIndex;

    return safeAIndex - safeBIndex;
  });

  const cleanedRecommendationPoints = (selectedRec?.recommendationPoints || []).filter(
    (p) => !/inventory/i.test(p)
  );

  const cleanedAdvertisingPoints = (selectedRec?.advertisingPoints || []).filter(
    (p) => !/inventory/i.test(p)
  );

  const derivedInventoryPoints = [
    ...(selectedRec?.inventoryPoints || []),
    ...(selectedRec?.recommendationPoints || []).filter((p) => /inventory/i.test(p)),
    ...(selectedRec?.advertisingPoints || []).filter((p) => /inventory/i.test(p)),
  ];

  const isSkuGroupDrawer =
    selectedRec?.isOtherSkus ||
    isSkuGroupCardName(selectedRec?.productName || "");

  const getMetricBorderColorByLabel = (label: string, fallbackIndex = 0) => {
    const normalizedLabel = label.trim().toLowerCase();
    const metricIndex = metricOrder.indexOf(normalizedLabel);

    return metricColors[
      metricIndex !== -1 ? metricIndex : fallbackIndex % metricColors.length
    ];
  };

  useEffect(() => {
    if (!open) return;
    if (!selectedRec?.productName) return;

    const productName = String(selectedRec.productName || "").trim();

    if (!productName) return;

    const lowerName = productName.toLowerCase();

    if (
      lowerName === "total" ||
      lowerName === "grand total" ||
      selectedRec?.isOtherSkus ||
      isSkuGroupCardName(productName)
    ) {
      setBestPerformanceData(null);
      setBestPerformanceError(null);
      setBestPerformanceLoading(false);
      return;
    }

    const ac = new AbortController();

    const fetchBestPerformance = async () => {
      try {
        setBestPerformanceLoading(true);
        setBestPerformanceError(null);
        setBestPerformanceData(null);

        const token =
          typeof window !== "undefined"
            ? localStorage.getItem("jwtToken")
            : null;

        if (!token) throw new Error("Missing token");

        const apiCountry = String(sourceCountryName || countryName || "global")
          .trim()
          .toLowerCase();

        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductBestPerformance`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Authorization: `Bearer ${token}`,
            },
            body: JSON.stringify({
              product_name: productName,
              country: apiCountry,
              home_currency: displayCurrency || "USD",
            }),
            cache: "no-store",
            signal: ac.signal,
          }
        );

        const json = await res.json().catch(() => ({}));

        if (!res.ok) {
          throw new Error(json?.error || "Failed to fetch best performance");
        }

        setBestPerformanceData(json?.best_performance ?? null);
      } catch (e: any) {
        if (e?.name === "AbortError") return;
        setBestPerformanceError(e?.message || "Failed to load best performance");
      } finally {
        setBestPerformanceLoading(false);
      }
    };

    fetchBestPerformance();

    return () => ac.abort();
  }, [
    open,
    selectedRec?.productName,
    selectedRec?.isOtherSkus,
    countryName,
    sourceCountryName,
    displayCurrency,
  ]);

  if (!open || !selectedRec) return null;

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
            <div className="flex flex-col gap-4 h-full">
              <div className="shrink-0 border-b border-slate-200 p-3 flex items-center justify-between gap-3">
                <div>
                  <div className="flex items-center gap-1 flex-wrap">
                    <PageBreadcrumb
                      pageTitle="Detailed View - "
                      variant="page"
                      textSize="2xl"
                    />

                    <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
                      {selectedRec?.productName || "Details"}
                    </span>

                    {formattedMonthYear && (
                      <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
                        ( {formattedMonthYear} )
                      </span>
                    )}
                  </div>
                </div>

                <button
                  onClick={onClose}
                  className="rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
                >
                  ✕
                </button>
              </div>

              <div className="flex-1 overflow-y-auto space-y-6 px-3">
                <div>
                  <div className="text-xs sm:text-sm 2xl:text-lg font-semibold text-charcoal-700 mb-2">
                    Metrics
                  </div>

                  <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-5 gap-3">
                    {sortedMetrics.map((m, i) => (
                      <div
                        key={i}
                        className={`rounded-lg border border-t-4 ${getMetricBorderColorByLabel(m.label, i)} px-3 py-2`}
                      >
                        <div className="text-[10px] 2xl:text-xs text-charcoal-400">
                          {m.label
                            .replace(/\b\w/g, (char) => char.toUpperCase())
                            .replace("Cm1", "CM1")}
                        </div>

                        <div className="flex flex-col leading-tight">
                          {(() => {
                            const match = m.value.match(/^([^\(]+)\s*(\(.+\))?$/);
                            const mainValue = match?.[1]?.trim() || m.value;
                            const percentPart = match?.[2] || "";

                            const isAdsMetric = m.label.trim().toLowerCase() === "ads";
                            const isNegative = percentPart.includes("-");
                            const percentColor = isAdsMetric
                              ? "#414042"
                              : isNegative
                                ? "#FF5C5C"
                                : "#5EA68E";

                            return (
                              <>
                                <span
                                  className="text-sm 2xl:text-lg font-bold"
                                  style={{ color: "#414042" }}
                                >
                                  {mainValue}
                                </span>

                                {percentPart && (
                                  <span
                                    className="text-[10px] 2xl:text-xs font-semibold"
                                    style={{ color: percentColor }}
                                  >
                                    {percentPart}
                                  </span>
                                )}
                              </>
                            );
                          })()}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {!isSkuGroupDrawer && (
                  <div>
                    <div className="mb-2 text-xs font-semibold text-charcoal-700 sm:text-sm 2xl:text-lg">
                      Overall Best Performance
                    </div>
                    <div className="mb-2 text-[11px] text-charcoal-400 2xl:text-xs">
                      Best performance is calculated from overall historical data, not just the selected period.
                    </div>

                    {bestPerformanceLoading ? (
                      <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-xs text-charcoal-500 2xl:text-sm">
                        Loading best performance...
                      </div>
                    ) : bestPerformanceError ? (
                      <div className="rounded-lg border border-red-100 bg-red-50 px-3 py-3 text-xs text-red-600 2xl:text-sm">
                        {bestPerformanceError}
                      </div>
                    ) : bestPerformanceData ? (
                      <div className="grid grid-cols-1 gap-3 sm:grid-cols-3  xl:grid-cols-5">
                        {[
                          {
                            label: "Units",
                            value: formatUnitsNoDecimal(bestPerformanceData?.units?.units),
                            period: formatBestPerformancePeriod(
                              bestPerformanceData?.units?.month,
                              bestPerformanceData?.units?.year
                            ),
                          },
                          {
                            label: "Net Sales",
                            value: formatMoneyNoDecimal(
                              bestPerformanceData?.net_sales?.net_sales,
                              displayCurrency
                            ),
                            period: formatBestPerformancePeriod(
                              bestPerformanceData?.net_sales?.month,
                              bestPerformanceData?.net_sales?.year
                            ),
                          },
                          {
                            label: "ASP",
                            value: formatMoneyTwoDecimal(
                              bestPerformanceData?.asp?.asp,
                              displayCurrency
                            ),
                            period: formatBestPerformancePeriod(
                              bestPerformanceData?.asp?.month,
                              bestPerformanceData?.asp?.year
                            ),
                          },
                          {
                            label: "CM1 Profit",
                            value: formatMoneyNoDecimal(
                              bestPerformanceData?.cm1_profit?.cm1_profit,
                              displayCurrency
                            ),
                            period: formatBestPerformancePeriod(
                              bestPerformanceData?.cm1_profit?.month,
                              bestPerformanceData?.cm1_profit?.year
                            ),
                          },
                          {
                            label: "CM1 Profit Per Unit",
                            value: formatMoneyTwoDecimal(
                              bestPerformanceData?.unit_wise_profitability?.unit_wise_profitability,
                              displayCurrency
                            ),
                            period: formatBestPerformancePeriod(
                              bestPerformanceData?.unit_wise_profitability?.month,
                              bestPerformanceData?.unit_wise_profitability?.year
                            ),
                          },
                        ].map((card, index) => (
                          <div
                            key={card.label}
                            className={`rounded-lg border border-t-4 ${getMetricBorderColorByLabel(card.label, index)} px-3 py-2`}
                          >
                            <div className="text-[10px] 2xl:text-xs text-charcoal-400">
                              {card.label}
                            </div>

                            <div className="flex flex-col leading-tight">
                              <span className="mt-1 text-[10px] 2xl:text-xs text-[#414042]">
                                {card.period}
                              </span>

                              <span className=" text-sm 2xl:text-lg font-bold text-[#414042]">
                                {card.value}
                              </span>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-xs text-charcoal-500 2xl:text-sm">
                        —
                      </div>
                    )}
                  </div>
                )}

                <div>
                  <div className="mb-2 text-xs font-semibold text-charcoal-500 sm:text-sm 2xl:text-lg">
                    Recommendations
                  </div>

                  {cleanedRecommendationPoints.length ? (
                    <div>
                      <div className="text-xs 2xl:text-sm font-semibold text-charcoal-500">
                        Action
                      </div>
                      <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
                        {cleanedRecommendationPoints.map((p, i) => (
                          <li key={i}>{p}</li>
                        ))}
                      </ul>
                    </div>
                  ) : (
                    <div className="text-xs 2xl:text-sm text-charcoal-500">—</div>
                  )}

                  {cleanedAdvertisingPoints.length ? (
                    <div className="mt-2">
                      <div className="text-xs 2xl:text-sm font-semibold text-charcoal-500">
                        Advertising
                      </div>
                      <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
                        {cleanedAdvertisingPoints.map((p, i) => (
                          <li key={i}>{p}</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}

                  {derivedInventoryPoints.length ? (
                    <div className="mt-2">
                      <div className="text-xs 2xl:text-sm font-semibold text-charcoal-500">
                        Inventory
                      </div>
                      <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
                        {derivedInventoryPoints.map((p, i) => (
                          <li key={i}>{p.replace(/^•\s*/, "")}</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}
                </div>

                {selectedRec?.showChart && selectedRec?.productName && (
                  <div className="w-full">
                    <Productinfoinpopup
                      productname={selectedRec.productName}
                      countryName={countryName}
                      sourceCountryName={sourceCountryName}
                      displayCurrency={displayCurrency}
                      isOtherSkus={selectedRec.isOtherSkus || isSkuGroupCardName(selectedRec.productName)}
                      otherSkuProductNames={selectedRec.otherSkuProductNames || []}
                    />
                  </div>
                )}

                <div className="pb-4">
                  <div className="flex items-center gap-1 flex-wrap">
                    <PageBreadcrumb
                      pageTitle="Product Journey"
                      variant="page"
                      textSize="lg"
                    />
                  </div>

                  {selectedRec?.journeyPoints?.length ? (
                    <ol className="list-decimal pl-3 space-y-1 text-xs text-charcoal-500 2xl:text-sm marker:font-semibold marker:text-charcoal-400">
                      {selectedRec.journeyPoints.map((p, i) => (
                        <li key={i}>{p}</li>
                      ))}
                    </ol>
                  ) : (
                    <div className="text-xs 2xl:text-sm text-charcoal-500">—</div>
                  )}
                </div>
              </div>
            </div>
          </motion.aside>
        </>
      )}
    </AnimatePresence>
  );
}