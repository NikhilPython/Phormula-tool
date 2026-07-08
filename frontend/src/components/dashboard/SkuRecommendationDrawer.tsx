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
  "border border-[#FDD36F] border-t-4", // Units
  "border border-[#75BBDA] border-t-4", // Net Sales
  "border border-[#B75A5A] border-t-4", // ASP
  "border border-[#C49466] border-t-4", // Ads
  "border border-[#7B9A6D] border-t-4", // CM2 Profit
  "border border-[#C49466] border-t-4", // CM2 Profit Per Unit
  "border border-[#7B9A6D] border-t-4", // CM1 Profit
  "border border-[#C49466] border-t-4", // CM1 Profit Per Unit
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

const splitMetricValue = (value: string) => {
  const v = String(value || "").trim();

  // Supports:
  // $862 (+0.00%)
  // $439 (-97.07%)
  // $0.00 (0.00%)
  const match = v.match(/^(.+?)\s*(\(([+-]?)[^)]+\))\s*$/);

  if (!match) {
    return {
      main: v,
      delta: "",
      deltaColor: "",
    };
  }

  const main = match[1].trim();
  const delta = match[2].trim();
  const sign = match[3];

  return {
    main,
    delta,
    deltaColor:
      sign === "+"
        ? "text-emerald-600"
        : sign === "-"
          ? "text-red-600"
          : "text-charcoal-500",
  };
};

const formatMetricDelta = (delta: string) => {
  const cleanDelta = String(delta || "")
    .replace(/[()]/g, "")
    .trim();

  if (!cleanDelta) return "";

  const isPositive = cleanDelta.startsWith("+");
  const isNegative = cleanDelta.startsWith("-");
  const valueWithoutSign = cleanDelta.replace(/^[-+]/, "");

  if (isPositive) return `▲ ${valueWithoutSign}`;
  if (isNegative) return `▼ ${valueWithoutSign}`;

  // For neutral value like (0.00%), show only value, no arrow
  return valueWithoutSign;
};

const formatRecommendationCardMainValue = (
  label: string,
  main: string
) => {
  const normalizedLabel = String(label || "").trim().toLowerCase();

  if (
    normalizedLabel !== "net sales" &&
    normalizedLabel !== "cm1 profit" &&
    normalizedLabel !== "cm2 profit"
  ) {
    return main;
  }

  const currencyMatch = main.match(/^([^0-9-]*)/);
  const currency = currencyMatch?.[1] ?? "";

  const numberPart = main.replace(/[^0-9.-]/g, "");
  const numberValue = Number(numberPart);

  if (!Number.isFinite(numberValue)) return main;

  return `${currency}${Math.round(numberValue).toLocaleString()}`;
};

const formatMetricTitle = (label: string) => {
  return String(label || "")
    .replace(/\b\w/g, (char) => char.toUpperCase())
    .replace("Cm1", "CM1")
    .replace("Cm2", "CM2");
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
                  <PageBreadcrumb
                    pageTitle="Metrics"
                    variant="page"
                    align="left"
                    textSize="xl"
                    className="mb-2"
                  />

                  <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-4 min-[1700px]:grid-cols-4">
                    {sortedMetrics.map((m, i) => {
                      const { main, delta, deltaColor } = splitMetricValue(m.value);

                      const displayMain = formatRecommendationCardMainValue(
                        m.label,
                        main
                      );

                      const isAdsMetric = m.label.trim().toLowerCase() === "ads";

                      return (
                        <div
                          key={`${m.label}-${i}`}
                          className={[
                            "w-full rounded-xl bg-white shadow-sm p-1.5 2xl:p-2",
                            "flex flex-col justify-between min-h-[72px]",
                            getMetricBorderColorByLabel(m.label, i),
                          ].join(" ")}
                        >
                          <div className="flex justify-between items-center">
                            <span className="text-[10px] 2xl:text-xs font-medium text-charcoal-500">
                              {formatMetricTitle(m.label)}
                            </span>
                          </div>

                          <div className="mt-1 flex items-baseline justify-between gap-3 leading-tight tabular-nums">
                            <span className="text-sm 2xl:text-lg font-semibold text-charcoal-500 truncate">
                              {displayMain}
                            </span>

                            {delta ? (
                              <span
                                className={[
                                  "text-[10px] 2xl:text-xs font-semibold whitespace-nowrap text-right",
                                  isAdsMetric ? "text-charcoal-500" : deltaColor,
                                ].join(" ")}
                              >
                                {formatMetricDelta(delta)}
                              </span>
                            ) : null}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>

                {!isSkuGroupDrawer && (
                  <div>
                    <PageBreadcrumb
                      pageTitle="Overall Best Performance"
                      variant="page"
                      align="left"
                      textSize="xl"
                    />

                    <p className="mb-2 text-xs 2xl:text-sm text-charcoal-500 mt-1">
                      Best performance is calculated from overall historical data, not just the selected period.
                    </p>

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
                            className={[
                              "w-full rounded-xl bg-white shadow-sm p-1.5 2xl:p-2",
                              "flex flex-col justify-between min-h-[78px]",
                              getMetricBorderColorByLabel(card.label, index),
                            ].join(" ")}
                          >
                            <div className="flex justify-between items-center">
                              <span className="text-[10px] 2xl:text-xs font-medium text-charcoal-500">
                                {formatMetricTitle(card.label)}
                              </span>
                            </div>

                            <div className="mt-1 flex items-end justify-between gap-3 leading-tight tabular-nums">
                              <div className="min-w-0">
                                <div className="text-[10px] 2xl:text-xs font-medium text-charcoal-500 whitespace-nowrap">
                                  {card.period}
                                </div>

                                <div className="mt-1 text-sm 2xl:text-lg font-semibold text-charcoal-500 whitespace-nowrap">
                                  {card.value}
                                </div>
                              </div>
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
                  <PageBreadcrumb
                    pageTitle="Recommendations"
                    variant="page"
                    align="left"
                    textSize="xl"
                    className="mb-2"
                  />

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
                      textSize="xl"
                      className="mb-2"
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