import type { ActionCardItem, ActionLogicItem } from "./ActionBasedDashboard";
import type { AgeingBucket, AgeingRiskHeatmapRow } from "./AgeingRiskHeatmap";
import type { AgeingTrendAllSeriesItem, AgeingTrendItem } from "./AgeingTrendChart";
import type { DonutChartItem } from "./SkuAgeingDonutChart";

export type ZeroInventoryInsightsData = {
  heatmapBuckets: AgeingBucket[];
  heatmapData: AgeingRiskHeatmapRow[];
  donutSku: string;
  donutData: DonutChartItem[];
  donutTotalUnits: number;
  trendSelectedBucket: string;
  trendData: AgeingTrendItem[];
  trendLineColor: string;
  trendAllSeriesData: AgeingTrendAllSeriesItem[];
  trendBucketOptions: { label: string; value: string; color: string }[];
  actions: ActionCardItem[];
  actionLogic: ActionLogicItem[];
  inventoryAgeSummary: {
    total: number;
    current_month_units_sold_total: number;
    percentage_base_total: number;
    sellable_total: number;
    unfulfillable_total: number;
    total_units_summary: {
      current_month_units_sold: { total: number; percentage_share: number };
      sellable: { total: number; percentage_share: number };
      unfulfillable: { total: number; percentage_share: number };
    };
    columns: Record<string, { total: number; percentage_share: number }>;
  };
};

const buckets = [
  { label: "0–180 Days", value: "0-180 days", key: "zeroToOneEighty", color: "#7B9A6D" },
  { label: "181–270 Days", value: "181-270 days", key: "oneEightyOneToTwoSeventy", color: "#ED9F50" },
  { label: "271–365 Days", value: "271-365 days", key: "twoSeventyOneToThreeSixtyFive", color: "#C49466" },
  { label: "365+ Days", value: "365+ days", key: "threeSixtyFivePlus", color: "#B75A5A" },
] as const;

const zeroMonths: AgeingTrendItem[] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"].map(
  (label) => ({ label, value: 0 })
);

export const createZeroInventoryInsightsData = (
  storageCostCurrencySymbol = "$"
): ZeroInventoryInsightsData => {
  const zeroHeatmapValues = {
    zeroToOneEighty: 0,
    oneEightyOneToTwoSeventy: 0,
    twoSeventyOneToThreeSixtyFive: 0,
    threeSixtyFivePlus: 0,
    available: 0,
    fcTransfer: 0,
    totalUnits: 0,
    inboundUnits: 0,
    unsellableUnits: 0,
    currentFba: 0,
    currentAwd: 0,
    transitFba: 0,
    transitAwd: 0,
    totalInStock: 0,
    totalInTransit: 0,
    unsellableFba: 0,
    unsellableAwd: 0,
    storageCostUsd: 0,
    coverageCurrentAndTransit: 0,
    unitsSold: 0,
    salesLast30Days: 0,
    coverageRatio: 0,
    inventoryAlert: "",
  };

  const dummyProductRows: AgeingRiskHeatmapRow[] = Array.from(
    { length: 6 },
    (_, index) => ({
      productName: `Demo Product ${String.fromCharCode(65 + index)}`,
      sku: `SKU-${String.fromCharCode(65 + index)}`,
      ...zeroHeatmapValues,
      isDummyRow: true,
    })
  );

  const heatmapData: AgeingRiskHeatmapRow[] = [
    ...dummyProductRows,
    { productName: "Total", sku: "-", ...zeroHeatmapValues, isTotalRow: true },
    { productName: "% of Total", sku: "-", ...zeroHeatmapValues, isPercentageRow: true },
  ];

  const actionLogic: ActionLogicItem[] = [
    { key: "healthy", label: "Healthy", description: "Stock covers 0–180 days", color: "#7B9A6D" },
    { key: "high_alert", label: "High Alert", description: "Shipment Required", color: "#B75A5A" },
    { key: "liquidate", label: "Liquidate", description: "Stock older than 180 days", color: "#ED9F50" },
    { key: "unfulfillable", label: "Unfulfillable", description: "Remove or dispose stock", color: "#3A8EA4" },
    { key: "estimated_storage_cost", label: "Estimate Storage", description: "Monthly storage estimate", color: "#C49466" },
  ];

  const actions: ActionCardItem[] = actionLogic.map((item) => ({
    ...item,
    count: 0,
    displayValue: item.key === "estimated_storage_cost" ? `${storageCostCurrencySymbol}0.00` : 0,
    skuCount: 0,
    unitCount: 0,
    avgCoverageRatio: item.key === "high_alert" ? 0 : undefined,
    backgroundColor: "#ffffff",
  }));

  const inventoryAgeSummaryColumns = Object.fromEntries(
    [
      "inv-age-0-to-180-days",
      "inv-age-181-to-270-days",
      "inv-age-271-to-365-days",
      "inv-age-365-plus-days",
    ].map((column) => [column, { total: 0, percentage_share: 0 }])
  );

  return {
    heatmapBuckets: buckets.map(({ key, label, color }) => ({ key, label, color })),
    heatmapData,
    donutSku: "Overall",
    donutData: buckets.map(({ label, color }) => ({
      bucket: label,
      units: 0,
      percentageShare: 0,
      color,
    })),
    donutTotalUnits: 0,
    trendSelectedBucket: "all",
    trendData: zeroMonths,
    trendLineColor: "#B75A5A",
    trendAllSeriesData: buckets.map(({ value, label, color }) => ({
      bucketValue: value,
      bucketLabel: label,
      color,
      data: zeroMonths,
    })),
    trendBucketOptions: buckets.map(({ label, value, color }) => ({ label, value, color })),
    actions,
    actionLogic,
    inventoryAgeSummary: {
      total: 0,
      current_month_units_sold_total: 0,
      percentage_base_total: 0,
      sellable_total: 0,
      unfulfillable_total: 0,
      total_units_summary: {
        current_month_units_sold: { total: 0, percentage_share: 0 },
        sellable: { total: 0, percentage_share: 0 },
        unfulfillable: { total: 0, percentage_share: 0 },
      },
      columns: inventoryAgeSummaryColumns,
    },
  };
};
