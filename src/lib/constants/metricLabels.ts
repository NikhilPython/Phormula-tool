export const METRIC_LABELS = {
  sales: {
    full: "Net Sales",
    short: "Sales",
  },
  total_cous: {
    full: "COGS",
    short: "COGS",
  },
  taxncredit: {
    full: "Taxes & Credits",
    short: "Taxes",
  },
  AmazonExpense: {
    full: "Amazon Fees",
    short: "Fees",
  },
  advertisingCosts: {
    full: "Advertising Costs",
    short: "Ads",
  },
  Other: {
    full: "Other Expenses",
    short: "Other",
  },
  profit: {
    full: "CM2 Profit",
    short: "CM2",
  },
  profit2: {
    full: "CM1 Profit",
    short: "CM1",
  },
} as const;

export type MetricKey = keyof typeof METRIC_LABELS;

export const getMetricLabel = (
  metric: MetricKey,
  isCollapsed: boolean
) => {
  return isCollapsed
    ? METRIC_LABELS[metric].short
    : METRIC_LABELS[metric].full;
};
