// lib/dashboard/types.ts

export type RegionKey = "Global" | "UK" | "US" | "CA";

export type RegionMetrics = {
  mtdUSD: number;
  lastMonthToDateUSD: number;
  lastMonthTotalUSD: number;
  targetUSD: number;
    decTargetUSD?: number;
};
