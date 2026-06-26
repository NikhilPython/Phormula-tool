'use client';

import React, { use, useEffect, useMemo, useState } from 'react';
import * as XLSX from 'xlsx';
import '@/app/(admin)/pnlforecast/[countryName]/[month]/[year]/Styles.css';
import Modalmsg from '@/components/ui/modal/Modalmsg';
import SkuMultiuseCountryUpload from '@/components/ui/modal/SkuMultiCountryUpload';
import DataTable, { ColumnDef } from '@/components/ui/table/DataTable';
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import SegmentedToggle from '@/components/ui/SegmentedToggle';
import DownloadIconButton from '@/components/ui/button/DownloadButton';
import { useRouter } from 'next/navigation';
import { IoMdLock } from "react-icons/io";
import WarehouseMultiCountryUpload from "@/components/ui/modal/WarehouseMultiCountryUpload";
import Loader from '@/components/loader/Loader';
import { exportSkuInformationExcel, exportWarehouseDataExcel } from '@/lib/excel/exportCurrentInventoryExcel';
import { useGetUserDataQuery } from '@/lib/api/profileApi';
import InventoryInsightsSection from "@/components/common/inventory/InventoryInsightsSection";

import type {
  AgeingBucket,
  AgeingRiskHeatmapRow,
} from "@/components/common/inventory/AgeingRiskHeatmap";

import type {
  DonutChartItem,
} from "@/components/common/inventory/SkuAgeingDonutChart";

import type {
  AgeingTrendItem,
  AgeingTrendAllSeriesItem,
} from "@/components/common/inventory/AgeingTrendChart";

import type {
  ActionCardItem,
  ActionLogicItem,
} from "@/components/common/inventory/ActionBasedDashboard";
import PeriodFiltersTable from "@/components/filters/PeriodFiltersTable";

// =========================
// Warehouse upload format
// =========================
const WAREHOUSE_FIXED_COLUMNS = [
  's_no',
  'sku_us',
  'sku_uk',
  'local_stock',
  'in_transit_units',
  'month',
  'year',
];

const normalizeWarehouseHeader = (key: string) => {
  const raw = key.trim().toLowerCase().replace(/\s+/g, '_');

  const map: Record<string, string> = {
    's.no': 's_no',
    's.no.': 's_no',
    's._no': 's_no',
    's._no.': 's_no',
    's_no': 's_no',
    'serial_no': 's_no',
    'serial_number': 's_no',

    'sku_us': 'sku_us',
    'sku_(us)': 'sku_us',
    'sku-us': 'sku_us',
    'sku us': 'sku_us',

    'sku_uk': 'sku_uk',
    'sku_(uk)': 'sku_uk',
    'sku-uk': 'sku_uk',
    'sku uk': 'sku_uk',

    'stock': 'local_stock',
    'local_stock': 'local_stock',
    'local stock': 'local_stock',

    'in_transit': 'in_transit_units',
    'transit_units': 'in_transit_units',
    'in_transit_units': 'in_transit_units',
    'in transit units': 'in_transit_units',

    'month': 'month',
    'year': 'year',
  };

  return map[raw] || raw;
};

const validateWarehouseHeaders = (row: Record<string, any>) => {
  const normalized = Object.keys(row).map(normalizeWarehouseHeader);

  const missing = WAREHOUSE_FIXED_COLUMNS.filter(
    (col) => !normalized.includes(col)
  );

  if (missing.length) {
    return 'Invalid file format. Please upload a file using the provided template.';
  }

  return '';
};

// Types
interface Params {
  params: Promise<{
    countryName: string;
    month: string;
    year: string;
  }>;
}

interface SkuRow {
  s_no: number;
  product_name: string;
  sku_uk?: string;
  sku_us?: string;
  sku_canada?: string;
  asin?: string;
  product_barcode?: string;
  price?: number;
  currency?: string;
  month?: string;
  year?: string | number;
  local_stock?: number;
  in_transit_units?: number;
  [key: string]: any;
}

type TableRow = {
  id: string;
  s_no: React.ReactNode;
  product_name: React.ReactNode;
  sku_uk?: React.ReactNode;
  sku_us?: React.ReactNode;
  sku_canada?: React.ReactNode;
  asin?: React.ReactNode;
  product_barcode?: React.ReactNode;
  month_year?: React.ReactNode;
  price?: React.ReactNode;
  gross_margin_uk?: React.ReactNode;
  gross_margin_us?: React.ReactNode;
  gross_margin_canada?: React.ReactNode;
  gross_margin_eu?: React.ReactNode;
  gross_margin_europe?: React.ReactNode;
  gross_margin_global?: React.ReactNode;
  [key: string]: React.ReactNode;
};

type RangeType = 'monthly' | 'quarterly' | 'yearly';
type Quarter = 'Q1' | 'Q2' | 'Q3' | 'Q4';

type InventoryCurrentRow = Record<string, any>;

type InventoryCurrentApiResponse = {
  success: boolean;
  rows?: InventoryCurrentRow[];
  categories?: Record<
    string,
    {
      items?: any[];
      product_count?: number;
      sku_count?: number;
    }
  >;
  month?: string;
  year?: number;
  country_key?: string;
  inventory_age_summary?: {
    total?: number;
    columns?: Record<
      string,
      {
        total?: number;
        percentage_share?: number;
      }
    >;
  };
  message?: string;
};

type InventoryAgeSummaryApiResponse = {
  success: boolean;
  month?: string;
  year?: number;
  country_key?: string;
  totals?: Record<string, number>;
  age_summary?: {
    month: string;
    month_number?: number;
    year: number;
    age_bucket: string;
    column: string;
    units: number;
  }[];
  month_summary?: {
    month: string;
    month_number: number;
    year: number;
    source?: string;
    totals: Record<string, number>;
  }[];
  message?: string;
};

type InventoryInsightsData = {
  heatmapBuckets: AgeingBucket[];
  heatmapData: AgeingRiskHeatmapRow[];
  donutSku: string;
  donutData: DonutChartItem[];
  donutTotalUnits: number;
  trendSelectedBucket: string;
  trendData: AgeingTrendItem[];
  trendLineColor: string;
  trendAllSeriesData: AgeingTrendAllSeriesItem[];
  trendBucketOptions: {
    label: string;
    value: string;
    color: string;
  }[];
  actions: ActionCardItem[];
  actionLogic: ActionLogicItem[];
};

const allMonths = [
  'january',
  'february',
  'march',
  'april',
  'may',
  'june',
  'july',
  'august',
  'september',
  'october',
  'november',
  'december',
];

const getPreviousCompletedPeriod = () => {
  const now = new Date();
  const previousMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);

  return {
    month: allMonths[previousMonthDate.getMonth()],
    year: String(previousMonthDate.getFullYear()),
    monthIndex: previousMonthDate.getMonth(),
  };
};

const isCurrentOrFutureMonth = (monthName: string, yearValue: string) => {
  const monthIndex = allMonths.indexOf(String(monthName || '').toLowerCase());
  const yearNumber = Number(yearValue);

  if (monthIndex < 0 || !Number.isFinite(yearNumber)) return false;

  const now = new Date();
  const currentYear = now.getFullYear();
  const currentMonthIndex = now.getMonth();

  return (
    yearNumber > currentYear ||
    (yearNumber === currentYear && monthIndex >= currentMonthIndex)
  );
};

const getSafeInventoryMonth = (monthName: string, yearValue: string) => {
  const previousCompleted = getPreviousCompletedPeriod();

  if (!monthName || !allMonths.includes(String(monthName).toLowerCase())) {
    return previousCompleted.month;
  }

  if (isCurrentOrFutureMonth(monthName, yearValue)) {
    return previousCompleted.month;
  }

  return String(monthName).toLowerCase();
};

const getSafeInventoryYear = (yearValue: string) => {
  const parsed = Number(yearValue);
  const previousCompleted = getPreviousCompletedPeriod();

  if (!Number.isFinite(parsed) || parsed > new Date().getFullYear()) {
    return previousCompleted.year;
  }

  return String(yearValue);
};

const quarterToMonths: Record<Quarter, string[]> = {
  Q1: ['january', 'february', 'march'],
  Q2: ['april', 'may', 'june'],
  Q3: ['july', 'august', 'september'],
  Q4: ['october', 'november', 'december'],
};

const INVENTORY_BUCKETS: AgeingBucket[] = [
  { key: 'zeroToNinety', label: '0–90 Days', color: '#7B9A6D' },
  { key: 'ninetyOneToOneEighty', label: '91–180 Days', color: '#FDD36F' },
  { key: 'oneEightyOneToTwoSeventy', label: '181–270 Days', color: '#ED9F50' },
  { key: 'twoSeventyOneToThreeSixtyFive', label: '271–365 Days', color: '#C49466' },
  { key: 'threeSixtyFivePlus', label: '365+ Days', color: '#B75A5A' },
];

const AGEING_TREND_BUCKET_OPTIONS = [
  {
    label: '181–270 Days',
    value: '181-270 days',
    column: 'inv-age-181-to-270-days',
    color: '#ED9F50',
  },
  {
    label: '271–365 Days',
    value: '271-365 days',
    column: 'inv-age-271-to-365-days',
    color: '#C49466',
  },
  {
    label: '365+ Days',
    value: '365+ days',
    column: 'inv-age-365-plus-days',
    color: '#B75A5A',
  },
];

const INVENTORY_ACTION_LOGIC: ActionLogicItem[] = [
  {
    key: 'healthy',
    label: 'Healthy',
    description: 'Stock covers 0–90 days',
    color: '#7B9A6D',
  },
  {
    key: 'high_alert',
    label: 'High Alert',
    description: 'Shipment Required',
    color: '#B75A5A',
  },
  {
    key: 'discount',
    label: 'Discount',
    description: 'Stock aged 91–180 days',
    color: '#FDD36F',
  },
  {
    key: 'liquidate',
    label: 'Liquidate',
    description: 'Stock older than 180 days',
    color: '#ED9F50',
  },
  {
    key: 'unfulfillable',
    label: 'Unfulfillable',
    description: 'Remove or dispose stock',
    color: '#3A8EA4',
  },
  {
    key: 'estimated_storage_cost',
    label: 'Estimate Storage',
    description: 'Monthly storage estimate',
    color: '#C49466',
  },
];

const toNum = (v: any) => {
  if (v === null || v === undefined) return 0;
  if (typeof v === 'number') return Number.isFinite(v) ? v : 0;
  const n = Number(String(v).replace(/,/g, '').trim());
  return Number.isFinite(n) ? n : 0;
};

const normalizeInventoryKey = (key: string) =>
  String(key || '')
    .toLowerCase()
    .trim()
    .replace(/[%()]/g, '')
    .replace(/[-\s]+/g, '_')
    .replace(/__+/g, '_');

const pickInventoryNumber = (
  row: InventoryCurrentRow,
  keys: string[]
): number => {
  if (!row) return 0;

  for (const key of keys) {
    const directValue = row?.[key];

    if (directValue !== null && directValue !== undefined && directValue !== '') {
      const value = toNum(directValue);
      if (value !== 0) return value;
    }
  }

  const normalizedTargetKeys = keys.map(normalizeInventoryKey);

  for (const [rowKey, rowValue] of Object.entries(row)) {
    const normalizedRowKey = normalizeInventoryKey(rowKey);

    if (normalizedTargetKeys.includes(normalizedRowKey)) {
      const value = toNum(rowValue);
      if (value !== 0) return value;
    }
  }

  return 0;
};

const getInventoryRowProductName = (row: InventoryCurrentRow) => {
  const possibleKeys = [
    'product_name',
    'Product Name',
    'product name',
    'productName',
    'product_name_x',
    'product_name_y',
    'parent_product_name',
    'item_name',
    'item-name',
    'itemName',
    'title',
    'product',
    'Product',
    'asin_title',
    'item-title',
    'item_title',
    'name',
  ];

  for (const key of possibleKeys) {
    const value = row?.[key];

    if (
      value !== null &&
      value !== undefined &&
      String(value).trim() !== '' &&
      String(value).trim().toLowerCase() !== 'nan' &&
      String(value).trim().toLowerCase() !== 'none' &&
      String(value).trim().toLowerCase() !== 'null'
    ) {
      return String(value).trim();
    }
  }

  return getInventoryRowSku(row) || 'Unknown Product';
};

const getInventoryRowSku = (row: InventoryCurrentRow) =>
  String(row?.sku || row?.SKU || row?.seller_sku || row?.fnsku || '').trim();

const getInventoryAgeValue = (row: InventoryCurrentRow, key: string) =>
  toNum(row?.[key]);

const getInventoryRowTotalUnits = (row: InventoryCurrentRow) => {
  const bucketTotal =
    getInventoryAgeValue(row, 'inv-age-0-to-90-days') +
    getInventoryAgeValue(row, 'inv-age-91-to-180-days') +
    getInventoryAgeValue(row, 'inv-age-181-to-270-days') +
    getInventoryAgeValue(row, 'inv-age-271-to-365-days') +
    getInventoryAgeValue(row, 'inv-age-365-plus-days');

  return bucketTotal || toNum(row?.available ?? row?.total_quantity);
};

const getInventoryRowUnfulfillableUnits = (row: InventoryCurrentRow) => {
  return pickInventoryNumber(row, [
    'unfulfillableUnits',
    'unfulfillable_units',
    'unfulfillable-quantity',
    'unfulfillable_quantity',
    'Unfulfillable Units',
    'unfulfillable units',
    'unsellableUnits',
    'unsellable_units',
    'unsellable-quantity',
    'unsellable_quantity',
    'afn_unsellable_quantity',
    'afn-unsellable-quantity',
    'afn-unsellable-quantity',
  ]);
};

const getInventoryRowCoverageRatio = (row: InventoryCurrentRow) => {
  return pickInventoryNumber(row, [
    'coverageRatio',
    'coverage_ratio',
    'coverage_ratio_in_months',
    'coverage_ratio_months',
    'Coverage Ratio (in Months)',
    'Coverage Ratio (In Months)',
    'coverage ratio in months',
    'coverage ratio',
    'months_of_cover',
    'month_cover',
    'stock_cover_months',
    'cover_months',
  ]);
};

const getInventoryRowEstimatedStorageCost = (row: InventoryCurrentRow) => {
  return pickInventoryNumber(row, [
    // actual backend key used by dropdown/dashboard
    'estimated-storage-cost-next-month',
    'estimated_storage_cost_next_month',
    'estimatedStorageCostNextMonth',
    'Estimated Storage Cost Next Month',
    'estimated storage cost next month',

    // existing fallback keys
    'estimatedStorageCost',
    'estimated_storage_cost',
    'estimated-storage-cost',
    'Estimated Storage Cost',
    'estimated storage cost',
    'estimate_storage',
    'estimated_storage',
    'monthly_storage_cost',
    'monthly_storage_fee',
    'storage_cost',
    'storage_fee',
  ]);
};

const getShortMonthLabel = (monthName?: string) => {
  const clean = String(monthName || '').trim();
  return clean ? clean.slice(0, 3) : '-';
};

const buildInventoryInsightsFromResponses = (
  inventoryResponses: InventoryCurrentApiResponse[],
  ageSummaryResponses: InventoryAgeSummaryApiResponse[],
  selectedTrendBucket: string,
  countryName: string
): InventoryInsightsData => {
  const rows = inventoryResponses.flatMap((res) => {
    const directRows = Array.isArray(res?.rows) ? res.rows : [];

    const categoryRows = res?.categories
      ? Object.values(res.categories).flatMap((category) =>
        Array.isArray(category?.items) ? category.items : []
      )
      : [];

    return directRows.length ? directRows : categoryRows;
  });

  const productRows = rows.filter((row) => {
    const productName = getInventoryRowProductName(row).trim().toLowerCase();
    const sku = getInventoryRowSku(row).trim().toLowerCase();

    return (
      productName !== 'total' &&
      sku !== 'total' &&
      productName !== 'grand total' &&
      sku !== 'grand total'
    );
  });

  const heatmapData: AgeingRiskHeatmapRow[] = productRows
    .map((row) => {
      const zeroToNinety = getInventoryAgeValue(row, 'inv-age-0-to-90-days');
      const ninetyOneToOneEighty = getInventoryAgeValue(row, 'inv-age-91-to-180-days');
      const oneEightyOneToTwoSeventy = getInventoryAgeValue(row, 'inv-age-181-to-270-days');
      const twoSeventyOneToThreeSixtyFive = getInventoryAgeValue(row, 'inv-age-271-to-365-days');
      const threeSixtyFivePlus = getInventoryAgeValue(row, 'inv-age-365-plus-days');

      const totalUnits =
        zeroToNinety +
        ninetyOneToOneEighty +
        oneEightyOneToTwoSeventy +
        twoSeventyOneToThreeSixtyFive +
        threeSixtyFivePlus;

      return {
        productName: getInventoryRowProductName(row),
        sku: getInventoryRowSku(row),
        zeroToNinety,
        ninetyOneToOneEighty,
        oneEightyOneToTwoSeventy,
        twoSeventyOneToThreeSixtyFive,
        threeSixtyFivePlus,
        totalUnits: totalUnits || getInventoryRowTotalUnits(row),

        unsellableUnits: getInventoryRowUnfulfillableUnits(row),
        coverageRatio: getInventoryRowCoverageRatio(row),
        estimatedStorageCost: getInventoryRowEstimatedStorageCost(row),
      };
    })
    .filter(
  (row) =>
    toNum(row.totalUnits) > 0 ||
    toNum((row as any).unsellableUnits) > 0 ||
    toNum((row as any).coverageRatio) > 0 ||
    toNum((row as any).estimatedStorageCost) > 0
);

  const overallAgeing = heatmapData.reduce(
    (acc, row) => {
      acc.zeroToNinety += toNum(row.zeroToNinety);
      acc.ninetyOneToOneEighty += toNum(row.ninetyOneToOneEighty);
      acc.oneEightyOneToTwoSeventy += toNum(row.oneEightyOneToTwoSeventy);
      acc.twoSeventyOneToThreeSixtyFive += toNum(row.twoSeventyOneToThreeSixtyFive);
      acc.threeSixtyFivePlus += toNum(row.threeSixtyFivePlus);

      return acc;
    },
    {
      zeroToNinety: 0,
      ninetyOneToOneEighty: 0,
      oneEightyOneToTwoSeventy: 0,
      twoSeventyOneToThreeSixtyFive: 0,
      threeSixtyFivePlus: 0,
    }
  );

  const donutData: DonutChartItem[] = [
    {
      bucket: '0–90 Days',
      units: overallAgeing.zeroToNinety,
      color: '#7B9A6D',
    },
    {
      bucket: '91–180 Days',
      units: overallAgeing.ninetyOneToOneEighty,
      color: '#FDD36F',
    },
    {
      bucket: '181–270 Days',
      units: overallAgeing.oneEightyOneToTwoSeventy,
      color: '#ED9F50',
    },
    {
      bucket: '271–365 Days',
      units: overallAgeing.twoSeventyOneToThreeSixtyFive,
      color: '#C49466',
    },
    {
      bucket: '365+ Days',
      units: overallAgeing.threeSixtyFivePlus,
      color: '#B75A5A',
    },
  ].filter((item) => item.units > 0);

  const donutTotalUnits = donutData.reduce(
    (sum, item) => sum + toNum(item.units),
    0
  );

  const selectedTrendOption =
    AGEING_TREND_BUCKET_OPTIONS.find(
      (option) => option.value === selectedTrendBucket
    ) || AGEING_TREND_BUCKET_OPTIONS[2];

  const monthSummaryMap = new Map<
    string,
    {
      month: string;
      month_number: number;
      year: number;
      value: number;
    }
  >();

  ageSummaryResponses.forEach((res) => {
    if (!res?.success) return;

    if (Array.isArray(res.month_summary)) {
      res.month_summary.forEach((item) => {
        const key = `${item.year}-${item.month_number}`;
        const previous = monthSummaryMap.get(key);

        monthSummaryMap.set(key, {
          month: item.month,
          month_number: item.month_number,
          year: item.year,
          value:
            (previous?.value || 0) +
            toNum(item?.totals?.[selectedTrendOption.column]),
        });
      });
    }

    if (Array.isArray(res.age_summary)) {
      res.age_summary.forEach((item) => {
        if (item.column !== selectedTrendOption.column) return;

        const monthNumber =
          item.month_number ||
          allMonths.indexOf(String(item.month || '').toLowerCase()) + 1;

        const key = `${item.year}-${monthNumber}`;
        const previous = monthSummaryMap.get(key);

        monthSummaryMap.set(key, {
          month: item.month,
          month_number: monthNumber,
          year: item.year,
          value: (previous?.value || 0) + toNum(item.units),
        });
      });
    }
  });

  const trendData: AgeingTrendItem[] = Array.from(monthSummaryMap.values())
    .sort((a, b) => a.year - b.year || a.month_number - b.month_number)
    .map((item) => ({
      label: getShortMonthLabel(item.month),
      value: toNum(item.value),
    }));

  const sortedMonthSummaryValues = Array.from(monthSummaryMap.values()).sort(
    (a, b) => a.year - b.year || a.month_number - b.month_number
  );

  const trendAllSeriesData: AgeingTrendAllSeriesItem[] =
    AGEING_TREND_BUCKET_OPTIONS.map((bucket) => ({
      bucketValue: bucket.value,
      bucketLabel: bucket.label,
      color: bucket.color,
      data: sortedMonthSummaryValues.map((item) => ({
        label: getShortMonthLabel(item.month),
        value: ageSummaryResponses.reduce((sum, res) => {
          const monthSummary = res.month_summary?.find(
            (m) =>
              m.year === item.year &&
              m.month_number === item.month_number
          );

          return sum + toNum(monthSummary?.totals?.[bucket.column]);
        }, 0),
      })),
    }));

  const totalHealthy = heatmapData.reduce(
    (sum, row) => sum + toNum(row.zeroToNinety),
    0
  );

  const totalHighAlert = heatmapData.reduce(
    (sum, row) => sum + toNum(row.threeSixtyFivePlus),
    0
  );

  const totalDiscount = heatmapData.reduce(
    (sum, row) => sum + toNum(row.ninetyOneToOneEighty),
    0
  );

  const totalLiquidate = heatmapData.reduce(
    (sum, row) =>
      sum +
      toNum(row.oneEightyOneToTwoSeventy) +
      toNum(row.twoSeventyOneToThreeSixtyFive) +
      toNum(row.threeSixtyFivePlus),
    0
  );

  const totalUnfulfillable = heatmapData.reduce(
    (sum, row) => sum + toNum((row as any).unsellableUnits),
    0
  );

  const getEstimatedStorageCostFromCategories = () => {
  return inventoryResponses.reduce((sum, res) => {
    const category = (res?.categories as any)?.estimated_storage_cost;

    return (
      sum +
      toNum(
        category?.value ??
        category?.total ??
        category?.total_value ??
        category?.estimated_storage_cost ??
        category?.storage_cost ??
        category?.next_month_storage_cost
      )
    );
  }, 0);
};

 const totalEstimatedStorageCostFromCategories =
  getEstimatedStorageCostFromCategories();

const totalEstimatedStorageCost =
  totalEstimatedStorageCostFromCategories ||
  heatmapData.reduce(
    (sum, row) => sum + toNum((row as any).estimatedStorageCost),
    0
  );

  const actions: ActionCardItem[] = [
    {
      key: 'healthy',
      label: 'Healthy',
      count: totalHealthy,
      displayValue: totalHealthy,
      skuCount: heatmapData.filter((row) => toNum(row.zeroToNinety) > 0).length,
      unitCount: totalHealthy,
      description: 'Stock covers 0–90 days',
      color: '#7B9A6D',
      backgroundColor: '#ffffff',
    },
    {
      key: 'high_alert',
      label: 'High Alert',
      count: totalHighAlert,
      displayValue: totalHighAlert,
      skuCount: heatmapData.filter((row) => toNum(row.threeSixtyFivePlus) > 0).length,
      unitCount: totalHighAlert,
      description: 'Shipment Required',
      color: '#B75A5A',
      backgroundColor: '#ffffff',
    },
    {
      key: 'discount',
      label: 'Discount',
      count: totalDiscount,
      displayValue: totalDiscount,
      skuCount: heatmapData.filter((row) => toNum(row.ninetyOneToOneEighty) > 0).length,
      unitCount: totalDiscount,
      description: 'Stock aged 91–180 days',
      color: '#FDD36F',
      backgroundColor: '#ffffff',
    },
    {
      key: 'liquidate',
      label: 'Liquidate',
      count: totalLiquidate,
      displayValue: totalLiquidate,
      skuCount: heatmapData.filter(
        (row) =>
          toNum(row.oneEightyOneToTwoSeventy) > 0 ||
          toNum(row.twoSeventyOneToThreeSixtyFive) > 0 ||
          toNum(row.threeSixtyFivePlus) > 0
      ).length,
      unitCount: totalLiquidate,
      description: 'Stock older than 180 days',
      color: '#ED9F50',
      backgroundColor: '#ffffff',
    },
    {
      key: 'unfulfillable',
      label: 'Unfulfillable',
      count: totalUnfulfillable,
      displayValue: totalUnfulfillable,
      skuCount: heatmapData.filter(
        (row) => toNum((row as any).unsellableUnits) > 0
      ).length,
      unitCount: totalUnfulfillable,
      description: 'Remove or dispose stock',
      color: '#3A8EA4',
      backgroundColor: '#ffffff',
    },
   {
  key: 'estimated_storage_cost',
  label: 'Estimated Storage Cost',
  count: totalEstimatedStorageCost,
  displayValue: `${getCurrencySymbol(getCurrencyForCountry(countryName))}${totalEstimatedStorageCost.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`,
  skuCount: heatmapData.filter(
    (row) => toNum((row as any).estimatedStorageCost) > 0
  ).length,
  unitCount: heatmapData.reduce(
    (sum, row) => sum + toNum(row.totalUnits),
    0
  ),
  description: 'Monthly storage estimate',
  color: '#C49466',
  backgroundColor: '#ffffff',
},
  ];

  return {
    heatmapBuckets: INVENTORY_BUCKETS,
    heatmapData,
    donutSku: '',
    donutData,
    donutTotalUnits,
    trendSelectedBucket: selectedTrendOption.value,
    trendData,
    trendLineColor: selectedTrendOption.color,
    trendAllSeriesData,
    trendBucketOptions: AGEING_TREND_BUCKET_OPTIONS.map((bucket) => ({
      label: bucket.label,
      value: bucket.value,
      color: bucket.color,
    })),
    actions,
    actionLogic: INVENTORY_ACTION_LOGIC,
  };
};

const getCurrencySymbol = (country: string | undefined): string => {
  switch (country) {
    case 'GBP':
      return '£';
    case 'INR':
      return '₹';
    case 'USD':
      return '$';
    case 'europe':
    case 'eu':
    case 'EUR':
      return '€';
    case 'CAD':
      return '$';
    case 'global':
      return '$';
    default:
      return '$';
  }
};

function getCurrencyForCountry(country: string): string {
  switch (country.toLowerCase()) {
    case 'uk':
      return 'GBP';
    case 'us':
      return 'USD';
    case 'canada':
      return 'CAD';
    case 'eu':
    case 'europe':
      return 'EUR';
    default:
      return 'USD';
  }
}

const DUMMY_SKU_DATA: SkuRow[] = [
  {
    s_no: 1,
    product_name: 'Sample Product A',
    sku_uk: 'UK-SKU-001',
    // sku_us: 'US-SKU-001',
    // sku_canada: 'CA-SKU-001',
    asin: 'B0DUMMY001',
    product_barcode: '1234567890123',
    price: 0,
    currency: 'GBP',
    month: 'January',
    year: '2026',
    landing_cost: 0,
  },
  {
    s_no: 2,
    product_name: 'Sample Product B',
    sku_uk: 'UK-SKU-002',
    // sku_us: 'US-SKU-002',
    // sku_canada: 'CA-SKU-002',
    asin: 'B0DUMMY002',
    product_barcode: '2234567890123',
    price: 0,
    currency: 'GBP',
    month: 'January',
    year: '2026',
    landing_cost: 0,
  },
  {
    s_no: 3,
    product_name: 'Sample Product C',
    sku_uk: 'UK-SKU-003',
    // sku_us: 'US-SKU-003',
    // sku_canada: 'CA-SKU-003',
    asin: 'B0DUMMY003',
    product_barcode: '3234567890123',
    price: 0,
    currency: 'GBP',
    month: 'January',
    year: '2026',
    landing_cost: 0,
  },
  {
    s_no: 4,
    product_name: 'Sample Product D',
    sku_uk: 'UK-SKU-004',
    // sku_us: 'US-SKU-004',
    // sku_canada: 'CA-SKU-004',
    asin: 'B0DUMMY004',
    product_barcode: '4234567890123',
    price: 0,
    currency: 'GBP',
    month: 'January',
    year: '2026',
    landing_cost: 0,
  },
  {
    s_no: 5,
    product_name: 'Sample Product E',
    sku_uk: 'UK-SKU-005',
    // sku_us: 'US-SKU-005',
    // sku_canada: 'CA-SKU-005',
    asin: 'B0DUMMY005',
    product_barcode: '5234567890123',
    price: 0,
    currency: 'GBP',
    month: 'January',
    year: '2026',
    landing_cost: 0,
  },
];

const DUMMY_ASP_DATA: Record<string, number> = {
  'Sample Product A': 0,
  'Sample Product B': 0,
  'Sample Product C': 0,
  'Sample Product A_uk': 0,
  'Sample Product B_uk': 0,
  'Sample Product C_uk': 0,
  'Sample Product A_us': 0,
  'Sample Product B_us': 0,
  'Sample Product C_us': 0,
  'Sample Product A_canada': 0,
  'Sample Product B_canada': 0,
  'Sample Product C_canada': 0,
};

const DUMMY_CURRENCY_RATES: Record<string, number> = {
  GBP: 1,
  USD: 1,
  CAD: 1,
  EUR: 1,
  INR: 1,
  gbp: 1,
  usd: 1,
  cad: 1,
  eur: 1,
  inr: 1,
  GBP_uk: 1,
  USD_us: 1,
  CAD_canada: 1,
  EUR_europe: 1,
  EUR_eu: 1,
  USD_global: 1,
};

const DUMMY_WAREHOUSE_DATA = [
  {
    s_no: 1,
    // sku_us: 'US-SKU-001',
    sku_uk: 'UK-SKU-001',
    local_stock: 0,
    in_transit_units: 0,
    month: 'January',
    year: '2026',
  },
  {
    s_no: 2,
    // sku_us: 'US-SKU-002',
    sku_uk: 'UK-SKU-002',
    local_stock: 0,
    in_transit_units: 0,
    month: 'January',
    year: '2026',
  },
  {
    s_no: 3,
    // sku_us: 'US-SKU-002',
    sku_uk: 'UK-SKU-003',
    local_stock: 0,
    in_transit_units: 0,
    month: 'January',
    year: '2026',
  },
  {
    s_no: 4,
    // sku_us: 'US-SKU-002',
    sku_uk: 'UK-SKU-004',
    local_stock: 0,
    in_transit_units: 0,
    month: 'January',
    year: '2026',
  },
  {
    s_no: 5,
    // sku_us: 'US-SKU-002',
    sku_uk: 'UK-SKU-005',
    local_stock: 0,
    in_transit_units: 0,
    month: 'January',
    year: '2026',
  },
];



export default function InputCostPage({ params }: Params) {
  const { countryName: countryNameRaw, month: monthRaw, year: yearRaw } = use(params);
  const countryName = decodeURIComponent(countryNameRaw ?? '').toLowerCase();
  const monthParam = decodeURIComponent(monthRaw ?? '');
  const yearParam = decodeURIComponent(yearRaw ?? '');

  const [skuData, setSkuData] = useState<SkuRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isEditing, setIsEditing] = useState(false);
  const [editedPrices, setEditedPrices] = useState<Record<string, number>>({});
  const [showModal, setShowModal] = useState(false);
  const [modalMessage, setModalMessage] = useState('');
  const [visibleColumns, setVisibleColumns] = useState<string[]>([]);
  const [currencyRates, setCurrencyRates] = useState<Record<string, number>>({});
  const [aspData, setAspData] = useState<Record<string, number>>({});
  const [showMultiuseCountry, setShowMultiuseCountry] = useState(false);
  const [warehouseData, setWarehouseData] = useState<Record<string, any>[]>([]);
  const [warehouseColumns, setWarehouseColumns] = useState<string[]>([]);
  const [warehouseLoading, setWarehouseLoading] = useState(false);
  const [showWarehouseUpload, setShowWarehouseUpload] = useState(false);
  const [selectedWarehouseFile, setSelectedWarehouseFile] = useState<File | null>(null);
  const [warehouseUploadError, setWarehouseUploadError] = useState('');

  const { data: userData } = useGetUserDataQuery();
  const router = useRouter();

  const companyName =
    (userData as any)?.companyName ||
    (userData as any)?.company_name ||
    (userData as any)?.company ||
    "";

  const brandName =
    (userData as any)?.brandName ||
    (userData as any)?.brand_name ||
    (userData as any)?.brand ||
    "";


  const isNA =
    monthParam?.toLowerCase() === 'na' ||
    yearParam?.toLowerCase() === 'na';

  type InputCostTab = 'inventory-insights' | 'sku-info' | 'extra';
  const [activeTab, setActiveTab] = useState<InputCostTab>('inventory-insights');

  const getDefaultMonth = () => {
    const clean = String(monthParam || '').toLowerCase();
    const safeYear = getDefaultYear();

    return getSafeInventoryMonth(clean, safeYear);
  };

  const getDefaultYear = () => {
    const parsed = Number(yearParam);
    const previousCompleted = getPreviousCompletedPeriod();

    if (!Number.isFinite(parsed) || parsed <= 2000) {
      return previousCompleted.year;
    }

    if (parsed > new Date().getFullYear()) {
      return previousCompleted.year;
    }

    return String(parsed);
  };

  const [range, setRange] = useState<RangeType>('monthly');
  const [selectedMonth, setSelectedMonth] = useState<string>(getDefaultMonth());
  const [selectedQuarter, setSelectedQuarter] = useState<Quarter | ''>('');
  const [selectedYear, setSelectedYear] = useState<string>(getDefaultYear());

  const [selectedAgeingTrendBucket, setSelectedAgeingTrendBucket] =
    useState<string>('365+ days');

  const [inventoryInsightsData, setInventoryInsightsData] =
    useState<InventoryInsightsData | null>(null);

  const [inventoryInsightsLoading, setInventoryInsightsLoading] = useState(false);

  const [inventoryInsightsError, setInventoryInsightsError] =
    useState<string | null>(null);

  const [inventoryRawResponses, setInventoryRawResponses] = useState<{
    inventory: InventoryCurrentApiResponse[];
    ageSummary: InventoryAgeSummaryApiResponse[];
  } | null>(null);

  const yearOptions = useMemo(() => {
    const currentYear = new Date().getFullYear();
    return Array.from({ length: 6 }, (_, index) => String(currentYear - index));
  }, []);

  const handleRangeChange = (nextRange: RangeType) => {
    setRange(nextRange);

    if (nextRange === 'monthly') {
      setSelectedQuarter('');
      if (!selectedMonth) setSelectedMonth(getDefaultMonth());
    }

    if (nextRange === 'quarterly') {
      setSelectedMonth('');
      if (!selectedQuarter) setSelectedQuarter('Q1');
    }

    if (nextRange === 'yearly') {
      setSelectedMonth('');
      setSelectedQuarter('');
    }
  };

  const handleMonthChange = (nextMonth: string) => {
    setSelectedMonth(getSafeInventoryMonth(nextMonth, selectedYear));
  };

  const handleQuarterChange = (nextQuarter: string) => {
    setSelectedQuarter(nextQuarter as Quarter);
  };

  const handleYearChange = (nextYear: string) => {
    const safeYear = getSafeInventoryYear(nextYear);

    setSelectedYear(safeYear);

    if (range === 'monthly') {
      setSelectedMonth((prev) => getSafeInventoryMonth(prev, safeYear));
    }

    if (range === 'quarterly') {
      const now = new Date();
      const currentYear = now.getFullYear();
      const selectedYearNumber = Number(safeYear);

      if (selectedYearNumber >= currentYear) {
        const previousCompleted = getPreviousCompletedPeriod();
        const monthIndex = previousCompleted.monthIndex;

        const safeQuarter: Quarter =
          monthIndex <= 2 ? 'Q1' : monthIndex <= 5 ? 'Q2' : monthIndex <= 8 ? 'Q3' : 'Q4';

        setSelectedQuarter(safeQuarter);
      }
    }
  };

  const isEmptyCellValue = (value: any) => {
    if (value === null || value === undefined) return true;

    if (typeof value === 'number') {
      return Number.isNaN(value);
    }

    if (typeof value === 'string') {
      const normalized = value.trim().toLowerCase();
      return (
        normalized === '' ||
        normalized === 'nan' ||
        normalized === 'none' ||
        normalized === 'null' ||
        normalized === 'undefined' ||
        normalized === '-'
      );
    }

    return false;
  };

  const isColumnEmpty = (data: SkuRow[], columnName: string) => {
    return data.every((row) => isEmptyCellValue(row[columnName]));
  };

  // Remove local_stock and in_transit_units from SKU tab
  const getVisibleColumns = (data: SkuRow[]) => {
    if (!data || data.length === 0) return [] as string[];

    const baseColumns: string[] = ['s_no', 'product_name'];
    let skuColumns: string[] = [];
    let grossMarginColumns: string[] = [];

    if (countryName === 'global') {
      const potentialSkuColumns = ['sku_uk', 'sku_us', 'sku_canada'];

      skuColumns = potentialSkuColumns.filter((col) => !isColumnEmpty(data, col));

      grossMarginColumns = skuColumns.map((skuCol) => {
        const c = skuCol.replace('sku_', '');
        return `gross_margin_${c}`;
      });
    } else {
      const skuColumn = `sku_${countryName}`;
      if (!isColumnEmpty(data, skuColumn)) {
        skuColumns.push(skuColumn);
      }
      grossMarginColumns.push(`gross_margin_${countryName}`);
    }

    const otherColumns = [
      'asin',
      'product_barcode',
      'month_year',
      'price',
    ];

    const visibleOtherColumns = otherColumns.filter((col) => {
      if (col === 'month_year') {
        return data.some((row) => row.month || row.year);
      }
      return !isColumnEmpty(data, col);
    });

    return [...baseColumns, ...skuColumns, ...visibleOtherColumns, ...grossMarginColumns];
  };

  const getColumnDisplayName = (column: string): React.ReactNode => {
    switch (column) {
      case 's_no':
        return 'S.No.';
      case 'product_name':
        return 'Product Name';
      case 'sku_uk':
        return countryName === 'global' ? 'SKU (UK)' : 'SKU';
      case 'sku_us':
        return countryName === 'global' ? 'SKU (US)' : 'SKU';
      case 'sku_canada':
        return countryName === 'global' ? 'SKU (CANADA)' : 'SKU';
      case 'asin':
        return 'ASIN';
      case 'product_barcode':
        return 'Product Barcode';
      case 'month_year':
        return 'Month / Year';
      case 'price':
        return 'Landing Cost';
      default:
        if (column.startsWith('sku_')) {
          if (countryName !== 'global') return 'SKU';

          const c = column.replace('sku_', '').toUpperCase();
          return `SKU (${c})`;
        }
        if (column.startsWith('gross_margin_')) {
          const c = column.replace('gross_margin_', '').toUpperCase();

          return (
            <div className="text-center leading-tight">
              <span className="hidden xl:inline whitespace-nowrap">
                {countryName === 'global'
                  ? `Gross Margin (%) ${c}`
                  : 'Gross Margin (%)'}
              </span>

              <span className="xl:hidden flex flex-col items-center justify-center whitespace-normal">
                <span>Gross Margin</span>
                <span>
                  (%) {countryName === 'global' ? c : ''}
                </span>
              </span>

              <span
                className="mt-0.5 inline-flex cursor-pointer align-middle"
                title="*Gross Margin calculation is based on previous month’s ASP"
              >
                <i className="fa-solid fa-circle-info ml-1" style={{ color: '#f8edcf' }}></i>
              </span>
            </div>
          );
        }
        return column.charAt(0).toUpperCase() + column.slice(1);
    }
  };

  const getCurrencyRate = (currency: string | undefined, country: string) => {
    if (!currency || !currencyRates || Object.keys(currencyRates).length === 0) return 1;
    const possibleKeys = [
      `${currency}_${country}`,
      `${currency.toLowerCase()}_${country.toLowerCase()}`,
      `${currency.toUpperCase()}_${country.toLowerCase()}`,
      currency,
      currency.toLowerCase(),
      currency.toUpperCase(),
    ];
    for (const key of possibleKeys) {
      if (currencyRates[key] !== undefined) return currencyRates[key];
    }
    return 1;
  };

  const getAspForProduct = (productName: string, targetCountry: string | null = null) => {
    if (!aspData || Object.keys(aspData).length === 0) return null;

    if (targetCountry && countryName === 'global') {
      const countrySpecificKey = `${productName}_${targetCountry}`;
      if (aspData[countrySpecificKey] !== undefined) return aspData[countrySpecificKey];
      for (const key in aspData) {
        if (key.includes(`_${targetCountry}`) && key.includes(productName)) return aspData[key];
      }
    }

    if (countryName === 'global') {
      if (aspData[productName] !== undefined) return aspData[productName];
      for (const key in aspData) {
        if (key.includes(productName) || productName.includes(key)) return aspData[key];
      }
    } else {
      return aspData[productName] ?? null;
    }

    return null;
  };

  const calculateGrossMargin = (
    price: number | undefined,
    sourceCurrency: string | undefined,
    targetCountry: string,
    productName: string
  ): string => {
    try {
      const asp = getAspForProduct(productName, targetCountry);

      // ✅ Preview / dummy case → show 0 instead of N/A
      if (isNA) return '0.00';

      if (!asp || asp === 0) return '0.00';

      const safePrice = price ?? 0;

      let convertedPrice: number;

      if (countryName === 'global') {
        const targetCurrency = getCurrencyForCountry(targetCountry);

        if (sourceCurrency === targetCurrency) {
          convertedPrice = safePrice;
        } else {
          const sourceToUsdRate = getCurrencyRate(sourceCurrency, 'global') || 1;
          const usdToTargetRate = getCurrencyRate(targetCurrency, targetCountry) || 1;
          convertedPrice = safePrice * sourceToUsdRate * usdToTargetRate;
        }
      } else {
        const currencyRate = getCurrencyRate(sourceCurrency, targetCountry);
        convertedPrice = safePrice * currencyRate;
      }

      const grossMargin = ((asp - convertedPrice) / asp) * 100;

      // ✅ Handle NaN/Infinity safely
      if (!isFinite(grossMargin)) return '0.00';

      return grossMargin.toFixed(2);
    } catch {
      return '0.00';
    }
  };

  const fetchCurrencyRates = async () => {
    if (isNA) {
      setCurrencyRates(DUMMY_CURRENCY_RATES);
      return;
    }

    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) return;

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/currency-rates`, {
        method: 'GET',
        headers: { Authorization: `Bearer ${token}` },
      });
      if (response.ok) {
        const rates: Array<{ user_currency: string; country: string; conversion_rate: number }> =
          await response.json();
        const map: Record<string, number> = {};
        rates.forEach((rate) => {
          const keys = [
            `${rate.user_currency}_${rate.country}`,
            `${rate.user_currency.toLowerCase()}_${rate.country.toLowerCase()}`,
            `${rate.user_currency.toUpperCase()}_${rate.country.toLowerCase()}`,
            rate.user_currency,
            rate.user_currency.toLowerCase(),
            rate.user_currency.toUpperCase(),
          ];
          keys.forEach((k) => (map[k] = rate.conversion_rate));
        });
        setCurrencyRates(map);
      }
    } catch (e) {
      console.error('Error fetching currency rates', e);
    }
  };

  const fetchAspData = async () => {
    if (isNA) {
      setAspData(DUMMY_ASP_DATA);
      return;
    }

    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) return;

    const monthNames = [
      'january', 'february', 'march', 'april', 'may', 'june',
      'july', 'august', 'september', 'october', 'november', 'december',
    ];

    const normalizedMonth = (() => {
      const m = monthParam.toLowerCase();
      if (/^\d+$/.test(m)) {
        const idx = Math.min(Math.max(parseInt(m, 10) - 1, 0), 11);
        return monthNames[idx];
      }
      return monthNames.includes(m) ? m : monthNames[new Date().getMonth()];
    })();

    const normalizedYear = (() => {
      const y = parseInt(yearParam, 10);
      if (!isNaN(y) && y > 2000 && y < 2100) return y;
      return new Date().getFullYear();
    })();

    try {
      let currentMonthIndex = monthNames.indexOf(normalizedMonth);
      let currentYear = normalizedYear;

      for (let attempt = 0; attempt < 12; attempt++) {
        const monthName = monthNames[currentMonthIndex];
        try {
          const response = await fetch(
            `${process.env.NEXT_PUBLIC_API_BASE_URL}/asp-data?country=${countryName}&month=${monthName}&year=${currentYear}`,
            {
              method: 'GET',
              headers: { Authorization: `Bearer ${token}` },
            }
          );
          if (response.ok) {
            const aspArray: Array<{ product_name: string; asp: number; source_country?: string }> =
              await response.json();
            const map: Record<string, number> = {};
            aspArray.forEach((item) => {
              map[item.product_name] = item.asp;
            });
            setAspData(map);
            return;
          }
        } catch {
          // continue
        }
        currentMonthIndex--;
        if (currentMonthIndex < 0) {
          currentMonthIndex = 11;
          currentYear--;
        }
      }
      setAspData({});
    } catch (e) {
      console.error('Error in fetchAspData', e);
      setAspData({});
    }
  };

  const fetchSkuData = async () => {
    if (isNA) {
      const previewCurrency =
        countryName === 'uk'
          ? 'GBP'
          : countryName === 'us'
            ? 'USD'
            : countryName === 'canada'
              ? 'CAD'
              : countryName === 'eu' || countryName === 'europe'
                ? 'EUR'
                : 'USD';

      const previewRows = DUMMY_SKU_DATA.map((row) => ({
        ...row,
        currency: countryName === 'global' ? row.currency : previewCurrency,
        month: 'January',
        year: '2026',
      }));

      const sorted = [...previewRows].sort((a, b) => (a.s_no ?? 0) - (b.s_no ?? 0));
      setSkuData(sorted);
      setVisibleColumns(getVisibleColumns(sorted));
      setLoading(false);
      setError(null);
      return;
    }

    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) {
      setError('Authorization token is missing');
      setLoading(false);
      return;
    }

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/skuprice`, {
        method: 'GET',
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!response.ok) throw new Error('Failed to fetch data');
      const data: SkuRow[] = await response.json();
      const sorted = [...data].sort((a, b) => (a.s_no ?? 0) - (b.s_no ?? 0));
      setSkuData(sorted);
      setVisibleColumns(getVisibleColumns(sorted));
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchSkuData();
    fetchCurrencyRates();
    fetchAspData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [countryName, monthParam, yearParam, isNA]);

  const handlePriceChange = (productName: string, value: string) => {
    setEditedPrices((prev) => ({
      ...prev,
      [productName]: value === '' ? NaN : parseFloat(value),
    }));
  };

  const saveChanges = async () => {
    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) {
      alert('Authorization token is missing');
      return;
    }
    if (Object.keys(editedPrices).length === 0) {
      alert('No changes to save.');
      return;
    }
    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/updatePrices`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ prices: editedPrices }),
      });
      if (response.ok) {
        const result = await response.json();
        setModalMessage('Prices updated successfully');
        setShowModal(true);
        setIsEditing(false);
        setEditedPrices({});
        if (result.data) {
          const sortedData: SkuRow[] = result.data.sort((a: SkuRow, b: SkuRow) => (a.s_no ?? 0) - (b.s_no ?? 0));
          setSkuData(sortedData);
          setVisibleColumns(getVisibleColumns(sortedData));
        } else {
          window.location.reload();
        }
      } else {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to update prices');
      }
    } catch (e: any) {
      alert(`Error: ${e.message}`);
      console.error('Update prices error:', e);
    }
  };

  const getOrderedWarehouseColumns = (cols: string[]) => {
    const preferredOrder = [
      's_no',
      'product_name',
      'sku_us',
      'sku_uk',
      'local_stock',
      'in_transit_units',
      'month',
      'year',
    ];

    const preferred = preferredOrder.filter((c) => cols.includes(c));
    const remaining = cols.filter((c) => !preferredOrder.includes(c));

    return [...preferred, ...remaining];
  };

  const fetchInventoryCurrentByPeriod = async (
    signal?: AbortSignal
  ): Promise<InventoryCurrentApiResponse> => {
    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    if (!token) throw new Error('Missing token');

    const url = new URL(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/inventory_current`
    );

    url.searchParams.set('country_key', String(countryName).toLowerCase());
    url.searchParams.set('year', String(selectedYear));

    if (range === 'monthly') {
  url.searchParams.set('range_type', 'monthly');
  url.searchParams.set(
    'month_name',
    getSafeInventoryMonth(selectedMonth, selectedYear)
  );
}

    if (range === 'quarterly') {
      url.searchParams.set('range_type', 'quarter_months');
      url.searchParams.set('quarter', String(selectedQuarter));
    }

    if (range === 'yearly') {
      url.searchParams.set('range_type', 'yearly');
    }

    const res = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`,
      },
      cache: 'no-store',
      signal,
    });

    const json = await res.json().catch(() => ({}));

    if (!res.ok) {
      throw new Error(
        json?.message ||
        json?.error ||
        `Failed to fetch inventory data for ${range}`
      );
    }

    return json;
  };

  const fetchSingleMonthInventoryAgeSummary = async (
    monthName: string,
    yearValue: string,
    countryValue: string,
    signal?: AbortSignal
  ): Promise<InventoryAgeSummaryApiResponse> => {
    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    if (!token) throw new Error('Missing token');

    const url = new URL(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/inventory_current_age_summary`
    );

    url.searchParams.set('country_key', String(countryValue).toLowerCase());
    url.searchParams.set('month_name', String(monthName).toLowerCase());
    url.searchParams.set('year', String(yearValue));

    const res = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`,
      },
      cache: 'no-store',
      signal,
    });

    const json = await res.json().catch(() => ({}));

    if (!res.ok) {
      throw new Error(
        json?.message ||
        json?.error ||
        `Failed to fetch inventory age summary for ${monthName} ${yearValue}`
      );
    }

    return json;
  };

  const fetchWarehouseData = async () => {
    if (isNA) {
      setWarehouseData(DUMMY_WAREHOUSE_DATA);
      setWarehouseColumns(getOrderedWarehouseColumns(Object.keys(DUMMY_WAREHOUSE_DATA[0] || {})));
      setWarehouseLoading(false);
      return;
    }

    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    const skuFallbackRows = skuData
      .filter((row) => row.local_stock !== undefined || row.in_transit_units !== undefined)
      .map((row, index) => ({
        s_no: row.s_no ?? index + 1,
        product_name: row.product_name ?? '',
        sku_us: row.sku_us ?? '',
        sku_uk: row.sku_uk ?? '',
        local_stock: row.local_stock ?? '',
        in_transit_units: row.in_transit_units ?? '',
        month: row.month ?? '',
        year: row.year ?? '',
      }));

    if (!token) {
      setWarehouseData(skuFallbackRows);
      setWarehouseColumns(
        skuFallbackRows.length > 0
          ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
          : []
      );
      return;
    }

    try {
      setWarehouseLoading(true);

      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/uploadWarehouseData?country=${countryName}`,
        {
          method: 'GET',
          headers: {
            Authorization: `Bearer ${token}`,
          },
        }
      );

      const result = await response.json().catch(() => ({}));

      if (!response.ok) {
        setWarehouseData(skuFallbackRows);
        setWarehouseColumns(
          skuFallbackRows.length > 0
            ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
            : []
        );
        return;
      }

      const rows = Array.isArray(result?.data) ? result.data : [];

      if (rows.length > 0) {
        const enrichedRows = rows.map((row: Record<string, any>, index: number) => {
          const matchedSku = skuData.find((sku) => {
            if (countryName === 'global') {
              return (
                (row.sku_uk && sku.sku_uk === row.sku_uk) ||
                (row.sku_us && sku.sku_us === row.sku_us) ||
                (row.sku_canada && sku.sku_canada === row.sku_canada)
              );
            }

            const skuKey = `sku_${countryName}`;
            return sku[skuKey] && row[skuKey] && sku[skuKey] === row[skuKey];
          });

          return {
            s_no: row.s_no ?? matchedSku?.s_no ?? index + 1,
            product_name: row.product_name ?? matchedSku?.product_name ?? '',
            ...row,
          };
        });

        const cols = Array.isArray(result?.columns)
          ? result.columns
          : Object.keys(enrichedRows[0]);

        setWarehouseData(enrichedRows);
        setWarehouseColumns(
          getOrderedWarehouseColumns(
            cols.includes('product_name') ? cols : ['product_name', ...cols]
          )
        );
      } else {
        setWarehouseData(skuFallbackRows);
        setWarehouseColumns(
          skuFallbackRows.length > 0
            ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
            : []
        );
      }
    } catch (e) {
      console.error('Failed to fetch warehouse data', e);

      setWarehouseData(skuFallbackRows);
      setWarehouseColumns(
        skuFallbackRows.length > 0
          ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
          : []
      );
    } finally {
      setWarehouseLoading(false);
    }
  };

  const uploadWarehouseToServer = async (file: File) => {
    if (isNA) {
      setModalMessage('Preview mode only. Connect your account to upload warehouse data.');
      setShowModal(true);
      return;
    }

    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    if (!token) {
      setModalMessage('Authorization token is missing');
      setShowModal(true);
      return;
    }

    try {
      setWarehouseLoading(true);

      const formData = new FormData();
      formData.append('file', file);
      formData.append('country', countryName);

      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/uploadWarehouseData`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
        },
        body: formData,
      });

      const result = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(result?.error || result?.message || 'Failed to upload warehouse file');
      }

      const rows = Array.isArray(result?.data) ? result.data : [];
      setWarehouseData(rows);

      const cols = Array.isArray(result?.columns)
        ? result.columns
        : rows.length > 0
          ? Object.keys(rows[0])
          : [];

      setWarehouseColumns(getOrderedWarehouseColumns(cols));

      setSelectedWarehouseFile(null);
      setShowWarehouseUpload(false);
      setModalMessage(result?.message || 'Warehouse file uploaded successfully');
      setShowModal(true);
    } catch (e: any) {
      setModalMessage(e?.message || 'Failed to upload warehouse file');
      setShowModal(true);
    } finally {
      setWarehouseLoading(false);
    }
  };

  const handleWarehouseUpload = async (file: File) => {
    try {
      const reader = new FileReader();

      reader.onload = async (e) => {
        try {
          const wb = XLSX.read(e.target?.result as ArrayBuffer, {
            type: 'array',
          });

          const firstSheetName = wb.SheetNames[0];
          if (!firstSheetName) {
            setModalMessage('The uploaded Excel file has no sheets.');
            setShowModal(true);
            return;
          }

          const sheet = wb.Sheets[firstSheetName];
          const json = XLSX.utils.sheet_to_json(sheet);

          if (!json.length) {
            setModalMessage('The uploaded file is empty.');
            setShowModal(true);
            return;
          }

          const headerError = validateWarehouseHeaders(json[0] as Record<string, any>);
          if (headerError) {
            setModalMessage(headerError);
            setShowModal(true);
            return;
          }

          await uploadWarehouseToServer(file);
        } catch (err) {
          console.error(err);
          setModalMessage('Invalid warehouse file format.');
          setShowModal(true);
        }
      };

      reader.onerror = () => {
        setModalMessage('Failed to read the selected file.');
        setShowModal(true);
      };

      reader.readAsArrayBuffer(file);
    } catch {
      setModalMessage('Invalid warehouse file');
      setShowModal(true);
    }
  };

  useEffect(() => {
    if (activeTab === 'extra') {
      void fetchWarehouseData();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, countryName, skuData]);

  useEffect(() => {
    if (activeTab !== 'inventory-insights') return;

    if (isNA) {
      setInventoryInsightsData(null);
      setInventoryRawResponses(null);
      setInventoryInsightsError(null);
      return;
    }

    const ready =
      (range === 'monthly' && !!selectedMonth && !!selectedYear) ||
      (range === 'quarterly' && !!selectedQuarter && !!selectedYear) ||
      (range === 'yearly' && !!selectedYear);

    if (!ready || !countryName) {
      setInventoryInsightsData(null);
      setInventoryRawResponses(null);
      setInventoryInsightsError(null);
      return;
    }

    const ac = new AbortController();

    const fetchInventoryInsights = async () => {
      try {
        setInventoryInsightsLoading(true);
        setInventoryInsightsError(null);

        const [inventoryResult, ageSummaryResults] = await Promise.all([
          fetchInventoryCurrentByPeriod(ac.signal),
          Promise.allSettled(
            allMonths.map((monthName) =>
              fetchSingleMonthInventoryAgeSummary(
                monthName,
                selectedYear,
                countryName,
                ac.signal
              )
            )
          ),
        ]);

        const fulfilledInventory: InventoryCurrentApiResponse[] =
          inventoryResult?.success ? [inventoryResult] : [];

        const fulfilledAgeSummary = ageSummaryResults
          .filter(
            (
              result
            ): result is PromiseFulfilledResult<InventoryAgeSummaryApiResponse> =>
              result.status === 'fulfilled'
          )
          .map((result) => result.value);

        if (!fulfilledInventory.length) {
          throw new Error('No inventory data found');
        }

        setInventoryRawResponses({
          inventory: fulfilledInventory,
          ageSummary: fulfilledAgeSummary,
        });

        setInventoryInsightsData(
  buildInventoryInsightsFromResponses(
    fulfilledInventory,
    fulfilledAgeSummary,
    selectedAgeingTrendBucket,
    countryName
  )
);
      } catch (e: any) {
        if (e?.name === 'AbortError') return;

        setInventoryInsightsData(null);
        setInventoryRawResponses(null);
        setInventoryInsightsError(
          e?.message || 'Failed to load inventory insights'
        );
      } finally {
        setInventoryInsightsLoading(false);
      }
    };

    fetchInventoryInsights();

    return () => ac.abort();
  }, [
    activeTab,
    isNA,
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
    countryName,
  ]);

 useEffect(() => {
  if (!inventoryRawResponses) return;

  setInventoryInsightsData(
  buildInventoryInsightsFromResponses(
    inventoryRawResponses.inventory,
    inventoryRawResponses.ageSummary,
    selectedAgeingTrendBucket,
    countryName
  )
);
}, [
  selectedAgeingTrendBucket,
  inventoryRawResponses,
  range,
  selectedMonth,
  selectedQuarter,
  selectedYear,
  countryName,
]);

  const renderGrossMarginCell = (row: SkuRow, column: string) => {
    const targetCountry = column.replace('gross_margin_', '');
    const currentPrice =
      editedPrices[row.product_name] !== undefined ? editedPrices[row.product_name] : row.price;
    const currency = row.currency;
    const grossMargin = calculateGrossMargin(currentPrice, currency, targetCountry, row.product_name);

    if (grossMargin === 'N/A') return <span className="gross-margin-na">N/A</span>;
    const marginValue = parseFloat(grossMargin);
    const className = marginValue >= 0 ? 'gross-margin-positive' : 'gross-margin-negative';
    return <span className={className}>{grossMargin}%</span>;
  };

  const getMonthYearDisplay = (row: SkuRow) => {
    const month = row.month ?? '';
    const year = row.year ?? '';
    if (month && year) return `${month} ${year}`;
    if (month) return String(month);
    if (year) return String(year);
    return '—';
  };

  const handleDownloadXLSX = async () => {
    if (!skuData || skuData.length === 0) {
      alert('No data available to download.');
      return;
    }

    const dataToExport = skuData.map((row) => {
      const updatedPrice =
        editedPrices[row.product_name] !== undefined
          ? editedPrices[row.product_name]
          : row.price;

      const exportRow: Record<string, any> = {
        s_no: row.s_no ?? '',
        product_name: row.product_name ?? '',
      };

      visibleColumns.forEach((col) => {
        if (col.startsWith('gross_margin_')) {
          const targetCountry = col.replace('gross_margin_', '');
          const gm = calculateGrossMargin(
            updatedPrice,
            row.currency,
            targetCountry,
            row.product_name
          );

          exportRow[col] = gm !== 'N/A' ? Number(gm) : '';
        } else if (col === 'price') {
          exportRow.price = updatedPrice ?? '';
        } else if (col === 'month_year') {
          exportRow.month_year = getMonthYearDisplay(row);
        } else {
          exportRow[col] = row[col] ?? '';
        }
      });

      return exportRow;
    });

    await exportSkuInformationExcel({
      filename: `SKU_Information_${countryName?.toUpperCase() || 'EXPORT'}.xlsx`,
      countryName,
      titleLine: 'SKU Information',
      titleCountry: countryName === 'global' ? 'Global' : countryName.toUpperCase(),
      platformLabel: 'Phormula',
      periodLabel: `${monthParam} ${yearParam}`,
      companyName,
      brandName,
      dataRows: dataToExport,
    });
  };

  const tableData: TableRow[] = useMemo(() => {
    return skuData.map((row, index) => {
      const item: TableRow = {
        id: `${row.product_name}-${index}`,
        s_no: row.s_no ?? index + 1,
        product_name: row.product_name ?? '—',
        sku_uk: isEmptyCellValue(row.sku_uk) ? '—' : row.sku_uk,
        sku_us: isEmptyCellValue(row.sku_us) ? '—' : row.sku_us,
        sku_canada: isEmptyCellValue(row.sku_canada) ? '—' : row.sku_canada,
        asin: row.asin ?? '—',
        product_barcode: row.product_barcode ?? '—',
        month_year: getMonthYearDisplay(row),
        price: row.price ?? '',
      };

      visibleColumns.forEach((col) => {
        if (col.startsWith('gross_margin_')) {
          item[col] = '';
        } else if (col in row) {
          item[col] = isEmptyCellValue(row[col]) ? '—' : row[col];
        }
      });

      return item;
    });
  }, [skuData, visibleColumns]);

  const columns: ColumnDef<TableRow>[] = useMemo(() => {
    return visibleColumns.map((column) => {
      const col: ColumnDef<TableRow> = {
        key: column,
        header: getColumnDisplayName(column),
      };

      if (column === 's_no') col.width = '60px';

      if (column === 'product_name') {
        col.width = '160px';
        col.cellClassName = 'text-left';
        col.render = (tableRow) => <span>{tableRow.product_name}</span>;
      }

      if (column === 'sku_uk' || column === 'sku_us' || column === 'sku_canada') {
        col.width = '130px';
      }

      if (column === 'asin') col.width = '140px';
      if (column === 'product_barcode') col.width = '160px';
      if (column === 'month_year') col.width = '140px';

      if (column === 'price') {
        col.width = '100px';
        col.render = (tableRow) => {
          const originalRow = skuData.find((r) => r.product_name === String(tableRow.product_name));
          if (!originalRow) return '—';

          return isEditing ? (
            <div className="flex items-center justify-center gap-1">
              <span>{getCurrencySymbol(originalRow.currency)}</span>
              <input
                type="number"
                className="border border-gray-300 rounded px-2 py-1 w-[90px] text-center"
                value={
                  editedPrices[originalRow.product_name] !== undefined
                    ? editedPrices[originalRow.product_name]
                    : originalRow.price ?? ''
                }
                onChange={(e) => handlePriceChange(originalRow.product_name, e.target.value)}
              />
            </div>
          ) : (
            <span>
              {getCurrencySymbol(originalRow.currency)} {originalRow.price ?? '—'}
            </span>
          );
        };
      }

      if (column.startsWith('gross_margin_')) {
        col.width = '150px';
        col.render = (tableRow) => {
          const originalRow = skuData.find((r) => r.product_name === String(tableRow.product_name));
          if (!originalRow) return 'N/A';
          return renderGrossMarginCell(originalRow, column);
        };
      }

      return col;
    });
  }, [visibleColumns, skuData, isEditing, editedPrices]);

  const tabOptions = useMemo(
    () => [
      { value: 'inventory-insights' as const, label: 'Inventory Insights' },
      { value: 'sku-info' as const, label: 'SKU Information' },
      { value: 'extra' as const, label: 'Upload Warehouse Data' },

    ],
    []
  );

  // 4) Make warehouse header label nicer
  const getWarehouseHeaderLabel = (col: string) => {
    if (col === 's_no') return 'S.No.';
    if (col === 'product_name') return 'Product Name';

    if (col === 'sku_uk' || col === 'sku_us' || col === 'sku_canada') {
      if (countryName === 'global') {
        const c = col.replace('sku_', '').toUpperCase();
        return `SKU (${c})`;
      }

      return 'SKU';
    }

    const formatted = col
      .replaceAll('_', ' ')
      .replace(/\b\w/g, (c) => c.toUpperCase());

    return formatted;
  };

  // 5) Optional: make Product Name column a bit wider
  const warehouseTableColumns: ColumnDef<Record<string, any>>[] = useMemo(() => {
    const filteredWarehouseColumns = warehouseColumns.filter((col) => {
      // Global should show all country SKU columns
      if (countryName === 'global') return true;

      // For country-wise pages, show only that country's SKU column
      if (col.startsWith('sku_')) {
        return col === `sku_${countryName}`;
      }

      return true;
    });

    return filteredWarehouseColumns.map((col) => ({
      key: col,
      header: getWarehouseHeaderLabel(col),
      width:
        col === 's_no'
          ? '70px'
          : col === 'product_name'
            ? '220px'
            : col === 'sku_us' || col === 'sku_uk' || col === 'sku_canada'
              ? '120px'
              : col === 'local_stock' || col === 'in_transit_units'
                ? '150px'
                : col === 'month' || col === 'year'
                  ? '110px'
                  : '140px',
      cellClassName: col === 'product_name' ? 'text-left' : 'text-center',
      render: (row) => {
        const value = row[col];

        if (col === 'month' && typeof value === 'string') {
          return value.charAt(0).toUpperCase() + value.slice(1).toLowerCase();
        }

        return value === null || value === undefined || value === '' ? '—' : String(value);
      },
    }));
  }, [warehouseColumns, countryName]);

  if (error) return <div>Error: {error}</div>;

  const PreviewLockedSection = ({
    enabled,
    children,
    title,
    description,
    buttonText,
    onAction,
  }: {
    enabled: boolean;
    children: React.ReactNode;
    title?: string;
    description?: string;
    buttonText?: string;
    onAction?: () => void;
  }) => {
    return (
      <div className="relative w-full">
        <div
          className={
            enabled
              ? "pointer-events-none select-none opacity-45 transition-all duration-300"
              : "opacity-100 transition-all duration-300"
          }
        >
          {children}
        </div>

        {enabled && (
          <>
            <div className="absolute inset-0 z-10 rounded-xl bg-white/45" />

            <div className="absolute inset-0 z-20 pointer-events-none">
              <div className="sticky top-[18vh] sm:top-[20vh] lg:top-[22vh] 2xl:top-[24vh] flex justify-center px-4">
                <div className="pointer-events-auto w-full max-w-md rounded-2xl bg-white shadow-2xl p-6 text-center">
                  <div className="mb-4 flex justify-center">
                    <div className="flex h-16 w-16 items-center justify-center rounded-full bg-[#37455F]">
                      <IoMdLock className="text-3xl text-[#F8EDCE]" />
                    </div>
                  </div>

                  <h3 className="text-lg font-semibold text-[#414042]">
                    {title}
                  </h3>

                  <p className="mt-2 text-sm text-gray-600 leading-6">
                    {description}
                  </p>

                  <button
                    onClick={onAction}
                    className="mt-4 rounded-md bg-[#37455F] px-4 py-2 text-sm text-[#F8EDCE] hover:opacity-90 transition"
                  >
                    {buttonText}
                  </button>

                  {/* <p className="mt-3 text-xs text-gray-500">
                    Demo data is shown for preview only.
                  </p> */}
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    );
  };

  const handleConnectAmazonPreview = () => {
    router.push(`/profile/${countryName}/NA/NA`);
  };

  const handleWarehouseDownload = async () => {
    if (!warehouseData || warehouseData.length === 0) {
      alert('No warehouse data available to download.');
      return;
    }

    const exportWarehouseColumns = warehouseColumns.filter((col) => {
      if (countryName === 'global') return true;

      if (col.startsWith('sku_')) {
        return col === `sku_${countryName}`;
      }

      return true;
    });

    const exportData = warehouseData.map((row, index) => {
      const exportRow: Record<string, any> = {};

      exportWarehouseColumns.forEach((col) => {
        if (col === 's_no') {
          exportRow.s_no = row.s_no ?? index + 1;
        } else if (col === 'product_name') {
          exportRow.product_name = row.product_name ?? '';
        } else if (col === 'month' && typeof row[col] === 'string') {
          exportRow.month =
            row[col].charAt(0).toUpperCase() + row[col].slice(1).toLowerCase();
        } else {
          exportRow[col] = isEmptyCellValue(row[col]) ? '' : row[col];
        }
      });

      return exportRow;
    });

    await exportWarehouseDataExcel({
      filename: `Warehouse_Data_${countryName?.toUpperCase() || 'EXPORT'}.xlsx`,
      countryName,
      titleLine: 'Warehouse Data',
      titleCountry: countryName === 'global' ? 'Global' : countryName.toUpperCase(),
      platformLabel: 'Phormula',
      periodLabel: `${monthParam} ${yearParam}`,
      companyName,
      brandName,
      dataRows: exportData,
    });
  };

  const INPUT_COST_VISIBLE_ROWS = 15;
  const INPUT_COST_ROW_HEIGHT = 40;

  const shouldScrollSkuInfoTable = tableData.length > INPUT_COST_VISIBLE_ROWS;

  const shouldScrollWarehouseTable =
    warehouseData.length > INPUT_COST_VISIBLE_ROWS;

  return (
    <div>
      <style>{`
        div { font-family: 'Lato', sans-serif; }
        .gross-margin-positive { color: #28a745; font-weight: bold; }
        .gross-margin-negative { color: #dc3545; font-weight: bold; }
        .gross-margin-na { color: #6c757d; font-style: italic; }
      `}</style>

      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div className="min-w-0">
          <div className="flex flex-wrap items-baseline gap-2 justify-start">
            <PageBreadcrumb
              variant="page"
              align="left"
              textSize="2xl"
              pageTitle={
                <div className="flex flex-wrap items-baseline gap-2">
                  <span className="text-[#414042] font-bold">
                    Input Cost – Amazon
                  </span>
                  <span className="text-green-500 font-bold">
                    {countryName?.toUpperCase()}
                  </span>
                </div>
              }
            />
          </div>

          <div className="mt-3">
            <SegmentedToggle
              value={activeTab}
              options={tabOptions}
              onChange={(val) => setActiveTab(val as InputCostTab)}
              compact
              textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
            />
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-3 md:justify-end">
          {activeTab === 'sku-info' ? (
            <>
              {isEditing && (
                <button
                  className="ml-auto cursor-pointer rounded-[5px] bg-[#2c3e50] px-4 py-2 font-['Lato'] text-[clamp(12px,0.729vw,16px)] font-bold text-[#f8edcf] hover:bg-[#34495e]"
                  onClick={saveChanges}
                >
                  Save Changes
                </button>
              )}

              <button
                className="ml-auto cursor-pointer rounded-[5px] bg-[#2c3e50] px-4 py-2 font-['Lato'] text-[clamp(12px,0.729vw,16px)] font-bold text-[#f8edcf] hover:bg-[#34495e]"
                onClick={() => setShowMultiuseCountry(true)}
                disabled={isNA}
              >
                Upload File
              </button>

              <DownloadIconButton onClick={handleDownloadXLSX} size="md" disabled={isNA} />
            </>
          ) : activeTab === 'extra' ? (
            <>
              <button
                className="ml-auto cursor-pointer rounded-[5px] bg-[#2c3e50] px-4 py-2 font-['Lato'] text-[clamp(12px,0.729vw,16px)] font-bold text-[#f8edcf] hover:bg-[#34495e]"
                onClick={() => setShowWarehouseUpload(true)}
                disabled={isNA}
              >
                Upload File
              </button>

              <DownloadIconButton
                onClick={handleWarehouseDownload}
                size="md"
                disabled={isNA}
              />
            </>
          ) : (
            <PeriodFiltersTable
              range={range}
              selectedMonth={selectedMonth}
              selectedQuarter={selectedQuarter}
              selectedYear={selectedYear}
              yearOptions={yearOptions}
              onRangeChange={handleRangeChange}
              onMonthChange={handleMonthChange}
              onQuarterChange={handleQuarterChange}
              onYearChange={handleYearChange}
              allowedRanges={['monthly', 'quarterly', 'yearly']}
            />
          )}
        </div>
      </div>

      <PreviewLockedSection
        enabled={isNA}
        title="Preview Mode"
        description="To view your real business data and analytics, please complete your profile and connect your Amazon account. This will unlock your performance dashboard and insights."
        buttonText="Complete Setup"
        onAction={handleConnectAmazonPreview}
      >
        <>

          {activeTab === 'inventory-insights' && (
            <div className="mt-5">
              {inventoryInsightsLoading ? (
                <div className="rounded-xl border border-slate-200 bg-white min-h-[420px] flex items-center justify-center">
                  <Loader transparent />
                </div>
              ) : inventoryInsightsError ? (
                <div className="w-full rounded-2xl border-2 border-red-200 bg-red-50 p-6 space-y-2">
                  <div className="flex items-center gap-2">
                    <span className="text-lg">⚠️</span>
                    <p className="font-semibold text-red-700">
                      Unable to Load Inventory Insights
                    </p>
                  </div>

                  <p className="text-sm text-red-600">{inventoryInsightsError}</p>
                </div>
              ) : inventoryInsightsData ? (
                <InventoryInsightsSection
                  heatmapBuckets={inventoryInsightsData.heatmapBuckets}
                  heatmapData={inventoryInsightsData.heatmapData}
                  donutData={inventoryInsightsData.donutData}
                  donutTotalUnits={inventoryInsightsData.donutTotalUnits}
                  trendSelectedBucket={inventoryInsightsData.trendSelectedBucket}
                  trendData={inventoryInsightsData.trendData}
                  trendLineColor={inventoryInsightsData.trendLineColor}
                  trendBucketOptions={inventoryInsightsData.trendBucketOptions}
                  trendAllSeriesData={inventoryInsightsData.trendAllSeriesData}
                  onTrendBucketChange={setSelectedAgeingTrendBucket}
                  actions={inventoryInsightsData.actions}
                  actionLogic={inventoryInsightsData.actionLogic}
                  onHeatmapProductClick={() => { }}
                />
              ) : (
                <div className="rounded-xl border border-slate-200 bg-white p-6 text-sm text-slate-500">
                  No inventory insights found for the selected period.
                </div>
              )}
            </div>
          )}
          {activeTab === 'sku-info' && (
            <>
              {loading ? (
                <div className="mt-5 rounded-xl border border-slate-200 bg-white min-h-[420px] flex items-center justify-center">
                  <Loader transparent />
                </div>
              ) : skuData.length > 0 ? (
                <div className="mt-5">
                  <DataTable<TableRow>
                    columns={columns}
                    data={tableData}
                    loading={false}
                    paginate={false}
                    pageSize={10}
                    stickyHeader={true}
                    zebra={true}
                    scrollY={false}
                    maxHeight="none"
                    bodyMaxHeight={
                      shouldScrollSkuInfoTable
                        ? INPUT_COST_ROW_HEIGHT * INPUT_COST_VISIBLE_ROWS
                        : undefined
                    }
                    emptyMessage="No data available"
                    tableClassName="text-sm"
                    className="rounded-xl"
                  />
                </div>
              ) : (
                <p className="mt-5">No data available</p>
              )}
            </>
          )}

          {activeTab === 'extra' && (
            <div className="mt-5">
              {warehouseLoading ? (
                <div className="rounded-xl border border-slate-200 bg-white min-h-[320px] flex items-center justify-center">
                  <Loader transparent />
                </div>
              ) : warehouseData.length > 0 ? (
                <DataTable<Record<string, any>>
                  columns={warehouseTableColumns}
                  data={warehouseData}
                  paginate={false}
                  pageSize={10}
                  stickyHeader
                  scrollY={false}
                  maxHeight="none"
                  bodyMaxHeight={
                    shouldScrollWarehouseTable
                      ? INPUT_COST_ROW_HEIGHT * INPUT_COST_VISIBLE_ROWS
                      : undefined
                  }
                  emptyMessage="No warehouse data available"
                  tableClassName="text-sm"
                  className="rounded-xl"
                />
              ) : (
                <div className="rounded-xl border border-slate-200 bg-white p-6 text-sm text-slate-500">
                  Upload a warehouse file to view data here.
                </div>
              )}
            </div>
          )}
        </>
      </PreviewLockedSection>

      {showWarehouseUpload && (
        <div
          onClick={() => setShowWarehouseUpload(false)}
          className="fixed inset-0 z-[10001] flex items-center justify-center bg-black/50 p-4"
        >
          <div
            onClick={(e) => e.stopPropagation()}
            className="relative m-4 w-full max-w-[500px] rounded-xl border border-[#D9D9D9] bg-white shadow-[6px_6px_7px_0px_#00000026]"
          >
            <button
              onClick={() => setShowWarehouseUpload(false)}
              type="button"
              className="absolute right-4 top-3 z-10 text-2xl leading-none text-neutral-500 hover:text-neutral-800"
            >
              &times;
            </button>

            <div className="relative w-full rounded-xl bg-white/30 p-4 no-scrollbar lg:p-9">
              <WarehouseMultiCountryUpload
                countryName={countryName}
                onClose={() => setShowWarehouseUpload(false)}
                onComplete={() => {
                  setShowWarehouseUpload(false);
                  void fetchWarehouseData();
                  setModalMessage("Warehouse file uploaded successfully");
                  setShowModal(true);
                }}
              />
            </div>
          </div>
        </div>
      )}

      {showMultiuseCountry && (
        <div
          onClick={() => setShowMultiuseCountry(false)}
          className="fixed inset-0 z-[10001] flex items-center justify-center bg-black/50 p-4"
        >
          <div
            onClick={(e) => e.stopPropagation()}
            className="relative m-4 w-full max-w-[500px] rounded-xl border border-[#D9D9D9] bg-white shadow-[6px_6px_7px_0px_#00000026]"
          >
            <button
              onClick={() => setShowMultiuseCountry(false)}
              type="button"
              className="absolute right-4 top-3 z-10 text-2xl leading-none text-neutral-500 hover:text-neutral-800"
            >
              &times;
            </button>

            <div className="relative w-full rounded-xl bg-white/30 p-4 no-scrollbar lg:p-9">
              <SkuMultiuseCountryUpload
                onClose={() => setShowMultiuseCountry(false)}
                onComplete={() => {
                  setShowMultiuseCountry(false);
                  void fetchSkuData();
                }}
              />
            </div>
          </div>
        </div>
      )}

      <Modalmsg
        show={showModal}
        message={modalMessage}
        onClose={() => setShowModal(false)}
        onCancel={() => setShowModal(false)}
      />
    </div>
  );
}