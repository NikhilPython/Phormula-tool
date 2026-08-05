'use client';

import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useParams, useRouter } from 'next/navigation';
import * as XLSX from 'xlsx';

import DisplayInventoryForecast from '@/components/inventory/DisplayInventoryForecast';
import Loading from '@/components/inventory/Loading';
import { Modal } from '@/components/ui/modal';
import FileUploadForm from '@/app/(admin)/(ui-elements)/modals/FileUploadForm';
import InventoryFlowTabs, { InventoryFlowTab } from '@/components/inventory/InventoryFlowTabs';
import DispatchPage from '@/app/(admin)/dispatch/[countryName]/[month]/[year]/DispatchClient';
import PurchaseOrderPage from '@/app/(admin)/purchase-order/[countryName]/[month]/[year]/PurchaseOrderClient';
import PageBreadcrumb from '@/components/common/PageBreadCrumb';
import MonthYearPickerTable from '@/components/filters/MonthYearPickerTable';
import DownloadIconButton from '@/components/ui/button/DownloadButton';
import { IoMdLock } from 'react-icons/io';
import IntegrationsModal from '@/features/integration/IntegrationsModal';
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";
import { AnimatePresence, motion } from "framer-motion";
import Productinfoinpopup from '@/components/businessInsight/Productinfoinpopup';
import Loader from "@/components/loader/Loader";

type UploadItem = {
  filename?: string;
  month?: string | number;
  year?: string | number;
  country?: string;
  [key: string]: any;
};

type UploadHistoryRes = { uploads: UploadItem[] };
type ForecastRow = Record<string, any>;

const HASH_TO_TAB: Record<string, InventoryFlowTab> = {
  'inventory-forecast': 'inventory',
  dispatch: 'dispatch',
  'purchase-order': 'purchaseOrder',
};

const TAB_TO_HASH: Record<InventoryFlowTab, string> = {
  inventory: 'inventory-forecast',
  dispatch: 'dispatch',
  purchaseOrder: 'purchase-order',
};

const INVENTORY_FORECAST_MIN_DATA_MESSAGE =
  "At least 6 months data to be fetched in order to view inventory forecast";

const DUMMY_INVENTORY_FORECAST = [
  {
    sku: 'SKU-DEMO-1',
    'Product Name': 'Demo Product A',
    "Oct'25 Sold": 220,
    "Nov'25 Sold": 250,
    "Dec'25 Sold": 280,
    "Jan'26": 300,
    "Feb'26": 330,
    "Mar'26": 360,
  },
  {
    sku: 'SKU-DEMO-5',
    'Product Name': 'Demo Product E',
    "Oct'25 Sold": 120,
    "Nov'25 Sold": 140,
    "Dec'25 Sold": 160,
    "Jan'26": 180,
    "Feb'26": 200,
    "Mar'26": 220,
  },
  {
    sku: 'SKU-DEMO-2',
    'Product Name': 'Demo Product B',
    "Oct'25 Sold": 220,
    "Nov'25 Sold": 250,
    "Dec'25 Sold": 280,
    "Jan'26": 300,
    "Feb'26": 330,
    "Mar'26": 360,
  },
  {
    sku: 'SKU-DEMO-3',
    'Product Name': 'Demo Product C',
    "Oct'25 Sold": 220,
    "Nov'25 Sold": 250,
    "Dec'25 Sold": 280,
    "Jan'26": 300,
    "Feb'26": 330,
    "Mar'26": 360,
  },
  {
    sku: 'SKU-DEMO-4',
    'Product Name': 'Demo Product D',
    "Oct'25 Sold": 220,
    "Nov'25 Sold": 250,
    "Dec'25 Sold": 280,
    "Jan'26": 300,
    "Feb'26": 330,
    "Mar'26": 360,
  },
  {
    sku: 'Total',
    'Product Name': 'Total',
    "Oct'25 Sold": 340,
    "Nov'25 Sold": 390,
    "Dec'25 Sold": 440,
    "Jan'26": 480,
    "Feb'26": 530,
    "Mar'26": 580,
  },
];

const DUMMY_DISPATCH_DATA = [
  {
    sNo: 1,
    productName: 'Demo Product A',
    sku: 'DSP-DEMO-1',
    inventoryAtMonthEnd: 0,
    coverageRatioBeforeDispatch: 0,
    dispatch: 0,
    currentInventoryPlusDispatch: 0,
  },
  {
    sNo: 2,
    productName: 'Demo Product B',
    sku: 'DSP-DEMO-1',
    inventoryAtMonthEnd: 0,
    coverageRatioBeforeDispatch: 0,
    dispatch: 0,
    currentInventoryPlusDispatch: 0,
  },
  {
    sNo: 3,
    productName: 'Demo Product C',
    sku: 'DSP-DEMO-3',
    inventoryAtMonthEnd: 0,
    coverageRatioBeforeDispatch: 0,
    dispatch: 0,
    currentInventoryPlusDispatch: 0,
  },
  {
    sNo: 4,
    productName: 'Demo Product D',
    sku: 'DSP-DEMO-4',
    inventoryAtMonthEnd: 0,
    coverageRatioBeforeDispatch: 0,
    dispatch: 0,
    currentInventoryPlusDispatch: 0,
  },
  {
    sNo: 5,
    productName: 'Demo Product E',
    sku: 'DSP-DEMO-5',
    inventoryAtMonthEnd: 0,
    coverageRatioBeforeDispatch: 0,
    dispatch: 0,
    currentInventoryPlusDispatch: 0,
  },
];

const DUMMY_PO_DATA = [
  {
    sNo: 1,
    productName: 'Demo Product A',
    dispatchesUK: 0,
    totalDispatches: 0,
    currentInventory: 0,
    inTransitUnits: 0,
    poToBeRaised: 0,
    costPerUnit: 0,
    poCost: 0,
  },
  {
    sNo: 2,
    productName: 'Demo Product B',
    dispatchesUK: 0,
    totalDispatches: 0,
    currentInventory: 0,
    inTransitUnits: 0,
    poToBeRaised: 0,
    costPerUnit: 0,
    poCost: 0,
  },
  {
    sNo: 3,
    productName: 'Demo Product C',
    dispatchesUK: 0,
    totalDispatches: 0,
    currentInventory: 0,
    inTransitUnits: 0,
    poToBeRaised: 0,
    costPerUnit: 0,
    poCost: 0,
  },
  {
    sNo: 4,
    productName: 'Demo Product D',
    dispatchesUK: 0,
    totalDispatches: 0,
    currentInventory: 0,
    inTransitUnits: 0,
    poToBeRaised: 0,
    costPerUnit: 0,
    poCost: 0,
  },
  {
    sNo: 5,
    productName: 'Demo Product E',
    dispatchesUK: 0,
    totalDispatches: 0,
    currentInventory: 0,
    inTransitUnits: 0,
    poToBeRaised: 0,
    costPerUnit: 0,
    poCost: 0,
  },
];

const INVENTORY_REQUIREMENT_MESSAGE =
  "This section requires you to fetch at least 6 months of data";


type DrawerMetric = {
  label: string;
  value: string;
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

type DrawerData = {
  source: "live" | "historic";
  productName: string;
  sku?: string;
  metrics: DrawerMetric[];
  recommendation: string[];
  inventoryRecommendation: string[];
  adsRecommendation: string[];
  journey: string[];
};

const drawerToNumber = (value: any) => {
  if (value === null || value === undefined || value === "") return 0;
  const n = Number(String(value).replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : 0;
};

const drawerMoney = (value: any, symbol: string) =>
  `${symbol}${drawerToNumber(value).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;

const drawerNumber = (value: any) =>
  Math.round(drawerToNumber(value)).toLocaleString();

const drawerPct = (value: any) => {
  if (value === null || value === undefined || value === "") return "";
  const n = Number(value);
  if (!Number.isFinite(n)) return "";
  return ` (${n >= 0 ? "+" : ""}${n.toFixed(2)}%)`;
};

const drawerBullets = (value: any): string[] => {
  if (Array.isArray(value)) return value.map(String).map((x) => x.trim()).filter(Boolean);
  if (!value) return [];
  return String(value)
    .split(/(?:\n|;\s+|\.\s+)/g)
    .map((x) => x.replace(/^[-•]\s*/, "").trim())
    .filter(Boolean);
};

const drawerNormalize = (value: any) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s*\+\s*/g, "+")
    .replace(/\s+/g, " ");

const drawerCurrencySymbol = (country: string) => {
  const key = String(country || "").toLowerCase();
  if (key === "uk") return "£";
  if (key === "india" || key === "in") return "₹";
  if (key === "ca" || key === "canada") return "C$";
  return "$";
};

const findByProductOrSku = (
  rows: any[],
  productName: string,
  sku?: string
) => {
  const productKey = drawerNormalize(productName);
  const skuKey = drawerNormalize(sku);

  return (rows || []).find((row) => {
    const rowSku = drawerNormalize(
      row?.sku ?? row?.SKU
    );

    const rowProduct = drawerNormalize(
      row?.product_name ??
      row?.productName ??
      row?.name
    );

    if (skuKey && rowSku === skuKey) {
      return true;
    }

    return Boolean(
      productKey &&
      rowProduct === productKey
    );
  });
};

const getLiveRows = (json: any): any[] => {
  const candidates = [
    json?.all_action_rows,
    json?.recommendation_card_rows?.all_skus,
    json?.focus_sku_rows,
    json?.recommendation_card_rows?.focus_skus,
    json?.growth_data,
    json?.sku_growth,
  ];

  const combinedRows = candidates
    .filter(Array.isArray)
    .flat();

  const uniqueRows = new Map<string, any>();

  combinedRows.forEach((row: any) => {
    const skuKey = drawerNormalize(row?.sku);
    const productKey = drawerNormalize(
      row?.product_name || row?.name
    );

    const key = skuKey || productKey;

    if (!key) return;

    // Pehla complete record preserve karo.
    if (!uniqueRows.has(key)) {
      uniqueRows.set(key, row);
    }
  });

  return Array.from(uniqueRows.values());
};

const getGrowth = (row: any, directKeys: string[], objectKeys: string[]) => {
  for (const key of directKeys) {
    const value = row?.[key];
    if (value !== null && value !== undefined && Number.isFinite(Number(value))) return Number(value);
  }
  for (const key of objectKeys) {
    const value = row?.[key];
    if (value && typeof value === "object" && Number.isFinite(Number(value.value))) return Number(value.value);
  }
  return null;
};

const buildLiveDrawerData = (
  json: any,
  clickedRow: ForecastRow,
  productName: string,
  sku: string | undefined,
  country: string
): DrawerData => {
  const symbol = drawerCurrencySymbol(country);
  const row = findByProductOrSku(getLiveRows(json), productName, sku) || clickedRow;
  const actions = json?.sku_strategy_actions || json?.recommended_actions_mtd || {};
  const rec = actions?.[sku || ""] || actions?.[productName] || {};

  const cm1 = row?.profit_curr ?? row?.profit?.current ?? row?.profit ?? 0;
  const cm2 = row?.cm2_profit_curr ?? row?.cm2_profit?.current ?? row?.cm2_profit;
  const hasCm2 = cm2 !== null && cm2 !== undefined && Math.abs(drawerToNumber(cm2) - drawerToNumber(cm1)) >= 0.01;

  const metrics: DrawerMetric[] = [
    {
      label: "Units",
      value: `${drawerNumber(row?.quantity_curr ?? row?.total_quantity?.current ?? row?.total_quantity ?? row?.quantity)}${drawerPct(getGrowth(row, ["quantity_growth_pct", "unit_growth_pct"], ["Unit Growth (%)", "Quantity Growth (%)"]))}`,
    },
    {
      label: "Net Sales",
      value: `${drawerMoney(row?.net_sales_curr ?? row?.net_sales?.current ?? row?.net_sales, symbol)}${drawerPct(getGrowth(row, ["net_sales_growth_pct"], ["Net Sales Growth (%)", "Sales Growth (%)"]))}`,
    },
    {
      label: "ASP",
      value: `${drawerMoney(row?.asp_curr ?? row?.asp?.current ?? row?.asp, symbol)}${drawerPct(getGrowth(row, ["asp_growth_pct"], ["ASP Growth (%)"]))}`,
    },
    {
      label: "Ads",
      value: `${drawerMoney(row?.ads_spend_curr ?? row?.productwise_ads_spend?.current ?? row?.ads_spend ?? 0, symbol)}${drawerPct(row?.ads_spend_growth_pct ?? row?.productwise_ads_spend?.delta_pct)}`,
    },
    {
      label: hasCm2 ? "CM2 Profit" : "CM1 Profit",
      value: `${drawerMoney(hasCm2 ? cm2 : cm1, symbol)}${drawerPct(hasCm2 ? getGrowth(row, ["cm2_profit_growth_pct"], ["CM2 Profit Growth (%)"]) : getGrowth(row, ["profit_growth_pct"], ["CM1 Profit Impact (%)", "Profit Growth (%)"]))}`,
    },
    {
      label: hasCm2 ? "CM2 Profit Per Unit" : "CM1 Profit Per Unit",
      value: `${drawerMoney(hasCm2 ? (row?.cm2_profit_per_unit_curr ?? row?.cm2_profit_per_unit?.current ?? row?.cm2_profit_per_unit) : (row?.unit_wise_profitability_curr ?? row?.unit_wise_profitability?.current ?? row?.unit_wise_profitability), symbol)}${drawerPct(hasCm2 ? getGrowth(row, ["cm2_profit_per_unit_growth_pct"], ["CM2 Profit Per Unit Growth (%)"]) : getGrowth(row, ["unit_profit_growth_pct"], ["Profit Per Unit (%)"]))}`,
    },

    { label: "Current Inventory", value: drawerNumber(row?.current_inventory ?? row?.available_inventory ?? 0) },
    { label: "Stock Cover", value: String(row?.coverage_ratio_months ?? row?.selected_period_coverage_ratio ?? "-") },
  ];

  return {
    source: "live",
    productName,
    sku,
    metrics,
    recommendation: drawerBullets(rec?.recommendation),
    inventoryRecommendation: drawerBullets(rec?.inventory_recommendation),
    adsRecommendation: drawerBullets(rec?.ads_recommendation),
    journey: drawerBullets(rec?.journey_summary),
  };
};

const drawerCompactKey = (value: any) =>
  String(value || "")
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]/g, "");

const drawerSkuKeys = (value: any) =>
  String(value || "")
    .split(/[,|/]+/g)
    .map((item) => drawerCompactKey(item))
    .filter(Boolean);

const cleanHistoricLine = (value: any) =>
  String(value || "")
    .replace(/^\s*#{1,6}\s*/, "")
    .replace(/^\s*[-•]\s*/, "")
    .replace(/^\s*\d+\.\s*/, "")
    .replace(/\*\*/g, "")
    .trim();

const historicLineMatchesProduct = (
  line: string,
  productName: string,
  sku?: string
) => {
  const cleanLine = cleanHistoricLine(line);
  const compactLine = drawerCompactKey(cleanLine);
  const targetProduct = drawerCompactKey(productName);
  const targetSkus = drawerSkuKeys(sku);

  if (!compactLine) return false;

  if (targetSkus.some((skuKey) => compactLine.includes(skuKey))) {
    return true;
  }

  if (!targetProduct) return false;

  // Handles headers such as:
  // Product Name
  // Product Name (SKU-123)
  // SKU-123 - Product Name
  return (
    compactLine === targetProduct ||
    compactLine.startsWith(targetProduct) ||
    compactLine.endsWith(targetProduct)
  );
};

const parseHistoricProductBlock = (
  textValue: any,
  productName: string,
  sku?: string
) => {
  const lines = Array.isArray(textValue)
    ? textValue.flatMap((item) => String(item).split(/\r?\n/))
    : String(textValue || "").split(/\r?\n/);

  let active = false;
  const block: string[] = [];

  for (const raw of lines) {
    const line = cleanHistoricLine(raw);
    if (!line) continue;

    const isTarget = historicLineMatchesProduct(line, productName, sku);
    const lowerLine = line.toLowerCase();
    const isKnownSection =
      lowerLine.startsWith("performance summary") ||
      lowerLine.startsWith("product insights") ||
      lowerLine.startsWith("portfolio") ||
      lowerLine.startsWith("inventory alerts");

    const looksLikeMetric = /^[^:]{2,50}:\s*.+$/.test(line);
    const looksLikeAction =
      /^product journey/i.test(line) ||
      /^recommendation:/i.test(line) ||
      /^inventory action:/i.test(line) ||
      /^ads action:/i.test(line) ||
      /^sku:/i.test(line) ||
      /^bucket:/i.test(line);

    // A non-metric/non-action line after a populated block is the next product header.
    const looksLikeNextProductHeader =
      active &&
      block.length > 1 &&
      !looksLikeMetric &&
      !looksLikeAction &&
      !isKnownSection;

    if (isTarget) {
      active = true;
      block.push(line);
      continue;
    }

    if (looksLikeNextProductHeader) break;
    if (active) block.push(line);
  }

  return block;
};

const getHistoricMetricNumber = (value?: string) => {
  const mainValue = String(value || "").split("(")[0];

  const numericValue = Number(
    mainValue.replace(/[^0-9.-]/g, "")
  );

  return Number.isFinite(numericValue)
    ? numericValue
    : null;
};

const buildHistoricDrawerData = (
  json: any,
  clickedRow: ForecastRow,
  productName: string,
  sku: string | undefined,
  country: string
): DrawerData => {
  const symbol = drawerCurrencySymbol(country);
  const allSkuMomCandidates = [
    json?.all_sku_mom,
    json?.sku_mom,
    json?.metrics?.all_sku_mom,
    json?.metrics?.sku_mom,
    json?.data?.all_sku_mom,
    json?.data?.sku_mom,
  ].filter((value) => value && typeof value === "object");

  const targetSkuKeys = drawerSkuKeys(sku);
  const targetProductKey = drawerCompactKey(productName);

  const historicEntries = allSkuMomCandidates.flatMap((candidate: any) =>
    Array.isArray(candidate)
      ? candidate.map((row: any, index: number) => [String(index), row] as const)
      : Object.entries(candidate)
  );

  const structuredSkuRow = historicEntries.find(([entryKey, row]: any) => {
    const rowSkuKeys = [
      ...drawerSkuKeys(entryKey),
      ...drawerSkuKeys(row?.sku),
      ...drawerSkuKeys(row?.SKU),
      ...drawerSkuKeys(row?.seller_sku),
    ];

    const skuMatches = targetSkuKeys.some((targetSku) =>
      rowSkuKeys.some(
        (rowSku) => rowSku === targetSku || rowSku.includes(targetSku) || targetSku.includes(rowSku)
      )
    );

    if (skuMatches) return true;

    const rowProductKey = drawerCompactKey(
      row?.product_name ?? row?.productName ?? row?.name ?? row?.["Product Name"]
    );

    return Boolean(
      targetProductKey &&
      rowProductKey &&
      (rowProductKey === targetProductKey ||
        rowProductKey.startsWith(targetProductKey) ||
        targetProductKey.startsWith(rowProductKey))
    );
  })?.[1] as any;

  // /summary can expose top products in sku_insights, while the remaining
  // products are present in the markdown summary under
  // "ALL SKU INDIVIDUAL INSIGHTS". Do not use `||` here because it drops
  // summary as soon as sku_insights exists.
  const sourceText = [
    json?.sku_insights,
    json?.summary,
    json?.insights,
    json?.data?.sku_insights,
    json?.data?.summary,
    json?.data?.insights,
  ]
    .flatMap((value) =>
      Array.isArray(value)
        ? value.map(String)
        : value
          ? [String(value)]
          : []
    )
    .join("\n");
  const block = parseHistoricProductBlock(sourceText, productName, sku);
  const metrics: DrawerMetric[] = [];
  if (structuredSkuRow) {
  const cm1Profit = structuredSkuRow?.profit;
  const cm2Profit = structuredSkuRow?.cm2_profit;

  const cm1Value = drawerToNumber(cm1Profit?.current);
  const cm2Value = drawerToNumber(cm2Profit?.current);

  const hasStructuredCm2 =
    cm2Profit?.current !== null &&
    cm2Profit?.current !== undefined &&
    Math.abs(cm2Value - cm1Value) >= 0.01;

  metrics.push(
    {
      label: "Units",
      value: `${drawerNumber(
        structuredSkuRow?.total_quantity?.current ??
        structuredSkuRow?.quantity?.current
      )}${drawerPct(
        structuredSkuRow?.total_quantity?.delta_pct ??
        structuredSkuRow?.quantity?.delta_pct
      )}`,
    },
    {
      label: "Net Sales",
      value: `${drawerMoney(
        structuredSkuRow?.net_sales?.current,
        symbol
      )}${drawerPct(
        structuredSkuRow?.net_sales?.delta_pct
      )}`,
    },
    {
      label: "ASP",
      value: `${drawerMoney(
        structuredSkuRow?.asp?.current,
        symbol
      )}${drawerPct(
        structuredSkuRow?.asp?.delta_pct
      )}`,
    }
  );

  if (hasStructuredCm2) {
    metrics.push(
      {
        label: "Ads",
        value: `${drawerMoney(
          structuredSkuRow?.productwise_ads_spend?.current ??
          structuredSkuRow?.total_ads?.current,
          symbol
        )}${drawerPct(
          structuredSkuRow?.productwise_ads_spend?.delta_pct ??
          structuredSkuRow?.total_ads?.delta_pct
        )}`,
      },
      {
        label: "CM2 Profit",
        value: `${drawerMoney(
          structuredSkuRow?.cm2_profit?.current,
          symbol
        )}${drawerPct(
          structuredSkuRow?.cm2_profit?.delta_pct
        )}`,
      },
      {
        label: "CM2 Profit Per Unit",
        value: `${drawerMoney(
          structuredSkuRow?.cm2_profit_per_unit?.current,
          symbol
        )}${drawerPct(
          structuredSkuRow?.cm2_profit_per_unit?.delta_pct
        )}`,
      }
    );
  } else {
    metrics.push(
      {
        label: "CM1 Profit",
        value: `${drawerMoney(
          structuredSkuRow?.profit?.current,
          symbol
        )}${drawerPct(
          structuredSkuRow?.profit?.delta_pct
        )}`,
      },
      {
        label: "CM1 Profit Per Unit",
        value: `${drawerMoney(
          structuredSkuRow?.unit_wise_profitability?.current,
          symbol
        )}${drawerPct(
          structuredSkuRow?.unit_wise_profitability?.delta_pct
        )}`,
      }
    );
  }

  metrics.push(
    {
      label: "Current Inventory",
      value: drawerNumber(
        structuredSkuRow?.current_inventory
      ),
    },
    {
      label: "Stock Cover",
      value:
        structuredSkuRow?.selected_period_coverage_ratio !== null &&
        structuredSkuRow?.selected_period_coverage_ratio !== undefined
          ? Number(
              structuredSkuRow.selected_period_coverage_ratio
            ).toFixed(2)
          : "-",
    }
  );
}
  const journey: string[] = [];
  const recommendation: string[] = [];
  const inventoryRecommendation: string[] = [];
  const adsRecommendation: string[] = [];
  let inJourney = false;

  if (!structuredSkuRow) {
  for (const line of block) {
    if (/^product journey/i.test(line)) {
      inJourney = true;
      continue;
    }

    if (/^recommendation:/i.test(line)) {
      recommendation.push(
        line.replace(/^recommendation:\s*/i, "")
      );
      inJourney = false;
      continue;
    }

    if (/^inventory action:/i.test(line)) {
      inventoryRecommendation.push(
        line.replace(/^inventory action:\s*/i, "")
      );
      inJourney = false;
      continue;
    }

    if (/^ads action:/i.test(line)) {
      adsRecommendation.push(
        line.replace(/^ads action:\s*/i, "")
      );
      inJourney = false;
      continue;
    }

    const metricMatch = line.match(/^([^:]+):\s*(.+)$/);

    if (metricMatch) {
      metrics.push({
        label: metricMatch[1].trim(),
        value: metricMatch[2].trim(),
      });
      continue;
    }

    if (
      inJourney &&
      drawerNormalize(line) !== drawerNormalize(productName)
    ) {
      journey.push(line);
    }
  }
}

  /*
    Genuine CM2 tabhi maana jayega jab:
  
    1. CM2 Profit metric available ho
    2. CM2 Profit Per Unit available ho
    3. CM2 aur CM1 same fallback value na hon
  */

  const recMap =
  json?.recommendations?.sku_actions ||
  json?.sku_actions ||
  json?.recommendations ||
  {};

const rec =
  (sku && (
    recMap?.[sku] ||
    recMap?.[String(sku).toUpperCase()] ||
    recMap?.[String(sku).toLowerCase()]
  )) ||
  recMap?.[productName] ||
  Object.entries(recMap).find(([key, value]: any) => {
    const keyMatchesSku = drawerSkuKeys(sku).some((targetSku) =>
      drawerSkuKeys(key).some((candidateSku) =>
        candidateSku === targetSku ||
        candidateSku.includes(targetSku) ||
        targetSku.includes(candidateSku)
      )
    );

    const valueProductKey = drawerCompactKey(
      value?.product_name ?? value?.productName ?? value?.name
    );

    return keyMatchesSku ||
      (targetProductKey && valueProductKey === targetProductKey) ||
      drawerCompactKey(key) === targetProductKey;
  })?.[1] ||
  {};

  if (!metrics.length) {
    metrics.push(
      {
        label: "Units",
        value: String(
          clickedRow?.quantity ??
          clickedRow?.units ??
          clickedRow?.["Units"] ??
          "-"
        ),
      },
      {
        label: "Net Sales",
        value: String(
          clickedRow?.net_sales ??
          clickedRow?.["Net Sales"] ??
          "-"
        ),
      },
      {
        label: "ASP",
        value: String(
          clickedRow?.asp ??
          clickedRow?.["ASP"] ??
          "-"
        ),
      },
      {
        label: "CM1 Profit",
        value: String(
          clickedRow?.profit ??
          clickedRow?.cm1_profit ??
          clickedRow?.["CM1 Profit"] ??
          "-"
        ),
      },
      {
        label: "CM1 Profit Per Unit",
        value: String(
          clickedRow?.unit_wise_profitability ??
          clickedRow?.profit_per_unit ??
          clickedRow?.["CM1 Profit Per Unit"] ??
          "-"
        ),
      },
      {
        label: "Current Inventory",
        value: String(
          clickedRow?.current_inventory ??
          clickedRow?.available_inventory ??
          clickedRow?.["Current Inventory"] ??
          "-"
        ),
      },
      {
        label: "Stock Cover",
        value: String(
          clickedRow?.coverage_ratio_months ??
          clickedRow?.selected_period_coverage_ratio ??
          clickedRow?.["Stock Cover"] ??
          "-"
        ),
      }
    )
  }

  const currentCm1ProfitMetric = metrics.find(
    (metric) =>
      drawerNormalize(metric.label) === "cm1 profit"
  )

  const currentCm2ProfitMetric = metrics.find(
    (metric) =>
      drawerNormalize(metric.label) === "cm2 profit"
  )

  const currentCm2ProfitPerUnitMetric = metrics.find(
    (metric) =>
      drawerNormalize(metric.label) ===
      "cm2 profit per unit"
  )

  const currentCm1Value = getHistoricMetricNumber(
    currentCm1ProfitMetric?.value
  )

  const currentCm2Value = getHistoricMetricNumber(
    currentCm2ProfitMetric?.value
  )

  const finalHasHistoricCm2 =
    Boolean(currentCm2ProfitMetric) &&
    Boolean(currentCm2ProfitPerUnitMetric) &&
    currentCm2Value !== null &&
    (
      currentCm1Value === null ||
      Math.abs(currentCm2Value - currentCm1Value) >= 0.01
    )

  const filteredHistoricMetrics = metrics.filter((metric) => {
    const label = drawerNormalize(metric.label)

    const isCm1Metric =
      label === "cm1 profit" ||
      label === "cm1 profit per unit"

    const isCm2Metric =
      label === "cm2 profit" ||
      label === "cm2 profit per unit"

    const isAdsMetric =
      label === "ads" ||
      label === "productwise ads spend"

    if (finalHasHistoricCm2) {
      return !isCm1Metric
    }

    return !isCm2Metric && !isAdsMetric
  })

  return {
    source: "historic",
    productName,
    sku,
    metrics: filteredHistoricMetrics.map((metric) => ({
      ...metric,
      value: metric.value.replace(/^\$/, symbol),
    })),
    recommendation: drawerBullets(rec?.recommendation).length ? drawerBullets(rec?.recommendation) : recommendation,
    inventoryRecommendation: drawerBullets(rec?.inventory_recommendation).length ? drawerBullets(rec?.inventory_recommendation) : inventoryRecommendation,
    adsRecommendation: drawerBullets(rec?.ads_recommendation).length ? drawerBullets(rec?.ads_recommendation) : adsRecommendation,
    journey: drawerBullets(rec?.journey_summary).length ? drawerBullets(rec?.journey_summary) : journey,
  };
};


const drawerMetricOrder = [
  "units",
  "net sales",
  "asp",
  "ads",
  "cm2 profit",
  "cm2 profit per unit",
  "cm1 profit",
  "cm1 profit per unit",
  "current inventory",
  "stock cover",
];

const drawerMetricColors = [
  "border-[#FDD36F]",
  "border-[#75BBDA]",
  "border-[#B75A5A]",
  "border-[#C49466]",
  "border-[#7B9A6D]",
  "border-[#C49466]",
  "border-[#7B9A6D]",
  "border-[#C49466]",
  "border-[#7B9A6D]",
  "border-[#C49466]",
];

const getDrawerMetricBorder = (label: string, fallbackIndex = 0) => {
  const index = drawerMetricOrder.indexOf(drawerNormalize(label));
  return drawerMetricColors[index >= 0 ? index : fallbackIndex % drawerMetricColors.length];
};

const splitDrawerMetricValue = (value: string) => {
  const text = String(value || "").trim();
  const match = text.match(/^(.+?)\s*(\(([+-]?)[^)]+\))\s*$/);
  if (!match) return { main: text, delta: "", sign: "" };
  return { main: match[1].trim(), delta: match[2].trim(), sign: match[3] || "" };
};

const formatDrawerMainValue = (label: string, rawValue: string) => {
  const normalized = drawerNormalize(label);
  const currency = rawValue.match(/^([^0-9-]*)/)?.[1] || "";
  const numeric = Number(rawValue.replace(/[^0-9.-]/g, ""));
  if (!Number.isFinite(numeric)) return rawValue || "-";

  const wholeMetrics = new Set([
    "units",
    "net sales",
    "ads",
    "cm1 profit",
    "cm2 profit",
    "current inventory",
  ]);

  const decimalMetrics = new Set([
    "asp",
    "cm1 profit per unit",
    "cm2 profit per unit",
    "stock cover",
  ]);

  if (wholeMetrics.has(normalized)) {
    return `${currency}${Math.round(numeric).toLocaleString()}`;
  }

  if (decimalMetrics.has(normalized)) {
    return `${currency}${numeric.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })}`;
  }

  return rawValue || "-";
};

const formatDrawerDelta = (delta: string) => {
  const clean = String(delta || "").replace(/[()]/g, "").trim();
  if (!clean) return "";
  if (clean.startsWith("+")) return `▲ ${clean.slice(1)}`;
  if (clean.startsWith("-")) return `▼ ${clean.slice(1)}`;
  return clean;
};

const formatBestPerformancePeriod = (month?: string, year?: string | number) => {
  if (!month) return "-";
  const shortMonth = String(month).slice(0, 3);
  const shortYear = year ? String(year).slice(-2) : "";
  return shortYear ? `${shortMonth}'${shortYear}` : shortMonth;
};

const ProductInsightDrawer = ({
  open,
  onClose,
  data,
  loading,
  error,
  month,
  year,
  countryName,
  bestPerformanceLoading,
  bestPerformanceError,
  bestPerformanceData,
}: {
  open: boolean;
  onClose: () => void;
  data: DrawerData | null;
  loading: boolean;
  error: string | null;
  month: string;
  year: string;
  countryName: string;
  bestPerformanceLoading: boolean;
  bestPerformanceError: string | null;
  bestPerformanceData: ProductBestPerformanceData | null;
}) => {
  if (!open) return null;

  const monthShort = String(month || "").slice(0, 3);
  const formattedMonthShort = monthShort
    ? monthShort.charAt(0).toUpperCase() + monthShort.slice(1).toLowerCase()
    : "";
  const period = `${formattedMonthShort}'${String(year || "").slice(-2)}`;
  const symbol = drawerCurrencySymbol(countryName);

  const selectedMonthIndex = [
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
  ].indexOf(String(month || "").trim().toLowerCase());

  const selectedYear = Number(year);

  const now = new Date();

  const currentMonthIndex = now.getMonth();
  const currentYear = now.getFullYear();

  const previousMonthDate = new Date(
    currentYear,
    currentMonthIndex - 1,
    1
  );

  const previousMonthIndex = previousMonthDate.getMonth();
  const previousMonthYear = previousMonthDate.getFullYear();

  const showRecommendations =
    (
      selectedMonthIndex === currentMonthIndex &&
      selectedYear === currentYear
    ) ||
    (
      selectedMonthIndex === previousMonthIndex &&
      selectedYear === previousMonthYear
    );

  const bestCards = [
    {
      label: "Units",
      value: Math.round(drawerToNumber(bestPerformanceData?.units?.units)).toLocaleString(),
      period: formatBestPerformancePeriod(bestPerformanceData?.units?.month, bestPerformanceData?.units?.year),
    },
    {
      label: "Net Sales",
      value: `${symbol}${Math.round(drawerToNumber(bestPerformanceData?.net_sales?.net_sales)).toLocaleString()}`,
      period: formatBestPerformancePeriod(bestPerformanceData?.net_sales?.month, bestPerformanceData?.net_sales?.year),
    },
    {
      label: "ASP",
      value: `${symbol}${drawerToNumber(bestPerformanceData?.asp?.asp).toFixed(2)}`,
      period: formatBestPerformancePeriod(bestPerformanceData?.asp?.month, bestPerformanceData?.asp?.year),
    },
    {
      label: "CM1 Profit",
      value: `${symbol}${Math.round(drawerToNumber(bestPerformanceData?.cm1_profit?.cm1_profit)).toLocaleString()}`,
      period: formatBestPerformancePeriod(bestPerformanceData?.cm1_profit?.month, bestPerformanceData?.cm1_profit?.year),
    },
    {
      label: "CM1 Profit Per Unit",
      value: `${symbol}${drawerToNumber(bestPerformanceData?.unit_wise_profitability?.unit_wise_profitability).toFixed(2)}`,
      period: formatBestPerformancePeriod(
        bestPerformanceData?.unit_wise_profitability?.month,
        bestPerformanceData?.unit_wise_profitability?.year
      ),
    },
  ];

  return (
    <AnimatePresence>
      <>
        <motion.div
          className="fixed inset-0 z-999999 bg-black/40"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          onClick={onClose}
        />

        <motion.aside
          className="fixed right-0 top-0 z-1000000 h-screen w-[95vw] bg-white shadow-2xl sm:w-[75vw] md:w-[60vw] lg:w-[50vw]"
          initial={{ x: 520 }}
          animate={{ x: 0 }}
          exit={{ x: 520 }}
          transition={{ type: "tween", duration: 0.25 }}
        >
          <div className="flex h-full flex-col gap-4">
            <div className="flex shrink-0 items-center justify-between gap-3 border-b border-slate-200 p-3">
              <div className="min-w-0 flex-1">
                <div className="flex flex-col gap-1 sm:flex-row sm:items-center">
                  <PageBreadcrumb pageTitle="Detailed View - " variant="page" textSize="2xl" />
                  <div className="flex flex-wrap items-center gap-1">
                    <span className="text-base font-bold text-green-500 sm:text-xl lg:text-lg 2xl:text-2xl">
                      {data?.productName || "Product"}
                    </span>
                    <span className="text-base font-bold text-green-500 sm:text-xl lg:text-lg 2xl:text-2xl">
                      ({period})
                    </span>
                  </div>
                </div>
              </div>

              <button
                type="button"
                onClick={onClose}
                className="shrink-0 rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
              >
                ✕
              </button>
            </div>

            <div className="flex-1 space-y-6 overflow-y-auto px-3 pb-5">
              {loading ? (
                <div className="flex min-h-[calc(100vh-80px)] items-center justify-center text-center">
                  <Loader transparent />
                </div>
              ) : error ? (
                <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">{error}</div>
              ) : data ? (
                <>
                  <div>
                    <PageBreadcrumb pageTitle="Metrics" variant="page" align="left" textSize="xl" className="mb-2" />
                    <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-4">
                      {data.metrics.map((metric, index) => {
                        const { main, delta, sign } = splitDrawerMetricValue(metric.value);
                        const normalizedLabel = drawerNormalize(metric.label);
                        const displayLabel = normalizedLabel === "stock cover" ? "Stock Cover (Months)" : metric.label;

                        return (
                          <div
                            key={`${metric.label}-${index}`}
                            className={[
                              "flex min-h-17.5 w-full flex-col justify-between rounded-xl border border-t-4 bg-white p-1.5 shadow-sm 2xl:min-h-18 2xl:p-2",
                              getDrawerMetricBorder(metric.label, index),
                            ].join(" ")}
                          >
                            <span className="text-[10px] font-medium text-charcoal-500 2xl:text-xs">{displayLabel}</span>
                            <div className="mt-1 flex items-baseline justify-between gap-3 leading-tight tabular-nums">
                              <span className="truncate text-sm font-semibold text-charcoal-500 2xl:text-lg">
                                {formatDrawerMainValue(metric.label, main)}
                              </span>
                              {delta ? (
                                <span
                                  className={[
                                    "whitespace-nowrap text-right text-[10px] font-semibold 2xl:text-xs",
                                    sign === "+" ? "text-emerald-600" : sign === "-" ? "text-red-600" : "text-charcoal-500",
                                  ].join(" ")}
                                >
                                  {formatDrawerDelta(delta)}
                                </span>
                              ) : null}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>

                  <div>
                    <PageBreadcrumb pageTitle="Overall Best Performance" variant="page" align="left" textSize="xl" />
                    <p className="mb-2 mt-1 text-xs text-charcoal-500 2xl:text-sm">
                      Best performance is calculated from overall historical data, not just the selected period.
                    </p>

                    {bestPerformanceLoading ? (
                      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-5">
                        {Array.from({ length: 5 }).map((_, index) => (
                          <div key={index} className="h-20.5 animate-pulse rounded-xl border border-slate-200 bg-slate-50" />
                        ))}
                      </div>
                    ) : bestPerformanceError ? (
                      <div className="rounded-lg border border-red-100 bg-red-50 px-3 py-3 text-xs text-red-600 2xl:text-sm">
                        {bestPerformanceError}
                      </div>
                    ) : bestPerformanceData ? (
                      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-5">
                        {bestCards.map((card, index) => (
                          <div
                            key={card.label}
                            className={[
                              "flex min-h-19.5 w-full flex-col justify-between rounded-xl border border-t-4 bg-white p-1.5 shadow-sm 2xl:p-2",
                              getDrawerMetricBorder(card.label, index),
                            ].join(" ")}
                          >
                            <span className="text-[10px] font-medium text-charcoal-500 2xl:text-xs">{card.label}</span>
                            <div className="mt-1">
                              <div className="whitespace-nowrap text-[10px] font-medium text-charcoal-500 2xl:text-xs">{card.period}</div>
                              <div className="mt-1 whitespace-nowrap text-sm font-semibold text-charcoal-500 2xl:text-lg">{card.value}</div>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-xs text-charcoal-500 2xl:text-sm">—</div>
                    )}
                  </div>

                  {showRecommendations ? (
                    <div>
                      <PageBreadcrumb
                        pageTitle="Recommendations"
                        variant="page"
                        align="left"
                        textSize="xl"
                        className="mb-2"
                      />

                      {[
                        ["Action", data.recommendation],
                        ["Inventory", data.inventoryRecommendation],
                        ["Ads", data.adsRecommendation],
                      ].map(([title, items]: any) =>
                        items?.length ? (
                          <div key={title} className="mb-2">
                            <div className="text-xs font-semibold text-charcoal-500 2xl:text-sm">
                              {title}
                            </div>

                            <ul className="list-disc space-y-1 pl-5 text-xs text-charcoal-500 2xl:text-sm">
                              {items.map((item: string, index: number) => (
                                <li key={index}>{item}</li>
                              ))}
                            </ul>
                          </div>
                        ) : null
                      )}

                      {!data.recommendation.length &&
                        !data.inventoryRecommendation.length &&
                        !data.adsRecommendation.length ? (
                        <div className="text-xs text-charcoal-500">—</div>
                      ) : null}
                    </div>
                  ) : null}

                  <div className="w-full">
                    <Productinfoinpopup productname={data.productName} countryName={countryName} />
                  </div>

                  <div>
                    <PageBreadcrumb pageTitle="Product Journey" variant="page" align="left" textSize="xl" className="mb-2" />
                    {data.journey.length ? (
                      <ol className="list-decimal space-y-1 pl-5 text-xs text-charcoal-500 marker:font-semibold marker:text-charcoal-400 2xl:text-sm">
                        {data.journey.map((item, index) => (
                          <li key={index}>{item.replace(/^\d+\.\s*/, "").replace(/^-+\s*/, "")}</li>
                        ))}
                      </ol>
                    ) : (
                      <div className="text-xs text-charcoal-500 2xl:text-sm">—</div>
                    )}
                  </div>
                </>
              ) : null}
            </div>
          </div>
        </motion.aside>
      </>
    </AnimatePresence>
  );
};

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
              ? "pointer-events-none select-none opacity-45  transition-all duration-300"
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
                    <div className="flex h-16 w-16 items-center justify-center rounded-full  bg-blue-700">
                      <IoMdLock className="text-3xl text-yellow-200" />
                    </div>
                  </div>

                  <h3 className="text-lg font-semibold text-charcoal-500">
                    {title}
                  </h3>

                  <p className="mt-2 text-sm text-gray-600 leading-6">
                    {description}
                  </p>

                  <button
                    onClick={onAction}
                    className="mt-4 rounded-md bg-blue-700 px-4 py-2 text-sm text-yellow-200 hover:opacity-90 transition"
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

export default function InventoryFlowPage() {
  const params = useParams() as {
    countryName?: string;
    month?: string;
    year?: string;
  };

  const router = useRouter();
  const countryName = (params?.countryName ?? '').toLowerCase();

  const today = new Date();
  const currentMonthIndex = today.getMonth();
  const thisYear = today.getFullYear();

  const monthNames = [
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
  ] as const;

  const urlMonth = (params.month ?? '').toLowerCase().trim();
  const effectiveYear =
    /^\d{4}$/.test(params.year ?? '') ? String(params.year) : String(thisYear);

  const effectiveMonth: string = useMemo(() => {
    if (!urlMonth) return monthNames[currentMonthIndex];

    const numericMonthMatch = urlMonth.match(/\b(1[0-2]|0?[1-9])\b/);
    if (numericMonthMatch) {
      return monthNames[parseInt(numericMonthMatch[0], 10) - 1];
    }

    const shortMonths = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
    const shortMonthIndex = shortMonths.indexOf(urlMonth.slice(0, 3));
    if (shortMonthIndex !== -1) {
      return monthNames[shortMonthIndex];
    }

    const fullMonthIndex = monthNames.indexOf(urlMonth as any);
    return fullMonthIndex !== -1 ? monthNames[fullMonthIndex] : monthNames[currentMonthIndex];
  }, [urlMonth, currentMonthIndex]);

  const isDemoMode =
    params.month?.toUpperCase() === 'NA' &&
    params.year?.toUpperCase() === 'NA';

  const [activeTab, setActiveTab] = useState<InventoryFlowTab>('inventory');
  const [, setUploads] = useState<UploadItem[]>([]);
  const [filteredUploads, setFilteredUploads] = useState<UploadItem[]>([]);
  const [missingMonths, setMissingMonths] = useState<string[]>([]);
  const [excelData, setExcelData] = useState<ForecastRow[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showUpload, setShowUpload] = useState(false);
  const [showIntegrationModal, setShowIntegrationModal] = useState(false);
  const [sharedMonth, setSharedMonth] = useState<string>(effectiveMonth);
  const [sharedYear, setSharedYear] = useState<string>(effectiveYear);
  const [showInventoryRequirementPopup, setShowInventoryRequirementPopup] = useState(false);
  const uploadHistoryInFlightRef = useRef<string | null>(null);
  const forecastInFlightRef = useRef<string | null>(null);
  const latestForecastRequestRef = useRef<string | null>(null);
  const lastPoTriggerRef = useRef<string | null>(null);
  const [showAllDispatchRows, setShowAllDispatchRows] = useState(false);
  const [showAllPoRows, setShowAllPoRows] = useState(false);

  const [drawerOpen, setDrawerOpen] = useState(false);
  const [drawerLoading, setDrawerLoading] = useState(false);
  const [drawerError, setDrawerError] = useState<string | null>(null);
  const [drawerData, setDrawerData] = useState<DrawerData | null>(null);
  const [bestPerformanceLoading, setBestPerformanceLoading] = useState(false);
  const [bestPerformanceError, setBestPerformanceError] = useState<string | null>(null);
  const [bestPerformanceData, setBestPerformanceData] = useState<ProductBestPerformanceData | null>(null);
  const [drawerPeriodMonth, setDrawerPeriodMonth] = useState<string>(effectiveMonth);
  const [drawerPeriodYear, setDrawerPeriodYear] = useState<string>(effectiveYear);

  const isCurrentForecastMonth = useMemo(() => {
    const now = new Date();
    return (
      String(effectiveMonth || "").toLowerCase() === monthNames[now.getMonth()] &&
      Number(effectiveYear) === now.getFullYear()
    );
  }, [effectiveMonth, effectiveYear]);

  const openProductDrawer = async (
    productName: string,
    sku?: string,
    periodMonth: string = effectiveMonth,
    periodYear: string = effectiveYear
  ) => {
    const normalizedProduct = drawerNormalize(productName);
    if (!productName || ["total", "grand total", "others", "other skus", "-"].includes(normalizedProduct)) return;

    const clickedRow = (excelData || []).find((row) => {
      const rowProduct = row?.["Product Name"] ?? row?.product_name ?? row?.productName;
      const rowSku = row?.sku ?? row?.SKU;
      return (sku && drawerNormalize(rowSku) === drawerNormalize(sku)) || drawerNormalize(rowProduct) === normalizedProduct;
    }) || {};

    setDrawerPeriodMonth(periodMonth);
    setDrawerPeriodYear(periodYear);
    setDrawerOpen(true);
    setDrawerLoading(true);
    setDrawerError(null);
    setBestPerformanceLoading(true);
    setBestPerformanceError(null);
    setBestPerformanceData(null);

    const normalizedPeriodMonth = normalizeMonthForApi(periodMonth);
    const now = new Date();
    const isCurrentDrawerMonth =
      normalizedPeriodMonth === monthNames[now.getMonth()] &&
      Number(periodYear) === now.getFullYear();

    setDrawerData({ source: isCurrentDrawerMonth ? "live" : "historic", productName, sku, metrics: [], recommendation: [], inventoryRecommendation: [], adsRecommendation: [], journey: [] });

    try {
      const token = tokenOrFail();
      const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
      if (!baseUrl) throw new Error("Missing NEXT_PUBLIC_API_BASE_URL");

      const bestPerformancePromise = fetch(`${baseUrl}/ProductBestPerformance`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          product_name: productName,
          country: countryName,
          home_currency: drawerCurrencySymbol(countryName),
        }),
        cache: "no-store",
      })
        .then(async (response) => {
          const json = await response.json().catch(() => ({}));
          if (!response.ok) {
            throw new Error(json?.error || json?.message || "Failed to load best performance");
          }
          setBestPerformanceData(json?.best_performance ?? null);
        })
        .catch((bestError: any) => {
          setBestPerformanceError(bestError?.message || "Failed to load best performance");
        })
        .finally(() => {
          setBestPerformanceLoading(false);
        });

      if (isCurrentDrawerMonth) {
        const params = new URLSearchParams({ countryName });
        const response = await fetch(`${baseUrl}/live_mtd_bi?${params.toString()}`, { headers: { Authorization: `Bearer ${token}` }, cache: "no-store" });
        const json = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(json?.error || json?.message || "Failed to load live product insight");
        setDrawerData(buildLiveDrawerData(json, clickedRow, productName, sku, countryName));
      } else {
        const timeline = String(monthNames.indexOf(normalizedPeriodMonth as any) + 1);
        const params = new URLSearchParams({ country: countryName, period: "monthly", timeline, year: periodYear });
        const response = await fetch(`${baseUrl}/summary?${params.toString()}`, { headers: { Authorization: `Bearer ${token}` }, cache: "no-store" });
        const json = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(json?.error || json?.message || "Failed to load historic product insight");
        setDrawerData(buildHistoricDrawerData(json, clickedRow, productName, sku, countryName));
      }

      await bestPerformancePromise;
    } catch (err: any) {
      setDrawerError(err?.message || "Failed to load product details");
      setBestPerformanceLoading(false);
    } finally {
      setDrawerLoading(false);
    }
  };

  const handleForecastProductClick = (event: React.MouseEvent<HTMLDivElement>) => {
    const target = event.target as HTMLElement;
    const cell = target.closest("td") as HTMLTableCellElement | null;
    const row = target.closest("tr") as HTMLTableRowElement | null;
    const table = target.closest("table") as HTMLTableElement | null;
    if (!cell || !row || !table) return;

    const headers = Array.from(table.querySelectorAll("thead th")).map((node) => String(node.textContent || "").replace(/\s+/g, " ").trim());
    const cells = Array.from(row.querySelectorAll("td"));
    const clickedIndex = cells.indexOf(cell);
    const productIndex = headers.findIndex((x) => x.toLowerCase().includes("product name"));
    if (productIndex < 0 || clickedIndex !== productIndex) return;

    const values = cells.map((node) => String(node.textContent || "").replace(/\s+/g, " ").trim());
    const skuIndex = headers.findIndex((x) => ["sku", "sku code"].includes(x.toLowerCase()));
    event.preventDefault();
    event.stopPropagation();
    void openProductDrawer(
      values[productIndex],
      skuIndex >= 0 ? values[skuIndex] : undefined,
      effectiveMonth,
      effectiveYear
    );
  };

  useEffect(() => {
    if (typeof window === 'undefined') return;

    const applyHash = (rawHash?: string) => {
      const hash = (rawHash ?? window.location.hash).replace('#', '');

      if (!hash) {
        setActiveTab('inventory');
        return;
      }

      const tab = HASH_TO_TAB[hash];
      if (tab) {
        setActiveTab(tab);
      }
    };

    const onHashChange = () => applyHash(window.location.hash);

    const onPageHashNavigate = (event: Event) => {
      const customEvent = event as CustomEvent<{ hash?: string }>;
      applyHash(customEvent.detail?.hash ? `#${customEvent.detail.hash}` : window.location.hash);
    };

    applyHash(window.location.hash);

    window.addEventListener('hashchange', onHashChange);
    window.addEventListener('page-hash-navigate', onPageHashNavigate as EventListener);

    return () => {
      window.removeEventListener('hashchange', onHashChange);
      window.removeEventListener('page-hash-navigate', onPageHashNavigate as EventListener);
    };
  }, []);

  const handleTabChange = (tab: InventoryFlowTab) => {
    setActiveTab(tab);
    const hash = TAB_TO_HASH[tab];

    if (typeof window !== 'undefined') {
      window.location.hash = hash;
    }
  };

  useEffect(() => {
    setSharedMonth(effectiveMonth);
    setSharedYear(effectiveYear);
  }, [effectiveMonth, effectiveYear]);

  const tokenOrFail = () => {
    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    if (!token) {
      throw new Error('Authorization token is missing');
    }

    return token;
  };

  const normalizeMonthForApi = (month: string) => {
    const raw = String(month || '').trim().toLowerCase();

    const shortMonths = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
    const fullMonths = [
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

    const fullIndex = fullMonths.indexOf(raw);
    if (fullIndex !== -1) return fullMonths[fullIndex];

    const shortIndex = shortMonths.indexOf(raw.slice(0, 3));
    if (shortIndex !== -1) return fullMonths[shortIndex];

    const numeric = parseInt(raw, 10);
    if (!Number.isNaN(numeric) && numeric >= 1 && numeric <= 12) {
      return fullMonths[numeric - 1];
    }

    return raw;
  };

  const ensureForecastExists = async (country: string, month: string, year: string) => {
    const token = tokenOrFail();
    const normalizedMonth = normalizeMonthForApi(month);

    const res = await fetch(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/api/forecast?country=${encodeURIComponent(
        country.toLowerCase()
      )}&month=${encodeURIComponent(normalizedMonth)}&year=${encodeURIComponent(year)}`,
      {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
        },
      }
    );

    if (!res.ok) {
      const json = await res.json().catch(() => ({}));
      throw new Error(json?.error || json?.message || 'Inventory forecast not found');
    }

    return res;
  };

  async function fetchUploadHistory() {
    if (!countryName || !effectiveMonth || !effectiveYear) return;

    const requestKey = `${countryName}-${effectiveMonth}-${effectiveYear}`;

    if (uploadHistoryInFlightRef.current === requestKey) return;
    uploadHistoryInFlightRef.current = requestKey;

    setLoading(true);
    setError(null);

    try {
      const token = tokenOrFail();

      const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_history`, {
        method: 'GET',
        headers: { Authorization: `Bearer ${token}` },
      });

      if (!res.ok) {
        let message = 'An error occurred';

        try {
          const json = await res.json();
          message = json?.error || json?.message || message;
        } catch { }

        throw new Error(message);
      }

      const data = (await res.json()) as UploadHistoryRes;

      const filtered =
        countryName === 'global'
          ? data.uploads.filter((upload) =>
            ['uk', 'us'].includes((upload.country ?? '').toString().toLowerCase())
          )
          : countryName
            ? data.uploads.filter(
              (upload) => (upload.country ?? '').toString().toLowerCase() === countryName
            )
            : data.uploads;

      setUploads(data.uploads);
      setFilteredUploads(filtered);

      const months = filtered.map((upload) => {
        const monthIndex = new Date(`${upload.month} 1, ${upload.year}`).getMonth();
        return `${upload.year}-${String(monthIndex + 1).padStart(2, '0')}`;
      });

      const uniqueMonths = new Set(months);

      const currentDate = new Date();
      const currentMonth = currentDate.getMonth();
      const currentYear = currentDate.getFullYear();

      const previousFiveMonths: string[] = [];

      for (let i = 1; i <= 5; i += 1) {
        let month = currentMonth - i;
        let year = currentYear;

        if (month < 0) {
          month += 12;
          year -= 1;
        }

        previousFiveMonths.push(`${year}-${String(month + 1).padStart(2, '0')}`);
      }

      const missing = previousFiveMonths.filter((value) => !uniqueMonths.has(value));

      if (missing.length > 0) {
        const formattedMissing = missing.map((value) => {
          const [year, month] = value.split('-').map(Number);
          const date = new Date(year, month - 1, 1);
          return date.toLocaleString('default', { month: 'long', year: 'numeric' });
        });

        setMissingMonths(formattedMissing);
        setExcelData([]);
        setError(INVENTORY_REQUIREMENT_MESSAGE);
        setShowInventoryRequirementPopup(true);
        setLoading(false);
        return;
      }

      setMissingMonths([]);

      if (filtered.length < 6) {
        setExcelData([]);
        setError(INVENTORY_REQUIREMENT_MESSAGE);
        setShowInventoryRequirementPopup(true);
        setLoading(false);
        return;
      }

      await fetchForecastData();
    } catch (err: any) {
      setError(err?.message ?? 'Failed to fetch upload history');
      setLoading(false);
    } finally {
      if (uploadHistoryInFlightRef.current === requestKey) {
        uploadHistoryInFlightRef.current = null;
      }
    }
  }

  async function fetchForecastData() {
    if (!countryName || !effectiveMonth || !effectiveYear) return;

    const requestKey = `${countryName}-${effectiveMonth}-${effectiveYear}`;

    if (forecastInFlightRef.current === requestKey) return;

    forecastInFlightRef.current = requestKey;
    latestForecastRequestRef.current = requestKey;

    try {
      const token = tokenOrFail();

      const endpoint =
        countryName === 'global'
          ? `${process.env.NEXT_PUBLIC_API_BASE_URL}/forecast_global?month=${encodeURIComponent(
            effectiveMonth
          )}&year=${encodeURIComponent(effectiveYear)}`
          : `${process.env.NEXT_PUBLIC_API_BASE_URL}/api/forecast?country=${encodeURIComponent(
            countryName
          )}&month=${encodeURIComponent(effectiveMonth)}&year=${encodeURIComponent(
            effectiveYear
          )}`;

      const res = await fetch(endpoint, {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
      });

      if (latestForecastRequestRef.current !== requestKey) return;

      const contentType = res.headers.get('Content-Type') || '';

      if (!res.ok) {
        let serverMessage = '';

        try {
          const errJson = await res.json();
          serverMessage = errJson?.error || errJson?.message || errJson?.warning || '';
        } catch { }

        setExcelData([]);
        setError(serverMessage || `Server error (${res.status})`);
        setLoading(false);
        return;
      }

      if (
        contentType.includes('spreadsheetml') ||
        contentType.includes('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
      ) {
        const blob = await res.blob();
        const buffer = await blob.arrayBuffer();
        const workbook = XLSX.read(buffer, { type: 'array' });
        const sheet = workbook.Sheets[workbook.SheetNames[0]];

        const rows = XLSX.utils.sheet_to_json<any[]>(sheet, {
          header: 1,
          defval: '',
        });

        if (!rows || rows.length < 7) {
          setExcelData([]);
          setError('Forecast file format is invalid.');
          setLoading(false);
          return;
        }

        const headerRowIndex = countryName === 'global' ? 0 : 6;
        const rawHeaders = (rows[headerRowIndex] || []).map((header) =>
          String(header ?? '').trim().replace(/\s+Sold$/i, '')
        );

        const dataRows = rows.slice(headerRowIndex + 1);

        const jsonRows: ForecastRow[] = dataRows
          .filter(
            (row) => Array.isArray(row) && row.some((cell) => String(cell ?? '').trim() !== '')
          )
          .map((row) => {
            const obj: ForecastRow = {};
            rawHeaders.forEach((header, index) => {
              if (header) obj[header] = row[index] ?? '';
            });
            return obj;
          });

        setExcelData(jsonRows);
        setError(null);
        setLoading(false);
        return;
      }

      const data = await res.json();

      if (Array.isArray(data?.forecast)) {
        setExcelData(data.forecast);
        setError(null);
        setLoading(false);
        return;
      }
      setExcelData([]);
      setError(data?.warning || data?.message || 'Forecast generated, but no file was returned.');
      setLoading(false);
    } catch (err: any) {
      setExcelData([]);
      setError(err?.message || 'Failed to fetch forecast');
      setLoading(false);
    } finally {
      if (forecastInFlightRef.current === requestKey) {
        forecastInFlightRef.current = null;
      }
    }
  }

  const triggerPurchaseOrderApi = async (country: string, month: string, year: string) => {
    const jwtToken =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    if (!jwtToken) {
      throw new Error('Missing jwt token');
    }

    const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
    if (!baseUrl) {
      throw new Error('Missing NEXT_PUBLIC_API_BASE_URL');
    }

    const normalizedMonth = normalizeMonthForApi(month).toLowerCase();

    const formData = new FormData();
    formData.append('month', normalizedMonth);
    formData.append('year', year);
    formData.append('country', country.toLowerCase());

    const res = await fetch(`${baseUrl}/purchase_order`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${jwtToken}`,
        Accept: 'application/json',
      },
      body: formData,
    });

    const json = await res.json().catch(() => ({}));

    if (!res.ok) {
      throw new Error(json?.error || json?.message || 'Purchase order API failed');
    }

    return json;
  };

  useEffect(() => {
    if (activeTab !== 'purchaseOrder' || isDemoMode) return;
    if (!countryName || !sharedMonth || !sharedYear) return;

    const normalizedMonth = normalizeMonthForApi(sharedMonth);
    const key = `${countryName}-${normalizedMonth}-${sharedYear}`;
    if (lastPoTriggerRef.current === key) return;

    lastPoTriggerRef.current = key;

    const run = async () => {
      try {
        await ensureForecastExists(countryName, normalizedMonth, sharedYear);
        await triggerPurchaseOrderApi(countryName, normalizedMonth, sharedYear);
      } catch (err: any) {
        console.error('PO API error:', err);
      }
    };

    void run();
  }, [activeTab, countryName, sharedMonth, sharedYear, isDemoMode]);

  useEffect(() => {
    if (isDemoMode) {
      setExcelData(DUMMY_INVENTORY_FORECAST);
      setLoading(false);
      setError(null);
      return;
    }

    if (!countryName || !effectiveMonth || !effectiveYear) return;

    void fetchUploadHistory();
  }, [countryName, effectiveMonth, effectiveYear, isDemoMode]);

  useEffect(() => {
    const hasValidData =
      Array.isArray(excelData) &&
      excelData.length > 0 &&
      missingMonths.length === 0 &&
      !error;

    if (hasValidData) {
      setShowInventoryRequirementPopup(false);
    } else {
      setShowInventoryRequirementPopup(true);
    }
  }, [excelData, missingMonths, error]);

  const DemoDispatchPreview = () => {
    return (
      <div className="rounded-xl border border-slate-200 bg-white shadow-sm overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-green-500 text-yellow-200">
              <tr>
                <th className="px-4 py-3 text-center">S. No.</th>
                <th className="px-4 py-3 text-center">Product Name</th>
                <th className="px-4 py-3 text-center">SKU</th>
                <th className="px-4 py-3 text-center">Inventory At Month End</th>
                <th className="px-4 py-3 text-center">Coverage Ratio Before Dispatch</th>
                <th className="px-4 py-3 text-center">Dispatch</th>
                <th className="px-4 py-3 text-center">Current Inventory + Dispatch</th>
              </tr>
            </thead>

            <tbody>
              {DUMMY_DISPATCH_DATA.map((row, idx) => (
                <tr key={idx} className="border-b border-slate-100">
                  <td className="px-4 py-3 text-center">{row.sNo}</td>
                  <td className="px-4 py-3 text-center">{row.productName}</td>
                  <td className="px-4 py-3 text-center">{row.sku}</td>
                  <td className="px-4 py-3 text-center">{row.inventoryAtMonthEnd}</td>
                  <td className="px-4 py-3 text-center">{row.coverageRatioBeforeDispatch}</td>
                  <td className="px-4 py-3 text-center">{row.dispatch}</td>
                  <td className="px-4 py-3 text-center">{row.currentInventoryPlusDispatch}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  const DemoPurchaseOrderPreview = () => {
    return (
      <div className="rounded-xl border border-slate-200 bg-white shadow-sm overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-green-500 text-yellow-200">
              <tr>
                <th className="px-4 py-3 text-center">S. No.</th>
                <th className="px-4 py-3 text-center">Product Name</th>
                <th className="px-4 py-3 text-center">Dispatches UK</th>
                <th className="px-4 py-3 text-center">Total Dispatches</th>
                <th className="px-4 py-3 text-center">Current Inventory - Local Warehouse</th>
                <th className="px-4 py-3 text-center">In Transit Units</th>
                <th className="px-4 py-3 text-center">PO To Be Raised</th>
                <th className="px-4 py-3 text-center">Cost Per Unit (in INR)</th>
                <th className="px-4 py-3 text-center">PO Cost (in INR)</th>
              </tr>
            </thead>
            <tbody>
              {DUMMY_PO_DATA.map((row, idx) => (
                <tr key={idx} className="border-b border-slate-100">
                  <td className="px-4 py-3 text-center">{row.sNo}</td>
                  <td className="px-4 py-3 text-center">{row.productName}</td>
                  <td className="px-4 py-3 text-center">{row.dispatchesUK}</td>
                  <td className="px-4 py-3 text-center">{row.totalDispatches}</td>
                  <td className="px-4 py-3 text-center">{row.currentInventory}</td>
                  <td className="px-4 py-3 text-center">{row.inTransitUnits}</td>
                  <td className="px-4 py-3 text-center">{row.poToBeRaised}</td>
                  <td className="px-4 py-3 text-center">{row.costPerUnit}</td>
                  <td className="px-4 py-3 text-center">{row.poCost}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  

  const handleConnectAmazonPreview = () => {
    router.push(`/profile/${countryName}/NA/NA`);
  };

  return (
    <>
      <style jsx>{`
        .fetch-button {
          font-family: 'Lato', sans-serif;
          font-size: clamp(12px, 0.729vw, 16px) !important;
          background-color: #2c3e50;
          color: #f8edcf;
          font-weight: bold;
          border: none;
          border-radius: 5px;
          cursor: pointer;
          text-align: center;
          padding: 10px 18px;
          transition: background-color 0.2s ease;
          box-shadow: 0 3px 6px rgba(0, 0, 0, 0.15);
          white-space: nowrap;
        }

        .fetch-button:hover:not(:disabled) {
          background-color: #1f2a36;
        }

        .fetch-button:disabled {
          background-color: #6b7280;
          cursor: not-allowed;
          opacity: 0.8;
        }

        .styled-button {
          font-family: 'Lato', sans-serif;
          font-size: clamp(12px, 0.729vw, 16px) !important;
          background-color: #2c3e50;
          color: #f8edcf;
          font-weight: bold;
          border: none;
          border-radius: 5px;
          cursor: pointer;
          text-align: center;
          padding: 9px 18px;
          margin-left: auto;
        }

        .alert-container {
          display: flex;
          align-items: center;
          background-color: #f2f2f2;
          border-top: 4px solid #ff5c5c;
          padding: 12px 16px;
          border-radius: 6px;
          font-family: 'Lato', sans-serif;
          width: 100%;
          justify-content: space-between;
          box-sizing: border-box;
          margin-top: 20px;
          gap: 12px;
          flex-wrap: wrap;
        }

        .alert-message {
          display: flex;
          align-items: center;
          color: #414042;
          font-size: 12px;
        }

        .alert-icon {
          color: #ff5c5c;
          font-size: 18px;
          margin-right: 10px;
        }

        .product-click-table tbody td:nth-child(2) {
          cursor: zoom-in;
          color: #5ea68e;
          font-weight: 600;
        }

        .product-click-table tbody td:nth-child(2):hover {
          text-decoration: underline;
        }

        .alert-button {
          background: none;
          border: none;
          color: #414042;
          font-weight: 600;
          cursor: pointer;
          font-size: 14px;
          text-decoration: underline;
          display: inline-flex;
          align-items: center;
          gap: 5px;
          padding: 0;
          white-space: nowrap;
        }
      `}</style>

      <div className="font-lato">
        <div className="flex flex-col justify-start">
          <div className="sticky top-0 z-40 w-full bg-[#F7F7F7] pb-2">
            <div className="flex flex-col gap-4 pb-1 md:flex-row md:items-center md:justify-between">
              <div className="flex w-full flex-col leading-tight md:w-auto">
                <div className="flex items-baseline gap-2">
                  <PageBreadcrumb
                    pageTitle={
                      activeTab === 'dispatch'
                        ? 'Dispatch Report - Amazon'
                        : activeTab === 'purchaseOrder'
                          ? 'PO Report - Amazon'
                          : 'Inventory Forecast - Amazon'
                    }
                    variant="page"
                    align="left"
                    textSize="2xl"
                  />

                  <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
                    {countryName?.toLowerCase() === 'global'
                      ? 'Global'
                      : countryName?.toUpperCase()}
                  </span>
                </div>

                <p className="mt-1 text-xs text-charcoal-500 2xl:text-sm">
                  Plan inventory, dispatches, and purchase orders from one connected workflow.
                </p>
              </div>

              {(activeTab === 'dispatch' || activeTab === 'purchaseOrder') && (
                <div className="flex flex-wrap items-center gap-3 justify-start md:justify-end">
                  <MonthYearPickerTable
                    month={sharedMonth}
                    year={sharedYear}
                    yearOptions={[new Date().getFullYear(), new Date().getFullYear() - 1]}
                    onMonthChange={(value) => setSharedMonth(value)}
                    onYearChange={(value) => setSharedYear(value)}
                    valueMode="lower"
                  />

                  <DownloadIconButton
                    size="md"
                    onClick={() => {
                      window.dispatchEvent(
                        new CustomEvent(
                          activeTab === 'dispatch'
                            ? 'dispatch-report-download'
                            : 'po-report-download'
                        )
                      );
                    }}
                    disabled={isDemoMode}
                  />
                </div>
              )}
            </div>

            <div className="flex items-center justify-between gap-3 border-b border-gray-200 py-1.5">
              <InventoryFlowTabs value={activeTab} onChange={handleTabChange} />

              {(activeTab === 'dispatch' || activeTab === 'purchaseOrder') && (
                <button
                  type="button"
                  onClick={() => {
                    if (activeTab === 'dispatch') {
                      setShowAllDispatchRows((prev) => !prev);
                      return;
                    }

                    setShowAllPoRows((prev) => !prev);
                  }}
                  title={
                    activeTab === 'dispatch'
                      ? showAllDispatchRows
                        ? "Collapse rows"
                        : "Expand all rows"
                      : showAllPoRows
                        ? "Collapse rows"
                        : "Expand all rows"
                  }
                  aria-label={
                    activeTab === 'dispatch'
                      ? showAllDispatchRows
                        ? "Collapse rows"
                        : "Expand all rows"
                      : showAllPoRows
                        ? "Collapse rows"
                        : "Expand all rows"
                  }
                  disabled={isDemoMode}
                  className="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-0.5 hover:shadow-lg active:translate-y-0 active:shadow-md disabled:cursor-not-allowed disabled:opacity-50"
                >
                  {activeTab === 'dispatch' ? (
                    showAllDispatchRows ? (
                      <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                    ) : (
                      <RiExpandDiagonalFill size={18} className="font-extrabold" />
                    )
                  ) : showAllPoRows ? (
                    <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                  ) : (
                    <RiExpandDiagonalFill size={18} className="font-extrabold" />
                  )}
                </button>
              )}
            </div>
          </div>

          <PreviewLockedSection
            enabled={isDemoMode}
            title="Preview Mode"
            description="To view your real business data and analytics, please complete your profile and connect your Amazon account. This will unlock your performance dashboard and insights."
            buttonText="Complete Setup"
            onAction={handleConnectAmazonPreview}
          >
            <div>
              {loading ? (
                <Loading />
              ) : activeTab === 'inventory' ? (
                <div id="inventory-forecast" className="product-click-table scroll-mt-20 relative" onClickCapture={handleForecastProductClick}>
                  <DisplayInventoryForecast
                    countryName={countryName}
                    month={effectiveMonth}
                    year={effectiveYear}
                    data={excelData ?? []}
                    isDemoMode={isDemoMode}
                  />

                  {(missingMonths.length > 0 || !excelData || excelData.length === 0) && (
                    <div className="fixed inset-0 z-100 pointer-events-none">
                      <div className="absolute inset-0 bg-white/45" />

                      <div className="absolute inset-0 flex items-center justify-center px-4 py-8">
                        <div className="pointer-events-auto w-full max-w-md rounded-2xl bg-white shadow-2xl border border-slate-200 p-6 text-center">
                          <div className="mx-auto mb-4 flex h-14 w-14 items-center justify-center rounded-full bg-amber-50 border border-amber-200">
                            <svg
                              width="24"
                              height="24"
                              viewBox="0 0 24 24"
                              fill="none"
                              className="text-amber-600"
                            >
                              <path
                                d="M12 9V13M12 16H12.01M10.29 3.86L1.82 18A2 2 0 0 0 3.55 21H20.45A2 2 0 0 0 22.18 18L13.71 3.86A2 2 0 0 0 10.29 3.86Z"
                                stroke="currentColor"
                                strokeWidth="2"
                                strokeLinecap="round"
                                strokeLinejoin="round"
                              />
                            </svg>
                          </div>

                          <h3 className="text-lg font-semibold text-charcoal-500">
                            Data required
                          </h3>

                          <p className="mt-2 text-sm leading-6 text-gray-600">
                            This section requires you to fetch at least 6 months of data.
                          </p>

                          <div className="mt-5 flex items-center justify-center gap-3">

                            <button
                              onClick={() => {
                                setShowIntegrationModal(true);
                              }}
                              className="rounded-md bg-blue-700 px-4 py-2 text-sm text-yellow-200 hover:opacity-90 transition"
                            >
                              Fetch Data
                            </button>
                          </div>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              ) : activeTab === 'dispatch' ? (
                <div id="dispatch" className="scroll-mt-20">
                  {isDemoMode ? (
                    <DemoDispatchPreview />
                  ) : (
                    <DispatchPage
                      embedded
                      countryNameProp={countryName}
                      selectedMonthProp={sharedMonth}
                      selectedYearProp={sharedYear}
                      showAllRowsProp={showAllDispatchRows}
                      onShowAllRowsChange={setShowAllDispatchRows}
                      onProductNameClick={(productName, sku) =>
                        void openProductDrawer(productName, sku, sharedMonth, sharedYear)
                      }
                    />
                  )}
                </div>
              ) : (
                <div id="purchase-order" className="scroll-mt-20">
                  {isDemoMode ? (
                    <DemoPurchaseOrderPreview />
                  ) : (
                    <PurchaseOrderPage
                      embedded
                      countryNameProp={countryName}
                      selectedMonthProp={sharedMonth}
                      selectedYearProp={sharedYear}
                      showAllRowsProp={showAllPoRows}
                      onShowAllRowsChange={setShowAllPoRows}
                      onProductNameClick={(productName) =>
                        void openProductDrawer(productName, undefined, sharedMonth, sharedYear)
                      }
                    />
                  )}
                </div>
              )}
            </div>
          </PreviewLockedSection>
        </div>

        <ProductInsightDrawer
          open={drawerOpen}
          onClose={() => {
            setDrawerOpen(false);
            setDrawerData(null);
            setDrawerError(null);
            setBestPerformanceData(null);
            setBestPerformanceError(null);
          }}
          data={drawerData}
          loading={drawerLoading}
          error={drawerError}
          month={drawerPeriodMonth}
          year={drawerPeriodYear}
          countryName={countryName}
          bestPerformanceLoading={bestPerformanceLoading}
          bestPerformanceError={bestPerformanceError}
          bestPerformanceData={bestPerformanceData}
        />

        <Modal
          isOpen={showUpload}
          onClose={() => setShowUpload(false)}
          showCloseButton
          className="max-w-4xl w-full mx-auto p-0"
        >
          <FileUploadForm
            initialCountry={countryName}
            onClose={() => setShowUpload(false)}
            onComplete={() => {
              setShowUpload(false);
              void fetchUploadHistory();
            }}
          />
        </Modal>
        <IntegrationsModal
          open={showIntegrationModal}
          onClose={() => setShowIntegrationModal(false)}
        />
      </div>
    </>
  );
}
