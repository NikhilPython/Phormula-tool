"use client";

import { useParams } from "next/navigation";
import { usePlatform } from "@/components/context/PlatformContext";

const skuColumnName = /^(?:(?:seller|merchant|product|amazon|uk|us|ca|canada|eu)?sku(?:code|id|number|asin|uk|us|ca|canada|eu)?|msku|asinsku)$/;

export function isSkuColumn(key: unknown, heading?: unknown): boolean {
  // Some product-name columns use a SKU key to look up the display name.
  const name = typeof heading === "string" && heading.trim() ? heading : key;
  return typeof name === "string" && skuColumnName.test(name.toLowerCase().replace(/[^a-z0-9]/g, ""));
}

export function filterSkuColumns<T extends { key: PropertyKey; header?: unknown; label?: unknown }>(
  columns: T[],
  hideSku: boolean
): T[] {
  return hideSku ? columns.filter(({ key, header, label }) => !isSkuColumn(key, header ?? label)) : columns;
}

export function useHideSkuColumn(marketplace?: string): boolean {
  const params = useParams<{ countryName?: string }>();
  const { platform } = usePlatform();
  const market = marketplace || params?.countryName || platform;
  return market.trim().toLowerCase() === "global";
}
