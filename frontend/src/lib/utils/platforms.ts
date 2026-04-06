// src/lib/utils/platforms.ts
import type { RegionOption } from "@/components/sidebar/RegionSelect";

export type PlatformId =
  | "global"
  | "amazon-uk"
  | "amazon-us"
  | "amazon-ca"
  | "amazon-ads"
  | "shopify";

export type ConnectedPlatforms = {
  amazonUk: boolean;
  amazonUs: boolean;
  amazonCa: boolean;
  amazonAds: boolean;
  shopify: boolean;
};

export const ALL_PLATFORM_DEFS: { id: PlatformId; label: string }[] = [
  { id: "global", label: "Global Snapshot" },
  { id: "amazon-uk", label: "Amazon UK" },
  { id: "amazon-us", label: "Amazon US" },
  { id: "amazon-ca", label: "Amazon CA" },
  { id: "amazon-ads", label: "Amazon Ads" },
  { id: "shopify", label: "Shopify" },
];

export const buildPlatformOptions = (
  connected: ConnectedPlatforms
): RegionOption[] => {
  const opts: RegionOption[] = [];

  // Always show Global
  opts.push({ value: "global", label: "Global Snapshot" });

  if (connected.amazonUk) {
    opts.push({ value: "amazon-uk", label: "Amazon UK" });
  }
  if (connected.amazonUs) {
    opts.push({ value: "amazon-us", label: "Amazon US" });
  }
  if (connected.amazonCa) {
    opts.push({ value: "amazon-ca", label: "Amazon CA" });
  }

  // Intentionally NOT adding amazon-ads here,
  // because this list powers the sidebar region/platform selector.
  // Add it only if you want Amazon Ads selectable as a page platform.

  if (connected.shopify) {
    opts.push({ value: "shopify", label: "Shopify" });
  }

  return opts;
};

export const platformToCountryName = (platform: PlatformId): string => {
  switch (platform) {
    case "global":
      return "global";
    case "amazon-uk":
      return "uk";
    case "amazon-us":
      return "us";
    case "amazon-ca":
      return "ca";
    case "amazon-ads":
      return "global";
    case "shopify":
      return "global";
    default:
      return "global";
  }
};