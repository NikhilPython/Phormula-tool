import type { PlatformId } from "@/lib/utils/platforms";

export const platformToCurrencyCode = (platform?: PlatformId): string => {
  switch (platform) {
    case "amazon-uk":
      return "GBP";
    case "amazon-us":
      return "USD";
    case "amazon-ca":
      return "CAD";
    case "global":
      return "USD"; // pick what you prefer for global (USD is common)
    case "shopify":
      return "USD"; // or derive from store later if you want
    default:
      return "USD";
  }
};

export const currencySymbolMap: Record<string, string> = {
  USD: "$",
  GBP: "£",
  CAD: "C$",
  INR: "₹",
  EUR: "€",
  AED: "د.إ",
};

export const getCurrencySymbol = (currencyCode?: string) =>
  currencySymbolMap[currencyCode ?? ""] || currencyCode || "";
