
import type { PlatformId } from "./platforms";

export type AmazonMarketplaceConfig = {
  region: string;
  marketplaceId: string;
};

export type AmazonMarketplaceDetails = AmazonMarketplaceConfig & {
  label: string;
  country: string;
};

// Map platform → Amazon API region + marketplace_id
export const AMAZON_MARKETPLACE_CONFIG: Partial<
  Record<PlatformId, AmazonMarketplaceConfig>
> = {
  "amazon-uk": {
    region: "eu-west-1",
    marketplaceId: "A1F83G8C2ARO7P", // UK, from your DB row
  },
  "amazon-us": {
    region: "na-east-1",             // TODO: adjust if your backend expects different
    marketplaceId: "ATVPDKIKX0DER",  // Standard US marketplace id
  },
  "amazon-ca": {
    region: "na-west-1",             // TODO: adjust if your backend expects different
    marketplaceId: "A2EUQ1WTGCTBG2", // Standard CA marketplace id
  },
};

export const AMAZON_MARKETPLACE_DETAILS: Record<string, AmazonMarketplaceDetails> = {
  "A1F83G8C2ARO7P": {
    label: "UK",
    country: "uk",
    region: "eu-west-1",
    marketplaceId: "A1F83G8C2ARO7P",
  },
  ATVPDKIKX0DER: {
    label: "US",
    country: "us",
    region: "na-east-1",
    marketplaceId: "ATVPDKIKX0DER",
  },
  A2EUQ1WTGCTBG2: {
    label: "Canada",
    country: "canada",
    region: "na-west-1",
    marketplaceId: "A2EUQ1WTGCTBG2",
  },
};

const COUNTRY_LABELS: Record<string, string> = {
  uk: "UK",
  gb: "UK",
  "great britain": "UK",
  "united kingdom": "UK",
  us: "US",
  usa: "US",
  "united states": "US",
  "united states of america": "US",
  ca: "Canada",
  canada: "Canada",
};

export const getAmazonMarketplaceDetails = (
  marketplaceId?: string | null
): AmazonMarketplaceDetails | null => {
  const firstMarketplaceId = String(marketplaceId || "")
    .split(",")
    .map((value) => value.trim())
    .find(Boolean);

  if (!firstMarketplaceId) return null;

  return (
    AMAZON_MARKETPLACE_DETAILS[firstMarketplaceId] || {
      label: firstMarketplaceId,
      country: "",
      region: "",
      marketplaceId: firstMarketplaceId,
    }
  );
};

export const getMarketplaceCountryLabel = (
  country?: string | null,
  marketplaceId?: string | null
) => {
  const marketplaceDetails = getAmazonMarketplaceDetails(marketplaceId);

  if (marketplaceDetails?.label) {
    return marketplaceDetails.label;
  }

  const normalizedCountry = String(country || "")
    .trim()
    .toLowerCase();

  if (!normalizedCountry) return "Unassigned";

  return COUNTRY_LABELS[normalizedCountry] || String(country).trim().toUpperCase();
};

export const getMarketplaceDisplay = (
  country?: string | null,
  marketplaceId?: string | null
) => {
  const label = getMarketplaceCountryLabel(country, marketplaceId);
  const details = getAmazonMarketplaceDetails(marketplaceId);

  return {
    label,
    marketplaceId: details?.marketplaceId || String(marketplaceId || "").trim(),
    hasMarketplace:
      label !== "Unassigned" || Boolean(String(marketplaceId || "").trim()),
  };
};

export const getMarketplaceDisplays = (
  country?: string | null,
  marketplaceId?: string | null,
  marketplaceIds?: string[] | null
) => {
  const ids = (
    marketplaceIds?.length
      ? marketplaceIds
      : String(marketplaceId || "")
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean)
  ).filter(Boolean);

  if (!ids.length) {
    return [getMarketplaceDisplay(country, marketplaceId)];
  }

  return ids.map((id) => getMarketplaceDisplay(country, id));
};

export const getAmazonConfigForPlatform = (
  platform: PlatformId
): AmazonMarketplaceConfig | null => {
  return (AMAZON_MARKETPLACE_CONFIG[platform] as AmazonMarketplaceConfig) ?? null;
};
