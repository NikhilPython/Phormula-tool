
import type { PlatformId } from "./platforms";

export type AmazonMarketplaceConfig = {
  region: string;
  marketplaceId: string;
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

export const getAmazonConfigForPlatform = (
  platform: PlatformId
): AmazonMarketplaceConfig | null => {
  return (AMAZON_MARKETPLACE_CONFIG[platform] as AmazonMarketplaceConfig) ?? null;
};
