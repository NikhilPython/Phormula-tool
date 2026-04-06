// src/lib/hooks/useConnectedPlatforms.ts
"use client";

import { useMemo } from "react";
import type { ConnectedPlatforms } from "@/lib/utils/platforms";
import { useShopifyStore } from "./useShopifyStore";
import { useAmazonConnections } from "./useAmazonConnections";
import { useGetUserDataQuery } from "../api/profileApi";

// marketplace IDs you use in DB for each marketplace
const AMAZON_UK_MARKETPLACE_ID = "A1F83G8C2ARO7P";
const AMAZON_US_MARKETPLACE_ID = "ATVPDKIKX0DER";
const AMAZON_CA_MARKETPLACE_ID = "A2EUQ1WTGCTBG2";

export const useConnectedPlatforms = (): ConnectedPlatforms => {
  const { shopifyStore } = useShopifyStore();
  const { connections } = useAmazonConnections();

const { data: userData } = useGetUserDataQuery();

const hasAmazonAds = !!userData?.amazon_ads_exists;

  const hasShopify = !!shopifyStore?.isActive;

  const connectedIds = new Set(
    (connections || []).map((c) => c.marketplace_id)
  );

  const hasAmazonUk = connectedIds.has(AMAZON_UK_MARKETPLACE_ID);
  const hasAmazonUs = connectedIds.has(AMAZON_US_MARKETPLACE_ID);
  const hasAmazonCa = connectedIds.has(AMAZON_CA_MARKETPLACE_ID);
  
  return useMemo(
    () => ({
      amazonUk: hasAmazonUk,
      amazonUs: hasAmazonUs,
      amazonCa: hasAmazonCa,
      amazonAds: hasAmazonAds,
      shopify: hasShopify,
    }),
    [hasAmazonUk, hasAmazonUs, hasAmazonCa, hasAmazonAds, hasShopify]
  );
};