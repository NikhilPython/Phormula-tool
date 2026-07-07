// src/lib/hooks/useShopifyStore.ts
"use client";

import { useEffect, useState } from "react";
import { useSelector } from "react-redux";
import type { RootState } from "@/lib/store"; // adjust to your store setup

type ShopifyStore = {
  shop: string;    // "skin-elements.myshopify.com"
  token: string;   // "shpca_..."
  email: string;   // "raghav@solidus.life"
  isActive: boolean;
};

type UseShopifyStoreReturn = {
  shopifyStore: ShopifyStore | null;
  loading: boolean;
};

export const useShopifyStore = (): UseShopifyStoreReturn => {
  const reduxToken = useSelector(
    (state: RootState) => state.auth.token 
  );

  const [shopifyStore, setShopifyStore] = useState<ShopifyStore | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const fetchShopifyStore = async () => {
      if (!reduxToken) {
        setShopifyStore(null);
        return;
      }

      try {
        setLoading(true);

        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/shopify/store`,
          {
            headers: {
              Authorization: `Bearer ${reduxToken}`,
            },
          }
        );

        const contentType = res.headers.get("content-type") || "";
        if (!contentType.includes("application/json")) {
          const text = await res.text();
          console.error("Non-JSON response from /shopify/store:", text);
          setShopifyStore(null);
          return;
        }

        const data = await res.json();
        // console.log("Shopify store from backend:", data);

        if (!res.ok || data?.error) {
          setShopifyStore(null);
          return;
        }

        // 🔴 THIS is the important mapping to YOUR shape:
        setShopifyStore({
          shop: data.shop_name,          // "skin-elements.myshopify.com"
          token: data.access_token,      // "shpca_..."
          email: data.email,             // "raghav@solidus.life"
          isActive: !!data.is_active,
        });
      } catch (err) {
        console.error("Error fetching Shopify store:", err);
        setShopifyStore(null);
      } finally {
        setLoading(false);
      }
    };

    fetchShopifyStore();
  }, [reduxToken]);

  return { shopifyStore, loading };
};
