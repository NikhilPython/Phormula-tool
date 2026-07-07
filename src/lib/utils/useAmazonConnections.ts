"use client";

import { useEffect, useState } from "react";
import { useSelector, useDispatch } from "react-redux";
import type { RootState } from "@/lib/store";
import { setConnections } from "../api/amazonSlice";

export type AmazonConnection = {
  region: string;
  marketplace_id: string;
  marketplace_name: string;
  currency: string | null;
};

type UseAmazonConnectionsReturn = {
  connections: AmazonConnection[];
  loading: boolean;
};

export const useAmazonConnections = (): UseAmazonConnectionsReturn => {
  const jwt = useSelector((state: RootState) => state.auth.token);

  // ✅ read connections from Redux
  const connections = useSelector(
    (state: RootState) => state.amazon.connections
  );

  const [loading, setLoading] = useState(false);
  const dispatch = useDispatch();

  useEffect(() => {
    if (!jwt) {
      dispatch(setConnections([]));
      return;
    }

    const fetchConnections = async () => {
      try {
        setLoading(true);
        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/amazon_api/connections`,
          {
            headers: {
              Authorization: `Bearer ${jwt}`,
            },
          }
        );

        const contentType = res.headers.get("content-type") || "";
        if (!contentType.includes("application/json")) {
          const text = await res.text();
          console.error(
            "Non-JSON response from /amazon_api/connections:",
            text
          );
          dispatch(setConnections([]));
          return;
        }

        const data = await res.json();
        console.log("Connections Data", data);

        if (!res.ok || !data?.success) {
          dispatch(setConnections([]));
          return;
        }

        // ✅ save to Redux
        dispatch(setConnections(data.connections || []));
      } catch (err) {
        console.error("Error fetching Amazon connections:", err);
        dispatch(setConnections([]));
      } finally {
        setLoading(false);
      }
    };

    fetchConnections();
  }, [jwt, dispatch]);

  return { connections, loading };
};
