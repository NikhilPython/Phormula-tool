// components/charts/_apex-helpers.ts
"use client";

import { useEffect, useMemo, useState } from "react";

export type FetcherParams = Record<string, string | number | undefined>;

export function useAuthedJson<T = any>(
  endpoint: string,
  params?: FetcherParams,
  options?: { enabled?: boolean; log?: boolean }
) {
  const enabled = options?.enabled ?? true;
  const log = options?.log ?? false;

  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(enabled);

  const qs = useMemo(() => {
    const url = new URL(endpoint);
    Object.entries(params || {}).forEach(([k, v]) => {
      if (v !== undefined && v !== null && String(v) !== "") {
        url.searchParams.set(k, String(v));
      }
    });
    return url.toString();
  }, [endpoint, params]);

  useEffect(() => {
    let mounted = true;
    if (!enabled) {
      setLoading(false);
      setError(null);
      setData(null);
      return;
    }
    async function run() {
      try {
        setLoading(true);
        const token =
          typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
        if (log) console.debug("[GET]", qs);
        const res = await fetch(qs, {
          headers: token ? { Authorization: `Bearer ${token}` } : {},
        });
        if (log) console.debug("[status]", res.status);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json = await res.json();
        if (mounted) setData(json);
      } catch (e: any) {
        if (mounted) setError(e?.message || "Failed to fetch");
      } finally {
        if (mounted) setLoading(false);
      }
    }
    run();
    return () => {
      mounted = false;
    };
  }, [qs, enabled, log]);

  return { data, error, loading } as const;
}
