
"use client";

import { useEffect } from "react";
import { usePathname } from "next/navigation";
import { toast } from "sonner";

const baseURL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

const REFRESH_SCOPE_KEY = "live-dashboard-refresh-scope";
const NOTIFIED_REFRESH_KEY = "live-dashboard-refresh-notified";

type RefreshScope = {
  country: string;
  platform: string;
  region: string;
  month: number;
  year: number;
  startDay?: number;
  endDay?: number;
};

type RefreshStatus = {
  success?: boolean;
  status?: "idle" | "running" | "saving" | "completed" | "failed";
  refresh_id?: string | null;
  error?: string;
};

export default function GlobalRefreshToast() {
  const pathname = usePathname();

  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    if (pathname?.startsWith("/live-dashboard/")) return;

    const scheduleNextCheck = () => {
      if (cancelled || timer) return;
      timer = setTimeout(() => {
        timer = undefined;
        void checkRefreshStatus();
      }, 3000);
    };

    const checkRefreshStatus = async () => {
      try {
        const token = localStorage.getItem("jwtToken");
        const savedScope = localStorage.getItem(REFRESH_SCOPE_KEY);

        if (!token || !savedScope) return;

        const scope = JSON.parse(savedScope) as RefreshScope;

        const params = new URLSearchParams();

        Object.entries(scope).forEach(([key, value]) => {
          if (value !== undefined && value !== null) {
            params.set(key, String(value));
          }
        });

        const response = await fetch(
          `${baseURL}/amazon_api/live-dashboard/refresh?${params}`,
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${token}`,
              Accept: "application/json",
            },
            cache: "no-store",
          }
        );

        if (!response.ok) {
          scheduleNextCheck();
          return;
        }

        const result = (await response.json()) as RefreshStatus;

        if (cancelled) return;

        const refreshId = String(result.refresh_id || "");

        if (result.status === "running" || result.status === "saving") {
          scheduleNextCheck();
          return;
        }

        if (refreshId) {
          const notifiedId = localStorage.getItem(NOTIFIED_REFRESH_KEY);

          if (result.status === "completed" && notifiedId !== refreshId) {
            localStorage.setItem(NOTIFIED_REFRESH_KEY, refreshId);

            toast.success("All data updated successfully", {
              duration: 5000,
            });
          }

          if (result.status === "failed" && notifiedId !== refreshId) {
            localStorage.setItem(NOTIFIED_REFRESH_KEY, refreshId);

            toast.error(
              result.error || "Refresh failed. Please try again.",
              { duration: 7000 }
            );
          }
        }

        if (localStorage.getItem(REFRESH_SCOPE_KEY) === savedScope) {
          localStorage.removeItem(REFRESH_SCOPE_KEY);
        }
      } catch (error) {
        console.error("Global refresh status check failed:", error);
        scheduleNextCheck();
      }
    };

    void checkRefreshStatus();

    const handleRefreshScopeChange = (event: StorageEvent) => {
      if (event.key !== REFRESH_SCOPE_KEY || !event.newValue) return;
      if (timer) clearTimeout(timer);
      timer = undefined;
      void checkRefreshStatus();
    };
    window.addEventListener("storage", handleRefreshScopeChange);

    return () => {
      cancelled = true;

      if (timer) {
        clearTimeout(timer);
      }
      window.removeEventListener("storage", handleRefreshScopeChange);
    };
  }, [pathname]);

  return null;
}
