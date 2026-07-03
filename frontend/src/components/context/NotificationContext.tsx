"use client";

import React, {
  createContext,
  useContext,
  useMemo,
  useState,
  useEffect,
  useCallback,
} from "react";

export type HeaderNotificationItem = {
  id: string;
  title: string;
  message: string;
  type?: string;
  href?: string;
  timeAgo?: string;
  alertTime?: string | null;
};

type NotificationContextType = {
  items: HeaderNotificationItem[];
  loading: boolean;
  refreshNotifications: () => Promise<void>;
};

const NotificationContext = createContext<NotificationContextType | undefined>(
  undefined
);

function getTimeAgo(dateString?: string | null) {
  if (!dateString) return "";

  const date = new Date(dateString);
  if (Number.isNaN(date.getTime())) return "";

  const now = new Date();
  const diffMs = now.getTime() - date.getTime();

  if (diffMs <= 0) return "just now";

  const seconds = Math.floor(diffMs / 1000);

  if (seconds < 10) return "just now";
  if (seconds < 60) return `${seconds}s ago`;

  const minutes = Math.floor(seconds / 60);
  if (minutes === 1) return "1 min ago";
  if (minutes < 60) return `${minutes} min ago`;

  const hours = Math.floor(minutes / 60);
  if (hours === 1) return "1 hr ago";
  if (hours < 24) return `${hours} hr ago`;

  const days = Math.floor(hours / 24);
  if (days === 1) return "1 day ago";
  if (days < 7) return `${days} days ago`;

  const weeks = Math.floor(days / 7);
  if (weeks === 1) return "1 week ago";
  if (weeks < 5) return `${weeks} weeks ago`;

  const months = Math.floor(days / 30);
  if (months === 1) return "1 month ago";
  if (months < 12) return `${months} months ago`;

  const years = Math.floor(days / 365);
  return years === 1 ? "1 year ago" : `${years} years ago`;
}

function isWithinLast7Days(dateString?: string | null) {
  if (!dateString) return false;

  const date = new Date(dateString);
  if (Number.isNaN(date.getTime())) return false;

  const now = new Date();
  const diffMs = now.getTime() - date.getTime();

  if (diffMs < 0) return true;

  const sevenDaysMs = 7 * 24 * 60 * 60 * 1000;

  return diffMs <= sevenDaysMs;
}

export function NotificationProvider({
  children,
  countryName,
  month: routeMonth,
  year: routeYear,
}: {
  children: React.ReactNode;
  countryName?: string;
  month?: string;
  year?: string;
}) {
  const [items, setItems] = useState<HeaderNotificationItem[]>([]);
  const [loading, setLoading] = useState(false);

  const rawCountry = (countryName || "uk").toLowerCase();

  const country = ["uk", "us", "global"].includes(rawCountry)
    ? rawCountry
    : "uk";

  const month = routeMonth || "NA";
  const year = routeYear || "NA";

  const refreshNotifications = useCallback(async () => {
    try {
      setLoading(true);

      const token =
        typeof window !== "undefined"
          ? localStorage.getItem("jwtToken")
          : null;

      console.log("[NOTIFICATION DEBUG]", {
        baseURL: process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000",
        country,
        month,
        year,
        hasToken: !!token,
      });

      if (!token) {
        setItems([]);
        return;
      }

      const baseURL =
        process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

      const response = await fetch(`${baseURL}/notification`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({
          country,
          month: month !== "NA" ? month : null,
          year: year !== "NA" ? year : null,
        }),
      });

      const result = await response.json();

      if (!response.ok || !result?.success) {
        console.error("Failed to fetch notifications:", result);
        setItems([]);
        return;
      }

      const data = result?.data || {};

      const mappedItems: HeaderNotificationItem[] = Object.entries(data)
        .map(([productName, value]: [string, any]) => {
          const alert = String(value?.alert || "").trim().toLowerCase();
          const ratio = value?.inventory_coverage_ratio;

          if (alert !== "high alert") {
            return null;
          }

          const alertTime =
            value?.last_alert_time ||
            value?.first_alert_time ||
            null;

          if (!isWithinLast7Days(alertTime)) {
            return null;
          }

          return {
            id: value?.sku || productName,
            title: value?.product_name || productName,
            message:
              ratio !== null && ratio !== undefined
                ? `High alert (Coverage ratio (in months): ${Number(ratio).toFixed(2)})`
                : "High alert",
            type: "critical",
            href: `/live-dashboard/${country}/${month}/${year}#inventory-insights`,
            timeAgo: getTimeAgo(alertTime),
            alertTime,
          };
        })
        .filter(Boolean) as HeaderNotificationItem[];

      setItems(mappedItems);
    } catch (error) {
      console.error("Notification fetch error:", error);
      setItems([]);
    } finally {
      setLoading(false);
    }
  }, [country, month, year]);

  useEffect(() => {
    refreshNotifications();
  }, [refreshNotifications]);

  // Auto-refresh relative time every 1 minute
  // Also remove notifications older than 7 days
  useEffect(() => {
    const interval = setInterval(() => {
      setItems((prev) =>
        prev
          .filter((item) => isWithinLast7Days(item.alertTime))
          .map((item) => ({
            ...item,
            timeAgo: getTimeAgo(item.alertTime),
          }))
      );
    }, 60000);

    return () => clearInterval(interval);
  }, []);

  const value = useMemo(
    () => ({
      items,
      loading,
      refreshNotifications,
    }),
    [items, loading, refreshNotifications]
  );

  return (
    <NotificationContext.Provider value={value}>
      {children}
    </NotificationContext.Provider>
  );
}



export function useHeaderNotifications() {
  const context = useContext(NotificationContext);

  if (!context) {
    throw new Error(
      "useHeaderNotifications must be used within NotificationProvider"
    );
  }

  return context;
}