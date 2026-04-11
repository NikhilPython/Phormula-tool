"use client";

import React, {
  createContext,
  useContext,
  useMemo,
  useState,
  useEffect,
  useCallback,
} from "react";
import { useParams } from "next/navigation";

export type HeaderNotificationItem = {
  id: string;
  title: string;
  message: string;
  type?: string;
  href?: string;
};

type NotificationContextType = {
  items: HeaderNotificationItem[];
  loading: boolean;
  refreshNotifications: () => Promise<void>;
};

const NotificationContext = createContext<NotificationContextType | undefined>(
  undefined
);

export function NotificationProvider({
  children,
}: {
  children: React.ReactNode;
}) {
  const [items, setItems] = useState<HeaderNotificationItem[]>([]);
  const [loading, setLoading] = useState(false);

  const params = useParams();
  const country = ((params?.countryName as string) || "uk").toLowerCase();

  const refreshNotifications = useCallback(async () => {
    try {
      setLoading(true);

      const token =
        typeof window !== "undefined"
          ? localStorage.getItem("jwtToken")
          : null;

      if (!token) {
        setItems([]);
        return;
      }

      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/notification`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify({ country }),
        }
      );

      const result = await response.json();

      if (!response.ok || !result?.success) {
        console.error("Failed to fetch notifications:", result);
        setItems([]);
        return;
      }

      const data = result?.data || {};

      const mappedItems: HeaderNotificationItem[] = Object.entries(data)
        .map(([sku, value]: [string, any]) => {
          const alert = value?.alert;
          const ratio = value?.inventory_coverage_ratio;

          // Only keep High alert from notification API
          if (!alert || alert.trim().toLowerCase() !== "high alert") {
            return null;
          }

          return {
            id: sku,
            title: sku,
            message:
              ratio !== null && ratio !== undefined
                ? `High alert (Coverage ratio: ${Number(ratio).toFixed(2)})`
                : "High alert",
            type: "critical",
            href: `/inventory/${country}`,
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
  }, [country]);

  useEffect(() => {
    refreshNotifications();
  }, [refreshNotifications]);

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