"use client";

import React, { createContext, useContext, useMemo, useState } from "react";

export type HeaderNotificationItem = {
  id: string;
  title: string;
  message: string;
  type?: string;
  href?: string;
};

type NotificationContextType = {
  items: HeaderNotificationItem[];
  setItems: React.Dispatch<React.SetStateAction<HeaderNotificationItem[]>>;
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

  const value = useMemo(() => ({ items, setItems }), [items]);

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