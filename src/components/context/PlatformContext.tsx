"use client";

import React, { createContext, useContext, useEffect, useMemo, useState } from "react";
import type { PlatformId } from "@/lib/utils/platforms";

type PlatformContextValue = {
  platform: PlatformId;
  setPlatform: (p: PlatformId) => void;
};

const PlatformContext = createContext<PlatformContextValue | null>(null);

export function PlatformProvider({ children }: { children: React.ReactNode }) {
  const [platform, setPlatformState] = useState<PlatformId>("global");

  // init from localStorage
  useEffect(() => {
    const saved = localStorage.getItem("selectedPlatform") as PlatformId | null;
    if (saved) setPlatformState(saved);
  }, []);

  const setPlatform = (p: PlatformId) => {
    setPlatformState(p);
    localStorage.setItem("selectedPlatform", p);
  };

  const value = useMemo(() => ({ platform, setPlatform }), [platform]);
  return <PlatformContext.Provider value={value}>{children}</PlatformContext.Provider>;
}

export function usePlatform() {
  const ctx = useContext(PlatformContext);
  if (!ctx) throw new Error("usePlatform must be used within PlatformProvider");
  return ctx;
}
