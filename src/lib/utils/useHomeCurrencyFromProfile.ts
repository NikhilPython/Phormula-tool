// lib/hooks/useHomeCurrencyFromProfile.ts
"use client";

import { useEffect } from "react";
import { useFx, HomeCurrency } from "@/components/dashboard/useFx";
import { useGetUserDataQuery } from "@/lib/api/profileApi";

export function useHomeCurrencyFromProfile() {
  const { homeCurrency, setHomeCurrency, convertToHomeCurrency, formatHomeAmount } = useFx();
  const { data: userData } = useGetUserDataQuery();

  const profileHomeCurrency = (userData?.homeCurrency || "USD").toUpperCase() as HomeCurrency;

  useEffect(() => {
    if (profileHomeCurrency && profileHomeCurrency !== homeCurrency) {
      setHomeCurrency(profileHomeCurrency);
    }
  }, [profileHomeCurrency, homeCurrency, setHomeCurrency]);

  return {
    homeCurrency,              
    convertToHomeCurrency,     
    formatHomeAmount,          
    profileHomeCurrency,
    userData,
  };
}
