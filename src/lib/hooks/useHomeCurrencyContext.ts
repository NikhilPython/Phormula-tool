import { useMemo } from "react";
import { useGetUserDataQuery } from "@/lib/api/profileApi";

const getCurrencySymbol = (codeOrCountry: string) => {
  const v = (codeOrCountry || "").toLowerCase();

  switch (v) {
    case "usd":
    case "us":
    case "global":
      return "$";
    case "inr":
    case "india":
      return "₹";
    case "gbp":
    case "uk":
      return "£";
    case "eur":
    case "europe":
    case "eu":
      return "€";
    case "cad":
    case "ca":
    case "canada":
      return "C$";
    default:
      return "¤";
  }
};

export function useHomeCurrencyContext(countryName: string) {
  const { data: userData } = useGetUserDataQuery();

  // normalized (same as your page)
  const homeCurrency = (userData?.homeCurrency || "USD").toLowerCase();

  const isGlobalPage = (countryName || "").toLowerCase() === "global";

  // pass to child charts only when global
  const globalHomeCurrency = isGlobalPage ? homeCurrency : undefined;

  // show symbol on cards
  const currencySymbol = useMemo(() => {
    return isGlobalPage
      ? getCurrencySymbol(homeCurrency)
      : getCurrencySymbol(countryName || "");
  }, [isGlobalPage, homeCurrency, countryName]);

  // used for effects (only change when global currency changes)
  const fetchCurrencyKey = isGlobalPage ? homeCurrency : "country";

  return {
    homeCurrency,        // always available
    isGlobalPage,
    globalHomeCurrency,  // undefined when not global
    currencySymbol,
    fetchCurrencyKey,
  };
}
