// // // components/productwise/productwiseHelpers.ts
// // export type CountryKey = "uk" | "us" | "global" | string;
// // export type Range = "monthly" | "quarterly" | "yearly";

// // export type MonthDatum = {
// //   month: string;
// //   net_sales: number;
// //   quantity: number;
// //   profit: number;
// // };

// // export type APIResponse = {
// //   success: boolean;
// //   message?: string;
// //   data: Record<CountryKey, MonthDatum[]>;
// // };

// // export const monthOrder = [
// //   "January",
// //   "February",
// //   "March",
// //   "April",
// //   "May",
// //   "June",
// //   "July",
// //   "August",
// //   "September",
// //   "October",
// //   "November",
// //   "December",
// // ];

// // export const GBP_TO_USD_RATE = 1.27;

// // export const formatCountryLabel = (country: string) => {
// //   const lower = country.toLowerCase();
// //   if (lower === "global") return "Global";
// //   return country.toUpperCase();
// // };

// // export const getCurrencyByCountry = (country: string, value: number) => {
// //   const lower = country.toLowerCase();

// //   if (lower === "uk") {
// //     return new Intl.NumberFormat("en-GB", {
// //       style: "currency",
// //       currency: "GBP",
// //       minimumFractionDigits: 0,
// //       maximumFractionDigits: 0,
// //     }).format(value);
// //   }

// //   return new Intl.NumberFormat("en-US", {
// //     style: "currency",
// //     currency: "USD",
// //     minimumFractionDigits: 0,
// //     maximumFractionDigits: 0,
// //   }).format(value);
// // };

// // export const formatCurrencyByCountry = (country: string, value: number) =>
// //   getCurrencyByCountry(country, value);

// // export const getCountryColor = (country: CountryKey) => {
// //   const colors: Record<string, string> = {
// //     uk: "#AB64B5",
// //     us: "#87AD12",
// //     global: "#F47A00",
// //   };
// //   return colors[country] || "#ff7c7c";
// // };

// // export const formatMonthYear = (monthName: string, year: number | string) => {
// //   const MONTH_ABBRS = [
// //     "Jan",
// //     "Feb",
// //     "Mar",
// //     "Apr",
// //     "May",
// //     "Jun",
// //     "Jul",
// //     "Aug",
// //     "Sep",
// //     "Oct",
// //     "Nov",
// //     "Dec",
// //   ];

// //   if (!monthName) return "";

// //   const fullNames = [
// //     "january",
// //     "february",
// //     "march",
// //     "april",
// //     "may",
// //     "june",
// //     "july",
// //     "august",
// //     "september",
// //     "october",
// //     "november",
// //     "december",
// //   ];

// //   const idx = fullNames.findIndex(
// //     (full) =>
// //       monthName.toLowerCase().startsWith(full.slice(0, 3)) ||
// //       monthName.toLowerCase() === full
// //   );

// //   const abbr = idx >= 0 ? MONTH_ABBRS[idx] : monthName.slice(0, 3) || monthName;
// //   const y = String(year);
// //   const shortYear = y.slice(-2);
// //   return `${abbr}'${shortYear}`;
// // };

















// // components/productwise/productwiseHelpers.ts

// // Include known country / region keys plus a string fallback
// export type CountryKey =
//   | "uk"
//   | "us"
//   | "global"
//   | "global_gbp"
//   | "global_inr"
//   | "global_cad"
//   | string;

// export type Range = "monthly" | "quarterly" | "yearly";

// export type MonthDatum = {
//   month: string;          // e.g. "October"
//   net_sales: number;
//   quantity: number;
//   profit: number;
// };

// export type APIResponse = {
//   success: boolean;
//   message?: string;
//   // backend now returns an array of MonthDatum per country
//   data: Record<CountryKey, MonthDatum[]>;
// };

// export const monthOrder = [
//   "October",
//   "November",
//   "December",
//   "January",
//   "February",
//   "March",
//   "April",
//   "May",
//   "June",
//   "July",
//   "August",
//   "September",
// ];

// // fallback constant (not heavily used now that FX is dynamic)
// export const GBP_TO_USD_RATE = 1.27;

// // --------------------------------------------------
// // Labels / formatting
// // --------------------------------------------------

// export const formatCountryLabel = (country: string) => {
//   const lower = country.toLowerCase();

//   if (lower === "uk") return "UK";
//   if (lower === "us") return "US";

//   // Treat any "global_*" key as just "Global"
//   if (lower === "global" || lower.startsWith("global_")) {
//     return "Global";
//   }

//   return country.toUpperCase();
// };

// /**
//  * Formats a value as currency given a logical "country" key.
//  * We use "uk" to mean GBP and "us" to mean USD.
//  * (Other callers should pass "uk" or "us" based on homeCurrency.)
//  */
// export const getCurrencyByCountry = (country: string, value: number) => {
//   const lower = country.toLowerCase();

//   if (lower === "uk") {
//     return new Intl.NumberFormat("en-GB", {
//       style: "currency",
//       currency: "GBP",
//       minimumFractionDigits: 0,
//       maximumFractionDigits: 0,
//     }).format(value);
//   }

//   // default: USD
//   return new Intl.NumberFormat("en-US", {
//     style: "currency",
//     currency: "USD",
//     minimumFractionDigits: 0,
//     maximumFractionDigits: 0,
//   }).format(value);
// };

// export const formatCurrencyByCountry = (country: string, value: number) =>
//   getCurrencyByCountry(country, value);

// // --------------------------------------------------
// // Colors
// // --------------------------------------------------

// export const getCountryColor = (country: CountryKey) => {
//   const lower = String(country).toLowerCase();

//   // Share the same colour for all "global" variants
//   if (lower === "global" || lower.startsWith("global_")) {
//     return "#F47A00"; // orange
//   }

//   if (lower === "uk") return "#AB64B5"; // purple
//   if (lower === "us") return "#87AD12"; // lime

//   // fallback colour
//   return "#ff7c7c";
// };

// // --------------------------------------------------
// // Misc helpers
// // --------------------------------------------------

// export const formatMonthYear = (monthName: string, year: number | string) => {
//   const MONTH_ABBRS = [
//     "Jan",
//     "Feb",
//     "Mar",
//     "Apr",
//     "May",
//     "Jun",
//     "Jul",
//     "Aug",
//     "Sep",
//     "Oct",
//     "Nov",
//     "Dec",
//   ];

//   if (!monthName) return "";

//   const fullNames = [
//     "january",
//     "february",
//     "march",
//     "april",
//     "may",
//     "june",
//     "july",
//     "august",
//     "september",
//     "october",
//     "november",
//     "december",
//   ];

//   const idx = fullNames.findIndex(
//     (full) =>
//       monthName.toLowerCase().startsWith(full.slice(0, 3)) ||
//       monthName.toLowerCase() === full
//   );

//   const abbr = idx >= 0 ? MONTH_ABBRS[idx] : monthName.slice(0, 3) || monthName;
//   const y = String(year);
//   const shortYear = y.slice(-2);
//   return `${abbr}'${shortYear}`;
// };


















// components/productwise/productwiseHelpers.ts

export type CountryKey =
  | "uk"
  | "us"
  | "global"
  | "global_gbp"
  | "global_inr"
  | "global_cad";

export type Range = "monthly" | "quarterly" | "yearly";

export type MonthDatum = {
  month: string;                 // "October"
  month_num?: string;            // "10" (optional)
  net_sales: number;
  quantity: number;
  profit: number;
  gross_margin?: number;         // %
  year?: number;
  conversion_rate_applied?: number | null;
};

export type APIResponse = {
  success: boolean;
  message?: string;
  // Backend sends e.g. "global": MonthDatum[], "uk": MonthDatum[], "us": MonthDatum[]
  data: Record<CountryKey, MonthDatum[]>;
  available_countries?: string[];
  time_range?: string;
  year?: number;
  quarter?: string | null;
};

export const monthOrder = [
  "October",   // you only care about ordering; your API uses capitalized names
  "November",
  "December",
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
];

// legacy – keep for other places if they still use it
// export const GBP_TO_USD_RATE = 1.27;

export const formatCountryLabel = (country: string) => {
  const lower = country.toLowerCase();
  if (lower === "global") return "Global";
  if (lower.startsWith("global_")) {
    // e.g. global_gbp, global_inr
    return "Global";
  }
  return country.toUpperCase();
};

// ---- currency helpers ----

/**
 * Given a country or global variant, decide which ISO currency code to use.
 * This is purely a mapping – NOT conversion.
 */
export const inferCurrencyFromKey = (countryOrKey: string): string => {
  const lower = countryOrKey.toLowerCase();

  if (
    lower === "uk" ||
    lower === "gbp" ||
    lower === "global_gbp" ||
    lower === "uk_gbp"
  ) {
    return "GBP";
  }

  if (
    lower === "us" ||
    lower === "usa" ||
    lower === "usd" ||
    lower === "global" ||
    lower === "global_usd"
  ) {
    return "USD";
  }

  // Fallback – most of your code is USD by default
  return "USD";
};

/**
 * Generic formatter when you already know the currency.
 */
export const formatCurrency = (value: number, currency: string) =>
  new Intl.NumberFormat(currency === "GBP" ? "en-GB" : "en-US", {
    style: "currency",
    currency,
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);

/**
 * Backwards-compatible helper; can be given "uk", "us", "global", "global_gbp", etc.
 * Uses inferCurrencyFromKey() internally.
 */
export const formatCurrencyByCountry = (
  countryOrKey: string,
  value: number
) => {
  const currency = inferCurrencyFromKey(countryOrKey);
  return formatCurrency(value, currency);
};

export const getCountryColor = (country: CountryKey) => {
  const lower = (country || "").toLowerCase();
  const colors: Record<string, string> = {
    uk: "#AB64B5",
    us: "#87AD12",
    global: "#F47A00",
    global_gbp: "#F47A00",
    global_usd: "#F47A00",
  };
  return colors[lower] || "#ff7c7c";
};

export const formatMonthYear = (monthName: string, year: number | string) => {
  const MONTH_ABBRS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];

  if (!monthName) return "";

  const fullNames = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
  ];

  const idx = fullNames.findIndex(
    (full) =>
      monthName.toLowerCase().startsWith(full.slice(0, 3)) ||
      monthName.toLowerCase() === full
  );

  const abbr = idx >= 0 ? MONTH_ABBRS[idx] : monthName.slice(0, 3) || monthName;
  const y = String(year);
  const shortYear = y.slice(-2);
  return `${abbr}'${shortYear}`;
};
