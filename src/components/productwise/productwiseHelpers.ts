export type CountryKey =
  | "global"
  | "global_gbp"
  | "global_inr"
  | "global_cad"
  | "uk"
  | "us"
  | "ca"
  | "india";

// ✅ UI ranges (compatible with PeriodFiltersTable)
export type PeriodRange = "monthly" | "quarterly" | "yearly";

// ✅ Full ranges (used in productwise pages / API)
export type Range = PeriodRange | "lifetime";

// ✅ Helpers to safely pass to PeriodFiltersTable
export const isPeriodRange = (r: any): r is PeriodRange =>
  r === "monthly" || r === "quarterly" || r === "yearly";

export const toPeriodRange = (r?: Range): PeriodRange | undefined =>
  isPeriodRange(r) ? r : undefined;

export const toPeriodRanges = (arr?: Range[]): PeriodRange[] =>
  (arr ?? []).filter(isPeriodRange);

export type MonthDatum = {
  month: string; // "October"
  month_num?: string; // "10" (optional)
  net_sales: number;
  quantity: number;
  profit: number;
  gross_margin?: number; // %
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

// productwiseHelpers.ts

export const normalizeCountryKey = (key: string): CountryKey => {
  const lower = key.toLowerCase();

  if (lower.startsWith("global")) return "global";
  if (lower.startsWith("uk")) return "uk";
  if (lower.startsWith("us")) return "us";
  if (lower.startsWith("ca")) return "ca" as CountryKey;

  return lower as CountryKey;
};

export const monthOrder = [
  "October", // you only care about ordering; your API uses capitalized names
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

export const formatCountryLabel = (country: string) => {
  const lower = country.toLowerCase();
  if (lower === "global") return "Global";
  if (lower.startsWith("global_")) {
    // e.g. global_gbp, global_inr, global_cad
    return "Global";
  }
  return country.toUpperCase();
};

// ---- currency helpers ----

export const inferCurrencyFromKey = (countryOrKey: string): string => {
  const lower = countryOrKey.toLowerCase();

  // Pounds
  if (
    lower === "uk" ||
    lower === "gbp" ||
    lower === "global_gbp" ||
    lower === "uk_gbp"
  ) {
    return "GBP";
  }

  // Rupees
  if (
    lower === "in" ||
    lower === "india" ||
    lower === "inr" ||
    lower === "global_inr"
  ) {
    return "INR";
  }

  // Canadian dollars
  if (
    lower === "ca" ||
    lower === "canada" ||
    lower === "cad" ||
    lower === "global_cad"
  ) {
    return "CAD";
  }

  // US dollars (default)
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
export const formatCurrency = (value: number, currency: string) => {
  const upper = (currency || "").toUpperCase();

  const locale =
    upper === "GBP"
      ? "en-GB"
      : upper === "INR"
      ? "en-IN"
      : upper === "CAD"
      ? "en-CA"
      : "en-US";

  return new Intl.NumberFormat(locale, {
    style: "currency",
    currency: upper,
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
};


export const formatCurrencyByCountry = (countryOrKey: string, value: number) => {
  const currency = inferCurrencyFromKey(countryOrKey);
  return formatCurrency(value, currency);
};

export const getCountryColor = (country: CountryKey) => {
  const lower = (country || "").toLowerCase();
  const colors: Record<string, string> = {
    uk: "#7B9A6D",
    us: "#3A8EA4",
    ca: "#FDD36F",
    global: "#ED9F50",
    global_gbp: "#ED9F50",
    global_inr: "#ED9F50",
    global_cad: "#ED9F50",
    global_usd: "#ED9F50",
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
