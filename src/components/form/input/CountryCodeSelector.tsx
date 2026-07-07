"use client";

import { countryCodeToFlagEmoji } from "@/lib/utils/flags";
import React from "react";

export type CountryOption = {
  name: string;
  nameLocal?: string; // ✅ ADD THIS (optional)
  dialCode: string;
  iso2: string;
};

const COUNTRY_OPTIONS: CountryOption[] = [
  { name: "India", nameLocal: "India", dialCode: "+91", iso2: "IN" },
  { name: "United States", dialCode: "+1", iso2: "US" },
  { name: "United Kingdom", dialCode: "+44", iso2: "GB" },
  { name: "Canada", dialCode: "+1", iso2: "CA" },
  { name: "Australia", dialCode: "+61", iso2: "AU" },
  { name: "Germany", dialCode: "+49", iso2: "DE" },
  { name: "France", dialCode: "+33", iso2: "FR" },
  { name: "Singapore", dialCode: "+65", iso2: "SG" },
  { name: "United Arab Emirates", dialCode: "+971", iso2: "AE" },
  { name: "Japan", dialCode: "+81", iso2: "JP" },
  { name: "Brazil", dialCode: "+55", iso2: "BR" },
  { name: "South Africa", dialCode: "+27", iso2: "ZA" },
  // 👉 add all remaining countries here using the full JSON you have
];

interface CountryCodeSelectorProps {
  value: string; // dial code: "+91"
  onChange: (value: string) => void;
}

export default function CountryCodeSelector({
  value,
  onChange,
}: CountryCodeSelectorProps) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="w-full rounded-lg border border-gray-300 bg-white px-2 py-2 text-sm text-gray-800 dark:border-gray-700 dark:bg-gray-900 dark:text-white/90"
    >
      {COUNTRY_OPTIONS.map((c) => (
        <option key={c.iso2} value={c.dialCode}>
          {countryCodeToFlagEmoji(c.iso2)} {c.name} ({c.dialCode})
        </option>
      ))}
    </select>
  );
}
