"use client";

import Dropdowns from "@/components/dropdowns/Dropdowns";

export default function CountryClient({
  ranged,
  countryName,
  month,
  year,
}: {
  ranged: string;
  countryName: string;
  month: string;
  year: string;
}) {
  return (
    <Dropdowns
      key={`${ranged}-${countryName}-${month}-${year}`}
      initialRanged={ranged}
      initialCountryName={countryName}
      initialMonth={month}
      initialYear={year}
    />
  );
}
