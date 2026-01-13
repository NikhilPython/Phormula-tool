import type { Metadata } from "next";
import LiveBusinessClient from "./liveBusinessClient";

type Params = {
  ranged: string;
  countryName: string;
  month: string;
  year: string;
};

export async function generateMetadata(
  { params }: { params: Promise<Params> }
): Promise<Metadata> {
  const { countryName, month, year } = await params;

  return {
    title: `Live Business Insight | ${countryName.toUpperCase()} ${month} ${year}`,
    robots: { index: false, follow: false },
  };
}

export default function Page({ params }: any) {
  const { ranged, countryName, month, year } = params;

  return (
    <LiveBusinessClient
      ranged={ranged}
      countryName={countryName}
      month={month}
      year={year}
    />
  );
}
