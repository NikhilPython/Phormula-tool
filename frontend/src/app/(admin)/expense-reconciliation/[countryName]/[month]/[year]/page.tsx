import type { Metadata } from "next";
import ReferralFeesClient from "./ReferralFeesClient";

type Params = {
  countryName: string;
};

const formatCountry = (c: string) => {
  const v = (c || "").toLowerCase();
  if (v === "uk") return "UK";
  if (v === "us") return "US";
  if (v === "india") return "India";
  if (v === "ca") return "Canada";
  if (v === "global") return "Global";
  return v.toUpperCase();
};

export async function generateMetadata({
  params,
}: {
  params: Promise<Params>;
}): Promise<Metadata> {
  const p = await params;

  const country = formatCountry(p.countryName);

  const title = `Expense Reconciliation | Amazon ${country}`;

  return {
    title,
    description: `Expense Reconciliation dashboard for ${country}. Review sales summaries, referral fee breakdowns, and product-wise overcharge details.`,
    robots: { index: false, follow: false },
  };
}

export default function Page() {
  return <ReferralFeesClient />;
}
