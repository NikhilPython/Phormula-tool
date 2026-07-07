import type { Metadata } from "next";
import CashFlowClient from "./CashFlowClient";

type Params = {
  countryName: string;
  month: string;
  year: string;
};

const monthName = (m: string) => {
  const v = String(m).toLowerCase();
  const map: Record<string, string> = {
    "01": "January", "1": "January", "jan": "January",
    "02": "February", "2": "February", "feb": "February",
    "03": "March", "3": "March", "mar": "March",
    "04": "April", "4": "April", "apr": "April",
    "05": "May", "5": "May",
    "06": "June", "6": "June", "jun": "June",
    "07": "July", "7": "July", "jul": "July",
    "08": "August", "8": "August", "aug": "August",
    "09": "September", "9": "September", "sep": "September",
    "10": "October", "oct": "October",
    "11": "November", "nov": "November",
    "12": "December", "dec": "December",
  };
  return map[v] ?? m;
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
  const monthFormatted = monthName(decodeURIComponent(p.month || ""));
  const year = String(p.year || "");

  const title = `Cash Flow | Amazon ${country}`;

  return {
    title,
    description: `Cash flow dashboard for ${country} in ${monthFormatted} ${year}. Track sales, fees, advertising spend, taxes, reimbursements, credits, and net cash generated.`,
    robots: { index: false, follow: false },
  };
}

export default function Page() {
  return <CashFlowClient />;
}
