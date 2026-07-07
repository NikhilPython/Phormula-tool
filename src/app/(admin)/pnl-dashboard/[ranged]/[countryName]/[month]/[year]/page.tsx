// import type { Metadata } from "next";
// import CountryClient from "./CountryClient";

// type Params = {
//   ranged: string;
//   countryName: string;
//   month: string;
//   year: string;
// };

// const monthName = (m: string) => {
//   const v = String(m).toLowerCase();
//   const map: Record<string, string> = {
//     "01": "January", "1": "January", "jan": "January",
//     "02": "February", "2": "February", "feb": "February",
//     "03": "March", "3": "March", "mar": "March",
//     "04": "April", "4": "April", "apr": "April",
//     "05": "May", "5": "May",
//     "06": "June", "6": "June", "jun": "June",
//     "07": "July", "7": "July", "jul": "July",
//     "08": "August", "8": "August", "aug": "August",
//     "09": "September", "9": "September", "sep": "September",
//     "10": "October", "oct": "October",
//     "11": "November", "nov": "November",
//     "12": "December", "dec": "December",
//   };
//   return map[v] ?? m;
// };

// const formatCountry = (c: string) => {
//   const v = (c || "").toLowerCase();
//   if (v === "uk") return "UK";
//   if (v === "us") return "US";
//   if (v === "global") return "Global";
//   return v.toUpperCase();
// };

// export function generateMetadata({
//   params,
// }: {
//   params: Params;
// }): Metadata {
//   const country = formatCountry(params.countryName);
//   const monthFormatted = monthName(params.month);
//   const range = params.ranged.toUpperCase();

//   const title = `P&L Dashboard | Amazon ${country}`;
//   //  const title = ` Profit Dashboard | Amazon ${country}`;

//   return {
//     title,
//     description: `Profit and sales dashboard for ${country} (${range}) in ${monthFormatted} ${params.year}.`,
//     robots: { index: false, follow: false },
//   };
// }

// export default function Page({ params }: { params: Params }) {
//   return (
//     <CountryClient
//       ranged={params.ranged}
//       countryName={params.countryName}
//       month={params.month}
//       year={params.year}
//     />
//   );
// }










import type { Metadata } from "next";
import CountryClient from "./CountryClient";

type Params = {
  ranged: string;
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
  const monthFormatted = monthName(p.month);
  const range = p.ranged.toUpperCase();

  const title = `P&L Dashboard | Amazon ${country}`;

  return {
    title,
    description: `Profit and sales dashboard for ${country} (${range}) in ${monthFormatted} ${p.year}.`,
    robots: { index: false, follow: false },
  };
}



export default async function Page({ params }: { params: Promise<Params> }) {
  const p = await params;

  return (
    <CountryClient
      ranged={p.ranged}
      countryName={p.countryName}
      month={p.month}
      year={p.year}
    />
  );
}
