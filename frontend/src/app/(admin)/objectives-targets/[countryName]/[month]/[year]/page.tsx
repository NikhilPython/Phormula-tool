// import ObjectivesPageClient from "@/components/user-profile/ObjectivesPageClient";

// export default async function Page({
//   params,
// }: {
//   params: Promise<{
//     countryName: string;
//     year: string;
//     month: string;
//   }>;
// }) {
//   const { countryName, year, month } = await params;

//   return (
//     <ObjectivesPageClient
//       country={countryName}
//       year={year}
//       month={month}
//     />
//   );
// }






import ObjectivesPageClient from "@/components/user-profile/ObjectivesPageClient";
import { Metadata } from "next";

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
  params: {
    countryName: string;
    year: string;
    month: string;
  };
}): Promise<Metadata> {
  const { countryName, year, month } = params;

  const country = formatCountry(params.countryName);
  const monthFormatted = monthName(params.month);

  return {
    title: `Business Overview | Amazon ${country}`,
    description: `Objectives for ${country} for ${monthFormatted}`,
  };
}

export default async function Page({
  params,
}: {
  params: Promise<{
    countryName: string;
    year: string;
    month: string;
  }>;
}) {
  const { countryName, year, month } = await params;

  return (
    <ObjectivesPageClient
      country={countryName}
      year={year}
      month={month}
    />
  );
}