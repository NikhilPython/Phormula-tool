import ObjectivesPageClient from "@/components/user-profile/ObjectivesPageClient";

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