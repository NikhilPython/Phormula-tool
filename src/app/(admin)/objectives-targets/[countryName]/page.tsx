import ObjectivesPageClient from "@/components/user-profile/ObjectivesPageClient";

export default async function Page({
  params,
}: {
  params: Promise<{ countryName: string }>;
}) {
  const { countryName } = await params;

  return <ObjectivesPageClient country={countryName} />;
}