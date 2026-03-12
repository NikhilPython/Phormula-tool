import ObjectivesPageClient from "@/components/user-profile/ObjectivesPageClient";

export default function Page({
  params,
}: {
  params: { countryName: string };
}) {
  return <ObjectivesPageClient country={params.countryName} />;
}