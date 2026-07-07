import type { Metadata } from "next";
import UploadsPageClient from "./UploadsPageClient";

export function generateMetadata(): Metadata {
  const title = "Upload History";

  return {
    title: `${title}`,
    description:
      "View your uploaded MTD files grouped by country and quarter. Quickly jump to monthly performance dashboards for each upload.",
    robots: {
      index: false,
      follow: false,
    },
  };
}

export default function Page() {
  return <UploadsPageClient />;
}
