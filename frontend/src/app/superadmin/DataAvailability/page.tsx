import type { Metadata } from "next";
import DataAvailabilityClient from "./DataAvailabilityClient";

export const metadata: Metadata = {
  title: "Data Availability | Phormula Super Admin",
  description:
    "Check connected user data availability by country, period, sync status, and data source.",
  robots: {
    index: false,
    follow: false,
  },
};

export default function DataAvailabilityPage() {
  return <DataAvailabilityClient />;
}
