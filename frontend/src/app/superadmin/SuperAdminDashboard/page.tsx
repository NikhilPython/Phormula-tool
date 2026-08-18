import type { Metadata } from "next";
import SuperAdminDashboardClient from "./SuperAdminDashboardClient";

export const metadata: Metadata = {
  title: "Super Admin Dashboard",
  description:
    "Manage users, brands, companies, countries, and account status from the Phormula Super Admin Dashboard.",
  robots: {
    index: false,
    follow: false,
  },
};

export default function SuperAdminDashboardPage() {
  return <SuperAdminDashboardClient />;
}
