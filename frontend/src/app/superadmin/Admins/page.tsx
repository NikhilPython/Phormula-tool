import type { Metadata } from "next";
import AdminsClient from "./AdminsClient";

export const metadata: Metadata = {
  title: "Admins | Phormula Super Admin",
  description:
    "View, search, and manage admin details, status, brands, companies, countries, and account information in Phormula.",
  robots: {
    index: false,
    follow: false,
  },
};

export default function AdminsPage() {
  return <AdminsClient />;
}
