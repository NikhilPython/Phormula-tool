import type { Metadata } from "next";
import BrandsClient from "./BrandsClient";

export const metadata: Metadata = {
  title: "Brands | Phormula Super Admin",
  description:
    "View, search, and manage brand details, company information, marketplaces, users, and account records in Phormula.",
  robots: {
    index: false,
    follow: false,
  },
};

export default function BrandsPage() {
  return <BrandsClient />;
}