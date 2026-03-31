
import type { Metadata } from "next";
import ProfileClient from "./ProfileClient";

export const metadata: Metadata = {
  title: "Account Settings",
  description:
    "Manage your Phormula account profile. View and update personal information, address details, and account metadata securely.",
  robots: { index: false, follow: false },
  openGraph: {
    title: "Account Settings",
    description:
      "Manage your Phormula account profile. View and update personal information, address details, and account metadata securely.",
    type: "website",
  },
};

export default function ProfilePage() {
  return <ProfileClient />;
}
