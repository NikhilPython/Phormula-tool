
import type { Metadata } from "next";
import ProfileClient from "./ProfileClient";

export const metadata: Metadata = {
  title: "User Profile",
  description:
    "Manage your Phormula account profile. View and update personal information, address details, and account metadata securely.",
  robots: { index: false, follow: false },
  openGraph: {
    title: "User Profile",
    description:
      "Access and manage your personal profile, address, and account settings in Phormula.",
    type: "website",
  },
};

export default function ProfilePage() {
  return <ProfileClient />;
}
