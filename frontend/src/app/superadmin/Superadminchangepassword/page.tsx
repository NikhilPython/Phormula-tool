import type { Metadata } from "next";
import SuperadminChangePasswordClient from "./SuperadminChangePasswordClient";

export const metadata: Metadata = {
  title: "Change Password | Phormula Super Admin",
  description:
    "Change your Phormula Super Admin account password securely to protect dashboard, marketplace, and user management access.",
  robots: {
    index: false,
    follow: false,
  },
};

export default function SuperadminChangePasswordPage() {
  return <SuperadminChangePasswordClient />;
}