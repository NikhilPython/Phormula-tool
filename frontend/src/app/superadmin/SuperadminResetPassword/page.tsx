import type { Metadata } from "next";
import SuperadminResetPasswordClient from "./SuperadminResetPasswordClient";

export const metadata: Metadata = {
  title: "Reset Password | Phormula Super Admin",
  description:
    "Reset your Phormula Super Admin or Admin account password securely using email OTP verification.",
  robots: {
    index: false,
    follow: false,
  },
};

export default function SuperadminResetPasswordPage() {
  return <SuperadminResetPasswordClient />;
}