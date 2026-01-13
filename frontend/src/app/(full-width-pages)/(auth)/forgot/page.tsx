import type { Metadata } from "next";
import ForgotPasswordClient from "./ForgotPasswordClient";
import { Suspense } from "react";

export const metadata: Metadata = {
  title: "Forgot Password",
  description:
    "Reset your Phormula account password securely. Enter your email to receive a password reset link.",
  robots: {
    index: false,
    follow: false,
  },
};

export default function Page() {
  return (
    <Suspense fallback={<div className="p-6">Loading...</div>}>
      <ForgotPasswordClient />
    </Suspense>
  );
}
