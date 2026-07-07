import type { Metadata } from "next";
import ResetPasswordClient from "./ResetPasswordClient";
import { Suspense } from "react";

export const metadata: Metadata = {
  title: "Reset Password",
  description:
    "Set a new password for your Phormula account securely using your password reset link.",
  robots: {
    index: false,
    follow: false,
  },
};

export default function Page() {
  return (
    <Suspense fallback={<div className="p-6">Loading...</div>}>
      <ResetPasswordClient />
    </Suspense>
  );
}
