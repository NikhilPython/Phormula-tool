import type { Metadata } from "next";
import VerifyEmailClient from "./VerifyEmailClient";
import { Suspense } from "react";

export const metadata: Metadata = {
  title: "Verify Email",
  description:
    "Verify your email address to activate your Phormula account and complete sign-up.",
  robots: {
    index: false,
    follow: false,
  },
};

export default function Page() {
  return (
    <Suspense fallback={<div className="p-6">Loading...</div>}>
      <VerifyEmailClient />
    </Suspense>
  );
}
