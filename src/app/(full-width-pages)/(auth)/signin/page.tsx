import type { Metadata } from "next";
import SignInForm from "@/components/auth/SignInForm";
import { Suspense } from "react";

export const metadata: Metadata = {
  title: "Sign In",
  description:
    "Sign in to your Phormula account to access dashboards, analytics, forecasting, and reports.",
  robots: { index: false, follow: false },
};

export default function SignIn() {
  return (
    <Suspense fallback={<div className="p-6">Loading...</div>}>
      <SignInForm />
    </Suspense>
  );
}
