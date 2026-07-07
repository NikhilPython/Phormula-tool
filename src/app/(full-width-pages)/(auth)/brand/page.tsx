import type { Metadata } from "next";
import BrandForm from "@/components/auth/BrandForm";
import { Suspense } from "react";

export const metadata: Metadata = {
  title: "Company & Brand Details",
  description:
    "Provide your company and brand details to personalize your Phormula experience during onboarding.",
  robots: { index: false, follow: false },
};

export default function BrandPage() {
  return (
    <Suspense fallback={<div className="p-6">Loading...</div>}>
      <BrandForm />
    </Suspense>
  );
}
