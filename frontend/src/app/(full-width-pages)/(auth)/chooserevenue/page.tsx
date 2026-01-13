import type { Metadata } from "next";
import RevenueForm from "@/components/auth/RevenueForm";
import { Suspense } from "react";

export const metadata: Metadata = {
  title: "Estimated Revenue",
  description:
    "Choose your estimated revenue for the next year to tailor insights, forecasts, and analytics in Phormula.",
  robots: { index: false, follow: false },
};

export default function ChooseRevenuePage() {
  return (
    <Suspense fallback={<div className="p-6">Loading...</div>}>
      <RevenueForm />
    </Suspense>
  );
}
