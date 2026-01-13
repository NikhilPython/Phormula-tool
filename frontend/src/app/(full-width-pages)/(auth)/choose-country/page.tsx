import ChooseCountryForm from "@/components/auth/ChooseCountryForm";
import { Metadata } from "next";
import { Suspense } from "react";

export const metadata: Metadata = {
  title: "Choose Country | TailAdmin - Next.js Dashboard Template",
  description: "Select countries to get started",
};

export default function ChooseCountryPage() {
  return (
    <Suspense fallback={<div className="p-6">Loading...</div>}>
      <ChooseCountryForm />
    </Suspense>
  );
}
