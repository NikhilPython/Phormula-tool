import type { Metadata } from "next";
import { Suspense } from "react";
import IssuesClient from "./IssuesClient";

export const metadata: Metadata = {
  title: "Issues | Phormula Super Admin",
  description: "Review automated Super Admin issue detection results.",
  robots: {
    index: false,
    follow: false,
  },
};

export default function IssuesPage() {
  return (
    <Suspense
      fallback={
        <div className="flex min-h-[260px] items-center justify-center">
          <div className="text-sm text-white/60">Loading issues...</div>
        </div>
      }
    >
      <IssuesClient />
    </Suspense>
  );
}