import type { Metadata } from "next";
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
  return <IssuesClient />;
}
