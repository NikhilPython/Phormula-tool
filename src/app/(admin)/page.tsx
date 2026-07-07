import type { Metadata } from "next";
import DashboardClient from "@/app/(admin)/dashboard/DashboardClient";

const title = "Live Dashboard";

export const metadata: Metadata = {
  title: {
    default: title,
    template: "%s | Phormula",
  },
  description:
    "Real-time sales dashboard with Amazon, Shopify, and BI insights including MTD performance, profit metrics, and trends.",
  keywords: [
    "sales dashboard",
    "amazon analytics",
    "shopify analytics",
    "business intelligence",
    "profit analysis",
    "real time dashboard",
    "phormula",
  ],
  robots: { index: false, follow: false },
  openGraph: {
    title: `${title} | Phormula`,
    description:
      "Track real-time sales, CM2 profit, BI trends, and performance across Amazon and Shopify.",
    type: "website",
  },
};

export default function Page() {
  return <DashboardClient />;
}
