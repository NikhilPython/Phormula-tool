import type { Metadata } from "next";
import CustomerReviewsClient from "./CustomerReviewsClient";

export const metadata: Metadata = {
  title: "Customer Reviews | Phormula",
  description:
    "Analyze Amazon customer review topics, rating impact, and positive and negative review trends.",
  robots: {
    index: false,
    follow: false,
  },
};

export default function CustomerReviewsPage() {
  return <CustomerReviewsClient />;
}
