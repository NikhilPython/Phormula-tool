import type { Metadata } from "next";
import CDPAdminConsoleClient from "./CDPAdminConsoleClient";

export const metadata: Metadata = {
  title: "CDP Admin Console",
  description:
    "Securely sign in to the Phormula CDP Admin Console to manage products, inventory, forecasts, purchase orders, and business performance.",
  robots: {
    index: false,
    follow: false,
  },
};

export default function CDPAdminConsolePage() {
  return <CDPAdminConsoleClient />;
}