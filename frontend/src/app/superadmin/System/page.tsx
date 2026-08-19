// superadmin/System/page.tsx

import type { Metadata } from "next";
import SystemClient from "./SystemClient";

export const metadata: Metadata = {
  title: "System | Phormula Super Admin",
  description:
    "Manage system settings, configurations, and operational metrics.",
};

export default function SystemPage() {
  return <SystemClient />;
}