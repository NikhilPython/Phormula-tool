// app/integration/page.tsx
"use client";

import { useState } from "react";
import IntegrationDashboard from "@/features/integration/IntegrationDashboard";

export default function Page() {
  const [open, setOpen] = useState(false);

  return <IntegrationDashboard open={open} onClose={() => setOpen(false)} />;
}
