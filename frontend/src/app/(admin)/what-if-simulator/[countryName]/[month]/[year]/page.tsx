import type { Metadata } from "next";
import WhatIfSimulatorClient from "./WhatIfSimulatorClient";

export const metadata: Metadata = {
  title: "What-if Simulator | Phormula",
  description: "Model product and portfolio CM2 scenarios using previous-month costs.",
  robots: { index: false, follow: false },
};

export default function WhatIfSimulatorPage() {
  return <WhatIfSimulatorClient />;
}
