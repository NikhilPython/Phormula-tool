// "use client";

// import React from "react";
// import dynamic from "next/dynamic";

// // Adjust this import to wherever you placed the component you pasted in your message
// const AmazonFinancialDashboard = dynamic(
//   () => import("@/features/integration/AmazonFinancialDashboard"),
//   { ssr: false }
// );

// export default function AmazonFinancialsPage() {
//   return (
//     <div className="mx-auto max-w-6xl p-4 sm:p-6">
//       <AmazonFinancialDashboard />
//     </div>
//   );
// }



"use client";

import React from "react";
import dynamic from "next/dynamic";

const AmazonFinancialsWithIntegrations = dynamic(
  () => import("@/components/amazon/AmazonFinancialsWithIntegrations"),
  { ssr: false }
);

export default function AmazonFinancialsPage() {
  return <AmazonFinancialsWithIntegrations />;
}
