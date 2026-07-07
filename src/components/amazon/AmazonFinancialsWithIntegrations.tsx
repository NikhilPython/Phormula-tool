"use client";

import AmazonFinancialDashboard from "@/features/integration/AmazonFinancialDashboard";
import IntegrationsModal from "@/features/integration/IntegrationsModal";
import React, { useEffect, useState } from "react";


/**
 * Shell that shows Step 2 as a modal over the dashboard.
 * - Shows the modal on first load (configurable).
 * - Keeps the dashboard mounted (no redirect).
 */
const AmazonFinancialsWithIntegrations: React.FC = () => {
  const [showModal, setShowModal] = useState(true);

  // Optional: if you want to auto-hide the modal when already connected,
  // you could expose a prop/callback from the dashboard that reports connOK.

  return (
    <div className="mx-auto max-w-6xl p-4 sm:p-6 relative">
      {/* Modal on top */}
      <IntegrationsModal
        open={showModal}
        onClose={() => setShowModal(false)}
        onConnected={() => {
          // After connect completes, the dashboard is right below.
          setShowModal(false);
        }}
      />

      {/* Dashboard underneath */}
      <AmazonFinancialDashboard />
    </div>
  );
};

export default AmazonFinancialsWithIntegrations;
