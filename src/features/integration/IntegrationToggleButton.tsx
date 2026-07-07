"use client";

import React, { useState } from "react";
import IntegrationsModal from "./IntegrationsModal";
import { GrIntegration } from "react-icons/gr";

const IntegrationToggleButton: React.FC = () => {
  const [open, setOpen] = useState(false);

  return (
    <>
      <button
        onClick={() => setOpen(true)}
        aria-label="Integrations"
        className="
    inline-flex items-center justify-center rounded-full bg-blue-700 shadow
    p-2 sm:p-2.5 lg:p-1 xl:p-2 2xl:p-2.5
    active:scale-95 transition
  "
      >
        <GrIntegration className="text-yellow-200 w-3 h-3 sm:w-4 sm:h-4 2xl:w-3 2xl:h-3" />
      </button>


      <IntegrationsModal open={open} onClose={() => setOpen(false)} />
    </>
  );
};

export default IntegrationToggleButton;
