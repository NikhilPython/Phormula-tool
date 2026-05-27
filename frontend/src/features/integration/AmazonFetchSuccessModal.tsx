"use client";

import React from "react";
import { Modal } from "@/components/ui/modal";

type Props = {
  isOpen: boolean;
  onClose: () => void;
  onConnectAds: (country: "UK" | "US" | "CA") => void;
  country: "UK" | "US" | "CA";
};

export default function AmazonFetchSuccessModal({
  isOpen,
  onClose,
  onConnectAds,
  country,
}: Props) {
  return (
    <Modal
      isOpen={isOpen}
      onClose={onClose}
      className="m-4 max-w-md"
      showCloseButton
    >
      <div className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        {/* <div className="mb-4 flex h-11 w-11 items-center justify-center rounded-full border border-[#5EA68E]/30 bg-[#5EA68E]/10">
          <span className="text-xl font-semibold text-[#5EA68E]">✓</span>
        </div> */}

        <h2 className="text-lg font-semibold text-[#414042]">
          Amazon data fetched successfully
        </h2>

        <p className="mt-2 text-sm leading-6 text-gray-600">
          Your Amazon data has been synced successfully. Would you like to
          connect Amazon Ads now?
        </p>

        <div className="mt-6 flex items-center justify-end gap-3 pt-4">
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 transition hover:bg-gray-50"
          >
            Later
          </button>

          <button
            type="button"
            onClick={() => onConnectAds(country)}
            className="rounded-lg border border-[#37455F] bg-[#37455F] px-4 py-2 text-sm font-medium text-[#F8EDCE] transition hover:opacity-90"
          >
            Connect Now
          </button>
        </div>
      </div>
    </Modal>
  );
}