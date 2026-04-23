"use client";

import React from "react";
import { Modal } from "@/components/ui/modal";

// type Props = {
//   isOpen: boolean;
//   onClose: () => void;
//   onConnectAds: () => void;
// };

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
      <div className="rounded-2xl bg-white p-6">
        <h2 className="text-xl font-semibold text-[#414042]">
          Amazon data fetched successfully
        </h2>

        <p className="mt-3 text-sm text-gray-600 leading-6">
          Your Amazon data has been synced successfully.
          Want to connect Amazon Ads now?
        </p>

        <div className="mt-6 flex gap-3">
          <button
            onClick={() => onConnectAds(country)}
            className="rounded-md bg-[#37455F] px-4 py-2 text-sm text-[#F8EDCE] hover:opacity-90"
          >
            Connect Now
          </button>

          <button
            onClick={onClose}
            className="rounded-md border border-gray-300 px-4 py-2 text-sm text-gray-700"
          >
            Later
          </button>
        </div>
      </div>
    </Modal>
  );
}