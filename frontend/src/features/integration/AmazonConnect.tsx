"use client";

import React, { useState } from "react";
import { FaLink } from "react-icons/fa";
import AmazonConnectLegacy from "./AmazonConnectLegacy";
import Button from "@/components/ui/button/Button";
import { TiTick } from "react-icons/ti";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

const ICONS = {
  amazonLogo: "/amazon.png",
  secure: "/secure_black.png",
};

type Props = {
  onClose?: () => void;
  onConnected?: (refreshToken?: string) => void;
  onChooseManual?: () => void;
};

export default function AmazonConnect({
  onClose,
  onConnected,
  onChooseManual,
}: Props) {
  const [showLegacy, setShowLegacy] = useState(false);

  if (showLegacy) {
    return (
      <AmazonConnectLegacy
        onClose={onClose}
        onConnected={onConnected}
      />
    );
  }

  return (
    <div
      className="fixed inset-0 z-99999 flex items-center justify-center bg-black/50 p-3 sm:p-4 md:p-6"
      role="dialog"
      aria-modal="true"
      aria-labelledby="amazon-connect-title"
      onClick={onClose}
    >
      <div
        className="
          relative w-full max-w-md sm:max-w-lg md:max-w-xl
          bg-white rounded-2xl shadow-xl
          p-4 sm:p-6 md:p-8
        "
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between relative">
          <div className="flex justify-center w-full mt-1 sm:mt-2">
            <img
              src={ICONS.amazonLogo}
              alt="Amazon"
              className="w-12 h-12 sm:w-14 sm:h-14 md:w-16 md:h-16 mx-auto"
            />
          </div>
        </div>

        <PageBreadcrumb
          pageTitle="Connect Amazon Account"
          align="center2"
          variant="table"
          textSize="2xl"
        />

        <p
          className="
            text-center
            text-xs sm:text-sm md:text-base
            text-[#414042] font-bold
            mt-2 mb-4 sm:mb-5 md:mb-6
            w-[90%] sm:w-[80%] m-auto
          "
        >
          Sync your Amazon Seller Central data to access analytics and insights
        </p>

        <div className="w-full border-t border-gray-300 mb-4 sm:mb-6" />

        <div
          className="
            rounded-lg border border-[#5EA68E26]
            bg-emerald-50/50
            p-3 sm:p-4 mb-4 sm:mb-5
          "
        >
          <div className="flex items-center gap-3 mb-2 sm:mb-3">
            <div className="flex items-center justify-center w-9 h-9 sm:w-10 sm:h-10 rounded-lg bg-[#D9D9D9]">
              <FaLink size={16} color="#5EA68E" />
            </div>
            <div className="flex flex-col">
              <h3 className="font-semibold text-[#414042] text-xs sm:text-sm md:text-base leading-tight">
                Link your Amazon Account
              </h3>
              <p className="text-[10px] sm:text-xs md:text-sm text-[#5EA68E] mt-0.5">
                Setup in 30 seconds
              </p>
            </div>
          </div>

          <ul className="text-xs sm:text-sm text-[#414042] space-y-2 sm:space-y-3 mb-3 sm:mb-4 ml-3 sm:ml-4">
            {[
              "Real-time data synchronization",
              "Always up-to-date analytics",
              "Secure encrypted connection",
              "No manual work required",
            ].map((text) => (
              <li key={text} className="flex items-start gap-2">
                <TiTick className="w-4 h-4 sm:w-5 sm:h-5 text-green-500 mt-[1px]" />
                <span>{text}</span>
              </li>
            ))}
          </ul>

          <Button
            type="button"
            variant="primary"
            size="sm"
            onClick={() => setShowLegacy(true)}
            className="w-full bg-blue-700"
          >
            <FaLink size={14} />
            <span className="text-[#F8EDCE] text-xs sm:text-sm">
              Connect
            </span>
          </Button>
        </div>

        <div className="flex items-center justify-center w-full mt-4 sm:mt-5 mb-3 sm:mb-4">
          <div className="flex-grow border-t border-gray-300" />
          <span className="mx-3 text-xs sm:text-sm text-[#414042] font-medium">
            or
          </span>
          <div className="flex-grow border-t border-gray-300" />
        </div>

        <div className="text-center mb-4 sm:mb-5">
          <button
            type="button"
            onClick={() => onChooseManual?.()}
            className="mt-1 text-[#414042] text-xs sm:text-sm font-medium"
          >
            Set up manual file uploads instead &raquo;
          </button>
        </div>

        <div className="w-full border-t border-gray-300 mb-4 sm:mb-5" />

        <div className="flex items-center justify-center gap-2 text-[10px] sm:text-xs md:text-sm text-[#414042]">
          <img
            src={ICONS.secure}
            alt="Secure"
            className="w-3 h-3 sm:w-4 sm:h-4 opacity-70"
          />
          <span>Your credentials are encrypted and stored securely</span>
        </div>
      </div>
    </div>
  );
}