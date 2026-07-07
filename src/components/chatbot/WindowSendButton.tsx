"use client";
import React from "react";

interface WindowSendButtonProps {
  onClick: () => void;
  disabled?: boolean;
}

export default function WindowSendButton({
  onClick,
  disabled,
}: WindowSendButtonProps) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className="
        flex items-center justify-center
        w-8 h-8
        rounded-full
        bg-[#5EA68E]
        hover:bg-[#4e957f]
        active:scale-95
        transition
        disabled:opacity-50
      "
      title="Send"
    >
      <svg
        xmlns="http://www.w3.org/2000/svg"
        viewBox="0 0 24 24"
        fill="white"
        className="w-4 h-4 text-[#F8EDCE]"
      >
        <path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z" />
      </svg>
    </button>
  );
}
