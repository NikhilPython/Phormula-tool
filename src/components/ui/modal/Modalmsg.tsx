"use client";

import React, { useEffect, useMemo } from "react";

type ModalMsgProps = {
  show: boolean;
  message: string;
  onClose: () => void;
  onCancel?: () => void;
  /** Messages that should auto-close the modal when shown (case-insensitive contains). */
  autoClosePhrases?: string[];
  /** Auto close delay (ms). Default: 1000 */
  autoCloseMs?: number;
};

export default function Modalmsg({
  show,
  message,
  onClose,
  onCancel,
  autoClosePhrases = ["file uploaded successfully", "saved and applied"],
  autoCloseMs = 1000,
}: ModalMsgProps) {
  const messageText = useMemo(() => (message || "").toLowerCase(), [message]);

  const shouldAutoClose = useMemo(
    () => autoClosePhrases.some((phrase) => messageText.includes(phrase.toLowerCase())),
    [autoClosePhrases, messageText]
  );

  // Auto-close after a short delay for specific success messages
  useEffect(() => {
    if (!show || !shouldAutoClose) return;
    const t = setTimeout(onClose, autoCloseMs);
    return () => clearTimeout(t);
  }, [show, shouldAutoClose, autoCloseMs, onClose]);

  // Close on Escape
  useEffect(() => {
    if (!show) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [show, onClose]);

  if (!show) return null;

  return (
    <div
      className="fixed inset-0 z-[999] flex items-center justify-center bg-black/40"
      role="dialog"
      aria-modal="true"
      aria-live="polite"
    >
      <div className="w-full max-w-sm rounded-lg bg-white p-6 shadow-xl dark:bg-gray-900">
        <p className="text-sm text-gray-700 dark:text-gray-200">{message}</p>

        {!shouldAutoClose && (
          <div className="mt-5 flex items-center gap-3">
            <button
              onClick={onClose}
              className="flex-1 rounded-md bg-slate-800 px-4 py-2 text-xs font-bold text-amber-100 shadow hover:opacity-95"
            >
              OK
            </button>
            <button
              onClick={onCancel}
              className="flex-1 rounded-md bg-slate-800 px-4 py-2 text-xs font-bold text-amber-100 shadow hover:opacity-95"
            >
              Cancel
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
