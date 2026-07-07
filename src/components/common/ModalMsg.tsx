"use client";

import React from "react";

type Props = {
  show: boolean;
  message: string;
  onClose: () => void;
  onCancel?: () => void;
};

export default function ModalMsg({ show, message, onClose, onCancel }: Props) {
  if (!show) return null;
  return (
    <div className="fixed inset-0 z-[1000] flex items-center justify-center bg-black/40 p-4">
      <div className="w-full max-w-md rounded-xl bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 p-6 shadow-xl">
        <h3 className="text-lg font-semibold text-gray-800 dark:text-white/90 mb-2">
          Heads up
        </h3>
        <p className="text-gray-700 dark:text-gray-300">{message}</p>
        <div className="mt-5 flex justify-end gap-3">
          {onCancel && (
            <button
              onClick={onCancel}
              className="rounded-lg border border-gray-300 dark:border-gray-700 px-4 py-2 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-white/5"
            >
              Cancel
            </button>
          )}
          <button
            onClick={onClose}
            className="rounded-lg bg-gray-900 text-white px-4 py-2 text-sm hover:bg-gray-800"
          >
            OK
          </button>
        </div>
      </div>
    </div>
  );
}
