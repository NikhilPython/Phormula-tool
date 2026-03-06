"use client";

import React, { useState } from "react";
import { useRouter } from "next/navigation";
import { Trash2, AlertTriangle } from "lucide-react";

type ApiResponse = {
  message?: string;
};

export default function DeleteAdminPage() {
  const [email, setEmail] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(false);
  const [message, setMessage] = useState<string>("");

  const router = useRouter();

  const handleDelete = async () => {
    if (!email.trim()) {
      setMessage("Please enter an email");
      return;
    }

    setLoading(true);

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/delete_admin`, {
        method: "DELETE",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${localStorage.getItem("admin_token") ?? ""}`,
        },
        body: JSON.stringify({ email: email.trim() }),
      });

      const data = (await response.json()) as ApiResponse;

      if (response.ok) {
        setMessage("Admin deleted successfully");
        setEmail("");
        router.push("/superadmin/SuperAdminDashboard");
      } else {
        setMessage(data.message || "Failed to delete admin");
      }
    } catch (error) {
      setMessage("Network error. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  const isSuccess = message.toLowerCase().includes("success");

  return (
    <div className="min-h-screen flex items-center justify-center bg-[#5EA68E] p-4 sm:p-6">
      <div className="w-full max-w-md rounded-2xl bg-white shadow-2xl p-6 sm:p-8 text-center">
        {/* Header */}
        <div className="flex flex-col items-center mb-6 sm:mb-8">
          <div className="mb-4 flex h-20 w-20 items-center justify-center rounded-full bg-gradient-to-br from-[#5EA68E] to-[#1f5274] shadow-lg">
            <Trash2 size={40} color="white" />
          </div>
          <h1 className="text-2xl font-bold text-slate-800">Delete Admin</h1>
          <p className="text-slate-500 mt-1 text-sm sm:text-base">
            Remove an admin from the system. This action cannot be undone.
          </p>
        </div>

        {/* Message banner */}
        {message && (
          <div
            className={`mb-6 rounded-lg border px-4 py-3 text-sm text-left ${
              isSuccess
                ? "border-emerald-200 bg-emerald-50 text-emerald-800"
                : "border-red-200 bg-red-50 text-red-700"
            }`}
          >
            {message}
          </div>
        )}

        {/* Form */}
        <div className="mb-6 text-left">
          <label className="block text-sm font-semibold text-slate-700 mb-2">
            Admin Email Address
          </label>
          <input
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            placeholder="Enter admin email to delete"
            disabled={loading}
            className="w-full rounded-lg border border-slate-300 bg-white px-3 py-3 text-base outline-none transition placeholder:text-slate-400 disabled:opacity-60
                       focus:border-[#5EA68E] focus:ring-4 focus:ring-[#5EA68E]/20"
          />
        </div>

        <button
          onClick={handleDelete}
          disabled={loading || !email.trim()}
          className={`w-full inline-flex items-center justify-center gap-2 rounded-lg px-4 py-3 text-base font-semibold text-white transition
            ${
              loading || !email.trim()
                ? "cursor-not-allowed opacity-60 bg-red-400"
                : "bg-red-600 hover:bg-red-700 shadow-lg"
            }`}
        >
          {loading ? "Deleting…" : "Delete Admin"}
        </button>

        {/* Warning box */}
        <div className="mt-6 rounded-lg border border-amber-200 bg-amber-50 p-4 flex items-start gap-3 text-left">
          <AlertTriangle className="text-amber-600 flex-shrink-0" size={22} />
          <div>
            <h3 className="text-sm font-semibold text-amber-800">Warning</h3>
            <p className="text-xs sm:text-sm text-amber-800 mt-1">
              This action will permanently remove the admin from the system.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}