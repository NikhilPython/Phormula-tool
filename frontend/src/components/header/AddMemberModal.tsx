"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useGetUserDataQuery } from "@/lib/api/profileApi";
import { IoInformationCircleOutline } from "react-icons/io5";

const MODULE_OPTIONS = [
  "LIVE_DASHBOARD",
  "FINANCE_DASHBOARDS",
  "BUSINESS_INTELLIGENCE",
  "INVENTORY_PLANNING",
];

const ROLE_OPTIONS = [
  { label: "Marketing", value: "MARKETING" },
  { label: "Accountant", value: "ACCOUNTANT" },
  { label: "Inventory", value: "INVENTORY" },
] as const;

type RoleOption = (typeof ROLE_OPTIONS)[number]["value"];

const COUNTRY_OPTIONS = [
  { label: "United States", value: "US" },
  { label: "United Kingdom", value: "UK" },
  { label: "Canada", value: "CA" },
  { label: "Germany", value: "DE" },
];

const COUNTRY_TO_MARKETPLACES: Record<string, string[]> = {
  US: ["ATVPDKIKX0DER"],
  UK: ["A1F83G8C2ARO7P"],
  CA: ["A2EUQ1WTGCTBG2"],
  DE: ["A1PA6795UKMFR9"],
};

const formatLabel = (value: string) =>
  value
    .toLowerCase()
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());

function SectionAccessGrid({
  options,
  value,
  onChange,
}: {
  options: string[];
  value: string[];
  onChange: (next: string[]) => void;
}) {
  const toggle = (opt: string) => {
    if (value.includes(opt)) {
      onChange(value.filter((v) => v !== opt));
    } else {
      onChange([...value, opt]);
    }
  };

  const getModuleMeta = (opt: string) => {
    switch (opt) {
      case "LIVE_DASHBOARD":
        return {
          title: "Live Dashboard",
          subtitle: "MTD Sales, AI Insights, P&L Breakdown, Current Inventory",
        };
      case "FINANCE_DASHBOARDS":
        return {
          title: "Finance Dashboards",
          subtitle: "Financial Dashboard, P&L Breakdown, Cash Flow, SKU wise Profit, AI Insights",
        };
      case "BUSINESS_INTELLIGENCE":
        return {
          title: "Business Intelligence",
          subtitle: "AI Insights, Inventory Forecast, Dispatch Planing, Purchase Order , P&L Forecast",
        };
      case "INVENTORY_PLANNING":
        return {
          title: "Inventory Planning",
          subtitle: "Input Cost, Inventory Reconciliation, Expenses Reconciliation",
        };
      default:
        return {
          title: formatLabel(opt),
          subtitle: "",
        };
    }
  };

  return (
    <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
      {options.map((opt) => {
        const checked = value.includes(opt);
        const meta = getModuleMeta(opt);

        return (
          <button
            key={opt}
            type="button"
            onClick={() => toggle(opt)}
            className={`w-full rounded-2xl border px-4 py-4 text-left transition ${
              checked
                ? "border-[#86E0B8] bg-[#EAF7F1]"
                : "border-gray-200 bg-white hover:bg-gray-50"
            }`}
          >
            <div className="flex items-start gap-3">
              <div
                className={`mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-md border text-xs ${
                  checked
                    ? "border-[#10B981] bg-[#10B981] text-white"
                    : "border-gray-300 bg-white text-transparent"
                }`}
              >
                ✓
              </div>

              <div className="min-w-0">
                <div className="text-sm font-semibold text-gray-900">
                  {meta.title}
                </div>
                {meta.subtitle && (
                  <div className="mt-0.5 text-xs text-gray-500">
                    {meta.subtitle}
                  </div>
                )}
              </div>
            </div>
          </button>
        );
      })}
    </div>
  );
}

export default function AddMemberModal({
  isOpen,
  onClose,
  token,
  onSuccess,
}: {
  isOpen: boolean;
  onClose: () => void;
  token?: string;
  onSuccess?: () => void;
}) {
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [country, setCountry] = useState<string>("");
  const [modules, setModules] = useState<string[]>([]);
  const [role, setRole] = useState<RoleOption>("MARKETING");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");
  const [success, setSuccess] = useState<string>("");

  const { data: userData } = useGetUserDataQuery();
  const ownerEmail = (userData as any)?.owner_email?.toLowerCase?.() || "";

  useEffect(() => {
    if (!isOpen) {
      setName("");
      setEmail("");
      setCountry("");
      setModules([]);
      setRole("MARKETING");
      setLoading(false);
      setError("");
      setSuccess("");
    }
  }, [isOpen]);

  const marketplaces = useMemo(() => {
    return country ? COUNTRY_TO_MARKETPLACES[country] || [] : [];
  }, [country]);

  const isSelfAdd = ownerEmail && email.trim().toLowerCase() === ownerEmail;

  const canSubmit =
    !!name.trim() &&
    !!email.trim() &&
    marketplaces.length > 0 &&
    modules.length > 0 &&
    !loading &&
    !isSelfAdd;

  const handleSave = async () => {
    setSuccess("");
    setError("");

    if (!name.trim()) {
      setError("Name is required");
      return;
    }

    if (!email.trim()) {
      setError("Email is required");
      return;
    }

    if (ownerEmail && email.trim().toLowerCase() === ownerEmail) {
      setError("You cannot add yourself as a member.");
      return;
    }

    if (!country) {
      setError("Country is required");
      return;
    }

    if (modules.length === 0) {
      setError("Please select at least one Section Access");
      return;
    }

    const payload = {
      member_name: name.trim(),
      email: email.trim().toLowerCase(),
      marketplaces,
      modules,
      role,
    };

    try {
      setLoading(true);

      const baseUrl =
        process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, "") || "";

      const res = await fetch(`${baseUrl}/add_member`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify(payload),
      });

      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        setError(data?.error || data?.message || "Failed to add member");
        return;
      }

      setSuccess(
        "✅ Member added successfully. A temporary password has been generated and emailed."
      );

      onSuccess?.();

      setTimeout(() => {
        onClose();
      }, 1200);
    } catch (e: any) {
      setError(e?.message || "Something went wrong");
    } finally {
      setLoading(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div
      className="fixed inset-0 z-[999999] flex items-center justify-center bg-black/40 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-[720px] rounded-2xl border border-gray-200 bg-white shadow-theme-lg dark:border-gray-800 dark:bg-gray-dark"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="p-6">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Name <span className="text-red-500">*</span>
              </label>
              <input
                className="mt-1 w-full rounded-md border border-gray-200 bg-white px-3 py-2 text-sm dark:border-gray-800 dark:bg-gray-dark"
                placeholder="Member Name"
                value={name}
                onChange={(e) => setName(e.target.value)}
              />
            </div>

            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Email Address <span className="text-red-500">*</span>
              </label>
              <input
                className="mt-1 w-full rounded-md border border-gray-200 bg-white px-3 py-2 text-sm dark:border-gray-800 dark:bg-gray-dark"
                placeholder="member1@company.com"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
              />
              {ownerEmail && email.trim().toLowerCase() === ownerEmail && (
                <p className="mt-1 text-xs text-red-600">
                  You cannot add yourself as a member.
                </p>
              )}
            </div>

            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Country <span className="text-red-500">*</span>
              </label>
              <select
                className="mt-1 w-full rounded-md border border-gray-200 bg-white px-3 py-2 text-sm dark:border-gray-800 dark:bg-gray-dark"
                value={country}
                onChange={(e) => setCountry(e.target.value)}
              >
                <option value="">Select Country</option>
                {COUNTRY_OPTIONS.map((c) => (
                  <option key={c.value} value={c.value}>
                    {c.label}
                  </option>
                ))}
              </select>

              {country && (
                <p className="mt-1 text-xs text-gray-500">
                  Marketplaces: {marketplaces.join(", ")}
                </p>
              )}
            </div>

            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Role
              </label>
              <select
                className="mt-1 w-full rounded-md border border-gray-200 bg-white px-3 py-2 text-sm dark:border-gray-800 dark:bg-gray-dark"
                value={role}
                onChange={(e) => setRole(e.target.value as RoleOption)}
              >
                {ROLE_OPTIONS.map((r) => (
                  <option key={r.value} value={r.value}>
                    {r.label}
                  </option>
                ))}
              </select>
            </div>

            <div className="md:col-span-2">
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Section Access <span className="text-red-500">*</span>
              </label>

              <div className="mt-2">
                <SectionAccessGrid
                  options={MODULE_OPTIONS}
                  value={modules}
                  onChange={setModules}
                />
              </div>
            </div>
          </div>

          {success && (
            <div className="mt-4 rounded-lg border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">
              {success}
            </div>
          )}

          {error && <div className="mt-4 text-sm text-red-600">{error}</div>}

          <div className="mt-4 flex items-start gap-2 rounded-lg border border-yellow-100 bg-[#FDD36F4D] px-3 py-2 text-xs text-gray-700 dark:border-gray-800 dark:bg-white/5 dark:text-gray-200">
            <IoInformationCircleOutline className="text-charcoal-500 flex-shrink-0 text-base" />
            <span>
              Members can only view the sections you grant access to. A
              temporary password will be auto-generated and sent to the member's
              email.
            </span>
          </div>

          <div className="mt-6 flex justify-end gap-3">
            <button
              type="button"
              onClick={onClose}
              className="rounded-lg border border-gray-200 px-4 py-2 text-sm hover:bg-gray-50 dark:border-gray-800 dark:hover:bg-white/5"
            >
              Cancel
            </button>

            <button
              type="button"
              onClick={handleSave}
              disabled={!canSubmit}
              className="rounded-lg bg-blue-700 px-4 py-2 text-sm text-white hover:bg-blue-600 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {loading ? "Saving..." : "Save Info"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}