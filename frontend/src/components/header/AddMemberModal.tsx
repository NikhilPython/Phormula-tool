"use client";

import React, { useEffect, useMemo, useState } from "react";

const MODULE_OPTIONS = [
  "LIVE_DASHBOARD",
  "FINANCE_DASHBOARDS",
  "BUSINESS_INTELLIGENCE",
  "INVENTORY_PLANNING",
];

const ROLE_OPTIONS = ["MARKETING", "ACCOUNTED", "INVENTORY"] as const;
type RoleOption = (typeof ROLE_OPTIONS)[number];

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

function ChipsMultiSelect({
  options,
  value,
  onChange,
  placeholder = "Select the section you want to give access of",
}: {
  options: string[];
  value: string[];
  onChange: (next: string[]) => void;
  placeholder?: string;
}) {
  const [open, setOpen] = useState(false);

  const toggle = (opt: string) => {
    if (value.includes(opt)) onChange(value.filter((v) => v !== opt));
    else onChange([...value, opt]);
  };

  const remove = (opt: string) => onChange(value.filter((v) => v !== opt));

  return (
    <div className="relative">
      <div
        className="min-h-[40px] w-full rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark px-3 py-2 flex flex-wrap gap-2 items-center cursor-pointer"
        onClick={() => setOpen((p) => !p)}
      >
        {value.length === 0 ? (
          <span className="text-gray-400 text-sm">{placeholder}</span>
        ) : (
          value.map((v) => (
            <span
              key={v}
              className="inline-flex items-center gap-2 rounded-full bg-gray-100 dark:bg-white/5 px-3 py-1 text-xs text-gray-700 dark:text-gray-200"
              onClick={(e) => e.stopPropagation()}
            >
              {v}
              <button
                type="button"
                className="text-gray-500 hover:text-gray-800 dark:hover:text-white"
                onClick={() => remove(v)}
                aria-label={`Remove ${v}`}
              >
                ✕
              </button>
            </span>
          ))
        )}
      </div>

      {open && (
        <div
          className="absolute z-[999999] mt-2 w-full rounded-xl border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark shadow-theme-lg p-2"
          onClick={(e) => e.stopPropagation()}
        >
          <div className="max-h-52 overflow-auto">
            {options.map((opt) => {
              const checked = value.includes(opt);
              return (
                <button
                  type="button"
                  key={opt}
                  onClick={() => toggle(opt)}
                  className="w-full flex items-center justify-between px-3 py-2 rounded-lg hover:bg-gray-100 dark:hover:bg-white/5 text-left"
                >
                  <span className="text-sm text-gray-700 dark:text-gray-200">
                    {opt}
                  </span>
                  <span className="text-sm">{checked ? "✅" : ""}</span>
                </button>
              );
            })}
          </div>

          <div className="pt-2 flex justify-end">
            <button
              type="button"
              className="text-xs text-gray-500 hover:text-gray-800 dark:hover:text-gray-200"
              onClick={() => setOpen(false)}
            >
              Close
            </button>
          </div>
        </div>
      )}
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
  onSuccess?: () => void; // ✅ so ProfileClient refresh kar sake
}) {
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [country, setCountry] = useState<string>("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [modules, setModules] = useState<string[]>([]);
  const [role, setRole] = useState<RoleOption>("MARKETING");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");

  useEffect(() => {
    if (!isOpen) {
      setName("");
      setEmail("");
      setCountry("");
      setPassword("");
      setConfirmPassword("");
      setModules([]);
      setRole("MARKETING");
      setLoading(false);
      setError("");
    }
  }, [isOpen]);

  const marketplaces = useMemo(() => {
    return country ? COUNTRY_TO_MARKETPLACES[country] || [] : [];
  }, [country]);

  const canSubmit =
    name.trim() &&
    email.trim() &&
    password.length >= 6 &&
    password === confirmPassword &&
    marketplaces.length > 0 &&
    modules.length > 0 &&
    !loading;

  const handleSave = async () => {
    setError("");

    if (!name.trim()) return setError("Name is required");
    if (!email.trim()) return setError("Email is required");
    if (!country) return setError("Country is required");
    if (password.length < 6) return setError("Password must be at least 6 characters");
    if (password !== confirmPassword) return setError("Password and Confirm Password must match");
    if (modules.length === 0) return setError("Please select at least one Section Access");

    const payload = {
      member_name: name.trim(),
      email: email.trim().toLowerCase(),
      password,
      marketplaces,
      modules,
      // role,
    };

    try {
      setLoading(true);

      const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, "") || "";
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

      onClose();
      onSuccess?.(); // ✅ refresh list
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
        className="w-full max-w-[720px] rounded-2xl bg-white dark:bg-gray-dark shadow-theme-lg border border-gray-200 dark:border-gray-800"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="p-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Name */}
            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Name *
              </label>
              <input
                className="mt-1 w-full rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark px-3 py-2 text-sm"
                placeholder="John Doe"
                value={name}
                onChange={(e) => setName(e.target.value)}
              />
            </div>

            {/* Email */}
            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Email Address *
              </label>
              <input
                className="mt-1 w-full rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark px-3 py-2 text-sm"
                placeholder="member1@company.com"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
              />
            </div>

            {/* Country */}
            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Country *
              </label>
              <select
                className="mt-1 w-full rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark px-3 py-2 text-sm"
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

            {/* Password */}
            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Password *
              </label>
              <input
                type="password"
                className="mt-1 w-full rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark px-3 py-2 text-sm"
                placeholder="Min. 6 Characters"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
            </div>

            {/* Confirm Password */}
            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Confirm Password *
              </label>
              <input
                type="password"
                className="mt-1 w-full rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark px-3 py-2 text-sm"
                placeholder="Re-enter password"
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
              />
            </div>

            {/* Section Access */}
            <div className="md:col-span-2">
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Section Access *
              </label>
              <div className="mt-1">
                <ChipsMultiSelect
                  options={MODULE_OPTIONS}
                  value={modules}
                  onChange={setModules}
                />
              </div>

              <div className="mt-3 rounded-lg bg-yellow-50 dark:bg-white/5 border border-yellow-100 dark:border-gray-800 px-3 py-2 text-xs text-gray-700 dark:text-gray-200">
                ℹ️ Members can only view the sections you grant access to. You can update permissions anytime.
              </div>
            </div>

            {/* Role */}
            <div className="md:col-span-2">
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Role
              </label>
              <select
                className="mt-1 w-full rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark px-3 py-2 text-sm"
                value={role}
                onChange={(e) => setRole(e.target.value as RoleOption)}
              >
                {ROLE_OPTIONS.map((r) => (
                  <option key={r} value={r}>
                    {r}
                  </option>
                ))}
              </select>
            </div>
          </div>

          {error && <div className="mt-4 text-sm text-red-600">{error}</div>}

          <div className="mt-6 flex justify-end gap-3">
            <button
              type="button"
              onClick={onClose}
              className="rounded-lg px-4 py-2 text-sm border border-gray-200 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-white/5"
            >
              Cancel
            </button>

            <button
              type="button"
              onClick={handleSave}
              disabled={!canSubmit}
              className="rounded-lg px-4 py-2 text-sm bg-blue-600 text-white disabled:opacity-50 disabled:cursor-not-allowed hover:bg-blue-700"
            >
              {loading ? "Saving..." : "Save Info"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}