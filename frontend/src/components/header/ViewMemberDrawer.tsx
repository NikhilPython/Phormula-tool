"use client";

import React from "react";

const ALL_MODULES = [
  "LIVE_DASHBOARD",
  "FINANCE_DASHBOARDS",
  "BUSINESS_INTELLIGENCE",
  "INVENTORY_PLANNING",
];

const MODULE_META: Record<string, { title: string; }> = {
  LIVE_DASHBOARD: {
    title: "Live Dashboard",
  },
  FINANCE_DASHBOARDS: {
    title: "Finance Dashboards",
  },
  BUSINESS_INTELLIGENCE: {
    title: "Business Intelligence",
  },
  INVENTORY_PLANNING: {
    title: "Inventory Planning",
  },
};

const COUNTRY_LABELS: Record<string, string> = {
  US: "United States",
  UK: "United Kingdom",
  CA: "Canada",
  DE: "Germany",
};

const MODULE_COLORS: Record<string, string> = {
  LIVE_DASHBOARD:
    "border-green-500 bg-green-50 text-green-600 dark:bg-green-500/10 dark:text-green-400",
  FINANCE_DASHBOARDS:
    "border-[#B75A5A] bg-[#B75A5A26] text-[#B75A5A]",
  INVENTORY_PLANNING:
    "border-[#3A8EA4] bg-[#3A8EA426] text-[#3A8EA4]",
  BUSINESS_INTELLIGENCE:
    "border-[#ED9F50] bg-[#ED9F5026] text-[#ED9F50]",
};

const formatLabel = (value: string) =>
  value
    .toLowerCase()
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());

const normalizeCountryAccess = (member: any): Record<string, string[]> => {
  const rawAccess = member?.country_access;

  if (rawAccess && typeof rawAccess === "object" && !Array.isArray(rawAccess)) {
    return Object.fromEntries(
      Object.entries(rawAccess)
        .map(([country, modules]) => [
          String(country).toUpperCase(),
          Array.isArray(modules) ? modules.map(String) : [],
        ])
        .filter(([, modules]) => modules.length > 0)
    );
  }

  const countries = Array.isArray(member?.countries)
    ? member.countries.map((country: any) => String(country).toUpperCase())
    : [];

  const modules = Array.isArray(member?.modules)
    ? member.modules.map(String)
    : [];

  if (countries.length === 0 || modules.length === 0) {
    return {};
  }

  return Object.fromEntries(
    countries.map((country: string) => [country, modules])
  );
};

export default function ViewMemberDrawer({
  isOpen,
  onClose,
  member,
  addedBy,
}: {
  isOpen: boolean;
  onClose: () => void;
  member: any;
  addedBy?: string;
}) {
  if (!isOpen || !member) return null;

  const name = String(member?.member_name || "—");
  const email = String(member?.email || "—");
  const role = String(member?.role || member?.member_role || "—");
  const createdAt = member?.created_at ? new Date(member.created_at) : null;

  const countryAccess = normalizeCountryAccess(member);
  const countries = Object.keys(countryAccess);

  const flatModules = Array.from(
    new Set(Object.values(countryAccess).flat())
  );

  const countryDisplay =
    countries.length > 0
      ? countries
        .map((country) => COUNTRY_LABELS[country] || country)
        .join(", ")
      : Array.isArray(member?.countries)
        ? member.countries.join(", ")
        : member?.country || "—";

  const initials =
    name
      .split(" ")
      .filter(Boolean)
      .slice(0, 2)
      .map((x: string) => x[0]?.toUpperCase())
      .join("") || "U";

  return (
    <div className="fixed inset-0 z-[999999] flex">
      <div className="flex-1 bg-black/30" onClick={onClose} />

      <div className="w-[440px] bg-white dark:bg-gray-dark shadow-xl overflow-y-auto border-l border-gray-200 dark:border-gray-800">
        <div className="p-5 border-b border-gray-200 dark:border-gray-800">
          <div className="flex items-start justify-between gap-3">
            <div className="flex items-center gap-3 min-w-0">
              <div className="w-9 h-9 rounded-full bg-slate-100 dark:bg-white/10 flex items-center justify-center text-xs font-semibold text-slate-700 dark:text-gray-100">
                {initials}
              </div>

              <div className="min-w-0">
                <div className="font-semibold text-slate-900 dark:text-white truncate">
                  {name}
                </div>
                <div className="text-xs text-gray-500 truncate">{email}</div>
              </div>
            </div>

            <button
              onClick={onClose}
              className="w-8 h-8 rounded-md border border-gray-200 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-white/5 flex items-center justify-center text-sm"
              aria-label="Close drawer"
              type="button"
            >
              ✕
            </button>
          </div>
        </div>

        <div className="p-5 space-y-6">
          <SectionTitle title="Account Info" />

          <div className="space-y-3">
            <Row label="Email" value={email} />
            <Row label="Country" value={countryDisplay || "—"} />
            <Row label="Role" value={formatLabel(role)} />

            <Row
              label="Added On"
              value={
                createdAt
                  ? createdAt.toLocaleDateString("en-GB", {
                    day: "2-digit",
                    month: "short",
                    year: "numeric",
                  })
                  : "—"
              }
            />

            <div className="flex items-center justify-between text-sm">
              <span className="text-gray-600 dark:text-gray-300">Status</span>
              <span className="inline-flex items-center gap-2 text-xs font-medium text-emerald-700 dark:text-emerald-400">
                <span className="w-2 h-2 rounded-full bg-emerald-500" />
                Active
              </span>
            </div>

            <Row label="Added By" value={addedBy || "—"} />
          </div>

          <SectionTitle title="Country & Section Access" />

          {countries.length === 0 ? (
            <div className="rounded-lg border border-gray-200 bg-gray-50 px-3 py-3 text-sm text-gray-600 dark:border-gray-800 dark:bg-white/5 dark:text-gray-300">
              No section access assigned.
            </div>
          ) : (
            <div className="space-y-4">
              {countries.map((country) => {
                const modules = countryAccess[country] || [];

                return (
                  <div
                    key={country}
                    className="rounded-xl border border-gray-200 bg-white p-4 dark:border-gray-800 dark:bg-gray-dark"
                  >
                    <div className="mb-3 flex items-center justify-between gap-3">
                      <div>
                        <div className="text-sm font-semibold text-slate-900 dark:text-white">
                          {COUNTRY_LABELS[country] || country}
                        </div>
                        {/* <div className="text-xs text-gray-500">{country}</div> */}
                      </div>

                      <span className="rounded-full border border-gray-200 bg-gray-50 px-2 py-1 text-[11px] text-gray-600 dark:border-gray-800 dark:bg-white/5 dark:text-gray-300">
                        {modules.length} section{modules.length === 1 ? "" : "s"}
                      </span>
                    </div>

                    <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                      {modules.map((mod) => {
                        const meta = MODULE_META[mod] || {
                          title: formatLabel(mod),
                        };

                        return (
                          <div
                            key={`${country}-${mod}`}
                            className={`rounded-xl border px-3 py-3 ${MODULE_COLORS[mod] ||
                              "border-gray-200 bg-gray-50 text-gray-700 dark:border-gray-800 dark:bg-white/5 dark:text-gray-300"
                              }`}
                          >
                            <div className="flex items-start gap-3">
                              {/* <div className="mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-md border border-current text-xs">
                                ✓
                              </div> */}

                              <div className="min-w-0">
                                <div className="text-sm font-semibold">
                                  {meta.title}
                                </div>

                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              })}
            </div>
          )}

          {/* <SectionTitle title="All Granted Sections" />

          <div className="flex flex-wrap gap-2">
            {flatModules.length > 0 ? (
              flatModules.map((mod) => (
                <span
                  key={mod}
                  className={`rounded-full border px-2 py-1 text-[11px] ${
                    MODULE_COLORS[mod] ||
                    "border-gray-200 bg-gray-50 text-gray-600 dark:border-gray-800 dark:bg-white/5 dark:text-gray-300"
                  }`}
                >
                  {formatLabel(mod)}
                </span>
              ))
            ) : (
              <span className="text-sm text-gray-500">—</span>
            )}
          </div> */}

          <div className="h-2" />
        </div>
      </div>
    </div>
  );
}

function SectionTitle({ title }: { title: string }) {
  return (
    <div>
      <div className="text-xs font-semibold text-gray-700 dark:text-gray-200">
        {title}
      </div>
      <div className="mt-2 h-px bg-gray-200 dark:bg-gray-800" />
    </div>
  );
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-4 text-sm">
      <span className="text-gray-600 dark:text-gray-300">{label}</span>
      <span className="text-slate-900 dark:text-white text-right truncate max-w-[230px]">
        {value || "—"}
      </span>
    </div>
  );
}