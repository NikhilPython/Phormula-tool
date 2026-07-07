"use client";
import React from "react";

const ALL_MODULES = [
  "LIVE_DASHBOARD",
  "FINANCE_DASHBOARDS",
  "BUSINESS_INTELLIGENCE",
  "INVENTORY_PLANNING",
];

// Optional: labels/subtitles to look like screenshot
const MODULE_META: Record<string, { title: string; subtitle: string }> = {
  LIVE_DASHBOARD: { title: "Dashboard", subtitle: "Profit, SKU & Cash Flow" },
  FINANCE_DASHBOARDS: { title: "Finance Dashboards", subtitle: "Profit, SKU & Cash Flow" },
  BUSINESS_INTELLIGENCE: { title: "Business Intelligence", subtitle: "Insights, Chatbot, Forecast" },
  INVENTORY_PLANNING: { title: "Inventory", subtitle: "Stock, Dispatches, PO" },
};

export default function ViewMemberDrawer({
  isOpen,
  onClose,
  member,
}: {
  isOpen: boolean;
  onClose: () => void;
  member: any;
}) {
  if (!isOpen || !member) return null;

  const name = String(member?.member_name || "—");
  const email = String(member?.email || "—");
  const country = Array.isArray(member?.countries) ? member.countries.join(", ") : (member?.country || "—");
  const createdAt = member?.created_at ? new Date(member.created_at) : null;

  const modules: string[] = Array.isArray(member?.modules) ? member.modules : [];

  const initials = name
    .split(" ")
    .filter(Boolean)
    .slice(0, 2)
    .map((x: string) => x[0]?.toUpperCase())
    .join("") || "U";

  return (
    <div className="fixed inset-0 z-[999999] flex">
      {/* Overlay */}
      <div className="flex-1 bg-black/30" onClick={onClose} />

      {/* Drawer */}
      <div className="w-[400px] bg-white dark:bg-gray-dark shadow-xl overflow-y-auto border-l border-gray-200 dark:border-gray-800">
        {/* Header */}
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
            >
              ✕
            </button>
          </div>
        </div>

        {/* Body */}
        <div className="p-5 space-y-6">
          {/* Account Info */}
          <SectionTitle title="Account Info" />

          <div className="space-y-3">
            <Row label="Email" value={email} />
            <Row label="Country" value={country || "—"} />
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

            <Row label="Added By" value={member?.added_by || "Raghav Sood"} />
          </div>

          {/* Section Access */}
          <SectionTitle title="Section Access" />

          <div className="space-y-3">
            {ALL_MODULES.map((mod) => {
              const checked = modules.includes(mod);
              const meta = MODULE_META[mod] || { title: mod.replaceAll("_", " "), subtitle: "" };

              return (
                <div
                  key={mod}
                  className={[
                    "rounded-xl border px-4 py-3 flex items-start gap-3",
                    checked
                      ? "border-emerald-200 bg-emerald-50 dark:bg-emerald-500/10 dark:border-emerald-500/30"
                      : "border-gray-200 bg-white dark:bg-gray-dark dark:border-gray-800",
                  ].join(" ")}
                >
                  <div className="pt-1">
                    <div
                      className={[
                        "w-5 h-5 rounded-md border flex items-center justify-center text-xs",
                        checked
                          ? "border-emerald-500 bg-emerald-500 text-white"
                          : "border-gray-300 dark:border-gray-700 bg-white dark:bg-transparent",
                      ].join(" ")}
                      aria-label={checked ? "Has access" : "No access"}
                    >
                      {checked ? "✓" : ""}
                    </div>
                  </div>

                  <div className="min-w-0">
                    <div className="text-sm font-semibold text-slate-900 dark:text-white">
                      {meta.title}
                    </div>
                    {meta.subtitle ? (
                      <div className="text-xs text-gray-600 dark:text-gray-300">
                        {meta.subtitle}
                      </div>
                    ) : null}
                  </div>
                </div>
              );
            })}
          </div>

          {/* Optional: Bottom spacing */}
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
      <span className="text-slate-900 dark:text-white text-right truncate max-w-[220px]">
        {value || "—"}
      </span>
    </div>
  );
}