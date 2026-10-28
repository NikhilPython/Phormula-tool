"use client";

import React, { useCallback, useEffect, useState } from "react";
import { Target, X, CalendarDays, CheckCircle2, Info } from "lucide-react";
import Button from "@/components/ui/button/Button";
import AmazonStatCard from "@/components/dashboard/AmazonStatCard";

export type MidMonthTargetReminderData = {
  country: string;
  monthName: string;
  year: number;
  monthLabel: string;
  homeCurrency: string;
  nativeCurrency: string;
  mtdNetSalesHome: number;
  lastMonthNetSalesHome: number;
  currentTargetNative: number;
};

interface Props {
  open: boolean;
  data: MidMonthTargetReminderData | null;
  onClose: () => void;
  onSaved?: () => void;
  onViewPerformance?: () => void;
}

const money = (amount: number, currency: string) =>
  new Intl.NumberFormat(undefined, {
    style: "currency",
    currency,
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(Number.isFinite(amount) ? amount : 0);

export default function MidMonthTargetReminderModal({
  open,
  data,
  onClose,
  onSaved,
}: Props) {
  const [mounted, setMounted] = useState(false);

  const [targetDraft, setTargetDraft] = useState("");
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  useEffect(() => setMounted(true), []);

  useEffect(() => {
    if (!open || !data) return;
    setTargetDraft(String(data.currentTargetNative || ""));
    setSaveError(null);
    setSaving(false);
  }, [open, data]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !saving) onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, saving, onClose]);

  const hasTarget = Number(data?.currentTargetNative ?? 0) > 0;

  // Format previous month name
  const prevMonthName = React.useMemo(() => {
    if (!data) return "";
    const d = new Date(data.year, new Date(`${data.monthName} 1`).getMonth(), 1);
    d.setMonth(d.getMonth() - 1);
    return d.toLocaleString("en-US", { month: "long" });
  }, [data]);

  const handleSave = useCallback(async (forcedValue?: number) => {
    if (!data) return;

    const nextTarget = forcedValue !== undefined ? forcedValue : Number(targetDraft);

    if (!Number.isFinite(nextTarget) || nextTarget < 0) {
      setSaveError("Please enter a valid target.");
      return;
    }

    const token =
      typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

    if (!token) {
      setSaveError("Authorization token is missing.");
      return;
    }

    const baseURL = process.env.NEXT_PUBLIC_API_BASE_URL || "";

    try {
      setSaving(true);
      setSaveError(null);

      const targetPayload = {
        month: data.monthName,
        year: data.year,
        country: data.country,
        target_sales: nextTarget,
      };

      const targetRes = await fetch(`${baseURL}/target-summary`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify(targetPayload),
      });

      const targetJson = await targetRes.json().catch(() => null);

      if (!targetRes.ok) {
        throw new Error(
          targetJson?.error || "Failed to save monthly target summary"
        );
      }

      onSaved?.();
      onClose();
    } catch (err: any) {
      console.error("Failed to save mid-month target:", err);
      setSaveError(err?.message || "Failed to save. Please try again.");
    } finally {
      setSaving(false);
    }
  }, [data, targetDraft, onSaved, onClose]);

  if (!mounted || !open || !data) return null;

  const mtdDisplay = money(data.mtdNetSalesHome, data.homeCurrency);
  const lastMonthDisplay = money(data.lastMonthNetSalesHome, data.homeCurrency);

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="midmonth-title"
      className="fixed inset-0 z-[100] flex items-center justify-center bg-slate-900/40 px-4 py-6 backdrop-blur-[2px]"
      onClick={() => {
        if (!saving) onClose();
      }}
    >
      <div
        className="relative w-full max-w-[560px] max-h-[92vh] overflow-y-auto rounded-2xl bg-white p-6 shadow-2xl sm:p-8"
        onClick={(e) => e.stopPropagation()}
      >
        <button
          type="button"
          onClick={() => !saving && onClose()}
          aria-label="Close"
          disabled={saving}
          className="absolute right-4 top-4 rounded-full p-2 text-slate-400 transition hover:bg-slate-100 hover:text-slate-600 disabled:opacity-40"
        >
          <X className="h-5 w-5" />
        </button>

        <div className="mb-5 flex flex-col gap-4 sm:flex-row sm:items-start">
          <div>
            <h2
              id="midmonth-title"
              className="text-2xl font-bold text-[#37455F]"
            >
              Set a target for {data.monthName}
            </h2>
            <p className="mt-2 text-sm leading-5 text-slate-600">
              {hasTarget
                ? `You currently have a target set. You can review and update it below for ${data.monthLabel}.`
                : `You haven't set a monthly net sales target yet. Set one now, or we'll use ${prevMonthName}'s net sales as your ${data.monthName} target.`}
            </p>
          </div>
        </div>

        {/* Performance Snapshot Row */}
        {/* <div className="mb-5 flex items-center justify-between rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm shadow-sm">
          <div className="flex items-center gap-3">
            <span className="text-slate-500">MTD Net Sales:</span>
            <span className="font-semibold text-[#37455F]">{mtdDisplay}</span>
          </div>
          <div className="h-4 w-px bg-slate-200" />
          <div className="flex items-center gap-3">
            <span className="text-slate-500">{prevMonthName} Total:</span>
            <span className="font-semibold text-[#37455F]">{lastMonthDisplay}</span>
          </div>
        </div> */}

        {/* Performance Snapshot Row (Replaced with Cards) */}
        <div className="mb-5 grid grid-cols-2 gap-4">
          <AmazonStatCard
            label="MTD Net Sales"
            current={data.mtdNetSalesHome}
            previous={null}
            deltaPct={null}
            bottomLabel=""
            hidePrevious={true} // 👈 This will now perfectly hide the bottom row
            loading={false}
            formatter={(val) => money(val, data.homeCurrency)}
            className="border-[#ED9F50] border-t-4 border-t-[#ED9F50]"
          />

          <AmazonStatCard
            label={`${prevMonthName} Net Sales`}
            current={data.lastMonthNetSalesHome}
            previous={null}
            deltaPct={null}
            bottomLabel=""
            hidePrevious={true} // 👈 Hides the bottom row for the previous metric
            loading={false}
            formatter={(val) => money(val, data.homeCurrency)}
            className="border-[#75BBDA] border-t-4 border-t-[#75BBDA]" // Example color for the 2nd card
          />
        </div>

        {/* Fallback Banner (Only if no target set yet) */}
        {!hasTarget && (
          <div className="mb-5 rounded-xl border border-[#D1E7E0] bg-[#F0F9F6] p-5">
            <div className="mb-4 text-sm font-semibold text-[#5EA68E]">
              Automatic fallback
            </div>
            <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <p className="mb-1 text-sm text-slate-600">
                  {prevMonthName} Total Net Sales
                </p>
                <p className="text-3xl font-bold text-[#37455F]">
                  {lastMonthDisplay}
                </p>
              </div>
              <div className="inline-flex items-center gap-2 rounded-full border border-[#D1E7E0] bg-white px-3 py-1.5 text-xs font-semibold text-[#5EA68E]">
                <CheckCircle2 className="h-4 w-4" />
                Will be used if skipped
              </div>
            </div>
          </div>
        )}

        {/* Input Section */}
        <div className="mb-5 rounded-xl border border-slate-200 bg-slate-50 p-5">
          <label className="mb-2 block text-sm font-semibold text-[#37455F]">
            {hasTarget ? "Update your target" : "Or set a custom target"} ({data.nativeCurrency})
          </label>
          <div className="relative">
            <span className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400">
              {data.nativeCurrency === "USD" ? "$" : data.nativeCurrency === "GBP" ? "£" : data.nativeCurrency === "CAD" ? "C$" : ""}
            </span>
            <input
              type="number"
              min={0}
              step={1}
              value={targetDraft}
              onChange={(e) => setTargetDraft(e.target.value)}
              placeholder="Enter target amount"
              disabled={saving}
              className="w-full rounded-md border border-slate-300 bg-white py-2.5 pl-8 pr-3 text-sm font-medium outline-none focus:border-[#37455F] disabled:opacity-60"
            />
          </div>

          {saveError && (
            <p className="mt-3 rounded-md bg-red-50 px-3 py-2 text-xs font-medium text-red-600">
              {saveError}
            </p>
          )}
        </div>

        {/* Footer info note */}
        <div className="mb-6 flex items-center gap-2 rounded-lg bg-[#F8F9FA] px-4 py-3 text-xs text-slate-500">
          <Info className="h-4 w-4 shrink-0 text-slate-400" />
          You can update your target at any time from Targets & Objectives.
        </div>

        {/* Action Buttons */}
        <div className="flex flex-col-reverse gap-3 sm:flex-row sm:items-center sm:justify-between">
          <button
            type="button"
            onClick={() => {
              if (!saving) onClose();
            }}
            disabled={saving}
            className="text-sm font-semibold text-[#5EA68E] hover:underline disabled:opacity-50"
          >
            Maybe later
          </button>

          <div className="flex flex-col gap-2 sm:flex-row">
            {!hasTarget && (
              <button
                type="button"
                onClick={() => handleSave(data.lastMonthNetSalesHome)}
                disabled={saving}
                className="rounded-md border border-slate-300 bg-white px-4 py-2.5 text-sm font-semibold text-[#37455F] transition hover:bg-slate-50 disabled:opacity-60"
              >
                Use {lastMonthDisplay}
              </button>
            )}

            <Button
              type="button"
              onClick={() => handleSave()}
              disabled={saving || !targetDraft || Number(targetDraft) <= 0}
              className="rounded-md bg-[#2E3A59] px-6 py-2.5 text-sm font-semibold text-white hover:bg-[#1f2943] disabled:opacity-60"
            >
              {saving ? "Saving…" : `Set ${data.monthName} target`}
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}