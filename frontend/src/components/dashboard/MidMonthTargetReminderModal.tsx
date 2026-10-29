"use client";

import React, { useCallback, useEffect, useState } from "react";
import { X, CheckCircle2, Info } from "lucide-react";
import Button from "@/components/ui/button/Button";
import AmazonStatCard from "@/components/dashboard/AmazonStatCard";
import { createPortal } from "react-dom";

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

// Quick-fill chips: percentage uplift applied on last month's net sales
const QUICK_FILL_UPLIFTS = [10, 20];

export default function MidMonthTargetReminderModal({
  open,
  data,
  onClose,
  onSaved,
}: Props) {
  const [mounted, setMounted] = useState(false);

  // Digits only (e.g. "9999"). Formatting (9,999) happens at render time.
  const [targetDraft, setTargetDraft] = useState("");
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  useEffect(() => setMounted(true), []);

  useEffect(() => {
    if (!open || !data) return;
    const initial = Math.round(Number(data.currentTargetNative) || 0);
    setTargetDraft(initial > 0 ? String(initial) : "");
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

  // Currency symbol derived from Intl so every currency works (not just USD/GBP/CAD)
  const currencySymbol = React.useMemo(() => {
    if (!data?.nativeCurrency) return "";
    try {
      return (
        new Intl.NumberFormat(undefined, {
          style: "currency",
          currency: data.nativeCurrency,
        })
          .formatToParts(0)
          .find((p) => p.type === "currency")?.value ?? data.nativeCurrency
      );
    } catch {
      return data.nativeCurrency;
    }
  }, [data?.nativeCurrency]);

  const draftNumber = Number(targetDraft) || 0;
  const displayValue = targetDraft ? draftNumber.toLocaleString() : "";

  const progressPct =
    data && draftNumber > 0
      ? Math.round((data.mtdNetSalesHome / draftNumber) * 100)
      : null;

  const handleDraftChange = (raw: string) => {
    const digits = raw.replace(/\D/g, "").slice(0, 12);
    setTargetDraft(digits);
    if (saveError) setSaveError(null);
  };

  const applyQuickFill = (uplift: number) => {
    if (!data) return;
    const next = Math.round(data.lastMonthNetSalesHome * (1 + uplift / 100));
    setTargetDraft(String(next));
    if (saveError) setSaveError(null);
  };

  const handleSave = useCallback(async (forcedValue?: number) => {
    if (!data) return;

    const nextTarget = forcedValue !== undefined ? forcedValue : Number(targetDraft);

    if (!Number.isFinite(nextTarget) || nextTarget < 0) {
      setSaveError("Enter a valid target.");
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
      setSaveError(err?.message || "Failed to save. Try again.");
    } finally {
      setSaving(false);
    }
  }, [data, targetDraft, onSaved, onClose]);

  if (!mounted || !open || !data) return null;

  const lastMonthDisplay = money(data.lastMonthNetSalesHome, data.homeCurrency);
  const canSave = !saving && draftNumber > 0;

  return createPortal(
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="midmonth-title"
      className="fixed top-0 right-0 bottom-0 left-0 z-[2147483647] flex items-center justify-center bg-black/40 px-4 py-6"
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
        <div className="mb-5 grid grid-cols-2 gap-4">
          <AmazonStatCard
            label="MTD Net Sales"
            current={data.mtdNetSalesHome}
            previous={null}
            deltaPct={null}
            bottomLabel=""
            hidePrevious={true}
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
            hidePrevious={true}
            loading={false}
            formatter={(val) => money(val, data.homeCurrency)}
            className="border-[#75BBDA] border-t-4 border-t-[#75BBDA]"
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

        {/* Target input: hero input + quick-fill chips + progress */}
        <div className="mb-5">
          <label
            htmlFor="midmonth-target-input"
            className="mb-2 block text-sm font-semibold capitalize text-slate-500"
          >
            {hasTarget
              ? `Your ${data.monthName} target`
              : `Custom ${data.monthName} target`}
          </label>

          <div
            className={`flex items-center gap-2 rounded-xl border bg-white px-4 py-3 transition ${saveError
              ? "border-red-300 focus-within:border-red-400"
              : "border-slate-300 focus-within:border-green-500"
              } ${saving ? "opacity-60" : ""}`}
          >
            <span
              className="text-lg font-medium leading-none text-slate-400"
              aria-hidden="true"
            >
              {currencySymbol}
            </span>
            <input
              id="midmonth-target-input"
              type="text"
              inputMode="numeric"
              autoComplete="off"
              value={displayValue}
              onChange={(e) => handleDraftChange(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && canSave) handleSave();
              }}
              placeholder="0"
              disabled={saving}
              aria-invalid={!!saveError}
              aria-describedby={saveError ? "midmonth-target-error" : undefined}
              className="w-full min-w-0 bg-transparent text-lg font-semibold leading-tight text-charcoal-500 outline-none placeholder:text-slate-300"
            />
          </div>

          {/* Quick-fill chips */}
          {data.lastMonthNetSalesHome > 0 && (
            <div className="mt-3 flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => applyQuickFill(0)}
                disabled={saving}
                className="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-semibold text-[#37455F] transition hover:border-[#2E3A59] hover:bg-slate-50 disabled:opacity-50"
              >
                Same as {prevMonthName}
              </button>
              {QUICK_FILL_UPLIFTS.map((pct) => (
                <button
                  key={pct}
                  type="button"
                  onClick={() => applyQuickFill(pct)}
                  disabled={saving}
                  className="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-semibold text-[#37455F] transition hover:border-[#2E3A59] hover:bg-slate-50 disabled:opacity-50"
                >
                  +{pct}%
                </button>
              ))}
            </div>
          )}

          {/* Progress / error */}
          {saveError ? (
            <p
              id="midmonth-target-error"
              role="alert"
              className="mt-3 text-xs font-medium text-red-600"
            >
              {saveError}
            </p>
          ) : (
            <div className="mt-4">
              <div className="h-1.5 w-full overflow-hidden rounded-full bg-slate-200">
                <div
                  className="h-full rounded-full bg-green-500 transition-[width] duration-300"
                  style={{
                    width: `${progressPct === null ? 0 : Math.min(progressPct, 100)}%`,
                  }}
                />
              </div>
              <p className="mt-2 text-xs text-slate-500">
                {progressPct === null ? (
                  "Enter a target to see your progress."
                ) : (
                  <>
                    You&apos;re at{" "}
                    <span className="font-semibold text-[#37455F]">
                      {progressPct}%
                    </span>{" "}
                    of this target so far.
                  </>
                )}
              </p>
            </div>
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
              disabled={!canSave}
              className="rounded-md bg-[#2E3A59] px-6 py-2.5 text-sm font-semibold text-white hover:bg-[#1f2943] disabled:opacity-60"
            >
              {saving ? "Saving…" : `Set ${data.monthName} target`}
            </Button>
          </div>
        </div>
      </div>
    </div>,
    document.body
  );
}