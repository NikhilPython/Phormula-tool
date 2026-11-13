"use client";

import React, { useCallback, useEffect, useState } from "react";
import { X, Info } from "lucide-react";
import Button from "@/components/ui/button/Button";
import AmazonStatCard from "@/components/dashboard/AmazonStatCard";
import { createPortal } from "react-dom";

export type GrowthIntent = "conservative" | "balanced" | "aggressive";
export type ProfitPriority = "high" | "protect_growth" | "sacrifice_short_term";

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
  initialGrowthIntent?: GrowthIntent;
  initialProfitPriority?: ProfitPriority;
  initialInventoryClearance?: boolean;
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

/* ------------------------------------------------------------------ */
/* Objective options: label + the caption shown under the selection    */
/* ------------------------------------------------------------------ */
type Option<T extends string> = { value: T; label: string; caption: string };

const GROWTH_OPTIONS: Option<GrowthIntent>[] = [
  { value: "conservative", label: "Conservative", caption: "Steady, low-risk growth." },
  { value: "balanced", label: "Balanced", caption: "Grow while staying stable." },
  { value: "aggressive", label: "Aggressive", caption: "Push hard for more sales." },
];

const PROFIT_OPTIONS: Option<ProfitPriority>[] = [
  { value: "high", label: "High", caption: "Keep margins strong." },
  { value: "protect_growth", label: "Protect growth", caption: "Margins flex to keep sales moving." },
  { value: "sacrifice_short_term", label: "Sacrifice short term", caption: "Trade profit now for faster growth." },
];

/* ------------------------------------------------------------------ */
/* Segmented control with a live caption                               */
/* ------------------------------------------------------------------ */
function SegmentedControl<T extends string>({
  label,
  options,
  value,
  onChange,
  disabled,
}: {
  label: string;
  options: Option<T>[];
  value: T;
  onChange: (v: T) => void;
  disabled?: boolean;
}) {
  const active = options.find((o) => o.value === value);

  return (
    <div>
      <p id={`seg-${label}`} className="text-sm font-semibold text-[#37455F]">
        {label}
      </p>

      <div
        role="radiogroup"
        aria-labelledby={`seg-${label}`}
        className="mt-2 grid grid-cols-3 gap-0.5 rounded-xl bg-slate-100 p-1"
      >
        {options.map((opt) => {
          const selected = opt.value === value;
          return (
            <button
              key={opt.value}
              type="button"
              role="radio"
              aria-checked={selected}
              disabled={disabled}
              onClick={() => onChange(opt.value)}
              className={`rounded-lg px-1.5 py-2 text-center text-xs font-semibold leading-tight transition focus:outline-none focus-visible:ring-2 focus-visible:ring-[#75BBDA] disabled:opacity-50 sm:text-[13px] ${selected
                ? "bg-green-500 text-yellow-200 shadow-sm"
                : "text-charcoal-500 hover:bg-white/70 hover:text-[#37455F]"
                }`}
            >
              {opt.label}
            </button>
          );
        })}
      </div>

      {/* <p className="mt-2 min-h-[16px] text-xs text-slate-500" aria-live="polite">
        {active?.caption}
      </p> */}
    </div>
  );
}

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

  const [growthIntent, setGrowthIntent] = useState<GrowthIntent>("balanced");
  const [profitPriority, setProfitPriority] = useState<ProfitPriority>("protect_growth");
  const [inventoryClearance, setInventoryClearance] = useState(false);

  const [currentStep, setCurrentStep] = useState<1 | 2>(1);

  useEffect(() => setMounted(true), []);

  useEffect(() => {
    if (!open || !data) return;
    const initial = Math.round(Number(data.currentTargetNative) || 0);
    setTargetDraft(initial > 0 ? String(initial) : "");
    setSaveError(null);
    setSaving(false);
    setCurrentStep(1);
    setGrowthIntent(data.initialGrowthIntent || "balanced");
    setProfitPriority(data.initialProfitPriority || "protect_growth");
    setInventoryClearance(data.initialInventoryClearance || false);
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

  const handleSave = useCallback(async (
    includeObjectives: boolean = false,
    forcedValue?: number
  ) => {
    if (!data) return;

    const nextTarget =
      forcedValue !== undefined ? forcedValue : Number(targetDraft);

    if (!Number.isFinite(nextTarget) || nextTarget <= 0) {
      setSaveError("Enter a valid target.");
      return;
    }

    const token =
      typeof window !== "undefined"
        ? localStorage.getItem("jwtToken")
        : null;

    if (!token) {
      setSaveError("Authorization token is missing.");
      return;
    }

    const baseURL = process.env.NEXT_PUBLIC_API_BASE_URL || "";

    try {
      setSaving(true);
      setSaveError(null);

      // 1. Save Monthly Target
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

      // 2. Save Objectives only when requested
      if (includeObjectives) {
        const monthIndex =
          new Date(`${data.monthName} 1, ${data.year}`).getMonth() + 1;

        const formattedMonth =
          `${data.year}-${String(monthIndex).padStart(2, "0")}`;

        const objectivePayload = {
          month: formattedMonth,
          country: data.country,
          growth_intent: growthIntent,
          profit_priority: profitPriority,
          inventory_clearance_priority: inventoryClearance,
        };

        const objectiveRes = await fetch(`${baseURL}/objective`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify(objectivePayload),
        });

        const objJson = await objectiveRes.json().catch(() => null);

        if (!objectiveRes.ok) {
          throw new Error(
            objJson?.error || "Failed to update business objectives"
          );
        }
      }

      onSaved?.();
      onClose();

    } catch (err: any) {
      console.error("Failed to save:", err);
      setSaveError(err?.message || "Failed to save. Try again.");
    } finally {
      setSaving(false);
    }
  }, [
    data,
    targetDraft,
    growthIntent,
    profitPriority,
    inventoryClearance,
    onSaved,
    onClose,
  ]);

  if (!mounted || !open || !data) return null;

  const canSave = !saving && draftNumber > 0;

  return createPortal(
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="midmonth-title"
      className="fixed inset-0 z-[2147483647] flex items-center justify-center bg-black/40 px-4 py-6"
      onClick={() => {
        if (!saving) onClose();
      }}
    >
      {/* Main Modal Container */}
      <div
        className="relative flex max-h-[92vh] w-full max-w-[560px] flex-col overflow-hidden rounded-2xl bg-white shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Scrollable Content */}
        <div className="overflow-y-auto p-6 sm:p-8">

          {/* Close Button */}
          <button
            type="button"
            onClick={() => !saving && onClose()}
            aria-label="Close"
            disabled={saving}
            className="absolute right-4 top-4 z-10 rounded-full p-2 text-slate-400 transition hover:bg-slate-100 hover:text-slate-600 disabled:opacity-40"
          >
            <X className="h-5 w-5" />
          </button>

          {/* Dynamic Heading */}
          <div className="mb-5 pr-8">
            <h2
              id="midmonth-title"
              className="text-2xl font-bold text-[#37455F]"
            >
              {currentStep === 1
                ? `Set a target for ${data.monthName}`
                : "Update Business Objectives"}
            </h2>

            <p className="mt-2 text-sm leading-5 text-slate-600">
              {currentStep === 1
                ? hasTarget
                  ? `Review and update your monthly net sales target for ${data.monthLabel}.`
                  : `You haven't set a monthly net sales target yet. Set one now, or we'll use ${prevMonthName}'s net sales as your ${data.monthName} target.`
                : `Define your business priorities for ${data.monthName}. You can change these preferences at any time.`}
            </p>
          </div>

          {/* ================================= */}
          {/* STEP 1: MONTHLY TARGET            */}
          {/* ================================= */}

          {currentStep === 1 && (
            <>
              {/* Performance Snapshot */}
              <div className="mb-5 grid grid-cols-2 gap-3 sm:gap-4">
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

              {/* Target Input */}
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
                  <span className="text-lg font-medium leading-none text-slate-400">
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
                      if (e.key === "Enter" && canSave) {
                        handleSave(false);
                      }
                    }}
                    placeholder="0"
                    disabled={saving}
                    aria-invalid={!!saveError}
                    className="w-full min-w-0 bg-transparent text-lg font-semibold leading-tight text-charcoal-500 outline-none placeholder:text-slate-300"
                  />
                </div>

                {/* Quick Fill */}
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

                {/* Progress */}
                <div className="mt-4">
                  <div className="h-1.5 w-full overflow-hidden rounded-full bg-slate-200">
                    <div
                      className="h-full rounded-full bg-green-500 transition-[width] duration-300"
                      style={{
                        width: `${progressPct === null
                          ? 0
                          : Math.min(progressPct, 100)
                          }%`,
                      }}
                    />
                  </div>

                  <p className="mt-2 text-xs text-slate-500">
                    {progressPct === null
                      ? "Enter a target to see your progress."
                      : (
                        <>
                          You're at{" "}
                          <span className="font-semibold text-[#37455F]">
                            {progressPct}%
                          </span>{" "}
                          of this target so far.
                        </>
                      )}
                  </p>
                </div>
              </div>
            </>
          )}

          {/* ================================= */}
          {/* STEP 2: BUSINESS OBJECTIVES       */}
          {/* ================================= */}

          {currentStep === 2 && (
            <div className="space-y-6">
              <SegmentedControl
                label="Growth intent"
                options={GROWTH_OPTIONS}
                value={growthIntent}
                onChange={setGrowthIntent}
                disabled={saving}
              />

              <SegmentedControl
                label="Profit priority"
                options={PROFIT_OPTIONS}
                value={profitPriority}
                onChange={setProfitPriority}
                disabled={saving}
              />

              {/* Inventory clearance */}
              <div className="flex items-center justify-between gap-4 border-t border-slate-200 pt-5">
                <div>
                  <p
                    id="inventory-clearance-label"
                    className="text-sm font-semibold text-[#37455F]"
                  >
                    Clear Ageing Inventory
                  </p>
                  {/* <p className="mt-0.5 text-xs text-slate-500" aria-live="polite">
                    {inventoryClearance
                      ? "Slow-moving stock gets priority in recommendations."
                      : "Off. No special focus on slow-moving stock."}
                  </p> */}
                </div>

                <button
                  type="button"
                  role="switch"
                  aria-checked={inventoryClearance}
                  aria-labelledby="inventory-clearance-label"
                  onClick={() => setInventoryClearance(!inventoryClearance)}
                  disabled={saving}
                  className={`relative inline-flex h-6 w-11 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none focus-visible:ring-2 focus-visible:ring-green-500 focus-visible:ring-offset-2 disabled:opacity-50 ${inventoryClearance ? "bg-green-500" : "bg-slate-200"
                    }`}
                >
                  <span
                    className={`pointer-events-none inline-block h-5 w-5 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out ${inventoryClearance ? "translate-x-5" : "translate-x-0"
                      }`}
                  />
                </button>
              </div>
            </div>
          )}

          {/* Dynamic Footer Information Note */}
          <div className="mt-5 flex items-center gap-2 rounded-lg bg-[#F8F9FA] px-4 py-3 text-xs text-slate-500">
            <Info className="h-4 w-4 shrink-0 text-slate-400" />

            {currentStep === 1
              ? "You can update your target at any time from Targets & Objectives."
              : "You can update your objectives at any time from Targets & Objectives."}
          </div>

          {/* Common Save Error - visible on both steps */}
          {saveError && (
            <p
              role="alert"
              className="mt-3 rounded-lg bg-red-50 px-3 py-2 text-xs font-medium text-red-600"
            >
              {saveError}
            </p>
          )}

        </div>

        {/* ================================= */}
        {/* FOOTER: STEP ACTIONS              */}
        {/* ================================= */}

        <div className="shrink-0 border-t border-slate-200 bg-white px-6 py-4 sm:px-8">

          {/* STEP 1 FOOTER */}
          {currentStep === 1 && (
            <div className="flex items-center justify-between gap-3">

              <button
                type="button"
                onClick={() => !saving && onClose()}
                disabled={saving}
                className="text-sm font-semibold text-[#5EA68E] hover:underline disabled:opacity-50"
              >
                Maybe later
              </button>

              <div className="flex items-center gap-2">
                <Button
                  type="button"
                  size="md"
                  onClick={() => handleSave(false)}
                  disabled={!canSave}
                  className="px-5 py-2.5 text-sm font-semibold text-white hover:bg-[#1f2943] disabled:opacity-60"
                >
                  {saving ? "Saving..." : "Done"}
                </Button>

                {/* <button
                  type="button"
                  onClick={() => {
                    setSaveError(null);
                    setCurrentStep(2);
                  }}
                  disabled={saving}
                  className="rounded-lg border border-slate-200 shadow-sm bg-white px-4 py-2.5 text-sm font-semibold text-charcoal-500 transition hover:bg-slate-50 disabled:opacity-50"
                >
                  Update Objectives
                </button> */}

                <Button
                  size="md"
                  variant="outline"
                  onClick={() => {
                    setSaveError(null);
                    setCurrentStep(2);
                  }}
                  className="text-sm font-semibold text-charcoal-500"
                >
                  Update Objectives
                </Button>
              </div>
            </div>
          )}

          {/* STEP 2 FOOTER */}
          {currentStep === 2 && (
            <div className="flex items-center justify-between gap-3">

              <Button
                size="md"
                variant="outline"
                onClick={() => {
                  setSaveError(null);
                  setCurrentStep(1);
                }}
                disabled={saving}
                className="px-4 py-2.5 text-sm font-semibold text-charcoal-500 transition hover:bg-slate-50 disabled:opacity-50"
              >
                Back
              </Button>

              <Button
                type="button"
                size="md"
                onClick={() => handleSave(true)}
                disabled={!canSave}
                className="px-6 py-2.5 text-sm font-semibold text-white hover:bg-[#1f2943] disabled:opacity-60"
              >
                {saving ? "Updating..." : "Update"}
              </Button>
            </div>
          )}
        </div>

      </div>
    </div>,
    document.body
  );
}
