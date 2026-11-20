"use client";

import React, { useCallback, useEffect, useRef, useState } from "react";
import {
  X,
  Info,
  UploadCloud,
  FileSpreadsheet,
  CheckCircle2,
  XCircle,
  Loader2,
} from "lucide-react";
import Button from "@/components/ui/button/Button";
import AmazonStatCard from "@/components/dashboard/AmazonStatCard";
import { getMonthCompletedPctTillYesterday } from "@/lib/dashboard/date";
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
  lastMonthToDateHome?: number;
  currentTargetHome?: number;
  monthCompletedPct?: number;
  completedAt?: number | string | Date | null;
  completedTimeZone?: string;
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

const QUICK_FILL_UPLIFTS = [10, 20];

const WAREHOUSE_MAX_SIZE_MB = 20;

const humanFileSize = (bytes: number) => {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
};

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
  const gaugeWrapRef = useRef<HTMLDivElement | null>(null);
  const [gaugeTooltipPosition, setGaugeTooltipPosition] = useState<{
    x: number;
    y: number;
  } | null>(null);

  // Target digits only
  const [targetDraft, setTargetDraft] = useState("");
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  const [growthIntent, setGrowthIntent] = useState<GrowthIntent>("balanced");
  const [profitPriority, setProfitPriority] = useState<ProfitPriority>("protect_growth");
  const [inventoryClearance, setInventoryClearance] = useState(false);

  const [currentStep, setCurrentStep] = useState<1 | 2 | 3>(1);

  // ---- Warehouse stock (step 3) state ----
  const [warehouseFile, setWarehouseFile] = useState<File | null>(null);
  const [warehouseUploading, setWarehouseUploading] = useState(false);
  const [warehouseError, setWarehouseError] = useState<string | null>(null);
  const [warehouseSuccess, setWarehouseSuccess] = useState<string | null>(null);
  const [warehouseDragOver, setWarehouseDragOver] = useState(false);
  const warehouseInputRef = useRef<HTMLInputElement | null>(null);

  // Prevent state reset when parent re-fetches data via onSaved()
  const initializedRef = useRef(false);

  useEffect(() => setMounted(true), []);

  useEffect(() => {
    if (!open) {
      initializedRef.current = false;
      return;
    }
    if (!data) return;

    // Only initialize state when the modal first opens
    if (!initializedRef.current) {
      const savedTarget = Number(data.currentTargetNative);
      const initial = Math.round(
        Number.isFinite(savedTarget) && savedTarget > 0
          ? savedTarget
          : Number(data.lastMonthNetSalesHome) || 0,
      );
      setTargetDraft(initial > 0 ? String(initial) : "");
      setSaveError(null);
      setSaving(false);
      setCurrentStep(1);
      setGrowthIntent(data.initialGrowthIntent || "balanced");
      setProfitPriority(data.initialProfitPriority || "protect_growth");
      setInventoryClearance(data.initialInventoryClearance || false);

      // Reset warehouse step state
      setWarehouseFile(null);
      setWarehouseUploading(false);
      setWarehouseError(null);
      setWarehouseSuccess(null);
      setWarehouseDragOver(false);

      initializedRef.current = true;
    }
  }, [open, data]);

  const isBusy = saving || warehouseUploading;

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !isBusy) onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, isBusy, onClose]);

  const hasTarget = Number(data?.currentTargetNative ?? 0) > 0;

  const prevMonthName = React.useMemo(() => {
    if (!data) return "";
    const d = new Date(data.year, new Date(`${data.monthName} 1`).getMonth(), 1);
    d.setMonth(d.getMonth() - 1);
    return d.toLocaleString("en-US", { month: "long" });
  }, [data]);

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
      ? (data.mtdNetSalesHome / (data.currentTargetHome && data.currentTargetNative > 0
        ? data.currentTargetHome * (draftNumber / data.currentTargetNative)
        : draftNumber)) * 100
      : null;

  const monthCompletedPct =
    data?.completedAt && data.completedTimeZone
      ? getMonthCompletedPctTillYesterday(data.completedAt, data.completedTimeZone)
      : data && Number.isFinite(Number(data.monthCompletedPct))
        ? Number(data.monthCompletedPct)
        : 0;

  const gaugeSize = 220;
  const gaugeCenterX = gaugeSize / 2;
  const gaugeCenterY = 110;
  const gaugeArc = (radius: number, pct: number) => {
    const clamped = Math.min(Math.max(pct, 0), 100);
    const endAngle = (180 - clamped * 1.8) * (Math.PI / 180);
    const startX = gaugeCenterX - radius;
    const startY = gaugeCenterY;
    const endX = gaugeCenterX + radius * Math.cos(endAngle);
    const endY = gaugeCenterY - radius * Math.sin(endAngle);
    return `M ${startX} ${startY} A ${radius} ${radius} 0 0 1 ${endX} ${endY}`;
  };
  const gaugePoint = (radius: number, pct: number) => {
    const clamped = Math.min(Math.max(pct, 0), 100);
    const angle = (180 - clamped * 1.8) * (Math.PI / 180);
    return {
      x: gaugeCenterX + radius * Math.cos(angle),
      y: gaugeCenterY - radius * Math.sin(angle),
    };
  };

  const gaugeTargetHome =
    data && draftNumber > 0
      ? data.currentTargetHome && data.currentTargetNative > 0
        ? data.currentTargetHome * (draftNumber / data.currentTargetNative)
        : draftNumber
      : data?.currentTargetHome || data?.lastMonthNetSalesHome || 0;
  const gaugeMax = Math.max(
    data?.mtdNetSalesHome || 0,
    gaugeTargetHome,
    data?.lastMonthNetSalesHome || 0,
    data?.lastMonthToDateHome || 0,
    1,
  );
  const targetGaugePct = (gaugeTargetHome / gaugeMax) * 100;
  const mtdGaugePct = ((data?.mtdNetSalesHome || 0) / gaugeMax) * 100;
  const lastMonthGaugePct = ((data?.lastMonthNetSalesHome || 0) / gaugeMax) * 100;
  const lastMonthToDateGaugePct = ((data?.lastMonthToDateHome || 0) / gaugeMax) * 100;
  const targetGaugeRadius = 98.5;
  const targetGaugeEnd = gaugePoint(targetGaugeRadius, targetGaugePct);
  const mtdGaugeEnd = gaugePoint(92, mtdGaugePct);
  const lastMonthGaugeEnd = gaugePoint(78, lastMonthGaugePct);
  const lastMonthToDateMarker = gaugePoint(78, lastMonthToDateGaugePct);
  const gaugeTooltipLines = data
    ? [
      {
        label: "MTD Sale",
        value: `${money(data.mtdNetSalesHome, data.homeCurrency)} (${progressPct?.toFixed(2) ?? "0.00"}%)`,
        color: "#ED9F50",
      },
      {
        label: `${data.monthName} Target`,
        value: money(gaugeTargetHome, data.homeCurrency),
        color: "#5EA68E",
      },
      {
        label: `${prevMonthName} Sale`,
        value: money(data.lastMonthNetSalesHome, data.homeCurrency),
        color: "#9CA3AF",
      },
      {
        label: "Last month by today",
        value: money(data.lastMonthToDateHome || 0, data.homeCurrency),
        color: "#B75A5A",
      },
    ]
    : [];

  const handleGaugeMouseMove = (event: React.MouseEvent<SVGSVGElement>) => {
    const rect = gaugeWrapRef.current?.getBoundingClientRect();
    if (!rect) return;
    setGaugeTooltipPosition({
      x: event.clientX - rect.left,
      y: event.clientY - rect.top,
    });
  };

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


  const handleSave = useCallback(
    async (includeObjectives: boolean = false): Promise<boolean> => {
      if (!data) return false;

      const nextTarget = Number(targetDraft);

      if (!Number.isFinite(nextTarget) || nextTarget <= 0) {
        setSaveError("Enter a valid target.");
        return false;
      }

      const token =
        typeof window !== "undefined"
          ? localStorage.getItem("jwtToken")
          : null;

      if (!token) {
        setSaveError("Authorization token is missing.");
        return false;
      }

      const baseURL = process.env.NEXT_PUBLIC_API_BASE_URL || "";

      try {
        setSaving(true);
        setSaveError(null);

        // 1. Save monthly target
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

        // 2. Save business objectives
        if (includeObjectives) {
          const monthIndex =
            new Date(`${data.monthName} 1, ${data.year}`).getMonth() + 1;

          const formattedMonth = `${data.year}-${String(monthIndex).padStart(
            2,
            "0"
          )}`;

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

        // Final save is handled from Step 3.
        return true;

      } catch (err: any) {
        console.error("Failed to save:", err);
        setSaveError(err?.message || "Failed to save. Try again.");
        return false;

      } finally {
        setSaving(false);
      }
    },
    [
      data,
      targetDraft,
      growthIntent,
      profitPriority,
      inventoryClearance,
      onSaved,
    ]
  );


  const validateWarehouseFile = (f: File): string | null => {
    const lower = f.name.toLowerCase();
    if (!/\.(xlsx|xls|csv)$/.test(lower)) {
      return "Please upload an Excel (.xlsx, .xls) or CSV file.";
    }
    if (f.size > WAREHOUSE_MAX_SIZE_MB * 1024 * 1024) {
      return `File is too large. Max size is ${WAREHOUSE_MAX_SIZE_MB} MB.`;
    }
    return null;
  };

  const handleWarehouseFile = (f: File | null) => {
    setWarehouseError(null);
    setWarehouseSuccess(null);
    if (!f) return;
    const err = validateWarehouseFile(f);
    if (err) {
      setWarehouseError(err);
      return;
    }
    setWarehouseFile(f);
  };

  const handleWarehouseDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setWarehouseDragOver(false);
    const f = e.dataTransfer.files?.[0];
    if (f) handleWarehouseFile(f);
  };

  const handleWarehouseUpload = async () => {
    if (!warehouseFile || !data) return;

    const token =
      typeof window !== "undefined"
        ? localStorage.getItem("jwtToken")
        : null;

    if (!token) {
      setWarehouseError("Authorization token is missing.");
      return;
    }

    const baseURL = process.env.NEXT_PUBLIC_API_BASE_URL || "";

    try {
      setWarehouseUploading(true);
      setWarehouseError(null);
      setWarehouseSuccess(null);

      const form = new FormData();
      form.append("file", warehouseFile);
      form.append("country", data.country);
      form.append("month", data.monthName);
      form.append("year", String(data.year));

      const res = await fetch(`${baseURL}/warehouse/stock/upload`, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}` },
        body: form,
      });

      const json = await res.json().catch(() => ({}));

      if (!res.ok) {
        throw new Error(
          json?.message || json?.error || `Upload failed (${res.status})`
        );
      }

      setWarehouseSuccess(
        json?.message ||
        (json?.rows_imported != null
          ? `Uploaded ${json.rows_imported} rows.`
          : "Warehouse stock updated successfully.")
      );

      // Brief pause so the success tick is visible, then close.
      setTimeout(() => {
        onSaved?.();
        onClose();
      }, 1000);
    } catch (err: any) {
      setWarehouseError(err?.message || "Upload failed. Try again.");
    } finally {
      setWarehouseUploading(false);
    }
  };


  const handleFinishSetup = async () => {
    // Save monthly target and business objectives first.
    const saved = await handleSave(true);

    if (!saved) return;

    // Warehouse upload is optional.
    if (warehouseFile) {
      await handleWarehouseUpload();
    } else {
      // No warehouse file selected: finish successfully.
      onClose();
    }
  };


  const handleWarehouseSkip = () => {
    onClose();
  };

  if (!mounted || !open || !data) return null;

  // const canSave = !saving && draftNumber > 0;
  // const canUpload = !!warehouseFile && !warehouseUploading && !warehouseSuccess;

  const headingText =
    currentStep === 1
      ? `Set a target for ${data.monthName}`
      : currentStep === 2
        ? "Update Business Objectives"
        : "Update Local Warehouse Stock";

  const subtitleText =
    currentStep === 1
      ? hasTarget
        ? `Review and update your monthly net sales target for ${data.monthLabel}.`
        : `You haven't set a monthly net sales target yet. Set one now, or we'll use ${prevMonthName}'s net sales as your ${data.monthName} target.`
      : currentStep === 2
        ? `Define your business priorities for ${data.monthName}. You can change these preferences at any time.`
        : `Upload your ${data.monthName} ${data.year} local warehouse stock sheet. This is optional — you can also upload it later from the Inventory section.`;

  const infoText =
    currentStep === 1
      ? "You can update your target at any time from Targets & Objectives."
      : currentStep === 2
        ? "You can update your objectives at any time from Targets & Objectives."
        : "You can update warehouse stock at any time from the Inventory section.";

  return createPortal(
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="midmonth-title"
      className="fixed inset-0 z-[2147483647] flex items-center justify-center bg-black/40 px-4 py-6"
      onClick={() => {
        if (!isBusy) onClose();
      }}
    >
      <div
        className="relative flex max-h-[92vh] w-full max-w-[560px] flex-col overflow-hidden rounded-2xl bg-white shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="overflow-y-auto p-6 sm:p-8">
          <button
            type="button"
            onClick={() => !isBusy && onClose()}
            aria-label="Close"
            disabled={isBusy}
            className="absolute right-4 top-4 z-10 rounded-full p-2 text-slate-400 transition hover:bg-slate-100 hover:text-slate-600 disabled:opacity-40"
          >
            <X className="h-5 w-5" />
          </button>

          <div className="mb-5 pr-8">
            {/* STEP INDICATOR */}
            <span className="inline-block mb-3 rounded-md bg-slate-100 px-2.5 py-1 text-xs font-semibold text-slate-600">
              Step {currentStep} of 3
            </span>

            <h2
              id="midmonth-title"
              className="text-2xl font-bold text-[#37455F]"
            >
              {headingText}
            </h2>
            <p className="mt-2 text-sm leading-5 text-slate-600">
              {subtitleText}
            </p>
          </div>

          {/* =========================== */}
          {/* STEP 1: MONTHLY TARGET      */}
          {/* =========================== */}
          {currentStep === 1 && (
            <>
              <div className="mb-5 grid grid-cols-3 gap-2 sm:gap-3">
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
                <AmazonStatCard
                  label={`${prevMonthName} MTD Sales`}
                  current={data.lastMonthToDateHome || 0}
                  previous={null}
                  deltaPct={null}
                  bottomLabel=""
                  hidePrevious={true}
                  loading={false}
                  formatter={(val) => money(val, data.homeCurrency)}
                  className="border-[#B75A5A] border-t-4 border-t-[#B75A5A]"
                />
              </div>

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
                      if (e.key === "Enter") {
                        setSaveError(null);
                        setCurrentStep(2);
                      }
                    }}
                    placeholder="0"
                    disabled={saving}
                    aria-invalid={!!saveError}
                    className="w-full min-w-0 bg-transparent text-lg font-semibold leading-tight text-charcoal-500 outline-none placeholder:text-slate-300"
                  />
                </div>

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

                <div className="mt-4">
                  {/* <div className="h-1.5 w-full overflow-hidden rounded-full bg-slate-200">
                    <div
                      className="h-full rounded-full bg-green-500 transition-[width] duration-300"
                      style={{
                        width: `${progressPct === null
                          ? 0
                          : Math.min(progressPct, 100)
                          }%`,
                      }}
                    />
                  </div> */}
                  {/* <p className="mt-2 text-xs text-slate-500">
                    {progressPct === null ? (
                      "Enter a target to see your progress."
                    ) : (
                      <>
                        <span className="font-bold text-green-500">
                          {monthCompletedPct.toFixed(2)}%
                        </span>{" "}
                        of Month Completed vs{" "}
                        <span className="font-bold text-green-500">
                          {progressPct.toFixed(2)}%
                        </span>{" "}
                        of Target Achieved
                      </>
                    )}
                  </p> */}
                </div>

                {progressPct !== null && (
                  <div
                    ref={gaugeWrapRef}
                    className="relative mt-4 flex flex-col items-center"
                  >
                    <svg
                      width={gaugeSize}
                      height={gaugeSize / 2 + 10}
                      viewBox={`0 0 ${gaugeSize} ${gaugeSize / 2 + 10}`}
                      role="img"
                      aria-label={`${progressPct.toFixed(2)}% target achieved`}
                      onMouseMove={handleGaugeMouseMove}
                      onMouseLeave={() => setGaugeTooltipPosition(null)}
                    >
                      <path
                        d={gaugeArc(targetGaugeRadius, targetGaugePct)}
                        fill="none"
                        stroke="#5EA68E"
                        strokeWidth="5"
                        strokeLinecap="round"
                      />
                      <path
                        d={gaugeArc(92, mtdGaugePct)}
                        fill="none"
                        stroke="#ED9F50"
                        strokeWidth="8"
                        strokeLinecap="round"
                      />
                      <path
                        d={gaugeArc(78, lastMonthGaugePct)}
                        fill="none"
                        stroke="#9CA3AF"
                        strokeWidth="6"
                        strokeLinecap="round"
                      />
                      <circle
                        cx={targetGaugeEnd.x}
                        cy={targetGaugeEnd.y}
                        r="5"
                        fill="#5EA68E"
                        stroke="white"
                        strokeWidth="2"
                      />
                      <circle
                        cx={mtdGaugeEnd.x}
                        cy={mtdGaugeEnd.y}
                        r="5"
                        fill="#ED9F50"
                        stroke="white"
                        strokeWidth="2"
                      />
                      <circle
                        cx={lastMonthGaugeEnd.x}
                        cy={lastMonthGaugeEnd.y}
                        r="5"
                        fill="#9CA3AF"
                        stroke="white"
                        strokeWidth="2"
                      />
                      {(data?.lastMonthToDateHome || 0) > 0 && (
                        <circle
                          cx={lastMonthToDateMarker.x}
                          cy={lastMonthToDateMarker.y}
                          r="5"
                          fill="#B75A5A"
                          stroke="white"
                          strokeWidth="2"
                        />
                      )}
                    </svg>
                    {gaugeTooltipPosition && (
                      <div
                        className="pointer-events-none absolute z-20 w-max max-w-[220px] rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs shadow-md"
                        style={{
                          top: gaugeTooltipPosition.y - 12,
                          left:
                            gaugeTooltipPosition.x + 205 > gaugeSize
                              ? gaugeTooltipPosition.x - 205
                              : gaugeTooltipPosition.x + 12,
                        }}
                      >
                        <div className="font-semibold text-gray-900">
                          Sales Snapshot
                        </div>
                        <div className="mt-1 space-y-1 text-gray-600">
                          {gaugeTooltipLines.map((line, index) => (
                            <div
                              key={index}
                              className="flex items-center gap-1.5"
                            >
                              <span
                                className="h-2 w-2 shrink-0 rounded-full"
                                style={{ backgroundColor: line.color }}
                              />

                              <span>
                                <span className="font-medium">{line.label}:</span>{" "}
                                {line.value}
                              </span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                    <div className="-mt-1 text-center">
                      <div className="text-3xl font-semibold text-[#37455F]">
                        {progressPct.toFixed(2)}%
                      </div>
                      <div className="text-[10px] text-charcoal-500 2xl:text-xs">
                        Target Achieved
                      </div>
                    </div>
                  </div>
                )}

                <p className="mt-2 text-xs text-slate-500 text-center">
                  {progressPct === null ? (
                    "Enter a target to see your progress."
                  ) : (
                    <>
                      <span className="font-bold text-green-500">
                        {monthCompletedPct.toFixed(2)}%
                      </span>{" "}
                      of Month Completed vs{" "}
                      <span className="font-bold text-green-500">
                        {progressPct.toFixed(2)}%
                      </span>{" "}
                      of Target Achieved
                    </>
                  )}
                </p>
              </div>
            </>
          )}

          {/* =========================== */}
          {/* STEP 2: BUSINESS OBJECTIVES */}
          {/* =========================== */}
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

              <div className="flex items-center justify-between gap-4 border-t border-slate-200 pt-5">
                <div>
                  <p
                    id="inventory-clearance-label"
                    className="text-sm font-semibold text-[#37455F]"
                  >
                    Clear Ageing Inventory
                  </p>
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

          {/* =========================== */}
          {/* STEP 3: WAREHOUSE STOCK     */}
          {/* =========================== */}
          {currentStep === 3 && (
            <div>
              <div
                role="button"
                tabIndex={0}
                aria-label="Upload warehouse stock Excel"
                onClick={() => {
                  if (!warehouseUploading) warehouseInputRef.current?.click();
                }}
                onKeyDown={(e) => {
                  if (
                    (e.key === "Enter" || e.key === " ") &&
                    !warehouseUploading
                  ) {
                    warehouseInputRef.current?.click();
                  }
                }}
                onDragOver={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  setWarehouseDragOver(true);
                }}
                onDragLeave={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  setWarehouseDragOver(false);
                }}
                onDrop={handleWarehouseDrop}
                className={[
                  "relative flex cursor-pointer flex-col items-center justify-center rounded-xl border-2 border-dashed px-4 py-8 text-center transition",
                  warehouseDragOver
                    ? "border-green-500 bg-green-50"
                    : "border-slate-300 bg-slate-50 hover:border-green-400 hover:bg-green-50/60",
                  warehouseUploading ? "pointer-events-none opacity-60" : "",
                ].join(" ")}
              >
                <UploadCloud className="mb-3 h-8 w-8 text-slate-400" />
                <p className="text-sm font-medium text-slate-700">
                  Drag &amp; drop your Excel file here
                </p>
                <p className="mt-1 text-xs text-slate-500">
                  or{" "}
                  <span className="font-semibold text-green-700">browse</span>{" "}
                  — .xlsx, .xls, .csv (max {WAREHOUSE_MAX_SIZE_MB} MB)
                </p>
                <input
                  ref={warehouseInputRef}
                  type="file"
                  accept=".xlsx,.xls,.csv"
                  className="hidden"
                  onChange={(e) =>
                    handleWarehouseFile(e.target.files?.[0] || null)
                  }
                />
              </div>

              {warehouseFile && (
                <div className="mt-3 flex items-center justify-between gap-3 rounded-lg border border-slate-200 bg-white px-3 py-2.5">
                  <div className="flex min-w-0 items-center gap-2">
                    <FileSpreadsheet className="h-5 w-5 shrink-0 text-green-600" />
                    <div className="min-w-0">
                      <p className="truncate text-sm font-medium text-slate-800">
                        {warehouseFile.name}
                      </p>
                      <p className="text-xs text-slate-500">
                        {humanFileSize(warehouseFile.size)}
                      </p>
                    </div>
                  </div>
                  {!warehouseUploading && (
                    <button
                      type="button"
                      onClick={() => {
                        setWarehouseFile(null);
                        setWarehouseError(null);
                        setWarehouseSuccess(null);
                      }}
                      className="rounded-md p-1 text-slate-400 hover:bg-slate-100 hover:text-slate-700"
                      aria-label="Remove selected file"
                    >
                      <X className="h-4 w-4" />
                    </button>
                  )}
                </div>
              )}

              {warehouseUploading && (
                <div className="mt-4 flex items-center gap-2 rounded-lg border border-blue-200 bg-blue-50 px-3 py-2.5 text-sm text-blue-700">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Uploading your warehouse stock...
                </div>
              )}

              {warehouseSuccess && (
                <div className="mt-4 flex items-start gap-2 rounded-lg border border-green-200 bg-green-50 px-3 py-2.5 text-sm text-green-700">
                  <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" />
                  <span>{warehouseSuccess}</span>
                </div>
              )}

              {warehouseError && (
                <div className="mt-4 flex items-start gap-2 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2.5 text-sm text-rose-700">
                  <XCircle className="mt-0.5 h-4 w-4 shrink-0" />
                  <span>{warehouseError}</span>
                </div>
              )}
            </div>
          )}

          <div className="mt-5 flex items-center gap-2 rounded-lg bg-[#F8F9FA] px-4 py-3 text-xs text-slate-500">
            <Info className="h-4 w-4 shrink-0 text-slate-400" />
            {infoText}
          </div>

          {saveError && (
            <p
              role="alert"
              className="mt-3 rounded-lg bg-red-50 px-3 py-2 text-xs font-medium text-red-600"
            >
              {saveError}
            </p>
          )}
        </div>

        {/* =========================== */}
        {/* FOOTER: STEP ACTIONS        */}
        {/* =========================== */}
        <div className="shrink-0 border-t border-slate-200 bg-white px-6 py-4 sm:px-8">

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

              <Button
                type="button"
                size="md"
                onClick={() => {
                  setSaveError(null);
                  setCurrentStep(2);
                }}
                disabled={saving}
                className="px-6 py-2.5 text-sm font-semibold text-white hover:bg-[#1f2943] disabled:opacity-60"
              >
                Next
              </Button>
            </div>
          )}



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
                onClick={() => {
                  setSaveError(null);
                  setCurrentStep(3);
                }}
                disabled={saving}
                className="px-6 py-2.5 text-sm font-semibold text-white hover:bg-[#1f2943] disabled:opacity-60"
              >
                Next
              </Button>
            </div>
          )}


          {currentStep === 3 && (
            <div className="flex items-center justify-between gap-3">
              <Button
                size="md"
                variant="outline"
                onClick={() => {
                  setWarehouseError(null);
                  setCurrentStep(2);
                }}
                disabled={warehouseUploading || !!warehouseSuccess || saving}
                className="px-4 py-2.5 text-sm font-semibold text-charcoal-500 transition hover:bg-slate-50 disabled:opacity-50"
              >
                Back
              </Button>

              <Button
                type="button"
                size="md"
                onClick={handleFinishSetup}
                disabled={saving || warehouseUploading || !!warehouseSuccess}
                className="inline-flex items-center gap-2 px-6 py-2.5 text-sm font-semibold text-white hover:bg-[#1f2943] disabled:opacity-60"
              >
                {saving ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Saving...
                  </>
                ) : warehouseUploading ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Uploading...
                  </>
                ) : (
                  <>
                    {/* <CheckCircle2 className="h-4 w-4" /> */}
                    Save
                  </>
                )}
              </Button>
            </div>
          )}

        </div>
      </div>
    </div>,
    document.body
  );
}