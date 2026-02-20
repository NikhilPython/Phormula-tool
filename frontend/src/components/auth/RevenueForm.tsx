"use client";

import Link from "next/link";
import React, { useEffect, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { ChevronLeftIcon } from "@/icons";

// RTK Query hooks
import { useSubmitSelectFormMutation, useMarkOnboardingCompleteMutation } from "@/lib/api/onboardingApi";

const REVENUE_OPTIONS = [
  "$0 - $50K",
  "$50K - $100K",
  "$100K - $500K",
  "$500K - $1M",
  "$1M+",
] as const;

export default function RevenueForm() {
  const router = useRouter();
  const search = useSearchParams();
  const forceOnboard = search.get("onboard") === "1";

  const [selectedRevenue, setSelectedRevenue] = useState<string>("");
  const [guarding, setGuarding] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // RTK Query
  const [submitSelectForm] = useSubmitSelectFormMutation();
  const [markOnboardingComplete] = useMarkOnboardingCompleteMutation();

  // --- Guard: stay on page during onboarding, only check token ---
  useEffect(() => {
    let cancelled = false;

    const ensureToken = async (): Promise<string | null> => {
      // allow a short race where login just stored token
      for (let i = 0; i < 6; i++) {
        const t = localStorage.getItem("jwtToken");
        if (t) return t;
        await new Promise((r) => setTimeout(r, 100));
      }
      return localStorage.getItem("jwtToken");
    };

    (async () => {
      const token = await ensureToken();
      if (!token) {
        if (!cancelled) router.replace(`/signin?redirect=/chooserevenue?onboard=1`);
        return;
      }

      // If we’re onboarding, do not auto-redirect away.
      setGuarding(false);
    })();

    return () => {
      cancelled = true;
    };
  }, [router, forceOnboard]);

  // const onSubmit = async (e: React.FormEvent) => {
  //   e.preventDefault();
  //   if (!selectedRevenue) {
  //     setError("Please select a revenue range.");
  //     return;
  //   }

  //   setError(null);
  //   setLoading(true);

  //   // Pull values already collected in earlier steps
  //   const countries = JSON.parse(localStorage.getItem("selectedCountries") || "[]") as string[];
  //   const companyName = localStorage.getItem("companyName") || "";
  //   const brandName = localStorage.getItem("brandName") || "";
  //   const homeCurrency = localStorage.getItem("homeCurrency") || "";

  //   try {
  //     // Submit selection to backend
  //     await submitSelectForm({
  //       annual_sales_range: selectedRevenue,
  //       country: countries.join(", "),
  //       company_name: companyName,
  //       brand_name: brandName,
  //       homeCurrency,
  //     }).unwrap();

  //     // Mark onboarding as done on client + server
  //     localStorage.setItem("onboardDone", "true");
  //     try {
  //       await markOnboardingComplete({ onboarding_complete: true }).unwrap();
  //     } catch {
  //       // non-blocking
  //     }
  //   } finally {
  //     setLoading(false);
  //   }

  //   // Safe default: go to dashboard
  //   router.push("/");
  // };

//   const onSubmit = (e: React.FormEvent) => {
//   e.preventDefault();

//   if (!selectedRevenue) {
//     setError("Please select a revenue range.");
//     return;
//   }

//   setError(null);
//   setLoading(true);

//   // Pull values already collected in earlier steps
//   const countries = JSON.parse(
//     localStorage.getItem("selectedCountries") || "[]"
//   ) as string[];
//   const companyName = localStorage.getItem("companyName") || "";
//   const brandName = localStorage.getItem("brandName") || "";
//   const homeCurrency = localStorage.getItem("homeCurrency") || "";

//   // Mark onboarding done on client immediately
//   localStorage.setItem("onboardDone", "true");

//   // 🔥 Fire-and-forget submit + mark complete
//   submitSelectForm({
//     annual_sales_range: selectedRevenue,
//     country: countries.join(", "),
//     company_name: companyName,
//     brand_name: brandName,
//     homeCurrency,
//   })
//     .unwrap()
//     .then(() =>
//       markOnboardingComplete({ onboarding_complete: true }).unwrap()
//     )
//     .catch((e) => {
//       console.warn("Onboarding completion failed (non-blocking):", e);
//     });

//   // 🚀 Go to dashboard right away
//   router.push("/");

//   // ❌ No setLoading(false) – component is about to unmount
// };

const onSubmit = (e: React.FormEvent) => {
  e.preventDefault();

  if (!selectedRevenue) {
    setError("Please select a revenue range.");
    return;
  }

  setError(null);
  setLoading(true);

  // Pull values already collected in earlier steps
 const countries = JSON.parse(
  localStorage.getItem("selectedCountries") || "[]"
) as string[];
const companyName = localStorage.getItem("companyName") || "";
const brandName = localStorage.getItem("brandName") || "";
const homeCurrency = localStorage.getItem("homeCurrency") || "";

// ✅ ADD THESE TWO LINES HERE
localStorage.setItem("annualRevenue", selectedRevenue);
localStorage.setItem("onboardDone", "true");

// 🔥 Fire-and-forget submit + mark complete
submitSelectForm({
  annual_sales_range: selectedRevenue,
  country: countries.join(", "),
  company_name: companyName,
  brand_name: brandName,
  homeCurrency,
})
    .unwrap()
    
    .catch((e) => {
      console.warn("Onboarding completion failed (non-blocking):", e);
    });

  // 👉 Build the FIRST-TIME Profits route
  // You can adjust these if you store them differently
  const ranged =
    localStorage.getItem("ranged") || "monthly"; // or "yearly", "range", etc.
  const countryName = countries[0] || "global";

  const now = new Date();
  const month = String(now.getMonth() + 1).padStart(2, "0"); // "01".."12"
  const year = String(now.getFullYear());

  const profitPath = `/pnl-dashboard/QTD/${countryName}/NA/NA`;

  // (Optional) store that user has already seen first-time Profits
  localStorage.setItem("hasSeenFirstTimeRoute", "true");
  localStorage.setItem("firstProfitPath", profitPath);

  // 🚀 FIRST TIME: go to Profits screen
  router.push(profitPath);

  // ❌ No setLoading(false) – component is about to unmount
};


  const onBack = () => {
    if (typeof window !== "undefined" && window.history.length > 1) {
      router.replace("/brand?onboard=1");
    } else {
      router.push("/brand?onboard=1");
    }
  };

  if (guarding) {
    return (
      <div className="flex min-h-[60vh] items-center justify-center">
        <p className="text-gray-600 dark:text-gray-300">Loading…</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col flex-1 lg:w-1/2 w-full">

      {/* Form column */}
      <div className="flex flex-col justify-center flex-1 w-full  xl:max-w-lg xl:mx-auto lg:mx-6 max-w-md mx-auto">
        <div className="flex flex-col 2xl:gap-14 gap-10">
          <div className="">
            <h1 className=" font-semibold text-charcoal-500 text-title-sm dark:text-white/90 xl:text-title-md lg:text-3xl sm:text-title-md">
            Select  <span className="text-[#5EA68E]">Estimated Revenue</span> achieved in the past 12 Months
            </h1>
          </div>

         <form onSubmit={onSubmit} className="space-y-4">
  {REVENUE_OPTIONS.map((label) => {
    const checked = selectedRevenue === label;

    return (
      <label
        key={label}
        className={`flex items-center justify-between rounded-lg border px-4 py-3 cursor-pointer transition ${
          checked
            ? "border-[#48A887] bg-[#f5faff]"
            : "border-gray-300 bg-white dark:bg-gray-900"
        }`}
      >
        <span className="text-base text-[#414042] dark:text-gray-200">
          {label}
        </span>

        {/* Hidden checkbox (single-select behavior controlled by your state) */}
        <input
          type="checkbox"
          checked={checked}
          onChange={() => setSelectedRevenue(checked ? "" : label)}
          className="sr-only peer"
        />

        {/* Custom checkbox */}
        <span
          className="
            h-5 w-5 flex items-center justify-center rounded border border-gray-400
            peer-checked:bg-[#48A887]
            peer-checked:border-[#48A887]
          "
        >
          {checked && (
            <svg
              xmlns="http://www.w3.org/2000/svg"
              viewBox="0 0 24 24"
              fill="none"
              stroke="#f8edce"
              strokeWidth="3"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="h-4 w-4"
            >
              <polyline points="20 6 9 17 4 12" />
            </svg>
          )}
        </span>
      </label>
    );
  })}

  {error && (
    <p className="text-sm text-red-500" aria-live="polite">
      {error}
    </p>
  )}

  <div className="mt-10 flex items-center justify-end gap-3 ">
    <button
      type="button"
      onClick={onBack}
      className="inline-flex justify-center rounded-lg bg-gray-200 px-4 py-2 text-sm font-medium text-gray-800 hover:bg-gray-300 dark:bg-white/10 dark:text-gray-200 dark:hover:bg-white/15"
    >
      Back
    </button>
    <button
      type="submit"
      disabled={loading}
      className="inline-flex justify-center rounded-lg bg-[#2c3854] px-4 py-2 text-sm font-semibold text-[#f8edcf] hover:opacity-95 disabled:opacity-60"
    >
      {loading ? "Please wait…" : "Submit"}
    </button>
  </div>
</form>

        </div>
      </div>
    </div>
  );
}
