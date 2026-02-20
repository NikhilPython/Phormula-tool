
"use client";

import Link from "next/link";
import React, { useEffect, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { ChevronLeftIcon } from "@/icons";

// RTK Query hooks
import { useLazyGetUserDataQuery } from "@/lib/api/profileApi";
import { useSubmitSelectFormMutation } from "@/lib/api/onboardingApi";
import Button from "../ui/button/Button";
import { useAppDispatch, useAppSelector } from "@/lib/hooks";
import { setUser } from "@/lib/features/auth/authSlice";

export default function BrandForm() {
  const router = useRouter();
  const search = useSearchParams();

  const dispatch = useAppDispatch();
const userFromStore = useAppSelector((s: any) => s.auth.user);

  const forceOnboard = search.get("onboard") === "1";

  const [companyName, setCompanyName] = useState("");
  const [brandName, setBrandName] = useState("");
  const [guarding, setGuarding] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // RTK Query hooks
  const [triggerUser] = useLazyGetUserDataQuery();
  const [submitSelectForm] = useSubmitSelectFormMutation();

  useEffect(() => {
    let cancelled = false;

    const ensureToken = async (): Promise<string | null> => {
      // small poll to avoid race right after login
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
        if (!cancelled) router.replace(`/signin?redirect=/brand?onboard=1`);
        return;
      }

      if (forceOnboard) {
        if (!cancelled) setGuarding(false);
        return; // stay here during onboarding
      }

      // Non-onboarding visits: skip if already configured
      try {
        const user = await triggerUser().unwrap();
        const hasBrand =
          typeof user?.brand_name === "string" && user.brand_name.trim().length > 0;

        if (hasBrand && !cancelled) {
          router.replace("/");
          return;
        }
      } catch {
        // fail open
      } finally {
        if (!cancelled) setGuarding(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [forceOnboard, router, triggerUser]);

  const onSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!companyName.trim() || !brandName.trim()) {
      setError("Please enter both Company name and Brand name.");
      return;
    }
    setError(null);
    setLoading(true);

    const company = companyName.trim();
    const brand = brandName.trim();

    // Persist locally (for later steps / reuse)
    localStorage.setItem("companyName", company);
    localStorage.setItem("brandName", brand);

    dispatch(
      setUser({
        ...userFromStore,
        company_name: company,
        brand_name: brand,
      })
    );

    // Prepare payload (reuse local storage values for countries/currency)
    const selected = JSON.parse(localStorage.getItem("selectedCountries") || "[]") as string[];
    const homeCurrency = localStorage.getItem("homeCurrency") || "";

    // 🔥 Fire-and-forget update to backend
    submitSelectForm({
      country: selected.join(", "),
      company_name: company,
      brand_name: brand,
      homeCurrency,
    })
      .unwrap()
      .catch((e) => {
        console.warn("Profile update failed (non-blocking):", e);
      });

    // 🚀 Continue onboarding immediately
    router.push("/chooserevenue?onboard=1");
    // no setLoading(false); component will unmount after navigation
  };

  const onBack = () => {
    if (typeof window !== "undefined" && window.history.length > 1) {
router.replace("/choose-country?onboard=1");
    } else {
      router.push("/choose-country?onboard=1");
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
      

      <div className="flex flex-col justify-center  flex-1 w-full xl:max-w-lg xl:mx-auto lg:mx-6 max-w-md mx-auto">
        <div>
          <div className="mb-5 sm:mb-8">
            <h1 className="mb-2 font-semibold text-[#414042] text-title-sm dark:text-white/90 xl:text-title-md lg:text-4xl sm:text-title-md">
              What is your <span className="text-[#5EA68E]">Company</span> and <span className="text-[#5EA68E]">Brand</span> name?
            </h1>
          </div>

          <form onSubmit={onSubmit} className="space-y-5">
            <div className="flex flex-col gap-1">
              <label className="xl:text-base text-sm font-medium text-charcoal-500 dark:text-gray-300">
                Company Name<span className="text-error-500">*</span>
              </label>
              <input
                type="text"
                className="xl:h-12 w-full rounded-lg border border-charcoal-500 bg-white px-4 py-2.5 text-sm  placeholder:text-gray-400 outline-green-500"
                placeholder="Acme Corp"
                value={companyName}
                onChange={(e) => setCompanyName(e.target.value)}
                autoComplete="organization"
                required
              />
            </div>

            <div className="flex flex-col gap-1">
              <label className="xl:text-base text-sm font-medium text-charcoal-500 dark:text-gray-300">
                Brand Name<span className="text-error-500">*</span>
              </label>
              <input
                type="text"
                className="xl:h-12 w-full rounded-lg border border-charcoal-500 bg-white px-4 py-2.5 text-sm shadow-theme-xs placeholder:text-gray-400 outline-green-500 "
                placeholder="Acme"
                value={brandName}
                onChange={(e) => setBrandName(e.target.value)}
                required
              />
            </div>

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
      {loading ? "Please wait…" : "Next"}
    </button>
  </div>
          </form>
        </div>
      </div>
    </div>
  );
}
