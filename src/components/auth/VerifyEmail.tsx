
"use client";

import Link from "next/link";
import React, { useEffect, useMemo, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { ChevronLeftIcon } from "@/icons";
import { useResendVerificationMutation } from "@/lib/api/userApi";

export default function VerifyEmail() {
  const router = useRouter();
  const search = useSearchParams();

  // /verify-email?status=success|failed|...&email=you@example.com
  const status = (search.get("status") || "").toLowerCase();
  const email = search.get("email") || "";

  const [flash, setFlash] = useState<string>("");

  const isSuccess = status === "success";
  const isFailed = status === "failed";
  const isInvalid = !isSuccess && !isFailed;

  // RTK Query mutation
  const [resendVerification, { isLoading: resendLoading }] =
    useResendVerificationMutation();

  // If success → briefly show the success UI then send to Sign In
  useEffect(() => {
  if (isSuccess) {
    // 🔥 CLEAR previous user onboarding state
    localStorage.removeItem("onboardDone");
    localStorage.removeItem("selectedCountries");
    localStorage.removeItem("brandName");
    localStorage.removeItem("companyName");
    localStorage.removeItem("annualRevenue");
    localStorage.removeItem("hasSeenFirstTimeRoute");
    localStorage.removeItem("firstProfitPath");

    const t = setTimeout(() => {
      router.push("/signin");
    }, 3000);

    return () => clearTimeout(t);
  }
}, [isSuccess, router]);

  const message = useMemo(() => {
    if (isSuccess) {
      return "Your email has been successfully verified. You now have full access to all platform features.";
    }
    if (isFailed) {
      return "Email verification failed. Please try again.";
    }
    return "Invalid request.";
  }, [isSuccess, isFailed]);

  const resend = async () => {
    if (!email) {
      setFlash("Email not found in the link.");
      return;
    }
    try {
      const res = await resendVerification({ email }).unwrap();
      if (res?.success !== false) {
        setFlash(res?.message || "Verification email has been successfully resent!");
      } else {
        setFlash(res?.message || "Failed to resend the verification email.");
      }
    } catch (e: any) {
      setFlash(
        e?.data?.message ||
          "An error occurred while resending the email. Please try again."
      );
    }
  };

  return (
    <div className="flex flex-col flex-1 lg:w-1/2 w-full">
      
      {/* Centered card area */}
      <div className="flex flex-col justify-center flex-1 w-full max-w-md mx-auto">
        <div>
          <div className="mb-5 sm:mb-8">
            <h1 className="mb-2 font-semibold text-green-500 text-title-sm dark:text-white/90 sm:text-title-md">
              {isSuccess ? "Email Verified!" : "Email Verification"}
            </h1>
            <p className="text-sm text-charcoal-500 dark:text-gray-400">{message}</p>
          </div>

          {/* Success View */}
          {isSuccess && (
            <div className="rounded-xl border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 p-5 shadow-theme-xs">
              <div className="flex flex-col items-center text-center space-y-3">
                <div className="h-12 w-12 rounded-full bg-emerald-100 flex items-center justify-center dark:bg-emerald-900/20">
                  <svg viewBox="0 0 24 24" fill="none" className="h-7 w-7" aria-hidden="true">
                    <path
                      d="M9 12.5l2 2 5-5"
                      stroke="currentColor"
                      strokeWidth="2"
                      className="text-emerald-600 dark:text-emerald-400"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                    <circle
                      cx="12"
                      cy="12"
                      r="9"
                      stroke="currentColor"
                      strokeWidth="1.5"
                      className="text-emerald-600/40 dark:text-emerald-400/30"
                    />
                  </svg>
                </div>
                <p className="text-sm text-gray-500 dark:text-gray-400">
                  Redirecting you to Sign In…
                </p>
                <Link
                  href="/signin"
                  className="inline-flex items-center rounded-lg bg-[#2c3854] px-4 py-2 text-sm font-semibold text-[#f8edcf] hover:opacity-95"
                >
                  Go to Sign In now
                </Link>
              </div>
            </div>
          )}

          {/* Failed / Invalid View */}
          {(isFailed || isInvalid) && (
            <div className="rounded-xl border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 p-5 shadow-theme-xs space-y-4">
              <div className="space-y-1">
                <p className="text-sm text-gray-600 dark:text-gray-300">
                  {isFailed
                    ? "If this keeps happening, try requesting a new verification email."
                    : "The verification link seems invalid or expired."}
                </p>
                {email ? (
                  <p className="text-xs text-gray-400">Email: {email}</p>
                ) : (
                  <p className="text-xs text-gray-400">No email found in the link.</p>
                )}
              </div>

              <div className="flex items-center gap-3">
                <button
                  onClick={resend}
                  disabled={resendLoading}
                  className="inline-flex justify-center rounded-lg bg-[#2c3854] px-4 py-2 text-sm font-semibold text-[#f8edcf] hover:opacity-95 disabled:opacity-60"
                >
                  {resendLoading ? "Resending…" : "Resend Verification Email"}
                </button>

                <Link
                  href="/signin"
                  className="text-sm text-brand-500 hover:text-brand-600 dark:text-brand-400"
                >
                  Back to Sign In
                </Link>
              </div>

              {flash && (
                <p className="text-sm mt-1 text-gray-600 dark:text-gray-300">
                  {flash}
                </p>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
