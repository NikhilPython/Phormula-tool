"use client";

import React, { useState } from "react";
import Image from "next/image";
import { useRouter } from "next/navigation";
import { toast } from "sonner";
import {
  Mail,
  Lock,
  Key,
  ArrowLeft,
  Eye,
  EyeOff,
  CheckCircle,
  Loader2,
  Shield,
} from "lucide-react";

type ApiResponse = {
  message?: string;
};

export default function SuperadminResetPasswordPage() {
  const [step, setStep] = useState<1 | 2>(1);
  const [email, setEmail] = useState<string>("");
  const [otp, setOtp] = useState<string>("");
  const [newPassword, setNewPassword] = useState<string>("");
  const [confirmPassword, setConfirmPassword] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(false);
  const [showPassword, setShowPassword] = useState<boolean>(false);
  const [showConfirmPassword, setShowConfirmPassword] =
    useState<boolean>(false);

  const router = useRouter();

  const validateEmail = (emailValue: string): boolean => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(emailValue);
  };

  const validatePassword = (password: string): boolean => password.length >= 8;

  const showSuccess = (msg: string) => {
    toast.success(msg);
  };

  const showError = (msg: string) => {
    toast.error(msg);
  };

  const requestOtp = async () => {
    if (!email.trim()) {
      showError("Please enter your email address");
      return;
    }

    if (!validateEmail(email)) {
      showError("Please enter a valid email address");
      return;
    }

    setLoading(true);

    try {
      const requestBody = JSON.stringify({ email });
      const headers = { "Content-Type": "application/json" };

      const superadminRequest = fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin_reset_otp`,
        {
          method: "POST",
          headers,
          body: requestBody,
        }
      );

      const adminRequest = fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/admin_reset_otp`,
        {
          method: "POST",
          headers,
          body: requestBody,
        }
      );

      const [superadminRes, adminRes] = await Promise.all([
        superadminRequest,
        adminRequest,
      ]);

      const [superadminData, adminData] = (await Promise.all([
        superadminRes.json(),
        adminRes.json(),
      ])) as [ApiResponse, ApiResponse];

      if (superadminRes.ok || adminRes.ok) {
        showSuccess("OTP has been sent to your email address.");
        setStep(2);
      } else {
        const errors: string[] = [];

        if (!superadminRes.ok) {
          errors.push(
            superadminData.message || "Superadmin OTP request failed"
          );
        }

        if (!adminRes.ok) {
          errors.push(adminData.message || "Admin OTP request failed");
        }

        showError(errors.join(". "));
      }
    } catch {
      showError("Network error. Please check your connection and try again.");
    } finally {
      setLoading(false);
    }
  };

  const resetPassword = async () => {
    if (!otp.trim()) {
      showError("Please enter the OTP");
      return;
    }

    if (!newPassword.trim()) {
      showError("Please enter a new password");
      return;
    }

    if (!validatePassword(newPassword)) {
      showError("Password must be at least 8 characters long");
      return;
    }

    if (newPassword !== confirmPassword) {
      showError("Passwords do not match");
      return;
    }

    setLoading(true);

    try {
      const requestBody = JSON.stringify({
        email,
        otp,
        new_password: newPassword,
      });

      const headers = { "Content-Type": "application/json" };

      const superadminRequest = fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin_reset_password`,
        {
          method: "POST",
          headers,
          body: requestBody,
        }
      );

      const adminRequest = fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/admin_reset_password`,
        {
          method: "POST",
          headers,
          body: requestBody,
        }
      );

      const [superadminRes, adminRes] = await Promise.all([
        superadminRequest,
        adminRequest,
      ]);

      const [superadminData, adminData] = (await Promise.all([
        superadminRes.json(),
        adminRes.json(),
      ])) as [ApiResponse, ApiResponse];

      if (superadminRes.ok || adminRes.ok) {
        showSuccess(
          "Password reset successful! You can now login with your new password."
        );

        setTimeout(() => {
          setStep(1);
          setEmail("");
          setOtp("");
          setNewPassword("");
          setConfirmPassword("");
          router.push("/superadmin/CDPAdminConsole");
        }, 1200);
      } else {
        const errors: string[] = [];

        if (!superadminRes.ok) {
          errors.push(
            superadminData.message || "Superadmin password reset failed"
          );
        }

        if (!adminRes.ok) {
          errors.push(adminData.message || "Admin password reset failed");
        }

        showError(errors.join(". "));
      }
    } catch {
      showError("Network error. Please check your connection and try again.");
    } finally {
      setLoading(false);
    }
  };

  const goBackToEmailStep = () => {
    setStep(1);
    setOtp("");
    setNewPassword("");
    setConfirmPassword("");
  };

  const resendOtp = () => requestOtp();

  return (
    <>
      <style jsx global>{`
        html,
        body {
          height: 100%;
          margin: 0;
          overflow: hidden;
          background: #37384f;
        }

        input.reset-input:-webkit-autofill,
        input.reset-input:-webkit-autofill:hover,
        input.reset-input:-webkit-autofill:focus,
        input.reset-input:-webkit-autofill:active {
          -webkit-box-shadow: 0 0 0 1000px rgba(255, 255, 255, 0.06)
            inset !important;
          -webkit-text-fill-color: #ffffff !important;
          caret-color: #ffffff !important;
          transition: background-color 9999s ease-in-out 0s;
        }
      `}</style>

      <div className="flex min-h-screen w-full items-center justify-center overflow-hidden bg-[#37384f] px-4 py-6">
        <div className="relative grid min-h-[620px] w-full max-w-[1120px] overflow-hidden rounded-[30px] bg-[#484962] shadow-[0_28px_70px_rgba(20,22,45,0.48)] lg:grid-cols-2">

          {/* LEFT SECTION */}
          <div className="relative hidden h-full border-r border-white/10 bg-[#42435c] px-8 py-8 lg:flex lg:flex-col lg:justify-between">
            <div>
              <Image
                width={185}
                height={54}
                src="/images/auth/Phormula.png"
                alt="Phormula"
                priority
                className="mx-auto h-auto w-[165px] object-contain sm:w-[185px]"
              />

              <h1 className="mt-6 max-w-[360px] m-auto text-center text-3xl font-bold leading-[1.15] tracking-[-0.03em] text-white">
                Recover your Super Admin account securely.
              </h1>

              <p className="mt-6 text-center text-sm leading-6 text-white/60">
                Verify your email with an OTP and create a new password to restore
                access to dashboards, admin controls, marketplace data, and user
                management tools.
              </p>
            </div>

            <div className="rounded-2xl border border-white/10 bg-white/[0.05] p-5">
              <p className="text-sm font-semibold text-white">Reset Password Steps</p>

              <ul className="mt-4 space-y-3 text-sm text-white/65">
                <li className="flex gap-3">
                  <CheckCircle className="mt-0.5 h-4 w-4 shrink-0 text-[#31d9e5]" />
                  Enter your registered email address
                </li>
                <li className="flex gap-3">
                  <CheckCircle className="mt-0.5 h-4 w-4 shrink-0 text-[#31d9e5]" />
                  Verify the OTP sent to your email
                </li>
                <li className="flex gap-3">
                  <CheckCircle className="mt-0.5 h-4 w-4 shrink-0 text-[#31d9e5]" />
                  Create a password with at least 8 characters
                </li>
                <li className="flex gap-3">
                  <CheckCircle className="mt-0.5 h-4 w-4 shrink-0 text-[#31d9e5]" />
                  Sign in again with your new password
                </li>
              </ul>
            </div>

            <div className="absolute -bottom-20 -left-20 h-56 w-56 rounded-full bg-[#31d9e5]/10 blur-3xl" />
            <div className="absolute -right-16 top-20 h-44 w-44 rounded-full bg-white/10 blur-3xl" />
          </div>

          {/* RIGHT FORM SECTION */}
          <div className="flex min-h-0 items-center justify-center px-6 py-4 sm:px-10 lg:px-14">
            <div className="w-full max-w-[430px]">
              <div className="mb-5 text-center">
                <div className="mx-auto mb-4 flex h-16 w-16 items-center justify-center rounded-2xl border border-[#31d9e5]/25 bg-[#31d9e5]/15 text-[#31d9e5] lg:hidden">
                  <Shield size={32} />
                </div>

                <h1 className="text-2xl font-bold tracking-tight text-white sm:text-3xl">
                  Reset Password
                </h1>

                <p className="mt-2 text-sm text-white/60">
                  {step === 1
                    ? "Enter your email to receive an OTP"
                    : "Enter OTP and set your new password"}
                </p>
              </div>

              <div className="mb-6 flex items-center justify-center">
                <div
                  className={`flex h-8 w-8 items-center justify-center rounded-full text-sm font-semibold ${step >= 1
                    ? "bg-[#31d9e5] text-[#303247]"
                    : "bg-white/10 text-white/50"
                    }`}
                >
                  1
                </div>

                <div
                  className={`mx-2 h-1 w-14 rounded-full ${step >= 2 ? "bg-[#31d9e5]" : "bg-white/10"
                    }`}
                />

                <div
                  className={`flex h-8 w-8 items-center justify-center rounded-full text-sm font-semibold ${step >= 2
                    ? "bg-[#31d9e5] text-[#303247]"
                    : "bg-white/10 text-white/50"
                    }`}
                >
                  2
                </div>
              </div>

              {step === 1 && (
                <div className="space-y-4">
                  <div>
                    <label
                      htmlFor="reset-email"
                      className="mb-2 block text-sm font-semibold text-white/85"
                    >
                      Email Address
                    </label>

                    <div className="relative">
                      <Mail
                        className="absolute left-3 top-1/2 -translate-y-1/2 text-white/45"
                        size={20}
                      />

                      <input
                        id="reset-email"
                        type="email"
                        placeholder="Enter your email address"
                        value={email}
                        onChange={(e) => setEmail(e.target.value)}
                        disabled={loading}
                        className="reset-input h-12 w-full rounded-xl border border-white/10 bg-white/[0.06] pl-10 pr-4 text-sm text-white outline-none transition placeholder:text-white/35 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15 disabled:cursor-not-allowed disabled:opacity-60"
                        required
                      />
                    </div>
                  </div>

                  <button
                    type="button"
                    onClick={requestOtp}
                    disabled={loading}
                    className="inline-flex h-12 w-full items-center justify-center gap-2 rounded-xl bg-[#31d9e5] px-5 text-sm font-semibold text-[#303247] shadow-[0_10px_22px_rgba(20,220,230,0.20)] transition hover:-translate-y-0.5 hover:bg-[#28cbd6] disabled:cursor-not-allowed disabled:translate-y-0 disabled:opacity-60"
                  >
                    {loading ? (
                      <>
                        <Loader2 size={18} className="animate-spin" />
                        Sending OTP...
                      </>
                    ) : (
                      <>
                        <Mail size={18} />
                        Send OTP
                      </>
                    )}
                  </button>
                </div>
              )}

              {step === 2 && (
                <div className="space-y-3">
                  <button
                    type="button"
                    onClick={goBackToEmailStep}
                    disabled={loading}
                    className="mb-1 flex items-center text-sm font-medium text-[#31d9e5] transition hover:text-white disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    <ArrowLeft size={16} className="mr-1" />
                    Back to email
                  </button>

                  <div>
                    <label
                      htmlFor="reset-otp"
                      className="mb-2 block text-sm font-semibold text-white/85"
                    >
                      OTP Code
                    </label>

                    <div className="relative">
                      <Key
                        className="absolute left-3 top-1/2 -translate-y-1/2 text-white/45"
                        size={20}
                      />

                      <input
                        id="reset-otp"
                        type="text"
                        inputMode="numeric"
                        placeholder="Enter 6-digit OTP"
                        value={otp}
                        onChange={(e) =>
                          setOtp(e.target.value.replace(/\D/g, "").slice(0, 6))
                        }
                        disabled={loading}
                        className="reset-input h-12 w-full rounded-xl border border-white/10 bg-white/[0.06] pl-10 pr-4 text-center text-sm tracking-[0.3em] text-white outline-none transition placeholder:tracking-normal placeholder:text-white/35 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15 disabled:cursor-not-allowed disabled:opacity-60"
                        maxLength={6}
                        required
                      />
                    </div>

                    <button
                      type="button"
                      onClick={resendOtp}
                      disabled={loading}
                      className="mt-2 text-xs font-medium text-[#31d9e5] transition hover:text-white disabled:cursor-not-allowed disabled:opacity-60"
                    >
                      Didn&apos;t receive OTP? Resend
                    </button>
                  </div>

                  <div>
                    <label
                      htmlFor="reset-new-password"
                      className="mb-2 block text-sm font-semibold text-white/85"
                    >
                      New Password
                    </label>

                    <div className="relative">
                      <Lock
                        className="absolute left-3 top-1/2 -translate-y-1/2 text-white/45"
                        size={20}
                      />

                      <input
                        id="reset-new-password"
                        type={showPassword ? "text" : "password"}
                        placeholder="Enter new password"
                        value={newPassword}
                        onChange={(e) => setNewPassword(e.target.value)}
                        disabled={loading}
                        className="reset-input h-12 w-full rounded-xl border border-white/10 bg-white/[0.06] pl-10 pr-12 text-sm text-white outline-none transition placeholder:text-white/35 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15 disabled:cursor-not-allowed disabled:opacity-60"
                        minLength={8}
                        required
                      />

                      <button
                        type="button"
                        onClick={() => setShowPassword((prev) => !prev)}
                        disabled={loading}
                        className="absolute right-3 top-1/2 -translate-y-1/2 text-white/45 transition hover:text-[#31d9e5] disabled:cursor-not-allowed disabled:opacity-60"
                        aria-label={
                          showPassword ? "Hide password" : "Show password"
                        }
                      >
                        {showPassword ? <EyeOff size={20} /> : <Eye size={20} />}
                      </button>
                    </div>
                  </div>

                  <div>
                    <label
                      htmlFor="reset-confirm-password"
                      className="mb-2 block text-sm font-semibold text-white/85"
                    >
                      Confirm New Password
                    </label>

                    <div className="relative">
                      <Lock
                        className="absolute left-3 top-1/2 -translate-y-1/2 text-white/45"
                        size={20}
                      />

                      <input
                        id="reset-confirm-password"
                        type={showConfirmPassword ? "text" : "password"}
                        placeholder="Confirm new password"
                        value={confirmPassword}
                        onChange={(e) => setConfirmPassword(e.target.value)}
                        disabled={loading}
                        className="reset-input h-12 w-full rounded-xl border border-white/10 bg-white/[0.06] pl-10 pr-12 text-sm text-white outline-none transition placeholder:text-white/35 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15 disabled:cursor-not-allowed disabled:opacity-60"
                        minLength={8}
                        required
                      />

                      <button
                        type="button"
                        onClick={() =>
                          setShowConfirmPassword((prev) => !prev)
                        }
                        disabled={loading}
                        className="absolute right-3 top-1/2 -translate-y-1/2 text-white/45 transition hover:text-[#31d9e5] disabled:cursor-not-allowed disabled:opacity-60"
                        aria-label={
                          showConfirmPassword
                            ? "Hide confirm password"
                            : "Show confirm password"
                        }
                      >
                        {showConfirmPassword ? (
                          <EyeOff size={20} />
                        ) : (
                          <Eye size={20} />
                        )}
                      </button>
                    </div>
                  </div>

                  <button
                    type="button"
                    onClick={resetPassword}
                    disabled={loading}
                    className="inline-flex h-12 w-full items-center justify-center gap-2 rounded-xl bg-[#31d9e5] px-5 text-sm font-semibold text-[#303247] shadow-[0_10px_22px_rgba(20,220,230,0.20)] transition hover:-translate-y-0.5 hover:bg-[#28cbd6] disabled:cursor-not-allowed disabled:translate-y-0 disabled:opacity-60"
                  >
                    {loading ? (
                      <>
                        <Loader2 size={18} className="animate-spin" />
                        Resetting Password...
                      </>
                    ) : (
                      <>
                        <Shield size={18} />
                        Reset Password
                      </>
                    )}
                  </button>
                </div>
              )}

              <div className="mt-5 rounded-xl border border-white/10 bg-white/[0.05] p-4 lg:hidden">
                <p className="mb-2 text-sm font-semibold text-white">
                  Reset Password Steps:
                </p>

                <ul className="list-disc space-y-1 pl-5 text-xs leading-5 text-white/60">
                  <li>Enter your registered email address</li>
                  <li>Verify the OTP sent to your email</li>
                  <li>Create a password with at least 8 characters</li>
                  <li>Sign in again with your new password</li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}