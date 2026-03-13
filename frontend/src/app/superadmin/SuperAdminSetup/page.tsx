"use client";

import React, { useState } from "react";
import { Eye, EyeOff, Mail, Lock, Shield } from "lucide-react";
import { useRouter } from "next/navigation";
import { toast } from "sonner";
import Image from "next/image";
import { Check } from "lucide-react";

type ApiError = {
  message?: string;
};

export default function SuperAdminSetupPage() {
  const [email, setEmail] = useState<string>("");
  const [otp, setOtp] = useState<string>("");
  const [password, setPassword] = useState<string>("");
  const [confirmPassword, setConfirmPassword] = useState<string>("");
  const [step, setStep] = useState<1 | 2>(1);
  const [loading, setLoading] = useState<boolean>(false);
  const [showPassword, setShowPassword] = useState<boolean>(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState<boolean>(false);

  const router = useRouter();

  const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:5000";

  // ---- Send OTP ----
  const handleEmailSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setLoading(true);

    try {
      const response = await fetch(`${API_BASE}/superadmin_setup_otp`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email }),
      });

      const data: ApiError = await response.json().catch(() => ({}));

      if (!response.ok) throw new Error(data.message || "Failed to send OTP");

      toast.success("OTP sent successfully to your email!");
      window.setTimeout(() => setStep(2), 1500);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Failed to send OTP. Please try again.";
      toast.error(msg);
    } finally {
      setLoading(false);
    }
  };

  // ---- Complete Setup ----
  const handleSetupComplete = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();

    if (!otp.trim()) return toast.warning("Please enter the OTP");
    if (password.length < 8) return toast.warning("Password must be at least 8 characters long");
    if (password !== confirmPassword) return toast.warning("Passwords do not match");

    setLoading(true);

    try {
      const response = await fetch(`${API_BASE}/superadmin_setup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, otp: otp.trim(), password }),
      });

      const data: ApiError = await response.json().catch(() => ({}));

      if (!response.ok) throw new Error(data.message || "Setup failed");

      toast.success("SuperAdmin setup completed successfully!");
      window.setTimeout(() => router.push("/superadmin/SuperAdminDashboard"), 800);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Setup failed. Please try again.";
      toast.error(msg);
    } finally {
      setLoading(false);
    }
  };

  const resetToStep1 = () => {
    setStep(1);
    setOtp("");
    setPassword("");
    setConfirmPassword("");
  };

  return (
    <div className="min-h-screen flex items-center justify-center px-4 sm:px-6 lg:px-8 bg-[#EFEFEF]">
      <div className="absolute top-6 left-6">
          <Image
            width={220}
            height={40}
            src="/images/logo/Logo_Phormula.png"
            alt="Phormula"
            priority
            className="w-[150px] xl:w-[180px] 2xl:w-[220px]"
          />
        </div>
      <div className="w-full max-w-md sm:max-w-lg md:max-w-xl bg-white/95 backdrop-blur border border-t-8 border-t-[#5EA68E] rounded-2xl shadow-2xl p-6 sm:p-8">
        {/* Header */}
        <div className="text-center mb-8">
          <div className="w-14 h-14 sm:w-16 sm:h-16 mx-auto mb-4 rounded-full bg-gradient-to-br from-[#5EA68E] to-[#1f5274] flex items-center justify-center text-white">
            <Shield size={28} />
          </div>
          <h1 className="text-xl sm:text-2xl font-semibold text-gray-800 mb-1">
            SuperAdmin Setup
          </h1>
          <p className="text-xs sm:text-sm text-gray-500">
            {step === 1 ? "Enter your email to get started" : "Complete your account setup"}
          </p>
        </div>

        {/* Progress */}
       <div className="flex flex-col items-center mb-8">

  {/* Circles */}
  <div className="flex items-center">
    
    {/* STEP 1 */}
    <div
      className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-semibold ${
        step >= 1
          ? "bg-gradient-to-br from-[#5EA68E] to-[#1f5274] text-white"
          : "bg-gray-200 text-gray-400"
      }`}
    >
      {step > 1 ? <Check className="w-4 h-4" /> : "1"}
    </div>

    {/* LINE */}
    <div
      className={`w-14 h-[2px] mx-3 ${
        step >= 2
          ? "bg-gradient-to-r from-[#5EA68E] to-[#1f5274]"
          : "bg-gray-200"
      }`}
    />

    {/* STEP 2 */}
    <div
      className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-semibold ${
        step >= 2
          ? "bg-gradient-to-br from-[#5EA68E] to-[#1f5274] text-white"
          : "bg-gray-200 text-gray-400"
      }`}
    >
      2
    </div>
  </div>

  {/* Labels */}
  <div className="flex items-center mt-3 text-sm font-medium">
    
    <div className="w-[80px] text-center text-[#1f5274]">
      {step === 1 ? "Email" : "Email & OTP"}
    </div>

    <div className="w-14"></div>

    <div className="w-[100px] text-center text-[#1f5274]">
      {step === 1 ? "Verify OTP" : "Set Password"}
    </div>

  </div>
</div>

        {/* Step 1 */}
        {step === 1 && (
          <form onSubmit={handleEmailSubmit}>
            <div className="mb-5">
              <label className="block text-sm font-medium text-gray-700 mb-2">Email Address*</label>
              <div className="relative">
                <Mail className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" size={18} />
                <input
                  type="email"
                  placeholder="admin@company.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  required
                  className="w-full pl-10 pr-4 py-3 rounded-lg border-2 border-gray-200 text-sm sm:text-base focus:border-[#5EA68E] focus:ring-4 focus:ring-[#5EA68E]/20 outline-none transition"
                />
              </div>
            </div>

            <button
              type="submit"
              disabled={loading || !email.trim()}
              className={`w-full py-3 text-sm sm:text-base font-semibold text-[#F8EDCE] rounded-lg transition-all duration-300 ${
                loading || !email.trim()
                  ? "cursor-not-allowed opacity-60 bg-[#37455F]"
                    : "bg-[#37455F] hover:shadow-lg"
              }`}
            >
              {loading ? "Sending OTP..." : "Send OTP"}
            </button>
          </form>
        )}

        {/* Step 2 */}
        {step === 2 && (
          <form onSubmit={handleSetupComplete}>
            <div className="mb-5">
              <label className="block text-sm font-medium text-gray-700 mb-2">OTP Code</label>
              <input
                type="text"
                placeholder="Enter 6-digit OTP"
                value={otp}
                onChange={(e) => setOtp(e.target.value.replace(/\D/g, "").slice(0, 6))}
                required
                className="w-full py-3 text-left px-3 tracking-wider rounded-lg border-2 border-gray-200 text-sm sm:text-base focus:border-[#5EA68E] focus:ring-4 focus:ring-[#5EA68E]/20 font-semibold outline-none transition"
              />
            </div>

            <div className="mb-5">
              <label className="block text-sm font-medium text-gray-700 mb-2">Password</label>
              <div className="relative">
                <input
                  type={showPassword ? "text" : "password"}
                  placeholder="Enter password (min 8 characters)"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                  className="w-full px-3 py-3 rounded-lg border-2 border-gray-200 text-sm sm:text-base focus:border-[#5EA68E] focus:ring-4 focus:ring-[#5EA68E]/20 outline-none transition"
                />
                <button
                  type="button"
                  onClick={() => setShowPassword((v) => !v)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-[#1f5274]"
                >
                  {showPassword ? <EyeOff size={18} /> : <Eye size={18} />}
                </button>
              </div>
            </div>

            <div className="mb-6">
              <label className="block text-sm font-medium text-gray-700 mb-2">Confirm Password</label>
              <div className="relative">
                <input
                  type={showConfirmPassword ? "text" : "password"}
                  placeholder="Confirm your password"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  required
                  className="w-full px-3 py-3 rounded-lg border-2 border-gray-200 text-sm sm:text-base focus:border-[#5EA68E] focus:ring-4 focus:ring-[#5EA68E]/20 outline-none transition"
                />
                <button
                  type="button"
                  onClick={() => setShowConfirmPassword((v) => !v)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-[#1f5274]"
                >
                  {showConfirmPassword ? <EyeOff size={18} /> : <Eye size={18} />}
                </button>
              </div>
            </div>

            <div className="flex flex-col sm:flex-row gap-3">
              <button
                type="button"
                onClick={resetToStep1}
                className="w-full sm:w-1/2 py-3 text-sm sm:text-base font-semibold border-2 border-[#37455F] text-[#37455F] rounded-lg transition-all hover:bg-[#5EA68E] hover:text-white"
              >
                Back
              </button>

              <button
                type="submit"
                disabled={loading || !otp.trim() || !password || !confirmPassword}
                className={`w-full sm:w-1/2 py-3 text-sm sm:text-base font-semibold text-[#F8EDCE] rounded-lg transition-all duration-300 ${
                  loading || !otp.trim() || !password || !confirmPassword
                    ? "cursor-not-allowed opacity-60 bg-[#37455F]"
                    : "bg-[#37455F] hover:shadow-lg"
                }`}
              >
                {loading ? "Setting up..." : "Complete Setup"}
              </button>
            </div>
          </form>
        )}

        <div className="mt-8 text-center border-t border-gray-200 pt-4 text-xs sm:text-sm text-gray-400">
          This is a one-time setup process for SuperAdmin access.
        </div>
      </div>
    </div>
  );
}