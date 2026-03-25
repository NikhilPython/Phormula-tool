"use client";

import React, { useState } from "react";
import { useRouter } from "next/navigation";
import {
  Mail,
  Lock,
  Key,
  ArrowLeft,
  Eye,
  EyeOff,
  CheckCircle,
  AlertCircle,
  Loader2,
} from "lucide-react";
import Image from "next/image";

type MessageType = "" | "success" | "error";

type ApiResponse = {
  message?: string;
};

export default function SuperadminResetPasswordPage() {
  const [step, setStep] = useState<1 | 2>(1);
  const [email, setEmail] = useState<string>("");
  const [otp, setOtp] = useState<string>("");
  const [newPassword, setNewPassword] = useState<string>("");
  const [confirmPassword, setConfirmPassword] = useState<string>("");
  const [message, setMessage] = useState<string>("");
  const [messageType, setMessageType] = useState<MessageType>("");
  const [loading, setLoading] = useState<boolean>(false);
  const [showPassword, setShowPassword] = useState<boolean>(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState<boolean>(false);
  const [hovered, setHovered] = useState<boolean>(false);

  const router = useRouter();

  const validateEmail = (emailValue: string): boolean => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(emailValue);
  };

  const validatePassword = (password: string): boolean => password.length >= 8;

  const setSuccessMessage = (msg: string) => {
    setMessage(msg);
    setMessageType("success");
  };

  const setErrorMessage = (msg: string) => {
    setMessage(msg);
    setMessageType("error");
  };

  const requestOtp = async () => {
    if (!email.trim()) {
      setErrorMessage("Please enter your email address");
      setTimeout(() => setErrorMessage(""), 2000);
      return;
    }
    if (!validateEmail(email)) {
      setErrorMessage("Please enter a valid email address");
      setTimeout(() => setErrorMessage(""), 2000);
      return;
    }

    setLoading(true);
    setMessage("");
    setErrorMessage("");

    try {
      const requestBody = JSON.stringify({ email });
      const headers = { "Content-Type": "application/json" };

      const superadminRequest = fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin_reset_otp`, {
        method: "POST",
        headers,
        body: requestBody,
      });

      const adminRequest = fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/admin_reset_otp`, {
        method: "POST",
        headers,
        body: requestBody,
      });

      const [superadminRes, adminRes] = await Promise.all([superadminRequest, adminRequest]);

      const [superadminData, adminData] = (await Promise.all([
        superadminRes.json(),
        adminRes.json(),
      ])) as [ApiResponse, ApiResponse];

      if (superadminRes.ok || adminRes.ok) {
        setSuccessMessage("OTP has been sent to your email address");
        setStep(2);
        setTimeout(() => setMessage(""), 3000);
      } else {
        const errors: string[] = [];
        if (!superadminRes.ok)
          errors.push(superadminData.message || "Superadmin OTP request failed");
        if (!adminRes.ok) errors.push(adminData.message || "Admin OTP request failed");
        setErrorMessage(errors.join(". "));
      }
    } catch {
      setErrorMessage("Network error. Please check your connection and try again.");
    } finally {
      setLoading(false);
    }
  };

  const resetPassword = async () => {
    if (!otp.trim()) {
      setErrorMessage("Please enter the OTP");
      setTimeout(() => setErrorMessage(""), 2000);
      return;
    }
    if (!newPassword.trim()) {
      setErrorMessage("Please enter a new password");
      setTimeout(() => setErrorMessage(""), 2000);
      return;
    }
    if (!validatePassword(newPassword)) {
      setErrorMessage("Password must be at least 8 characters long");
      setTimeout(() => setErrorMessage(""), 2000);
      return;
    }
    if (newPassword !== confirmPassword) {
      setErrorMessage("Passwords do not match");
      setTimeout(() => setErrorMessage(""), 2000);
      return;
    }

    setLoading(true);
    setMessage("");
    setErrorMessage("");

    try {
      const requestBody = JSON.stringify({
        email,
        otp,
        new_password: newPassword,
      });

      const headers = { "Content-Type": "application/json" };

      const superadminRequest = fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin_reset_password`, {
        method: "POST",
        headers,
        body: requestBody,
      });

      const adminRequest = fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/admin_reset_password`, {
        method: "POST",
        headers,
        body: requestBody,
      });

      const [superadminRes, adminRes] = await Promise.all([superadminRequest, adminRequest]);

      const [superadminData, adminData] = (await Promise.all([
        superadminRes.json(),
        adminRes.json(),
      ])) as [ApiResponse, ApiResponse];

      if (superadminRes.ok || adminRes.ok) {
        setSuccessMessage("Password reset successful! You can now login with your new password.");
        router.push("/superadmin/CDPAdminConsole");

        setTimeout(() => {
          setStep(1);
          setEmail("");
          setOtp("");
          setNewPassword("");
          setConfirmPassword("");
          setMessage("");
        }, 3000);
      } else {
        const errors: string[] = [];
        if (!superadminRes.ok)
          errors.push(superadminData.message || "Superadmin password reset failed");
        if (!adminRes.ok) errors.push(adminData.message || "Admin password reset failed");
        setErrorMessage(errors.join(". "));
      }
    } catch {
      setErrorMessage("Network error. Please check your connection and try again.");
    } finally {
      setLoading(false);
    }
  };

  const goBackToEmailStep = () => {
    setStep(1);
    setOtp("");
    setNewPassword("");
    setConfirmPassword("");
    setMessage("");
  };

  const resendOtp = () => requestOtp();

  return (
    <div className="min-h-screen flex items-center justify-center p-4 font-sans bg-[#EFEFEF]">
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
      <div className="w-full max-w-md p-8 rounded-2xl shadow-2xl bg-white backdrop-blur border border-t-8 border-t-[#5EA68E]">
        <div className="text-center mb-8">
          <div className="mx-auto mb-4 w-16 h-16 rounded-full bg-gradient-to-br from-[#5EA68E] to-[#1f5274] flex items-center justify-center">
            <Lock size={32} color="#fff" />
          </div>
          <h1 className="text-2xl font-bold bg-gradient-to-br from-[#1f5274] to-[#5EA68E] bg-clip-text text-transparent mb-2">
            Reset Password
          </h1>
          <p className="text-[#60a68e] text-sm">
            {step === 1 ? "Enter your email to receive an OTP" : "Enter OTP and set new password"}
          </p>
        </div>

        <div className="flex items-center justify-center mb-8">
          <div
            className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-medium ${
              step >= 1
                ? "bg-gradient-to-br from-[#1f5274] to-[#5EA68E] text-white"
                : "bg-[rgba(204,229,221,0.5)] text-gray-500"
            }`}
          >
            1
          </div>
          <div
            className={`w-12 h-1 mx-2 ${
              step >= 2
                ? "bg-gradient-to-r from-[#5EA68E] to-[#1f5274]"
                : "bg-[linear-gradient(90deg,rgba(96,166,142,0.3),rgba(31,82,116,0.3))]"
            }`}
          ></div>
          <div
            className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-medium ${
              step >= 2
                ? "bg-gradient-to-br from-[#1f5274] to-[#5EA68E] text-white"
                : "bg-[rgba(204,229,221,0.5)] text-gray-500"
            }`}
          >
            2
          </div>
        </div>

        {step === 1 && (
          <div>
            <div className="mb-6">
              <label className="block text-sm font-medium text-[#1f5274] mb-2">
                Email Address
              </label>
              <div className="relative">
                <Mail
                  size={20}
                  className="absolute left-3 top-1/2 -translate-y-1/2 text-[#60a68e]"
                />
                <input
                  type="email"
                  placeholder="Enter your email address"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="w-full pl-10 pr-4 py-3 rounded-lg text-base border border-[#5EA68E] outline-none transition bg-[rgba(96,166,142,0.05)] focus:outline-none focus:border-[#5EA68E] focus:bg-white focus:ring-4 focus:ring-[#5EA68E]/30"
                  required
                />
              </div>
            </div>

            <button
              onClick={requestOtp}
              disabled={loading}
              onMouseEnter={() => setHovered(true)}
              onMouseLeave={() => setHovered(false)}
              className="w-full inline-flex items-center justify-center gap-2 py-2.5 sm:py-3 px-4 text-sm sm:text-base font-semibold text-[#F8EDCE] rounded-lg bg-[#37455F] transition disabled:opacity-50 disabled:cursor-not-allowed hover:scale-[1.02]"
            >
              {loading ? (
                <>
                  <Loader2 size={20} className="mr-2 animate-spin" />
                  Sending OTP...
                </>
              ) : (
                "Send OTP"
              )}
            </button>
          </div>
        )}

        {step === 2 && (
          <div>
            <button
              onClick={goBackToEmailStep}
              className="flex items-center text-[#60a68e] text-sm font-medium mb-4 hover:opacity-80"
            >
              <ArrowLeft size={16} className="mr-1" />
              Back to email
            </button>

            <div className="mb-6">
              <label className="block text-sm font-medium text-[#1f5274] mb-2">OTP Code</label>
              <div className="relative">
                <Key
                  size={20}
                  className="absolute left-3 top-1/2 -translate-y-1/2 text-[#60a68e]"
                />
                <input
                  type="text"
                  placeholder="Enter 6-digit OTP"
                  value={otp}
                  onChange={(e) => setOtp(e.target.value.replace(/\D/g, "").slice(0, 6))}
                  className="w-full pl-10 pr-4 py-3 rounded-lg text-lg border border-[#5EA68E] outline-none transition text-center tracking-wider bg-[rgba(96,166,142,0.05)] focus:border-[#1f5274] focus:ring-4 focus:ring-[#1f5274]/20 focus:bg-[#1f5274]/5"
                  maxLength={6}
                  required
                />
              </div>
              <button onClick={resendOtp} className="text-sm text-[#60a68e] mt-2 hover:opacity-80">
                Didn&apos;t receive OTP? Resend
              </button>
            </div>

            <div className="mb-6">
              <label className="block text-sm font-medium text-[#1f5274] mb-2">New Password</label>
              <div className="relative">
                <Lock
                  size={20}
                  className="absolute left-3 top-1/2 -translate-y-1/2 text-[#60a68e]"
                />
                <input
                  type={showPassword ? "text" : "password"}
                  placeholder="Enter new password"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  className="w-full pl-10 pr-10 py-3 rounded-lg text-base border border-[#5EA68E] outline-none transition bg-[rgba(96,166,142,0.05)] focus:border-[#1f5274] focus:ring-4 focus:ring-[#1f5274]/20 focus:bg-[#1f5274]/5"
                  minLength={8}
                  required
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-[#60a68e] hover:text-[#1f5274]"
                >
                  {showPassword ? <EyeOff size={20} /> : <Eye size={20} />}
                </button>
              </div>
            </div>

            <div className="mb-6">
              <label className="block text-sm font-medium text-[#1f5274] mb-2">
                Confirm New Password
              </label>
              <div className="relative">
                <Lock
                  size={20}
                  className="absolute left-3 top-1/2 -translate-y-1/2 text-[#60a68e]"
                />
                <input
                  type={showConfirmPassword ? "text" : "password"}
                  placeholder="Confirm new password"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  className="w-full pl-10 pr-10 py-3 rounded-lg text-base border border-[#5EA68E] outline-none transition bg-[rgba(96,166,142,0.05)] focus:border-[#1f5274] focus:ring-4 focus:ring-[#1f5274]/20 focus:bg-[#1f5274]/5"
                  minLength={8}
                  required
                />
                <button
                  type="button"
                  onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-[#60a68e] hover:text-[#1f5274]"
                >
                  {showConfirmPassword ? <EyeOff size={20} /> : <Eye size={20} />}
                </button>
              </div>
            </div>

            <button
              onClick={resetPassword}
              disabled={loading}
              className="w-full inline-flex items-center justify-center gap-2 py-2.5 sm:py-3 px-4 text-sm sm:text-base font-semibold text-[#F8EDCE] rounded-lg bg-[#37455F] transition disabled:opacity-50 disabled:cursor-not-allowed hover:scale-[1.02]"
            >
              {loading ? (
                <>
                  <Loader2 size={20} className="mr-2 animate-spin" />
                  Resetting Password...
                </>
              ) : (
                "Reset Password"
              )}
            </button>
          </div>
        )}

        {message && (
          <div
            className={`mt-6 p-4 rounded-lg flex items-center text-sm border ${
              messageType === "success"
                ? "border-[rgba(96,166,142,0.3)] text-[#1f5274] bg-[linear-gradient(135deg,rgba(96,166,142,0.1),rgba(31,82,116,0.1))]"
                : "border-[rgba(220,38,38,0.3)] text-red-600 bg-[linear-gradient(135deg,rgba(239,68,68,0.1),rgba(220,38,38,0.1))]"
            }`}
          >
            {messageType === "success" ? (
              <CheckCircle size={20} className="mr-2 shrink-0" />
            ) : (
              <AlertCircle size={20} className="mr-2 shrink-0" />
            )}
            <p>{message}</p>
          </div>
        )}

        <div className="mt-8 text-center text-sm text-gray-500">
          <p>Need help? Contact your system administrator</p>
        </div>
      </div>
    </div>
  );
}