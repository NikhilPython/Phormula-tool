"use client";

import React, { useState } from "react";
import { useRouter } from "next/navigation";
import { Eye, EyeOff, Lock, Mail, AlertCircle, CheckCircle } from "lucide-react";
import { toast } from "sonner";
import Image from "next/image";

type LoginResponse = {
  token: string;
  message?: string;
};

type ErrorResponse = {
  message?: string;
};

export default function CDPAdminConsolePage() {
  const [email, setEmail] = useState<string>("");
  const [password, setPassword] = useState<string>("");
  const [showPassword, setShowPassword] = useState<boolean>(false);
  const [error, setError] = useState<string>("");
  const [success, setSuccess] = useState<string>("");
  const [token, setToken] = useState<string>("");
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [userRole, setUserRole] = useState<string>("");

  const router = useRouter();

  const handleLogin = async (e: React.MouseEvent<HTMLButtonElement>) => {
    e.preventDefault();
    setError("");
    setSuccess("");
    setToken("");
    setIsLoading(true);

    // Basic validation
    if (!email || !password) {
      toast.warning("Please fill in all fields");
      setIsLoading(false);
      return;
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      toast.warning("Please enter a valid email address");
      setIsLoading(false);
      return;
    }

    try {
      const loadingToast = toast.loading("Authenticating...");

      const [superadminResponse, adminResponse] = await Promise.allSettled([
        fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin_login`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email, password }),
        }),
        fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/admin_login`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email, password }),
        }),
      ]);

      // SuperAdmin login
      if (superadminResponse.status === "fulfilled" && superadminResponse.value.ok) {
        const data = (await superadminResponse.value.json()) as LoginResponse;

        localStorage.setItem("superadmin_token", data.token);
        setToken(data.token);
        setUserRole("SuperAdmin");

        toast.success(data.message || "SuperAdmin login successful!", { id: loadingToast });

        setTimeout(() => router.push("/superadmin/SuperAdminDashboard"), 1200);
        setIsLoading(false);
        return;
      }

      // Admin login
      if (adminResponse.status === "fulfilled" && adminResponse.value.ok) {
        const data = (await adminResponse.value.json()) as LoginResponse;

        localStorage.setItem("admin_token", data.token);
        setToken(data.token);
        setUserRole("Admin");

        toast.success(data.message || "Admin login successful!", { id: loadingToast });

        setTimeout(() => router.push("/admin/AdminDashboard"), 1200);
        setIsLoading(false);
        return;
      }

      // Both failed — parse error
      let errorMsg = "Invalid credentials. Please try again.";

      if (superadminResponse.status === "fulfilled" && !superadminResponse.value.ok) {
        try {
          const errData = (await superadminResponse.value.json()) as ErrorResponse;
          errorMsg = errData.message || errorMsg;
        } catch {
          errorMsg = `HTTP ${superadminResponse.value.status}: ${superadminResponse.value.statusText}`;
        }
      } else if (adminResponse.status === "fulfilled" && !adminResponse.value.ok) {
        try {
          const errData = (await adminResponse.value.json()) as ErrorResponse;
          errorMsg = errData.message || errorMsg;
        } catch {
          errorMsg = `HTTP ${adminResponse.value.status}: ${adminResponse.value.statusText}`;
        }
      }

      toast.error(errorMsg, { id: loadingToast });
      setError(errorMsg);
    } catch (err) {
      const msg =
        err instanceof Error &&
        err.name === "TypeError" &&
        err.message.toLowerCase().includes("fetch")
          ? "Cannot connect to server. Please check if the backend is running on localhost:5000"
          : err instanceof Error
            ? err.message
            : "Network error. Please check your connection.";

      toast.error(msg);
      setError(msg);
    } finally {
      setIsLoading(false);
    }
  };

  const handleForgotPassword = () => router.push("/superadmin/SuperadminResetPassword");

  return (
    <div className="min-h-[100svh] flex items-center justify-center px-4 py-6 sm:px-6 lg:px-8 font-sans bg-[#EFEFEF]">
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
      <div className="w-full max-w-sm sm:max-w-md md:max-w-lg border border-t-8 border-t-[#5EA68E]  rounded-2xl bg-white/95 shadow-2xl backdrop-blur px-6 py-7 sm:px-8 sm:py-9 md:px-10 md:py-10">
        {/* Header */}
        <div className="text-center mb-6 sm:mb-8">
          <div className="inline-flex items-center justify-center w-14 h-14 sm:w-16 sm:h-16 rounded-full  mb-3 sm:mb-4">
            <Lock className="w-6 h-6 sm:w-8 sm:h-8 text-white" />
            <Image
      width={220}
      height={40}
      src="/Favicon2.png"
      alt="Phormula"
      priority
      className="w-[150px] xl:w-[180px] 2xl:w-[220px]"
    />
          </div>
          <h2 className="text-2xl sm:text-3xl md:text-4xl font-bold text-gray-900 m-0">
            SuperAdmin Portal
          </h2>
          <p className="text-xs sm:text-sm md:text-base text-gray-600 m-0 mt-1">
            Sign in to access the CDP dashboard
          </p>
        </div>

        {/* Email */}
        <div className="mb-5 sm:mb-6">
          <label htmlFor="email" className="block text-xs sm:text-sm font-medium text-gray-700 mb-2">
            Email Address
          </label>
          <div className="relative">
            <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 sm:w-5 sm:h-5 text-gray-400" />
            <input
              id="email"
              type="email"
              placeholder="Enter your email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="w-full text-sm sm:text-base pl-9 sm:pl-10 pr-3 sm:pr-4 py-2.5 sm:py-3 border border-gray-300 rounded-lg bg-gray-50 transition focus:outline-none focus:border-[#5EA68E] focus:bg-white focus:ring-4 focus:ring-[#5EA68E]/30 disabled:opacity-50"
              required
              disabled={isLoading}
            />
          </div>
        </div>

        {/* Password */}
        <div className="mb-5 sm:mb-6">
          <label htmlFor="password" className="block text-xs sm:text-sm font-medium text-gray-700 mb-2">
            Password
          </label>
          <div className="relative">
            <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 sm:w-5 sm:h-5 text-gray-400" />
            <input
              id="password"
              type={showPassword ? "text" : "password"}
              placeholder="Enter your password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full text-sm sm:text-base pl-9 sm:pl-10 pr-10 sm:pr-12 py-2.5 sm:py-3 border border-gray-300 rounded-lg bg-gray-50 transition focus:outline-none focus:border-[#5EA68E] focus:bg-white focus:ring-4 focus:ring-[#5EA68E]/30 disabled:opacity-50"
              required
              disabled={isLoading}
            />
            <button
              type="button"
              onClick={() => setShowPassword(!showPassword)}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600 disabled:cursor-not-allowed"
              disabled={isLoading}
              aria-label={showPassword ? "Hide password" : "Show password"}
            >
              {showPassword ? <EyeOff className="w-5 h-5" /> : <Eye className="w-5 h-5" />}
            </button>
          </div>
        </div>

        {/* Error */}
        {error && (
          <div className="flex items-center gap-2 p-3 mb-4 rounded-lg border border-red-200 bg-red-50">
            <AlertCircle className="w-5 h-5 text-red-500 shrink-0" />
            <span className="text-xs sm:text-sm text-red-700">{error}</span>
          </div>
        )}

        {/* Success */}
        {success && (
          <div className="flex items-center gap-2 p-3 mb-4 rounded-lg border border-green-200 bg-green-50">
            <CheckCircle className="w-5 h-5 text-green-500 shrink-0" />
            <div className="flex-1">
              <span className="text-xs sm:text-sm text-green-700">{success}</span>
              {userRole && (
                <div className="text-[11px] sm:text-xs text-green-600 mt-1">
                  Logged in as: <span className="font-semibold">{userRole}</span>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Submit */}
        <button
          type="submit"
          onClick={handleLogin}
          disabled={isLoading}
          className="w-full inline-flex items-center justify-center gap-2 py-2.5 sm:py-3 px-4 text-sm sm:text-base font-semibold text-[#F8EDCE] rounded-lg bg-[#37455F] transition disabled:opacity-50 disabled:cursor-not-allowed hover:scale-[1.02]"
        >
          {isLoading ? <>Signing In...</> : "Sign In"}
        </button>

        {/* Forgot Password */}
        <div className="text-center mt-4">
          <button
            type="button"
            onClick={handleForgotPassword}
            disabled={isLoading}
            className="text-xs sm:text-sm font-medium text-[#1f5274] hover:text-[#19455f] disabled:opacity-50"
          >
            Forgot your password?
          </button>
        </div>
      </div>
    </div>
  );
}