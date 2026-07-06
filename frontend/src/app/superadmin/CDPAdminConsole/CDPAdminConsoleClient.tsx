"use client";

import React, { useState } from "react";
import { useRouter } from "next/navigation";
import { Eye, EyeOff, AlertCircle, CheckCircle, Check } from "lucide-react";
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

      if (
        superadminResponse.status === "fulfilled" &&
        superadminResponse.value.ok
      ) {
        const data = (await superadminResponse.value.json()) as LoginResponse;

        localStorage.setItem("superadmin_token", data.token);
        localStorage.setItem("superadmin_email", email);
        setToken(data.token);
        setUserRole("SuperAdmin");
        setSuccess(data.message || "SuperAdmin login successful!");

        toast.success(data.message || "SuperAdmin login successful!", {
          id: loadingToast,
        });

        setTimeout(() => router.push("/superadmin/SuperAdminDashboard"), 1200);
        setIsLoading(false);
        return;
      }

      if (adminResponse.status === "fulfilled" && adminResponse.value.ok) {
        const data = (await adminResponse.value.json()) as LoginResponse;

        localStorage.setItem("admin_token", data.token);
        setToken(data.token);
        setUserRole("Admin");
        setSuccess(data.message || "Admin login successful!");

        toast.success(data.message || "Admin login successful!", {
          id: loadingToast,
        });

        setTimeout(() => router.push("/admin/AdminDashboard"), 1200);
        setIsLoading(false);
        return;
      }

      let errorMsg = "Invalid credentials. Please try again.";

      if (
        superadminResponse.status === "fulfilled" &&
        !superadminResponse.value.ok
      ) {
        try {
          const errData =
            (await superadminResponse.value.json()) as ErrorResponse;
          errorMsg = errData.message || errorMsg;
        } catch {
          errorMsg = `HTTP ${superadminResponse.value.status}: ${superadminResponse.value.statusText}`;
        }
      } else if (
        adminResponse.status === "fulfilled" &&
        !adminResponse.value.ok
      ) {
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

  const handleForgotPassword = () =>
    router.push("/superadmin/SuperadminResetPassword");

  const emailIsValid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  const passwordIsStrong = password.length >= 6;

  return (
    <div className="relative flex min-h-[100svh] items-center justify-center overflow-hidden bg-[#37384f] px-4 py-6 font-sans">
      <div className="relative grid min-h-[620px] w-full max-w-[1120px] overflow-hidden rounded-[30px] bg-[#484962] shadow-[0_28px_70px_rgba(20,22,45,0.48)] lg:grid-cols-2">

        {/* LEFT SECTION */}
        <div className="relative hidden overflow-hidden border-r border-white/10 bg-[#42435c] px-10 py-12 lg:flex lg:flex-col lg:justify-between">
          <div>
            <Image
              width={185}
              height={54}
              src="/images/auth/Phormula.png"
              alt="Phormula"
              priority
              className="h-auto w-[175px] m-auto object-contain"
            />
          </div>

          <div>
            <h1 className="max-w-[300px] text-center m-auto text-3xl font-semibold leading-tight tracking-[-0.03em] text-white">
              Everything you need to manage in one place.
            </h1>

          </div>

          <div className="relative my-10">
            {/* Decorative illustration card */}
            <div className="rounded-[28px] border border-white/10 bg-white/[0.06] p-6 shadow-[0_24px_55px_rgba(20,22,45,0.28)]">
              <div className="mb-5 flex items-center justify-between">
                <div>
                  <p className="text-xs text-white/55">Dashboard preview</p>
                  <h3 className="mt-1 text-xl font-semibold text-white">
                    Manage smarter
                  </h3>
                </div>

                <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-[#31d9e5]/15 text-[#31d9e5]">
                  <CheckCircle className="h-5 w-5" />
                </div>
              </div>

              <div className="space-y-3">
                <div className="rounded-2xl bg-white/[0.07] p-4">
                  <div className="mb-3 h-2 w-24 rounded-full bg-white/25" />
                  <div className="h-2 w-full rounded-full bg-[#31d9e5]/80" />
                </div>

                <div className="grid grid-cols-2 gap-3">
                  <div className="rounded-2xl bg-white/[0.07] p-4">
                    <p className="text-[11px] text-white/50">Products</p>
                    <p className="mt-2 text-2xl font-semibold text-white">248</p>
                  </div>

                  <div className="rounded-2xl bg-white/[0.07] p-4">
                    <p className="text-[11px] text-white/50">Orders</p>
                    <p className="mt-2 text-2xl font-semibold text-white">1.2k</p>
                  </div>
                </div>

                <div className="rounded-2xl bg-white/[0.07] p-4">
                  <div className="flex items-center gap-3">
                    <div className="h-9 w-9 rounded-xl bg-[#31d9e5]/20" />
                    <div className="flex-1">
                      <div className="mb-2 h-2 w-28 rounded-full bg-white/25" />
                      <div className="h-2 w-40 rounded-full bg-white/15" />
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* Glow */}
            <div className="absolute -right-10 -top-10 h-32 w-32 rounded-full bg-[#31d9e5]/20 blur-3xl" />
            <div className="absolute -bottom-10 -left-10 h-36 w-36 rounded-full bg-white/10 blur-3xl" />
          </div>


        </div>

        {/* RIGHT SECTION */}
        <div className="relative flex items-center justify-center px-5 py-10 sm:px-8 lg:px-12">
          <div className="relative z-10 flex w-full max-w-[390px] flex-col">
            <div className="mb-8 text-center sm:mb-9 lg:hidden">
              <Image
                width={185}
                height={54}
                src="/Logo_phormula.png"
                alt="Phormula"
                priority
                className="mx-auto h-auto w-[165px] object-contain sm:w-[185px]"
              />

              <p className="mt-3 text-[11px] font-medium tracking-wide text-white/85 sm:text-xs">
                Fast & Easy Product Management
              </p>
            </div>

            <div className="mb-10 text-center">
              <h2 className="text-[24px] font-semibold tracking-[-0.03em] text-white sm:text-[26px]">
                Welcome Back Super Admin!
              </h2>

              <p className="mx-auto mt-5 max-w-[360px] text-center text-sm leading-6 text-white/65">
                Access your admin console to track products, inventory, forecasts,
                purchase orders, and business performance with ease.
              </p>
            </div>

            <div className="mx-auto w-full max-w-[335px]">
              {/* Email */}
              <div className="mb-5">
                <label
                  htmlFor="email"
                  className="mb-2 block text-[13px] font-medium text-white"
                >
                  Email
                </label>

                <div
                  className={`relative flex h-11 items-center rounded-xl border bg-white/[0.06] transition ${email
                    ? emailIsValid
                      ? "border-[#30dce7]"
                      : "border-red-300"
                    : "border-white/10"
                    } focus-within:border-[#30dce7] focus-within:ring-4 focus-within:ring-[#30dce7]/15`}
                >
                  <input
                    id="email"
                    type="email"
                    value={email}
                    placeholder="emilie.smith@gmail.com"
                    onChange={(e) => setEmail(e.target.value)}
                    disabled={isLoading}
                    className="login-input h-full w-full rounded-xl bg-transparent px-4 pr-10 text-[13px] font-medium text-white outline-none placeholder:text-white/45 disabled:opacity-60"
                  />


                </div>

                <div className="min-h-[16px]">
                  {email && !emailIsValid && (
                    <p className="mt-1 text-[10px] font-semibold text-red-200">
                      Please enter a valid email.
                    </p>
                  )}
                </div>
              </div>

              {/* Password */}
              <div className="mb-11">
                <label
                  htmlFor="password"
                  className="mb-2 block text-[13px] font-medium text-white"
                >
                  Password
                </label>

                <div
                  className={`relative flex h-11 items-center rounded-xl border bg-white/[0.06] transition ${password
                    ? passwordIsStrong
                      ? "border-[#30dce7]"
                      : "border-white/10"
                    : "border-white/10"
                    } focus-within:border-[#30dce7] focus-within:ring-4 focus-within:ring-[#30dce7]/15`}
                >
                  <input
                    id="password"
                    type={showPassword ? "text" : "password"}
                    value={password}
                    placeholder="••••••••••"
                    onChange={(e) => setPassword(e.target.value)}
                    disabled={isLoading}
                    className="login-input h-full w-full rounded-xl bg-transparent px-4 pr-10 text-[13px] font-medium text-white outline-none placeholder:text-white/45 disabled:opacity-60"
                  />

                  <button
                    type="button"
                    onClick={() => setShowPassword((prev) => !prev)}
                    disabled={isLoading}
                    className="absolute right-3 top-1/2 flex h-6 w-6 -translate-y-1/2 items-center justify-center rounded-full text-[#30dce7] transition hover:bg-white/10 hover:text-white disabled:cursor-not-allowed disabled:opacity-60"
                    aria-label={showPassword ? "Hide password" : "Show password"}
                  >
                    {showPassword ? (
                      <EyeOff className="h-4 w-4" />
                    ) : (
                      <Eye className="h-4 w-4" />
                    )}
                  </button>
                </div>
              </div>

              {/* {error && (
                <div className="mb-4 flex items-start gap-2 rounded-xl border border-red-300/25 bg-red-500/10 px-3 py-2.5">
                  <AlertCircle className="mt-0.5 h-4 w-4 shrink-0 text-red-200" />
                  <span className="text-xs leading-5 text-red-100">{error}</span>
                </div>
              )} */}

              {/* {success && (
                <div className="mb-4 flex items-start gap-2 rounded-xl border border-emerald-300/25 bg-emerald-500/10 px-3 py-2.5">
                  <CheckCircle className="mt-0.5 h-4 w-4 shrink-0 text-emerald-200" />

                  <div className="flex-1">
                    <span className="text-xs leading-5 text-emerald-100">
                      {success}
                    </span>

                    {userRole && (
                      <div className="mt-1 text-[11px] text-emerald-200">
                        Logged in as:{" "}
                        <span className="font-semibold">{userRole}</span>
                      </div>
                    )}
                  </div>
                </div>
              )} */}

              <button
                type="submit"
                onClick={handleLogin}
                disabled={isLoading}
                className="h-11 w-full rounded-md bg-[#31d9e5] text-sm font-semibold text-[#303247] shadow-[0_10px_22px_rgba(20,220,230,0.20)] transition hover:-translate-y-0.5 hover:bg-[#28cbd6] disabled:cursor-not-allowed disabled:translate-y-0 disabled:opacity-60"
              >
                {isLoading ? "Signing in..." : "Sign in"}
              </button>

              <div className="mt-7 text-center">
                <button
                  type="button"
                  onClick={handleForgotPassword}
                  disabled={isLoading}
                  className="text-xs font-medium text-white/95 transition hover:text-[#31d9e5] disabled:opacity-60 sm:text-[13px]"
                >
                  Forgot My Password
                </button>
              </div>
            </div>

            <div className="mt-20 flex justify-center gap-3 text-[11px] text-white/85 sm:mt-24">
              <button type="button" className="transition hover:text-[#31d9e5]">
                Term of use
              </button>

              <span>|</span>

              <button type="button" className="transition hover:text-[#31d9e5]">
                Privacy policy
              </button>
            </div>
          </div>
        </div>
      </div>
      <style jsx global>{`
  input.login-input:-webkit-autofill,
  input.login-input:-webkit-autofill:hover,
  input.login-input:-webkit-autofill:focus,
  input.login-input:-webkit-autofill:active {
    -webkit-box-shadow: 0 0 0 1000px rgba(255, 255, 255, 0.06) inset !important;
    -webkit-text-fill-color: #ffffff !important;
    caret-color: #ffffff !important;
    transition: background-color 9999s ease-in-out 0s;
  }
`}</style>
    </div>
  );
}