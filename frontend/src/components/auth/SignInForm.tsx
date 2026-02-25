"use client";

import Checkbox from "@/components/form/input/Checkbox";
import Input from "@/components/form/input/InputField";
import Label from "@/components/form/Label";
import Button from "@/components/ui/button/Button";
import { EyeCloseIcon, EyeIcon } from "@/icons";
import Link from "next/link";
import React, { useEffect, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { useAppDispatch, useAppSelector } from "@/lib/store";
import {
  setAuthError,
  setAuthLoading,
  setCredentials,
  setUser,
} from "@/lib/features/auth/authSlice";
import { useLoginMutation, useMemberLoginMutation } from "@/lib/api/authApi"; // ✅
import { API_BASE } from "@/config/env";
import ForgotPasswordModal from "./ForgotPasswordModal";
import { auth, googleProvider } from "@/lib/firebase/firebase";
import { signInWithPopup } from "firebase/auth";
import axios from "axios";

export default function SignInForm() {
  const router = useRouter();
  const search = useSearchParams();
  const dispatch = useAppDispatch();
  const { status, error } = useAppSelector((s) => s.auth);

  const redirect = search.get("redirect") || "/";

  const [showPassword, setShowPassword] = useState(false);
  const [isChecked, setIsChecked] = useState(true);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");

  const [showForgotModal, setShowForgotModal] = useState(false);

  const [login, { isLoading: isLoggingIn }] = useLoginMutation();
  const [memberLogin, { isLoading: isMemberLoggingIn }] =
    useMemberLoginMutation(); // ✅

  useEffect(() => {
    const savedEmail = localStorage.getItem("email") || "";
    const savedPassword = localStorage.getItem("password") || "";
    if (savedEmail && savedPassword) {
      setEmail(savedEmail);
      setPassword(savedPassword);
      setIsChecked(true);
    }
  }, []);

  const routeToDashboard = (country: string) => {
    const now = new Date();
    const currentMonth = now.toLocaleString("en-US", { month: "long" });
    const currentYear = String(now.getFullYear());
    router.replace(`/live-dashboard/${country}/${currentMonth}/${currentYear}`);
  };

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (
      status === "loading" ||
      isLoggingIn ||
      isMemberLoggingIn
    )
      return;

    dispatch(setAuthLoading());

    const cleanEmail = email.trim().toLowerCase();

    try {
      // ✅ 1) Try CLIENT login first
      let result: any;
      let loginType: "client" | "member" = "client";

      try {
        result = await login({ email: cleanEmail, password }).unwrap();
        loginType = "client";
      } catch (clientErr: any) {
        // ✅ if client login fails, try member login
        result = await memberLogin({ email: cleanEmail, password }).unwrap();
        loginType = "member";
      }

      // Save token
      dispatch(setCredentials({ token: result.token }));

      // Remember Me
      if (isChecked) {
        localStorage.setItem("email", cleanEmail);
        localStorage.setItem("password", password);
      } else {
        localStorage.removeItem("email");
        localStorage.removeItem("password");
      }

      // ✅ CLIENT FLOW
      if (loginType === "client") {
        const me = await fetch(`${API_BASE}/get_user_data`, {
          method: "GET",
          headers: { Authorization: `Bearer ${result.token}` },
        })
          .then((r) => r.json())
          .catch(() => null);

        if (me) dispatch(setUser({ ...me, is_member: false }));

        const hasMarketplace =
          typeof me?.marketplace_id === "string" &&
          me.marketplace_id.trim().length > 0;

        if (!hasMarketplace) {
          router.replace("/choose-country?onboard=1");
          return;
        }

        const countryFromBackend =
          typeof me?.country === "string" && me.country.trim().length > 0
            ? me.country.split(",")[0]
            : "global";

        routeToDashboard(countryFromBackend);
        return;
      }

      // ✅ MEMBER FLOW (no /get_user_data call)
      // backend member_login already returns modules/marketplaces/countries
      dispatch(
        setUser({
          email: cleanEmail,
          is_member: true,
          member_id: result.member_id,
          owner_user_id: result.owner_user_id,
          modules: result.modules || [],
          marketplaces: result.marketplaces || [],
          countries: result.countries || [],
        })
      );

      const memberCountry =
        Array.isArray(result?.countries) && result.countries.length > 0
          ? result.countries[0]
          : "global";

      routeToDashboard(memberCountry);
    } catch (err: any) {
      // ✅ nicer error msg
      const msg =
        err?.status === 403
          ? "Please verify your email first."
          : err?.data?.message || err?.error || "Login failed. Please try again.";
      dispatch(setAuthError(msg));
    }
  };

  // ✅ Google login stays client-only (optional)
  const onGoogleSignIn = async () => {
    if (status === "loading" || isLoggingIn) return;

    dispatch(setAuthLoading());

    try {
      const cred = await signInWithPopup(auth, googleProvider);
      const email = cred.user.email;
      const name = cred.user.displayName;
      if (!email) throw new Error("Google account did not return an email");

      const { data } = await axios.post(`${API_BASE}/google_register`, {
        email,
        name,
      });
      if (!data?.token) throw new Error(data?.message || "No token returned from server");

      dispatch(setCredentials({ token: data.token }));

      const me = await fetch(`${API_BASE}/get_user_data`, {
        method: "GET",
        headers: { Authorization: `Bearer ${data.token}` },
      }).then((r) => r.json()).catch(() => null);

      if (me) dispatch(setUser({ ...me, is_member: false }));

      const hasMarketplace =
        typeof me?.marketplace_id === "string" &&
        me.marketplace_id.trim().length > 0;

      if (!hasMarketplace) {
        router.replace("/choose-country?onboard=1");
        return;
      }

      const countryFromBackend =
        typeof me?.country === "string" && me.country.trim().length > 0
          ? me.country.split(",")[0]
          : "global";

      routeToDashboard(countryFromBackend);
    } catch (err: any) {
      const msg =
        err?.response?.data?.message ||
        err?.message ||
        "Google sign-in failed. Please try again.";
      dispatch(setAuthError(msg));
    }
  };

  return (
    <div className="flex flex-col  lg:w-1/2 w-full">
      <div className="flex flex-col justify-center  flex-1 w-full xl:max-w-lg lg:mx-6 max-w-md xl:mx-auto mx-auto ">
        <div>
          <div className="mb-5 2xl:mb-8">
            <h1 className="mb-2  text-green-500 text-title-sm dark:text-white/90 xl:text-title-lg lg:text-4xl sm:text-title-lg">
              Welcome!
            </h1>
            <p className="2xl:text-base text-sm text-gray-500 dark:text-gray-400">
              Please enter your login details
            </p>
          </div>

          <form onSubmit={onSubmit} noValidate>
            <div className="space-y-3">
              <div>
                <Label>
                  Email <span className="text-error-500 ">*</span>{" "}
                </Label>
                <Input
                  placeholder="info@gmail.com"
                  type="email"
                  value={email}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    setEmail(e.target.value)
                  }
                  required
                  autoComplete="email"
                />
              </div>

              <div>
                <Label>
                  Password <span className="text-error-500">*</span>{" "}
                </Label>
                <div className="relative">
                  <Input
                    type={showPassword ? "text" : "password"}
                    placeholder="Enter your password"
                    value={password}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                      setPassword(e.target.value)
                    }
                    required
                    autoComplete="current-password"
                  />
                  <button
                    type="button"
                    onClick={() => setShowPassword(!showPassword)}
                    className="absolute z-30 -translate-y-1/2 right-4 top-1/2"
                    aria-label={showPassword ? "Hide password" : "Show password"}
                  >
                    {showPassword ? (
                      <EyeIcon className="fill-gray-500 dark:fill-gray-400" />
                    ) : (
                      <EyeCloseIcon className="fill-gray-500 dark:fill-gray-400" />
                    )}
                  </button>
                </div>
              </div>

              <div className="flex items-center justify-between">
                <label className="inline-flex items-center gap-3 cursor-pointer">
                  <Checkbox checked={isChecked} onChange={setIsChecked} />
                  <span className="block font-normal text-gray-700 text-theme-sm dark:text-gray-400">
                    Keep me logged in
                  </span>
                </label>
                <button
                  type="button"
                  onClick={() => setShowForgotModal(true)}
                  className="text-theme-sm text-blue-700"
                >
                  Forgot password?
                </button>
              </div>

              {error && (
                <p className="text-sm text-red-500 -mt-2" aria-live="polite">
                  {error}
                </p>
              )}

              <div>
                <Button
                  className="w-full"
                  size="md"
                  type="submit"
                  disabled={status === "loading" || isLoggingIn || isMemberLoggingIn}
                >
                  {status === "loading" || isLoggingIn || isMemberLoggingIn
                    ? "Signing in…"
                    : "Sign in"}
                </Button>
              </div>
            </div>
          </form>

          {/* ...rest same (Google, signup, modal) */}
          {showForgotModal && (
            <ForgotPasswordModal onClose={() => setShowForgotModal(false)} />
          )}
        </div>
      </div>
    </div>
  );
}