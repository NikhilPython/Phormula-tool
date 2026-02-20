
"use client";

import Checkbox from "@/components/form/input/Checkbox";
import Input from "@/components/form/input/InputField";
import Label from "@/components/form/Label";
import { EyeCloseIcon, EyeIcon } from "@/icons";
import Link from "next/link";
import React, { useEffect, useMemo, useState } from "react";
import "react-phone-input-2/lib/style.css";
import PhoneInput from "@/components/form/group-input/PhoneInput";
import { useRegisterMutation } from "@/lib/api/authApi";
import { formatPhoneNumber } from "@/lib/utils/phone";
import Button from "../ui/button/Button";
import { Modal } from "../ui/modal";
import { useRouter } from "next/navigation";
import { ALL_COUNTRIES } from "@/lib/utils/countryCodes";
import { auth, googleProvider } from "@/lib/firebase/firebase";
import { signInWithPopup } from "firebase/auth";
import { API_BASE } from "@/config/env";
import axios from "axios";

type PhoneMeta = { dialCode: string; iso2?: string };

const emailRegex =
  /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/i;

// Very small dialCode -> typical national number length map (NOT including country code)
// You can extend this easily later.
const NATIONAL_LENGTH_BY_DIAL: Record<string, number[]> = {
  // India
  "91": [10],
  // US/CA
  "1": [10],
  // UK (often 10/11 depending on type; keep flexible)
  "44": [10, 11],
  // UAE
  "971": [9],
  // Pakistan
  "92": [10],
  // Bangladesh
  "880": [10],
  // Sri Lanka
  "94": [9],
  // Nepal
  "977": [10],
};

export default function SignUpForm() {
  const router = useRouter();

  const [registerUser, { isLoading, isSuccess, error: regError }] =
    useRegisterMutation();

  const [showPassword, setShowPassword] = useState(false);
  const [showConfirm, setShowConfirm] = useState(false);
  const [isChecked, setIsChecked] = useState(false);

  const [showSuccessModal, setShowSuccessModal] = useState(false);

  // form fields
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [phoneRaw, setPhoneRaw] = useState("");
  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");

  const [phoneDialCode, setPhoneDialCode] = useState("");
  const [phoneMeta, setPhoneMeta] = useState<PhoneMeta>({ dialCode: "" });

  // "touched" for showing validation messages nicely
  const [touched, setTouched] = useState({
    name: false,
    email: false,
    phone: false,
    password: false,
    confirm: false,
  });

  // open modal on success
  useEffect(() => {
    if (isSuccess) setShowSuccessModal(true);
  }, [isSuccess]);

  // ------------------------
  // Email validation
  // ------------------------
  const emailError = useMemo(() => {
    if (!touched.email) return "";
    if (!email.trim()) return "Email is required";
    if (!emailRegex.test(email.trim())) return "Enter a valid email (must include @)";
    return "";
  }, [email, touched.email]);

  const nameError = useMemo(() => {
    if (!touched.name) return "";
    if (!name.trim()) return "Name is required";
    if (name.trim().length < 2) return "Name must be at least 2 characters";
    return "";
  }, [name, touched.name]);

  // ------------------------
  // Phone validation (digits only + country wise length)
  // ------------------------
  const phoneDigitsOnly = useMemo(() => phoneRaw.replace(/\D/g, ""), [phoneRaw]);

  const phoneError = useMemo(() => {
    if (!touched.phone) return "";
    if (!phoneDigitsOnly) return "Phone number is required";
    if (!phoneDialCode) return "Select a country code";

    // national length check (based on dial code)
    const allowedLens = NATIONAL_LENGTH_BY_DIAL[phoneDialCode];
    if (allowedLens && !allowedLens.includes(phoneDigitsOnly.length)) {
      return `Invalid number length for +${phoneDialCode} (need ${allowedLens.join(" or ")} digits)`;
    }

    // fallback if we don't have mapping: at least 6 digits and max 14
    if (!allowedLens) {
      if (phoneDigitsOnly.length < 6) return "Phone number is too short";
      if (phoneDigitsOnly.length > 14) return "Phone number is too long";
    }

    // digits only rule (your requirement)
    if (phoneRaw && /[^\d\s()-]/.test(phoneRaw)) {
      return "Only numbers are allowed";
    }

    return "";
  }, [touched.phone, phoneDigitsOnly, phoneDialCode, phoneRaw]);

  const hasValidPhoneNumber = useMemo(() => {
    // Keep your existing usage but make it real-valid now
    return !phoneError && !!phoneDigitsOnly && !!phoneDialCode;
  }, [phoneError, phoneDigitsOnly, phoneDialCode]);

  // ------------------------
  // Password rules (for tooltip)
  // ------------------------
  const passwordRules = useMemo(() => {
    const atLeast6 = password.length >= 6;
    const twoNumbers = /\d.*\d/.test(password);
    const twoAlphabets = /[a-zA-Z].*[a-zA-Z]/.test(password);
    return [
      { label: "Atleast of 6 characters", ok: atLeast6 },
      { label: "2 numbers", ok: twoNumbers },
      { label: "2 Alphabets", ok: twoAlphabets },
    ];
  }, [password]);

  const confirmError = useMemo(() => {
    if (!touched.confirm) return "";
    if (!confirm) return "Confirm password is required";
    if (confirm !== password) return "Passwords do not match";
    return "";
  }, [confirm, password, touched.confirm]);

  // keep your old passwordErrors logic (but we won't show the old text now)
  const passwordErrors = useMemo(() => {
    const errs: string[] = [];
    if (password.length < 6) errs.push("At least 6 characters");
    if (!/\d.*\d/.test(password)) errs.push("At least 2 numbers");
    if (!/[a-zA-Z].*[a-zA-Z]/.test(password)) errs.push("At least 2 alphabets");
    if (confirm && confirm !== password) errs.push("Passwords do not match");
    return errs;
  }, [password, confirm]);

  const canSubmit =
    !nameError &&               // ✅ NEW
    !emailError &&
    !phoneError &&
    name.trim() &&              // ✅ NEW
    email.trim() &&
    hasValidPhoneNumber &&
    password &&
    confirm &&
    isChecked &&
    passwordErrors.filter((e) => e !== "Passwords do not match").length === 0 &&
    confirm === password;


  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setTouched((p) => ({ ...p, email: true, phone: true, password: true, confirm: true }));
    if (isLoading || !canSubmit) return;

    try {
      const localDigits = phoneDigitsOnly;
      const dialDigits = phoneDialCode.replace(/\D/g, "");
      const fullPhone = `+${dialDigits}${localDigits}`;
      const formatted = formatPhoneNumber(fullPhone);
      await registerUser({
        name: name.trim(),
        email: email.trim(),
        password,
        phone_number: formatted,
        phone_number_raw: fullPhone,
      }).unwrap();
    } catch {
      // handled by regError
    }
  };

  const serverErrorMessage = (() => {
    const msg =
      (regError as any)?.data?.message ||
      (regError as any)?.error ||
      "";

    if (msg.toLowerCase().includes("already")) {
      return "Account already exists. Please sign in.";
    }

    return msg;
  })();

  const showPasswordTooltip =
    (touched.password || password.length > 0) &&
    passwordRules.some((r) => !r.ok);

 const onGoogleSignUp = async () => {
  if (isLoading) return;

  try {
    const cred = await signInWithPopup(auth, googleProvider);
    const email = cred.user.email;
    const name = cred.user.displayName; 
    if (!email) throw new Error("Google account did not return an email");

    const { data } = await axios.post(`${API_BASE}/google_register`, { email, name });
    if (!data?.token) throw new Error(data?.message || "No token returned from server");

    router.replace("/choose-country?onboard=1");
  } catch (err: any) {
    const msg =
      err?.response?.data?.message ||
      err?.message ||
      "Google sign-up failed. Please try again.";
    alert(msg);
  }
};


  return (
    <div className="flex flex-col flex-1 lg:w-1/2 w-full overflow-y-auto no-scrollbar relative">
      <div className="flex flex-col justify-center flex-1 w-full xl:max-w-lg xl:mx-auto lg:mx-6 max-w-md mx-auto ">
        <div>
          <div className="mb-3 2xl:mb-8">
            <h1 className="mb-2 font-semibold text-green-500 text-title-sm dark:text-white/90 xl:text-title-md lg:text-4xl sm:text-title-md">
              Sign Up!
            </h1>
            <p className="text-sm text-charcoal-500 dark:text-gray-400">
              Enter your details to create an account.
            </p>
          </div>

          <div>
            <form onSubmit={onSubmit} noValidate>
              <div className="2xl:space-y-3 space-y-2">

                {/* Name + Email */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {/* Name */}
                  <div>
                    <Label>
                      Name<span className="text-error-500">*</span>
                    </Label>
                    <Input
                      type="text"
                      id="name"
                      name="name"
                      placeholder="Enter your full name"
                      value={name}
                      onChange={(e) => setName(e.target.value)}
                      onBlur={() => setTouched((p) => ({ ...p, name: true }))}
                      autoComplete="name"
                      required
                    />
                    {nameError && (
                      <p className="mt-1.5 text-xs text-red-500" aria-live="polite">
                        {nameError}
                      </p>
                    )}
                  </div>

                  {/* Email */}
                  <div>
                    <Label>
                      Email<span className="text-error-500">*</span>
                    </Label>
                    <Input
                      type="email"
                      id="email"
                      name="email"
                      placeholder="Enter your email"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                      onBlur={() => setTouched((p) => ({ ...p, email: true }))}
                      autoComplete="email"
                      required
                    />
                    {emailError && (
                      <p className="mt-1.5 text-xs text-red-500" aria-live="polite">
                        {emailError}
                      </p>
                    )}
                  </div>
                </div>


                {/* Phone */}
                <div>
                  <Label>
                    Phone Number<span className="text-error-500">*</span>
                  </Label>
                  <div className="border border-gray-300 dark:border-gray-700 rounded-lg p-1 dark:bg-gray-900">
                    <PhoneInput
                      countries={ALL_COUNTRIES}
                      placeholder="Enter phone number"
                      onChange={(value, meta) => {
                        const digits = String(value || "").replace(/\D/g, "");
                        setPhoneRaw(digits);
                        setPhoneDialCode(meta?.dialCode || "");
                        setPhoneMeta({
                          dialCode: meta?.dialCode || "",
                          iso2: meta?.country?.code,
                        });
                        setTouched((p) => ({ ...p, phone: true })); // 👈 IMPORTANT
                      }}
                      onBlur={() => setTouched((p) => ({ ...p, phone: true }))}
                    />
                  </div>
                  {phoneError && (
                    <p className="mt-1.5 text-xs text-red-500" aria-live="polite">
                      {phoneError}
                    </p>
                  )}
                </div>

                {/* Password */}
                <div className="space-y-3 flex 2xl:flex-col flex-row gap-3 ">
 <div className="2xl:w-full w-1/2 ">
                  <Label>
                    Password<span className="text-error-500">*</span>
                  </Label>

                  <div className="relative">
                    <Input
                      placeholder="Enter your password"
                      type={showPassword ? "text" : "password"}
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      onFocus={() => setTouched((p) => ({ ...p, password: true }))}
                      onBlur={() => setTouched((p) => ({ ...p, password: true }))}
                      autoComplete="new-password"
                      required
                    />

                    <button
                      type="button"
                      onClick={() => setShowPassword((v) => !v)}
                      className="absolute z-30 -translate-y-1/2 right-4 top-1/2"
                      aria-label={showPassword ? "Hide password" : "Show password"}
                    >
                      {showPassword ? (
                        <EyeIcon className="fill-gray-500 dark:fill-gray-400" />
                      ) : (
                        <EyeCloseIcon className="fill-gray-500 dark:fill-gray-400" />
                      )}
                    </button>

                    {/* Tooltip (chat-bubble style) */}
                    {showPasswordTooltip && (
                      <div className="absolute right-0 top-[calc(100%+8px)] z-40">
                        <div className="relative min-w-[210px] rounded-lg border bg-[#EDEDED] px-3 py-2 shadow-md">
                          <div className="text-xs font-medium text-gray-800 mb-1">
                            Password must have -
                          </div>

                          <ul className="space-y-1">
                            {passwordRules.map((r) => (
                              <li key={r.label} className="flex items-center gap-2 text-xs">
                                <span
                                  className={`inline-flex h-4 w-4 items-center justify-center rounded-full border ${r.ok
                                    ? "border-green-500 text-green-600"
                                    : "border-red-500 text-red-600"
                                    }`}
                                  aria-hidden="true"
                                >
                                  {r.ok ? "✓" : "×"}
                                </span>
                                <span className={r.ok ? "text-gray-800" : "text-gray-700"}>
                                  {r.label}
                                </span>
                              </li>
                            ))}
                          </ul>

                          {/* arrow (top-right pointing to input) */}
                          <div className="absolute right-4 top-[-6px] h-3 w-3 rotate-45 border-l border-t bg-[#EDEDED]" />
                        </div>
                      </div>
                    )}
                  </div>
                </div>

                {/* Confirm Password */}
                <div className="2xl:w-full w-1/2 ">
                  <Label>
                    Confirm Password<span className="text-error-500">*</span>
                  </Label>
                  <div className="relative">
                    <Input
                      placeholder="Confirm your password"
                      type={showConfirm ? "text" : "password"}
                      value={confirm}
                      onChange={(e) => {
                        setConfirm(e.target.value);
                        setTouched((p) => ({ ...p, confirm: true }));
                      }}
                      onBlur={() => setTouched((p) => ({ ...p, confirm: true }))}
                      autoComplete="new-password"
                      required
                    />
                    <button
                      type="button"
                      onClick={() => setShowConfirm((v) => !v)}
                      className="absolute z-30 -translate-y-1/2 right-4 top-1/2"
                      aria-label={showConfirm ? "Hide confirm password" : "Show confirm password"}
                    >
                      {showConfirm ? (
                        <EyeIcon className="fill-gray-500 dark:fill-gray-400" />
                      ) : (
                        <EyeCloseIcon className="fill-gray-500 dark:fill-gray-400" />
                      )}
                    </button>
                  </div>

                  {confirmError && (
                    <p className="mt-1.5 text-xs text-red-500" aria-live="polite">
                      {confirmError}
                    </p>
                  )}
                </div>
                </div>
               

                {/* Terms */}
                <div className="flex items-center gap-3">
                  <Checkbox
                    className="w-3 h-3"
                    checked={isChecked}
                    onChange={setIsChecked}
                  />
                  <p className="inline-block font-normal text-gray-500 dark:text-gray-400 text-xs">
                    By creating an account you agree to the{" "}
                    <span className="text-gray-800 dark:text-white/90">
                      Terms and Conditions
                    </span>
                    , and our{" "}
                    <span className="text-gray-800 dark:text-white">
                      Privacy Policy
                    </span>
                    .
                  </p>
                </div>

                {/* Server error */}
                {serverErrorMessage && (
                  <p className="text-sm text-red-500 -mt-1" aria-live="polite">
                    {serverErrorMessage}
                  </p>
                )}

                {/* Submit */}
                <div className="">
                  <Button
                    type="submit"
                    disabled={isLoading || !canSubmit}
                    className="flex items-center justify-center w-full px-4 text-sm font-medium transition rounded-lg disabled:opacity-60 disabled:cursor-not-allowed"
                  >
                    {isLoading ? "Please wait…" : "Sign Up"}
                  </Button>
                </div>
              </div>
            </form>

            {/* Divider */}
            <div className="relative">
              <div className="absolute inset-0 flex items-center">
                <div className="w-full border-t border-charcoal-500"></div>
              </div>
              <div className="relative flex justify-center text-sm">
                <span className="p-2 text-charcoal-500 bg-white sm:px-5 sm:py-2">
                  or
                </span>
              </div>
            </div>

            {/* Google Sign-in */}
           
              <button
                type="button"
                onClick={onGoogleSignUp}
                className=" inline-flex items-center justify-center gap-3 px-4 xl:py-3 py-2 w-full border border-charcoal-500 rounded-lg h-10 2xl:h-11
    text-charcoal-500  transition-colors text-md font-bold
    dark:bg-white/5 dark:text-white/90 dark:hover:bg-white/10"
              >
                <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
                  <path d="M18.7511 10.1944C18.7511 9.47495 18.6915 8.94995 18.5626 8.40552H10.1797V11.6527H15.1003C15.0011 12.4597 14.4654 13.675 13.2749 14.4916L13.2582 14.6003L15.9087 16.6126L16.0924 16.6305C17.7788 15.1041 18.7511 12.8583 18.7511 10.1944Z" fill="#4285F4" />
                  <path d="M10.1788 18.75C12.5895 18.75 14.6133 17.9722 16.0915 16.6305L13.274 14.4916C12.5201 15.0068 11.5081 15.3666 10.1788 15.3666C7.81773 15.3666 5.81379 13.8402 5.09944 11.7305L4.99473 11.7392L2.23868 13.8295L2.20264 13.9277C3.67087 16.786 6.68674 18.75 10.1788 18.75Z" fill="#34A853" />
                  <path d="M5.10014 11.7305C4.91165 11.186 4.80257 10.6027 4.80257 9.99992C4.80257 9.3971 4.91165 8.81379 5.09022 8.26935L5.08523 8.1534L2.29464 6.02954L2.20333 6.0721C1.5982 7.25823 1.25098 8.5902 1.25098 9.99992C1.25098 11.4096 1.5982 12.7415 2.20333 13.9277L5.10014 11.7305Z" fill="#FBBC05" />
                  <path d="M10.1789 4.63331C11.8554 4.63331 12.9864 5.34303 13.6312 5.93612L16.1511 3.525C14.6035 2.11528 12.5895 1.25 10.1789 1.25C6.68676 1.25 3.67088 3.21387 2.20264 6.07218L5.08953 8.26943C5.81381 6.15972 7.81776 4.63331 10.1789 4.63331Z" fill="#EB4335" />
                </svg>
                Continue with Google
              </button>
           

            <div className="2xl:mt-5 mt-3 max-w-fit mx-auto">
              <p className="2xl:text-sm text-xs font-normal text-center text-blue-700 sm:text-start">
                Already a user ?{" "}
                <Link href="/signin" className="text-blue-700 ">
                  Login here
                </Link>
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* SUCCESS MODAL */}
      <Modal
        isOpen={showSuccessModal}
        onClose={() => setShowSuccessModal(false)}
        showCloseButton={false}
        className="m-4 max-w-sm"
      >
        <div className="w-full rounded-xl bg-white px-6 py-8 text-center shadow-lg">
          <div className="mx-auto mb-4 flex h-14 w-14 items-center justify-center rounded-full bg-green-500">
            <svg className="h-7 w-7 text-white" viewBox="0 0 24 24" fill="none">
              <path
                d="M20 6L9 17L4 12"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          </div>

          <h2 className="text-xl font-semibold text-gray-800">
            Registration Successful!
          </h2>
          <p className="mt-2 text-sm text-gray-600">
            You need to verify your email first. Please check your inbox for the
            verification email.
          </p>
        </div>
      </Modal>
    </div>
  );
}
