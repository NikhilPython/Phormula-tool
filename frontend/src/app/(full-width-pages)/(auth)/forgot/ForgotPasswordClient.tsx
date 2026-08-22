"use client";

import { useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { FaCircleCheck } from "react-icons/fa6";

import {
  useForgotPasswordMutation,
  useResetPasswordOtpMutation,
} from "@/lib/api/profileApi";

type Step = "email" | "reset" | "success";

const ForgotPasswordPage = () => {
  const router = useRouter();

  const [step, setStep] = useState<Step>("email");

  const [email, setEmail] = useState("");

  const [otp, setOtp] = useState<string[]>([
    "",
    "",
    "",
    "",
    "",
    "",
  ]);

  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");

  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const inputRefs = useRef<Array<HTMLInputElement | null>>([]);

  const [forgotPassword, { isLoading: isSending }] =
    useForgotPasswordMutation();

  const [resetPasswordOtp, { isLoading: isResetting }] =
    useResetPasswordOtpMutation();

  const normalizedEmail = email.trim().toLowerCase();
  const otpValue = otp.join("");

  const clearMessages = () => {
    setMessage("");
    setError("");
  };

  // =========================================================
  // SEND PASSWORD RESET OTP
  // =========================================================

  const handleForgotPassword = async (
    e: React.FormEvent<HTMLFormElement>
  ) => {
    e.preventDefault();

    clearMessages();

    if (!normalizedEmail) {
      setError("Please enter your email address.");
      return;
    }

    try {
      const response = await forgotPassword({
        email: normalizedEmail,
      }).unwrap();

      setMessage(
        response?.message ||
        "Password reset code sent to your email."
      );

      setStep("reset");

      window.setTimeout(() => {
        inputRefs.current[0]?.focus();
      }, 100);
    } catch (err: any) {
      setError(
        err?.data?.message ||
        err?.error ||
        "Unable to send password reset code."
      );
    }
  };

  // =========================================================
  // OTP INPUT
  // =========================================================

  const handleOtpChange = (
    index: number,
    value: string
  ) => {
    const digits = value.replace(/\D/g, "");

    // Browser/mobile OTP autofill
    if (digits.length > 1) {
      const next = ["", "", "", "", "", ""];

      digits
        .slice(0, 6)
        .split("")
        .forEach((digit, digitIndex) => {
          next[digitIndex] = digit;
        });

      setOtp(next);
      clearMessages();

      const focusIndex = Math.min(digits.length, 6) - 1;

      inputRefs.current[
        Math.max(focusIndex, 0)
      ]?.focus();

      return;
    }

    const digit = digits.slice(-1);

    setOtp((prev) => {
      const next = [...prev];

      next[index] = digit;

      return next;
    });

    clearMessages();

    if (digit && index < 5) {
      inputRefs.current[index + 1]?.focus();
    }
  };

  const handleOtpKeyDown = (
    index: number,
    e: React.KeyboardEvent<HTMLInputElement>
  ) => {
    if (e.key === "Backspace") {
      if (otp[index]) {
        setOtp((prev) => {
          const next = [...prev];
          next[index] = "";
          return next;
        });

        return;
      }

      if (index > 0) {
        inputRefs.current[index - 1]?.focus();

        setOtp((prev) => {
          const next = [...prev];
          next[index - 1] = "";
          return next;
        });
      }

      return;
    }

    if (e.key === "ArrowLeft" && index > 0) {
      e.preventDefault();
      inputRefs.current[index - 1]?.focus();
    }

    if (e.key === "ArrowRight" && index < 5) {
      e.preventDefault();
      inputRefs.current[index + 1]?.focus();
    }
  };

  const handleOtpPaste = (
    e: React.ClipboardEvent<HTMLInputElement>
  ) => {
    e.preventDefault();

    const pasted = e.clipboardData
      .getData("text")
      .replace(/\D/g, "")
      .slice(0, 6);

    if (!pasted) return;

    const next = ["", "", "", "", "", ""];

    pasted.split("").forEach((digit, index) => {
      next[index] = digit;
    });

    setOtp(next);
    clearMessages();

    const focusIndex = Math.min(pasted.length, 6) - 1;

    inputRefs.current[
      Math.max(focusIndex, 0)
    ]?.focus();
  };

  // =========================================================
  // RESET PASSWORD
  // =========================================================

  const handleResetPassword = async (
    e: React.FormEvent<HTMLFormElement>
  ) => {
    e.preventDefault();

    clearMessages();

    if (otpValue.length !== 6) {
      setError("Please enter the complete 6-digit code.");
      return;
    }

    if (!password) {
      setError("Please enter your new password.");
      return;
    }

    if (password.length < 6) {
      setError(
        "Password must contain at least 6 characters."
      );
      return;
    }

    if (password !== confirmPassword) {
      setError("Passwords do not match.");
      return;
    }

    try {
      const response = await resetPasswordOtp({
        email: normalizedEmail,
        otp: otpValue,
        password,
      }).unwrap();

      setMessage(
        response?.message ||
        "Password reset successfully."
      );

      setStep("success");
    } catch (err: any) {
      setError(
        err?.data?.message ||
        err?.error ||
        "Unable to reset password."
      );
    }
  };

  // =========================================================
  // CHANGE EMAIL
  // =========================================================

  const handleChangeEmail = () => {
    setStep("email");

    setOtp([
      "",
      "",
      "",
      "",
      "",
      "",
    ]);

    setPassword("");
    setConfirmPassword("");

    clearMessages();
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 px-4">
      <div className="relative w-full max-w-[450px] rounded-2xl border border-gray-200 bg-white p-7 font-[Lato] shadow-xl sm:p-9">

        {/* Close */}
        <button
          type="button"
          onClick={() => router.push("/signin")}
          className="absolute right-4 top-4 flex h-8 w-8 items-center justify-center rounded-full text-lg text-gray-400 transition hover:bg-gray-100 hover:text-gray-600"
          aria-label="Back to sign in"
        >
          ×
        </button>

        {/* =====================================================
            EMAIL STEP
        ===================================================== */}

        {step === "email" && (
          <>
            <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-full bg-[#EEF7F3]">
              <svg
                width="26"
                height="26"
                viewBox="0 0 24 24"
                fill="none"
                className="text-[#5EA68E]"
                aria-hidden="true"
              >
                <path
                  d="M4 6.5L12 12L20 6.5"
                  stroke="currentColor"
                  strokeWidth="1.8"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />

                <rect
                  x="3"
                  y="5"
                  width="18"
                  height="14"
                  rx="3"
                  stroke="currentColor"
                  strokeWidth="1.8"
                />
              </svg>
            </div>

            <div className="mt-5 text-center">
              <h1 className="text-2xl font-semibold text-[#303030]">
                Forgot password?
              </h1>

              <p className="mt-2 text-sm leading-6 text-gray-500">
                Enter your registered email address and
                we&apos;ll send you a verification code.
              </p>
            </div>

            <form
              className="mt-7"
              onSubmit={handleForgotPassword}
            >
              <label
                htmlFor="email"
                className="mb-2 block text-sm font-medium text-[#414042]"
              >
                Email address
              </label>

              <input
                type="email"
                id="email"
                placeholder="Enter your email"
                className="
                  h-11 w-full
                  rounded-lg
                  border border-gray-300
                  px-3
                  text-sm
                  text-[#37455F]
                  outline-none
                  transition
                  focus:border-[#5EA68E]
                  focus:ring-4
                  focus:ring-[#5EA68E]/10
                "
                value={email}
                onChange={(e) => {
                  setEmail(e.target.value);
                  clearMessages();
                }}
                autoComplete="email"
                required
              />

              {error && (
                <div className="mt-4 rounded-lg border border-red-100 bg-red-50 px-3 py-2.5 text-center text-sm text-red-600">
                  {error}
                </div>
              )}

              <button
                type="submit"
                disabled={
                  isSending ||
                  !normalizedEmail
                }
                className="
                  mt-6 flex h-12 w-full
                  items-center justify-center
                  rounded-xl
                  bg-[#37455F]
                  text-sm font-semibold
                  text-[#F8EDCE]
                  transition
                  hover:bg-[#2F3B52]
                  disabled:cursor-not-allowed
                  disabled:bg-gray-300
                  disabled:text-white
                "
              >
                {isSending
                  ? "Sending..."
                  : "Send Verification Code"}
              </button>

              <button
                type="button"
                onClick={() => router.push("/signin")}
                className="mt-4 w-full text-center text-sm font-medium text-gray-500 transition hover:text-[#37455F]"
              >
                ← Back to Sign In
              </button>
            </form>
          </>
        )}

        {/* =====================================================
            OTP + PASSWORD STEP
        ===================================================== */}

        {step === "reset" && (
          <>
            <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-full bg-[#EEF7F3]">
              <svg
                width="27"
                height="27"
                viewBox="0 0 24 24"
                fill="none"
                className="text-[#5EA68E]"
                aria-hidden="true"
              >
                <rect
                  x="5"
                  y="10"
                  width="14"
                  height="10"
                  rx="2"
                  stroke="currentColor"
                  strokeWidth="1.8"
                />

                <path
                  d="M8 10V7.5C8 5.57 9.57 4 11.5 4H12.5C14.43 4 16 5.57 16 7.5V10"
                  stroke="currentColor"
                  strokeWidth="1.8"
                  strokeLinecap="round"
                />
              </svg>
            </div>

            <div className="mt-5 text-center">
              <h1 className="text-2xl font-semibold text-[#303030]">
                Reset your password
              </h1>

              <p className="mt-2 text-sm leading-6 text-gray-500">
                Enter the 6-digit verification code sent to
              </p>

              <div className="mt-3 inline-flex max-w-full rounded-full border border-[#D9EDE6] bg-[#F4FAF7] px-4 py-1.5">
                <span className="truncate text-sm font-semibold text-[#37455F]">
                  {normalizedEmail}
                </span>
              </div>
            </div>

            <form
              onSubmit={handleResetPassword}
              className="mt-7"
            >
              <p className="mb-3 text-sm font-medium text-[#414042]">
                Verification code
              </p>

              <div className="flex items-center justify-between gap-2">
                {otp.map((digit, index) => (
                  <input
                    key={index}
                    ref={(element) => {
                      inputRefs.current[index] = element;
                    }}
                    type="text"
                    value={digit}
                    onChange={(e) =>
                      handleOtpChange(
                        index,
                        e.target.value
                      )
                    }
                    onKeyDown={(e) =>
                      handleOtpKeyDown(index, e)
                    }
                    onPaste={handleOtpPaste}
                    inputMode="numeric"
                    pattern="[0-9]*"
                    autoComplete={
                      index === 0
                        ? "one-time-code"
                        : "off"
                    }
                    maxLength={1}
                    disabled={isResetting}
                    className="
                      h-14 w-12
                      rounded-xl
                      border border-gray-300
                      bg-white
                      text-center text-xl
                      font-semibold
                      text-[#37455F]
                      outline-none
                      transition-all
                      focus:border-[#5EA68E]
                      focus:ring-4
                      focus:ring-[#5EA68E]/10
                      disabled:bg-gray-50
                      sm:w-[52px]
                    "
                    aria-label={`OTP digit ${index + 1
                      }`}
                  />
                ))}
              </div>

              {/* Password */}
              <div className="mt-5">
                <label
                  htmlFor="new-password"
                  className="mb-2 block text-sm font-medium text-[#414042]"
                >
                  New Password
                </label>

                <input
                  id="new-password"
                  type="password"
                  value={password}
                  onChange={(e) => {
                    setPassword(e.target.value);
                    clearMessages();
                  }}
                  placeholder="Enter new password"
                  autoComplete="new-password"
                  className="
                    h-11 w-full
                    rounded-lg
                    border border-gray-300
                    px-3
                    text-sm
                    outline-none
                    transition
                    focus:border-[#5EA68E]
                    focus:ring-4
                    focus:ring-[#5EA68E]/10
                  "
                />
              </div>

              {/* Confirm */}
              <div className="mt-4">
                <label
                  htmlFor="confirm-password"
                  className="mb-2 block text-sm font-medium text-[#414042]"
                >
                  Confirm Password
                </label>

                <input
                  id="confirm-password"
                  type="password"
                  value={confirmPassword}
                  onChange={(e) => {
                    setConfirmPassword(e.target.value);
                    clearMessages();
                  }}
                  placeholder="Confirm new password"
                  autoComplete="new-password"
                  className="
                    h-11 w-full
                    rounded-lg
                    border border-gray-300
                    px-3
                    text-sm
                    outline-none
                    transition
                    focus:border-[#5EA68E]
                    focus:ring-4
                    focus:ring-[#5EA68E]/10
                  "
                />
              </div>

              {message && (
                <div className="mt-4 rounded-lg border border-green-100 bg-green-50 px-3 py-2.5 text-center text-sm text-green-700">
                  {message}
                </div>
              )}

              {error && (
                <div className="mt-4 rounded-lg border border-red-100 bg-red-50 px-3 py-2.5 text-center text-sm text-red-600">
                  {error}
                </div>
              )}

              <button
                type="submit"
                disabled={
                  isResetting ||
                  otpValue.length !== 6 ||
                  !password ||
                  !confirmPassword
                }
                className="
                  mt-6 flex h-12 w-full
                  items-center justify-center
                  rounded-xl
                  bg-[#37455F]
                  text-sm font-semibold
                  text-[#F8EDCE]
                  transition
                  hover:bg-[#2F3B52]
                  disabled:cursor-not-allowed
                  disabled:bg-gray-300
                  disabled:text-white
                "
              >
                {isResetting
                  ? "Resetting..."
                  : "Reset Password"}
              </button>

              <button
                type="button"
                onClick={handleChangeEmail}
                className="mt-4 w-full text-center text-sm font-medium text-gray-500 transition hover:text-[#37455F]"
              >
                ← Change Email
              </button>
            </form>
          </>
        )}

        {/* =====================================================
            SUCCESS
        ===================================================== */}

        {step === "success" && (
          <div className="py-3 text-center">
            <div className="flex flex-col items-center gap-3 text-[#5EA68E]">
              <FaCircleCheck size={52} />

              <h1 className="text-2xl font-semibold text-[#303030]">
                Password Updated
              </h1>
            </div>

            <p className="mt-3 text-sm leading-6 text-gray-500">
              Your password has been reset successfully.
              You can now sign in using your new password.
            </p>

            {message && (
              <p className="mt-3 text-sm text-green-600">
                {message}
              </p>
            )}

            <button
              type="button"
              onClick={() =>
                router.replace("/signin")
              }
              className="
                mt-6 flex h-12 w-full
                items-center justify-center
                rounded-xl
                bg-[#37455F]
                text-sm font-semibold
                text-[#F8EDCE]
                transition
                hover:bg-[#2F3B52]
              "
            >
              Back to Sign In
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

export default ForgotPasswordPage;