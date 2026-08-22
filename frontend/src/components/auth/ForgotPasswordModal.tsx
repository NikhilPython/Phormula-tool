"use client";

import { useRef, useState } from "react";

type ForgotPasswordModalProps = {
  onClose: () => void;
};

type Step = "email" | "reset" | "success";

const ForgotPasswordModal: React.FC<ForgotPasswordModalProps> = ({
  onClose,
}) => {
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
  const [loading, setLoading] = useState(false);

  const inputRefs = useRef<Array<HTMLInputElement | null>>([]);

  const normalizedEmail = email.trim().toLowerCase();
  const otpValue = otp.join("");

  const clearMessages = () => {
    setMessage("");
    setError("");
  };

  // =========================================================
  // SEND RESET OTP
  // =========================================================

  const handleSendOtp = async (
    e: React.FormEvent<HTMLFormElement>
  ) => {
    e.preventDefault();

    clearMessages();

    if (!normalizedEmail) {
      setError("Please enter your email address.");
      return;
    }

    setLoading(true);

    try {
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/forgot_password`,
        {
          method: "POST",

          headers: {
            "Content-Type": "application/json",
          },

          body: JSON.stringify({
            email: normalizedEmail,
          }),
        }
      );

      const data = await response
        .json()
        .catch(() => null);

      if (!response.ok) {
        setError(
          data?.message ||
            "Unable to send password reset code."
        );

        return;
      }

      setMessage(
        data?.message ||
          "Password reset code sent to your email."
      );

      setStep("reset");

      window.setTimeout(() => {
        inputRefs.current[0]?.focus();
      }, 100);
    } catch {
      setError(
        "Unable to send password reset code. Please try again."
      );
    } finally {
      setLoading(false);
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

      const focusIndex =
        Math.min(digits.length, 6) - 1;

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

    const focusIndex =
      Math.min(pasted.length, 6) - 1;

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
      setError(
        "Please enter the complete 6-digit code."
      );

      return;
    }

    if (!password) {
      setError("Please enter your new password.");
      return;
    }

    if (password.length < 6) {
      setError(
        "Password must be at least 6 characters."
      );

      return;
    }

    if (password !== confirmPassword) {
      setError("Passwords do not match.");
      return;
    }

    setLoading(true);

    try {
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/reset-password-otp`,
        {
          method: "POST",

          headers: {
            "Content-Type": "application/json",
          },

          body: JSON.stringify({
            email: normalizedEmail,
            otp: otpValue,
            password,
          }),
        }
      );

      const data = await response
        .json()
        .catch(() => null);

      if (!response.ok) {
        setError(
          data?.message ||
            "Unable to reset password."
        );

        return;
      }

      setStep("success");

      setMessage(
        data?.message ||
          "Password reset successfully."
      );
    } catch {
      setError(
        "Unable to reset password. Please try again."
      );
    } finally {
      setLoading(false);
    }
  };

  // =========================================================
  // UI
  // =========================================================

  return (
    <div
      className="
        fixed inset-0 z-[9999]
        flex items-center justify-center
        bg-black/40
        px-4 py-6
        backdrop-blur-[2px]
      "
    >
      <div
        role="dialog"
        aria-modal="true"
        className="
          relative
          w-full max-w-[440px]
          rounded-2xl
          border border-gray-200
          bg-white
          px-7 py-8
          shadow-2xl
          sm:px-9
        "
      >
        {/* Close */}

        <button
          type="button"
          onClick={onClose}
          disabled={loading}
          className="
            absolute right-4 top-4
            flex h-8 w-8
            items-center justify-center
            rounded-full
            text-lg text-gray-400
            transition
            hover:bg-gray-100
            hover:text-gray-600
          "
          aria-label="Close"
        >
          ×
        </button>

        {/* =====================================================
            STEP 1 - EMAIL
        ===================================================== */}

        {step === "email" && (
          <>
            <div
              className="
                mx-auto flex
                h-14 w-14
                items-center justify-center
                rounded-full
                bg-[#EEF7F3]
              "
            >
              <svg
                width="26"
                height="26"
                viewBox="0 0 24 24"
                fill="none"
                className="text-[#5EA68E]"
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
              <h2
                className="
                  text-2xl font-semibold
                  text-[#303030]
                "
              >
                Forgot password?
              </h2>

              <p
                className="
                  mt-2 text-sm
                  leading-6 text-gray-500
                "
              >
                Enter your registered email address
                and we&apos;ll send you a verification
                code.
              </p>
            </div>

            <form
              onSubmit={handleSendOtp}
              className="mt-7"
            >
              <label
                htmlFor="forgot-email"
                className="
                  mb-2 block text-sm
                  font-medium text-[#414042]
                "
              >
                Email address
              </label>

              <input
                id="forgot-email"
                type="email"
                value={email}
                onChange={(e) => {
                  setEmail(e.target.value);
                  clearMessages();
                }}
                placeholder="Enter your email"
                autoComplete="email"
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
              />

              {error && (
                <div
                  className="
                    mt-4 rounded-lg
                    border border-red-100
                    bg-red-50
                    px-3 py-2.5
                    text-center text-sm
                    text-red-600
                  "
                >
                  {error}
                </div>
              )}

              <button
                type="submit"
                disabled={
                  loading ||
                  !normalizedEmail
                }
                className="
                  mt-6 flex h-12
                  w-full items-center
                  justify-center
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
                {loading
                  ? "Sending..."
                  : "Send verification code"}
              </button>
            </form>
          </>
        )}

        {/* =====================================================
            STEP 2 - OTP + PASSWORD
        ===================================================== */}

        {step === "reset" && (
          <>
            <div
              className="
                mx-auto flex
                h-14 w-14
                items-center justify-center
                rounded-full
                bg-[#EEF7F3]
              "
            >
              <svg
                width="26"
                height="26"
                viewBox="0 0 24 24"
                fill="none"
                className="text-[#5EA68E]"
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
              <h2
                className="
                  text-2xl font-semibold
                  text-[#303030]
                "
              >
                Reset your password
              </h2>

              <p
                className="
                  mt-2 text-sm
                  leading-6 text-gray-500
                "
              >
                Enter the verification code sent to
              </p>

              <div
                className="
                  mt-3 inline-flex
                  max-w-full rounded-full
                  border border-[#D9EDE6]
                  bg-[#F4FAF7]
                  px-4 py-1.5
                "
              >
                <span
                  className="
                    truncate text-sm
                    font-semibold
                    text-[#37455F]
                  "
                >
                  {normalizedEmail}
                </span>
              </div>
            </div>

            <form
              onSubmit={handleResetPassword}
              className="mt-7"
            >
              <p
                className="
                  mb-3 text-sm
                  font-medium text-[#414042]
                "
              >
                Verification code
              </p>

              <div
                className="
                  flex items-center
                  justify-between gap-2
                "
              >
                {otp.map((digit, index) => (
                  <input
                    key={index}
                    ref={(element) => {
                      inputRefs.current[index] =
                        element;
                    }}
                    value={digit}
                    type="text"
                    inputMode="numeric"
                    pattern="[0-9]*"
                    maxLength={1}
                    autoComplete={
                      index === 0
                        ? "one-time-code"
                        : "off"
                    }
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
                    className="
                      h-14 w-12
                      rounded-xl
                      border border-gray-300
                      bg-white
                      text-center text-xl
                      font-semibold
                      text-[#37455F]
                      outline-none
                      transition
                      focus:border-[#5EA68E]
                      focus:ring-4
                      focus:ring-[#5EA68E]/10
                      sm:w-[52px]
                    "
                    aria-label={`OTP digit ${
                      index + 1
                    }`}
                  />
                ))}
              </div>

              {/* New password */}

              <div className="mt-5">
                <label
                  htmlFor="new-password"
                  className="
                    mb-2 block text-sm
                    font-medium text-[#414042]
                  "
                >
                  New password
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

              {/* Confirm password */}

              <div className="mt-4">
                <label
                  htmlFor="confirm-new-password"
                  className="
                    mb-2 block text-sm
                    font-medium text-[#414042]
                  "
                >
                  Confirm password
                </label>

                <input
                  id="confirm-new-password"
                  type="password"
                  value={confirmPassword}
                  onChange={(e) => {
                    setConfirmPassword(
                      e.target.value
                    );

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
                <div
                  className="
                    mt-4 rounded-lg
                    border border-green-100
                    bg-green-50
                    px-3 py-2.5
                    text-center text-sm
                    text-green-700
                  "
                >
                  {message}
                </div>
              )}

              {error && (
                <div
                  className="
                    mt-4 rounded-lg
                    border border-red-100
                    bg-red-50
                    px-3 py-2.5
                    text-center text-sm
                    text-red-600
                  "
                >
                  {error}
                </div>
              )}

              <button
                type="submit"
                disabled={
                  loading ||
                  otpValue.length !== 6 ||
                  !password ||
                  !confirmPassword
                }
                className="
                  mt-6 flex h-12
                  w-full items-center
                  justify-center
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
                {loading
                  ? "Resetting..."
                  : "Reset Password"}
              </button>

              <button
                type="button"
                onClick={() => {
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
                }}
                className="
                  mt-3 w-full
                  text-center text-sm
                  font-medium text-gray-500
                  hover:text-[#37455F]
                "
              >
                ← Change email
              </button>
            </form>
          </>
        )}

        {/* =====================================================
            SUCCESS
        ===================================================== */}

        {step === "success" && (
          <div className="py-3 text-center">
            <div
              className="
                mx-auto flex
                h-16 w-16
                items-center justify-center
                rounded-full
                bg-[#EEF7F3]
                text-[#5EA68E]
              "
            >
              <svg
                width="30"
                height="30"
                viewBox="0 0 24 24"
                fill="none"
              >
                <path
                  d="M5 12.5L9.2 16.5L19 7"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            </div>

            <h2
              className="
                mt-5 text-2xl
                font-semibold text-[#303030]
              "
            >
              Password updated
            </h2>

            <p
              className="
                mt-2 text-sm
                leading-6 text-gray-500
              "
            >
              Your password has been reset successfully.
              You can now sign in using your new password.
            </p>

            <button
              type="button"
              onClick={onClose}
              className="
                mt-6 flex h-12
                w-full items-center
                justify-center
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

export default ForgotPasswordModal;