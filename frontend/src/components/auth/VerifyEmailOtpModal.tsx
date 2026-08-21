"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";

import {
  useResendVerificationOtpMutation,
  useVerifyEmailOtpMutation,
} from "@/lib/api/authApi";

import { useAppDispatch } from "@/lib/store";
import {
  setCredentials,
  setUser,
} from "@/lib/features/auth/authSlice";

type VerifyEmailOtpModalProps = {
  email: string;
  open: boolean;
  onClose?: () => void;
};

export default function VerifyEmailOtpModal({
  email,
  open,
  onClose,
}: VerifyEmailOtpModalProps) {
  const router = useRouter();
  const dispatch = useAppDispatch();

  const normalizedEmail = email.trim().toLowerCase();

  const [otp, setOtp] = useState<string[]>([
    "",
    "",
    "",
    "",
    "",
    "",
  ]);

  const [message, setMessage] = useState("");
  const [errorMessage, setErrorMessage] = useState("");
  const [resendSeconds, setResendSeconds] = useState(60);

  const inputRefs = useRef<Array<HTMLInputElement | null>>([]);

  const [verifyEmailOtp, { isLoading: isVerifying }] =
    useVerifyEmailOtpMutation();

  const [resendVerificationOtp, { isLoading: isResending }] =
    useResendVerificationOtpMutation();

  const otpValue = otp.join("");

  useEffect(() => {
    if (!open) return;

    setOtp(["", "", "", "", "", ""]);
    setMessage("");
    setErrorMessage("");
    setResendSeconds(60);

    const focusTimer = window.setTimeout(() => {
      inputRefs.current[0]?.focus();
    }, 100);

    return () => window.clearTimeout(focusTimer);
  }, [open]);

  useEffect(() => {
    if (!open || resendSeconds <= 0) return;

    const timer = window.setInterval(() => {
      setResendSeconds((prev) => {
        if (prev <= 1) {
          window.clearInterval(timer);
          return 0;
        }

        return prev - 1;
      });
    }, 1000);

    return () => window.clearInterval(timer);
  }, [open, resendSeconds]);

  const clearMessages = () => {
    setMessage("");
    setErrorMessage("");
  };

  const handleOtpChange = (index: number, value: string) => {
    const digits = value.replace(/\D/g, "");

    /*
     * Mobile/browser OTP autofill can sometimes put the
     * complete OTP inside the first input.
     */
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

      inputRefs.current[Math.max(focusIndex, 0)]?.focus();

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

  const handleKeyDown = (
    index: number,
    event: React.KeyboardEvent<HTMLInputElement>
  ) => {
    if (event.key === "Backspace") {
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

    if (event.key === "ArrowLeft" && index > 0) {
      event.preventDefault();
      inputRefs.current[index - 1]?.focus();
    }

    if (event.key === "ArrowRight" && index < 5) {
      event.preventDefault();
      inputRefs.current[index + 1]?.focus();
    }
  };

  const handlePaste = (
    event: React.ClipboardEvent<HTMLInputElement>
  ) => {
    event.preventDefault();

    const pasted = event.clipboardData
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

    inputRefs.current[Math.max(focusIndex, 0)]?.focus();
  };

  const handleVerify = async (
    event: React.FormEvent<HTMLFormElement>
  ) => {
    event.preventDefault();

    clearMessages();

    if (!normalizedEmail) {
      setErrorMessage("Email address is missing.");
      return;
    }

    if (otpValue.length !== 6) {
      setErrorMessage(
        "Please enter the complete 6-digit verification code."
      );
      return;
    }

    try {
      const response = await verifyEmailOtp({
        email: normalizedEmail,
        otp: otpValue,
      }).unwrap();

      if (!response?.token) {
        setErrorMessage(
          "Email verified, but authentication token was not returned."
        );
        return;
      }

      /*
       * Same authentication handling as your SignInForm.
       */
      dispatch(
        setCredentials({
          token: response.token,
        })
      );

      localStorage.setItem(
        "jwtToken",
        response.token
      );

      dispatch(
        setUser({
          id: response.user_id,
          email: normalizedEmail,
          is_member: false,
        })
      );

      setMessage(
        response?.message ||
          "Email verified successfully."
      );

      /*
       * New signup has no Amazon/data yet.
       * Send directly to NA/NA dashboard.
       */
      window.setTimeout(() => {
        router.replace(
          "/live-dashboard/global/NA/NA"
        );
      }, 700);
    } catch (error: any) {
      setErrorMessage(
        error?.data?.message ||
          error?.data?.error ||
          error?.error ||
          "Invalid or expired verification code."
      );
    }
  };

  const handleResend = async () => {
    if (
      !normalizedEmail ||
      resendSeconds > 0 ||
      isResending
    ) {
      return;
    }

    clearMessages();

    try {
      const response =
        await resendVerificationOtp({
          email: normalizedEmail,
        }).unwrap();

      setOtp(["", "", "", "", "", ""]);
      setResendSeconds(60);

      setMessage(
        response?.message ||
          "A new verification code has been sent."
      );

      window.setTimeout(() => {
        inputRefs.current[0]?.focus();
      }, 50);
    } catch (error: any) {
      setErrorMessage(
        error?.data?.message ||
          error?.data?.error ||
          error?.error ||
          "Unable to resend verification code."
      );
    }
  };

  if (!open) {
    return null;
  }

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
        aria-labelledby="verify-email-title"
        className="
          relative w-full max-w-[430px]
          rounded-2xl
          border border-gray-200
          bg-white
          px-7 py-8
          shadow-2xl
          sm:px-9
        "
      >
        {/* Optional close */}
        {onClose && (
          <button
            type="button"
            onClick={onClose}
            disabled={isVerifying}
            aria-label="Close verification"
            className="
              absolute right-4 top-4
              flex h-8 w-8
              items-center justify-center
              rounded-full
              text-lg text-gray-400
              transition
              hover:bg-gray-100
              hover:text-gray-600
              disabled:cursor-not-allowed
            "
          >
            ×
          </button>
        )}

        {/* Mail icon */}
        <div
          className="
            mx-auto flex h-14 w-14
            items-center justify-center
            rounded-full bg-[#EEF7F3]
          "
        >
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

        {/* Heading */}
        <div className="mt-5 text-center">
          <h2
            id="verify-email-title"
            className="
              text-2xl font-semibold
              tracking-tight text-[#303030]
            "
          >
            Verify your email
          </h2>

          <p
            className="
              mt-2 text-sm
              leading-6 text-gray-500
            "
          >
            We&apos;ve sent a 6-digit verification code to
          </p>

          <div
            className="
              mt-3 inline-flex max-w-full
              rounded-full
              border border-[#D9EDE6]
              bg-[#F4FAF7]
              px-4 py-1.5
            "
          >
            <span
              className="
                truncate text-sm
                font-semibold text-[#37455F]
              "
            >
              {normalizedEmail}
            </span>
          </div>
        </div>

        <form
          onSubmit={handleVerify}
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
                type="text"
                value={digit}
                onChange={(event) =>
                  handleOtpChange(
                    index,
                    event.target.value
                  )
                }
                onKeyDown={(event) =>
                  handleKeyDown(index, event)
                }
                onPaste={handlePaste}
                inputMode="numeric"
                pattern="[0-9]*"
                autoComplete={
                  index === 0
                    ? "one-time-code"
                    : "off"
                }
                maxLength={1}
                disabled={isVerifying}
                aria-label={`OTP digit ${
                  index + 1
                }`}
                className="
                  h-14 w-12
                  rounded-xl
                  border border-gray-300
                  bg-white
                  text-center text-xl
                  font-semibold text-[#37455F]
                  outline-none
                  transition-all

                  focus:border-[#5EA68E]
                  focus:ring-4
                  focus:ring-[#5EA68E]/10

                  disabled:cursor-not-allowed
                  disabled:bg-gray-50

                  sm:w-[52px]
                "
              />
            ))}
          </div>

          {errorMessage && (
            <div
              className="
                mt-4 rounded-lg
                border border-red-100
                bg-red-50
                px-3 py-2.5
                text-center text-sm
                text-red-600
              "
              aria-live="polite"
            >
              {errorMessage}
            </div>
          )}

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
              aria-live="polite"
            >
              {message}
            </div>
          )}

          <button
            type="submit"
            disabled={
              isVerifying ||
              otpValue.length !== 6
            }
            className="
              mt-6 flex h-12 w-full
              items-center justify-center
              rounded-xl
              bg-[#37455F]
              px-4
              text-sm font-semibold
              text-[#F8EDCE]
              transition-all

              hover:bg-[#2F3B52]
              active:scale-[0.99]

              disabled:cursor-not-allowed
              disabled:bg-gray-300
              disabled:text-white
            "
          >
            {isVerifying
              ? "Verifying..."
              : "Verify Email"}
          </button>
        </form>

        {/* Resend */}
        <div className="mt-5 text-center">
          <p className="text-sm text-gray-500">
            Didn&apos;t receive the code?
          </p>

          <button
            type="button"
            onClick={handleResend}
            disabled={
              resendSeconds > 0 ||
              isResending
            }
            className="
              mt-1 text-sm
              font-semibold text-[#5EA68E]
              transition
              hover:text-[#4D927B]

              disabled:cursor-not-allowed
              disabled:text-gray-400
            "
          >
            {isResending
              ? "Sending new code..."
              : resendSeconds > 0
                ? `Resend code in ${resendSeconds}s`
                : "Resend verification code"}
          </button>
        </div>

        <p
          className="
            mt-6 text-center
            text-xs text-gray-400
          "
        >
          For your security, this verification code will
          expire shortly.
        </p>
      </div>
    </div>
  );
}