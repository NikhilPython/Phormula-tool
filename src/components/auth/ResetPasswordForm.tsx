// // app/reset-password/[token]/page.tsx  (or your preferred location)
// "use client";

// import Link from "next/link";
// import React, { useMemo, useState } from "react";
// import { useRouter, useParams } from "next/navigation";
// import { ChevronLeftIcon } from "@/icons";
// import { useResetPasswordMutation } from "@/lib/api/authApi";

// export default function ResetPasswordPage() {
//   const router = useRouter();
//   const { token } = useParams<{ token: string }>();

//   const [password, setPassword] = useState("");
//   const [confirm, setConfirm] = useState("");
//   const [showPassword, setShowPassword] = useState(false);
//   const [showConfirm, setShowConfirm] = useState(false);
//   const [flash, setFlash] = useState<string>("");

//   const [resetPassword, { isLoading }] = useResetPasswordMutation();

//   // At least 8 chars, letters, numbers, and symbol
//   const passwordPattern = useMemo(
//     () => /^(?=.*[A-Za-z])(?=.*\d)(?=.*[@$!%*#?&])[A-Za-z\d@$!%*#?&]{8,}$/,
//     []
//   );

//   const errorText = useMemo(() => {
//     if (!password && !confirm) return "";
//     if (password !== confirm) return "Passwords do not match.";
//     if (password && !passwordPattern.test(password)) {
//       return "Password must be at least 8 characters long and include letters, numbers, and symbols.";
//     }
//     return "";
//   }, [password, confirm, passwordPattern]);

//   const onSubmit = async (e: React.FormEvent) => {
//     e.preventDefault();
//     setFlash("");

//     if (!token) {
//       setFlash("Invalid or missing token.");
//       return;
//     }
//     if (errorText) return;

//     try {
//       const res = await resetPassword({ token, password }).unwrap();
//       if (res?.success) {
//         setFlash(`✅ ${res.message ?? "Password reset successful."}`);
//         setTimeout(() => router.push("/signin"), 2000);
//       } else {
//         setFlash(res?.message || "Password reset failed.");
//       }
//     } catch (err: any) {
//       setFlash(err?.data?.message || "An error occurred. Please try again.");
//     }
//   };

//   return (
//     <div className="flex flex-col flex-1 lg:w-1/2 w-full">
      
//       {/* Centered column */}
//       <div className="flex flex-col justify-center flex-1 w-full max-w-md mx-auto">
//         <div>
//           <div className="mb-5 sm:mb-8">
//             <h1 className="mb-2 font-semibold text-gray-800 text-title-sm dark:text-white/90 sm:text-title-md">
//               Reset Your Password
//             </h1>
//             <p className="text-sm text-gray-500 dark:text-gray-400">
//               Enter a new password and confirm it to continue.
//             </p>
//           </div>

//           <form onSubmit={onSubmit} className="space-y-4">
//             {/* New password */}
//             <label className="flex items-center justify-between rounded-lg border px-4 py-3 bg-white dark:bg-gray-900 border-gray-300">
//               <input
//                 type={showPassword ? "text" : "password"}
//                 className="w-full bg-transparent outline-none text-sm text-gray-800 dark:text-white/90"
//                 placeholder="New Password"
//                 value={password}
//                 onChange={(e) => setPassword(e.target.value)}
//                 required
//                 autoComplete="new-password"
//               />
//               <button
//                 type="button"
//                 className="ml-3 text-gray-500 dark:text-gray-400"
//                 onClick={() => setShowPassword((v) => !v)}
//                 aria-label={showPassword ? "Hide password" : "Show password"}
//               >
//                 {showPassword ? (
//                   // eye-off
//                   <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
//                     <path d="M3 3l18 18M10.58 10.58A2 2 0 0012 14a2 2 0 001.42-.58" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
//                     <path d="M17.94 17.94C16.2 19.02 14.17 19.67 12 19.67 7 19.67 2.73 16.6 1 12c.61-1.55 1.57-2.94 2.79-4.1M9.9 4.24A10.45 10.45 0 0112 4.33c5 0 9.27 3.07 11 7.67a11.64 11.64 0 01-2.11 3.28" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
//                   </svg>
//                 ) : (
//                   // eye
//                   <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
//                     <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8S1 12 1 12z" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
//                     <circle cx="12" cy="12" r="3" stroke="currentColor" strokeWidth="2" />
//                   </svg>
//                 )}
//               </button>
//             </label>

//             {/* Confirm password */}
//             <label className="flex items-center justify-between rounded-lg border px-4 py-3 bg-white dark:bg-gray-900 border-gray-300">
//               <input
//                 type={showConfirm ? "text" : "password"}
//                 className="w-full bg-transparent outline-none text-sm text-gray-800 dark:text-white/90"
//                 placeholder="Confirm Password"
//                 value={confirm}
//                 onChange={(e) => setConfirm(e.target.value)}
//                 required
//                 autoComplete="new-password"
//               />
//               <button
//                 type="button"
//                 className="ml-3 text-gray-500 dark:text-gray-400"
//                 onClick={() => setShowConfirm((v) => !v)}
//                 aria-label={showConfirm ? "Hide confirm password" : "Show confirm password"}
//               >
//                 {showConfirm ? (
//                   // eye-off
//                   <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
//                     <path d="M3 3l18 18M10.58 10.58A2 2 0 0012 14a2 2 0 001.42-.58" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
//                     <path d="M17.94 17.94C16.2 19.02 14.17 19.67 12 19.67 7 19.67 2.73 16.6 1 12c.61-1.55 1.57-2.94 2.79-4.1M9.9 4.24A10.45 10.45 0 0112 4.33c5 0 9.27 3.07 11 7.67a11.64 11.64 0 01-2.11 3.28" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
//                   </svg>
//                 ) : (
//                   // eye
//                   <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
//                     <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8S1 12 1 12z" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
//                     <circle cx="12" cy="12" r="3" stroke="currentColor" strokeWidth="2" />
//                   </svg>
//                 )}
//               </button>
//             </label>

//             {(errorText || flash) && (
//               <p
//                 className={`text-sm ${
//                   (flash && flash.startsWith("✅")) ? "text-emerald-600" : "text-red-500"
//                 }`}
//                 aria-live="polite"
//               >
//                 {errorText || flash}
//               </p>
//             )}

//             <div className="mt-6 flex items-center justify-end gap-3">
//               <Link
//                 href="/signin"
//                 className="inline-flex justify-center rounded-lg bg-gray-200 px-4 py-2 text-sm font-medium text-gray-800 hover:bg-gray-300 dark:bg-white/10 dark:text-gray-200 dark:hover:bg-white/15"
//               >
//                 Back to Sign In
//               </Link>
//               <button
//                 type="submit"
//                 disabled={isLoading}
//                 className="inline-flex justify-center rounded-lg bg-[#2c3854] px-4 py-2 text-sm font-semibold text-[#f8edcf] hover:opacity-95 disabled:opacity-60"
//               >
//                 {isLoading ? "Resetting…" : "Reset Password"}
//               </button>
//             </div>
//           </form>
//         </div>
//       </div>
//     </div>
//   );
// }


















"use client";

import React, { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { useResetPasswordMutation } from "@/lib/api/authApi";
import PageBreadcrumb from "../common/PageBreadCrumb";
import Button from "../ui/button/Button";

type ResetPasswordFormProps = {
  token: string;
};

export default function ResetPasswordForm({ token }: ResetPasswordFormProps) {
  const router = useRouter();

  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirm, setShowConfirm] = useState(false);
  const [flash, setFlash] = useState<string>("");

  const [resetPassword, { isLoading }] = useResetPasswordMutation();

  const passwordPattern = useMemo(
    () => /^(?=.*[A-Za-z])(?=.*\d)(?=.*[@$!%*#?&])[A-Za-z\d@$!%*#?&]{8,}$/,
    []
  );

  const errorText = useMemo(() => {
    if (!password && !confirm) return "";
    if (password !== confirm) return "Passwords do not match.";
    if (password && !passwordPattern.test(password)) {
      return "Password must be at least 8 characters long and include letters, numbers, and symbols.";
    }
    return "";
  }, [password, confirm, passwordPattern]);

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setFlash("");

    if (!token) {
      setFlash("Invalid or missing token.");
      return;
    }
    if (errorText) return;

    try {
      const res = await resetPassword({ token, password }).unwrap();
      if (res?.success) {
        setFlash(`✅ ${res.message ?? "Password reset successful."}`);
        setTimeout(() => router.push("/signin"), 2000);
      } else {
        setFlash(res?.message || "Password reset failed.");
      }
    } catch (err: any) {
      setFlash(err?.data?.message || "An error occurred. Please try again.");
    }
  };

  const handleClose = () => {
    router.push("/signin");
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
      onClick={handleClose}   // close on backdrop click
    >
      <div
        className="bg-white dark:bg-gray-900 rounded-2xl shadow-xl w-full max-w-lg mx-4 p-6 relative"
        onClick={(e) => e.stopPropagation()} // don't close when clicking inside
      >
        {/* X button */}
        <button
          type="button"
          onClick={handleClose}
          className="absolute top-3 right-3 text-gray-500 hover:text-gray-700 text-sm"
        >
          ✕
        </button>

        <div className="mb-5 sm:mb-8">
          {/* <h1 className="mb-2 font-semibold text-gray-800 text-title-sm dark:text-white/90 sm:text-title-md">
            Reset Your Password
          </h1> */}

          <PageBreadcrumb pageTitle="Reset Your Password" align="center" variant="table"/>
          <p className="text-sm text-chacoal-500 text-center">
            Enter a new password and confirm it to continue.
          </p>
        </div>

        <form onSubmit={onSubmit} className="space-y-4">
          {/* New password */}
          <label className="flex items-center justify-between rounded-lg border px-4 py-3 bg-white dark:bg-gray-900 border-gray-300">
            <input
              type={showPassword ? "text" : "password"}
              className="w-full bg-transparent outline-none text-sm text-gray-800 dark:text-white/90"
              placeholder="New Password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              autoComplete="new-password"
            />
            <button
              type="button"
              className="ml-3 text-gray-500 dark:text-gray-400"
              onClick={() => setShowPassword((v) => !v)}
              aria-label={showPassword ? "Hide password" : "Show password"}
            >
              {/* your eye / eye-off SVGs here */}
            </button>
          </label>

          {/* Confirm password */}
          <label className="flex items-center justify-between rounded-lg border px-4 py-3 bg-white dark:bg-gray-900 border-gray-300">
            <input
              type={showConfirm ? "text" : "password"}
              className="w-full bg-transparent outline-none text-sm text-gray-800 dark:text-white/90"
              placeholder="Confirm Password"
              value={confirm}
              onChange={(e) => setConfirm(e.target.value)}
              required
              autoComplete="new-password"
            />
            <button
              type="button"
              className="ml-3 text-gray-500 dark:text-gray-400"
              onClick={() => setShowConfirm((v) => !v)}
              aria-label={showConfirm ? "Hide confirm password" : "Show confirm password"}
            >
              {/* your eye / eye-off SVGs here */}
            </button>
          </label>

          {(errorText || flash) && (
            <p
              className={`text-sm ${
                flash && flash.startsWith("✅") ? "text-green-500" : "text-red-500"
              }`}
              aria-live="polite"
            >
              {errorText || flash}
            </p>
          )}

          <div className="mt-6 flex items-center justify-end gap-3">
            <Button
              type="button"
              onClick={handleClose}
              variant="outline"
              size="sm"
              // className="inline-flex justify-center rounded-lg bg-gray-200 px-4 py-2 text-sm font-medium text-gray-800 hover:bg-gray-300 dark:bg-white/10 dark:text-gray-200 dark:hover:bg:white/15"
            >
              Back to Sign In
            </Button>
            <Button
              type="submit"
              disabled={isLoading}
              variant="primary"
              size="sm"
              // className="inline-flex justify-center rounded-lg bg-[#2c3854] px-4 py-2 text-sm font-semibold text-[#f8edcf] hover:opacity-95 disabled:opacity-60"
            >
              {isLoading ? "Resetting…" : "Reset Password"}
            </Button>
          </div>
        </form>
      </div>
    </div>
  );
}
