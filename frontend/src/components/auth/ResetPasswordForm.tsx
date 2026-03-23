// "use client";

// import React, { useMemo, useState } from "react";
// import { useRouter } from "next/navigation";
// import { useResetPasswordMutation } from "@/lib/api/authApi";
// import PageBreadcrumb from "../common/PageBreadCrumb";
// import Button from "../ui/button/Button";

// type ResetPasswordFormProps = {
//   token: string;
// };

// export default function ResetPasswordForm({ token }: ResetPasswordFormProps) {
//   const router = useRouter();

//   const [password, setPassword] = useState("");
//   const [confirm, setConfirm] = useState("");
//   const [showPassword, setShowPassword] = useState(false);
//   const [showConfirm, setShowConfirm] = useState(false);
//   const [flash, setFlash] = useState<string>("");

//   const [resetPassword, { isLoading }] = useResetPasswordMutation();

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

//   const handleClose = () => {
//     router.push("/signin");
//   };

//   return (
//     <div
//       className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
//       onClick={handleClose}   // close on backdrop click
//     >
//       <div
//         className="bg-white dark:bg-gray-900 rounded-2xl shadow-xl w-full max-w-lg mx-4 p-6 relative"
//         onClick={(e) => e.stopPropagation()} // don't close when clicking inside
//       >
//         {/* X button */}
//         <button
//           type="button"
//           onClick={handleClose}
//           className="absolute top-3 right-3 text-gray-500 hover:text-gray-700 text-sm"
//         >
//           ✕
//         </button>

//         <div className="mb-5 sm:mb-8">
//           {/* <h1 className="mb-2 font-semibold text-gray-800 text-title-sm dark:text-white/90 sm:text-title-md">
//             Reset Your Password
//           </h1> */}

//           <PageBreadcrumb pageTitle="Reset Your Password" align="center" variant="table"/>
//           <p className="text-sm text-chacoal-500 text-center">
//             Enter a new password and confirm it to continue.
//           </p>
//         </div>

//         <form onSubmit={onSubmit} className="space-y-4">
//           {/* New password */}
//           <label className="flex items-center justify-between rounded-lg border px-4 py-3 bg-white dark:bg-gray-900 border-gray-300">
//             <input
//               type={showPassword ? "text" : "password"}
//               className="w-full bg-transparent outline-none text-sm text-gray-800 dark:text-white/90"
//               placeholder="New Password"
//               value={password}
//               onChange={(e) => setPassword(e.target.value)}
//               required
//               autoComplete="new-password"
//             />
//             <button
//               type="button"
//               className="ml-3 text-gray-500 dark:text-gray-400"
//               onClick={() => setShowPassword((v) => !v)}
//               aria-label={showPassword ? "Hide password" : "Show password"}
//             >
//               {/* your eye / eye-off SVGs here */}
//             </button>
//           </label>

//           {/* Confirm password */}
//           <label className="flex items-center justify-between rounded-lg border px-4 py-3 bg-white dark:bg-gray-900 border-gray-300">
//             <input
//               type={showConfirm ? "text" : "password"}
//               className="w-full bg-transparent outline-none text-sm text-gray-800 dark:text-white/90"
//               placeholder="Confirm Password"
//               value={confirm}
//               onChange={(e) => setConfirm(e.target.value)}
//               required
//               autoComplete="new-password"
//             />
//             <button
//               type="button"
//               className="ml-3 text-gray-500 dark:text-gray-400"
//               onClick={() => setShowConfirm((v) => !v)}
//               aria-label={showConfirm ? "Hide confirm password" : "Show confirm password"}
//             >
//               {/* your eye / eye-off SVGs here */}
//             </button>
//           </label>

//           {(errorText || flash) && (
//             <p
//               className={`text-sm ${
//                 flash && flash.startsWith("✅") ? "text-green-500" : "text-red-500"
//               }`}
//               aria-live="polite"
//             >
//               {errorText || flash}
//             </p>
//           )}

//           <div className="mt-6 flex items-center justify-end gap-3">
//             <Button
//               type="button"
//               onClick={handleClose}
//               variant="outline"
//               size="sm"
//               // className="inline-flex justify-center rounded-lg bg-gray-200 px-4 py-2 text-sm font-medium text-gray-800 hover:bg-gray-300 dark:bg-white/10 dark:text-gray-200 dark:hover:bg:white/15"
//             >
//               Back to Sign In
//             </Button>
//             <Button
//               type="submit"
//               disabled={isLoading}
//               variant="primary"
//               size="sm"
//               // className="inline-flex justify-center rounded-lg bg-[#2c3854] px-4 py-2 text-sm font-semibold text-[#f8edcf] hover:opacity-95 disabled:opacity-60"
//             >
//               {isLoading ? "Resetting…" : "Reset Password"}
//             </Button>
//           </div>
//         </form>
//       </div>
//     </div>
//   );
// }















"use client";

import React, { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { Eye, EyeOff } from "lucide-react";
import { useResetPasswordMutation } from "@/lib/api/authApi";
import PageBreadcrumb from "../common/PageBreadCrumb";
import Button from "../ui/button/Button";
import {
  resetPasswordSchema,
  sanitizePassword,
} from "@/lib/validations/authValidation"; 

type ResetPasswordFormProps = {
  token: string;
};

type FieldErrors = {
  password?: string;
  confirm?: string;
};

export default function ResetPasswordForm({ token }: ResetPasswordFormProps) {
  const router = useRouter();

  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirm, setShowConfirm] = useState(false);
  const [flash, setFlash] = useState("");
  const [errors, setErrors] = useState<FieldErrors>({});
  const [submitted, setSubmitted] = useState(false);

  const [resetPassword, { isLoading }] = useResetPasswordMutation();

  const passwordChecks = useMemo(() => {
    return {
      minLength: password.length >= 6,
      numbers: (password.match(/\d/g) || []).length >= 2,
      alphabets: (password.match(/[a-zA-Z]/g) || []).length >= 2,
      special: /[^A-Za-z0-9]/.test(password),
    };
  }, [password]);

  const passwordHint = useMemo(() => {
    if (!password) return "";

    if (!passwordChecks.minLength) return "At least 6 characters";
    if (!passwordChecks.numbers) return "At least 2 numbers";
    if (!passwordChecks.alphabets) return "At least 2 alphabets";
    if (!passwordChecks.special) return "At least 1 special character";

    return "";
  }, [password, passwordChecks]);

  const validateOnSubmit = () => {
    const result = resetPasswordSchema.safeParse({ password, confirm });

    if (result.success) {
      setErrors({});
      return true;
    }

    const fieldErrors = result.error.flatten().fieldErrors;

    setErrors({
      password: fieldErrors.password?.[0],
      confirm: fieldErrors.confirm?.[0],
    });

    return false;
  };

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitted(true);
    setFlash("");

    if (!token) {
      setFlash("Invalid or missing token.");
      return;
    }

    const isValid = validateOnSubmit();
    if (!isValid) return;

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

  const passwordErrorToShow = passwordHint || (submitted ? errors.password : "");
  const confirmErrorToShow = submitted ? errors.confirm : "";

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
      onClick={handleClose}
    >
      <div
        className="bg-white dark:bg-gray-900 rounded-2xl shadow-xl w-full max-w-lg mx-4 p-6 relative"
        onClick={(e) => e.stopPropagation()}
      >
        <button
          type="button"
          onClick={handleClose}
          className="absolute top-3 right-3 text-gray-500 hover:text-gray-700 text-sm"
          aria-label="Close"
        >
          ✕
        </button>

        <div className="mb-5 sm:mb-8">
          <PageBreadcrumb
            pageTitle="Reset Your Password"
            align="center"
            variant="table"
          />
          <p className="text-sm text-chacoal-500 text-center">
            Enter a new password and confirm it to continue.
          </p>
        </div>

        <form onSubmit={onSubmit} className="space-y-4">
          {/* New password */}
          <div>
            <label
              className={`flex items-center justify-between rounded-lg border px-4 py-3 bg-white dark:bg-gray-900 ${
                passwordErrorToShow
                  ? "border-red-400"
                  : "border-gray-300"
              }`}
            >
              <input
                type={showPassword ? "text" : "password"}
                className="w-full bg-transparent outline-none text-sm text-gray-800 dark:text-white/90"
                placeholder="New Password"
                value={password}
                onChange={(e) => {
                  const value = sanitizePassword(e.target.value);
                  setPassword(value);
                  setFlash("");

                  setErrors((prev) => ({
                    ...prev,
                    password: undefined,
                  }));
                }}
                autoComplete="new-password"
              />

              <button
                type="button"
                className="ml-3 text-gray-500 dark:text-gray-400"
                onClick={() => setShowPassword((v) => !v)}
                aria-label={showPassword ? "Hide password" : "Show password"}
              >
                {showPassword ? (
                  <EyeOff className="h-5 w-5" />
                ) : (
                  <Eye className="h-5 w-5" />
                )}
              </button>
            </label>

            {passwordErrorToShow && (
              <p className="mt-1 px-1 text-sm text-red-500">
                {passwordErrorToShow}
              </p>
            )}
          </div>

          {/* Confirm password */}
          <div>
            <label
              className={`flex items-center justify-between rounded-lg border px-4 py-3 bg-white dark:bg-gray-900 ${
                confirmErrorToShow
                  ? "border-red-400"
                  : "border-gray-300"
              }`}
            >
              <input
                type={showConfirm ? "text" : "password"}
                className="w-full bg-transparent outline-none text-sm text-gray-800 dark:text-white/90"
                placeholder="Confirm Password"
                value={confirm}
                onChange={(e) => {
                  const value = sanitizePassword(e.target.value);
                  setConfirm(value);
                  setFlash("");

                  setErrors((prev) => ({
                    ...prev,
                    confirm: undefined,
                  }));
                }}
                autoComplete="new-password"
              />

              <button
                type="button"
                className="ml-3 text-gray-500 dark:text-gray-400"
                onClick={() => setShowConfirm((v) => !v)}
                aria-label={
                  showConfirm ? "Hide confirm password" : "Show confirm password"
                }
              >
                {showConfirm ? (
                  <EyeOff className="h-5 w-5" />
                ) : (
                  <Eye className="h-5 w-5" />
                )}
              </button>
            </label>

            {confirmErrorToShow && (
              <p className="mt-1 px-1 text-sm text-red-500">
                {confirmErrorToShow}
              </p>
            )}
          </div>

          {flash && (
            <p
              className={`text-sm ${
                flash.startsWith("✅") ? "text-green-500" : "text-red-500"
              }`}
              aria-live="polite"
            >
              {flash}
            </p>
          )}

          <div className="mt-6 flex items-center justify-end gap-3">
            <Button
              type="button"
              onClick={handleClose}
              variant="outline"
              size="sm"
            >
              Back to Sign In
            </Button>

            <Button
              type="submit"
              disabled={isLoading}
              variant="primary"
              size="sm"
            >
              {isLoading ? "Resetting…" : "Reset Password"}
            </Button>
          </div>
        </form>
      </div>
    </div>
  );
}