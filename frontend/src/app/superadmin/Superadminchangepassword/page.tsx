"use client";

import React, { useState } from "react";
import { useRouter } from "next/navigation";
import { Eye, EyeOff, Lock, Shield, CheckCircle, XCircle } from "lucide-react";

type FormData = {
  currentPassword: string;
  newPassword: string;
  confirmPassword: string;
};

type ShowPasswords = {
  current: boolean;
  new: boolean;
  confirm: boolean;
};

type Errors = Partial<Record<keyof FormData, string>>;

type ApiResponse = {
  message?: string;
};

export default function SuperadminResetPasswordPage() {
  const [formData, setFormData] = useState<FormData>({
    currentPassword: "",
    newPassword: "",
    confirmPassword: "",
  });

  const [showPasswords, setShowPasswords] = useState<ShowPasswords>({
    current: false,
    new: false,
    confirm: false,
  });

  const [loading, setLoading] = useState<boolean>(false);
  const [message, setMessage] = useState<string>("");
  const [messageType, setMessageType] = useState<"" | "success" | "error">("");
  const [errors, setErrors] = useState<Errors>({});

  const router = useRouter();

  const validatePassword = (password: string): string[] => {
    const errs: string[] = [];
    if (password.length < 8) errs.push("At least 8 characters");
    if (!/(?=.*[a-z])/.test(password)) errs.push("One lowercase letter");
    if (!/(?=.*[A-Z])/.test(password)) errs.push("One uppercase letter");
    if (!/(?=.*\d)/.test(password)) errs.push("One number");
    if (!/(?=.*[@$!%*?&])/.test(password)) errs.push("One special character");
    return errs;
  };

  const handleInputChange = (field: keyof FormData, value: string) => {
    setFormData((prev) => ({ ...prev, [field]: value }));

    if (errors[field]) setErrors((prev) => ({ ...prev, [field]: "" }));

    if (message) {
      setMessage("");
      setMessageType("");
    }
  };

  const togglePasswordVisibility = (field: keyof ShowPasswords) => {
    setShowPasswords((prev) => ({ ...prev, [field]: !prev[field] }));
  };

  const validateForm = (): boolean => {
    const newErrors: Errors = {};

    if (!formData.currentPassword) {
      newErrors.currentPassword = "Current password is required";
    }

    if (!formData.newPassword) {
      newErrors.newPassword = "New password is required";
    } else {
      const pwErrs = validatePassword(formData.newPassword);
      if (pwErrs.length > 0) {
        newErrors.newPassword = "Password must contain: " + pwErrs.join(", ");
      }
    }

    if (!formData.confirmPassword) {
      newErrors.confirmPassword = "Please confirm your new password";
    } else if (formData.newPassword !== formData.confirmPassword) {
      newErrors.confirmPassword = "Passwords do not match";
    }

    if (formData.currentPassword === formData.newPassword && formData.newPassword) {
      newErrors.newPassword = "New password must be different from current password";
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!validateForm()) return;

    setLoading(true);
    setMessage("");
    setMessageType("");

    try {
      const token = localStorage.getItem("superadmin_token");
      if (!token) {
        setMessage("Authentication required. Please log in again.");
        setMessageType("error");
        setLoading(false);
        return;
      }

      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin_change_password`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            current_password: formData.currentPassword,
            new_password: formData.newPassword,
          }),
        }
      );

      const data = (await response.json()) as ApiResponse;

      if (response.ok) {
        setMessage(data.message || "Password changed successfully!");
        setMessageType("success");
        setFormData({ currentPassword: "", newPassword: "", confirmPassword: "" });
        router.push("/superadmin/SuperAdminDashboard");
      } else {
        setMessage(data.message || "Failed to change password. Please try again.");
        setMessageType("error");
      }
    } catch (err) {
      setMessage("Network error. Please check your connection and try again.");
      setMessageType("error");
    } finally {
      setLoading(false);
    }
  };

  const passwordStrength = (password: string): { strength: number; text: string } => {
    const score = validatePassword(password);
    if (password.length === 0) return { strength: 0, text: "" };
    if (score.length === 0) return { strength: 100, text: "Strong" };
    if (score.length <= 2) return { strength: 60, text: "Medium" };
    return { strength: 30, text: "Weak" };
  };

  const strength = passwordStrength(formData.newPassword);

  return (
    <>
      <style jsx global>{`
        html,
        body {
          overflow: hidden;
        }
      `}</style>

      <div className="flex h-[calc(100dvh-72px)] min-h-0 items-center justify-center overflow-hidden bg-[#37384f] px-4 py-4">
        <div className="grid h-full max-h-[720px] w-full max-w-[1080px] overflow-hidden rounded-[28px] border border-white/10 bg-[#484962] text-white shadow-[0_28px_70px_rgba(20,22,45,0.45)] lg:grid-cols-[0.9fr_1.1fr]">
          {/* LEFT INFO SECTION */}
          <div className="relative hidden h-full border-r border-white/10 bg-[#42435c] px-8 py-8 lg:flex lg:flex-col lg:justify-between">
            <div>
              <div className="flex h-16 w-16 items-center justify-center rounded-2xl border border-[#31d9e5]/25 bg-[#31d9e5]/15 text-[#31d9e5]">
                <Shield size={34} />
              </div>

              <h1 className="mt-4 max-w-[360px] text-3xl font-bold leading-[1.15] tracking-[-0.03em] text-white">
                Keep your Super Admin account secure.
              </h1>

              <p className="mt-2 max-w-[360px] text-sm leading-5 text-white/60">
                Update your password regularly to protect access to dashboards,
                admin controls, marketplace data, and user management tools.
              </p>
            </div>

            <div className="rounded-2xl border border-white/10 bg-white/[0.05] p-5">
              <p className="text-sm font-semibold text-white">
                Security Requirements
              </p>

              <ul className="mt-4 space-y-3 text-sm text-white/65">
                <li className="flex gap-3">
                  <CheckCircle className="mt-0.5 h-4 w-4 shrink-0 text-[#31d9e5]" />
                  At least 8 characters long
                </li>
                <li className="flex gap-3">
                  <CheckCircle className="mt-0.5 h-4 w-4 shrink-0 text-[#31d9e5]" />
                  Contains uppercase and lowercase letters
                </li>
                <li className="flex gap-3">
                  <CheckCircle className="mt-0.5 h-4 w-4 shrink-0 text-[#31d9e5]" />
                  Contains at least one number
                </li>
                <li className="flex gap-3">
                  <CheckCircle className="mt-0.5 h-4 w-4 shrink-0 text-[#31d9e5]" />
                  Contains at least one special character
                </li>
              </ul>
            </div>

            <div className="absolute -bottom-20 -left-20 h-56 w-56 rounded-full bg-[#31d9e5]/10 blur-3xl" />
            <div className="absolute -right-16 top-20 h-44 w-44 rounded-full bg-white/10 blur-3xl" />
          </div>

          {/* RIGHT FORM SECTION */}
          <div className="flex min-h-0 items-center justify-center px-6 py-4 sm:px-10 lg:px-14">
            <div className="w-full max-w-[430px]">
              <div className="mb-5 text-center">
                <div className="mx-auto mb-4 flex h-16 w-16 items-center justify-center rounded-2xl border border-[#31d9e5]/25 bg-[#31d9e5]/15 text-[#31d9e5] lg:hidden">
                  <Shield size={32} />
                </div>

                <h1 className="text-2xl font-bold tracking-tight text-white sm:text-3xl">
                  Change Password
                </h1>

                <p className="mt-2 text-sm text-white/60">
                  Update your SuperAdmin password
                </p>
              </div>

              {message && (
                <div
                  className={`mb-5 flex items-center gap-2 rounded-xl border px-4 py-3 text-sm ${messageType === "success"
                      ? "border-emerald-300/25 bg-emerald-500/10 text-emerald-100"
                      : "border-red-300/25 bg-red-500/10 text-red-100"
                    }`}
                >
                  {messageType === "success" ? (
                    <CheckCircle size={18} />
                  ) : (
                    <XCircle size={18} />
                  )}

                  <span className="font-medium">{message}</span>
                </div>
              )}

              <form onSubmit={handleSubmit} className="space-y-3">
                {/* Current Password */}
                <div>
                  <label className="mb-2 block text-sm font-semibold text-white/85">
                    Current Password
                  </label>

                  <div className="relative">
                    <Lock
                      className="absolute left-3 top-1/2 -translate-y-1/2 text-white/45"
                      size={20}
                    />

                    <input
                      type={showPasswords.current ? "text" : "password"}
                      value={formData.currentPassword}
                      onChange={(e) =>
                        handleInputChange("currentPassword", e.target.value)
                      }
                      placeholder="Enter your current password"
                      required
                      className={`h-12 w-full rounded-xl border bg-white/[0.06] pl-10 pr-12 text-sm text-white outline-none transition placeholder:text-white/35 ${errors.currentPassword
                          ? "border-red-300/60 focus:border-red-300 focus:ring-4 focus:ring-red-500/10"
                          : "border-white/10 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15"
                        }`}
                    />

                    <button
                      type="button"
                      onClick={() => togglePasswordVisibility("current")}
                      className="absolute right-3 top-1/2 -translate-y-1/2 text-white/45 transition hover:text-[#31d9e5]"
                    >
                      {showPasswords.current ? (
                        <EyeOff size={20} />
                      ) : (
                        <Eye size={20} />
                      )}
                    </button>
                  </div>

                  {errors.currentPassword && (
                    <p className="mt-1 text-sm text-red-200">
                      {errors.currentPassword}
                    </p>
                  )}
                </div>

                {/* New Password */}
                <div>
                  <label className="mb-2 block text-sm font-semibold text-white/85">
                    New Password
                  </label>

                  <div className="relative">
                    <Lock
                      className="absolute left-3 top-1/2 -translate-y-1/2 text-white/45"
                      size={20}
                    />

                    <input
                      type={showPasswords.new ? "text" : "password"}
                      value={formData.newPassword}
                      onChange={(e) =>
                        handleInputChange("newPassword", e.target.value)
                      }
                      placeholder="Enter a new secure password"
                      required
                      className={`h-12 w-full rounded-xl border bg-white/[0.06] pl-10 pr-12 text-sm text-white outline-none transition placeholder:text-white/35 ${errors.newPassword
                          ? "border-red-300/60 focus:border-red-300 focus:ring-4 focus:ring-red-500/10"
                          : "border-white/10 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15"
                        }`}
                    />

                    <button
                      type="button"
                      onClick={() => togglePasswordVisibility("new")}
                      className="absolute right-3 top-1/2 -translate-y-1/2 text-white/45 transition hover:text-[#31d9e5]"
                    >
                      {showPasswords.new ? (
                        <EyeOff size={20} />
                      ) : (
                        <Eye size={20} />
                      )}
                    </button>
                  </div>

                  {formData.newPassword && (
                    <div className="mt-2">
                      <div className="h-1.5 w-full overflow-hidden rounded-full bg-white/10">
                        <div
                          className={`h-full transition-all ${strength.strength >= 60
                              ? "bg-[#31d9e5]"
                              : strength.strength >= 30
                                ? "bg-amber-400"
                                : "bg-red-400"
                            }`}
                          style={{ width: `${strength.strength}%` }}
                        />
                      </div>

                      {strength.text && (
                        <p
                          className={`mt-1 text-xs ${strength.strength >= 60
                              ? "text-[#31d9e5]"
                              : strength.strength >= 30
                                ? "text-amber-200"
                                : "text-red-200"
                            }`}
                        >
                          Password strength: {strength.text}
                        </p>
                      )}
                    </div>
                  )}

                  {errors.newPassword && (
                    <p className="mt-1 text-sm text-red-200">
                      {errors.newPassword}
                    </p>
                  )}
                </div>

                {/* Confirm Password */}
                <div>
                  <label className="mb-2 block text-sm font-semibold text-white/85">
                    Confirm New Password
                  </label>

                  <div className="relative">
                    <Lock
                      className="absolute left-3 top-1/2 -translate-y-1/2 text-white/45"
                      size={20}
                    />

                    <input
                      type={showPasswords.confirm ? "text" : "password"}
                      value={formData.confirmPassword}
                      onChange={(e) =>
                        handleInputChange("confirmPassword", e.target.value)
                      }
                      placeholder="Confirm your new password"
                      required
                      className={`h-12 w-full rounded-xl border bg-white/[0.06] pl-10 pr-12 text-sm text-white outline-none transition placeholder:text-white/35 ${errors.confirmPassword
                          ? "border-red-300/60 focus:border-red-300 focus:ring-4 focus:ring-red-500/10"
                          : "border-white/10 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15"
                        }`}
                    />

                    <button
                      type="button"
                      onClick={() => togglePasswordVisibility("confirm")}
                      className="absolute right-3 top-1/2 -translate-y-1/2 text-white/45 transition hover:text-[#31d9e5]"
                    >
                      {showPasswords.confirm ? (
                        <EyeOff size={20} />
                      ) : (
                        <Eye size={20} />
                      )}
                    </button>
                  </div>

                  {errors.confirmPassword && (
                    <p className="mt-1 text-sm text-red-200">
                      {errors.confirmPassword}
                    </p>
                  )}
                </div>

                <button
                  type="submit"
                  disabled={loading}
                  className="inline-flex h-12 w-full items-center justify-center gap-2 rounded-xl bg-[#31d9e5] px-5 text-sm font-semibold text-[#303247] shadow-[0_10px_22px_rgba(20,220,230,0.20)] transition hover:-translate-y-0.5 hover:bg-[#28cbd6] disabled:cursor-not-allowed disabled:translate-y-0 disabled:opacity-60"
                >
                  {loading ? (
                    <>
                      <span className="h-5 w-5 animate-spin rounded-full border-2 border-[#303247]/30 border-t-[#303247]" />
                      Changing Password...
                    </>
                  ) : (
                    <>
                      <Shield size={18} />
                      Change Password
                    </>
                  )}
                </button>
              </form>

              <div className="mt-5 rounded-xl border border-white/10 bg-white/[0.05] p-4 lg:hidden">
                <p className="mb-2 text-sm font-semibold text-white">
                  Security Requirements:
                </p>

                <ul className="list-disc space-y-1 pl-5 text-xs leading-5 text-white/60">
                  <li>At least 8 characters long</li>
                  <li>Contains uppercase and lowercase letters</li>
                  <li>Contains at least one number</li>
                  <li>Contains at least one special character</li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
