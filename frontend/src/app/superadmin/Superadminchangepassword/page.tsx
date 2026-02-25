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

    if (!formData.currentPassword) newErrors.currentPassword = "Current password is required";

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

      const response = await fetch("http://127.0.0.1:5000/superadmin_change_password", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          current_password: formData.currentPassword,
          new_password: formData.newPassword,
        }),
      });

      const data = (await response.json()) as ApiResponse;

      if (response.ok) {
        setMessage(data.message || "Password changed successfully!");
        setMessageType("success");
        router.push("/superadmin/SuperAdminDashboard");
        setFormData({ currentPassword: "", newPassword: "", confirmPassword: "" });
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
    <div className="min-h-screen bg-[#5EA68E] flex items-center justify-center p-4">
      <div className="w-full max-w-md rounded-2xl bg-white shadow-2xl p-8">
        <div className="text-center mb-8">
          <div className="mx-auto mb-4 h-20 w-20 rounded-full bg-gradient-to-br from-[#5EA68E] to-[#1f5274] flex items-center justify-center">
            <Shield size={40} color="#fff" />
          </div>
          <h1 className="text-2xl sm:text-3xl font-bold text-slate-800">Change Password</h1>
          <p className="text-slate-500 mt-1">Update your SuperAdmin password</p>
        </div>

        {message && (
          <div
            className={`mb-6 flex items-center gap-2 rounded-lg border px-4 py-3 text-sm ${
              messageType === "success"
                ? "border-emerald-200 bg-emerald-50 text-emerald-800"
                : "border-red-200 bg-red-50 text-red-700"
            }`}
          >
            {messageType === "success" ? <CheckCircle size={18} /> : <XCircle size={18} />}
            <span className="font-medium">{message}</span>
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-6">
          {/* Current Password */}
          <div>
            <label className="block text-sm font-semibold text-slate-700 mb-2">
              Current Password
            </label>
            <div className="relative">
              <Lock className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={20} />
              <input
                type={showPasswords.current ? "text" : "password"}
                value={formData.currentPassword}
                onChange={(e) => handleInputChange("currentPassword", e.target.value)}
                placeholder="Enter your current password"
                required
                className={`w-full rounded-lg border-2 bg-white pl-10 pr-12 py-3 text-base outline-none transition
                  ${errors.currentPassword ? "border-red-500" : "border-slate-200"}
                  focus:border-[#5EA68E] focus:ring-4 focus:ring-[#5EA68E]/20`}
              />
              <button
                type="button"
                onClick={() => togglePasswordVisibility("current")}
                className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600"
              >
                {showPasswords.current ? <EyeOff size={20} /> : <Eye size={20} />}
              </button>
            </div>
            {errors.currentPassword && (
              <p className="mt-1 text-sm text-red-600">{errors.currentPassword}</p>
            )}
          </div>

          {/* New Password */}
          <div>
            <label className="block text-sm font-semibold text-slate-700 mb-2">New Password</label>
            <div className="relative">
              <Lock className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={20} />
              <input
                type={showPasswords.new ? "text" : "password"}
                value={formData.newPassword}
                onChange={(e) => handleInputChange("newPassword", e.target.value)}
                placeholder="Enter a new secure password"
                required
                className={`w-full rounded-lg border-2 bg-white pl-10 pr-12 py-3 text-base outline-none transition
                  ${errors.newPassword ? "border-red-500" : "border-slate-200"}
                  focus:border-[#5EA68E] focus:ring-4 focus:ring-[#5EA68E]/20`}
              />
              <button
                type="button"
                onClick={() => togglePasswordVisibility("new")}
                className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600"
              >
                {showPasswords.new ? <EyeOff size={20} /> : <Eye size={20} />}
              </button>
            </div>

            {formData.newPassword && (
              <div className="mt-2">
                <div className="h-1.5 w-full rounded bg-slate-200 overflow-hidden">
                  <div
                    className={`h-full transition-all ${
                      strength.strength >= 60
                        ? "bg-emerald-500"
                        : strength.strength >= 30
                          ? "bg-amber-500"
                          : "bg-red-500"
                    }`}
                    style={{ width: `${strength.strength}%` }}
                  />
                </div>
                {strength.text && (
                  <p
                    className={`mt-1 text-xs ${
                      strength.strength >= 60
                        ? "text-emerald-600"
                        : strength.strength >= 30
                          ? "text-amber-600"
                          : "text-red-600"
                    }`}
                  >
                    Password strength: {strength.text}
                  </p>
                )}
              </div>
            )}

            {errors.newPassword && <p className="mt-1 text-sm text-red-600">{errors.newPassword}</p>}
          </div>

          {/* Confirm Password */}
          <div>
            <label className="block text-sm font-semibold text-slate-700 mb-2">
              Confirm New Password
            </label>
            <div className="relative">
              <Lock className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={20} />
              <input
                type={showPasswords.confirm ? "text" : "password"}
                value={formData.confirmPassword}
                onChange={(e) => handleInputChange("confirmPassword", e.target.value)}
                placeholder="Confirm your new password"
                required
                className={`w-full rounded-lg border-2 bg-white pl-10 pr-12 py-3 text-base outline-none transition
                  ${errors.confirmPassword ? "border-red-500" : "border-slate-200"}
                  focus:border-[#5EA68E] focus:ring-4 focus:ring-[#5EA68E]/20`}
              />

              <button
                type="button"
                onClick={() => togglePasswordVisibility("confirm")}
                className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600"
              >
                {showPasswords.confirm ? <EyeOff size={20} /> : <Eye size={20} />}
              </button>
            </div>

            {errors.confirmPassword && (
              <p className="mt-1 text-sm text-red-600">{errors.confirmPassword}</p>
            )}
          </div>

          <button
            type="submit"
            disabled={loading}
            className={`w-full inline-flex items-center justify-center gap-2 rounded-lg px-5 py-3 text-white font-semibold transition
              ${
                loading
                  ? "cursor-not-allowed opacity-60 bg-[linear-gradient(135deg,rgba(31,82,116,0.5),rgba(96,166,142,0.5))]"
                  : "bg-gradient-to-r from-[#5EA68E] to-[#1f5274] hover:from-[#1f5274] hover:to-[#5EA68E] hover:shadow-lg"
              }`}
          >
            {loading ? (
              <>
                <span className="h-5 w-5 rounded-full border-2 border-white/30 border-t-white animate-spin" />
                Changing Password...
              </>
            ) : (
              <>
                <Shield size={20} />
                Change Password
              </>
            )}
          </button>
        </form>

        <div className="mt-6 rounded-lg border border-slate-200 bg-slate-50 p-4">
          <p className="text-sm font-semibold text-slate-700 mb-2">Security Requirements:</p>
          <ul className="list-disc pl-5 text-xs text-slate-600 space-y-1">
            <li>At least 8 characters long</li>
            <li>Contains uppercase and lowercase letters</li>
            <li>Contains at least one number</li>
            <li>Contains at least one special character</li>
          </ul>
        </div>
      </div>
    </div>
  );
}