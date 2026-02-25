"use client";
import Image from "next/image";
import Link from "next/link";
import React, { useEffect, useMemo, useState } from "react";
import { Dropdown } from "../ui/dropdown/Dropdown";
import { DropdownItem } from "../ui/dropdown/DropdownItem";
import { useRouter } from "next/navigation";
import IntegrationToggleButton from "@/features/integration/IntegrationToggleButton";
import { useAppDispatch } from "@/lib/hooks";
import { logout, setUser } from "@/lib/features/auth/authSlice";
import { useAppSelector } from "@/lib/store";
import { useGetUserQuery } from "@/lib/api/userApi";

/** ✅ Replace these with your real values (backend allowed marketplaces/modules) */
const MODULE_OPTIONS = [
    "LIVE_DASHBOARD",
    "FINANCE_DASHBOARDS",
    "BUSINESS_INTELLIGENCE",
    "INVENTORY_PLANNING",
];

const ROLE_OPTIONS = ["MARKETING", "ACCOUNTED", "INVENTORY"] as const;
type RoleOption = (typeof ROLE_OPTIONS)[number];

const COUNTRY_OPTIONS = [
  { label: "United States", value: "US" },
  { label: "United Kingdom", value: "UK" },
  { label: "Canada", value: "CA" },
  { label: "Germany", value: "DE" },
];

// ✅ Country -> marketplace IDs mapping (update as per your backend)
const COUNTRY_TO_MARKETPLACES: Record<string, string[]> = {
  US: ["ATVPDKIKX0DER"],
  UK: ["A1F83G8C2ARO7P"],
  CA: ["A2EUQ1WTGCTBG2"],
  DE: ["A1PA6795UKMFR9"],
};

function ChipsMultiSelect({
  options,
  value,
  onChange,
  placeholder = "Select the section you want to give access of",
}: {
  options: string[];
  value: string[];
  onChange: (next: string[]) => void;
  placeholder?: string;
}) {
  const [open, setOpen] = useState(false);

  const toggle = (opt: string) => {
    if (value.includes(opt)) onChange(value.filter((v) => v !== opt));
    else onChange([...value, opt]);
  };

  const remove = (opt: string) => onChange(value.filter((v) => v !== opt));

  return (
    <div className="relative">
      {/* Selected chips */}
      <div
        className="min-h-[40px] w-full rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark px-3 py-2 flex flex-wrap gap-2 items-center cursor-pointer"
        onClick={() => setOpen((p) => !p)}
      >
        {value.length === 0 ? (
          <span className="text-gray-400 text-sm">{placeholder}</span>
        ) : (
          value.map((v) => (
            <span
              key={v}
              className="inline-flex items-center gap-2 rounded-full bg-gray-100 dark:bg-white/5 px-3 py-1 text-xs text-gray-700 dark:text-gray-200"
              onClick={(e) => e.stopPropagation()}
            >
              {v}
              <button
                type="button"
                className="text-gray-500 hover:text-gray-800 dark:hover:text-white"
                onClick={() => remove(v)}
                aria-label={`Remove ${v}`}
              >
                ✕
              </button>
            </span>
          ))
        )}
      </div>

      {/* Dropdown list */}
      {open && (
        <div
          className="absolute z-[999999] mt-2 w-full rounded-xl border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark shadow-theme-lg p-2"
          onClick={(e) => e.stopPropagation()}
        >
          <div className="max-h-52 overflow-auto">
            {options.map((opt) => {
              const checked = value.includes(opt);
              return (
                <button
                  type="button"
                  key={opt}
                  onClick={() => toggle(opt)}
                  className="w-full flex items-center justify-between px-3 py-2 rounded-lg hover:bg-gray-100 dark:hover:bg-white/5 text-left"
                >
                  <span className="text-sm text-gray-700 dark:text-gray-200">
                    {opt}
                  </span>
                  <span className="text-sm">{checked ? "✅" : ""}</span>
                </button>
              );
            })}
          </div>

          <div className="pt-2 flex justify-end">
            <button
              type="button"
              className="text-xs text-gray-500 hover:text-gray-800 dark:hover:text-gray-200"
              onClick={() => setOpen(false)}
            >
              Close
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function AddMemberModal({
  isOpen,
  onClose,
  token,
}: {
  isOpen: boolean;
  onClose: () => void;
  token?: string;
}) {
  const [email, setEmail] = useState("");
  const [country, setCountry] = useState<string>("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [modules, setModules] = useState<string[]>([]);
  const [role, setRole] = useState<RoleOption>("MARKETING");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");

  useEffect(() => {
    if (!isOpen) {
      setEmail("");
      setCountry("");
      setPassword("");
      setConfirmPassword("");
      setModules([]);
      setRole("MARKETING");
      setLoading(false);
      setError("");
    }
  }, [isOpen]);

  const marketplaces = useMemo(() => {
    return country ? COUNTRY_TO_MARKETPLACES[country] || [] : [];
  }, [country]);

  const canSubmit =
    email.trim() &&
    password.length >= 6 &&
    password === confirmPassword &&
    marketplaces.length > 0 &&
    modules.length > 0 &&
    !loading;

  const handleSave = async () => {
    setError("");

    if (!email.trim()) return setError("Email is required");
    if (!country) return setError("Country is required");
    if (password.length < 6) return setError("Password must be at least 6 characters");
    if (password !== confirmPassword) return setError("Password and Confirm Password must match");
    if (modules.length === 0) return setError("Please select at least one Section Access");

    const payload = {
      email: email.trim().toLowerCase(),
      password,
      marketplaces, // derived from country
      modules, // multi-select
      // role: role, // backend currently ignores this, but you can send if you want
    };

    try {
      setLoading(true);

      const baseUrl =
        process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, "") || "";
      const res = await fetch(`${baseUrl}/add_member`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify(payload),
      });

      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        setError(data?.error || data?.message || "Failed to add member");
        return;
      }

      onClose();
    } catch (e: any) {
      setError(e?.message || "Something went wrong");
    } finally {
      setLoading(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div
      className="fixed inset-0 z-[999999] flex items-center justify-center bg-black/40 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-[720px] rounded-2xl bg-white dark:bg-gray-dark shadow-theme-lg border border-gray-200 dark:border-gray-800"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="p-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Email */}
            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Email Address *
              </label>
              <input
                className="mt-1 w-full rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark px-3 py-2 text-sm"
                placeholder="member1@company.com"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
              />
            </div>

            {/* Country */}
            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Country *
              </label>
              <select
                className="mt-1 w-full rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark px-3 py-2 text-sm"
                value={country}
                onChange={(e) => setCountry(e.target.value)}
              >
                <option value="">Select Country</option>
                {COUNTRY_OPTIONS.map((c) => (
                  <option key={c.value} value={c.value}>
                    {c.label}
                  </option>
                ))}
              </select>
              {country && (
                <p className="mt-1 text-xs text-gray-500">
                  Marketplaces: {marketplaces.join(", ")}
                </p>
              )}
            </div>

            {/* Password */}
            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Password *
              </label>
              <input
                type="password"
                className="mt-1 w-full rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark px-3 py-2 text-sm"
                placeholder="Min. 6 Characters"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
            </div>

            {/* Confirm Password */}
            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Confirm Password *
              </label>
              <input
                type="password"
                className="mt-1 w-full rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark px-3 py-2 text-sm"
                placeholder="Re-enter password"
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
              />
            </div>

            {/* Section Access (multi select) */}
            <div className="md:col-span-2">
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Section Access *
              </label>
              <div className="mt-1">
                <ChipsMultiSelect
                  options={MODULE_OPTIONS}
                  value={modules}
                  onChange={setModules}
                />
              </div>

              <div className="mt-3 rounded-lg bg-yellow-50 dark:bg-white/5 border border-yellow-100 dark:border-gray-800 px-3 py-2 text-xs text-gray-700 dark:text-gray-200">
                ℹ️ Members can only view the sections you grant access to. You can update permissions anytime.
              </div>
            </div>

            {/* Role */}
            {/* Role */}
<div className="md:col-span-2">
  <label className="text-sm text-gray-700 dark:text-gray-200">
    Role
  </label>
  <select
    className="mt-1 w-full rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-dark px-3 py-2 text-sm"
    value={role}
    onChange={(e) => setRole(e.target.value as RoleOption)}
  >
    {ROLE_OPTIONS.map((r) => (
      <option key={r} value={r}>
        {r}
      </option>
    ))}
  </select>
</div>
          </div>

          {error && (
            <div className="mt-4 text-sm text-red-600">{error}</div>
          )}

          <div className="mt-6 flex justify-end gap-3">
            <button
              type="button"
              onClick={onClose}
              className="rounded-lg px-4 py-2 text-sm border border-gray-200 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-white/5"
            >
              Cancel
            </button>

            <button
              type="button"
              onClick={handleSave}
              disabled={!canSubmit}
              className="rounded-lg px-4 py-2 text-sm bg-blue-600 text-white disabled:opacity-50 disabled:cursor-not-allowed hover:bg-blue-700"
            >
              {loading ? "Saving..." : "Save Info"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default function UserDropdown() {
  const [isOpen, setIsOpen] = useState(false);
  const [isAddMemberOpen, setIsAddMemberOpen] = useState(false);

  const dispatch = useAppDispatch();
  const router = useRouter();

  const { user: userFromStore, token } = useAppSelector((s: any) => s.auth);

  const { data: userFromApi } = useGetUserQuery(undefined, {
    skip: !token,
  });

  useEffect(() => {
    if (userFromApi) dispatch(setUser(userFromApi));
  }, [userFromApi, dispatch]);

  useEffect(() => {
    if (!userFromStore && userFromApi) {
      dispatch(setUser(userFromApi));
    }
  }, [userFromStore, userFromApi, dispatch]);

  useEffect(() => {
    if (!userFromStore && typeof window !== "undefined") {
      const brand = localStorage.getItem("brandName");
      const company = localStorage.getItem("companyName");
      const name = localStorage.getItem("name");

      if (brand || company) {
        dispatch(
          setUser({
            brand_name: brand || undefined,
            company_name: company || undefined,
            name: name || undefined,
          })
        );
      }
    }
  }, [userFromStore, dispatch]);

  function toggleDropdown(e: React.MouseEvent<HTMLElement, MouseEvent>) {
    e.stopPropagation();
    setIsOpen((prev) => !prev);
  }

  function closeDropdown() {
    setIsOpen(false);
  }

  const handleLogout = () => {
    dispatch(logout());

    if (typeof window !== "undefined") {
      localStorage.removeItem("jwtToken");
      localStorage.clear();
      sessionStorage.clear();
    }

    closeDropdown();
    window.location.href = "/signin";
  };

  const initials = userFromStore?.name
    ?.split(" ")
    .map((n: any) => n[0])
    .slice(0, 2)
    .join("")
    .toUpperCase();

  return (
    <div className="relative z-99999">
      <div className="flex items-center text-gray-700 dark:text-gray-400">
        <span className="mr-1 font-normal text-xs md:text-sm 2xl:text-base inline-flex items-center gap-2">
          Welcome,
          <span className="font-bold  inline-flex items-center gap-2">
            <i>{userFromStore?.name}!</i>

            <div
              onClick={toggleDropdown}
              className=" cursor-pointer
                flex items-center justify-center
                w-7 h-7 sm:w-8 sm:h-8 2xl:w-8 2xl:h-8
                rounded-full
                bg-blue-700
                text-yellow-200
                text-xs
                font-semibold
                leading-none
              "
            >
              {initials}
            </div>

            <IntegrationToggleButton />
          </span>
        </span>
      </div>

      <Dropdown
        isOpen={isOpen}
        onClose={closeDropdown}
        className="absolute right-0 mt-[17px] flex w-[260px] flex-col rounded-2xl border border-gray-200 bg-white p-3 shadow-theme-lg dark:border-gray-800 dark:bg-gray-dark"
      >
        <div>
          <span className="block font-medium text-gray-700 text-theme-sm dark:text-gray-400">
            {userFromStore?.name || userFromStore?.brand_name || "User"}
          </span>
          <span className="mt-0.5 block text-theme-xs text-gray-500 dark:text-gray-400">
            {userFromStore?.email}
          </span>
        </div>

        <ul className="flex flex-col gap-1 pt-4 pb-3 border-b border-gray-200 dark:border-gray-800">
          <li>
            <DropdownItem
              onItemClick={closeDropdown}
              tag="a"
              href="/profile"
              className="flex items-center gap-3 px-3 py-2 font-medium text-gray-700 rounded-lg group text-theme-sm hover:bg-gray-100 hover:text-gray-700 dark:text-gray-400 dark:hover:bg-white/5 dark:hover:text-gray-300"
            >
              Account Settings
            </DropdownItem>
          </li>

          <li>
            <DropdownItem
              onItemClick={closeDropdown}
              tag="a"
              href="/settings"
              className="flex items-center gap-3 px-3 py-2 font-medium text-gray-700 rounded-lg group text-theme-sm hover:bg-gray-100 hover:text-gray-700 dark:text-gray-400 dark:hover:bg-white/5 dark:hover:text-gray-300"
            >
              Create New Country Profile
            </DropdownItem>
          </li>

          <li>
            <DropdownItem
              onItemClick={closeDropdown}
              tag="a"
              href="/settings"
              className="flex items-center gap-3 px-3 py-2 font-medium text-gray-700 rounded-lg group text-theme-sm hover:bg-gray-100 hover:text-gray-700 dark:text-gray-400 dark:hover:bg-white/5 dark:hover:text-gray-300"
            >
              Your Country Profile
            </DropdownItem>
          </li>

          <li>
            <DropdownItem
              onItemClick={closeDropdown}
              tag="a"
              href="/uploads"
              className="flex items-center gap-3 px-3 py-2 font-medium text-gray-700 rounded-lg group text-theme-sm hover:bg-gray-100 hover:text-gray-700 dark:text-gray-400 dark:hover:bg-white/5 dark:hover:text-gray-300"
            >
              Your Uploads
            </DropdownItem>
          </li>

          {/* ✅ NEW: Add Members */}
          <li>
            <button
              type="button"
              onClick={() => {
                closeDropdown();
                setIsAddMemberOpen(true);
              }}
              className="w-full flex items-center gap-3 px-3 py-2 font-medium text-gray-700 rounded-lg group text-theme-sm hover:bg-gray-100 hover:text-gray-700 dark:text-gray-400 dark:hover:bg-white/5 dark:hover:text-gray-300 text-left"
            >
              Add Members
            </button>
          </li>
        </ul>

        <button
          onClick={handleLogout}
          className="flex items-center gap-3 px-3 py-2 mt-3 font-medium text-red-700 rounded-lg group text-theme-sm hover:bg-red-200 hover:text-red-700 dark:text-gray-400 dark:hover:bg-white/5 dark:hover:text-gray-300 w-full text-left"
        >
          {/* icon same */}
          <svg
            className="fill-red-700 group-hover:fill-red-700 dark:group-hover:fill-red-700"
            width="24"
            height="24"
            viewBox="0 0 24 24"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              fillRule="evenodd"
              clipRule="evenodd"
              d="M15.1007 19.247C14.6865 19.247 14.3507 18.9112 14.3507 18.497L14.3507 14.245H12.8507V18.497C12.8507 19.7396 13.8581 20.747 15.1007 20.747H18.5007C19.7434 20.747 20.7507 19.7396 20.7507 18.497L20.7507 5.49609C20.7507 4.25345 19.7433 3.24609 18.5007 3.24609H15.1007C13.8581 3.24609 12.8507 4.25345 12.8507 5.49609V9.74501L14.3507 9.74501V5.49609C14.3507 5.08188 14.6865 4.74609 15.1007 4.74609L18.5007 4.74609C18.9149 4.74609 19.2507 5.08188 19.2507 5.49609L19.2507 18.497C19.2507 18.9112 18.9149 19.247 18.5007 19.247H15.1007ZM3.25073 11.9984C3.25073 12.2144 3.34204 12.4091 3.48817 12.546L8.09483 17.1556C8.38763 17.4485 8.86251 17.4487 9.15549 17.1559C9.44848 16.8631 9.44863 16.3882 9.15583 16.0952L5.81116 12.7484L16.0007 12.7484C16.4149 12.7484 16.7507 12.4127 16.7507 11.9984C16.7507 11.5842 16.4149 11.2484 16.0007 11.2484L5.81528 11.2484L9.15585 7.90554C9.44864 7.61255 9.44847 7.13767 9.15547 6.84488C8.86248 6.55209 8.3876 6.55226 8.09481 6.84525L3.52309 11.4202C3.35673 11.5577 3.25073 11.7657 3.25073 11.9984Z"
              fill=""
            />
          </svg>
          Sign out
        </button>
      </Dropdown>

      {/* ✅ Modal */}
      <AddMemberModal
        isOpen={isAddMemberOpen}
        onClose={() => setIsAddMemberOpen(false)}
        token={token}
      />
    </div>
  );
}