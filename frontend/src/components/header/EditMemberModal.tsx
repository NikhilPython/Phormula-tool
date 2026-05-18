"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useGetUserDataQuery } from "@/lib/api/profileApi";
import { IoInformationCircleOutline } from "react-icons/io5";

const MODULE_OPTIONS = [
  "LIVE_DASHBOARD",
  "FINANCE_DASHBOARDS",
  "BUSINESS_INTELLIGENCE",
  "INVENTORY_PLANNING",
];

const ROLE_OPTIONS = ["MARKETING", "ACCOUNTANT", "INVENTORY"] as const;
type RoleOption = (typeof ROLE_OPTIONS)[number];

type UserDataResponse = {
  owner_email?: string;
  marketplace_ids?: string[];
  marketplaces?: string[];
  connected_marketplaces?: string[];
  countries?: string[];
  connected_countries?: string[];
  integrated_countries?: string[];
};

const COUNTRY_OPTIONS = [
  { label: "United States", value: "US" },
  { label: "United Kingdom", value: "UK" },
  { label: "Canada", value: "CA" },
  { label: "Germany", value: "DE" },
];

const COUNTRY_TO_MARKETPLACES: Record<string, string[]> = {
  US: ["ATVPDKIKX0DER"],
  UK: ["A1F83G8C2ARO7P"],
  CA: ["A2EUQ1WTGCTBG2"],
  DE: ["A1PA6795UKMFR9"],
};

const formatLabel = (value: string) =>
  value
    .toLowerCase()
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());

const normalizeCountryAccess = (member: any): Record<string, string[]> => {
  const rawAccess = member?.country_access;

  if (rawAccess && typeof rawAccess === "object" && !Array.isArray(rawAccess)) {
    return Object.fromEntries(
      Object.entries(rawAccess)
        .map(([country, modules]) => [
          String(country).toUpperCase(),
          Array.isArray(modules) ? modules.map(String) : [],
        ])
        .filter(([, modules]) => modules.length > 0)
    );
  }

  const countries = Array.isArray(member?.countries)
    ? member.countries.map((country: any) => String(country).toUpperCase())
    : [];

  const modules = Array.isArray(member?.modules)
    ? member.modules.map(String)
    : [];

  if (countries.length === 0 || modules.length === 0) {
    return {};
  }

  return Object.fromEntries(countries.map((country: string) => [country, modules]));
};

export default function EditMemberModal({
  isOpen,
  onClose,
  token,
  member,
  onSuccess,
}: {
  isOpen: boolean;
  onClose: () => void;
  token?: string;
  member: any | null;
  onSuccess?: () => void;
}) {
  const [countryAccess, setCountryAccess] = useState<Record<string, string[]>>(
    {}
  );
  const [role, setRole] = useState<RoleOption>("MARKETING");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");

  const { data: userData } = useGetUserDataQuery();
  const typedUserData = userData as UserDataResponse | undefined;

  const integratedMarketplaces = useMemo(() => {
    return [
      ...(Array.isArray(typedUserData?.marketplace_ids)
        ? typedUserData.marketplace_ids
        : []),
      ...(Array.isArray(typedUserData?.marketplaces)
        ? typedUserData.marketplaces
        : []),
      ...(Array.isArray(typedUserData?.connected_marketplaces)
        ? typedUserData.connected_marketplaces
        : []),
    ].map(String);
  }, [typedUserData]);

  const integratedCountries = useMemo(() => {
    const countriesFromApi = [
      ...(Array.isArray(typedUserData?.countries) ? typedUserData.countries : []),
      ...(Array.isArray(typedUserData?.connected_countries)
        ? typedUserData.connected_countries
        : []),
      ...(Array.isArray(typedUserData?.integrated_countries)
        ? typedUserData.integrated_countries
        : []),
    ].map((country) => String(country).toUpperCase());

    const countriesFromMarketplaces = COUNTRY_OPTIONS.filter((country) => {
      const countryMarketplaces = COUNTRY_TO_MARKETPLACES[country.value] || [];

      return countryMarketplaces.some((marketplaceId) =>
        integratedMarketplaces.includes(marketplaceId)
      );
    }).map((country) => country.value);

    return Array.from(
      new Set([...countriesFromApi, ...countriesFromMarketplaces])
    );
  }, [typedUserData, integratedMarketplaces]);

  const availableCountryOptions = useMemo(() => {
    return COUNTRY_OPTIONS.filter((country) =>
      integratedCountries.includes(country.value)
    );
  }, [integratedCountries]);

  useEffect(() => {
    if (!isOpen || !member) return;

    setCountryAccess(normalizeCountryAccess(member));
    setRole((member?.role as RoleOption) || "MARKETING");
    setLoading(false);
    setError("");
  }, [isOpen, member]);

  useEffect(() => {
    if (!isOpen) return;

    setCountryAccess((prev) => {
      const allowedCountryCodes = new Set(
        availableCountryOptions.map((country) => country.value)
      );

      return Object.fromEntries(
        Object.entries(prev).filter(([countryCode]) =>
          allowedCountryCodes.has(countryCode)
        )
      );
    });
  }, [isOpen, availableCountryOptions]);

  const selectedCountries = useMemo(
    () => Object.keys(countryAccess),
    [countryAccess]
  );

  const selectedModules = useMemo(
    () => Array.from(new Set(Object.values(countryAccess).flat())),
    [countryAccess]
  );

  const selectedMarketplaces = useMemo(() => {
    return selectedCountries.flatMap(
      (countryCode) => COUNTRY_TO_MARKETPLACES[countryCode] || []
    );
  }, [selectedCountries]);

  const hasCountryAccess = selectedCountries.length > 0;

  const hasModuleAccess = Object.values(countryAccess).some(
    (modules) => modules.length > 0
  );

  const canSubmit =
    !!member?.id &&
    availableCountryOptions.length > 0 &&
    hasCountryAccess &&
    hasModuleAccess &&
    !loading;

  const toggleCountry = (countryCode: string) => {
    setCountryAccess((prev) => {
      const next = { ...prev };

      if (next[countryCode]) {
        delete next[countryCode];
      } else {
        next[countryCode] = [];
      }

      return next;
    });
  };

  const toggleCountryModule = (countryCode: string, module: string) => {
    setCountryAccess((prev) => {
      const currentModules = prev[countryCode] || [];

      const nextModules = currentModules.includes(module)
        ? currentModules.filter((m) => m !== module)
        : [...currentModules, module];

      return {
        ...prev,
        [countryCode]: nextModules,
      };
    });
  };

  const handleSave = async () => {
    setError("");

    if (!member?.id) {
      setError("Member is missing");
      return;
    }

    if (availableCountryOptions.length === 0) {
      setError("No connected countries found. Please connect a marketplace first.");
      return;
    }

    if (!hasCountryAccess) {
      setError("Please select at least one country");
      return;
    }

    if (!hasModuleAccess) {
      setError("Please select at least one section for at least one country");
      return;
    }

    const cleanedCountryAccess = Object.fromEntries(
      Object.entries(countryAccess).filter(([, modules]) => modules.length > 0)
    );

    if (Object.keys(cleanedCountryAccess).length === 0) {
      setError("Please select at least one section for at least one country");
      return;
    }

    const payload = {
      member_id: member.id,
      marketplace_ids: selectedMarketplaces,
      modules: selectedModules,
      country_access: cleanedCountryAccess,
      role,
    };

    try {
      setLoading(true);

      const baseUrl =
        process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, "") || "";

      const res = await fetch(`${baseUrl}/update_member_access`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify(payload),
      });

      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        setError(data?.error || data?.message || "Failed to update member");
        return;
      }

      onClose();
      onSuccess?.();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Something went wrong");
    } finally {
      setLoading(false);
    }
  };

  if (!isOpen || !member) return null;

  return (
    <div
      className="fixed inset-0 z-[999999] flex items-center justify-center bg-black/40 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-[980px] rounded-2xl bg-white dark:bg-gray-dark shadow-theme-lg border border-gray-200 dark:border-gray-800"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="max-h-[90vh] overflow-y-auto p-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Name
              </label>

              <input
                disabled
                className="mt-1 w-full rounded-md border border-gray-200 dark:border-gray-800 bg-gray-100 dark:bg-white/5 px-3 py-2 text-sm cursor-not-allowed"
                value={member?.member_name || ""}
              />
            </div>

            <div>
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Email Address
              </label>

              <input
                disabled
                className="mt-1 w-full rounded-md border border-gray-200 dark:border-gray-800 bg-gray-100 dark:bg-white/5 px-3 py-2 text-sm cursor-not-allowed"
                value={member?.email || ""}
              />
            </div>

            <div>
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
                    {formatLabel(r)}
                  </option>
                ))}
              </select>
            </div>

            <div className="md:col-span-2">
              <label className="text-sm text-gray-700 dark:text-gray-200">
                Country & Section Access{" "}
                <span className="text-red-500">*</span>
              </label>

              {availableCountryOptions.length === 0 ? (
                <div className="mt-2 rounded-lg border border-yellow-200 bg-yellow-50 px-3 py-3 text-sm text-yellow-800">
                  No connected countries found. Please connect/integrate a
                  marketplace before editing this member.
                </div>
              ) : (
                <div className="mt-2 overflow-x-auto rounded-2xl border border-gray-200">
                  <div className="min-w-[760px]">
                    <div className="grid grid-cols-[180px_repeat(4,minmax(140px,1fr))] bg-gray-50 text-xs font-semibold text-gray-600">
                      <div className="border-r border-gray-200 px-3 py-3">
                        Country
                      </div>

                      {MODULE_OPTIONS.map((module) => (
                        <div
                          key={module}
                          className="border-r border-gray-200 px-3 py-3 text-center last:border-r-0"
                        >
                          {formatLabel(module)}
                        </div>
                      ))}
                    </div>

                    {availableCountryOptions.map((countryOption) => {
                      const countryCode = countryOption.value;
                      const enabled = !!countryAccess[countryCode];
                      const selectedModulesForCountry =
                        countryAccess[countryCode] || [];

                      return (
                        <div
                          key={countryCode}
                          className="grid grid-cols-[180px_repeat(4,minmax(140px,1fr))] border-t border-gray-200 bg-white text-sm"
                        >
                          <div className="flex items-center gap-2 border-r border-gray-200 px-3 py-3">
                            <input
                              type="checkbox"
                              checked={enabled}
                              onChange={() => toggleCountry(countryCode)}
                              className="h-4 w-4"
                            />

                            <div>
                              <div className="font-medium text-gray-900">
                                {countryOption.label}
                              </div>

                              {/* <div className="text-[11px] text-gray-500">
                                {COUNTRY_TO_MARKETPLACES[countryCode]?.join(
                                  ", "
                                )}
                              </div> */}
                            </div>
                          </div>

                          {MODULE_OPTIONS.map((module) => {
                            const checked =
                              selectedModulesForCountry.includes(module);

                            return (
                              <div
                                key={`${countryCode}-${module}`}
                                className="flex items-center justify-center border-r border-gray-200 px-3 py-3 last:border-r-0"
                              >
                                <button
                                  type="button"
                                  disabled={!enabled}
                                  onClick={() =>
                                    toggleCountryModule(countryCode, module)
                                  }
                                  className={`flex h-7 w-7 items-center justify-center rounded-md border text-xs transition ${checked
                                      ? "border-green-500 bg-green-500 text-white"
                                      : enabled
                                        ? "border-gray-300 bg-white text-transparent hover:bg-gray-50"
                                        : "cursor-not-allowed border-gray-200 bg-gray-100 text-transparent opacity-60"
                                    }`}
                                >
                                  ✓
                                </button>
                              </div>
                            );
                          })}
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}

              <p className="mt-2 text-xs text-gray-500">
                Select one or more connected countries, then choose exactly
                which sections this member can access.
              </p>

             
          <div className="mt-4 flex items-start gap-2 rounded-lg border border-yellow-100 bg-[#FDD36F4D] px-3 py-2 text-xs text-gray-700 dark:border-gray-800 dark:bg-white/5 dark:text-gray-200">
                <IoInformationCircleOutline className="text-charcoal-500 flex-shrink-0 text-base" />
                Update permissions and role. Name/Email are locked.
              </div>
            </div>
          </div>

          {error && <div className="mt-4 text-sm text-red-600">{error}</div>}

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
              className="rounded-lg px-4 py-2 text-sm bg-blue-700 text-yellow-200 disabled:opacity-50 disabled:cursor-not-allowed hover:bg-blue-700"
            >
              {loading ? "Saving..." : "Save Changes"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}