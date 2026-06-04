"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { FaSearch, FaLongArrowAltRight } from "react-icons/fa";
import { Settings, Trash2 } from "lucide-react";
import { toast } from "sonner";
import Image from "next/image";
import Loader from "@/components/loader/Loader";
import SegmentedToggle from "@/components/ui/SegmentedToggle";
import SummaryMetricCardLarge from "../ViewUserPage/SummaryMetricCardLarge";

const MIN_LOADER_MS = 3000;

type AdminRow = {
  id: number | string;
  email: string;
};

type UserRow = {
  id: number | string;
  email: string;
  brand_name: string;
  name: string;
  company_name: string;
  country?: string;
  marketplace_id?: string;
  status?: string | boolean;
  address?: {
    building?: string;
    city?: string;
    country?: string;
    state?: string;
    zipcode?: string;
  };
};

type DashboardResponse = {
  user_admins?: AdminRow[];
  users?: UserRow[];
};

type ApiError = {
  message?: string;
};

type StatusFilter = "all" | "active" | "inactive";

export default function SuperAdminDashboardPage() {
  const [showSettings, setShowSettings] = useState<boolean>(false);
  const [formulaUpdating, setFormulaUpdating] = useState<boolean>(false);
  const [emailInput, setEmailInput] = useState<string>("");
  const [searchResult, setSearchResult] = useState<DashboardResponse | null>(null);
  const [defaultData, setDefaultData] = useState<DashboardResponse | null>(null);
  const [usersData, setUsersData] = useState<UserRow[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>("");
  const [showSearch, setShowSearch] = useState<boolean>(false);
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [actionLoading, setActionLoading] = useState<Record<string, boolean>>({});
  const [showFormulaCountryModal, setShowFormulaCountryModal] = useState(false);
  const [selectedFormulaCountry, setSelectedFormulaCountry] = useState<"uk" | "us">("uk");
  const router = useRouter();

  const finishLoadingWithMinDelay = (startedAt: number) => {
    const elapsed = Date.now() - startedAt;
    const remaining = Math.max(0, MIN_LOADER_MS - elapsed);
    window.setTimeout(() => setLoading(false), remaining);
  };

  const STATUS_OPTIONS = [
    { value: "all" as const, label: "All" },
    { value: "active" as const, label: "Active" },
    { value: "inactive" as const, label: "Inactive" },
  ];

  const normalizeStatus = (status?: string | boolean) => {
    if (status === true) return "active";
    if (status === false) return "inactive";

    const s = String(status ?? "").trim().toLowerCase();

    if (s === "inactive" || s === "disabled" || s === "false") {
      return "inactive";
    }

    return "active";
  };

  useEffect(() => {
    const fetchDefaultData = async () => {
      const startedAt = Date.now();
      setLoading(true);

      try {
        const token = localStorage.getItem("superadmin_token");
        if (!token) {
          setError("No authentication token found");
          router.push("/superadmin/CDPAdminConsole");
          return;
        }

        const response = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/dashboard?authenticated_user=superadmin`,
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
          }
        );

        if (!response.ok) {
          if (response.status === 401) {
            localStorage.removeItem("superadmin_token");
            router.push("/superadmin/CDPAdminConsole");
            return;
          }
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = (await response.json()) as DashboardResponse;
        setDefaultData(data);
      } catch (err) {
        setError("Failed to load users.");

        const msg = err instanceof Error ? err.message : String(err);
        if (msg.includes("401") || msg.toLowerCase().includes("authentication")) {
          localStorage.removeItem("superadmin_token");
          router.push("/superadmin/CDPAdminConsole");
        }
      } finally {
        finishLoadingWithMinDelay(startedAt);
      }
    };

    fetchDefaultData();
  }, [router]);

  useEffect(() => {
    setUsersData(defaultData?.users || []);
  }, [defaultData]);

  const handleFormulaUpdate = async (country: "uk" | "us") => {
    const token = localStorage.getItem("superadmin_token");

    if (!token) {
      toast.error("Superadmin token not found. Please login again.");
      router.push("/superadmin/CDPAdminConsole");
      return;
    }

    const marketplaces = {
      uk: {
        country: "uk",
        marketplace_id: "A1F83G8C2ARO7P",
      },
      us: {
        country: "us",
        marketplace_id: "ATVPDKIKX0DER",
      },
    };

    const selectedMarketplace = marketplaces[country];

    try {
      setFormulaUpdating(true);
      setShowFormulaCountryModal(false);

      const transactionStatus =
        selectedMarketplace.country === "us" ? "RELEASED,DEFERRED" : "RELEASED";

      const url =
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/amazon_api/formula_update` +
        `?country=${selectedMarketplace.country}` +
        `&marketplace_id=${selectedMarketplace.marketplace_id}` +
        `&store_in_db=true` +
        `&run_upload_pipeline=true` +
        `&transaction_status=${encodeURIComponent(transactionStatus)}`;

      const response = await fetch(url, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
      });

      const json = await response.json().catch(() => ({}));

      if (!response.ok || !json?.success) {
        throw new Error(
          json?.error ||
          json?.message ||
          `Formula update failed for ${selectedMarketplace.country.toUpperCase()}`
        );
      }

      toast.success(
        `Formula update completed for ${selectedMarketplace.country.toUpperCase()}. Runs: ${json.total_month_runs || 0
        }, Success: ${json.success_count || 0}, Failed: ${json.failed_count || 0
        }, Skipped: ${json.total_skipped_old_months || 0}`
      );

      console.log("Formula update result:", json);
    } catch (error) {
      const msg = error instanceof Error ? error.message : "Formula update failed";
      toast.error(msg);
    } finally {
      setFormulaUpdating(false);
    }
  };

  const handleLogout = async () => {
    setShowSettings(false);

    const token = localStorage.getItem("superadmin_token");

    try {
      if (token) {
        await toast.promise(
          fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin_logout`, {
            method: "POST",
            headers: { Authorization: `Bearer ${token}` },
          }),
          {
            loading: "Logging you out…",
            success: "Logged out successfully.",
            error: "Logout failed. Please try again.",
          }
        );
      } else {
        toast.info("You’re already logged out.");
      }
    } finally {
      localStorage.removeItem("superadmin_token");
      router.push("/superadmin/CDPAdminConsole");
    }
  };

  const handleEmailSearch = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!emailInput.trim()) {
      setSearchResult(null);
      return;
    }

    const startedAt = Date.now();
    setLoading(true);
    setError("");
    setSearchResult(null);

    try {
      const token = localStorage.getItem("superadmin_token");
      if (!token) {
        setError("No authentication token found");
        router.push("/superadmin/CDPAdminConsole");
        return;
      }

      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/dashboard?email=${encodeURIComponent(
          emailInput.trim()
        )}`,
        {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
        }
      );

      const data = (await response.json()) as DashboardResponse & ApiError;

      if (!response.ok) {
        if (response.status === 401) {
          localStorage.removeItem("superadmin_token");
          router.push("/superadmin/CDPAdminConsole");
          return;
        }
        throw new Error(data.message || "Search failed");
      }

      setSearchResult(data);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    } finally {
      finishLoadingWithMinDelay(startedAt);
    }
  };

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.ctrlKey && e.key.toLowerCase() === "f") {
        e.preventDefault();
        setShowSearch(true);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  const handleSearchInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setEmailInput(e.target.value);
    if (!e.target.value.trim()) {
      setSearchResult(null);
    }
  };

  const allUsers = useMemo(() => {
    if (searchResult?.users?.length) return searchResult.users;
    return usersData || [];
  }, [searchResult, usersData]);

  const filteredUsers = useMemo(() => {
    return allUsers.filter((user) => {
      const query = emailInput.trim().toLowerCase();

      const matchesSearch =
        !query ||
        (user.email || "").toLowerCase().includes(query) ||
        (user.brand_name || "").toLowerCase().includes(query) ||
        (user.name || "").toLowerCase().includes(query) ||
        (user.company_name || "").toLowerCase().includes(query);

      const userStatus = normalizeStatus(user.status);
      const matchesStatus =
        statusFilter === "all" ? true : userStatus === statusFilter;

      return matchesSearch && matchesStatus;
    });
  }, [allUsers, emailInput, statusFilter]);

  const totalUsers = usersData.length;
  const activeUsers = usersData.filter(
    (u) => normalizeStatus(u.status) === "active"
  ).length;
  const inactiveUsers = usersData.filter(
    (u) => normalizeStatus(u.status) === "inactive"
  ).length;

  const totalBrands = new Set(
    usersData.map((u) => (u.brand_name || "").trim()).filter(Boolean)
  ).size;

  const totalCompanies = new Set(
    usersData.map((u) => (u.company_name || "").trim()).filter(Boolean)
  ).size;

  const totalMarketplaces = new Set(
    usersData.map((u) => (u.country || "").trim().toLowerCase()).filter(Boolean)
  ).size;

  const summaryCards = [
    {
      title: "Total Users",
      value: totalUsers,
      accent: "border-[#F4C04E] border-t-[#F4C04E]",
      subText: `${activeUsers} active users`,
    },
    {
      title: "Active Brands",
      value: totalBrands,
      accent: "border-[#C78B57] border-t-[#C78B57]",
      subText: `${totalBrands} total brands`,
    },
    {
      title: "Companies",
      value: totalCompanies,
      accent: "border-[#2EA8E5] border-t-[#2EA8E5]",
      subText: `${totalCompanies} total companies`,
    },
    {
      title: "Marketplaces",
      value: totalMarketplaces,
      accent: "border-[#93A95B] border-t-[#93A95B]",
      subText: `${inactiveUsers} inactive users`,
    },
  ];

  const handleToggleStatus = async (user: UserRow) => {
    const emailKey = user.email;
    const currentStatus = normalizeStatus(user.status);
    const nextStatus = currentStatus === "active" ? false : true;

    try {
      setActionLoading((prev) => ({ ...prev, [emailKey]: true }));

      const token = localStorage.getItem("superadmin_token");
      if (!token) {
        toast.error("No authentication token found");
        router.push("/superadmin/CDPAdminConsole");
        return;
      }

      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/dashboard/update_user_status`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify({
            user_id: user.id,
            status: nextStatus,
          }),
        }
      );

      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(data?.message || "Failed to update status");
      }

      const updatedStatus =
        data?.status === true || String(data?.status).toLowerCase() === "true"
          ? "active"
          : "inactive";

      setUsersData((prev) =>
        prev.map((u) =>
          String(u.id) === String(user.id)
            ? { ...u, status: updatedStatus }
            : u
        )
      );

      setDefaultData((prev) => {
        if (!prev?.users) return prev;
        return {
          ...prev,
          users: prev.users.map((u) =>
            String(u.id) === String(user.id)
              ? { ...u, status: updatedStatus }
              : u
          ),
        };
      });

      setSearchResult((prev) => {
        if (!prev?.users) return prev;
        return {
          ...prev,
          users: prev.users.map((u) =>
            String(u.id) === String(user.id)
              ? { ...u, status: updatedStatus }
              : u
          ),
        };
      });

      toast.success(
        `User ${updatedStatus === "active" ? "enabled" : "disabled"} successfully`
      );
    } catch (error) {
      const msg =
        error instanceof Error ? error.message : "Failed to update status";
      toast.error(msg);
    } finally {
      setActionLoading((prev) => ({ ...prev, [emailKey]: false }));
    }
  };

  const handleDeleteAdmin = async (email: string) => {
    const confirmed = window.confirm(
      `Are you sure you want to delete ${email}?`
    );
    if (!confirmed) return;

    try {
      setActionLoading((prev) => ({ ...prev, [email]: true }));

      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/delete_admin`,
        {
          method: "DELETE",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${localStorage.getItem("superadmin_token") ?? ""
              }`,
          },
          body: JSON.stringify({ email: email.trim() }),
        }
      );

      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(data?.message || "Failed to delete admin");
      }

      setUsersData((prev) => prev.filter((u) => u.email !== email));
      setSearchResult((prev) => {
        if (!prev?.users) return null;
        return {
          ...prev,
          users: prev.users.filter((u) => u.email !== email),
        };
      });

      if (emailInput.trim().toLowerCase() === email.toLowerCase()) {
        setEmailInput("");
        setSearchResult(null);
      }

      toast.success("Admin deleted successfully");
    } catch (error) {
      const msg =
        error instanceof Error
          ? error.message
          : "Network error. Please try again.";
      toast.error(msg);
    } finally {
      setActionLoading((prev) => ({ ...prev, [email]: false }));
    }
  };

  const renderTableRows = (rows: UserRow[]) =>
    rows.map((user) => {
      const userStatus = normalizeStatus(user.status);
      const nativeCountry = user.address?.country?.trim() || "Not added";
      const marketplaceIntegration = user.country || "Not added";
      const isBusy = !!actionLoading[user.email];

      return (
        <tr key={user.id} className="border-t">
          <td className="px-4 py-3 text-sm text-slate-700">
            {user.brand_name || "Not added"}
          </td>

          <td className="px-4 py-3 text-sm text-slate-700">
            {user.company_name || "Not added"}
          </td>

          <td className="px-4 py-3 text-sm text-slate-700">
            {nativeCountry}
          </td>

          <td className="px-4 py-3 text-sm text-slate-700 capitalize">
            {marketplaceIntegration}
          </td>

          <td className="px-4 py-3 text-sm text-slate-700">
            {user.name || "Not added"}
          </td>

          <td className="px-4 py-3 text-sm text-slate-700 break-all">
            {user.email || "Not added"}
          </td>

          <td className="px-4 py-3 text-sm text-slate-700">
            <div className="flex items-center gap-3">
              <div className="inline-flex items-center gap-2">
                <span
                  className={`inline-block h-2 w-2 rounded-full ${userStatus === "active" ? "bg-green-500" : "bg-red-500"
                    }`}
                />
                <span className="capitalize">{userStatus}</span>
              </div>

              <button
                type="button"
                onClick={() => handleToggleStatus(user)}
                disabled={isBusy}
                className={`relative inline-flex h-6 w-11 items-center rounded-full transition ${userStatus === "active" ? "bg-emerald-500" : "bg-slate-300"
                  } ${isBusy ? "opacity-60 cursor-not-allowed" : ""}`}
                title={
                  userStatus === "active" ? "Disable admin" : "Enable admin"
                }
              >
                <span
                  className={`inline-block h-4 w-4 transform rounded-full bg-white transition ${userStatus === "active" ? "translate-x-6" : "translate-x-1"
                    }`}
                />
              </button>
            </div>
          </td>

          <td className="px-4 py-3 text-sm text-slate-700">
            <div className="flex items-center gap-2">
              <button
                onClick={() =>
                  router.push(
                    `/superadmin/ViewUserPage/${encodeURIComponent(user.email)}`
                  )
                }
                className="inline-flex items-center justify-center p-1.5 rounded-md bg-white border border-[#414042] text-[#414042] shadow"
              >
                View <FaLongArrowAltRight className="pl-1" />
              </button>

              <button
                type="button"
                onClick={() => handleDeleteAdmin(user.email)}
                disabled={isBusy}
                className={`inline-flex items-center justify-center px-3 py-2 rounded-md border border-red-200 bg-red-50 text-red-600 shadow-sm hover:bg-red-100 ${isBusy ? "opacity-60 cursor-not-allowed" : ""
                  }`}
                title="Delete admin"
              >
                <Trash2 size={16} />
              </button>
            </div>
          </td>
        </tr>
      );
    });

  return (
    <div className="min-h-screen bg-gradient-to-br from-emerald-50 to-slate-100 p-4 sm:p-6">
      <header className="sticky top-0 z-40 w-full bg-gradient-to-r from-[#5EA68E] to-[#1f5274] rounded-lg shadow-lg">
        <div className="mx-auto px-4 sm:px-6 mb-6">
          <div className="flex items-center justify-between gap-3 py-3 sm:py-4">
            <div className="flex items-center gap-3 min-w-0">
              <Image
                width={220}
                height={40}
                src="/images/auth/Phormula.png"
                alt="Phormula"
                priority
                className="2xl:w-[220px] 2xl:h-[50px] xl:w-[150px] w-auto"
              />
            </div>

            <div className="flex items-center gap-2 sm:gap-3">
              <button
                type="button"
                onClick={() => setShowSearch((s) => !s)}
                className="md:hidden inline-flex items-center justify-center rounded-lg px-3 py-2 text-white bg-white/10 hover:bg-white/20 focus:outline-none focus:ring-4 focus:ring-white/30"
              >
                <FaSearch />
              </button>

              <div className="relative">
                <button
                  type="button"
                  onClick={() => setShowSettings((s) => !s)}
                  className="inline-flex items-center justify-center rounded-lg p-3 text-white bg-white/10 hover:bg-white/20 font-medium shadow focus:outline-none focus:ring-4 focus:ring-white/30"
                  aria-label="Open settings"
                >
                  <Settings size={20} />
                </button>

                {showSettings && (
                  <div className="absolute right-0 mt-2 w-56 origin-top-right rounded-lg bg-white text-slate-800 shadow-2xl ring-1 ring-black/5 overflow-hidden">
                    <button
                      onClick={() =>
                        router.push("/superadmin/Superadminchangepassword")
                      }
                      className="w-full text-left px-4 py-3 hover:bg-slate-50 border-b"
                    >
                      Change Password
                    </button>

                    <button
                      onClick={() => {
                        setShowSettings(false);
                        setSelectedFormulaCountry("uk");
                        setShowFormulaCountryModal(true);
                      }}
                      disabled={formulaUpdating}
                      className="w-full text-left px-4 py-3 hover:bg-slate-50 border-b disabled:cursor-not-allowed disabled:opacity-60"
                    >
                      {formulaUpdating ? "Updating Formula..." : "Formula Update"}
                    </button>

                    <button
                      onClick={handleLogout}
                      className="w-full text-left px-4 py-3 hover:bg-slate-50"
                    >
                      Logout
                    </button>
                  </div>
                )}
              </div>
            </div>
          </div>

          {showSearch && (
            <form onSubmit={handleEmailSearch} className="md:hidden pb-3">
              <div className="relative">
                <input
                  type="text"
                  value={emailInput}
                  onChange={handleSearchInputChange}
                  placeholder="Search email, brand..."
                  className="w-full rounded-lg bg-white/95 text-slate-800 placeholder:text-slate-400 border border-white/40 focus:border-white focus:ring-4 focus:ring-white/20 px-4 py-2.5 shadow"
                />
                <button
                  type="submit"
                  disabled={loading}
                  className="absolute right-1.5 top-1.5 inline-flex items-center justify-center px-3 py-1.5 rounded-md text-white bg-[linear-gradient(135deg,#5EA68E,#1f5274)] hover:opacity-90 focus:outline-none"
                >
                  Go
                </button>
              </div>
            </form>
          )}
        </div>
      </header>

      <div className="flex justify-between items-start py-5">
        <h1 className="truncate text-lg sm:text-2xl font-semibold tracking-tight text-[#414042]">
          Super Admin Dashboard
        </h1>

        <form
          onSubmit={handleEmailSearch}
          className="hidden md:flex items-center gap-2"
        >
          <div className="relative">
            <input
              type="text"
              value={emailInput}
              onChange={handleSearchInputChange}
              placeholder="Search email, brand..."
              className="w-[280px] lg:w-[360px] rounded-lg bg-white/95 text-slate-800 placeholder:text-slate-400 border border-white/40 focus:border-white focus:ring-4 focus:ring-white/20 px-4 py-2.5 shadow"
            />
            <span className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-slate-400">
              <FaSearch />
            </span>
          </div>

          <button
            type="submit"
            disabled={loading}
            className="inline-flex items-center justify-center gap-2 rounded-lg px-4 py-2.5 font-semibold text-[#f8edce] bg-[#37455F] hover:bg-white/20 focus:outline-none focus:ring-4 focus:ring-white/30 disabled:opacity-60"
          >
            Search
          </button>
        </form>
      </div>

      <div className="space-y-4 mb-6">
        <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
          {summaryCards.map((card) => (
            <SummaryMetricCardLarge
              key={card.title}
              title={card.title}
              value={card.value.toLocaleString()}
              className={`bg-white border border-t-4 ${card.accent}`}
              comparisons={[
                {
                  label: "Summary",
                  valueText: card.subText,
                  deltaText: "-",
                  deltaClassName: "text-gray-400",
                },
              ]}
              valueClassName="text-[#414042]"
            />
          ))}
        </div>

        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 rounded-t-xl border border-b-0 border-slate-200 bg-white px-4 py-3">
          <h2 className="text-sm sm:text-base font-semibold text-[#5EA68E]">
            User Brand Registry
          </h2>

          <SegmentedToggle
            value={statusFilter}
            options={STATUS_OPTIONS}
            onChange={setStatusFilter}
            compact
            className="w-fit"
            textSizeClass="text-[10px] sm:text-xs"
          />
        </div>
      </div>

      <div className="mx-auto">
        {error && (
          <div className="mb-4 rounded-lg border border-red-200 bg-red-50 text-red-700 px-4 py-3">
            {error}
          </div>
        )}

        {loading ? (
          <Loader fullscreen backgroundClass="bg-white/80" />
        ) : (
          <div className="space-y-8">
            {searchResult ? (
              <section>
                <h3 className="text-xl font-semibold text-slate-700 mb-3">
                  Search Results for:{" "}
                  <span className="text-slate-900">{emailInput}</span>
                </h3>

                <div className="overflow-x-auto bg-white rounded-xl shadow ring-1 ring-slate-200">
                  <table className="min-w-[1100px] w-full table-auto">
                    <thead className="bg-slate-50">
                      <tr>
                        <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                          Brand Name
                        </th>
                        <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                          Company Name
                        </th>
                        <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                          Native Country
                        </th>
                        <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                          Marketplace Integration
                        </th>
                        <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                          Name
                        </th>
                        <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                          Email
                        </th>
                        <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                          Status
                        </th>
                        <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                          Actions
                        </th>
                      </tr>
                    </thead>
                    <tbody>{renderTableRows(filteredUsers)}</tbody>
                  </table>
                </div>
              </section>
            ) : (
              defaultData && (
                <section>
                  <div className="overflow-x-auto bg-white rounded-xl shadow ring-1 ring-slate-200">
                    <table className="min-w-[1100px] w-full table-auto">
                      <thead className="bg-slate-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                            Brand Name
                          </th>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                            Company Name
                          </th>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                            Native Country
                          </th>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                            Marketplace Integration
                          </th>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                            Name
                          </th>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                            Email
                          </th>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                            Status
                          </th>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                            Actions
                          </th>
                        </tr>
                      </thead>
                      <tbody>{renderTableRows(filteredUsers)}</tbody>
                    </table>
                  </div>
                </section>
              )
            )}
          </div>
        )}
      </div>
      {showFormulaCountryModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 px-4">
          <div className="w-full max-w-md rounded-xl bg-white shadow-2xl">
            <div className="border-b px-6 py-4">
              <h2 className="text-lg font-semibold text-slate-800">
                Formula Update
              </h2>
              <p className="mt-1 text-sm text-slate-500">
                Select country for formula update
              </p>
            </div>

            <div className="space-y-3 px-6 py-5">
              <label
                className={`flex cursor-pointer items-center justify-between rounded-lg border px-4 py-3 transition ${selectedFormulaCountry === "uk"
                  ? "border-[#5EA68E] bg-emerald-50"
                  : "border-slate-200 bg-white"
                  }`}
              >
                <div>
                  <p className="font-medium text-slate-800">UK</p>
                  <p className="text-xs text-slate-500">
                    Marketplace: A1F83G8C2ARO7P
                  </p>
                </div>

                <input
                  type="radio"
                  name="formula_country"
                  value="uk"
                  checked={selectedFormulaCountry === "uk"}
                  onChange={() => setSelectedFormulaCountry("uk")}
                  className="h-4 w-4 accent-[#5EA68E]"
                />
              </label>

              <label
                className={`flex cursor-pointer items-center justify-between rounded-lg border px-4 py-3 transition ${selectedFormulaCountry === "us"
                  ? "border-[#5EA68E] bg-emerald-50"
                  : "border-slate-200 bg-white"
                  }`}
              >
                <div>
                  <p className="font-medium text-slate-800">US</p>
                  <p className="text-xs text-slate-500">
                    Marketplace: ATVPDKIKX0DER
                  </p>
                </div>

                <input
                  type="radio"
                  name="formula_country"
                  value="us"
                  checked={selectedFormulaCountry === "us"}
                  onChange={() => setSelectedFormulaCountry("us")}
                  className="h-4 w-4 accent-[#5EA68E]"
                />
              </label>
            </div>

            <div className="flex justify-end gap-3 border-t px-6 py-4">
              <button
                type="button"
                onClick={() => setShowFormulaCountryModal(false)}
                disabled={formulaUpdating}
                className="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-60"
              >
                Cancel
              </button>

              <button
                type="button"
                onClick={() => handleFormulaUpdate(selectedFormulaCountry)}
                disabled={formulaUpdating}
                className="rounded-lg bg-[#37455F] px-4 py-2 text-sm font-semibold text-[#f8edce] hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {formulaUpdating
                  ? "Updating..."
                  : `Update ${selectedFormulaCountry.toUpperCase()}`}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}