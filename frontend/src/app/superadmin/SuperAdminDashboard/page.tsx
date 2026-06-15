"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { FaSearch, FaLongArrowAltRight } from "react-icons/fa";
import { Trash2 } from "lucide-react";
import { toast } from "sonner";
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
  // const [showSettings, setShowSettings] = useState<boolean>(false);
  // const [formulaUpdating, setFormulaUpdating] = useState<boolean>(false);
  const [emailInput, setEmailInput] = useState<string>("");
  const [searchResult, setSearchResult] = useState<DashboardResponse | null>(null);
  const [defaultData, setDefaultData] = useState<DashboardResponse | null>(null);
  const [usersData, setUsersData] = useState<UserRow[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>("");
  const [showSearch, setShowSearch] = useState<boolean>(false);
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [actionLoading, setActionLoading] = useState<Record<string, boolean>>({});
  // const [showFormulaCountryModal, setShowFormulaCountryModal] = useState(false);
  // const [selectedFormulaCountry, setSelectedFormulaCountry] = useState<"uk" | "us">("uk");
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

  // const handleFormulaUpdate = async (country: "uk" | "us") => {
  //   const token = localStorage.getItem("superadmin_token");

  //   if (!token) {
  //     toast.error("Superadmin token not found. Please login again.");
  //     router.push("/superadmin/CDPAdminConsole");
  //     return;
  //   }

  //   const marketplaces = {
  //     uk: {
  //       country: "uk",
  //       marketplace_id: "A1F83G8C2ARO7P",
  //     },
  //     us: {
  //       country: "us",
  //       marketplace_id: "ATVPDKIKX0DER",
  //     },
  //   };

  //   const selectedMarketplace = marketplaces[country];

  //   try {
  //     setFormulaUpdating(true);
  //     setShowFormulaCountryModal(false);

  //     const transactionStatus =
  //       selectedMarketplace.country === "us" ? "RELEASED,DEFERRED" : "RELEASED";

  //     const url =
  //       `${process.env.NEXT_PUBLIC_API_BASE_URL}/amazon_api/formula_update` +
  //       `?country=${selectedMarketplace.country}` +
  //       `&marketplace_id=${selectedMarketplace.marketplace_id}` +
  //       `&store_in_db=true` +
  //       `&run_upload_pipeline=true` +
  //       `&transaction_status=${encodeURIComponent(transactionStatus)}`;

  //     const response = await fetch(url, {
  //       method: "GET",
  //       headers: {
  //         Authorization: `Bearer ${token}`,
  //         "Content-Type": "application/json",
  //       },
  //     });

  //     const json = await response.json().catch(() => ({}));

  //     if (!response.ok || !json?.success) {
  //       throw new Error(
  //         json?.error ||
  //         json?.message ||
  //         `Formula update failed for ${selectedMarketplace.country.toUpperCase()}`
  //       );
  //     }

  //     toast.success(
  //       `Formula update completed for ${selectedMarketplace.country.toUpperCase()}. Runs: ${json.total_month_runs || 0
  //       }, Success: ${json.success_count || 0}, Failed: ${json.failed_count || 0
  //       }, Skipped: ${json.total_skipped_old_months || 0}`
  //     );

  //     console.log("Formula update result:", json);
  //   } catch (error) {
  //     const msg = error instanceof Error ? error.message : "Formula update failed";
  //     toast.error(msg);
  //   } finally {
  //     setFormulaUpdating(false);
  //   }
  // };

  // const handleLogout = async () => {
  //   setShowSettings(false);

  //   const token = localStorage.getItem("superadmin_token");

  //   try {
  //     if (token) {
  //       await toast.promise(
  //         fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin_logout`, {
  //           method: "POST",
  //           headers: { Authorization: `Bearer ${token}` },
  //         }),
  //         {
  //           loading: "Logging you out…",
  //           success: "Logged out successfully.",
  //           error: "Logout failed. Please try again.",
  //         }
  //       );
  //     } else {
  //       toast.info("You’re already logged out.");
  //     }
  //   } finally {
  //     localStorage.removeItem("superadmin_token");
  //     router.push("/superadmin/CDPAdminConsole");
  //   }
  // };

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
    <>
      <div className="flex justify-end pb-5">
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
              className="w-[280px] lg:w-[360px] rounded-lg bg-white text-slate-800 placeholder:text-slate-400 border border-slate-200 focus:border-[#5EA68E] focus:ring-4 focus:ring-[#5EA68E]/20 px-4 py-2.5 shadow"
            />
            <span className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-slate-400">
              <FaSearch />
            </span>
          </div>

          <button
            type="submit"
            disabled={loading}
            className="inline-flex items-center justify-center gap-2 rounded-lg bg-[#37455F] px-4 py-2.5 font-semibold text-[#f8edce] hover:opacity-90 disabled:opacity-60"
          >
            Search
          </button>
        </form>
      </div>

      {showSearch && (
        <form onSubmit={handleEmailSearch} className="mb-5 md:hidden">
          <div className="relative">
            <input
              type="text"
              value={emailInput}
              onChange={handleSearchInputChange}
              placeholder="Search email, brand..."
              className="w-full rounded-lg border border-slate-200 bg-white px-4 py-2.5 text-slate-800 shadow placeholder:text-slate-400 focus:border-[#5EA68E] focus:ring-4 focus:ring-[#5EA68E]/20"
            />
            <button
              type="submit"
              disabled={loading}
              className="absolute right-1.5 top-1.5 inline-flex items-center justify-center rounded-md bg-[#37455F] px-3 py-1.5 text-white hover:opacity-90"
            >
              Go
            </button>
          </div>
        </form>
      )}

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
          <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-red-700">
            {error}
          </div>
        )}

        {loading ? (
          <Loader fullscreen backgroundClass="bg-white/80" />
        ) : (
          <div className="space-y-8">
            {searchResult ? (
              <section>
                <h3 className="mb-3 text-xl font-semibold text-slate-700">
                  Search Results for:{" "}
                  <span className="text-slate-900">{emailInput}</span>
                </h3>

                <div className="overflow-x-auto rounded-xl bg-white shadow ring-1 ring-slate-200">
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
                  <div className="overflow-x-auto rounded-xl bg-white shadow ring-1 ring-slate-200">
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
    </>
  );
}