"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { FaSearch } from "react-icons/fa";
import { Eye, Trash2 } from "lucide-react";
import { toast } from "sonner";
import Loader from "@/components/loader/Loader";
import SegmentedToggle from "@/components/ui/SegmentedToggle";
import SummaryMetricCardLarge from "../ViewUserPage/SummaryMetricCardLarge";
import AdminDataTable from "@/components/admin/AdminDataTable";

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
      subText: `${activeUsers} active users`,
      accent: "from-amber-400 to-orange-500",
      bg: "bg-amber-50",
      text: "text-amber-700",
    },
    {
      title: "Active Brands",
      value: totalBrands,
      subText: `${totalBrands} total brands`,
      accent: "from-emerald-400 to-teal-500",
      bg: "bg-emerald-50",
      text: "text-emerald-700",
    },
    {
      title: "Companies",
      value: totalCompanies,
      subText: `${totalCompanies} total companies`,
      accent: "from-sky-400 to-blue-500",
      bg: "bg-sky-50",
      text: "text-sky-700",
    },
    {
      title: "Marketplaces",
      value: totalMarketplaces,
      subText: `${inactiveUsers} inactive users`,
      accent: "from-violet-400 to-indigo-500",
      bg: "bg-violet-50",
      text: "text-violet-700",
    },
  ];

  const tableColumns = [
    "Brand Name",
    "Company",
    "Native Country",
    "Marketplace",
    "Name",
    "Email",
    "Status",
    "Actions",
  ];

  const confirmToggleStatus = (user: UserRow) => {
    const currentStatus = normalizeStatus(user.status);
    const nextLabel = currentStatus === "active" ? "disable" : "enable";

    toast.custom(
      (toastId) => (
        <div className="w-[360px] rounded-xl border border-slate-200 bg-white p-4 shadow-xl">
          <div>
            <h3 className="text-sm font-semibold text-slate-900">
              Confirm status change
            </h3>

            <p className="mt-1 text-sm text-slate-600">
              Are you sure you want to {nextLabel}{" "}
              <span className="font-medium text-slate-900">
                {user.email}
              </span>
              ?
            </p>
          </div>

          <div className="mt-4 flex justify-end gap-2">
            <button
              type="button"
              onClick={() => toast.dismiss(toastId)}
              className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-600 transition hover:bg-slate-50"
            >
              Cancel
            </button>

            <button
              type="button"
              onClick={() => {
                toast.dismiss(toastId);
                handleToggleStatus(user);
              }}
              className={`rounded-lg px-3 py-2 text-xs font-semibold text-white transition ${currentStatus === "active"
                ? "bg-red-600 hover:bg-red-700"
                : "bg-emerald-600 hover:bg-emerald-700"
                }`}
            >
              Yes, {nextLabel}
            </button>
          </div>
        </div>
      ),
      {
        duration: Infinity,
        position: "top-center",
      }
    );
  };

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

  const confirmDeleteAdmin = (email: string) => {
    toast.custom(
      (toastId) => (
        <div className="w-[360px] rounded-xl border border-red-100 bg-white p-4 shadow-xl">
          <div>
            <h3 className="text-sm font-semibold text-slate-900">
              Delete admin?
            </h3>

            <p className="mt-1 text-sm text-slate-600">
              This will permanently delete{" "}
              <span className="font-medium text-slate-900">{email}</span>.
            </p>
          </div>

          <div className="mt-4 flex justify-end gap-2">
            <button
              type="button"
              onClick={() => toast.dismiss(toastId)}
              className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-600 transition hover:bg-slate-50"
            >
              Cancel
            </button>

            <button
              type="button"
              onClick={() => {
                toast.dismiss(toastId);
                handleDeleteAdmin(email);
              }}
              className="rounded-lg bg-red-600 px-3 py-2 text-xs font-semibold text-white transition hover:bg-red-700"
            >
              Yes, delete
            </button>
          </div>
        </div>
      ),
      {
        duration: Infinity,
        position: "top-center",
      }
    );
  };

  const handleDeleteAdmin = async (email: string) => {

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
        <tr
          key={user.id}
          className="group border-b border-slate-100 transition hover:bg-slate-50/80"
        >
          <td className="px-5 py-4">
            <div className="font-medium text-slate-900">
              {user.brand_name || "Not added"}
            </div>
          </td>

          <td className="px-5 py-4 text-sm text-slate-600">
            {user.company_name || "Not added"}
          </td>

          <td className="px-5 py-4 text-sm text-slate-600">
            {nativeCountry}
          </td>

          <td className="px-5 py-4">
            <span className="inline-flex rounded-full bg-slate-100 px-3 py-1 text-xs font-medium capitalize text-slate-700">
              {marketplaceIntegration}
            </span>
          </td>

          <td className="px-5 py-4 text-sm font-medium text-slate-800">
            {user.name || "Not added"}
          </td>

          <td className="px-5 py-4 text-sm text-slate-600 break-all">
            {user.email || "Not added"}
          </td>

          <td className="px-5 py-4">
            <button
              type="button"
              onClick={() => confirmToggleStatus(user)}
              disabled={isBusy}
              className={`relative inline-flex h-6 w-11 items-center rounded-full transition ${userStatus === "active" ? "bg-emerald-500" : "bg-slate-300"
                } ${isBusy ? "cursor-not-allowed opacity-60" : ""}`}
              title={userStatus === "active" ? "Disable admin" : "Enable admin"}
              aria-label={
                userStatus === "active"
                  ? `Disable ${user.email}`
                  : `Enable ${user.email}`
              }
            >
              <span
                className={`inline-block h-4 w-4 rounded-full bg-white shadow transition ${userStatus === "active" ? "translate-x-6" : "translate-x-1"
                  }`}
              />
            </button>
          </td>

          <td className="px-5 py-4">
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() =>
                  router.push(
                    `/superadmin/ViewUserPage/${encodeURIComponent(user.email)}`
                  )
                }
                className="inline-flex h-10 w-10 items-center justify-center rounded-lg border border-slate-200 bg-white text-slate-700 shadow-sm transition hover:border-slate-300 hover:bg-slate-50 hover:text-[#37455F]"
                title="View user"
                aria-label={`View ${user.email}`}
              >
                <Eye size={18} />
              </button>

              <button
                type="button"
                onClick={() => confirmDeleteAdmin(user.email)}
                disabled={isBusy}
                className={`inline-flex h-10 w-10 items-center justify-center rounded-lg border border-red-200 bg-red-50 text-red-600 shadow-sm transition hover:bg-red-100 ${isBusy ? "cursor-not-allowed opacity-60" : ""
                  }`}
                title="Delete admin"
                aria-label={`Delete ${user.email}`}
              >
                <Trash2 size={17} />
              </button>
            </div>
          </td>
        </tr>
      );
    });

  return (
    <>
      <div className="space-y-6">
        {/* Header / Hero */}
        <section className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
          <div className="relative bg-gradient-to-r from-[#37455F] via-[#40516E] to-[#5EA68E] px-6 py-6 text-white">
            <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(255,255,255,0.22),transparent_35%)]" />

            <div className="relative">
              <h1 className="mt-2 text-2xl font-bold tracking-tight">
                Super Admin Dashboard
              </h1>

              <p className="mt-1 max-w-2xl text-sm text-white/75">
                Manage users, brands, companies, marketplace access, account status and
                registry actions from one place.
              </p>
            </div>
          </div>

          <div className="grid grid-cols-1 gap-4 p-5 sm:grid-cols-2 xl:grid-cols-4">
            {summaryCards.map((card) => (
              <div
                key={card.title}
                className="relative overflow-hidden rounded-2xl border border-slate-200 bg-white p-5 shadow-sm transition hover:-translate-y-0.5 hover:shadow-md"
              >
                <div
                  className={`absolute left-0 top-0 h-1 w-full bg-gradient-to-r ${card.accent}`}
                />

                <div className="flex items-start justify-between gap-4">
                  <div>
                    <p className="text-sm font-medium text-slate-500">
                      {card.title}
                    </p>

                    <h3 className="mt-2 text-3xl font-bold text-slate-950">
                      {card.value.toLocaleString()}
                    </h3>

                    <p className="mt-2 text-sm text-slate-500">
                      {card.subText}
                    </p>
                  </div>

                  <div
                    className={`flex h-11 w-11 items-center justify-center rounded-xl ${card.bg} ${card.text}`}
                  >
                    <span className="text-lg font-bold">
                      {String(card.title).charAt(0)}
                    </span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </section>

        {/* Error */}
        {error && (
          <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm font-medium text-red-700">
            {error}
          </div>
        )}

        {/* Registry Panel */}
        {/* Registry Panel */}
        <section className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
          {/* Section Header */}
          <div className="border-b border-slate-200 bg-white px-5 py-4">
            <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
              <div>
                <h2 className="text-lg font-bold text-slate-900">
                  User Brand Registry
                </h2>
              </div>

              <div className="flex flex-col gap-3 lg:flex-row lg:items-center">
                <div className="relative w-full sm:w-[340px] lg:w-[400px]">
                  <input
                    type="text"
                    value={emailInput}
                    onChange={handleSearchInputChange}
                    placeholder="Search by Email, Brand or Company..."
                    className="h-[38px] w-full rounded-xl border border-slate-200 bg-white px-4 pr-11 text-sm text-slate-800 shadow-sm outline-none placeholder:text-slate-400 focus:border-[#5EA68E] focus:ring-4 focus:ring-[#5EA68E]/15"
                  />

                  <span className="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-slate-400">
                    <FaSearch />
                  </span>
                </div>

                <SegmentedToggle
                  value={statusFilter}
                  options={STATUS_OPTIONS}
                  onChange={setStatusFilter}
                  compact
                  className="w-fit rounded-xl bg-white shadow-sm"
                  textSizeClass="text-xs"
                />
              </div>
            </div>
          </div>

          {/* Table */}
          {loading ? (
            <Loader fullscreen backgroundClass="bg-white/80" />
          ) : (
            <AdminDataTable columns={tableColumns} minWidth="1150px">
              {filteredUsers.length > 0 ? (
                renderTableRows(filteredUsers)
              ) : (
                <tr>
                  <td colSpan={8} className="px-5 py-16 text-center">
                    <div className="mx-auto max-w-sm">
                      <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-full bg-slate-100 text-slate-400">
                        <FaSearch />
                      </div>

                      <h3 className="mt-4 text-base font-semibold text-slate-900">
                        No users found
                      </h3>

                      <p className="mt-1 text-sm text-slate-500">
                        Try changing your search keyword or status filter.
                      </p>
                    </div>
                  </td>
                </tr>
              )}
            </AdminDataTable>
          )}
        </section>
      </div>
    </>
  );
}