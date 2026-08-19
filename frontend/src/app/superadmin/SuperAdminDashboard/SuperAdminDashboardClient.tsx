"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { FaSearch } from "react-icons/fa";
import { Users, Tags, Building2, Globe2 } from "lucide-react";
import { toast } from "sonner";
import Loader from "@/components/loader/Loader";
import SuperAdminUsersTable from "@/components/admin/table/SuperAdminUsersTable";

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
  countries?: string[];
  marketplace_id?: string;
  marketplace_ids?: string[];
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

const getConnectedCountries = (user: UserRow) => {
  const countries = Array.isArray(user.countries)
    ? user.countries
    : user.country?.split(",") || [];

  return countries
    .map((country) => country.trim())
    .filter(Boolean);
};

const formatCountryLabel = (country: string) => country.trim().toUpperCase();

const getConnectedCountryLabel = (user: UserRow) =>
  getConnectedCountries(user).map(formatCountryLabel).join(", ");

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

  const handleViewUser = (email: string) => {
    router.push(`/superadmin/ViewUserPage/${encodeURIComponent(email)}`);
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
        (user.company_name || "").toLowerCase().includes(query) ||
        getConnectedCountryLabel(user).toLowerCase().includes(query);

      const userStatus = normalizeStatus(user.status);
      const matchesStatus =
        statusFilter === "all" ? true : userStatus === statusFilter;

      return matchesSearch && matchesStatus;
    });
  }, [allUsers, emailInput, statusFilter]);

  const totalUsers = usersData.length;

  const totalBrands = new Set(
    usersData.map((u) => (u.brand_name || "").trim()).filter(Boolean)
  ).size;

  const totalCompanies = new Set(
    usersData.map((u) => (u.company_name || "").trim()).filter(Boolean)
  ).size;

  const totalCountries = new Set(
    usersData
      .flatMap(getConnectedCountries)
      .map((country) => country.toLowerCase())
      .filter(Boolean)
  ).size;

  const summaryCards = [
    {
      title: "Total Users",
      value: totalUsers,
      icon: Users,
      iconBg: "bg-[#31d9e5]/15",
      iconText: "text-[#31d9e5]",
      borderTop: "border-t-[#31d9e5]",
    },
    {
      title: "Active Brands",
      value: totalBrands,
      icon: Tags,
      iconBg: "bg-[#31d9e5]/15",
      iconText: "text-[#31d9e5]",
      borderTop: "border-t-[#31d9e5]",
    },
    {
      title: "Companies",
      value: totalCompanies,
      icon: Building2,
      iconBg: "bg-[#31d9e5]/15",
      iconText: "text-[#31d9e5]",
      borderTop: "border-t-[#31d9e5]",
    },
    {
      title: "Countries",
      value: totalCountries,
      icon: Globe2,
      iconBg: "bg-[#31d9e5]/15",
      iconText: "text-[#31d9e5]",
      borderTop: "border-t-[#31d9e5]",
    },
  ];

  const confirmToggleStatus = (user: UserRow) => {
    const currentStatus = normalizeStatus(user.status);
    const nextLabel = currentStatus === "active" ? "disable" : "enable";

    toast.custom(
      (toastId) => (
        <div className="w-[360px] rounded-xl border border-white/10 bg-[#37384f] p-4 text-white shadow-[0_24px_55px_rgba(20,22,45,0.45)]">
          <div>
            <h3 className="text-sm font-semibold text-white">
              Confirm status change
            </h3>

            <p className="mt-1 text-sm text-white/60">
              Are you sure you want to {nextLabel}{" "}
              <span className="font-medium text-white">
                {user.company_name?.trim() || "Company not added"}
              </span>
              ?
            </p>
          </div>

          <div className="mt-4 flex justify-end gap-2">
            <button
              type="button"
              onClick={() => toast.dismiss(toastId)}
              className="rounded-lg border border-white/10 bg-white/[0.06] px-3 py-2 text-xs font-semibold text-white/75 transition hover:bg-white/10 hover:text-white"
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


  return (
    <>
      <div className="space-y-7 text-white">
        {/* Page Heading */}
        <div className="rounded-2xl border border-white/10 bg-[#484962] px-5 py-5 shadow-[0_18px_40px_rgba(20,22,45,0.25)]">
          <div className="flex flex-col leading-tight">
            <div className="flex items-baseline gap-2">
              <h1 className="text-2xl font-bold tracking-tight text-white">
                Welcome,
              </h1>

              <span className="text-2xl font-bold tracking-tight text-[#31d9e5]">
                Super Admin!
              </span>
            </div>

            <p className="mt-2 text-sm text-white/60">
              Manage users, brands, companies, countries and account status.
            </p>
          </div>
        </div>

        {/* Summary Cards */}
        <section className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
          {summaryCards.map((card) => {
            const Icon = card.icon;

            return (
              <div
                key={card.title}
                className={`rounded-2xl border border-t-4 border-white/10 bg-[#484962] p-5 shadow-[0_18px_40px_rgba(20,22,45,0.22)] transition hover:-translate-y-0.5 hover:bg-[#4f506b] hover:shadow-[0_22px_48px_rgba(20,22,45,0.30)] ${card.borderTop}`}
              >
                <div className="flex items-center gap-4">
                  <div
                    className={`flex h-12 w-12 shrink-0 items-center justify-center rounded-xl ${card.iconBg}`}
                  >
                    <Icon size={22} className={card.iconText} />
                  </div>

                  <div className="min-w-0">
                    <p className="text-sm font-medium text-white/60">
                      {card.title}
                    </p>

                    <h3 className="mt-1 text-xl font-bold tracking-tight text-white">
                      {card.value.toLocaleString()}
                    </h3>
                  </div>
                </div>
              </div>
            );
          })}
        </section>

        {/* Error */}
        {error && (
          <div className="rounded-xl border border-red-300/25 bg-red-500/10 px-4 py-3 text-sm font-medium text-red-100">
            {error}
          </div>
        )}

        {/* Registry Section */}
        <section className="space-y-4">
          <div className="rounded-2xl border border-white/10 bg-[#484962] p-5 shadow-[0_18px_40px_rgba(20,22,45,0.25)]">
            {/* Header */}
            <div className="mb-5 flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
              <div>
                <h2 className="text-2xl font-bold tracking-tight text-white">
                  Recent Brands
                </h2>

                <p className="mt-1 text-sm text-white/55">
                  View, search, and manage registered user brands.
                </p>
              </div>

              <div className="flex flex-col gap-3 lg:flex-row lg:items-center">
                <div className="relative w-full sm:w-[340px] lg:w-[400px]">
                  <input
                    type="text"
                    value={emailInput}
                    onChange={handleSearchInputChange}
                    placeholder="Search by Email, Brand, Company or Country..."
                    className="h-[42px] w-full rounded-xl border border-white/10 bg-white/[0.06] px-4 pr-11 text-sm text-white shadow-sm outline-none placeholder:text-white/40 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15"
                  />

                  <span className="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-[#31d9e5]">
                    <FaSearch />
                  </span>
                </div>
              </div>
            </div>

            {/* Table Card */}
            {loading ? (
              <Loader fullscreen backgroundClass="bg-[#37384f]/80" />
            ) : (
              <div className="overflow-hidden rounded-2xl border border-white/10 bg-white/[0.04]">
                <SuperAdminUsersTable
                  users={filteredUsers}
                  actionLoading={actionLoading}
                  normalizeStatus={normalizeStatus}
                  onToggleStatus={confirmToggleStatus}
                  onDeleteUser={confirmDeleteAdmin}
                  onViewUser={handleViewUser}
                />
              </div>
            )}
          </div>
        </section>
      </div>
    </>
  );
}
