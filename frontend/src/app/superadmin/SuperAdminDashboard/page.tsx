"use client";

import React, { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { FaSearch } from "react-icons/fa";
import { Settings } from "lucide-react";
import { toast } from "sonner";
import Image from "next/image";
import Loader from "@/components/loader/Loader";
import { FaLongArrowAltRight } from "react-icons/fa";
import SegmentedToggle from "@/components/ui/SegmentedToggle";


// put gif in /public OR keep as import (both work)
// If you want import style, keep it like below and ensure next.config allows it if needed.


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
  status?: string;
};

type DashboardResponse = {
  user_admins?: AdminRow[];
  users?: UserRow[];
};

type ApiError = {
  message?: string;
};

export default function SuperAdminDashboardPage() {
  const [showSettings, setShowSettings] = useState<boolean>(false);
  const [emailInput, setEmailInput] = useState<string>("");
  const [searchResult, setSearchResult] = useState<DashboardResponse | null>(null);
  const [defaultData, setDefaultData] = useState<DashboardResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>("");
  const [searchTerm, setSearchTerm] = useState<string>("");
  const [showSearch, setShowSearch] = useState<boolean>(false);
  const [statusFilter, setStatusFilter] = useState<"all" | "active" | "inactive">("all");

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

const normalizeStatus = (status?: string) => {
  const s = String(status || "").trim().toLowerCase();
  if (s === "inactive") return "inactive";
  return "active";
};

const allUsers = defaultData?.users || [];

const filteredUsers = allUsers.filter((user) => {
  const matchesSearch =
    !emailInput.trim() ||
    user.email.toLowerCase().includes(emailInput.toLowerCase()) ||
    user.brand_name.toLowerCase().includes(emailInput.toLowerCase()) ||
    user.name.toLowerCase().includes(emailInput.toLowerCase()) ||
    user.company_name.toLowerCase().includes(emailInput.toLowerCase());

  const userStatus = normalizeStatus(user.status);

  const matchesStatus =
    statusFilter === "all" ? true : userStatus === statusFilter;

  return matchesSearch && matchesStatus;
});

const totalUsers = allUsers.length;
const activeUsers = allUsers.filter((u) => normalizeStatus(u.status) === "active").length;
const inactiveUsers = allUsers.filter((u) => normalizeStatus(u.status) === "inactive").length;

const totalBrands = new Set(
  allUsers.map((u) => (u.brand_name || "").trim()).filter(Boolean)
).size;

const totalCompanies = new Set(
  allUsers.map((u) => (u.company_name || "").trim()).filter(Boolean)
).size;

const totalMarketplaces = new Set(
  allUsers.map((u) => (u.marketplace_id || "").trim()).filter(Boolean)
).size;

const summaryCards = [
  {
    title: "Total Users",
    value: totalUsers,
    accent: "border-t-[#F4C04E]",
    subText: `↑ ${activeUsers} active users`,
  },
  {
    title: "Active Brands",
    value: totalBrands,
    accent: "border-t-[#C78B57]",
    subText: `↑ ${totalBrands} total brands`,
  },
  {
    title: "Companies",
    value: totalCompanies,
    accent: "border-t-[#2EA8E5]",
    subText: `↑ ${totalCompanies} total companies`,
  },
  {
    title: "Marketplaces",
    value: totalMarketplaces,
    accent: "border-t-[#93A95B]",
    subText: `↑ ${inactiveUsers} inactive users`,
  },
];

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

  const handleCreateAdmin = () => {
    setShowSettings(false);
    router.push("/CreateAdmin");
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
    if (!emailInput.trim()) return;

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
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/dashboard?email=${encodeURIComponent(emailInput)}`,
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

  const highlightText = (term: string) => {
    removeHighlights();
    if (!term) return;

    const bodyTextNodes = document.body.querySelectorAll(
      "*:not(script):not(style):not(input):not(textarea)"
    );

    let firstMatchFound = false;

    bodyTextNodes.forEach((node) => {
      node.childNodes.forEach((child) => {
        if (
          child.nodeType === 3 &&
          (child.textContent || "").toLowerCase().includes(term.toLowerCase())
        ) {
          const span = document.createElement("span");
          const regex = new RegExp(`(${term})`, "gi");
          const parts = (child.textContent || "").split(regex);

          span.innerHTML = parts
            .map((part) =>
              part.toLowerCase() === term.toLowerCase()
                ? `<mark style="background: yellow">${part}</mark>`
                : part
            )
            .join("");

          child.parentNode?.replaceChild(span, child);

          if (!firstMatchFound) {
            firstMatchFound = true;
            setTimeout(() => {
              const firstMark = document.querySelector("mark");
              firstMark?.scrollIntoView({ behavior: "smooth", block: "center" });
            }, 0);
          }
        }
      });
    });
  };

  const removeHighlights = () => {
    const marks = document.querySelectorAll("mark");
    marks.forEach((mark) => {
      const parent = mark.parentNode;
      if (!parent) return;
      parent.replaceChild(document.createTextNode(mark.textContent || ""), mark);
      parent.normalize();
    });
  };

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.ctrlKey && e.key === "f") {
        e.preventDefault();
        setShowSearch(true);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  const handleSearchInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setEmailInput(value);
    setSearchTerm(value);
    highlightText(value);
  };

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
                            className="2xl:w-[220px] 2xl:h-[50px] xl:w-[150px] w-auto "
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
                  <Settings size={20}  />
                </button>

                {showSettings && (
                  <div className="absolute right-0 mt-2 w-56 origin-top-right rounded-lg bg-white text-slate-800 shadow-2xl ring-1 ring-black/5 overflow-hidden">
                    {/* <button
                      onClick={handleCreateAdmin}
                      className="w-full text-left px-4 py-3 hover:bg-slate-50 border-b"
                    >
                      Create Admin
                    </button>
                    <button
                      onClick={() => router.push("/UploadCurrentRatefile")}
                      className="w-full text-left px-4 py-3 hover:bg-slate-50 border-b"
                    >
                      Upload Current Rate file
                    </button>
                    <button
                      onClick={() => router.push("/UploadCurrentRatefile")}
                      className="w-full text-left px-4 py-3 hover:bg-slate-50 border-b"
                    >
                      Upload Reff fee file
                    </button> */}
                    <button
                      onClick={() => router.push("/superadmin/Superadminchangepassword")}
                      className="w-full text-left px-4 py-3 hover:bg-slate-50 border-b"
                    >
                      Change Password
                    </button>
                    <button
                      onClick={() => router.push("/superadmin/DeleteAdmin")}
                      className="w-full text-left px-4 py-3 hover:bg-slate-50 border-b"
                    >
                      Delete Admin
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
              <form onSubmit={handleEmailSearch} className="hidden md:flex items-center gap-2">
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
  {/* Summary Cards */}
  <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
    {summaryCards.map((card) => (
      <div
        key={card.title}
        className={`rounded-xl border border-slate-200 border-t-4 ${card.accent} bg-white shadow-sm px-4 py-4`}
      >
        <div className="text-[11px] sm:text-xs text-slate-500 font-medium">
          {card.title}
        </div>
        <div className="mt-2 text-xl sm:text-2xl font-semibold text-[#414042]">
          {card.value.toLocaleString()}
        </div>
        <div className="mt-2 text-[11px] sm:text-xs text-[#5EA68E] font-medium">
          {card.subText}
        </div>
      </div>
    ))}
  </div>

  {/* Table Header Row */}
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
            {!searchResult && defaultData && (
              <div className="space-y-8">
                <section>
                  <div className="overflow-x-auto bg-white rounded-xl shadow ring-1 ring-slate-200">
                    <table className="min-w-[700px] w-full table-auto">
                      <thead className="bg-slate-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                              Name
                          </th>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                              Email
                          </th>
                          
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                            Brand Name
                          </th>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                            Company Name
                          </th>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                            Country
                          </th>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
                            Marketplace ID
                          </th>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
  Status
</th>
                          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600"></th>
                        </tr>
                      </thead>
                      <tbody>
                       {filteredUsers.map((user) => {
  const userStatus = normalizeStatus(user.status);

  return (
    <tr key={user.id} className="border-t">
      <td className="px-4 py-3 text-sm text-slate-700">{user.name}</td>
      <td className="px-4 py-3 text-sm text-slate-700">{user.email}</td>
      <td className="px-4 py-3 text-sm text-slate-700">{user.brand_name}</td>
      <td className="px-4 py-3 text-sm text-slate-700">{user.company_name}</td>
      <td className="px-4 py-3 text-sm text-slate-700">{user.country}</td>
      <td className="px-4 py-3 text-sm text-slate-700">{user.marketplace_id}</td>

      <td className="px-4 py-3 text-sm text-slate-700">
        <div className="inline-flex items-center gap-2">
          <span
            className={`inline-block h-2 w-2 rounded-full ${
              userStatus === "active" ? "bg-green-500" : "bg-red-500"
            }`}
          />
          <span className="capitalize">
            {userStatus}
          </span>
        </div>
      </td>

      <td className="px-4 py-3 text-sm text-slate-700">
        <button
          onClick={() =>
            router.push(`/superadmin/ViewUserPage/${encodeURIComponent(user.email)}`)
          }
          className="inline-flex items-center justify-center px-3 py-2 rounded-md bg-white border border-[#414042] text-[#414042] shadow"
        >
          View <FaLongArrowAltRight className="pl-1" />
        </button>
      </td>
    </tr>
  );
})}
                      </tbody>
                    </table>
                  </div>
                </section>
              </div>
            )}

            {searchResult && defaultData && (
              <section>
  <h3 className="text-xl font-semibold text-slate-700 mb-3">
    Search Results for: <span className="text-slate-900">{emailInput}</span>
  </h3>

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

  <div className="overflow-x-auto bg-white rounded-b-xl shadow ring-1 ring-slate-200">
    <table className="min-w-[900px] w-full table-auto">
      <thead className="bg-slate-50">
        <tr>
          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
            Name
          </th>
          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
            Email
          </th>
          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
            Brand Name
          </th>
          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
            Company Name
          </th>
          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
            Country
          </th>
          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
            Marketplace ID
          </th>
          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600">
            Status
          </th>
          <th className="px-4 py-3 text-left text-sm font-semibold text-slate-600"></th>
        </tr>
      </thead>
      <tbody>
        {filteredUsers.map((user) => {
          const userStatus = normalizeStatus(user.status);

          return (
            <tr key={user.id} className="border-t">
              <td className="px-4 py-3 text-sm text-slate-700">{user.name}</td>
              <td className="px-4 py-3 text-sm text-slate-700">{user.email}</td>
              <td className="px-4 py-3 text-sm text-slate-700">{user.brand_name}</td>
              <td className="px-4 py-3 text-sm text-slate-700">{user.company_name}</td>
              <td className="px-4 py-3 text-sm text-slate-700">{user.country}</td>
              <td className="px-4 py-3 text-sm text-slate-700">{user.marketplace_id}</td>
              <td className="px-4 py-3 text-sm text-slate-700">
                <div className="inline-flex items-center gap-2">
                  <span
                    className={`inline-block h-2 w-2 rounded-full ${
                      userStatus === "active" ? "bg-green-500" : "bg-red-500"
                    }`}
                  />
                  <span className="capitalize">{userStatus}</span>
                </div>
              </td>
              <td className="px-4 py-3 text-sm text-slate-700">
                <button
                  onClick={() =>
                    router.push(`/superadmin/ViewUserPage/${encodeURIComponent(user.email)}`)
                  }
                  className="inline-flex items-center justify-center px-3 py-2 rounded-md bg-white border border-[#414042] text-[#414042] shadow"
                >
                  View <FaLongArrowAltRight className="pl-1" />
                </button>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  </div>
</section>
            )}
          </div>
        )}
      </div>
    </div>
  );
}