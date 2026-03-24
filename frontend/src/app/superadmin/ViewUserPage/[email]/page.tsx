"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import Image from "next/image";
import { Settings, Mail, Building2, BarChart3, Users, UserCircle2 } from "lucide-react";
import { toast } from "sonner";

type AnyRecord = Record<string, any>;

type UploadHistoryRow = {
  id: number | string;
  country: string;
  month: string | number;
  year: string | number;
  total_sales: number;
  total_profit: number;
  total_expense: number;
};

type CountryProfileRow = {
  id: number | string;
  country: string;
  stock_unit: string | number;
  transit_time: string | number;
};

type SkuWiseTable = {
  table?: string;
  rows?: AnyRecord[];
  error?: string;
};

type ViewUserData = {
  user_id?: number | string;
  brand_name?: string;
  name?: string;
  email?: string;
  annual_sales_range?: string;
  related_upload_history?: UploadHistoryRow[];
  related_country_profiles?: CountryProfileRow[];
  skuwise_tables?: SkuWiseTable[];
};

type SummaryData = {
  costOfAdvertisement: number;
  platformFees: number;
  cm2ProfitLoss: number;
  cm2Margins: string;
  acos: string;
  netReimbursement: number;
  reimbursementVsCM2Margins: string;
  reimbursementVsSales: string;
};

type MemberRow = {
  countries?: string[];
  email?: string;
  marketplace_ids?: string[];
  member_name?: string;
  modules?: string[];
  role?: string;
};

export default function ViewUserPage() {
  const params = useParams<{ email: string }>();
  const email = params?.email ? decodeURIComponent(params.email) : "";

  const [data, setData] = useState<ViewUserData | null>(null);
  const [members, setMembers] = useState<MemberRow[]>([]);
  const [membersLoading, setMembersLoading] = useState<boolean>(true);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string>("");
  const [openRegions, setOpenRegions] = useState<Record<string, boolean>>({});
  const [openTables, setOpenTables] = useState<Record<string, boolean>>({});
  const [openSubsections, setOpenSubsections] = useState<Record<string, boolean>>({});
  const [showSettings, setShowSettings] = useState<boolean>(false);

  const router = useRouter();

  const preferredColumns = [
    "product_name",
    "sku",
    "quantity",
    "asp",
    "net_sales",
    "cost_of_unit_sold",
    "amazon_fee",
    "selling_fees",
    "fba_fees",
    "net_credits",
    "net_taxes",
    "profit",
    "profit_percentage",
    "sales_mix",
    "profit_mix",
    "price_in_gbp",
  ].filter(Boolean);

  const toggleRegion = (region: string) =>
    setOpenRegions((prev) => ({ ...prev, [region]: !prev[region] }));

  const toggleTable = (key: string) =>
    setOpenTables((prev) => ({ ...prev, [key]: !prev[key] }));

  const toggleSubsection = (region: string, type: string) => {
    const key = `${region}-${type}`;
    setOpenSubsections((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const extractSummaryData = (rows?: AnyRecord[]): SummaryData | null => {
    if (!rows || rows.length === 0) return null;
    const lastRow = rows[rows.length - 1];

    const pct = (v: any) => {
      const n = Number(v);
      return Number.isNaN(n) ? "0.00%" : `${n.toFixed(2)}%`;
    };

    return {
      costOfAdvertisement: Number(lastRow.advertising_total) || 0,
      platformFees: Number(lastRow.platform_fee) || 0,
      cm2ProfitLoss: Number(lastRow.cm2_profit) || 0,
      cm2Margins: pct(lastRow.cm2_margins),
      acos: pct(lastRow.acos),
      netReimbursement: Number(lastRow.rembursement_fee) || 0,
      reimbursementVsCM2Margins: pct(lastRow.rembursment_vs_cm2_margins),
      reimbursementVsSales: pct(lastRow.reimbursement_vs_sales),
    };
  };

  useEffect(() => {
    const fetchUserDetails = async () => {
      const token = localStorage.getItem("superadmin_token");
      const admin_token = localStorage.getItem("admin_token");

      if (!token && !admin_token) {
        setError("You are not authorized to view this page.");
        setLoading(false);
        setMembersLoading(false);
        return;
      }

      try {
        let result: ViewUserData | null = null;
        const authToken = token || admin_token;

        if (token) {
          const res = await fetch(
            `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/dashboard?email=${encodeURIComponent(
              email
            )}`,
            {
              method: "GET",
              headers: { Authorization: `Bearer ${token}` },
            }
          );
          const json = (await res.json()) as any;
          if (!res.ok) throw new Error(json.message || "Superadmin fetch failed");
          result = json as ViewUserData;
        }

        if (admin_token) {
          const res = await fetch(
            `${process.env.NEXT_PUBLIC_API_BASE_URL}/admin/dashboard?email=${encodeURIComponent(
              email
            )}`,
            {
              method: "GET",
              headers: { Authorization: `Bearer ${admin_token}` },
            }
          );
          const json = (await res.json()) as any;
          if (!res.ok) throw new Error(json.message || "Admin fetch failed");
          result = result ? ({ ...result, ...json } as ViewUserData) : (json as ViewUserData);
        }

        setData(result);

        try {
          setMembersLoading(true);
         const membersRes = await fetch(
  `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/dashboard/members?email=${encodeURIComponent(
    email
  )}`,
  {
    method: "GET",
    headers: {
      Authorization: `Bearer ${authToken}`,
    },
  }
);

          const membersJson = await membersRes.json();

          if (membersRes.ok) {
            setMembers(Array.isArray(membersJson?.data) ? membersJson.data : []);
          } else {
            setMembers([]);
          }
        } catch {
          setMembers([]);
        } finally {
          setMembersLoading(false);
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        setLoading(false);
      }
    };

    if (email) fetchUserDetails();
  }, [email]);

  const groupedTables: Record<
    "UK" | "US" | "GLOBAL",
    { MTDs: SkuWiseTable[]; QTDs: SkuWiseTable[]; YTDs: SkuWiseTable[] }
  > = {
    UK: { MTDs: [], QTDs: [], YTDs: [] },
    US: { MTDs: [], QTDs: [], YTDs: [] },
    GLOBAL: { MTDs: [], QTDs: [], YTDs: [] },
  };

  if (Array.isArray(data?.skuwise_tables)) {
    const seen: Record<
      "UK" | "US" | "GLOBAL",
      { MTDs: Set<string>; QTDs: Set<string>; YTDs: Set<string> }
    > = {
      UK: { MTDs: new Set(), QTDs: new Set(), YTDs: new Set() },
      US: { MTDs: new Set(), QTDs: new Set(), YTDs: new Set() },
      GLOBAL: { MTDs: new Set(), QTDs: new Set(), YTDs: new Set() },
    };

    data.skuwise_tables.forEach((table) => {
      const name = String(table.table || "").toUpperCase();
      const hasUK = name.includes("UK");
      const hasUS = name.includes("US");
      const hasGLOBAL = name.includes("GLOBAL");

      let type: "MTDs" | "QTDs" | "YTDs" = "MTDs";
      if (name.includes("YEARLY")) type = "YTDs";
      else if (name.includes("QUARTER")) type = "QTDs";

      let region: "UK" | "US" | "GLOBAL" | null = null;
      if (hasUK && !hasGLOBAL) region = "UK";
      else if (hasUS && !hasGLOBAL) region = "US";
      else if (hasGLOBAL) region = "GLOBAL";

      if (region && !seen[region][type].has(name)) {
        seen[region][type].add(name);
        groupedTables[region][type].push(table);
      }
    });
  }

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

  const renderSummarySection = (summaryData: SummaryData | null, colCount: number) => {
    if (!summaryData) return null;

    const labelCell = (label: string) => (
      <td
        colSpan={Math.max(colCount - 1, 1)}
        className="bg-slate-50 px-3 py-2 text-xs sm:text-sm font-medium text-slate-600 text-right"
      >
        {label}
      </td>
    );

    const valCell = (val: string | number) => (
      <td className="bg-slate-50 px-3 py-2 text-xs sm:text-sm font-semibold text-slate-800">
        {typeof val === "number" ? val.toLocaleString() : val}
      </td>
    );

    return (
      <tfoot className="border-t">
        <tr>
          {labelCell("Cost of Advertisement")}
          {valCell(summaryData.costOfAdvertisement)}
        </tr>
        <tr>
          {labelCell("Platform Fees")}
          {valCell(summaryData.platformFees)}
        </tr>
        <tr>
          {labelCell("CM2 Profit/Loss")}
          {valCell(summaryData.cm2ProfitLoss)}
        </tr>
        <tr>
          {labelCell("CM2 Margins")}
          {valCell(summaryData.cm2Margins)}
        </tr>
        <tr>
          {labelCell("ACOS (Average Cost of Sales)")}
          {valCell(summaryData.acos)}
        </tr>
        <tr>
          {labelCell("Net Reimbursement during the month")}
          {valCell(summaryData.netReimbursement)}
        </tr>
        <tr>
          {labelCell("Reimbursement vs CM2 Margins")}
          {valCell(summaryData.reimbursementVsCM2Margins)}
        </tr>
        <tr>
          {labelCell("Reimbursement vs Sales")}
          {valCell(summaryData.reimbursementVsSales)}
        </tr>
      </tfoot>
    );
  };

  const displayName = useMemo(() => data?.name || "Admin Profile", [data?.name]);
  const displayEmail = useMemo(() => data?.email || email || "-", [data?.email, email]);

  const initials = useMemo(() => {
    const source = data?.name || email || "A";
    const parts = source.trim().split(" ").filter(Boolean);
    if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
    return `${parts[0][0] || ""}${parts[1][0] || ""}`.toUpperCase();
  }, [data?.name, email]);

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-emerald-50 to-slate-100">
        <div className="h-10 w-10 animate-spin rounded-full border-2 border-[#1f5274]/30 border-t-[#1f5274]" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen p-4 sm:p-6 bg-gradient-to-br from-emerald-50 to-slate-100">
        <div className="max-w-full mx-auto">
          <div className="rounded-lg border border-red-200 bg-red-50 text-red-700 px-4 py-3">
            {error}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen w-full bg-gradient-to-br from-emerald-50 via-slate-50 to-slate-100 p-4 sm:p-6">
      <header className="sticky top-0 z-40 w-full bg-gradient-to-r from-[#5EA68E] to-[#1f5274] rounded-2xl shadow-lg">
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
        </div>
      </header>

      <div className="mt-6 space-y-8">
        {/* Admin profile card */}
        <section className="rounded-3xl bg-white shadow-xl ring-1 ring-slate-200 overflow-hidden">
          <div className="bg-gradient-to-r from-[#1f5274] via-[#2d6a8d] to-[#5EA68E] px-6 sm:px-8 py-8">
            <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-6">
              <div className="flex items-center gap-4 sm:gap-5">
                <div className="h-20 w-20 sm:h-24 sm:w-24 rounded-2xl bg-white/15 backdrop-blur-sm border border-white/20 text-white flex items-center justify-center text-2xl sm:text-3xl font-bold shadow-lg">
                  {initials}
                </div>

                <div className="min-w-0">
                  <p className="text-white/80 text-sm sm:text-base font-medium">
                    Admin Profile
                  </p>
                  <h1 className="text-white text-2xl sm:text-3xl font-bold break-words">
                    {displayName}
                  </h1>
                  <p className="text-white/90 text-sm sm:text-base break-all mt-1">
                    {displayEmail}
                  </p>
                </div>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 w-full lg:w-auto">
                <div className="rounded-2xl bg-white/10 backdrop-blur-sm border border-white/15 px-4 py-4 min-w-[180px]">
                  <p className="text-white/75 text-xs uppercase tracking-wide">Brand Name</p>
                  <p className="text-white text-sm sm:text-base font-semibold mt-1 break-words">
                    {data?.brand_name || "-"}
                  </p>
                </div>

                <div className="rounded-2xl bg-white/10 backdrop-blur-sm border border-white/15 px-4 py-4 min-w-[180px]">
                  <p className="text-white/75 text-xs uppercase tracking-wide">Annual Sales</p>
                  <p className="text-white text-sm sm:text-base font-semibold mt-1 break-words">
                    {data?.annual_sales_range || "-"}
                  </p>
                </div>

                <div className="rounded-2xl bg-white/10 backdrop-blur-sm border border-white/15 px-4 py-4 min-w-[180px]">
                  <p className="text-white/75 text-xs uppercase tracking-wide">Members Added</p>
                  <p className="text-white text-sm sm:text-base font-semibold mt-1">
                    {members.length}
                  </p>
                </div>
              </div>
            </div>
          </div>

          {/* <div className="px-6 sm:px-8 py-6">
            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
              <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-4">
                <div className="flex items-center gap-3">
                  <div className="rounded-xl bg-[#1f5274]/10 p-3 text-[#1f5274]">
                    <UserCircle2 size={20} />
                  </div>
                  <div>
                    <p className="text-xs text-slate-500 font-medium">Admin Name</p>
                    <p className="text-sm font-semibold text-slate-800 break-words">
                      {displayName}
                    </p>
                  </div>
                </div>
              </div>

              <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-4">
                <div className="flex items-center gap-3">
                  <div className="rounded-xl bg-[#5EA68E]/10 p-3 text-[#5EA68E]">
                    <Mail size={20} />
                  </div>
                  <div>
                    <p className="text-xs text-slate-500 font-medium">Email</p>
                    <p className="text-sm font-semibold text-slate-800 break-all">
                      {displayEmail}
                    </p>
                  </div>
                </div>
              </div>

              <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-4">
                <div className="flex items-center gap-3">
                  <div className="rounded-xl bg-emerald-100 p-3 text-emerald-700">
                    <Building2 size={20} />
                  </div>
                  <div>
                    <p className="text-xs text-slate-500 font-medium">Brand</p>
                    <p className="text-sm font-semibold text-slate-800 break-words">
                      {data?.brand_name || "-"}
                    </p>
                  </div>
                </div>
              </div>

              <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-4">
                <div className="flex items-center gap-3">
                  <div className="rounded-xl bg-blue-100 p-3 text-blue-700">
                    <BarChart3 size={20} />
                  </div>
                  <div>
                    <p className="text-xs text-slate-500 font-medium">Annual Sales Range</p>
                    <p className="text-sm font-semibold text-slate-800 break-words">
                      {data?.annual_sales_range || "-"}
                    </p>
                  </div>
                </div>
              </div>
            </div>
          </div> */}
        </section>

        {/* Members section */}
        <section className="rounded-3xl bg-white shadow-xl ring-1 ring-slate-200 overflow-hidden">
          <div className="flex items-center justify-between gap-3 px-6 sm:px-8 py-5 border-b border-slate-200 bg-slate-50">
            <div className="flex items-center gap-3">
              <div className="rounded-xl bg-[#5EA68E]/10 p-3 text-[#5EA68E]">
                <Users size={20} />
              </div>
              <div>
                <h2 className="text-lg sm:text-xl font-bold text-[#1f5274]">
                  Members Information
                </h2>
                <p className="text-sm text-slate-500">
                  {members.length} member{members.length !== 1 ? "s" : ""} added by this admin
                </p>
              </div>
            </div>
          </div>

          <div className="p-6 sm:p-8">
            {membersLoading ? (
              <div className="flex items-center justify-center py-10">
                <div className="h-8 w-8 animate-spin rounded-full border-2 border-[#1f5274]/30 border-t-[#1f5274]" />
              </div>
            ) : members.length > 0 ? (
              <div className="overflow-x-auto rounded-2xl border border-slate-200">
                <table className="w-full table-auto">
                  <thead className="bg-slate-100">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs sm:text-sm font-semibold text-slate-600 whitespace-nowrap">
                        Member Name
                      </th>
                      <th className="px-4 py-3 text-left text-xs sm:text-sm font-semibold text-slate-600 whitespace-nowrap">
                        Email
                      </th>
                      <th className="px-4 py-3 text-left text-xs sm:text-sm font-semibold text-slate-600 whitespace-nowrap">
                        Role
                      </th>
                      <th className="px-4 py-3 text-left text-xs sm:text-sm font-semibold text-slate-600 whitespace-nowrap">
                        Countries
                      </th>
                      <th className="px-4 py-3 text-left text-xs sm:text-sm font-semibold text-slate-600 whitespace-nowrap">
                        Modules
                      </th>
                      <th className="px-4 py-3 text-left text-xs sm:text-sm font-semibold text-slate-600 whitespace-nowrap">
                        Marketplace IDs
                      </th>
                    </tr>
                  </thead>

                  <tbody>
                    {members.map((member, index) => (
                      <tr
                        key={`${member.email || member.member_name || "member"}-${index}`}
                        className="border-t hover:bg-slate-50 transition-colors"
                      >
                        <td className="px-4 py-4 text-sm text-slate-800 font-semibold whitespace-nowrap">
                          {member.member_name || "-"}
                        </td>
                        <td className="px-4 py-4 text-sm text-slate-700 break-all">
                          {member.email || "-"}
                        </td>
                        <td className="px-4 py-4 text-sm text-slate-700 whitespace-nowrap">
                          <span className="inline-flex rounded-full bg-[#1f5274]/10 text-[#1f5274] px-3 py-1 text-xs font-semibold">
                            {member.role || "-"}
                          </span>
                        </td>
                        <td className="px-4 py-4 text-sm text-slate-700">
                          <div className="flex flex-wrap gap-2">
                            {(member.countries || []).length > 0 ? (
                              member.countries?.map((country, i) => (
                                <span
                                  key={`${country}-${i}`}
                                  className="inline-flex rounded-full bg-emerald-100 text-emerald-700 px-2.5 py-1 text-xs font-medium"
                                >
                                  {country}
                                </span>
                              ))
                            ) : (
                              <span>-</span>
                            )}
                          </div>
                        </td>
                        <td className="px-4 py-4 text-sm text-slate-700 min-w-[220px]">
                          <div className="flex flex-wrap gap-2">
                            {(member.modules || []).length > 0 ? (
                              member.modules?.map((module, i) => (
                                <span
                                  key={`${module}-${i}`}
                                  className="inline-flex rounded-full bg-slate-100 text-slate-700 px-2.5 py-1 text-xs font-medium"
                                >
                                  {module.replaceAll("_", " ")}
                                </span>
                              ))
                            ) : (
                              <span>-</span>
                            )}
                          </div>
                        </td>
                        <td className="px-4 py-4 text-sm text-slate-700">
                          <div className="flex flex-wrap gap-2">
                            {(member.marketplace_ids || []).length > 0 ? (
                              member.marketplace_ids?.map((id, i) => (
                                <span
                                  key={`${id}-${i}`}
                                  className="inline-flex rounded-full bg-blue-100 text-blue-700 px-2.5 py-1 text-xs font-medium"
                                >
                                  {id}
                                </span>
                              ))
                            ) : (
                              <span>-</span>
                            )}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-6 py-10 text-center">
                <p className="text-slate-600 font-medium">No members found for this admin.</p>
              </div>
            )}
          </div>
        </section>

        {data?.related_country_profiles?.length ? (
          <section className="space-y-3">
            <div className="rounded-2xl bg-white shadow-xl ring-1 ring-slate-200 overflow-hidden">
              <div className="px-6 sm:px-8 py-5 border-b border-slate-200 bg-slate-50">
                <h3 className="text-lg sm:text-xl font-bold text-[#1f5274]">
                  Country Profiles
                </h3>
                <p className="text-sm text-slate-500 mt-1">
                  Region-specific settings for this admin
                </p>
              </div>

              <div className="p-6 sm:p-8">
                <div className="overflow-x-auto rounded-2xl border border-slate-200">
                  <table className="w-full table-auto">
                    <thead className="bg-slate-100">
                      <tr>
                        <th className="px-3 sm:px-4 py-3 text-left text-xs sm:text-sm font-semibold text-slate-600">
                          Country
                        </th>
                        <th className="px-3 sm:px-4 py-3 text-left text-xs sm:text-sm font-semibold text-slate-600">
                          Stock Unit
                        </th>
                        <th className="px-3 sm:px-4 py-3 text-left text-xs sm:text-sm font-semibold text-slate-600">
                          Transit Time
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.related_country_profiles?.map((p) => (
                        <tr key={p.id} className="border-t">
                          <td className="px-3 sm:px-4 py-3 text-xs sm:text-sm text-slate-700 font-medium">
                            {p.country.toUpperCase()}
                          </td>
                          <td className="px-3 sm:px-4 py-3 text-xs sm:text-sm text-slate-700">
                            {p.stock_unit}
                          </td>
                          <td className="px-3 sm:px-4 py-3 text-xs sm:text-sm text-slate-700">
                            {p.transit_time}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </section>
        ) : null}

        {Array.isArray(data?.skuwise_tables) && data.skuwise_tables.length > 0 && (
          <section className="space-y-6">
            <div className="rounded-3xl bg-white shadow-xl ring-1 ring-slate-200 overflow-hidden">
              <div className="px-6 sm:px-8 py-5 border-b border-slate-200 bg-slate-50">
                <h3 className="text-lg sm:text-xl font-bold text-[#1f5274]">
                  Performance & Region Wise Data
                </h3>
                <p className="text-sm text-slate-500 mt-1">
                  Detailed region wise MTD, QTD and YTD tables
                </p>
              </div>

              <div className="p-4 sm:p-6 space-y-6">
                {Object.entries(groupedTables).map(([region, tables]) => (
                  <div key={region} className="space-y-3">
                    <button
                      onClick={() => toggleRegion(region)}
                      className="w-full text-left px-4 py-4 rounded-2xl bg-gradient-to-r from-slate-50 to-slate-100 hover:from-slate-100 hover:to-slate-200 text-[#1f5274] font-semibold border border-slate-200 flex items-center gap-2 transition-all"
                    >
                      <i
                        className={`fa-solid ${
                          openRegions[region] ? "fa-chevron-down" : "fa-chevron-right"
                        } text-slate-500`}
                      />
                      {region} REGION
                    </button>

                    {openRegions[region] && (
                      <div className="space-y-3 pl-0 sm:pl-4">
                        {(["MTDs", "QTDs", "YTDs"] as const).map((type) =>
                          tables[type].length > 0 ? (
                            <div key={type} className="space-y-2">
                              <button
                                onClick={() => toggleSubsection(region, type)}
                                className="w-full text-left px-4 py-3 rounded-xl bg-white hover:bg-slate-50 text-[#5EA68E] font-semibold border border-slate-200 flex items-center gap-2 shadow-sm"
                              >
                                <i
                                  className={`fa-solid ${
                                    openSubsections[`${region}-${type}`]
                                      ? "fa-chevron-down"
                                      : "fa-chevron-right"
                                  } text-slate-500`}
                                />
                                {type}
                              </button>

                              {openSubsections[`${region}-${type}`] &&
                                tables[type].map((table, index) => {
                                  const key = `${region}-${type}-${index}`;
                                  const allColumns =
                                    Array.isArray(table.rows) && table.rows.length > 0
                                      ? Object.keys(table.rows[0])
                                      : [];

                                  const columnOrder = [
                                    ...preferredColumns,
                                    ...allColumns.filter((c) => !preferredColumns.includes(c)),
                                  ];

                                  const summaryData = extractSummaryData(table.rows);

                                  return (
                                    <div key={key} className="space-y-2 pl-0 sm:pl-6">
                                      <button
                                        onClick={() => toggleTable(key)}
                                        className="w-full text-left px-4 py-3 rounded-xl bg-white hover:bg-slate-50 text-slate-700 font-medium border border-slate-200 flex items-center gap-2 shadow-sm"
                                      >
                                        <i
                                          className={`fa-solid ${
                                            openTables[key]
                                              ? "fa-chevron-down"
                                              : "fa-chevron-right"
                                          } text-slate-500`}
                                        />
                                        {String(table.table || "")
                                          .replaceAll("_", " ")
                                          .toUpperCase()}
                                      </button>

                                      {openTables[key] &&
                                        (table.error ? (
                                          <p className="text-amber-600 text-sm px-3">
                                            ⚠️ {table.error}
                                          </p>
                                        ) : (
                                          <div className="overflow-x-auto rounded-2xl bg-white shadow ring-1 ring-slate-200">
                                            <table className="w-full table-auto">
                                              <thead className="bg-slate-50">
                                                <tr>
                                                  {columnOrder.map((col) => (
                                                    <th
                                                      key={col}
                                                      className="px-3 sm:px-4 py-3 text-left text-[11px] sm:text-xs md:text-sm font-semibold text-slate-600 whitespace-nowrap"
                                                    >
                                                      {col.replaceAll("_", " ").toUpperCase()}
                                                    </th>
                                                  ))}
                                                </tr>
                                              </thead>

                                              <tbody>
                                                {table.rows?.map((row, i) => (
                                                  <tr
                                                    key={i}
                                                    className="border-t hover:bg-slate-50/60"
                                                  >
                                                    {columnOrder.map((col) => (
                                                      <td
                                                        key={col}
                                                        className="px-3 sm:px-4 py-3 text-[11px] sm:text-xs md:text-sm text-slate-700"
                                                      >
                                                        {row?.[col]}
                                                      </td>
                                                    ))}
                                                  </tr>
                                                ))}
                                              </tbody>

                                              {renderSummarySection(
                                                summaryData,
                                                columnOrder.length
                                              )}
                                            </table>
                                          </div>
                                        ))}
                                    </div>
                                  );
                                })}
                            </div>
                          ) : null
                        )}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          </section>
        )}
      </div>
    </div>
  );
}