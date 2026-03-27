"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import Image from "next/image";
import { Settings, Mail, Building2, BarChart3, Users, UserCircle2 } from "lucide-react";
import { toast } from "sonner";
import SummaryMetricCard from "@/components/dropdowns/SummaryMetricCard";
import SummaryMetricCardLarge from "../SummaryMetricCardLarge";

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

type SkuWiseTable = {
  table?: string;
  rows?: AnyRecord[];
  error?: string;
};

type CountryProfileRow = {
  id: number | string;
  country: string;
  stock_unit: string | number;
  transit_time: string | number;
  target_sales?: string | number | null;
};

type ViewUserData = {
  user_id?: number | string;
  brand_name?: string;
  company_name?: string;
  name?: string;
  email?: string;
  annual_sales_range?: string;
  marketplace_id?: string;
  months_of_data_count?: number;
  created_at?: string;
  ai_business_journey?: string | null;
  target_sales?: string | number | null;
  sku_count?: number;
  country?: string;
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
  const [isSummaryExpanded, setIsSummaryExpanded] = useState(false);

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

  const fullSummary = data?.ai_business_journey?.trim() || "No business journey available yet.";
const shortSummary =
  fullSummary.length > 320 ? `${fullSummary.slice(0, 320)}...` : fullSummary;


  const onboardSince = useMemo(() => {
  if (!data?.created_at) return "-";
  const parsed = new Date(data.created_at);
  if (Number.isNaN(parsed.getTime())) return "-";
  return parsed.toLocaleString("en-US", {
    month: "short",
    year: "numeric",
  });
}, [data?.created_at]);

const dataFetchLabel = useMemo(() => {
  const count = Number(data?.months_of_data_count || 0);
  if (!count) return "0 months";
  return `${count} month${count > 1 ? "s" : ""}`;
}, [data?.months_of_data_count]);

const profitabilityLabel = useMemo(() => {
  const rows = data?.skuwise_tables?.flatMap((t) => t.rows || []) || [];
  const lastRow = rows[rows.length - 1];
  const val = Number(lastRow?.cm2_margins);
  if (Number.isNaN(val)) return "Not available";
  return `${val.toFixed(2)}%`;
}, [data?.skuwise_tables]);

const savingsLabel = useMemo(() => {
  const rows = data?.skuwise_tables?.flatMap((t) => t.rows || []) || [];
  const lastRow = rows[rows.length - 1];
  const val = Number(lastRow?.rembursement_fee);
  if (Number.isNaN(val)) return "Not available";
  return val.toLocaleString("en-US", {
    style: "currency",
    currency: "GBP",
    maximumFractionDigits: 0,
  });
}, [data?.skuwise_tables]);

const businessJourneyText = useMemo(() => {
  if (data?.ai_business_journey?.trim()) return data.ai_business_journey;
  return "No business journey available yet.";
}, [data?.ai_business_journey]);

const infoCards = [
  {
    key: "brandName",
    title: "Brand Name",
    value: data?.brand_name || "-",
    className: "bg-white border border-[#FDD36F] border-t-4 border-t-[#FDD36F]",
  },
  {
    key: "companyName",
    title: "Company Name",
    value: data?.company_name || "-",
    className: "bg-white border border-[#ED9F50] border-t-4 border-t-[#ED9F50]",
  },
  {
    key: "totalSku",
    title: "Total SKU",
    value: data?.sku_count ?? "-",
    className: "bg-white border border-[#75BBDA] border-t-4 border-t-[#75BBDA]",
  },
  {
    key: "marketplaceId",
    title: "Marketplace ID",
    value: data?.marketplace_id || "-",
    className: "bg-white border border-[#B75A5A] border-t-4 border-t-[#B75A5A]",
  },
  {
    key: "dataFetch",
    title: "Data Fetch",
    value: dataFetchLabel,
    className: "bg-white border border-[#C49466] border-t-4 border-t-[#C49466]",
  },
  {
    key: "onboardSince",
    title: "Onboard Since",
    value: onboardSince,
    className: "bg-white border border-[#3A8EA4] border-t-4 border-t-[#3A8EA4]",
  },
  {
    key: "profitability",
    title: "Profitability",
    value: profitabilityLabel,
    className: "bg-white border border-[#B8C78C] border-t-4 border-t-[#B8C78C]",
  },
  {
    key: "savings",
    title: "Savings",
    value: savingsLabel,
    className: "bg-white border border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]",
  },
];

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
        <div className="flex items-center my-2">
  <button
    onClick={() => {
      if (window.history.length > 1) {
        router.back();
      } else {
        router.push("/superadmin/CDPAdminConsole");
      }
    }}
    className="inline-flex items-center gap-2 text-sm font-medium text-[#1f5274] hover:text-[#5EA68E] transition"
  >
    ← Back to Admins
  </button>
</div>
        {/* Admin profile card */}
      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
  {infoCards.map((card) => (
    <SummaryMetricCardLarge
      key={card.key}
      title={card.title}
      value={card.value}
      className={card.className}
    />
  ))}
</div>

<section className="rounded-3xl bg-white shadow-xl ring-1 ring-slate-200 overflow-hidden">
  <div className="px-6 sm:px-8 py-5 border-b border-slate-200 bg-slate-50">
    <h2 className="text-lg sm:text-xl font-bold text-[#1f5274]">
      Business Journey
    </h2>
    <p className="text-sm text-slate-500 mt-1">
      AI-generated summary of the admin's business journey
    </p>
  </div>

  <div className="p-6 sm:p-8">
    <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
      <p className="text-sm leading-7 text-slate-600 whitespace-pre-line">
        {isSummaryExpanded ? fullSummary : shortSummary}
      </p>

      {fullSummary.length > 320 && (
        <button
          type="button"
          onClick={() => setIsSummaryExpanded((prev) => !prev)}
          className="mt-4 inline-flex items-center rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-[#1f5274] hover:bg-slate-50"
        >
          {isSummaryExpanded ? "Show less" : "Show more"}
        </button>
      )}
    </div>
  </div>
</section>

        {data?.related_country_profiles?.length ? (
          <section className="space-y-3">
            <div className="rounded-2xl bg-white shadow-xl ring-1 ring-slate-200 overflow-hidden">
              <div className="px-6 sm:px-8 py-5 border-b border-slate-200 bg-slate-50">
               <h3 className="text-lg sm:text-xl font-bold text-[#1f5274]">
  Stock, Transit & Targets
</h3>
<p className="text-sm text-slate-500 mt-1">
  Operational settings for this admin
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
    <th className="px-3 sm:px-4 py-3 text-left text-xs sm:text-sm font-semibold text-slate-600">
      Target
    </th>
  </tr>
</thead>
<tbody>
  {data.related_country_profiles?.map((p, index) => (
    <tr
      key={`${p.id ?? "country-profile"}-${p.country ?? "unknown"}-${index}`}
      className="border-t"
    >
      <td className="px-3 sm:px-4 py-3 text-xs sm:text-sm text-slate-700 font-medium">
        {p.country?.toUpperCase() || "-"}
      </td>
      <td className="px-3 sm:px-4 py-3 text-xs sm:text-sm text-slate-700">
        {p.stock_unit ?? "-"}
      </td>
      <td className="px-3 sm:px-4 py-3 text-xs sm:text-sm text-slate-700">
        {p.transit_time ?? "-"}
      </td>
      <td className="px-3 sm:px-4 py-3 text-xs sm:text-sm text-slate-700">
        {p.target_sales ?? data?.target_sales ?? "-"}
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


      </div>
    </div>
  );
}