"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import Image from "next/image";
import {
  Settings,
  Users,
  ArrowLeft,
  Building2,
  Package,
  Database,
  CalendarDays,
  BadgePoundSterling,
  Landmark,
  ClipboardList,
  Sparkles,
} from "lucide-react";
import { toast } from "sonner";
// import SummaryMetricCard from "@/components/dropdowns/SummaryMetricCard";
// import SummaryMetricCardLarge from "../SummaryMetricCardLarge";

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
  profitability?: number | null;
  profitability_month?: string | null;
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

type AdminSectionCardProps = {
  title: string;
  description?: string;
  icon?: React.ReactNode;
  children: React.ReactNode;
};

function AdminSectionCard({
  title,
  description,
  icon,
  children,
}: AdminSectionCardProps) {
  return (
    <section className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
      <div className="flex items-center gap-3 border-b border-slate-200 bg-white px-5 py-4">
        {icon && (
          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-[#5EA68E]/10 text-[#5EA68E]">
            {icon}
          </div>
        )}

        <div>
          <h2 className="text-lg font-bold text-slate-900">{title}</h2>

          {description && (
            <p className="mt-1 text-sm text-slate-500">{description}</p>
          )}
        </div>
      </div>

      <div>{children}</div>
    </section>
  );
}

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
    const val = Number(data?.profitability);
    if (data?.profitability == null || Number.isNaN(val)) return "Not available";
    return Math.round(val).toLocaleString("en-US");
  }, [data?.profitability]);

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
      icon: <Building2 size={20} />,
      accent: "from-amber-400 to-orange-500",
      bg: "bg-amber-50",
      text: "text-amber-700",
    },
    {
      key: "companyName",
      title: "Company Name",
      value: data?.company_name || "-",
      icon: <Landmark size={20} />,
      accent: "from-orange-400 to-rose-500",
      bg: "bg-orange-50",
      text: "text-orange-700",
    },
    {
      key: "totalSku",
      title: "Total SKU",
      value: data?.sku_count ?? "-",
      icon: <Package size={20} />,
      accent: "from-sky-400 to-blue-500",
      bg: "bg-sky-50",
      text: "text-sky-700",
    },
    {
      key: "marketplaceId",
      title: "Marketplace ID",
      value: data?.marketplace_id || "-",
      icon: <ClipboardList size={20} />,
      accent: "from-red-400 to-rose-500",
      bg: "bg-red-50",
      text: "text-red-700",
    },
    {
      key: "dataFetch",
      title: "Data Fetch",
      value: dataFetchLabel,
      icon: <Database size={20} />,
      accent: "from-stone-400 to-amber-600",
      bg: "bg-stone-50",
      text: "text-stone-700",
    },
    {
      key: "onboardSince",
      title: "Onboard Since",
      value: onboardSince,
      icon: <CalendarDays size={20} />,
      accent: "from-cyan-400 to-teal-500",
      bg: "bg-cyan-50",
      text: "text-cyan-700",
    },
    {
      key: "profitability",
      title: "CM2 Profit",
      value: profitabilityLabel,
      icon: <BadgePoundSterling size={20} />,
      accent: "from-lime-400 to-emerald-500",
      bg: "bg-lime-50",
      text: "text-lime-700",
    },
    {
      key: "savings",
      title: "Savings",
      value: savingsLabel,
      icon: <BadgePoundSterling size={20} />,
      accent: "from-emerald-400 to-green-600",
      bg: "bg-emerald-50",
      text: "text-emerald-700",
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
      <div className="space-y-6">
        {/* Hero */}
        <section className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
          <div className="relative bg-gradient-to-r from-[#37455F] via-[#40516E] to-[#5EA68E] px-6 py-6 text-white">
            <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(255,255,255,0.22),transparent_35%)]" />

            <div className="relative flex flex-col gap-5 lg:flex-row lg:items-center lg:justify-between">
              <div className="flex min-w-0 items-center gap-4">
                <Image
                  width={210}
                  height={44}
                  src="/images/auth/Phormula.png"
                  alt="Phormula"
                  priority
                  className="h-auto w-[170px] sm:w-[210px]"
                />

                <div className="hidden h-10 w-px bg-white/20 sm:block" />

                <div className="min-w-0">
                  <p className="text-xs font-semibold uppercase tracking-[0.22em] text-white/70">
                    Admin Profile
                  </p>

                  <h1 className="mt-1 truncate text-2xl font-bold tracking-tight">
                    {data?.brand_name || "Admin Details"}
                  </h1>

                  <p className="mt-1 truncate text-sm text-white/75">
                    {data?.email || email}
                  </p>
                </div>
              </div>

              <div className="flex items-center gap-3">
                <button
                  type="button"
                  onClick={() => {
                    if (window.history.length > 1) {
                      router.back();
                    } else {
                      router.push("/superadmin/CDPAdminConsole");
                    }
                  }}
                  className="inline-flex h-11 items-center gap-2 rounded-xl bg-white/10 px-4 text-sm font-semibold text-white shadow-sm transition hover:bg-white/20"
                >
                  <ArrowLeft size={17} />
                  Back to Admins
                </button>

                <div className="relative">
                  <button
                    type="button"
                    onClick={() => setShowSettings((s) => !s)}
                    className="inline-flex h-11 w-11 items-center justify-center rounded-xl bg-white/10 text-white shadow-sm transition hover:bg-white/20"
                    aria-label="Open settings"
                  >
                    <Settings size={19} />
                  </button>

                  {showSettings && (
                    <div className="absolute right-0 z-50 mt-2 w-56 overflow-hidden rounded-xl border border-slate-200 bg-white text-slate-800 shadow-2xl">
                      <button
                        onClick={() =>
                          router.push("/superadmin/Superadminchangepassword")
                        }
                        className="w-full px-4 py-3 text-left text-sm font-medium hover:bg-slate-50"
                      >
                        Change Password
                      </button>

                      <button
                        onClick={() => router.push("/superadmin/DeleteAdmin")}
                        className="w-full border-t border-slate-100 px-4 py-3 text-left text-sm font-medium hover:bg-slate-50"
                      >
                        Delete Admin
                      </button>

                      <button
                        onClick={handleLogout}
                        className="w-full border-t border-slate-100 px-4 py-3 text-left text-sm font-medium text-red-600 hover:bg-red-50"
                      >
                        Logout
                      </button>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>

          {/* KPI Cards */}
          <div className="grid grid-cols-1 gap-4 p-5 sm:grid-cols-2 xl:grid-cols-4">
            {infoCards.map((card) => (
              <div
                key={card.key}
                className="relative overflow-hidden rounded-2xl border border-slate-200 bg-white p-5 shadow-sm transition hover:-translate-y-0.5 hover:shadow-md"
              >
                <div
                  className={`absolute left-0 top-0 h-1 w-full bg-gradient-to-r ${card.accent}`}
                />

                <div className="flex items-start justify-between gap-4">
                  <div className="min-w-0">
                    <p className="text-sm font-medium text-slate-500">
                      {card.title}
                    </p>

                    <h3 className="mt-2 truncate text-2xl font-bold text-slate-950">
                      {card.value}
                    </h3>
                  </div>

                  <div
                    className={`flex h-11 w-11 shrink-0 items-center justify-center rounded-xl ${card.bg} ${card.text}`}
                  >
                    {card.icon}
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

        {/* Business Journey */}
        <AdminSectionCard
          title="Business Journey"
          // description="AI-generated summary of this admin's business journey"
          icon={<Sparkles size={20} />}
        >
          <div className="p-5">
            <div className="rounded-2xl border border-slate-100 bg-slate-50/60 p-5">
              <p className="text-sm leading-6 text-slate-600 whitespace-pre-line">
                {isSummaryExpanded ? fullSummary : shortSummary}
              </p>

              {fullSummary.length > 320 && (
                <button
                  type="button"
                  onClick={() => setIsSummaryExpanded((prev) => !prev)}
                  className="mt-4 inline-flex h-10 items-center rounded-xl border border-slate-200 bg-white px-4 text-sm font-semibold text-[#37455F] shadow-sm transition hover:bg-slate-50"
                >
                  {isSummaryExpanded ? "Show less" : "Show more"}
                </button>
              )}
            </div>
          </div>
        </AdminSectionCard>

        {/* Stock, Transit & Targets */}
        {data?.related_country_profiles?.length ? (
          <AdminSectionCard
            title="Stock, Transit & Targets"
            // description="Operational settings configured for this admin"
            icon={<ClipboardList size={20} />}
          >
            <div className="overflow-x-auto">
              <table className="w-full min-w-[700px] table-auto">
                <thead className="bg-white">
                  <tr className="border-b border-slate-200">
                    <th className="px-5 py-4 text-left text-xs font-bold uppercase tracking-wide text-slate-500">
                      Country
                    </th>
                    <th className="px-5 py-4 text-left text-xs font-bold uppercase tracking-wide text-slate-500">
                      Stock Unit
                    </th>
                    <th className="px-5 py-4 text-left text-xs font-bold uppercase tracking-wide text-slate-500">
                      Transit Time
                    </th>
                    <th className="px-5 py-4 text-left text-xs font-bold uppercase tracking-wide text-slate-500">
                      Target
                    </th>
                  </tr>
                </thead>

                <tbody>
                  {data.related_country_profiles?.map((p, index) => (
                    <tr
                      key={`${p.id ?? "country-profile"}-${p.country ?? "unknown"}-${index}`}
                      className="border-b border-slate-100 transition hover:bg-slate-50/80"
                    >
                      <td className="px-5 py-4 text-sm font-semibold text-slate-900">
                        <span className="inline-flex rounded-full bg-[#5EA68E]/10 px-3 py-1 text-xs font-bold text-[#5EA68E]">
                          {p.country?.toUpperCase() || "-"}
                        </span>
                      </td>

                      <td className="px-5 py-4 text-sm text-slate-600">
                        {p.stock_unit ?? "-"}
                      </td>

                      <td className="px-5 py-4 text-sm text-slate-600">
                        {p.transit_time ?? "-"}
                      </td>

                      <td className="px-5 py-4 text-sm text-slate-600">
                        {p.target_sales ?? data?.target_sales ?? "-"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </AdminSectionCard>
        ) : null}

        {/* Members */}
        <AdminSectionCard
          title="Members Information"
          // description={`${members.length} member${members.length !== 1 ? "s" : ""} added by this admin`}
          icon={<Users size={20} />}
        >
          <div>
            {membersLoading ? (
              <div className="flex items-center justify-center py-14">
                <div className="h-8 w-8 animate-spin rounded-full border-2 border-[#1f5274]/30 border-t-[#1f5274]" />
              </div>
            ) : members.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full min-w-[1050px] table-auto">
                  <thead className="bg-white">
                    <tr className="border-b border-slate-200">
                      <th className="px-5 py-4 text-left text-xs font-bold uppercase tracking-wide text-slate-500">
                        Member Name
                      </th>
                      <th className="px-5 py-4 text-left text-xs font-bold uppercase tracking-wide text-slate-500">
                        Email
                      </th>
                      <th className="px-5 py-4 text-left text-xs font-bold uppercase tracking-wide text-slate-500">
                        Role
                      </th>
                      <th className="px-5 py-4 text-left text-xs font-bold uppercase tracking-wide text-slate-500">
                        Countries
                      </th>
                      <th className="px-5 py-4 text-left text-xs font-bold uppercase tracking-wide text-slate-500">
                        Modules
                      </th>
                      <th className="px-5 py-4 text-left text-xs font-bold uppercase tracking-wide text-slate-500">
                        Marketplace IDs
                      </th>
                    </tr>
                  </thead>

                  <tbody>
                    {members.map((member, index) => (
                      <tr
                        key={`${member.email || member.member_name || "member"}-${index}`}
                        className="border-b border-slate-100 transition hover:bg-slate-50/80"
                      >
                        <td className="px-5 py-4 text-sm font-semibold text-slate-900 whitespace-nowrap">
                          {member.member_name || "-"}
                        </td>

                        <td className="px-5 py-4 text-sm text-slate-600 break-all">
                          {member.email || "-"}
                        </td>

                        <td className="px-5 py-4 text-sm text-slate-600 whitespace-nowrap">
                          <span className="inline-flex rounded-full bg-[#37455F]/10 px-3 py-1 text-xs font-bold text-[#37455F]">
                            {member.role || "-"}
                          </span>
                        </td>

                        <td className="px-5 py-4 text-sm text-slate-600">
                          <div className="flex flex-wrap gap-2">
                            {(member.countries || []).length > 0 ? (
                              member.countries?.map((country, i) => (
                                <span
                                  key={`${country}-${i}`}
                                  className="inline-flex rounded-full bg-emerald-50 px-2.5 py-1 text-xs font-semibold text-emerald-700"
                                >
                                  {country}
                                </span>
                              ))
                            ) : (
                              <span>-</span>
                            )}
                          </div>
                        </td>

                        <td className="min-w-[220px] px-5 py-4 text-sm text-slate-600">
                          <div className="flex flex-wrap gap-2">
                            {(member.modules || []).length > 0 ? (
                              member.modules?.map((module, i) => (
                                <span
                                  key={`${module}-${i}`}
                                  className="inline-flex rounded-full bg-slate-100 px-2.5 py-1 text-xs font-medium text-slate-700"
                                >
                                  {module.replaceAll("_", " ")}
                                </span>
                              ))
                            ) : (
                              <span>-</span>
                            )}
                          </div>
                        </td>

                        <td className="px-5 py-4 text-sm text-slate-600">
                          <div className="flex flex-wrap gap-2">
                            {(member.marketplace_ids || []).length > 0 ? (
                              member.marketplace_ids?.map((id, i) => (
                                <span
                                  key={`${id}-${i}`}
                                  className="inline-flex rounded-full bg-sky-50 px-2.5 py-1 text-xs font-semibold text-sky-700"
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
              <div className="p-5">
                <div className="rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-6 py-12 text-center">
                  <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-full bg-slate-100 text-slate-400">
                    <Users size={22} />
                  </div>

                  <p className="mt-4 font-semibold text-slate-700">
                    No members found for this admin.
                  </p>

                  <p className="mt-1 text-sm text-slate-500">
                    Members added by this admin will appear here.
                  </p>
                </div>
              </div>
            )}
          </div>
        </AdminSectionCard>
      </div>
    </div>
  );
}