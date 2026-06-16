"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import {
  Users,
  ArrowLeft,
  Building2,
  Package,
  Database,
  CalendarDays,
  BadgePoundSterling,
  Landmark,
  ClipboardList,
} from "lucide-react";

import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { toast } from "sonner";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

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
  children: React.ReactNode;
};

function AdminSectionCard({ children }: AdminSectionCardProps) {
  return (
    <section className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
      {children}
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
    fullSummary.length > 1800 ? `${fullSummary.slice(0, 1800)}...` : fullSummary;


  const businessJourneySections = useMemo(() => {
    const sourceText = isSummaryExpanded ? fullSummary : shortSummary;

    const blocks = sourceText
      .split(/\n\s*\n/)
      .map((block) => block.trim())
      .filter(Boolean);

    const sections: { title: string; paragraphs: string[] }[] = [];
    let currentSection: { title: string; paragraphs: string[] } | null = null;

    blocks.forEach((block) => {
      const isTitle = /^\d+\.\s/.test(block);

      if (isTitle) {
        currentSection = {
          title: block,
          paragraphs: [],
        };
        sections.push(currentSection);
        return;
      }

      if (!currentSection) {
        currentSection = {
          title: "Overview",
          paragraphs: [],
        };
        sections.push(currentSection);
      }

      currentSection.paragraphs.push(block);
    });

    return sections;
  }, [fullSummary, shortSummary, isSummaryExpanded]);

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
      icon: <Building2 size={22} />,
      bg: "bg-amber-50",
      text: "text-amber-700",
      borderTop: "border-t-amber-500",
    },
    {
      key: "companyName",
      title: "Company Name",
      value: data?.company_name || "-",
      icon: <Landmark size={22} />,
      bg: "bg-orange-50",
      text: "text-orange-700",
      borderTop: "border-t-orange-500",
    },
    {
      key: "totalSku",
      title: "Total SKU",
      value: data?.sku_count ?? "-",
      icon: <Package size={22} />,
      bg: "bg-sky-50",
      text: "text-sky-700",
      borderTop: "border-t-sky-500",
    },
    {
      key: "marketplaceId",
      title: "Marketplace ID",
      value: data?.marketplace_id || "-",
      icon: <ClipboardList size={22} />,
      bg: "bg-red-50",
      text: "text-red-700",
      borderTop: "border-t-red-500",
    },
    {
      key: "dataFetch",
      title: "Data Fetch",
      value: dataFetchLabel,
      icon: <Database size={22} />,
      bg: "bg-stone-50",
      text: "text-stone-700",
      borderTop: "border-t-stone-500",
    },
    {
      key: "onboardSince",
      title: "Onboard Since",
      value: onboardSince,
      icon: <CalendarDays size={22} />,
      bg: "bg-cyan-50",
      text: "text-cyan-700",
      borderTop: "border-t-cyan-500",
    },
    {
      key: "profitability",
      title: "CM2 Profit",
      value: profitabilityLabel,
      icon: <BadgePoundSterling size={22} />,
      bg: "bg-lime-50",
      text: "text-lime-700",
      borderTop: "border-t-lime-500",
    },
    {
      key: "savings",
      title: "Savings",
      value: savingsLabel,
      icon: <BadgePoundSterling size={22} />,
      bg: "bg-emerald-50",
      text: "text-emerald-700",
      borderTop: "border-t-emerald-500",
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
    <div className="w-full">
      <div className="space-y-6">
        {/* Page Heading */}
        <div className="flex items-start gap-3">
          <button
            type="button"
            onClick={() => {
              if (window.history.length > 1) {
                router.back();
              } else {
                router.push("/superadmin/CDPAdminConsole");
              }
            }}
            className="mt-1 inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-slate-200 bg-white text-slate-700 shadow-sm transition hover:bg-slate-50"
            aria-label="Go back"
            title="Back"
          >
            <ArrowLeft size={17} />
          </button>

          <div className="flex flex-col leading-tight">
            <PageBreadcrumb
              pageTitle="Admin Profile"
              variant="page"
              align="left"
              textSize="2xl"
            />

            <p className="mt-1 text-xs text-charcoal-500 2xl:text-sm">
              View user details, business journey, members and marketplace settings.
            </p>
          </div>
        </div>

        {/* Info Cards */}
        <section className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
          {infoCards.map((card) => (
            <div
              key={card.key}
              className={`rounded-2xl border border-t-4 border-slate-200 bg-white p-5 shadow-sm transition  hover:shadow-md ${card.borderTop}`}
            >
              <div className="flex items-center gap-4">
                <div
                  className={`flex h-12 w-12 shrink-0 items-center justify-center rounded-xl ${card.bg} ${card.text}`}
                >
                  {card.icon}
                </div>

                <div className="min-w-0">
                  <p className="text-sm font-medium text-slate-500">
                    {card.title}
                  </p>

                  <h3 className="mt-1 truncate text-xl font-bold tracking-tight text-charcoal-500">
                    {card.value}
                  </h3>
                </div>
              </div>
            </div>
          ))}
        </section>

        {/* Error */}
        {error && (
          <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm font-medium text-red-700">
            {error}
          </div>
        )}

        {/* Business Journey */}
        <section className="space-y-3">
          {/* <div>
            <h2 className="text-xl font-bold text-slate-900">
              Business Journey
            </h2>
           
          </div> */}

          <PageBreadcrumb pageTitle="Business Journey" variant="page" align="left" textSize="2xl" />

          <AdminSectionCard>
            <div
              className={`relative overflow-hidden ${isSummaryExpanded ? "max-h-none" : "max-h-[520px]"
                }`}
            >
              <div className="space-y-4 p-5">
                {businessJourneySections.map((section, sectionIndex) => (
                  <div
                    key={`${section.title}-${sectionIndex}`}
                    className="rounded-2xl border border-slate-100 bg-slate-50/60 p-5"
                  >
                    <div className="flex items-start gap-3">
                      <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-emerald-50 text-sm font-bold text-emerald-700">
                        {sectionIndex + 1}
                      </div>

                      <div className="min-w-0 flex-1">
                        <h3 className="text-sm font-bold text-slate-900">
                          {section.title.replace(/^\d+\.\s*/, "")}
                        </h3>

                        <div className="mt-4 space-y-4">
                          {section.paragraphs.map((paragraph, paragraphIndex) => (
                            <p
                              key={`${section.title}-${paragraphIndex}`}
                              className="text-sm leading-7 text-slate-600"
                            >
                              {paragraph}
                            </p>
                          ))}
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>

              {!isSummaryExpanded && fullSummary.length > 1800 && (
                <div className="pointer-events-none absolute inset-x-0 bottom-0 h-28 bg-gradient-to-t from-white via-white/95 to-transparent" />
              )}
            </div>

            {fullSummary.length > 1800 && (
              <div className="flex items-center justify-between border-t border-slate-100 px-5 py-4">
                <p className="text-xs text-slate-500">
                  {isSummaryExpanded ? "Showing full journey" : "Showing preview"}
                </p>

                <button
                  type="button"
                  onClick={() => setIsSummaryExpanded((prev) => !prev)}
                  className="inline-flex h-10 items-center rounded-xl border border-slate-200 bg-white px-4 text-sm font-semibold text-slate-700 shadow-sm transition hover:bg-slate-50"
                >
                  {isSummaryExpanded ? "Show less" : "Read full journey"}
                </button>
              </div>
            )}
          </AdminSectionCard>
        </section>

        {/* Stock, Transit & Targets */}
        {data?.related_country_profiles?.length ? (
          <section className="space-y-3">
            {/* <div>
              <h2 className="text-xl font-bold text-slate-900">
                Stock, Transit & Targets
              </h2>
            </div> */}
            <PageBreadcrumb pageTitle="Stock, Transit & Targets" variant="page" align="left" textSize="2xl" />

            <AdminSectionCard>
              <div className="max-w-full overflow-x-auto">
                <div className="min-w-[700px]">
                  <Table>
                    <TableHeader className="border-b border-gray-100 bg-slate-50/60 dark:border-white/[0.05]">
                      <TableRow>
                        {["Country", "Stock Unit", "Transit Time", "Target"].map(
                          (column) => (
                            <TableCell
                              key={column}
                              isHeader
                              className="px-5 py-3 text-center text-theme-xs font-medium text-gray-500 dark:text-gray-400"
                            >
                              {column}
                            </TableCell>
                          )
                        )}
                      </TableRow>
                    </TableHeader>

                    <TableBody className="divide-y divide-gray-100 dark:divide-white/[0.05]">
                      {data.related_country_profiles.map((p, index) => (
                        <TableRow
                          key={`${p.id ?? "country-profile"}-${p.country ?? "unknown"}-${index}`}
                        >
                          <TableCell className="px-5 py-4 text-center text-theme-sm font-medium text-gray-800 dark:text-white/90">
                            <span className="inline-flex rounded-full bg-emerald-50 px-3 py-1 text-xs font-semibold text-emerald-700">
                              {p.country?.toUpperCase() || "-"}
                            </span>
                          </TableCell>

                          <TableCell className="px-5 py-4 text-center text-theme-sm text-gray-500 dark:text-gray-400">
                            {p.stock_unit ?? "-"}
                          </TableCell>

                          <TableCell className="px-5 py-4 text-center text-theme-sm text-gray-500 dark:text-gray-400">
                            {p.transit_time ?? "-"}
                          </TableCell>

                          <TableCell className="px-5 py-4 text-center text-theme-sm text-gray-500 dark:text-gray-400">
                            {p.target_sales ?? data?.target_sales ?? "-"}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </div>
            </AdminSectionCard>
          </section>
        ) : null}

        {/* Members */}
        <section className="space-y-3">
          <PageBreadcrumb pageTitle="Members Information" variant="page" align="left" textSize="2xl" />

          <AdminSectionCard>
            {membersLoading ? (
              <div className="flex items-center justify-center py-14">
                <div className="h-8 w-8 animate-spin rounded-full border-2 border-[#1f5274]/30 border-t-[#1f5274]" />
              </div>
            ) : members.length > 0 ? (
              <div className="max-w-full overflow-x-auto">
                <div className="min-w-[1050px]">
                  <Table>
                    <TableHeader className="border-b border-gray-100 bg-slate-50/60 dark:border-white/[0.05]">
                      <TableRow>
                        {[
                          "Member Name",
                          "Email",
                          "Role",
                          "Countries",
                          "Modules",
                          "Marketplace IDs",
                        ].map((column) => (
                          <TableCell
                            key={column}
                            isHeader
                            className="px-5 py-3 text-center text-theme-xs font-medium text-gray-500 dark:text-gray-400"
                          >
                            {column}
                          </TableCell>
                        ))}
                      </TableRow>
                    </TableHeader>

                    <TableBody className="divide-y divide-gray-100 dark:divide-white/[0.05]">
                      {members.map((member, index) => (
                        <TableRow
                          key={`${member.email || member.member_name || "member"}-${index}`}
                        >
                          <TableCell className="whitespace-nowrap px-5 py-4 text-center text-theme-sm font-medium text-gray-800 dark:text-white/90">
                            {member.member_name || "-"}
                          </TableCell>

                          <TableCell className="break-all px-5 py-4 text-center text-theme-sm text-gray-500 dark:text-gray-400">
                            {member.email || "-"}
                          </TableCell>

                          <TableCell className="whitespace-nowrap px-5 py-4 text-center text-theme-sm text-gray-500 dark:text-gray-400">
                            <span className="inline-flex rounded-full bg-gray-100 px-3 py-1 text-xs font-semibold text-gray-700 dark:bg-white/[0.06] dark:text-gray-300">
                              {member.role || "-"}
                            </span>
                          </TableCell>

                          <TableCell className="px-5 py-4 text-center text-theme-sm text-gray-500 dark:text-gray-400">
                            <div className="flex flex-wrap justify-center gap-2">
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
                          </TableCell>

                          <TableCell className="min-w-[220px] px-5 py-4 text-center text-theme-sm text-gray-500 dark:text-gray-400">
                            <div className="flex flex-wrap justify-center gap-2">
                              {(member.modules || []).length > 0 ? (
                                member.modules?.map((module, i) => (
                                  <span
                                    key={`${module}-${i}`}
                                    className="inline-flex rounded-full bg-gray-100 px-2.5 py-1 text-xs font-medium text-gray-700 dark:bg-white/[0.06] dark:text-gray-300"
                                  >
                                    {module.replaceAll("_", " ")}
                                  </span>
                                ))
                              ) : (
                                <span>-</span>
                              )}
                            </div>
                          </TableCell>

                          <TableCell className="px-5 py-4 text-center text-theme-sm text-gray-500 dark:text-gray-400">
                            <div className="flex flex-wrap justify-center gap-2">
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
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
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
          </AdminSectionCard>
        </section>
      </div>
    </div>
  );
}