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
import SuperAdminUsersTable, {
  SuperAdminTableColumn,
} from "@/components/admin/table/SuperAdminUsersTable";
import { toast } from "sonner";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import Loader from "@/components/loader/Loader";
import { getMarketplaceDisplays } from "@/lib/utils/amazonMarketplaces";

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
  user_id?: number | string;
  country: string;
  marketplace?: string;
  ship_time_weeks: string | number;
  air_time_weeks: string | number;
  stock_unit_weeks: string | number;
  ship_alert_threshold_weeks: string | number;
  air_alert_threshold_weeks: string | number;
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
  marketplace_ids?: string[];
  countries?: string[];
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
    <section className="overflow-hidden rounded-xl border border-white/10 bg-[#484962] text-white shadow-[0_16px_38px_rgba(20,22,45,0.22)]">
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
              headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
              },
            }
          );

          const json = await res.json().catch(() => ({}));

          if (!res.ok) {
            const message = String(json?.message || "").toLowerCase();

            if (
              res.status === 401 ||
              message.includes("token") ||
              message.includes("expired") ||
              message.includes("unauthorized")
            ) {
              localStorage.removeItem("superadmin_token");
              toast.error("Session expired. Please login again.");
              router.replace("/superadmin/CDPAdminConsole");
              return;
            }

            throw new Error(json?.message || "Superadmin fetch failed");
          }

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

          const membersJson = await membersRes.json().catch(() => ({}));

          if (!membersRes.ok) {
            const message = String(membersJson?.message || "").toLowerCase();

            if (
              membersRes.status === 401 ||
              message.includes("token") ||
              message.includes("expired") ||
              message.includes("unauthorized")
            ) {
              localStorage.removeItem("superadmin_token");
              toast.error("Session expired. Please login again.");
              router.replace("/superadmin/CDPAdminConsole");
              return;
            }
            setMembers([]);
          } else {
            setMembers(Array.isArray(membersJson?.data) ? membersJson.data : []);
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
  }, [email, router]);

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

  const marketplaceIdsLabel = useMemo(() => {
    const marketplaceIds = Array.isArray(data?.marketplace_ids)
      ? data.marketplace_ids
      : [];

    const values = marketplaceIds.length
      ? marketplaceIds
      : data?.marketplace_id
        ? [data.marketplace_id]
        : [];

    return values
      .map((id) => String(id).trim())
      .filter(Boolean)
      .join(", ") || "-";
  }, [data?.marketplace_id, data?.marketplace_ids]);
  const connectedCountries = useMemo(() => {
    const source = Array.isArray(data?.countries) && data.countries.length
      ? data.countries
      : data?.country
        ? [data.country]
        : [];

    return Array.from(
      new Set(
        source
          .flatMap((value) => String(value || "").split(","))
          .map((value) => value.trim().toUpperCase())
          .filter(Boolean)
      )
    );
  }, [data?.countries, data?.country]);

  const countriesCardValue =
    connectedCountries.length > 0 ? (
      <div className="mt-1.5 flex flex-wrap gap-1.5">
        {connectedCountries.map((country) => (
          <span
            key={country}
            className="inline-flex rounded-lg border border-[#31d9e5]/25 bg-[#31d9e5]/10 px-2.5 py-1.5"
          >
            <span className="text-[11px] font-semibold leading-4 text-white 2xl:text-xs">
              {country}
            </span>
          </span>
        ))}
      </div>
    ) : (
      "-"
    );

  const infoCards = [
    {
      key: "brandName",
      title: "Brand Name",
      value: data?.brand_name || "-",
      icon: <Building2 size={22} />,
    },
    {
      key: "companyName",
      title: "Company Name",
      value: data?.company_name || "-",
      icon: <Landmark size={22} />,
    },
    {
      key: "totalSku",
      title: "Total SKU",
      value: data?.sku_count ?? "-",
      icon: <Package size={22} />,
    },
    {
      key: "countries",
      title: "Countries",
      value: countriesCardValue,
      icon: <ClipboardList size={22} />,
    },
    {
      key: "dataFetch",
      title: "Data Fetch",
      value: dataFetchLabel,
      icon: <Database size={22} />,
    },
    {
      key: "onboardSince",
      title: "Onboard Since",
      value: onboardSince,
      icon: <CalendarDays size={22} />,
    },
    {
      key: "profitability",
      title: "CM2 Profit",
      value: profitabilityLabel,
      icon: <BadgePoundSterling size={22} />,
    },
    {
      key: "savings",
      title: "Savings",
      value: savingsLabel,
      icon: <BadgePoundSterling size={22} />,
    },
  ];

  if (loading) {
    return <Loader fullscreen backgroundClass="bg-[#37384f]" />;
  }

  if (error) {
    return (
      <div className="min-h-screen bg-[#37384f] p-4 text-white sm:p-6">
        <div className="mx-auto max-w-full">
          <div className="rounded-xl border border-red-300/25 bg-red-500/10 px-4 py-3 text-xs font-medium text-red-100 2xl:text-sm">
            {error}
          </div>
        </div>
      </div>
    );
  }

  const countryProfileColumns: SuperAdminTableColumn<CountryProfileRow>[] = [
    {
      key: "serial_no",
      label: "S.No.",
      render: (row: CountryProfileRow) => {
        const index = data?.related_country_profiles?.findIndex(
          (item) => item.id === row.id
        );

        return (
          <span className="font-medium text-white/65">
            {index !== undefined && index >= 0 ? index + 1 : "-"}
          </span>
        );
      },
    },
    {
      key: "country",
      label: "Country",
      render: (p) => (
        <span className="inline-flex rounded-full border border-[#31d9e5]/25 bg-[#31d9e5]/10 px-2.5 py-1 text-[11px] font-semibold 2xl:text-xs text-[#31d9e5]">
          {p.country?.toUpperCase() || "-"}
        </span>
      ),
    },
    {
      key: "marketplace",
      label: "Marketplace",
      render: (p) => p.marketplace || "-",
    },
    {
      key: "ship_time_weeks",
      label: "Ship Time (Weeks)",
      render: (p) => p.ship_time_weeks ?? "-",
    },
    {
      key: "air_time_weeks",
      label: "Air Time (Weeks)",
      render: (p) => p.air_time_weeks ?? "-",
    },
    {
      key: "stock_unit_weeks",
      label: "Stock Unit (Weeks)",
      render: (p) => p.stock_unit_weeks ?? "-",
    },
    // {
    //   key: "ship_alert_threshold_weeks",
    //   label: "Ship Alert Threshold",
    //   render: (p) => p.ship_alert_threshold_weeks ?? "-",
    // },
    // {
    //   key: "air_alert_threshold_weeks",
    //   label: "Air Alert Threshold",
    //   render: (p) => p.air_alert_threshold_weeks ?? "-",
    // },
    {
      key: "target",
      label: "Target",
      render: (p) => p.target_sales ?? data?.target_sales ?? "-",
    },
  ];

  const memberColumns: SuperAdminTableColumn<MemberRow>[] = [
    {
      key: "member_name",
      label: "Member Name",
      cellClassName: "whitespace-nowrap",
      render: (member) => (
        <span className="font-medium text-white">
          {member.member_name || "-"}
        </span>
      ),
    },
    {
      key: "email",
      label: "Email",
      cellClassName: "break-all",
      render: (member) => member.email || "-",
    },
    {
      key: "role",
      label: "Role",
      render: (member) => (
        <span className="inline-flex rounded-full border border-white/10 bg-white/[0.06] px-2.5 py-1 text-[11px] font-semibold 2xl:text-xs text-white/75">
          {member.role || "-"}
        </span>
      ),
    },
    {
      key: "countries",
      label: "Countries",
      render: (member) => (
        <div className="flex flex-wrap justify-center gap-2">
          {(member.countries || []).length > 0 ? (
            member.countries?.map((country, i) => (
              <span
                key={`${country}-${i}`}
                className="inline-flex rounded-full border border-[#31d9e5]/25 bg-[#31d9e5]/10 px-2.5 py-1 text-[11px] font-semibold 2xl:text-xs text-[#31d9e5]"
              >
                {country}
              </span>
            ))
          ) : (
            <span>-</span>
          )}
        </div>
      ),
    },
    {
      key: "modules",
      label: "Modules",
      cellClassName: "min-w-[220px]",
      render: (member) => (
        <div className="flex flex-wrap justify-center gap-2">
          {(member.modules || []).length > 0 ? (
            member.modules?.map((module, i) => (
              <span
                key={`${module}-${i}`}
                className="inline-flex rounded-full border border-white/10 bg-white/[0.06] px-2.5 py-1 text-[11px] font-medium 2xl:text-xs text-white/75"
              >
                {module.replaceAll("_", " ")}
              </span>
            ))
          ) : (
            <span>-</span>
          )}
        </div>
      ),
    },
    {
      key: "marketplace_ids",
      label: "Marketplace IDs",
      render: (member) => (
        <div className="flex flex-wrap justify-center gap-2">
          {(member.marketplace_ids || []).length > 0 ? (
            member.marketplace_ids?.map((id, i) => (
              <span
                key={`${id}-${i}`}
                className="inline-flex rounded-full border border-[#31d9e5]/25 bg-[#31d9e5]/10 px-2.5 py-1 text-[11px] font-semibold 2xl:text-xs text-[#31d9e5]"
              >
                {id}
              </span>
            ))
          ) : (
            <span>-</span>
          )}
        </div>
      ),
    },
  ];

  return (
    <div className="w-full">
      <div className="space-y-5 2xl:space-y-6">
        {/* PAGE HEADER */}
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
            className="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-white/10 bg-white/[0.06] text-white/80 transition hover:border-[#31d9e5]/40 hover:bg-[#31d9e5]/10 hover:text-[#31d9e5]"
            aria-label="Go back"
            title="Back"
          >
            <ArrowLeft size={17} />
          </button>

          <PageBreadcrumb
            pageTitle="Admin Profile"
            variant="superadmin"
            align="left"
            textSize="2xl"
          />
        </div>

        {/* Info Cards */}
        <section className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
          {infoCards.map((card) => (
            <div
              key={card.key}
              className="relative min-h-[88px] overflow-hidden rounded-xl border-t-4 border-[#31D9E5] bg-[#484962] p-3 text-white shadow-[0_14px_32px_rgba(20,22,45,0.20)] 2xl:min-h-[96px] 2xl:p-4"
            >
              <div className="flex items-center gap-4">
                <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-[#31d9e5]/15 text-[#31d9e5] 2xl:h-11 2xl:w-11">
                  {card.icon}
                </div>

                <div className="min-w-0">
                  <p className="text-[11px] font-medium text-white/70 2xl:text-xs">
                    {card.title}
                  </p>

                  {React.isValidElement(card.value) ? (
                    card.value
                  ) : (
                    <h3 className="mt-1 truncate text-base font-bold tracking-tight text-white 2xl:text-lg">
                      {card.value}
                    </h3>
                  )}
                </div>
              </div>
            </div>
          ))}
        </section>

        {/* Error */}
        {error && (
          <div className="rounded-xl border border-red-300/25 bg-red-500/10 px-4 py-3 text-xs font-medium text-red-100 2xl:text-sm">
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

          <PageBreadcrumb
            pageTitle="Business Journey"
            variant="superadmin"
            align="left"
            textSize="xl"
          />

          <AdminSectionCard>
            <div
              className={`relative overflow-hidden ${isSummaryExpanded ? "max-h-none" : "max-h-[520px]"
                }`}
            >
              <div className="space-y-3 p-4 2xl:p-5">
                {businessJourneySections.map((section, sectionIndex) => (
                  <div
                    key={`${section.title}-${sectionIndex}`}
                    className="rounded-xl border border-white/10 bg-white/[0.04] p-4"
                  >
                    <div className="flex items-start gap-3">
                      <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-[#31d9e5]/15 text-xs font-bold text-[#31d9e5]">
                        {sectionIndex + 1}
                      </div>

                      <div className="min-w-0 flex-1">
                        <h3 className="text-xs font-bold text-white 2xl:text-sm">
                          {section.title.replace(/^\d+\.\s*/, "")}
                        </h3>

                        <div className="mt-3 space-y-3">
                          {section.paragraphs.map((paragraph, paragraphIndex) => (
                            <p
                              key={`${section.title}-${paragraphIndex}`}
                              className="text-xs leading-6 text-white/65 2xl:text-sm"
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
                <div className="pointer-events-none absolute inset-x-0 bottom-0 h-28 bg-gradient-to-t from-[#484962] via-[#484962]/95 to-transparent" />
              )}
            </div>

            {fullSummary.length > 1800 && (
              <div className="flex items-center justify-between border-t border-white/10 px-5 py-4">
                <p className="text-[11px] text-white/45 2xl:text-xs">
                  {isSummaryExpanded ? "Showing full journey" : "Showing preview"}
                </p>

                <button
                  type="button"
                  onClick={() => setIsSummaryExpanded((prev) => !prev)}
                  className="inline-flex h-9 items-center rounded-lg border border-[#31d9e5]/25 bg-[#31d9e5]/10 px-3 text-xs font-semibold text-[#31d9e5] transition hover:bg-[#31d9e5]/15 2xl:text-sm"
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
            <PageBreadcrumb
              pageTitle="Stock, Transit & Alert Thresholds"
              variant="superadmin"
              align="left"
              textSize="xl"
            />

            <AdminSectionCard>
              <SuperAdminUsersTable
                columns={countryProfileColumns}
                data={data.related_country_profiles}
                minWidth="1200px"
                emptyTitle="No country profiles found"
                emptyDescription="Ship time, air time, stock units, and alert thresholds will appear here."
              />
            </AdminSectionCard>
          </section>
        ) : null}

        {/* Members */}
        <section className="space-y-3">
          <PageBreadcrumb
            pageTitle="Members Information"
            variant="superadmin"
            align="left"
            textSize="xl"
          />

          <AdminSectionCard>
            {membersLoading ? (
              <div className="flex min-h-[260px] items-center justify-center">
                <Loader backgroundClass="bg-transparent" />
              </div>
            ) : (
              <SuperAdminUsersTable
                columns={memberColumns}
                data={members}
                minWidth="1050px"
                emptyTitle="No members found"
                emptyDescription="Members added by this admin will appear here."
              />
            )}
          </AdminSectionCard>
        </section>
      </div>
    </div>
  );
}
