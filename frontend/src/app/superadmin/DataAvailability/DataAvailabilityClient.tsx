"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import {
  AlertTriangle,
  CheckCircle2,
  CircleDashed,
  Database,
  Eye,
  Search,
  XCircle,
} from "lucide-react";
import { toast } from "sonner";
import SuperAdminUsersTable from "@/components/admin/table/SuperAdminUsersTable";
import SummaryMetricCard from "@/components/dropdowns/SummaryMetricCard";
import Loader from "@/components/loader/Loader";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import { LuCircleCheckBig, LuCircleX, LuLayers3 } from "react-icons/lu";

const MONTHS = [
  { value: "january", label: "January" },
  { value: "february", label: "February" },
  { value: "march", label: "March" },
  { value: "april", label: "April" },
  { value: "may", label: "May" },
  { value: "june", label: "June" },
  { value: "july", label: "July" },
  { value: "august", label: "August" },
  { value: "september", label: "September" },
  { value: "october", label: "October" },
  { value: "november", label: "November" },
  { value: "december", label: "December" },
];

const STATUS_OPTIONS = ["All", "Complete", "Missing"] as const;

type AvailabilityStatus = "Complete" | "Missing" | "Stale" | "Failed";

type AvailabilityRow = {
  user_id: number | string;
  name?: string;
  email: string;
  brand_name?: string;
  company_name?: string;
  country: string;
  country_key: string;
  marketplace_id?: string | null;
  checks: {
    sku_data: boolean;
    pnl_data: boolean;
    ads_connected: boolean;
    inventory_connected: boolean;
    currency_global_data: boolean;
  };
  counts: {
    sku_rows: number;
    pnl_rows: number;
    inventory_rows: number;
    monthwise_inventory_rows: number;
    current_inventory_rows: number;
    currency_rows: number;
    global_rows: number;
  };
  status: AvailabilityStatus;
  missing: string[];
};

type BrandAvailabilityRow = {
  brand_key: string;

  user_id: number | string;
  name?: string;
  email: string;

  brand_name: string;
  company_name?: string;

  countries: string[];
  marketplaces: string[];

  checks: AvailabilityRow["checks"];

  status: AvailabilityStatus;
  missing: string[];

  sourceRows: AvailabilityRow[];
};

type AvailabilityResponse = {
  period?: {
    month: string;
    month_number: number;
    year: number;
  };
  summary?: {
    total: number;
    complete: number;
    missing: number;
    stale: number;
    failed: number;
  };
  rows?: AvailabilityRow[];
  message?: string;
};

type IssueSummary = {
  open: number;
  resolved: number;
  critical: number;
  high: number;
};

type IssueDetectionResponse = {
  success?: boolean;
  created?: number;
  updated?: number;
  resolved?: number;
  open_count?: number;
  message?: string;
};

type IssuesResponse = {
  success?: boolean;
  summary?: IssueSummary;
  message?: string;
};

const currentMonth = () => MONTHS[new Date().getMonth()]?.value || "january";
const currentYear = () => new Date().getFullYear();

const statusStyles: Record<AvailabilityStatus, string> = {
  Complete: "border-emerald-300/25 bg-emerald-500/10 text-emerald-200",
  Missing: "border-amber-300/25 bg-amber-500/10 text-amber-100",
  Stale: "border-sky-300/25 bg-sky-500/10 text-sky-100",
  Failed: "border-red-300/25 bg-red-500/10 text-red-100",
};

const checkLabels: Array<{
  key: keyof AvailabilityRow["checks"];
  label: string;
  countKey?: keyof AvailabilityRow["counts"];
}> = [
    { key: "sku_data", label: "SKU", countKey: "sku_rows" },
    { key: "pnl_data", label: "P&L", countKey: "pnl_rows" },
    { key: "ads_connected", label: "Ads" },
    { key: "inventory_connected", label: "Inventory", countKey: "inventory_rows" },
    {
      key: "currency_global_data",
      label: "Currency/Global",
      countKey: "global_rows",
    },
  ];

const getVisibleMissing = (row: AvailabilityRow) =>
  (row.missing || []).filter(
    (item) => item.trim().toLowerCase() !== "transactions"
  );

const hasAllVisibleChecks = (row: AvailabilityRow) =>
  checkLabels.every((check) => row.checks[check.key]);

const getVisibleStatus = (row: AvailabilityRow): AvailabilityStatus => {
  if (hasAllVisibleChecks(row)) return "Complete";
  if (getVisibleMissing(row).length > 0) return "Missing";
  return row.status === "Failed" ? "Missing" : row.status;
};

const normalizeAvailabilityRow = (row: AvailabilityRow): AvailabilityRow => {
  const status = getVisibleStatus(row);

  return {
    ...row,
    status,
    missing: hasAllVisibleChecks(row) ? [] : getVisibleMissing(row),
  };
};

const buildBrandAvailabilityRows = (
  rows: AvailabilityRow[]
): BrandAvailabilityRow[] => {
  const groups = new Map<string, AvailabilityRow[]>();

  rows.forEach((row) => {
    // Brand is the main identity.
    // Email is only a fallback if brand_name is missing.
    const brandKey =
      row.brand_name?.trim().toLowerCase() ||
      row.email.trim().toLowerCase();

    const existing = groups.get(brandKey) || [];

    existing.push(row);

    groups.set(brandKey, existing);
  });

  return Array.from(groups.entries()).map(
    ([brandKey, brandRows]) => {
      const primary = brandRows[0];

      const countries = Array.from(
        new Set(
          brandRows
            .map((row) => row.country?.trim())
            .filter(Boolean) as string[]
        )
      );

      const marketplaces = Array.from(
        new Set(
          brandRows
            .map((row) => row.marketplace_id?.trim())
            .filter(Boolean) as string[]
        )
      );

      // A brand-level check is true only when
      // ALL connected country rows pass that check.
      const checks: AvailabilityRow["checks"] = {
        sku_data: brandRows.every(
          (row) => row.checks.sku_data
        ),

        pnl_data: brandRows.every(
          (row) => row.checks.pnl_data
        ),

        ads_connected: brandRows.every(
          (row) => row.checks.ads_connected
        ),

        inventory_connected: brandRows.every(
          (row) => row.checks.inventory_connected
        ),

        currency_global_data: brandRows.every(
          (row) => row.checks.currency_global_data
        ),
      };

      const missing = Array.from(
        new Set(
          brandRows.flatMap((row) =>
            getVisibleMissing(row)
          )
        )
      );

      const complete = Object.values(checks).every(
        Boolean
      );

      const hasStale = brandRows.some(
        (row) => getVisibleStatus(row) === "Stale"
      );

      const status: AvailabilityStatus = complete
        ? "Complete"
        : missing.length > 0
          ? "Missing"
          : hasStale
            ? "Stale"
            : "Missing";

      return {
        brand_key: brandKey,

        user_id: primary.user_id,
        name: primary.name,
        email: primary.email,

        brand_name:
          primary.brand_name || "Brand not added",

        company_name: primary.company_name,

        countries,
        marketplaces,

        checks,
        status,
        missing,

        sourceRows: brandRows,
      };
    }
  );
};

export default function DataAvailabilityClient() {
  const router = useRouter();

  const [rows, setRows] = useState<AvailabilityRow[]>([]);
  const month = currentMonth();
  const year = currentYear();
  const [statusFilter, setStatusFilter] =
    useState<(typeof STATUS_OPTIONS)[number]>("All");
  const [selectedCountry, setSelectedCountry] = useState("All");
  const [searchQuery, setSearchQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [issueSummary, setIssueSummary] = useState<IssueSummary>({
    open: 0,
    resolved: 0,
    critical: 0,
    high: 0,
  });

  const fetchAvailability = useCallback(async () => {
    setLoading(true);

    try {
      const token = localStorage.getItem("superadmin_token");

      if (!token) {
        toast.error("No authentication token found");
        router.push("/superadmin/CDPAdminConsole");
        return;
      }

      const params = new URLSearchParams({
        month,
        year: String(year),
      });

      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/data_availability?${params.toString()}`,
        {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
        }
      );

      const data = (await response.json().catch(() => ({}))) as AvailabilityResponse;

      if (!response.ok) {
        if (response.status === 401) {
          localStorage.removeItem("superadmin_token");
          router.push("/superadmin/CDPAdminConsole");
          return;
        }

        throw new Error(data.message || "Failed to load data availability.");
      }

      setRows((data.rows || []).map(normalizeAvailabilityRow));

      try {
        const issueParams = new URLSearchParams({
          month,
          year: String(year),
        });

        const issueRunResponse = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/issue_detection/run?${issueParams.toString()}`,
          {
            method: "POST",
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
          }
        );

        const issueRunData = (await issueRunResponse
          .json()
          .catch(() => ({}))) as IssueDetectionResponse;

        if (!issueRunResponse.ok) {
          throw new Error(
            issueRunData.message || "Failed to run automated issue detection."
          );
        }

        const issuesResponse = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/issues?${issueParams.toString()}&status=open`,
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
          }
        );

        const issuesData = (await issuesResponse
          .json()
          .catch(() => ({}))) as IssuesResponse;

        if (!issuesResponse.ok) {
          throw new Error(issuesData.message || "Failed to load automated issues.");
        }

        setIssueSummary(
          issuesData.summary || {
            open: issueRunData.open_count || 0,
            resolved: 0,
            critical: 0,
            high: 0,
          }
        );
      } catch (issueError) {
        const issueMessage =
          issueError instanceof Error
            ? issueError.message
            : "Automated issue detection failed.";
        toast.error(issueMessage);
        setIssueSummary({
          open: 0,
          resolved: 0,
          critical: 0,
          high: 0,
        });
      }
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Failed to load data availability.";
      toast.error(message);
      setRows([]);
      setIssueSummary({
        open: 0,
        resolved: 0,
        critical: 0,
        high: 0,
      });
    } finally {
      setLoading(false);
    }
  }, [month, router, year]);

  useEffect(() => {
    fetchAvailability();
  }, [fetchAvailability]);

  const integratedCountries = useMemo(() => {
    return Array.from(
      new Set(
        rows
          .map((row) => row.country?.trim())
          .filter(Boolean) as string[]
      )
    ).sort((a, b) => a.localeCompare(b));
  }, [rows]);

  useEffect(() => {
    if (
      selectedCountry !== "All" &&
      !integratedCountries.includes(selectedCountry)
    ) {
      setSelectedCountry("All");
    }
  }, [integratedCountries, selectedCountry]);

  const countryRows = useMemo(
    () =>
      selectedCountry === "All"
        ? rows
        : rows.filter(
          (row) =>
            row.country?.trim().toLowerCase() ===
            selectedCountry.toLowerCase()
        ),
    [rows, selectedCountry]
  );

  const brandRows = useMemo(
    () => buildBrandAvailabilityRows(countryRows),
    [countryRows]
  );

  const filteredBrandRows = useMemo(() => {
    const query = searchQuery
      .trim()
      .toLowerCase();

    return brandRows.filter((brand) => {
      const matchesStatus =
        statusFilter === "All"
          ? true
          : brand.status === statusFilter;

      const searchable = [
        brand.brand_name,
        brand.company_name,
        brand.name,
        brand.email,
        brand.status,
        brand.missing.join(" "),
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();

      return (
        matchesStatus &&
        (!query || searchable.includes(query))
      );
    });
  }, [
    brandRows,
    searchQuery,
    statusFilter,
  ]);

  const visibleSummary = useMemo(() => {
    const counts = {
      total: brandRows.length,
      complete: 0,
      missing: 0,
      stale: 0,
      failed: 0,
    };

    brandRows.forEach((brand) => {
      const status = brand.status.toLowerCase() as
        | "complete"
        | "missing"
        | "stale"
        | "failed";
      counts[status] += 1;
    });

    return counts;
  }, [brandRows]);

  const summaryCards = [
    {
      label: "Total Brands",
      value: visibleSummary.total,
      icon: LuLayers3,
      detail: "Availability records checked",
    },
    {
      label: "Complete Brands",
      value: visibleSummary.complete,
      icon: LuCircleCheckBig,
      detail: "All required data available",
    },
    {
      label: "Incomplete Brands",
      value: visibleSummary.missing,
      icon: LuCircleX,
      detail: "Records with missing data",
    },
    {
      label: "Open Issues",
      value: issueSummary.open,
      icon: AlertTriangle,
      detail: "Issues requiring attention",
      onClick: () =>
        router.push(
          `/superadmin/Issues?month=${month}&year=${year}`
        ),
    },
  ];

  const availabilityColumns = [
    {
      key: "serial_no",
      label: "S.No.",

      render: (brand: BrandAvailabilityRow) => {
        const index =
          filteredBrandRows.findIndex(
            (item) =>
              item.brand_key ===
              brand.brand_key
          );

        return (
          <span className="font-medium text-white/65">
            {index >= 0 ? index + 1 : "-"}
          </span>
        );
      },
    },

    {
      key: "brand",
      label: "Brand",
      cellClassName: "font-semibold text-white",

      render: (brand: BrandAvailabilityRow) => (
        <span>
          {brand.brand_name ||
            "Not added"}
        </span>
      ),
    },

    {
      key: "company",
      label: "Company",

      render: (brand: BrandAvailabilityRow) =>
        brand.company_name ||
        "Not added",
    },

    {
      key: "admin",
      label: "Admin",

      render: (brand: BrandAvailabilityRow) => (
        <span className="font-medium text-white/75">
          {brand.name || "Not added"}
        </span>
      ),
    },

    {
      key: "email",
      label: "Email",

      render: (brand: BrandAvailabilityRow) => (
        <span className="break-all text-[10px] text-white/55 2xl:text-xs">
          {brand.email}
        </span>
      ),
    },

    ...checkLabels.map((check) => ({
      key: check.key,
      label: check.label,

      render: (
        brand: BrandAvailabilityRow
      ) => {
        const ok =
          brand.checks[check.key];

        return (
          <span
            className={`inline-flex min-w-[52px] items-center justify-center rounded-full border px-2.5 py-1 text-[10px] font-semibold 2xl:text-xs ${ok
              ? "border-emerald-300/25 bg-emerald-500/10 text-emerald-200"
              : "border-red-300/25 bg-red-500/10 text-red-100"
              }`}
          >
            {ok ? "Yes" : "No"}
          </span>
        );
      },
    })),

    {
      key: "status",
      label: "Status",

      render: (
        brand: BrandAvailabilityRow
      ) => (
        <div className="flex flex-col items-center">
          <span
            className={`inline-flex rounded-full border px-3 py-1 text-[10px] font-semibold 2xl:text-xs ${statusStyles[brand.status]
              }`}
          >
            {brand.status}
          </span>

          {brand.missing.length > 0 && (
            <span className="mt-1 max-w-[150px] whitespace-normal text-center text-[10px] leading-4 text-white/40">
              {brand.missing.join(", ")}
            </span>
          )}
        </div>
      ),
    },

  ];

  return (
    <div className="w-full text-white">
      <div className="space-y-5 2xl:space-y-6">

        {/* PAGE HEADER */}
        <PageBreadcrumb
          pageTitle="Data Availability"
          variant="superadmin"
          align="left"
          textSize="2xl"
        />

        {/* SUMMARY CARDS */}
        <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          {summaryCards.map((card) => {
            const Icon = card.icon;

            const content = (
              <div className="flex h-full items-center gap-3">
                <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-[#31d9e5]/15 text-[#31d9e5] 2xl:h-11 2xl:w-11">
                  <Icon size={20} strokeWidth={2.1} />
                </span>

                <div className="min-w-0">
                  <SummaryMetricCard
                    title={card.label}
                    value={card.value.toLocaleString()}
                    className="!bg-transparent !p-0 !shadow-none"
                    titleClassName="!text-white/70"
                    valueClassName="!text-white"
                  />

                  <p className="mt-1 text-[11px] text-white/45 2xl:text-xs">
                    {card.detail}
                  </p>
                </div>
              </div>
            );

            if (card.onClick) {
              return (
                <button
                  key={card.label}
                  type="button"
                  onClick={card.onClick}
                  className="relative min-h-[88px] overflow-hidden rounded-xl border-t-4 border-[#31D9E5] bg-[#484962] p-3 text-left shadow-[0_14px_32px_rgba(20,22,45,0.20)] transition hover:bg-[#50516d] 2xl:min-h-[96px] 2xl:p-4"
                >
                  {content}
                </button>
              );
            }

            return (
              <div
                key={card.label}
                className="relative min-h-[88px] overflow-hidden rounded-xl border-t-4 border-[#31D9E5] bg-[#484962] p-3 shadow-[0_14px_32px_rgba(20,22,45,0.20)] 2xl:min-h-[96px] 2xl:p-4"
              >
                {content}
              </div>
            );
          })}
        </section>

        {/* FILTERS */}
        <section className="rounded-xl border border-white/10 bg-[#484962] p-4 shadow-[0_16px_38px_rgba(20,22,45,0.20)]">
          <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
              <select
                value={selectedCountry}
                onChange={(event) => setSelectedCountry(event.target.value)}
                className="h-10 rounded-lg border border-white/10 bg-white/[0.06] px-3 text-xs text-white outline-none focus:border-[#31d9e5] focus:ring-2 focus:ring-[#31d9e5]/15 2xl:text-sm"
              >
                <option disabled className="bg-[#37384f] text-white/50">
                  Select Country
                </option>

                <option value="All" className="bg-[#37384f] text-white">
                  All Countries
                </option>

                {integratedCountries.map((country) => (
                  <option
                    key={country}
                    value={country}
                    className="bg-[#37384f] text-white"
                  >
                    {country.toUpperCase()}
                  </option>
                ))}
              </select>

              <select
                value={statusFilter}
                onChange={(event) =>
                  setStatusFilter(event.target.value as typeof statusFilter)
                }
                className="h-10 rounded-lg border border-white/10 bg-white/[0.06] px-3 text-xs text-white outline-none focus:border-[#31d9e5] focus:ring-2 focus:ring-[#31d9e5]/15 2xl:text-sm"
              >
                <option disabled className="bg-[#37384f] text-white/50">
                  Select Status
                </option>

                <option value="All" className="bg-[#37384f] text-white">
                  All
                </option>

                <option value="Complete" className="bg-[#37384f] text-white">
                  Complete
                </option>

                <option value="Missing" className="bg-[#37384f] text-white">
                  Missing
                </option>
              </select>
            </div>

            <div className="relative w-full lg:w-[340px] 2xl:w-[380px]">
              <input
                type="text"
                value={searchQuery}
                onChange={(event) => setSearchQuery(event.target.value)}
                placeholder="Search ..."
                className="h-10 w-full rounded-lg border border-white/10 bg-white/[0.06] px-3 pr-9 text-xs text-white outline-none placeholder:text-white/40 focus:border-[#31d9e5] focus:ring-2 focus:ring-[#31d9e5]/15 2xl:text-sm"
              />

              <Search
                size={14}
                className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-[#31d9e5]"
              />
            </div>
          </div>
        </section>

        {/* AVAILABILITY TABLE */}
        {loading ? (
          <div className="relative min-h-[260px] overflow-hidden rounded-xl border border-white/10 bg-[#484962]">
            <Loader
              fullscreen
              backgroundClass="bg-[#37384f]/65"
              roundedClass="rounded-xl"
            />
          </div>
        ) : (
          <SuperAdminUsersTable
            data={filteredBrandRows}
            columns={availabilityColumns}
            minWidth="1050px"
            emptyTitle="No availability records found"
            emptyDescription={
              searchQuery.trim()
                ? "Try changing your search or selected filters."
                : "No availability records are available for the current period."
            }
          />
        )}
      </div>
    </div>
  );
}
