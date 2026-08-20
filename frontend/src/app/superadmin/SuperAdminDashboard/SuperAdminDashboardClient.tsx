"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import {
  AlertTriangle,
  Building2,
  CheckCircle2,
  Database,
  Eye,
  Globe2,
  Search,
  Store,
  Tags,
  Trash2,
  UserCheck,
  UserCog,
  Users,
} from "lucide-react";
import { toast } from "sonner";

import Loader from "@/components/loader/Loader";
import SuperAdminUsersTable from "@/components/admin/table/SuperAdminUsersTable";
import SummaryMetricCard from "@/components/dropdowns/SummaryMetricCard";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL || "";
const MIN_LOADER_MS = 3000;

type StatusFilter = "all" | "active" | "inactive";
type WorkspaceTab = "accounts" | "admins" | "users" | "data" | "system";
type UploadKind = "currency" | "referral";
type CurrencyRateMode = "auto" | "manual";

type FormulaMarketplace = {
  label: string;
  country: string;
  marketplaceId?: string | null;
  transactionStatus?: string | null;
  sourceTableCount?: number;
  userCount?: number;
};

type Address = {
  building?: string;
  city?: string;
  country?: string;
  state?: string;
  zipcode?: string;
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
  address?: Address;
  created_at?: string | null;
  is_verified?: boolean;
  homeCurrency?: string | null;
  amazon_user_exists?: boolean;
  amazon_ads_exists?: boolean;
  user_table_exists?: boolean;
  sku_sheet_exists?: boolean;
  steps_exists?: boolean;
  amazon_connected?: boolean;
  connected_marketplaces_count?: number | null;
};

type AdminRow = {
  id: number | string;
  email: string;
  name?: string;
  company_name?: string;
  brand_name?: string;
  country?: string;
  marketplace_id?: string;
  annual_sales_range?: string;
  created_at?: string | null;
  is_admin?: boolean;
  is_superadmin?: boolean;
  is_verified?: boolean;
};

type DashboardResponse = {
  user_admins?: AdminRow[];
  users?: UserRow[];
  formula_marketplaces?: FormulaMarketplace[];
  message?: string;
};

type CurrencyRecord = {
  id?: number | string;
  user_currency?: string;
  selected_currency?: string;
  conversion_rate?: number | string;
  month?: string;
  year?: number | string;
};

type RiskItem = {
  id: string;
  label: string;
  count: number;
  tone: "amber" | "emerald" | "red";
  icon: React.ComponentType<{ size?: number; className?: string }>;
  detail: string;
};

const STATUS_OPTIONS: Array<{ value: StatusFilter; label: string }> = [
  { value: "all", label: "All" },
  { value: "active", label: "Active" },
  { value: "inactive", label: "Inactive" },
];

const WORKSPACE_TABS: Array<{ value: WorkspaceTab; label: string }> = [
  { value: "accounts", label: "Accounts" },
  { value: "admins", label: "Admins" },
  { value: "users", label: "Users" },
  { value: "data", label: "Data" },
  { value: "system", label: "System" },
];

const monthLookup: Record<string, number> = {
  jan: 1,
  january: 1,
  feb: 2,
  february: 2,
  mar: 3,
  march: 3,
  apr: 4,
  april: 4,
  may: 5,
  jun: 6,
  june: 6,
  jul: 7,
  july: 7,
  aug: 8,
  august: 8,
  sep: 9,
  sept: 9,
  september: 9,
  oct: 10,
  october: 10,
  nov: 11,
  november: 11,
  dec: 12,
  december: 12,
};

const shortMonthLabels = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
];

const currencySymbols: Record<string, string> = {
  USD: "$",
  GBP: "£",
  EUR: "€",
  INR: "₹",
  CAD: "C$",
  AUD: "A$",
  JPY: "¥",
};

const currencyCoverageOrder = ["GBP", "USD", "EUR", "INR", "CAD", "AUD", "JPY"];

const uploadSpecs: Record<
  UploadKind,
  { title: string; endpoint: string; accept: string }
> = {
  currency: {
    title: "Currency file",
    endpoint: "/superadmin/dashboard/upload_currency_file",
    accept: ".csv,.xlsx,.xls",
  },
  referral: {
    title: "Referral file",
    endpoint: "/superadmin/dashboard/upload_referral_file",
    accept: ".csv,.xlsx,.xls",
  },
};

function getToken() {
  return localStorage.getItem("superadmin_token") || "";
}

function formatNumber(value: number) {
  return value.toLocaleString("en-US");
}

function normalizeStatus(status?: string | boolean) {
  if (status === true) return "active";
  if (status === false) return "inactive";

  const value = String(status ?? "").trim().toLowerCase();

  if (["inactive", "disabled", "false", "0"].includes(value)) {
    return "inactive";
  }

  return "active";
}

function normalizeCurrencyCode(currency?: string) {
  return String(currency || "").trim().toUpperCase();
}

function getMonthNumber(month?: string) {
  const value = String(month || "").trim().toLowerCase();

  if (!value) return null;

  if (/^\d+$/.test(value)) {
    const numericMonth = Number(value);
    return numericMonth >= 1 && numericMonth <= 12 ? numericMonth : null;
  }

  return monthLookup[value] || null;
}

function formatCurrencyWithSymbol(currency?: string) {
  const currencyCode = normalizeCurrencyCode(currency);
  if (!currencyCode) return "-";

  const symbol = currencySymbols[currencyCode];
  return symbol ? `${currencyCode} (${symbol})` : currencyCode;
}

function getCurrencyCoverageGroupKey(record: CurrencyRecord) {
  return normalizeCurrencyCode(record.user_currency) || "OTHER";
}

function getCurrencyCoverageGroupRank(currency: string) {
  const index = currencyCoverageOrder.indexOf(currency);
  return index === -1 ? currencyCoverageOrder.length : index;
}

function isNeutralCurrencyConversion(record: CurrencyRecord) {
  const conversionRate = Number(record.conversion_rate);

  if (
    Number.isFinite(conversionRate) &&
    Math.abs(conversionRate - 1) < 0.000001
  ) {
    return true;
  }

  const fromCurrency = normalizeCurrencyCode(record.user_currency);
  const toCurrency = normalizeCurrencyCode(record.selected_currency);

  return Boolean(fromCurrency && toCurrency && fromCurrency === toCurrency);
}

function formatShortCurrencyPeriod(month?: string, year?: number | string) {
  const monthNumber = getMonthNumber(month);
  const numericYear = Number(year);

  if (!monthNumber || !numericYear) return "";

  return `${shortMonthLabels[monthNumber - 1]}'${String(numericYear).slice(-2)}`;
}

function sortByNewestId<T extends { id: number | string }>(items: T[]) {
  return [...items].sort((a, b) => {
    const aNumber = Number(a.id);
    const bNumber = Number(b.id);

    if (Number.isFinite(aNumber) && Number.isFinite(bNumber)) {
      return bNumber - aNumber;
    }

    return String(b.id).localeCompare(String(a.id), undefined, {
      numeric: true,
      sensitivity: "base",
    });
  });
}

function sortUsersByRecentJoin(items: UserRow[]) {
  return [...items].sort((a, b) => {
    const aTime = a.created_at ? new Date(a.created_at).getTime() : NaN;
    const bTime = b.created_at ? new Date(b.created_at).getTime() : NaN;
    const aHasDate = Number.isFinite(aTime);
    const bHasDate = Number.isFinite(bTime);

    if (aHasDate && bHasDate && aTime !== bTime) {
      return bTime - aTime;
    }

    if (aHasDate !== bHasDate) {
      return bHasDate ? 1 : -1;
    }

    const aNumber = Number(a.id);
    const bNumber = Number(b.id);

    if (Number.isFinite(aNumber) && Number.isFinite(bNumber)) {
      return bNumber - aNumber;
    }

    return String(b.id).localeCompare(String(a.id), undefined, {
      numeric: true,
      sensitivity: "base",
    });
  });
}

function isWorkspaceTab(value: string): value is WorkspaceTab {
  return WORKSPACE_TABS.some((tab) => tab.value === value);
}

function getCountryValues(value?: string | string[]) {
  const values = Array.isArray(value) ? value : String(value || "").split(",");

  return Array.from(
    new Set(
      values.map((country) => country.trim()).filter(Boolean)
    )
  );
}

function getUserCurrentCountries(user: UserRow) {
  return getCountryValues(
    Array.isArray(user.countries) && user.countries.length
      ? user.countries
      : user.country
  );
}

function getNativeCountry(user: UserRow) {
  return user.address?.country?.trim() || "Not added";
}

function formatCountryDisplay(countries: string[]) {
  return countries.length
    ? countries.map((country) => country.toUpperCase()).join(", ")
    : "Not added";
}

function getMarketplaceDisplays(
  country?: string,
  marketplaceId?: string,
  marketplaceIds?: string[]
) {
  const ids = Array.from(
    new Set(
      [marketplaceId, ...(marketplaceIds || [])]
        .map((value) => String(value || "").trim())
        .filter(Boolean)
    )
  );

  const countries = String(country || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);

  if (ids.length) {
    return ids.map((id, index) => ({
      label: countries[index] || countries[0] || id,
      marketplaceId: id,
      hasMarketplace: true,
    }));
  }

  if (countries.length) {
    return countries.map((label) => ({
      label,
      marketplaceId: "",
      hasMarketplace: true,
    }));
  }

  return [
    {
      label: "Unassigned",
      marketplaceId: "",
      hasMarketplace: false,
    },
  ];
}

export default function SuperAdminDashboardPage() {
  const router = useRouter();

  const [usersData, setUsersData] = useState<UserRow[]>([]);
  const [adminsData, setAdminsData] = useState<AdminRow[]>([]);
  const [currencyRecords, setCurrencyRecords] = useState<CurrencyRecord[]>([]);
  const [workspaceTab, setWorkspaceTab] = useState<WorkspaceTab>("accounts");
  const [searchQuery, setSearchQuery] = useState("");
  const [adminSearchQuery, setAdminSearchQuery] = useState("");
  const [userSearchQuery, setUserSearchQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [marketplaceFilter, setMarketplaceFilter] = useState("all");
  const [loading, setLoading] = useState(false);
  const [currencyLoading, setCurrencyLoading] = useState(false);
  const [error, setError] = useState("");
  const [actionLoading, setActionLoading] = useState<Record<string, boolean>>({});
  const [formulaMarketplaces, setFormulaMarketplaces] = useState<
    FormulaMarketplace[]
  >([]);
  const [formulaUpdating, setFormulaUpdating] = useState<string | null>(null);
  const [uploadFiles, setUploadFiles] = useState<Record<UploadKind, File | null>>({
    currency: null,
    referral: null,
  });
  const [uploading, setUploading] = useState<UploadKind | null>(null);
  const [fetchingCurrencyRates, setFetchingCurrencyRates] = useState(false);
  const [currencyRateMode, setCurrencyRateMode] =
    useState<CurrencyRateMode>("auto");

  const finishLoadingWithMinDelay = (startedAt: number) => {
    const elapsed = Date.now() - startedAt;
    const remaining = Math.max(0, MIN_LOADER_MS - elapsed);
    window.setTimeout(() => setLoading(false), remaining);
  };

  const handleUnauthorized = useCallback(() => {
    localStorage.removeItem("superadmin_token");
    router.push("/superadmin/CDPAdminConsole");
  }, [router]);

  const fetchDashboard = useCallback(
    async (silent = false) => {
      const startedAt = Date.now();

      if (!silent) setLoading(true);
      setError("");

      try {
        const token = getToken();

        if (!token) {
          handleUnauthorized();
          return;
        }

        const response = await fetch(
          `${API_BASE}/superadmin/dashboard?authenticated_user=superadmin`,
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
          }
        );

        const data = (await response
          .json()
          .catch(() => ({}))) as DashboardResponse;

        if (!response.ok) {
          if (response.status === 401 || response.status === 403) {
            handleUnauthorized();
            return;
          }

          throw new Error(data.message || "Failed to load superadmin dashboard");
        }

        setUsersData(data.users || []);
        setAdminsData(data.user_admins || []);
        setFormulaMarketplaces(data.formula_marketplaces || []);
      } catch (err) {
        const message =
          err instanceof Error ? err.message : "Failed to load dashboard data";
        setError(message);
        toast.error(message);
      } finally {
        if (!silent) finishLoadingWithMinDelay(startedAt);
      }
    },
    [handleUnauthorized]
  );

  const fetchCurrencyRecords = useCallback(async () => {
    try {
      setCurrencyLoading(true);
      const token = getToken();

      const response = await fetch(
        `${API_BASE}/superadmin/dashboard/view_currency_file`,
        {
          method: "GET",
          headers: token ? { Authorization: `Bearer ${token}` } : undefined,
        }
      );

      const data = (await response.json().catch(() => ({}))) as {
        data?: CurrencyRecord[];
        message?: string;
      };

      if (!response.ok) {
        throw new Error(data.message || "Failed to load currency records");
      }

      setCurrencyRecords(Array.isArray(data.data) ? data.data : []);
    } catch {
      setCurrencyRecords([]);
    } finally {
      setCurrencyLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchDashboard();
    fetchCurrencyRecords();
  }, [fetchDashboard, fetchCurrencyRecords]);

  useEffect(() => {
    const syncTabFromHash = (event?: Event) => {
      const customHash = (event as CustomEvent<{ hash?: string }>)?.detail?.hash;
      const hash = (customHash || window.location.hash).replace("#", "");

      if (isWorkspaceTab(hash)) {
        setWorkspaceTab(hash);
      }
    };

    syncTabFromHash();
    window.addEventListener("hashchange", syncTabFromHash);
    window.addEventListener("popstate", syncTabFromHash);
    window.addEventListener(
      "page-hash-navigate",
      syncTabFromHash as EventListener
    );

    return () => {
      window.removeEventListener("hashchange", syncTabFromHash);
      window.removeEventListener("popstate", syncTabFromHash);
      window.removeEventListener(
        "page-hash-navigate",
        syncTabFromHash as EventListener
      );
    };
  }, []);

  const currentCurrencyPeriod = useMemo(() => {
    const now = new Date();

    return {
      monthNumber: now.getMonth() + 1,
      year: now.getFullYear(),
      label: now.toLocaleString("en-US", {
        month: "long",
        year: "numeric",
      }),
    };
  }, []);

  const currentCurrencyPeriodShort = useMemo(
    () =>
      formatShortCurrencyPeriod(
        String(currentCurrencyPeriod.monthNumber),
        currentCurrencyPeriod.year
      ) || currentCurrencyPeriod.label,
    [currentCurrencyPeriod]
  );

  const currentMonthCurrencyRecords = useMemo(
    () =>
      currencyRecords.filter((record) => {
        const recordMonth = getMonthNumber(record.month);
        const recordYear = Number(record.year);

        return (
          recordMonth === currentCurrencyPeriod.monthNumber &&
          recordYear === currentCurrencyPeriod.year
        );
      }),
    [currencyRecords, currentCurrencyPeriod]
  );

  const visibleCurrencyRecords = useMemo(
    () =>
      currentMonthCurrencyRecords
        .filter((record) => !isNeutralCurrencyConversion(record))
        .sort((firstRecord, secondRecord) => {
          const firstGroup = getCurrencyCoverageGroupKey(firstRecord);
          const secondGroup = getCurrencyCoverageGroupKey(secondRecord);
          const groupDiff =
            getCurrencyCoverageGroupRank(firstGroup) -
            getCurrencyCoverageGroupRank(secondGroup);

          if (groupDiff !== 0) return groupDiff;

          return getCurrencyCoverageGroupRank(
            normalizeCurrencyCode(firstRecord.selected_currency)
          ) -
            getCurrencyCoverageGroupRank(
              normalizeCurrencyCode(secondRecord.selected_currency)
            );
        }),
    [currentMonthCurrencyRecords]
  );

  const groupedCurrencyRecords = useMemo(() => {
    const groups = new Map<string, CurrencyRecord[]>();

    visibleCurrencyRecords.forEach((record) => {
      const groupKey = getCurrencyCoverageGroupKey(record);
      const groupRecords = groups.get(groupKey) || [];
      groupRecords.push(record);
      groups.set(groupKey, groupRecords);
    });

    return Array.from(groups, ([currency, records]) => ({ currency, records }));
  }, [visibleCurrencyRecords]);

  const userOnlyRows = useMemo(() => usersData || [], [usersData]);
  const sortedAdmins = useMemo(() => sortByNewestId(adminsData), [adminsData]);
  const recentBrandRows = useMemo(
    () => sortUsersByRecentJoin(userOnlyRows).slice(0, 10),
    [userOnlyRows]
  );

  const filteredRecentBrands = useMemo(() => {
    const query = searchQuery.trim().toLowerCase();

    if (!query) return recentBrandRows;

    return recentBrandRows.filter((user) =>
      [
        user.email,
        user.name,
        user.company_name,
        user.brand_name,
        getNativeCountry(user),
        ...getUserCurrentCountries(user),
      ]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(query))
    );
  }, [recentBrandRows, searchQuery]);

  const filteredAdmins = useMemo(() => {
    const query = adminSearchQuery.trim().toLowerCase();

    if (!query) return sortedAdmins;

    return sortedAdmins.filter((admin) =>
      [
        admin.email,
        admin.name,
        admin.company_name,
        admin.brand_name,
        admin.country,
        admin.marketplace_id,
      ]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(query))
    );
  }, [adminSearchQuery, sortedAdmins]);

  const marketplaceCounts = useMemo(
    () =>
      userOnlyRows.reduce<Record<string, number>>((counts, user) => {
        const marketplaces = getMarketplaceDisplays(
          user.country,
          user.marketplace_id,
          user.marketplace_ids
        );

        const labels = marketplaces
          .filter((marketplace) => marketplace.hasMarketplace)
          .map((marketplace) => marketplace.label || "Unassigned");

        if (!labels.length) {
          counts.Unassigned = (counts.Unassigned || 0) + 1;
          return counts;
        }

        labels.forEach((label) => {
          counts[label] = (counts[label] || 0) + 1;
        });

        return counts;
      }, {}),
    [userOnlyRows]
  );

  const marketplaceOptions = useMemo(
    () => [
      "all",
      ...Object.keys(marketplaceCounts)
        .filter((country) => country !== "Unassigned")
        .sort((a, b) => a.localeCompare(b)),
    ],
    [marketplaceCounts]
  );

  const missingMarketplaceUsers = useMemo(
    () =>
      userOnlyRows.filter((user) => {
        const marketplaces = getMarketplaceDisplays(
          user.country,
          user.marketplace_id,
          user.marketplace_ids
        );

        return !marketplaces.some(
          (marketplace) => marketplace.hasMarketplace
        );
      }),
    [userOnlyRows]
  );

  const filteredUsers = useMemo(() => {
    const query = userSearchQuery.trim().toLowerCase();

    return userOnlyRows.filter((user) => {
      const marketplaces = getMarketplaceDisplays(
        user.country,
        user.marketplace_id,
        user.marketplace_ids
      );
      const marketplaceLabels = marketplaces.map(
        (marketplace) => marketplace.label
      );
      const userStatus = normalizeStatus(user.status);

      const matchesSearch =
        !query ||
        (user.email || "").toLowerCase().includes(query) ||
        (user.brand_name || "").toLowerCase().includes(query) ||
        (user.name || "").toLowerCase().includes(query) ||
        (user.company_name || "").toLowerCase().includes(query);

      const matchesStatus =
        statusFilter === "all" || userStatus === statusFilter;

      const matchesMarketplace =
        marketplaceFilter === "all" ||
        marketplaceLabels.includes(marketplaceFilter);

      return matchesSearch && matchesStatus && matchesMarketplace;
    });
  }, [marketplaceFilter, statusFilter, userOnlyRows, userSearchQuery]);

  const totalUsers = usersData.length;
  const activeUsers = usersData.filter(
    (user) => normalizeStatus(user.status) === "active"
  ).length;
  const inactiveUsers = usersData.filter(
    (user) => normalizeStatus(user.status) === "inactive"
  ).length;
  const verifiedUsers = usersData.filter((user) => user.is_verified).length;

  const totalAdmins = adminsData.length;

  const totalBrands = new Set(
    userOnlyRows
      .map((user) => (user.brand_name || "").trim())
      .filter(Boolean)
  ).size;

  const activeBrands = new Set(
    userOnlyRows
      .filter((user) => normalizeStatus(user.status) === "active")
      .map((user) => (user.brand_name || "").trim())
      .filter(Boolean)
  ).size;

  const inactiveBrands = new Set(
    userOnlyRows
      .filter((user) => normalizeStatus(user.status) === "inactive")
      .map((user) => (user.brand_name || "").trim())
      .filter(Boolean)
  ).size;

  const totalCompanies = new Set(
    userOnlyRows
      .map((user) => (user.company_name || "").trim())
      .filter(Boolean)
  ).size;

  const totalCountries = new Set(
    userOnlyRows
      .flatMap((user) => {
        const currentCountries = getUserCurrentCountries(user);
        return currentCountries.length ? currentCountries : [getNativeCountry(user)];
      })
      .map((country) => country.trim().toLowerCase())
      .filter((country) => country && country !== "not added")
  ).size;

  const totalMarketplaces = Object.keys(marketplaceCounts).filter(
    (key) => key !== "Unassigned"
  ).length;

  const summaryCards = [
    { title: "Active Brands", value: activeBrands, icon: Tags },
    { title: "Inactive Brands", value: inactiveBrands, icon: AlertTriangle },
    { title: "Admins", value: totalAdmins, icon: UserCog },
    { title: "Companies", value: totalCompanies, icon: Building2 },
    { title: "Countries", value: totalCountries, icon: Globe2 },
  ];

  const snapshotCards = [
    {
      title: "User Accounts",
      value: totalUsers,
      subtext: `${formatNumber(activeUsers)} active`,
      icon: Users,
    },
    {
      title: "Brands",
      value: totalBrands,
      subtext: `${formatNumber(totalCompanies)} companies`,
      icon: Building2,
    },
    {
      title: "Admins",
      value: totalAdmins,
      subtext: "Access operators",
      icon: UserCog,
    },
    {
      title: "Marketplaces",
      value: totalMarketplaces,
      subtext: `${formatNumber(missingMarketplaceUsers.length)} unassigned`,
      icon: Store,
    },
    {
      title: "Verified Users",
      value: verifiedUsers,
      subtext: `${formatNumber(totalUsers - verifiedUsers)} pending`,
      icon: UserCheck,
    },
    {
      title: "Inactive Users",
      value: inactiveUsers,
      subtext: "Currently disabled",
      icon: AlertTriangle,
    },
  ];

  const riskItems: RiskItem[] = [
    {
      id: "missing-marketplace",
      label: "Missing Marketplace",
      count: missingMarketplaceUsers.length,
      tone: missingMarketplaceUsers.length > 0 ? "amber" : "emerald",
      icon: Store,
      detail:
        missingMarketplaceUsers.length > 0
          ? "Members still need an Amazon marketplace assignment."
          : "All members have marketplace data.",
    },
    {
      id: "unverified-users",
      label: "Pending Verification",
      count: Math.max(totalUsers - verifiedUsers, 0),
      tone: totalUsers - verifiedUsers > 0 ? "amber" : "emerald",
      icon: UserCheck,
      detail: "Accounts waiting on verification are tracked here.",
    },
    {
      id: "inactive-users",
      label: "Inactive Users",
      count: inactiveUsers,
      tone: inactiveUsers > 0 ? "red" : "emerald",
      icon: AlertTriangle,
      detail: "Disabled member accounts are included in this count.",
    },
    {
      id: "currency-coverage",
      label: "Currency Rates",
      count: visibleCurrencyRecords.length,
      tone: visibleCurrencyRecords.length > 0 ? "emerald" : "amber",
      icon: Database,
      detail: `Rates loaded for ${currentCurrencyPeriod.label}.`,
    },
  ];

  const confirmToggleStatus = (user: UserRow) => {
    const currentStatus = normalizeStatus(user.status);
    const nextLabel = currentStatus === "active" ? "disable" : "enable";

    toast.custom(
      (toastId) => (
        <div className="w-[360px] rounded-xl border border-white/10 bg-[#37384f] p-4 text-white shadow-[0_24px_55px_rgba(20,22,45,0.45)]">
          <h3 className="text-xs lg:text-sm font-semibold text-white">
            Confirm status change
          </h3>
          <p className="mt-1 text-xs lg:text-sm text-white/60">
            Are you sure you want to {nextLabel}{" "}
            <span className="font-medium text-white">
              {user.brand_name || user.email}
            </span>
          </p>

          <div className="mt-4 flex justify-end gap-2">
            <button
              type="button"
              onClick={() => toast.dismiss(toastId)}
              className="rounded-lg border border-white/10 bg-white/[0.06] px-3 py-2 text-[11px] sm:text-xs font-semibold text-white/75 transition hover:bg-white/10 hover:text-white"
            >
              Cancel
            </button>

            <button
              type="button"
              onClick={() => {
                toast.dismiss(toastId);
                handleToggleStatus(user);
              }}
              className={`rounded-lg px-3 py-2 text-[11px] sm:text-xs font-semibold text-white transition ${currentStatus === "active"
                ? "bg-red-600 hover:bg-red-700"
                : "bg-emerald-600 hover:bg-emerald-700"
                }`}
            >
              Yes, {nextLabel}
            </button>
          </div>
        </div>
      ),
      { duration: Infinity, position: "top-center" }
    );
  };

  const handleToggleStatus = async (user: UserRow) => {
    const emailKey = user.email;
    const currentStatus = normalizeStatus(user.status);
    const nextStatus = currentStatus !== "active";

    try {
      setActionLoading((prev) => ({ ...prev, [emailKey]: true }));

      const token = getToken();

      if (!token) {
        handleUnauthorized();
        return;
      }

      const response = await fetch(
        `${API_BASE}/superadmin/dashboard/update_user_status`,
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
        throw new Error(data?.message || "Failed to update account status");
      }

      const updatedStatus =
        data?.status === true || String(data?.status).toLowerCase() === "true"
          ? "active"
          : data?.status === false ||
            String(data?.status).toLowerCase() === "false"
            ? "inactive"
            : nextStatus
              ? "active"
              : "inactive";

      setUsersData((prev) =>
        prev.map((item) =>
          String(item.id) === String(user.id)
            ? { ...item, status: updatedStatus }
            : item
        )
      );

      toast.success(
        `Account ${updatedStatus === "active" ? "enabled" : "disabled"}`
      );
    } catch (err) {
      toast.error(
        err instanceof Error ? err.message : "Status update failed"
      );
    } finally {
      setActionLoading((prev) => ({ ...prev, [emailKey]: false }));
    }
  };

  const deleteAccount = async (
    email: string,
    accountType: "user" | "admin"
  ) => {
    try {
      setActionLoading((prev) => ({ ...prev, [email]: true }));

      const token = getToken();

      if (!token) {
        handleUnauthorized();
        return;
      }

      const response = await fetch(`${API_BASE}/delete_admin`, {
        method: "DELETE",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ email: email.trim() }),
      });

      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(data?.message || "Failed to delete account");
      }

      if (accountType === "admin") {
        setAdminsData((prev) =>
          prev.filter((admin) => admin.email !== email)
        );
      } else {
        setUsersData((prev) =>
          prev.filter((user) => user.email !== email)
        );
      }

      toast.success("Account deleted successfully");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Delete failed");
    } finally {
      setActionLoading((prev) => ({ ...prev, [email]: false }));
    }
  };

  const confirmDelete = (
    email: string,
    accountType: "user" | "admin",
    brandName?: string
  ) => {
    toast.custom(
      (toastId) => (
        <div className="w-[360px] rounded-lg border border-white/10 bg-[#37384f] p-4 text-white shadow-[0_24px_55px_rgba(20,22,45,0.45)]">
          <h3 className="text-xs lg:text-sm font-semibold text-white">
            Delete account?
          </h3>
          <p className="mt-1 text-xs lg:text-sm text-white/60">
            This will permanently delete{" "}
            <span className="font-medium text-white">
              {brandName?.trim() || email}
            </span>.
          </p>

          <div className="mt-4 flex justify-end gap-2">
            <button
              type="button"
              onClick={() => toast.dismiss(toastId)}
              className="rounded-lg border border-white/10 bg-white/[0.06] px-3 py-2 text-[11px] sm:text-xs font-semibold text-white/75 transition hover:bg-white/10 hover:text-white"
            >
              Cancel
            </button>

            <button
              type="button"
              onClick={() => {
                toast.dismiss(toastId);
                deleteAccount(email, accountType);
              }}
              className="rounded-lg bg-red-600 px-3 py-2 text-[11px] sm:text-xs font-semibold text-white transition hover:bg-red-700"
            >
              Delete
            </button>
          </div>
        </div>
      ),
      { duration: Infinity, position: "top-center" }
    );
  };

  const handleViewUser = (email: string) => {
    router.push(
      `/superadmin/ViewUserPage/${encodeURIComponent(email)}`
    );
  };

  const uploadFile = async (kind: UploadKind) => {
    const selectedFile = uploadFiles[kind];
    const spec = uploadSpecs[kind];

    if (!selectedFile) {
      toast.error(`Select a ${spec.title.toLowerCase()} first`);
      return;
    }

    try {
      setUploading(kind);

      const token = getToken();

      if (!token) {
        handleUnauthorized();
        return;
      }

      const formData = new FormData();
      formData.append("file", selectedFile);

      const response = await fetch(`${API_BASE}${spec.endpoint}`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
        },
        body: formData,
      });

      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(
          data?.message || `${spec.title} upload failed`
        );
      }

      setUploadFiles((prev) => ({ ...prev, [kind]: null }));

      const successCount =
        typeof data?.success_count === "number"
          ? ` ${formatNumber(data.success_count)} rows processed.`
          : "";

      toast.success(`${spec.title} uploaded.${successCount}`);

      if (kind === "currency") {
        fetchCurrencyRecords();
      }
    } catch (err) {
      toast.error(
        err instanceof Error
          ? err.message
          : `${spec.title} upload failed`
      );
    } finally {
      setUploading(null);
    }
  };

  const fetchAutomaticCurrencyRates = async () => {
    try {
      setFetchingCurrencyRates(true);

      const token = getToken();

      if (!token) {
        handleUnauthorized();
        return;
      }

      const response = await fetch(
        `${API_BASE}/superadmin/dashboard/fetch_currency_rates`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            month: currentCurrencyPeriod.monthNumber,
            year: currentCurrencyPeriod.year,
          }),
        }
      );

      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(
          data?.message ||
          data?.error ||
          "Failed to fetch currency rates"
        );
      }

      const rowsFetched = Array.isArray(data?.data)
        ? data.data.length
        : 0;
      const errorCount = Array.isArray(data?.errors)
        ? data.errors.length
        : 0;

      if (errorCount > 0) {
        toast.warning(
          `Fetched ${formatNumber(rowsFetched)} rates with ${formatNumber(
            errorCount
          )} errors`
        );
      } else {
        toast.success(
          `Fetched ${formatNumber(rowsFetched)} rates for ${currentCurrencyPeriod.label
          }`
        );
      }

      fetchCurrencyRecords();
    } catch (err) {
      toast.error(
        err instanceof Error
          ? err.message
          : "Failed to fetch currency rates"
      );
    } finally {
      setFetchingCurrencyRates(false);
    }
  };

  const runFormulaUpdate = async (
    selectedMarketplace: FormulaMarketplace
  ) => {
    try {
      setFormulaUpdating(selectedMarketplace.country);

      const token = getToken();

      if (!token) {
        handleUnauthorized();
        return;
      }

      const url =
        `${API_BASE}/amazon_api/formula_update` +
        `?country=${encodeURIComponent(selectedMarketplace.country)}` +
        `&marketplace_id=${encodeURIComponent(
          selectedMarketplace.marketplaceId || ""
        )}` +
        `&store_in_db=true` +
        `&run_upload_pipeline=true` +
        `&transaction_status=${encodeURIComponent(
          selectedMarketplace.transactionStatus || "RELEASED"
        )}`;

      const response = await fetch(url, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
      });

      const data = await response.json().catch(() => ({}));

      if (!response.ok || !data?.success) {
        throw new Error(
          data?.error ||
          data?.message ||
          `Formula update failed for ${selectedMarketplace.label}`
        );
      }

      toast.success(
        `Formula update completed for ${selectedMarketplace.label}`
      );
    } catch (err) {
      toast.error(
        err instanceof Error ? err.message : "Formula update failed"
      );
    } finally {
      setFormulaUpdating(null);
    }
  };

  const adminColumns = [
    {
      key: "email",
      label: "Admin Email",
      cellClassName: "font-semibold text-white",
      render: (admin: AdminRow) => (
        <span className="break-all">{admin.email}</span>
      ),
    },
    {
      key: "role",
      label: "Role",
      render: (admin: AdminRow) => (
        <span className="inline-flex rounded-full border border-[#31d9e5]/25 bg-[#31d9e5]/10 px-3 py-1 text-[11px] sm:text-xs font-semibold text-[#31d9e5]">
          {admin.is_superadmin ? "Super Admin" : "Admin"}
        </span>
      ),
    },
    {
      key: "verified",
      label: "Verified",
      render: (admin: AdminRow) =>
        admin.is_verified ? (
          <CheckCircle2
            className="mx-auto text-emerald-300"
            size={18}
          />
        ) : (
          <AlertTriangle
            className="mx-auto text-amber-300"
            size={18}
          />
        ),
    },
    {
      key: "actions",
      label: "Actions",
      render: (admin: AdminRow) => {
        const isBusy = !!actionLoading[admin.email];

        return (
          <div className="flex items-center justify-center gap-2">
            <button
              type="button"
              onClick={() =>
                router.push(
                  `/superadmin/admins/${encodeURIComponent(admin.email)}`
                )
              }
              className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-white/10 bg-white/[0.06] text-white/75 transition hover:border-[#31d9e5]/40 hover:bg-[#31d9e5]/10 hover:text-[#31d9e5]"
              title="View admin"
              aria-label={`View ${admin.email}`}
            >
              <Eye size={17} />
            </button>

            <button
              type="button"
              onClick={() => confirmDelete(admin.email, "admin", admin.brand_name)}
              disabled={isBusy}
              className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-red-300/25 bg-red-500/10 text-red-200 transition hover:bg-red-500/20 hover:text-red-100 disabled:cursor-not-allowed disabled:opacity-60"
              title="Delete admin"
              aria-label={`Delete ${admin.email}`}
            >
              <Trash2 size={16} />
            </button>
          </div>
        );
      },
    },
  ];

  const userColumns = [
    {
      key: "name",
      label: "Name",
      cellClassName: "font-semibold text-white",
      render: (user: UserRow) => user.name || "Not added",
    },
    {
      key: "email",
      label: "Email",
      render: (user: UserRow) => (
        <span className="break-all">{user.email}</span>
      ),
    },
    {
      key: "brand",
      label: "Brand",
      render: (user: UserRow) => user.brand_name || "Not added",
    },
    {
      key: "company",
      label: "Company",
      render: (user: UserRow) => user.company_name || "Not added",
    },
    {
      key: "marketplace",
      label: "Marketplace",
      render: (user: UserRow) => {
        const labels = getMarketplaceDisplays(
          user.country,
          user.marketplace_id,
          user.marketplace_ids
        ).map((item) => item.label);

        return labels.join(", ");
      },
    },
    {
      key: "status",
      label: "Status",
      render: (user: UserRow) => {
        const userStatus = normalizeStatus(user.status);
        const isBusy = !!actionLoading[user.email];

        return (
          <button
            type="button"
            onClick={() => confirmToggleStatus(user)}
            disabled={isBusy}
            className={`relative inline-flex h-6 w-11 items-center rounded-full transition ${userStatus === "active"
              ? "bg-[#31d9e5]"
              : "bg-white/20"
              } ${isBusy ? "cursor-not-allowed opacity-60" : ""
              }`}
            title={
              userStatus === "active"
                ? "Disable user"
                : "Enable user"
            }
            aria-label={
              userStatus === "active"
                ? `Disable ${user.email}`
                : `Enable ${user.email}`
            }
          >
            <span
              className={`inline-block h-4 w-4 rounded-full bg-white shadow transition ${userStatus === "active"
                ? "translate-x-6"
                : "translate-x-1"
                }`}
            />
          </button>
        );
      },
    },
    {
      key: "actions",
      label: "Actions",
      render: (user: UserRow) => {
        const isBusy = !!actionLoading[user.email];

        return (
          <div className="flex items-center justify-center gap-2">
            <button
              type="button"
              onClick={() => handleViewUser(user.email)}
              className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-white/10 bg-white/[0.06] text-white/75 transition hover:border-[#31d9e5]/40 hover:bg-[#31d9e5]/10 hover:text-[#31d9e5]"
              title="View user"
              aria-label={`View ${user.email}`}
            >
              <Eye size={17} />
            </button>

            <button
              type="button"
              onClick={() => confirmDelete(user.email, "user", user.brand_name)}
              disabled={isBusy}
              className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-red-300/25 bg-red-500/10 text-red-200 transition hover:bg-red-500/20 hover:text-red-100 disabled:cursor-not-allowed disabled:opacity-60"
              title="Delete user"
              aria-label={`Delete ${user.email}`}
            >
              <Trash2 size={16} />
            </button>
          </div>
        );
      },
    },
  ];

  const setTab = (tab: WorkspaceTab) => {
    setWorkspaceTab(tab);

    if (window.location.hash !== `#${tab}`) {
      window.history.replaceState(
        null,
        "",
        `${window.location.pathname}${window.location.search}#${tab}`
      );
    }
  };

  const renderAccountsPanel = () => (
    <div id="accounts" className="space-y-5 2xl:space-y-6">
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-5">
        {summaryCards.map((card) => {
          const Icon = card.icon;

          return (
            <div
              key={card.title}
              className="relative min-h-[88px] overflow-hidden rounded-xl border-t-4 border-[#31D9E5] bg-[#484962] p-3 shadow-[0_14px_32px_rgba(20,22,45,0.20)] before:absolute before:inset-x-0 before:top-0 before:h-0.5 2xl:min-h-[96px] 2xl:p-4"
            >


              {/* className="relative min-h-[88px] overflow-hidden rounded-2xl border-t-4 border-[#31d9e5]/25 bg-[#484962] p-3 shadow-[0_14px_32px_rgba(20,22,45,0.20)]  before:absolute before:inset-x-0 before:top-0 before:h-[2px] before:rounded-t-2xl before:bg-[#31d9e5] 2xl:min-h-[96px] 2xl:p-4"
             */}

              <div className="flex h-full items-center gap-3">
                <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-[#31d9e5]/15 text-[#31d9e5] 2xl:h-11 2xl:w-11">
                  <Icon size={20} strokeWidth={2.1} />
                </span>

                <SummaryMetricCard
                  title={card.title}
                  value={formatNumber(card.value)}
                  className="!p-0 !shadow-none bg-transparent"
                  titleClassName="!text-white/70"
                  valueClassName="!text-white"
                />
              </div>
            </div>
          );
        })}
      </div>

      <section className="rounded-xl border border-white/10 bg-[#484962] p-4 text-white shadow-[0_16px_38px_rgba(20,22,45,0.22)] 2xl:p-5">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
          <div>
            <h2 className="text-lg font-bold leading-tight text-white 2xl:text-xl">
              Recent Brands
            </h2>
            <p className="mt-1 text-xs text-white/60 2xl:text-sm">
              View, search, and manage registered user brands.
            </p>
          </div>

          <div className="relative w-full sm:w-[360px] 2xl:w-[420px]">
            <input
              type="text"
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
              placeholder="Search by Email, Brand, Company or Country..."
              className="h-10 w-full rounded-xl border border-white/10 bg-white/[0.06] px-4 pr-10 text-xs text-white outline-none placeholder:text-white/40 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15 2xl:h-11 2xl:pr-11 2xl:text-sm"
            />
            <Search
              size={17}
              className="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-[#31d9e5]"
            />
          </div>
        </div>

        {loading ? (
          <div className="relative mt-5 min-h-[240px] overflow-hidden rounded-xl border border-white/10 bg-white/[0.03]">
            <Loader
              fullscreen
              backgroundClass="bg-[#37384f]/65"
              roundedClass="rounded-xl"
            />
          </div>
        ) : (
          <div className="mt-5">
            <SuperAdminUsersTable
              data={filteredRecentBrands}
              minWidth="1050px"
              emptyTitle={
                searchQuery.trim()
                  ? "No matching brands found"
                  : "No recent brands found"
              }
              emptyDescription={
                searchQuery.trim()
                  ? "Try searching by another brand, company, name, email or country."
                  : "Recent brand registrations will appear here."
              }
              columns={[
                {
                  key: "serial_no",
                  label: "S.No.",
                  render: (user) => {
                    const index = filteredRecentBrands.findIndex(
                      (item) => String(item.id) === String(user.id)
                    );

                    return (
                      <span className="font-medium text-white/65">
                        {index >= 0 ? index + 1 : "-"}
                      </span>
                    );
                  },
                },
                {
                  key: "brand_name",
                  label: "Brand Name",
                  cellClassName: "font-semibold text-white",
                  render: (user) => user.brand_name || "Not added",
                },
                {
                  key: "company_name",
                  label: "Company",
                  render: (user) => user.company_name || "Not added",
                },
                {
                  key: "native_country",
                  label: "Native Country",
                  render: (user) => getNativeCountry(user),
                },
                {
                  key: "country",
                  label: "Country",
                  render: (user) => {
                    const currentCountries = getUserCurrentCountries(user);

                    return currentCountries.length ? (
                      <div className="flex flex-wrap items-center justify-center gap-1.5">
                        {currentCountries.map((country) => (
                          <span
                            key={country}
                            className="inline-flex rounded-full border border-white/10 bg-white/[0.06] px-3 py-1 text-xs font-medium text-white/75"
                          >
                            {country.toUpperCase()}
                          </span>
                        ))}
                      </div>
                    ) : (
                      <span className="text-white/45">
                        Not added
                      </span>
                    );
                  },
                },
                {
                  key: "name",
                  label: "Admin Name",
                  render: (user) => user.name || "Not added",
                },
                {
                  key: "email",
                  label: "Email",
                  render: (user) => (
                    <span className="break-all">
                      {user.email || "Not added"}
                    </span>
                  ),
                },
                {
                  key: "status",
                  label: "Status",
                  render: (user) => {
                    const userStatus = normalizeStatus(user.status);
                    const isBusy = !!actionLoading[user.email];

                    return (
                      <button
                        type="button"
                        onClick={() => confirmToggleStatus(user)}
                        disabled={isBusy}
                        className={`relative inline-flex h-6 w-11 items-center rounded-full transition ${userStatus === "active"
                          ? "bg-[#31d9e5]"
                          : "bg-white/20"
                          } ${isBusy
                            ? "cursor-not-allowed opacity-60"
                            : ""
                          }`}
                        title={
                          userStatus === "active"
                            ? "Disable brand"
                            : "Enable brand"
                        }
                        aria-label={
                          userStatus === "active"
                            ? `Disable ${user.brand_name}`
                            : `Enable ${user.brand_name}`
                        }
                      >
                        <span
                          className={`inline-block h-4 w-4 rounded-full bg-white shadow transition ${userStatus === "active"
                            ? "translate-x-6"
                            : "translate-x-1"
                            }`}
                        />
                      </button>
                    );
                  },
                },
                {
                  key: "actions",
                  label: "Actions",
                  render: (user) => {
                    const isBusy = !!actionLoading[user.email];

                    return (
                      <div className="flex items-center justify-center gap-2">
                        <button
                          type="button"
                          onClick={() => handleViewUser(user.email)}
                          className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-white/10 bg-white/[0.06] text-white/75 shadow-sm transition hover:border-[#31d9e5]/40 hover:bg-[#31d9e5]/10 hover:text-[#31d9e5]"
                          title="View brand"
                          aria-label={`View ${user.email}`}
                        >
                          <Eye size={17} />
                        </button>

                        <button
                          type="button"
                          onClick={() =>
                            confirmDelete(
                              user.email,
                              "user",
                              user.brand_name
                            )
                          }
                          disabled={isBusy}
                          className={`inline-flex h-9 w-9 items-center justify-center rounded-lg border border-red-300/25 bg-red-500/10 text-red-200 shadow-sm transition hover:bg-red-500/20 hover:text-red-100 ${isBusy
                            ? "cursor-not-allowed opacity-60"
                            : ""
                            }`}
                          title="Delete brand"
                          aria-label={`Delete ${user.brand_name}`}
                        >
                          <Trash2 size={16} />
                        </button>
                      </div>
                    );
                  },
                },
              ]}
            />
          </div>
        )}
      </section>
    </div>
  );

  const renderAdminsPanel = () => (
    <div id="admins" className="space-y-5">
      <section className="rounded-lg border border-white/10 bg-[#484962] p-5 text-white shadow-[0_18px_40px_rgba(20,22,45,0.22)]">
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <h2 className="text-base lg:text-lg font-bold text-white">
              Admin Account Registry
            </h2>
            <p className="mt-1 text-xs lg:text-sm text-white/55">
              {formatNumber(filteredAdmins.length)} admin accounts
            </p>
          </div>

          <div className="relative w-full sm:w-[340px] lg:w-[400px]">
            <input
              type="text"
              value={adminSearchQuery}
              onChange={(event) =>
                setAdminSearchQuery(event.target.value)
              }
              placeholder="Search by Email, Brand or Company..."
              className="h-[42px] w-full rounded-xl border border-white/10 bg-white/[0.06] px-4 pr-11 text-xs lg:text-sm text-white shadow-sm outline-none placeholder:text-white/40 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15"
            />
            <Search
              size={17}
              className="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-[#31d9e5]"
            />
          </div>
        </div>
      </section>

      {loading ? (
        <Loader fullscreen backgroundClass="bg-[#37384f]/80" />
      ) : (
        <SuperAdminUsersTable
          columns={adminColumns}
          data={filteredAdmins}
          minWidth="720px"
          emptyTitle="No admins found"
          emptyDescription="Admin accounts will appear here."
        />
      )}
    </div>
  );

  const renderUsersPanel = () => (
    <div id="users" className="space-y-5">
      <section className="rounded-lg border border-white/10 bg-[#484962] p-5">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
          <div>
            <h2 className="text-base lg:text-lg font-bold text-white">
              User Accounts
            </h2>
            <p className="mt-1 text-xs lg:text-sm text-white/55">
              {formatNumber(filteredUsers.length)} matching users
            </p>
          </div>

          <div className="flex flex-col gap-3 lg:flex-row">
            <div className="relative w-full lg:w-[320px]">
              <input
                value={userSearchQuery}
                onChange={(event) =>
                  setUserSearchQuery(event.target.value)
                }
                placeholder="Search users..."
                className="h-11 w-full rounded-lg border border-white/10 bg-white/[0.06] px-4 pr-11 text-xs lg:text-sm text-white outline-none placeholder:text-white/40"
              />
              <Search
                size={17}
                className="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-[#31d9e5]"
              />
            </div>

            <select
              value={statusFilter}
              onChange={(event) =>
                setStatusFilter(event.target.value as StatusFilter)
              }
              className="h-11 rounded-lg border border-white/10 bg-[#37384f] px-3 text-xs lg:text-sm text-white"
            >
              {STATUS_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>

            <select
              value={marketplaceFilter}
              onChange={(event) =>
                setMarketplaceFilter(event.target.value)
              }
              className="h-11 rounded-lg border border-white/10 bg-[#37384f] px-3 text-xs lg:text-sm text-white"
            >
              {marketplaceOptions.map((option) => (
                <option key={option} value={option}>
                  {option === "all" ? "All marketplaces" : option}
                </option>
              ))}
            </select>
          </div>
        </div>
      </section>

      {loading ? (
        <Loader fullscreen backgroundClass="bg-[#37384f]/80" />
      ) : (
        <SuperAdminUsersTable
          columns={userColumns}
          data={filteredUsers}
          minWidth="1100px"
          emptyTitle="No users found"
          emptyDescription="Try changing the search or filters."
        />
      )}
    </div>
  );

  const renderDataPanel = () => (
    <div id="data" className="space-y-5">
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        {snapshotCards.map((card) => {
          const Icon = card.icon;

          return (
            <div
              key={card.title}
              className="rounded-xl border border-white/10 bg-[#484962] p-4"
            >
              <div className="flex items-center justify-between">
                <p className="text-xs lg:text-sm text-white/60">{card.title}</p>
                <Icon size={18} className="text-[#31d9e5]" />
              </div>
              <p className="mt-2 text-xl lg:text-2xl font-bold">
                {formatNumber(card.value)}
              </p>
              <p className="mt-1 text-[11px] sm:text-xs text-white/45">
                {card.subtext}
              </p>
            </div>
          );
        })}
      </div>

      <section className="rounded-xl border border-white/10 bg-[#484962] p-5">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
          <div>
            <h2 className="text-base lg:text-lg font-bold">Data uploads</h2>
            <p className="mt-1 text-xs lg:text-sm text-white/55">
              Upload currency or referral source files.
            </p>
          </div>

          <div className="flex rounded-lg border border-white/10 bg-white/[0.04] p-1">
            {(["auto", "manual"] as CurrencyRateMode[]).map(
              (mode) => (
                <button
                  key={mode}
                  type="button"
                  onClick={() => setCurrencyRateMode(mode)}
                  className={`rounded-md px-3 py-2 text-[11px] sm:text-xs font-semibold capitalize ${currencyRateMode === mode
                    ? "bg-[#31d9e5] text-[#37384f]"
                    : "text-white/60"
                    }`}
                >
                  {mode}
                </button>
              )
            )}
          </div>
        </div>

        <div className="mt-5 grid gap-4 md:grid-cols-2">
          {(Object.keys(uploadSpecs) as UploadKind[]).map((kind) => {
            const spec = uploadSpecs[kind];

            return (
              <div
                key={kind}
                className="rounded-lg border border-white/10 bg-white/[0.04] p-4"
              >
                <p className="font-semibold">{spec.title}</p>
                <input
                  type="file"
                  accept={spec.accept}
                  onChange={(event) =>
                    setUploadFiles((prev) => ({
                      ...prev,
                      [kind]: event.target.files?.[0] || null,
                    }))
                  }
                  className="mt-3 block w-full text-xs lg:text-sm text-white/70"
                />
                <button
                  type="button"
                  onClick={() => uploadFile(kind)}
                  disabled={uploading === kind}
                  className="mt-3 rounded-lg bg-[#31d9e5] px-4 py-2 text-xs lg:text-sm font-semibold text-[#37384f] disabled:opacity-60"
                >
                  {uploading === kind ? "Uploading..." : "Upload"}
                </button>
              </div>
            );
          })}
        </div>

        {currencyRateMode === "auto" && (
          <button
            type="button"
            onClick={fetchAutomaticCurrencyRates}
            disabled={fetchingCurrencyRates}
            className="mt-4 rounded-lg border border-[#31d9e5]/30 bg-[#31d9e5]/10 px-4 py-2 text-xs lg:text-sm font-semibold text-[#31d9e5] disabled:opacity-60"
          >
            {fetchingCurrencyRates
              ? "Fetching rates..."
              : `Fetch rates for ${currentCurrencyPeriod.label}`}
          </button>
        )}
      </section>

      <section className="rounded-xl border border-white/10 bg-[#484962] p-5">
        <h2 className="text-base lg:text-lg font-bold">
          Currency coverage · {currentCurrencyPeriodShort}
        </h2>

        {currencyLoading ? (
          <p className="mt-4 text-xs lg:text-sm text-white/50">
            Loading currency records...
          </p>
        ) : groupedCurrencyRecords.length ? (
          <div className="mt-4 space-y-4">
            {groupedCurrencyRecords.map((group) => (
              <div
                key={group.currency}
                className="rounded-lg border border-white/10 bg-white/[0.04] p-4"
              >
                <p className="font-semibold">
                  {formatCurrencyWithSymbol(group.currency)}
                </p>

                <div className="mt-3 space-y-2">
                  {group.records.map((record, index) => (
                    <div
                      key={`${record.id ?? index}`}
                      className="flex items-center justify-between text-xs lg:text-sm"
                    >
                      <span className="text-white/60">
                        {formatCurrencyWithSymbol(
                          record.selected_currency
                        )}
                      </span>
                      <span className="font-medium">
                        {String(record.conversion_rate ?? "-")}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <p className="mt-4 text-xs lg:text-sm text-white/50">
            No currency records for this period.
          </p>
        )}
      </section>
    </div>
  );

  const renderSystemPanel = () => (
    <div id="system" className="space-y-5">
      <section className="rounded-xl border border-white/10 bg-[#484962] p-5">
        <h2 className="text-base lg:text-lg font-bold">System health</h2>

        <div className="mt-4 grid gap-3 md:grid-cols-2">
          {riskItems.map((item) => {
            const Icon = item.icon;

            return (
              <div
                key={item.id}
                className="rounded-lg border border-white/10 bg-white/[0.04] p-4"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Icon size={17} className="text-[#31d9e5]" />
                    <p className="font-semibold">{item.label}</p>
                  </div>
                  <span className="text-base lg:text-lg font-bold">{item.count}</span>
                </div>
                <p className="mt-2 text-xs lg:text-sm text-white/50">
                  {item.detail}
                </p>
              </div>
            );
          })}
        </div>
      </section>

      <section className="rounded-xl border border-white/10 bg-[#484962] p-5">
        <h2 className="text-base lg:text-lg font-bold">Formula updates</h2>
        <p className="mt-1 text-xs lg:text-sm text-white/55">
          Run the formula upload pipeline for a marketplace.
        </p>

        <div className="mt-4 grid gap-3 md:grid-cols-2">
          {formulaMarketplaces.length ? (
            formulaMarketplaces.map((marketplace) => {
              const isBusy =
                formulaUpdating === marketplace.country;

              return (
                <div
                  key={`${marketplace.country}-${marketplace.marketplaceId || marketplace.label}`}
                  className="rounded-lg border border-white/10 bg-white/[0.04] p-4"
                >
                  <p className="font-semibold">{marketplace.label}</p>
                  <p className="mt-1 text-[11px] sm:text-xs text-white/45">
                    {marketplace.country}
                    {marketplace.marketplaceId
                      ? ` · ${marketplace.marketplaceId}`
                      : ""}
                  </p>
                  <button
                    type="button"
                    onClick={() => runFormulaUpdate(marketplace)}
                    disabled={isBusy}
                    className="mt-3 rounded-lg bg-[#31d9e5] px-4 py-2 text-xs lg:text-sm font-semibold text-[#37384f] disabled:opacity-60"
                  >
                    {isBusy ? "Updating..." : "Run update"}
                  </button>
                </div>
              );
            })
          ) : (
            <p className="text-xs lg:text-sm text-white/50">
              No formula marketplaces returned by the dashboard API.
            </p>
          )}
        </div>
      </section>
    </div>
  );

  return (
    <div className="space-y-5 text-white 2xl:space-y-6">

      {/* Show Welcome ONLY on SuperAdmin Dashboard / Accounts */}
      {workspaceTab === "accounts" && (
        <div className="rounded-xl border border-white/10 bg-[#484962] p-4 shadow-[0_16px_38px_rgba(20,22,45,0.22)] 2xl:p-5">
          <div className="flex flex-col leading-tight">
            <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
              <h1 className="text-lg font-bold tracking-tight text-white 2xl:text-xl">
                Welcome,
              </h1>

              <span className="text-lg font-bold tracking-tight text-[#31d9e5] 2xl:text-xl">
                Super Admin!
              </span>
            </div>

            <p className="mt-2 text-xs text-white/65 2xl:text-sm">
              Manage users, brands, companies, countries and account status.
            </p>
          </div>
        </div>
      )}

      {error && (
        <div className="rounded-lg border border-red-300/20 bg-red-500/10 px-4 py-3 text-xs lg:text-sm text-red-100">
          {error}
        </div>
      )}

      {workspaceTab === "accounts" && renderAccountsPanel()}
      {workspaceTab === "admins" && renderAdminsPanel()}
      {workspaceTab === "users" && renderUsersPanel()}
      {workspaceTab === "data" && renderDataPanel()}
      {workspaceTab === "system" && renderSystemPanel()}
    </div>
  );
}
