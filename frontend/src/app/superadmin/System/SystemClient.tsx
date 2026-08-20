"use client";

import React, { useEffect, useMemo, useState } from "react";
import {
    AlertTriangle,
    BadgeDollarSign,
    Building2,
    Database,
    Store,
    UserCheck,
    UserCog,
    Users,
} from "lucide-react";
import { toast } from "sonner";

import SummaryMetricCard from "@/components/dropdowns/SummaryMetricCard";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import { LuUserRoundX, LuWorkflow } from "react-icons/lu";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL || "";

type FormulaMarketplace = {
    label: string;
    country: string;
    marketplaceId?: string | null;
    transactionStatus?: string | null;
    sourceTableCount?: number;
    userCount?: number;
};

type DashboardResponse = {
    users?: UserRow[];
    user_admins?: AdminRow[];
    formula_marketplaces?: FormulaMarketplace[];
    message?: string;
};

type RiskItem = {
    id: string;
    label: string;
    count: number;
    icon: React.ComponentType<{
        size?: number;
        className?: string;
    }>;
    detail: string;
};

type UserRow = {
    id: number | string;
    email: string;
    brand_name?: string;
    company_name?: string;
    status?: string | boolean;
    is_verified?: boolean;
    country?: string;
    marketplace_id?: string;
    marketplace_ids?: string[];
};

type AdminRow = {
    id: number | string;
    email: string;
};

type CurrencyRecord = {
    id?: number | string;
    user_currency?: string;
    selected_currency?: string;
    conversion_rate?: number | string;
    month?: string;
    year?: number | string;
};

const currencySymbols: Record<string, string> = {
    USD: "$",
    GBP: "£",
    EUR: "€",
    INR: "₹",
    CAD: "C$",
    AUD: "A$",
    JPY: "¥",
};

const currencyCoverageOrder = [
    "GBP",
    "USD",
    "EUR",
    "INR",
    "CAD",
    "AUD",
    "JPY",
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

const getToken = () =>
    localStorage.getItem("superadmin_token") || "";

const formatNumber = (value: number) =>
    value.toLocaleString("en-US");

const normalizeStatus = (status?: string | boolean) => {
    if (status === true) return "active";
    if (status === false) return "inactive";

    const value = String(status ?? "").trim().toLowerCase();

    return ["inactive", "disabled", "false", "0"].includes(value)
        ? "inactive"
        : "active";
};

const normalizeCurrencyCode = (currency?: string) =>
    String(currency || "").trim().toUpperCase();

const getMonthNumber = (month?: string) => {
    const value = String(month || "").trim().toLowerCase();

    if (!value) return null;

    if (/^\d+$/.test(value)) {
        const numericMonth = Number(value);
        return numericMonth >= 1 && numericMonth <= 12
            ? numericMonth
            : null;
    }

    return monthLookup[value] || null;
};

const formatCurrencyWithSymbol = (currency?: string) => {
    const code = normalizeCurrencyCode(currency);

    if (!code) return "-";

    const symbol = currencySymbols[code];

    return symbol ? `${code} (${symbol})` : code;
};

const getCurrencyCoverageGroupKey = (
    record: CurrencyRecord
) => normalizeCurrencyCode(record.user_currency) || "OTHER";

const getCurrencyCoverageGroupRank = (currency: string) => {
    const index = currencyCoverageOrder.indexOf(currency);

    return index === -1
        ? currencyCoverageOrder.length
        : index;
};

const isNeutralCurrencyConversion = (
    record: CurrencyRecord
) => {
    const rate = Number(record.conversion_rate);

    if (
        Number.isFinite(rate) &&
        Math.abs(rate - 1) < 0.000001
    ) {
        return true;
    }

    const from = normalizeCurrencyCode(record.user_currency);
    const to = normalizeCurrencyCode(
        record.selected_currency
    );

    return Boolean(from && to && from === to);
};

const formatShortCurrencyPeriod = (
    month?: string,
    year?: number | string
) => {
    const monthNumber = getMonthNumber(month);
    const numericYear = Number(year);

    if (!monthNumber || !numericYear) return "";

    return `${shortMonthLabels[monthNumber - 1]}'${String(
        numericYear
    ).slice(-2)}`;
};

export default function DataOperationsClient() {
    const [users, setUsers] = useState<UserRow[]>([]);
    const [admins, setAdmins] = useState<AdminRow[]>([]);
    const [currencyRecords, setCurrencyRecords] = useState<
        CurrencyRecord[]
    >([]);

    const [loading, setLoading] = useState(true);
    const [currencyLoading, setCurrencyLoading] =
        useState(true);

    const [formulaMarketplaces, setFormulaMarketplaces] = useState<
        FormulaMarketplace[]
    >([]);

    const [formulaUpdating, setFormulaUpdating] =
        useState<string | null>(null);

    const [
        fetchingCurrencyRates,
        setFetchingCurrencyRates,
    ] = useState(false);

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

    useEffect(() => {
        const fetchData = async () => {
            try {
                setLoading(true);

                const token = getToken();

                const response = await fetch(
                    `${API_BASE}/superadmin/dashboard?authenticated_user=superadmin`,
                    {
                        headers: {
                            Authorization: `Bearer ${token}`,
                            "Content-Type": "application/json",
                        },
                    }
                );

                const data = (await response.json()) as DashboardResponse;

                if (!response.ok) {
                    throw new Error(
                        data?.message || "Failed to load data"
                    );
                }

                setUsers(
                    Array.isArray(data?.users) ? data.users : []
                );

                setFormulaMarketplaces(
                    Array.isArray(data?.formula_marketplaces)
                        ? data.formula_marketplaces
                        : []
                );

                setFormulaMarketplaces(
                    Array.isArray(data?.formula_marketplaces)
                        ? data.formula_marketplaces
                        : []
                );

            } catch (error) {
                toast.error(
                    error instanceof Error
                        ? error.message
                        : "Failed to load data"
                );
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, []);

    const fetchCurrencyRecords = async () => {
        try {
            setCurrencyLoading(true);

            const token = getToken();

            const response = await fetch(
                `${API_BASE}/superadmin/dashboard/view_currency_file`,
                {
                    headers: token
                        ? {
                            Authorization: `Bearer ${token}`,
                        }
                        : undefined,
                }
            );

            const data = await response.json();

            if (!response.ok) {
                throw new Error(
                    data?.message ||
                    "Failed to load currency records"
                );
            }

            setCurrencyRecords(
                Array.isArray(data?.data) ? data.data : []
            );
        } catch {
            setCurrencyRecords([]);
        } finally {
            setCurrencyLoading(false);
        }
    };

    useEffect(() => {
        fetchCurrencyRecords();
    }, []);

    const currentMonthCurrencyRecords = useMemo(
        () =>
            currencyRecords.filter((record) => {
                return (
                    getMonthNumber(record.month) ===
                    currentCurrencyPeriod.monthNumber &&
                    Number(record.year) ===
                    currentCurrencyPeriod.year
                );
            }),
        [currencyRecords, currentCurrencyPeriod]
    );

    const visibleCurrencyRecords = useMemo(
        () =>
            currentMonthCurrencyRecords
                .filter(
                    (record) =>
                        !isNeutralCurrencyConversion(record)
                )
                .sort((a, b) => {
                    const aGroup =
                        getCurrencyCoverageGroupKey(a);
                    const bGroup =
                        getCurrencyCoverageGroupKey(b);

                    const groupDiff =
                        getCurrencyCoverageGroupRank(aGroup) -
                        getCurrencyCoverageGroupRank(bGroup);

                    if (groupDiff !== 0) {
                        return groupDiff;
                    }

                    return (
                        getCurrencyCoverageGroupRank(
                            normalizeCurrencyCode(
                                a.selected_currency
                            )
                        ) -
                        getCurrencyCoverageGroupRank(
                            normalizeCurrencyCode(
                                b.selected_currency
                            )
                        )
                    );
                }),
        [currentMonthCurrencyRecords]
    );

    const groupedCurrencyRecords = useMemo(() => {
        const groups = new Map<
            string,
            CurrencyRecord[]
        >();

        visibleCurrencyRecords.forEach((record) => {
            const key =
                getCurrencyCoverageGroupKey(record);

            const rows = groups.get(key) || [];

            rows.push(record);

            groups.set(key, rows);
        });

        return Array.from(
            groups,
            ([currency, records]) => ({
                currency,
                records,
            })
        );
    }, [visibleCurrencyRecords]);

    const totalUsers = users.length;

    const activeUsers = users.filter(
        (user) =>
            normalizeStatus(user.status) === "active"
    ).length;

    const inactiveUsers = users.filter(
        (user) =>
            normalizeStatus(user.status) === "inactive"
    ).length;

    const verifiedUsers = users.filter(
        (user) => user.is_verified
    ).length;

    const totalAdmins = admins.length;

    const totalBrands = new Set(
        users
            .map((user) =>
                String(user.brand_name || "").trim()
            )
            .filter(Boolean)
    ).size;

    const totalCompanies = new Set(
        users
            .map((user) =>
                String(user.company_name || "").trim()
            )
            .filter(Boolean)
    ).size;

    const marketplaceValues = new Set(
        users
            .flatMap((user) => [
                user.marketplace_id,
                ...(user.marketplace_ids || []),
            ])
            .filter(Boolean)
    );

    const totalMarketplaces =
        marketplaceValues.size;

    const missingMarketplaceUsers =
        users.filter(
            (user) =>
                !user.marketplace_id &&
                !(user.marketplace_ids || []).length
        ).length;

    const snapshotCards = [
        {
            title: "User Accounts",
            value: totalUsers,
            subtext: `${formatNumber(
                activeUsers
            )} active`,
            icon: Users,
        },
        {
            title: "Brands",
            value: totalBrands,
            subtext: `${formatNumber(
                totalCompanies
            )} companies`,
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
            subtext: `${formatNumber(
                missingMarketplaceUsers
            )} unassigned`,
            icon: Store,
        },
        {
            title: "Verified Users",
            value: verifiedUsers,
            subtext: `${formatNumber(
                totalUsers - verifiedUsers
            )} pending`,
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
            count: missingMarketplaceUsers,
            icon: Store,
            detail:
                missingMarketplaceUsers > 0
                    ? "Users still need an Amazon marketplace assignment."
                    : "All users have marketplace data.",
        },
        {
            id: "unverified-users",
            label: "Pending Verification",
            count: Math.max(totalUsers - verifiedUsers, 0),
            icon: UserCheck,
            detail: "Accounts waiting on verification are tracked here.",
        },
        {
            id: "inactive-users",
            label: "Inactive Users",
            count: inactiveUsers,
            icon: LuUserRoundX,
            detail: "Disabled user accounts are included in this count.",
        },
        {
            id: "currency-coverage",
            label: "Currency Rates",
            count: visibleCurrencyRecords.length,
            icon: BadgeDollarSign,
            detail: `Rates loaded for ${currentCurrencyPeriod.label}.`,
        },
    ];

    const fetchAutomaticCurrencyRates =
        async () => {
            try {
                setFetchingCurrencyRates(true);

                const token = getToken();

                const response = await fetch(
                    `${API_BASE}/superadmin/dashboard/fetch_currency_rates`,
                    {
                        method: "POST",
                        headers: {
                            Authorization: `Bearer ${token}`,
                            "Content-Type": "application/json",
                        },
                        body: JSON.stringify({
                            month:
                                currentCurrencyPeriod.monthNumber,
                            year: currentCurrencyPeriod.year,
                        }),
                    }
                );

                const data = await response.json();

                if (!response.ok) {
                    throw new Error(
                        data?.message ||
                        data?.error ||
                        "Failed to fetch currency rates"
                    );
                }

                toast.success(
                    `Currency rates fetched for ${currentCurrencyPeriod.label}`
                );

                fetchCurrencyRecords();
            } catch (error) {
                toast.error(
                    error instanceof Error
                        ? error.message
                        : "Failed to fetch rates"
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
                toast.error("Super admin authentication required");
                return;
            }

            const url =
                `${API_BASE}/amazon_api/formula_update` +
                `?country=${encodeURIComponent(
                    selectedMarketplace.country
                )}` +
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

            const data = await response
                .json()
                .catch(() => ({}));

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
        } catch (error) {
            toast.error(
                error instanceof Error
                    ? error.message
                    : "Formula update failed"
            );
        } finally {
            setFormulaUpdating(null);
        }
    };

    return (
        <div className="w-full space-y-5 text-white 2xl:space-y-6">
            <PageBreadcrumb
                pageTitle="System"
                variant="superadmin"
                align="left"
                textSize="2xl"
            />


            {/* SYSTEM HEALTH */}
            <section>
                {/* <div className="mb-4">
                    

                    <p className="mt-1 text-xs text-white/55 lg:text-sm">
                        Monitor account, marketplace and currency data health.
                    </p>
                </div> */}

                <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                    {riskItems.map((item) => {
                        const Icon = item.icon;

                        return (
                            <div
                                key={item.id}
                                className="relative min-h-[88px] overflow-hidden rounded-xl border-t-4 border-[#31D9E5] bg-[#484962] p-3 shadow-[0_14px_32px_rgba(20,22,45,0.20)] 2xl:min-h-[96px] 2xl:p-4"
                            >
                                <div className="flex h-full items-center gap-3">
                                    <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-[#31d9e5]/15 text-[#31d9e5] 2xl:h-11 2xl:w-11">
                                        <Icon size={20} />
                                    </span>

                                    <div className="min-w-0">
                                        <SummaryMetricCard
                                            title={item.label}
                                            value={formatNumber(item.count)}
                                            className="!p-0 !shadow-none bg-transparent"
                                            titleClassName="!text-white/70"
                                            valueClassName="!text-white"
                                        />

                                        <p className="mt-1 text-[11px] leading-4 text-white/45 2xl:text-xs">
                                            {item.detail}
                                        </p>
                                    </div>
                                </div>
                            </div>
                        );
                    })}
                </div>
            </section>

            {/* CURRENCY + FORMULA OPERATIONS */}
            <section className="rounded-xl border border-white/10 bg-[#484962] p-5">
                <div>
                    {/* <h2 className="text-base font-bold text-white lg:text-lg">
                        Data Operations
                    </h2> */}

                    <PageBreadcrumb
                        pageTitle="Data Operations"
                        variant="superadmin"
                        align="left"
                        textSize="xl"
                    />

                    <p className="mt-1 text-xs text-white/55 lg:text-sm">
                        Manage currency rates and formula processing.
                    </p>
                </div>

                <div className="mt-5 space-y-4">

                    {/* RIGHT - FORMULA UPDATES */}
                    <div className="rounded-xl border border-white/10 bg-white/[0.04] p-4">
                        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
                            <div className="flex items-start gap-3">
                                <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-[#31d9e5]/15 text-[#31d9e5]">
                                    <BadgeDollarSign size={19} />
                                </span>

                                <div>
                                    <h3 className="text-sm font-semibold text-white lg:text-base">
                                        Currency Rates
                                    </h3>

                                    <p className="mt-1 text-xs leading-5 text-white/50">
                                        Fetch the latest currency conversion rates for the current period.
                                    </p>
                                </div>
                            </div>

                            <button
                                type="button"
                                onClick={fetchAutomaticCurrencyRates}
                                disabled={fetchingCurrencyRates}
                                className="inline-flex h-10 shrink-0 items-center justify-center rounded-lg border border-[#31d9e5]/30 bg-[#31d9e5]/10 px-4 text-xs font-semibold text-[#31d9e5] transition hover:bg-[#31d9e5]/15 disabled:cursor-not-allowed disabled:opacity-60 lg:text-sm"
                            >
                                {fetchingCurrencyRates
                                    ? "Fetching rates..."
                                    : `Fetch rates for ${currentCurrencyPeriod.label}`}
                            </button>
                        </div>
                    </div>

                   
                    {/* FORMULA UPDATES */}
                    <div className="rounded-xl border border-white/10 bg-white/[0.04] p-4">
                        <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">

                            {/* LEFT - TITLE */}
                            <div className="flex items-start gap-3 xl:min-w-[320px]">
                                <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-[#31d9e5]/15 text-[#31d9e5]">
                                    <LuWorkflow size={19} />
                                </span>

                                <div>
                                    <h3 className="text-sm font-semibold text-white lg:text-base">
                                        Formula Updates
                                    </h3>

                                    <p className="mt-1 text-xs leading-5 text-white/50">
                                        Run the formula processing pipeline for a marketplace.
                                    </p>
                                </div>
                            </div>

                            {/* RIGHT - MARKETPLACES */}
                            {formulaMarketplaces.length ? (
                                <div className="grid w-full gap-3 sm:grid-cols-2 xl:max-w-[720px]">
                                    {formulaMarketplaces.map((marketplace) => {
                                        const isBusy =
                                            formulaUpdating === marketplace.country;

                                        return (
                                            <div
                                                key={`${marketplace.country}-${marketplace.marketplaceId ||
                                                    marketplace.label
                                                    }`}
                                                className="rounded-lg border border-white/10 bg-[#37384f]/35 p-3"
                                            >
                                                <div className="flex items-center justify-between gap-4">
                                                    <div className="min-w-0">
                                                        <p className="text-xs font-semibold text-white lg:text-sm">
                                                            {marketplace.label}
                                                        </p>

                                                        <p className="mt-1 truncate text-[10px] text-white/40 lg:text-[11px]">
                                                            {marketplace.marketplaceId ||
                                                                marketplace.country}
                                                        </p>
                                                    </div>

                                                    <button
                                                        type="button"
                                                        onClick={() =>
                                                            runFormulaUpdate(marketplace)
                                                        }
                                                        disabled={isBusy}
                                                        className="inline-flex h-9 shrink-0 items-center justify-center rounded-lg bg-[#31d9e5] px-4 text-[11px] font-semibold text-[#37384f] transition hover:bg-[#31d9e5]/90 disabled:cursor-not-allowed disabled:opacity-60"
                                                    >
                                                        {isBusy ? "Updating..." : "Run"}
                                                    </button>
                                                </div>
                                            </div>
                                        );
                                    })}
                                </div>
                            ) : (
                                <p className="text-xs text-white/45">
                                    No formula marketplaces available.
                                </p>
                            )}
                        </div>
                    </div>
                </div>
            </section>

            {/* CURRENCY COVERAGE */}
            <section className="rounded-xl border border-white/10 bg-[#484962] p-5">
                <div className="flex items-center justify-between gap-4">
                    <div>
                        {/* <h2 className="text-base font-bold text-white lg:text-lg">
                            Currency Coverage
                        </h2> */}

                        <PageBreadcrumb
                            pageTitle="Currency Coverage"
                            variant="superadmin"
                            align="left"
                            textSize="xl"
                        />

                        <p className="mt-1 text-xs text-white/50 lg:text-sm">
                            Conversion rates for {currentCurrencyPeriodShort}
                        </p>
                    </div>

                    {/* <span className="rounded-lg border border-[#31d9e5]/20 bg-[#31d9e5]/10 px-3 py-1.5 text-xs font-semibold text-[#31d9e5]">
                        {visibleCurrencyRecords.length} rates
                    </span> */}
                </div>

                {currencyLoading ? (
                    <p className="mt-4 text-xs text-white/50 lg:text-sm">
                        Loading currency records...
                    </p>
                ) : groupedCurrencyRecords.length ? (
                    <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                        {groupedCurrencyRecords.map((group) => (
                            <div
                                key={group.currency}
                                className="rounded-xl border border-white/10 bg-white/[0.04] p-4"
                            >
                                <div className="mb-3 flex items-center justify-between">
                                    <h3 className="text-sm font-semibold text-white lg:text-base">
                                        {formatCurrencyWithSymbol(group.currency)}
                                    </h3>
                                </div>

                                <div className="space-y-1.5">
                                    {group.records.map((record, index) => (
                                        <div
                                            key={record.id ?? index}
                                            className="flex items-center justify-between rounded-lg bg-[#37384f]/30 px-3 py-2"
                                        >
                                            <span className="text-xs text-white/60">
                                                {formatCurrencyWithSymbol(
                                                    record.selected_currency
                                                )}
                                            </span>

                                            <span className="text-xs font-semibold text-white lg:text-sm">
                                                {String(record.conversion_rate ?? "-")}
                                            </span>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        ))}
                    </div>
                ) : (
                    <p className="mt-4 text-xs text-white/50 lg:text-sm">
                        No currency records for this period.
                    </p>
                )}
            </section>
        </div>
    );
}