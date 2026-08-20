"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import Image from "next/image";
import { usePathname, useRouter } from "next/navigation";
import {
    Users,
    UserCheck,
    RefreshCw,
    LogOut,
    Menu,
    X,
    Shield,
    Settings,
    KeyRound,
    ChevronLeft,
    ChevronRight,
    Building2,
    Database,
    AlertTriangle,
    Activity,
} from "lucide-react";
import { toast } from "sonner";
import { MdDashboard } from "react-icons/md";
import { FaTags } from "react-icons/fa";
import { RiAdminFill } from "react-icons/ri";
import { BsDatabaseCheck } from "react-icons/bs";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL || "";

type FormulaMarketplace = {
    label: string;
    country: string;
    marketplaceId?: string | null;
    transactionStatus?: string | null;
    sourceTableCount?: number;
};

type SuperAdminHash = "accounts" | "admins" | "users" | "data" | "system";

const SUPERADMIN_HASHES: SuperAdminHash[] = [
    "accounts",
    "admins",
    "users",
    "data",
    "system",
];

function getSuperAdminHash(hash?: string) {
    const navItems = [
        {
            label: "Accounts",
            href: "/superadmin/SuperAdminDashboard#accounts",
            icon: Users,
        },
        {
            label: "Admins",
            href: "/superadmin/Admins",
            icon: Shield,
        },
        {
            label: "Members",
            href: "/superadmin/SuperAdminDashboard#users",
            icon: UserCheck,
        },
        {
            label: "Data Ops",
            href: "/superadmin/SuperAdminDashboard#data",
            icon: Database,
        },
        {
            label: "System",
            href: "/superadmin/SuperAdminDashboard#system",
            icon: Activity,
        },
        {
            label: "Data Availability",
            href: "/superadmin/DataAvailability",
            icon: Database,
        },
        {
            label: "Issues",
            href: "/superadmin/Issues",
            icon: AlertTriangle,
        },
    ];
    const value = (hash || "").replace("#", "");
    return SUPERADMIN_HASHES.includes(value as SuperAdminHash)
        ? (value as SuperAdminHash)
        : "accounts";
}

export default function SuperAdminLayoutClient({
    children,
}: {
    children: React.ReactNode;
}) {
    const router = useRouter();
    const pathname = usePathname();

    const [sidebarOpen, setSidebarOpen] = useState(false);
    const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
    const [showSettings, setShowSettings] = useState(false);
    const [formulaUpdating, setFormulaUpdating] = useState(false);
    const [showFormulaCountryModal, setShowFormulaCountryModal] = useState(false);
    const [formulaMarketplaces, setFormulaMarketplaces] = useState<
        FormulaMarketplace[]
    >([]);
    const [selectedFormulaCountry, setSelectedFormulaCountry] = useState("");
    const [superAdminEmail, setSuperAdminEmail] = useState<string>("");
    const [currentHash, setCurrentHash] =
        useState<SuperAdminHash>("accounts");

    const publicSuperAdminRoutes = [
        "/superadmin/CDPAdminConsole",
        "/superadmin/SuperadminResetPassword",
    ];

    const isPublicRoute = publicSuperAdminRoutes.includes(pathname);

    useEffect(() => {
        if (isPublicRoute) return;

        const token = localStorage.getItem("superadmin_token");
        const storedEmail = localStorage.getItem("superadmin_email");

        if (!token) {
            router.push("/superadmin/CDPAdminConsole");
            return;
        }

        setSuperAdminEmail(storedEmail || "");
    }, [isPublicRoute, router]);

    useEffect(() => {
        if (isPublicRoute) return;

        const fetchFormulaMarketplaces = async () => {
            const token = localStorage.getItem("superadmin_token");
            if (!token) return;

            try {
                const response = await fetch(
                    `${API_BASE}/superadmin/dashboard/formula_marketplaces`,
                    {
                        method: "GET",
                        headers: {
                            Authorization: `Bearer ${token}`,
                        },
                    }
                );
                const data = await response.json().catch(() => ({}));
                const marketplaces = Array.isArray(data?.formula_marketplaces)
                    ? data.formula_marketplaces
                    : [];

                setFormulaMarketplaces(marketplaces);
                setSelectedFormulaCountry((currentCountry) =>
                    marketplaces.some(
                        (marketplace: FormulaMarketplace) =>
                            marketplace.country === currentCountry
                    )
                        ? currentCountry
                        : marketplaces[0]?.country || ""
                );
            } catch {
                setFormulaMarketplaces([]);
                setSelectedFormulaCountry("");
            }
        };

        fetchFormulaMarketplaces();
    }, [isPublicRoute]);

    useEffect(() => {
        if (isPublicRoute) return;

        const syncHash = (event?: Event) => {
            const customHash = (event as CustomEvent<{ hash?: string }>)?.detail
                ?.hash;
            setCurrentHash(getSuperAdminHash(customHash || window.location.hash));
        };

        syncHash();
        window.addEventListener("hashchange", syncHash);
        window.addEventListener("popstate", syncHash);
        window.addEventListener("page-hash-navigate", syncHash as EventListener);

        return () => {
            window.removeEventListener("hashchange", syncHash);
            window.removeEventListener("popstate", syncHash);
            window.removeEventListener(
                "page-hash-navigate",
                syncHash as EventListener
            );
        };
    }, [isPublicRoute, pathname]);

    if (isPublicRoute) {
        return <>{children}</>;
    }

    const navItems = [
        {
            label: "Dashboard",
            href: "/superadmin/SuperAdminDashboard",
            icon: MdDashboard,
        },
        {
            label: "Brands",
            href: "/superadmin/Brands",
            icon: FaTags,
        },
        {
            label: "Admins",
            href: "/superadmin/Admins",
            icon: RiAdminFill,
        },
        {
            label: "System",
            href: "/superadmin/System",
            icon: Activity,
        },
        {
            label: "Data Availability",
            href: "/superadmin/DataAvailability",
            icon: BsDatabaseCheck,
        },
        // {
        //     label: "Issues",
        //     href: "/superadmin/Issues",
        //     icon: AlertTriangle,
        // },
    ];

    const handleNavClick = (
        event: React.MouseEvent<HTMLAnchorElement>,
        href: string
    ) => {
        setSidebarOpen(false);

        // Normal routes like /Admins, /Brands, /Issues
        if (!href.includes("#")) {
            return;
        }

        // Hash based dashboard sections
        const [targetPath, targetHash = "accounts"] = href.split("#");

        if (pathname !== targetPath) {
            return;
        }

        event.preventDefault();

        const nextUrl = `${targetPath}#${targetHash}`;

        window.history.pushState(null, "", nextUrl);

        setCurrentHash(getSuperAdminHash(targetHash));

        window.dispatchEvent(
            new CustomEvent("page-hash-navigate", {
                detail: { hash: targetHash },
            })
        );
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
                        loading: "Logging you out...",
                        success: "Logged out successfully.",
                        error: "Logout failed. Please try again.",
                    }
                );
            } else {
                toast.info("You are already logged out.");
            }
        } finally {
            localStorage.removeItem("superadmin_token");
            router.push("/superadmin/CDPAdminConsole");
        }
    };

    const handleFormulaUpdate = async (country: string) => {
        const token = localStorage.getItem("superadmin_token");

        if (!token) {
            toast.error("Superadmin token not found. Please login again.");
            router.push("/superadmin/CDPAdminConsole");
            return;
        }

        const selectedMarketplace = formulaMarketplaces.find(
            (marketplace) => marketplace.country === country
        );

        if (!selectedMarketplace) {
            toast.error("Please select a valid marketplace.");
            return;
        }

        try {
            setFormulaUpdating(true);
            setShowFormulaCountryModal(false);

            const params = new URLSearchParams({
                country: selectedMarketplace.country,
                marketplace_id: selectedMarketplace.marketplaceId || "",
                transaction_status:
                    selectedMarketplace.transactionStatus || "RELEASED",
            });

            const url =
                `${API_BASE}/amazon_api/formula_update?${params.toString()}`;

            const response = await fetch(url, {
                method: "GET",
                headers: {
                    Authorization: `Bearer ${token}`,
                    "Content-Type": "application/json",
                },
            });

            const json = await response.json().catch(() => ({}));

            if (!response.ok || !json?.success) {
                throw new Error(
                    json?.error ||
                    json?.message ||
                    `Formula update failed for ${selectedMarketplace.label}`
                );
            }

            toast.success(
                `Formula update completed for ${selectedMarketplace.label}`
            );
        } catch (error) {
            const msg = error instanceof Error ? error.message : "Formula update failed";
            toast.error(msg);
        } finally {
            setFormulaUpdating(false);
        }
    };


    return (
        <div className="min-h-screen bg-[#37384f] text-white">
            {sidebarOpen && (
                <div
                    className="fixed inset-0 z-40 bg-black/50 lg:hidden"
                    onClick={() => setSidebarOpen(false)}
                />
            )}

            <aside
                className={`fixed left-0 top-0 z-50 h-screen overflow-hidden border-r border-white/10 bg-[#42435c] text-white shadow-[0_28px_70px_rgba(20,22,45,0.45)] transition-all duration-300 lg:translate-x-0 ${sidebarCollapsed ? "lg:w-0 lg:border-0 lg:shadow-none" : "lg:w-72"
                    } w-72 ${sidebarOpen ? "translate-x-0" : "-translate-x-full"}`}
            >
                <div className="relative flex h-20 items-center justify-center border-b border-white/10 px-5">
                    <Image
                        width={180}
                        height={40}
                        src="/images/auth/Phormula.png"
                        alt="Phormula"
                        priority
                        className="h-auto w-[200px]"
                    />

                    <button
                        type="button"
                        onClick={() => setSidebarOpen(false)}
                        className="absolute right-4 rounded-lg p-2 text-white/80 transition hover:bg-white/10 hover:text-white lg:hidden"
                        aria-label="Close sidebar"
                    >
                        <X size={22} />
                    </button>
                </div>

                <div className="flex h-[calc(100vh-80px)] flex-col justify-between px-4 py-5">
                    <div>
                        <nav className="space-y-2">
                            {navItems.map((item) => {
                                const Icon = item.icon;

                                const hasHash = item.href.includes("#");

                                let isActive = false;

                                if (hasHash) {
                                    const [itemPath, itemHash = "accounts"] = item.href.split("#");

                                    isActive =
                                        pathname === itemPath &&
                                        currentHash === itemHash;
                                } else {
                                    isActive =
                                        pathname === item.href ||
                                        pathname.startsWith(`${item.href}/`);
                                }

                                return (
                                    <Link
                                        key={item.label}
                                        href={item.href}
                                        onClick={(event) => handleNavClick(event, item.href)}
                                        className={`flex items-center gap-3 rounded-xl px-4 py-3 text-sm font-medium transition ${isActive
                                            ? "bg-[#31d9e5] text-[#303247] shadow-[0_10px_22px_rgba(20,220,230,0.18)]"
                                            : "text-white/75 hover:bg-white/[0.07] hover:text-white"
                                            }`}
                                    >
                                        <Icon size={18} />
                                        <span>{item.label}</span>
                                    </Link>
                                );
                            })}
                        </nav>
                    </div>

                    <div className="flex items-center gap-3 rounded-2xl border border-white/10 bg-white/[0.06] px-4 py-3 shadow-sm">
                        <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-[#31d9e5]/15 text-[#31d9e5]">
                            <Shield size={20} />
                        </span>

                        <div>
                            <p className="text-sm font-semibold text-white">Super Admin</p>
                            <p className="text-xs text-white/60">Control Panel</p>
                        </div>
                    </div>
                </div>
            </aside>

            <div
                className={`transition-all duration-300 ${sidebarCollapsed ? "lg:pl-0" : "lg:pl-72"
                    }`}
            >
                <header className="sticky top-0 z-30 border-b border-white/10 bg-[#484962]/90 shadow-[0_12px_30px_rgba(20,22,45,0.20)] backdrop-blur">
                    <div className="flex h-20 items-center justify-between gap-4 px-4 sm:px-6">
                        <div className="flex items-center gap-4">
                            <button
                                type="button"
                                onClick={() => setSidebarOpen(true)}
                                className="rounded-xl border border-white/10 bg-white/[0.06] p-2 text-white shadow-sm transition hover:bg-white/10 lg:hidden"
                                aria-label="Open sidebar"
                            >
                                <Menu size={22} />
                            </button>

                            <button
                                type="button"
                                onClick={() => setSidebarCollapsed((prev) => !prev)}
                                className="hidden rounded-xl border border-white/10 bg-white/[0.06] p-2 text-white shadow-sm transition hover:bg-white/10 lg:inline-flex"
                                aria-label={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
                            >
                                {sidebarCollapsed ? (
                                    <ChevronRight size={22} />
                                ) : (
                                    <ChevronLeft size={22} />
                                )}
                            </button>
                        </div>

                        <div className="relative">
                            <button
                                type="button"
                                onClick={() => setShowSettings((s) => !s)}
                                className={`inline-flex h-11 w-11 items-center justify-center rounded-xl bg-[#31d9e5] text-[#303247] shadow-[0_10px_22px_rgba(20,220,230,0.20)] transition hover:-translate-y-0.5 hover:bg-[#28cbd6] ${showSettings ? "ring-2 ring-[#31d9e5]/40" : ""
                                    }`}
                                aria-label="Open settings"
                            >
                                <Settings size={20} />
                            </button>

                            {showSettings && (
                                <>
                                    <button
                                        type="button"
                                        className="fixed inset-0 z-40 cursor-default"
                                        onClick={() => setShowSettings(false)}
                                        aria-label="Close settings menu"
                                    />

                                    <div className="absolute right-0 z-50 mt-3 w-64 overflow-hidden rounded-2xl border border-white/10 bg-[#484962] shadow-[0_24px_55px_rgba(20,22,45,0.40)]">
                                        <div className="border-b border-white/10 px-4 py-3">
                                            <p className="text-sm font-semibold text-white">
                                                Super Admin
                                            </p>
                                            <p className="text-xs text-white/55">
                                                {superAdminEmail || "care@phormula.io"}
                                            </p>
                                        </div>

                                        <div className="p-2">
                                            <button
                                                type="button"
                                                onClick={() => {
                                                    setShowSettings(false);
                                                    router.push("/superadmin/Superadminchangepassword");
                                                }}
                                                className="flex w-full items-center gap-3 rounded-xl px-3 py-3 text-left text-sm font-medium text-white/80 transition hover:bg-white/[0.07] hover:text-white"
                                            >
                                                <span className="flex h-8 w-8 items-center justify-center rounded-lg bg-[#31d9e5]/15 text-[#31d9e5]">
                                                    <KeyRound size={16} />
                                                </span>
                                                Change Password
                                            </button>

                                            <button
                                                type="button"
                                                onClick={() => {
                                                    setShowSettings(false);
                                                    setSelectedFormulaCountry(
                                                        (currentCountry) =>
                                                            currentCountry ||
                                                            formulaMarketplaces[0]?.country ||
                                                            ""
                                                    );
                                                    setShowFormulaCountryModal(true);
                                                }}
                                                disabled={
                                                    formulaUpdating ||
                                                    formulaMarketplaces.length === 0
                                                }
                                                className="flex w-full items-center gap-3 rounded-xl px-3 py-3 text-left text-sm font-medium text-white/80 transition hover:bg-white/[0.07] hover:text-white disabled:cursor-not-allowed disabled:opacity-60"
                                            >
                                                <span className="flex h-8 w-8 items-center justify-center rounded-lg bg-[#31d9e5]/15 text-[#31d9e5]">
                                                    <RefreshCw size={16} />
                                                </span>
                                                {formulaUpdating ? "Updating Formula..." : "Formula Update"}
                                            </button>

                                            <div className="my-2 border-t border-white/10" />

                                            <button
                                                type="button"
                                                onClick={handleLogout}
                                                className="flex w-full items-center gap-3 rounded-xl px-3 py-3 text-left text-sm font-medium text-red-200 transition hover:bg-red-500/10 hover:text-red-100"
                                            >
                                                <span className="flex h-8 w-8 items-center justify-center rounded-lg bg-red-500/10 text-red-200">
                                                    <LogOut size={16} />
                                                </span>
                                                Logout
                                            </button>
                                        </div>
                                    </div>
                                </>
                            )}
                        </div>
                    </div>
                </header>

                <main className="p-4 sm:p-6">{children}</main>
            </div>

            {
                showFormulaCountryModal && (
                    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-black/50 px-4">
                        <div className="w-full max-w-md overflow-hidden rounded-2xl border border-white/10 bg-[#484962] text-white shadow-[0_28px_70px_rgba(20,22,45,0.50)]">
                            <div className="border-b border-white/10 px-6 py-4">
                                <h2 className="text-lg font-semibold text-white">
                                    Formula Update
                                </h2>
                                <p className="mt-1 text-sm text-white/60">
                                    Select country for formula update
                                </p>
                            </div>

                            <div className="space-y-3 px-6 py-5">
                                {formulaMarketplaces.length > 0 ? (
                                    formulaMarketplaces.map((marketplace) => (
                                        <label
                                            key={marketplace.country}
                                            className={`flex cursor-pointer items-center justify-between rounded-xl border px-4 py-3 transition ${selectedFormulaCountry === marketplace.country
                                                ? "border-[#31d9e5] bg-[#31d9e5]/10"
                                                : "border-white/10 bg-white/[0.04] hover:bg-white/[0.07]"
                                                }`}
                                        >
                                            <div>
                                                <p className="font-medium text-white">
                                                    {marketplace.label}
                                                </p>
                                                <p className="text-xs text-white/55">
                                                    Marketplace:{" "}
                                                    {marketplace.marketplaceId ||
                                                        "Unavailable"}
                                                </p>
                                            </div>

                                            <input
                                                type="radio"
                                                name="formula_country"
                                                value={marketplace.country}
                                                checked={
                                                    selectedFormulaCountry ===
                                                    marketplace.country
                                                }
                                                onChange={() =>
                                                    setSelectedFormulaCountry(
                                                        marketplace.country
                                                    )
                                                }
                                                className="h-4 w-4 accent-[#31d9e5]"
                                            />
                                        </label>
                                    ))
                                ) : (
                                    <div className="rounded-xl border border-white/10 bg-white/[0.04] px-4 py-5 text-sm text-white/60">
                                        No formula-ready countries found.
                                    </div>
                                )}
                            </div>

                            <div className="flex justify-end gap-3 border-t border-white/10 px-6 py-4">
                                <button
                                    type="button"
                                    onClick={() => setShowFormulaCountryModal(false)}
                                    disabled={formulaUpdating}
                                    className="rounded-lg border border-white/10 px-4 py-2 text-sm font-medium text-white/80 transition hover:bg-white/[0.07] hover:text-white disabled:opacity-60"
                                >
                                    Cancel
                                </button>

                                <button
                                    type="button"
                                    onClick={() => handleFormulaUpdate(selectedFormulaCountry)}
                                    disabled={formulaUpdating || !selectedFormulaCountry}
                                    className="rounded-lg bg-[#31d9e5] px-4 py-2 text-sm font-semibold text-[#303247] transition hover:bg-[#28cbd6] disabled:cursor-not-allowed disabled:opacity-60"
                                >
                                    {formulaUpdating
                                        ? "Updating..."
                                        : selectedFormulaCountry
                                            ? `Update ${selectedFormulaCountry.toUpperCase()}`
                                            : "Update"}
                                </button>
                            </div>
                        </div>
                    </div>
                )
            }
        </div >
    );
}
