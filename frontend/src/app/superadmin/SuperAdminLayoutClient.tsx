"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import Image from "next/image";
import { usePathname, useRouter } from "next/navigation";
import {
    LayoutDashboard,
    Users,
    KeyRound,
    RefreshCw,
    LogOut,
    Menu,
    X,
    Shield,
    Settings,
    ChevronLeft,
    ChevronRight,
} from "lucide-react";
import { toast } from "sonner";

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
    const [selectedFormulaCountry, setSelectedFormulaCountry] =
        useState<"uk" | "us">("uk");



    const isLoginPage = pathname === "/superadmin/CDPAdminConsole";

    const navItems = [
        {
            label: "Dashboard",
            href: "/superadmin/SuperAdminDashboard",
            icon: LayoutDashboard,
        },
        {
            label: "User Brand Registry",
            href: "/superadmin/SuperAdminDashboard",
            icon: Users,
        },
        {
            label: "Change Password",
            href: "/superadmin/Superadminchangepassword",
            icon: KeyRound,
        },
    ];

    useEffect(() => {
        if (isLoginPage) return;

        const token = localStorage.getItem("superadmin_token");

        if (!token) {
            router.push("/superadmin/CDPAdminConsole");
        }
    }, [isLoginPage, router]);

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

    const handleFormulaUpdate = async (country: "uk" | "us") => {
        const token = localStorage.getItem("superadmin_token");

        if (!token) {
            toast.error("Superadmin token not found. Please login again.");
            router.push("/superadmin/CDPAdminConsole");
            return;
        }

        const marketplaces = {
            uk: {
                country: "uk",
                marketplace_id: "A1F83G8C2ARO7P",
            },
            us: {
                country: "us",
                marketplace_id: "ATVPDKIKX0DER",
            },
        };

        const selectedMarketplace = marketplaces[country];

        try {
            setFormulaUpdating(true);
            setShowFormulaCountryModal(false);

            const transactionStatus =
                selectedMarketplace.country === "us" ? "RELEASED,DEFERRED" : "RELEASED";

            const url =
                `${process.env.NEXT_PUBLIC_API_BASE_URL}/amazon_api/formula_update` +
                `?country=${selectedMarketplace.country}` +
                `&marketplace_id=${selectedMarketplace.marketplace_id}` +
                `&store_in_db=true` +
                `&run_upload_pipeline=true` +
                `&transaction_status=${encodeURIComponent(transactionStatus)}`;

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
                    `Formula update failed for ${selectedMarketplace.country.toUpperCase()}`
                );
            }

            toast.success(
                `Formula update completed for ${selectedMarketplace.country.toUpperCase()}`
            );
        } catch (error) {
            const msg = error instanceof Error ? error.message : "Formula update failed";
            toast.error(msg);
        } finally {
            setFormulaUpdating(false);
        }
    };

    if (isLoginPage) {
        return <>{children}</>;
    }

    return (
        <div className="min-h-screen bg-gradient-to-br from-emerald-50 to-slate-100">
            {sidebarOpen && (
                <div
                    className="fixed inset-0 z-40 bg-black/40 lg:hidden"
                    onClick={() => setSidebarOpen(false)}
                />
            )}

            <aside
                className={`fixed left-0 top-0 z-50 h-screen bg-[#37455F] text-white shadow-2xl transition-all duration-300 overflow-hidden lg:translate-x-0 ${sidebarCollapsed ? "lg:w-0 lg:border-0 lg:shadow-none" : "lg:w-72"
                    } w-72 ${sidebarOpen ? "translate-x-0" : "-translate-x-full"
                    }`}
            >
                <div className="flex h-20 items-center justify-between border-b border-white/10 px-5">
                    <Image
                        width={180}
                        height={40}
                        src="/images/auth/Phormula.png"
                        alt="Phormula"
                        priority
                        className="w-[160px] h-auto"
                    />

                    <button
                        type="button"
                        onClick={() => setSidebarOpen(false)}
                        className="rounded-lg p-2 text-white hover:bg-white/10 lg:hidden"
                        aria-label="Close sidebar"
                    >
                        <X size={22} />
                    </button>
                </div>

                <div className="px-4 py-5">
                    <div className="mb-5 flex items-center gap-3 rounded-xl bg-white/10 px-4 py-3">
                        <Shield size={20} />

                        <div>
                            <p className="text-sm font-semibold">Super Admin</p>
                            <p className="text-xs text-white/70">Control Panel</p>
                        </div>
                    </div>

                    <nav className="space-y-2">
                        {navItems.map((item) => {
                            const Icon = item.icon;
                            const isActive = pathname === item.href;

                            return (
                                <Link
                                    key={item.label}
                                    href={item.href}
                                    onClick={() => setSidebarOpen(false)}
                                    className={`flex items-center gap-3 rounded-xl px-4 py-3 text-sm font-medium transition ${isActive
                                        ? "bg-[#5EA68E] text-white"
                                        : "text-white/80 hover:bg-white/10 hover:text-white"
                                        }`}
                                >
                                    <Icon size={18} />
                                    {item.label}
                                </Link>
                            );
                        })}

                        <button
                            type="button"
                            onClick={() => {
                                setSelectedFormulaCountry("uk");
                                setShowFormulaCountryModal(true);
                                setSidebarOpen(false);
                            }}
                            disabled={formulaUpdating}
                            className="flex w-full items-center gap-3 rounded-xl px-4 py-3 text-left text-sm font-medium text-white/80 transition hover:bg-white/10 hover:text-white disabled:cursor-not-allowed disabled:opacity-60"
                        >
                            <RefreshCw size={18} />
                            <span>{formulaUpdating ? "Updating Formula..." : "Formula Update"}</span>
                        </button>
                    </nav>
                </div>

                <div className="absolute bottom-0 left-0 w-full border-t border-white/10 p-4">
                    <button
                        type="button"
                        onClick={handleLogout}
                        className="flex w-full items-center gap-3 rounded-xl px-4 py-3 text-sm font-medium text-white/80 transition hover:bg-red-500/20 hover:text-white"
                    >
                        <LogOut size={18} />
                        Logout
                    </button>
                </div>
            </aside>

            <div
                className={`transition-all duration-300 ${sidebarCollapsed ? "lg:pl-0" : "lg:pl-72"
                    }`}
            >
                <header className="sticky top-0 z-30 border-b border-slate-200 bg-white/90 backdrop-blur">
                    <div className="flex h-20 items-center justify-between gap-4 px-4 sm:px-6">
                        <div className="flex items-center gap-4">
                            <button
                                type="button"
                                onClick={() => setSidebarOpen(true)}
                                className="rounded-lg border border-slate-200 bg-white p-2 text-slate-700 shadow-sm lg:hidden"
                                aria-label="Open sidebar"
                            >
                                <Menu size={22} />
                            </button>

                            <button
                                type="button"
                                onClick={() => setSidebarCollapsed((prev) => !prev)}
                                className="hidden rounded-lg border border-slate-200 bg-white p-2 text-slate-700 shadow-sm hover:bg-slate-50 lg:inline-flex"
                                aria-label={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
                            >
                                {sidebarCollapsed ? <ChevronRight size={22} /> : <ChevronLeft size={22} />}
                            </button>

                            <div>
                                <h1 className="truncate text-lg sm:text-2xl font-semibold tracking-tight text-[#414042]">
                                    Super Admin Dashboard
                                </h1>
                                <p className="hidden text-xs text-slate-500 sm:block">
                                    Manage users, brands, companies and marketplaces
                                </p>
                            </div>
                        </div>

                        <div className="relative">
                            <button
                                type="button"
                                onClick={() => setShowSettings((s) => !s)}
                                className="inline-flex items-center justify-center rounded-lg bg-[#37455F] p-3 text-[#f8edce] shadow hover:opacity-90"
                                aria-label="Open settings"
                            >
                                <Settings size={20} />
                            </button>

                            {showSettings && (
                                <div className="absolute right-0 mt-2 w-56 origin-top-right overflow-hidden rounded-lg bg-white text-slate-800 shadow-2xl ring-1 ring-black/5">
                                    <button
                                        onClick={() => {
                                            setShowSettings(false);
                                            router.push("/superadmin/Superadminchangepassword");
                                        }}
                                        className="w-full border-b px-4 py-3 text-left hover:bg-slate-50"
                                    >
                                        Change Password
                                    </button>

                                    <button
                                        onClick={() => {
                                            setShowSettings(false);
                                            setSelectedFormulaCountry("uk");
                                            setShowFormulaCountryModal(true);
                                        }}
                                        disabled={formulaUpdating}
                                        className="w-full border-b px-4 py-3 text-left hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
                                    >
                                        {formulaUpdating ? "Updating Formula..." : "Formula Update"}
                                    </button>

                                    <button
                                        onClick={handleLogout}
                                        className="w-full px-4 py-3 text-left hover:bg-slate-50"
                                    >
                                        Logout
                                    </button>
                                </div>
                            )}
                        </div>
                    </div>
                </header>

                <main className="p-4 sm:p-6">{children}</main>
            </div>

            {showFormulaCountryModal && (
                <div className="fixed inset-0 z-[60] flex items-center justify-center bg-black/40 px-4">
                    <div className="w-full max-w-md rounded-xl bg-white shadow-2xl">
                        <div className="border-b px-6 py-4">
                            <h2 className="text-lg font-semibold text-slate-800">
                                Formula Update
                            </h2>
                            <p className="mt-1 text-sm text-slate-500">
                                Select country for formula update
                            </p>
                        </div>

                        <div className="space-y-3 px-6 py-5">
                            <label
                                className={`flex cursor-pointer items-center justify-between rounded-lg border px-4 py-3 transition ${selectedFormulaCountry === "uk"
                                    ? "border-[#5EA68E] bg-emerald-50"
                                    : "border-slate-200 bg-white"
                                    }`}
                            >
                                <div>
                                    <p className="font-medium text-slate-800">UK</p>
                                    <p className="text-xs text-slate-500">
                                        Marketplace: A1F83G8C2ARO7P
                                    </p>
                                </div>

                                <input
                                    type="radio"
                                    name="formula_country"
                                    value="uk"
                                    checked={selectedFormulaCountry === "uk"}
                                    onChange={() => setSelectedFormulaCountry("uk")}
                                    className="h-4 w-4 accent-[#5EA68E]"
                                />
                            </label>

                            <label
                                className={`flex cursor-pointer items-center justify-between rounded-lg border px-4 py-3 transition ${selectedFormulaCountry === "us"
                                    ? "border-[#5EA68E] bg-emerald-50"
                                    : "border-slate-200 bg-white"
                                    }`}
                            >
                                <div>
                                    <p className="font-medium text-slate-800">US</p>
                                    <p className="text-xs text-slate-500">
                                        Marketplace: ATVPDKIKX0DER
                                    </p>
                                </div>

                                <input
                                    type="radio"
                                    name="formula_country"
                                    value="us"
                                    checked={selectedFormulaCountry === "us"}
                                    onChange={() => setSelectedFormulaCountry("us")}
                                    className="h-4 w-4 accent-[#5EA68E]"
                                />
                            </label>
                        </div>

                        <div className="flex justify-end gap-3 border-t px-6 py-4">
                            <button
                                type="button"
                                onClick={() => setShowFormulaCountryModal(false)}
                                disabled={formulaUpdating}
                                className="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-60"
                            >
                                Cancel
                            </button>

                            <button
                                type="button"
                                onClick={() => handleFormulaUpdate(selectedFormulaCountry)}
                                disabled={formulaUpdating}
                                className="rounded-lg bg-[#37455F] px-4 py-2 text-sm font-semibold text-[#f8edce] hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
                            >
                                {formulaUpdating
                                    ? "Updating..."
                                    : `Update ${selectedFormulaCountry.toUpperCase()}`}
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}