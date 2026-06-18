"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import Image from "next/image";
import { usePathname, useRouter } from "next/navigation";
import {
    LayoutDashboard,
    Users,
    RefreshCw,
    LogOut,
    Menu,
    X,
    Shield,
    Settings,
    KeyRound,
    ChevronLeft,
    ChevronRight,
    Building2 , 
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


    const publicSuperAdminRoutes = [
        "/superadmin/CDPAdminConsole",
        "/superadmin/SuperadminResetPassword",
    ];

    const isPublicRoute = publicSuperAdminRoutes.includes(pathname);

    useEffect(() => {
        if (isPublicRoute) return;

        const token = localStorage.getItem("superadmin_token");

        if (!token) {
            router.push("/superadmin/CDPAdminConsole");
        }
    }, [isPublicRoute, router]);

    if (isPublicRoute) {
        return <>{children}</>;
    }

    const navItems = [
        {
            label: "Dashboard",
            href: "/superadmin/SuperAdminDashboard",
            icon: LayoutDashboard,
        },
        {
            label: "Admins",
            href: "/superadmin/Admins",
            icon: Users,
        },
        {
            label: "Brands",
            href: "/superadmin/Brands",
            icon: Building2  ,
        },
    ];



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
                        <div className="mb-5 flex items-center gap-3 rounded-2xl border border-white/10 bg-white/[0.06] px-4 py-3 shadow-sm">
                            <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-[#31d9e5]/15 text-[#31d9e5]">
                                <Shield size={20} />
                            </span>

                            <div>
                                <p className="text-sm font-semibold text-white">Super Admin</p>
                                <p className="text-xs text-white/60">Control Panel</p>
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
                                                Settings
                                            </p>
                                            <p className="text-xs text-white/55">
                                                Super Admin controls
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
                                                    setSelectedFormulaCountry("uk");
                                                    setShowFormulaCountryModal(true);
                                                }}
                                                disabled={formulaUpdating}
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

            {showFormulaCountryModal && (
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
                            <label
                                className={`flex cursor-pointer items-center justify-between rounded-xl border px-4 py-3 transition ${selectedFormulaCountry === "uk"
                                    ? "border-[#31d9e5] bg-[#31d9e5]/10"
                                    : "border-white/10 bg-white/[0.04] hover:bg-white/[0.07]"
                                    }`}
                            >
                                <div>
                                    <p className="font-medium text-white">UK</p>
                                    <p className="text-xs text-white/55">
                                        Marketplace: A1F83G8C2ARO7P
                                    </p>
                                </div>

                                <input
                                    type="radio"
                                    name="formula_country"
                                    value="uk"
                                    checked={selectedFormulaCountry === "uk"}
                                    onChange={() => setSelectedFormulaCountry("uk")}
                                    className="h-4 w-4 accent-[#31d9e5]"
                                />
                            </label>

                            <label
                                className={`flex cursor-pointer items-center justify-between rounded-xl border px-4 py-3 transition ${selectedFormulaCountry === "us"
                                    ? "border-[#31d9e5] bg-[#31d9e5]/10"
                                    : "border-white/10 bg-white/[0.04] hover:bg-white/[0.07]"
                                    }`}
                            >
                                <div>
                                    <p className="font-medium text-white">US</p>
                                    <p className="text-xs text-white/55">
                                        Marketplace: ATVPDKIKX0DER
                                    </p>
                                </div>

                                <input
                                    type="radio"
                                    name="formula_country"
                                    value="us"
                                    checked={selectedFormulaCountry === "us"}
                                    onChange={() => setSelectedFormulaCountry("us")}
                                    className="h-4 w-4 accent-[#31d9e5]"
                                />
                            </label>
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
                                disabled={formulaUpdating}
                                className="rounded-lg bg-[#31d9e5] px-4 py-2 text-sm font-semibold text-[#303247] transition hover:bg-[#28cbd6] disabled:cursor-not-allowed disabled:opacity-60"
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