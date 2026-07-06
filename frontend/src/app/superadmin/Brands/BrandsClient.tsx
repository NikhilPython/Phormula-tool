"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { ArrowLeft, Eye, Trash2 } from "lucide-react";
import { FaSearch } from "react-icons/fa";
import { toast } from "sonner";

import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import SuperAdminUsersTable from "@/components/admin/table/SuperAdminUsersTable";
import Loader from "@/components/loader/Loader";

const MIN_LOADER_MS = 3000;

type BrandRow = {
    id: number | string;
    email: string;
    brand_name: string;
    name: string;
    company_name: string;
    country?: string;
    marketplace_id?: string;
    status?: string | boolean;
    address?: {
        building?: string;
        city?: string;
        country?: string;
        state?: string;
        zipcode?: string;
    };
};

type DashboardResponse = {
    users?: BrandRow[];
    message?: string;
};

const BrandsPage = () => {
    const router = useRouter();

    const [brands, setBrands] = useState<BrandRow[]>([]);
    const [searchQuery, setSearchQuery] = useState<string>("");
    const [loading, setLoading] = useState<boolean>(false);
    const [actionLoading, setActionLoading] = useState<Record<string, boolean>>(
        {}
    );

    const finishLoadingWithMinDelay = (startedAt: number) => {
        const elapsed = Date.now() - startedAt;
        const remaining = Math.max(0, MIN_LOADER_MS - elapsed);
        window.setTimeout(() => setLoading(false), remaining);
    };

    const normalizeStatus = (status?: string | boolean) => {
        if (status === true) return "active";
        if (status === false) return "inactive";

        const s = String(status ?? "").trim().toLowerCase();

        if (s === "inactive" || s === "disabled" || s === "false") {
            return "inactive";
        }

        return "active";
    };

    const fetchBrands = useCallback(async () => {
        const startedAt = Date.now();
        setLoading(true);

        try {
            const token = localStorage.getItem("superadmin_token");

            if (!token) {
                toast.error("No authentication token found");
                router.push("/superadmin/CDPAdminConsole");
                return;
            }

            const response = await fetch(
                `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/dashboard?authenticated_user=superadmin`,
                {
                    method: "GET",
                    headers: {
                        Authorization: `Bearer ${token}`,
                        "Content-Type": "application/json",
                    },
                }
            );

            const data = (await response.json()) as DashboardResponse;

            if (!response.ok) {
                if (response.status === 401) {
                    localStorage.removeItem("superadmin_token");
                    router.push("/superadmin/CDPAdminConsole");
                    return;
                }

                throw new Error(data.message || `HTTP error! status: ${response.status}`);
            }

            setBrands(data.users || []);
        } catch (error) {
            const message =
                error instanceof Error ? error.message : "Failed to load brands.";

            toast.error(message);
            setBrands([]);
        } finally {
            finishLoadingWithMinDelay(startedAt);
        }
    }, [router]);

    useEffect(() => {
        fetchBrands();
    }, [fetchBrands]);

    const handleViewBrand = (email: string) => {
        router.push(`/superadmin/ViewUserPage/${encodeURIComponent(email)}`);
    };

    const handleToggleStatus = async (brand: BrandRow) => {
        const emailKey = brand.email;
        const currentStatus = normalizeStatus(brand.status);
        const nextStatus = currentStatus === "active" ? false : true;

        try {
            setActionLoading((prev) => ({ ...prev, [emailKey]: true }));

            const token = localStorage.getItem("superadmin_token");

            if (!token) {
                toast.error("No authentication token found");
                router.push("/superadmin/CDPAdminConsole");
                return;
            }

            const response = await fetch(
                `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/dashboard/update_user_status`,
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        Authorization: `Bearer ${token}`,
                    },
                    body: JSON.stringify({
                        user_id: brand.id,
                        status: nextStatus,
                    }),
                }
            );

            const data = await response.json().catch(() => ({}));

            if (!response.ok) {
                throw new Error(data?.message || "Failed to update status");
            }

            const updatedStatus =
                data?.status === true || String(data?.status).toLowerCase() === "true"
                    ? "active"
                    : "inactive";

            setBrands((prev) =>
                prev.map((item) =>
                    String(item.id) === String(brand.id)
                        ? {
                            ...item,
                            status: updatedStatus,
                        }
                        : item
                )
            );

            toast.success(
                `Brand ${updatedStatus === "active" ? "enabled" : "disabled"} successfully`
            );
        } catch (error) {
            const message =
                error instanceof Error ? error.message : "Failed to update status";

            toast.error(message);
        } finally {
            setActionLoading((prev) => ({ ...prev, [emailKey]: false }));
        }
    };

    const confirmToggleStatus = (brand: BrandRow) => {
        const currentStatus = normalizeStatus(brand.status);
        const nextLabel = currentStatus === "active" ? "disable" : "enable";

        toast.custom(
            (toastId) => (
                <div className="w-[360px] rounded-xl border border-white/10 bg-[#37384f] p-4 text-white shadow-[0_24px_55px_rgba(20,22,45,0.45)]">
                    <div>
                        <h3 className="text-sm font-semibold text-white">
                            Confirm status change
                        </h3>

                        <p className="mt-1 text-sm text-white/60">
                            Are you sure you want to {nextLabel}{" "}
                            <span className="font-medium text-white">{brand.email}</span>?
                        </p>
                    </div>

                    <div className="mt-4 flex justify-end gap-2">
                        <button
                            type="button"
                            onClick={() => toast.dismiss(toastId)}
                            className="rounded-lg border border-white/10 bg-white/[0.06] px-3 py-2 text-xs font-semibold text-white/75 transition hover:bg-white/10 hover:text-white"
                        >
                            Cancel
                        </button>

                        <button
                            type="button"
                            onClick={() => {
                                toast.dismiss(toastId);
                                handleToggleStatus(brand);
                            }}
                            className={`rounded-lg px-3 py-2 text-xs font-semibold text-white transition ${currentStatus === "active"
                                ? "bg-red-600 hover:bg-red-700"
                                : "bg-emerald-600 hover:bg-emerald-700"
                                }`}
                        >
                            Yes, {nextLabel}
                        </button>
                    </div>
                </div>
            ),
            {
                duration: Infinity,
                position: "top-center",
            }
        );
    };

    const handleDeleteBrand = async (email: string) => {
        try {
            setActionLoading((prev) => ({ ...prev, [email]: true }));

            const token = localStorage.getItem("superadmin_token");

            if (!token) {
                toast.error("No authentication token found");
                router.push("/superadmin/CDPAdminConsole");
                return;
            }

            const response = await fetch(
                `${process.env.NEXT_PUBLIC_API_BASE_URL}/delete_admin`,
                {
                    method: "DELETE",
                    headers: {
                        "Content-Type": "application/json",
                        Authorization: `Bearer ${token}`,
                    },
                    body: JSON.stringify({ email: email.trim() }),
                }
            );

            const data = await response.json().catch(() => ({}));

            if (!response.ok) {
                throw new Error(data?.message || "Failed to delete brand");
            }

            setBrands((prev) => prev.filter((brand) => brand.email !== email));

            toast.success("Brand deleted successfully");
        } catch (error) {
            const message =
                error instanceof Error
                    ? error.message
                    : "Network error. Please try again.";

            toast.error(message);
        } finally {
            setActionLoading((prev) => ({ ...prev, [email]: false }));
        }
    };

    const confirmDeleteBrand = (email: string) => {
        toast.custom(
            (toastId) => (
                <div className="w-[360px] rounded-xl border border-white/10 bg-[#37384f] p-4 text-white shadow-[0_24px_55px_rgba(20,22,45,0.45)]">
                    <div>
                        <h3 className="text-sm font-semibold text-white">Delete brand?</h3>

                        <p className="mt-1 text-sm text-white/60">
                            This will permanently delete{" "}
                            <span className="font-medium text-white">{email}</span>.
                        </p>
                    </div>

                    <div className="mt-4 flex justify-end gap-2">
                        <button
                            type="button"
                            onClick={() => toast.dismiss(toastId)}
                            className="rounded-lg border border-white/10 bg-white/[0.06] px-3 py-2 text-xs font-semibold text-white/75 transition hover:bg-white/10 hover:text-white"
                        >
                            Cancel
                        </button>

                        <button
                            type="button"
                            onClick={() => {
                                toast.dismiss(toastId);
                                handleDeleteBrand(email);
                            }}
                            className="rounded-lg bg-red-600 px-3 py-2 text-xs font-semibold text-white transition hover:bg-red-700"
                        >
                            Yes, delete
                        </button>
                    </div>
                </div>
            ),
            {
                duration: Infinity,
                position: "top-center",
            }
        );
    };

    const brandRows = useMemo(() => {
        return brands.filter((brand) => {
            return Boolean(
                brand.brand_name ||
                brand.company_name ||
                brand.marketplace_id ||
                brand.email
            );
        });
    }, [brands]);

    const filteredBrandRows = useMemo(() => {
        const query = searchQuery.trim().toLowerCase();

        if (!query) return brandRows;

        return brandRows.filter((brand) => {
            return (
                (brand.brand_name || "").toLowerCase().includes(query) ||
                (brand.company_name || "").toLowerCase().includes(query) ||
                (brand.address?.country || "").toLowerCase().includes(query) ||
                (brand.country || "").toLowerCase().includes(query) ||
                (brand.name || "").toLowerCase().includes(query) ||
                (brand.email || "").toLowerCase().includes(query) ||
                (brand.marketplace_id || "").toLowerCase().includes(query)
            );
        });
    }, [brandRows, searchQuery]);

    return (
        <div className="w-full">
            <div className="space-y-6">
                {/* Page Heading */}
                <div className="rounded-2xl border border-white/10 bg-[#484962] px-5 py-5 text-white shadow-[0_18px_40px_rgba(20,22,45,0.25)]">
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
                            className="mt-1 inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-white/10 bg-white/[0.06] text-white/80 shadow-sm transition hover:bg-white/10 hover:text-[#31d9e5]"
                            aria-label="Go back"
                            title="Back"
                        >
                            <ArrowLeft size={17} />
                        </button>

                        <div className="flex flex-col leading-tight">
                            <PageBreadcrumb
                                pageTitle="Brands"
                                variant="superadmin"
                                align="left"
                                textSize="2xl"
                            />

                            <p className="mt-2 text-sm text-white/60">
                                View all brand details, status and marketplace information.
                            </p>
                        </div>
                    </div>
                </div>

                <section className="space-y-3">
                    <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
                        <PageBreadcrumb
                            pageTitle="Brands"
                            variant="superadmin"
                            align="left"
                            textSize="2xl"
                        />

                        <div className="relative w-full md:w-[360px]">
                            <input
                                type="text"
                                value={searchQuery}
                                onChange={(e) => setSearchQuery(e.target.value)}
                                placeholder="Search by Brand, Company, Name or Email..."
                                className="h-[42px] w-full rounded-xl border border-white/10 bg-white/[0.06] px-4 pr-11 text-sm text-white shadow-sm outline-none placeholder:text-white/40 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15"
                            />

                            <span className="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-[#31d9e5]">
                                <FaSearch />
                            </span>
                        </div>
                    </div>

                    {loading ? (
                        <Loader fullscreen backgroundClass="bg-[#37384f]/80" />
                    ) : (
                        <SuperAdminUsersTable
                            data={filteredBrandRows}
                            minWidth="950px"
                            emptyTitle={searchQuery.trim() ? "No matching brands found" : "No brands found"}
                            emptyDescription={
                                searchQuery.trim()
                                    ? "Try searching by another brand, company, name, email, country or marketplace."
                                    : "Brands added to the system will appear here."
                            }
                            columns={[
                                {
                                    key: "brand_name",
                                    label: "Brand Name",
                                    cellClassName: "font-semibold text-white",
                                    render: (brand) => brand.brand_name || "Not added",
                                },
                                {
                                    key: "company_name",
                                    label: "Company",
                                    render: (brand) => brand.company_name || "Not added",
                                },
                                {
                                    key: "native_country",
                                    label: "Native Country",
                                    render: (brand) => brand.address?.country?.trim() || "Not added",
                                },
                                {
                                    key: "marketplace",
                                    label: "Marketplace",
                                    render: (brand) => (
                                        <span className="inline-flex rounded-full border border-white/10 bg-white/[0.06] px-3 py-1 text-xs font-medium capitalize text-white/75">
                                            {brand.country || "Not added"}
                                        </span>
                                    ),
                                },
                                {
                                    key: "name",
                                    label: "Name",
                                    render: (brand) => brand.name || "Not added",
                                },
                                {
                                    key: "email",
                                    label: "Email",
                                    render: (brand) => (
                                        <span className="break-all">{brand.email || "Not added"}</span>
                                    ),
                                },
                                {
                                    key: "actions",
                                    label: "Actions",
                                    render: (brand) => {
                                        const isBusy = !!actionLoading[brand.email];

                                        return (
                                            <div className="flex items-center justify-center gap-2">
                                                <button
                                                    type="button"
                                                    onClick={() => handleViewBrand(brand.email)}
                                                    className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-white/10 bg-white/[0.06] text-white/75 shadow-sm transition hover:border-[#31d9e5]/40 hover:bg-[#31d9e5]/10 hover:text-[#31d9e5]"
                                                    title="View brand"
                                                    aria-label={`View ${brand.email}`}
                                                >
                                                    <Eye size={17} />
                                                </button>

                                                <button
                                                    type="button"
                                                    onClick={() => confirmDeleteBrand(brand.email)}
                                                    disabled={isBusy}
                                                    className={`inline-flex h-9 w-9 items-center justify-center rounded-lg border border-red-300/25 bg-red-500/10 text-red-200 shadow-sm transition hover:bg-red-500/20 hover:text-red-100 ${isBusy ? "cursor-not-allowed opacity-60" : ""
                                                        }`}
                                                    title="Delete brand"
                                                    aria-label={`Delete ${brand.email}`}
                                                >
                                                    <Trash2 size={16} />
                                                </button>
                                            </div>
                                        );
                                    },
                                },
                            ]}
                        />
                    )}
                </section>
            </div>
        </div>
    );
};

export default BrandsPage;