// components/admin/SuperAdminUsersTable.tsx
"use client";

import React from "react";
import { Eye, Trash2 } from "lucide-react";
import { FaSearch } from "react-icons/fa";

import {
    Table,
    TableBody,
    TableCell,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { getMarketplaceDisplays } from "@/lib/utils/amazonMarketplaces";

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
    address?: {
        building?: string;
        city?: string;
        country?: string;
        state?: string;
        zipcode?: string;
    };
};

export type SuperAdminTableColumn<T> = {
    key: string;
    label: string;
    headerClassName?: string;
    cellClassName?: string;
    render: (row: T, index: number) => React.ReactNode;
};

type SuperAdminUsersTableProps<T = UserRow> = {
    users?: UserRow[];
    actionLoading?: Record<string, boolean>;
    normalizeStatus?: (status?: string | boolean) => string;
    onToggleStatus?: (user: UserRow) => void;
    onDeleteUser?: (email: string) => void;
    onViewUser?: (email: string) => void;

    columns?: SuperAdminTableColumn<T>[];
    data?: T[];
    minWidth?: string;
    emptyTitle?: string;
    emptyDescription?: string;
};

const defaultTableColumns = [
    "Brand Name",
    "Company",
    "Native Country",
    "Current Country",
    "Country",
    "Name",
    "Email",
    "Status",
    "Actions",
];

const getConnectedCountries = (user: UserRow) => {
    const countries = Array.isArray(user.countries)
        ? user.countries
        : user.country?.split(",") || [];

    return countries.map((country) => country.trim()).filter(Boolean);
};

const getConnectedCountryLabel = (user: UserRow) =>
    getConnectedCountries(user)
        .map((country) => country.toUpperCase())
        .join(", ");

export default function SuperAdminUsersTable<T = UserRow>({
    users = [],
    actionLoading = {},
    normalizeStatus,
    onToggleStatus,
    onDeleteUser,
    onViewUser,

    columns,
    data,
    minWidth,
    emptyTitle = "No users found",
    emptyDescription = "Try changing your search keyword or status filter.",
}: SuperAdminUsersTableProps<T>) {
    const isGenericTable = !!columns && !!data;

    // Generic table
    if (isGenericTable) {
        return (
            <div className="relative w-full max-w-full overflow-hidden rounded-xl border border-white/10 bg-[#484962] shadow-[0_18px_40px_rgba(20,22,45,0.22)]">
                <div className="w-full max-w-full overflow-x-auto [-webkit-overflow-scrolling:touch]">
                    <div style={{ minWidth: minWidth || "900px" }}>
                        <Table>
                            <TableHeader className="border-b border-white/10 bg-white/[0.04]">
                                <TableRow>
                                    {columns.map((column) => (
                                        <TableCell
                                            key={column.key}
                                            isHeader
                                            className={`
  px-3 py-2
  text-center
  align-middle
  whitespace-nowrap
  text-xs
  2xl:text-sm
  font-bold
  capitalize
  tracking-wide
  text-white/55
  ${column.headerClassName || ""}
`}
                                        >
                                            {column.label}
                                        </TableCell>
                                    ))}
                                </TableRow>
                            </TableHeader>

                            {data.length > 0 && (
                                <TableBody className="divide-y divide-white/10">
                                    {data.map((row, rowIndex) => (
                                        <TableRow
                                            key={rowIndex}
                                            className="h-[40px] transition-colors hover:bg-white/[0.04]"
                                        >
                                            {columns.map((column) => (
                                                <TableCell
                                                    key={column.key}
                                                    className={`
                            px-3 py-2
                            text-center
                            align-middle
                            whitespace-nowrap
                            text-xs
                            2xl:text-sm
                            text-white/65
                            ${column.cellClassName || ""}
                          `}
                                                >
                                                    {column.render(row, rowIndex)}
                                                </TableCell>
                                            ))}
                                        </TableRow>
                                    ))}
                                </TableBody>
                            )}
                        </Table>

                        {data.length === 0 && (
                            <div className="flex min-h-[200px] w-full items-center justify-center px-3 py-8 text-center">
                                <div className="mx-auto flex max-w-sm flex-col items-center justify-center">
                                    <div className="flex h-12 w-12 items-center justify-center rounded-full border border-white/10 bg-[#31d9e5]/10 text-[#31d9e5]">
                                        <FaSearch className="text-base" />
                                    </div>

                                    <h3 className="mt-4 text-sm font-semibold text-white 2xl:text-base">
                                        {emptyTitle}
                                    </h3>

                                    <p className="mt-2 text-xs leading-5 text-white/60 2xl:text-sm">
                                        {emptyDescription}
                                    </p>
                                </div>
                            </div>
                        )}
                    </div>
                </div>
            </div>
        );
    }

    // Default users table
    return (
        <div className="relative w-full max-w-full overflow-hidden rounded-xl border border-white/10 bg-[#484962] shadow-[0_18px_40px_rgba(20,22,45,0.22)]">
            <div className="w-full max-w-full overflow-x-auto [-webkit-overflow-scrolling:touch]">
                <div className="min-w-[1150px]">
                    <Table>
                        <TableHeader className="border-b border-white/10 bg-white/[0.04]">
                            <TableRow>
                                {defaultTableColumns.map((column) => (
                                    <TableCell
                                        key={column}
                                        isHeader
                                        className="
  px-3 py-2
  text-center
  align-middle
  whitespace-nowrap
  text-xs
  2xl:text-sm
  font-bold
  capitalize
  tracking-wide
  text-white/55
"
                                    >
                                        {column}
                                    </TableCell>
                                ))}
                            </TableRow>
                        </TableHeader>

                        <TableBody className="divide-y divide-white/10">
                            {users.length > 0 ? (
                                users.map((user) => {
                                    const userStatus =
                                        normalizeStatus?.(user.status) || "active";

                                    const nativeCountry =
                                        user.address?.country?.trim() || "Not added";

                                    const currentCountry =
                                        getConnectedCountryLabel(user) || "Not added";

                                    const marketplaceIntegrations = getMarketplaceDisplays(
                                        user.country,
                                        user.marketplace_id,
                                        user.marketplace_ids
                                    );

                                    const hasMarketplace = marketplaceIntegrations.some(
                                        (marketplace) => marketplace.hasMarketplace
                                    );

                                    const isBusy = !!actionLoading[user.email];

                                    return (
                                        <TableRow
                                            key={user.id}
                                            className="h-[40px] transition-colors hover:bg-white/[0.04]"
                                        >
                                            <TableCell className="px-3 py-2 text-center align-middle text-xs text-white 2xl:text-sm">
                                                <span className="font-semibold">
                                                    {user.brand_name || "Not added"}
                                                </span>
                                            </TableCell>

                                            <TableCell className="px-3 py-2 text-center align-middle whitespace-nowrap text-xs text-white/65 2xl:text-sm">
                                                {user.company_name || "Not added"}
                                            </TableCell>

                                            <TableCell className="px-3 py-2 text-center align-middle whitespace-nowrap text-xs text-white/65 2xl:text-sm">
                                                {nativeCountry}
                                            </TableCell>

                                            <TableCell className="px-3 py-2 text-center align-middle text-xs text-white/65 2xl:text-sm">
                                                <span className="inline-flex rounded-full border border-white/10 bg-white/[0.06] px-2.5 py-1 text-[10px] font-medium capitalize text-white/75 2xl:text-xs">
                                                    {currentCountry}
                                                </span>
                                            </TableCell>

                                            <TableCell className="px-3 py-2 text-center align-middle text-xs text-white/65 2xl:text-sm">
                                                {hasMarketplace ? (
                                                    <div className="flex flex-wrap items-center justify-center gap-1.5">
                                                        {marketplaceIntegrations
                                                            .filter(
                                                                (marketplace) => marketplace.hasMarketplace
                                                            )
                                                            .map((marketplace) => (
                                                                <span
                                                                    key={`${marketplace.label}-${marketplace.marketplaceId}`}
                                                                    className="inline-flex rounded-full border border-white/10 bg-white/[0.06] px-2.5 py-1 text-[10px] font-medium text-white/75 2xl:text-xs"
                                                                >
                                                                    {marketplace.label}
                                                                </span>
                                                            ))}
                                                    </div>
                                                ) : (
                                                    <span className="inline-flex rounded-full border border-white/10 bg-white/[0.06] px-2.5 py-1 text-[10px] font-medium text-white/75 2xl:text-xs">
                                                        Not Added
                                                    </span>
                                                )}
                                            </TableCell>

                                            <TableCell className="px-3 py-2 text-center align-middle whitespace-nowrap text-xs text-white/65 2xl:text-sm">
                                                {user.name || "Not added"}
                                            </TableCell>

                                            <TableCell className="px-3 py-2 text-center align-middle text-xs text-white/65 2xl:text-sm">
                                                <span className="break-all">
                                                    {user.email || "Not added"}
                                                </span>
                                            </TableCell>

                                            <TableCell className="px-3 py-2 text-center align-middle">
                                                <button
                                                    type="button"
                                                    onClick={() => onToggleStatus?.(user)}
                                                    disabled={isBusy}
                                                    className={`
                            relative inline-flex h-6 w-11 items-center rounded-full transition
                            ${userStatus === "active"
                                                            ? "bg-[#31d9e5]"
                                                            : "bg-white/20"
                                                        }
                            ${isBusy
                                                            ? "cursor-not-allowed opacity-60"
                                                            : ""
                                                        }
                          `}
                                                    title={
                                                        userStatus === "active"
                                                            ? "Disable admin"
                                                            : "Enable admin"
                                                    }
                                                    aria-label={
                                                        userStatus === "active"
                                                            ? `Disable ${user.email}`
                                                            : `Enable ${user.email}`
                                                    }
                                                >
                                                    <span
                                                        className={`
                              inline-block h-4 w-4 rounded-full bg-white shadow transition
                              ${userStatus === "active"
                                                                ? "translate-x-6"
                                                                : "translate-x-1"
                                                            }
                            `}
                                                    />
                                                </button>
                                            </TableCell>

                                            <TableCell className="px-3 py-2 text-center align-middle">
                                                <div className="flex items-center justify-center gap-2">
                                                    <button
                                                        type="button"
                                                        onClick={() => onViewUser?.(user.email)}
                                                        className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/[0.06] text-white/75 shadow-sm transition hover:border-[#31d9e5]/40 hover:bg-[#31d9e5]/10 hover:text-[#31d9e5]"
                                                        title="View user"
                                                        aria-label={`View ${user.email}`}
                                                    >
                                                        <Eye size={15} />
                                                    </button>

                                                    <button
                                                        type="button"
                                                        onClick={() => onDeleteUser?.(user.email)}
                                                        disabled={isBusy}
                                                        className={`
                              inline-flex h-8 w-8 items-center justify-center
                              rounded-lg
                              border border-red-300/25
                              bg-red-500/10
                              text-red-200
                              shadow-sm
                              transition
                              hover:bg-red-500/20
                              hover:text-red-100
                              ${isBusy
                                                                ? "cursor-not-allowed opacity-60"
                                                                : ""
                                                            }
                            `}
                                                        title="Delete admin"
                                                        aria-label={`Delete ${user.email}`}
                                                    >
                                                        <Trash2 size={15} />
                                                    </button>
                                                </div>
                                            </TableCell>
                                        </TableRow>
                                    );
                                })
                            ) : (
                                <TableRow>
                                    <TableCell
                                        colSpan={9}
                                        className="px-3 py-12 text-center"
                                    >
                                        <div className="mx-auto flex max-w-sm flex-col items-center justify-center">
                                            <div className="flex h-12 w-12 items-center justify-center rounded-full border border-white/10 bg-[#31d9e5]/10 text-[#31d9e5]">
                                                <FaSearch className="text-base" />
                                            </div>

                                            <h3 className="mt-4 text-sm font-semibold text-white 2xl:text-base">
                                                {emptyTitle}
                                            </h3>

                                            <p className="mt-2 text-xs leading-5 text-white/60 2xl:text-sm">
                                                {emptyDescription}
                                            </p>
                                        </div>
                                    </TableCell>
                                </TableRow>
                            )}
                        </TableBody>
                    </Table>
                </div>
            </div>
        </div>
    );
}