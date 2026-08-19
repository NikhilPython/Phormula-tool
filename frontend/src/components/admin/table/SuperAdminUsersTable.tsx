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
    "Name",
    "Email",
    "Status",
    "Actions",
];

const getConnectedCountries = (user: UserRow) => {
    const countries = Array.isArray(user.countries)
        ? user.countries
        : user.country?.split(",") || [];

    return countries
        .map((country) => country.trim())
        .filter(Boolean);
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

    if (isGenericTable) {
        return (
            <div className="overflow-hidden rounded-2xl border border-white/10 bg-[#484962] shadow-[0_18px_40px_rgba(20,22,45,0.22)]">
                <div className="max-w-full overflow-x-auto">
                    <div style={{ minWidth: minWidth || "900px" }}>
                        <Table>
                            <TableHeader className="border-b border-white/10 bg-white/[0.04]">
                                <TableRow>
                                    {columns.map((column) => (
                                        <TableCell
                                            key={column.key}
                                            isHeader
                                            className={`px-5 py-4 text-center text-theme-xs font-semibold uppercase tracking-wide text-white/55 ${column.headerClassName || ""
                                                }`}
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
                                            className="transition hover:bg-white/[0.04]"
                                        >
                                            {columns.map((column) => (
                                                <TableCell
                                                    key={column.key}
                                                    className={`px-5 py-4 text-center text-theme-sm text-white/65 ${column.cellClassName || ""
                                                        }`}
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
                            <div className="flex min-h-[240px] w-full items-center justify-center px-5 py-12 text-center">
                                <div className="mx-auto flex max-w-sm flex-col items-center justify-center">
                                    <div className="flex h-14 w-14 items-center justify-center rounded-full border border-white/10 bg-[#31d9e5]/10 text-[#31d9e5]">
                                        <FaSearch className="text-lg" />
                                    </div>

                                    <h3 className="mt-5 text-base font-semibold text-white">
                                        {emptyTitle}
                                    </h3>

                                    <p className="mt-2 text-sm leading-6 text-white/60">
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

    return (
        <div className="overflow-hidden rounded-2xl border border-white/10 bg-[#484962] shadow-[0_18px_40px_rgba(20,22,45,0.22)]">
            <div className="max-w-full overflow-x-auto">
                <div className="min-w-[1150px]">
                    <Table>
                        <TableHeader className="border-b border-white/10 bg-white/[0.04]">
                            <TableRow>
                                {defaultTableColumns.map((column) => (
                                    <TableCell
                                        key={column}
                                        isHeader
                                        className="px-5 py-4 text-start text-theme-xs font-semibold uppercase tracking-wide text-white/55"
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

                                    const currentCountry = getConnectedCountryLabel(user) || "Not added";
                                    const isBusy = !!actionLoading[user.email];

                                    return (
                                        <TableRow
                                            key={user.id}
                                            className="transition hover:bg-white/[0.04]"
                                        >
                                            <TableCell className="px-5 py-4 text-start sm:px-6">
                                                <span className="block text-theme-sm font-semibold text-white">
                                                    {user.brand_name || "Not added"}
                                                </span>
                                            </TableCell>

                                            <TableCell className="px-4 py-4 text-start text-theme-sm text-white/65">
                                                {user.company_name || "Not added"}
                                            </TableCell>

                                            <TableCell className="px-4 py-4 text-start text-theme-sm text-white/65">
                                                {nativeCountry}
                                            </TableCell>

                                            <TableCell className="px-4 py-4 text-start text-theme-sm text-white/65">
                                                <span className="inline-flex rounded-full border border-white/10 bg-white/[0.06] px-3 py-1 text-xs font-medium capitalize text-white/75">
                                                    {currentCountry}
                                                </span>
                                            </TableCell>

                                            <TableCell className="px-4 py-4 text-start text-theme-sm text-white/65">
                                                {user.name || "Not added"}
                                            </TableCell>

                                            <TableCell className="px-4 py-4 text-start text-theme-sm text-white/65">
                                                <span className="break-all">
                                                    {user.email || "Not added"}
                                                </span>
                                            </TableCell>

                                            <TableCell className="px-4 py-4 text-start">
                                                <button
                                                    type="button"
                                                    onClick={() => onToggleStatus?.(user)}
                                                    disabled={isBusy}
                                                    className={`relative inline-flex h-6 w-11 items-center rounded-full transition ${userStatus === "active"
                                                        ? "bg-[#31d9e5]"
                                                        : "bg-white/20"
                                                        } ${isBusy ? "cursor-not-allowed opacity-60" : ""}`}
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
                                                        className={`inline-block h-4 w-4 rounded-full bg-white shadow transition ${userStatus === "active"
                                                            ? "translate-x-6"
                                                            : "translate-x-1"
                                                            }`}
                                                    />
                                                </button>
                                            </TableCell>

                                            <TableCell className="px-4 py-4 text-start">
                                                <div className="flex items-center gap-2">
                                                    <button
                                                        type="button"
                                                        onClick={() => onViewUser?.(user.email)}
                                                        className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-white/10 bg-white/[0.06] text-white/75 shadow-sm transition hover:border-[#31d9e5]/40 hover:bg-[#31d9e5]/10 hover:text-[#31d9e5]"
                                                        title="View user"
                                                        aria-label={`View ${user.email}`}
                                                    >
                                                        <Eye size={17} />
                                                    </button>

                                                    <button
                                                        type="button"
                                                        onClick={() => onDeleteUser?.(user.email)}
                                                        disabled={isBusy}
                                                        className={`inline-flex h-9 w-9 items-center justify-center rounded-lg border border-red-300/25 bg-red-500/10 text-red-200 shadow-sm transition hover:bg-red-500/20 hover:text-red-100 ${isBusy ? "cursor-not-allowed opacity-60" : ""
                                                            }`}
                                                        title="Delete admin"
                                                        aria-label={`Delete ${user.email}`}
                                                    >
                                                        <Trash2 size={16} />
                                                    </button>
                                                </div>
                                            </TableCell>
                                        </TableRow>
                                    );
                                })
                            ) : (
                                <TableRow>
                                    <TableCell
                                        colSpan={8}
                                        className="px-5 py-20 text-center"
                                    >
                                        <div className="mx-auto flex max-w-sm flex-col items-center justify-center">
                                            <div className="flex h-14 w-14 items-center justify-center rounded-full border border-white/10 bg-[#31d9e5]/10 text-[#31d9e5]">
                                                <FaSearch className="text-lg" />
                                            </div>

                                            <h3 className="mt-5 text-base font-semibold text-white">
                                                {emptyTitle}
                                            </h3>

                                            <p className="mt-2 text-sm leading-6 text-white/60">
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
