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

type SuperAdminUsersTableProps = {
    users: UserRow[];
    actionLoading: Record<string, boolean>;
    normalizeStatus: (status?: string | boolean) => string;
    onToggleStatus: (user: UserRow) => void;
    onDeleteUser: (email: string) => void;
    onViewUser: (email: string) => void;
};

const tableColumns = [
    "Brand Name",
    "Company",
    "Native Country",
    "Marketplace",
    "Name",
    "Email",
    "Status",
    "Actions",
];

export default function SuperAdminUsersTable({
    users,
    actionLoading,
    normalizeStatus,
    onToggleStatus,
    onDeleteUser,
    onViewUser,
}: SuperAdminUsersTableProps) {
    return (
        <div className="overflow-hidden rounded-xl border border-gray-200 bg-white dark:border-white/[0.05] dark:bg-white/[0.03]">
            <div className="max-w-full overflow-x-auto">
                <div className="min-w-[1150px]">
                    <Table>
                        <TableHeader className="border-b border-gray-100 dark:border-white/[0.05]">
                            <TableRow>
                                {tableColumns.map((column) => (
                                    <TableCell
                                        key={column}
                                        isHeader
                                        className="px-5 py-3 text-start text-theme-xs font-medium text-gray-500 dark:text-gray-400"
                                    >
                                        {column}
                                    </TableCell>
                                ))}
                            </TableRow>
                        </TableHeader>

                        <TableBody className="divide-y divide-gray-100 dark:divide-white/[0.05]">
                            {users.length > 0 ? (
                                users.map((user) => {
                                    const userStatus = normalizeStatus(user.status);
                                    const nativeCountry =
                                        user.address?.country?.trim() || "Not added";
                                    const marketplaceIntegration = user.country || "Not added";
                                    const isBusy = !!actionLoading[user.email];

                                    return (
                                        <TableRow key={user.id}>
                                            <TableCell className="px-5 py-4 text-start sm:px-6">
                                                <span className="block text-theme-sm font-medium text-gray-800 dark:text-white/90">
                                                    {user.brand_name || "Not added"}
                                                </span>
                                            </TableCell>

                                            <TableCell className="px-4 py-3 text-start text-theme-sm text-gray-500 dark:text-gray-400">
                                                {user.company_name || "Not added"}
                                            </TableCell>

                                            <TableCell className="px-4 py-3 text-start text-theme-sm text-gray-500 dark:text-gray-400">
                                                {nativeCountry}
                                            </TableCell>

                                            <TableCell className="px-4 py-3 text-start text-theme-sm text-gray-500 dark:text-gray-400">
                                                <span className="inline-flex rounded-full bg-gray-100 px-3 py-1 text-xs font-medium capitalize text-gray-700 dark:bg-white/[0.06] dark:text-gray-300">
                                                    {marketplaceIntegration}
                                                </span>
                                            </TableCell>

                                            <TableCell className="px-4 py-3 text-start text-theme-sm text-gray-500 dark:text-gray-400">
                                                {user.name || "Not added"}
                                            </TableCell>

                                            <TableCell className="px-4 py-3 text-start text-theme-sm text-gray-500 dark:text-gray-400">
                                                <span className="break-all">
                                                    {user.email || "Not added"}
                                                </span>
                                            </TableCell>

                                            <TableCell className="px-4 py-3 text-start">
                                                <button
                                                    type="button"
                                                    onClick={() => onToggleStatus(user)}
                                                    disabled={isBusy}
                                                    className={`relative inline-flex h-6 w-11 items-center rounded-full transition ${userStatus === "active" ? "bg-emerald-500" : "bg-gray-300"
                                                        } ${isBusy ? "cursor-not-allowed opacity-60" : ""}`}
                                                    title={userStatus === "active" ? "Disable admin" : "Enable admin"}
                                                    aria-label={
                                                        userStatus === "active"
                                                            ? `Disable ${user.email}`
                                                            : `Enable ${user.email}`
                                                    }
                                                >
                                                    <span
                                                        className={`inline-block h-4 w-4 rounded-full bg-white shadow transition ${userStatus === "active" ? "translate-x-6" : "translate-x-1"
                                                            }`}
                                                    />
                                                </button>
                                            </TableCell>

                                            <TableCell className="px-4 py-3 text-start">
                                                <div className="flex items-center gap-2">
                                                    <button
                                                        type="button"
                                                        onClick={() => onViewUser(user.email)}
                                                        className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-gray-200 bg-white text-gray-700 shadow-sm transition hover:border-gray-300 hover:bg-gray-50 hover:text-[#37455F] dark:border-white/[0.08] dark:bg-white/[0.03] dark:text-gray-300"
                                                        title="View user"
                                                        aria-label={`View ${user.email}`}
                                                    >
                                                        <Eye size={17} />
                                                    </button>

                                                    <button
                                                        type="button"
                                                        onClick={() => onDeleteUser(user.email)}
                                                        disabled={isBusy}
                                                        className={`inline-flex h-9 w-9 items-center justify-center rounded-lg border border-red-200 bg-red-50 text-red-600 shadow-sm transition hover:bg-red-100 ${isBusy ? "cursor-not-allowed opacity-60" : ""
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
                                    <TableCell colSpan={8} className="px-5 py-16 text-center">
                                        <div className="mx-auto max-w-sm">
                                            <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-full bg-gray-100 text-gray-400 dark:bg-white/[0.06]">
                                                <FaSearch />
                                            </div>

                                            <h3 className="mt-4 text-base font-semibold text-gray-900 dark:text-white/90">
                                                No users found
                                            </h3>

                                            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                                                Try changing your search keyword or status filter.
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