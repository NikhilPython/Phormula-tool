"use client";

import React, { useState } from "react";
import { useRouter } from "next/navigation";
import { ArrowLeft } from "lucide-react";

import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import SuperAdminUsersTable from "@/components/admin/table/SuperAdminUsersTable";

type AdminRow = {
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

const dummyAdmins: AdminRow[] = [
  {
    id: 1,
    brand_name: "Skin Elements",
    company_name: "Skin Elements Pvt Ltd",
    name: "ABC",
    email: "abc@skinelements.com",
    country: "UK",
    marketplace_id: "A1F83G8C2ARO7P",
    status: "active",
    address: {
      country: "India",
      city: "New Delhi",
      state: "Delhi",
      zipcode: "110001",
    },
  },
  {
    id: 2,
    brand_name: "Glow Naturals",
    company_name: "Glow Naturals Ltd",
    name: "Nikhil Dubey",
    email: "backend@skinelements.com",
    country: "US",
    marketplace_id: "ATVPDKIKX0DER",
    status: "inactive",
    address: {
      country: "India",
      city: "Mumbai",
      state: "Maharashtra",
      zipcode: "400001",
    },
  },
  {
    id: 3,
    brand_name: "Pure Care",
    company_name: "Pure Care Wellness",
    name: "Amit Sharma",
    email: "amit@example.com",
    country: "Global",
    marketplace_id: "GLOBAL-001",
    status: "active",
    address: {
      country: "United Kingdom",
      city: "London",
      state: "England",
      zipcode: "SW1A",
    },
  },
];

const AdminPage = () => {
  const router = useRouter();

  const [admins, setAdmins] = useState<AdminRow[]>(dummyAdmins);
  const [actionLoading, setActionLoading] = useState<Record<string, boolean>>({});

  const normalizeStatus = (status?: string | boolean) => {
    if (typeof status === "boolean") {
      return status ? "active" : "inactive";
    }

    return String(status || "inactive").toLowerCase();
  };

  const handleToggleStatus = (admin: AdminRow) => {
    setActionLoading((prev) => ({
      ...prev,
      [admin.email]: true,
    }));

    setAdmins((prev) =>
      prev.map((item) => {
        if (item.email !== admin.email) return item;

        const currentStatus = normalizeStatus(item.status);

        return {
          ...item,
          status: currentStatus === "active" ? "inactive" : "active",
        };
      })
    );

    setActionLoading((prev) => ({
      ...prev,
      [admin.email]: false,
    }));
  };

  const handleDeleteAdmin = (email: string) => {
    setAdmins((prev) => prev.filter((admin) => admin.email !== email));
  };

  // const handleViewAdmin = (email: string) => {
  //   router.push(`/superadmin/admins/${encodeURIComponent(email)}`);
  // };

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
              pageTitle="Admins"
              variant="superadmin"
              align="left"
              textSize="2xl"
            />

            <p className="mt-2 text-sm text-white/60">
              View all admin details, status and marketplace information.
            </p>
          </div>
        </div>
      </div>

      {/* Admin Table */}
      <section className="space-y-3">
        <PageBreadcrumb
          pageTitle="Admin Registry"
          variant="superadmin"
          align="left"
          textSize="2xl"
        />

        <SuperAdminUsersTable
          users={admins}
          actionLoading={actionLoading}
          normalizeStatus={normalizeStatus}
          onToggleStatus={handleToggleStatus}
          onDeleteUser={handleDeleteAdmin}
          // onViewUser={handleViewAdmin}
          emptyTitle="No admins found"
          emptyDescription="Admins added to the system will appear here."
        />
      </section>
    </div>
  </div>
);
};

export default AdminPage;