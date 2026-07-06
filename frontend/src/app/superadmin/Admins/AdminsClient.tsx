"use client";

import React, { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { ArrowLeft, Eye, Trash2 } from "lucide-react";
import { FaSearch } from "react-icons/fa";

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
  const [searchQuery, setSearchQuery] = useState<string>("");
  const [actionLoading, setActionLoading] = useState<Record<string, boolean>>(
    {}
  );

  const normalizeStatus = (status?: string | boolean) => {
    if (typeof status === "boolean") {
      return status ? "active" : "inactive";
    }

    return String(status || "inactive").toLowerCase();
  };

  const filteredAdmins = useMemo(() => {
    const query = searchQuery.trim().toLowerCase();

    if (!query) return admins;

    return admins.filter((admin) => {
      const status = normalizeStatus(admin.status);

      return (
        (admin.brand_name || "").toLowerCase().includes(query) ||
        (admin.company_name || "").toLowerCase().includes(query) ||
        (admin.address?.country || "").toLowerCase().includes(query) ||
        (admin.country || "").toLowerCase().includes(query) ||
        (admin.name || "").toLowerCase().includes(query) ||
        (admin.email || "").toLowerCase().includes(query) ||
        (admin.marketplace_id || "").toLowerCase().includes(query) ||
        status.includes(query)
      );
    });
  }, [admins, searchQuery]);

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
          <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
            <PageBreadcrumb
              pageTitle="Admins"
              variant="superadmin"
              align="left"
              textSize="2xl"
            />

            <div className="relative w-full md:w-[360px]">
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Search by Admin, Brand, Company or Email..."
                className="h-[42px] w-full rounded-xl border border-white/10 bg-white/[0.06] px-4 pr-11 text-sm text-white shadow-sm outline-none placeholder:text-white/40 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15"
              />

              <span className="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-[#31d9e5]">
                <FaSearch />
              </span>
            </div>
          </div>

          <SuperAdminUsersTable
            data={filteredAdmins}
            minWidth="1150px"
            emptyTitle={
              searchQuery.trim() ? "No matching admins found" : "No admins found"
            }
            emptyDescription={
              searchQuery.trim()
                ? "Try searching by another admin, brand, company, email, country, marketplace or status."
                : "Admins added to the system will appear here."
            }
            columns={[
              {
                key: "name",
                label: "Admin Name",
                cellClassName: "font-semibold text-white",
                render: (admin) => admin.name || "Not added",
              },
              {
                key: "email",
                label: "Email",
                render: (admin) => (
                  <span className="break-all">{admin.email || "Not added"}</span>
                ),
              },
              {
                key: "brand_name",
                label: "Brand Name",
                render: (admin) => admin.brand_name || "Not added",
              },
              {
                key: "company_name",
                label: "Company",
                render: (admin) => admin.company_name || "Not added",
              },
              {
                key: "native_country",
                label: "Native Country",
                render: (admin) => admin.address?.country?.trim() || "Not added",
              },
              {
                key: "marketplace",
                label: "Marketplace",
                render: (admin) => (
                  <span className="inline-flex rounded-full border border-white/10 bg-white/[0.06] px-3 py-1 text-xs font-medium capitalize text-white/75">
                    {admin.country || "Not added"}
                  </span>
                ),
              },
              {
                key: "status",
                label: "Status",
                render: (admin) => {
                  const adminStatus = normalizeStatus(admin.status);
                  const isBusy = !!actionLoading[admin.email];

                  return (
                    <button
                      type="button"
                      onClick={() => handleToggleStatus(admin)}
                      disabled={isBusy}
                      className={`relative inline-flex h-6 w-11 items-center rounded-full transition ${adminStatus === "active" ? "bg-[#31d9e5]" : "bg-white/20"
                        } ${isBusy ? "cursor-not-allowed opacity-60" : ""}`}
                      title={
                        adminStatus === "active" ? "Disable admin" : "Enable admin"
                      }
                      aria-label={
                        adminStatus === "active"
                          ? `Disable ${admin.email}`
                          : `Enable ${admin.email}`
                      }
                    >
                      <span
                        className={`inline-block h-4 w-4 rounded-full bg-white shadow transition ${adminStatus === "active" ? "translate-x-6" : "translate-x-1"
                          }`}
                      />
                    </button>
                  );
                },
              },
              {
                key: "actions",
                label: "Actions",
                render: (admin) => {
                  const isBusy = !!actionLoading[admin.email];

                  return (
                    <div className="flex items-center justify-center gap-2">
                      <button
                        type="button"
                        // onClick={() => handleViewAdmin(admin.email)}
                        className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-white/10 bg-white/[0.06] text-white/75 shadow-sm transition hover:border-[#31d9e5]/40 hover:bg-[#31d9e5]/10 hover:text-[#31d9e5]"
                        title="View admin"
                        aria-label={`View ${admin.email}`}
                      >
                        <Eye size={17} />
                      </button>

                      <button
                        type="button"
                        onClick={() => handleDeleteAdmin(admin.email)}
                        disabled={isBusy}
                        className={`inline-flex h-9 w-9 items-center justify-center rounded-lg border border-red-300/25 bg-red-500/10 text-red-200 shadow-sm transition hover:bg-red-500/20 hover:text-red-100 ${isBusy ? "cursor-not-allowed opacity-60" : ""
                          }`}
                        title="Delete admin"
                        aria-label={`Delete ${admin.email}`}
                      >
                        <Trash2 size={16} />
                      </button>
                    </div>
                  );
                },
              },
            ]}
          />
        </section>
      </div>
    </div>
  );
};

export default AdminPage;