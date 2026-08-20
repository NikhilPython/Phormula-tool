"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { ArrowLeft, Eye, Trash2 } from "lucide-react";
import { FaSearch } from "react-icons/fa";

import SuperAdminUsersTable from "@/components/admin/table/SuperAdminUsersTable";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

type AdminRow = {
  id: number | string;
  email: string;
  name?: string;
  phone_number?: string;
  company_name?: string;
  brand_name?: string;
  country?: string;
  native_country?: string;
  address?: {
    building?: string;
    city?: string;
    country?: string;
    state?: string;
    zipcode?: string;
  };
};

const sortByNewestId = (rows: AdminRow[]) =>
  [...rows].sort((a, b) => {
    const aNumber = typeof a.id === "number" ? a.id : Number(a.id);
    const bNumber = typeof b.id === "number" ? b.id : Number(b.id);

    if (Number.isFinite(aNumber) && Number.isFinite(bNumber)) {
      return bNumber - aNumber;
    }

    return String(b.id).localeCompare(String(a.id), undefined, {
      numeric: true,
      sensitivity: "base",
    });
  });

// const normalizeStatus = (status?: string | boolean) => {
//   if (typeof status === "boolean") {
//     return status ? "active" : "inactive";
//   }

//   return String(status || "inactive").toLowerCase();
// };

const AdminPage = () => {
  const router = useRouter();

  const [admins, setAdmins] = useState<AdminRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState("");
  // const [actionLoading, setActionLoading] = useState<Record<string, boolean>>(
  //   {}
  // );

  useEffect(() => {
    const fetchAdmins = async () => {
      try {
        setLoading(true);

        const token = localStorage.getItem("superadmin_token");

        if (!token) {
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

        const data = await response.json().catch(() => ({}));

        if (!response.ok) {
          throw new Error(data?.message || "Failed to fetch admins");
        }

        const users = Array.isArray(data?.users) ? data.users : [];

        setAdmins(
          users.map((user: AdminRow) => ({
            ...user,
            native_country:
              user.native_country ||
              user.address?.country ||
              user.country ||
              "",
          }))
        );
      } catch (error) {
        console.error("Failed to fetch admins:", error);
        setAdmins([]);
      } finally {
        setLoading(false);
      }
    };

    fetchAdmins();
  }, [router]);

  const filteredAdmins = useMemo(() => {
    const query = searchQuery.trim().toLowerCase();
    const sorted = sortByNewestId(admins);

    if (!query) {
      return sorted;
    }

    return sorted.filter((admin) =>
      [
        admin.name,
        admin.email,
        admin.phone_number,
        admin.company_name,
        admin.brand_name,
        admin.native_country,
        admin.address?.country,
        admin.country,
      ]
        .filter(Boolean)
        .some((value) =>
          String(value).toLowerCase().includes(query)
        )
    );
  }, [admins, searchQuery]);

  return (
    <div className="w-full text-white">
      <div className="space-y-5 2xl:space-y-6">


        {/* Admins Header */}
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <PageBreadcrumb
            pageTitle="Admins"
            variant="superadmin"
            align="left"
            textSize="2xl"
          />

          <div className="relative w-full md:w-[260px] 2xl:w-[300px]">
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search admins..."
              className="h-9 w-full rounded-lg border border-white/10 bg-white/[0.06] px-3 pr-9 text-xs text-white outline-none placeholder:text-white/40 focus:border-[#31d9e5] focus:ring-2 focus:ring-[#31d9e5]/15"
            />

            <span className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-[#31d9e5]">
              <FaSearch size={13} />
            </span>
          </div>
        </div>


        {/* Admins Table */}
        <SuperAdminUsersTable
          data={filteredAdmins}
          minWidth="1250px"
          emptyTitle={
            searchQuery.trim()
              ? "No matching admins found"
              : "No admins found"
          }
          emptyDescription={
            searchQuery.trim()
              ? "Try searching by another name, brand, company, email, country or status."
              : "Admins added to the system will appear here."
          }
          columns={[
            {
              key: "serial_no",
              label: "S.No.",
              render: (admin) => {
                const index = filteredAdmins.findIndex(
                  (item) => String(item.id) === String(admin.id)
                );

                return (
                  <span className="font-medium text-white/65">
                    {index >= 0 ? index + 1 : "-"}
                  </span>
                );
              },
            },
            {
              key: "name",
              label: "Admin Name",
              cellClassName: "font-semibold text-white",
              render: (admin) =>
                admin.name || "Not added",
            },
            {
              key: "email",
              label: "Email",
              render: (admin) => (
                <span className="break-all">
                  {admin.email || "Not added"}
                </span>
              ),
            },
            {
              key: "phone_number",
              label: "Phone Number",
              render: (admin) =>
                admin.phone_number || "Not added",
            },
            {
              key: "brand_name",
              label: "Brand",
              render: (admin) =>
                admin.brand_name || "Not added",
            },
            {
              key: "native_country",
              label: "Native Country",
              render: (admin) =>
                admin.native_country ||
                admin.address?.country ||
                "Not added",
            },
          ]}
        />
      </div>
    </div>
  );
};

export default AdminPage;