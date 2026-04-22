"use client";

import React from "react";
import { useParams, useRouter, useSearchParams } from "next/navigation";
import UserInfoCard from "@/components/user-profile/UserInfoCard";
import UserAddressCard from "@/components/user-profile/UserAddressCard";
import Button from "@/components/ui/button/Button";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import AddMemberModal from "@/components/header/AddMemberModal";
import ViewMemberDrawer from "@/components/header/ViewMemberDrawer";
import EditMemberModal from "@/components/header/EditMemberModal";
import SegmentedToggle from "@/components/ui/SegmentedToggle";
import DataTable, { type ColumnDef, type Row } from "@/components/ui/table/DataTable";
import { IoEyeOutline } from "react-icons/io5";
import { FiEdit, FiCheck, FiX } from "react-icons/fi";
import { MdOutlineDeleteOutline } from "react-icons/md";
import { FiSearch } from "react-icons/fi";

type TeamMemberRow = Row & {
  sno: number;
  name: React.ReactNode;
  email: React.ReactNode;
  country: React.ReactNode;
  role: React.ReactNode;
  sectionAccess: React.ReactNode;
  addedOn: React.ReactNode;
  actions: React.ReactNode;
};

export default function ProfileClient() {
  const [tab, setTab] = React.useState<"personal" | "objectives" | "teamMembers">("personal");
  const [isMember, setIsMember] = React.useState<boolean | null>(null);
  const [members, setMembers] = React.useState<any[]>([]);
  const [membersLoading, setMembersLoading] = React.useState(false);
  const [membersError, setMembersError] = React.useState<string | null>(null);
  const [search, setSearch] = React.useState("");
  const [isAddMemberOpen, setIsAddMemberOpen] = React.useState(false);
  const [selectedMember, setSelectedMember] = React.useState<any | null>(null);
  const [isViewOpen, setIsViewOpen] = React.useState(false);
  const [isEditOpen, setIsEditOpen] = React.useState(false);
  const [actionLoading, setActionLoading] = React.useState(false);
  const [ownerName, setOwnerName] = React.useState("");

  const params = useParams();
  const router = useRouter();

  const countryName = (params?.countryName as string) || "global";
  const month = (params?.month as string) || "NA";
  const year = (params?.year as string) || "NA";
  const isPreviewMode = month === "NA" && year === "NA";
  const searchParams = useSearchParams();
  const shouldOpenAddMember = searchParams.get("addMember") === "true";

  const token =
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") || "" : "";

  const fetchMembers = React.useCallback(async () => {
    try {
      setMembersLoading(true);
      setMembersError(null);

      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/get_user_data`,
        {
          headers: token ? { Authorization: `Bearer ${token}` } : {},
          cache: "no-store",
        }
      );

      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data?.error || "Failed to load user data");
      setOwnerName(String(data?.name || ""));
      setIsMember(!!data?.is_member);
      setMembers(Array.isArray(data?.members) ? data.members : []);
    } catch (e: any) {
      setMembersError(e?.message || "Failed to load members");
      setIsMember(true); // safe default: tab hide
      setMembers([]);
    } finally {
      setMembersLoading(false);
    }
  }, [token]);

  React.useEffect(() => {
    fetchMembers();
  }, [fetchMembers]);

  React.useEffect(() => {
    if (shouldOpenAddMember && isMember === false && !isPreviewMode) {
      setTab("teamMembers");
      setIsAddMemberOpen(true);
    }
  }, [shouldOpenAddMember, isMember, isPreviewMode]);

  const filteredMembers = React.useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return members;

    return members.filter((m) => {
      const name = String(m?.member_name || "").toLowerCase();
      const email = String(m?.email || "").toLowerCase();
      return name.includes(q) || email.includes(q);
    });
  }, [members, search]);

  const handleDelete = async (member: any) => {
    if (!confirm("Are you sure you want to delete this member?")) return;

    try {
      setActionLoading(true);

      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/delete_member`,
        {
          method: "DELETE",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify({
            member_id: member.id,
          }),
        }
      );

      if (!res.ok) throw new Error("Delete failed");

      fetchMembers();
    } catch (err) {
      alert("Failed to delete member");
    } finally {
      setActionLoading(false);
    }
  };

  React.useEffect(() => {
    if (isMember !== false && tab === "teamMembers") {
      setTab("personal");
    }
  }, [isMember, tab]);

  React.useEffect(() => {
    if (isPreviewMode && tab === "teamMembers") {
      setTab("personal");
    }
  }, [isPreviewMode, tab]);

  const memberColumns = React.useMemo<ColumnDef<TeamMemberRow>[]>(
    () => [
      {
        key: "sno",
        header: "S.No.",
        width: "90px",
        cellClassName: "text-center",
        headerClassName: "text-center",
      },
      {
        key: "name",
        header: "Name",
        width: "220px",
        cellClassName: "text-center",
        headerClassName: "text-center",
      },
      {
        key: "email",
        header: "Email",
        width: "260px",
        cellClassName: "text-center",
        headerClassName: "text-center",
      },
      {
        key: "country",
        header: "Country",
        width: "140px",
        cellClassName: "text-center",
        headerClassName: "text-center",
      },
      {
        key: "role",
        header: "Role",
        width: "140px",
        cellClassName: "text-center",
        headerClassName: "text-center",
      },
      {
        key: "sectionAccess",
        header: "Section Access",
        width: "260px",
        cellClassName: "text-center",
        headerClassName: "text-center",
      },
      {
        key: "addedOn",
        header: "Added On",
        width: "160px",
        cellClassName: "text-center",
        headerClassName: "text-center",
      },
      {
        key: "actions",
        header: "Actions",
        width: "140px",
        cellClassName: "text-center",
        headerClassName: "text-center",
      },
    ],
    []
  );

  const moduleColors: Record<string, string> = {
    LIVE_DASHBOARD: "border-green-500 bg-green-50 text-green-500",
    FINANCE_DASHBOARDS: "border-[#B75A5A] bg-[#B75A5A26] text-[#B75A5A]",
    INVENTORY_PLANNING: "border-[#3A8EA4] bg-[#3A8EA426] text-[#3A8EA4]",
    BUSINESS_INTELLIGENCE: "border-[#ED9F50] bg-[#ED9F5026] text-[#ED9F50]",
    DEFAULT: "border-gray-200 bg-gray-50 text-gray-600",
  };

  const memberRows = React.useMemo<TeamMemberRow[]>(() => {
    return filteredMembers.map((m, idx) => {
      const name = String(m?.member_name || "-");
      const email = String(m?.email || "-");
      const countries = Array.isArray(m?.countries) ? m.countries : [];
      const modules = Array.isArray(m?.modules) ? m.modules : [];
      const createdAt = m?.created_at ? new Date(m.created_at) : null;
      const formatRole = (role: string) => {
        if (!role) return "-";

        return role
          .toLowerCase()
          .replace(/_/g, " ")
          .replace(/\b\w/g, (l) => l.toUpperCase());
      };
      const roleRaw = String(m?.role || m?.member_role || "");
      const role = formatRole(roleRaw);

      const showModules = modules.slice(0, 2);
      const extraCount = Math.max(0, modules.length - 2);

      return {
        sno: idx + 1,

        name: <span className="font-semibold text-slate-800">{name}</span>,

        email: <span className="text-gray-500">{email}</span>,

        country: <span className="text-slate-700">{countries?.[0] || "-"}</span>,

        role: <span className="text-slate-700">{role}</span>,

        sectionAccess: (
          <div className="flex flex-wrap items-center justify-center gap-2">
            {showModules.map((mod: string) => (
              // <span
              //   key={mod}
              //   className="rounded-full border border-emerald-200 bg-emerald-50 px-2 py-1 text-[10px] text-emerald-700"
              // >
              //   {mod
              //     .replaceAll("_", " ")
              //     .replace(/\b\w/g, (l) => l.toUpperCase())}
              // </span>
              <span
                key={mod}
                className={`rounded-full px-2 py-1 text-[10px] border ${moduleColors[mod] || moduleColors.DEFAULT
                  }`}
              >
                {mod
                  .replaceAll("_", " ")
                  .replace(/\b\w/g, (l) => l.toUpperCase())}
              </span>
            ))}

            {extraCount > 0 && (
              <span className="rounded-full border border-gray-200 bg-gray-50 px-2 py-1 text-[10px] text-gray-600">
                +{extraCount}
              </span>
            )}
          </div>
        ),

        addedOn: (
          <span className="text-slate-700">
            {createdAt
              ? createdAt.toLocaleDateString("en-GB", {
                day: "2-digit",
                month: "short",
                year: "numeric",
              })
              : "-"}
          </span>
        ),

        actions: (
          <div className="flex justify-center gap-2">
            <button
              onClick={() => {
                setSelectedMember(m);
                setIsViewOpen(true);
              }}
              className="p-1.5 rounded-md border border-gray-200 text-xs hover:bg-gray-50"
              type="button"
            >
              <IoEyeOutline size={15} />
            </button>

            <button
              onClick={() => {
                setSelectedMember(m);
                setIsEditOpen(true);
              }}
              className="p-1.5 rounded-md border border-gray-200 text-xs hover:bg-gray-50"
              type="button"
            >
              <FiEdit size={15} />
            </button>

            <button
              onClick={() => handleDelete(m)}
              className="p-1.5 rounded-md border border-gray-200 text-xs "
              type="button"
            >
              < MdOutlineDeleteOutline size={17} />
            </button>
          </div>
        ),
      };
    });
  }, [filteredMembers]);

  return (
    <div>
      <div className="rounded-2xl">
        {/* <div className="sticky top-0 z-40 w-full flex flex-col bg-white  md:flex-row md:items-center md:justify-between gap-4 border-b border-gray-200"> */}

        {/* LEFT: Title + Subtitle */}
        <div className="flex flex-col leading-tight w-full md:w-auto md:mb-5">
          <PageBreadcrumb
            pageTitle="Account Settings"
            variant="page"
            align="left"
            textSize="2xl"
          />



          <p className="text-xs 2xl:text-sm text-charcoal-500 mt-1">
            Manage your profile, country, integrations and performance targets
          </p>
        </div>

        <div className="mt-3">
          <SegmentedToggle<"personal" | "objectives" | "teamMembers">
            value={tab}
            options={[
              { value: "personal", label: "User Details" },
              ...(isMember === false && !isPreviewMode
                ? [{ value: "teamMembers" as const, label: "Team Members" }]
                : []),
            ]}
            onChange={(val) => {
              if (isPreviewMode && val === "teamMembers") return;
              setTab(val);
            }}
            className="max-w-full sm:max-w-[520px]"
            compact
          />
        </div>

        <div className="mt-4 space-y-4 ">
          {tab === "personal" && (
            <>
              <UserInfoCard
                activeTab="personal"
              />
            </>
          )}

          {tab === "objectives" && (
            <UserInfoCard
              activeTab="objectives"
            />
          )}

          {/* {tab === "integrations" && <UserInfoCard activeTab="integrations" />} */}
        </div>

        {tab === "teamMembers" && isMember === false && (
          <div className="space-y-4">
            <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
              {/* Search */}
              <div className="relative w-full sm:max-w-[380px]">
                <FiSearch
                  className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400"
                  size={16}
                />

                <input
                  className="h-10 sm:h-11 w-full rounded-md border border-gray-200 bg-white pl-10 pr-3 text-sm
               focus:outline-none focus:border-green-500 
               transition-all duration-200"
                  placeholder="Search by name or email..."
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                />
              </div>

              {/* Add Member Button */}
              <Button
                variant="primary"
                size="sm"
                onClick={() => setIsAddMemberOpen(true)}
              >
                + Add Member
              </Button>
            </div>

            {membersLoading ? (
              <div className="text-sm text-gray-500">Loading members…</div>
            ) : membersError ? (
              <div className="text-sm text-red-600">{membersError}</div>
            ) : (
              <DataTable<TeamMemberRow>
                columns={memberColumns}
                data={memberRows}
                paginate={memberRows.length > 10}
                pageSize={10}
                scrollY={false}
                stickyHeader={false}
                emptyMessage="No members found."
                className="rounded-xl"
                tableClassName="max-w-full"
              />
            )}

            <AddMemberModal
              isOpen={isAddMemberOpen}
              onClose={() => {
                setIsAddMemberOpen(false);
                router.replace(`/profile/${countryName}/${month || "NA"}/${year || "NA"}`);
              }}
              token={token}
              onSuccess={() => {
                fetchMembers();
                setIsAddMemberOpen(false);
                router.replace(`/profile/${countryName}/${month || "NA"}/${year || "NA"}`);
              }}
            />
            <ViewMemberDrawer
              isOpen={isViewOpen}
              onClose={() => setIsViewOpen(false)}
              member={selectedMember}
              addedBy={ownerName}
            />

            <EditMemberModal
              isOpen={isEditOpen}
              onClose={() => setIsEditOpen(false)}
              member={selectedMember}
              token={token}
              onSuccess={fetchMembers}
            />
          </div>
        )}
      </div>
    </div>
  );
}
