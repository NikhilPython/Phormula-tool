"use client";

import React from "react";
import UserInfoCard from "@/components/user-profile/UserInfoCard";
import UserAddressCard from "@/components/user-profile/UserAddressCard";
import Button from "@/components/ui/button/Button";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import AddMemberModal from "@/components/header/AddMemberModal";
import ViewMemberDrawer from "@/components/header/ViewMemberDrawer";
import EditMemberModal from "@/components/header/EditMemberModal";
import SegmentedToggle from "@/components/ui/SegmentedToggle";

// function TabButton({
//   active,
//   onClick,
//   children,
// }: {
//   active: boolean;
//   onClick: () => void;
//   children: React.ReactNode;
// }) {
//   return (
//     <Button
//       variant={active ? "primary" : "outline"}
//       size="sm"
//       onClick={onClick}>{children}</Button>
//   );
// }

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
        {/* </div> */}

        {/* Tabs */}
        {/* <div className="flex flex-wrap gap-2 ">
          <TabButton
            active={tab === "personal"}
            onClick={() => setTab("personal")}
          >
            User Details
          </TabButton>

          <TabButton
            active={tab === "objectives"}
            onClick={() => setTab("objectives")}
          >
            Performance Targets
          </TabButton>

          {isMember === false && (
  <TabButton
    active={tab === "teamMembers"}
    onClick={() => setTab("teamMembers")}
  >
    Team Members
  </TabButton>
)}

           <TabButton
            active={tab === "integrations"}
            onClick={() => setTab("integrations")}
          >
            Integrations
          </TabButton> 
        </div> */}

        <div className="mt-3">
          <SegmentedToggle<"personal" | "objectives" | "teamMembers">
            value={tab}
            options={[
              { value: "personal", label: "User Details" },
              { value: "objectives", label: "Performance Targets" },
              ...(isMember === false ? [{ value: "teamMembers" as const, label: "Team Members" }] : []),
            ]}
            onChange={(val) => setTab(val)}
            className="max-w-full sm:max-w-[520px]"
            compact
          />
        </div>

        <div className="space-y-4 py-3">
          {tab === "personal" && (
            <>
              <UserInfoCard activeTab="personal" />
              {/* <UserAddressCard /> */}
            </>
          )}

          {tab === "objectives" && <UserInfoCard activeTab="objectives" />}

          {/* {tab === "integrations" && <UserInfoCard activeTab="integrations" />} */}
        </div>
        {tab === "teamMembers" && isMember === false && (
          <div className="space-y-4 py-3">
            <div className="flex items-center justify-between gap-3">
              <input
                className="w-full max-w-[320px] rounded-md border border-gray-200 bg-white px-3 py-2 text-sm"
                placeholder="Search by name or email..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />

              <Button variant="primary" size="sm" onClick={() => setIsAddMemberOpen(true)}>
                + Add Member
              </Button>
            </div>

            {membersLoading ? (
              <div className="text-sm text-gray-500">Loading members…</div>
            ) : membersError ? (
              <div className="text-sm text-red-600">{membersError}</div>
            ) : (
              <div className="rounded-xl border border-gray-200 bg-white overflow-hidden">
                <div className="grid grid-cols-12 gap-2 px-4 py-3 text-xs font-semibold text-gray-500 border-b">
                  <div className="col-span-4">Member</div>
                  <div className="col-span-2">Country</div>
                  <div className="col-span-3">Section Access</div>
                  <div className="col-span-2">Added on</div>
                  <div className="col-span-1 text-right">Actions</div>
                </div>

                {filteredMembers.length === 0 ? (
                  <div className="px-4 py-6 text-sm text-gray-500">No members found.</div>
                ) : (
                  filteredMembers.map((m, idx) => {
                    const name = String(m?.member_name || "-");
                    const email = String(m?.email || "-");
                    const countries = Array.isArray(m?.countries) ? m.countries : [];
                    const modules = Array.isArray(m?.modules) ? m.modules : [];
                    const createdAt = m?.created_at ? new Date(m.created_at) : null;

                    const showModules = modules.slice(0, 2);
                    const extraCount = Math.max(0, modules.length - showModules.length);

                    const initials = name
                      .split(" ")
                      .filter(Boolean)
                      .slice(0, 2)
                      .map((x: string) => x[0]?.toUpperCase())
                      .join("");

                    return (
                      <div
                        key={m?.id ?? idx}
                        className="grid grid-cols-12 gap-2 px-4 py-3 text-sm border-b last:border-b-0 items-center"
                      >
                        {/* Member */}
                        <div className="col-span-4 flex items-center gap-3 min-w-0">
                          <div className="w-8 h-8 rounded-full bg-slate-100 flex items-center justify-center text-xs font-bold text-slate-600">
                            {initials || "?"}
                          </div>
                          <div className="min-w-0">
                            <div className="font-semibold text-slate-800 truncate">{name}</div>
                            <div className="text-xs text-gray-500 truncate">{email}</div>
                          </div>
                        </div>

                        {/* Country */}
                        <div className="col-span-2 text-slate-700">
                          {countries?.[0] || "-"}
                        </div>

                        {/* Section Access */}
                        <div className="col-span-3 flex items-center gap-2 flex-wrap">
                          {showModules.map((mod: string) => (
                            <span
                              key={mod}
                              className="text-[10px] px-2 py-1 rounded-full border border-emerald-200 bg-emerald-50 text-emerald-700"
                            >
                              {mod.replaceAll("_", " ")}
                            </span>
                          ))}
                          {extraCount > 0 && (
                            <span className="text-[10px] px-2 py-1 rounded-full border border-gray-200 bg-gray-50 text-gray-600">
                              +{extraCount}
                            </span>
                          )}
                        </div>

                        {/* Added on */}
                        <div className="col-span-2 text-slate-700">
                          {createdAt ? createdAt.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" }) : "-"}
                        </div>

                        {/* Actions */}
                        <div className="col-span-1 flex justify-end gap-2">
                          <button
                            onClick={() => {
                              setSelectedMember(m);
                              setIsViewOpen(true);
                            }}
                            className="w-7 h-7 rounded-md border border-gray-200 hover:bg-gray-50 text-xs"
                          >
                            👁️
                          </button>

                          <button
                            onClick={() => {
                              setSelectedMember(m);
                              setIsEditOpen(true);
                            }}
                            className="w-7 h-7 rounded-md border border-gray-200 hover:bg-gray-50 text-xs"
                          >
                            ✏️
                          </button>

                          <button
                            onClick={() => handleDelete(m)}
                            className="w-7 h-7 rounded-md border border-red-200 hover:bg-red-50 text-xs"
                          >
                            🗑️
                          </button>
                        </div>
                      </div>
                    );
                  })
                )}
              </div>
            )}

            <AddMemberModal
              isOpen={isAddMemberOpen}
              onClose={() => setIsAddMemberOpen(false)}
              token={token}
              onSuccess={fetchMembers}
            />
            <ViewMemberDrawer
              isOpen={isViewOpen}
              onClose={() => setIsViewOpen(false)}
              member={selectedMember}
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
