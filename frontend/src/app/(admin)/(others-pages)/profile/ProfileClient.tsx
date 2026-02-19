"use client";

import React from "react";
import UserInfoCard from "@/components/user-profile/UserInfoCard";
import UserAddressCard from "@/components/user-profile/UserAddressCard";
import Button from "@/components/ui/button/Button";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

function TabButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <Button
      variant={active ? "primary" : "outline"}
      size="sm"
      onClick={onClick}>{children}</Button>
  );
}

export default function ProfileClient() {
  const [tab, setTab] = React.useState<
    "personal" | "objectives" | "integrations"
  >("personal");

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
        <div className="flex flex-wrap gap-2 ">
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

          {/* <TabButton
            active={tab === "integrations"}
            onClick={() => setTab("integrations")}
          >
            Integrations
          </TabButton> */}
        </div>

        <div className="space-y-4 py-3">
          {tab === "personal" && (
            <>
              <UserInfoCard activeTab="personal" />
              <UserAddressCard />
            </>
          )}

          {tab === "objectives" && <UserInfoCard activeTab="objectives" />}

          {/* {tab === "integrations" && <UserInfoCard activeTab="integrations" />} */}
        </div>
      </div>
    </div>
  );
}
