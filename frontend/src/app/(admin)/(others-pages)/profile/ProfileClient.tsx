"use client";

import React from "react";
import UserInfoCard from "@/components/user-profile/UserInfoCard";
import UserAddressCard from "@/components/user-profile/UserAddressCard";
import Button from "@/components/ui/button/Button";

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
