"use client";
import Image from "next/image";
import Link from "next/link";
import React, { useEffect, useRef, useState } from "react";
import { Dropdown } from "../ui/dropdown/Dropdown";
import { DropdownItem } from "../ui/dropdown/DropdownItem";
import { useParams, useRouter } from "next/navigation";
import IntegrationToggleButton from "@/features/integration/IntegrationToggleButton";
import { useAppDispatch } from "@/lib/hooks";
import { logout, setUser } from "@/lib/features/auth/authSlice";
import { useAppSelector } from "@/lib/store";
import { useGetUserQuery } from "@/lib/api/userApi";
import NotificationDropdown from "./NotificationDropdown";
import { useHeaderNotifications } from "../context/NotificationContext";

type OpenDropdown = "user" | "notifications" | null;

export default function UserDropdown() {
  const [openDropdown, setOpenDropdown] = useState<OpenDropdown>(null);
  const wrapperRef = useRef<HTMLDivElement>(null);

  const dispatch = useAppDispatch();
  const router = useRouter();
  const routeParams = useParams();
  const { user: userFromStore, token } = useAppSelector((s: any) => s.auth);

  const { month, year } = routeParams as {
    month?: string;
    year?: string;
  };

  const isNAMonthYear = month === "NA" && year === "NA";

  const { data: userFromApi } = useGetUserQuery(undefined, {
    skip: !token,
  });

  useEffect(() => {
    if (userFromApi) dispatch(setUser(userFromApi));
  }, [userFromApi, dispatch]);

  useEffect(() => {
    if (!userFromStore && userFromApi) {
      dispatch(setUser(userFromApi));
    }
  }, [userFromStore, userFromApi, dispatch]);

  useEffect(() => {
    if (!userFromStore && typeof window !== "undefined") {
      const brand = localStorage.getItem("brandName");
      const company = localStorage.getItem("companyName");
      const name = localStorage.getItem("name");

      if (brand || company) {
        dispatch(
          setUser({
            brand_name: brand || undefined,
            company_name: company || undefined,
            name: name || undefined,
          })
        );
      }
    }
  }, [userFromStore, dispatch]);

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (
        wrapperRef.current &&
        !wrapperRef.current.contains(event.target as Node)
      ) {
        setOpenDropdown(null);
      }
    }

    document.addEventListener("mousedown", handleClickOutside);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, []);

  function toggleUserDropdown(e: React.MouseEvent<HTMLElement>) {
    e.stopPropagation();
    setOpenDropdown((prev) => (prev === "user" ? null : "user"));
  }

  function toggleNotificationDropdown(e: React.MouseEvent<HTMLElement>) {
    e.stopPropagation();
    setOpenDropdown((prev) => (prev === "notifications" ? null : "notifications"));
  }

  function closeAllDropdowns() {
    setOpenDropdown(null);
  }

  const handleLogout = () => {
    dispatch(logout());

    if (typeof window !== "undefined") {
      localStorage.removeItem("jwtToken");
      localStorage.clear();
      sessionStorage.clear();
    }

    closeAllDropdowns();
    window.location.href = "/signin";
  };

  const initials = userFromStore?.name
    ?.split(" ")
    .map((n: any) => n[0])
    .slice(0, 2)
    .join("")
    .toUpperCase();

  const isMember = userFromStore?.is_member === true;
  const currentCountryName = (routeParams?.countryName as string) || "global";

  const showIntegrationButton =
    !!month && !!year && month !== "NA" && year !== "NA" && !isMember;

  const { items } = useHeaderNotifications();

  return (
    <div ref={wrapperRef} className="relative z-99999">
      <div className="flex items-center text-gray-700 dark:text-gray-400">
        <span className="mr-1 font-normal text-xs md:text-sm 2xl:text-base inline-flex items-center gap-2">
          Welcome,
          <span className="font-bold inline-flex items-center gap-2">
            <i>{userFromStore?.name}!</i>

            <div
              onClick={toggleUserDropdown}
              className="cursor-pointer flex items-center justify-center w-7 h-7 sm:w-8 sm:h-8 2xl:w-8 2xl:h-8 rounded-full bg-blue-700 text-yellow-200 text-xs font-semibold leading-none"
            >
              {initials}
            </div>

            {showIntegrationButton && <IntegrationToggleButton />}

            <NotificationDropdown
              items={items}
              isOpen={openDropdown === "notifications"}
              onToggle={toggleNotificationDropdown}
              onClose={closeAllDropdowns}
            />
          </span>
        </span>
      </div>

      <Dropdown
        isOpen={openDropdown === "user"}
        onClose={closeAllDropdowns}
        className="absolute right-0 mt-[17px] flex w-[260px] flex-col rounded-2xl border border-gray-200 bg-white p-3 shadow-theme-lg dark:border-gray-800 dark:bg-gray-dark"
      >
        <div>
          <span className="block font-medium text-gray-700 text-theme-sm dark:text-gray-400">
            {userFromStore?.name || userFromStore?.brand_name || "User"}
          </span>
          <span className="mt-0.5 block text-theme-xs text-gray-500 dark:text-gray-400">
            {userFromStore?.email}
          </span>
        </div>

        <ul className="flex flex-col gap-1 pt-4 pb-3 border-b border-gray-200 dark:border-gray-800">
          <li>
            <DropdownItem
              onItemClick={closeAllDropdowns}
              tag="a"
              href={`/objectives-targets/${currentCountryName}/${month || "NA"}/${year || "NA"}`}
              className="flex items-center gap-3 px-3 py-2 font-medium text-gray-700 rounded-lg group text-theme-sm hover:bg-gray-100 hover:text-gray-700 dark:text-gray-400 dark:hover:bg-white/5 dark:hover:text-gray-300"
            >
              Business Overview
            </DropdownItem>
          </li>

          {!isMember && (
            <li>
              <DropdownItem
                onItemClick={() => {
                  if (isNAMonthYear) return;
                  closeAllDropdowns();
                  router.push(`/profile/${currentCountryName}/${month || "NA"}/${year || "NA"}?addMember=true`);
                }}
                tag="button"
                className={`flex items-center gap-3 px-3 py-2 font-medium rounded-lg group text-theme-sm w-full text-left ${
                  isNAMonthYear
                    ? "cursor-not-allowed text-gray-400 opacity-50 "
                    : "text-gray-700 hover:bg-gray-100 hover:text-gray-700"
                }`}
              >
                Add Members
              </DropdownItem>
            </li>
          )}

          <li>
            <DropdownItem
              onItemClick={closeAllDropdowns}
              tag="a"
              href={`/profile/${currentCountryName}/${month || "NA"}/${year || "NA"}`}
              className="flex items-center gap-3 px-3 py-2 font-medium text-gray-700 rounded-lg group text-theme-sm hover:bg-gray-100 hover:text-gray-700 dark:text-gray-400 dark:hover:bg-white/5 dark:hover:text-gray-300"
            >
              Account Settings
            </DropdownItem>
          </li>
        </ul>

        <button
          onClick={handleLogout}
          className="flex items-center gap-3 px-3 py-2 mt-3 font-medium text-red-700 rounded-lg group text-theme-sm hover:bg-red-200 hover:text-red-700 dark:text-gray-400 dark:hover:bg-white/5 dark:hover:text-gray-300 w-full text-left"
        >
          Sign out
        </button>
      </Dropdown>
    </div>
  );
}