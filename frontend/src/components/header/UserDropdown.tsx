"use client";
import Image from "next/image";
import Link from "next/link";
import React, { useEffect, useState } from "react";
import { Dropdown } from "../ui/dropdown/Dropdown";
import { DropdownItem } from "../ui/dropdown/DropdownItem";
import { useParams, useRouter } from "next/navigation";
import IntegrationToggleButton from "@/features/integration/IntegrationToggleButton";
import { useAppDispatch } from "@/lib/hooks";
import { logout, setUser } from "@/lib/features/auth/authSlice";
import { useAppSelector } from "@/lib/store";
import { useGetUserQuery } from "@/lib/api/userApi";
import AddMemberModal from "@/components/header/AddMemberModal";

export default function UserDropdown() {
  const [isOpen, setIsOpen] = useState(false);
  const [isAddMemberOpen, setIsAddMemberOpen] = useState(false);

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

  function toggleDropdown(e: React.MouseEvent<HTMLElement, MouseEvent>) {
    e.stopPropagation();
    setIsOpen((prev) => !prev);
  }

  function closeDropdown() {
    setIsOpen(false);
  }

  const handleLogout = () => {
    dispatch(logout());

    if (typeof window !== "undefined") {
      localStorage.removeItem("jwtToken");
      localStorage.clear();
      sessionStorage.clear();
    }

    closeDropdown();
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

  // console.log(currentCountryName)

  return (
    <div className="relative z-99999">
      <div className="flex items-center text-gray-700 dark:text-gray-400">
        <span className="mr-1 font-normal text-xs md:text-sm 2xl:text-base inline-flex items-center gap-2">
          Welcome,
          <span className="font-bold inline-flex items-center gap-2">
            <i>{userFromStore?.name}!</i>

            <div
              onClick={toggleDropdown}
              className="cursor-pointer flex items-center justify-center w-7 h-7 sm:w-8 sm:h-8 2xl:w-8 2xl:h-8 rounded-full bg-blue-700 text-yellow-200 text-xs font-semibold leading-none"
            >
              {initials}
            </div>

            {/* <IntegrationToggleButton /> */}
            {!isNAMonthYear && <IntegrationToggleButton />}
          </span>
        </span>
      </div>

      <Dropdown
        isOpen={isOpen}
        onClose={closeDropdown}
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
      onItemClick={closeDropdown}
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
          closeDropdown();
          setIsAddMemberOpen(true);
        }}
        tag="button"
        className={`flex items-center gap-3 px-3 py-2 font-medium rounded-lg group text-theme-sm w-full text-left ${
          isNAMonthYear
            ? "cursor-not-allowed text-gray-400 bg-gray-100 dark:text-gray-500 dark:bg-white/5"
            : "text-gray-700 hover:bg-gray-100 hover:text-gray-700 dark:text-gray-400 dark:hover:bg-white/5 dark:hover:text-gray-300"
        }`}
      >
        Add Members
      </DropdownItem>
    </li>
  )}

  <li>
    <DropdownItem
      onItemClick={closeDropdown}
      tag="a"
      href="/profile"
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
          <svg
            className="fill-red-700 group-hover:fill-red-700 dark:group-hover:fill-red-700"
            width="24"
            height="24"
            viewBox="0 0 24 24"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              fillRule="evenodd"
              clipRule="evenodd"
              d="M15.1007 19.247C14.6865 19.247 14.3507 18.9112 14.3507 18.497L14.3507 14.245H12.8507V18.497C12.8507 19.7396 13.8581 20.747 15.1007 20.747H18.5007C19.7434 20.747 20.7507 19.7396 20.7507 18.497L20.7507 5.49609C20.7507 4.25345 19.7433 3.24609 18.5007 3.24609H15.1007C13.8581 3.24609 12.8507 4.25345 12.8507 5.49609V9.74501L14.3507 9.74501V5.49609C14.3507 5.08188 14.6865 4.74609 15.1007 4.74609L18.5007 4.74609C18.9149 4.74609 19.2507 5.08188 19.2507 5.49609L19.2507 18.497C19.2507 18.9112 18.9149 19.247 18.5007 19.247H15.1007ZM3.25073 11.9984C3.25073 12.2144 3.34204 12.4091 3.48817 12.546L8.09483 17.1556C8.38763 17.4485 8.86251 17.4487 9.15549 17.1559C9.44848 16.8631 9.44863 16.3882 9.15583 16.0952L5.81116 12.7484L16.0007 12.7484C16.4149 12.7484 16.7507 12.4127 16.7507 11.9984C16.7507 11.5842 16.4149 11.2484 16.0007 11.2484L5.81528 11.2484L9.15585 7.90554C9.44864 7.61255 9.44847 7.13767 9.15547 6.84488C8.86248 6.55209 8.3876 6.55226 8.09481 6.84525L3.52309 11.4202C3.35673 11.5577 3.25073 11.7657 3.25073 11.9984Z"
              fill=""
            />
          </svg>
          Sign out
        </button>
      </Dropdown>

      {!isMember && (
        <AddMemberModal
          isOpen={isAddMemberOpen}
          onClose={() => setIsAddMemberOpen(false)}
          token={token}
        />
      )}
    </div>
  );
}