"use client";
import UserDropdown from "@/components/header/UserDropdown";
import { useSidebar } from "@/context/SidebarContext";
import Image from "next/image";
import Link from "next/link";
import React, { useState, useEffect, useRef } from "react";
import AmazonConnect from "@/features/integration/AmazonConnect";
import AmazonFinancialDashboard from "@/features/integration/AmazonFinancialDashboard";
import ConnectShopifyModal from "@/features/integration/ConnectShopifyModal"; 
import { useParams } from "next/navigation";

const AppHeader: React.FC = () => {
  const [isApplicationMenuOpen, setApplicationMenuOpen] = useState(false);

  // Amazon modals
  const [openAmazonConnect, setOpenAmazonConnect] = useState(false);
  const [openAmazonFinance, setOpenAmazonFinance] = useState(false);

  // ⬇️ NEW: Shopify modal
  const [openShopify, setOpenShopify] = useState(false);

  const { isExpanded, toggleSidebar, toggleMobileSidebar } = useSidebar();
  const inputRef = useRef<HTMLInputElement>(null);

  const handleToggle = () => {
    if (typeof window !== "undefined" && window.innerWidth >= 1024) {
      toggleSidebar();
    } else {
      toggleMobileSidebar();
    }
  };

  const toggleApplicationMenu = () => setApplicationMenuOpen((v) => !v);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        inputRef.current?.focus();
      }
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, []);

  // Listen for Integrations selection (Amazon or Shopify)
  useEffect(() => {
    const onChoose = (e: Event) => {
      const { provider } = (e as CustomEvent<{ provider?: string }>).detail || {};
      if (provider === "amazon") setOpenAmazonConnect(true);
      if (provider === "shopify") setOpenShopify(true);           // ⬅️ open Shopify modal
    };
    window.addEventListener("integration:choose", onChoose as EventListener);
    return () =>
      window.removeEventListener("integration:choose", onChoose as EventListener);
  }, []);

  const params = useParams();

  const monthNames = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
  ];

  const today = new Date();

  const currentCountry =
    (params?.countryName as string) || "uk";

  const currentMonth =
    (params?.month as string) || monthNames[today.getMonth()];

  const currentYear =
    (params?.year as string) || String(today.getFullYear());

  const realTimeHref = `/live-dashboard/${currentCountry}/${currentMonth}/${currentYear}`;

  return (
    <>
      <header className="sticky top-0 flex w-full bg-white border-gray-200  dark:border-gray-800 dark:bg-gray-900 z-[1100]">
        <div className="flex flex-col items-center justify-between grow lg:flex-row lg:px-6">
          <div className="flex items-center justify-between w-full gap-2 px-3 py-2.5 border-b border-gray-200 dark:border-gray-800 sm:gap-4 lg:justify-normal lg:border-b-0 lg:px-0 lg:py-1.5 xl:py-2">
            {!isExpanded && (
              <button
                onClick={handleToggle}
                className="flex items-center justify-center w-10 h-10 rounded-lg lg:hidden"
                aria-label="Open sidebar"
              >
                <Image
                  src="/images/icons/hamburger.png"
                  alt="Open sidebar"
                  width={20}
                  height={20}
                />
              </button>
            )}
            <Link href={realTimeHref} className="lg:hidden">
              <Image width={154} height={32} className="dark:hidden" src="/images/logo/Logo_Phormula.png" alt="Logo" />
              <Image width={154} height={32} className="hidden dark:block" src="./images/logo/logo-dark.svg" alt="Logo" />
            </Link>

            <button
              onClick={toggleApplicationMenu}
              className="flex items-center justify-center w-10 h-10 text-gray-700 rounded-lg z-99999 hover:bg-gray-100 dark:text-gray-400 dark:hover:bg-gray-800 lg:hidden"
            >
              <svg width="24" height="24" viewBox="0 0 24 24" fill="none"><path fillRule="evenodd" clipRule="evenodd" d="M5.99902 10.4951C6.82745 10.4951 7.49902 11.1667 7.49902 11.9951V12.0051C7.49902 12.8335 6.82745 13.5051 5.99902 13.5051C5.1706 13.5051 4.49902 12.8335 4.49902 12.0051V11.9951C4.49902 11.1667 5.1706 10.4951 5.99902 10.4951ZM17.999 10.4951C18.8275 10.4951 19.499 11.1667 19.499 11.9951V12.0051C19.499 12.8335 18.8275 13.5051 17.999 13.5051C17.1706 13.5051 16.499 12.8335 16.499 12.0051V11.9951C16.499 11.1667 17.1706 10.4951 17.999 10.4951ZM13.499 11.9951C13.499 11.1667 12.8275 10.4951 11.999 10.4951C11.1706 10.4951 10.499 11.1667 10.499 11.9951V12.0051C10.499 12.8335 11.1706 13.5051 11.999 13.5051C12.8275 13.5051 13.499 12.8335 13.499 12.0051V11.9951Z" fill="currentColor" /></svg>
            </button>
          </div>

          <div className={`${isApplicationMenuOpen ? "flex" : "hidden"} items-center  w-full gap-3 lg:gap-2
    px-4 sm:px-5 lg:px-0
    py-2.5 sm:py-3 lg:py-1.5 xl:py-2.5
    lg:flex shadow-theme-md justify-end lg:px-0 lg:shadow-none`}>
            <UserDropdown />
          </div>

          {/* <div className="flex items-center gap-2 2xsm:gap-3">

            <NotificationDropdown items={items} />

          </div> */}

        </div>
      </header>

      {openAmazonConnect && (
        <AmazonConnect
          onClose={() => setOpenAmazonConnect(false)}
          onConnected={() => {
            setOpenAmazonConnect(false);
            setOpenAmazonFinance(true);
          }}
          onChooseManual={() => {
            setOpenAmazonConnect(false);
          }}
        />
      )}

      {/* Amazon Financial Dashboard as a modal */}
      {openAmazonFinance && (
        <div className="fixed inset-0 z-[100000] flex items-center justify-center p-4" role="dialog" aria-modal="true">
          <div className="absolute inset-0 bg-white/60" />
          <div className="relative w-full max-w-3xl rounded-xl border border-gray-200 bg-white p-4 shadow-2xl dark:border-gray-800 dark:bg-gray-900">
            <div className="mt-3">
              <AmazonFinancialDashboard onClose={() => setOpenAmazonFinance(false)} />
            </div>
          </div>
        </div>
      )}

      {openShopify && (
        <ConnectShopifyModal
          onClose={() => setOpenShopify(false)}
        />
      )}
    </>
  );
};

export default AppHeader;

