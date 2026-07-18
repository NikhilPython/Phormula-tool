"use client";

import AuthGuard from "@/components/auth/AuthGuard";
import { NotificationProvider } from "@/components/context/NotificationContext";
import { useSidebar } from "@/context/SidebarContext";
import AppHeader from "@/layout/AppHeader";
import AppSidebar from "@/layout/AppSidebar";
import Backdrop from "@/layout/Backdrop";
import React from "react";
import { useParams, usePathname } from "next/navigation";
import ChatbotWidget from "@/components/chatbot/ChatbotWidget";
// import PreviewModeNotice from "@/components/amazon/PreviewModeNotice";
import { useGetUserDataQuery } from "@/lib/api/profileApi";
import AmazonAdsSuccessPopup from "@/components/amazon/AmazonAdsSuccessPopup";

export default function AdminLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const { isExpanded, isHovered, isMobileOpen } = useSidebar();
  const { data: userData } = useGetUserDataQuery();
  const pathname = usePathname();

  const currentParams = useParams() as {
    ranged?: string;
    countryName?: string;
    month?: string;
    year?: string;
  };

  const showExpanded = isExpanded || isHovered;

  const expandedMargin =
    "lg:ml-[clamp(155px,13vw,210px)] xl:ml-[clamp(180px,16vw,250px)]";
  const collapsedMargin = "lg:ml-[56px] sm:lg:ml-[64px] xl:ml-[72px]";

  const mainContentMargin = isMobileOpen
    ? "ml-0"
    : showExpanded
      ? expandedMargin
      : collapsedMargin;

  const isPreviewMode =
    currentParams.month === "NA" ||
    currentParams.year === "NA";
  const isChatbotPage = pathname === "/chatbot" || pathname?.startsWith("/chatbot/");

  return (
    <AuthGuard>
      <NotificationProvider
        countryName={currentParams.countryName}
        month={currentParams.month}
        year={currentParams.year}
      >
        <div className="h-screen xl:flex bg-[#D9D9D933] overflow-hidden">
          <AppSidebar />
          <Backdrop />

          <div
            className={`flex-1 overflow-x-hidden transition-all duration-300 ease-in-out ${mainContentMargin}`}
          >
            <AppHeader />

            <div
              className={`flex flex-col h-[calc(100vh-64px)] ${
                isChatbotPage ? "overflow-hidden" : "overflow-y-auto"
              }`}
            >
              <div
                className={`p-3 sm:p-4 lg:p-3 xl:p-5 border-l border-t border-gray-200 ${
                  isChatbotPage ? "h-full min-h-0 overflow-hidden" : ""
                }`}
              >
                {children}
              </div>
            </div>
          </div>

          <ChatbotWidget hide={isPreviewMode} />
          {/* <PreviewModeNotice /> */}
          <AmazonAdsSuccessPopup />
        </div>
      </NotificationProvider>
    </AuthGuard>
  );
}
