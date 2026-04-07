// "use client";

// import AuthGuard from "@/components/auth/AuthGuard";
// import { useSidebar } from "@/context/SidebarContext";
// import AppHeader from "@/layout/AppHeader";
// import AppSidebar from "@/layout/AppSidebar";
// import Backdrop from "@/layout/Backdrop";
// import React from "react";
// import { useParams, useRouter } from "next/navigation";
// import ChatbotWidget from "@/components/chatbot/ChatbotWidget";
// import PreviewModeNotice from "@/components/amazon/PreviewModeNotice";

// export default function AdminLayout({
//   children,
// }: {
//   children: React.ReactNode;
// }) {
//   const { isExpanded, isHovered, isMobileOpen } = useSidebar();
//   const router = useRouter();

//   const currentParams = useParams() as {
//     ranged?: string;
//     countryName?: string;
//     month?: string;
//     year?: string;
//   };

//   const chatbotUrl = `/chatbot/${currentParams.ranged || "NA"}/${currentParams.countryName || "NA"
//     }/${currentParams.month || "NA"}/${currentParams.year || "NA"}`;

//   const showExpanded = isExpanded || isHovered;

//   // ✅ These MUST match your AppSidebar widths
//   const expandedMargin =
//     "lg:ml-[clamp(155px,13vw,210px)] xl:ml-[clamp(180px,16vw,250px)]";
//   const collapsedMargin = "lg:ml-[56px] sm:lg:ml-[64px] xl:ml-[72px]";

//   const mainContentMargin = isMobileOpen
//     ? "ml-0"
//     : showExpanded
//       ? expandedMargin
//       : collapsedMargin;

//   return (
//     <AuthGuard>
//       <div className="h-screen xl:flex bg-[#D9D9D933] overflow-hidden">
//         <AppSidebar />
//         <Backdrop />

//         {/* ✅ Use mainContentMargin here (no duplicate logic) */}
//         <div
//           className={`flex-1 overflow-x-hidden transition-all duration-300 ease-in-out ${mainContentMargin}`}
//         >
//           <AppHeader />

//           <div className="flex flex-col h-[calc(100vh-64px)] overflow-y-auto">
//             <div className="p-3 sm:p-4 lg:p-3 xl:p-5 border-l border-t border-gray-200">
//               {children}
//             </div>
//           </div>
//         </div>

//         <ChatbotWidget />
//         {/* <PreviewModeNotice /> */}
//       </div>
//     </AuthGuard>
//   );
// }





























"use client";

import AuthGuard from "@/components/auth/AuthGuard";
import { useSidebar } from "@/context/SidebarContext";
import AppHeader from "@/layout/AppHeader";
import AppSidebar from "@/layout/AppSidebar";
import Backdrop from "@/layout/Backdrop";
import React from "react";
import { useParams } from "next/navigation";
import ChatbotWidget from "@/components/chatbot/ChatbotWidget";
import PreviewModeNotice from "@/components/amazon/PreviewModeNotice";
import { useGetUserDataQuery } from "@/lib/api/profileApi";

export default function AdminLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const { isExpanded, isHovered, isMobileOpen } = useSidebar();
  const { data: userData } = useGetUserDataQuery();

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

  return (
    <AuthGuard>
      <div className="h-screen xl:flex bg-[#D9D9D933] overflow-hidden">
        <AppSidebar />
        <Backdrop />

        <div
          className={`flex-1 overflow-x-hidden transition-all duration-300 ease-in-out ${mainContentMargin}`}
        >
          <AppHeader />

          <div className="flex flex-col h-[calc(100vh-64px)] overflow-y-auto">
            <div className="p-3 sm:p-4 lg:p-3 xl:p-5 border-l border-t border-gray-200">
              {children}
            </div>
          </div>
        </div>

        <ChatbotWidget hide={isPreviewMode} />
        {/* <PreviewModeNotice /> */}
      </div>
    </AuthGuard>
  );
}