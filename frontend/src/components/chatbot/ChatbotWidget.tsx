"use client";
import { useState, useEffect  } from "react";
import ChatbotCore from "./ChatbotCore";
import { RiExpandDiagonalSLine } from "react-icons/ri";
import { useRouter, useParams, usePathname } from "next/navigation";
import { useGetUserDataQuery } from "@/lib/api/profileApi";

export default function ChatbotWidget() {
  const [open, setOpen] = useState(false);
 const { data: userData } = useGetUserDataQuery();

  const router = useRouter();
  const params = useParams();
   const pathname = usePathname(); // 👈 important
  
  const ranged = params?.ranged;
  const countryName = params?.countryName;
  const month = params?.month;
  const year = params?.year;

   if (pathname?.startsWith("/chatbot")) {
    return null;
  }

  return (
    <>
      {/* Floating Icon */}
      <button
        onClick={() => setOpen(true)}
        className="fixed bottom-4 right-4 z-[9999]
                   hover:scale-105 transition-transform"
      >
        <img
          src="/Chatbot.png"
          alt="Chatbot"
          className="w-16 h-16 object-contain"
        />
      </button>

      {/* Overlay */}
      {open && (
        <div className="fixed inset-0 z-[10000] pointer-events-none">
          
          {/* Chat Window */}
          <div
            className="
              pointer-events-auto
              fixed bottom-20 right-4
              w-[360px] sm:w-[380px]
              h-[520px]
              bg-white
              rounded-2xl
              shadow-[0_20px_50px_rgba(0,0,0,0.25)]
              flex flex-col
              animate-chat-open
            "
          >
            {/* Header */}
           <div
  style={{
    background:
      "linear-gradient(180deg, #5EA68E 12.02%, #37455F 100%)",
  }}
  className="
    flex items-center justify-between
    px-4 py-3
    text-white rounded-t-2xl
  "
>
              <div>
                <p className="font-semibold text-base text-[#F8EDCE] ">Hi {userData?.company_name || "there"} </p>
                <p className="text-[11px] opacity-90">
                  Analytics Assistant
                </p>
              </div>
              <div className='flex gap-4 items-center cursor-pointer'>
              <RiExpandDiagonalSLine
  size={20}
  onClick={() => {
    const now = new Date();

    const finalRanged = ranged ?? "QTD";
    const finalCountry = countryName ?? "uk";
    const finalMonth = month ?? String(now.getMonth() + 1).padStart(2, "0");
    const finalYear = year ?? String(now.getFullYear());

    const newPath = `/chatbot/${finalRanged}/${finalCountry}/${finalMonth}/${finalYear}`;
    router.push(newPath);
  }}
/>
 <button
                onClick={() => setOpen(false)}
                className="text-xl leading-none hover:opacity-80 "
              >
                ✕
              </button>
              </div>
             
            </div>

            {/* Chat */}
            <ChatbotCore />
          </div>
        </div>
      )}
    </>
  );
}
