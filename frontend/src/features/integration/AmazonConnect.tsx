// "use client";

// import React, { useEffect, useMemo, useRef, useState } from "react";
// import { FaLink } from "react-icons/fa";
// import AmazonConnectLegacy from "./AmazonConnectLegacy";
// import Button from "@/components/ui/button/Button";
// import { TiTick } from "react-icons/ti";

// const API_BASE =
//   process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

// const getAuthToken = () =>
//   typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

// async function api(path: string, options: RequestInit = {}) {
//   const token = getAuthToken();
//   const res = await fetch(`${API_BASE}${path}`, {
//     ...options,
//     headers: {
//       "Content-Type": "application/json",
//       ...(options.headers || {}),
//       ...(token ? { Authorization: `Bearer ${token}` } : {}),
//     },
//   });
//   const data = await res.json().catch(() => ({}));
//   if (!res.ok)
//     throw new Error(
//       (data as any)?.error || (data as any)?.message || `HTTP ${res.status}`
//     );
//   return data;
// }

// const MARKETPLACE_BY_REGION: Record<string, string> = {
//   "us-east-1": "ATVPDKIKX0DER",
//   "eu-west-1": "A1F83G8C2ARO7P",
//   "ap-southeast-1": "A1VC38T7YXB528",
// };

// const ICONS = {
//   amazonLogo: "/amazon.png",
//   back: "/BackArrow.png",
//   tick: "/Tick_small.png",
//   secure: "/secure_black.png",
// };

// type Props = {
//   onClose?: () => void;
//   onConnected?: (refreshToken?: string) => void;
//   onChooseManual?: () => void;
// };

// export default function AmazonConnect({
//   onClose,
//   onConnected,
//   onChooseManual,
// }: Props) {
//   const [region, setRegion] = useState("eu-west-1");
//   const [isConnecting, setIsConnecting] = useState(false);
//   const [error, setError] = useState("");
//   const [message, setMessage] = useState("");

//   // Switch to legacy modal (as in your current flow)
//   const [showLegacy, setShowLegacy] = useState(false);

//   const popupRef = useRef<Window | null>(null);
//   const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);

//   const marketplaceId = useMemo(
//     () => MARKETPLACE_BY_REGION[region] || MARKETPLACE_BY_REGION["us-east-1"],
//     [region]
//   );

//   const stopPolling = () => {
//     if (pollingRef.current) {
//       clearInterval(pollingRef.current);
//       pollingRef.current = null;
//     }
//   };

//   const closePopup = () => {
//     try {
//       if (popupRef.current && !popupRef.current.closed) popupRef.current.close();
//     } catch { }
//     popupRef.current = null;
//   };

//   const finalizeConnection = () => {
//     setMessage("Connected to Amazon ✅");
//     stopPolling();
//     closePopup();
//     onConnected?.();
//     onClose?.();
//   };

//   const handleStatusPoll = async () => {
//     try {
//       const qs = new URLSearchParams({ region }).toString();
//       const s = await api(`/amazon_api/status?${qs}`);
//       if ((s as any)?.success) finalizeConnection();
//     } catch { }
//   };

//   const handleAmazonLogin = async () => {
//     setError("");
//     setMessage("");
//     setIsConnecting(true);
//     try {
//       const qs = new URLSearchParams({
//         region,
//         marketplace_id: marketplaceId,
//       }).toString();
//       const data = await api(`/amazon_api/login?${qs}`);

//       const msg = String((data as any)?.message || "").toLowerCase();
//       if ((data as any)?.success && msg.includes("refresh")) {
//         finalizeConnection();
//         return;
//       }

//       const authUrl = (data as any)?.auth_url;
//       if (!authUrl) throw new Error("Failed to get Amazon login URL");

//       const w = window.open(authUrl, "amazon_oauth", "width=720,height=800");
//       if (!w) throw new Error("Popup blocked. Please allow popups for this site.");
//       popupRef.current = w;

//       stopPolling();
//       pollingRef.current = setInterval(handleStatusPoll, 2000);
//     } catch (e: any) {
//       setError(e.message || "Error connecting to Amazon login.");
//       stopPolling();
//       closePopup();
//     } finally {
//       setIsConnecting(false);
//     }
//   };

//   useEffect(() => {
//     const onMessage = async (event: MessageEvent) => {
//       const { data } = event || {};
//       if (!data || typeof data !== "object") return;

//       if ((data as any).type === "amazon_oauth_success") {
//         try {
//           await handleStatusPoll();
//         } catch (e: any) {
//           setError(e.message || "Post-auth follow-up failed.");
//         }
//       }
//       if ((data as any).type === "amazon_oauth_error") {
//         stopPolling();
//         closePopup();
//         setError((data as any).error || "Amazon connection failed.");
//       }
//     };
//     window.addEventListener("message", onMessage);
//     return () => {
//       window.removeEventListener("message", onMessage);
//       stopPolling();
//       closePopup();
//     };
//   }, [region, marketplaceId]);

//   // Show legacy flow if toggled
//   if (showLegacy) {
//     return (
//       <AmazonConnectLegacy
//         onClose={onClose}
//         onConnected={onConnected}
//       />
//     );
//   }

//   return (
//     <div
//       className="fixed inset-0 z-99999 flex items-center justify-center bg-black/50 p-4"
//       role="dialog"
//       aria-modal="true"
//       aria-labelledby="amazon-connect-title"
//       onClick={onClose} 
//     >
//       <div className="relative w-full max-w-xl bg-white rounded-2xl shadow-xl p-6 sm:p-8"
//       onClick={(e) => e.stopPropagation()}
//       >
//     {/* Header: back + logo */}
//         <div className="flex items-center justify-between relative">
//           <div className="flex justify-center w-full mt-2">
//             <img src={ICONS.amazonLogo} alt="Amazon" className="w-16 h-16 mx-auto" />
//           </div>
//         </div>

//         <h2 id="amazon-connect-title" className="mt-3 text-center text-[26px] font-semibold text-[#5EA68E]">
//           Connect Amazon Account
//         </h2>
//         <p className="text-center text-sm text-[#414042] mb-6 font-bold w-[80%] m-auto">
//           Sync your Amazon Seller Central data to access analytics and insights
//         </p>

//         <div className="w-full border-t border-gray-300 mb-6" />

//         <div className="rounded-lg border border-[#5EA68E26] bg-emerald-50/50 p-4 mb-5">
//           <div className="flex items-center gap-3 mb-3">
//             <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-[#D9D9D9]">
//               <FaLink size={18} color="#5EA68E" />
//             </div>
//             <div className="flex flex-col">
//               <h3 className="font-semibold text-[#414042] text-sm sm:text-base leading-tight">
//                 Link your Amazon Account
//               </h3>
//               <p className="text-xs sm:text-sm text-[#5EA68E] mt-0.5">Setup in 30 seconds</p>
//             </div>
//           </div>

//           <ul className="text-sm text-[#414042] space-y-3 mb-3 ml-4">
//             {[
//               "Real-time data synchronization",
//               "Always up-to-date analytics",
//               "Secure encrypted connection",
//               "No manual work required",
//             ].map((text) => (
//               <li key={text} className="flex items-start gap-2">
//                 {/* <img src={ICONS.tick} alt="tick" className="w-3 h-3 mt-[2px] flex-shrink-0" /> */}
//                 <TiTick className="w-5 h-5 text-green-500"/>
//                 <span>{text}</span>
//               </li>
//             ))}
//           </ul>

//           <Button
//             type="button"
//             variant="primary"
//             size="sm"
//             // onClick={handleAmazonLogin} // original
//             onClick={() => setShowLegacy(true)} // show legacy modal
//             disabled={isConnecting}
//             className={`w-full 
//               ${isConnecting ? "bg-blue-700 cursor-not-allowed" : "bg-blue-700"}`}
//           >
//             <FaLink size={16} />
//             <span className="text-[#F8EDCE]">
//               {isConnecting ? "Connecting..." : "Connect"}
//             </span>
//           </Button>
//         </div>

//         {/* OR */}
//         <div className="flex items-center justify-center w-full mt-5 mb-4">
//           <div className="flex-grow border-t border-gray-300" />
//           <span className="mx-3 text-sm text-[#414042] font-medium">or</span>
//           <div className="flex-grow border-t border-gray-300" />
//         </div>

//         <div className="text-center mb-5">
//           <button
//             type="button"
//             onClick={() => onChooseManual?.()}
//             className="mt-1 text-[#414042] text-sm font-medium"
//           >
//             Set up manual file uploads instead &raquo;
//           </button>
//         </div>

//         <div className="w-full border-t border-gray-300 mb-5" />

//         <div className="flex items-center justify-center gap-2 text-xs text-[#414042]">
//           <img src={ICONS.secure} alt="Secure" className="w-4 h-4 opacity-70" />
//           <span>Your credentials are encrypted and stored securely</span>
//         </div>

//         {message && (
//           <div className="mt-4 text-center text-emerald-700 text-sm bg-emerald-50 border border-emerald-200 rounded-md p-2">
//             {message}
//           </div>
//         )}
//         {error && (
//           <div className="mt-4 text-center text-red-700 text-sm bg-red-50 border border-red-200 rounded-md p-2">
//             {error}
//           </div>
//         )}
//       </div>
//     </div>
//   );
// }



























"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import { FaLink } from "react-icons/fa";
import AmazonConnectLegacy from "./AmazonConnectLegacy";
import Button from "@/components/ui/button/Button";
import { TiTick } from "react-icons/ti";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

const getAuthToken = () =>
  typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

async function api(path: string, options: RequestInit = {}) {
  const token = getAuthToken();
  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok)
    throw new Error(
      (data as any)?.error || (data as any)?.message || `HTTP ${res.status}`
    );
  return data;
}

const MARKETPLACE_BY_REGION: Record<string, string> = {
  "us-east-1": "ATVPDKIKX0DER",
  "eu-west-1": "A1F83G8C2ARO7P",
  "ap-southeast-1": "A1VC38T7YXB528",
};

const ICONS = {
  amazonLogo: "/amazon.png",
  back: "/BackArrow.png",
  tick: "/Tick_small.png",
  secure: "/secure_black.png",
};

type Props = {
  onClose?: () => void;
  onConnected?: (refreshToken?: string) => void;
  onChooseManual?: () => void;
};

export default function AmazonConnect({
  onClose,
  onConnected,
  onChooseManual,
}: Props) {
  const [region, setRegion] = useState("eu-west-1");
  const [isConnecting, setIsConnecting] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const [showLegacy, setShowLegacy] = useState(false);

  const popupRef = useRef<Window | null>(null);
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const marketplaceId = useMemo(
    () => MARKETPLACE_BY_REGION[region] || MARKETPLACE_BY_REGION["us-east-1"],
    [region]
  );

  const stopPolling = () => {
    if (pollingRef.current) {
      clearInterval(pollingRef.current);
      pollingRef.current = null;
    }
  };

  const closePopup = () => {
    try {
      if (popupRef.current && !popupRef.current.closed) popupRef.current.close();
    } catch {}
    popupRef.current = null;
  };

  const finalizeConnection = () => {
    setMessage("Connected to Amazon ✅");
    stopPolling();
    closePopup();
    onConnected?.();
    onClose?.();
  };

  const handleStatusPoll = async () => {
    try {
      const qs = new URLSearchParams({ region }).toString();
      const s = await api(`/amazon_api/status?${qs}`);
      if ((s as any)?.success) finalizeConnection();
    } catch {}
  };

  const handleAmazonLogin = async () => {
    setError("");
    setMessage("");
    setIsConnecting(true);
    try {
      const qs = new URLSearchParams({
        region,
        marketplace_id: marketplaceId,
      }).toString();
      const data = await api(`/amazon_api/login?${qs}`);

      const msg = String((data as any)?.message || "").toLowerCase();
      if ((data as any)?.success && msg.includes("refresh")) {
        finalizeConnection();
        return;
      }

      const authUrl = (data as any)?.auth_url;
      if (!authUrl) throw new Error("Failed to get Amazon login URL");

      const w = window.open(authUrl, "amazon_oauth", "width=720,height=800");
      if (!w) throw new Error("Popup blocked. Please allow popups for this site.");
      popupRef.current = w;

      stopPolling();
      pollingRef.current = setInterval(handleStatusPoll, 2000);
    } catch (e: any) {
      setError(e.message || "Error connecting to Amazon login.");
      stopPolling();
      closePopup();
    } finally {
      setIsConnecting(false);
    }
  };

  useEffect(() => {
    const onMessage = async (event: MessageEvent) => {
      const { data } = event || {};
      if (!data || typeof data !== "object") return;

      if ((data as any).type === "amazon_oauth_success") {
        try {
          await handleStatusPoll();
        } catch (e: any) {
          setError(e.message || "Post-auth follow-up failed.");
        }
      }
      if ((data as any).type === "amazon_oauth_error") {
        stopPolling();
        closePopup();
        setError((data as any).error || "Amazon connection failed.");
      }
    };
    window.addEventListener("message", onMessage);
    return () => {
      window.removeEventListener("message", onMessage);
      stopPolling();
      closePopup();
    };
  }, [region, marketplaceId]);

  if (showLegacy) {
    return <AmazonConnectLegacy onClose={onClose} onConnected={onConnected} />;
  }

  return (
    <div
      className="fixed inset-0 z-99999 flex items-center justify-center bg-black/50 p-3 sm:p-4 md:p-6"
      role="dialog"
      aria-modal="true"
      aria-labelledby="amazon-connect-title"
      onClick={onClose}
    >
      <div
  className="
    relative w-full max-w-md sm:max-w-lg md:max-w-xl
    bg-white rounded-2xl shadow-xl
    p-4 sm:p-6 md:p-8
  "
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header: logo */}
        <div className="flex items-center justify-between relative">
          <div className="flex justify-center w-full mt-1 sm:mt-2">
            <img
              src={ICONS.amazonLogo}
              alt="Amazon"
              className="w-12 h-12 sm:w-14 sm:h-14 md:w-16 md:h-16 mx-auto"
            />
          </div>
        </div>

        {/* <h2
          id="amazon-connect-title"
          className="
            mt-2 sm:mt-3 text-center
            text-lg sm:text-xl md:text-2xl
            font-semibold text-[#5EA68E]
          "
        >
          Connect Amazon Account
        </h2> */}

        <PageBreadcrumb pageTitle="Connect Amazon Account" align="center" variant="table" textSize="2xl" />

        <p
          className="
            text-center
            text-xs sm:text-sm md:text-base
            text-[#414042] font-bold
            mt-2 mb-4 sm:mb-5 md:mb-6
            w-[90%] sm:w-[80%] m-auto
          "
        >
          Sync your Amazon Seller Central data to access analytics and insights
        </p>

        <div className="w-full border-t border-gray-300 mb-4 sm:mb-6" />

        <div
          className="
            rounded-lg border border-[#5EA68E26]
            bg-emerald-50/50
            p-3 sm:p-4 mb-4 sm:mb-5
          "
        >
          <div className="flex items-center gap-3 mb-2 sm:mb-3">
            <div className="flex items-center justify-center w-9 h-9 sm:w-10 sm:h-10 rounded-lg bg-[#D9D9D9]">
              <FaLink size={16} color="#5EA68E" />
            </div>
            <div className="flex flex-col">
              <h3 className="font-semibold text-[#414042] text-xs sm:text-sm md:text-base leading-tight">
                Link your Amazon Account
              </h3>
              <p className="text-[10px] sm:text-xs md:text-sm text-[#5EA68E] mt-0.5">
                Setup in 30 seconds
              </p>
            </div>
          </div>

          <ul className="text-xs sm:text-sm text-[#414042] space-y-2 sm:space-y-3 mb-3 sm:mb-4 ml-3 sm:ml-4">
            {[
              "Real-time data synchronization",
              "Always up-to-date analytics",
              "Secure encrypted connection",
              "No manual work required",
            ].map((text) => (
              <li key={text} className="flex items-start gap-2">
                <TiTick className="w-4 h-4 sm:w-5 sm:h-5 text-green-500 mt-[1px]" />
                <span>{text}</span>
              </li>
            ))}
          </ul>

          <Button
            type="button"
            variant="primary"
            size="sm"
            // onClick={handleAmazonLogin}
            onClick={() => setShowLegacy(true)}
            disabled={isConnecting}
            className={`w-full ${
              isConnecting ? "bg-blue-700 cursor-not-allowed" : "bg-blue-700"
            }`}
          >
            <FaLink size={14} />
            <span className="text-[#F8EDCE] text-xs sm:text-sm">
              {isConnecting ? "Connecting..." : "Connect"}
            </span>
          </Button>
        </div>

        {/* OR */}
        <div className="flex items-center justify-center w-full mt-4 sm:mt-5 mb-3 sm:mb-4">
          <div className="flex-grow border-t border-gray-300" />
          <span className="mx-3 text-xs sm:text-sm text-[#414042] font-medium">
            or
          </span>
          <div className="flex-grow border-t border-gray-300" />
        </div>

        <div className="text-center mb-4 sm:mb-5">
          <button
            type="button"
            onClick={() => onChooseManual?.()}
            className="mt-1 text-[#414042] text-xs sm:text-sm font-medium"
          >
            Set up manual file uploads instead &raquo;
          </button>
        </div>

        <div className="w-full border-t border-gray-300 mb-4 sm:mb-5" />

        <div className="flex items-center justify-center gap-2 text-[10px] sm:text-xs md:text-sm text-[#414042]">
          <img
            src={ICONS.secure}
            alt="Secure"
            className="w-3 h-3 sm:w-4 sm:h-4 opacity-70"
          />
          <span>Your credentials are encrypted and stored securely</span>
        </div>

        {message && (
          <div
            className="
              mt-3 sm:mt-4 text-center text-xs sm:text-sm
              text-emerald-700 bg-emerald-50 border border-emerald-200
              rounded-md p-2
            "
          >
            {message}
          </div>
        )}
        {error && (
          <div
            className="
              mt-3 sm:mt-4 text-center text-xs sm:text-sm
              text-red-700 bg-red-50 border border-red-200
              rounded-md p-2
            "
          >
            {error}
          </div>
        )}
      </div>
    </div>
  );
}
