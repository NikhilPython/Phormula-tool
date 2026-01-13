// "use client";

// import PageBreadcrumb from "@/components/common/PageBreadCrumb";
// import React, { useEffect, useState } from "react";
// import { createPortal } from "react-dom";
// import { useSelector } from "react-redux";
// import { useRouter } from "next/navigation";

// type Provider = "amazon" | "shopify";

// type Props = {
//   open: boolean;
//   onClose: () => void;
//   onAmazonClick?: () => void; 
//    onShopifyClick?: () => void; 
// };

// const options: { key: Provider; title: string; icon: string }[] = [
//   { key: "amazon", title: "Amazon ", icon: "/amazon.png" },
//   { key: "shopify", title: "Shopify ", icon: "/shopify.png" },
// ];

// const IntegrationsModal: React.FC<Props> = ({
//   open,
//   onClose,
//   onAmazonClick,
//   onShopifyClick,
// }) => {
//   const [mounted, setMounted] = useState(false);
//   const router = useRouter();
//   const reduxToken = useSelector((state: any) => state.auth?.token);
//   const [shopifyStore, setShopifyStore] = useState<any | null>(null);
//   const [shopifyLoading, setShopifyLoading] = useState(false);
//   const isShopifyConnected = !!(shopifyStore && shopifyStore.access_token);

//   useEffect(() => {
//     setMounted(true);
//   }, []);

//   useEffect(() => {
//     const fetchShopifyStore = async () => {
//       if (!reduxToken) return;

//       try {
//         setShopifyLoading(true);

//         const res = await fetch(
//           `${process.env.NEXT_PUBLIC_API_BASE_URL}/shopify/store`,
//           {
//             headers: {
//               Authorization: `Bearer ${reduxToken}`,
//             },
//           }
//         );

//         const contentType = res.headers.get("content-type") || "";
//         if (!contentType.includes("application/json")) return;

//         const data = await res.json();
//         console.log("Shopify store (header modal):", data);

//         if (!res.ok || data?.error) return;

//         setShopifyStore(data);
//       } catch (err) {
//         console.error(
//           "Error fetching Shopify store in IntegrationsModal:",
//           err
//         );
//       } finally {
//         setShopifyLoading(false);
//       }
//     };

//     fetchShopifyStore();
//   }, [reduxToken]);

//   if (!open || !mounted) return null;

//   const buildShopifyOrdersUrl = () => {
//     if (!shopifyStore?.shop_name || !shopifyStore?.access_token) {
//       return "/orders";
//     }

//     const params = new URLSearchParams({
//       shop: shopifyStore.shop_name,
//       token: shopifyStore.access_token,
//     });

//     if (shopifyStore.email) params.set("email", shopifyStore.email);

//     return `/orders?${params.toString()}`;
//   };

//   const handleChoose = (key: Provider) => {
//     if (key === "amazon") {
//       onAmazonClick?.();
//       onClose();
//       return;
//     }

//     if (key === "shopify") {
//       if (isShopifyConnected) {
//         const url = buildShopifyOrdersUrl();
//         console.log(
//           "Header modal: Shopify already connected, redirecting to:",
//           url
//         );
//         router.push(url);
//         onClose();
//         return;
//       }

//       // ❌ not connected → HEADER flow: open ShopifyIntroModal via parent
//       onShopifyClick?.();
//       onClose();
//       return;
//     }
//   };

//   const locked = false;

//   const modalContent = (
//     <div
//       className="fixed inset-0 z-[9999] flex items-center justify-center p-4"
//       role="dialog"
//       aria-modal="true"
//     >
//       {/* Backdrop */}
//       <div
//         className="absolute inset-0 bg-black/40 backdrop-blur-sm"
//         onClick={onClose}
//       />

//       {/* Modal box */}
//       <div className="relative z-10 w-full max-w-lg rounded-xl border border-gray-200 bg-white px-5 py-8 shadow-2xl dark:border-gray-800 dark:bg-gray-900">
//         <PageBreadcrumb
//           pageTitle="Select Your Integration"
//           variant="table"
//           textSize="2xl"
//         />

//         {/* Integration selection block */}
//         <div className="flex flex-wrap justify-center gap-6 sm:gap-10">
//           {options.map((opt) => (
//             <button
//               key={opt.key}
//               type="button"
//               disabled={locked || (opt.key === "shopify" && shopifyLoading)}
//               onClick={() =>
//                 !locked && !shopifyLoading && handleChoose(opt.key)
//               }
//               title={opt.title}
//               aria-label={opt.title}
//               className={`flex flex-col items-center justify-center 
//                 w-36 h-36 sm:w-40 sm:h-40
//                 rounded-2xl border border-[#5EA68E] bg-white
//                 transition-all 
//                 ${
//                   locked
//                     ? "cursor-not-allowed opacity-60"
//                     : "hover:bg-emerald-50 hover:scale-105"
//                 }`}
//             >
//               <img
//                 src={opt.icon}
//                 alt={opt.title}
//                 className="h-16 w-16 sm:h-20 sm:w-20 object-contain mb-3"
//               />
//               <span className="text-sm sm:text-base font-semibold text-charcoal-500 whitespace-nowrap">
//                 {opt.title}
//               </span>
//             </button>
//           ))}
//         </div>
//       </div>
//     </div>
//   );

//   return createPortal(modalContent, document.body);
// };

// export default IntegrationsModal;




















"use client";

import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import React, { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import { useSelector } from "react-redux";
import { useRouter } from "next/navigation";

type Provider = "amazon" | "shopify";

type Props = {
  open: boolean;
  onClose: () => void;
  onConnected?: () => void;
};

const options: { key: Provider; title: string; icon: string }[] = [
  { key: "amazon", title: "Amazon ", icon: "/amazon.png" },
  { key: "shopify", title: "Shopify ", icon: "/shopify.png" },
];

const IntegrationsModal: React.FC<Props> = ({ open, onClose, onConnected }) => {
  const [mounted, setMounted] = useState(false);
  const router = useRouter();
  const reduxToken = useSelector((state: any) => state.auth?.token);
  const [shopifyStore, setShopifyStore] = useState<any | null>(null);
  const [shopifyLoading, setShopifyLoading] = useState(false);
  const isShopifyConnected = !!(shopifyStore && shopifyStore.access_token);

  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    const fetchShopifyStore = async () => {
      if (!reduxToken) return;

      try {
        setShopifyLoading(true);

        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/shopify/store`,
          {
            headers: {
              Authorization: `Bearer ${reduxToken}`,
            },
          }
        );

        const contentType = res.headers.get("content-type") || "";
        if (!contentType.includes("application/json")) return;

        const data = await res.json();
        console.log("Shopify store (header modal):", data);

        if (!res.ok || data?.error) return;

        setShopifyStore(data);
      } catch (err) {
        console.error(
          "Error fetching Shopify store in IntegrationsModal:",
          err
        );
      } finally {
        setShopifyLoading(false);
      }
    };

    fetchShopifyStore();
  }, [reduxToken]);

  if (!open || !mounted) return null;

  const buildShopifyOrdersUrl = () => {
    if (!shopifyStore?.shop_name || !shopifyStore?.access_token) {
      return "/orders";
    }

    const params = new URLSearchParams({
      shop: shopifyStore.shop_name,
      token: shopifyStore.access_token,
    });

    if (shopifyStore.email) params.set("email", shopifyStore.email);

    return `/orders?${params.toString()}`;
  };

  const handleChoose = (key: Provider) => {
    // Shopify header: if already connected, go straight to orders
    if (key === "shopify" && isShopifyConnected) {
      const url = buildShopifyOrdersUrl();
      console.log(
        "Header modal: Shopify already connected, redirecting to:",
        url
      );
      router.push(url);
      onClose();
      onConnected?.();
      return;
    }

    // For everything else, delegate to IntegrationDashboard
    window.dispatchEvent(
      new CustomEvent("integration:choose", {
        detail: { provider: key },
      })
    );

    onClose();
  };

  const locked = false;

  const modalContent = (
    <div
      className="fixed inset-0 z-[9999] flex items-center justify-center p-4"
      role="dialog"
      aria-modal="true"
      onClick={onClose}
    >
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/40 "
        onClick={onClose}
      />

      {/* Modal box */}
      <div className="relative z-10 w-full max-w-md rounded-xl border border-gray-200 bg-white px-1 py-7 shadow-2xl dark:border-gray-800 dark:bg-gray-900">
        <PageBreadcrumb
          pageTitle="Select Your Integration"
          variant="table"
          textSize="2xl"
        />

        {/* Integration selection block */}
        <div className="flex flex-col sm:flex-row justify-center items-center gap-6 sm:gap-10 mt-6">
          {options.map((opt) => (
            <button
              key={opt.key}
              type="button"
              disabled={locked || (opt.key === "shopify" && shopifyLoading)}
              onClick={() =>
                !locked && !shopifyLoading && handleChoose(opt.key)
              }
              title={opt.title}
              aria-label={opt.title}
              className={`flex flex-col items-center justify-center 
                w-36 h-36 sm:w-40 sm:h-40
                rounded-2xl border border-[#5EA68E] bg-white
                transition-all 
                ${
                  locked
                    ? "cursor-not-allowed opacity-60"
                    : "hover:bg-emerald-50 hover:scale-105"
                }`}
            >
              <img
                src={opt.icon}
                alt={opt.title}
                className="h-16 w-16 sm:h-20 sm:w-20 object-contain mb-3"
              />
              <span className="text-sm sm:text-base font-semibold text-charcoal-500 whitespace-nowrap">
                {opt.title}
              </span>
            </button>
          ))}
        </div>
      </div>
    </div>
  );

  return createPortal(modalContent, document.body);
};

export default IntegrationsModal;
