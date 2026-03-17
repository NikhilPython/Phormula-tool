// // features/integration/IntegrationDashboard.tsx
// "use client";

// import React, { useMemo, useState, useEffect } from "react";
// import { useParams } from "next/navigation";
// import { useRouter } from "next/navigation";
// import { useIntegrationProgress, LS_KEYS } from "./useIntegrationProgress";

// import { Step1ProductList } from "./steps/Step1ProductList";
// import { Step2Integration } from "./steps/Step2Integration";
// import { Step3FeePreview } from "./steps/Step3FeePreview";
// import { Step4MTD } from "./steps/Step4MTD";
// import { Step3AmazonFinancial } from "./steps/Step3AmazonFinancial";

// import SkuMultiCountryUpload from "@/components/ui/modal/SkuMultiCountryUpload";
// import FeepreviewUpload from "@/components/ui/modal/FeepreviewUpload";

// import AmazonConnect from "./AmazonConnect";
// import AmazonConnectLegacy from "./AmazonConnectLegacy";
// import { Modal } from "@/components/ui/modal";
// import FileUploadForm from "@/app/(admin)/(ui-elements)/modals/FileUploadForm";
// import ConnectShopifyModal from "./ConnectShopifyModal";
// import ShopifyIntroModal from "./ShopifyIntroModal";
// import AmazonFinancialDashboard from "./AmazonFinancialDashboard";
// import { useSelector } from "react-redux";
// import PageBreadcrumb from "@/components/common/PageBreadCrumb";

// type Origin = "header" | "page";
// type Provider = "amazon" | "shopify";

// type IntegrationDashboardProps = {
//   open?: boolean;
//   onClose?: () => void;
// };

// export default function IntegrationDashboard(_: IntegrationDashboardProps) {
//   const router = useRouter();
//   const { countryName } = useParams<{ countryName: string }>();
//   const selectedCountry = (countryName || "").toLowerCase();

//   const [shopifyStore, setShopifyStore] = useState<any | null>(null);
//   const [shopifyLoading, setShopifyLoading] = useState(false);
//   const isShopifyConnected = !!(shopifyStore && shopifyStore.access_token);

//   const [shopifyStage, setShopifyStage] = useState<"none" | "intro" | "manual">(
//     "none"
//   );

//   const {
//     fileUploaded,
//     profileExists,
//     integrationMethod,
//     setIntegrationMethod,
//     setAmazonConnected,
//     amazonConnected,
//     mtdUploaded,
//     setMtdUploaded,
//     refetchFileStatus,
//   } = useIntegrationProgress(selectedCountry);

//   const [activePopup, setActivePopup] = useState<number | null>(null);
//   const [showAmazonConnect, setShowAmazonConnect] = useState(false);
//   const [showAmazonLegacyConnect, setShowAmazonLegacyConnect] = useState(false);
//   const [openAmazonFinance, setOpenAmazonFinance] = useState(false);

//   const [mtdCountry, setMtdCountry] = useState<string>("");

//   const reduxToken = useSelector((state: any) => state.auth?.token);

//   // ✅ local in-session flag for SKU step completion
//   const [skuCompleted, setSkuCompleted] = useState(false);

// useEffect(() => {
//   const handler = (e: Event) => {
//     const custom = e as CustomEvent<{ provider: Provider; origin?: Origin }>;
//     const { provider, origin = "header" } = custom.detail || {};
//     if (!provider) return;

//     // Let dashboard handle BOTH header & page events
//     chooseIntegration(provider, origin);
//   };

//   window.addEventListener("integration:choose", handler as EventListener);
//   return () => {
//     window.removeEventListener("integration:choose", handler as EventListener);
//   };
// }, []);


//   // Fetch Shopify store info
//   useEffect(() => {
//     const fetchShopifyStore = async () => {
//       try {
//         setShopifyLoading(true);

//         const token = reduxToken; // from useSelector
//         console.log("JWT from Redux:", token);
//         console.log("API BASE URL:", process.env.NEXT_PUBLIC_API_BASE_URL);

//         if (!token) {
//           console.log("No JWT found in Redux store.");
//           return;
//         }

//         const res = await fetch(
//           `${process.env.NEXT_PUBLIC_API_BASE_URL}/shopify/store`,
//           {
//             headers: {
//               Authorization: `Bearer ${token}`,
//             },
//           }
//         );

//         const contentType = res.headers.get("content-type") || "";

//         // If backend failed and returned HTML, avoid res.json() crash
//         if (!contentType.includes("application/json")) {
//           const text = await res.text();
//           console.error("Non-JSON response from /shopify/store:", text);
//           return;
//         }

//         const data = await res.json();
//         console.log("Shopify store from backend:", data);

//         if (!res.ok || data?.error) {
//           return;
//         }

//         setShopifyStore(data);
//       } catch (err) {
//         console.error("Error fetching Shopify store:", err);
//       } finally {
//         setShopifyLoading(false);
//       }
//     };

//     fetchShopifyStore();
//   }, [reduxToken]);

//   // Close Amazon Finance modal on country / integration change
//   useEffect(() => {
//     setOpenAmazonFinance(false);
//   }, [selectedCountry, integrationMethod]);

//   const steps = useMemo(() => {
//     const step1Done = fileUploaded || skuCompleted;

//     const manual = [
//       {
//         id: 1,
//         completed: step1Done,
//         enabled: true,
//         action: () => setActivePopup(1),
//       },
//       { id: 2, completed: !!integrationMethod, enabled: step1Done },
//       {
//         id: 3,
//         completed: profileExists,
//         enabled: !!integrationMethod,
//         action: () => setActivePopup(2),
//       },
//       {
//         id: 4,
//         completed: mtdUploaded,
//         enabled: profileExists,
//         action: () => setActivePopup(3),
//       },
//     ];

//     const amazon = [
//       {
//         id: 1,
//         completed: step1Done,
//         enabled: true,
//         action: () => setActivePopup(1),
//       },
//       {
//         id: 2,
//         completed: !!integrationMethod && amazonConnected,
//         enabled: step1Done,
//       },
//       {
//         id: 3,
//         completed: mtdUploaded,
//         enabled: !!integrationMethod && amazonConnected,
//         action: () => setActivePopup(3),
//       },
//     ];

//     return integrationMethod === "amazon" ? amazon : manual;
//   }, [
//     integrationMethod,
//     amazonConnected,
//     fileUploaded,
//     skuCompleted,
//     profileExists,
//     mtdUploaded,
//   ]);

//   // shopify orders url
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

// const chooseIntegration = (key: Provider, origin: Origin = "page") => {
//   if (origin === "header" && key === "amazon") {
//     setShowAmazonLegacyConnect(true);
//     return;
//   }

//   setIntegrationMethod(key);
//   setActivePopup(null);

//   setShopifyStage("none");

//   if (key === "amazon") {
//     setShowAmazonConnect(true);
//     return;
//   }

//   if (key === "shopify") {
//     if (isShopifyConnected) {
//       const url = buildShopifyOrdersUrl();
//       console.log("Shopify already connected, redirecting to:", url);
//       router.push(url);
//       return;
//     }

//     console.log("No Shopify access token – opening Shopify intro modal");
//     setShopifyStage("intro");
//   }
// };


//   return (
//     <div className="font-lato bg-white box-border">
//       <PageBreadcrumb pageTitle="Start your Journey with Phormula!" variant="page" textSize="2xl" align="left" className="mb-4"/>

//       {/* Step 1 */}
//       <Step1ProductList
//         completed={steps[0].completed}
//         onOpen={() => steps[0].enabled && steps[0].action?.()}
//       />

//       {/* Step 2 */}
//       <Step2Integration
//         locked={!steps[1].enabled}
//         completed={steps[1].completed}
//         onChoose={(key) => chooseIntegration(key, "page")}
//       />

//       {/* Manual flow */}
//       {integrationMethod === "manual" && (
//         <>
//           <Step3FeePreview
//             enabled={steps[2].enabled}
//             completed={steps[2].completed}
//             onOpen={() => steps[2].enabled && steps[2].action?.()}
//           />

//           <Step4MTD
//             enabled={steps[3]?.enabled}
//             completed={steps[3]?.completed}
//             selectedCountry={selectedCountry}
//             onOpenForCountry={(code) => {
//               if (code !== selectedCountry) return;
//               setMtdCountry(code);
//               setActivePopup(3);
//             }}
//           />
//         </>
//       )}

//       {/* Amazon flow */}
//       {integrationMethod === "amazon" && (
//         <Step3AmazonFinancial
//           enabled={steps[2].enabled}
//           completed={steps[2].completed}
//           onOpen={() => setOpenAmazonFinance(true)}
//         />
//       )}

//       {/* Step 1 modal (SKU upload) */}
//       {activePopup === 1 && (
//         <Modal
//           isOpen
//           onClose={() => setActivePopup(null)}
//           className="m-4 max-w-xl"
//           showCloseButton
//         >
//           <div className="relative w-full rounded-3xl shadow-[6px_6px_7px_0px_#00000026] border border-[#D9D9D9] p-4 no-scrollbar dark:bg-gray-900 lg:p-11">
//             <SkuMultiCountryUpload
//               onClose={() => setActivePopup(null)}
//               onComplete={async () => {
//                 // mark step 1 done right away
//                 setSkuCompleted(true);

//                 // refresh backend status for future sessions
//                 try {
//                   await refetchFileStatus();
//                 } catch {
//                   /* ignore */
//                 }

//                 setActivePopup(null);
//               }}
//             />
//           </div>
//         </Modal>
//       )}

//       {/* Step 3 modal (Fee Preview) */}
//       {activePopup === 2 && (
//         <Modal
//           isOpen
//           onClose={() => setActivePopup(null)}
//           className="m-4 max-w-[800px]"
//           showCloseButton
//         >
//           <div className="relative w-full rounded-3xl bg-white p-4 no-scrollbar dark:bg-gray-900 lg:p-11">
//             <FeepreviewUpload
//               country={selectedCountry}
//               onClose={() => setActivePopup(null)}
//             />
//           </div>
//         </Modal>
//       )}

//       {/* Step 4 modal (MTD Upload) */}
//       {activePopup === 3 && (
//         <Modal
//           isOpen
//           onClose={() => setActivePopup(null)}
//           className="m-4 max-w-3xl"
//           showCloseButton
//         >
//           <FileUploadForm
//             initialCountry={mtdCountry || selectedCountry}
//             onClose={() => {
//               setActivePopup(null);
//             }}
//             onComplete={() => {
//               setMtdUploaded(true);
//               setActivePopup(null);
//             }}
//           />
//         </Modal>
//       )}

//       {/* Amazon connect (page flow) */}
//       {showAmazonConnect && (
//         <Modal
//           isOpen
//           onClose={() => setShowAmazonConnect(false)}
//           className="m-4 z-99999 max-w-xl"
//           showCloseButton
//         >
//           <AmazonConnect
//             onClose={() => setShowAmazonConnect(false)}
//             onConnected={(refreshToken?: string) => {
//               if (typeof window !== "undefined") {
//                 localStorage.setItem(
//                   LS_KEYS.amazonRefreshToken(selectedCountry),
//                   String(refreshToken ?? "")
//                 );
//               }
//               setAmazonConnected(true);
//               setShowAmazonConnect(false);
//             }}
//             onChooseManual={() => {
//               setShowAmazonConnect(false);
//               setIntegrationMethod("manual");
//             }}
//           />
//         </Modal>
//       )}

//       {/* Amazon LEGACY connect (header flow) */}
//       {showAmazonLegacyConnect && (
//         <Modal
//           isOpen
//           onClose={() => setShowAmazonLegacyConnect(false)}
//           className="m-4 z-99999 max-w-xl"
//           showCloseButton
//         >
//           <AmazonConnectLegacy
//             onClose={() => setShowAmazonLegacyConnect(false)}
//             onConnected={(refreshToken?: string) => {
//               if (typeof window !== "undefined") {
//                 localStorage.setItem(
//                   LS_KEYS.amazonRefreshToken(selectedCountry),
//                   String(refreshToken ?? "")
//                 );
//               }
//               setAmazonConnected(true);
//               setShowAmazonLegacyConnect(false);
//             }}
//           />
//         </Modal>
//       )}

//       {/* Amazon financial dashboard */}
//       {integrationMethod === "amazon" &&
//         amazonConnected &&
//         openAmazonFinance && (
//           <div
//             className="fixed inset-0 z-[100000] flex items-center justify-center p-4"
//             role="dialog"
//             aria-modal="true"
//           >
//             <div
//               className="absolute inset-0 bg-black/40 "
//             />
//             <div className="relative w-full max-w-xl rounded-xl bg-white p-4 dark:border-gray-800 dark:bg-gray-900">
//               <div className="mt-3">
//                 <AmazonFinancialDashboard
//                   onClose={() => setOpenAmazonFinance(false)}
//                 />
//               </div>
//             </div>
//           </div>
//       )}

//       {/* Shopify intro (first step) */}
//       {shopifyStage === "intro" && !isShopifyConnected && (
//         <ShopifyIntroModal
//           onClose={() => setShopifyStage("none")}
//           onManual={() => {
//             // go from intro → manual
//             setShopifyStage("manual");
//           }}
//         />
//       )}

//       {/* Shopify connect (manual store name) */}
//       {shopifyStage === "manual" && !isShopifyConnected && (
//         <ConnectShopifyModal onClose={() => setShopifyStage("none")} />
//       )}
//     </div>
//   );
// }








































// features/integration/IntegrationDashboard.tsx
"use client";

import React, { useMemo, useState, useEffect } from "react";
import { useParams } from "next/navigation";
import { useRouter } from "next/navigation";
import { useIntegrationProgress, LS_KEYS } from "./useIntegrationProgress";

import { Step1ProductList } from "./steps/Step1ProductList";
import { Step2Integration } from "./steps/Step2Integration";
import { Step3FeePreview } from "./steps/Step3FeePreview";
import { Step4MTD } from "./steps/Step4MTD";
import { Step3AmazonFinancial } from "./steps/Step3AmazonFinancial";

import SkuMultiCountryUpload from "@/components/ui/modal/SkuMultiCountryUpload";
import FeepreviewUpload from "@/components/ui/modal/FeepreviewUpload";

import AmazonConnect from "./AmazonConnect";
import AmazonConnectLegacy from "./AmazonConnectLegacy";
import { Modal } from "@/components/ui/modal";
import FileUploadForm from "@/app/(admin)/(ui-elements)/modals/FileUploadForm";
import ConnectShopifyModal from "./ConnectShopifyModal";
import ShopifyIntroModal from "./ShopifyIntroModal";
import AmazonFinancialDashboard from "./AmazonFinancialDashboard";
import { useSelector } from "react-redux";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

// ✅ NEW
import AmazonAdsConnectModal from "./AmazonAdsConnectModal";

type Origin = "header" | "page";
type Provider = "amazon" | "shopify";

type IntegrationDashboardProps = {
  open?: boolean;
  onClose?: () => void;
};

export default function IntegrationDashboard(_: IntegrationDashboardProps) {
  const router = useRouter();
  const { countryName } = useParams<{ countryName: string }>();
  const selectedCountry = (countryName || "").toLowerCase();

  const [shopifyStore, setShopifyStore] = useState<any | null>(null);
  const [shopifyLoading, setShopifyLoading] = useState(false);
  const isShopifyConnected = !!(shopifyStore && shopifyStore.access_token);

  const [shopifyStage, setShopifyStage] = useState<"none" | "intro" | "manual">(
    "none"
  );

  const {
    fileUploaded,
    profileExists,
    integrationMethod,
    setIntegrationMethod,
    setAmazonConnected,
    amazonConnected,
    mtdUploaded,
    setMtdUploaded,
    refetchFileStatus,
  } = useIntegrationProgress(selectedCountry);

  const [activePopup, setActivePopup] = useState<number | null>(null);
  const [showAmazonConnect, setShowAmazonConnect] = useState(false);
  const [showAmazonLegacyConnect, setShowAmazonLegacyConnect] = useState(false);
  const [openAmazonFinance, setOpenAmazonFinance] = useState(false);

  const [mtdCountry, setMtdCountry] = useState<string>("");

  const reduxToken = useSelector((state: any) => state.auth?.token);

  // ✅ local in-session flag for SKU step completion
  const [skuCompleted, setSkuCompleted] = useState(false);

  // ✅ NEW: Amazon Ads modal controls (minimal, no backend wiring required to open)
  const [showAmazonAdsConnect, setShowAmazonAdsConnect] = useState(false);
  const [adsStatusLoading] = useState(false);
  const [adsStatus] = useState<any | null>(null);
  const [adsConnecting, setAdsConnecting] = useState(false);
  const [adsError, setAdsError] = useState<string | null>(null);

  useEffect(() => {
    const handler = (e: Event) => {
      // accept any provider at runtime (so amazon_ads doesn't break)
      const custom = e as CustomEvent<{ provider: any; origin?: Origin }>;
      const { provider, origin = "header" } = custom.detail || {};
      if (!provider) return;

      // ✅ NEW: open Amazon Ads modal and do NOTHING else (no existing flow changes)
      if (provider === "amazon_ads") {
        console.log("Opening Amazon Ads modal from event:", custom.detail);
        setShowAmazonAdsConnect(true);
        return;
      }

      // Only allow existing providers to proceed unchanged
      if (provider !== "amazon" && provider !== "shopify") return;

      // Let dashboard handle BOTH header & page events (existing behavior)
      chooseIntegration(provider as Provider, origin);
    };

    window.addEventListener("integration:choose", handler as EventListener);
    return () => {
      window.removeEventListener("integration:choose", handler as EventListener);
    };
  }, []);

  const onConnectOrSyncAds = async () => {
    // safe stub: only for UI opening; wire to API later
    try {
      setAdsConnecting(true);
      setAdsError(null);
      console.log("Amazon Ads Connect/Sync clicked");
    } catch (err) {
      console.error(err);
      setAdsError("Amazon Ads action failed");
    } finally {
      setAdsConnecting(false);
    }
  };

  // Fetch Shopify store info
  useEffect(() => {
    const fetchShopifyStore = async () => {
      try {
        setShopifyLoading(true);

        const token = reduxToken; // from useSelector
        console.log("JWT from Redux:", token);
        console.log("API BASE URL:", process.env.NEXT_PUBLIC_API_BASE_URL);

        if (!token) {
          console.log("No JWT found in Redux store.");
          return;
        }

        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/shopify/store`,
          {
            headers: {
              Authorization: `Bearer ${token}`,
            },
          }
        );

        const contentType = res.headers.get("content-type") || "";

        // If backend failed and returned HTML, avoid res.json() crash
        if (!contentType.includes("application/json")) {
          const text = await res.text();
          console.error("Non-JSON response from /shopify/store:", text);
          return;
        }

        const data = await res.json();
        console.log("Shopify store from backend:", data);

        if (!res.ok || data?.error) {
          return;
        }

        setShopifyStore(data);
      } catch (err) {
        console.error("Error fetching Shopify store:", err);
      } finally {
        setShopifyLoading(false);
      }
    };

    fetchShopifyStore();
  }, [reduxToken]);

  // Close Amazon Finance modal on country / integration change
  useEffect(() => {
    setOpenAmazonFinance(false);
  }, [selectedCountry, integrationMethod]);

  const steps = useMemo(() => {
    const step1Done = fileUploaded || skuCompleted;

    const manual = [
      {
        id: 1,
        completed: step1Done,
        enabled: true,
        action: () => setActivePopup(1),
      },
      { id: 2, completed: !!integrationMethod, enabled: step1Done },
      {
        id: 3,
        completed: profileExists,
        enabled: !!integrationMethod,
        action: () => setActivePopup(2),
      },
      {
        id: 4,
        completed: mtdUploaded,
        enabled: profileExists,
        action: () => setActivePopup(3),
      },
    ];

    const amazon = [
      {
        id: 1,
        completed: step1Done,
        enabled: true,
        action: () => setActivePopup(1),
      },
      {
        id: 2,
        completed: !!integrationMethod && amazonConnected,
        enabled: step1Done,
      },
      {
        id: 3,
        completed: mtdUploaded,
        enabled: !!integrationMethod && amazonConnected,
        action: () => setActivePopup(3),
      },
    ];

    return integrationMethod === "amazon" ? amazon : manual;
  }, [
    integrationMethod,
    amazonConnected,
    fileUploaded,
    skuCompleted,
    profileExists,
    mtdUploaded,
  ]);

  // shopify orders url
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

  const chooseIntegration = (key: Provider, origin: Origin = "page") => {
    if (origin === "header" && key === "amazon") {
      setShowAmazonLegacyConnect(true);
      return;
    }

    setIntegrationMethod(key);
    setActivePopup(null);

    setShopifyStage("none");

    if (key === "amazon") {
      setShowAmazonConnect(true);
      return;
    }

    if (key === "shopify") {
      if (isShopifyConnected) {
        const url = buildShopifyOrdersUrl();
        console.log("Shopify already connected, redirecting to:", url);
        router.push(url);
        return;
      }

      console.log("No Shopify access token – opening Shopify intro modal");
      setShopifyStage("intro");
    }
  };

  return (

    <div className="font-lato box-border">
      <PageBreadcrumb
        pageTitle="Start your Journey with Phormula!"
        variant="page"
        textSize="2xl"
        align="left"
        className="mb-4"
      />



      {/* ✅ NEW: Amazon Ads modal render (independent of existing flows) */}
      {showAmazonAdsConnect && (
        <AmazonAdsConnectModal
          isOpen={showAmazonAdsConnect}
          onClose={() => setShowAmazonAdsConnect(false)}
          adsStatusLoading={adsStatusLoading}
          adsStatus={adsStatus}
          adsConnecting={adsConnecting}
          adsError={adsError}
          onConnectOrSync={onConnectOrSyncAds}
        />
      )}

      {/* Step 1 */}
      <Step1ProductList
        completed={steps[0].completed}
        onOpen={() => steps[0].enabled && steps[0].action?.()}
      />

      {/* Step 2 */}
      <Step2Integration
        locked={!steps[1].enabled}
        completed={steps[1].completed}
        onChoose={(key) => chooseIntegration(key, "page")}
      />

      {/* Manual flow */}
      {integrationMethod === "manual" && (
        <>
          <Step3FeePreview
            enabled={steps[2].enabled}
            completed={steps[2].completed}
            onOpen={() => steps[2].enabled && steps[2].action?.()}
          />

          <Step4MTD
            enabled={steps[3]?.enabled}
            completed={steps[3]?.completed}
            selectedCountry={selectedCountry}
            onOpenForCountry={(code) => {
              if (code !== selectedCountry) return;
              setMtdCountry(code);
              setActivePopup(3);
            }}
          />
        </>
      )}

      {/* Amazon flow */}
      {integrationMethod === "amazon" && (
        <Step3AmazonFinancial
          enabled={steps[2].enabled}
          completed={steps[2].completed}
          onOpen={() => setOpenAmazonFinance(true)}
        />
      )}

      {/* Step 1 modal (SKU upload) */}
      {activePopup === 1 && (
        <Modal
          isOpen
          onClose={() => setActivePopup(null)}
          className="m-4 max-w-xl"
          showCloseButton
        >
          <div className="relative w-full rounded-3xl shadow-[6px_6px_7px_0px_#00000026] border border-[#D9D9D9] p-4 no-scrollbar dark:bg-gray-900 lg:p-11">
            <SkuMultiCountryUpload
              onClose={() => setActivePopup(null)}
              onComplete={async () => {
                setSkuCompleted(true);
                try {
                  await refetchFileStatus();
                } catch {
                  /* ignore */
                }
                setActivePopup(null);
              }}
            />
          </div>
        </Modal>
      )}

      {/* Step 3 modal (Fee Preview) */}
      {activePopup === 2 && (
        <Modal
          isOpen
          onClose={() => setActivePopup(null)}
          className="m-4 max-w-[800px]"
          showCloseButton
        >
          <div className="relative w-full rounded-3xl bg-white p-4 no-scrollbar dark:bg-gray-900 lg:p-11">
            <FeepreviewUpload
              country={selectedCountry}
              onClose={() => setActivePopup(null)}
            />
          </div>
        </Modal>
      )}

      {/* Step 4 modal (MTD Upload) */}
      {activePopup === 3 && (
        <Modal
          isOpen
          onClose={() => setActivePopup(null)}
          className="m-4 max-w-3xl"
          showCloseButton
        >
          <FileUploadForm
            initialCountry={mtdCountry || selectedCountry}
            onClose={() => {
              setActivePopup(null);
            }}
            onComplete={() => {
              setMtdUploaded(true);
              setActivePopup(null);
            }}
          />
        </Modal>
      )}

      {/* Amazon connect (page flow) */}
      {showAmazonConnect && (
        <Modal
          isOpen
          onClose={() => setShowAmazonConnect(false)}
          className="m-4 z-99999 max-w-xl"
          showCloseButton
        >
          <AmazonConnect
            onClose={() => setShowAmazonConnect(false)}
            onConnected={(refreshToken?: string) => {
              if (typeof window !== "undefined") {
                localStorage.setItem(
                  LS_KEYS.amazonRefreshToken(selectedCountry),
                  String(refreshToken ?? "")
                );
              }
              setAmazonConnected(true);
              setShowAmazonConnect(false);
            }}
            onChooseManual={() => {
              setShowAmazonConnect(false);
              setIntegrationMethod("manual");
            }}
          />
        </Modal>
      )}

      {/* Amazon LEGACY connect (header flow) */}
      {showAmazonLegacyConnect && (
        <Modal
          isOpen
          onClose={() => setShowAmazonLegacyConnect(false)}
          className="m-4 z-99999 max-w-xl"
          showCloseButton
        >
          <AmazonConnectLegacy
            onClose={() => setShowAmazonLegacyConnect(false)}
            onConnected={(refreshToken?: string) => {
              if (typeof window !== "undefined") {
                localStorage.setItem(
                  LS_KEYS.amazonRefreshToken(selectedCountry),
                  String(refreshToken ?? "")
                );
              }
              setAmazonConnected(true);
              setShowAmazonLegacyConnect(false);
            }}
          />
        </Modal>
      )}

      {/* Amazon financial dashboard */}
      {integrationMethod === "amazon" && amazonConnected && openAmazonFinance && (
        <div
          className="fixed inset-0 z-[100000] flex items-center justify-center p-4"
          role="dialog"
          aria-modal="true"
        >
          <div className="absolute inset-0 bg-black/40 " />
          <div className="relative w-full max-w-xl rounded-xl bg-white p-4 dark:border-gray-800 dark:bg-gray-900">
            <div className="mt-3">
              <AmazonFinancialDashboard
                onClose={() => setOpenAmazonFinance(false)}
              />
            </div>
          </div>
        </div>
      )}

      {/* Shopify intro (first step) */}
      {shopifyStage === "intro" && !isShopifyConnected && (
        <ShopifyIntroModal
          onClose={() => setShopifyStage("none")}
          onManual={() => {
            setShopifyStage("manual");
          }}
        />
      )}

      {/* Shopify connect (manual store name) */}
      {shopifyStage === "manual" && !isShopifyConnected && (
        <ConnectShopifyModal onClose={() => setShopifyStage("none")} />
      )}
    </div>
  );
}
