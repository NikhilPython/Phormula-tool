"use client";

import React, { useEffect } from "react";
import { useModal } from "../../hooks/useModal";
import { Modal } from "../ui/modal";
import { MdEdit } from "react-icons/md";
import { TiUpload } from "react-icons/ti";
import { useGetCountriesQuery } from "@/lib/api/profileApi";
import FeepreviewUpload from "../ui/modal/FeepreviewUpload";
import SkuMultiCountryUpload from "../ui/modal/SkuMultiCountryUpload";
import { useSelector } from "react-redux";
import PageBreadcrumb from "../common/PageBreadCrumb";

function InfoCard({
  title,
  children,
  action,
}: {
  title: React.ReactNode;
  children: React.ReactNode;
  action?: React.ReactNode;
}) {
  return (
    <div className="rounded-2xl border border-gray-200 bg-white p-4 dark:border-gray-800 dark:bg-gray-900">
      <div className="mb-3 flex items-center justify-between">
        <div className="text-sm font-semibold text-gray-800 dark:text-white/90">
          {title}
        </div>
        {action}
      </div>
      {children}
    </div>
  );
}

export default function UserAddressCard() {
  // Fee Preview modal state
  const feeModal = useModal();
  const [selectedCountry, setSelectedCountry] = React.useState<string | null>(
    null
  );

  // SKU Upload modal state
  const skuModal = useModal();

  // Access state from Redux store
  const userData = useSelector((state: any) => state.auth.user);

  useEffect(() => {
    console.log("User Data from Redux (auth.user):", userData);
  }, [userData]);

  // Countries for Fee Preview chips
  const { data, isLoading, isError, error } = useGetCountriesQuery();
  const countries: string[] = data?.countries ?? [];

  const openFeePreview = (country: string) => {
    setSelectedCountry(country);
    feeModal.openModal();
  };
  const closeFeePreview = () => {
    setSelectedCountry(null);
    feeModal.closeModal();
  };

  return (
    <>
      {/* ✅ ONLY Product Information wrapped in InfoCard */}
      <InfoCard
        title={
          <PageBreadcrumb
            pageTitle="Product & Inventory Controls"
            variant="table"
            align="left"
          // textSize="base"
          />
        }
      >
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2 lg:gap-7 2xl:gap-x-32">
          {/* SKU Information */}
          <div className="flex items-center gap-2">
            <p className="text-sm font-bold leading-normal text-charcoal-500">
              SKU Information
            </p>

            <div className="flex items-center gap-2">
              <button
                onClick={skuModal.openModal}
                className="
        inline-flex items-center gap-1.5
        rounded-md
        px-2 py-1
        text-xs font-semibold text-gray-700
        hover:bg-gray-100
        dark:text-gray-200 dark:hover:bg-gray-800
      "
                aria-label="Upload SKU"
                title="Upload SKU"
              >
                <TiUpload size={14} />
                {/* <span>Upload</span> */}
              </button>


            </div>
          </div>


          {/* Update Fee Preview Information */}
          <div>
            <p className="mb-2 text-sm leading-normal text-charcoal-500 font-bold">
              Warehouse Inventory            </p>

            {/* Uncomment when you want chips */}
            {/* {isLoading && (
              <p className="text-sm text-gray-500 dark:text-gray-400">
                Loading countries…
              </p>
            )}

            {isError && (
              <p className="text-sm text-red-500">
                {(error as any)?.data?.message || "Failed to load countries."}
              </p>
            )}

            {!isLoading && !isError && countries.length === 0 && (
              <p className="text-sm font-medium text-gray-800 dark:text-white/90">
                No countries found.
              </p>
            )}

            {!isLoading && !isError && countries.length > 0 && (
              <div className="flex flex-wrap gap-2">
                {countries.map((country) => (
                  <div
                    key={country}
                    className="flex items-center gap-2 rounded-full border border-gray-300 bg-gray-50 px-3 py-1 text-xs font-medium text-gray-700 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300"
                  >
                    <span>{country.toUpperCase()}</span>
                    <button
                      onClick={() => openFeePreview(country)}
                      className="p-1 transition-colors hover:text-teal-600"
                      title={`View Fee Preview for ${country}`}
                    >
                      <MdEdit size={14} />
                    </button>
                  </div>
                ))}
              </div>
            )} */}
          </div>
        </div>
      </InfoCard>

      {/* 🔵 Fee Preview Modal */}
      <Modal
        isOpen={feeModal.isOpen}
        onClose={closeFeePreview}
        className="m-4 max-w-[800px] shadow-[6px_6px_7px_0px_#00000026] border border-[#D9D9D9]"
      >
        <div className="relative w-full rounded-3xl bg-white p-4 no-scrollbar dark:bg-gray-900 lg:p-11">
          {selectedCountry ? (
            <FeepreviewUpload
              country={selectedCountry}
              onClose={closeFeePreview}
            />
          ) : (
            <p className="text-center text-gray-500 dark:text-gray-400">
              No country selected
            </p>
          )}
        </div>
      </Modal>

      {/* 🟣 SKU Upload Modal */}
      <Modal
        isOpen={skuModal.isOpen}
        onClose={skuModal.closeModal}
        className="m-4 max-w-[500px] shadow-[6px_6px_7px_0px_#00000026] border border-[#D9D9D9]"
      >
        <div className="relative w-full rounded-xl bg-white/30 p-4 no-scrollbar dark:bg-gray-900 lg:p-9">
        <SkuMultiCountryUpload
  onClose={skuModal.closeModal}
  onComplete={() => {
    skuModal.closeModal(); // upload ke baad modal band
  }}
/>
        </div>
      </Modal>
    </>
  );
}
