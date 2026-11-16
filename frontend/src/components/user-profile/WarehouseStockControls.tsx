"use client";

import React, { useEffect } from "react";
import * as XLSX from "xlsx";
import { FiCloud, FiDownload, FiLock } from "react-icons/fi";
import { Modal } from "@/components/ui/modal";
import Button from "@/components/ui/button/Button";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import WarehouseMultiCountryUpload from "@/components/ui/modal/WarehouseMultiCountryUpload";
import { useModal } from "@/hooks/useModal";
import { useLazyGetCurrentSkuSheetQuery } from "@/lib/api/skuApi";

export default function WarehouseStockControls() {
  const uploadModal = useModal();
  const [getCurrentSkuSheet, { data, isFetching }] =
    useLazyGetCurrentSkuSheetQuery();
  const hasSkuSheet = Boolean(data?.rows?.length);

  const downloadTemplate = () => {
    const workbook = XLSX.utils.book_new();
    const worksheet = XLSX.utils.aoa_to_sheet([
      ["S.No.", "Product Name", "Country", "SKU", "Local Stock"],
      [1, "Classic", "UK", "BV-6X5T-6CY1", 1000],
      [2, "Classic", "US", "SEMNIW1", 1000],
    ]);
    XLSX.utils.book_append_sheet(workbook, worksheet, "Warehouse Stock");
    XLSX.writeFile(workbook, "Warehouse_Upload_Template.xlsx");
  };

  useEffect(() => {
    void getCurrentSkuSheet();
  }, [getCurrentSkuSheet]);

  return (
    <>
      <section className="warehouse-panel flex h-full flex-col rounded-2xl border border-[#E3E8E5] bg-white shadow-[0_12px_34px_rgba(39,49,64,0.055)]">
        <div className="flex h-full flex-col p-4">
          <div className="flex items-center justify-between gap-3">
            <PageBreadcrumb
              pageTitle="Warehouse Local Stock"
              variant="table"
              align="left"
            />
            <span
              className={`shrink-0 rounded-full px-2.5 py-1 text-[10px] font-bold ${
                hasSkuSheet
                  ? "bg-[#E8F7EF] text-[#087A51]"
                  : "bg-[#F1F3F2] text-[#74807A]"
              }`}
            >
              {isFetching ? "Loading" : hasSkuSheet ? "Ready" : "SKU required"}
            </span>
          </div>

          <p className="mt-1.5 text-sm font-bold text-charcoal-500">
            {hasSkuSheet ? "Update marketplace Local Stock" : "Upload a SKU sheet first"}
          </p>
          <p className="mt-1 text-xs leading-4 text-charcoal-500">
            Upload warehouse quantities by country and SKU. Matching stock values are applied to your product records.
          </p>

          <button
            type="button"
            disabled={!hasSkuSheet || isFetching}
            onClick={uploadModal.openModal}
            className={`mt-3 flex min-h-[118px] w-full flex-1 flex-col items-center justify-center rounded-2xl border border-dashed px-4 py-3 text-center transition ${
              hasSkuSheet
                ? "border-[#B9D8C9] bg-[#FBFEFC] hover:border-green-500 hover:bg-[#F4FBF7]"
                : "cursor-not-allowed border-[#DDE2DF] bg-[#F5F6F5] opacity-70"
            }`}
          >
            {hasSkuSheet ? (
              <FiCloud className="text-green-500" size={28} />
            ) : (
              <FiLock className="text-[#8D9792]" size={23} />
            )}
            <span className="mt-2 text-sm font-bold text-[#37455F]">
              {hasSkuSheet ? "Upload warehouse stock" : "Warehouse upload locked"}
            </span>
            <span className="mt-1 text-xs text-charcoal-500">
              {hasSkuSheet ? "Click to choose an .xlsx or .xls file" : "Upload your SKU sheet to continue"}
            </span>
          </button>

          <div className="mt-2 flex justify-center">
            <button
              type="button"
              disabled={!hasSkuSheet}
              onClick={downloadTemplate}
              className="inline-flex items-center gap-1.5 text-[13px] font-semibold text-green-500 transition hover:text-[#4a907a] disabled:cursor-not-allowed disabled:opacity-50"
            >
              Download stock format <FiDownload size={14} />
            </button>
          </div>
        </div>
      </section>

      <Modal
        isOpen={uploadModal.isOpen}
        onClose={uploadModal.closeModal}
        className="m-4 max-w-2xl w-full border border-[#D9D9D9] shadow-[6px_6px_7px_0px_#00000026]"
      >
        <div className="relative w-full rounded-2xl bg-white p-5 dark:bg-gray-900 lg:p-6">
          <WarehouseMultiCountryUpload
            countryName="global"
            marketplaceCountry="us"
            onClose={uploadModal.closeModal}
            onComplete={() => {
              uploadModal.closeModal();
              void getCurrentSkuSheet();
            }}
          />
        </div>
      </Modal>
    </>
  );
}
