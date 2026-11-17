"use client";

import React, { useEffect, useMemo, useState } from "react";
import * as XLSX from "xlsx";
import { FiCloud, FiDownload, FiEye, FiLock, FiX } from "react-icons/fi";
import { Modal } from "@/components/ui/modal";
import DataTable, { type ColumnDef, type Row } from "@/components/ui/table/DataTable";
import Button from "@/components/ui/button/Button";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import WarehouseMultiCountryUpload from "@/components/ui/modal/WarehouseMultiCountryUpload";
import { useModal } from "@/hooks/useModal";
import { useLazyGetCurrentSkuSheetQuery } from "@/lib/api/skuApi";

export default function WarehouseStockControls({ country = "us" }: { country?: string }) {
  const uploadModal = useModal();
  const viewModal = useModal();
  const [warehouseRows, setWarehouseRows] = useState<Record<string, any>[]>([]);
  const [warehouseLoading, setWarehouseLoading] = useState(false);
  const [warehouseError, setWarehouseError] = useState(false);
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

  const loadWarehouseSheet = async () => {
    viewModal.openModal();
    setWarehouseLoading(true);
    setWarehouseError(false);
    try {
      const token = localStorage.getItem("jwtToken");
      if (!token) throw new Error("Authorization token is missing");
      const marketplaceRows = await Promise.all(["uk", "us"].map(async (marketplace) => {
        const response = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/uploadWarehouseData?country=${marketplace}`,
          { headers: { Authorization: `Bearer ${token}` } }
        );
        if (!response.ok) throw new Error(`Unable to load ${marketplace.toUpperCase()} warehouse stock`);
        const result = await response.json();
        return (Array.isArray(result?.data) ? result.data : []).map((row: Record<string, any>) => ({
          ...row,
          __marketplace: marketplace,
        }));
      }));
      setWarehouseRows(marketplaceRows.flat());
    } catch (error) {
      console.error("Failed to load warehouse sheet:", error);
      setWarehouseError(true);
      setWarehouseRows([]);
    } finally {
      setWarehouseLoading(false);
    }
  };

  const displayRows = useMemo(() => [...warehouseRows]
    .sort((a, b) => {
      const aSequence = Number(a.s_no ?? a["S. No."]);
      const bSequence = Number(b.s_no ?? b["S. No."]);
      if (Number.isFinite(aSequence) && Number.isFinite(bSequence) && aSequence !== bSequence) {
        return aSequence - bSequence;
      }
      return 0;
    })
    .flatMap((row) => {
    const marketplace = row.__marketplace === "us" ? "us" : "uk";
    const countryValue = String(row.country ?? row.Country ?? "").toLowerCase();
    const countrySku = row[`sku_${marketplace}`] ?? row[`SKU_${marketplace.toUpperCase()}`];
    const countryStock = row[`local_stock_${marketplace}`] ?? row[`Local Stock ${marketplace.toUpperCase()}`];
    const hasCountrySpecificData = countrySku !== undefined || countryStock !== undefined;
    if (!hasCountrySpecificData && countryValue && !countryValue.includes(marketplace)) return [];

    const sku = countrySku ?? row.sku ?? row.SKU ?? "";
    if (!sku) return [];
    return [{
      "Product Name": row.product_name ?? row["Product Name"] ?? "",
      Country: marketplace.toUpperCase(),
      SKU: sku,
      "Local Stock": countryStock ?? row.local_stock ?? row["Local Stock"] ?? "",
      "Last Updated": row.last_update ?? row.last_updated ?? row["Last Updated"] ?? "",
    }];
  }).map((row, index) => ({ "S. No.": index + 1, ...row })), [warehouseRows]);

  const warehouseColumns: ColumnDef<Row>[] = useMemo(() => [
    { key: "S. No.", header: "S. No.", width: "70px", cellClassName: "text-center" },
    { key: "Product Name", header: "Product Name", width: "220px", cellClassName: "text-left" },
    { key: "Country", header: "Country", width: "100px", cellClassName: "text-center" },
    { key: "SKU", header: "SKU", width: "180px", cellClassName: "text-left" },
    { key: "Local Stock", header: "Local Stock", width: "140px", cellClassName: "text-center" },
  ], []);

  const downloadWarehouseSheet = () => {
    if (!displayRows.length) return;
    const worksheet = XLSX.utils.json_to_sheet(displayRows);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Warehouse Stock");
    XLSX.writeFile(workbook, "Warehouse Stock.xlsx");
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

          <div className="mt-2 flex items-center justify-center gap-4">
            <button
              type="button"
              disabled={!hasSkuSheet}
              onClick={downloadTemplate}
              className="inline-flex items-center gap-1.5 text-[13px] font-semibold text-green-500 transition hover:text-[#4a907a] disabled:cursor-not-allowed disabled:opacity-50"
            >
              Download stock format <FiDownload size={14} />
            </button>
            <span className="h-4 border-l border-[#DDE5E1]" />
            <button
              type="button"
              disabled={!hasSkuSheet || isFetching}
              onClick={() => void loadWarehouseSheet()}
              className="inline-flex items-center gap-1.5 text-[13px] font-semibold text-green-500 transition hover:text-[#4a907a] disabled:cursor-not-allowed disabled:opacity-50"
            >
              View your sheet <FiEye size={14} />
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

      <Modal
        isOpen={viewModal.isOpen}
        onClose={viewModal.closeModal}
        className="m-4 w-full max-w-6xl border border-[#D9D9D9] shadow-[6px_6px_7px_0px_#00000026]"
      >
        <div className="w-full rounded-2xl bg-white p-5 lg:p-6">
          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-[#E8ECEA] pb-3">
            <div>
              <h2 className="text-base font-bold text-[#37455F]">View your warehouse stock</h2>
              <p className="mt-1 text-xs text-[#78847E]">Your currently uploaded warehouse stock information.</p>
            </div>
            <div className="flex items-center gap-2">
              <button type="button" onClick={downloadWarehouseSheet} disabled={!displayRows.length} title="Download warehouse sheet" aria-label="Download warehouse sheet" className="flex h-9 w-9 items-center justify-center rounded-lg border border-[#B9D8C9] bg-[#F8FCFA] text-[#0E8558] disabled:opacity-50">
                <FiDownload size={16} />
              </button>
              <button type="button" onClick={viewModal.closeModal} title="Close" aria-label="Close warehouse sheet" className="flex h-9 w-9 items-center justify-center rounded-lg border border-[#DDE5E1] text-[#66746D]"><FiX size={16} /></button>
            </div>
          </div>
          {warehouseLoading ? (
            <div className="grid min-h-[300px] place-items-center text-sm text-[#78847E]">Loading your warehouse sheet...</div>
          ) : warehouseError ? (
            <div className="grid min-h-[300px] place-items-center text-center">
              <div><p className="text-sm font-semibold text-[#B75A5A]">Could not load your warehouse sheet</p><button type="button" onClick={() => void loadWarehouseSheet()} className="mt-3 text-xs font-semibold text-green-500">Try again</button></div>
            </div>
          ) : !displayRows.length ? (
            <div className="grid min-h-[300px] place-items-center text-sm text-[#78847E]">No warehouse stock data found.</div>
          ) : (
            <DataTable
              columns={warehouseColumns}
              data={displayRows as Row[]}
              pageSize={10}
              stickyHeader
              zebra
              emptyMessage="No warehouse stock data found."
              className="mt-4 w-full max-w-full min-w-0"
              tableClassName="[&_th]:whitespace-nowrap [&_td]:whitespace-nowrap"
            />
          )}
        </div>
      </Modal>
    </>
  );
}
